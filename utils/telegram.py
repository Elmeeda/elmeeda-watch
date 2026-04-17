"""Thin Telegram notifier for samsara-collector run summaries.

Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from the environment. If
either is missing the notifier is a no-op so local runs don't require
credentials. The chat_id is normalised to the ``-100<id>`` supergroup
format when someone drops the prefix (Telegram channel ids are always
negative).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Iterable

import requests

logger = logging.getLogger(__name__)


def _resolve_chat_id(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Accept @handle, -100..., negative ids, and bare positives (auto-prefix).
    if s.startswith("@"):
        return s
    try:
        n = int(s)
    except ValueError:
        return s
    if n >= 0:
        return f"-100{n}"
    return str(n)


def _format_table(rows: Iterable[tuple[str, str]]) -> str:
    """Fixed-width key/value table (monospace-friendly)."""
    rows = list(rows)
    if not rows:
        return ""
    key_width = max(len(str(k)) for k, _ in rows)
    lines = [f"{str(k).ljust(key_width)}  {v}" for k, v in rows]
    return "\n".join(lines)


def send_run_summary(
    *,
    job: str,
    stats: dict,
    tenants: dict[str, dict] | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> None:
    """Post a run summary to the configured Telegram chat. Swallows errors."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = _resolve_chat_id(os.getenv("TELEGRAM_CHAT_ID"))
    if not token or not chat_id:
        logger.debug("telegram_notifier_skipped (no credentials)")
        return

    completed_at = completed_at or datetime.now(timezone.utc)
    duration = ""
    if started_at:
        duration = f" · {(completed_at - started_at).total_seconds():.1f}s"

    lines: list[str] = []
    lines.append(f"*samsara-collector: `{job}`*")
    lines.append(f"_{completed_at.strftime('%Y-%m-%d %H:%M:%SZ')}{duration}_")

    # Top-level stats
    top_rows: list[tuple[str, str]] = []
    for k, v in (stats or {}).items():
        if isinstance(v, dict):
            continue  # nested → rendered below
        top_rows.append((str(k), str(v)))
    if top_rows:
        lines.append("```")
        lines.append(_format_table(top_rows))
        lines.append("```")

    # Per-tenant breakdown when provided
    if tenants:
        lines.append("*Per tenant*")
        lines.append("```")
        tenant_rows = []
        for tid, t in tenants.items():
            label = t.get("name") or tid[:8]
            if "summary" in t:
                tenant_rows.append((label, str(t["summary"])))
            else:
                tenant_rows.append((label, ", ".join(f"{k}={v}" for k, v in t.items() if k != "name")))
        lines.append(_format_table(tenant_rows))
        lines.append("```")

    # Nested sub-collector results (for collect-full)
    for k, v in (stats or {}).items():
        if not isinstance(v, dict):
            continue
        sub_rows = [(sk, str(sv)) for sk, sv in v.items() if not isinstance(sv, (dict, list))]
        if not sub_rows:
            continue
        lines.append(f"*{k}*")
        lines.append("```")
        lines.append(_format_table(sub_rows))
        lines.append("```")

    text = "\n".join(lines)
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": "true",
            },
            timeout=10,
        )
        if r.status_code != 200:
            logger.warning("telegram_notifier_bad_response | status=%s body=%s",
                           r.status_code, r.text[:200])
    except Exception as e:
        logger.warning("telegram_notifier_failed | err=%s", e)

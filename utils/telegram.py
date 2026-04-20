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

    stats = stats or {}
    context = stats.get("context") if isinstance(stats.get("context"), dict) else None

    # Top-level stats (skip keys rendered specially below)
    _special_keys = {"context"}
    top_rows: list[tuple[str, str]] = []
    for k, v in stats.items():
        if k in _special_keys or isinstance(v, dict):
            continue
        top_rows.append((str(k), str(v)))
    if top_rows:
        lines.append("```")
        lines.append(_format_table(top_rows))
        lines.append("```")

    # Informative zero-state for watch-faults — the realtime webhook's
    # catching everything, watcher has nothing to catch up on.
    if (
        job == "watch-faults"
        and stats.get("events_scanned", 1) == 0
        and stats.get("dispatches_created", 0) == 0
        and context
        and context.get("total", 0) > 0
    ):
        status_bits = ", ".join(f"{k}={v}" for k, v in context["by_status"].items())
        lines.append(
            f"_✓ All faults handled by realtime webhook — nothing for watcher to catch up "
            f"(last {context.get('window_minutes', 60)}m: {context.get('total')} events → {status_bits})_"
        )

    # Backend activity context (last-hour window, all pipeline_status buckets)
    if context and context.get("total"):
        win = context.get("window_minutes", 60)
        lines.append(f"*Backend activity · last {win}m*")
        rows = [(k, str(v)) for k, v in context.get("by_status", {}).items()]
        if rows:
            lines.append("```")
            lines.append(_format_table(rows))
            lines.append("```")

    # Top recent faults
    top_faults = (context or {}).get("top_faults") or []
    if top_faults:
        lines.append("*Top recent faults*")
        lines.append("```")
        fault_rows = []
        for f in top_faults:
            sev = f.get("severity")
            age = f.get("age_min")
            age_str = f"{age}m ago" if age is not None else "?"
            label = f"#{f.get('unit') or '-'}  {f.get('code')}"
            tail = f"sev {sev if sev is not None else '-'}  {f.get('category') or '-'}  [{f.get('status')}] {age_str}"
            fault_rows.append((label, tail))
        lines.append(_format_table(fault_rows))
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
                tenant_rows.append((label, ", ".join(f"{k}={v}" for k, v in t.items() if k not in ("name", "details"))))
        lines.append(_format_table(tenant_rows))
        lines.append("```")

        # Per-dispatch detail rows — only present when the watcher actually
        # created dispatches, so zero-state summaries stay terse.
        detail_rows: list[str] = []
        for tid, t in tenants.items():
            for d in t.get("details") or []:
                tag = t.get("name") or tid[:8]
                detail_rows.append(f"{tag}  {d}")
        if detail_rows:
            lines.append("*Dispatches created*")
            lines.append("```")
            lines.extend(detail_rows)
            lines.append("```")

    # Nested sub-collector results (for collect-full) — unchanged
    for k, v in stats.items():
        if k in _special_keys or not isinstance(v, dict):
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

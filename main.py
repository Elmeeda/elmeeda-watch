"""elmeeda-watch — Cloud Run Job entrypoint.

Scans Elmeeda's fault_code_change_events every few minutes and creates
pending_review breakdown_dispatches for high-severity faults that the
realtime webhook may have missed (restarts, network blips, retries).

Usage:
    python main.py watch-faults          # single pass, exit

Environment:
    DATABASE_URL                 Postgres DSN for the elmeeda_v1 DB
    TELEGRAM_BOT_TOKEN           optional — per-run summary
    TELEGRAM_CHAT_ID             optional — chat id (auto -100 prefix)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("elmeeda-watch")


def _watch_faults():
    from database import SessionLocal
    from services.breakdown_watcher import watch_fault_codes
    db = SessionLocal()
    try:
        return watch_fault_codes(db)
    finally:
        db.close()


COMMANDS = {
    "watch-faults": _watch_faults,
}


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "watch-faults"
    handler = COMMANDS.get(command)
    if not handler:
        print(__doc__)
        print(f"Available commands: {', '.join(COMMANDS.keys())}")
        sys.exit(1)

    from utils.telegram import send_run_summary

    started_at = datetime.now(timezone.utc)
    logger.info(f"Starting: {command}")
    try:
        result = handler()
        logger.info(f"Completed: {command} — {result}")
        try:
            stats, tenants = result if isinstance(result, tuple) else (result, None)
            send_run_summary(
                job=command,
                stats=stats if isinstance(stats, dict) else {"result": stats},
                tenants=tenants,
                started_at=started_at,
            )
        except Exception:
            logger.exception("telegram_summary_failed")
    except Exception:
        logger.exception(f"Failed: {command}")
        try:
            send_run_summary(job=command, stats={"status": "FAILED"}, started_at=started_at)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()

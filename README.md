# elmeeda-watch

Fault-code → breakdown-dispatch watcher. Scans Elmeeda's
`fault_code_change_events` every few minutes and creates
`pending_review` rows in `breakdown_dispatches` for high-severity
faults that the realtime Samsara webhook may have missed.

Lives as a Cloud Run Job triggered by Cloud Scheduler. Safety net —
the realtime webhook path (`Elmeeda-app/backend/routers/webhooks_samsara.py::_maybe_create_pending_dispatch`) handles new events first;
this backfills whatever slipped through.

## Logic

- SELECT from `fault_code_change_events` where
  `event_type IN ('appeared', 'recurred')`,
  `severity_score >= 80`,
  `pipeline_status IS NULL`,
  `detected_at > now() - 24h`.
- Dedup — skip if any non-terminal `breakdown_dispatches` row exists
  for the same vehicle within the last 1 hour. Matches the realtime
  webhook dedup so neither path double-creates.
- Create `breakdown_dispatches(status='pending_review')` with full
  vehicle metadata (`vin`, `year/make/model`, mileage, lat/lng).
- Mark the event `pipeline_status='dispatched_pending_review' | 'skipped' | 'below_threshold'` so subsequent runs don't reconsider.
- Emit a Telegram summary per run with per-tenant counts.

## Run locally

```bash
export DATABASE_URL='postgresql://user:pass@host/elmeeda_v1'
export TELEGRAM_BOT_TOKEN='…'
export TELEGRAM_CHAT_ID='-1003850294229'
python main.py watch-faults
```

## Deploy

```bash
./deploy.sh
```

Creates/updates:
- Cloud Run Job `samsara-watch` (name kept for scheduler continuity)
- Cloud Scheduler `samsara-job-watch` cron `*/5 * * * *` UTC

## Tickets
- ELM-212 — initial shipping (in samsara-collector)
- ELM-213 — extracted to its own repo

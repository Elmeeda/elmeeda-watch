"""samsara-watch: scan fault_code_change_events and auto-create pending_review
breakdown_dispatches for high-severity faults the webhook may have missed.

This is a belt-and-suspenders catch-up pass on top of the realtime webhook
path in `routers/webhooks_samsara._maybe_create_pending_dispatch`. If the
Samsara webhook fired cleanly, these events already have
pipeline_status='dispatched_pending_review' and we skip them.
"""

from __future__ import annotations

import json
import logging
import uuid as _uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


_SEVERITY_FLOOR = 80             # matches existing auto-dispatch threshold
_DEDUP_WINDOW_HOURS = 1          # skip if recent dispatch exists for same vehicle
_LOOKBACK_HOURS = 24             # don't dispatch events older than this


def _tenant_name(db: Session, tenant_id: str) -> str:
    try:
        row = db.execute(text("SELECT name FROM tenants WHERE id=:tid"),
                         {"tid": tenant_id}).fetchone()
        return (row[0] if row and row[0] else tenant_id[:8])
    except Exception:
        return tenant_id[:8]


def watch_fault_codes(db: Session):
    """Single pass over unprocessed fault_code_change_events. Returns
    (stats, per-tenant breakdown)."""
    stats = {
        "events_scanned": 0,
        "dispatches_created": 0,
        "dedup_skipped": 0,
        "below_threshold": 0,
        "errors": 0,
    }
    tenants: dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=_LOOKBACK_HOURS)

    # Pull candidate events — one query, cross-tenant
    rows = db.execute(text("""
        SELECT e.id, e.tenant_id, e.vehicle_id, e.unit_number,
               e.fault_code_id, e.severity_score, e.component_category,
               e.spn, e.fmi, e.dtc_code, e.detected_at
          FROM fault_code_change_events e
         WHERE e.event_type IN ('appeared', 'recurred')
           AND e.detected_at > :cutoff
           AND (e.pipeline_status IS NULL OR e.pipeline_status = '')
           AND (e.severity_score IS NOT NULL)
         ORDER BY e.severity_score DESC, e.detected_at DESC
         LIMIT 500
    """), {"cutoff": cutoff}).fetchall()

    stats["events_scanned"] = len(rows)
    if not rows:
        logger.info("samsara_watch: no candidate events")
        return stats, tenants

    for r in rows:
        (event_id, tenant_id, vehicle_id, unit_number, fault_code_id,
         severity_score, component_category, spn, fmi, dtc_code, _detected) = r

        tb = tenants.setdefault(tenant_id, {
            "name": _tenant_name(db, tenant_id),
            "created": 0,
            "dedup": 0,
            "below": 0,
        })

        if (severity_score or 0) < _SEVERITY_FLOOR:
            stats["below_threshold"] += 1
            tb["below"] += 1
            # Mark processed so we don't rescan every run
            db.execute(text("""
                UPDATE fault_code_change_events
                   SET pipeline_status = 'below_threshold',
                       pipeline_completed_at = :now, updated_at = :now
                 WHERE id = :eid
            """), {"eid": event_id, "now": now})
            continue

        try:
            # Dedup — any non-terminal dispatch for this vehicle recently?
            existing = db.execute(text("""
                SELECT id FROM breakdown_dispatches
                 WHERE tenant_id = :tid AND vehicle_id = :vid
                   AND created_at > :cutoff
                   AND status NOT IN ('cancelled', 'completed', 'failed')
                 LIMIT 1
            """), {
                "tid": tenant_id, "vid": vehicle_id,
                "cutoff": now - timedelta(hours=_DEDUP_WINDOW_HOURS),
            }).fetchone()

            if existing:
                db.execute(text("""
                    UPDATE fault_code_change_events
                       SET pipeline_status = 'skipped',
                           breakdown_dispatch_id = :did,
                           pipeline_result = :result,
                           pipeline_completed_at = :now,
                           updated_at = :now
                     WHERE id = :eid
                """), {
                    "eid": event_id, "did": existing[0], "now": now,
                    "result": json.dumps({"reason": "duplicate_dispatch",
                                          "source": "samsara-watch"}),
                })
                stats["dedup_skipped"] += 1
                tb["dedup"] += 1
                continue

            # Fetch vehicle metadata for the dispatch row
            vehicle = db.execute(text("""
                SELECT vin, year, make, model, engine_make, engine_model,
                       COALESCE(current_odometer, current_mileage) AS mileage,
                       last_location_lat, last_location_lon, last_location_description
                  FROM fleet_vehicles
                 WHERE id = :vid AND tenant_id = :tid
            """), {"vid": vehicle_id, "tid": tenant_id}).fetchone()

            parts = []
            if spn is not None: parts.append(f"SPN {spn}")
            if fmi is not None: parts.append(f"FMI {fmi}")
            if dtc_code: parts.append(f"DTC {dtc_code}")
            if component_category: parts.append(f"({component_category})")
            description = f"Auto-detected critical fault: {' '.join(parts)}"

            fc_desc = db.execute(text(
                "SELECT full_description FROM vehicle_fault_codes "
                "WHERE id = :id AND tenant_id = :tid"
            ), {"id": fault_code_id, "tid": tenant_id}).fetchone()
            if fc_desc and fc_desc[0]:
                description += f" -- {fc_desc[0]}"

            dispatch_id = str(_uuid.uuid4())
            lat = float(vehicle[7]) if vehicle and vehicle[7] is not None else None
            lon = float(vehicle[8]) if vehicle and vehicle[8] is not None else None
            addr = (vehicle[9] if vehicle else None) or ""

            db.execute(text("""
                INSERT INTO breakdown_dispatches
                  (id, tenant_id, vehicle_id, orchestrator_incident_id, status,
                   driver_name, driver_phone, location_lat, location_lng,
                   location_address, breakdown_description, maintenance_category,
                   created_at, updated_at)
                VALUES
                  (:id, :tid, :vid, :oid, 'pending_review',
                   'Fleet Driver', 'not provided', :lat, :lng, :addr,
                   :desc, :cat, :now, :now)
            """), {
                "id": dispatch_id, "tid": tenant_id, "vid": vehicle_id,
                "oid": f"pending-review-{dispatch_id[:8]}",
                "lat": lat, "lng": lon, "addr": addr,
                "desc": description, "cat": component_category,
                "now": now,
            })

            db.execute(text("""
                UPDATE fault_code_change_events
                   SET pipeline_status = 'dispatched_pending_review',
                       breakdown_dispatch_id = :did,
                       pipeline_completed_at = :now,
                       updated_at = :now
                 WHERE id = :eid
            """), {"eid": event_id, "did": dispatch_id, "now": now})

            stats["dispatches_created"] += 1
            tb["created"] += 1
            logger.info(
                "samsara_watch_dispatched | tenant=%s unit=%s spn=%s fmi=%s severity=%s dispatch=%s",
                tenant_id, unit_number, spn, fmi, severity_score, dispatch_id,
            )
        except Exception as e:
            stats["errors"] += 1
            logger.error(
                "samsara_watch_failed | tenant=%s event=%s err=%s",
                tenant_id, event_id, e,
            )
            try: db.rollback()
            except Exception: pass

    try:
        db.commit()
    except Exception as e:
        logger.error("samsara_watch_commit_failed: %s", e)
        db.rollback()

    # Build tenant summaries for Telegram
    out: dict[str, dict] = {}
    for tid, t in tenants.items():
        parts = []
        if t["created"]: parts.append(f"{t['created']} new")
        if t["dedup"]:   parts.append(f"{t['dedup']} dedup")
        if t["below"]:   parts.append(f"{t['below']} below")
        out[tid] = {"name": t["name"], "summary": ", ".join(parts) or "nothing"}

    logger.info(f"samsara-watch: {stats}")
    return stats, out

"""``/api/health`` 与 ``/api/meta``：应用存活与数据新鲜度概览（SPEC §3.1/§3.2）。"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter

from .. import db
from ..config import settings
from ..serialization import as_int, as_iso

router = APIRouter()


@router.get("/health")
def health() -> dict[str, object]:
    """纯存活探针：不查库，MySQL 抖动时容器仍可被判定为存活。"""
    return {"status": "ok", "ts": as_iso(datetime.now(UTC))}


@router.get("/meta")
def meta() -> dict[str, object]:
    traffic = db.table_ref(settings.dw_database, "ads_traffic_daily")
    publish_log = db.table_ref(settings.dw_database, "ads_publish_log")
    quality_run = db.table_ref(settings.dw_database, "quality_check_run")
    rt_traffic = db.table_ref(settings.rt_database, "rt_traffic_daily")
    rt_funnel = db.table_ref(settings.rt_database, "rt_funnel_daily")
    rt_behavior = db.table_ref(settings.rt_database, "rt_item_behavior_daily")
    rt_recon = db.table_ref(settings.rt_database, "rt_reconciliation_daily")

    bounds = db.POOL.fetch_one(
        f"SELECT MIN(event_date_utc) AS date_min, MAX(event_date_utc) AS date_max, "
        f"COALESCE(SUM(CASE WHEN is_full_day = 1 THEN 1 ELSE 0 END), 0) AS full_days "
        f"FROM {traffic}"
    )
    partial_rows = db.POOL.fetch_all(
        f"SELECT event_date_utc FROM {traffic} WHERE is_full_day = 0 ORDER BY event_date_utc"
    )

    publish = db.POOL.fetch_one(
        f"SELECT publish_id, scope_type, dates_csv, quality_status, published_at "
        f"FROM {publish_log} ORDER BY publish_id DESC LIMIT 1"
    )
    quality = db.POOL.fetch_one(
        f"SELECT run_id, status, rules_total, rules_passed, rules_failed, checks_total "
        f"FROM {quality_run} ORDER BY run_id DESC LIMIT 1"
    )

    rt_stats = db.POOL.fetch_one(
        f"SELECT COUNT(*) AS row_count, "
        f"COALESCE(SUM(is_final), 0) AS final_rows, "
        f"MAX(updated_at) AS updated_at "
        f"FROM {rt_traffic}"
    )
    latest_rt_update = db.POOL.fetch_one(
        f"SELECT MAX(updated_at) AS latest_updated_at FROM ("
        f"SELECT MAX(updated_at) AS updated_at FROM {rt_traffic} "
        f"UNION ALL SELECT MAX(updated_at) FROM {rt_funnel} "
        f"UNION ALL SELECT MAX(updated_at) FROM {rt_behavior}"
        f") AS rt_updates"
    )
    recon_rows = db.POOL.fetch_all(
        f"SELECT status, COUNT(*) AS cnt FROM {rt_recon} GROUP BY status"
    )
    reconciliation = {"MATCH": 0, "DIFF": 0, "SKIP": 0}
    for row in recon_rows:
        if row["status"] in reconciliation:
            reconciliation[row["status"]] = as_int(row["cnt"])

    return {
        "date_min": as_iso(bounds["date_min"]) if bounds else None,
        "date_max": as_iso(bounds["date_max"]) if bounds else None,
        "full_days": as_int(bounds["full_days"]) if bounds else 0,
        "partial_days": [as_iso(row["event_date_utc"]) for row in partial_rows],
        "latest_publish": (
            {
                "publish_id": as_int(publish["publish_id"]),
                "scope_type": publish["scope_type"],
                "dates_csv": publish["dates_csv"],
                "quality_status": publish["quality_status"],
                "published_at": as_iso(publish["published_at"]),
            }
            if publish
            else None
        ),
        "latest_quality": (
            {
                "run_id": as_int(quality["run_id"]),
                "status": quality["status"],
                "rules_total": as_int(quality["rules_total"]),
                "rules_passed": as_int(quality["rules_passed"]),
                "rules_failed": as_int(quality["rules_failed"]),
                "checks_total": as_int(quality["checks_total"]),
            }
            if quality
            else None
        ),
        "realtime": {
            "latest_updated_at": (
                as_iso(latest_rt_update["latest_updated_at"]) if latest_rt_update else None
            ),
            "final_days": as_int(rt_stats["final_rows"]) if rt_stats else 0,
            "rows": as_int(rt_stats["row_count"]) if rt_stats else 0,
            "reconciliation": reconciliation,
        },
    }

"""``/api/ops``：管道运行、发布、水位线与白名单表行数（SPEC §3.8）。

``table_stats`` 只使用后端白名单内的物理表，且全部为精确 ``COUNT(*)``
（这些表最大约 1.4 万行，毫秒级）。``dwd_events`` 等大表不在 ``webapp_ro``
授权白名单内，因此不展示 —— 不用 ``information_schema`` 近似值冒充精确行数。
"""

from __future__ import annotations

from fastapi import APIRouter

from .. import db
from ..config import settings
from ..serialization import as_int, as_iso

router = APIRouter()

_LIST_LIMIT = 20

# 物理表白名单：全部在 SPEC §2 的 webapp_ro 授权内，COUNT(*) 成本可控。
_DW_TABLE_STATS = (
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_funnel_interval",
    "ads_item_ranking_daily",
    "ads_item_behavior_daily",
    "ads_retention_cohort",
    "ads_publish_log",
    "quality_check_run",
    "quality_check_result",
    "airflow_pipeline_run",
    "etl_watermark",
)
_RT_TABLE_STATS = (
    "rt_traffic_daily",
    "rt_funnel_daily",
    "rt_item_behavior_daily",
    "rt_reconciliation_daily",
    "rt_late_events",
)


def _table_stats() -> list[dict[str, object]]:
    selects = []
    for sort_no, table in enumerate(_DW_TABLE_STATS):
        ref = db.table_ref(settings.dw_database, table)
        selects.append(
            f"SELECT {sort_no} AS sort_no, '{table}' AS table_name, COUNT(*) AS row_count "
            f"FROM {ref}"
        )
    offset = len(_DW_TABLE_STATS)
    for sort_no, table in enumerate(_RT_TABLE_STATS, start=offset):
        ref = db.table_ref(settings.rt_database, table)
        selects.append(
            f"SELECT {sort_no} AS sort_no, '{table}' AS table_name, COUNT(*) AS row_count "
            f"FROM {ref}"
        )
    rows = db.POOL.fetch_all(" UNION ALL ".join(selects) + " ORDER BY sort_no")
    return [{"table": row["table_name"], "rows": as_int(row["row_count"])} for row in rows]


@router.get("/ops")
def ops() -> dict[str, object]:
    pipeline_table = db.table_ref(settings.dw_database, "airflow_pipeline_run")
    publish_table = db.table_ref(settings.dw_database, "ads_publish_log")
    watermark_table = db.table_ref(settings.dw_database, "etl_watermark")

    pipeline_rows = db.POOL.fetch_all(
        f"SELECT run_id, dag_run_id, mode, window_start, window_end, status, published, "
        f"started_at, finished_at FROM {pipeline_table} "
        f"ORDER BY run_id DESC LIMIT %s",
        (_LIST_LIMIT,),
    )
    publish_rows = db.POOL.fetch_all(
        f"SELECT publish_id, scope_type, dates_csv, quality_status, published_at "
        f"FROM {publish_table} ORDER BY publish_id DESC LIMIT %s",
        (_LIST_LIMIT,),
    )
    watermark_rows = db.POOL.fetch_all(
        f"SELECT w.pipeline, w.event_date_utc, w.max_ingest_batch, w.processed_at "
        f"FROM {watermark_table} AS w "
        f"JOIN (SELECT pipeline, MAX(event_date_utc) AS max_date "
        f"      FROM {watermark_table} GROUP BY pipeline) AS m "
        f"ON w.pipeline = m.pipeline AND w.event_date_utc = m.max_date "
        f"ORDER BY w.pipeline"
    )

    return {
        "pipeline_runs": [
            {
                "run_id": as_int(row["run_id"]),
                "dag_run_id": row["dag_run_id"],
                "mode": row["mode"],
                "window_start": as_iso(row["window_start"]),
                "window_end": as_iso(row["window_end"]),
                "status": row["status"],
                "published": row["published"],
                "started_at": as_iso(row["started_at"]),
                "finished_at": as_iso(row["finished_at"]),
            }
            for row in pipeline_rows
        ],
        "publish_log": [
            {
                "publish_id": as_int(row["publish_id"]),
                "scope_type": row["scope_type"],
                "dates_csv": row["dates_csv"],
                "quality_status": row["quality_status"],
                "published_at": as_iso(row["published_at"]),
            }
            for row in publish_rows
        ],
        "watermark": [
            {
                "pipeline": row["pipeline"],
                "event_date_utc": as_iso(row["event_date_utc"]),
                "max_ingest_batch": as_int(row["max_ingest_batch"]),
                "processed_at": as_iso(row["processed_at"]),
            }
            for row in watermark_rows
        ],
        "table_stats": _table_stats(),
    }

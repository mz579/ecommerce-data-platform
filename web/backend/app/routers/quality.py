"""``/api/quality``：质量门禁运行列表与逐规则明细（SPEC §3.7）。"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query

from .. import db
from ..config import settings
from ..serialization import as_int, as_iso, truncate_detail

router = APIRouter()

_RUN_FIELDS = (
    "run_id, scope_type, dates_csv, status, rules_total, rules_passed, "
    "rules_failed, checks_total, started_at, finished_at"
)


def _run_payload(row: dict[str, object]) -> dict[str, object]:
    return {
        "run_id": as_int(row["run_id"]),
        "scope_type": row["scope_type"],
        "dates_csv": row["dates_csv"],
        "status": row["status"],
        "rules_total": as_int(row["rules_total"]),
        "rules_passed": as_int(row["rules_passed"]),
        "rules_failed": as_int(row["rules_failed"]),
        "checks_total": as_int(row["checks_total"]),
        "started_at": as_iso(row["started_at"]),
        "finished_at": as_iso(row["finished_at"]),
    }


@router.get("/quality/runs")
def quality_runs(
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict[str, object]:
    run_table = db.table_ref(settings.dw_database, "quality_check_run")
    rows = db.POOL.fetch_all(
        f"SELECT {_RUN_FIELDS} FROM {run_table} ORDER BY run_id DESC LIMIT %s",
        (limit,),
    )
    return {"runs": [_run_payload(row) for row in rows]}


@router.get("/quality/runs/{run_id}")
def quality_run_detail(
    run_id: Annotated[int, Path(ge=1, le=9_223_372_036_854_775_807)],
) -> dict[str, object]:
    run_table = db.table_ref(settings.dw_database, "quality_check_run")
    result_table = db.table_ref(settings.dw_database, "quality_check_result")
    run = db.POOL.fetch_one(
        f"SELECT {_RUN_FIELDS} FROM {run_table} WHERE run_id = %s",
        (run_id,),
    )
    if run is None:
        raise HTTPException(status_code=404, detail="quality run not found")

    rule_rows = db.POOL.fetch_all(
        f"SELECT rule_key, category, status, checked, failed_rows, detail, duration_ms "
        f"FROM {result_table} WHERE run_id = %s ORDER BY result_id",
        (run_id,),
    )
    payload = _run_payload(run)
    payload["rules"] = [
        {
            "rule_key": row["rule_key"],
            "category": row["category"],
            "status": row["status"],
            "checked": as_int(row["checked"]),
            "failed_rows": as_int(row["failed_rows"]),
            "detail": truncate_detail(row["detail"]),
            "duration_ms": as_int(row["duration_ms"]),
        }
        for row in rule_rows
    ]
    return payload

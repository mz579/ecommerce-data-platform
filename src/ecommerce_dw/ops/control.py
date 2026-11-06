"""运行控制表 ``airflow_pipeline_run`` 与 DAG 计划/收尾函数（Phase 6）。

一张新增的 MySQL 控制表（与 Phase 5 的 ``etl_watermark`` / ``quality_check_run``
同层，绝不写业务数据）：Airflow 每次 DAG 运行在此登记一个 RUNNING 批次
（``batch_id``），各 ETL 任务的 CLU/DAG 日志里可按它检索；``finish_run`` 在
成功/失败时回写状态、门禁 run id、发布 id 与错误摘要——"任务日志能查到 batch_id、
处理行数、耗时、错误摘要"的落点之一。

计划语义（``plan_and_register``）完全复用 Phase 5 的水位线规划器：

- ``TAIL``：``plan_incremental`` 给出「新尾巴 + 窗口内迟到」，再裁到本次窗口；
- ``FORCE``：``plan_backfill`` 显式重建窗口内 ODS 有数据的日期（深修正）。

聚合层还需要把「受影响日 ∪ {前一日且 DWD 有行}」作为 scope
（次日留存 age-1 依赖，见 Phase 4/5 文档），与 ``incremental.runner._agg_scope``
保持一致。
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path

import pymysql

from ecommerce_dw.config import DbConfig
from ecommerce_dw.incremental.planner import plan_backfill, plan_incremental
from ecommerce_dw.incremental.runner import _agg_scope, _dwd_days_present, current_batch_by_date
from ecommerce_dw.incremental.runner import connect as runner_connect
from ecommerce_dw.incremental.watermark import DEFAULT_PIPELINE, WatermarkRepo
from ecommerce_dw.ops.window import MODE_FORCE, MODE_TAIL, parse_dates_csv
from ecommerce_dw.quality.runner import QualityGateError, publish_ads_scope

logger = logging.getLogger(__name__)

_DDL_REL = Path("sql") / "ddl" / "airflow_pipeline_run.sql"
_ERROR_MAX = 2048


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _clip(text: str | None, limit: int = _ERROR_MAX) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _ensure_control_table(conn: pymysql.connections.Connection) -> None:
    path = Path(__file__).resolve().parents[3] / _DDL_REL
    if not path.is_file():  # pragma: no cover - repo layout regression
        logger.warning("missing airflow_pipeline_run DDL at %s", path)
        return
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))
    conn.commit()


def _read_current_and_watermark(
    conn: pymysql.connections.Connection, pipeline: str
) -> tuple[dict[date, int], dict[date, int]]:
    current = current_batch_by_date(conn)
    watermark = WatermarkRepo(conn).load(pipeline)
    return current, watermark


def _upsert_start(
    conn: pymysql.connections.Connection,
    *,
    dag_run_id: str,
    pipeline: str,
    mode: str,
    window_start: date,
    window_end: date,
    dwd_dates: tuple[date, ...],
    agg_dates: tuple[date, ...],
) -> int:
    """登记（或复用，保证重试幂等）一个 RUNNING 批次，返回 batch_id。"""
    ws, we = window_start, window_end
    dwd_csv = ",".join(d.isoformat() for d in dwd_dates) if dwd_dates else ""
    agg_csv = ",".join(d.isoformat() for d in agg_dates) if agg_dates else ""
    cur = conn.cursor()
    cur.execute(
        "SELECT run_id FROM airflow_pipeline_run "
        "WHERE pipeline = %s AND dag_run_id = %s AND status = 'RUNNING' "
        "ORDER BY run_id ASC LIMIT 1",
        (pipeline, dag_run_id),
    )
    row = cur.fetchone()
    if row is not None:
        run_id = int(row[0])
        cur.execute(
            "UPDATE airflow_pipeline_run SET mode = %s, window_start = %s, window_end = %s,"
            " dwd_dates = %s, agg_dates = %s, started_at = %s WHERE run_id = %s",
            (mode, ws, we, dwd_csv, agg_csv, _now(), run_id),
        )
    else:
        cur.execute(
            "INSERT INTO airflow_pipeline_run"
            " (pipeline, dag_run_id, mode, window_start, window_end,"
            "  dwd_dates, agg_dates, status, started_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (pipeline, dag_run_id, mode, ws, we, dwd_csv, agg_csv, "RUNNING", _now()),
        )
        run_id = int(cur.lastrowid)
    conn.commit()
    return run_id


def plan_and_register(
    cfg: DbConfig,
    *,
    mode: str,
    window: str,
    dag_run_id: str,
    pipeline: str = DEFAULT_PIPELINE,
    late_window_days: int = 7,
) -> dict:
    """规划本次窗口的受影响业务日期，并登记 ``airflow_pipeline_run`` RUNNING 批次。

    返回（小 XCom）：

    - ``batch_id``：本次 DAG 运行的批次号（日志可检索）；
    - ``dwd_dates``：本次要重建的 DWD 业务日期（TAIL→水位线裁剪，FORCE→显式区间）；
    - ``agg_dates``：DWS/ADS 重建 scope = dwd_dates ∪ 留存前驱；
    - ``mode`` / ``window``：透传。
    """
    if mode not in (MODE_TAIL, MODE_FORCE):
        raise ValueError(f"unknown mode {mode!r}")
    days = parse_dates_csv(window)
    ws, we = days[0], days[-1]

    conn = runner_connect(cfg)
    try:
        _ensure_control_table(conn)
        current, watermark = _read_current_and_watermark(conn, pipeline)
        if not current:
            raise RuntimeError("ods_events_raw has no business dates - nothing to plan")

        if mode == MODE_TAIL:
            plan = plan_incremental(current, watermark, late_window_days=late_window_days)
            dwd_dates = tuple(d for d in plan.affected if ws <= d <= we)
            out_of_window = tuple(d for d in plan.out_of_window_dates if ws <= d <= we)
            if out_of_window:
                logger.warning(
                    "out-of-window corrections inside window %s..%s: %s "
                    "(run FORCE conf from/to to process them)",
                    ws,
                    we,
                    ", ".join(d.isoformat() for d in out_of_window),
                )
        else:
            plan = plan_backfill(ws, we, current, watermark, late_window_days=late_window_days)
            dwd_dates = plan.affected
            out_of_window = ()

        agg_dates = _agg_scope(dwd_dates, _dwd_days_present(conn))
        batch_id = _upsert_start(
            conn,
            dag_run_id=dag_run_id,
            pipeline=pipeline,
            mode=mode,
            window_start=ws,
            window_end=we,
            dwd_dates=dwd_dates,
            agg_dates=agg_dates,
        )
        logger.info(
            "control run #%d pipeline=%s mode=%s window=%s dwd_dates=%s agg_dates=%s "
            "out_of_window=%s",
            batch_id,
            pipeline,
            mode,
            window,
            ",".join(d.isoformat() for d in dwd_dates) or "-",
            ",".join(d.isoformat() for d in agg_dates) or "-",
            ",".join(d.isoformat() for d in out_of_window) or "-",
        )
        return {
            "batch_id": batch_id,
            "mode": mode,
            "window": window,
            "dwd_dates": ",".join(d.isoformat() for d in dwd_dates),
            "agg_dates": ",".join(d.isoformat() for d in agg_dates),
            "dwd_count": len(dwd_dates),
            "agg_count": len(agg_dates),
        }
    finally:
        conn.close()


def finish_run(
    cfg: DbConfig,
    run_id: int,
    status: str,
    *,
    published: str | None = None,
    publish_id: int | None = None,
    quality_run_id: int | None = None,
    error_summary: str | None = None,
) -> None:
    """回写控制批次状态（SUCCESS / FAILED），含门禁与发布 id、错误摘要。"""
    if status not in ("SUCCESS", "FAILED"):
        raise ValueError(f"status must be SUCCESS or FAILED, got {status!r}")
    conn = runner_connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE airflow_pipeline_run SET status = %s, finished_at = %s,"
                " published = %s, publish_id = %s, quality_run_id = %s,"
                " error_summary = %s WHERE run_id = %s",
                (
                    status,
                    _now(),
                    published,
                    publish_id,
                    quality_run_id,
                    _clip(error_summary),
                    run_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def publish_after_gate(
    cfg: DbConfig,
    *,
    batch_id: int,
    window: str,
    dwd_dates: str,
    gate_run_id: int,
    gate_status: str,
) -> dict:
    """受门禁保护的发布收尾：写入 ``ads_publish_log`` 并关闭控制批次。

    - 门禁未 PASS → 抛 ``QualityGateError``（Phase 5 发布门控，绝不写发布日志）；
    - 本次窗口没有实际重建（stable-state 重跑）→ 发布 SKIP，控制批次记 SUCCESS，
      不产生新的 ``ads_publish_log`` 行 —— 幂等复跑的正反馈；
    - 否则 ``publish_ads_scope`` 写入一行并回填 publish_id。
    """
    if gate_status != "PASS":
        raise QualityGateError(
            f"publish refused: quality gate run #{gate_run_id} status={gate_status}"
        )
    if not dwd_dates:
        finish_run(
            cfg,
            batch_id,
            "SUCCESS",
            published="SKIPPED",
            quality_run_id=gate_run_id,
            error_summary="stable-state re-run: nothing rebuilt in window; publish skipped",
        )
        return {"published": False, "publish_id": None, "reason": "nothing-rebuilt"}

    dates = parse_dates_csv(dwd_dates)
    publish_id = publish_ads_scope(cfg, dates=dates, quality_run_id=gate_run_id)
    finish_run(
        cfg,
        batch_id,
        "SUCCESS",
        published="PUBLISHED",
        publish_id=publish_id,
        quality_run_id=gate_run_id,
    )
    logger.info(
        "publish ok: publish_id=%d quality_run=%d window=%s dwd_dates=%s",
        publish_id,
        gate_run_id,
        window,
        dwd_dates,
    )
    return {"published": True, "publish_id": publish_id, "reason": "rebuilt"}

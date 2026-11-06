"""Quality-gate runner: execute rules, persist results, gate ADS publishing (Phase 5).

Flow::

    run_quality_checks(cfg)          # full-library run, persists one quality_check_run
    run_quality_gate(cfg, dates=..)  # hard gate: any FAIL -> QualityGateError
    publish_ads_scope(cfg, dates=..) # only after a PASS run; writes ads_publish_log

Failure handling is binary: a run whose any rule reports ``failed_rows > 0`` is
recorded as FAIL and raises on the gate path. There is deliberately no
warn-and-continue severity; a publish request against a FAILed (or missing) run
is refused and never touches ``ads_publish_log``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import pymysql
import pymysql.cursors

from ecommerce_dw.config import DbConfig
from ecommerce_dw.dws.constants import FULL_WINDOW_END, FULL_WINDOW_START
from ecommerce_dw.quality.models import CheckRule, RuleOutcome, RunStatus
from ecommerce_dw.quality.rules import build_rule_catalog

logger = logging.getLogger(__name__)

_DETAIL_MAX = 4_000

CONTROL_TABLES = (
    "etl_watermark",
    "quality_check_run",
    "quality_check_result",
    "ads_publish_log",
)


class QualityGateError(RuntimeError):
    """Raised on a FAILed gate run (also used to refuse a publish request)."""


@dataclass(frozen=True)
class QualityReport:
    """Result of one gate run, mirroring the persisted quality_check_run row."""

    run_id: int
    status: str
    scope_type: str
    dates_csv: str | None
    window_start: date | None
    window_end: date | None
    rules_total: int
    passed: int
    failed: int
    checks_total: int
    started_at: datetime
    finished_at: datetime
    failures: dict[str, RuleOutcome] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == RunStatus.PASS


@dataclass(frozen=True)
class RecordedRun:
    """Headline of a persisted gate run (for ``quality show``)."""

    run_id: int
    status: str
    scope_type: str
    dates_csv: str | None
    rules_total: int
    passed: int
    failed: int
    checks_total: int
    started_at: datetime
    finished_at: datetime


def connect(cfg: DbConfig) -> pymysql.connections.Connection:
    """Open a quality-gate connection (long read timeout for the full-library scans)."""
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=3600,
        autocommit=False,
        cursorclass=pymysql.cursors.Cursor,
    )


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _truncate(text: str | None, limit: int = _DETAIL_MAX) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _scope(dates: tuple[date, ...] | None) -> tuple[str, str | None]:
    if not dates:
        return "FULL", None
    return "DATES", ",".join(d.isoformat() for d in sorted(set(dates)))


def run_quality_checks(
    cfg: DbConfig,
    *,
    dates: tuple[date, ...] | None = None,
    window_start: date | None = FULL_WINDOW_START,
    window_end: date | None = FULL_WINDOW_END,
    catalog: tuple[CheckRule, ...] | None = None,
) -> QualityReport:
    """Run every rule in the catalog and persist run + per-rule results.

    ``dates=None`` scopes nothing (FULL) - every date-keyed rule runs over all
    days, which is the standard production gate.
    """
    catalog = catalog if catalog is not None else build_rule_catalog()
    conn = connect(cfg)
    started_at = _now()
    try:
        _ensure_control_tables(conn)
        outcomes = [(rule, rule.run(conn, dates=dates)) for rule in catalog]
        passed = [r for _, r in outcomes if r.passed]
        failed = [r for _, r in outcomes if not r.passed]
        status = RunStatus.PASS if not failed else RunStatus.FAIL
        scope_type, dates_csv = _scope(dates)
        finished_at = _now()

        cur = conn.cursor()
        cur.execute(
            "INSERT INTO quality_check_run"
            " (scope_type, dates_csv, window_start, window_end, status,"
            "  rules_total, rules_passed, rules_failed, checks_total, started_at, finished_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                scope_type,
                dates_csv,
                window_start,
                window_end,
                status,
                len(catalog),
                len(passed),
                len(failed),
                sum(outcome.checked for _, outcome in outcomes),
                started_at,
                finished_at,
            ),
        )
        run_id = int(cur.lastrowid)
        for rule, outcome in outcomes:
            cur.execute(
                "INSERT INTO quality_check_result"
                " (run_id, rule_key, category, status, checked, failed_rows,"
                "  detail, duration_ms)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    run_id,
                    rule.key,
                    rule.category,
                    RunStatus.PASS if outcome.passed else RunStatus.FAIL,
                    outcome.checked,
                    outcome.failed_rows,
                    _truncate(outcome.detail),
                    int(outcome.duration_ms),
                ),
            )
        conn.commit()
        failures = {rule.key: outcome for rule, outcome in outcomes if not outcome.passed}
        return QualityReport(
            run_id=run_id,
            status=status,
            scope_type=scope_type,
            dates_csv=dates_csv,
            window_start=window_start,
            window_end=window_end,
            rules_total=len(catalog),
            passed=len(passed),
            failed=len(failed),
            checks_total=sum(outcome.checked for _, outcome in outcomes),
            started_at=started_at,
            finished_at=finished_at,
            failures=failures,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_quality_gate(
    cfg: DbConfig,
    *,
    dates: tuple[date, ...] | None = None,
    window_start: date | None = FULL_WINDOW_START,
    window_end: date | None = FULL_WINDOW_END,
    catalog: tuple[CheckRule, ...] | None = None,
) -> QualityReport:
    """Hard gate: run all rules and raise :class:`QualityGateError` on any FAIL."""
    report = run_quality_checks(
        cfg,
        dates=dates,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
    )
    if not report.ok:
        lines = [
            f"run #{report.run_id} -> {report.status}: "
            f"{report.failed}/{report.rules_total} rules FAILED "
            f"({report.checks_total:,} rows checked)",
        ]
        for key, outcome in sorted(report.failures.items()):
            lines.append(
                f"  FAIL {key}: {outcome.failed_rows:,} violating rows of {outcome.checked:,}"
                + (f" ({outcome.detail})" if outcome.detail else "")
            )
        raise QualityGateError("\n".join(lines))
    return report


def publish_ads_scope(
    cfg: DbConfig,
    *,
    dates: tuple[date, ...] | None = None,
    quality_run_id: int | None = None,
    catalog: tuple[CheckRule, ...] | None = None,
) -> int:
    """Publish an ADS scope, guarded by a PASSing gate run.

    - with ``quality_run_id``: the referenced run must exist and be PASS, else the
      publish is refused.
    - without it: a fresh full gate run is executed and must pass.
    On approval one ``ads_publish_log`` row is written. Returns the publish_id.
    """
    conn = connect(cfg)
    try:
        _ensure_control_tables(conn)
        run_id = quality_run_id
        if run_id is None:
            logger.info(
                "publish: no --run given - running a fresh FULL quality gate "
                "(full-scan of ODS/DWD/DWS/ADS, expect a few minutes) before writing"
                " ads_publish_log..."
            )
            report = run_quality_gate(cfg, dates=dates, catalog=catalog)
            run_id = report.run_id
        else:
            cur = conn.cursor()
            cur.execute("SELECT status FROM quality_check_run WHERE run_id = %s", (run_id,))
            row = cur.fetchone()
            if row is None:
                raise QualityGateError(
                    f"publish refused: run #{run_id} does not exist - run the gate first"
                )
            if row[0] != RunStatus.PASS:
                raise QualityGateError(f"publish refused: run #{run_id} status={row[0]} (not PASS)")
        scope_type, dates_csv = _scope(dates)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ads_publish_log"
            " (scope_type, dates_csv, quality_run_id, quality_status, published_at)"
            " VALUES (%s, %s, %s, %s, %s)",
            (scope_type, dates_csv, run_id, RunStatus.PASS, _now()),
        )
        conn.commit()
        return int(cur.lastrowid)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_runs(cfg: DbConfig, limit: int = 20) -> list[RecordedRun]:
    """Most recent gate runs for ``quality show``."""
    conn = connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT run_id, status, scope_type, dates_csv, rules_total, rules_passed,"
            " rules_failed, checks_total, started_at, finished_at"
            " FROM quality_check_run ORDER BY run_id DESC LIMIT %s",
            (limit,),
        )
        return [
            RecordedRun(
                run_id=int(r[0]),
                status=r[1],
                scope_type=r[2],
                dates_csv=r[3],
                rules_total=int(r[4]),
                passed=int(r[5]),
                failed=int(r[6]),
                checks_total=int(r[7]),
                started_at=r[8],
                finished_at=r[9],
            )
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def list_run_results(cfg: DbConfig, run_id: int) -> list[tuple]:
    """Per-rule result rows for ``quality show <run>``."""
    conn = connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT rule_key, category, status, checked, failed_rows, detail, duration_ms"
            " FROM quality_check_result WHERE run_id = %s"
            " ORDER BY status DESC, rule_key ASC",
            (run_id,),
        )
        return list(cur.fetchall())
    finally:
        conn.close()


def _ensure_control_tables(conn: pymysql.connections.Connection) -> None:
    """Create missing phase-5 control tables (idempotent, mirrors sql/ddl/*)."""
    from pathlib import Path

    ddl_dir = Path(__file__).resolve().parents[3] / "sql" / "ddl"
    cur = conn.cursor()
    for table in CONTROL_TABLES:
        path = ddl_dir / f"{table}.sql"
        if not path.is_file():
            logger.warning("missing DDL for %s at %s - relying on caller", table, path)
            continue
        stmt = path.read_text(encoding="utf-8")
        cur.execute(stmt)
        conn.commit()

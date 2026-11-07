"""Full-range batch-vs-realtime reconciliation for ``ecommerce_rt`` (Phase 9.4).

Phase 9.3 shipped ``verify_rt.py`` for a handful of days. Phase 9.4 replays the
complete 2015-05-03..2015-09-18 window and writes one audit row per
``(UTC day, metric)`` into ``ecommerce_rt.rt_reconciliation_daily``:

- **full days** (``ads_traffic_daily.is_full_day = 1``): 13 metrics, MATCH/DIFF;
- **partial days** (2015-05-03, 2015-09-18): 13 rows with status SKIP and all
  value columns NULL.

The offline side is read from the published ADS tables and independently
cross-checked against ``ecommerce_dw.dwd_events`` with the same SQL used by
``verify_rt`` (never the Flink aggregation code). Connections are deliberately
split by least privilege:

===================  =====================================================
purpose              account
===================  =====================================================
read DWD/ADS         ``ecom_app`` (``MYSQL_USER`` / ``MYSQL_PASSWORD``)
read RT tables       ``rt_recon_ro`` (read-only; RT_RECON_RO_*)
write audit table    ``rt_app`` (DML only; RT_APP_*)
===================  =====================================================

No password is compiled in; every credential comes from the environment / the
project ``.env`` loaded by the CLI wrapper.

Usage::

    python scripts/reconcile_all.py --dry-run
    python scripts/reconcile_all.py --write --json-out <evidence>/recon.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import DbConfig, db_config_from_env
from ecommerce_dw.streaming.verify_rt import (
    ALL_METRICS,
    FUNNEL_METRICS,
    read_expected,
    read_realtime,
    rt_recon_credentials,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RT_DATABASE = "ecommerce_rt"
RECON_TABLE = "rt_reconciliation_daily"
RT_APP_DEFAULT_USER = "rt_app"
REL_QUANTUM = Decimal("0.000001")

_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}
_RT_DEFAULTS = {"PORT": "3306", "HOST": "127.0.0.1"}

ADS_TRAFFIC_SQL = """
SELECT event_date_utc, pv, uv, dau, is_full_day
FROM ads_traffic_daily
WHERE event_date_utc BETWEEN %s AND %s
ORDER BY event_date_utc
""".strip()

ADS_FUNNEL_SQL = """
SELECT event_date_utc, metric_name, metric_value, is_full_day
FROM ads_funnel_daily
WHERE event_date_utc BETWEEN %s AND %s
ORDER BY event_date_utc, metric_name
""".strip()

ADS_ITEM_SQL = """
SELECT event_date_utc, items_viewed, items_carted, items_bought, is_full_day
FROM ads_item_behavior_daily
WHERE event_date_utc BETWEEN %s AND %s
ORDER BY event_date_utc
""".strip()

UPSERT_SQL = f"""
INSERT INTO {RECON_TABLE}
    (event_date_utc, metric_name, offline_value, realtime_value,
     abs_diff, rel_diff, status, checked_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    offline_value = VALUES(offline_value),
    realtime_value = VALUES(realtime_value),
    abs_diff = VALUES(abs_diff),
    rel_diff = VALUES(rel_diff),
    status = VALUES(status),
    checked_at = VALUES(checked_at)
""".strip()


@dataclass(frozen=True)
class OfflineDataset:
    """ADS values plus the authoritative ``is_full_day`` flag per date."""

    metrics: dict[date, dict[str, Decimal | None]]
    is_full_day: dict[date, int]


@dataclass(frozen=True)
class ReconciliationRow:
    event_date: date
    metric: str
    offline_value: Decimal | None
    realtime_value: Decimal | None
    abs_diff: Decimal | None
    rel_diff: Decimal | None
    status: str
    checked_at: datetime

    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.event_date,
            self.metric,
            self.offline_value,
            self.realtime_value,
            self.abs_diff,
            self.rel_diff,
            self.status,
            self.checked_at,
        )


@dataclass(frozen=True)
class CrosscheckMismatch:
    event_date: date
    metric: str
    ads_value: Decimal | None
    dwd_value: Decimal | None


def relative_diff(offline: Decimal | None, realtime: Decimal | None) -> Decimal | None:
    """``(realtime - offline) / offline`` rounded to 6 decimals; NULL when undefined."""
    if offline is None or realtime is None or offline == 0:
        return None
    return ((realtime - offline) / offline).quantize(REL_QUANTUM, rounding=ROUND_HALF_UP)


def _abs_diff(offline: Decimal | None, realtime: Decimal | None) -> Decimal | None:
    if offline is None or realtime is None:
        return None
    return realtime - offline


def reconciliation_row(
    event_date: date,
    metric: str,
    offline: Decimal | None,
    realtime: Decimal | None,
    checked_at: datetime,
) -> ReconciliationRow:
    """Classify one full-day metric; an absent realtime value is always a DIFF."""
    status = "MATCH" if offline == realtime else "DIFF"
    return ReconciliationRow(
        event_date=event_date,
        metric=metric,
        offline_value=offline,
        realtime_value=realtime,
        abs_diff=_abs_diff(offline, realtime),
        rel_diff=relative_diff(offline, realtime),
        status=status,
        checked_at=checked_at,
    )


def build_full_day_rows(
    event_date: date,
    offline: Mapping[str, Decimal | None],
    realtime: Mapping[str, Decimal | None],
    checked_at: datetime,
) -> list[ReconciliationRow]:
    """Exactly 13 rows; a metric missing from ``realtime`` is a DIFF with NULL actual."""
    rows: list[ReconciliationRow] = []
    for metric in ALL_METRICS:
        if metric not in realtime:
            rows.append(
                ReconciliationRow(
                    event_date=event_date,
                    metric=metric,
                    offline_value=offline.get(metric),
                    realtime_value=None,
                    abs_diff=None,
                    rel_diff=None,
                    status="DIFF",
                    checked_at=checked_at,
                )
            )
        else:
            rows.append(
                reconciliation_row(
                    event_date,
                    metric,
                    offline.get(metric),
                    realtime[metric],
                    checked_at,
                )
            )
    return rows


def build_skip_rows(event_date: date, checked_at: datetime) -> list[ReconciliationRow]:
    """Exactly 13 partial-day rows: every value NULL, status SKIP."""
    return [
        ReconciliationRow(
            event_date=event_date,
            metric=metric,
            offline_value=None,
            realtime_value=None,
            abs_diff=None,
            rel_diff=None,
            status="SKIP",
            checked_at=checked_at,
        )
        for metric in ALL_METRICS
    ]


def build_rows(
    full_days: Iterable[date],
    partial_days: Iterable[date],
    offline_by_date: Mapping[date, Mapping[str, Decimal | None]],
    realtime_by_date: Mapping[date, Mapping[str, Decimal | None]],
    checked_at: datetime,
) -> list[ReconciliationRow]:
    """Build the complete audit row set (full days first, then partial days)."""
    rows: list[ReconciliationRow] = []
    for event_date in sorted(full_days):
        rows.extend(
            build_full_day_rows(
                event_date,
                offline_by_date.get(event_date, {}),
                realtime_by_date.get(event_date, {}),
                checked_at,
            )
        )
    for event_date in sorted(partial_days):
        rows.extend(build_skip_rows(event_date, checked_at))
    return rows


def load_ads(cursor: Any, date_from: date, date_to: date) -> OfflineDataset:
    """Read the published ADS daily tables (offline source of truth)."""
    metrics: dict[date, dict[str, Decimal | None]] = {}
    is_full_day: dict[date, int] = {}

    cursor.execute(ADS_TRAFFIC_SQL, (date_from, date_to))
    for row in cursor.fetchall():
        event_date = row["event_date_utc"]
        metrics[event_date] = {
            "pv": Decimal(int(row["pv"] or 0)),
            "uv": Decimal(int(row["uv"] or 0)),
            "dau": Decimal(int(row["dau"] or 0)),
        }
        is_full_day[event_date] = int(row["is_full_day"])

    cursor.execute(ADS_FUNNEL_SQL, (date_from, date_to))
    for row in cursor.fetchall():
        event_date = row["event_date_utc"]
        metric = row["metric_name"]
        if metric in FUNNEL_METRICS:
            bucket = metrics.setdefault(event_date, {})
            value = row["metric_value"]
            bucket[metric] = None if value is None else Decimal(value)

    cursor.execute(ADS_ITEM_SQL, (date_from, date_to))
    for row in cursor.fetchall():
        event_date = row["event_date_utc"]
        bucket = metrics.setdefault(event_date, {})
        bucket["items_viewed"] = Decimal(int(row["items_viewed"] or 0))
        bucket["items_carted"] = Decimal(int(row["items_carted"] or 0))
        bucket["items_bought"] = Decimal(int(row["items_bought"] or 0))

    return OfflineDataset(metrics=metrics, is_full_day=is_full_day)


def ads_dates(cursor: Any) -> tuple[date, date]:
    """Return the published ADS date range (used when --from/--to are omitted)."""
    cursor.execute(
        "SELECT MIN(event_date_utc) AS mn, MAX(event_date_utc) AS mx FROM ads_traffic_daily"
    )
    row = cursor.fetchone()
    if not row or row["mn"] is None or row["mx"] is None:
        raise ValueError("ads_traffic_daily is empty")
    return row["mn"], row["mx"]


def crosscheck_ads_vs_dwd(
    ads: OfflineDataset,
    dwd: Mapping[date, Mapping[str, Decimal | None]],
    days: Iterable[date],
) -> list[CrosscheckMismatch]:
    """Compare every ADS metric against the independent DWD aggregate.

    ADS is built from DWD, so a mismatch indicates either a stale ADS publication
    or a definition drift; it must be investigated rather than hidden.
    """
    mismatches: list[CrosscheckMismatch] = []
    for event_date in sorted(days):
        ads_metrics = ads.metrics.get(event_date, {})
        dwd_metrics = dwd.get(event_date, {})
        for metric in ALL_METRICS:
            ads_value = ads_metrics.get(metric)
            dwd_value = dwd_metrics.get(metric)
            if ads_value != dwd_value:
                mismatches.append(
                    CrosscheckMismatch(
                        event_date=event_date,
                        metric=metric,
                        ads_value=ads_value,
                        dwd_value=dwd_value,
                    )
                )
    return mismatches


def write_reconciliation(
    connection: pymysql.connections.Connection,
    rows: Sequence[ReconciliationRow],
) -> int:
    """Idempotently upsert audit rows; reruns overwrite the same primary keys.

    The database guard must work with either a dict cursor or the pymysql default
    tuple cursor: the CLI passes a ``DictCursor``, but this helper is also called
    from tests and scripts that use the default cursor.
    """
    if not rows:
        return 0
    with connection.cursor() as cursor:
        cursor.execute("SELECT DATABASE() AS db")
        result = cursor.fetchone()
        current = result["db"] if isinstance(result, Mapping) else result[0]
        if current != RT_DATABASE:
            raise RuntimeError(
                f"refusing to write reconciliation outside {RT_DATABASE!r} "
                f"(connected to {current!r})"
            )
        cursor.executemany(UPSERT_SQL, [row.as_tuple() for row in rows])
    connection.commit()
    return len(rows)


def summarize_rows(rows: Sequence[ReconciliationRow]) -> dict[str, int]:
    summary = {"MATCH": 0, "DIFF": 0, "SKIP": 0}
    for row in rows:
        summary[row.status] = summary.get(row.status, 0) + 1
    return summary


def day_statuses(rows: Sequence[ReconciliationRow]) -> dict[date, dict[str, int]]:
    result: dict[date, dict[str, int]] = {}
    for row in rows:
        bucket = result.setdefault(row.event_date, {"MATCH": 0, "DIFF": 0, "SKIP": 0})
        bucket[row.status] = bucket.get(row.status, 0) + 1
    return result


def render_summary(
    rows: Sequence[ReconciliationRow],
    *,
    final_flags: Mapping[date, int] | None = None,
    detail_days: int = 2,
) -> str:
    """Human-readable per-day summary plus sampled details and every DIFF row."""
    statuses = day_statuses(rows)
    lines = [
        f"{'date':10s} {'kind':7s} {'match':>5s} {'diff':>5s} {'skip':>5s} {'final':>6s}",
        "-" * 44,
    ]
    full_detail_dates: list[date] = []
    for event_date in sorted(statuses):
        counts = statuses[event_date]
        kind = "SKIP" if counts["SKIP"] else "FULL"
        if kind == "FULL" and len(full_detail_dates) < detail_days:
            full_detail_dates.append(event_date)
        is_final = None if final_flags is None else final_flags.get(event_date)
        lines.append(
            f"{event_date.isoformat():10s} {kind:7s} {counts['MATCH']:5d} "
            f"{counts['DIFF']:5d} {counts['SKIP']:5d} "
            f"{'NULL' if is_final is None else is_final:>6}"
        )

    detail_rows = [
        row
        for row in rows
        if row.event_date in set(full_detail_dates)
        or row.status == "DIFF"
        or (row.status == "SKIP" and row.metric == ALL_METRICS[0])
    ]
    if detail_rows:
        lines.append("")
        lines.append("sampled detail (first full days, all DIFF rows, one SKIP row):")
        header = (
            f"{'date':10s} {'metric':20s} {'offline':>16s} {'realtime':>16s} "
            f"{'abs_diff':>12s} {'rel_diff':>12s} {'status':6s}"
        )
        lines.append(header)
        lines.append("-" * len(header))
        for row in detail_rows:
            lines.append(
                f"{row.event_date.isoformat():10s} {row.metric:20s} "
                f"{_format(row.offline_value):>16s} {_format(row.realtime_value):>16s} "
                f"{_format(row.abs_diff):>12s} {_format(row.rel_diff):>12s} {row.status:6s}"
            )

    summary = summarize_rows(rows)
    lines.append("")
    lines.append(
        f"SUMMARY match={summary['MATCH']} diff={summary['DIFF']} "
        f"skip={summary['SKIP']} total={len(rows)}"
    )
    return "\n".join(lines)


def _format(value: Decimal | None) -> str:
    if value is None:
        return "NULL"
    if value == value.to_integral_value():
        return str(int(value))
    return format(value, "f")


def rt_app_credentials(environ: Mapping[str, str]) -> tuple[str, str]:
    """Resolve the DML-only RT account; never falls back to another account."""
    user = environ.get("RT_APP_USER") or RT_APP_DEFAULT_USER
    password = environ.get("RT_APP_PASSWORD")
    if not password:
        raise KeyError("RT_APP_PASSWORD")
    return user, password


def connect_mysql(cfg: DbConfig, *, read_only: bool, cursorclass: Any = None) -> Any:
    kwargs: dict[str, Any] = {}
    if cursorclass is not None:
        kwargs["cursorclass"] = cursorclass
    connection = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
        autocommit=not read_only,
        **kwargs,
    )
    if read_only:
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
            cursor.execute("SELECT 1")
            cursor.fetchall()
    return connection


def _rt_config(environ: Mapping[str, str], user: str, password: str) -> DbConfig:
    return DbConfig(
        engine="mysql",
        host=environ.get("MYSQL_HOST") or _RT_DEFAULTS["HOST"],
        port=int(environ.get("MYSQL_PORT") or _RT_DEFAULTS["PORT"]),
        user=user,
        password=password,
        database=RT_DATABASE,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from",
        dest="date_from",
        type=date.fromisoformat,
        default=None,
        help="UTC 起始日（含）；默认取 ads_traffic_daily 最小值",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        type=date.fromisoformat,
        default=None,
        help="UTC 结束日（含）；默认取 ads_traffic_daily 最大值",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="只对账不写库（默认）")
    mode.add_argument("--write", action="store_true", help="写入 rt_reconciliation_daily")
    parser.add_argument("--json-out", type=Path, default=None, help="完整结果 JSON 输出路径")
    parser.add_argument(
        "--detail-days",
        type=int,
        default=2,
        help="摘要中展示明细的完整日数量（默认 2）",
    )
    return parser


def _json_payload(
    rows: Sequence[ReconciliationRow],
    mismatches: Sequence[CrosscheckMismatch],
    *,
    date_from: date,
    date_to: date,
    written: int,
    final_flags: Mapping[date, int],
) -> dict[str, Any]:
    summary = summarize_rows(rows)
    return {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "summary": summary,
        "total": len(rows),
        "written": written,
        "ads_vs_dwd_mismatches": len(mismatches),
        "ads_vs_dwd_mismatch_detail": [
            {
                "event_date": item.event_date.isoformat(),
                "metric": item.metric,
                "ads_value": None if item.ads_value is None else str(item.ads_value),
                "dwd_value": None if item.dwd_value is None else str(item.dwd_value),
            }
            for item in mismatches
        ],
        "days": [
            {
                "event_date": event_date.isoformat(),
                "is_final": final_flags.get(event_date),
                "match": statuses["MATCH"],
                "diff": statuses["DIFF"],
                "skip": statuses["SKIP"],
            }
            for event_date, statuses in sorted(day_statuses(rows).items())
        ],
        "rows": [
            {
                "event_date": row.event_date.isoformat(),
                "metric": row.metric,
                "offline_value": _format(row.offline_value),
                "realtime_value": _format(row.realtime_value),
                "abs_diff": _format(row.abs_diff),
                "rel_diff": _format(row.rel_diff),
                "status": row.status,
                "checked_at": row.checked_at.isoformat(timespec="milliseconds"),
            }
            for row in rows
        ],
    }


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    write_enabled = bool(args.write)
    checked_at = datetime.now(UTC).replace(tzinfo=None)

    mysql_cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    try:
        recon_user, recon_password = rt_recon_credentials(os.environ)
    except KeyError as exc:
        print(f"[error] missing environment variable: {exc}", file=sys.stderr)
        return 2

    dwd_conn = None
    rt_ro_conn = None
    rt_app_conn = None
    try:
        dwd_conn = connect_mysql(mysql_cfg, read_only=True, cursorclass=pymysql.cursors.DictCursor)
        with dwd_conn.cursor() as cursor:
            if args.date_from is None or args.date_to is None:
                default_from, default_to = ads_dates(cursor)
                date_from = args.date_from or default_from
                date_to = args.date_to or default_to
            else:
                date_from, date_to = args.date_from, args.date_to
            if date_to < date_from:
                parser.error(f"--to ({date_to}) must be >= --from ({date_from})")

            ads = load_ads(cursor, date_from, date_to)
            dwd_expected = read_expected(cursor, date_from, date_to)

        missing = [day for day in _date_range(date_from, date_to) if day not in ads.is_full_day]
        if missing:
            print(
                f"[error] ADS has no row for {len(missing)} date(s), first={missing[0]}",
                file=sys.stderr,
            )
            return 2

        rt_ro_cfg = _rt_config(os.environ, recon_user, recon_password)
        rt_ro_conn = connect_mysql(
            rt_ro_cfg, read_only=True, cursorclass=pymysql.cursors.DictCursor
        )
        with rt_ro_conn.cursor() as cursor:
            rt_metrics, final_flags = read_realtime(cursor, date_from, date_to)

        days = _date_range(date_from, date_to)
        full_days = [day for day in days if ads.is_full_day.get(day) == 1]
        partial_days = [day for day in days if ads.is_full_day.get(day) != 1]

        rows = build_rows(full_days, partial_days, ads.metrics, rt_metrics, checked_at)
        mismatches = crosscheck_ads_vs_dwd(ads, dwd_expected, full_days)

        print(
            f"date_range={date_from}..{date_to} full_days={len(full_days)} "
            f"partial_days={len(partial_days)}"
        )
        print(render_summary(rows, final_flags=final_flags, detail_days=args.detail_days))
        print()
        print(
            f"ADS_VS_DWD full_days={len(full_days)} "
            f"matched={len(full_days) * len(ALL_METRICS) - len(mismatches)} "
            f"diff={len(mismatches)}"
        )
        for item in mismatches[:20]:
            print(
                f"  [ads-vs-dwd] {item.event_date} {item.metric}: "
                f"ads={_format(item.ads_value)} dwd={_format(item.dwd_value)}"
            )
        if len(mismatches) > 20:
            print(f"  ... {len(mismatches) - 20} more")

        written = 0
        if write_enabled:
            app_user, app_password = rt_app_credentials(os.environ)
            rt_app_cfg = _rt_config(os.environ, app_user, app_password)
            rt_app_conn = connect_mysql(
                rt_app_cfg, read_only=False, cursorclass=pymysql.cursors.DictCursor
            )
            written = write_reconciliation(rt_app_conn, rows)
            with rt_app_conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT status, COUNT(*) AS n FROM {RECON_TABLE} "
                    f"WHERE event_date_utc BETWEEN %s AND %s GROUP BY status",
                    (date_from, date_to),
                )
                db_counts = {row["status"]: int(row["n"]) for row in cursor.fetchall()}
            print(f"WROTE table={RECON_TABLE} rows={written} db_counts={db_counts}")
        else:
            print("[dry-run] no rows written; pass --write to upsert")

        summary = summarize_rows(rows)
        clean = summary["DIFF"] == 0 and not mismatches
        print(
            "RECONCILIATION_"
            + ("OK" if clean else "FAILED")
            + f" match={summary['MATCH']} diff={summary['DIFF']} skip={summary['SKIP']}"
        )

        if args.json_out is not None:
            payload = _json_payload(
                rows,
                mismatches,
                date_from=date_from,
                date_to=date_to,
                written=written,
                final_flags=final_flags,
            )
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            print(f"[json] {args.json_out}")

        return 0 if clean else 1
    except (pymysql.MySQLError, KeyError, ValueError, TypeError, RuntimeError, OSError) as exc:
        print(f"[fatal] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        for connection in (rt_app_conn, rt_ro_conn, dwd_conn):
            if connection is not None:
                connection.close()


def _date_range(date_from: date, date_to: date) -> list[date]:
    days: list[date] = []
    current = date_from
    while current <= date_to:
        days.append(current)
        current = date.fromordinal(current.toordinal() + 1)
    return days


if __name__ == "__main__":
    sys.exit(main())

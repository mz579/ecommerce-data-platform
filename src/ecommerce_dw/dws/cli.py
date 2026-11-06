"""Command-line entry point for the DWS/ADS aggregation build (Phase 4).

Examples (run from the project root, venv active):

    python -m ecommerce_dw.dws.cli build                        # full rebuild
    python -m ecommerce_dw.dws.cli build --dates 2015-05-04,2015-05-05
    python -m ecommerce_dw.dws.cli build --force                # reset failed builds
    python -m ecommerce_dw.dws.cli build --top-n 50             # ranking top-50/day
    python -m ecommerce_dw.dws.cli verify [--dates ...]         # independent re-check
    python -m ecommerce_dw.dws.cli status
    python -m ecommerce_dw.dws.cli cleanup --build 3

Credentials come from environment variables / the project .env only.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.dws import builder
from ecommerce_dw.dws.build_log import AggBuildLogRepo

PROJECT_ROOT = Path(__file__).resolve().parents[3]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

log = logging.getLogger("ecommerce_dw.dws")


def _build_config():
    return db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)


def _setup_logging(verbose: bool) -> None:
    root = logging.getLogger()
    if root.handlers:
        return  # pytest / callers already configured the root logger
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)
    log_dir = PROJECT_ROOT / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "dws_ads_build.log", encoding="utf-8")
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    except OSError:  # logs/ unusable -> keep console-only
        pass


def _parse_dates(raw: str | None) -> list[date] | None:
    if not raw:
        return None
    out: list[date] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(date.fromisoformat(token))
    return out or None


def _cmd_build(args: argparse.Namespace) -> None:
    result = builder.build_dws_ads(
        _build_config(),
        dates=_parse_dates(args.dates),
        top_n=args.top_n,
        force=args.force,
    )
    scope = result.dates_csv if result.scope_type == "DATES" else "FULL"
    print(
        f"loaded    : build #{result.build_id} ({scope}) status={result.status} "
        f"rows={result.rows_written:,} across {len(result.table_stats)} tables "
        f"duration={result.duration_seconds:.2f}s"
    )
    for table, rows in result.table_stats.items():
        print(f"  {table:<24} {rows:>12,}")


def _cmd_verify(args: argparse.Namespace) -> None:
    report = builder.verify_dws_ads(_build_config(), dates=_parse_dates(args.dates))
    mismatches = report.mismatches
    print(
        f"verify    : {len(report.checks)} checks across "
        f"{len(sorted({c.day for c in report.checks}))} days -> "
        f"{'ALL MATCH' if not mismatches else f'{len(mismatches)} MISMATCH(ES)'}"
    )
    if mismatches:
        for c in mismatches[:20]:
            print(f"  MISMATCH {c.metric}@{c.day}: ads={c.ads_value!r} direct={c.direct_value!r}")
        raise builder.AggBuildError(f"verify failed: {len(mismatches)} mismatches")


def _cmd_status(args: argparse.Namespace) -> None:
    conn = builder.connect(_build_config())
    try:
        entries = AggBuildLogRepo(conn).list_recent(limit=args.limit)
    finally:
        conn.close()
    if not entries:
        print("no DWS/ADS builds recorded yet")
        return
    print(f"{'build_id':>8}  {'status':<8}  {'scope':<7}  {'rows':>11}  {'started_at':<23}  dates")
    for e in entries:
        started = e.started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(
            f"{e.build_id:>8}  {e.status:<8}  {e.scope_type:<7}  {e.rows_written:>11,}  "
            f"{started}  {e.dates_csv or ''}"
        )


def _cmd_cleanup(args: argparse.Namespace) -> None:
    deleted = builder.cleanup_build(_build_config(), args.build)
    print(f"cleaned build #{args.build}: removed {deleted:,} rows")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecommerce-dw-dws",
        description="DWS/ADS build: dwd_events -> DWS daily aggregates -> ADS metrics.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug-level logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="build/rebuild the DWS/ADS layers")
    p_build.add_argument(
        "--dates",
        help="comma-separated UTC days to rebuild (default: full rebuild), "
        "e.g. --dates 2015-05-04,2015-05-05",
    )
    p_build.add_argument(
        "--top-n",
        type=int,
        default=builder.DEFAULT_TOP_N,
        help=f"daily top-N rows in ads_item_ranking_daily (default {builder.DEFAULT_TOP_N})",
    )
    p_build.add_argument(
        "--force",
        action="store_true",
        help="reset existing failed/crashed builds for the scope before rebuilding",
    )

    p_verify = sub.add_parser(
        "verify",
        help="recompute every ADS metric from dwd_events with independent SQL and diff",
    )
    p_verify.add_argument("--dates", help="restrict the check to these UTC days")

    p_status = sub.add_parser("status", help="show recent DWS/ADS build log entries")
    p_status.add_argument("--limit", type=int, default=20)

    p_cleanup = sub.add_parser("cleanup", help="wipe a build's rows and mark it CLEANED")
    p_cleanup.add_argument("--build", type=int, required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = build_parser().parse_args(argv)
    _setup_logging(args.verbose)
    try:
        if args.command == "build":
            _cmd_build(args)
        elif args.command == "verify":
            _cmd_verify(args)
        elif args.command == "status":
            _cmd_status(args)
        elif args.command == "cleanup":
            _cmd_cleanup(args)
        return 0
    except (builder.AggBuildError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        log.error("%s", exc)
        return 2
    except Exception as exc:
        print(f"unexpected error: {exc}", file=sys.stderr)
        log.error("unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

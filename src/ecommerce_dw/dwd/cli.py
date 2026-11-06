"""Command-line entry point for the DWD build (Phase 3).

Examples (run from the project root, venv active):

    python -m ecommerce_dw.dwd.cli build                      # full rebuild
    python -m ecommerce_dw.dwd.cli build --dates 2015-05-03,2015-05-04
    python -m ecommerce_dw.dwd.cli build --force             # reset a failed build
    python -m ecommerce_dw.dwd.cli verify [--dates ...]      # identity check only
    python -m ecommerce_dw.dwd.cli status
    python -m ecommerce_dw.dwd.cli cleanup --build 7

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
from ecommerce_dw.dwd import builder
from ecommerce_dw.dwd.build_log import BuildLogRepo
from ecommerce_dw.dwd.builder import BuildError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

log = logging.getLogger("ecommerce_dw.dwd")


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
        file_handler = logging.FileHandler(log_dir / "dwd_build.log", encoding="utf-8")
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
    result = builder.build_dwd(
        _build_config(),
        dates=_parse_dates(args.dates),
        chunk_size=args.chunk_size,
        force=args.force,
        max_resumes=args.max_resumes,
        resume_wait_seconds=args.resume_wait,
    )
    scope = result.dates_csv if result.scope_type == "DATES" else "FULL"
    print(
        f"loaded    : build #{result.build_id} ({scope}) status={result.status} "
        f"ods={result.ods_count:,} ok={result.ok_rows:,} dup={result.duplicate_rows:,} "
        f"reject={result.reject_rows:,} "
        f"duration={result.duration_seconds:.2f}s resumed_from={result.resumed_from:,}"
    )


def _cmd_verify(args: argparse.Namespace) -> None:
    counts = builder.verify_build(_build_config(), dates=_parse_dates(args.dates))
    scope = args.dates or "FULL"
    ok = counts["ods"] == counts["ok"] + counts["duplicate"] + counts["reject"]
    print(
        f"verify    : scope={scope} ods={counts['ods']:,} "
        f"ok={counts['ok']:,} dup={counts['duplicate']:,} reject={counts['reject']:,} "
        f"-> identity={'OK' if ok else 'VIOLATED'}"
    )
    if not ok:
        raise BuildError("identity check failed")


def _cmd_status(args: argparse.Namespace) -> None:
    conn = builder.connect(_build_config())  # repo needs a caller-supplied connection
    try:
        entries = BuildLogRepo(conn).list_recent(limit=args.limit)
    finally:
        conn.close()
    if not entries:
        print("no DWD builds recorded yet")
        return
    print(
        f"{'build_id':>8}  {'status':<8}  {'scope':<7}  {'processed':>11}  "
        f"{'ok':>9}  {'dup':>7}  {'reject':>7}  {'started_at':<23}  dates"
    )
    for e in entries:
        started = e.started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(
            f"{e.build_id:>8}  {e.status:<8}  {e.scope_type:<7}  {e.processed_rows:>11,}  "
            f"{e.ok_rows:>9,}  {e.duplicate_rows:>7,}  {e.reject_rows:>7,}  {started}  "
            f"{e.dates_csv or ''}"
        )


def _cmd_cleanup(args: argparse.Namespace) -> None:
    deleted = builder.cleanup_build(_build_config(), args.build)
    print(f"cleaned build #{args.build}: removed {deleted:,} rows")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecommerce-dw-dwd",
        description="DWD build: ODS events -> standard, deduplicated, auditable detail.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug-level logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="build/rebuild the DWD events layer")
    p_build.add_argument(
        "--dates",
        help="comma-separated UTC dates to rebuild (default: full rebuild), "
        "e.g. --dates 2015-05-03,2015-05-04",
    )
    p_build.add_argument("--chunk-size", type=int, default=builder.DEFAULT_CHUNK_SIZE)
    p_build.add_argument(
        "--force",
        action="store_true",
        help="reset an existing failed/crashed build before rebuilding",
    )
    p_build.add_argument(
        "--max-resumes",
        type=int,
        default=builder.DEFAULT_MAX_RESUMES,
        help="retry loop count if MySQL restarts mid-build (0 disables auto-resume)",
    )
    p_build.add_argument(
        "--resume-wait",
        type=int,
        default=builder.DEFAULT_RESUME_WAIT_SECONDS,
        help="seconds to wait for MySQL to come back per resume attempt",
    )

    p_verify = sub.add_parser("verify", help="run the ODS=DWD+dup+reject identity check")
    p_verify.add_argument("--dates", help="scope as in build; default full")

    p_status = sub.add_parser("status", help="show recent DWD build log entries")
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
    except (BuildError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        log.error("%s", exc)
        return 2
    except Exception as exc:
        print(f"unexpected error: {exc}", file=sys.stderr)
        log.error("unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

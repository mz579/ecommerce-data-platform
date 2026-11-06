"""Unified command-line entry point for ecommerce_dw (Phase 5).

One entry for the incremental / quality / publish workflow::

    python -m ecommerce_dw.cli incremental --init --to 2015-09-16   # bootstrap watermark
    python -m ecommerce_dw.cli incremental                          # tail + in-window late
    python -m ecommerce_dw.cli incremental --from 2015-09-10 --to 2015-09-12  # backfill
    python -m ecommerce_dw.cli quality run                          # hard gate, full library
    python -m ecommerce_dw.cli quality run --dates 2015-09-17,2015-09-18
    python -m ecommerce_dw.cli quality rules
    python -m ecommerce_dw.cli quality show [run_id]
    python -m ecommerce_dw.cli publish                              # full ADS publish
    python -m ecommerce_dw.cli publish --dates 2015-09-17,2015-09-18
    python -m ecommerce_dw.cli verify [--dates ...]
    python -m ecommerce_dw.cli status

A FAILed quality gate (or a publish request without a PASSing run) exits non-zero
and refuses to write ``ads_publish_log``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.dws import builder as dws_builder
from ecommerce_dw.incremental import IncrementalError, init_watermark, run_incremental
from ecommerce_dw.incremental.planner import compute_frontier
from ecommerce_dw.incremental.watermark import DEFAULT_PIPELINE, WatermarkRepo
from ecommerce_dw.quality import (
    QualityGateError,
    build_rule_catalog,
    list_run_results,
    list_runs,
    publish_ads_scope,
    run_quality_gate,
)
from ecommerce_dw.quality.runner import connect as quality_connect

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

log = logging.getLogger("ecommerce_dw.cli")


def _build_config():
    return db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)


def _setup_logging(verbose: bool) -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)
    log_dir = PROJECT_ROOT / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        root.addHandler(logging.FileHandler(log_dir / "phase5_ops.log", encoding="utf-8"))
    except OSError:
        pass


def _parse_dates(raw: str | None) -> tuple[date, ...] | None:
    if not raw:
        return None
    out: list[date] = []
    for token in raw.split(","):
        token = token.strip()
        if token:
            out.append(date.fromisoformat(token))
    return tuple(sorted(out))


# --------------------------------------------------------------------------- #
# commands                                                                    #
# --------------------------------------------------------------------------- #


def _cmd_incremental(args: argparse.Namespace) -> int:
    cfg = _build_config()
    if args.init:
        to = date.fromisoformat(args.to) if args.to else None
        written = init_watermark(cfg, pipeline=args.pipeline, to_date=to)
        print(f"watermark init: pipeline={args.pipeline} dates_recorded={written}")
        return 0
    result = run_incremental(
        cfg,
        from_date=date.fromisoformat(args.from_date) if args.from_date else None,
        to_date=date.fromisoformat(args.to) if args.to else None,
        pipeline=args.pipeline,
        late_window_days=args.late_window,
        dry_run=args.dry_run,
        force=args.force,
        top_n=args.top_n,
    )
    if result.dry_run:
        print(
            f"dry-run plan: pipeline={result.pipeline} dates={result.dates_csv or '(none)'} "
            f"([{compute_frontier_label(cfg, args.pipeline)}])"
        )
        return 0
    print(
        f"incremental: pipeline={result.pipeline} built={result.dates_csv or '(none)'} "
        f"dwd_builds={','.join(map(str, result.dwd_build_ids)) or '-'} "
        f"dwd_rows={result.dwd_rows:,} agg_build=#{result.agg_build_id} "
        f"agg_rows={result.agg_rows:,} watermark_upserted={result.watermark_upserted:,} "
        f"duration={result.duration_seconds:.2f}s"
    )
    if result.late_dates:
        print(f"  late (in-window): {', '.join(d.isoformat() for d in result.late_dates)}")
    if result.out_of_window_dates:
        print(
            f"  WARNING out-of-window corrections not auto-processed "
            f"(use --from/--to): "
            f"{', '.join(d.isoformat() for d in result.out_of_window_dates)}"
        )
    return 0


def compute_frontier_label(cfg, pipeline: str) -> str:
    conn = quality_connect(cfg)
    try:
        wm = WatermarkRepo(conn).load(pipeline)
    finally:
        conn.close()
    frontier = compute_frontier(wm, {})
    return "no watermark yet" if frontier is None else frontier.isoformat()


def _cmd_quality(args: argparse.Namespace) -> int:
    cfg = _build_config()
    if args.action == "rules":
        rules = build_rule_catalog()
        for rule in rules:
            print(f"{rule.key:<42} {rule.category:<14} {rule.description}")
        print(f"--- total rules: {len(rules)}")
        return 0
    if args.action == "show":
        runs = list_runs(cfg, limit=args.limit)
        if not runs:
            print("no quality runs recorded yet")
            return 0
        print(
            "run_id   status  scope  dates                    rules  passed  failed  "
            "checks_total  started_at"
        )
        for r in runs:
            scope = r.scope_type + (" " + r.dates_csv if r.dates_csv else "")
            print(
                f"{r.run_id:>6}  {r.status:<6} {scope:<29} "
                f"{r.rules_total:>5} {r.passed:>6} {r.failed:>6} "
                f"{r.checks_total:>12,}  {r.started_at:%Y-%m-%d %H:%M:%S}"
            )
        if args.run_id:
            rows = list_run_results(cfg, args.run_id)
            if not rows:
                print(f"  (no result rows for run #{args.run_id})")
            else:
                print(f"  rule results for run #{args.run_id}:")
                for rule_key, _category, status, checked, failed, detail, ms in rows:
                    extra = f"  -> {detail}" if detail else ""
                    print(
                        f"    {status:<5} {rule_key:<42} checked={checked:>12,} "
                        f"failed={failed:>8,} {ms}ms{extra}"
                    )
        return 0

    dates = _parse_dates(args.dates)
    try:
        report = run_quality_gate(cfg, dates=dates)
    except QualityGateError as exc:
        print(exc, file=sys.stderr)
        return 2
    print(
        f"quality gate: run #{report.run_id} {report.status} "
        f"{report.passed}/{report.rules_total} rules passed "
        f"({report.checks_total:,} rows checked) "
        f"duration={(report.finished_at - report.started_at).total_seconds():.2f}s"
    )
    return 0


def _cmd_publish(args: argparse.Namespace) -> int:
    cfg = _build_config()
    dates = _parse_dates(args.dates)
    try:
        publish_id = publish_ads_scope(cfg, dates=dates, quality_run_id=args.run)
    except QualityGateError as exc:
        print(exc, file=sys.stderr)
        return 2
    scope = "FULL" if dates is None else "DATES"
    label = ",".join(d.isoformat() for d in dates) if dates else ""
    print(
        f"publish ok: publish_id={publish_id} scope={scope} dates={label or '(full)'} "
        f"quality_run={args.run or 'fresh-gate'}"
    )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    report = dws_builder.verify_dws_ads(_build_config(), dates=_parse_dates(args.dates))
    mismatches = report.mismatches
    print(
        f"verify: {len(report.checks)} checks -> "
        f"{'ALL MATCH' if not mismatches else f'{len(mismatches)} MISMATCH(ES)'}"
    )
    for c in mismatches[:20]:
        print(f"  MISMATCH {c.metric}@{c.day}: ads={c.ads_value!r} direct={c.direct_value!r}")
    return 0 if not mismatches else 2


def _cmd_status(args: argparse.Namespace) -> int:
    cfg = _build_config()
    conn = quality_connect(cfg)
    try:
        wm = WatermarkRepo(conn).load(args.pipeline)
    finally:
        conn.close()
    frontier = compute_frontier(wm, {})
    print(f"watermark: pipeline={args.pipeline} dates={len(wm)} frontier={frontier}")
    if not wm:
        print("  (no watermark yet - run `incremental --init` first)")
    runs = list_runs(cfg, limit=args.limit)
    if runs:
        print(f"recent quality runs ({len(runs)}):")
        for r in runs:
            print(
                f"  #{r.run_id} {r.status:<5} dates={r.dates_csv or 'FULL':<30} "
                f"rules={r.passed}/{r.rules_total} checks={r.checks_total:,}"
            )
    return 0


# --------------------------------------------------------------------------- #
# parser                                                                      #
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecommerce-dw",
        description="Incremental ETL, quality gates and ADS publishing.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p_inc = sub.add_parser("incremental", help="watermark-driven incremental run")
    p_inc.add_argument("--init", action="store_true", help="bootstrap the watermark")
    p_inc.add_argument("--to", help="YYYY-MM-DD cap for --init (default: window end)")
    p_inc.add_argument("--from", dest="from_date", help="backfill range start (YYYY-MM-DD)")
    p_inc.add_argument("--late-window", type=int, default=7, help="late-data lookback in days")
    p_inc.add_argument("--pipeline", default=DEFAULT_PIPELINE)
    p_inc.add_argument("--dry-run", action="store_true", help="plan without building")
    p_inc.add_argument("--force", action="store_true", help="reset failed/crashed builds")
    p_inc.add_argument("--top-n", type=int, default=dws_builder.DEFAULT_TOP_N)

    p_q = sub.add_parser("quality", help="quality gate commands")
    qsub = p_q.add_subparsers(dest="action", required=True)
    q_run = qsub.add_parser("run", help="execute the gate (hard fail semantics)")
    q_run.add_argument("--dates", help="comma-separated UTC days to scope the gate")
    qsub.add_parser("rules", help="list the rule catalog")
    q_show = qsub.add_parser("show", help="recent gate runs")
    q_show.add_argument("--run", dest="run_id", type=int, help="show per-rule detail")
    q_show.add_argument("--limit", type=int, default=10)

    p_pub = sub.add_parser("publish", help="publish ADS after a PASSing gate")
    p_pub.add_argument("--dates", help="comma-separated UTC days (default: FULL)")
    p_pub.add_argument("--run", type=int, help="reuse a PASS run id (default: fresh gate)")

    p_ver = sub.add_parser("verify", help="independent ADS metric re-checks")
    p_ver.add_argument("--dates", help="comma-separated UTC days")

    p_st = sub.add_parser("status", help="watermark frontier + recent gate runs")
    p_st.add_argument("--pipeline", default=DEFAULT_PIPELINE)
    p_st.add_argument("--limit", type=int, default=10)
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = build_parser().parse_args(argv)
    _setup_logging(args.verbose)
    try:
        if args.command == "incremental":
            return _cmd_incremental(args)
        if args.command == "quality":
            return _cmd_quality(args)
        if args.command == "publish":
            return _cmd_publish(args)
        if args.command == "verify":
            return _cmd_verify(args)
        if args.command == "status":
            return _cmd_status(args)
        return 1
    except (QualityGateError, IncrementalError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        log.error("%s", exc)
        return 2
    except Exception as exc:
        print(f"unexpected error: {exc}", file=sys.stderr)
        log.error("unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

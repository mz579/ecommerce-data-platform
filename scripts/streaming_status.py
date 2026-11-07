"""CLI wrapper: realtime job / checkpoint / lag / RT freshness status (Phase 9.4).

Implementation lives in ``ecommerce_dw.streaming.monitoring``.

Examples:

    python scripts/streaming_status.py
    python scripts/streaming_status.py --job-id <32-hex> --json
    python scripts/streaming_status.py --baseline --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import DbConfig, db_config_from_env
from ecommerce_dw.streaming.monitoring import (
    collect_status,
    discover_bootstrap,
    production_baseline,
    rt_freshness,
)
from ecommerce_dw.streaming.reconcile_all import connect_mysql
from ecommerce_dw.streaming.verify_rt import rt_recon_credentials

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flink-url", default=os.environ.get("FLINK_REST_URL", "http://localhost:8081")
    )
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--group", default="ecommerce-rt-flink-v1-recon")
    parser.add_argument("--topic", default="ecommerce.events.v1")
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--json", action="store_true", help="emit full JSON")
    parser.add_argument("--baseline", action="store_true", help="check the 13 production tables")
    parser.add_argument("--no-rt", action="store_true", help="skip RT freshness queries")
    return parser


def _connect_rt_ro():
    user, password = rt_recon_credentials(os.environ)
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    return connect_mysql(
        DbConfig(
            engine="mysql",
            host=cfg.host,
            port=cfg.port,
            user=user,
            password=password,
            database="ecommerce_rt",
        ),
        read_only=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = build_arg_parser().parse_args(argv)
    bootstrap = args.bootstrap or discover_bootstrap()

    if args.baseline:
        cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
        connection = connect_mysql(
            cfg,
            read_only=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            payload = production_baseline(connection)
        finally:
            connection.close()
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(f"PRODUCTION_BASELINE {'MATCH' if payload['match'] else 'MISMATCH'}")
            for table, count in payload["observed"].items():
                marker = "OK" if table not in payload["mismatches"] else "DIFF"
                print(f"  {table:28s} {count:>10d} {marker}")
        return 0 if payload["match"] else 1

    status = collect_status(
        flink_url=args.flink_url,
        job_id=args.job_id,
        group=args.group,
        topic=args.topic,
        bootstrap=bootstrap,
    )
    if not args.no_rt:
        try:
            connection = _connect_rt_ro()
            try:
                status["rt"] = rt_freshness(connection)
            finally:
                connection.close()
        except Exception as exc:
            status["rt_error"] = f"{type(exc).__name__}: {exc}"

    if args.json:
        print(json.dumps(status, ensure_ascii=False, indent=2))
    else:
        selected = status.get("selected_job") or {}
        print(f"collected_at={status['collected_at']}")
        print(
            f"job={selected.get('jid')} state={selected.get('state')} "
            f"start={selected.get('start-time')} duration_ms={selected.get('duration')}"
        )
        checkpoints = status.get("checkpoints")
        if checkpoints:
            print(
                "checkpoints: completed={} failed={} success_rate={} "
                "duration_p95_ms={} state_p95_bytes={}".format(
                    checkpoints["counts"]["completed"],
                    checkpoints["counts"]["failed"],
                    checkpoints["success_rate"],
                    checkpoints["duration_ms"]["p95"],
                    checkpoints["state_size_bytes"]["p95"],
                )
            )
        lag = status.get("kafka_lag")
        if lag:
            print(
                "kafka_lag: total={} partitions={}".format(
                    lag["total_lag"],
                    [
                        (item["partition"], item["committed"], item["end"], item["lag"])
                        for item in lag["partitions"]
                    ],
                )
            )
        rt = status.get("rt")
        if rt:
            print(
                "rt_traffic: rows={} final={} max_updated_at={} last_final_date={}".format(
                    rt["traffic"]["rows"],
                    rt["traffic"]["final_rows"],
                    rt["traffic"]["max_updated_at"],
                    rt["traffic"]["last_final_date"],
                )
            )
            for table, info in rt["tables"].items():
                print(f"  {table:28s} rows={info['rows']}")
        if "rt_error" in status:
            print(f"rt_error={status['rt_error']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

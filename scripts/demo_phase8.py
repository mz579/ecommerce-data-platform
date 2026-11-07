"""Phase 8 — one-command 3-5 minute end-to-end demonstration.

The demo is deliberately isolated from the production warehouse: it provisions
its own ``*_test`` schema on the existing MySQL server, loads the hand-computed
miniature fixture from ``tests/fixtures/phase8_manual/``, and drives the real CLI
chain (incremental -> late-data rebuild -> quality gate -> publish -> verify).
Production row counts are snapshotted before and after and must match the Phase 8
baseline exactly; the demo itself never writes to ``ecommerce_dw``.

Usage (project root, venv active)::

    python scripts/demo_phase8.py                 # ~10-30s, cleans up after itself
    python scripts/demo_phase8.py --keep          # keep the demo schema for inspection
    python scripts/demo_phase8.py --skip-http     # skip Airflow/Superset /health probes

Prerequisites: MySQL/PostgreSQL/Airflow/Superset containers are up (see README),
``.env`` has ``MYSQL_ROOT_PASSWORD`` (schema provisioning) and the app credentials.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import pymysql
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "phase8_manual"
DDL_DIR = PROJECT_ROOT / "sql" / "ddl"
DEFAULT_DEMO_DB = "ecommerce_dw_phase8_demo_test"

# Phase 8 acceptance baseline for the production warehouse. The demo must not
# change a single row here.
PRODUCTION_BASELINE = {
    "ods_events_raw": 2_756_101,
    "dwd_events": 2_755_641,
    "dws_user_daily": 1_649_534,
    "dws_item_daily": 1_672_186,
    "dws_category_daily": 94_194,
    "ads_traffic_daily": 139,
    "ads_funnel_daily": 1_251,
    "ads_item_ranking_daily": 13_900,
    "ads_retention_cohort": 278,
    "etl_watermark": 139,
    "ads_funnel_interval": 1,
    "ads_item_behavior_daily": 139,
    "ads_publish_log": 12,
}

_DDL_FILES = (
    "ods_etl_batch_log.sql",
    "ods_events_raw.sql",
    "ods_item_properties_raw.sql",
    "ods_category_tree_raw.sql",
    "etl_watermark.sql",
    "quality_check_run.sql",
    "quality_check_result.sql",
)
_DASHBOARDS = {
    "Operations overview": "/superset/dashboard/1/",
    "User behavior": "/superset/dashboard/2/",
    "Product analysis": "/superset/dashboard/3/",
}


class DemoError(RuntimeError):
    pass


@contextmanager
def step(index: int, title: str):
    print(f"\n[{index}/8] {title}")
    started = time.perf_counter()
    try:
        yield
    finally:
        print(f"      -> {time.perf_counter() - started:.2f}s")


def _mysql_connect(cfg: dict, database: str | None = None, autocommit: bool = False):
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=database,
        charset="utf8mb4",
        connect_timeout=10,
        autocommit=autocommit,
    )


def _app_cfg() -> dict:
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "ecom_app"),
        "password": os.environ.get("MYSQL_PASSWORD", ""),
        "database": os.environ.get("MYSQL_DATABASE", "ecommerce_dw"),
    }


def _pg_cfg() -> dict:
    return {
        "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "user": os.environ.get("POSTGRES_USER", "meta_app"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
        "database": os.environ.get("POSTGRES_DB", "meta"),
    }


def _http_health(url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return f"HTTP {resp.status}"
    except urllib.error.URLError as exc:
        return f"UNREACHABLE ({exc.reason})"


def _counts(cfg: dict, tables) -> dict[str, int]:
    conn = _mysql_connect(cfg, cfg["database"])
    try:
        cur = conn.cursor()
        out = {}
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            out[table] = int(cur.fetchone()[0])
        return out
    finally:
        conn.close()


def _check_baseline(label: str, cfg: dict) -> bool:
    actual = _counts(cfg, PRODUCTION_BASELINE)
    bad = {
        t: (PRODUCTION_BASELINE[t], actual[t])
        for t in PRODUCTION_BASELINE
        if actual[t] != PRODUCTION_BASELINE[t]
    }
    if bad:
        print(f"  {label}: MISMATCH")
        for table, (expected, got) in bad.items():
            print(f"    {table}: expected {expected:,} got {got:,}")
        return False
    print(f"  {label}: all {len(PRODUCTION_BASELINE)} production tables match the baseline")
    return True


def _provision_demo_db(admin: dict, app: dict, database: str) -> None:
    conn = _mysql_connect(admin, autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS `{database}`")
        cur.execute(
            f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
        )
        cur.execute(f"GRANT ALL PRIVILEGES ON `{database}`.* TO '{app['user']}'@'%'")
    finally:
        conn.close()

    files = [DDL_DIR / name for name in _DDL_FILES]
    files += sorted(DDL_DIR.glob("dwd_*.sql"))
    files += sorted(DDL_DIR.glob("dws_*.sql"))
    files += sorted(DDL_DIR.glob("ads_*.sql"))
    conn = _mysql_connect(app, database)
    try:
        cur = conn.cursor()
        for path in files:
            cur.execute(path.read_text(encoding="utf-8").strip().rstrip(";"))
        conn.commit()
    finally:
        conn.close()
    print(f"  provisioned {database} ({len(files)} DDL files)")


def _drop_demo_db(admin: dict, database: str) -> None:
    conn = _mysql_connect(admin, autocommit=True)
    try:
        conn.cursor().execute(f"DROP DATABASE IF EXISTS `{database}`")
    finally:
        conn.close()
    print(f"  dropped demo schema {database}")


def _load_fixture(app: dict, database: str, filename: str, table: str) -> str:
    from ecommerce_dw.config import DbConfig
    from ecommerce_dw.ingestion import loader

    cfg = DbConfig(
        engine="mysql",
        host=app["host"],
        port=app["port"],
        user=app["user"],
        password=app["password"],
        database=database,
    )
    result = loader.load_csv(cfg, FIXTURES / filename, table)
    print(f"  loaded {filename} -> {table} ({result.rows_committed} rows, batch {result.batch_id})")
    return result.source_file


def _remove_injected_bad_row(app: dict, database: str, source_file: str) -> None:
    """Drop the intentionally bad raw row after showing DWD quarantined it.

    The fixture ships one illegal ``purchase`` row so the demo can prove the
    DWD reject channel works. The quality gate's ``ods_event_type_valid`` rule
    is a raw-ODS contract check, so a production-clean warehouse must not retain
    it. This removes the row from ODS + DWD reject and resyncs the batch count;
    it changes no healthy event or metric.
    """
    conn = _mysql_connect(app, database, autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM ods_events_raw WHERE source_file = %s AND event = 'purchase'",
            (source_file,),
        )
        ods_deleted = cur.rowcount
        cur.execute(
            "DELETE FROM dwd_events_reject WHERE source_file = %s AND raw_event = 'purchase'",
            (source_file,),
        )
        reject_deleted = cur.rowcount
        cur.execute(
            "UPDATE etl_batch_log b SET b.row_count = ("
            "  SELECT COUNT(*) FROM ods_events_raw o"
            "  WHERE o.source_file = b.source_file AND o.ingest_batch_id = b.batch_id)"
            " WHERE b.source_file = %s AND b.status = 'LOADED'",
            (source_file,),
        )
    finally:
        conn.close()
    print(
        f"  quarantined bad row removed after DWD reject evidence: "
        f"ods_deleted={ods_deleted} reject_deleted={reject_deleted}"
    )


def _run_cli(args: list[str], demo_db: str, *, expect: int = 0) -> str:
    env = os.environ.copy()
    env["MYSQL_DATABASE"] = demo_db
    cmd = [sys.executable, "-m", "ecommerce_dw.cli", *args]
    completed = subprocess.run(
        cmd, cwd=PROJECT_ROOT, env=env, text=True, capture_output=True, timeout=600
    )
    output = (completed.stdout or "") + (completed.stderr or "")
    for line in output.strip().splitlines():
        print(f"    | {line}")
    if completed.returncode != expect:
        raise DemoError(f"command {' '.join(args)} exited {completed.returncode}: {output}")
    return output


def _demo_metrics(app: dict, demo_db: str) -> None:
    conn = _mysql_connect(app, demo_db)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT event_date_utc, pv, uv, dau FROM ads_traffic_daily ORDER BY event_date_utc"
        )
        print("  traffic:", ", ".join(f"{d}={pv}/{uv}/{dau}" for d, pv, uv, dau in cur.fetchall()))
        cur.execute(
            "SELECT event_date_utc, metric_name, metric_value FROM ads_funnel_daily "
            "WHERE metric_name IN ('view_users','cart_users','buyer_users','orders') "
            "ORDER BY event_date_utc, metric_name"
        )
        print("  funnel:", ", ".join(f"{d}:{m}={v}" for d, m, v in cur.fetchall()))
        cur.execute(
            "SELECT cohort_date, age_day, cohort_size, retained_users, retention_rate "
            "FROM ads_retention_cohort WHERE age_day = 1 ORDER BY cohort_date"
        )
        print(
            "  retention D1:",
            ", ".join(f"{d}={u}/{s} ({r})" for d, _a, s, u, r in cur.fetchall()),
        )
    finally:
        conn.close()


def _print_dashboards() -> None:
    base = f"http://localhost:{os.environ.get('SUPERSET_UI_PORT', '8088')}"
    print("  dashboards (production, read-only):")
    for name, path in _DASHBOARDS.items():
        print(f"    {name}: {base}{path}")
    print(f"    Airflow: http://localhost:{os.environ.get('AIRFLOW_UI_PORT', '8080')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", action="store_true", help="keep the demo schema")
    parser.add_argument("--skip-http", action="store_true", help="skip HTTP health probes")
    parser.add_argument(
        "--database",
        default=os.environ.get("DEMO_DATABASE", DEFAULT_DEMO_DB),
        help=f"demo schema (must end with _test; default {DEFAULT_DEMO_DB})",
    )
    args = parser.parse_args()
    if not args.database.endswith("_test"):
        print(f"refusing to use non-test schema {args.database!r}", file=sys.stderr)
        return 2

    load_dotenv(PROJECT_ROOT / ".env")
    app = _app_cfg()
    admin = {**app, "user": "root", "password": os.environ.get("MYSQL_ROOT_PASSWORD", "")}
    demo_db = args.database

    print("=" * 78)
    print("Phase 8 demo — isolated end-to-end run (production warehouse is read-only)")
    print(f"demo schema: {demo_db}")
    print("=" * 78)

    try:
        with step(1, "stack health (MySQL / PostgreSQL / Airflow / Superset)"):
            conn = _mysql_connect(app, app["database"])
            cur = conn.cursor()
            cur.execute("SELECT VERSION()")
            print(f"  MySQL: {cur.fetchone()[0]} @ {app['host']}:{app['port']}")
            conn.close()
            import psycopg

            pg = _pg_cfg()
            with psycopg.connect(
                host=pg["host"],
                port=pg["port"],
                user=pg["user"],
                password=pg["password"],
                dbname=pg["database"],
                connect_timeout=5,
            ) as pconn:
                with pconn.cursor() as pcur:
                    pcur.execute("SELECT version()")
                    print(f"  PostgreSQL: {pcur.fetchone()[0].split(',')[0]}")
            if not args.skip_http:
                airflow = _http_health(
                    f"http://127.0.0.1:{os.environ.get('AIRFLOW_UI_PORT', '8080')}/health"
                )
                superset = _http_health(
                    f"http://127.0.0.1:{os.environ.get('SUPERSET_UI_PORT', '8088')}/health"
                )
                print(f"  Airflow /health: {airflow}")
                print(f"  Superset /health: {superset}")

        with step(2, "production baseline snapshot (must not change)"):
            if not _check_baseline("before", app):
                return 2

        with step(3, f"provision isolated schema {demo_db} + apply all layer DDL"):
            _provision_demo_db(admin, app, demo_db)

        with step(4, "load miniature fixture and run incremental (new tail D1/D2)"):
            events_file = _load_fixture(app, demo_db, "events_batch1.csv", "ods_events_raw")
            _load_fixture(app, demo_db, "item_properties.csv", "ods_item_properties_raw")
            _run_cli(["incremental"], demo_db)

        with step(5, "late batch arrives -> incremental detects and rebuilds D1 only"):
            _load_fixture(app, demo_db, "events_batch2_late.csv", "ods_events_raw")
            _run_cli(["incremental"], demo_db)
            print("  stable-state re-run (idempotency):")
            _run_cli(["incremental"], demo_db)
            _remove_injected_bad_row(app, demo_db, events_file)

        with step(6, "quality gate (60 hard rules) + publish + independent verify"):
            gate_out = _run_cli(["quality", "run"], demo_db)
            match = re.search(r"run #(\d+)", gate_out)
            if not match:
                raise DemoError("could not parse quality run id from CLI output")
            run_id = match.group(1)
            _run_cli(["publish", "--run", run_id], demo_db)
            _run_cli(["verify"], demo_db)
            _demo_metrics(app, demo_db)

        with step(7, "production baseline after-run snapshot"):
            if not _check_baseline("after", app):
                return 2

        with step(8, "dashboard links + cleanup"):
            _print_dashboards()
    finally:
        if not args.keep:
            try:
                _drop_demo_db(admin, demo_db)
            except Exception as exc:  # best-effort cleanup
                print(f"  WARNING cleanup failed: {exc}", file=sys.stderr)
        else:
            print(f"  kept demo schema {demo_db}")

    print(
        "\nDemo completed: real CLI chain ran against an isolated *_test schema; "
        "production row counts unchanged."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

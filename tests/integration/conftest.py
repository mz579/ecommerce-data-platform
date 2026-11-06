"""Shared test-schema provisioning for MySQL integration tests (Phase 2/3).

Every MySQL integration test runs against a dedicated ``*_test`` schema so that
a crashed / killed test run can never leave rows or log entries in the
production ``ecommerce_dw`` database. The session fixture ``mysql_testdb``:

1. **guards** the target (raises, never skips) unless the resolved schema name
   ends with ``_test`` — a fixture pointed at the production database must fail
   loudly, not fall back to it;
2. **provisions**: connects as ``root`` (password from ``MYSQL_ROOT_PASSWORD``),
   drops + recreates the schema, and grants the app user full rights on it;
3. **applies** the ODS + DWD + DWS + ADS DDL to the fresh schema;
4. yields the ``DbConfig`` and **drops the schema** afterwards.

If the schema cannot be created/reached, the whole integration module is
skipped with an explicit reason (never silently).
"""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

import pymysql
import pytest
from dotenv import load_dotenv

from ecommerce_dw.config import DbConfig, db_config_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_MYSQL_BASE_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": ""}

# Files that belong to the ODS layer (DWD/DWS/ADS files come from their own globs).
_ODS_DDL_FILES = (
    "ods_etl_batch_log.sql",
    "ods_events_raw.sql",
    "ods_item_properties_raw.sql",
    "ods_category_tree_raw.sql",
)


def resolve_test_cfg() -> DbConfig:
    """Build the app-side DbConfig for the dedicated test schema.

    Raises (never skips, never falls back) unless the schema name ends with
    ``_test``. This is the single resolution point every integration-test
    fixture goes through, so a misconfigured pointer to the production database
    surfaces as a hard error.
    """
    load_dotenv(PROJECT_ROOT / ".env")
    base = db_config_from_env("MYSQL", engine="mysql", defaults=dict(_MYSQL_BASE_DEFAULTS))
    test_database = os.environ.get("MYSQL_TEST_DATABASE") or f"{base.database}_test"
    cfg = replace(base, database=test_database)
    if not cfg.database.endswith("_test"):
        raise RuntimeError(
            "integration tests refuse to run against a non-test schema: got "
            f"{cfg.database!r} (from MYSQL_TEST_DATABASE / MYSQL_DATABASE); "
            "the schema name must end with '_test'"
        )
    return cfg


def _admin_config(cfg: DbConfig) -> DbConfig:
    return replace(
        cfg,
        user="root",
        password=os.environ.get("MYSQL_ROOT_PASSWORD", ""),
        database="",
    )


def _apply_schema(cfg: DbConfig) -> None:
    """Create the ODS + DWD + DWS + ADS tables in the test schema (IF NOT EXISTS)."""
    ddl_dir = PROJECT_ROOT / "sql" / "ddl"
    files = (
        [ddl_dir / name for name in _ODS_DDL_FILES]
        + sorted(ddl_dir.glob("dwd_*.sql"))
        + sorted(ddl_dir.glob("dws_*.sql"))
        + sorted(ddl_dir.glob("ads_*.sql"))
    )
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
    )
    try:
        for path in files:
            statement = path.read_text(encoding="utf-8").strip().rstrip(";")
            with conn.cursor() as cur:
                cur.execute(statement)
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="session")
def mysql_testdb() -> DbConfig:
    """Provision a fresh dedicated test schema and hand tests the DbConfig."""
    cfg = resolve_test_cfg()
    admin = _admin_config(cfg)
    provisioned_here = False

    admin_conn = None
    try:
        admin_conn = pymysql.connect(
            host=admin.host,
            port=admin.port,
            user=admin.user,
            password=admin.password,
            charset="utf8mb4",
            connect_timeout=3,
        )
    except Exception as admin_exc:  # pragma: no cover - environment-dependent
        # Fall back: a leftover test schema may already be usable by the app
        # user (e.g. when root access is unavailable). Tests still stay inside
        # the dedicated *_test schema, never near production.
        try:
            probe = pymysql.connect(
                host=cfg.host,
                port=cfg.port,
                user=cfg.user,
                password=cfg.password,
                database=cfg.database,
                charset="utf8mb4",
                connect_timeout=3,
            )
            probe.close()
        except Exception as app_exc:  # pragma: no cover - environment-dependent
            pytest.skip(
                "cannot provision the dedicated test schema: admin (root) "
                f"connection failed ({type(admin_exc).__name__}: {admin_exc}) "
                f"and the app user cannot reach existing {cfg.database!r} "
                f"({type(app_exc).__name__}: {app_exc}); "
                "set MYSQL_ROOT_PASSWORD in .env"
            )
            return  # pragma: no cover - pytest.skip always raises
    else:
        try:
            with admin_conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS `{cfg.database}`")
                cur.execute(
                    f"CREATE DATABASE `{cfg.database}` CHARACTER SET utf8mb4 "
                    "COLLATE utf8mb4_0900_ai_ci"
                )
                cur.execute(f"GRANT ALL PRIVILEGES ON `{cfg.database}`.* TO '{cfg.user}'@'%'")
            admin_conn.commit()
        finally:
            admin_conn.close()
        _apply_schema(cfg)
        provisioned_here = True

    yield cfg

    if provisioned_here:
        try:
            conn = pymysql.connect(
                host=admin.host,
                port=admin.port,
                user=admin.user,
                password=admin.password,
                charset="utf8mb4",
                connect_timeout=3,
            )
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS `{cfg.database}`")
            conn.commit()
            conn.close()
        except Exception as exc:  # pragma: no cover - environment-dependent
            print(f"[test-schema teardown] could not drop {cfg.database!r}: {exc}")

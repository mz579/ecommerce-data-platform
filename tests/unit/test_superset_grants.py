"""Pure-logic tests for the Superset grant script (no database connection)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "apply_superset_grants.py"
_spec = importlib.util.spec_from_file_location("apply_superset_grants", SCRIPT)
grants = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(grants)


def test_resolve_ro_user_defaults_to_superset_ro():
    assert grants.resolve_ro_user({}) == "superset_ro"
    assert grants.resolve_ro_user({"MYSQL_RO_USER": ""}) == "superset_ro"


def test_resolve_ro_user_honours_env():
    assert grants.resolve_ro_user({"MYSQL_RO_USER": "analytics_ro"}) == "analytics_ro"


def test_build_grant_sql_targets_configured_user():
    sql = grants.build_grant_sql(
        "ecommerce_dw", "analytics_ro", "s3cret", ["ads_traffic_daily", "v_ads_x"]
    )
    joined = "\n".join(sql)
    assert "CREATE USER IF NOT EXISTS 'analytics_ro'@'%'" in joined
    assert "ALTER USER 'analytics_ro'@'%' IDENTIFIED BY 's3cret'" in joined
    assert "GRANT SELECT ON `ecommerce_dw`.`ads_traffic_daily` TO 'analytics_ro'@'%'" in joined
    assert "GRANT SELECT ON `ecommerce_dw`.`v_ads_x` TO 'analytics_ro'@'%'" in joined
    assert "superset_ro" not in joined


def test_build_grant_sql_redacts_password_for_dry_run():
    sql = grants.build_grant_sql(
        "ecommerce_dw", "superset_ro", "s3cret", ["ads_traffic_daily"], redact_password=True
    )
    joined = "\n".join(sql)
    assert "s3cret" not in joined
    assert "IDENTIFIED BY '<MYSQL_RO_PASSWORD>'" in joined


def test_escape_literal_handles_quotes_and_backslashes():
    assert grants._escape_literal("a'b\\c") == "a''b\\\\c"

"""Pure-logic tests for RT DDL / grant scripts and their database guards."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

from ecommerce_dw.streaming import rt_ddl, rt_grants

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_script(name: str):
    path = PROJECT_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


apply_rt_ddl = _load_script("apply_rt_ddl")
apply_rt_grants = _load_script("apply_rt_grants")


def test_database_guard_allows_rt_and_test_schemas():
    assert rt_ddl.is_allowed_database("ecommerce_rt")
    assert rt_ddl.is_allowed_database("ecommerce_rt_test")
    assert rt_ddl.is_allowed_database("anything_test")


def test_database_guard_rejects_production_and_unsafe_names():
    assert not rt_ddl.is_allowed_database("ecommerce_dw")
    assert not rt_ddl.is_allowed_database("ecommerce_rt_prod")
    assert not rt_ddl.is_allowed_database("")
    assert not rt_ddl.is_allowed_database("rt; DROP DATABASE ecommerce_dw")
    assert not rt_ddl.is_allowed_database("ecommerce-rt")


def test_ensure_allowed_database_raises_with_explicit_message():
    with pytest.raises(ValueError, match="ecommerce_dw"):
        rt_ddl.ensure_allowed_database("ecommerce_dw")


def test_ddl_file_defines_exactly_the_five_design_tables():
    statements = rt_ddl.load_ddl_statements()
    names = []
    for statement in statements:
        match = re.match(r"CREATE TABLE IF NOT EXISTS (\w+)", statement)
        assert match, statement[:80]
        names.append(match.group(1))
    assert tuple(names) == rt_ddl.EXPECTED_RT_TABLES
    joined = "\n".join(statements)
    assert "PRIMARY KEY (event_date_utc, metric_name)" in joined
    assert "payload          JSON             NOT NULL" in joined
    assert "COLLATE = utf8mb4_0900_ai_ci" in joined


def test_split_sql_statements_ignores_comments():
    statements = rt_ddl.split_sql_statements("-- header\nCREATE TABLE a (id INT);\n-- x\n")
    assert statements == ["CREATE TABLE a (id INT)"]


def test_create_database_sql_is_guarded():
    sql = rt_ddl.build_create_database_sql("ecommerce_rt_test")
    assert "CREATE DATABASE IF NOT EXISTS `ecommerce_rt_test`" in sql
    assert "utf8mb4_0900_ai_ci" in sql
    with pytest.raises(ValueError):
        rt_ddl.build_create_database_sql("ecommerce_dw")


def test_apply_rt_ddl_refuses_protected_database_even_in_dry_run(capsys):
    assert apply_rt_ddl.main(["--database", "ecommerce_dw", "--dry-run"]) == 2
    assert "refusing" in capsys.readouterr().err


def test_apply_rt_ddl_dry_run_prints_statements(capsys):
    assert apply_rt_ddl.main(["--database", "ecommerce_rt_test", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "CREATE DATABASE IF NOT EXISTS `ecommerce_rt_test`" in out
    assert "CREATE TABLE IF NOT EXISTS rt_traffic_daily" in out
    assert out.count("CREATE TABLE IF NOT EXISTS") == 5


def test_app_grants_are_dml_only_and_never_touch_dw():
    statements = rt_grants.build_app_grants_sql("ecommerce_rt", "rt_app", "s3cret")
    joined = "\n".join(statements)
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON `ecommerce_rt`.*" in joined
    assert "ecommerce_dw" not in joined
    assert "ALTER USER 'rt_app'@'%' IDENTIFIED BY 's3cret'" in joined
    assert "GRANT ALL" not in joined
    assert "CREATE USER IF NOT EXISTS 'rt_app'@'%'" in joined


def test_recon_grants_read_rt_and_only_ads_tables_in_dw():
    statements = rt_grants.build_recon_grants_sql("ecommerce_rt", "rt_recon_ro", "s3cret")
    joined = "\n".join(statements)
    assert "GRANT SELECT ON `ecommerce_rt`.* TO 'rt_recon_ro'@'%'" in joined
    for table in rt_grants.ADS_TABLES:
        assert f"GRANT SELECT ON `ecommerce_dw`.`{table}` TO 'rt_recon_ro'@'%'" in joined
    assert "GRANT SELECT, INSERT" not in joined
    assert "GRANT INSERT" not in joined


def test_grant_password_redaction_uses_distinct_placeholders():
    app = "\n".join(
        rt_grants.build_app_grants_sql("ecommerce_rt", "rt_app", "s3cret", redact_password=True)
    )
    recon = "\n".join(
        rt_grants.build_recon_grants_sql(
            "ecommerce_rt", "rt_recon_ro", "other", redact_password=True
        )
    )
    assert "s3cret" not in app
    assert "other" not in recon
    assert rt_grants.APP_PASSWORD_PLACEHOLDER in app
    assert rt_grants.RECON_PASSWORD_PLACEHOLDER in recon


def test_resolve_password_requires_env_value():
    with pytest.raises(ValueError, match="RT_APP_PASSWORD"):
        rt_grants.resolve_password("RT_APP_PASSWORD", {})
    assert rt_grants.resolve_password("RT_APP_PASSWORD", {"RT_APP_PASSWORD": "x"}) == "x"


def test_escape_literal_handles_quotes_and_backslashes():
    assert rt_grants._escape_literal("a'b\\c") == "a''b\\\\c"


def test_apply_rt_grants_refuses_protected_database(capsys):
    assert apply_rt_grants.main(["--rt-database", "ecommerce_dw", "--dry-run"]) == 2
    assert "refusing" in capsys.readouterr().err


def test_apply_rt_grants_dry_run_never_prints_passwords(capsys, monkeypatch):
    monkeypatch.setenv("RT_APP_PASSWORD", "super-secret-app")
    monkeypatch.setenv("RT_RECON_RO_PASSWORD", "super-secret-recon")
    assert apply_rt_grants.main(["--rt-database", "ecommerce_rt", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "super-secret-app" not in out
    assert "super-secret-recon" not in out
    assert rt_grants.APP_PASSWORD_PLACEHOLDER in out
    assert rt_grants.RECON_PASSWORD_PLACEHOLDER in out
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON `ecommerce_rt`.*" in out

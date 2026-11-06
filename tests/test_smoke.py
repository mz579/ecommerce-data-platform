"""Phase 0 冒烟测试。

只测试不依赖数据库的最小功能（配置 / 环境变量解析、示例配置文件可读），
保证即便没有任何数据库环境，pytest 也能跑通并验证工程骨架可用。
"""

from pathlib import Path

import yaml

from ecommerce_dw.config import db_config_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_db_config_from_env_reads_mysql_settings(monkeypatch):
    monkeypatch.setenv("MYSQL_HOST", "127.0.0.1")
    monkeypatch.setenv("MYSQL_PORT", "3306")
    monkeypatch.setenv("MYSQL_USER", "ecom_app")
    monkeypatch.setenv("MYSQL_PASSWORD", "test_password")
    monkeypatch.setenv("MYSQL_DATABASE", "ecommerce_dw")

    cfg = db_config_from_env("MYSQL", engine="mysql")

    assert cfg.engine == "mysql"
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 3306
    assert cfg.user == "ecom_app"
    assert cfg.password == "test_password"
    assert cfg.database == "ecommerce_dw"


def test_db_config_uses_sensible_defaults(monkeypatch):
    monkeypatch.delenv("MYSQL_HOST", raising=False)
    monkeypatch.delenv("MYSQL_PORT", raising=False)
    monkeypatch.setenv("MYSQL_PASSWORD", "secret")

    cfg = db_config_from_env("MYSQL", engine="mysql")

    assert cfg.host == "127.0.0.1"
    assert cfg.port == 3306
    # 关键点：密码必须来自环境变量，而不是内置默认值
    assert cfg.password == "secret"


def test_db_config_supports_postgres_db_alias(monkeypatch):
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_USER", "meta_app")
    monkeypatch.setenv("POSTGRES_DB", "meta")

    cfg = db_config_from_env(
        "POSTGRES",
        engine="postgres",
        defaults={"PORT": "5432"},
        key_map={"DATABASE": "DB"},
    )

    assert cfg.engine == "postgres"
    assert cfg.port == 5432
    assert cfg.user == "meta_app"
    assert cfg.database == "meta"


def test_config_example_and_logging_yaml_are_valid():
    with (PROJECT_ROOT / "config" / "config.example.yaml").open(encoding="utf-8") as fh:
        config = yaml.safe_load(fh)
    assert config["app"]["name"] == "ecommerce-data-platform"

    with (PROJECT_ROOT / "config" / "logging.yaml").open(encoding="utf-8") as fh:
        logging_config = yaml.safe_load(fh)
    assert logging_config["version"] == 1

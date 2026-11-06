"""数据库连通性检查脚本（Phase 0）。

从环境变量读取连接信息（代码中不写死任何密码 / 绝对路径），分别对：
- MySQL（业务数仓，库名由 MYSQL_DATABASE 指定）
- PostgreSQL（Airflow/Superset 元数据库，库名由 POSTGRES_DB 指定）
执行一次 SELECT 1，打印清晰的成功 / 失败摘要。

任一项失败则进程返回非零退出码。

用法（在项目根目录，已激活虚拟环境）：
    python scripts/check_db_health.py
"""

import sys
from pathlib import Path

import psycopg
import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import DbConfig, db_config_from_env

# 加载项目根目录 .env（已存在的环境变量优先，不覆盖）
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# 各引擎连接参数的默认值；真实口令一律来自环境变量（见 .env.example）
_MYSQL_DEFAULTS: dict[str, str] = {
    "PORT": "3306",
    "USER": "ecom_app",
    "DATABASE": "ecommerce_dw",
}
_POSTGRES_DEFAULTS: dict[str, str] = {
    "PORT": "5432",
    "USER": "postgres",
    "DATABASE": "postgres",
}


def check_mysql(cfg: DbConfig) -> bool:
    """对 MySQL 执行 SELECT 1，连接成功且返回 1 才算通过。"""
    try:
        conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            charset="utf8mb4",
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
        finally:
            conn.close()
        return row is not None and int(row[0]) == 1
    except Exception as exc:
        print(f"  [MySQL]      失败原因: {type(exc).__name__}: {exc}")
        return False


def check_postgres(cfg: DbConfig) -> bool:
    """对 PostgreSQL 执行 SELECT 1，连接成功且返回 1 才算通过。"""
    try:
        conn = psycopg.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            dbname=cfg.database,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
        finally:
            conn.close()
        return row is not None and int(row[0]) == 1
    except Exception as exc:
        print(f"  [PostgreSQL] 失败原因: {type(exc).__name__}: {exc}")
        return False


def main() -> int:
    mysql_cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    postgres_cfg = db_config_from_env(
        "POSTGRES",
        engine="postgres",
        defaults=_POSTGRES_DEFAULTS,
        key_map={"DATABASE": "DB"},
    )

    print("=" * 60)
    print("ecommerce-data-platform 数据库连通性检查")
    print("=" * 60)
    print(
        f"[MySQL]      目标: {mysql_cfg.user}@{mysql_cfg.host}:{mysql_cfg.port}/"
        f"{mysql_cfg.database}"
    )
    print(
        f"[PostgreSQL] 目标: {postgres_cfg.user}@{postgres_cfg.host}:{postgres_cfg.port}/"
        f"{postgres_cfg.database}"
    )
    print("-" * 60)

    mysql_ok = check_mysql(mysql_cfg)
    print(f"[MySQL]      SELECT 1 -> {'通过' if mysql_ok else '失败'}")

    postgres_ok = check_postgres(postgres_cfg)
    print(f"[PostgreSQL] SELECT 1 -> {'通过' if postgres_ok else '失败'}")

    print("-" * 60)
    if mysql_ok and postgres_ok:
        print("结果: MySQL 与 PostgreSQL 均连接正常")
        return 0
    print("结果: 存在连接失败项，请确认 docker compose ps 正常、.env 配置无误。")
    return 1


if __name__ == "__main__":
    sys.exit(main())

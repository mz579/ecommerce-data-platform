"""``ecommerce_rt`` DDL 解析与目标库守卫（Phase 9.2）。

DDL 文件按设计 §7.2 维护；应用脚本在连接数据库前必须先通过
``ensure_allowed_database``：只允许 ``ecommerce_rt`` 或以 ``_test`` 结尾的库，
显式拒绝生产库 ``ecommerce_dw``。本模块无数据库 IO，便于单元测试。
"""

from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RT_DATABASE = "ecommerce_rt"
PROTECTED_DATABASE = "ecommerce_dw"
DEFAULT_DDL_PATH = PROJECT_ROOT / "sql" / "ddl" / "streaming" / "02_create_rt_tables.sql"

EXPECTED_RT_TABLES = (
    "rt_traffic_daily",
    "rt_funnel_daily",
    "rt_item_behavior_daily",
    "rt_reconciliation_daily",
    "rt_late_events",
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def is_allowed_database(name: str) -> bool:
    """目标库必须是 ``ecommerce_rt`` 或以 ``_test`` 结尾；``ecommerce_dw`` 永不允许。"""
    if not name or not _IDENTIFIER_RE.match(name):
        return False
    if name == PROTECTED_DATABASE:
        return False
    return name == DEFAULT_RT_DATABASE or name.endswith("_test")


def ensure_allowed_database(name: str) -> str:
    """校验目标库名，返回原值；不合法时抛 ``ValueError``（脚本据此退出 2）。"""
    if not is_allowed_database(name):
        raise ValueError(
            f"refusing to apply realtime DDL to database {name!r}: "
            f"target must be {DEFAULT_RT_DATABASE!r} or end with '_test' "
            f"(and must never be {PROTECTED_DATABASE!r})"
        )
    return name


def strip_sql_comments(text: str) -> str:
    """去掉整行 ``--`` 注释与空行，保留可执行 SQL。"""
    return "\n".join(
        line for line in text.splitlines() if not line.strip().startswith("--") and line.strip()
    )


def split_sql_statements(text: str) -> list[str]:
    """按 ``;`` 拆分 DDL 文件（本项目 DDL 不在字符串字面量中藏分号）。"""
    cleaned = strip_sql_comments(text)
    return [statement.strip() for statement in cleaned.split(";") if statement.strip()]


def load_ddl_statements(path: str | Path | None = None) -> list[str]:
    """读取并拆分 RT DDL 文件。"""
    ddl_path = Path(path) if path is not None else DEFAULT_DDL_PATH
    return split_sql_statements(ddl_path.read_text(encoding="utf-8"))


def build_create_database_sql(name: str) -> str:
    """生成目标库的 ``CREATE DATABASE IF NOT EXISTS``（名称先经过守卫校验）。"""
    ensure_allowed_database(name)
    return (
        f"CREATE DATABASE IF NOT EXISTS `{name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
    )

"""``ecommerce_rt`` 最小权限账号 SQL 构造（Phase 9.2）。

账号模型（设计 §7.1）：
- ``rt_app``：仅 ``ecommerce_rt.*`` 的 SELECT/INSERT/UPDATE/DELETE（DML），
  **不含** DDL，也**没有任何** ``ecommerce_dw`` 权限；
- ``rt_recon_ro``：``ecommerce_rt.*`` 只读 + ``ecommerce_dw.ads_*`` 只读。

密码只从环境变量读取，本模块只负责拼接 SQL；dry-run 时统一脱敏。
本模块无数据库 IO，便于单元测试。
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence

RT_DATABASE = "ecommerce_rt"
RT_APP_USER = "rt_app"
RT_RECON_USER = "rt_recon_ro"
USER_HOST = "%"

APP_PASSWORD_ENV = "RT_APP_PASSWORD"
RECON_PASSWORD_ENV = "RT_RECON_RO_PASSWORD"
APP_PASSWORD_PLACEHOLDER = "<RT_APP_PASSWORD>"
RECON_PASSWORD_PLACEHOLDER = "<RT_RECON_RO_PASSWORD>"

#: ecommerce_dw 中允许对账账号读取的 ADS 表（§7.1 / §8）。
ADS_TABLES = (
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_item_ranking_daily",
    "ads_retention_cohort",
    "ads_funnel_interval",
    "ads_item_behavior_daily",
    "ads_publish_log",
)


def resolve_password(
    env_name: str,
    environ: Mapping[str, str] | None = None,
    *,
    required: bool = True,
) -> str:
    """读取账号密码；非 dry-run 时必须非空。"""
    env = os.environ if environ is None else environ
    value = env.get(env_name, "")
    if required and not value:
        raise ValueError(f"environment variable {env_name} is required")
    return value


def _escape_literal(value: str) -> str:
    """转义 MySQL 字符串字面量中的反斜杠与单引号。"""
    return value.replace("\\", "\\\\").replace("'", "''")


def _user_statements(user: str, password: str, *, redact_password: bool) -> list[str]:
    escaped_user = _escape_literal(user)
    password_sql = "<PASSWORD>" if redact_password else _escape_literal(password)
    return [
        f"CREATE USER IF NOT EXISTS '{escaped_user}'@'{USER_HOST}'",
        f"ALTER USER '{escaped_user}'@'{USER_HOST}' IDENTIFIED BY '{password_sql}'",
        f"REVOKE ALL PRIVILEGES, GRANT OPTION FROM '{escaped_user}'@'{USER_HOST}'",
    ]


def _with_placeholder(statements: list[str], placeholder: str) -> list[str]:
    return [statement.replace("<PASSWORD>", placeholder) for statement in statements]


def build_app_grants_sql(
    database: str = RT_DATABASE,
    user: str = RT_APP_USER,
    password: str = "",
    *,
    redact_password: bool = False,
) -> list[str]:
    """``rt_app``：仅目标库的 DML，无 DDL、无 ``ecommerce_dw``。"""
    statements = _user_statements(user, password, redact_password=redact_password)
    statements.append(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON `{database}`.* "
        f"TO '{_escape_literal(user)}'@'{USER_HOST}'"
    )
    return (
        _with_placeholder(statements, APP_PASSWORD_PLACEHOLDER) if redact_password else statements
    )


def build_recon_grants_sql(
    database: str = RT_DATABASE,
    user: str = RT_RECON_USER,
    password: str = "",
    *,
    dw_database: str = "ecommerce_dw",
    ads_tables: Sequence[str] = ADS_TABLES,
    redact_password: bool = False,
) -> list[str]:
    """``rt_recon_ro``：RT 库只读 + 离线 ADS 表只读。"""
    statements = _user_statements(user, password, redact_password=redact_password)
    escaped_user = _escape_literal(user)
    statements.append(f"GRANT SELECT ON `{database}`.* TO '{escaped_user}'@'{USER_HOST}'")
    statements.extend(
        f"GRANT SELECT ON `{dw_database}`.`{table}` TO '{escaped_user}'@'{USER_HOST}'"
        for table in ads_tables
    )
    return (
        _with_placeholder(statements, RECON_PASSWORD_PLACEHOLDER) if redact_password else statements
    )


def build_all_grants_sql(
    app_user: str,
    app_password: str,
    recon_user: str,
    recon_password: str,
    *,
    rt_database: str = RT_DATABASE,
    dw_database: str = "ecommerce_dw",
    ads_tables: Sequence[str] = ADS_TABLES,
    redact_password: bool = False,
) -> list[str]:
    """按账号顺序返回全部 SQL（app 在前、recon 在后）。"""
    app = build_app_grants_sql(rt_database, app_user, app_password, redact_password=redact_password)
    recon = build_recon_grants_sql(
        rt_database,
        recon_user,
        recon_password,
        dw_database=dw_database,
        ads_tables=ads_tables,
        redact_password=redact_password,
    )
    return app + recon

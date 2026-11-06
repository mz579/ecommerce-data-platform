"""最小配置解析模块（Phase 0）。

约定：所有环境相关的连接信息一律通过环境变量注入（配合项目根目录 .env）、
或写入 config/*.yaml，代码中不写死任何绝对路径与凭据。

本模块不做任何数据库 / 网络 IO，方便在无数据库环境下做单元测试。
"""

import os
from dataclasses import dataclass

_DEFAULT_VALUES: dict[str, str] = {
    "HOST": "127.0.0.1",
    "PORT": "3306",
    "USER": "",
    "PASSWORD": "",
    "DATABASE": "",
}


@dataclass(frozen=True)
class DbConfig:
    """一份数据库连接配置，全部来自环境变量。"""

    engine: str
    host: str
    port: int
    user: str
    password: str
    database: str


def db_config_from_env(
    prefix: str,
    *,
    engine: str,
    defaults: dict[str, str] | None = None,
    key_map: dict[str, str] | None = None,
) -> DbConfig:
    """按前缀从环境变量组装 DbConfig。

    - ``prefix``: 环境变量前缀，如 "MYSQL" 会读取 MYSQL_HOST / MYSQL_PORT / ...
    - ``defaults``: 覆盖默认值（用于引擎间差异，如 PostgreSQL 默认端口 5432）
    - ``key_map``: 字段名到环境变量后缀的别名，如 {"DATABASE": "DB"}
      会在 POSTGRES 前缀下读取 POSTGRES_DB。

    密码不内置默认值：未提供时为空字符串，由调用方判断是否合理。
    """
    merged = {**_DEFAULT_VALUES, **(defaults or {})}

    def _lookup(key: str) -> str:
        suffix = (key_map or {}).get(key, key)
        return os.environ.get(f"{prefix}_{suffix}", merged[key])

    return DbConfig(
        engine=engine,
        host=_lookup("HOST"),
        port=int(_lookup("PORT")),
        user=_lookup("USER"),
        password=_lookup("PASSWORD"),
        database=_lookup("DATABASE"),
    )

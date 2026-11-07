"""Superset 运行配置（Phase 7）。

挂载到 /app/pythonpath/superset_config.py，由官方镜像自动加载
（镜像 ENV PYTHONPATH=/app/pythonpath）。所有敏感值都从环境变量读取，
这些变量由 docker-compose 注入（.env 已被 git 忽略，不入库）。

关键决策：
- 元数据库 = PostgreSQL 的独立库 superset_meta（SUPERSET_META_DB_URI），
  与 Airflow 的 meta 库物理隔库、隔用户；
- 业务数仓只通过只读账号 superset_ro 连接（ADIS 直查，见 docs/superset_design.md）；
- 本机经 HTTP 供看板、截图与验收使用，关闭 Talisman 强制 HTTPS。
"""

from __future__ import annotations

import os

# 官方镜像不带 MySQLdb(mysqlclient)。业务连接用 pymysql，Superset 的 MySQL
# 引擎 spec 在结果集元数据里 Lazy import MySQLdb 做 type_code 映射（pymysql
# 的 cursor.description 返回 int type_code）。install_as_MySQLdb() 让
# ``import MySQLdb`` 解析到 pymysql，彻底规避该缺失（两者 API 兼容）。
import pymysql  # noqa: E402

pymysql.install_as_MySQLdb()

# --- 元数据库（superset_meta 独立库） ---
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_META_DB_URI",
    f"sqlite:////{os.environ.get('SUPERSET_HOME', '/app/superset_home')}/superset.db",
)

# --- Web 会话 / Fernet 加密密钥（禁止空值部署） ---
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-production")

# 本地 HTTP 验收环境：不做 HTTPS 强制跳转（生产请交给反向代理）
TALISMAN_ENABLED = False

# SQLAlchemy 引擎：连接预检查 + 主动回收，避免 MySQL 8h wait_timeout 断链
SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 280,
}

# 默认行上限（本阶段最大图表行数在该值内）
ROW_LIMIT = 100_000

#!/usr/bin/env bash
# ============================================================================
# Phase 7 — Superset 一次性初始化（可重复执行 / 幂等）
#
# 在空元数据库上重建看板的完整命令（结合 .env 配置）：
#   docker compose up -d            # 自动执行本脚本（superset-init 容器）
#
# 步骤：
#   1) 在 PostgreSQL 上幂等创建独立元库 superset_meta 与专属角色（psycopg2，
#      不依赖 psql 客户端；POSTGRES_USER 是官方镜像创建的超管，可建库）；
#   2) superset db upgrade（alembic 迁移到 superset_meta）；
#   3) 幂等创建管理员；superset init（角色/权限种子）；
#   4) 若 dashboards/exports 挂载点有看板包（.zip），导入看板 JSON；
#   5) 修正数据库连接口令：导入包里的 SQLAlchemy URI 口令是掩码（导出时隐藏），
#      必须用 env 里的 superset_ro 口令重建（ORMD 写入，自动 Fernet 加密）。
# 脚本全程 set -e：任一步失败即容器退出非 0，可观察 logs 排查后重跑。
# ============================================================================
set -euo pipefail

echo "[superset-init] ===================================================================="
echo "[superset-init] Step 1/5: create isolated metadata DB '${SUPERSET_META_DB}' on postgres"

python3 - <<'PY'
import os
import time

import psycopg2

pg = {
    "host": "postgres",
    "port": 5432,
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "dbname": os.environ.get("POSTGRES_DB", "postgres"),
}
meta_db = os.environ["SUPERSET_META_DB"]
meta_user = os.environ["SUPERSET_META_USER"]
meta_pw = os.environ["SUPERSET_META_PASSWORD"]

con = None
for attempt in range(60):
    try:
        con = psycopg2.connect(connect_timeout=5, **pg)
        break
    except psycopg2.OperationalError as exc:
        if attempt == 59:
            raise RuntimeError(f"postgres not reachable: {exc}") from exc
        time.sleep(2)

con.autocommit = True
cur = con.cursor()
cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (meta_user,))
if cur.fetchone() is None:
    # 明文字符串拼接限定于本环境变量（本地开发弱口令、受控 compose 注入）
    cur.execute(f"CREATE ROLE {meta_user} LOGIN PASSWORD '{meta_pw}'")
    print(f"created role {meta_user}")
else:
    print(f"role {meta_user} already exists (skipped)")
cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (meta_db,))
if cur.fetchone() is None:
    cur.execute(f'CREATE DATABASE "{meta_db}" OWNER "{meta_user}"')
    print(f"created database {meta_db}")
else:
    print(f"database {meta_db} already exists (skipped)")
cur.close()
con.close()
PY

echo "[superset-init] Step 2/5: db upgrade (alembic migrations on ${SUPERSET_META_DB})"
superset db upgrade

echo "[superset-init] Step 3/5: idempotent admin + superset init"
if ! superset fab list-users | grep -qFw "${SUPERSET_ADMIN_USERNAME}"; then
    superset fab create-admin \
        --username "${SUPERSET_ADMIN_USERNAME}" \
        --firstname "Superset" \
        --lastname "Admin" \
        --email "${SUPERSET_ADMIN_EMAIL}" \
        --password "${SUPERSET_ADMIN_PASSWORD}"
    echo "created admin user '${SUPERSET_ADMIN_USERNAME}'"
else
    echo "admin user '${SUPERSET_ADMIN_USERNAME}' already exists (skipped)"
fi
superset init

echo "[superset-init] Step 4/5: import dashboard bundles from /opt/superset/exports"
shopt -s nullglob
FOUND_BUNDLES=0
for bundle in /opt/superset/exports/*.zip; do
    FOUND_BUNDLES=$((FOUND_BUNDLES + 1))
    echo "importing ${bundle}"
    # 4.1.x 的 import-dashboards 强制要求 -u/--username（导入对象归属用户）；
    # 缺省会直接报 Missing option 退出，导致看板永远导入不进来。
    superset import-dashboards -p "${bundle}" -u "${SUPERSET_ADMIN_USERNAME}" \
        || echo "[warning] import failed for ${bundle}"
done
if [ "$FOUND_BUNDLES" -eq 0 ]; then
    echo "no bundle found in /opt/superset/exports (first run: use scripts/superset_setup.py)"
fi

echo "[superset-init] Step 5/5: fix read-only DB connection password (masked by export)"
python3 /app/scripts/fix_database_password.py

echo "[superset-init] done"
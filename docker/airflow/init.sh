#!/usr/bin/env bash
# ============================================================================
# Phase 6 — Airflow 初始化（只跑一次）：建元数据库 schema + 幂等建管理员用户
#
# 由 docker-compose 的 airflow-init 服务以 `command: bash /opt/airflow/init.sh`
# 方式调用。即使重复执行也安全：
#   - airflow db migrate  是 alembic 幂等迁移；
#   - 用户创建前先查重，已存在则跳过。
# 凭据全部来自容器环境变量（compose 自宿主机 .env 注入），脚本内不出现口令。
# ============================================================================

set -euo pipefail

echo "=== airflow-init: db migrate ==="
airflow db migrate

echo "=== airflow-init: ensure admin user ==="
if airflow users list 2>/dev/null | grep -q "${AIRFLOW_WWW_USER_USERNAME}"; then
  echo "admin user '${AIRFLOW_WWW_USER_USERNAME}' already exists - skip"
else
  airflow users create \
    --username "${AIRFLOW_WWW_USER_USERNAME}" \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email "${AIRFLOW_WWW_USER_EMAIL}" \
    --password "${AIRFLOW_WWW_USER_PASSWORD}"
  echo "admin user '${AIRFLOW_WWW_USER_USERNAME}' created"
fi

echo "=== airflow-init: done (exit 0) ==="
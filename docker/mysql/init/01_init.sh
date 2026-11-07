#!/usr/bin/env bash
# ============================================================================
# ecommerce-data-platform / MySQL（业务数仓）初始化脚本（Phase 8 硬化）
#
# 由 MySQL 官方镜像在**空数据卷首次启动**时自动执行
# （docker compose 挂载到 /docker-entrypoint-initdb.d，仅在卷为空时运行一次）。
#
# Phase 8 起从硬编码库名的 01_init.sql 改为读取 MYSQL_DATABASE，目的：
#   1) 全新环境可用独立库名（如 ecommerce_dw_fresh）一键起栈，不误建生产库名；
#   2) 字符集/排序规则显式钉死为 utf8mb4 / utf8mb4_0900_ai_ci；
#   3) 幂等：CREATE DATABASE IF NOT EXISTS + ALTER DATABASE 可重复执行。
# 凭据只从容器环境变量读取，不落脚本。
# ============================================================================
set -euo pipefail

db="${MYSQL_DATABASE:?MYSQL_DATABASE must be set}"
root_pw="${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD must be set}"

echo "[mysql-init] ensuring database \`${db}\` utf8mb4/utf8mb4_0900_ai_ci"
mysql --protocol=socket -uroot -p"${root_pw}" <<SQL
CREATE DATABASE IF NOT EXISTS \`${db}\`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;
ALTER DATABASE \`${db}\`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;
SET NAMES utf8mb4;
SQL
echo "[mysql-init] done (exit 0)"

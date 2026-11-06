-- ==================================================================
-- ecommerce-data-platform / MySQL（业务数仓）初始化脚本
-- Phase 0
--
-- 重要：本库是【业务数仓】，与 Airflow/Superset 的 PostgreSQL
-- 元数据库在物理上严格分离：不同引擎、不同实例、不同凭据。
--
-- 由 MySQL 官方镜像首次初始化数据卷时自动执行
-- （docker compose 中挂载到 /docker-entrypoint-initdb.d，仅在卷为空时执行一次）。
-- 前提：.env 中的 MYSQL_DATABASE 与本脚本内库名保持一致。
-- ==================================================================

CREATE DATABASE IF NOT EXISTS `ecommerce_dw`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;

USE `ecommerce_dw`;

-- 统一 utf8mb4，保证中文、表情符等 4 字节字符可安全存储
ALTER DATABASE `ecommerce_dw`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;

SET NAMES utf8mb4;

-- Phase 0 只确保最小可用环境就绪；ODS / DWD / DWS / ADS 建表
-- 在后续 Phase 按需创建（sql/ddl 目录）。
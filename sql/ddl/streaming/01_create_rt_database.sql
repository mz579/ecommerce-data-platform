-- ==================================================================
-- Phase 9 实时结果库（独立于生产四层 ecommerce_dw）
--
-- 9.1 只建空库，证明实时链路有独立落点；结果表 DDL 与 rt_app 账号/
-- 授权在 9.2 落地（见 docs/streaming_design.md §7）。
--
-- 硬约束：实时作业只允许写 ecommerce_rt，不得写 ecommerce_dw 的
-- ODS/DWD/DWS/ADS/控制表。该库与生产库同实例、不同 database，
-- 便于本地单机复用 MySQL，但生命周期与权限必须隔离。
-- ==================================================================
CREATE DATABASE IF NOT EXISTS `ecommerce_rt`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

# ============================================================================
# Phase 7 — Superset 镜像（官方 4.1.2 + 补 DB 驱动）
#
# 基础镜像锁定官方补丁 tag；**严禁 :latest**。中国大陆拉取走镜像源后打回
# 标准名再构建（见 docs/superset_design.md「构建与启动」）：
#
#   docker pull docker.1ms.run/apache/superset:4.1.2
#   docker tag  docker.1ms.run/apache/superset:4.1.2 apache/superset:4.1.2
#   docker compose build superset-init        # 或 superset
#
# 官方镜像只自带 redis/cryptography 等通用依赖，**没有任何 DB 驱动**（已实测：
# pymysql / psycopg2 均缺失）。本镜像补两件事：
#   1) pymysql        —— 只读连接业务 MySQL（ADS 直查）；
#   2) psycopg2-binary—— 元数据库 superset_meta（PostgreSQL 独立库）。
# 国内 pip 镜像与 Phase 1~5 同源，避免默认 PyPI 超时。
# 运行态配置（元库 URI、密钥、管理员）全部来自环境变量，见 superset_config.py。
# ============================================================================

FROM apache/superset:4.1.2

ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

USER root

RUN /usr/local/bin/pip install --no-cache-dir \
        "pymysql>=1.1,<1.2" \
        "psycopg2-binary>=2.9,<2.10"

# 自定义配置（PYTHONPATH=/app/pythonpath，superset 自动加载）
COPY docker/superset/superset_config.py /app/pythonpath/superset_config.py
# 一次性初始化：建 superset_meta 库/角色 + db upgrade + 幂等建管理员 +
# 导入看板 JSON + 修正只读连接口令
COPY docker/superset/init.sh /app/scripts/init.sh
COPY docker/superset/fix_database_password.py /app/scripts/fix_database_password.py
RUN chmod +x /app/scripts/init.sh && chown -R superset:superset /app/scripts

USER superset
# 保持官方镜像默认 CMD（/usr/bin/run-server.sh → gunicorn :8088）
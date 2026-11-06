# ============================================================================
# Phase 6 — Airflow 镜像（LocalExecutor + 本项目，editable 安装）
#
# 基础镜像锁定官方补丁 tag；**严禁 :latest**。中国大陆拉取走镜像源后打回
# 标准名再构建（见 docs/airflow_design.md「镜像获取」）：
#
#   docker pull docker.1ms.run/apache/airflow:2.10.5-python3.11
#   docker tag  docker.1ms.run/apache/airflow:2.10.5-python3.11 \
#                apache/airflow:2.10.5-python3.11
#   docker compose build airflow-init
#
# 运行时源码/数据/凭据由 docker-compose 绑定挂载（容器内仓库根 = /opt/ecommerce_dw，
# 与本仓库各 CLI 的 PROJECT_ROOT 解析结果一致）。构建层只安装依赖与 editable
# 入口，不携带业务数据（已由 .dockerignore 挡在构建上下文之外）。
#
# 注意：官方镜像把 /usr/local/bin/pip 包成 wrapper，以 root 运行会直接报错——
# 一切 pip 安装必须落到 airflow 用户（本镜像 site-packages 属主已chown给 airflow）。
# 本项目不新建 Airflow Connections（CLI 只读环境变量），故无需 provider 包；
# 元库 URI 用 postgresql+psycopg2://（airflow 自身需要的 psycopg2 由官方镜像自带，
# 本项目依赖里的 psycopg/psycopg2 仅为 CLI 直连 MySQL 之外的运行时兜底）。
# ============================================================================

FROM apache/airflow:2.10.5-python3.11

# 国内 pip 镜像（与 Phase 1~5 的依赖安装同源），避免默认 PyPI 超时
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

USER root
WORKDIR /opt/ecommerce_dw
COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql
# docs/airflow_design.md 的 _ensure_control_table 等运行时读 sql/ddl，一并进镜像做兜底
# 注意：base 镜像里 airflow 用户 gid=0（root 组），无 airflow 组——chown 目标必须用组 root
RUN chown -R airflow:root /opt/ecommerce_dw \
    && chown -R airflow:root /usr/local/lib/python3.11/site-packages

# editable 安装本项目（pytest 之外的运行时依赖 pyyaml/pymysql/psycopg/python-dotenv
# 一并落位）：DAG 以 `python -m ecommerce_dw.*` 调用真实 CLI。
USER airflow
RUN pip install --no-cache-dir -e /opt/ecommerce_dw

USER airflow
# 保持官方镜像默认 entrypoint（/entrypoint）与 CMD（version）
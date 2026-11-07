# ecommerce-data-platform

电商用户行为数据仓库与实时分析平台。

**阶段进度：Phase 1（数据探索与契约）、Phase 2（ODS 贴源入库）、Phase 3（DWD 明细标准化）、Phase 4（DWS 汇总建模 + ADS 指标应用）、Phase 5（增量处理 + 质量门禁）、Phase 6（Airflow 编排）、Phase 7（Superset 看板）均已完成**（见「当前状态」）。尚未实现实时链路。

目标形态（后续 Phase）：Retailrocket 用户行为数据 → MySQL 四层数仓（ODS / DWD / DWS / ADS）→ Airflow 编排 → Superset 看板；之后扩展 Kafka + Flink 实时链路。

## 当前状态

- **Phase 1 ✅ 已完成**：全量数据探索（4 个 CSV，约 1GB）结论见
  `notebooks/01_eda.ipynb`（已实际执行）；字段/指标口径/数据契约分别沉淀在
  `docs/data_dictionary.md`、`docs/metric_definitions.md`、`docs/data_contract.md`。
- **Phase 2 ✅ 已完成**：ODS 贴源入库——`ods_events_raw` 2,756,101 行、
  `ods_item_properties_raw` 20,275,902 行、`ods_category_tree_raw` 1,669 行；
  分块幂等装载器（断点续跑）+ `etl_batch_log` 批次审计。设计见 `docs/ods_design.md`，
  代码在 `src/ecommerce_dw/ingestion/`。
- **Phase 3 ✅ 已完成**：DWD 明细标准化——**纯函数转换 + SQL 精确去重 + 异常隔离**
  把 ODS 事件转成标准明细：`dwd_events`（标准明细）+ `dwd_events_duplicate`
  （业务键去重掉的精确重复行）+ `dwd_events_reject`（异常隔离），恒等式
  `ODS = DWD + dup + reject` 每次构建强制校验；支持 FULL / DATES 幂等重建、断点续跑、
  构建审计 `dwd_build_log`。设计见 `docs/dwd_design.md`（事件类型以主方案 §2.2 标准
  枚举 `{view, add_to_cart, transaction}` 为准），代码在 `src/ecommerce_dw/dwd/`。
  **全量构建已完成（build #30 LOADED，finished_at 2026-11-07 19:41）**：
  `dwd_events = 2,755,641`、`dwd_events_duplicate = 460`、`dwd_events_reject = 0`，
  恒等式 `2,756,101 = 2,755,641 + 460 + 0` 由 `cli verify` 校验通过；
  去重后事件分布 view 2,664,218 / add_to_cart 68,966 / transaction 22,457，
  行级实测与按日重建幂等实测见 `docs/dwd_design.md` §9。
- **Phase 4 ✅ 已完成**：DWS/ADS 建模与指标落地——纯 SQL 服务端聚合
  （`INSERT ... SELECT`，Python 内存有界），3 张 DWS 日汇总表
  （`dws_user_daily` / `dws_item_daily` / `dws_category_daily`）+ 4 张 ADS 应用表
  （`ads_traffic_daily` PV/UV/DAU、`ads_funnel_daily` 漏斗长表 9 口径、`ads_item_ranking_daily`
  商品日排行 Top-100、`ads_retention_cohort` 次日留存），支持 FULL / DATES 幂等重建与
  `dws_ads_build_log` 批次审计，构建内不变量 + `verify` 独立 SQL 复算双闸门。
  **全量构建已完成（build #1 LOADED，3,431,482 行 / 7 表，164.5s）**：
  PV 全区间 2,649,436、区间 UV 1,396,937、D1 留存 2.75%，`verify` 1,668 checks
  全窗口 ALL MATCH，设计见 `docs/dws_ads_design.md`，代码在 `src/ecommerce_dw/dws/`。
- **Phase 5 ✅ 已完成**：增量处理 + 质量门禁。**增量**以
  `etl_watermark` 水位线（业务日期 × 已消费 ODS 批次）为唯一驱动，同一入口处理
  「新尾巴 + 7 天窗口内迟到」，历史修正走 `incremental --from/--to`；DWD→DWS→ADS
  按受影响日期幂等重建，成功才推水位，稳定态往返 = no-op。**生产实测**：水位引导
  137 个业务日期；普通增量 255.41s（DWD 11,654 行 + 聚合 32,014 行）；深度回填
  238.83s 并经历一次真实崩溃恢复（RUNNING 被 sweep、重跑自愈）。**质量门禁 60 条
  规则**（完整性 3 + 唯一性 12 + 合法性 9 + 范围 12 + 跨层对账 24）硬失败，任一 FAIL
  拒绝发布 ADS，运行与逐规则结果落 `quality_check_run` / `quality_check_result`，
  发布写 `ads_publish_log`。全量门禁实测：run #1 = PASS 59/59（216.74s）、
  run #2 = PASS（200.22s），加规则后 run #3 = **PASS 60/60，89,305,119 rows，224.65s**，
  `ads_publish_log` 保持 2 行。统一入口 `python -m ecommerce_dw.cli`
  （incremental / quality / publish / verify / status）。设计与实测详见
  `docs/incremental_and_quality.md`。
- **Phase 6 ✅ 已完成**：Airflow 编排——官方镜像 `apache/airflow:2.10.5-python3.11`
  + LocalExecutor + 已有 PostgreSQL 的 `meta` 元库（**业务 MySQL 零污染**），
  DAG `ecommerce_daily_pipeline` 严格十任务链
  `start → check_source_files → register_batch → load_ods → validate_ods → build_dwd → build_dws → build_ads → quality_gate → publish_success`，
  每任务调用项目真实 CLI/库（无哑占位）；`schedule="0 4 * * *"`、`catchup=False`、
  `max_active_runs=1`，业务日期由逻辑日期/水位线驱动（`dags trigger --conf` 显式回填），
  同逻辑日期可幂等复跑——**幂等是根本、retries 只兜底**；XCom 只传小元数据；
  DAG 源码零凭据（compose 从 `.env` 注入环境变量）。**生产实测**：小窗口回填
  （2015-09-16/17）十任务全 success 约 11 分钟（build_dwd 224s、build_dws 123s、
  build_ads 复核 ALL MATCH、门禁 PASS 60/60 / 76.5M 行 235s）、重跑业务行数不变；
  `phase6_retry_demo` 演示失败→自动重试→成功。设计/参数/回填手册/故障排查/
  全部实测证据见 `docs/airflow_design.md`。
- **Phase 7 ✅ 已完成**：Superset 看板——镜像锁 `apache/superset:4.1.2`
  （最小 Dockerfile 补 `pymysql` + `psycopg2-binary`），元数据落在 PostgreSQL
  独立库 `superset_meta`（与 Airflow 的 `meta` 物理隔离）；业务连接用独立只读账号
  `superset_ro`（仅 SELECT 4 张 ADS 应用表 + 5 张受控视图，**不直连 `ecom_app`**，
  控制表 `ads_publish_log` 不开放，新鲜度走 `v_ads_publish_status` 视图）。
  一个 `superset-init` 容器从空元库一键重建（建库 → `db upgrade` → 幂等 admin →
  `import-dashboards` 导入看板 JSON → 修正连接口令）。**三个看板 21 张图**：
  运营总览（11）/ 用户行为（5）/ 商品分析（5），各含口径说明 panel；原生筛选器
  （日期、转化口径、类目）before/after 与发布刷新（`publish --run 5`）均有真浏览器
  截图证据；5 项关键指标与直连 SQL 复核 **ALL MATCH**。设计/口径/全部实测证据见
  `docs/superset_design.md`，声明式生成脚本 `scripts/superset_setup.py`。
- 可复用流式统计模块在 `src/ecommerce_dw/eda/`（无数据库、只读本地 CSV）。
- 工程骨架 + 两个数据库的容器编排文件（`docker-compose.yml`）。
- 容器运行时为 **WSL2 内的 Docker Engine**（Windows 侧无 `docker` 命令，见「已验证环境」）；MySQL / PostgreSQL 容器已实际启动并通过健康检查。
- 业务数仓（MySQL）与 Airflow/Superset 元数据库（PostgreSQL）严格分离（见 `docs/architecture.md`）。

## 环境要求

- Windows 11（本说明面向 PowerShell；Git Bash 亦可）
- git >= 2.x
- uv（本项目用 uv 管理 Python 3.11 虚拟环境，不使用系统 py 启动器）
- 容器运行时（二选一；本机已验证的是第一种）：
  - **WSL2 内的 Docker Engine**（主路径）：Ubuntu24.04 + systemd + overlayfs；
    Windows 侧没有 `docker` 命令，所有 compose 命令在 WSL 里执行
    （见「Docker（数据库）启停」）。
  - Docker Desktop（WSL2 backend）：另一种可选路径，命令相同，可在 Windows 侧直接执行。

## 初始化步骤

在项目根目录打开 PowerShell：

```powershell
# 1. 创建 Python 3.11 虚拟环境
uv venv --python 3.11 .venv

# 2. 激活虚拟环境（每次新开终端都要执行一次）
.\.venv\Scripts\Activate.ps1

# 3. 安装项目依赖（运行时 + dev，版本由 uv.lock 锁定）
uv sync --all-extras
```

> 若 PowerShell 拒绝执行激活脚本：`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`。
>
> 依赖版本由 `uv.lock`（随仓库提交）锁定，换机器可复现。升级依赖用
> `uv lock --upgrade` 后重新 `uv sync`；lock 文件必须随代码一起提交，不要手工改动。

## 配置

```powershell
# 生成本地配置：按需修改 .env 里的用户名 / 口令（本地开发用弱口令即可）
Copy-Item .env.example .env
```

- `.env` 已被 git 忽略，**禁止提交到仓库**；其中口令仅用于本地开发。
- `config/config.yaml`（复制自 `config/config.example.yaml`）同样被 git 忽略；配置文件不含任何凭据。
- 集成测试专用 schema：默认为 `MYSQL_DATABASE + "_test"`（即 `ecommerce_dw_test`），
  可用 `MYSQL_TEST_DATABASE` 覆盖。**schema 名必须以 `_test` 结尾**，否则集成测试
  fixture 直接报错（绝不落生产库）。

## 代码检查与测试

```powershell
# 代码规范检查
ruff check .

# 格式检查（只检查，不改动文件）
ruff format --check .

# 冒烟测试（不依赖数据库）
pytest
```

> MySQL 集成测试（`tests/integration/`）在**独立 `*_test` schema** 中运行：session
> 级 fixture 用 `.env` 里的 root 凭据（`MYSQL_ROOT_PASSWORD`）自动 建库 → 授权给
> `MYSQL_USER` → 载入 ODS/DWD DDL → 测试 → 删库，生产库不会被触碰。数据库不可达
> 或无法建库时明确 skip（打印原因）；`MYSQL_TEST_DATABASE` 指向非 `_test` 结尾的
> schema 时直接 raise。因此跑 `pytest` 前请确认 `.env` 中已配置 `MYSQL_ROOT_PASSWORD`。

## 产物快速入口

### Phase 1 探索与契约

- **EDA notebook**：`notebooks/01_eda.ipynb`（已执行含图表，可 Restart & Run All 复现；
  重新生成 cell 用 `python scripts/build_eda_notebook.py`）。
- **流式统计模块**：`src/ecommerce_dw/eda/`——`scan_csv` 通用画像、
  `summarize_events` / `summarize_item_properties` / `summarize_category_tree` 专项统计。
- **口径文档**：`docs/data_dictionary.md`、`docs/metric_definitions.md`、`docs/data_contract.md`。

### Phase 2 ODS 入库

- **装载器**：`src/ecommerce_dw/ingestion/`（分块幂等、断点续跑、批次审计）。
- **运行**：`python -m ecommerce_dw.ingestion.cli load <file> <table>`（详见 `docs/ods_design.md`）。
- **DDL**：`scripts/apply_ods_ddl.py` 只应用 ODS 层 `sql/ddl/ods_*.sql`。

### Phase 3 DWD 明细标准化

- **设计文档**：`docs/dwd_design.md`（字段/粒度/转换规则/去重决定/重建语义/测量结果）。
- **构建**：`python -m ecommerce_dw.dwd.cli build [--dates ...] [--force]`
  （全量 / 按 UTC 日重建；另提供 `verify` / `status` / `cleanup` 子命令）。
- **DDL**：`scripts/apply_dwd_ddl.py` 应用 `sql/ddl/dwd_*.sql`（5 张表）。
- **转换核心**：`src/ecommerce_dw/dwd/transform.py`（纯函数，`TRANSFORM_VERSION="1"`）。

### Phase 4 DWS 汇总 / ADS 应用层

- **设计文档**：`docs/dws_ads_design.md`（表结构/粒度/口径/重建语义/独立复核/生产实测）。
- **构建**：`python -m ecommerce_dw.dws.cli build [--dates ...] [--top-n N] [--force]`
  （全量 / 按 UTC 日幂等重建；另提供 `verify` / `status` / `cleanup` 子命令）。
- **DDL**：`scripts/apply_dws_ads_ddl.py` 应用 `sql/ddl/dws_*.sql` + `ads_*.sql`（8 张表）。
- **聚合核心**：`src/ecommerce_dw/dws/`（`agg_sql.py` 纯 SQL 语句、`builder.py` 构建编排、
  `constants.py` 口径常量，`AGG_VERSION="1"`）。

### Phase 5 增量处理 / 质量门禁

- **设计文档**：`docs/incremental_and_quality.md`（水位线模型/规划器/增量语义/60 条门禁
  规则目录/发布门控/生产实测）。
- **统一入口**：`python -m ecommerce_dw.cli`（`incremental` / `quality` / `publish` /
  `verify` / `status`，见该文档 §8 CLI 速查）。
- **增量实现**：`src/ecommerce_dw/incremental/`（`watermark.py` 水位仓库、
  `planner.py` 日期规划、`runner.py` 编排）。
- **门禁实现**：`src/ecommerce_dw/quality/`（`rules.py` 规则目录、`runner.py` 运行器 /
  发布门控、`models.py` 模型与 `date_filter`）。
- **DDL**：`scripts/apply_quality_ddl.py` 应用 `sql/ddl/etl_watermark.sql` +
  `quality_check_run.sql` + `quality_check_result.sql` + `ads_publish_log.sql`。

### Phase 6 Airflow 编排

- **设计文档**：`docs/airflow_design.md`（部署拓扑 / DAG 结构 / 参数表 / 凭据 /
  回填手册 / 故障排查 / 生产实测）。
- **DAG**：`airflow/dags/ecommerce_daily_pipeline.py`（主线）、
  `airflow/dags/phase6_retry_demo.py`（重试演示，`schedule=None` 手动专用）。
- **镜像与初始化**：`docker/airflow/Dockerfile`（锁 `2.10.5-python3.11`）、
  `docker/airflow/init.sh`（`airflow db migrate` + 幂等建 admin）。
- **控制表**：`sql/ddl/airflow_pipeline_run.sql`（每次 DAG 运行的规划与收尾审计，
  `register_batch` 自动建、幂等）。
- **日常命令**（全部经 WSL 执行，见下文「Docker（数据库）启停」前提）：

```bash
# 启动（init 先迁移元库+建用户，scheduler/webserver 随后起来）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d"

# 看 DAG / 运行历史 / 任务状态
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-runs -d ecommerce_daily_pipeline
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  ecommerce_daily_pipeline <run_id>

# 显式回填（FORCE，业务日期，与机器日期无关）
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline \
  --conf '{"from": "2015-09-16", "to": "2015-09-17"}'
```

### Phase 7 Superset 看板

- **设计文档**：`docs/superset_design.md`（部署拓扑 / 只读访问控制 /
  `superset_meta` 元库隔离 / 可复现部署 / 口径 / 筛选器 / 刷新与新鲜度 / 验收证据）。
- **声明式生成**：`python scripts/superset_setup.py`（Windows venv 对生效实例
  upsert 数据库/数据集/图表/看板并导出看板包）。
- **只读账号与受控视图**：`python scripts/apply_superset_grants.py`（幂等，
  建 5 个受控视图 + `superset_ro` 授权并打印 `SHOW GRANTS`）。
- **验收脚本**：`superset_evidence.py`（指标一致性）、`superset_ui_evidence.py`
  （筛选器 before/after）、`superset_screenshots.py`（真实渲染截图）。
- **产物**：`dashboards/exports/`（可重建看板包）、`dashboards/screenshots/`（6 张 PNG）。
- **日常访问**：`http://localhost:8088`（admin）。

## Docker（数据库）启停

> 前提：容器运行时已就绪。本机经实际验证的是 **WSL2 内的 Docker Engine 29.9.3**
> （发行版 `Ubuntu24.04`，systemd 管理）；Windows 侧（PowerShell / Git Bash）**没有**
> `docker` 命令，所有容器命令都在 WSL 里执行。若改用 Docker Desktop（WSL2 backend），
> 下面命令可在 Windows 侧直接执行。

所有 compose 操作统一经 WSL 执行（每次一条命令，目录为 Windows 挂载点）：

```bash
# 启动 MySQL + PostgreSQL（后台）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d"

# 查看状态
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose ps"

# 查看日志（MySQL 首次建库脚本的输出也在这里）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose logs -f mysql"

# 停止（保留数据卷）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose down"

# 停止并删除数据卷（数据会丢，谨慎使用）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose down -v"
```

- 端口、库名、用户名、口令全部在 `.env` 中配置，`docker compose` 自动读取。
- MySQL 首次启动会自动执行 `docker/mysql/init/01_init.sql`（建库 + utf8mb4 字符集）。
- 镜像拉取：Docker Hub 直连不通，daemon 已配置 registry-mirrors
  （daocloud / 1ms.run / dockerproxy.net）。自动拉取曾因 CDN 节点超时失败一次，
  改用 `docker pull docker.1ms.run/library/<image>` 再 `docker tag` 回标准名即成功
  （详见「已验证环境」）。
- **镜像 tag 已核实存在并拉取成功**：`mysql:8.0.43`、`postgres:16.13`
  （详见 `docs/architecture.md` §4）。

## 数据库连接验证

容器启动后，用 Python 脚本分别对两个库执行 `SELECT 1`：

```powershell
python scripts/check_db_health.py
```

> Windows 终端若中文输出乱码，先执行 `chcp 65001`，或运行前设置 `$env:PYTHONUTF8=1`。

任一失败退出码非 0。也可以直接用客户端连接：

| 服务 | Host | Port | 库 | 用户 |
| --- | --- | --- | --- | --- |
| MySQL（业务数仓） | 127.0.0.1 | `MYSQL_PORT` | `MYSQL_DATABASE` | `MYSQL_USER` |
| PostgreSQL（元数据库） | 127.0.0.1 | `POSTGRES_PORT` | `POSTGRES_DB` | `POSTGRES_USER` |

`scripts/check_db_health.py` 会自动从 `.env` 读取以上变量。

## 已验证环境（Docker / 数据库实测记录）

> 以下均为本机**实际执行结果**，供运维与面试引用；环境变化时同步更新本节。

- **运行时**：WSL2 内 Docker Engine **29.9.3**（发行版 Ubuntu24.04，systemd 管理，
  overlayfs 存储驱动）；Windows 侧无 `docker` 命令，compose 一律在 WSL 中执行。
- **拉取方式**：Docker Hub 直连不通；daemon 已配置 registry-mirrors
  （daocloud / 1ms.run / dockerproxy.net）。`docker compose up -d` 自动拉取时曾因
  CDN 节点超时失败一次；改用 `docker pull docker.1ms.run/library/<image>` 再
  `docker tag` 回标准名后成功。
- **镜像**：`mysql:8.0.43` 与 `postgres:16.13` 两个 tag 均已实际拉取成功、确认存在。
- **容器健康**：两容器均为 `healthy`，`RestartCount=0`。
- **连通性**：Windows 侧 `python scripts/check_db_health.py` 退出码 **0**
  （两个库 SELECT 1 均通过，走宿主机发布端口 127.0.0.1）。
- **字符集**：业务库 `ecommerce_dw` 为 **utf8mb4 / utf8mb4_0900_ai_ci**
  （由 `docker/mysql/init/01_init.sql` 自动初始化，无需手工干预）。
- **命名卷**：`ecommerce-data-platform_mysql_data`、
  `ecommerce-data-platform_postgres_data` 已由 compose 创建。
- **Airflow（Phase 6）**：`ecommerce-airflow:2.10.5-python3.11` 镜像构建成功
  （base `apache/airflow:2.10.5-python3.11` 经 `docker.1ms.run` 拉取后 tag 回标准名）；
  `airflow-init` 退出码 0（元库 alembic 迁移 + admin 用户幂等创建）、
  `airflow-scheduler` / `airflow-webserver` 均 `healthy`；`dags list` 可见
  `ecommerce_daily_pipeline`（schedule `0 4 * * *`）；手动小窗口回填十任务全
  success、同区间幂等复跑业务行数不变、`phase6_retry_demo` 失败后自动重试成功
  （证据见 `docs/airflow_design.md` §9）。
- **Superset（Phase 7）**：`ecommerce-superset:4.1.2` 镜像构建成功
  （base `apache/superset:4.1.2` 经 `docker.1ms.run` 拉取后 tag 回标准名，
  最小 Dockerfile 补 `pymysql` + `psycopg2-binary`）；`superset-init` 退出码 0、
  `superset` 容器 `healthy`、`superset version` = `4.1.2`；元库
  `superset_meta` 建在 PostgreSQL（与 `meta` 隔离）；业务库只读账号
  `superset_ro` SHOW GRANTS = 4 张 ADS 表 + 5 张受控视图全 SELECT、无任何写权限；
  三个看板 dashboard_slices = 11 / 5 / 5，导入幂等复跑数量不变，5 项关键指标
  ALL MATCH，筛选器与刷新截图 6 张（证据见 `docs/superset_design.md` §9）。

## 目录结构

```text
ecommerce-data-platform/
├─ config/                # 配置文件模板（不含凭据）
├─ data/{raw,staging,sample}/   # 数据目录（数据文件不入库、不提交）
├─ scripts/               # 运维脚本（check_db_health、apply_*_ddl 等）
├─ src/ecommerce_dw/      # 项目 Python 包（eda / ingestion / dwd / dws / cli）
│  ├─ eda/                # Phase 1：无库流式统计
│  ├─ ingestion/          # Phase 2：ODS 分块幂等装载
│  ├─ dwd/                # Phase 3：DWD 构建（transform / builder / cli）
│  ├─ dws/                # Phase 4：DWS/ADS 聚合（agg_sql / builder / cli / constants）
│  ├─ incremental/        # Phase 5：水位线增量处理（watermark / planner / runner）
│  ├─ quality/            # Phase 5：质量门禁（rules / runner / models）
│  └─ cli.py、__main__.py # Phase 5：统一入口 `python -m ecommerce_dw.cli`
├─ sql/{ddl,dml,checks}/  # 数仓建模 SQL（含 Phase 5 控制表 etl_watermark / quality_check_run / quality_check_result / ads_publish_log）
├─ airflow/dags/          # Phase 6：Airflow DAG（ecommerce_daily_pipeline + retry demo）
├─ streaming/             # Kafka + Flink 实时链路（后续 Phase）
├─ dashboards/            # Phase 7：看板产物
│  ├─ exports/            #   可重建看板包（含 charts/datasets/databases/metadata）
│  └─ screenshots/        #   真实渲染截图（默认/筛选/刷新前后共 6 张）
├─ tests/                 # 冒烟/单元/集成/数据质量测试
├─ docs/                  # 架构文档
├─ docker/mysql/init/     # MySQL 初始化 SQL
├─ docker-compose.yml
├─ pyproject.toml
├─ .env.example
└─ .gitignore
```

## 已知限制与下一步

- 容器运行时已就绪且 Phase 0 容器相关交付物已实测通过（WSL2 内 Docker Engine，
  见「已验证环境」）；若日后迁到 Docker Desktop 或 CI 环境，以「已验证环境」为基线重新核对。
- Phase 3 DWD 全量构建在 WSL 里的 MySQL 上实测通过（行数恒等式成立，见
  `docs/dwd_design.md` §9）；Phase 4 DWS/ADS 全量构建同样实测通过（build #1，
  `verify` ALL MATCH，见 `docs/dws_ads_design.md` §8）；Phase 5 增量与 60 条质量门禁
  实测通过（见 `docs/incremental_and_quality.md` §9，含崩溃恢复实证）；下次启动前先
  `docker compose up -d` 并等待 MySQL 健康。
- `docs/architecture.md` 中包含完整 Roadmap；下一阶段为「Phase 8：测试、性能与交付
  （V1.0）」——在 Phase 1~7 全部就绪的基础上做系统性验收与性能压测后再交付。
- Phase 7 备注：本机 Superset 图表查询在筛选器 APPLY 触发 12 图并发重查时较慢
  （WSL2 环境 CPU/网络波动，单查询本身毫秒级、SQL 均走索引），已在前端证据脚本中
  做「等待收敛 + 超时重试」处理，不影响验收结论。

## 安全说明

- `.env`、`config/config.yaml`、`data/` 下的数据文件均已被 .gitignore 忽略。
- 本地弱口令仅供开发联调，生产环境必须换用密钥管理方案。
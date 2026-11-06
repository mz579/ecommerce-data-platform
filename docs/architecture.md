# 架构文档

## 1. 定位

本项目把 Retailrocket 电商用户行为数据（网页浏览 / 加购 / 交易事件）建设为
分层数仓（ODS / DWD / DWS / ADS），由 Airflow 定时编排加载，Superset 出看板；
后续扩展 Kafka + Flink 实时链路。

Phase 0 交付工程骨架：目录结构、Python 工程配置、两套数据库的容器编排、最小
可运行测试与运维脚本。Phase 1~5 依次完成探索 / ODS / DWD / DWS+ADS / 增量+质量门禁
（见 README「当前状态」），**Phase 6 已接入 Airflow 编排**（见 `docs/airflow_design.md`）。

## 2. 组件职责

| 组件 | 职责 | 形态 |
| --- | --- | --- |
| MySQL 8.0.x | 业务数仓：ODS / DWD / DWS / ADS 四层建模，库名 `ecommerce_dw` | Phase 0 起编排 |
| PostgreSQL 16.x | Airflow / Superset 元数据库：作业实例、DAG 定义、XCom、看板与用户权限，库名 `meta` | Phase 0 起编排 |
| Airflow 2.10.x | 定时编排 ETL 作业（LocalExecutor + 独立 meta 元库） | **Phase 6 已接入** |
| Superset | 可视化看板，只读访问业务数仓 | 后续 Phase |
| Kafka + Flink | 实时链路：点击 / 加购实时指标 | 后续 Phase |

## 3. 端口约定（宿主机映射，端口可在 `.env` 调整）

| 服务 | 容器端口 | 宿主机默认 | 环境变量 | 说明 |
| --- | --- | --- | --- | --- |
| MySQL | 3306 | 3306 | `MYSQL_PORT` | 业务数仓 |
| PostgreSQL | 5432 | 5432 | `POSTGRES_PORT` | 元数据库 |
| Airflow Web | 8080 | 8080 | `AIRFLOW_UI_PORT` | Phase 6 新增：Web UI（调度/日志/触发/回填） |

## 4. 版本决策

- 镜像一律不用 `:latest`；MySQL 固定在 8.0.x、PostgreSQL 固定在 16.x、
  Airflow 固定在 2.10.x 的具体补丁 tag。
- 锁死大版本的理由：数仓建模依赖的 SQL 方言、字符集与时区行为、CDC / 迁移工具
  兼容性与大版本强相关；锁死后可在明确的升级窗口内验证变化。
- 不用 `:latest` 的理由：latest 是不可预期的游标，某次重建容器时可能悄悄跨入
  新大版本，行为漂移难以排查。

**tag 核对情况（已核实）**：`mysql:8.0.43`、`postgres:16.13` 已在本机实际拉取确认
存在；`apache/airflow:2.10.5-python3.11` 同样通过
`docker pull docker.1ms.run/apache/airflow:2.10.5-python3.11` 拉取成功后 tag 回标准名
（Docker Hub 直连不通，见 README「已验证环境」）。自定义镜像
`ecommerce-airflow:2.10.5-python3.11` 由 `docker/airflow/Dockerfile` 构建
（在 base 之上 editable 安装本项目 + MySQL/Postgres provider）。

## 5. 数据流示意

```text
Retailrocket 用户行为数据
      │  导入 / 后续 Kafka 接入
      ▼
MySQL ODS ──清洗──► MySQL DWD ──聚合──► MySQL DWS ──展示──► MySQL ADS
      ▲                                            │          │
      │ Airflow 日任务链（ecommerce_daily_pipeline）
      │  start→check_source_files→register_batch→load_ods→validate_ods
      │  →build_dwd→build_dws→build_ads→quality_gate→publish_success
      │  （Phase 6，LocalExecutor）                 │          │
      └──── 幂等重建 / 水位线推进 ◄────────────── 质量门禁 60 条 ◄─┘
                                                              │ 只读
                                                              ▼
                                                         Superset 看板

Airflow 的非业务状态（DAG 运行记录、XCom、控制表引用）存于 PostgreSQL(meta)
```

## 6. 关键决策：业务数仓用 MySQL、元数据库用 PostgreSQL，且严格分离

**为什么分离**

- 职责不同：业务数仓是面向业务报表的事实 / 维度数据；元数据库存的是调度与看板
  自身的状态（DAG 运行记录、XCom、看板定义、用户、权限）。
- 生命周期与升级节奏不同：数仓 schema 随业务演进持续变更；元数据库跟随
  Airflow / Superset 版本做迁移。混在一个库会互相拖累升级与迁移。
- 安全边界：元数据库中没有业务敏感数据。分离后可以做最小权限授权——
  看板用户不碰元数据库，调度用户不碰业务库。
- 故障隔离：调度 / 看板异常不至于拖垮业务数仓的读写；反之亦然。

**Phase 6 的一个具体体现**：Airflow 元数据全部落在 PostgreSQL 的 `meta` 库
（库 / 用户独立于业务 MySQL 与 `ecommerce_dw`）；业务数据（含 Phase 6 新增的
`airflow_pipeline_run` 控制表）全部留在 MySQL。DAG 源码只读环境变量与文件系统，
不持有任何数据库凭据。

**为什么引擎这样选**

- 数仓载体用 MySQL：对 BI / 数仓建模足够通用、运维成本低、团队熟悉，
  作为 ODS~ADS 的落地引擎。
- 元数据库用 PostgreSQL：Airflow 官方支持与默认偏好的元数据库，
  Superset 同样原生支持；用其官方镜像可避开兼容性坑。

## 7. Roadmap（后续 Phase 计划，编号与主方案一致）

- **Phase 1**：数据探索与数据契约 —— EDA、字段梳理、指标口径初稿。
- **Phase 2**：ODS 入库与数据批次 —— Retailrocket 数据下载 / 预处理 → MySQL ODS。
- **Phase 3**：DWD 明细标准化 —— 清洗与明细事实层建模。
- **Phase 4**：DWS / ADS 建模 —— 汇总层与展示层建模；`sql/{ddl,dml,checks}` 落地。
- **Phase 5**：增量处理与质量门禁。
- **Phase 6 ✅ 已完成**：Airflow 编排与 DAG 开发（LocalExecutor + PostgreSQL
  元数据库；设计、参数/回填手册、故障排查与实测证据见 `docs/airflow_design.md`）。
- **Phase 7**：Superset 看板。
- **Phase 8**：测试、性能与交付（V1.0）。
- **Phase 9**：Kafka + Flink 实时扩展（`streaming/` 目录落地）。
- **Phase 10**：Spark 批处理（可选）。

## 8. Phase 0 交付物与验证状态

- 交付：目录骨架、`pyproject.toml`、`.gitignore`、`.env.example` / `.env`、
  `docker-compose.yml`、`docker/mysql/init/01_init.sql`、`scripts/check_db_health.py`、
  `tests/test_smoke.py`、`config/{config.example,logging}.yaml`、`README.md`、本文档。
- 已验证：ruff + pytest（最小测试不依赖数据库）。
- **已验证（实测）**：容器启动与真实连接 —— WSL2 内 Docker Engine 29.9.3；
  `mysql:8.0.43` + `postgres:16.13` 拉取成功；两容器 healthy、RestartCount=0；
  `scripts/check_db_health.py` 退出码 0；业务库 `ecommerce_dw` 为
  utf8mb4 / utf8mb4_0900_ai_ci；命名卷已创建（详见 `README.md`「已验证环境」）。
- **Phase 6 已验证（实测）**：`ecommerce-airflow:2.10.5-python3.11` 镜像构建成功；
  `airflow-init` / `airflow-scheduler` / `airflow-webserver` 三容器 healthy，
  元库 `meta` 完成 alembic 迁移；`dags list` 可见 `ecommerce_daily_pipeline`；
  手动触发小窗口全任务 success、幂等复跑行数不变、重试演示通过——
  证据与命令见 `docs/airflow_design.md` §9。
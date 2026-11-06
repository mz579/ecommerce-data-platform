# 架构文档（Phase 0：工程骨架）

## 1. 定位

本项目把 Retailrocket 电商用户行为数据（网页浏览 / 加购 / 交易事件）建设为
分层数仓（ODS / DWD / DWS / ADS），由 Airflow 定时编排加载，Superset 出看板；
后续扩展 Kafka + Flink 实时链路。

Phase 0 只交付工程骨架：目录结构、Python 工程配置、两套数据库的容器编排、
最小可运行测试与运维脚本。不实现任何 ETL。

## 2. 组件职责

| 组件 | 职责 | 形态 |
| --- | --- | --- |
| MySQL 8.0.x | 业务数仓：ODS / DWD / DWS / ADS 四层建模，库名 `ecommerce_dw` | Phase 0 起编排，后续 Phase 建四层 |
| PostgreSQL 16.x | Airflow / Superset 元数据库：作业实例、DAG 定义、XCom、看板与用户权限，库名 `meta` | Phase 0 起编排，后续 Phase 接入 |
| Airflow | 定时编排 ETL 作业 | 后续 Phase |
| Superset | 可视化看板，只读访问业务数仓 | 后续 Phase |
| Kafka + Flink | 实时链路：点击 / 加购实时指标 | 后续 Phase |

## 3. 端口约定（宿主机映射，端口可在 `.env` 调整）

| 服务 | 容器端口 | 宿主机默认 | 环境变量 | 说明 |
| --- | --- | --- | --- | --- |
| MySQL | 3306 | 3306 | `MYSQL_PORT` | 业务数仓 |
| PostgreSQL | 5432 | 5432 | `POSTGRES_PORT` | 元数据库 |

## 4. 版本决策

- 镜像一律不用 `:latest`；MySQL 固定在 8.0.x、PostgreSQL 固定在 16.x 的具体补丁 tag。
- 锁死大版本的理由：数仓建模依赖的 SQL 方言、字符集与时区行为、CDC / 迁移工具
  兼容性与大版本强相关；锁死后可在明确的升级窗口内验证变化。
- 不用 `:latest` 的理由：latest 是不可预期的游标，某次重建容器时可能悄悄跨入
  新大版本，行为漂移难以排查。

**tag 核对要求**：当前锁定的 `mysql:8.0.43` 与 `postgres:16.13` 是编写本文件时的
候选补丁版本。首次 pull 前必须通过镜像仓库接口核实存在（例如
<https://hub.docker.com/v2/repositories/library/mysql/tags/8.0.43>），若不存在，
换成同分支（8.0.x / 16.x）最近可用补丁 tag，并同步更新 `docker-compose.yml`
与本小节。

## 5. 数据流示意

```text
Retailrocket 用户行为数据
      │  导入 / 后续 Kafka 接入
      ▼
MySQL ODS ──清洗──► MySQL DWD ──聚合──► MySQL DWS ──展示──► MySQL ADS
                                                              │
                                                              │ 只读
                                                              ▼
                                                         Superset 看板

Airflow 定时编排以上 ODS→DWD→DWS→ADS 的 ETL 链路（后续 Phase）
Airflow / Superset 的非业务状态（DAG 运行记录、看板定义等）存于 PostgreSQL(meta)
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

**为什么引擎这样选**

- 数仓载体用 MySQL：对 BI / 数仓建模足够通用、运维成本低、团队熟悉，
  作为 ODS~ADS 的落地引擎。
- 元数据库用 PostgreSQL：Airflow 官方支持与默认偏好的元数据库，
  Superset 同样原生支持；用其官方镜像可避开兼容性坑。

## 7. Roadmap（后续 Phase 计划）

- **Phase 1**：ODS 层导入 —— Retailrocket 数据下载 / 预处理 → MySQL ODS。
- **Phase 2**：DWD 清洗清洗转换；DWS / ADS 聚合建模；`sql/{ddl,dml,checks}` 落地。
- **Phase 3**：Airflow 编排与 DAG 开发（依赖 PostgreSQL 元数据库）。
- **Phase 4**：Superset 看板 + 数据质量检查。
- **Phase 5**（扩展）：Kafka + Flink 实时链路，`streaming/` 目录落地。

## 8. Phase 0 交付物与验证状态

- 交付：目录骨架、`pyproject.toml`、`.gitignore`、`.env.example` / `.env`、
  `docker-compose.yml`、`docker/mysql/init/01_init.sql`、`scripts/check_db_health.py`、
  `tests/test_smoke.py`、`config/{config.example,logging}.yaml`、`README.md`、本文档。
- 已验证：ruff + pytest（最小测试不依赖数据库）。
- **未验证**：容器启动与真实连接（本机未安装 Docker Desktop / WSL2）。
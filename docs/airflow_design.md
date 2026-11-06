# Phase 6 — Airflow 编排设计

本文档描述 `ecommerce_daily_pipeline` DAG 的部署拓扑、任务结构、调度语义、
凭据管理、回填/重跑手册与故障排查。文中所有性能与规模数字均为实测值。

## 1. 部署拓扑

```text
                     ┌──────────────── ecommerce-net (bridge) ───────────────┐
                     │                                                       │
  宿主机 .env ──► ┌─┤  postgres:16.13          (元数据库 meta 专用)            │
 (凭据唯一入口)    │  │  POSTGRES_USER=meta_app          │ 5432（内部）         │
                  │  │                                │                      │
                  │  │  airflow-init（一次性）          │  Alembic 迁移 + 建 admin │
                  │  │  airflow-scheduler（LocalExecutor，任务线程执行）          │
                  │  │        │  ├─ BashOperator → python -m ecommerce_dw.*    │
                  │  │        │  └─ PythonOperator → ops.* callables           │
                  │  │  airflow-webserver ─ 8080:8080（UI / 日志 / 触发 / 回填）  │
                  │  │        │                                                │
                  │  │  mysql:8.0.43（业务数仓 ecommerce_dw：四层 + 控制表）      │
                  │  └────────│───────────────────────────────────────────────┘
                  └───────────┴── CLI 进程经环境变量 MYSQL_* 连 mysql:3306
```

要点：

- 每个 Airflow 容器绑定挂载 `./src / ./pyproject.toml / ./sql / ./data / ./logs / ./.env`
  到容器内 `/opt/ecommerce_dw`（editable 安装使 `import ecommerce_dw` 指向该路径，
  各 CLI 的 `PROJECT_ROOT` 同样解析到 `/opt/ecommerce_dw`）；`./airflow/dags` 挂到
  `/opt/airflow/dags`。
- **执行器 = LocalExecutor**：任务在 scheduler 进程内线程执行，无需独立 worker。
  这既是单机编排的合理选择，也让环境变量凭据天然下推到任务进程
  （“任务进程”= scheduler 子进程）。
- **元数据库 = 已有 PostgreSQL 的 `meta` 库**：`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
  = `postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}`
  （基础镜像用户站点 src 里的 SQLAlchemy 1.4 缺 psycopg v3 方言，故用 psycopg2 方言）。
  业务 MySQL 不受 Airflow 迁移影响；新增控制表 `airflow_pipeline_run`
  （`sql/ddl/airflow_pipeline_run.sql`）只落在业务库同层，与 Phase 5 控制表并列。
- 镜像：`ecommerce-airflow:2.10.5-python3.11`（FROM `apache/airflow:2.10.5-python3.11`，
  editable 安装本项目，无 provider 包）。构建方式见 §8。

### 1.1 被否决的备选方案

| 方案 | 否决理由 |
| --- | --- |
| CeleryExecutor + 独立 worker（多容器） | 单机数据规模（约 2.7M 事件行）LocalExecutor 全序串行足够；多进程只会引入 Redis/worker 的调度复杂度，且多 Worker 并发会破坏“同 scope 幂等重建”的串行前提 |
| 用 Airflow Connections（UI/Airflow 变量存库） | 项目 CLI 一律从环境变量读 `MYSQL_*`/`POSTGRES_*`；再在 Airflow 里存一份相同口令会导致两处保管、易漂移。单机 LocalExecutor 下 **环境变量** 就是最终凭据载体（理由见 §5） |
| 元数据放进业务 MySQL（新库） | 与 Phase 0 架构决策冲突：元数据必须与业务数仓严格分离（引擎、实例、凭据三隔离，见 docs/architecture.md §6） |
| `apache/airflow:latest` | 锁版本才能让 alembic 迁移/行为可预期；本阶段锁定 2.10.5-python3.11 并实测 |

## 2. DAG：`ecommerce_daily_pipeline`

任务序列严格按主方案 §7：

```text
start ──► check_source_files ──► register_batch ──► load_ods ──► validate_ods
   ──► build_dwd ──► build_dws ──► build_ads ──► quality_gate ──► publish_success
```

每个任务调用的是**项目真实脚本 / 真实库函数**（见下表），没有任何哑占位任务。

### 2.1 任务实现映射

| 任务 | 实现 | 行为 |
| --- | --- | --- |
| `start` | `PythonOperator` → `ops.window.resolve_window` | 纯函数解析窗口（TAIL / FORCE，见 §3）；把 `mode`、`window`、`logical_date` 推入 XCom（小型元数据） |
| `check_source_files` | `BashOperator` → `python -m ecommerce_dw.cli check-sources` | 磁盘闸：4 个 ODS CSV 存在、非空、表头符合契约 |
| `register_batch` | `PythonOperator` → `ops.control.plan_and_register` | 复用 Phase 5 水位线规划器，裁剪出本次受影响日期，登记 `airflow_pipeline_run` RUNNING 批次；返回 batch_id / dwd_dates / agg_dates（XCom） |
| `load_ods` | `BashOperator` → `python -m ecommerce_dw.ingestion.cli load` ×4 | 幂等装载；稳定态 sha 命中 → “skipped: already loaded”，合法 no-op |
| `validate_ods` | `BashOperator` → `python -m ecommerce_dw.cli validate-ods` | DB+磁盘契约：每文件最新批次 LOADED、rows>0、磁盘 sha256 == 入库 sha |
| `build_dwd` | `BashOperator` → `python -m ecommerce_dw.dwd.cli build --dates {{dwd_dates}}` | 受影响日期幂等重建 DWD；无受影响日期 → 打印原因 no-op 并 exit 0 |
| `build_dws` | `BashOperator` → `python -m ecommerce_dw.dws.cli build --dates {{agg_dates}}` | 重建 DWS（含 ADS 输出）；同上 no-op 语义 |
| `build_ads` | `BashOperator` → `python -m ecommerce_dw.dws.cli verify --dates {{agg_dates}}` | **ADS 独立复核**：DWS build 已产出 ADS（Phase 4 既定），本任务用独立 SQL 复算指标并 diff，是对 ADS 的第二道硬闸 |
| `quality_gate` | `PythonOperator` → `quality.runner.run_quality_gate` | 60 条规则硬门禁，只对本次实际重建日期起范围；稳定态打印跳过原因 |
| `publish_success` | `PythonOperator` → `ops.control.publish_after_gate` | 门禁 PASS → 写 `ads_publish_log` 并关批次；未重建 → SKIP 发布、关批次 SUCCESS/SKIPPED |
| 全局 | `on_failure_callback` | 尽力把仍 RUNNING 的控制批次标记 FAILED |

**合法 no-op 原则（DAG 级）**：DAG 是同逻辑日期可重跑的水位线驱动流水线。一旦某
窗口已被水位消费、ODS 无新批次，`register_batch` 规划出空日期集 —— 后续每个
构建任务都会**写日志说明原因并 exit 0**，`quality_gate` 跳过，`publish_success`
输出 SKIP 且不新增 `ads_publish_log`。这既是稳定态的电量回归,也是幂等复跑的
正反馈（见 §6 实测）。

### 2.2 XCom 白名单（协调者硬性要求 2）

只接受小型元数据 / 统计摘要，**禁止数据行**：

| task | XCom value |
| --- | --- |
| `start` | `mode`、`window`（日期 CSV）、`logical_date` |
| `register_batch` | `batch_id:int`、`dwd_dates:str`、`agg_dates:str`、`dwd_count/agg_count:int` |
| `quality_gate` | `status:str`、`run_id:int`、`passed/rules_total/checks_total:int` |
| `publish_success` | `published:bool`、`publish_id:int\|None` |

`dwd_dates` / `agg_dates` 是逗号分隔的 ISO 日期串（几十字节），不在 XCom 传数据行。

## 3. 调度与参数

### 3.1 DAG 参数表

| 参数 | 值 | 理由 |
| --- | --- | --- |
| `schedule` | `"0 4 * * *"` | 每天 04:00 UTC 一次。业务日期按 UTC 日界落仓；04:00 保证当天新批次（同 NAT 内 3 点前的最后一批）稳定入窗，且给“7 天迟到窗口回看”留余量 |
| `catchup` | `False` | 暂停/中断期间不隐性补跑历史区间；历史修正一律走显式回填（conf 或 §6 手册），行为可预测 |
| `max_active_runs` | `1` | 各层构建对同一 scope 是“删除旧行再重插”，多个并发 run 同窗口会互相清表，必须串行 |
| `depends_on_past` | `False` | 同逻辑日期可重跑；不以昨日成败为前置 |
| `default_args.retries` | `2` | 瞬态故障（MySQL 重连/网络抖动）兜底 |
| `default_args.retry_delay` | `2m` | 立即重试大概率复现故障，先退避 2 分钟 |
| `default_args.execution_timeout` | `3h` | 最重的单任务（ODS 全量装载 4 文件、DWD/DWS 重建）留足够上限后封顶，防卡死占用 |
| `start.retries` | `0` | 确定性问题（窗口解析）失败=输入错误，重试无益 |
| `register_batch.retries` | `0` | 同上：规划失败是输入/环境问题 |
| `publish_success.retries` | `0` | 发布是幂等审计事件，失败应人工复核，而不是自动重复写 |
| `build_dwd/build_dws` 超时 | `1h` | 按日期重建（每天 <1M 行 DWD）实测在秒~分钟级（§9 实测 build_dwd 2 日 = 224s） |

### 3.2 窗口语义（业务日期，UTC，与机器日期无关）

`ops.window.resolve_window` 的优先级：

1. `conf={"dates": "2015-09-16,2015-09-17"}` → **FORCE** 显式日期列表；
2. `conf={"from": ..., "to": ...}` → **FORCE** 显式区间（必须成对给出）；
3. 其余 → **TAIL**：窗口 = 逻辑日期（`logical_date`，回退 `data_interval_start`），
   当日重建哪些日期由 `register_batch` 的水位线规划器运行时决定。

`conf` 来自 `airflow dags trigger --conf '{...}'`——回填和同一逻辑日期重跑都
不走机器日期。**FORCE 是深修正**（无视水位，重建区间内 ODS 有数据的日期）；
**TAIL 是水位线裁剪**（稳定态 = 空日期集 = 全程合法 no-op）。

### 3.3 控制表与日志可检索

每次 DAG 运行在 `airflow_pipeline_run` 落一行：`batch_id / dag_run_id / mode /
window / dwd_dates / agg_dates / status / published / publish_id / quality_run_id /
error_summary / started_at / finished_at`。三者可互相检索：

- **任务日志里查 batch_id**：`register_batch` → `publish_success` 都打印
  `batch_id=`；`dags list-runs` 的 `dag_run_id` 与控制表 `dag_run_id` 一致。
- **处理行数 / 耗时**：`load_ods`/`build_dwd`/`build_dws` 的 CLI 输出 printing
  `rows=`,  `duration=Ns` 落任务日志；`quality_gate` 打印
  `(checks_total 行，duration)`。
- **错误摘要**：失败经 `on_failure_callback` 写入 `error_summary`。

检索 SQL 示例（连 MySQL 业务库）：

```sql
-- 最近一次 DAG 运行的规划与收尾
SELECT run_id, dag_run_id, mode, window_start, window_end, dwd_dates,
       agg_dates, status, published, publish_id, quality_run_id, error_summary
FROM airflow_pipeline_run
ORDER BY started_at DESC LIMIT 10;
```

## 4. 幂等是根本手段，重试只是兜底（协调者硬性要求 4）

仓库自 Phase 2 起的装载体、Phase 3/4 的构建器、Phase 5 的增量均为**同类 scope 幂等**
（删除目标日旧行并从源重算复原）。本 DAG 的编排事实是：

- 同逻辑日期重跑 DAG → `register_batch` 对同一 `dag_run_id`（RUNNING）复用同一
  `batch_id`，且规划出**与上次相同的**受 rebuild 日期（若无新 ODS 批次则为空）；
- 构建任务内容恒等：重跑 = 同一批日期以同样 SQL 重建，行数与校验结果不变；
- 发布是审计事件：只有“真的重建了日期”的窗口才写 `ads_publish_log`；稳定态
  重跑 SKIP 发布 → **发布日志不增长**（§6 实测）；
- `retries=2` 只是对瞬态故障（连接中断、上游抖动）的兜底，它**不承担**语义
  ／数据正确性——那部分由幂等重建 + 60 条质量门禁承担。文档口径与 DAG 注释一致。

## 5. 凭据管理（协调者硬性要求 3）

- **不用 Airflow Connections / Variables**。理由：项目 CLI 唯一凭据入口是
  `MYSQL_*` / `POSTGRES_*` 环境变量（`src/ecommerce_dw/config.py`）；在 Airflow 里
  再存一份相同口令等于双份保管、一份漂移。LocalExecutor 下任务子进程直接继承
  scheduler 容器环境变量，这就是最终凭据载体——**单一来源、零双写**。
- Docker Compose 从宿主机 `.env` 注入；`.env` 已被 git 忽略，**DAG 源码不含任何
  口令**（硬性要求 3）。新增的 4 个变量 `AIRFLOW_FERNET_KEY /
  AIRFLOW_WWW_USER_USERNAME / AIRFLOW_WWW_USER_PASSWORD / AIRFLOW_WWW_USER_EMAIL /
  AIRFLOW_WEBSERVER_SECRET_KEY` 在 `.env.example` 有占位符；本地 `.env` 放开发
  弱口令。
- Fernet key 生成：
  `python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`
- 管理员用户由 airflow-init 用 `airflow users create` 幂等创建。

## 6. 重跑 / 回填手册

### 6.1 稳定态复跑（同逻辑日期，无数据变化）

```bash
wsl -d Ubuntu24.04 -- bash -lc "cd <repo> && \
  docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline"
```

预期：`register_batch` 规划 `dwd_dates=(none)`；build_* 打印 no-op 原因并 exit 0；
`quality_gate` 跳过；`publish_success` 打印 SKIP、`ads_publish_log` 行数不变。
控制表新增一行 `status=SUCCESS published=SKIPPED`。

### 6.2 历史回填（FORCE，conf 指定区间）

```bash
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline \
  --conf '{"from": "2015-09-16", "to": "2015-09-17"}'
# 或显式日期列表：
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline \
  --conf '{"dates": "2015-09-16,2015-09-17"}'
```

预期：两日期被真正重建（DWD/DWS/ADS 删除重插），范围门禁 PASS 后写一条
`ads_publish_log`，控制表 `published=PUBLISHED publish_id=N`。**同一区间重复
回填** = 再一次幂等重建并**再多写一条**发布日志（发布日志按“每次真实发布”记账）——
业务表行数不变，发布日志 +1。

### 6.3 同逻辑日期失败重跑

直接再次 `airflow dags trigger`：窗口解析不依赖机器日期，`dag_run_id` 用新的
run id 会登记新批次；若上次失败残留 RUNNING 行，会按新 `dag_run_id` 插入新行
（同一 dag_run_id 的 RUNNING 行则被 UPDATE 复用）。各层根据水位/批次日志自动
跳过已完成部分（幂等续跑）。

## 7. 故障排查

| 现象 | 排查路径 |
| --- | --- |
| DAG 在 `dags list` 缺失 / import 失败 | `airflow dags list-import-errors`；元库 `dag` 表；改 DAG 后等待 scheduler 重新解析（~30s） |
| 任务失败但 `retries` 没接住 | 任务日志的 `[dag-failure]` 是否写了 error_summary；确定性问题（register/publish）本来 retries=0 |
| 构建卡死 / 超时 | `execution_timeout` 到点强制失败；`dwd.cli status` / `dws.cli status` 看 RUNNING 残留，`--force` 复位后重跑 |
| control 表缺 `airflow_pipeline_run` 表 | `register_batch` 会自动 `CREATE IF NOT EXISTS` 应用 `sql/ddl/airflow_pipeline_run.sql`（幂等） |
| 元库连接失败 | `POSTGRES_PASSWORD` 与 compose 注入不一致；`docker compose logs airflow-init` 看 alembic 迁移 |
| 容器健康但 Web 打不开 | `docker compose ps`（健康探针用 python urllib 打 /health）；日志看 `AIRFLOW__WEBSERVER__SECRET_KEY` 是否为空 |
| Windows 侧 Docker Hub 拉取超时 | 先 `docker pull docker.1ms.run/apache/airflow:2.10.5-python3.11`，`docker tag` 回标准名再构建（§8） |

## 8. 构建与启动

```bash
# 1) 镜像：走镜像源拉基础镜像 → 打回标准名 → 构建自定义镜像
wsl -d Ubuntu24.04 -- bash -lc "cd <repo> && \
  docker pull docker.1ms.run/apache/airflow:2.10.5-python3.11 && \
  docker tag  docker.1ms.run/apache/airflow:2.10.5-python3.11 apache/airflow:2.10.5-python3.11 && \
  docker compose build airflow-init"

# 2) 启动（init 先跑迁移+建用户，scheduler/webserver 随后起来）
wsl -d Ubuntu24.04 -- bash -lc "cd <repo> && docker compose up -d"

# 3) 状态与常用命令
wsl -d Ubuntu24.04 -- bash -lc "cd <repo> && docker compose ps"
wsl -d Ubuntu24.04 -- bash -lc "cd <repo> && \
  docker compose exec airflow-scheduler airflow dags list"
wsl -d Ubuntu24.04 -- bash -lc "cd <repo> && \
  docker compose exec airflow-scheduler airflow dags list-runs -d ecommerce_daily_pipeline"
wsl -d Ubuntu24.04 -- bash -lc "cd <repo> && \
  docker compose exec airflow-scheduler airflow tasks states-for-dag-run ecommerce_daily_pipeline <run_id>"
```

## 9. 生产实测证据

> 本节所有数字来自本机真实执行（2026-11-07，Windows 11 + WSL2 Docker Engine 29.9.3），
> 时间戳为 UTC（容器 `TZ=UTC`，业务日期即 UTC 日）。环境变化时以本节为准重新核录。

### 9.1 容器与镜像

```text
$ docker compose ps -a
NAME                          STATUS                    IMAGE
ecommerce-airflow-init        Exited (0)                ecommerce-airflow:2.10.5-python3.11
ecommerce-airflow-scheduler   Up (healthy)              ecommerce-airflow:2.10.5-python3.11
ecommerce-airflow-webserver   Up (healthy)              ecommerce-airflow:2.10.5-python3.11
ecommerce-mysql               Up (healthy)              mysql:8.0.43
ecommerce-postgres            Up (healthy)              postgres:16.13
```

- 镜像 `ecommerce-airflow:2.10.5-python3.11` 由 `docker/airflow/Dockerfile`
  （FROM `apache/airflow:2.10.5-python3.11`，editable 安装本项目）构建。
- `airflow-init` 一次性容器退出码 **0**：`airflow db migrate`（元库 `meta` alembic 迁移）
  + `airflow users create` 幂等建 Admin 用户 `admin`（`docker logs ecommerce-airflow-init`
  见 `User "admin" created with role "Admin"`）。
- 元数据库 = 已有 PostgreSQL 的 `meta` 库（`meta_app@meta`），业务 MySQL 上
  **没有任何** Airflow 迁移产物（airflow 的表全部落在 postgres）。

### 9.2 DAG 注册与调度

```text
$ airflow dags list
dag_id                   | fileloc                                        | owners  | is_paused
ecommerce_daily_pipeline | /opt/airflow/dags/ecommerce_daily_pipeline.py | dataeng | False
phase6_retry_demo        | /opt/airflow/dags/phase6_retry_demo.py        | dataeng | True
```

meta 库 `dag` 表实测（调度证据）：

```text
          dag_id          | schedule_interval | max_active_runs | max_active_tasks | is_paused
 phase6_retry_demo        | null              |               1 |               16 | t
 ecommerce_daily_pipeline | "0 4 * * *"       |               1 |               16 | f
```

- **`schedule`≠None**：`"0 4 * * *"`（每日 04:00 UTC）。
- **`catchup=False` 实测**：取消暂停的那一刻，scheduler 只补建了**当前时隙**
  的一个 run（`scheduled__2026-11-05T04:00:00+00:00`），**没有**对 start_date
  （2026-11-01）以来的每个空档爆出一串历史回填。
- 这个自动 run 是**稳定态 no-op**：整条链 10 任务全 success、耗时仅 24s
  （22:41:46 → 22:42:10）；`build_dwd` 打印
  `[build_dwd] no business dates to rebuild (register_batch planned none: ...
   exit 0)`；`publish_success` 输出 `nothing rebuilt ... ADS publish skipped`，
  控制表 run_id=1 = `TAIL | SUCCESS | SKIPPED`、`dwd_dates` 为空。
- **`max_active_runs=1` 实测**：连续触发第三个 run 时前一个仍在 running，新 run
  排队至资源释放后才开始（串行执行，无并发互相清表）。

### 9.3 手动触发小窗口（2015-09-16 ~ 2015-09-17）

```bash
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline \
  --conf '{"from": "2015-09-16", "to": "2015-09-17"}'
```

`states-for-dag-run`（run `manual__2026-11-06T22:41:48+00:00`）十任务**全 success**：

| task | attempt | start | end | 耗时 |
| --- | --- | --- | --- | --- |
| start | success | 22:42:11 | 22:42:12 | <1s |
| check_source_files | success | 22:42:12 | 22:42:12 | <1s |
| register_batch | success | 22:42:13 | 22:42:15 | 1.6s |
| load_ods | success | 22:42:16 | 22:42:21 | 5.9s（4 文件全部 skip，合法 no-op） |
| validate_ods | success | 22:42:22 | 22:42:27 | 4.7s |
| build_dwd | success | 22:42:28 | 22:46:04 | 224s |
| build_dws | success | 22:46:05 | 22:48:06 | 123s |
| build_ads | success | 22:48:08 | 22:49:26 | 78s（verify 36 checks ALL MATCH） |
| quality_gate | success | 22:49:27 | 22:53:14 | 235s |
| publish_success | success | 22:53:14 | 22:53:15 | <1s |

控制表 run_id=2：`mode=FORCE`，window `2015-09-16..2015-09-17`，
`dwd_dates=2015-09-16,2015-09-17`，`agg_dates=2015-09-15,2015-09-16,2015-09-17`
（`register_batch` 日志：`batch_id=2 mode=FORCE window=2015-09-16,2015-09-17
dwd_dates=2015-09-16,2015-09-17 agg_dates=2015-09-15,2015-09-16,2015-09-17`），
收尾 `status=SUCCESS published=PUBLISHED publish_id=3 quality_run_id=4
finished_at=22:53:15`。

质量门禁 `quality_check_run` #4：`status=PASS`，**60/60 rules**，
`checks_total=76,488,744`，dates=`2015-09-16,2015-09-17`。

### 9.4 幂等复跑（相同逻辑日期，FORCE 同区间再回填）

`airflow_pipeline_run` 三条记录（含两次 FORCE 与一次 scheduled TAIL）：

| run_id | dag_run_id | mode | dwd_dates | status | published | publish_id |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | scheduled__2026-11-05T04:00 | TAIL |（空） | SUCCESS | SKIPPED | NULL |
| 2 | manual__2026-11-06T22:41:48 | FORCE | 2015-09-16,2015-09-17 | SUCCESS | PUBLISHED | 3 |
| 3 | manual__2026-11-06T22:54:38 | FORCE | 2015-09-16,2015-09-17 | SUCCESS | PUBLISHED | 4 |

业务表行数对比（基线 = 回填前；实测三次一致）：

| 表 | 回填前 | 回填#1 后 | 同区间再回填后 |
| --- | --- | --- | --- |
| ods_events_raw | 2,756,101 | 2,756,101 | 2,756,101 |
| dwd_events | 2,755,641 | 2,755,641 | 2,755,641 |
| dws_user_daily | 1,649,534 | 1,649,534 | 1,649,534 |
| dws_item_daily | 1,672,186 | 1,672,186 | 1,672,186 |
| dws_category_daily | 94,194 | 94,194 | 94,194 |
| ads_funnel_daily | 1,251 | 1,251 | 1,251 |
| ads_item_ranking_daily | 13,900 | 13,900 | 13,900 |
| ads_publish_log | 2 | 3 | 4 |
| etl_watermark | 139 | 139 | 139 |

**为什么业务行数全部不变、而 `ads_publish_log` 却增长？**

- 各层构建都是**同类 scope 幂等**（删除目标日旧行、按同一 SQL 重算重插）：
  两次 FORCE 回填重建的是同一批日期、同样内容，DWD/DWS/ADS 业务表行数
  逐字节相同而 `verify` 仍 ALL MATCH（36 checks × 3 days）。
- **发布日志是审计事件**：只有「真的重建了日期的窗口」才记一笔发布
  （`publish_ads_scope` 写一行）。同区间重复回填 = 再一次真实重建 + 门禁 PASS
  后再记一笔，所以 `2 → 3 → 4`（每笔记录 `scope_type=DATES` 与 `quality_run_id`）。
- **“不增长”同样得到演示**：scheduled TAIL 稳定态 run（无受影响日期）走
  `published=SKIPPED`，`ads_publish_log` 在它前后都是 2（不增长）——与 §6.1 / §6.2
  文档口径一致。
- `etl_watermark` 保持 139：FORCE 深修正不推动水位（与 Phase 5 一致）。

### 9.5 重试演示（`phase6_retry_demo`，不触碰生产数据）

```bash
docker compose exec airflow-scheduler airflow dags trigger phase6_retry_demo \
  --conf '{"fail_first_tries": 1, "lag_seconds": 2}'
```

任务 `flaky_but_eventually_succeeds`（`retries=2`, `retry_delay=5s`,
`retry_exponential_backoff=True`）两次 attempt 日志：

```text
attempt=1.log:
[retry-demo] try #1 starting (fail_first_tries=1)
RuntimeError: [retry-demo] injected transient failure on try #1 (no DB touched; nothing to roll back)
{standard_task_runner.py:124} ERROR - Failed to execute job ... (return code 1698)   # 非 0 退出 → 进入退避重试

attempt=2.log（自动重试，间隔 5s+）:
[retry-demo] try #2 starting (fail_first_tries=1)
[retry-demo] try #2 Succeeded after 1 retries
Marking task as SUCCESS ... end_date=...
Task exited with return code 0
```

`dags list-runs -d phase6_retry_demo`：run 由 queued → success（终点 `success`）。
该 DAG `schedule=None`、只手动触发、无数据库副作用：重试多少次结果都一样
（幂等安全网）。

### 9.6 日志片段（row counts + duration，可直接 grep）

```text
# build_dwd（合法 no-op vs 真实重建）
[build_dwd] no business dates to rebuild (register_batch planned none: stable state...) exit 0   # 稳定态
[build_dwd] loaded    : build #52 (2015-09-16,2015-09-17) status=LOADED ods=21,623 ok=21,620 dup=3 reject=0 duration=224.07s resumed_from=0

# build_dws（7 张表 rows）
[build_dws] loaded    : build #6 (2015-09-15,2015-09-16,2015-09-17) status=LOADED rows=55,167 across 7 tables duration=123.23s
[build_dws]   dws_user_daily 25,781 / dws_item_daily 26,968 / dws_category_daily 2,082 /
              ads_traffic_daily 3 / ads_funnel_daily 27 / ads_item_ranking_daily 300 / ads_retention_cohort 6

# build_ads（ADS 独立复核）
[build_ads] verify    : 36 checks across 3 days -> ALL MATCH     (78s)

# quality_gate（规则 + 行数 + 耗时）
[quality_gate] run #4 PASS 60/60 rules passed (76,488,744 rows checked) dates=2015-09-16,2015-09-17 duration=234.59s

# load_ods / validate_ods（稳定态 skip 契约）
[load_ods]    skip events.csv: already loaded as batch #17 (sha256=3745aa...)
[validate_ods] OK events.csv  batch #17 rows=2,756,101 sha256 matches on disk
```

检索入口：任务日志内 `batch_id=2` / `run #4` / `duration=...s` 与
`airflow_pipeline_run` / `quality_check_run` / `ads_publish_log` 三表互通。
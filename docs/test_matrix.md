# Phase 8–9.4 测试矩阵与指标正确性基线

> 本文覆盖 Phase 8（测试 / 性能 / 交付硬化）与 Phase 9.2/9.3/9.4（Kafka 回放/校验、
> Flink 实时作业、全量批流对账）的测试矩阵与手工样例说明。测试只写 `*_test` schema
> 或一次性独立 schema，**不触碰生产库 `ecommerce_dw` 的数据**。所有结果均为本机真实
> 执行输出，不是推算。
>
> 交付命令（项目根目录，venv 已激活）：
>
> ```bash
> python -m pytest                 # 宿主：280 passed, 1 skipped
> bash scripts/build_flink_job.sh  # Java 自检 59 passed（同时产出作业 jar）
> docker compose exec -T airflow-scheduler python -m pytest tests/dag   # 5 passed
> ruff check . && ruff format --check .                                   # 全绿
> python scripts/demo_phase8.py    # 一键 8 步演示（见 performance.md §8）
> ```

## 1. 测试矩阵

| 层 | 测试文件 | 覆盖点 | 证据命令 | 结果 |
| --- | --- | --- | --- | --- |
| 入口 / 冒烟 | `tests/test_smoke.py` | 包可导入、CLI 入口存在、基础配置可读 | `pytest tests/test_smoke.py` | 4 passed |
| 单元：摄入 | `tests/unit/test_ingestion_chunk.py`、`test_ingestion_keys.py`、`test_ingestion_loader.py` | 分块读取、稳定 `row_key`/`dedup_key`、CSV 类型转换、批日志 | `pytest tests/unit` | 21 passed |
| 单元：DWD | `tests/unit/test_dwd_transform.py`、`test_dwd_builder.py` | 原始事件标准化、非法事件拒绝、重复判定、UTC 日界、失败恢复 | `pytest tests/unit` | 45 passed |
| 单元：DWS/EDA | `tests/unit/test_dws_helpers.py`、`test_eda.py` | 聚合辅助 SQL、窗口/完整性标志、EDA 输出 | `pytest tests/unit` | 20 passed |
| 单元：增量 | `tests/unit/test_incremental_planner.py` | 新尾巴 / 窗口内迟到 / 窗外修正 / 水位前推 | `pytest tests/unit` | 14 passed |
| 单元：质量 | `tests/unit/test_quality_rules.py`、`test_quality_runner.py` | 规则参数化、scope guard、FAIL 阻断、结果落库 | `pytest tests/unit` | 14 passed |
| 单元：运维 | `tests/unit/test_ops_validate.py`、`test_ops_window.py` | 源文件校验、窗口计算、边界日期 | `pytest tests/unit` | 18 passed |
| 单元：部署授权 | `tests/unit/test_superset_grants.py` | `MYSQL_RO_USER` 解析、CREATE/ALTER/GRANT 语句生成、dry-run 密码脱敏、字面量转义 | `pytest tests/unit/test_superset_grants.py` | 5 passed |
| 单元：实时回放/校验（9.2） | `tests/unit/test_streaming_events.py`、`test_streaming_replay.py`、`test_streaming_validate_consumer.py`、`test_rt_ddl_grants.py` | event_v1 消息构造/时间格式/quality_flags/transaction 条件、producer 配置、DLQ 决策与统计、RT DDL 5 表、库名守卫与最小权限 SQL | `pytest tests/unit/test_streaming_events.py tests/unit/test_streaming_replay.py tests/unit/test_streaming_validate_consumer.py tests/unit/test_rt_ddl_grants.py` | **56 passed** |
| 单元：批流对账（9.3） | `tests/unit/test_verify_rt.py` | 独立 SQL 文本/参数、6 位 HALF_UP 比率、分母 0 为 NULL、13 指标期望值、MATCH/DIFF/缺行判定、日期区间与表格输出、RT 三表合并、对账只读账号选择（不回退 `rt_app`） | `pytest tests/unit/test_verify_rt.py` | **20 passed** |
| 单元：全量对账（9.4） | `tests/unit/test_reconcile_all.py` | 13 指标 MATCH/DIFF、缺实时值、部分日 SKIP（数值列 NULL）、ADS 三表合并、ADS-vs-DWD 交叉复核、比率 NULL 规则、`rt_reconciliation_daily` 幂等 upsert（含默认元组游标回归）、拒绝写非 RT 库、`--json-out` 日期序列化回归、`rt_app` 专用账号 | `pytest tests/unit/test_reconcile_all.py` | **18 passed** |
| 单元：批量编排（9.4） | `tests/unit/test_batch_replay.py` | 批次切分/前瞻/pusher 边界/断点续跑、合成水位线事件合法性、回放 summary 解析、checkpoint 汇总、逐 operator 指标名解析、反压/延迟聚合、计数器差分吞吐（含 stale 读数折叠）、janitor 保留策略、状态脚本选作业、生产基线常量 | `pytest tests/unit/test_batch_replay.py` | **19 passed** |
| Java 自检（9.3/9.4） | `streaming/flink-job/src/test/java/com/ecommerce/rt/SelfTest.java` | 解析校验、同 `event_id` 去重、null visitor 排除 UV/DAU、orders 用 distinct transaction_id、严格转化交集、分母 0、窗口边界、`is_final` 边界、比率舍入、商品 distinct、48h TTL、transaction 契约、`ingest_time` 契约、`transaction_id=0` 契约、`is_final` 单调性（`copyWith` + 三条 upsert SQL 文本） | `bash scripts/build_flink_job.sh` | **59 passed / 0 failed** |
| 集成：ODS | `tests/integration/test_ods_mysql.py` | 真实 MySQL 建表、CSV 装载、幂等重载、脏行保留 | `pytest tests/integration/test_ods_mysql.py` | 7 passed |
| 集成：DWD | `tests/integration/test_dwd_mysql.py` | 真实 DWD 构建、去重/拒绝分表、账实一致、重复构建幂等 | `pytest tests/integration/test_dwd_mysql.py` | 3 passed |
| 集成：DWS/ADS | `tests/integration/test_dws_ads_mysql.py` | 既有手工样例全链路、指标口径、`verify_dws_ads` | `pytest tests/integration/test_dws_ads_mysql.py` | 1 passed |
| 集成：RT upsert 单调性 | `tests/integration/test_rt_upsert_mysql.py` | 从 Java 源码解析真实 upsert 语句，在 `ecommerce_rt_test` 临时库验证已 final 行不被降级（三表）、部分日保持 0、SQL 文本单调性 | `pytest tests/integration/test_rt_upsert_mysql.py` | **5 passed** |
| 集成：质量 + 增量 | `tests/integration/test_phase5_gate_incremental_mysql.py` | 60 条规则真实执行、publish 拦截、水位增量 | `pytest tests/integration/test_phase5_gate_incremental_mysql.py` | 2 passed |
| 集成：Airflow 控制面 | `tests/integration/test_phase6_airflow_control_mysql.py` | 任务控制表 / run 记录 / 重试状态 | `pytest tests/integration/test_phase6_airflow_control_mysql.py` | 3 passed |
| 集成：Kafka 实时链路（9.2） | `tests/integration/test_streaming_kafka.py` | 唯一 selftest topic 全合法往返、非法 JSON 进测试 DLQ、生产 topic/DLQ 不受影响 | `pytest tests/integration/test_streaming_kafka.py` | **2 passed**（Kafka 不可达时按设计 skip） |
| **E2E：ODS→DWD→DWS→ADS** | `tests/integration/test_phase8_e2e_mysql.py` | 手工样例端到端、独立 SQL/Python 复算、幂等快照 | `pytest tests/integration/test_phase8_e2e_mysql.py` | 1 passed |
| **DAG 契约** | `tests/dag/test_airflow_dag_contract.py` | 导入无 DB 副作用、10 任务线性链、重试/超时、调度语义、真实 CLI | 容器内 `pytest tests/dag` | 5 passed |
| 部署冒烟 | `scripts/check_db_health.py`、`scripts/demo_phase8.py` 第 1/7 步 | MySQL/PostgreSQL/Airflow/Superset `/health`、生产行数基线 | `python scripts/demo_phase8.py` | 通过（见 §5） |

宿主全量结果：**280 passed, 1 skipped**（2026-11-07 Phase 9.4 最新运行日志
`pytest_full.log`；耗时随机器负载略有波动，以最新日志为准）。构成：Phase 8 既有
**158 passed** + Phase 9.2 **60 passed**（58 单元 + 2 Kafka 集成；单元里含 9.4 补的
2 条 `transaction_id=0` 契约回归）+ Phase 9.3 **20 passed**（`test_verify_rt.py`）
+ Phase 9.4 新增 **37 passed**（`test_reconcile_all.py` 18 +
`test_batch_replay.py` 19）+ 5 条 is_final 单调性集成测试 = **280 passed**。
另有 Phase 9.3/9.4 的 Java 自检 59 项，由 `scripts/build_flink_job.sh` 在构建时执行
（不计入 pytest）。
1 skipped 是 `tests/dag` 在宿主缺少 `apache-airflow` 时按设计整模块跳过；同一文件在真实
Airflow scheduler 容器内 5 passed in 1.30s。

## 2. 手工样例数据（`tests/fixtures/phase8_manual/`）

时间戳是 2015-09-16（D1）/ 2015-09-17（D2）UTC 毫秒。`events_batch1.csv`
14 行原始事件；`events_batch2_late.csv` 1 行迟到事件；`item_properties.csv`
11 行类目快照。

| # | timestamp | visitor | event | item | txn | 设计意图 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1442361601000 | 1 | view | 101 | | 同一用户多浏览（第 1 次） |
| 2 | 1442361601001 | 1 | view | 101 | | 同一用户多浏览（第 2 次） |
| 3 | 1442361601002 | 2 | view | 102 | | 加购→交易链路起点 |
| 4 | 1442361601003 | 2 | addtocart | 102 | | 加购 |
| 5 | 1442361601004 | 2 | transaction | 102 | 9001 | 交易 |
| 6 | 1442361601005 | 3 | addtocart | 103 | | 仅有加购、无浏览 |
| 7 | 1442361601006 | 0 | view | 104 | | 无 user id → NULL + `visitor_zero` |
| 8 | 1442361601007 | 4 | transaction | 105 | 9002 | 无浏览直接交易（严格/全口径差异） |
| 9 | 1442361601000 | 1 | view | 101 | | 与第 1 行完全相同 → duplicate |
| 10 | 1442361601008 | 3 | purchase | 101 | | 非法事件 → reject |
| 11 | 1442448001000 | 1 | view | 101 | | 跨日回访（D2），喂 D1 留存 |
| 12 | 1442448001000 | 2 | view | 102 | | 跨日回访（D2） |
| 13 | 1442448001001 | 5 | view | 106 | | D2 新用户 |
| 14 | 1442448001002 | 9 | transaction | 107 | 9003 | D2 新用户直接交易 |
| 迟 | 1442361602000 | 1 | view | 101 | | 首批构建后才到，触发 D1 窗口内重算 |

类目快照覆盖：item 101 的旧快照 500 / 当日快照 501、102–107 的当日快照、
108 的非 `categoryid` 属性（应忽略）、109 的非数字值（应忽略）、110 的未来
快照（D1/D2 均不应使用）。

## 3. 手算期望值与断言位置

D1 批次 1 的 DWD 结果：**14 ODS = 12 ok + 1 duplicate + 1 reject**；第 9 行
duplicate、第 10 行 reject、其余 12 行进入 `dwd_events`。迟到批次追加 1 行后，
D1 共 9 条有效事件、D2 共 4 条。

| 指标（D1 / D2） | 手算过程 | 期望值 | 断言位置 |
| --- | --- | --- | --- |
| PV | D1：v1×2 + v2 + v0 = 4，加迟到 v1 = 5；D2：v1+v2+v5 = 3 | 5 / 3 | `_EXPECTED_TRAFFIC`，`test_phase8_manual_sample_end_to_end` |
| UV | 有 view 且 `visitor_zero=0` 的去重人数：D1 {1,2}=2；D2 {1,2,5}=3 | 2 / 3 | 同上 |
| DAU | 任意有效事件去重人数：D1 {1,2,3,4}=4；D2 {1,2,5,9}=4 | 4 / 4 | 同上 |
| 漏斗 view/cart/buyer/orders | D1 {1,2} / {2,3} / {2,4} / {9001,9002}；D2 {1,2,5} / ∅ / {9} / {9003} | D1 (2,2,2,2)；D2 (3,0,1,1) | `_EXPECTED_FUNNEL` |
| 加购率 | cart_users / view_users：D1 2/2=1；D2 0/3=0（有浏览，不是 NULL） | 1 / 0 | 同上 |
| 严格转化 | view ∩ buyer：D1 {2}=1；D2 ∅=0 | 1 / 0 | 同上 |
| 全口径转化 | buyer：D1 {2,4}=2；D2 {9}=1 | 2 / 1 | 同上 |
| 严格转化率 | 1/2=0.5；0/3=0 | 0.5 / 0 | 同上 |
| 全口径转化率 | 2/2=1；1/3=0.333333 | 1 / 0.333333 | 同上 |
| 留存 D1 age0 | cohort {1,2,3,4}，当日全活跃 | 4/4=1, full | `_EXPECTED_RETENTION` |
| 留存 D1 age1 | D2 回访 {1,2} | 2/4=0.5, full | 同上 |
| 留存 D2 age0 | cohort {5,9}，当日全活跃 | 2/2=1, full | 同上 |
| 留存 D2 age1 | D3 不在窗口内 | 0/2=0, partial | 同上 |
| 商品行为 D1 | distinct items：view {101,102,104}=3；cart {102,103}=2；buy {102,105}=2 | (3,2,2) | `_EXPECTED_ITEM_BEHAVIOR` |
| 商品行为 D2 | view {101,102,106}=3；cart ∅；buy {107}=1 | (3,0,1) | 同上 |
| 商品排行 D1 | view 降序 → cart 降序 → txn 降序 → item 降序 | 101,102,104,103,105 | `_EXPECTED_RANKING` |
| 商品排行 D2 | 106/102/101 同为 view=1 按 item 降序；107 为 txn=1 | 106,102,101,107 | 同上 |
| 表行数 | user 5+4；item 5+4；category 5+4；traffic 2；funnel 9×2；ranking 5+4；retention 4；behavior 2 | `{user:9,item:9,category:9,traffic:2,funnel:18,ranking:9,retention:4,behavior:2}` | `agg1.table_stats` 断言 |

独立复算不只依赖构建器自己的 `verify_dws_ads`：

- PV/UV/DAU 用独立 `SELECT ... FROM dwd_events ... GROUP BY event_date_utc`
  （`direct_traffic`）；
- 漏斗用 Python 集合运算重新从 `dwd_events` 行计算 `views/carts/buyers/orders/strict`；
- 留存用 Python 重新求 first-seen 与次日活跃集合；
- `verify_dws_ads` 作为第二种形状的校验，要求 `mismatches == ()`；
- 幂等：同 scope 再建一次，7 张业务表内容快照逐行相等，构建日志保持
  `LOADED` 状态。

历史批次（`test_dws_ads_mysql.py`）另有一套更早的手工样例，两套样例的输入
不同、断言不同，避免单一 fixture 自证。

## 4. DAG 测试（真实 Airflow 运行时）

`tests/dag/test_airflow_dag_contract.py` 在宿主无 Airflow 时整模块 skip；在
`airflow-scheduler` 容器内执行，直接读取真实 DAG 文件：

| 测试 | 断言 |
| --- | --- |
| `test_dag_import_has_no_database_side_effects` | monkeypatch `pymysql.connect` / `psycopg2.connect` 后导入 DAG，连接调用次数为 0；DAG id / 任务数正确 |
| `test_task_chain_is_linear_and_complete` | 10 个任务恰好构成 `start → check_source_files → register_batch → load_ods → validate_ods → build_dwd → build_dws → build_ads → quality_gate → publish_success`，无额外根/叶 |
| `test_retry_and_timeout_policy` | 非确定性任务 `retries=2 / retry_delay=2m`，确定性任务（start / register_batch / publish_success）`retries=0`，每个任务都有 `execution_timeout` |
| `test_schedule_semantics` | `0 4 * * *`、`catchup=False`、`max_active_runs=1`、`depends_on_past=False` |
| `test_bash_tasks_call_real_project_clis` | 每个 Bash 任务包含真实 `python -m ecommerce_dw...` 命令，无占位符 |

DAG 导入副作用测试不触发任何真实建连；依赖顺序测试只读 `DagBag` 拓扑；
重试配置测试只读 operator 属性。因此这些测试不会改动生产数据，也不需要
暂停/触发 DAG。

## 5. 已知边界与未纳入自动化的部分

- **Superset 看板视觉呈现**不由 pytest 断言。Phase 7 已有硬断言截图验收；
  Phase 8 只在全新环境里验证 3 个 dashboard / 15 个 dataset 能由
  `scripts/superset_setup.py` 从零重建（见 `performance.md` §9）。
- **调度器实跑 DAG** 不在测试里触发，避免与生产调度/发布日志互相影响；
  链路正确性由 DAG 契约测试 + 独立 E2E 测试覆盖。
- **23M 行全量重载**不作为常规测试；全量性能数字来自生产库按需实测，
  常规 CI 只跑小样例。
- **Flink 作业**不纳入 pytest：窗口/去重/解析逻辑由 `SelfTest.java` 在构建时执行
  （48 项），端到端行为由真实回放 + `verify_rt_vs_dwd.py` 验收（§11.3），
  全量 137 天对账由 `reconcile_all.py` 写入 `rt_reconciliation_daily`（§11.4），
  不在 CI 中启动 Kafka/Flink。

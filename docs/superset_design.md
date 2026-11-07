# Phase 7 — Superset 看板设计文档

> Phase 7 设计文档（V1.0 仍适用）。本文记录部署拓扑、访问控制、
> 元数据库隔离、可复现部署、三个看板的指标口径与筛选器、数据刷新与新鲜度、
> 验收证据与实测命令，以及反推过程中的被否决策。

---

## 1. 部署拓扑

```text
Windows 11 ── localhost:8088 ──► Superset 4.1.2（容器，ecommerce-superset）
                                     │  ├─ 元数据库：PostgreSQL 独立库 superset_meta
                                     │  └─ 业务连接：MySQL only-read 账号 superset_ro
                                     │        └─ 仅授权 6 张 ADS 应用表 + 5 张受控视图
MySQL（业务数仓 ecommerce_dw） ◄── superset_ro（SELECT 白名单）
PostgreSQL（元数据库 meta + superset_meta） ◄── superset 元数据（隔离于 Airflow meta）
```

- Web 端口：宿主机 `8088`（`.env` 的 `SUPERSET_UI_PORT`）。
- 一次初始化容器 `superset-init`（`docker/airflow/...` 对等模式），退出码 0 即完成；
  `superset` 服务 `depends_on` 它 `service_completed_successfully`。
- 所有容器共享 `ecommerce-net` 网络；业务 MySQL 与 Postgres 的端口只从宿主机映射
  给运维/CLI 使用（`127.0.0.1`），SUPERSET_MYSQL_HOST 在容器内用服务名 `mysql`。

## 2. 访问控制（只读）

- Superset **不直连 `ecom_app`**。业务连接使用独立只读账号 `superset_ro`，
  权限为 **6 张 ADS 应用表 + 5 张受控视图**（`SHOW GRANTS` 实测见 §9.2）。
  新增的两张（`ads_funnel_interval`、`ads_item_behavior_daily`）是 Phase 7 R16
  物化的昂贵窗口/按日聚合表：随 DWS/ADS 构建链幂等重建，把 Superset 冷启动口径
  SQL 从 ~9~10s 压到 ~0.00s（见 §9.10）。
- 受控视图（`sql/ddl/ads_views/`）：`v_ads_funnel_interval`、
  `v_ads_category_daily`、`v_ads_item_behavior_daily`、`v_ads_item_orders_daily`、
  `v_ads_publish_status`；`ads_publish_log` 控制表不对 `superset_ro` 开放
  （其新鲜度通过只读视图 `v_ads_publish_status` 暴露计数）。
- 口令不落任何代码/文档：只存在于被 `.gitignore` 忽略的 `.env`（与 `.env.example`
  占位 `CHANGE_ME`）。导入用看板包里的数据库 URL 是对导出口令掩码后的形式；
  `docker/superset/init.sh` Step 5 的 `fix_database_password.py` 从 env 重建口令，
  通过模型 `set_sqlalchemy_uri()` 把真实口令写进 Fernet 加密的 `password` 列、
  URI 列只留掩码（见 §9.3 连接测试：URI 无明文口令）。

## 3. 元数据库隔离（superset_meta）

- Superset 元数据落在 PostgreSQL 独立库 `superset_meta`，与 Airflow 元库 `meta`
  **物理隔离**，分别用不同角色（`SUPERSET_META_USER` vs `POSTGRES_USER`）。
- `superset-init` Step 1 幂等创建 DB 与角色（psycopg2，不依赖 psql 客户端）；
  `superset db upgrade` 执行 alembic 迁移到该库。
- 空元数据库一键重建命令：

```bash
docker compose up -d --force-recreate superset-init && docker compose up -d superset
```

## 4. 部署可复现

- 镜像锁 `apache/superset:4.1.2`，最小化 Dockerfile 只补 `pymysql` +
  `psycopg2-binary`（MySQL 驱动缺失是官方镜像唯一缺口）。
- 初始化（`docker/superset/init.sh`，`set -euo pipefail`，可重复执行）：
  1. 幂等建 `superset_meta` 库/角色；
  2. `superset db upgrade`；
  3. 幂等建 admin + `superset init`；
  4. `superset import-dashboards -p <bundle.zip> -u <admin>` 导入看板 JSON
     （4.1.x 该命令强制要求 `-u/--username`，缺省直接报错）；
  5. 用 env 口信重建业务数据库连接（`fix_database_password.py`）。
- 看板包（`dashboards/exports/`，`docker compose` 只读挂载到容器
  `/opt/superset/exports`）：由 `scripts/superset_setup.py` 声明性生成并导出，
  包含 `charts(21) / dashboards(3) / datasets(12) / databases(1) / metadata.yaml`，
  可原样重新导入（见 §9.5 import 幂等实测）。
- 正常开发路径：`python scripts/superset_setup.py`（Windows venv，走 REST API 对
  生效实例 upsert 数据库/数据集/图表/看板并导出包）；全新环境走 init.sh 导入。

## 5. 三个看板（图表 21 张，dashboard_slices = 11 / 5 / 5）

| 看板 | slug | 图表数 | 内容 |
| --- | --- | --- | --- |
| 运营总览 | `ops-overview` | 11 | 口径说明 panel；6 个 KPI（PV/区间UV/订单/加购/严格转化/全局转化）+ D1 留存；PV/UV/DAU 日趋势；转化率双口径 vs 严格口径（线）；加购转化率日趋势；D1 留存曲线 |
| 用户行为 | `user-behavior` | 5 | 口径说明 panel；漏斗（view→atc→buyer）；DAU/新增用户日趋势；新增用户 D1 留存曲线；新增用户留存热力图；用户会话/日漏斗明细表（9 指标） |
| 商品分析 | `product-analysis` | 5 | 口径说明 panel；累计发布次数 KPI（新鲜度）；Top-20 商品排行表（含 add_to_cart_cnt/transaction_cnt 等）；Top-20 区间 PV 条形（dist_bar）；类目 Top-20 区间 PV/订单（dist_bar）；每日 distinct 商品数（浏览/加购/成交） |

每个看板含一个 **Markdown 口径说明 panel**（说明 139 天周期、UTC 时区、
`orders = distinct(transaction_id)`、首尾天 `is_full_day=0` 等）。

## 6. 指标口径（§6 可直接引用）

- **orders = distinct transaction_id**（去重口径全仓一致）。
- **转化率双口径分开展示**：
  - 严格转化 = 成交用户 / 浏览用户；全局转化 = distinct 成交 / distinct 浏览 —
    **全局口径允许 >100%**，因此两口径不合并为一个数字。
  - 运营看板用「双口径趋势 + 两个 KPI」分别呈现，并用维度筛选器
    `metric_name` 控制线系列（默认双选，取消「严格」后只剩全局系列）。
- **D1 留存**：cohort（首访日）用户在次日回访的比例；部分首/末日
  （2015-05-03、2015-09-18）`is_full_day=0` 不做日级留存混淆。
- **时间范围**：全部图表 `event_date_utc` 驱动，筛选器支持按日期维度筛选。

## 7. 筛选器（原生 Native Filters）

每个看板在 `FILTER_ROW` 放置原生筛选器（`filter_select`），组件 id 与
`json_metadata.native_filter_configuration[].id` **完全一致**（`NATIVE_FILTER-PH7-N`），
否则 filter bar 不渲染。

| 看板 | 筛选器 | 目标 |
| --- | --- | --- |
| 运营总览 | 日期范围（`event_date_utc / cohort_date`） | traffic / funnel_conv / funnel_atc / retention |
| 运营总览 | 转化口径（严格 / 全局）默认双选 | funnel_conv.metric_name |
| 用户行为 | 日期范围（`event_date_utc / cohort_date`） | dau_new / retention |
| 商品分析 | 日期范围（`event_date_utc`） | item_behavior |
| 商品分析 | 类目 `category_id` | category_top |

- **默认值**写在 `defaultDataMask`（`extraFormData.filters` + `filterState.value`），
  只写 `controlValues.defaultValue` 前端不生效。
- 变更采用「staged → APPLY FILTERS」提交语义：筛选器改动后需点击
  `APPLY FILTERS` 按钮才触发全部图表重查（4.1.2 的 filter bar 行为）。

## 8. 刷新与新鲜度

- 看板数据来自 ADS 汇总表 + 受控视图，由 Airflow 日任务链更新；
  刷新链路：`python -m ecommerce_dw.cli publish --dates 2015-09-16,2015-09-17 --run 5`
  （真实跑一次发布，在 `ads_publish_log` 新增一行）→ 商品分析看板的「累计发布次数」
  KPI 立即 +1。
- **Phase 7 R16 门禁作用域规则**：发布的作用域必须与其背书的 PASS 质量门禁一致 ——
  FULL 发布需要 FULL 作用域 PASS 门禁；DATES 发布需要 DATES 作用域且日期集合完全一致
  的 PASS 门禁；不匹配则拒绝并 exit 2。这里 `--run 5` 是一次 DATES 门禁，其
  `dates_csv` 正好是 `2015-09-16,2015-09-17`，所以刷新演示请求同日期集合的
  DATES 发布：`--dates 2015-09-16,2015-09-17`（FULL 发布 / 其它日期集会在此规则下被拒）。
  实现：`validate_publish_scope()`（纯函数）+ `publish_ads_scope` 的执行守卫，单测
  `tests/unit/test_quality_runner.py`、集成 `tests/integration/test_phase5_gate_incremental_mysql.py`
  `test_publish_gate_scope_enforcement`（正/反例都覆盖）。
- 每次发布都写 `ads_publish_log` 审计行，看板通过 `v_ads_publish_status`
  只读视图读取计数（不开放控制表本身）。
- **历史发布记录（publish_id 5–9）说明**：这 5 条是门禁作用域规则上线前的
  「证据发布」产物 —— 当时以 run #5（DATES `2015-09-16,2015-09-17` PASS）为背书
  连续 5 次执行 FULL 作用域发布来刷看板刷新前后截图证据。在现行 R16 规则下这类
  FULL 发布背靠 DATES 门禁会被拒绝（不会再产生）；这些审计记录**保留不删**，
  作为回放运行时「看板数据未变化、KPI 递增」的确证。逐条明细见 §9.8。

## 9. 验收证据（实测命令 + 输出摘录）

下文的完整输出以 Repo 内脚本复跑为准；此处给出命令与结论。

### 9.1 服务与版本

```bash
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/... && docker compose ps"
# ecommerce-superset  Up (healthy)
wsl -d Ubuntu24.04 -- bash -lc "docker compose -f .../docker-compose.yml exec -T superset superset version"
# 4.1.2
```

### 9.2 SHOW GRANTS（superset_ro）

`python scripts/apply_superset_grants.py` 幂等重跑后的实际输出（只读白名单，11 个对象）：

```text
== SHOW GRANTS FOR 'superset_ro'@'%' ==
  GRANT USAGE ON *.* TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_funnel_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_funnel_interval` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_item_behavior_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_item_ranking_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_retention_cohort` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_traffic_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_category_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_funnel_interval` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_item_behavior_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_item_orders_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_publish_status` TO `superset_ro`@`%`
```

无任何 INSERT/UPDATE/DELETE；`ads_publish_log` 控制表不开放。SUPERSET_SETUP 的数据集
全是这些视图/表上的 `SELECT`（声明式 SQL 数据集），受上表白名单约束。

### 9.3 数据库连接（无明文口令）

- `scripts/superset_setup.py` 建连接后执行 `test_connection OK`；图表查询全走该连接，
  §9.6 的 ALL MATCH 即真实跑通证明。
- 4.1.2 的 `dbs` 表：`sqlalchemy_uri` 是**不加密**的 `String(1024)` 列，真正加密的是
  独立 `password` 列（Fernet）。因此重建口令必须走模型自带 `set_sqlalchemy_uri()`
  （`fix_database_password.py` Step 5）：真实口令进加密的 `password` 列，URI 列只留
  掩码 `XXXXXXXXXX`。直接赋 `row.sqlalchemy_uri` 会把明文口令落库（验收中实测抓到并
  修正，见 §10）。
- 落库实测（`psql -U superset_meta_app -d superset_meta`）：

```text
 id | database_name            | uri                                                        | pw_len
----+--------------------------+-------------------------------------+--------
  1 | ecommerce_dw (Superset RO) | mysql+pymysql://superset_ro:XXXXXXXXXX@mysql:3306/...  |   24
```

- 导出看板包里的 `databases/*.yaml` 也只带掩码 URI 且 `allow_dml: false`；真实口令
  只存在于被 `.gitignore` 忽略的 `.env`（compose 注入容器）。

### 9.4 三个看板与图表数

`docker compose exec -T postgres psql -U superset_meta_app -d superset_meta -c
"SELECT d.dashboard_title, count(ds.id) FROM dashboards d JOIN dashboard_slices ds ON ds.dashboard_id = d.id GROUP BY d.id ORDER BY d.id"`

结论：11 / 5 / 5。

### 9.5 包导入幂等

对现有实例重跑 `superset import-dashboards -p <bundle> -u admin` 后：
dashboard_slices 数量保持不变（不翻倍）；`fix_database_password.py` 重跑后连接测试 OK。

### 9.6 关键指标一致性（Superset chart/data API vs 直连 SQL，浮点容差）

`python scripts/superset_evidence.py` 实际输出（5 项关键指标 ALL MATCH）：

```text
chart                                   superset             sql    ok
------------------------------------------------------------------------
PV 总量（139 日）                              2664218         2664218  True
区间 UV（view 口径）                            1396937         1396937  True
DAU 2015-07-26                               17516           17516  True
区间严格转化率                                    0.008          0.0080  True
订单数 2015-07-26（distinct txn）               101.0      101.000000  True
```

同一脚本另输出数据层筛选验证：PV 全区间 2,664,218 vs 仅 07-25..07-27 的 86,250
（与直连 SQL `86,250` 一致）；转化率线「仅严格口径」0.007467 与「两口径全选」0.007694
数值不同 —— 严格/全局两口径分开呈现、不会混用。

### 9.7 R16 硬断言 UI 验收（before/after + 发布刷新）

`python scripts/superset_ui_evidence.py`（Playwright 真浏览器，每次为新会话）。
**Phase 7 R16 起带硬断言**：每一轮采集都
1. 把每个图表面板滚动进视口（触发 IntersectionObserver 懒加载），边界轮询直到
   「无 `Waiting on` / `No results` / `Timeout` / `trouble loading` / HTTP 错误 +
   至少一个面板有数值」，轮询有界（默认 ≤150s）；
2. 断言 `时间范围 / 时区 / 口径` markdown 面板存在；
3. 任一捕获出现卡死/空面板即在结束时 exit 1（此前已保存全部 PNG 供复查）。

采集序列（8 张 PNG，全含维度/日期 before-after 与发布前后）：

```text
ops_overview_default.png            ops 默认态
ops_overview_filter_metric_before.png   转化口径默认双系列（before）
ops_overview_filter_metric.png          取消 conv_strict_rate 后仅全局系列（after）
ops_overview_filter_date_before.png     日期范围默认（before）
ops_overview_filter_date.png            日期筛选 2015-07-26 后（after）
user_behavior_default.png           用户行为默认态
product_analysis_before_publish.png 发布前
product_analysis_after_publish.png  发布后（累计发布 KPI +1）
```

- 维度筛选（转化口径）：默认双系列，取消 `conv_strict_rate` 后 only
  `conv_global_rate` 单系列（UI 截图 + legend 文本双重验证）。
- 日期筛选（event_date_utc / cohort_date）：ops PV 全区间 `2,664,218`
  → 2015-07-26 单日窗口。
- **加购率日趋势（宕机空态修复）**：`ds_funnel_atc` 数据集 SQL 在 4.1.2 下不能暴露
  `metric_name` 列 —— 运营看板的转化口径原生筛选器按列名匹配，会把
  `metric_name IN ('conv_strict_rate','conv_global_rate')` 泄漏到该查询，
  与数据集内 `metric_name='add_to_cart_rate'` 冲突 → 空集/`No results`。
  修复分两层：(1) 数据集 SQL 不再 SELECT `metric_name`（WHERE 条件写死为
  `metric_name = 'add_to_cart_rate'`，`scripts/superset_setup.py`
  `FUNNEL_ATC_SQL`）；(2) **列元数据清理** —— 4.1.2 对 SQL 数据集的 PUT 不会
  收回已消失的列，残留的 `metric_name` 列仍会让原生筛选器按名匹配并泄漏语法错误
  （外侧 `WHERE metric_name IN (...)` 引用子查询之外的列 → 1054 报错）。
  `superset_setup.py` 的 `_prune_stale_columns()` 在每次数据集 upsert 后用
  MySQL 引擎直觉（`SELECT * FROM (<sql>) LIMIT 0`）对照，删除 SQL 不再投影的列，
  从根上杜绝该泄漏再犯。实测直连返回 139 行（2015-05-03..2015-09-18，无 NULL），
  看板截图呈现趋势数据。
- 轮询细节：不产生任何无界 sleep；超时即记为断言失败。

### 9.8 刷新数据变化 + 门禁作用域

**发布演示（R16 门禁作用域下发合法的一例）**：

```bash
python -m ecommerce_dw.cli publish --dates 2015-09-16,2015-09-17 --run 5
```

run #5 是 DATES 作用域 PASS 门禁（`dates_csv='2015-09-16,2015-09-17'`），故发布
请求必须同为该日期集合的 DATES 发布 —— 门禁作用域守卫放行，`ads_publish_log`
追加 1 行，商品分析看板「累计发布次数」+1；前后两张产品页截图可见 KPI 变化（§9.9）。

**门禁作用域守卫（正/反例）**：

| 发布请求 | 背书门禁（run） | 结果（允许/拒绝） |
| --- | --- | --- |
| FULL | FULL PASS | allowed |
| FULL | DATES PASS | refused（exit 2） |
| DATES `同日期集` | DATES PASS 同日期集 | allowed（顺序无关，去重排序后比较） |
| DATES 窄/宽/异日期集 | DATES PASS | refused（exit 2） |
| DATES | FULL PASS | refused（exit 2） |

实现：`validate_publish_scope()`（纯函数）；单测 `tests/unit/test_quality_runner.py`；
集成 `tests/integration/test_phase5_gate_incremental_mysql.py` →
`test_publish_gate_scope_enforcement`（含拒绝后 `ads_publish_log` 不增长的断言）。

**publish_id 5–9 出处审计表**（R16 要求：不得删除审计记录，说明来源）：

| publish_id | scope_type | dates_csv | quality_run_id | **说明** |
| --- | --- | --- | --- | --- |
| 1–2 | FULL | — | run 1、run 2 | 早期全量发布（上线前证据），门禁作用域规则前的合法产物 |
| 3–4 | DATES | `2015-09-16,2015-09-17` | run 4、run 5 | 第一次刷新演示（DATES，与 run 5 恰好一致），仍合法 |
| 5–9 | FULL | — | run 5 | **证据发布**：门禁作用域规则上线之前，以 run #5 为背书连续 5 次执行 FULL 发布，用于回放验证刷新前后截图；现行规则下 FULL↔DATES 会被拒（见上表），故该类记录不会再新增；历史行保留不删 |
| 10–12 | DATES | `2015-09-16,2015-09-17` | run 5 | R16 验收的三轮刷新演示（R16 硬断言证据脚本内跑的真实 CLI 发布，publish_id 10/11/12），规则下合法，构成要求「只增不改」 |

> 截止本验收快照：`ads_publish_log` 共 12 行。1–4、10–12 在现行规则下均合法；
> 5–9 为历史证据发布，保留不删（见 §8）。刷新演示当前指向最新 publish_id 12。

### 9.9 截图（真实渲染）

`dashboards/screenshots/`（8 张，均来自 `superset_ui_evidence.py` 的真实 Chromium
渲染，随 R16 硬断言：任何 `Waiting on` / `No results` 空洞都会让脚本 exit 1）：

| 文件 | 内容 |
| --- | --- |
| `ops_overview_default.png` | 运营总览默认态：KPI 行 + 双口径双系列 + 加购率趋势有数据 |
| `ops_overview_filter_metric_before.png` | 转化口径默认双系列（before） |
| `ops_overview_filter_metric.png` | 转化口径取消「严格」后：仅全局系列 |
| `ops_overview_filter_date_before.png` | 日期范围默认（before） |
| `ops_overview_filter_date.png` | 日期筛选 2015-07-26 后：PV 32,077 |
| `user_behavior_default.png` | 用户行为默认态 |
| `product_analysis_before_publish.png` | 商品分析发布前：累计发布 N |
| `product_analysis_after_publish.png` | 商品分析发布后：累计发布 N+1 |

### 9.10 图源冷启动耗时（物化前后，数据库侧实测）

以 `superset_ro` 账号直接执行每个图表的数据源查询 2 次（冷启动）计时。
R16 前测量抓到的根因（6 张 KPI 卡 × ~9s 叠加 > 前端 60s 超时 → `Waiting on` /
`No results`），R16 物化后同一图源：

| 图源（图表读取的查询） | R16 前 | R16 后 cold#1 / cold#2 | 行数 |
| --- | --- | --- | --- |
| `v_ads_funnel_interval`（6 张区间 KPI 卡） | 9.61s / 9.16s | **0.003s / 0.001s** | 1 |
| `v_ads_item_behavior_daily`（商品行为 KPI 卡） | 10.17s / 10.02s | **0.002s / 0.001s** | 139 |
| `v_ads_item_orders_daily`（TOP 商品表·单日） | 0.00s | 0.002s / 0.001s | 20 |
| `ads_traffic_daily`（PV/UV/DAU 卡 + 趋势线） | 0.00s | 0.001s / 0.001s | 139 |
| `ads_funnel_daily`（转化率/加购率趋势线） | 0.00s | 0.005s / 0.004s | 1,251 |
| 新增物理表 `ads_funnel_interval` | — | 0.001s / 0.001s | 1 |
| 新增物理表 `ads_item_behavior_daily` | — | 0.001s / 0.001s | 139 |

复测脚本：`C:\Zproject\ZClaude\probe\ph7_timings_after.py`（属本机工具，不在仓库；
仓库内证据链见 §9.6 与 `scripts/superset_ui_evidence.py` 的硬断言截图）。

物化细节（口径由 DWS/ADS 构建链保证，`superset_ro` 仍只见视图/表上的 SELECT）：
- `ads_funnel_interval`：窗口级区间漏斗单行（`view_users/cart_users/buyer_users/
  orders/conv_strict_users/conv_global_users` + 三个比率），INSERT..SELECT 服务端
  聚合，FULL 构建时重建（`dws`/`verify` 均有独立复核断言）；
  DDL `sql/ddl/ads_funnel_interval.sql`。
- `ads_item_behavior_daily`：按 UTC 日的去重商品数（view/add_to_cart/transaction），
  随构建按日幂等重建；DDL `sql/ddl/ads_item_behavior_daily.sql`。
- 两张表的每一行都有 `build_id + loaded_at` 血缘；`verify` 从 `dwd_events`
  独立重算（含 139 日 × 3 项 = 417 项逐日断言 + 区间 9 项），与构建结果 ALL MATCH
  后才认为该构建通过（见 `docs/dws_ads_design.md` §6）。

## 10. 被否决的备选方案

| 方案 | 否决理由 |
| --- | --- |
| Superset 直接连 `ecom_app` | 写权限暴露，不满足只读安全边界 |
| 元数据与 Airflow 共用 `meta` 库 | 升级/迁移互相拖累，破坏隔离（架构文档 §6） |
| 看板用 `viz_type: "bar"`（时间序） | 是时间序列图，groupby 分类轴报错；改 `dist_bar` |
| Markdown 写 `meta.text` | 4.1.2 前端读 `meta.code`，写 text 渲染为空占位 |
| `order_by_cols` 直接用列名 | 走 legacy formData 解析，`JSON.parse` 抛错；改空数组 + 数据集 SQL 内排序 |
| 筛滤器组件 id 用 `FILTER-01` | 与 `native_filter_configuration` id 不一致，filter bar 不渲染 |
| API 导入覆盖 | 4.1.x 无 CLI 覆盖语义时用 `import-dashboards` + `-u` 必需参数 |
| 直接给 `Database.sqlalchemy_uri` 赋值重建口令 | 4.1.2 的 URI 列不加密，直接赋值会把明文口令落库（巡检 `dbs` 表实测抓到）；必须走 `set_sqlalchemy_uri()`：口令进 Fernet 加密的 `password` 列、URI 列只存掩码 |

## 11. 相关文件

- `scripts/superset_setup.py` —— 声明式 upsert（DB/数据集/图表/看板/导出）；
  内含 R16 加购率数据集修复（`FUNNEL_ATC_SQL` 不暴露 `metric_name` 列）
- `scripts/superset_evidence.py` —— 指标一致性验收
- `scripts/superset_ui_evidence.py` —— R16 硬断言 UI 验收（before/after + 发布刷新）
- `scripts/superset_screenshots.py` —— 真实渲染截图（复用硬断言 helper）
- `sql/ddl/ads_funnel_interval.sql`、`sql/ddl/ads_item_behavior_daily.sql` —— R16 物化表
- `sql/ddl/ads_views/v_ads_funnel_interval.sql`、`v_ads_item_behavior_daily.sql` —— 薄视图
- `src/ecommerce_dw/dws/agg_sql.py`、`builder.py` —— 物化构建 + verify 独立复核
- `src/ecommerce_dw/quality/runner.py` —— `validate_publish_scope` 门禁作用域守卫
- `tests/unit/test_quality_runner.py` —— 门禁作用域单测（正/反例）
- `tests/integration/test_phase5_gate_incremental_mysql.py` —— 构建/门禁作用域集成
- `docker/superset/{Dockerfile,init.sh,superset_config.py,fix_database_password.py}`
- `dashboards/exports/`、`dashboards/screenshots/`
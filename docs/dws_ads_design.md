# DWS / ADS 汇总与应用层设计（Phase 4）

> 本文件是 `data-engineering/ecommerce-data-platform` 第四阶段（DWS 汇总层 +
> ADS 应用层建模与指标落地）的设计与实现说明。配套文档：`metric_definitions.md`
> （指标口径，**设计时的口径母本**）、`dwd_design.md`（DWD 明细层，本层的唯一事实源）、
> `data_contract.md`（契约）、`data_dictionary.md`（ODS 扫描事实）。
>
> 核心一句话：**从 DWD 标准明细 `dwd_events`（2,755,641 行）出发，以纯 SQL
> `INSERT ... SELECT` 服务端聚合出 3 张 DWS 日汇总表 + 4 张 ADS 应用表，逐步
> 支持 PV / UV / DAU / 加购率 / 转化率 / 商品排行 / 次日留存等指标直接查表；所有
> 聚合均服务端完成、Python 内存有界、按 UTC 日幂等重建。**
>
> 本文所有指标数值均为**生产库 `ecommerce_dw` 实测值**（来自构建后的直接查询与
> `verify`，无推算），每个 ADS 指标给出可复现 SQL（§8）。

## 0. 层定位与设计原则

| 维度 | 决定 |
| --- | --- |
| 层 | DWS（Data Warehouse Summary / 汇总层）+ ADS（Application Data Store / 应用层） |
| 输入 | `dwd_events`（DWD 去重后标准明细，唯一事实源）+ `ods_item_properties_raw`（categoryid 属性快照，仅供类目归属） |
| 输出 | DWS：`dws_user_daily` / `dws_item_daily` / `dws_category_daily`；ADS：`ads_traffic_daily` / `ads_funnel_daily` / `ads_item_ranking_daily` / `ads_retention_cohort`；审计：`dws_ads_build_log` |
| DWS 粒度 | 一行 = 一个「日 × 维度值」的去重行为汇总（人 / 商品 / 类目，UTC 日） |
| ADS 粒度 | 流量日报=一日一行；漏斗=一日一指 9 行；排行=一日最多 N 行；留存=一 cohort 一年龄行 |
| 时区 | 全链路 UTC；日界 = 世界时零点（`event_date_utc` 分区键） |
| 重建 | FULL（全量）/ DATES（按 UTC 日）两套语义，同 scope 幂等 |

设计原则（对应主方案 §2.4）：

1. **口径先钉死再建表**：每个指标的分子 / 分母 / 去重口径 / 分母为 0 的行为 /
   窗口不完整行为均来自 `metric_definitions.md`，在 `src/ecommerce_dw/dws/constants.py`
   落为可执行常量（`AGG_VERSION` / `FULL_WINDOW_START` / `FULL_WINDOW_END` /
   `FUNNEL_METRICS` / `retention_d0_range` / `is_full_day` / `retention_window_status`）。
2. **SQL-first、内存有界**：所有聚合是 `INSERT ... SELECT`，由 MySQL 服务端执行，
   Python 永不把百万行输入拉进内存（对比 DWD staging 的流式约束，本层更简单——
   聚合本身就是数据库擅长的操作）。构建顺序按依赖排序（§5.1）。
3. **幂等 + 可恢复**：`FULL` / `DATES` 两套重建语义（§5）；每次构建写入
   `dws_ads_build_log` 一行（scope、聚合版本、各表行数 JSON、起止、状态）驱动
   幂等判定与失败清理（§6）。
4. **审计到行**：每张输出表带 `build_id + loaded_at` 血缘（双向：从行可查构建，
   从构建可查该批各表写了几行）；`dws_ads_build_log` 与 DWD 的 `dwd_build_log`
   形态不同，独立建表（§6.3）。
5. **独立复核**：构建内在不变量 + 构建后 `verify` 用**与构建 SQL 写法不同的** SQL
   直接对 `dwd_events` 复算每个 ADS 指标，差值与列刻度的舍入容忍装箱（§7）。

## 1. 表结构、粒度与口径

### 1.1 DWS：`dws_user_daily`（用户行为日汇总）

一行 = 一个访客在一个 UTC 日的去重行为汇总。来源：`dwd_events`。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `event_date_utc` | DATE | PK（前导）。事件 UTC 日，日界=世界时零点 |
| `visitor_id` | BIGINT | PK。`>= 1` 为真实访客；`0` = 未知访客桶 |
| `visitor_zero` | TINYINT | `1` = 未知访客桶（DWD 中 `visitor_id IS NULL`，原始哨兵 `"0"`）；**计入 count、不计入任何去重用户口径** |
| `view_cnt` | BIGINT UNSIGNED | 当日 view 事件行数（不去重，含未知访客桶） |
| `add_to_cart_cnt` | BIGINT UNSIGNED | 当日 add_to_cart 事件行数 |
| `transaction_cnt` | BIGINT UNSIGNED | 当日 transaction 事件行数 |
| `order_cnt` | BIGINT UNSIGNED | 当日该访客 `distinct(transaction_id)`；**全局订单数不得对本列求和**（见 1.5 orders 说明） |
| `active_flag` | TINYINT | 恒 1（有事件必有本行）；供按人计数直接过滤 |
| `build_id` / `loaded_at` | BIGINT UNSIGNED / DATETIME(3) | 血缘：产生本行的聚合批次 / 写入时刻（UTC） |

索引：`idx_uds_user (visitor_id)`。空数说明：`visitor_id=0` 桶原数据仅 3 行哨兵，
真实访客 id 从 1 开始（DWD 构建约束），无冲突。

### 1.2 DWS：`dws_item_daily`（商品行为日汇总）

一行 = 一个商品在一个 UTC 日的去重行为汇总。来源：`dwd_events`。
字段与 `dws_user_daily` 同构但以 `(event_date_utc, item_id)` 为主键，另加
`idx_ids_item (item_id)`。去重口径：`view_uv` 等为「该行为且 `visitor_id IS NOT NULL`」
的 distinct visitor；**同一访客跨商品可重复计入**（商品维度的行内去重，不跨商品）。

### 1.3 DWS：`dws_category_daily`（类目行为日汇总）

一行 = 一个类目在一个 UTC 日的去重行为汇总。来源：`dwd_events`（行为）
+ `ods_item_properties_raw`（categoryid 快照，as-of）。

商品 → 类目的归属规则（`agg_sql.fill_item_category_sql`）：

- 取**事件日当天或之前最近**的 `categoryid` 属性快照（`snap_ms < 次日 0 点`），
  平局按 `category_id` 降序裁决保证确定性；**未来属性绝不参与**。
- 属性窗口早于事件日的未覆盖商品 → 未分类桶 `category_id = 0`（**不是**真实类目）。
- `categoryid` 属性值有 30 个不落在 `category_tree` 上，仍按属性值原样聚合，
  不校验类目树（保留脏值可审计）。

`event_date_utc, category_id` 为主键，`idx_cds_category (category_id)`。

### 1.4 ADS：`ads_traffic_daily`（站内流量日报）

一行 = 一个 UTC 日的流量三指标。来源：`dws_user_daily`。

| 字段 | 口径（均 UTC 日） |
| --- | --- |
| `pv` | 当日所有 view 事件行数（**不去重**，含未知访客桶；DWD 精确重复行默认不计） |
| `uv` | 当日发生过 view 的去重访客数（`view_cnt > 0 AND visitor_zero = 0`） |
| `dau` | 当日发生过任意事件的去重访客数（`active_flag = 1 AND visitor_zero = 0`） |
| `is_full_day` | `1` = 完整自然日（∈ 完整窗口 `[2015-05-04, 2015-09-17]`）；`0` = 部分日（首末日 05-03 / 09-18） |

### 1.5 ADS：`ads_funnel_daily`（行为漏斗日报，长表）

一行 = 一个 UTC 日的一个指标值（`(event_date_utc, metric_name)` 主键）。来源：
`dws_user_daily`（按人指标）+ `dwd_events`（orders 全局去重）。九种 `metric_name`：

| metric_name | 口径 |
| --- | --- |
| `view_users` | 浏览用户数 = 当日 UV（漏斗分母） |
| `cart_users` | 加购用户数（当日 add_to_cart 去重访客） |
| `buyer_users` | 交易用户数（当日 transaction 去重访客） |
| `orders` | **订单数** = 当日 `distinct(transaction_id)`，直接取自 `dwd_events` |
| `add_to_cart_rate` | 加购率口径 A = `cart_users / view_users`（分母 0 → NULL） |
| `conv_strict_users` | 严格漏斗转化分子 = 当日交易用户 ∩ 浏览用户 |
| `conv_global_users` | 全局转化分子 = 当日全部交易用户（**可 > 分母**，合法） |
| `conv_strict_rate` | `conv_strict_users / view_users`（分母 0 → NULL） |
| `conv_global_rate` | `conv_global_users / view_users`（可 > 100%，**不告警**） |

**为什么 orders 不来自用户行**（`dws_user_daily.order_cnt`）：`transaction_id` 是
订单级标识，**一个订单可以跨多行、甚至疑似跨日**；22,457 行 transaction 只对应
17,672 个不同 ID。若对用户行 `order_cnt` 求和，同一 transaction_id 每出现在一个
用户行就被计一次 → 生产实测**求和 17,674 ≠ 去重 17,672**（差 2）。口径钉死为
`orders = COUNT(DISTINCT transaction_id) FROM dwd_events`。

### 1.6 ADS：`ads_item_ranking_daily`（商品热度日排行 Top-N）

一行 = 一个商品在一个 UTC 日的上榜条目（默认每日 Top-100，`--top-n` 可调）。来源：
`dws_item_daily`。排序键：`view_cnt DESC → add_to_cart_cnt DESC → transaction_cnt DESC
→ item_id DESC`（确定性平局裁决）。`(event_date_utc, item_id)` 主键 +
`UNIQUE (event_date_utc, rank_no)`。

### 1.7 ADS：`ads_retention_cohort`（次日留存 cohort）

一行 = 一个 cohort 日 + 一个年龄日。来源：`dwd_events`（首次观测日 + 回访行为）。

| 字段 | 口径 |
| --- | --- |
| `cohort_date` | 首次观测日 = `min(event_date_utc)` per visitor（UTC） |
| `age_day` | 回访年龄日；`0` = cohort 自身规模行，`1` = 次日留存 |
| `cohort_size` | 分母：首次观测日为 cohort_date 的去重访客数 |
| `retained_users` | 分子：该 cohort 在 `cohort_date + age_day` 发生任意事件的去重访客数 |
| `retention_rate` | `retained_users / cohort_size`（DECIMAL(12,6)；分母 0 → NULL） |
| `window_status` | `'full'` = cohort 日与回访日均在完整窗口内；否则 `'partial'` |

**窗口不完整行为**：cohort D0 ∈ `[05-04, 09-16]` 才有完整次日观测（D1 回访日
∈ `[05-05, 09-17]`）。`09-17` / `09-18` 的 cohort 以及首日 `05-03` 一律标
`'partial'`（**不放 0**）——末尾不足的 D1 分子缺失，填 0 会误导报表。

## 2. 构建 SQL（SQL-first，服务端聚合）

所有聚合语句位于 `src/ecommerce_dw/dws/agg_sql.py`，一律 `INSERT INTO <表> SELECT`
由服务端执行；Python 只传入参数（scope 日集合、窗口、build_id、loaded_at），
**不读明细行到内存**。

统一 scope 机制：构建开始先建临时表 `tmp_agg_days`（PRIMARY KEY `event_date_utc`），
装下本 scope 的 UTC 日；**每条语句都 JOIN 它** → FULL 与 DATES 共享同一代码路径，
scope 日条件不做参数内联。

### 2.1 构建顺序（依赖排序）

```
tmp_agg_days（scope 日）
  ├─ dws_user_daily            ← dwd_events
  ├─ dws_item_daily            ← dwd_events
  ├─ dws_category_daily        ← dwd_events + tmp_item_category(as-of 映射，三张临时表)
  ├─ ads_traffic_daily         ← dws_user_daily
  └─ ads_funnel_daily          ← dws_user_daily(tmp_funnel_day) + dwd_events(orders)
     ads_item_ranking_daily    ← dws_item_daily
     ads_retention_cohort      ← dwd_events（全量窗口）
```

### 2.2 MySQL CTE / 临时表的一次引用约束

MySQL **不允许同一语句内重开派生表**（`insert ... select` 中一个 CTE / 临时表
只能被引用一次，否则 `ERROR 1137 Can't reopen table`）。本设计受此约束并绕开：

- 类目 as-of：先把「scope × 商品」落 `tmp_item_days`，再以单个引用一次的 `asof`
  CTE 完成 as-of 判重 → `tmp_item_category`（每语句只引用每个表一次）。
- 漏斗：先把逐日分子/分母（含窗口标记）定型到 `tmp_funnel_day`（一条语句、
  各源引用一次）；再把 9 个指标作为 **9 行的 metric 源表（CTE `m`）CROSS JOIN**
  展开，`o`（orders）CTE 引用一次，`tmp_funnel_day` 引用一次 — 避免 9 路 UNION
  对同一临时表重开。
- 留存：`fs`（首次观测）、`ad`（每日活跃足迹）、`age`（0/1 两行）、`joined`
  四个 CTE 各引用一次。

### 2.3 窗口 / 部分日标记

`is_full_day`（流量、漏斗、排行）与 `window_status`（留存）通过参数嵌入构建
（默认完整窗口 `[2015-05-04, 2015-09-17]`）。**部分日标记而非补零**：05-03/09-18
流量仍照实出数但 `is_full_day=0`，报表展示时剔除或打标；留存末尾 cohort 标
`'partial'`。

## 3. 幂等重建：FULL 与 DATES

同 scope 重建语义：

| | FULL | DATES |
| --- | --- | --- |
| 覆盖 | 全部 `dwd_events` 日 | 指定 UTC 日集合（`--dates 2015-05-04,...`） |
| delete-scope | 7 张输出表整表清空 | 日键表 `WHERE event_date_utc IN (...)`；留存表 `WHERE cohort_date IN (...)`（**cohort 日 = 重建日，不扩展**） |
| 用途 | 首次构建 / 口径升级（`AGG_VERSION` 变更） | 补数、修数 |

幂等保证：某 scope 的 `dates_csv` 恒定（`Scope` 归一排序去重），同一 scope 重复
执行 → 先把 scope 行删净再重插同样内容（volatile 血缘列除外）。`dws_ads_build_log`
判定：同 scope 已有 RUNNING → 视为崩溃遗留，sweep 后重建；已有 FAILED →
要求 `--force` 或 `cleanup`；`AGG_VERSION` 不一致 → 拒绝重建，要求 `--force`
（口径变更必须全量重建，防新旧逻辑混拼）。

## 4. 构建 CLI

`python -m ecommerce_dw.dws.cli`（`src/ecommerce_dw/dws/cli.py`）：

- `build [--dates ...] [--top-n N] [--force]`：构建/重建（§3）。
- `verify [--dates ...]`：只读独立复算比对（§7）。
- `status`：最近构建列表（build_id / status / scope / 行数 / 起止）。
- `cleanup --build <id>`：删除某构建产生的行并置 CLEANED。

## 5. 为什么不复用 DWD 的 `dwd_build_log`

`etl_batch_log` 以「源文件 + SHA-256」为幂等键（文件批幂等）；`dwd_build_log` 以
「transform 版本 + ok/duplicate/reject 三分类计数」为语义。DWS/ADS 构建**读的是
`dwd_events`（已标准化）**，幂等键是「scope（FULL 或 UTC 日集合）」而非文件，且
一次性写 7 张输出表，需要「每张表写了几行」的 JSON 计数与
`RUNNING → LOADED | FAILED → CLEANED` 状态机。所以独立建 `dws_ads_build_log`
（`sql/ddl/dws_ads_build_log.sql`），与 DWD 日志通过单据血缘分离、互不混淆。

## 6. 数据完整性：构建内不变量 + `verify` 独立复算

### 6.1 构建内不变量（`_verify_build`，任一项不成立 → 标记 FAILED）

1. `ads_traffic_daily.uv == ads_funnel_daily.view_users`（同一口径在两表间一致）；
2. `ads_funnel_daily.conv_global_users == buyer_users`（同一集合两条口径名）；
3. `conv_strict_users <= conv_global_users`（严格 ⊂ 全局，子集不变量）；
4. 留存 `age_day=0`：`retained_users == cohort_size` 且 `retention_rate == 1`；
5. 各表在 scope 内的行数与构建声称一致（row parity）。

### 6.2 `verify`：独立 SQL 复算（`verify_dws_ads`）

对每个 ADS 指标，用**与构建 SQL 写法不同**的 SQL 直接从 `dwd_events` 复算再比对：

| ADS 指标 | 独立复算 SQL 写法（差异点） |
| --- | --- |
| `pv` | 直接 `COUNT(*) ... WHERE event_type='view'`（构建是经 `dws_user_daily.view_cnt` 求和） |
| `uv` | `COUNT(DISTINCT visitor_id) WHERE event_type='view' AND visitor_id IS NOT NULL` |
| `dau` | `COUNT(DISTINCT visitor_id) WHERE visitor_id IS NOT NULL`（不经 `active_flag`） |
| `cart_users` | 直接加购去重（构建经 `add_to_cart_cnt > 0` 聚合） |
| `buyer_users` / `conv_global_users` | 直接交易去重 |
| `orders` | 直接 `COUNT(DISTINCT transaction_id)`（构建同样取 dwd_events，但写法独立） |
| `conv_strict_users` | 先各取 view/交易去重集合再同人同日 JOIN（构建是用户行双列同时 >0） |
| 三个 rate | 由独立分子分母现算（`denom=0 → NULL` 与构建一致） |
| `retention_d1` | `MIN(event_date_utc)` cohort + 次日足迹 LEFT JOIN（构建是 CTE 交） |

比对容差：**按列刻度感知**——比率列 `DECIMAL(...,6)` 存值（如 `0.333333`）与精确商
相差可达 5e-7，整数指标精确到 1e-9 也自动通过；统一容差 `< 1e-6`。`None` 与 `None`
相等（分母 0 的两边都空才算过）。

### 6.3 集成测试与口径样本

`tests/unit/test_dws_helpers.py`（纯函数单测：完整日 / 留存窗口 / scope 归一化）+
`tests/integration/test_dws_ads_mysql.py`（**全量端到端**，仅在 `ecommerce_dw_test`
运行；生产库不受影响）：覆盖同人多 view、加购→成交、精确重复事件、非法事件拒绝、
跨日事件、迟到事件、无访客（哨兵）事件等边界；基线全部手算；`verify` 断言 0 差异；
幂等重建 + 构建日志断言。

## 7. 时区与空数语义

- 全链路 UTC；`event_date_utc` 日界 = 世界时零点（与 DWD 一致，不做本地时区换算）。
- 未知访客桶（`visitor_id=0`）计入事件数（PV / 漏斗绝对量不含它，因为它没有
  view 事件），**不计入任何去重用户口径**。
- 分母为 0 → 指标输出 `NULL`（加购率、转化率、留存率；构建与 verify 两侧同规则）。
- 部分日 / 窗口不完整 → **标记而不补零**（`is_full_day=0`、`window_status='partial'`）。

## 8. 生产实测（Phase 4 measurement）

> 全部数字来自对生产库 `ecommerce_dw` 的**直接查询**（`ads_*` 表 + `dwd_events`
> 独立复算），非推算。构建批次见 `dws_ads_build_log`；`verify` 全指标 0 差异。

### 8.1 全量构建结果

全量构建 **build #1（FULL，LOADED）**，`AGG_VERSION=1`，累计 **3,431,482 行**
跨 7 张输出表。

| 表 | 行数 | 说明 |
| --- | --- | --- |
| `dws_user_daily` | 1,649,534 | 日 × 访客（含未知访客桶） |
| `dws_item_daily` | 1,672,186 | 日 × 商品 |
| `dws_category_daily` | 94,194 | 日 × 类目（含未分类桶 0） |
| `ads_traffic_daily` | 139 | 日 × 1（含 2 个部分日） |
| `ads_funnel_daily` | 1,251 | 139 日 × 9 指标 |
| `ads_item_ranking_daily` | 13,900 | 139 日 × 每日 Top-100 |
| `ads_retention_cohort` | 278 | 139 cohort × 2 年龄日 |

**类目 as-of 的一个可观察后果**：`categoryid` 属性快照最早出现在 2015-05-10，
故 **7 天**（05-04 ~ 05-10）的全部事件都落到未分类桶 `category_id=0`（当日唯一
一类目行）。全区间每日类目行数 1 ~ 791（均值 677.7/日）。

### 8.2 流量：全区间 PV / UV / DAU

> 完整窗口 = 完整自然日集合 `[2015-05-04, 2015-09-17]`（137 天）。

| 量 | 值 |
| --- | --- |
| 全区间 PV（完整窗口 view 行数之和） | **2,649,436** |
| 日均 PV（PV / 137） | 19,338.9 |
| 全区间 UV（完整窗口内发生过 view 的去重访客，**非日均求和**） | **1,396,937** |
| 全区间 DAU（完整窗口任意事件去重访客） | 1,400,324 |
| 日均 UV / 日均 DAU | 11,943.3 / 11,975.6 |
| 峰值日 | 2015-07-26：PV 32,077 / DAU 17,516 |

> 注：全 139 天口径的区间 UV = **1,404,178**（= 1,396,937 + 7,241，其中 7,241 是
> 只在部分日 05-03 / 09-18 出现过的 view 访客），与 Phase 1 `metric_definitions.md`
> §2 锚点一致；DWD 去重对区间 UV **无净影响**（精确重复行的 winner 仍保留该访客）。
> 部分日实测：05-03（PV 13,303 / UV 7,845 / DAU 7,865）、09-18（PV 1,479 / UV 1,012 /
> DAU 1,016），均 `is_full_day=0`，展示时剔除或打标。

复现 SQL：见 §8.7-1。

### 8.3 漏斗（完整窗口区间口径 + 日均口径）

**区间口径**（137 天窗口内去重，跨日只算一次）：

| 指标 | 全区间值 |
| --- | --- |
| `view_users`（浏览用户，去重） | 1,396,937 |
| `cart_users` | 37,515 |
| `buyer_users` | 11,667 |
| `orders`（distinct transaction_id） | 17,591 |
| `add_to_cart_rate`（口径 A） | 2.69% |
| `conv_strict_users` / `conv_strict_rate` | 11,238 / 0.80% |
| `conv_global_users` / `conv_global_rate` | 11,667 / 0.84% |

**日均口径**（长表逐日取均值）：`add_to_cart_rate` 0.025274、`conv_strict_rate`
0.007471、`conv_global_rate` 0.007931。

**orders 不可求和的实测证据**：对 `dws_user_daily.order_cnt` 求和 ≠ 全局去重——
全窗口口径 **17,593 vs 17,591**、全 139 天口径 **17,674 vs 17,672**（各差 2；存在
跨日/跨行的同一 transaction_id 被用户行重复计数）。口径钉死：
`orders = COUNT(DISTINCT transaction_id) FROM dwd_events`（§1.5）。

示例日 2015-05-04（完整日）九指标：view_users 10,995 / cart_users 304 /
buyer_users 101 / orders 128 / add_to_cart_rate 0.027649 / conv_strict_users 98 /
conv_global_users 101 / conv_strict_rate 0.008913 / conv_global_rate 0.009186。

### 8.4 商品排行

| 量 | 值 |
| --- | --- |
| 全区间浏览最高的单品 | item_id **187,946**，区间 PV **3,409** |
| 次高 | item_id 461,686（2,534）、5411（2,324）、370653（1,839）、219512（1,738） |
| 单日最高 PV 日与值 | 2015-07-26，PV 32,077 |

### 8.5 次日留存 D1

> cohort D0 ∈ `[05-04, 09-16]` 为完整窗口 cohort（136 天）；其余 3 天（05-03 / 09-17 /
> 09-18）标 `partial`。

| 量 | 值 |
| --- | --- |
| D0=全区间新客（完整 cohort 规模之和） | 1,393,829 |
| D1 留存用户（完整 cohort 分子之和） | 38,370 |
| **D1 留存率（完整 cohort 加权）** | **0.02753（2.75%）** |
| 最强 / 最弱完整 cohort 日 | 2015-05-05（0.041076）/ 2015-09-15（0.016686） |
| 部分 cohort 合计（3 天，不得当完整率） | 13,750 人 / 457 回流（粗 3.3%） |

### 8.6 构建与验证

| 项 | 值 |
| --- | --- |
| 全量构建 build_id / status | **build #1（FULL，LOADED）**，AGG_VERSION=1 |
| 构建时长 | **164.53s**（2 分 45 秒，含 7 张表 + 构建内不变量校验） |
| `verify` 独立复算 | **1,668 checks / 139 天，ALL MATCH（0 差异）** |
| 构建内不变量（1~5） | 全部通过（uv==view_users、conv_global==buyer、strict⊆global、age0 恒等、row parity） |
| 幂等验证 | **生产实测**：build #1（FULL）后对 2015-07-26 跑 DATES 重建（build #2），7 张表当日行数逐一不变（17,516 / 13,531 / 720 / 1 / 9 / 100 / 2），`verify --dates 2015-07-26` 12 checks ALL MATCH；集成测试另覆盖同 scope 重建内容一致的断言 |

### 8.7 每个 ADS 值的可复现 SQL

**泛化说明**：ADS 层是报表直查层，下列「直查」SQL 就是每个指标的值来源；独立
复算（与构建 SQL 不同写法）见 §6.2 与 §8.7-6。

#### 8.7-1 流量（`ads_traffic_daily`）

```sql
-- 日 PV/UV/DAU（完整日窗口，剔除部分日）
SELECT event_date_utc, pv, uv, dau
FROM ads_traffic_daily
WHERE is_full_day = 1
ORDER BY event_date_utc;

-- 全窗口 PV 合计 / 日均 PV / 日均 DAU
SELECT SUM(pv)                       AS pv_total,
       SUM(pv) / COUNT(*)            AS pv_daily_avg,
       SUM(dau) / COUNT(*)           AS dau_daily_avg
FROM ads_traffic_daily
WHERE is_full_day = 1;

-- 全区间 UV（独立于日表：跨日去重，≠ 日 UV 之和）
SELECT COUNT(DISTINCT visitor_id) AS uv_interval
FROM dwd_events
WHERE event_type = 'view' AND visitor_id IS NOT NULL
  AND event_date_utc BETWEEN '2015-05-04' AND '2015-09-17';
```

#### 8.7-2 漏斗（`ads_funnel_daily`）

```sql
-- 单日九指标（长表直接展开）
SELECT metric_name, metric_value
FROM ads_funnel_daily
WHERE event_date_utc = '2015-05-04'
ORDER BY FIELD(metric_name, 'view_users','cart_users','buyer_users','orders',
               'add_to_cart_rate','conv_strict_users','conv_global_users',
               'conv_strict_rate','conv_global_rate');

-- 全窗口用户口径（跨日去重，**不能**对日行求和；orders 直接去重，不求和用户行）
SELECT COUNT(DISTINCT CASE WHEN event_type = 'view' THEN visitor_id END)  AS view_users,
       COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN visitor_id END) AS cart_users,
       COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN visitor_id END) AS buyer_users,
       COUNT(DISTINCT transaction_id)                                 AS orders
FROM dwd_events
WHERE event_date_utc BETWEEN '2015-05-04' AND '2015-09-17'
  AND visitor_id IS NOT NULL AND event_type IN ('view','add_to_cart','transaction');
```

#### 8.7-3 商品排行（`ads_item_ranking_daily`）

```sql
-- 每日榜首
SELECT event_date_utc, item_id, rank_no, view_cnt, total_events
FROM ads_item_ranking_daily
WHERE rank_no = 1 AND is_full_day = 1
ORDER BY event_date_utc;

-- 全区间浏览最高的单品（直接对 dws_item_daily 求和再排序）
SELECT item_id, SUM(view_cnt) AS interval_pv
FROM dws_item_daily
WHERE event_date_utc BETWEEN '2015-05-04' AND '2015-09-17'
GROUP BY item_id
ORDER BY interval_pv DESC
LIMIT 10;
```

#### 8.7-4 次日留存（`ads_retention_cohort`）

```sql
-- 完整窗口 cohort 的 D1 留存（D0 ∈ [2015-05-04, 2015-09-16]）
SELECT cohort_date, cohort_size, retained_users, retention_rate, window_status
FROM ads_retention_cohort
WHERE age_day = 1 AND window_status = 'full'
ORDER BY cohort_date;

-- 完整 cohort 加权 D1 留存率
SELECT SUM(retained_users) / SUM(cohort_size) AS retention_d1
FROM ads_retention_cohort
WHERE age_day = 1 AND window_status = 'full';
```

#### 8.7-5 血缘

```sql
-- 任意 ADS/DWS 行的来源批次与耗时
SELECT b.build_id, b.scope_type, b.agg_version, b.status,
       b.rows_written, b.started_at, b.finished_at
FROM dws_ads_build_log b
ORDER BY b.build_id DESC LIMIT 5;

-- 某批次每张输出表写了几行（table_stats JSON）
SELECT build_id, table_stats FROM dws_ads_build_log ORDER BY build_id DESC LIMIT 1;
```

#### 8.7-6 独立复算（`verify` 用，与构建 / 直查 SQL 写法不同）

- PV / UV / DAU / cart / buyer / orders：直接 `COUNT(*)` / `COUNT(DISTINCT visitor_id)`
  对 `dwd_events` 按日分组（构建是经 `dws_user_daily` 求和）；
- `conv_strict_users`：view / 交易两个去重集合**同日 JOIN**（构建是用户行双列同时 >0）；
- `retention_d1`：`MIN(event_date_utc)` cohort 与次日足迹 LEFT JOIN（构建是 CTE 交）。
- 全部比对逻辑在 `src/ecommerce_dw/dws/builder.py::verify_dws_ads`，容差 `< 1e-6`。
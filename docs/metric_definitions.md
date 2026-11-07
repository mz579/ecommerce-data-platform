# 指标口径定义（V1.0 · 最终口径 + 实测值）

> 本文档是 Phase 1 口径的**回填版**：定义不变，补上 Phase 4/5/7/8 的最终实测值。
> 所有数字来自对生产库 `ecommerce_dw` 的直接查询、`ads_*` 表、`dwd_events` 独立复算
> 与 `docs/dws_ads_design.md` §8 的记录；不是估算。
>
> **引用约定**
> - “完整窗口” = `[2015-05-04, 2015-09-17]`，**137 个完整自然日**。
> - “全 139 日” = 全部数据窗口 `[2015-05-03, 2015-09-18]`，含首末日两个**部分日**
>   （2015-05-03 自 03:00 UTC 起、2015-09-18 至 02:59 UTC 止）。
> - 两个窗口的区间去重指标**不能混用**：完整窗口剔除部分日，全 139 日包含部分日。
> - 行级事件口径：ODS 原始行（未去重）与 DWD 标准行（精确去重后）分别标注。

## 0. 通用约定

- **时区**：一律 UTC。`event_time_utc`（毫秒 → 时间戳）→ `event_date_utc`；
  日界用 `day = ts // 86400000`（UTC 世界时），不做本地时区换算。
- **事件类型**：DWD 标准枚举 `{view, add_to_cart, transaction}`；ODS 原始拼写
  `addtocart` 保留在 `raw_event_type`。
- **未知访客**：原始 `visitorid="0"` 共 3 行，DWD 中 `visitor_id=NULL` +
  `quality_flags` 标记；计入事件行数（PV），不计入任何 `distinct visitor` 口径。
- **去重**：DWD 把业务键完全相同（仅 `source_row_number` 不同）的 460 行抽到
  `dwd_events_duplicate`（458 个 `dedup_key`）；指标默认使用去重后的 `dwd_events`。
  需要未去重口径时，把重复表对应行按 `dedup_key` 补回即可。
- **空值**：分母为 0 → 比率 `NULL`（不是 0）；部分日/窗口不足 → 打标
  （`is_full_day=0` / `window_status='partial'`），不补零。
- **订单**：`transaction_id` 是订单级标识；22,457 行交易事件只对应 17,672 个不同 ID。
  订单数必须 `COUNT(DISTINCT transaction_id)`，**不能**用交易事件行数或用户行 `order_cnt` 求和。

### 0.1 口径签名模板

每个指标在 ADS 落地时附一行签名：

```text
metric  = <指标名>
grain   = interval / daily(UTC) / cohort
scope   = <表与事件类型>
dedup   = <去重键>
filter  = <过滤条件与窗口>
tz      = UTC
```

### 0.2 数据事实速查

| 量 | ODS 原始行 | DWD 去重后 |
| --- | ---: | ---: |
| `view` 事件行数 | 2,664,312 | 2,664,218 |
| `add_to_cart` / `addtocart` 事件行数 | 69,332 | 68,966 |
| `transaction` 事件行数 | 22,457 | 22,457 |
| 事件合计 | 2,756,101 | 2,755,641 |
| 精确重复行 | — | 460（`dwd_events_duplicate`） |
| 拒绝行 | — | 0（`dwd_events_reject`） |
| 去重访客（任意行为） | 1,407,580（含哨兵 `0`） | 去哨兵后 1,407,579 |
| 去重访客（仅 `view`） | 1,404,178 | 1,404,178（精确去重不改变该集合） |
| 去重商品（事件侧） | 235,061 | 235,061 |
| 去重 `transaction_id` | 17,672 | 17,672 |
| 覆盖 UTC 日数 | 139（含 2 个部分日） | 139 |

## 1. PV（页面浏览量 / Page View）

- **业务定义**：站点页面被浏览的次数，衡量流量体量。
- **分子**：`event_type = 'view'` 的事件行数；**不去重**（同一 visitor 连点刷新计多次，同毫秒多行计多次）。
- **分母**：无。
- **过滤条件**：`event_type = 'view'`；`event_date_utc` 在观察窗口内；不过滤 visitor（含未知访客桶）。
- **去重口径**：不去重。DWD 默认剔除精确重复行；需要未去重口径时补回 `dwd_events_duplicate`。
- **时区**：UTC。
- **实测值**：

| 口径 | 值 | 说明 |
| --- | ---: | --- |
| 全 139 日 PV | 2,664,218 | `ads_traffic_daily.pv` 求和（含两个部分日：05-03 13,303 + 09-18 1,479） |
| 完整窗口 PV | **2,649,436** | `WHERE is_full_day=1` 的 `SUM(pv)` |
| 日均 PV（完整窗口） | 19,338.9 | 2,649,436 / 137 |
| 峰值日 | 2015-07-26：**32,077** | 同日 DAU 17,516 |

- **口径签名**：

```text
metric = PV
grain  = daily(UTC) / interval
scope  = dwd_events, event_type = 'view'
dedup  = 不去重（事件行级）
filter = event_date_utc in [window]；含未知访客桶
tz     = UTC
```

## 2. UV（独立访客数 / Unique Visitors）

- **业务定义**：指定窗口内发生过 `view` 行为的**独立访客数**（项目基线口径）。
- **分子**：`event_type='view'` 的 `distinct visitor_id`。
- **分母**：无。
- **过滤条件**：`event_type='view'`；`visitor_id IS NOT NULL`（哨兵 `"0"` 已置 NULL）；窗口过滤。
- **去重口径**：`visitor_id` 级；跨日/跨月累计 UV 是同一个去重集合，**不等于日 UV 求和**。
- **时区**：UTC。
- **实测值**：

| 口径 | 值 | 说明 |
| --- | ---: | --- |
| 完整窗口区间 UV | **1,396,937** | 137 个完整日内 `view` 去重访客 |
| 全 139 日区间 UV | **1,404,178** | 含只在 05-03 / 09-18 出现过的 7,241 名访客 |
| 日 UV 均值（完整窗口） | 11,943.3 | 日 UV 之和 / 137；**不是**区间 UV |

- **口径签名**：

```text
metric = UV
grain  = interval / daily(UTC)
scope  = dwd_events, event_type = 'view'
dedup  = visitor_id
filter = event_type='view' and visitor_id IS NOT NULL and event_date_utc in [window]
tz     = UTC
```

## 2.1 全行为活跃访客数（宽口径，非 UV）

- **业务定义**：窗口内发生过**任意事件**（view / add_to_cart / transaction）的独立访客数；不限定事件类型。
- **分子**：窗口内 `distinct visitor_id`。
- **过滤条件**：`visitor_id IS NOT NULL`；窗口过滤。
- **去重口径**：`visitor_id` 级；跨日累计不叠加（≠ DAU 求和）。
- **实测值**：全 139 日 = **1,407,579**（Phase 1 原始口径 1,407,580 含哨兵 `"0"`，去哨兵后 1,407,579）；完整窗口任意事件去重复算参考值 = **1,400,324**（`docs/dws_ads_design.md` §8.2，不是 `ads_funnel_interval` 的列）。
- **口径签名**：

```text
metric = 全行为活跃访客数
grain  = interval
scope  = dwd_events, event_type in {view, add_to_cart, transaction}
dedup  = visitor_id
filter = visitor_id IS NOT NULL and event_date_utc in [window]
tz     = UTC
```

## 3. DAU（日活跃用户数 / Daily Active Users）

- **业务定义**：某 UTC 日内发生过**任意行为**的独立访客数。
- **分子**：某日 `distinct visitor_id`，`event_type` 任意，`visitor_id IS NOT NULL`。
- **分母**：无。
- **过滤条件**：`event_date_utc = 观察日`。
- **去重口径**：日内按 `visitor_id` 去重；跨日不累计、不串。
- **时区**：UTC，日界为世界时零点。
- **边界情况**：
  - 首末日为部分日，DAU 系统性偏小；ADS 用 `is_full_day=0` 标记，展示时剔除或打标。
  - 极低活日 DAU=0 属正常，保留 0 不丢行。
- **实测值**：

| 口径 | 值 | 说明 |
| --- | ---: | --- |
| 完整窗口日 DAU 均值 | 11,975.6 | 137 个完整日的日 DAU 平均 |
| 峰值日 DAU | 2015-07-26：**17,516** | 同日 PV 32,077 |
| 完整窗口任意事件去重访客（区间参考） | 1,400,324 | 不等于日 DAU 求和 |
| 部分日示例 | 2015-05-03：7,865；2015-09-18：1,016 | 均 `is_full_day=0` |

- **口径签名**：

```text
metric = DAU
grain  = daily(UTC)
scope  = dwd_events, event_type in {view, add_to_cart, transaction}
dedup  = visitor_id
filter = visitor_id IS NOT NULL and event_date_utc = day
tz     = UTC
```

## 4. 加购用户数（Add-to-Cart Users）

- **业务定义**：窗口内至少发生过一次加购的独立访客数（人头口径，不是加购件数）。
- **分子**：`distinct visitor_id where event_type='add_to_cart'`。
- **分母**：无。
- **过滤条件**：`event_type='add_to_cart'`；`visitor_id IS NOT NULL`；窗口过滤。
- **去重口径**：`visitor_id` 级（一人多次加购只算 1 人）。
- **实测值**：

| 口径 | 值 | 说明 |
| --- | ---: | --- |
| 完整窗口加购用户 | **37,515** | 137 个完整日区间去重 |
| 全 139 日加购用户 | 37,722 | 含部分日；Phase 1 ODS 口径 |
| 加购事件行数（DWD） | 68,966 | 不是用户数；ODS 原始 69,332 |

- **口径签名**：

```text
metric = 加购用户数
grain  = interval / daily(UTC)
scope  = dwd_events, event_type = 'add_to_cart'
dedup  = visitor_id
filter = visitor_id IS NOT NULL and event_date_utc in [window]
tz     = UTC
```

## 5. 加购率（Add-to-Cart Rate）

- **业务定义**：访客从“看看”推进到“想买”的密度。**默认用户级口径 A**。
- **口径 A（用户级，默认）**：
  - 分子：加购用户数（§4）。
  - 分母：同期浏览用户数（= §2 UV，view 口径）。
- **口径 B（事件级，辅助）**：
  - 分子：`add_to_cart` 事件行数。
  - 分母：`view` 事件行数。
- **时区**：UTC；分子分母必须落在同一窗口（同一 UTC 日或同一区间）。
- **边界情况**：
  - 分母为 0 → 输出 `NULL`，不是 0，也不除零。
  - A 是“人”口径、B 是“行为密度”口径，**不可混用**；默认 A。
  - 「加购用户 ⊆ 浏览用户」**不恒成立**：全 139 日有 3,321 名加购用户从未产生 `view`；分日报表分子 > 分母时先校验来源，不要写死子集关系。
- **实测值**：

| 口径 | 值 | 说明 |
| --- | ---: | --- |
| 完整窗口区间加购率 A | **2.69%** | 37,515 / 1,396,937 |
| 日均加购率（逐日 A 的均值） | 0.025274（2.5274%） | `ads_funnel_daily` 逐日 `add_to_cart_rate` 均值 |
| 全 139 日参考 | 37,722 / 1,404,178 | 仍约 2.69% |

- **口径签名**：

```text
metric = 加购率(口径A，默认)
grain  = interval / daily(UTC)
scope  = dwd_events
dedup  = visitor_id（分子/分母各自按 §4 / §2 去重，不天然嵌套）
filter = 加购用户数 ÷ 浏览用户数，同区间/同日；分母 0 -> NULL
tz     = UTC
```

## 6. 交易行为用户数与订单数

### 6.1 交易用户数（成交人数）

- **业务定义**：窗口内至少发生一笔成交的独立访客数（人头口径）。
- **分子**：`distinct visitor_id where event_type='transaction'`。
- **分母**：无。
- **去重口径**：`visitor_id` 级。
- **实测值**：完整窗口 **11,667**；全 139 日 **11,719**（Phase 1 ODS 口径；428 名交易用户全程无 `view`）。
- **口径签名**：

```text
metric = 交易用户数
grain  = interval / daily(UTC)
scope  = dwd_events, event_type = 'transaction'
dedup  = visitor_id
filter = visitor_id IS NOT NULL and event_date_utc in [window]
tz     = UTC
```

### 6.2 订单数

- **业务定义**：窗口内去重订单数。
- **分子**：`COUNT(DISTINCT transaction_id)`，直接取自 `dwd_events`。
- **过滤条件**：`event_type='transaction'` 且 `transaction_id IS NOT NULL`；窗口过滤。
- **去重口径**：`transaction_id` 级；一个 `transaction_id` 可跨日/跨行，**不可用用户行 `order_cnt` 求和**。
- **实测值**：

| 口径 | 值 | 说明 |
| --- | ---: | --- |
| 完整窗口订单数 | **17,591** | `ads_funnel_interval.orders` |
| 全 139 日订单数 | 17,672 | 交易事件行 22,457 对应的去重 ID 数 |
| 用户行 `order_cnt` 求和（错误做法） | 17,593 / 17,674 | 各比正确值多 2（跨日/跨行重复） |

- **口径签名**：

```text
metric = 订单数
grain  = interval / daily(UTC)
scope  = dwd_events, event_type = 'transaction'
dedup  = transaction_id
filter = transaction_id IS NOT NULL and event_date_utc in [window]
tz     = UTC
note   = 不可用 dws_user_daily.order_cnt / dws_item_daily.order_cnt 求和替代
```

## 7. 行为转化率（浏览 → 成交）

- **业务定义**：整体漏斗顶（浏览）到底（成交）的转化强度。**默认严格漏斗口径**；另设全局口径（备选），两者名字必须区分。
- **分母**：同期浏览用户数（= §2 UV，view 口径）。
- **分子（严格漏斗口径，默认）**：窗口内 `distinct visitor_id` with `event_type='transaction'` **且**窗口内有过 `view`（交易用户 ∩ 浏览用户）；天然保证 分子 ⊆ 分母。
- **分子（全局转化口径，备选）**：窗口内全部 `distinct visitor_id` with `event_type='transaction'`；**没有**“分子 ≤ 分母”约束。
- **过滤条件**：
  - 同一用户：分子/分母各自按 `visitor_id` 去重后做比率。
  - 同一窗口：分子/分母落在同一 UTC 日或同一区间，不跨窗口混配。
  - 事件顺序：默认**不强求**（窗口内既出现 view 又出现 transaction 即算转化）；若业务要求“先浏览后成交”，作为独立变体另行命名。
- **边界情况**：
  - 分母为 0 → `NULL`。
  - 全 139 日有 428 名交易用户全程无 `view`；全局口径把这 428 人计入分子但不计入分母 → 单日转化率**可合法超过 100%**，不得按质量异常告警。
  - 分日转化率与区间转化率名称必须区分；ADS 同时提供。
- **实测值**：

| 口径 | 完整窗口 | 计算 |
| --- | ---: | --- |
| 严格漏斗分子（交易 ∩ 浏览） | **11,238** | 11,238 / 1,396,937 = **0.80%** |
| 全局分子（全部交易用户） | **11,667** | 11,667 / 1,396,937 = **0.84%** |
| 日均严格转化率 | 0.007471（0.7471%） | 逐日 `conv_strict_rate` 均值 |
| 日均全局转化率 | 0.007931（0.7931%） | 逐日 `conv_global_rate` 均值 |
| 全 139 日参考 | 严格 11,291 / 全局 11,719 | 分母 1,404,178 |

- **口径签名（严格，默认）**：

```text
metric = 行为转化率(严格漏斗口径，默认)
grain  = interval / daily(UTC)
scope  = dwd_events
dedup  = visitor_id
filter = 分子: event_type='transaction' 且窗口内有过 view；
         分母: event_type='view' and visitor_id IS NOT NULL；同窗口；事件顺序不强求
tz     = UTC
```

- **口径签名（全局，备选）**：

```text
metric = 行为转化率(全局口径，备选)
grain  = interval / daily(UTC)
scope  = dwd_events
dedup  = visitor_id
filter = 分子: event_type='transaction'；分母: event_type='view'；同窗口
tz     = UTC
note   = 分子 > 分母合法，不告警
```

## 8. 次日留存率（首次观测 cohort / Day-1 Retention）

- **业务定义**：以“首次在数据中出现行为的那一天”为 cohort 划分；统计该 cohort 在次日（D+1 UTC 自然日）再次发生任意行为的比例。
- **分子**：`{visitors first seen on D0}` ∩ `{visitors with any event on D0+1}`。
- **分母**：`{visitors first seen on D0}` 的 cohort 规模。
- **过滤条件**：
  - cohort 定义基于首次观测日 = `min(event_date_utc) per visitor_id`；
  - D+1 行为不限事件类型（view / add_to_cart / transaction 任意）。
- **去重口径**：`visitor_id` 级；分子分母同一次去重，分子 ≤ 分母恒成立。
- **边界情况**：
  - 分母为 0 → `NULL`，不除零。
  - 窗口不完整：D0 取数据首尾日会污染；D0+1 落在 2015-09-18 之后的 cohort 次日无观测，标记 `window_status='partial'`，**不放 0**。
  - 完整 cohort 区间：D0 ∈ `[2015-05-04, 2015-09-16]`（136 天）；05-03 / 09-17 / 09-18 标 `partial`。
  - 匿名 Cookie 重置会制造“伪新客”，系统性抬高分子分母。
- **实测值**：

| 口径 | 值 | 说明 |
| --- | ---: | --- |
| 完整 cohort 规模之和 | 1,393,829 | 136 个完整 D0 |
| D1 留存用户之和 | 38,370 | 完整 cohort 分子 |
| **D1 留存率（完整 cohort 加权）** | **0.02753（2.75%）** | 38,370 / 1,393,829 |
| 最强完整 cohort 日 | 2015-05-05：0.041076 | 4.11% |
| 最弱完整 cohort 日 | 2015-09-15：0.016686 | 1.67% |
| 部分 cohort 合计（3 天） | 13,750 人 / 457 回流（粗 3.3%） | 不得当完整率使用 |
| ADS 行数 | 278 | 139 cohort × 2 年龄日；`age_day=0` 为 cohort 规模行 |

- **口径签名**：

```text
metric = 次日留存率(D1)
grain  = cohort(cohort_date, age_day)
scope  = dwd_events, 任意事件类型（回访不限类型）
dedup  = visitor_id
filter = cohort_date = min(event_date_utc) per visitor；
         retained = cohort 中在 cohort_date+1 有任意事件；
         完整 cohort 仅 [2015-05-04, 2015-09-16]，其余标 partial
tz     = UTC
```

## 9. 商品行为：每日去重商品数

- **业务定义**：某 UTC 日内发生过某类行为的**去重商品数**（不是区间商品数）。
- **分子**：
  - `items_viewed = COUNT(DISTINCT item_id) where event_type='view'`
  - `items_carted = COUNT(DISTINCT item_id) where event_type='add_to_cart'`
  - `items_bought = COUNT(DISTINCT item_id) where event_type='transaction'`
- **过滤条件**：`item_id IS NOT NULL`；`event_date_utc = 观察日`。
- **去重口径**：按日、按 `item_id` 去重；**跨日求和会重复计数**，不能当区间商品数。
- **时区**：UTC。
- **实测行数**：`ads_item_behavior_daily` 139 行（每日一行）；每日具体值见该表。
- **相关商品排行**：`ads_item_ranking_daily` 每日 Top-100；全区间浏览最高单品 `item_id=187,946`，区间 PV 3,409。
- **口径签名**：

```text
metric = 每日去重商品数
grain  = daily(UTC)
scope  = dwd_events, event_type in {view, add_to_cart, transaction}
dedup  = item_id（按日）
filter = item_id IS NOT NULL and event_date_utc = day
tz     = UTC
```

## 10. 指标到 ADS 表的映射

| 指标 | 存储表/列 | 粒度 | 备注 |
| --- | --- | --- | --- |
| PV / UV / DAU | `ads_traffic_daily.pv/uv/dau` | 日 | `is_full_day` 标部分日 |
| 加购用户 / 交易用户 / 订单 | `ads_funnel_daily`（`cart_users` / `buyer_users` / `orders`） | 日 × 口径名 | 长表 |
| 加购率 / 严格转化 / 全局转化 | `ads_funnel_daily`（`*_rate`） | 日 × 口径名 | 分母 0 → NULL |
| 区间漏斗 9 指标 | `ads_funnel_interval`（单行） | 完整窗口 | KPI 卡直读，0.00s 冷启动 |
| 商品日排行 | `ads_item_ranking_daily` | 日 × 商品 | Top-100 |
| 次日留存 | `ads_retention_cohort` | cohort × age_day | `window_status` 标 partial |
| 每日去重商品数 | `ads_item_behavior_daily` | 日 | R16 物化 |
| 类目日汇总 | `dws_category_daily` / `v_ads_category_daily` | 日 × 类目 | 类目 0 = 未分类桶 |
| 商品订单数 | `v_ads_item_orders_daily` | 日 × 商品 | `distinct transaction_id` |

## 11. 最终值汇总（完整窗口，除非另注）

| 指标 | 值 |
| --- | ---: |
| PV | 2,649,436 |
| UV（区间去重） | 1,396,937 |
| 日均 PV / 日均 UV / 日均 DAU | 19,338.9 / 11,943.3 / 11,975.6 |
| 峰值日 PV / DAU（2015-07-26） | 32,077 / 17,516 |
| 加购用户 | 37,515 |
| 加购率 A | 2.69% |
| 交易用户 | 11,667 |
| 订单数 | 17,591 |
| 严格转化分子 / 率 | 11,238 / 0.80% |
| 全局转化分子 / 率 | 11,667 / 0.84% |
| D1 留存率（完整 cohort 加权） | 2.75% |
| 全 139 日 PV / UV / 加购用户 / 交易用户 / 订单 | 2,664,218 / 1,404,178 / 37,722 / 11,719 / 17,672 |
| `verify` 独立复核 | 2,094 checks，ALL MATCH |
| 质量门禁 | run #7 PASS 60/60，89,305,119 行，209.50s |

> 可复现 SQL 见 `docs/dws_ads_design.md` §8.7；Superset 与直连 SQL 的 5 项关键指标
> 一致性见 `docs/superset_design.md` §9.6（PV 2,664,218、区间 UV 1,396,937、
> 2015-07-26 DAU 17,516、严格转化率 0.008、2015-07-26 订单 101）。

## 12. 口径签名汇总

```text
metric = PV
grain  = daily(UTC) / interval
scope  = dwd_events, event_type = 'view'
dedup  = 不去重
filter = event_date_utc in [window]
tz     = UTC

metric = UV
grain  = interval / daily(UTC)
scope  = dwd_events, event_type = 'view'
dedup  = visitor_id
filter = event_type='view' and visitor_id IS NOT NULL and event_date_utc in [window]
tz     = UTC

metric = DAU
grain  = daily(UTC)
scope  = dwd_events, event_type in {view, add_to_cart, transaction}
dedup  = visitor_id
filter = visitor_id IS NOT NULL and event_date_utc = day
tz     = UTC

metric = 加购用户数
grain  = interval / daily(UTC)
scope  = dwd_events, event_type='add_to_cart'
dedup  = visitor_id
filter = visitor_id IS NOT NULL and event_date_utc in [window]
tz     = UTC

metric = 加购率(口径A)
grain  = interval / daily(UTC)
scope  = dwd_events
dedup  = visitor_id（分子/分母各自去重，不天然嵌套）
filter = 加购用户数 ÷ 浏览用户数，同窗口；分母 0 -> NULL
tz     = UTC

metric = 行为转化率(严格，默认)
grain  = interval / daily(UTC)
scope  = dwd_events
dedup  = visitor_id
filter = 分子: transaction 且窗口内有过 view；分母: view；同窗口；顺序不强求
tz     = UTC

metric = 行为转化率(全局，备选)
grain  = interval / daily(UTC)
scope  = dwd_events
dedup  = visitor_id
filter = 分子: transaction；分母: view；同窗口；分子可 > 分母
tz     = UTC

metric = 订单数
grain  = interval / daily(UTC)
scope  = dwd_events, event_type='transaction'
dedup  = transaction_id
filter = transaction_id IS NOT NULL and event_date_utc in [window]
tz     = UTC

metric = 次日留存率(D1)
grain  = cohort(cohort_date, age_day)
scope  = dwd_events, 任意事件
dedup  = visitor_id
filter = cohort_date = min(event_date_utc) per visitor；
         retained = cohort 中 cohort_date+1 有任意事件；
         完整 cohort [2015-05-04, 2015-09-16]
tz     = UTC

metric = 每日去重商品数
grain  = daily(UTC)
scope  = dwd_events, event_type in {view, add_to_cart, transaction}
dedup  = item_id（按日）
filter = item_id IS NOT NULL and event_date_utc = day
tz     = UTC
```

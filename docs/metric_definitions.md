# 指标口径定义（Phase 1）

> 本文档定义 Phase 4（DWS/ADS）将要实现的业务指标。所有指标的边界条件
> （分母为 0、观察窗口不完整、去重口径、时区）预先在此钉死，避免建模时
> 各层出现口径漂移。**引用的“数据集事实”均来自 `src/ecommerce_dw/eda`
> 实际全量扫描**（见 `notebooks/01_eda.ipynb` 与 `docs/data_dictionary.md`）。
>
> Phase 1 只给出口径与边界，不产出最终指标值（除可直接统计的原始量）；
> 指标数值在 Phase 4 DWS 聚合中落地后回填此处。

## 0. 通用约定（适用于所有指标）

- **时区**：一律 UTC。events 无显式时区，按 UTC 解读后窗口为
  2015-05-03 03:00:04 ~ 2015-09-18 02:59:47（139 个 UTC 日）。
- **行归属日**：`timestamp`（毫秒）→ UTC 日期。临界毫秒属于哪个日，
  用 `day = ts//86400000`（UTC 世界时）确定，不做本地时区换算。
- **visitorid 语义**：匿名 Cookie 标识，非注册用户。`visitorid="0"` 共 3 行
  视作哨兵值：**计入 count、不计入 distinct user**（口径在数据契约中钉死，
  不静默丢弃）。
- **dataset 事实速查**（全量扫描结果）：

| 量 | 值 |
| --- | --- |
| view 事件行数 | 2,664,312 |
| addtocart 事件行数 | 69,332 |
| transaction 事件行数 | 22,457 |
| 去重 visitorid（任意行为） | 1,407,580 |
| 其中仅 `view`（项目 UV 口径） | 1,404,178 |
| 加购 / 成交去重用户 | 37,722 / 11,719 |
| 去重 itemid（事件侧） | 235,061 |
| 去重 transactionid | 17,672 |
| events 覆盖 UTC 日数 | 139 |
| item_properties 行 / 去重商品 | 20,275,902 / 417,053 |

---

## 1. PV（页面浏览量 / Page View）

- **业务定义**：站点页面被浏览的次数，衡量流量体量。
- **分子**：`event = 'view'` 的事件行数。
- **分母**：无（绝对量指标）。
- **过滤条件**：`event = 'view'`；`timestamp` 在观察区间内（UTC 日分区）；
  不做 visitor 过滤（含未知 visitor 的 view 仍算一次浏览）。
- **去重口径**：**不去重**。同一 visitor 连点刷新计多次；同一毫秒多行计多次。
- **时区**：UTC。
- **边界情况**：无分母。需要决定是否把 `view` 同秒内的多行合并为 1（广告统计
  惯例），留到 ADS 按产品需求定，默认**不合并**（保持计量一致性与可校验性）。
- **实测锚点**：全量 `view` = 2,664,312 行。毫秒级时间戳重叠请区分两个数：
  2,756,101 行只落在 2,750,455 个不重复毫秒值上，冗余行 **5,646**
  （= 2,756,101 − 2,750,455）；实际与他人共享同一毫秒的行数为 **10,795**
  （分布在 5,149 个共享毫秒值上）。PV 不去重，直接计行数。

## 2. UV（独立访客数 / Unique Visitors）

- **业务定义**：**指定日期内发生过 `view` 行为的独立访客数**（项目基线口径，
  见主方案 §2.3：UV = 指定日期内发生 view 的去重 visitor_id 数）。
- **分子**：`event = 'view'` 的 distinct `visitorid`。
- **分母**：无。
- **过滤条件**：`event = 'view'`；`visitorid` 非空且 `≠ "0"`（哨兵值）；
  `timestamp` 在观察区间内。
- **去重口径**：按 visitorid 去重；跨日、跨月累计 UV 是同一个去重集合，**不等于
  DAU 求和**。
- **时区**：UTC。
- **边界情况**：匿名 Cookie 会被清理/更换，UV 存在系统性高估。全量区间
  view 口径去重访客 = **1,404,178**（3 行 `"0"` 哨兵全是 view 事件、只对应同一个
  值，按口径剔除该值后计数不变）；复现：`count(distinct visitorid) where
  event='view' and visitorid != '0'`。

## 2.1 全行为活跃访客数（任意行为去重访客，非 UV，勿与 §2 混用）

- **业务定义**：观察区间内发生过**任意事件（view / addtocart / transaction）**的
  独立访客数；与 UV 的区别是**不限定事件类型**。
- **分子**：区间内 distinct `visitorid`。
- **分母**：无。
- **过滤条件**：`visitorid` 非空且 `≠ "0"`（哨兵值）；`timestamp` 在区间内。
- **去重口径**：按 visitorid 去重；跨日累计不叠加（≠ DAU 求和）。
- **时区**：UTC。
- **边界情况**：匿名 Cookie 会被清理/更换，存在系统性高估。全量区间
  = 1,407,580（含哨兵值 `"0"` 1 个，去哨兵后 **1,407,579**）。本口径是「宽口径
  活跃访客」，必然 ≥ §2 UV（view 口径）。

## 3. DAU（日活跃用户数 / Daily Active Users）

- **业务定义**：某 UTC 日内发生过任何行为的独立访客数。
- **分子**：某日（UTC）`event` 任意类型、`visitorid` 非空且 ≠"0" 的 distinct
  visitorid。
- **分母**：无。
- **过滤条件**：`day(timestamp) = 观察日`。
- **去重口径**：日内按 visitorid 去重；跨日不累计、不串。
- **时区**：UTC，日界为世界时零点。
- **边界情况**：
  - **窗口不完整**：数据首日 2015-05-03 自 03:00 UTC 起、末日 2015-09-18 至
    02:59 UTC 止，均不是完整自然日。首末日 DAU 系统性偏小，**展示时剔除或加
    “部分日”标注**，纳入指标后的口径统一为 `[2015-05-04, 2015-09-17]` 全量日。
  - 极低活日 DAU=0 属正常，保留 0 不丢行。

## 4. 加购用户数（加购人数 / Add-to-Cart Users）

- **业务定义**：观察区间内至少发生过一次加购的独立访客数。
- **分子**：distinct `visitorid` where `event = 'addtocart'`。
- **分母**：无。
- **过滤条件**：`event = 'addtocart'`；区间过滤。
- **去重口径**：visitorid 级去重（一人多次加购只算 1 人）。
- **时区**：UTC。
- **边界情况**：无分母；若与“加购件数”区分，件数口径=行数不去重。
- **实测锚点**：全量 `addtocart` **事件行** = 69,332（占事件总量 2.52%，事件口径，
  **不是用户数**）；去重加购用户 = **37,722**（`distinct visitorid where
  event='addtocart'`，即本指标精确分子）。

## 5. 加购率（Add-to-Cart Rate）

- **业务定义**：访客从“看看”推进到“想买”的密度。**默认用户级口径 A**。
- **口径 A（用户级，默认）**
  - 分子：加购用户数（第 4 节）。
  - 分母：同期**浏览用户数**（= §2 UV，view 口径）。
- **口径 B（事件级，辅助）**
  - 分子：`addtocart` 事件行数。
  - 分母：`view` 事件行数（页面级点击密度）。
- **时区**：UTC；分子分母必须落在同一观察区间（同一 UTC 日或全区间）。
- **边界情况**：
  - **分母为 0**：空日（浏览用户数 / UV = 0）加购率未定义，输出空值而非 0 或除零错误；
    事件级 B 同理（无 view 但有 addtocart 的理论情形也要覆盖）。
  - **口径漂移风险**：A 是”人“口径、B 是”行为密度“口径，**不可混用**；
    报告中必须标注用的是哪个口径（默认 A）。
  - **非天然嵌套（实测）**：37,722 名加购用户中有 3,321 名从未产生过 `view`
    记录，故「加购用户 ⊆ 浏览用户」并不恒成立；分日报表若分子 > 分母，
    先校验事件来源再做告警，不要把用户级子集关系写死。
- **实测锚点**：全量区间 A ≈ 37,722 / 1,404,178（加购用户 / 浏览用户，均出自
  真实扫描去重），供参考；69,332 只是 `addtocart` **事件行数**，**不是** A 的分子。
  正式指标值仍待 DWS 复算。

## 6. 交易行为用户数（成交人数）

- **业务定义**：观察区间内至少发生一笔成交的独立访客数（**人头口径**，
  与“成交订单数”区分）。
- **分子**：distinct `visitorid` where `event = 'transaction'`。
- **分母**：无。
- **过滤条件**：`event = 'transaction'`；区间过滤。
- **去重口径**：visitorid 级去重。
- **时区**：UTC。
- **边界情况**：
  - 与“订单数”严格区分：订单数需按 `transactionid` 去重，且
    `transactionid` 是订单级标识 —— 22,457 行 transaction 事件只对应
    **17,672 个不同 transactionid**（2,710 个 ID 跨多行，单 ID 最多 31 行），
    说明一行 transaction ≠ 一单。订单数 = distinct(transactionid) 才正确。
  - 无交易日的指标值为空/0，保留。
- **实测锚点**：全量 `transaction` 事件行 22,457 对应 **11,719** 名去重用户（即本指标精确分子）。

## 7. 行为转化率（浏览→成交转化）

- **业务定义**：整体漏斗顶（浏览）到底（成交）的转化强度。
- **分子**：观察区间内 distinct `visitorid` with `event='transaction'`
  （即第 6 节“交易用户数”）。
- **分母**：同期**浏览用户数**（= §2 UV，view 口径；转化漏斗的上游）。
- **过滤条件**：区间过滤；不做“首次行为日”限制——D0 浏览、D1 成交的
  跨天转化计入分子，避免其消失于单日口径。
- **去重口径**：visitorid 级；分子分母是同一次去重——**分子必然 ≤ 分母**，
  任何分子 > 分母的异常值即本指标的数据质量问题告警条件。
- **时区**：UTC。
- **边界情况**：
  - 分母（浏览用户数 / UV）= 0：输出空。
  - “多日统一口径”与“分日口径”的关系：分日转化率分母若只取当日浏览用户数，
    会漏掉跨天转化者；Phase 4 同时产出“日转化率（分母=当日浏览用户数）”和
    “区间转化率（分母=区间浏览用户数）”两个版本，名称必须区分。
- **实测锚点**：`transaction` 事件行 22,457、去重交易用户 11,719、浏览用户
  （UV）1,404,178；转化率精确值待 DWS 复算。

## 8. 次日留存率（首次观测 cohort / Day-1 Retention）

- **业务定义**：以“首次在数据中出现行为的那一天”为 cohort 划分；统计该
  cohort 在次日（D+1 UTC 自然日）再次发生任意行为的比例。
- **分子**：`{visitors first seen on D0}` ∩ `{visitors with any event on D0+1}`
  （交集大小）。
- **分母**：`{visitors first seen on D0}` 大小（cohort 规模）。
- **过滤条件**：
  - cohort 定义基于**首次观测日** = `min(day(timestamp))` per visitorid；
  - D+1 行为不限事件类型（view/addtocart/transaction 任意）。
- **去重口径**：visitorid 级；分子分母同一次去重，分子 ≤ 分母应恒成立。
- **时区**：UTC。
- **边界情况**：
  - **分母为 0**（D0 无新访客）：留存率未定义 → 输出空，不除零。
  - **窗口不完整**：D0 取数据首尾日会污染——首日前的用户把“老客”误判成
    新人（抬高分母）；D0+1 落在 2015-09-18 之后（即 D0 = 09-17/09-18）的
    cohort 次日无观测，分母有效但分子缺失 → 标注“窗口不完整”，**不放 0**。
    建议最小有效 cohort 区间为 D0 ∈ [05-04, 09-16]。
  - 匿名 Cookie 重置会制造“伪新客”，系统性抬高分子分母，报告中注明局限。
- **落地说明**：首个 aggregation 键是 `(first_seen_day, return_day)` 的计数表，
  保留足够信息供 ADS 定义 7 日/30 日留存扩展，本阶段只钉死次日口径。

---

## 9. 口径签名（供 Phase 4 落地）

每个指标在 DWS 落地时附一行“口径签名”，形如：

```
metric = DAU
 grain   = daily(UTC)
 scope   = events.csv, event in {view, addtocart, transaction}
 dedup   = visitorid
 filter  = visitorid != "0" and timestamp in [window]
 tz      = UTC
```

```
metric = UV
 grain   = interval / daily(UTC)
 scope   = events.csv, event = 'view'
 dedup   = visitorid
 filter  = event = 'view' and visitorid != "0" and timestamp in [window]
 tz      = UTC
```

```
metric = 全行为活跃访客数
 grain   = interval
 scope   = events.csv, event in {view, addtocart, transaction}
 dedup   = visitorid
 filter  = visitorid != "0" and timestamp in [window]
 tz      = UTC
```

```
metric = 加购率(口径A)
 grain   = interval / daily(UTC)
 scope   = events.csv
 dedup   = visitorid（分子/分母各自按 §4 / §2 口径去重，不天然嵌套）
 filter  = 加购用户数(37,722) ÷ 浏览用户数(UV=1,404,178)，同区间/同日
 tz      = UTC
```

公式、过滤、去重三者并列保存，确保 ADS 层渲染报表时能回溯到定义，避免
“同一张报表两个口径”事故。
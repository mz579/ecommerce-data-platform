# 事件数据契约（Phase 1 · DWD 采用版）

> 契约定义 **原始 CSV → 标准事件** 的映射与异常处置政策，供 Phase 2（ODS 入库）
> 与 Phase 3（DWD 标准化）采用。核心原则：
> **任何一行原始记录都不得被静默丢弃**——凡无法映射为标准事件的行，必须带元数据
> 进入异常通道（`dwd_events_reject`）并保留原行、原 `source_row_number`，可审计、
> 可重放。
>
> **事件类型命名以 DWD 标准枚举 `{view, add_to_cart, transaction}` 为准（主方案
> §2.2）**：ODS 贴源层原样保留原始 `addtocart` 写法；DWD 在 `event_type` 列落标准名
> `add_to_cart`，并另列 `raw_event_type` 保真原始字符串。指标口径（
> `docs/metric_definitions.md`）一律引用 DWD 标准名。
>
> 观测数字均来自实际全量扫描（`docs/data_dictionary.md`）与 Phase 3 DWD 全量构建
> 实测（`docs/dwd_design.md`）。

## 1. 标准事件 Schema（DWD 明细事实目标，`dwd_events`）

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `event_sk` | BIGINT UNSIGNED | 是 | DWD 代理主键（重建后值可变化；血缘由 `row_key` 跨运行保证） |
| `row_key` | BINARY(32) | 是 | ODS 确定性代理键 = SHA-256(source_file \x1F source_row_number)，跨运行不变 |
| `dedup_key` | CHAR(64) | 是 | 业务键 SHA-256 = SHA-256(`event_time_utc, visitor_id, event_type, item_id, COALESCE(transaction_id, 0)`)，DWD 去重依据（见 §3.5） |
| `visitor_id` | BIGINT | 否* | 访客 ID；原始 `"0"` 哨兵 → NULL + `visitor_zero` flag |
| `item_id` | BIGINT | 是 | 商品 ID（缺失走异常通道） |
| `raw_event_type` | VARCHAR(32) | 是 | ODS 原始事件字符串（`view`/`addtocart`/`transaction`），保真 |
| `event_type` | VARCHAR(32) | 是 | **DWD 标准枚举 `{view, add_to_cart, transaction}`**（映射见 §2） |
| `event_time_utc` | DATETIME(3) | 是 | 事件时刻：原始毫秒 → UTC（保留毫秒位） |
| `event_date_utc` | DATE | 是 | 事件 UTC 日（分区键，`event_time_utc` 派生） |
| `transaction_id` | BIGINT | 否 | 订单级标识；仅 `transaction` 事件非空 |
| `quality_flags` | VARCHAR(128) | 是 | 逗号分隔异常 flag；正常为 `"ok"`。Phase 3 入标准行的唯一 flag = `visitor_zero` |
| `build_id` | BIGINT UNSIGNED | 是 | 产生本行的 `dwd_build_log.build_id` |
| `ingest_batch_id` | BIGINT UNSIGNED | 是 | 来源 ODS 行所属 `etl_batch_log.batch_id` |
| `source_file` | VARCHAR(255) | 是 | 来源文件名，如 `events.csv` |
| `source_row_number` | BIGINT UNSIGNED | 是 | CSV 物理行号（表头=1），`sed -n` 可回查 |
| `loaded_at` | DATETIME(3) | 是 | DWD 写入时刻（UTC） |

\* `visitor_id` 语义上必填（行为事件需要主体），但存在哨兵 `"0"` 与缺失两种退化形态：
   `"0"` 入标准行置 NULL + `visitor_zero` flag（不直接把哨兵当正常用户）；缺失直接进
   异常通道 `missing_visitor`（无法归因主体的行为不伪造维度）。

## 2. 原始事件 → 标准事件映射

来源：`events.csv`，表头 `timestamp,visitorid,event,itemid,transactionid`。映射为
**纯函数**（`src/ecommerce_dw/dwd/transform.py`），无全局状态，行级可测。

| 原始列 | 目标字段 | 转换规则 |
| --- | --- | --- |
| `timestamp` | `event_time_utc` / `event_date_utc` | 非空、纯整型、>0 的毫秒串 → BIGINT → UTC DATETIME(3) / DATE；否则（空/非整型/≤0/超出 DATETIME 范围）整行进异常通道 `invalid_timestamp` |
| `visitorid` | `visitor_id` | 非空 → BIGINT（越界拒绝）；`"0"` → NULL + `visitor_zero` flag 入标准行；空 → 异常通道 `missing_visitor`；非整型/越界 → `invalid_visitor_id` |
| `event` | `event_type` / `raw_event_type` | 命中原始白名单 `{view, addtocart, transaction}` → 按 §2.2 映射为标准枚举，`raw_event_type` 保真；未命中 → 异常通道 `unknown_event_type`（reject 行 `event_type` 列记 `unknown`，`raw_event` 保真原值） |
| `itemid` | `item_id` | 非空 → BIGINT（越界拒绝）；空 → 异常通道 `missing_item`；非整型/越界 → `invalid_item_id` |
| `transactionid` | `transaction_id` | `event=transaction` 且非空 → BIGINT（越界拒绝）；`event=transaction` 且空 → `missing_txn_on_transaction`；`event≠transaction` 且非空 → `unexpected_txnid`；非整型/越界 → `invalid_transaction_id`。**类型转换失败一律拒绝，绝不默认填充 0** |
| 整行 | `row_key`, `source_file`, `source_row_number` | DWD 构建时由 ODS 血缘字段派生；`quality_flags` 汇总行级 flag |

### 2.2 事件类型标准映射（主方案 §2.2）

| 原始 `event`（raw_event_type） | DWD 标准 `event_type` |
| --- | --- |
| `view` | `view` |
| `addtocart` | `add_to_cart` |
| `transaction` | `transaction` |
| 其他任何值 | 拒绝 → 异常通道（`unknown_event_type`） |

> 注意：本契约早前版本把白名单写成 `{view, addtocart, transaction}` 是沿用 ODS
> 原始命名。**DWD 一律使用标准名 `add_to_cart`**；ODS 原始值永远由 `raw_event_type`
> 列保真，不丢失。

## 3. 异常记录策略

三类异常一律进入 `dwd_events_reject`（reject_reason 枚举，携带 raw 字段与血缘），
**均不静默丢弃、不得用降级值填充**。

### 3.1 未知事件类型

- **检测**：`event` 不在原始白名单 `{view, addtocart, transaction}`。
- **实测**：全量 `events.csv` 中未知类型 **0 行**（无异常），但测试须保留该分支。
- **处置**：整行进异常通道，`event_type` 列记 `'unknown'`，`raw_event` 保真原值；
  不进入标准事件表，不给 metric 层使用。
- **不删除不计**：保留原行与 `source_row_number`，可随时重放核查。

### 3.2 非法 / 缺失时间戳

- **检测**：`timestamp` 为空、非纯整型、≤ 0、或转换超出 DATETIME 范围。
- **实测**：`events.csv` 缺失与非法时间戳均为 **0 行**；`item_properties` 同样为 0。
- **处置**：整行写入 `dwd_events_reject`，`reject_reason=invalid_timestamp`，并携带
  `source_file/source_row_number` 与原始行快照；**不得丢弃**，也不得用降级值填充。
- **时区语义**：非法时间戳行没有 UTC 日（`event_date_utc=NULL`），任何按日重建的
  日期范围都必须同时把它们捞起送审（见 `docs/dwd_design.md` §日期重建）。

### 3.3 缺失关键字段（visitorid / itemid）

- **检测**：列为空或非整型。
- **实测**：`visitorid` 缺失 **0 行**，`"0"` 哨兵 **3 行**；`itemid` 缺失 **0 行**。
- **处置**：
  - `visitorid` 为空 → **异常通道 `missing_visitor`**（无行为主体，不能当"未知访客"
    聚合）。
  - `visitorid="0"` → 标准行，`visitor_id=NULL` + `visitor_zero` flag（进计数、
    不进去重用户数）。
  - `itemid` 为空 → **异常通道 `missing_item`**（浏览行为无商品主体，不满足
    事实表粒度）。
  - `visitorid`/`itemid`/`transactionid` 非整型或超出 BIGINT → `invalid_visitor_id` /
    `invalid_item_id` / `invalid_transaction_id`。
- **原则**：凡不可归因于主体（visitor/item）的关键洞，宁可走异常通道也不伪造维度。

### 3.4 transactionid 语义与错配

- **契约（正向规则）**：
  - `event=transaction` 且 `transactionid` 非空 → 正常；
  - `event in {view, addtocart}` 且 `transactionid` 为空 → 正常；
- **负向异常（一律拒绝）**：
  - `event=transaction` 但 `transactionid` 为空 → `missing_txn_on_transaction`；
  - `event != transaction` 但 `transactionid` 非空 → `unexpected_txnid`。
- **实测**：两类负向异常当前均 **0 行**；22,457 行 transaction 事件 100% 带
  transactionid，非 transaction 行 100% 为空。
- **重要事实**：`transactionid` 是**订单级标识**，非行级唯一——22,457 行
  transaction 事件只对应 **17,672 个不同 transactionid**（2,710 个 ID 跨多行）。
  契约明确：`transaction_id` 列承载订单 ID，行数 ≠ 订单数，任何按订单计数的指标
  必须以 `distinct(transaction_id)` 实现（见 `docs/metric_definitions.md` §6）；
  DWD 业务键把 COALESCE(transaction_id, 0) 纳入分子，同一订单多行不互相去重。

### 3.5 完全重复行（业务键去重）

- **检测**：同一来源文件中业务键相同 —— 业务键 =
  `(event_time_utc, visitor_id, event_type, item_id, COALESCE(transaction_id, 0))`
  （SHA-256 落 `dedup_key` 列）。
- **实测**：`events.csv` 有 **460 行**重复（458 个不同 dedup_key 出现过 2+ 次），
  占 2,756,101 行的 0.017%；`item_properties` 全量 **0 重复**。
- **处置（Phase 3 最终口径）**：ODS 阶段**原样入库不过滤**（append-only）；
  DWD 构建时按业务键精确去重 —— 每个 dedup_key 保留 `source_row_number` 最小的一行
  进 `dwd_events`，其余进 **`dwd_events_duplicate`**（保留原 row_key、原始 5 字段、
  转换后字段与 dedup_key，可与保留行关联审计）。**重复行绝不丢弃**。行数恒等式：
  `ODS 行数 = dwd_events 行数 + dwd_events_duplicate 行数 + dwd_events_reject 行数`。
  PV/加购率等指标在 DWD 之上统计时天然使用去重后的标准事件。

## 4. 标准事件的派生与血缘

- `event_date_utc` 由 `event_time_utc` 派生并作为分区键；时区固定 UTC。
- `row_key`（ODS 生成）与 `source_row_number` 保证任意标准事件可 `grep` 回原始
  CSV 原文，满足可审计要求。
- 异常通道记录同样带 `source_file/source_row_number` 与 `reject_reason`，与标准
  通道按批次/build 关联，支持重放与 `cleanup_build` 清理。

## 5. 遵守性检查（供给 CI / 数据质量任务）

| 检查 | 期望 | 说明 |
| --- | --- | --- |
| 标准行中未知事件占比 | 0% | `unknown_event_type` 全部在异常通道 |
| 非法时间戳行 | 0 | 全部有合法毫秒时间戳（异常通道计数 0） |
| `missing_txn_on_transaction` | 0 | 交易事件必须带 transactionid |
| `unexpected_txnid` | 0 | 非交易事件不得带 transactionid |
| 行数恒等式 | 必须成立 | `ods_events_raw = dwd_events + dwd_events_duplicate + dwd_events_reject`（每次构建自动校验） |
| 完全重复行 | 全量 460 行进 `dwd_events_duplicate` | 业务键去重见 §3.5；`item_properties` 为 0 |
| visitor 为 `"0"` 行 | 3 行 | 标准行 + `visitor_zero` flag，进 count 不进 distinct user |
| 事件类型映射 | `addtocart → add_to_cart` | DWD 标准枚举 §2.2；`raw_event_type` 保真 |

> 检查项在 Phase 3 中实现为 SQL 恒等式校验与质量断言；
> 本契约钉死规则、阈值与命名口径。
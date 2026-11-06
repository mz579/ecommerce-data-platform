# 事件数据契约（Phase 1）

> 契约定义 **原始 CSV → 标准事件** 的映射与异常处置政策，供 Phase 2（ODS 入库）
> 与 Phase 3（DWD 标准化）直接采用。核心原则：
> **任何一行原始记录都不得被静默丢弃**——凡无法映射为标准事件的行，必须带元数据
> 进入错误通道（error 分区/错误表）并保留原行、原序列号，可审计、可重放。
>
> 观测数字均来自实际全量扫描（见 `docs/data_dictionary.md`）。

## 1. 标准事件 Schema（DWD 明细事实目标）

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `event_id` | BIGINT | 是 | 代理主键（行级），ODS/DWD 生成，非原始字段 |
| `event_ts_ms` | BIGINT | 是 | 原始 `timestamp`（Unix 毫秒），校验后原样进入 |
| `event_date` | CHAR(10) | 是 | `event_ts_ms` → UTC 日期 `YYYY-MM-DD`（派生、分区键） |
| `event_type` | VARCHAR(16) | 是 | 白名单 `{view, addtocart, transaction}` |
| `visitor_id` | VARCHAR(32) | 否* | 匿名访客标识；为空/`"0"` 时置 NULL 并打 flag（见下） |
| `item_id` | VARCHAR(32) | 是 | 商品标识；缺失按异常处理 |
| `transaction_id` | VARCHAR(32) | 否 | 仅 `transaction` 事件应非空；其余事件必须为空 |
| `unknown_event_raw` | VARCHAR(64) | 否 | 出现未知事件类型时的原始值（供回溯），否则 NULL |
| `quality_flags` | VARCHAR(128) | 是 | 逗号分隔 flag 集合（见 3.3），正常为 `"ok"` |
| `source_file` | VARCHAR(64) | 是 | 来源文件名，如 `events.csv` |
| `source_row` | BIGINT | 是 | CSV 中该行的行号（含表头偏移），保证血缘可溯 |

\* `visitor_id` 语义上必填（行为事件需要主体），但存在哨兵 `"0"` 与缺失两种
  退化形态，故设计为可空 + flag 显式记录，不直接把哨兵当正常用户。

## 2. 原始事件 → 标准事件映射

来源：`events.csv`，表头 `timestamp,visitorid,event,itemid,transactionid`。

| 原始列 | 目标字段 | 转换规则 |
| --- | --- | --- |
| `timestamp` | `event_ts_ms` / `event_date` | 原样；`int(ts)` 解析成功且 > 0 → 保留毫秒值；由 `datetime.fromtimestamp(ms/1000, utc)` 派生 UTC 日 |
| `visitorid` | `visitor_id` | 非空 → 原样；`"0"` → NULL + `visitor_zero`；空 → NULL + `missing_visitor` |
| `event` | `event_type` / `unknown_event_raw` | 命中白名单 → `event_type`；未命中 → `event_type='unknown'` + `unknown_event_raw=event` |
| `itemid` | `item_id` | 非空 → 原样；空 → NULL + 异常（见 3.2） |
| `transactionid` | `transaction_id` | `event=transaction` 且非空 → 原样；其余场景按 3.4 规则处置 |
| 整行 | `event_id`, `source_file`, `source_row` | 02 阶段生成；行级完全重复行额外打 `dup` flag |
| — | `quality_flags` | 由上述 flag 汇总，任一异常 flag 存在则不为 `"ok"` |

## 3. 异常记录策略

三类异常分别处置，均**不静默丢弃**：

### 3.1 未知事件类型

- **检测**：`event` 不在 `{view, addtocart, transaction}`。
- **实测**：全量 `events.csv` 中未知类型 **0 行**（无异常），但测试须保留该分支。
- **处置**：`event_type='unknown'`，原值写入 `unknown_event_raw`；
  进入标准事件表但**带强 flag**，供质量任务监控，不给 metric 层使用。
- **不删除不计**：保留原行与 `source_row`，可随时重放核查。

### 3.2 非法 / 缺失时间戳

- **检测**：`timestamp` 为空、非整型、或 ≤ 0。
- **实测**：`events.csv` 缺失与非法时间戳均为 **0 行**；`item_properties` 同样为 0。
- **处置**：整行写入错误通道（error），携带 `error_code=invalid_timestamp`、
  `source_row`、原始行快照；**不得丢弃**，也不得用降级值（如前后行）填充。

### 3.3 缺失关键字段（visitorid / itemid）

- **检测**：列为空或哨兵。
- **实测**：`visitorid` 缺失 **0 行**，`"0"` 哨兵 **3 行**；`itemid` 缺失 **0 行**。
- **处置**：
  - `visitorid` 为空 → 标准行 + `missing_visitor` flag（可做"未知访客"聚合，不可进 UV/DAU）。
  - `visitorid="0"` → NULL + `visitor_zero` flag（进入标准行，进计数不进去重用户数）。
  - `itemid` 为空 → 有 event 无商品：整行**进错误通道**（浏览行为无商品主体，
    不满足事实表粒度），`error_code=missing_item`。
- **原则**：凡不可归因于主体（visitor/item）的关键洞，宁可走错误通道也不伪造维度。

### 3.4 transactionid 语义与错配

- **契约（正向规则）**：
  - `event=transaction` 且 `transactionid` 非空 → 正常；
  - `event in {view, addtocart}` 且 `transactionid` 为空 → 正常；
- **负向异常**：
  - `event=transaction` 但 `transactionid` 为空 → 强 flag `missing_txn_on_transaction`
    + 错误通道快照（此类行 ODS 照收但 DWD 不得作为成交网元）；
  - `event != transaction` 但 `transactionid` 非空 → flag `unexpected_txnid`
    （数据矛盾，保留原值，交质量告警）。
- **实测**：两类负向异常当前均 **0 行**；22,457 行 transaction 事件 100% 带
  transactionid，非 transaction 行 100% 为空。
- **重要事实**：`transactionid` 是**订单级标识**，非行级唯一——22,457 行
  transaction 事件只对应 **17,672 个不同 transactionid**（2,710 个 ID 跨多行）。
  契约明确：`transaction_id` 列承载订单 ID，行数 ≠ 订单数，任何按订单计数
  的指标必须以 `distinct(transactionid)` 实现（见 `docs/metric_definitions.md` §6）。

### 3.5 完全重复行

- **检测**：同一来源文件中 5 个原始字段完全相同。
- **实测**：`events.csv` 有 **460 行**重复（458 个不同重复行，即 458 个行有过
  重复出现），占 2,756,101 行的 0.017%；`item_properties` 全量 **0 重复**。
- **处置**：ODS 阶段**原样入库不过滤**（append-only，保留物理完整性）；
  同一逻辑记录出现多次由 `quality_flags` 打 `dup`、DWD 按业务键
  `(event_ts_ms, visitor_id, event_type, item_id, COALESCE(transaction_id,''))`
  去重聚合。是否去重推迟到 DWD（影响 PV/加购率等分子）为一个**待决口径**（见报告）。

## 4. 标准事件的派生与血缘

- `event_date` 由 `event_ts_ms` 在入 ODS 时派生并作为分区键；时区固定 UTC。
- `source_row` 保证任意标准事件可 `grep` 回原始 CSV 原文，满足可审计要求。
- 错误通道记录同样带 `source_file/source_row` 与 `error_code`，与标准通道
  按批次号关联，支持重放。

## 5. 遵守性检查（供给 CI / 数据质量任务）

| 检查 | 期望 | 说明 |
| --- | --- | --- |
| 标准行中未知事件占比 | 0% | `event_type='unknown'` 数量为 0 |
| 非法时间戳行 | 0 | 全部有合法毫秒时间戳 |
| `missing_txn_on_transaction` | 0 | 交易事件必须带 transactionid |
| `unexpected_txnid` | 0 | 非交易事件不得带 transactionid |
| 完全重复行占比 | 当前 ≈0.017% | ODS 原样保留并打 `dup` flag，DWD 去重（见 §3.5） |
| visitor 为 `"0"` 行 | 当前 3 行 | 进 count 不进 distinct user，指标文档已钉 |

> 检查项在 Phase 3+（DWD + 质量任务）中实现为 SQL 断言或 python quality 任务；
> 本阶段只约定规则与阈值。
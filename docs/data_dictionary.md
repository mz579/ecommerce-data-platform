# 数据字典（V1.0）

> 覆盖生产库 `ecommerce_dw` 的全部 ODS / DWD / DWS / ADS 表、控制/审计表，以及
> Superset 只读受控视图。列类型以 `sql/ddl/*.sql` 为准；行数为 Phase 8 验收时
> 对生产库的实测基线（`scripts/demo_phase8.py` 逐表校验）。
>
> **通用约定**
> - 全链路 **UTC**；`event_date_utc` 由 `event_time_utc` 派生，日界为世界时零点。
> - MySQL 表**没有物理分区**；“分区/日期列”指逻辑上按日期裁剪/重建的列。
> - 统计/构建日志类表按“每次运行一行”增长，不设固定行数基线；下文给出当前可核对的记录。
> - 原始 CSV 字段在 ODS 层一律字符串保真；类型化只发生在 DWD 及之后。
> - 数据集窗口：全部数据 139 个 UTC 日（2015-05-03 ~ 2015-09-18）；完整窗口
>   `[2015-05-04, 2015-09-17]`（137 个完整自然日），首末日为部分日。

## 1. 表清单与行数基线

| 层 | 表 | 粒度 | 行数基线 |
| --- | --- | --- | ---: |
| ODS | `ods_events_raw` | 原始 CSV 一行 | 2,756,101 |
| ODS | `ods_item_properties_raw` | 原始 CSV 一行（属性快照） | 20,275,902 |
| ODS | `ods_category_tree_raw` | 原始 CSV 一行（类目节点） | 1,669 |
| ODS 审计 | `etl_batch_log` | 一次文件装载批次 | 4 个 LOADED 批次（#15/#17/#18/#28）+ 历史 CLEANED #16 |
| DWD | `dwd_events` | 一个去重后的标准事件 | 2,755,641 |
| DWD | `dwd_events_duplicate` | 一条被业务键去重掉的原始行 | 460 |
| DWD | `dwd_events_reject` | 一条无法转换的原始行 | 0 |
| DWD 内部 | `dwd_events_stage` | 一条 ODS 行的转换中间态 | 2,756,101 |
| DWD 审计 | `dwd_build_log` | 一次 DWD 构建 | 每次构建一行；Phase 8 最新 build #55（DATES 对照） |
| DWS | `dws_user_daily` | 一个访客一个 UTC 日 | 1,649,534 |
| DWS | `dws_item_daily` | 一个商品一个 UTC 日 | 1,672,186 |
| DWS | `dws_category_daily` | 一个类目一个 UTC 日 | 94,194 |
| ADS | `ads_traffic_daily` | 一个 UTC 日 | 139 |
| ADS | `ads_funnel_daily` | 一个 UTC 日 × 一个口径名 | 1,251（139 × 9） |
| ADS | `ads_item_ranking_daily` | 一个 UTC 日 × 一个上榜商品 | 13,900（139 × Top-100） |
| ADS | `ads_retention_cohort` | 一个 cohort 日 × 一个年龄日 | 278（139 × 2） |
| ADS | `ads_funnel_interval` | 一个完整窗口（单行） | 1 |
| ADS | `ads_item_behavior_daily` | 一个 UTC 日 | 139 |
| 控制 | `etl_watermark` | 一条流水线 × 一个业务日期 | 139 |
| 控制 | `quality_check_run` | 一次质量门禁运行 | 每次运行一行；最新 run #7（PASS 60/60） |
| 控制 | `quality_check_result` | 一次运行 × 一条规则 | 每轮 60 行 |
| 控制 | `ads_publish_log` | 一次 ADS 发布 | 12（只增不删） |
| 控制 | `airflow_pipeline_run` | 一次 DAG 运行 | 每次运行一行 |
| 审计 | `dws_ads_build_log` | 一次 DWS/ADS 聚合构建 | 每次构建一行；最新 FULL build #8 |

## 2. ODS 贴源层

### 2.1 `ods_events_raw`

| 项 | 值 |
| --- | --- |
| 粒度 | `events.csv` 原始数据行（一行 = 一条原始事件） |
| 唯一键 | `PRIMARY KEY (row_key)`；`UNIQUE (source_file, source_row_number)` |
| 逻辑分区/日期列 | 无物理分区；原始 `timestamp` 为 Unix 毫秒字符串，DWD 才派生 `event_date_utc` |
| 行数基线 | 2,756,101 |
| 上游 | `data/raw/retailrocket/events.csv`（SHA-256 见 `docs/dataset.md`） |
| 下游 | `dwd_events` / `dwd_events_duplicate` / `dwd_events_reject` |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `row_key` | BINARY(32) | 否 | 确定性代理键 = SHA-256(`source_file` + U+001F + `source_row_number`)，跨批次/运行不变 |
| `timestamp` | VARCHAR(64) | 否 | 原始事件时间戳（Unix 毫秒），字符串保真 |
| `visitorid` | VARCHAR(64) | 否 | 原始访客 ID，字符串保真（含哨兵 `0`） |
| `event` | VARCHAR(32) | 否 | 原始事件类型：`view` / `addtocart` / `transaction` |
| `itemid` | VARCHAR(64) | 否 | 原始商品 ID，字符串保真 |
| `transactionid` | VARCHAR(64) | 否 | 原始交易 ID，字符串保真（非交易事件为空串） |
| `source_file` | VARCHAR(255) | 否 | 来源文件名（basename） |
| `source_row_number` | BIGINT UNSIGNED | 否 | CSV 物理行号（表头 = 1，首条数据 = 2） |
| `ingest_batch_id` | BIGINT UNSIGNED | 否 | `etl_batch_log.batch_id`，逻辑外键 |
| `loaded_at` | DATETIME(3) | 否 | 本批次写入时刻（UTC） |

### 2.2 `ods_item_properties_raw`

| 项 | 值 |
| --- | --- |
| 粒度 | 一条商品属性快照（`item_properties_part1/part2.csv` 原始行） |
| 唯一键 | `PRIMARY KEY (row_key)`；`UNIQUE (source_file, source_row_number)` |
| 逻辑分区/日期列 | 无物理分区；原始 `timestamp` 为快照时刻（Unix 毫秒，观察值固定在 03:00:00 UTC） |
| 行数基线 | 20,275,902（part1 10,999,999 + part2 9,275,903） |
| 上游 | `item_properties_part1.csv`、`item_properties_part2.csv` |
| 下游 | `dws_category_daily` 的 `categoryid` as-of 快照；质量控制/EDA |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `row_key` | BINARY(32) | 否 | 确定性代理键（同上） |
| `timestamp` | VARCHAR(64) | 否 | 属性快照时间（Unix 毫秒），字符串保真 |
| `itemid` | VARCHAR(64) | 否 | 商品 ID，字符串保真 |
| `property` | VARCHAR(128) | 否 | 属性名（1,104 种自由文本，多为数字编码） |
| `value` | TEXT | 否 | 属性值，长度不定，字符串保真 |
| `source_file` | VARCHAR(255) | 否 | 来源文件名（part1 / part2） |
| `source_row_number` | BIGINT UNSIGNED | 否 | CSV 物理行号（表头 = 1） |
| `ingest_batch_id` | BIGINT UNSIGNED | 否 | `etl_batch_log.batch_id` |
| `loaded_at` | DATETIME(3) | 否 | 本批次写入时刻（UTC） |

### 2.3 `ods_category_tree_raw`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个类目树节点（`category_tree.csv` 一行） |
| 唯一键 | `PRIMARY KEY (row_key)`；`UNIQUE (source_file, source_row_number)` |
| 逻辑分区/日期列 | 无 |
| 行数基线 | 1,669 |
| 上游 | `category_tree.csv` |
| 下游 | 类目维度参考（当前 DWS 类目归属直接取 `item_properties` 的 `categoryid`，不强制 join 本表） |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `row_key` | BINARY(32) | 否 | 确定性代理键 |
| `categoryid` | VARCHAR(64) | 否 | 类目 ID，字符串保真 |
| `parentid` | VARCHAR(64) | 否 | 父类目 ID；根节点为空串 |
| `source_file` | VARCHAR(255) | 否 | 来源文件名 |
| `source_row_number` | BIGINT UNSIGNED | 否 | CSV 物理行号（表头 = 1） |
| `ingest_batch_id` | BIGINT UNSIGNED | 否 | `etl_batch_log.batch_id` |
| `loaded_at` | DATETIME(3) | 否 | 本批次写入时刻（UTC） |

### 2.4 `etl_batch_log`（ODS 批次审计）

| 项 | 值 |
| --- | --- |
| 粒度 | 一次源文件装载批次 |
| 唯一键 | `PRIMARY KEY (batch_id)`；重复批次检测键 `(source_file, file_sha256)` |
| 逻辑分区/日期列 | `started_at` / `finished_at`（UTC） |
| 行数基线 | 4 个 LOADED 批次：#15 `category_tree`、#17 `events`、#18 part1、#28 part2；历史 PENDING #16 已 CLEANED |
| 上游 | ODS 装载器 |
| 下游 | `validate_ods`、质量规则 `ods_batch_log_rowcount` / `ods_batch_coverage` |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `batch_id` | BIGINT UNSIGNED | 否 | 自增批次号 |
| `source_file` | VARCHAR(255) | 否 | 来源文件名 |
| `file_sha256` | CHAR(64) | 否 | 源文件整体 SHA-256（重复批次检测） |
| `table_name` | VARCHAR(64) | 否 | 目标 ODS 表 |
| `status` | VARCHAR(16) | 否 | `PENDING` / `LOADED` / `FAILED` / `CLEANED` |
| `row_count` | BIGINT UNSIGNED | 否 | 本批次实际提交行数（失败时为已提交部分） |
| `started_at` | DATETIME(3) | 否 | 开始时刻（UTC） |
| `finished_at` | DATETIME(3) | 是 | 结束时刻（UTC） |
| `error_summary` | TEXT | 是 | 失败原因（不含凭据） |
| `created_at` | DATETIME(3) | 否 | 登记时刻（UTC） |

## 3. DWD 明细标准化层

### 3.1 `dwd_events`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个去重后的标准行为事件 |
| 唯一键 | `PRIMARY KEY (event_sk)`；`UNIQUE (row_key)`、`UNIQUE (dedup_key)`、`UNIQUE (source_file, source_row_number)` |
| 逻辑分区/日期列 | `event_date_utc`（按日重建 scope） |
| 行数基线 | 2,755,641 |
| 上游 | `ods_events_raw` |
| 下游 | `dws_user_daily`、`dws_item_daily`、`dws_category_daily`、全部 ADS 表、质量门禁、`v_ads_item_orders_daily` |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_sk` | BIGINT UNSIGNED | 否 | 自增代理主键；重建后值可变，跨运行血缘由 `row_key` 保证 |
| `row_key` | BINARY(32) | 否 | ODS 确定性代理键（来源行血缘） |
| `dedup_key` | CHAR(64) | 否 | 业务键 SHA-256 = SHA-256(`event_time_ms`, `visitor_id`, `event_type`, `item_id`, `COALESCE(transaction_id,0)`) |
| `visitor_id` | BIGINT | 是 | 访客 ID；原始 `"0"` → NULL + `visitor_zero` 标记 |
| `item_id` | BIGINT | 否 | 商品 ID |
| `raw_event_type` | VARCHAR(32) | 否 | ODS 原始事件类型（`view` / `addtocart` / `transaction`） |
| `event_type` | VARCHAR(32) | 否 | DWD 标准枚举：`view` / `add_to_cart` / `transaction` |
| `event_time_utc` | DATETIME(3) | 否 | 事件时刻（毫秒 → UTC，保留毫秒） |
| `event_date_utc` | DATE | 否 | 事件 UTC 日（逻辑分区键） |
| `transaction_id` | BIGINT | 是 | 订单级标识；仅交易事件非空，非行级唯一 |
| `quality_flags` | VARCHAR(128) | 否 | 逗号分隔异常 flag；无异常为 `ok` |
| `build_id` | BIGINT UNSIGNED | 否 | `dwd_build_log.build_id` |
| `ingest_batch_id` | BIGINT UNSIGNED | 否 | 来源 ODS 行所属 `etl_batch_log.batch_id` |
| `source_file` | VARCHAR(255) | 否 | 来源文件名 |
| `source_row_number` | BIGINT UNSIGNED | 否 | 来源 CSV 物理行号 |
| `loaded_at` | DATETIME(3) | 否 | DWD 写入时刻（UTC） |

> 全量事件分布：`view` 2,664,218 / `add_to_cart` 68,966 / `transaction` 22,457。
> 精确重复 460 行（458 个 `dedup_key`）在 `dwd_events_duplicate`；拒绝 0 行。

### 3.2 `dwd_events_duplicate`

| 项 | 值 |
| --- | --- |
| 粒度 | 一条按业务键被去重掉的原始行 |
| 唯一键 | `PRIMARY KEY (row_key)`；`UNIQUE (source_file, source_row_number)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 460 |
| 上游 | `ods_events_raw` |
| 下游 | 审计/重放；需要“未去重口径”时可按 `dedup_key` 与 `dwd_events` 关联补回 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `row_key` | BINARY(32) | 否 | 被去掉原始行的 ODS 代理键 |
| `dedup_key` | CHAR(64) | 否 | 业务键 SHA-256；保留行在 `dwd_events` |
| `raw_timestamp` | VARCHAR(64) | 否 | 原始时间戳（字符串保真） |
| `raw_visitorid` | VARCHAR(64) | 否 | 原始访客 ID（含哨兵 `0`） |
| `raw_event` | VARCHAR(32) | 否 | 原始事件类型 |
| `raw_itemid` | VARCHAR(64) | 否 | 原始商品 ID |
| `raw_transactionid` | VARCHAR(64) | 否 | 原始交易 ID（非交易为空串） |
| `visitor_id` | BIGINT | 是 | 转换后访客 ID（原始 `0` → NULL） |
| `item_id` | BIGINT | 否 | 转换后商品 ID |
| `raw_event_type` | VARCHAR(32) | 否 | ODS 原始事件类型 |
| `event_type` | VARCHAR(32) | 否 | DWD 标准事件类型 |
| `event_time_utc` | DATETIME(3) | 否 | 事件时刻（UTC） |
| `event_date_utc` | DATE | 否 | 事件 UTC 日 |
| `transaction_id` | BIGINT | 是 | 订单级标识 |
| `quality_flags` | VARCHAR(128) | 否 | 与 `dwd_events` 相同的转换 flag |
| `build_id` | BIGINT UNSIGNED | 否 | `dwd_build_log.build_id` |
| `ingest_batch_id` | BIGINT UNSIGNED | 否 | 来源 ODS 批次 |
| `source_file` | VARCHAR(255) | 否 | 来源文件名 |
| `source_row_number` | BIGINT UNSIGNED | 否 | 来源 CSV 物理行号 |
| `loaded_at` | DATETIME(3) | 否 | DWD 写入时刻（UTC） |

### 3.3 `dwd_events_reject`

| 项 | 值 |
| --- | --- |
| 粒度 | 一条无法转换为标准事件的原始行（整行带证据） |
| 唯一键 | `PRIMARY KEY (row_key)`；`UNIQUE (source_file, source_row_number)` |
| 逻辑分区/日期列 | `event_date_utc`（时间戳非法时为 NULL） |
| 行数基线 | 0（生产数据全部可转换） |
| 上游 | `ods_events_raw` |
| 下游 | 异常排查；质量规则 `dwd_identity` 的组成部分 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `row_key` | BINARY(32) | 否 | 被拒原始行的 ODS 代理键 |
| `raw_timestamp` | VARCHAR(64) | 否 | 原始时间戳 |
| `raw_visitorid` | VARCHAR(64) | 否 | 原始访客 ID |
| `raw_event` | VARCHAR(32) | 否 | 原始事件类型 |
| `raw_itemid` | VARCHAR(64) | 否 | 原始商品 ID |
| `raw_transactionid` | VARCHAR(64) | 否 | 原始交易 ID |
| `event_type` | VARCHAR(32) | 是 | 转换尝试结果；未知事件为 `unknown`，其余 NULL |
| `event_time_utc` | DATETIME(3) | 是 | 时间戳合法时的事件时刻；非法为 NULL |
| `event_date_utc` | DATE | 是 | 时间戳合法时的事件 UTC 日；非法为 NULL |
| `reject_reason` | VARCHAR(32) | 否 | 拒绝原因枚举：`invalid_timestamp` / `missing_item` / `missing_visitor` / `invalid_item_id` / `invalid_visitor_id` / `unknown_event_type` / `missing_txn_on_transaction` / `unexpected_txnid` / `invalid_transaction_id` |
| `reject_message` | VARCHAR(255) | 是 | 人类可读说明 |
| `build_id` | BIGINT UNSIGNED | 否 | `dwd_build_log.build_id` |
| `ingest_batch_id` | BIGINT UNSIGNED | 否 | 来源 ODS 批次 |
| `source_file` | VARCHAR(255) | 否 | 来源文件名 |
| `source_row_number` | BIGINT UNSIGNED | 否 | 来源 CSV 物理行号 |
| `loaded_at` | DATETIME(3) | 否 | DWD 写入时刻（UTC） |

### 3.4 `dwd_events_stage`（内部中间态）

| 项 | 值 |
| --- | --- |
| 粒度 | 一条 ODS 行在本批次转换后的暂存记录（OK 与 REJECT 都进） |
| 唯一键 | `PRIMARY KEY (row_key)`；`UNIQUE (source_file, source_row_number)` |
| 逻辑分区/日期列 | `event_date_utc`；`build_id` + `record_status` 有索引 |
| 行数基线 | 2,756,101（全表；按 `build_id` 分属多次构建，build #30 子集 2,719,267，后续增量构建替换了部分 scope） |
| 上游 | `ods_events_raw`（流式读取 + 纯函数转换） |
| 下游 | `dwd_events` / `dwd_events_duplicate` / `dwd_events_reject`（SQL 窗口分发） |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `row_key` | BINARY(32) | 否 | ODS 确定性代理键 |
| `raw_timestamp` | VARCHAR(64) | 否 | 原始时间戳 |
| `raw_visitorid` | VARCHAR(64) | 否 | 原始访客 ID |
| `raw_event` | VARCHAR(32) | 否 | 原始事件类型 |
| `raw_itemid` | VARCHAR(64) | 否 | 原始商品 ID |
| `raw_transactionid` | VARCHAR(64) | 否 | 原始交易 ID |
| `visitor_id` | BIGINT | 是 | 转换后访客 ID |
| `item_id` | BIGINT | 是 | 转换后商品 ID |
| `raw_event_type` | VARCHAR(32) | 否 | ODS 原始事件类型 |
| `event_type` | VARCHAR(32) | 是 | OK 行为标准事件；REJECT 的未知事件为 `unknown`，其余 NULL |
| `event_time_utc` | DATETIME(3) | 是 | 事件时刻（UTC） |
| `event_date_utc` | DATE | 是 | 事件 UTC 日 |
| `transaction_id` | BIGINT | 是 | 订单级标识 |
| `dedup_key` | CHAR(64) | 是 | OK 行业务键 SHA-256；REJECT 行为 NULL |
| `quality_flags` | VARCHAR(128) | 否 | 转换 flag；无异常 `ok` |
| `record_status` | VARCHAR(8) | 否 | `OK` = 标准通道候选；`REJECT` = 异常通道 |
| `reject_reason` | VARCHAR(32) | 是 | 拒绝原因枚举 |
| `reject_message` | VARCHAR(255) | 是 | 拒绝说明 |
| `ingest_batch_id` | BIGINT UNSIGNED | 否 | 来源 ODS 批次 |
| `source_file` | VARCHAR(255) | 否 | 来源文件名 |
| `source_row_number` | BIGINT UNSIGNED | 否 | 来源 CSV 物理行号 |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |
| `build_id` | BIGINT UNSIGNED | 否 | `dwd_build_log.build_id` |

### 3.5 `dwd_build_log`

| 项 | 值 |
| --- | --- |
| 粒度 | 一次 DWD 构建（一个 scope：FULL 或一组 UTC 日） |
| 唯一键 | `PRIMARY KEY (build_id)` |
| 逻辑分区/日期列 | `started_at` / `finished_at` |
| 行数基线 | 每次构建一行；Phase 8 最新对照 build #55（DATES 2 日） |
| 上游 | DWD 构建器 |
| 下游 | 断点续跑、`cleanup --build`、运维审计 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `build_id` | BIGINT UNSIGNED | 否 | 自增构建号 |
| `scope_type` | VARCHAR(16) | 否 | `FULL` / `DATES` |
| `dates_csv` | VARCHAR(255) | 是 | `DATES` 的日期列表；`FULL` 为 NULL |
| `status` | VARCHAR(16) | 否 | `RUNNING` / `LOADED` / `FAILED` / `CLEANED` |
| `transform_version` | VARCHAR(32) | 否 | 转换规则版本（逻辑变更须全量重建） |
| `processed_rows` | BIGINT UNSIGNED | 否 | 本 scope ODS 处理行数（= OK + 重复 + 拒绝） |
| `ok_rows` | BIGINT UNSIGNED | 否 | 进入 `dwd_events` 的行数 |
| `duplicate_rows` | BIGINT UNSIGNED | 否 | 进入 `dwd_events_duplicate` 的行数 |
| `reject_rows` | BIGINT UNSIGNED | 否 | 进入 `dwd_events_reject` 的行数 |
| `started_at` | DATETIME(3) | 否 | 构建启动（UTC） |
| `finished_at` | DATETIME(3) | 是 | 构建结束（UTC） |
| `error_summary` | TEXT | 是 | 失败原因（不含凭据） |
| `created_at` | DATETIME(3) | 否 | 登记时刻（UTC） |

## 4. DWS 汇总层

### 4.1 `dws_user_daily`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个访客在一个 UTC 日的去重汇总 |
| 唯一键 | `PRIMARY KEY (event_date_utc, visitor_id)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 1,649,534 |
| 上游 | `dwd_events` |
| 下游 | `ads_traffic_daily`、`ads_funnel_daily`、质量门禁 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_date_utc` | DATE | 否 | 事件 UTC 日 |
| `visitor_id` | BIGINT | 否 | 访客 ID；`0` = 未知访客桶 |
| `visitor_zero` | TINYINT | 否 | `1` = 未知访客桶（DWD 中 `visitor_id IS NULL`）；不计入任何去重用户指标 |
| `view_cnt` | BIGINT UNSIGNED | 否 | 当日 `view` 事件行数（不去重） |
| `add_to_cart_cnt` | BIGINT UNSIGNED | 否 | 当日 `add_to_cart` 事件行数 |
| `transaction_cnt` | BIGINT UNSIGNED | 否 | 当日 `transaction` 事件行数 |
| `order_cnt` | BIGINT UNSIGNED | 否 | 当日该访客 `distinct(transaction_id)` 数；**不可对全表求和当全局订单数** |
| `active_flag` | TINYINT | 否 | 当日有任意事件（有事件必有本行，恒为 1） |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

### 4.2 `dws_item_daily`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个商品在一个 UTC 日的去重汇总 |
| 唯一键 | `PRIMARY KEY (event_date_utc, item_id)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 1,672,186 |
| 上游 | `dwd_events` |
| 下游 | `ads_item_ranking_daily`、商品分析看板、质量门禁 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_date_utc` | DATE | 否 | 事件 UTC 日 |
| `item_id` | BIGINT | 否 | 商品 ID |
| `view_cnt` | BIGINT UNSIGNED | 否 | 当日该商品 `view` 事件行数 |
| `add_to_cart_cnt` | BIGINT UNSIGNED | 否 | 当日该商品 `add_to_cart` 事件行数 |
| `transaction_cnt` | BIGINT UNSIGNED | 否 | 当日该商品 `transaction` 事件行数 |
| `view_uv` | BIGINT UNSIGNED | 否 | 当日浏览该商品的去重访客数（商品维度行内去重） |
| `add_to_cart_uv` | BIGINT UNSIGNED | 否 | 当日加购该商品的去重访客数 |
| `transaction_uv` | BIGINT UNSIGNED | 否 | 当日成交该商品的去重访客数 |
| `order_cnt` | BIGINT UNSIGNED | 否 | 当日该商品 `distinct(transaction_id)` 数 |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

### 4.3 `dws_category_daily`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个类目在一个 UTC 日的去重汇总 |
| 唯一键 | `PRIMARY KEY (event_date_utc, category_id)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 94,194 |
| 上游 | `dwd_events` + `ods_item_properties_raw` 的 `categoryid` as-of 快照 |
| 下游 | `v_ads_category_daily`（Superset 受控视图）、质量门禁 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_date_utc` | DATE | 否 | 事件 UTC 日 |
| `category_id` | BIGINT | 否 | 类目 ID；`0` = 事件日及之前无 `categoryid` 快照的未分类桶（含 2015-05-04~05-10 属性窗口前） |
| `view_cnt` | BIGINT UNSIGNED | 否 | 当日该类目 `view` 事件行数 |
| `add_to_cart_cnt` | BIGINT UNSIGNED | 否 | 当日该类目 `add_to_cart` 事件行数 |
| `transaction_cnt` | BIGINT UNSIGNED | 否 | 当日该类目 `transaction` 事件行数 |
| `view_uv` | BIGINT UNSIGNED | 否 | 当日浏览该类目商品的去重访客数 |
| `add_to_cart_uv` | BIGINT UNSIGNED | 否 | 当日加购该类目商品的去重访客数 |
| `transaction_uv` | BIGINT UNSIGNED | 否 | 当日成交该类目商品的去重访客数 |
| `order_cnt` | BIGINT UNSIGNED | 否 | 当日该类目 `distinct(transaction_id)` 数 |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

## 5. ADS 应用层

### 5.1 `ads_traffic_daily`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个 UTC 日 |
| 唯一键 | `PRIMARY KEY (event_date_utc)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 139 |
| 上游 | `dws_user_daily` |
| 下游 | Superset 运营总览；PV/UV/DAU 趋势 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_date_utc` | DATE | 否 | UTC 日 |
| `pv` | BIGINT UNSIGNED | 否 | 当日 `view` 事件行数（不去重，含未知访客桶） |
| `uv` | BIGINT UNSIGNED | 否 | 当日发生过 `view` 的去重访客数（排除未知访客桶） |
| `dau` | BIGINT UNSIGNED | 否 | 当日任意事件去重访客数（排除未知访客桶） |
| `is_full_day` | TINYINT | 否 | `1` = 完整自然日；`0` = 部分日（2015-05-03 / 2015-09-18） |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

### 5.2 `ads_funnel_daily`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个 UTC 日 × 一个口径名（长表，一行 = 一个指标值） |
| 唯一键 | `PRIMARY KEY (event_date_utc, metric_name)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 1,251（139 日 × 9 指标） |
| 上游 | `dws_user_daily`（按人指标）+ `dwd_events`（全局订单去重） |
| 下游 | Superset 漏斗/转化趋势；质量门禁 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_date_utc` | DATE | 否 | UTC 日 |
| `metric_name` | VARCHAR(48) | 否 | `view_users` / `cart_users` / `buyer_users` / `orders` / `add_to_cart_rate` / `conv_strict_users` / `conv_global_users` / `conv_strict_rate` / `conv_global_rate` |
| `metric_value` | DECIMAL(20,6) | 是 | 口径数值；比率为小数或 NULL（分母为 0） |
| `is_full_day` | TINYINT | 否 | 完整日标记，语义同 `ads_traffic_daily` |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

### 5.3 `ads_item_ranking_daily`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个 UTC 日 × 一个上榜商品（仅保留当日 Top-N，默认 100） |
| 唯一键 | `PRIMARY KEY (event_date_utc, item_id)`；`UNIQUE (event_date_utc, rank_no)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 13,900（139 日 × Top-100） |
| 上游 | `dws_item_daily` |
| 下游 | Superset 商品分析 Top-20 排行 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_date_utc` | DATE | 否 | UTC 日 |
| `item_id` | BIGINT | 否 | 商品 ID |
| `rank_no` | INT UNSIGNED | 否 | 当日排名（1 = 热度最高；平局按加购/交易/商品 ID 裁决） |
| `view_cnt` | BIGINT UNSIGNED | 否 | 当日该商品 `view` 事件行数 |
| `add_to_cart_cnt` | BIGINT UNSIGNED | 否 | 当日该商品 `add_to_cart` 事件行数 |
| `transaction_cnt` | BIGINT UNSIGNED | 否 | 当日该商品 `transaction` 事件行数 |
| `view_uv` | BIGINT UNSIGNED | 否 | 当日浏览该商品去重访客数 |
| `add_to_cart_uv` | BIGINT UNSIGNED | 否 | 当日加购该商品去重访客数 |
| `transaction_uv` | BIGINT UNSIGNED | 否 | 当日成交该商品去重访客数 |
| `total_events` | BIGINT UNSIGNED | 否 | 当日该商品三类事件合计 |
| `is_full_day` | TINYINT | 否 | 完整日标记 |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

### 5.4 `ads_retention_cohort`

| 项 | 值 |
| --- | --- |
| 粒度 | 一个 cohort 日 × 一个年龄日（`age_day=0` 为 cohort 规模行，`1` 为次日留存） |
| 唯一键 | `PRIMARY KEY (cohort_date, age_day)` |
| 逻辑分区/日期列 | `cohort_date` |
| 行数基线 | 278（139 cohort × 2 年龄日） |
| 上游 | `dwd_events`（首次观测日 + 回访行为） |
| 下游 | Superset 用户行为留存曲线/热力图 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `cohort_date` | DATE | 否 | 首次观测日（UTC）= `min(event_date_utc)` per visitor |
| `age_day` | TINYINT UNSIGNED | 否 | 回访年龄日；`0` = cohort 规模，`1` = 次日留存 |
| `cohort_size` | BIGINT UNSIGNED | 否 | 分母：`cohort_date` 首次出现的去重访客数 |
| `retained_users` | BIGINT UNSIGNED | 否 | 分子：cohort 中在 `cohort_date + age_day` 发生任意事件的去重访客数 |
| `retention_rate` | DECIMAL(12,6) | 是 | `retained_users / cohort_size`；分母 0 → NULL |
| `window_status` | VARCHAR(16) | 否 | `full` = cohort 与回访日均在完整窗口；`partial` = 部分日/窗口末不足，不得当完整率 |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

### 5.5 `ads_funnel_interval`（Phase 7 R16 物化）

| 项 | 值 |
| --- | --- |
| 粒度 | 一个完整窗口 `[window_start, window_end]`（单行，`id=1`） |
| 唯一键 | `PRIMARY KEY (id)` |
| 逻辑分区/日期列 | `window_start` / `window_end`（生产 = 2015-05-04 / 2015-09-17） |
| 行数基线 | 1 |
| 上游 | `dwd_events`（FULL 构建时 `INSERT ... SELECT`） |
| 下游 | Superset 6 张区间 KPI 卡；`v_ads_funnel_interval` |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `id` | TINYINT UNSIGNED | 否 | 固定 = 1 |
| `window_start` | DATE | 否 | 窗口起点（UTC，含） |
| `window_end` | DATE | 否 | 窗口终点（UTC，含） |
| `view_users` | BIGINT UNSIGNED | 否 | 窗口内 `view` 去重访客（区间 UV） |
| `cart_users` | BIGINT UNSIGNED | 否 | 窗口内加购去重访客 |
| `buyer_users` | BIGINT UNSIGNED | 否 | 窗口内交易去重访客 |
| `orders` | BIGINT UNSIGNED | 否 | 窗口内 `distinct(transaction_id)` |
| `conv_strict_users` | BIGINT UNSIGNED | 否 | 窗口内交易用户 ∩ 浏览用户 |
| `conv_global_users` | BIGINT UNSIGNED | 否 | 窗口内全部交易用户 |
| `add_to_cart_rate` | DECIMAL(20,6) | 是 | `cart_users / view_users`（分母 0 → NULL） |
| `conv_strict_rate` | DECIMAL(20,6) | 是 | `conv_strict_users / view_users` |
| `conv_global_rate` | DECIMAL(20,6) | 是 | `conv_global_users / view_users`（可 > 1，合法） |
| `build_id` | BIGINT UNSIGNED | 否 | 最后一次 FULL 构建批次 |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

### 5.6 `ads_item_behavior_daily`（Phase 7 R16 物化）

| 项 | 值 |
| --- | --- |
| 粒度 | 一个 UTC 日 |
| 唯一键 | `PRIMARY KEY (event_date_utc)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 139 |
| 上游 | `dwd_events`（仅 `item_id IS NOT NULL`） |
| 下游 | Superset 商品行为 KPI/每日 distinct 商品数；`v_ads_item_behavior_daily` |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `event_date_utc` | DATE | 否 | UTC 日 |
| `items_viewed` | BIGINT UNSIGNED | 否 | 当日 `view` 的 `COUNT(DISTINCT item_id)` |
| `items_carted` | BIGINT UNSIGNED | 否 | 当日 `add_to_cart` 的 `COUNT(DISTINCT item_id)` |
| `items_bought` | BIGINT UNSIGNED | 否 | 当日 `transaction` 的 `COUNT(DISTINCT item_id)` |
| `is_full_day` | TINYINT | 否 | 完整日标记 |
| `build_id` | BIGINT UNSIGNED | 否 | `dws_ads_build_log.build_id` |
| `loaded_at` | DATETIME(3) | 否 | 写入时刻（UTC） |

## 6. 控制与审计表

### 6.1 `etl_watermark`

| 项 | 值 |
| --- | --- |
| 粒度 | 一条流水线 × 一个业务日期 |
| 唯一键 | `PRIMARY KEY (pipeline, event_date_utc)` |
| 逻辑分区/日期列 | `event_date_utc` |
| 行数基线 | 139（2015-05-03 ~ 2015-09-18） |
| 上游 | 增量运行成功后写入 |
| 下游 | 增量规划器、`status`、Airflow `register_batch` |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `pipeline` | VARCHAR(32) | 否 | 流水线标识；默认 `incremental`（DDL 注释同时兼容 `dwd` / `dws_ads` 命名） |
| `event_date_utc` | DATE | 否 | 已处理到的业务日期（UTC） |
| `max_ingest_batch` | BIGINT UNSIGNED | 否 | 该日期已消费的最大 ODS 批次（迟到判定依据） |
| `processed_at` | DATETIME(3) | 否 | 本轮写入时刻（UTC） |

### 6.2 `quality_check_run`

| 项 | 值 |
| --- | --- |
| 粒度 | 一次质量门禁运行（FULL 或一组 UTC 日） |
| 唯一键 | `PRIMARY KEY (run_id)` |
| 逻辑分区/日期列 | `window_start` / `window_end` / `started_at` |
| 行数基线 | 每次运行一行；最新 run #7（PASS 60/60，89,305,119 行，209.50s） |
| 上游 | `quality run` / Airflow `quality_gate` |
| 下游 | `publish` 的背书依据；`quality show`；运维审计 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `run_id` | BIGINT UNSIGNED | 否 | 自增运行号 |
| `scope_type` | VARCHAR(16) | 否 | `FULL` / `DATES` |
| `dates_csv` | VARCHAR(255) | 是 | `DATES` 的日期列表；`FULL` 为 NULL |
| `window_start` | DATE | 是 | 本次门禁合法日期窗口下界 |
| `window_end` | DATE | 是 | 本次门禁合法日期窗口上界 |
| `status` | VARCHAR(16) | 否 | `PASS` / `FAIL`（任一规则 FAIL 即 FAIL） |
| `rules_total` | INT UNSIGNED | 否 | 执行规则总数（60） |
| `rules_passed` | INT UNSIGNED | 否 | 通过规则数 |
| `rules_failed` | INT UNSIGNED | 否 | 失败规则数 |
| `checks_total` | BIGINT UNSIGNED | 否 | 全部规则比对对象行数之和 |
| `started_at` | DATETIME(3) | 否 | 启动时刻（UTC） |
| `finished_at` | DATETIME(3) | 是 | 结束时刻（UTC） |

### 6.3 `quality_check_result`

| 项 | 值 |
| --- | --- |
| 粒度 | 一次运行 × 一条规则 |
| 唯一键 | `PRIMARY KEY (result_id)`；逻辑关联 `run_id`（不建物理外键） |
| 逻辑分区/日期列 | 无；随 `run_id` 关联 |
| 行数基线 | 每轮 60 行（run #7 = 60 行 PASS） |
| 上游 | 质量运行器逐规则执行 |
| 下游 | `quality show --run <id>`、耗时热点分析 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `result_id` | BIGINT UNSIGNED | 否 | 自增结果号 |
| `run_id` | BIGINT UNSIGNED | 否 | 所属 `quality_check_run.run_id` |
| `rule_key` | VARCHAR(128) | 否 | 规则稳定键（`quality/rules.py`） |
| `category` | VARCHAR(32) | 否 | `completeness` / `uniqueness` / `validity` / `range` / `reconciliation` |
| `status` | VARCHAR(16) | 否 | `PASS` / `FAIL` |
| `checked` | BIGINT UNSIGNED | 否 | 该规则比对对象行数 |
| `failed_rows` | BIGINT UNSIGNED | 否 | 违规行数 |
| `detail` | TEXT | 是 | FAIL 时前几条失败详情；PASS 为空 |
| `duration_ms` | INT UNSIGNED | 否 | 规则执行耗时（毫秒） |

### 6.4 `ads_publish_log`

| 项 | 值 |
| --- | --- |
| 粒度 | 一次 ADS 发布（审计事件，只增不删） |
| 唯一键 | `PRIMARY KEY (publish_id)` |
| 逻辑分区/日期列 | `published_at` |
| 行数基线 | 12（publish_id 1–12；5–9 为历史证据发布，保留不删） |
| 上游 | `publish`（门禁 PASS + 作用域匹配） |
| 下游 | `v_ads_publish_status`（Superset 新鲜度 KPI）、审计 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `publish_id` | BIGINT UNSIGNED | 否 | 自增发布号 |
| `scope_type` | VARCHAR(16) | 否 | `FULL` / `DATES` |
| `dates_csv` | VARCHAR(255) | 是 | `DATES` 的日期列表；`FULL` 为 NULL |
| `quality_run_id` | BIGINT UNSIGNED | 否 | 批准本次发布的门禁运行号（必须 PASS） |
| `quality_status` | VARCHAR(16) | 否 | 恒为 `PASS` |
| `published_at` | DATETIME(3) | 否 | 发布时刻（UTC） |

### 6.5 `airflow_pipeline_run`

| 项 | 值 |
| --- | --- |
| 粒度 | 一次 DAG 运行（控制/审计，不含业务数据） |
| 唯一键 | `PRIMARY KEY (run_id)`；`dag_run_id` 有索引 |
| 逻辑分区/日期列 | `window_start` / `window_end` / `started_at` |
| 行数基线 | 每次 DAG 运行一行 |
| 上游 | `register_batch` / `batch-finish` / Airflow 回调 |
| 下游 | 运维审计、DAG 重跑排查 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `run_id` | BIGINT | 否 | 运行批次号（batch_id） |
| `pipeline` | VARCHAR(64) | 否 | 处理链标识（默认 `incremental`） |
| `dag_run_id` | VARCHAR(256) | 否 | Airflow DAG run id |
| `mode` | VARCHAR(8) | 否 | `TAIL`（水位线裁剪）/ `FORCE`（显式回填） |
| `window_start` | DATE | 否 | 请求窗口起点（业务日期，UTC） |
| `window_end` | DATE | 否 | 请求窗口终点（业务日期，UTC） |
| `dwd_dates` | VARCHAR(1024) | 是 | 规划出的 DWD 重建日期；空 = stable-state no-op |
| `agg_dates` | VARCHAR(1024) | 是 | DWS/ADS scope = `dwd_dates` ∪ 留存前驱日 |
| `status` | VARCHAR(16) | 否 | `RUNNING` / `SUCCESS` / `FAILED` |
| `published` | VARCHAR(16) | 是 | `PUBLISHED` / `SKIPPED` |
| `publish_id` | BIGINT | 是 | `ads_publish_log.publish_id` |
| `quality_run_id` | BIGINT | 是 | `quality_check_run.run_id` |
| `error_summary` | VARCHAR(2048) | 是 | 失败回调写入的错误摘要 |
| `started_at` | DATETIME(3) | 否 | 登记时刻（UTC） |
| `finished_at` | DATETIME(3) | 是 | 收尾时刻（UTC） |

### 6.6 `dws_ads_build_log`

| 项 | 值 |
| --- | --- |
| 粒度 | 一次 DWS/ADS 聚合构建（FULL 或一组 UTC 日） |
| 唯一键 | `PRIMARY KEY (build_id)` |
| 逻辑分区/日期列 | `started_at` / `finished_at` |
| 行数基线 | 每次构建一行；Phase 4 全量 build #8（3,431,622 行 / 9 表 / 555.7s） |
| 上游 | DWS/ADS 构建器 |
| 下游 | 幂等重建、`cleanup --build`、审计 |

| 列 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| `build_id` | BIGINT UNSIGNED | 否 | 自增构建号 |
| `scope_type` | VARCHAR(16) | 否 | `FULL` / `DATES` |
| `dates_csv` | VARCHAR(255) | 是 | `DATES` 日期列表；`FULL` 为 NULL |
| `status` | VARCHAR(16) | 否 | `RUNNING` / `LOADED` / `FAILED` / `CLEANED` |
| `agg_version` | VARCHAR(32) | 否 | 聚合口径版本（口径变更须全量重建） |
| `table_stats` | TEXT | 是 | JSON：`{表名: 写入行数}` |
| `rows_written` | BIGINT UNSIGNED | 否 | 本批次全部表写入行数合计 |
| `started_at` | DATETIME(3) | 否 | 构建启动（UTC） |
| `finished_at` | DATETIME(3) | 是 | 构建结束（UTC） |
| `error_summary` | TEXT | 是 | 失败原因（不含凭据） |
| `created_at` | DATETIME(3) | 否 | 登记时刻（UTC） |

## 7. Superset 只读受控视图

| 视图 | 粒度 | 来源 | 用途 |
| --- | --- | --- | --- |
| `v_ads_category_daily` | 类目 × UTC 日 | `dws_category_daily` 列投影 | 类目 Top-20 PV/订单 |
| `v_ads_funnel_interval` | 完整窗口单行 | `ads_funnel_interval` 列投影 | 6 张区间 KPI 卡 |
| `v_ads_item_behavior_daily` | UTC 日 | `ads_item_behavior_daily` 列投影 | 每日 distinct 商品数 |
| `v_ads_item_orders_daily` | 商品 × UTC 日 | `dwd_events`（`transaction`）`COUNT(DISTINCT transaction_id)` | Top 商品订单数 |
| `v_ads_publish_status` | 单行聚合 | `ads_publish_log` 计数/最新发布 | 数据发布/新鲜度 KPI |

> 视图均为 `DEFINER` 视图；`superset_ro` 只需 `SELECT` 视图，不接触底层控制表。

## 8. 血缘与不变量

```text
events.csv ──► ods_events_raw ──► dwd_events
                         ├──► dwd_events_duplicate
                         └──► dwd_events_reject
item_properties_*.csv ──► ods_item_properties_raw ──► dws_category_daily（categoryid as-of）
category_tree.csv ──► ods_category_tree_raw（参考维度）

dwd_events ──► dws_user_daily ──► ads_traffic_daily
            ├──► dws_item_daily ──► ads_item_ranking_daily
            ├──► dws_category_daily ──► v_ads_category_daily
            └──► ads_funnel_daily / ads_retention_cohort
                 ads_funnel_interval / ads_item_behavior_daily
```

- **ODS 恒等式**：`ods_events_raw = dwd_events + dwd_events_duplicate + dwd_events_reject`（生产：2,756,101 = 2,755,641 + 460 + 0）。
- **DWS 一致性**：`ads_traffic_daily.uv == ads_funnel_daily.view_users`（同日）；`conv_global_users == buyer_users`；`conv_strict_users ⊆ conv_global_users`；`age_day=0` 行等于 cohort 规模。
- **订单口径**：`orders = COUNT(DISTINCT transaction_id) FROM dwd_events`；**禁止**对 `dws_user_daily.order_cnt` 或 `dws_item_daily.order_cnt` 求和当全局订单数（会因跨日/跨行重复多计）。
- **发布口径**：`ads_publish_log` 只由 PASS 门禁背书；作用域必须匹配（FULL↔FULL、DATES↔同日期集合）。
- **行数基线**：`scripts/demo_phase8.py` 的 `PRODUCTION_BASELINE` 固化了 13 张表（含 ODS/DWD/DWS/ADS/水位/发布日志）的验收值。

## 9. 时区、窗口与类型约定

- **时区**：全部 UTC。`timestamp`（Unix 毫秒）→ `event_time_utc` → `event_date_utc`，日界 = `ts // 86400000`，不做本地时区换算。
- **窗口**：
  - 全部数据：139 个 UTC 日（2015-05-03 03:00:04 ~ 2015-09-18 02:59:47 UTC）。
  - 完整窗口：`[2015-05-04, 2015-09-17]`（137 个完整自然日）；首末日 `is_full_day=0`。
  - 留存完整 cohort：D0 ∈ `[2015-05-04, 2015-09-16]`（136 天）；其余 3 天标 `partial`。
- **空值语义**：分母为 0 → 比率 NULL（不是 0）；部分日/窗口不足 → 打标（`is_full_day=0` / `window_status='partial'`），不补零。
- **字符串保真**：ODS 的 `timestamp`、`visitorid`、`itemid`、`transactionid` 均为 VARCHAR；DWD 才转 BIGINT/DATETIME。
- **未知访客**：原始 `visitorid=0`（3 行）在 DWD 置 `visitor_id=NULL` + `quality_flags` 标记；DWS 聚合为 `visitor_id=0, visitor_zero=1` 的桶；计入 PV，不计入 UV/DAU/去重用户。

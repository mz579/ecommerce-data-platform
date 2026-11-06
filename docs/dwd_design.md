# DWD 明细标准化层设计（Phase 3）

> 本文件是 `data-engineering/ecommerce-data-platform` 第三阶段（DWD 明细标准化）
> 的设计与实现说明。配套文档：`data_contract.md`（契约）、`metric_definitions.md`
> （指标口径）、`data_dictionary.md`（ODS 扫描事实）。
>
> 核心一句话：**从 ODS 贴源层读入 2,756,101 行原始事件，经纯函数转换 + SQL
> 精确去重，得到一个「标准化、去重、可审计」的 DWD 明细，任何一行原始记录都
> 不静默丢失**（恒等式 `ODS = dwd_events + dwd_events_duplicate + dwd_events_reject`
> 每次构建强制校验）。

## 0. 层定位与设计原则

| 维度 | 决定 |
| --- | --- |
| 层 | DWD（Data Warehouse Detail，明细标准化层），DWS/ADS 之上的所有指标都从这里取数 |
| 输入 | `ods_events_raw`（`events.csv` @ Phase 2，2,756,101 行） |
| 输出 | `dwd_events`（标准明细）＋ `dwd_events_duplicate`（业务键去重掉的重复行）＋ `dwd_events_reject`（异常隔离） |
| 粒度 | 一行 = 一个**去重后**的标准行为事件（`view` / `add_to_cart` / `transaction`） |
| 时区 | 全链路 UTC；事件毫秒 → `DATETIME(3)` 保留毫秒位；`event_date_utc` 为分区键 |

设计原则（对应主方案 §2.2）：

1. **不静默丢弃**：无法映射为标准事件的行带证据进入 `dwd_events_reject`；被去重的
   行带原值进入 `dwd_events_duplicate`。行数恒等式是构建成功的前置条件。
2. **转换即纯函数**：`src/ecommerce_dw/dwd/transform.py` 内所有转换都是无状态、行级、
   可单测的纯函数，SQL 不做业务加盐；**去重交给 MySQL 窗口函数**（`ROW_NUMBER()
   PARTITION BY dedup_key`），275 万行也精确、不占 Python 内存。
3. **幂等 + 可恢复**：`FULL` / `DATES` 两种重建语义（§5）；断点续跑靠
   `dwd_build_log.RUNNING` + stage 行计数 + `TRANSFORM_VERSION` 版本保护（§5.4）。
4. **命名对齐主方案**：原始事件 `addtocart` → 标准 `add_to_cart`，原始值由
   `raw_event_type` 保真（`data_contract.md` §2.2）。

## 1. 表结构与粒度

### 1.1 `dwd_events` —— 标准明细（一行 = 一个去重后的标准事件）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `event_sk` | BIGINT UNSIGNED PK AUTO_INCREMENT | 代理主键；**重建后值可变化**，跨运行血缘由 `row_key` 保证 |
| `row_key` | BINARY(32) UNIQUE | ODS 确定性代理键 = SHA-256(source_file ▿ source_row_number)，与 ODS/DUP/REJECT 同一键域 |
| `dedup_key` | CHAR(64) UNIQUE | 业务键 SHA-256（§4）；UNIQUE 约束本身即去重不变量 |
| `visitor_id` | BIGINT NULL | 访客 ID；原始 `"0"` 哨兵 → NULL + flag `visitor_zero`（软异常，行保留） |
| `item_id` | BIGINT NOT NULL | 商品 ID |
| `raw_event_type` | VARCHAR(32) | 原始事件字符串（`view`/`addtocart`/`transaction`），保真 |
| `event_type` | VARCHAR(32) | DWD 标准枚举 `{view, add_to_cart, transaction}` |
| `event_time_utc` | DATETIME(3) NOT NULL | 事件时刻（毫秒 → UTC，保毫秒位） |
| `event_date_utc` | DATE NOT NULL | 事件 UTC 日（分区键） |
| `transaction_id` | BIGINT NULL | 订单级标识（仅 transaction 事件非空；非行级唯一） |
| `quality_flags` | VARCHAR(128) | 逗号分隔 flag；唯一取值 `ok` | `visitor_zero` |
| `build_id` / `ingest_batch_id` | BIGINT UNSIGNED | 产出本行的 build / 来源 ODS 行所属上游批号（双向血缘） |
| `source_file` / `source_row_number` | VARCHAR(255) / BIGINT | 回查原始 CSV（`sed -n <row>p` 即可） |
| `loaded_at` | DATETIME(3) | DWD 写入时刻 |

索引：`(event_date_utc, event_type)` 支撑按日 + 事件类型扫描；`(event_date_utc,
visitor_id)` / `(event_date_utc, item_id)` 支撑 DAU / 商品维度分析。

### 1.2 `dwd_events_duplicate` —— 被去重的原始行（460 行）

- 一行 = 一条业务键去重的**落选行**。主键 `row_key`（与被保留的那行共享
  `dedup_key`，可 JOIN 审计）。
- 保留原始 5 字段字符串快照（`raw_timestamp` / `raw_visitorid` / `raw_event` /
  `raw_itemid` / `raw_transactionid`）＋ 转换后各字段 + 血缘。**不删除、不降级**。

### 1.3 `dwd_events_reject` —— 异常隔离表

- 一行 = 一条无法标准化的 ODS 行。`reject_reason` 枚举 9 种（§3）。
- `event_date_utc` / `event_time_utc` 在时间戳合法时照常落值（异常表与明细一样可查）；
  **非法时间戳行为 NULL**（没有 UTC 日，见 §8 时区语义）。
- 保留 raw 5 字段 + `source_row_number`，可回查原文。

### 1.4 `dwd_events_stage` —— 转换暂存（内部中间态）

- 转换 pass 的产物（OK 与 REJECT **都进**同一张 stage），跨 pass 的唯一载体是
  `row_key`。去重 pass 从这里做三路分发。
- 生命周期：构建期间存在；构建完成后**保留**到 `cleanup_build`（或下一次同 scope
  重建的 delete-scope 阶段）才被清空——保留是为了断点续跑直接复用已有 stage 行。
- 好处：转换与去重解耦 ⇒ 转换只是行级纯函数；幂等/续跑以 `row_key` 主键 + 计数实现
  （同 Phase 2 ODS 加载器思路）。

## 2. 转换规则（纯函数，`transform.py`，版本 `TRANSFORM_VERSION = "1"`）

输入恒为 `ods_events_raw` 的 5 个字符串列 `timestamp, visitorid, event, itemid, transactionid`，
输出 `TransformOutcome`（`OK` 或 `REJECT` + flags）。**函数对脏数据永不抛异常**——一律
通过 `reject_reason` 回报。

| 原始列 | 规则 | 异常（→ REJECT） |
| --- | --- | --- |
| `timestamp` | 非空、纯整型、>0 的毫秒串 → `event_time_utc`（DATETIME(3)，`1970-01-01 + ms`）与 `event_date_utc`（`ts//86400000` 世界时日） | 空 / 非整型 / ≤0 / 超 DATETIME 范围 → `invalid_timestamp` |
| `event` | 命中映射表 `view→view, addtocart→add_to_cart, transaction→transaction`；`raw_event_type` 恒记原值 | 其他任意值 → `unknown_event_type`（`event_type='unknown'` 落 reject 表） |
| `visitorid` | 非空非 `"0"` → BIGINT（MySQL BIGINT 有符号边界 `[-2^63, 2^63-1]` 校验） | 空 → `missing_visitor`；非整型/越界 → `invalid_visitor_id` |
| `itemid` | 非空 → BIGINT（同上边界） | 空 → `missing_item`；非整型/越界 → `invalid_item_id` |
| `transactionid` | transaction 事件必须非空 → BIGINT；非 transaction 事件必须为空 | transaction+空 → `missing_txn_on_transaction`；非 transaction+非空 → `unexpected_txnid`；非整型/越界 → `invalid_transaction_id` |

明确的拒绝哲学（契约 §3 钉死）：**类型转换失败一律拒绝，绝不默认填充 0**。

唯一的软异常：`visitorid="0"`（3 行）→ 标准行 + `visitor_id=NULL` + flag
`visitor_zero`（进 count、不进去重用户口径）。

## 3. 异常策略（三通道总览）

| 通道 | 分录规则 | 承载表 |
| --- | --- | --- |
| OK | 转换通过、业务键去重后胜出 | `dwd_events` |
| DUPLICATE | 转换通过、业务键去重后落选 | `dwd_events_duplicate` |
| REJECT | 转换判定异常（9 种 reason） | `dwd_events_reject` |

9 种拒绝原因与触发条件、当前数据实测（源自 `data_dictionary.md` 全量扫描与构建结果）：

| reject_reason | 含义 | 全量实测 |
| --- | --- | --- |
| `unknown_event_type` | `event` 不在白名单 | 0 行（但测试保留分支） |
| `invalid_timestamp` | 时间戳空 / 非整型 / ≤0 / 越界 | 0 行 |
| `missing_visitor` | `visitorid` 为空 | 0 行 |
| `invalid_visitor_id` | visitorid 非整型/越界 | 0 行 |
| `missing_item` | `itemid` 为空 | 0 行 |
| `invalid_item_id` | itemid 非整型/越界 | 0 行 |
| `missing_txn_on_transaction` | transaction 事件缺 transactionid | 0 行 |
| `unexpected_txnid` | 非 transaction 事件带 transactionid | 0 行 |
| `invalid_transaction_id` | transactionid 非整型/越界 | 0 行 |

## 4. 完全重复与去重决策

- **业务键**：`(event_time_utc, visitor_id, event_type, item_id,
  COALESCE(transaction_id, 0))`，SHA-256 → `dedup_key`。NULL 归一：`visitor_id=NULL`
  编码为空件、`transaction_id=NULL` 编码为 `"0"`（与 COALESCE 一致），NULL 键与值键
  正确区分。组件顺序固定，相同键必同哈希。
- **判定**：`ROW_NUMBER() OVER (PARTITION BY dedup_key ORDER BY source_row_number)`
  → `rn=1` 进 `dwd_events`，`rn>1` 进 `dwd_events_duplicate`。同一 dedup_key 内的
  winner 是 `source_row_number` 最小（最早出现在 CSV）的那行。
- **实测**：全量扫描出 **460 行**重复（458 个不同 dedup_key 出现过 2+ 次），占
  2,756,101 行的 0.017%；`item_properties` 全量 **0 重复**。
- **决策依据**：
  1. ODS 保持 append-only、原样入库不过滤（Phase 2 原则不变，重复是 DWD 层的治理职责）；
  2. **460 行绝不丢弃**——去重落在 `dwd_events_duplicate`，与保留行按 `dedup_key`
     关联即恢复原全集；审计/重放随时可查被去掉的是哪几行；
  3. 指标的隐含影响：PV / 加购率等在 DWD 之上统计时天然用去重后的标准事件
     （`metric_definitions.md` §1 已注明可补回 duplicate 表得到未去重口径）。

## 5. 构建编排与幂等重建

入口 `python -m ecommerce_dw.dwd.cli build [--dates ...] [--force] [--chunk-size N]
[--max-resumes M] [--resume-wait S]`（`src/ecommerce_dw/dwd/builder.py`）。

`build_dwd` 流程：

1. **`_ensure_tables`**：校验 DWD DDL 已应用（缺表直接报错，指引跑 `scripts/apply_dwd_ddl.py`）。
2. **scope 决策**（`Scope`：`FULL` 或 UTC 日期集合）：
   - 已有 **RUNNING** 同 scope 构建 → 续跑（§5.4）；transform 版本不一致则拒绝，要求 `--force`；
   - 否则注册新 `dwd_build_log`（status=RUNNING）并 `_delete_scope_rows` 清空本 scope 在
     四张 DWD 表中的行。
3. **`_stage_all`**：流式读 ODS → 行级纯函数转换 → `INSERT IGNORE` 进 stage（§5.3）。
4. **`_split_stage_to_tables`**：三条原子 SQL 窗口语句完成三路分发（顺序见 §5.2）。
5. **`_verify_identity`**：校验恒等式与 dedup 唯一性（§6），通过才 `mark_loaded`。

### 5.1 FULL vs DATES 重建语义

| | FULL | DATES |
| --- | --- | --- |
| 覆盖 | 全部 ODS 事件行 | 指定 UTC 日集合（`--dates 2015-05-03,...`） |
| delete-scope | `dwd_events(WHERE source_file)` 等四表整删 | 四表 `WHERE event_date_utc IN (...) OR event_date_utc IS NULL`（**零日期拒绝行每个 scope 都要捞起**，否则被静默放跑） |
| 去重视角 | 全量窗口函数 | `LEFT JOIN dwd_events` 中**既有保留 winner**，只重建日期的行与保留行做跨日去重——`DATES` 重建不破坏其他日期的去重结果 |
| 用途 | 首次构建 / transform 版本升级 | 补数、修数 |

`Scope` 归一排序去重日期，同一 scope 的 `dates_csv` 是恒定的字符串，因此「同 scope
幂等」判断可靠。

### 5.2 去重分发的 SQL（`_WINNER/_DUPLICATE/_REJECT_SELECT_SQL`）

三条语句都是**单事务原子** INSERT：断连即整体回滚，重试安全（INSERT IGNORE 使
结果已提交的重试变 no-op）。**顺序必须为 DUPLICATE → WINNER → REJECT**：
duplicate 语句 `LEFT JOIN dwd_events` 判定落选行，必须先于 winner 执行，否则它会把
本构建刚插入的 winner 也当重复。

三条语句**都按 `st.build_id = <本批次>` 过滤**，只切分本构建自己的 stage 行：当另
一个构建（例如一次被中断的 FULL 构建）的 stage 里还有未切分的行时，本构建的切分
绝不会把它们卷进去。每个语句再内联自己的 status 字面量（`OK` / `REJECT`），不让
它作为参数传递——历史 bug：把 `("OK",)` 当参数传给三条语句，参数泄漏让 REJECT
行被静默丢弃。

- **duplicate**：`WHERE record_status='OK' AND build_id=<本批次> AND (rn > 1 OR dedup_key IN dwd_events)`。
- **winner**：`WHERE record_status='OK' AND build_id=<本批次> AND rn = 1 AND dedup_key NOT IN dwd_events`
  （`DATES` scope 下既不侵犯已有保留行，又全量精确）。
- **reject**：`WHERE record_status='REJECT' AND build_id=<本批次>`。

### 5.3 内存有界流式（关键约束：全量不进内存）

- 读侧：`pymysql.cursors.SSCursor`（客户端无缓冲流式 `fetchmany`），**绝不**把 2.7M 行
  拉进 RAM；默认缓冲 `cursor.execute()` 会整表缓冲，违背约束，已避开。
- 写侧：**专用写连接**。同一 socket 上流式 SELECT 与并发 INSERT 不安全，且 pymysql 的
  `executemany` 会替换挂起的 SELECT 结果把流截断（探测过的真实故障），故读写分连接。
- 每块成批 `executemany`（`max_stmt_length=8MB`）+ `commit`。

### 5.4 断点续跑与可恢复

- 崩溃留下 `dwd_build_log.RUNNING` + 已提交的 stage 行；下次同 scope 运行：计数已有
  stage 行 → `skip_count` → 续跑未处理的部分，最后**不改**已有结果（INSERT IGNORE +
  delete-scope 只在注册新 build 时发生）。
- `TRANSFORM_VERSION` 保护：转换规则若变更而 stage 是旧版本，续跑被拒（要求 `--force`
  全新重建），杜绝「新旧逻辑混拼」。
- 连接丢失（WSL 里 MySQL 随虚拟机重启）：`_stage_all` 双连接重连 + stage 重计数续跑，
  `_split` 单语句事务重试；`--max-resumes` / `--resume-wait` 控制重试上限（生产全量跑
  用了 60 次 / 120s）。

## 6. 数据完整性：行数恒等式

每次构建结束（及 `verify` 命令随时可跑）校验：

```
ods_scope_rows == COUNT(dwd_events) + COUNT(dwd_events_duplicate) + COUNT(dwd_events_reject)
COUNT(dwd_events) == COUNT(DISTINCT dedup_key IN dwd_events)        -- 标准行无重复
COUNT(dwd_events_stage) == ok + dup + rej                            -- stage 被完全消费
```

任一不成立 → `BuildError`，不标记 LOADED。`python -m ecommerce_dw.dwd.cli verify`
可对任意 scope 只读复检。

## 7. 为什么新建 `dwd_build_log` 而不复用 `etl_batch_log`

Phase 2 已有 ODS 装载日志 `etl_batch_log`，三个实质理由支持独立建表：

1. **幂等键不同**：`etl_batch_log` 以「源文件 + 文件 SHA-256」为键，描述「一次文件装载」，
   文件的整个读写生命周期只发生一次；DWD build 的键是「scope（FULL 或 UTC 日期集合）」，
   同一个 `events.csv` 会被 FULL / DATES 反复重建，文件哈希在这里没有意义。
2. **计数与状态语义不同**：ODS 装载只关心「这文件装了多少行」；DWD build 需要
   `ok / duplicate / reject` 三元细粒度计数 + `transform_version` + `scope_type/dates_csv`
   ——塞进 etl_batch_log 要么变性改造，要么塞进一堆 NULL 专用列。
3. **生命周期不同**：文件批只写一次；DWD 同一 scope 有
   `RUNNING → LOADED | FAILED → CLEANED` 状态机，跨多次重建，语义与清理逻辑独立。

结论：独立 `dwd_build_log`（DDL 见 `sql/ddl/dwd_build_log.sql`），与 `etl_batch_log`
通过 `ingest_batch_id` 双向关联血缘。

## 8. 时区与零日期行语义

- 全链路 UTC。毫秒时间戳 → `datetime.fromtimestamp(ms, UTC)` 的 naive-UTC 表示 →
  `DATETIME(3)`（毫秒位保留）；`event_date_utc = ts // 86400000`（UTC 世界时，
  **不做**任何本地时区换算）。
- **零日期行**（`invalid_timestamp`）：没有 UTC 日 → `event_date_utc=NULL`。任何
  `DATES` scope 都必须显式包含它们（`_ods_where` 里 `OR CAST(timestamp AS UNSIGNED)=0`，
  `_dwd_where` 里 `OR event_date_utc IS NULL`），否则这类行会在 delete-scope / identity
  校验里被静默放跑。它们每个 scope 都被捞起并送审，正是「不静默丢弃」的体现。

## 9. 全量构建实测（Phase 3 measurement）

> 诚实记录：以下数字只写**实际探测到**的；未完成的步骤明确标注为「待执行」。

### 9.1 已实测（探索 / 构建过程）

| 量 | 值 | 来源 |
| --- | --- | --- |
| ODS 事件行数（`events.csv`） | 2,756,101 | `ods_events_raw` COUNT（构建前后一致） |
| 完全重复行（业务键） | 460 行（458 个 dedup_key 出现 2+ 次） | 全量 ODS 扫描（`data_dictionary.md`） |
| 异常行实测 | 0（9 种 reject_reason 全 0） | 全量扫描 + 构建 stage 无 REJECT |
| 事件类型分布（ODS 口径） | view 2,664,312 / addtocart 69,332 / transaction 22,457 | 全量扫描 |
| DWD stage 已提交行（build #30，FULL） | **2,740,000**（全为 `OK`，占 scope 98.0%） | 断点时的 stage COUNT |

### 9.2 全量 build 执行状态

2026-11-07 02:35 启动全量构建（build #30，FULL，`--max-resumes 60 --resume-wait 120`）。
staging pass 以每 20k 行一个 chunk 流式灌入 `dwd_events_stage`（读 SSD 25-40s/10万行），
到 **02:57 已提交 2,740,000 / 2,756,101 行（98.0%）** 时，后台进程被运行环境的
**harness 内存回收策略杀掉**（非构建失败；MySQL 侧无错误）。stage 数据全部保留、
`dwd_build_log` 记录 build #30 为 `RUNNING`（transform_version=1，与当前规则一致）。

**恢复方式**：等系统内存充裕后，在项目根目录再跑一次（幂等断点续跑，会自动接续
build #30，只补 stage 剩余 ~16k 行，再走 duplicate→winner→reject 切分与恒等式校验）：

```bash
uv run python -m ecommerce_dw.dwd.cli build --max-resumes 60 --resume-wait 120
```

预期（依据 9.1 的 ODS 实测推算，**恢复完成后才有最终证值**）：

- `dwd_events`（去重后标准明细）= **2,755,641** = 2,756,101 − 460；
- `dwd_events_duplicate` = **460**；
- `dwd_events_reject` = **0**；
- 恒等式 `2,756,101 = 2,755,641 + 460 + 0` 由 build 内 `_verify_identity` 强制核验。

## 10. 待决 / 已知口径

- 精确重复行的去重保留策略（保留 CSV 最早行 vs 其他策略）定为「source_row_number 最小」。
- `transactionid` 为订单级标识：22,457 行 transaction 对 17,672 个不同 ID（一个订单多行），
  DWD 不去重同一订单多行；按订单计数必须 `distinct(transaction_id)`（`metric_definitions.md` §6）。
- 指标层在 DWD 之上使用去重后标准事件；需要未去重口径时把 `dwd_events_duplicate` 对应行加回。
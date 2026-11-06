# 增量处理与质量门禁（Phase 5）

> 本文件是 `data-engineering/ecommerce-data-platform` 第五阶段（业务日期维度上的
> **增量处理** + 全链路**质量门禁**）的设计与实现说明。配套文档：`dws_ads_design.md`
> （Phase 4 汇总/应用层，增量重建的底层）、`dwd_design.md`（DWD 明细层，唯一事实源）、
> `metric_definitions.md`（指标口径母本）。
>
> 核心一句话：**以「业务日期 × 已消费 ODS 批次」的水位线（watermark）为唯一驱动，
> 同一套增量入口处理「新尾巴 + 窗口内迟到」两类业务日期，历史修正通过显式
> `--from/--to` 回填进入同一入口；每次处理完毕先跑 60 条硬门禁规则，任何一条 FAIL
> 即拒绝发布 ADS，状态与逐规则结果全部落库可查。**
>
> 本文所有数字均为**生产库 `ecommerce_dw` 实测值**（真实执行记录，无推算）。

## 0. 层定位与设计原则

| 维度 | 决定 |
| --- | --- |
| 增量主键 | **业务日期**（UTC 日，`event_date_utc`），不是机器时间 / 批处理时钟 |
| 驱动 | `etl_watermark` 水位线：每（pipeline, 业务日期）记「已消费的 ODS 最大 `ingest_batch_id`」 |
| 迟到策略 | N=7 天回看窗内自动重算受影响日期；窗外深度修正需显式 `--from/--to`，走同一入口 |
| 门禁 | 60 条规则 = 完整性 3 + 唯一性 12 + 合法性 9 + 范围 12 + 跨层对账 24，**硬失败**（任一 FAIL → 拒绝发布 ADS） |
| 发布 | `ads_publish_log` 只接受「本次新鲜全量门禁 PASS」或「引用一个既有 PASS run_id」的请求 |
| 幂等 | 同 scope（同一业务日期集合）重复执行内容恒定；中途失败后重跑自动沿用上次部分成果并补齐 |
| 依赖 | 纯 Python 3.11 + pymysql，无 Airflow / Kafka / Flink / Superset，无新增第三方库 |

设计原则：

1. **水位于数据、前水位于表**：每个业务日期的水位 = 该日期在 `ods_events_raw`
   里已入仓的**最大 `ingest_batch_id`**。ODS 新增批次（同一日期也有可能是迟到/修正文件）
   推高该日期的当前批次 → 与水位比对出「新日期 / 窗口内迟到 / 窗口外待修」三类。
2. **一个入口，三层联动**：增量只做「受影响业务日期」集合 → DWD 按日期重建 →
   DWS/ADS 按日期重建（自动带上留存依赖的前一日）→ 成功后才推高水位。中途失败
   不推水位，下次重跑计划出同样日期、幂等重建补齐（§4）。
3. **迟到看窗口不看事务**：业务日期距水位前沿 ≤ (N−1) 天且在窗口内出现更新的
    ODS 批次 → 视为「窗口内迟到」，本轮自动重算；更早 → 报告 `out_of_window_dates`
    并要求显式 `--from/--to`，避免不知不觉用新逻辑覆盖旧历史。
4. **修正即增量**：历史修正（含 Phase 4 `dws_ads_build_log` 驱动的 DATES 重建）的
    正确入口是**同一个 `incremental --from d --to d`**：DWS/ADS 的 `DATES` 重建
    只动表，增量入口在表重建之外再负责「关联业务日期集合 + 前驱依赖 + 水位前推 +
    门禁通过后可发布」——二者在 Phase 5 收敛为一条链（§3）。
5. **门禁与发布解耦、失败可审计**：门禁每次全量执行并落库
    `quality_check_run` / `quality_check_result`；FAIL 不会抛掉现场，逐规则
    `failed_rows` 与 SQL 错误留在结果表；`publish` 只在存在 PASS run（新鲜全量或引用
    指定 run_id）时写入 `ads_publish_log`（§6、§7）。

## 1. 水位线模型（`etl_watermark`）

DDL：`sql/ddl/etl_watermark.sql`；代码：`src/ecommerce_dw/incremental/watermark.py`。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `pipeline` | VARCHAR(64) | 处理链标识（默认 `incremental`；不同链可各记各的水位） |
| `event_date_utc` | DATE | **业务日期**（UTC 日，全链路口径与 DWD/DWS/ADS 一致） |
| `max_ingest_batch` | BIGINT | 该业务日期**已消费**的 ODS 最大 `ingest_batch_id` |
| `processed_at` | DATETIME(3) | 该水位写库时刻（UTC；幂等覆盖写） |

语义：`{pipeline, event_date_utc}` 联合主键；`upsert_many` 用
`INSERT ... ON DUPLICATE KEY UPDATE`，重跑同一（日期 → 批次）内容必然收敛到同一行
（幂等）。`WatermarkRepo.load` 读出 `{business_date: max_ingest_batch}` 供规划器比对。

**为什么按业务日期 + 批次、不按机器时间**：onsite 数据是「同一日期可能分批/迟到」
的电商事件流，最自然的增量边界是「某个业务日期此前已处理到哪个 ODS 批次」。
机器时间增量会漏掉当日直达历史日期的修正；纯 `MAX(ingest_batch_id)` 全局水位又
无法表达「每个日期各吃到第几批」。按日期 × 批次：`from --to`、迟到、全量回灌
在模型上都是对这张表的 Upsert。

**时间推算（tz-safe）**：业务日期由事件时间戳换算——

```sql
DATE('1970-01-01') + INTERVAL (CAST(`timestamp` AS UNSIGNED) DIV 86400000) DAY
```

与 DWD 的 UTC 日界完全一致，保证增量层与明细/汇总层不会出现日界错位。

### 1.1 水位前沿（frontier）与分类

```
frontier = MAX(watermark∪current)          # 最新已知业务日期（ODS 尾巴优先）
回看窗口   = [frontier − (late_window_days − 1), frontier]   # 默认 N=7 → 6 天窗口
```

对每个有正批次的业务日期 `day`（`current[day]` = ODS 当前最大批次，`wm[day]` = 已消费批次）：

| 情形 | 判定 | 处理 |
| --- | --- | --- |
| 新日期 | `wm[day] == 0` 且 `current[day] > 0` | 本轮重建（fresh tail） |
| 窗口内迟到 | `wm[day] > 0`、`current[day] > wm[day]`、`day ≥ 窗口起点` | 本轮重建（late） |
| 窗口外修正 | `wm[day] > 0`、`current[day] > wm[day]`、`day < 窗口起点` | 报告 `out_of_window_dates`，不自动处理，提示 `--from/--to` |
| 已消费 | `current[day] <= wm[day]` | 跳过 |

分类逻辑是纯函数（`planner.py::plan_incremental`），单测覆盖边界（含窗口边界
inclusive、零批次不建、无水位、空数据）。

### 1.2 引导（bootstrap）

存量仓库（Phase 4 已全量建好 DWD/DWS/ADS）第一次接入增量时执行
`incremental --init --to <日期>`：把 `≤ to` 的所有已建业务日期以当前 ODS 批次写入
水位（`to` 之后的日期留空 → 下一个普通增量会把它们当新尾巴接住）。默认
`--to` 不传时取完整窗口末日，等于记录全部日期。

## 2. 增量运行（`incremental` 命令）

### 2.1 执行顺序

```
run_incremental(cfg, from/to 可选, late_window_days=7, dry_run, force, top_n)
  ├─ 读水位 + 读当前 {日期 → 最大批次}，plan_incremental / plan_backfill 得受影响日
  ├─ (无受影响 → 早退 no-op，0.00s)
  ├─ DWD：对受影响日所属的每个 ODS source_file 调 build_dwd(dates=受影响日, force)
  ├─ DWS/ADS：受影响日 ∪ {前一日且 DWD 有行}（次日留存 age-1 依赖）
  │    调 build_dws_ads(dates=..., window_start/end, top_n)
  └─ 全部成功后：upsert 受影响日的 {日期 → 当前批次} 进水位
```

水位前移放在**最后**：DWD 或聚合任一层抛错 → 事务不推进、水位不动；下次重跑
replan 出相同的受影响日，各层自己的 scope 幂等语义自动「重建同一批日期」并收敛。

### 2.2 受影响日集合与留存前驱

- **DWD 层**：只重建受影响日自身（`dwd_events` 是明细日表）。
- **DWS/ADS 层**：重建受影响日 + 其**前一日**（前提：前一日在 DWD 有行）——
  因为 `ads_retention_cohort` 的 age-1 边（cohort 日 = 重建日）反过来引用「重建日 + 1
  天的回访」。重建 cohort D 必须同时重算 cohort D−1 的次日留存。例如受影响日
  {09-17, 09-18} → 聚合 scope {09-16, 09-17, 09-18}（`_agg_scope`，自动取 09-16 补上）。

### 2.3 显式回填（`--from/--to`）

`plan_backfill(from, to)` 直接给出该业务日期区间（只含 ODS 有行的日期），
走与普通增量**完全相同**的 DWD → DWS/ADS → 水位路径；区间内的日期无论均早于窗口
起点与否都会重建（深修正）。迟到分类仅用于日志报告，不影响重建范围。

### 2.4 `--dry-run` 与 `--force`

- `--dry-run`：只规划不构建，输出受影响日期 + 前台文字（便于调度前确认）。
- `--force`：传给两个 builder，用于清除同 scope 的 FAILED/RUNNING 遗留后重建。

## 3. 历史修正流（与 Phase 4 DATES 收敛）

Phase 4 的修正入口是 `dws.cli build --dates <公历日>`（只重写 DWS/ADS 输出表）；
Phase 5 的修正入口收敛为：

```
python -m ecommerce_dw.cli incremental --from <d1> --to <d2>      # 一体链
```

而不是先手动改 DWS/ADS 表、再让水位自欺式对齐。理由：

1. `dwd_events` 若因迟到/修复文件变化，DWS/ADS 必须跟着重建 —— 增量链从 DWD 开始
   （`_odv_source_files` 按受影响日取遍所有 ODS 源文件），天然覆盖「既有有、也有没
   有」的明细变化；
2. 留存前驱依赖（§2.2）由 `_agg_scope` 自动补，手动 `build --dates` 容易漏掉 D−1；
3. 修完必须过门禁才能发布：`publish` 只认 PASS run（§6.3），修正后不跑门禁 = 发不出去，
   从机制上杜绝「修了一半就上线」。

DATES 重建能力本身保留（`dws.cli build --dates`），但在 Phase 5 的日常操作里
「修正」的推荐入口是 `incremental --from/--to`。

## 4. 幂等与失败安全

| 场景 | 行为 |
| --- | --- |
| 同日期重复跑（无新批次） | 规划 no-op，0 行写入，水位不动（**生产实测** §9.3） |
| 同日期重跑（有更新批次） | 按「窗口内迟到」重建 → DWD/DWS/ADS 内容与该批次数据一致（同输入幂等） |
| 中途失败 | 水位不推进 → 下次 replan 同样日期 → 两个 builder 的 scope 幂等语义与该批次的 LOADED 日志驱动，自动重跑未完成部分 |
| 内容可重现 | DWD 按 (transform 版本, 分类计数) 判幂等；DWS/ADS 按 scope `dates_csv` 判同 scope 重建幂等 |

两次对同一「业务日期 × 批次」的增量，写出的表内容逐行一致（血缘列
`build_id / loaded_at` 除外）。集成测试端到端断言：稳定态增量 = no-op；注入迟到
批次后 D1 重建、再跑一次 = no-op；门禁全程 PASS。

## 5. 质量门禁规则库（60 条）

入口：`python -m ecommerce_dw.cli quality rules`；实现：
`src/ecommerce_dw/quality/rules.py`（`build_rule_catalog`）。规则是纯 SQL 计数
（`SqlCountRule`：一条查询返回 `(total, bad)`，`bad > 0` → FAIL）；`date_filter`
钩子让「全量跑」与「按 `--dates` 范围跑」共用同一 SQL。

| 类别 | 条数 | 规则 key 与语义 |
| --- | --- | --- |
| **完整性** | 3 | `ods_batch_log_rowcount`（每个 LOADED 批次的 `etl_batch_log.row_count` == ODS 实际行数）、`ods_batch_coverage`（每个 ODS (表,文件,批次) 都有 batch_log 背书）、`dwd_identity`（按源文件 ODS == DWD + duplicates + rejects，无静默丢失） |
| **唯一性** | 12 | ODS：`ods_row_key_unique` / `ods_source_path_unique`；DWD：`dwd_events_row_key_unique` / `dwd_dedup_key_unique`(业务键) / `dwd_aux_row_key_unique`(duplicate/reject 表)；DWS/ADS：8 张汇总与应用表各自主键唯一（`dws_{user,item,category}_daily_pk_unique`、`ads_{traffic_daily,item_ranking_daily,retention_cohort}_pk_unique` + `ads_funnel_daily_pk_unique`） |
| **合法性** | 9 | 事件类型枚举（DWD `{view, add_to_cart, transaction}` / ODS `{view, addtocart, transaction}`）、`dwd_event_date_in_window`（∈ 业务窗口 [2015-05-03, 2015-09-18]，按测试窗口参数化）、`dwd_visitor_null_flag_consistent`（`visitor_id IS NULL` ⟺ `quality_flags` 携带 `visitor_zero` 哨兵，其余非空）、`dwd_quality_flags_valid`（仅 `{ok, visitor_zero}`）、时间戳 / visitorid / itemid / transactionid 合法（ODS 五条） |
| **范围** | 12 | ID>0（`dwd_item_id_positive`）、访客桶非负且 `visitor_zero` 与 `visitor_id=0` 严格一致、DWS 计数非负 + `active_flag` 二元、流量 pv/uv/dau 非负 + `is_full_day` 二元、漏斗指标非负、归一化率 ≤ 1（`conv_global_rate` 可 >1 除外）、留存率 ∈ [0,1]（分母 0 → NULL）、逐日 UV≤DAU、UV≤PV、DAU≤用户行数、排行 rank 连续 | 
| **跨层对账** | 24 | DWS 逐维求和 == DWD 明细计数（user/item/category × view/cart/txn，9 条）；ADS == DWS/DWD 逐日逐指标（流量 3、漏斗 9、留存 2、排行 1：`ads_traffic_*_eq_*`、`ads_funnel_*_eq_*`、`ads_retention_age0_consistent` / `ads_retention_age1_le_cohort`、`ads_item_ranking_matches_dws_item`）；其中漏斗 9 条里 8 条对着 DWS 构建结果复核，新增的 `ads_funnel_conv_global_users_eq_dwd` 直接对 **DWD 明细**独立复算 `conv_global_users`（逐日 `event_type='transaction' AND visitor_id IS NOT NULL` 的 DISTINCT visitor_id 数，与构建口径 `SUM(transaction_cnt>0 AND visitor_zero=0)` 同集合、但完全不依赖 DWS 构建产物） |

**阈值/容差**：除比率对账 `tolerance < 1e-6`（比率列 DECIMAL 存值 vs 精确商的
浮点收敛）外，一律精确相等（整数计数）或方向不等式（≤ / ∈）。无「warn 且继续」：
`severity` 恒为 `error`，`bad > 0` 即 FAIL。

**日期窗口参数化**：`build_rule_catalog(valid_window=...)` 允许集成测试在合成
2099 日期上覆盖 `dwd_event_date_in_window`；生产默认窗口常量
`FULL_WINDOW_START/END` 派生 [2015-05-03, 2015-09-18]。

## 6. 门禁运行与发布门控

实现：`src/ecommerce_dw/quality/runner.py`。三个控制表
（`scripts/apply_quality_ddl.py` 或 `runner._ensure_control_tables` 自动补建）：

```sql
etl_watermark       -- §1
quality_check_run   -- 一次门禁运行头：scope_type(FULL|DATES)+dates_csv、window、
                      status(PASS|FAIL)、rules_total/passed/failed、checks_total、起止
quality_check_result-- 逐规则：run_id、rule_key、category、status(PASS|FAIL)、
                      checked、failed_rows、detail(截断 4k)、duration_ms
ads_publish_log     -- 发布记录：scope、引用的 run_id、quality_status、published_at
```

### 6.1 `quality run [--dates ...]`

执行全部 60 条规则 → 落库 `quality_check_run`（1 行）+ `quality_check_result`
（60 行）→ 返 `QualityReport`。**硬门禁**：任何规则 `bad > 0` → status=FAIL 并抛
`QualityGateError`（信息含 run_id、失败规则数与每条违反行数），CLI 退出码 2。

`--dates` 限定范围时做**范围门禁**（带日期窗口参数）——重建/修正了哪几天就验哪几天，
同时保留全量门禁作为发布前的一次性总闸。

### 6.2 规则 SQL 失败

规则 SQL 本身抛错（如列名笔误、表缺失）不会中断整个趟次：该规则记为
`failed_rows=1, detail="rule SQL failed: <异常>"` → status=FAIL。**坏的规则 = 门禁
FAIL**，从设计上防止「规则失效、实际是坏的」假绿（Fool-me-once）。

### 6.3 `publish [--run <id>] [--dates ...]`

发布只做一件事：写 `ads_publish_log` 一行。准入：

- 不传 `--run` → 现场先跑一次**全量门禁**，PASS 才写；该路径开头打印一行提示
  （`publish: no --run given - running a fresh FULL quality gate ... expect a few minutes`），
  说明发布前正在做全量扫描、预计数分钟，避免无感等待。
- 传 `--run` → 校验该 run 存在且 status=PASS（最近一次全量跑通过即可复用，不必重跑）。

以下情况一律拒绝（`QualityGateError`，`ads_publish_log` 零写入）：

- 引用 run 不存在或 status=FAIL（含「陈旧 FAIL」：上次 FAIL 之后未再跑）;
- 现场数据带病（新鲜全量门禁 FAIL）。

### 6.4 `quality show [--run <id>]` / `status`

`quality show`：最近 N 次门禁运行（run_id/status/scope/规则通过数/检行数/耗时）；
带 `--run` 再打印该 run 的逐规则结果。CLI `status` 同时显示水位
（pipeline / 日期数 / frontier）与最近门禁运行。

## 7. 失败处理汇总

| 失败 | 外部表现 | 处理 |
| --- | --- | --- |
| 门禁 FAIL | CLI 退出码 2；`quality_check_run.status=FAIL` | 查 `quality show --run <id>` 逐规则定位；修数据后重跑门禁；`publish` 拒绝直到 PASS |
| 发布被拒 | CLI 退出码 2 + 原因（无 run / FAIL run / 数据带菌） | 先修数据 → 门禁 PASS → 再 publish；`ads_publish_log` 不受污染（**集成测试断言**） |
| 增量中途失败 | 退出码非 0；水位未推进 | 修复后重跑同一 `incremental`；各层 scope 幂等自动补齐 |
| 窗口外修正 | 日志 WARNING：`out_of_window_dates` | 显式 `incremental --from --to` 执行深修正（§2.3） |

## 8. CLI 速查（`python -m ecommerce_dw.cli`）

```bash
python -m ecommerce_dw.cli incremental --init --to 2015-09-16   # 引导水位（存量仓库）
python -m ecommerce_dw.cli incremental                          # 尾巴 + 窗口内迟到
python -m ecommerce_dw.cli incremental --from 2015-09-10 --to 2015-09-12   # 深修正
python -m ecommerce_dw.cli incremental --dry-run                # 只规划
python -m ecommerce_dw.cli quality run                          # 全量硬门禁
python -m ecommerce_dw.cli quality run --dates 2015-09-17,2015-09-18
python -m ecommerce_dw.cli quality rules                        # 规则目录
python -m ecommerce_dw.cli quality show [--run <id>]            # 历史 / 逐规则
python -m ecommerce_dw.cli publish [--run <id>] [--dates ...]   # 门禁通过后发布 ADS
python -m ecommerce_dw.cli verify [--dates ...]                 # ADS 独立复算（Phase 4）
python -m ecommerce_dw.cli status                               # 水位 + 近期门禁
```

## 9. 生产实测（Phase 5）

> 全部数字来自对生产库 `ecommerce_dw` 的**真实执行**（本机 2026-11-07），非推算。

### 9.1 水位引导

| 项 | 值 |
| --- | --- |
| `incremental --init --to 2015-09-16` | 记录 **137** 个业务日期（2015-05-03 … 2015-09-16） |
| 引导后 workflow | `incremental` 普通运行接管 09-17 / 09-18 |

### 9.2 普通增量运行（尾巴 2 天）

| 项 | 值 |
| --- | --- |
| 受影响业务日期 | 2015-09-17, 2015-09-18（`_agg_scope` 自动补 2015-09-16 作为留存前驱） |
| DWD build | build #49，`ods=11,656, ok=11,654, dup=2, reject=0` |
| DWS/ADS build | build #3，聚合写入 **32,014 行**（3 张 DWS + 4 张 ADS） |
| 水位前推 | `watermark_upserted=2`（09-17 / 09-18） |
| 耗时 | **255.41s**（4 分 15 秒；DWD 阶段约 150s） |
| DWD 全表核对 | 重建后 `dwd_events` 总数仍 = **2,755,641**（无净增减） |

### 9.3 幂等（稳定态重跑）

| 项 | 值 |
| --- | --- |
| 紧随其后的第二次 `incremental` | `built=(none) dwd_rows=0 agg_rows=0 watermark_upserted=0 duration=0.00s`（no-op） |

### 9.4 全量质量门禁（最新 run #3，FULL，60 条规则）

| 项 | 值 |
| --- | --- |
| 规则总数 / 通过 | **60 / 60（100%）**，失败 0，status=PASS |
| 按类别 | 完整性 3/3、唯一性 12/12、合法性 9/9、范围 12/12、跨层对账 24/24 |
| 检行数（checked 合计） | **89,305,119**（60 条规则「被比对行数」求和；如 `ods_row_key_unique` checked=23,033,672 = 三张 ODS 表行数总和） |
| 运行耗时 | **224.65s**（3 分 45 秒墙钟；`quality_check_run` 记录 started→finished） |
| 结果落库 | `quality_check_run` 1 行 + `quality_check_result` 60 行（逐规则 status=PASS） |
| 新增规则 | `ads_funnel_conv_global_users_eq_dwd`（逐日 `conv_global_users` 独立对 DWD 复算，checked=139 = 每个业务日期一行），与其余 23 条对账规则全数通过 |
| 本轮是否触发发布 | 否（仅验证加规则后门禁仍 PASS）；`ads_publish_log` 保持 2 行不变 |

**历史门禁记录**（同生产库，加规则前的 59 条规则版本）：

| run_id | status | 规则 | 检行数 | 耗时 | 事件 |
| --- | --- | --- | --- | --- | --- |
| 1 | PASS | 59/59 | 89,304,980 | 216.74s | 首次全量门禁；`publish --run 1` → `ads_publish_log` publish_id=1 |
| 2 | PASS | 59/59 | 89,304,980 | 200.22s | §9.6 崩溃恢复重建后复跑，确认数据与 run #1 一致；`publish` 追加 publish_id=2 |

> 各规则耗时在多次运行间有小幅波动（MySQL 缓冲热身后重扫描更快，如
> `ods_source_path_unique` run #1 = 125.9s、run #3 = 95.5s）；数字以各 run 的
> `quality_check_result.duration_ms` 为准。

**耗时热点（top-8，run #3，来自 `quality_check_result.duration_ms`）**：

| 规则 | 耗时 | 说明 |
| --- | --- | --- |
| `ods_source_path_unique` | 95,454 ms | `COUNT(DISTINCT source_file, source_row_number)` 扫 23,033,672 行（双列 DISTINCT 最重） |
| `ods_batch_log_rowcount` | 50,718 ms | 三张 ODS 表按 (文件,批次) 分组计数 |
| `ods_batch_coverage` | 24,797 ms | 同上分组的 LEFT JOIN 覆盖检查 |
| `ads_funnel_conv_global_users_eq_dwd` | 9,000 ms | `COUNT(DISTINCT visitor_id)` 扫 dwd_events 139 日（新增规则，天然在对账热点区） |
| `ads_funnel_orders_eq_dwd_distinct` | 9,000 ms | `COUNT(DISTINCT transaction_id)` 扫 139 日 |
| `ods_row_key_unique` | 6,875 ms | `COUNT(DISTINCT row_key)`（BINARY(32) 单列） |
| `ods_event_type_valid` | 3,562 ms | 全表扫描 2,756,101 行 |
| `dwd_dedup_key_unique` | 3,468 ms | 扫 2,755,641 行 |

> 观察：耗时集中在 ODS 层的分组 / DISTINCT（`ods_source_path_unique` + `ods_batch_log_rowcount`
> + `ods_batch_coverage` + `ods_row_key_unique` 四条合计 ≈ 178s，占全量 224.65s 的 ~79%）；
> 跨层对账里两条 DWD/ADS 大 join（`ads_funnel_*_eq_dwd`）各 ≈ 9s，是门禁中「单条最贵」的
> 非 ODS 规则。**暂不新增投机性索引**，判断依据见 §11.1。

SQL 复核（与上面数字同源，可在任意时刻重放）：

```sql
-- 最近一次门禁的汇总
SELECT run_id, status, scope_type, window_start, window_end,
       rules_total, rules_passed, rules_failed, checks_total, started_at, finished_at
FROM quality_check_run ORDER BY run_id DESC LIMIT 3;

-- 最近一次门禁里最慢的规则（性能画像）
SELECT rule_key, duration_ms, checked
FROM quality_check_result
WHERE run_id = (SELECT MAX(run_id) FROM quality_check_run)
ORDER BY duration_ms DESC LIMIT 5;
```

### 9.5 迟到窗口（集成验证，合成 2099 数据）

`tests/integration/test_phase5_gate_incremental_mysql.py` 端到端（只在
`ecommerce_dw_test` 跑）：

- 干净 19 行样本（D1..D4，窗口 [D1,D3]）→ DWD (19,18,1,0) 三分类 → 流量
  D1=(6,4,5)、D2=(2,1,2)、D3=D4=(0,0,2)；
- 稳定态 `init_watermark` 后普通增量 = no-op；
- 门禁 PASS + 落库断言 + `publish` 成功（log=1）；
- 注入坏行（未知事件类型 + 未记录批次 + 伪造 row_count）→ 门禁 FAIL
  {`ods_batch_log_rowcount`, `ods_batch_coverage`, `ods_event_type_valid`,
  `dwd_identity`} → 陈旧 FAIL run 与新鲜门禁两路 `publish` 均被拒（log 仍 =1）；
- 删除坏行 + 复原 row_count → 门禁 PASS → `publish` 成功（log=2）；
- 迟到第二批次（v3 对 3003 的第二个 D1 view）→ `incremental` 识别 `late_dates=(D1,)`、
  只重建 D1（流量 D1=(7,4,5)，其余日不变）、水位 [D1]=新批次；随后普通增量再次 no-op；
- **迟到 = 可观测的证据**：`wm[D4](旧批次)` vs `wm[D1](新批次)`。

单位测试另覆盖 `planner.py`（frontier / 尾巴 / 窗口内迟到 / 窗口边界 inclusive /
窗口外待修 / no-op / 零批次 / backfill）与 `rules.py` 的目录形状、categories、
`date_filter_sql`、`_scope` 归一、窗口参数化与 RunStatus。

### 9.6 生产回填（`--from/--to` 深修正路径）+ 崩溃恢复实测

对已处理日期做一次「深修正回填」，走与普通增量完全相同的入口，论证：
(a) `--from/--to` 路径在生产可用；(b) 内容幂等（重建后各表行数恢复为 Phase 4
全量一致）；(c) 中途被杀后水位不动、RUNNING 遗留被 sweep、重跑即恢复。

```bash
python -m ecommerce_dw.cli incremental --from 2015-09-16 --to 2015-09-16
```

| 项 | 值 |
| --- | --- |
| 受影响业务日期 | 2015-09-16（DWS/ADS 自动补前驱 2015-09-15 → 聚合 scope 2 天） |
| DWD | build #51，`dwd_rows=11,494`（内容与全量一致，幂等重写） |
| DWS/ADS | build #5，`agg_rows=41,150`（2 天 scope，7 张表） |
| 水位 | `watermark_upserted=1`（09-16，同批次幂等 Upsert） |
| 耗时 | **238.83s**（DWD ≈ 150s + 聚合 ≈ 76s） |
| 重建后核对 | 各表行数逐一恢复为 Phase 4 全量值（`dws_user_daily` 1,649,534 / `dws_item_daily` 1,672,186 / `dws_category_daily` 94,194 / `ads_*` 139 / 1,251 / 13,900 / 278；`dwd_events` 2,755,641；`etl_watermark` 139 行），普通增量复跑 = no-op |

**过程中的一次真实崩溃恢复**：首次尝试在后台执行该回填时被外部机制中断
（系统内存压力杀掉后台 shell，并非程序错误），当时聚合 build #4 已删除 scope
行、尚未全部重插 → 留下 `dws_ads_build_log` RUNNING 遗留 + 5 张聚合表缺
09-15/09-16 行。处置：前台重跑同一命令 → builder 把 build #4 判为崩溃遗留清为
CLEANED，链上 build #5 重新完成 2 天 scope → 全表行数恢复。**这次事件正好实证了
§4 的失败安全：水位未前移、RUNNING 被 sweep、重跑自动补齐、无人工修数。**

> 说明：首次后台执行与恢复过程均无产物提交；真实落库结果以 build #5 为唯一
> 当前状态，重建后全量门禁 PASS（§9.4 附第二次门禁 run）。

## 10. 复现

1. 建控制表：`python scripts/apply_quality_ddl.py`
2. 引导水位：`python -m ecommerce_dw.cli incremental --init --to 2015-09-16`
3. 正常增量：`python -m ecommerce_dw.cli incremental`
4. 门禁：`python -m ecommerce_dw.cli quality run`
5. 发布：`python -m ecommerce_dw.cli publish`
6. 查看：`python -m ecommerce_dw.cli quality show` / `status`

## 11. 待决策事项（给评审/后续迭代）

1. **门禁耗时与索引（已解决）**：全量 60 条规则实测 224.65s（run #3；历史 run #1 =
   216.74s、run #2 = 200.22s，波动来自 MySQL 缓冲热身）。构成：ODS 层四重分组/DISTINCT
   （`ods_source_path_unique` 95.5s + `ods_batch_log_rowcount` 50.7s +
   `ods_batch_coverage` 24.8s + `ods_row_key_unique` 6.9s）合计 ≈ 178s（≈ 79%），
   另有 `ods_event_type_valid` 全表扫描 3.6s；两条 DWD/ADS 对账大 join（
   `ads_funnel_orders_eq_dwd_distinct` / `ads_funnel_conv_global_users_eq_dwd`）各 ≈ 9s，
   其余分布在后尾。
   **结论：不新增投机性索引**。日常迭代用 `quality run --dates <d1>,<d2>`（范围门禁，
   走 `date_filter` 收敛到受影响的几天，毫秒级）即可；发布前的全量门禁保留为一次性
   总闸。**何时才值得加索引**：某条规则稳定成为瓶颈且耗时 ≥ 全量 30%（≈ ≥ 67s）时，
   为它加索引——候选如 `etl_batch_log(ingest_batch_id)`、`ods_*_raw(source_file)`
   或把全表唯一性改为按新批次限扫（§9.4 观察）。
2. **`conv_global_users` 跨层对账（已解决）**：9 个漏斗指标现已全部有逐日对账规则。
   新增 **`ads_funnel_conv_global_users_eq_dwd`**——直接对 DWD 明细独立复算：该日
   `event_type='transaction' AND visitor_id IS NOT NULL` 的 DISTINCT visitor_id 数，
   必须等于 `ads_funnel_daily` 该日 `conv_global_users`（与构建口径
   `SUM(transaction_cnt>0 AND visitor_zero=0)` 同集合，但不依赖 DWS 构建产物）。
   run #3 中该规则实际执行 **PASS**（checked=139 日），与既有 8 条漏斗规则
   （`buyer_users`/`cart_users`/`view_users`/`conv_strict_users`/`orders`×DWD + 3 条 rate）
   一起并入 24 条对账规则。**`buyer_users` 不缺对账**：它已有逐日
   `ads_funnel_buyer_users_eq_dws`（且与 `conv_global_users` 构建期同集合），无需新增。
3. **README 阶段叙事（已解决）**：README 已同步 Phase 5 进度（当前状态新条目 +
   项目树 `incremental/`、`quality/`、统一 CLI + 产物快速入口 + 下一阶段编号），
   阶段编号统一为主方案（Phase 5 = 增量处理与质量门禁，Phase 6 = Airflow 编排）。
4. **`late_window_days=7` 的默认值**：产品上线后按「数据最晚可靠送达 SLA」微调；
   超出窗口的深修正目前要求人工 `--from/--to`，是否需要「窗口外自动告警/工单」?
5. **测试基础（已并入 1）**：生产大表暂不加索引；`etl_batch_log(ingest_batch_id)`、
   `ods_*_raw(source_file)` 列为「某规则耗时 ≥ 全量 30%」时的首选候选。
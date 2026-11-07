# Phase 8 性能分析、测试与交付硬化

> 本文记录 Phase 8 的性能优化、索引决策、可重复演示、全新环境部署验证与
> 容器启动硬化。所有耗时 / 行数均为本机真实执行值（Windows 11 + WSL2 Ubuntu
> 24.04，Docker Engine，MySQL 8.0.43，PostgreSQL 16.13，Airflow 2.10.5，
> Superset 4.1.2）。测试矩阵与手工样例见 `docs/test_matrix.md`。
>
> 本轮**不改** README / architecture，只新增本文件与 `docs/test_matrix.md`。

## 0. 数据规模与测量方法

生产库 `ecommerce_dw`（Phase 8 前后保持不变）：

| 表 | 行数 |
| --- | ---: |
| `ods_events_raw` | 2,756,101 |
| `dwd_events` | 2,755,641 |
| `dws_user_daily` | 1,649,534 |
| `dws_item_daily` | 1,672,186 |
| `dws_category_daily` | 94,194 |
| `ads_traffic_daily` | 139 |
| `ads_funnel_daily` | 1,251 |
| `ads_item_ranking_daily` | 13,900 |
| `ads_retention_cohort` | 278 |
| `ads_item_behavior_daily` | 139 |
| `ads_funnel_interval` | 1 |
| `etl_watermark` | 139 |
| `ads_publish_log` | 12 |

辅助规模：`ods_item_properties_raw` 20,275,902 行、`ods_category_tree_raw` 1,669 行，
三张 ODS 合计 23,033,672 行；`dwd_events_stage` 2,756,101 行。

测量方式：

- 计划：MySQL 8.0 `EXPLAIN`（传统表格），必要时 `EXPLAIN ANALYZE`；
- 耗时：同一 SQL / CLI 在本机重复执行，取稳定值；DWD / 门禁耗时来自构建日志
  的 `started_at` / `finished_at`；
- 对照：优化前数字取自 Phase 4–7 已记录的生产构建日志与本文优化前的实测，
  优化后数字为本轮同数据集实测。数据规模一致，未通过删数据“加速”。
  其中 `62.3s`（类目 as-of）、`21.7s`（门禁三表分组）与
  `18.8s → 0.08s`（已移除的函数索引）是**索引创建前的历史实测**；索引现已
  存在，当前环境不可直接复现，保留仅作优化前后对照。
- **注意**：EXPLAIN 输出里的 `rows` 是优化器估算，不是实际行数；实际行数以
  `COUNT(*)` 为准（§0 / §5）。本轮复测命令与原始输出见
  `C:\Zproject\ZClaude\ecommerce-phase8\timings_r18.log`。



## 1. 结论速览

| 项 | 优化前 | 优化后 | 说明 |
| --- | ---: | ---: | --- |
| DWD 单条 split 语句（新尾巴构建，stage 21,623） | 14.6s | 0.20s | 去掉 `SELECT DISTINCT dedup_key` 派生表，改走 UNIQUE `dedup_key` 的 `eq_ref` |
| DWD 单条 split 语句（全量 stage 2.72M） | — | 19.7s | 全量时 `ROW_NUMBER()` 窗口排序是主成本，join 已不是瓶颈 |
| 两日 DWD 重建（21,623 ODS） | 193.1s（build #53） | 178.0s（build #55） | 同 scope、同数据，整体约 −7.8% |
| DWS 类目 as-of 快照（20.3M 属性） | 62.3s | 16.2s | `(property)` 索引把实际候选从 20.3M 降到 0.79M（EXPLAIN 估算 1.71M） |
| 门禁 ODS 批次分组（3 张 ODS） | 21.7s | 4.6s | `(source_file, ingest_batch_id)` 覆盖索引 + index-only scan |
| 全量质量门禁（89,305,119 行） | 224.65s（run #3） | 209.50s（run #7） | 60/60 PASS，相同行数 |
| 区间漏斗（直接查询 DWD） | 8.7s | 0.001s | Phase 7 已物化 `ads_funnel_interval`；Phase 8 复核 |
| 商品行为（直接查询 DWD） | 10.5s | 0.001s | 物化 `ads_item_behavior_daily` |
| 商品排行（单日 top-100） | — | ~0.004s | `dws_item_daily` 单日主键扫描 + 窗口函数 |

## 2. EXPLAIN 摘录

以下为本轮在 `ecommerce_dw` 上真实执行 `EXPLAIN` 的输出摘录（完整日志见
`C:\Zproject\ZClaude\ecommerce-phase8\explain_phase8.log`）。

### Q1 DWD split：`dwd_events_stage` × `dwd_events`

```sql
SELECT s.dedup_key
FROM (
  SELECT st.dedup_key,
         ROW_NUMBER() OVER (PARTITION BY st.dedup_key
                            ORDER BY st.source_row_number) AS rn
  FROM dwd_events_stage st
  WHERE st.record_status = 'OK' AND st.build_id = 30
) s
LEFT JOIN dwd_events e ON e.dedup_key = s.dedup_key
WHERE s.rn = 1 AND e.dedup_key IS NULL;
```

```text
id | table          | type   | key                        | key_len | rows     | Extra
1  | <derived2>     | ALL    | NULL                       | NULL    | 1448532  | Using where
1  | e              | eq_ref | uk_dwd_events_dedup        | 256     | 1        | Using where; Not exists; Using index
2  | st             | ref    | idx_dwd_stage_build        | 42      | 1448532  | Using filesort
```

要点：join 侧是 `eq_ref`（UNIQUE `dedup_key`，每次 1 行，`Using index`），
stage 侧走 `idx_dwd_stage_build (build_id, record_status)`。
优化前 join 子查询 `LEFT JOIN (SELECT DISTINCT dedup_key FROM dwd_events)`
会先把 275 万个 key 物化一遍；改为直接 join UNIQUE 索引后，在新尾巴构建
（stage 21,623 行，build #55）上同一语句 **14.6s → 0.20s**，语义不变
（`rn=1` 且目标表无该 key 即为 winner）；duplicate 语句同样处理。
但全量 stage（build #30，2,719,267 行）上该语句仍约 **19.7s**，此时成本主要
来自 `ROW_NUMBER() OVER (PARTITION BY dedup_key)` 的窗口排序，join 已不是瓶颈。
因此不能把“2.7M stage 从 20s 降到 0.2s”作为结论。


### Q2 DWS 类目 as-of 快照

```sql
SELECT CAST(itemid AS UNSIGNED), CAST(`timestamp` AS UNSIGNED),
       CAST(`value` AS UNSIGNED)
FROM ods_item_properties_raw
WHERE property = 'categoryid' AND `value` REGEXP '^[0-9]+$';
```

```text
id | table                    | type | key                       | key_len | rows      | Extra
1  | ods_item_properties_raw  | ref  | idx_ods_ip_raw_property   | 514     | 1712938   | Using where
```

要点：`(property)` 索引把实际候选行从 20,275,902 降到 788,214（EXPLAIN 的
`rows=1,712,938` 是优化器估算，不是实际行数）；实测 62.3s → 16.2s。
`CAST` 只作用于已过滤行，不需要函数索引。


### Q3 门禁 ODS 批次分组

```sql
SELECT 'ods_events_raw' AS table_name, source_file, ingest_batch_id, COUNT(*) AS cnt
FROM ods_events_raw
GROUP BY source_file, ingest_batch_id;
```

```text
id | table          | type  | key                           | key_len | rows     | Extra
1  | ods_events_raw | index | idx_ods_events_raw_file_batch | 1030    | 3058729  | Using index
```

要点：`Using index` = 覆盖索引 index-only scan，不回表；三张 ODS 表同理。
这是门禁中最重的 `ods_batch_log_rowcount` / `ods_batch_coverage` 的主要成本。
单看 `ods_events_raw` 为 0.34s；三张 ODS 合并分组实测 21.7s → 4.6s。


### Q4 门禁账实对账 join

```text
id | table                  | type  | key                           | rows      | Extra
1  | b                      | ALL   | NULL                          | 5         | Using where
1  | <derived3>             | ALL   | NULL                          | 23198812  | Using where; Using join buffer (hash join)
3  | ods_events_raw         | index | idx_ods_events_raw_file_batch | 3058729   | Using index
4  | ods_item_properties_raw| index | idx_ods_ip_raw_file_batch     | 20138414  | Using index
5  | ods_category_tree_raw  | index | idx_ods_ct_raw_file_batch     | 1669      | Using index
```

要点：三张 ODS 的分组子查询全部 index-only；`etl_batch_log` 只有 5 行，
hash join 合理。上表 `rows` 均为优化器估算；实际三张 ODS 合计 23,033,672 行
（属性表 20,275,902 行）。若没有覆盖索引，2000 万行的属性表要回表，门禁会明显更慢。


### Q5 商品排行（单日 top-100）

```sql
SELECT event_date_utc, item_id, rank_no FROM (
  SELECT t.event_date_utc, t.item_id,
         ROW_NUMBER() OVER (
           PARTITION BY t.event_date_utc
           ORDER BY t.view_cnt DESC, t.add_to_cart_cnt DESC,
                    t.transaction_cnt DESC, t.item_id DESC) AS rank_no
  FROM dws_item_daily t
  WHERE t.event_date_utc BETWEEN '2015-09-01' AND '2015-09-30'
) ranked
WHERE ranked.rank_no <= 100;
```

```text
id | table   | type  | key     | rows    | Extra
1  | <derived2> | ALL | NULL  | 372036  | Using where
2  | t       | range | PRIMARY | 372036  | Using where; Using filesort
```

要点：按日主键区间扫描 + 窗口排序，单日实测约 0.004s（30 天窗口约 0.093s）；
没有为排行新增索引（`PRIMARY (event_date_utc, item_id)` 已足够，新增索引收益
无法证明）。


### Q6 区间漏斗（直接聚合 DWD）

```text
id | table        | type | key | rows     | Extra
1  | <derived2>   | ALL  | NULL| 367582   | NULL
2  | dwd_events   | ALL  | NULL| 2722837  | Using where; Using temporary
```

要点：窗口覆盖 2015-04-01–2015-09-30（接近全量），优化器放弃日期索引走全表
+ 临时表，实测 8.7s（EXPLAIN 的 `rows=2,722,837` 是估算，实际 2,755,641 行）。
该查询在 Superset 看板每次刷新都会触发，因此 Phase 7 已物化为
`ads_funnel_interval` 单行表；Phase 8 复核冷启动查询约 **0.001s**。
同理 `ads_item_behavior_daily` 直接聚合 10.5s，物化后约 0.001s。
结论：这类查询应继续“预计算 + 读表”，不靠加索引解决。


## 3. 索引决策

本轮新增 **5 个索引**，全部先在 `EXPLAIN` 证明被使用，再落到
`sql/ddl/*.sql`；已存在的库由幂等脚本 `scripts/apply_perf_indexes.py`
补建（检查 `information_schema.statistics`，重复执行不报错）。

| # | 表 | 索引 | 列 | 用途 / EXPLAIN 证据 |
| --- | --- | --- | --- | --- |
| 1 | `ods_events_raw` | `idx_ods_events_raw_file_batch` | `(source_file, ingest_batch_id)` | 门禁批次分组 index-only；之前全表回表 |
| 2 | `ods_item_properties_raw` | `idx_ods_ip_raw_property` | `(property)` | 类目 as-of 只取 `categoryid`，实际 20,275,902 → 788,214 行（EXPLAIN 估算 1,712,938） |
| 3 | `ods_item_properties_raw` | `idx_ods_ip_raw_file_batch` | `(source_file, ingest_batch_id)` | 门禁账实对账 index-only |
| 4 | `ods_category_tree_raw` | `idx_ods_ct_raw_file_batch` | `(source_file, ingest_batch_id)` | 同上（表小，但保持三表一致） |
| 5 | `dwd_events_stage` | `idx_dwd_stage_build` | `(build_id, record_status)` | split 只读当前 build 的 OK/REJECT 行 |

全部使用 `ALGORITHM=INPLACE, LOCK=NONE`（在 MySQL 8.0.43 上成功；函数索引那次
不支持 `LOCK=NONE`，见下），不改变表语义、事务边界或幂等逻辑。新增索引只增加
写入维护成本，5 个索引均为低频写 / 高频读的 ODS 与 stage 表，写入放大可接受。

### 被否决的方案

- **`ods_events_raw` 函数索引 `(source_file, CAST(timestamp AS UNSIGNED))`**：
  历史实测（索引当时仍存在）DWD 按日期读取 ODS 的查询从 18.8s 降到 0.08s；
  该索引已移除，当前不可复现。MySQL 会在 INSERT 时校验函数索引；ODS 契约允许
  保留脏行供 DWD 拒绝，插入 `timestamp='oops'` 会直接报
  **error 3751 “Data truncated for functional index”**，破坏“原始层不丢脏行”
  的硬约束，因此从生产库和 DDL 中移除。
- **给 `dwd_events` 加日期 + 事件类型复合索引**：Q6 的窗口接近全量，优化器
  仍会选全表；加索引只会增加写入成本。正确解法是物化，不是索引。
- **给 `ads_item_ranking_daily` 加排序索引**：表仅 13,900 行，单日查询已约
  0.004s，无收益。


## 4. SQL 重写（非索引）

`src/ecommerce_dw/dwd/builder.py` 的两条 split 语句把：

```sql
LEFT JOIN (SELECT DISTINCT dedup_key FROM dwd_events) e
       ON e.dedup_key = s.dedup_key
```

改为：

```sql
LEFT JOIN dwd_events e ON e.dedup_key = s.dedup_key
```

`dwd_events.dedup_key` 本身是 UNIQUE，`DISTINCT` 派生表只是把 275 万 key 物化
一次再做 join；直接 join 让优化器走 `eq_ref`。在新尾巴构建（stage 21,623 行）
上同一语句实测 **14.6s → 0.20s**；winner / duplicate 两条语句语义不变。
但全量 stage（2,719,267 行）上仍约 19.7s，窗口排序才是主成本。该改动已由
Phase 8 E2E 手工样例与全量质量门禁验证：去重数、拒绝数、账实一致均不变。


## 5. DWD / 门禁实测

### DWD 两日重建（同 scope 对照）

```text
build #53  DATES 2015-09-16,2015-09-17  ods=21,623 ok=21,620 dup=3 reject=0  193.1s  (优化前)
build #55  DATES 2015-09-16,2015-09-17  ods=21,623 ok=21,620 dup=3 reject=0  178.0s  (优化后)
```

单日 build #54（优化后）：11,495 ODS → 11,494 ok + 1 dup + 0 reject，126.6s。
两日整体约 −7.8%；新尾巴构建的单条 split 收益明显（14.6s → 0.20s），但全量
stage 的 split 仍约 19.7s，且 DWD 构建还包含 staging 写入、哈希计算、日志与
提交，因此整体不会同比例下降。

### 全量质量门禁

```text
优化前 run #3: PASS 60/60  89,305,119 rows  224.65s
优化后 run #7: PASS 60/60  89,305,119 rows  209.50s
```

同规则数、同行数，约 −6.7%。最重的三条规则（`ods_source_path_unique`、
`ods_batch_log_rowcount`、`ods_batch_coverage`）都受益于 ODS 覆盖索引。

### DWS/ADS 独立校验

```bash
python -m ecommerce_dw.dws.cli verify
# verify    : 2094 checks across 139 days -> ALL MATCH
```

2,094 项检查、139 个业务日，全部 MATCH；该结果不依赖本文的性能改动。


## 6. 可重复演示（`scripts/demo_phase8.py`）

一条命令完成 8 步链路，**只读生产库**，在独立 `*_test` schema 中跑真实 CLI：

```bash
python scripts/demo_phase8.py
```

| 步骤 | 内容 | 最终实测 |
| --- | --- | ---: |
| 1 | MySQL / PostgreSQL / Airflow / Superset 健康检查 | 0.27s |
| 2 | 生产 13 张表行数基线快照 | 0.57s |
| 3 | 建独立 schema + 应用 23 个 DDL | 0.64s |
| 4 | 装小样例 → `incremental`（D1/D2 新尾巴） | 0.59s |
| 5 | 迟到批次 → 自动重算 D1 → 稳定态重跑（幂等） | 0.78s |
| 6 | 60 条门禁 → `publish` → `verify` + 指标打印 | 0.74s |
| 7 | 生产行数基线复核 | 0.64s |
| 8 | 看板链接 + 删除演示 schema | 0.00s |

最终运行输出（`C:\Zproject\ZClaude\ecommerce-phase8\demo_run_final.log`）关键行：

```text
quality gate: run #1 PASS 60/60 rules passed (429 rows checked)
publish ok: publish_id=1 scope=FULL dates=(full) quality_run=1
verify: 30 checks -> ALL MATCH
traffic: 2015-09-16=5/2/4, 2015-09-17=3/3/4
funnel: ... 2015-09-16:view_users=2.000000 ...
retention D1: 2015-09-16=2/4 (0.500000), 2015-09-17=0/2 (0.000000)
after: all 13 production tables match the baseline
dropped demo schema ecommerce_dw_phase8_demo_test
```

整条链路约 **4.2s**（步骤耗时之和），远低于 5 分钟演示预算；失败时脚本以非 0
退出并保留错误输出。可用 `--keep` 保留 schema 供现场检查，`--skip-http` 跳过
Web 健康探测。生产行数在演示前后逐表比对，13 张表全部一致。

## 7. Docker Compose / 启动顺序 / 初始化硬化

### 7.1 启动顺序

- `mysql` / `postgres` 保留原有 healthcheck；
- `airflow-scheduler`、`airflow-webserver` 新增显式 `depends_on`：
  `postgres: service_healthy`、`mysql: service_healthy`、
  `airflow-init: service_completed_successfully`；
- `superset` 维持 `postgres healthy` + `superset-init completed_successfully`。
- Airflow 服务把 `tests/` 只读挂载到 `/opt/ecommerce_dw/tests`，DAG 契约测试
  可在真实调度器运行时执行；Airflow 镜像安装 `-e "/opt/ecommerce_dw[dev]"`
  以便容器内有 pytest。

### 7.2 MySQL 初始化幂等

`docker/mysql/init/01_init.sql` 替换为 `01_init.sh`：

```bash
db="${MYSQL_DATABASE:?MYSQL_DATABASE must be set}"
root_pw="${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD must be set}"
mysql --protocol=socket -uroot -p"${root_pw}" <<SQL
CREATE DATABASE IF NOT EXISTS \`${db}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
ALTER DATABASE \`${db}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET NAMES utf8mb4;
SQL
```

不再硬编码 `ecommerce_dw`，同一镜像可用于全新环境的独立库名；`IF NOT EXISTS`
+ `ALTER` 保证重复执行安全。

### 7.3 全新环境覆盖文件

`docker-compose.fresh.yml` 只覆盖容器名和命名卷名，不改端口 / 凭据 / 环境变量；
端口与库名全部由外部 `--env-file` 提供，生产栈（项目名
`ecommerce-data-platform`、默认端口 3306/5432/8080/8088）完全不受影响。

## 8. 全新环境部署验证（真实记录）

目标：用独立 compose 项目、独立端口 / 卷 / 库名，从**空库**按仓库文档启动，
只装小样例（不重载 23M 行），验证初始化、调度、看板、权限、清理。临时环境
文件放在仓库外：`C:\Zproject\ZClaude\ecommerce-phase8\fresh.env`（不入库）。

| 项 | 生产 | 全新环境 |
| --- | --- | --- |
| compose 项目 | `ecommerce-data-platform` | `ecommerce-fresh` |
| MySQL | 3306 / `ecommerce_dw` | 13306 / `ecommerce_dw_fresh` |
| PostgreSQL | 5432 / `meta` | 15432 / `meta_fresh` |
| Airflow UI | 8080 | 18080 |
| Superset UI | 8088 | 18088 |
| Superset 元库 | `superset_meta` | `superset_meta_fresh` |
| 命名卷 | 生产卷 | `ecommerce-fresh-*` |

### 8.1 启动与初始化

```bash
docker compose -p ecommerce-fresh \
  --env-file /mnt/c/Zproject/ZClaude/ecommerce-phase8/fresh.env \
  -f docker-compose.yml -f docker-compose.fresh.yml up -d mysql postgres
# 等待 mysql/postgres healthy
python scripts/apply_ods_ddl.py
python scripts/apply_dwd_ddl.py
python scripts/apply_dws_ads_ddl.py
python scripts/apply_quality_ddl.py
python scripts/apply_perf_indexes.py
# 装 14 行事件 + 11 行属性小样例，跑 incremental
python -m ecommerce_dw.cli incremental
# 移除演示用非法 purchase 行（DWD reject 证据之后）
python -m ecommerce_dw.cli quality run
python -m ecommerce_dw.cli publish --run 1
python -m ecommerce_dw.cli verify
python scripts/apply_superset_grants.py
docker compose ... up -d
```

真实输出摘要：

```text
ecommerce-fresh-mysql      Up (healthy)
ecommerce-fresh-postgres   Up (healthy)
ecommerce-fresh-airflow-init      Exited (0)
ecommerce-fresh-superset-init     Exited (0)
ecommerce-fresh-airflow-scheduler Up (healthy)
ecommerce-fresh-airflow-webserver Up (healthy)
ecommerce-fresh-superset          Up (healthy)

incremental: pipeline=incremental ... duration=0.20s
quality gate: run #1 PASS 60/60 rules passed (412 rows checked) duration=0.05s
publish ok: publish_id=1 scope=FULL dates=(full) quality_run=1
verify: 30 checks -> ALL MATCH
superset-grants OK  (5 views + 11 SELECT grants)
airflow /health HTTP 200
superset /health HTTP 200
airflow dags list -> ecommerce_daily_pipeline, phase6_retry_demo
Superset 4.1.2
dashboards: 3   datasets: 15
```

Airflow `DagBag` 测试在全新 scheduler 内同样 **5 passed in 1.30s**；Superset
由 `scripts/superset_setup.py` 从零重建 3 个看板；只读账号
`superset_ro` 对新库 `ecommerce_dw_fresh` 的 `ads_traffic_daily` 冒烟查询返回
`(2015-09-16, 4, 2, 4)`、`(2015-09-17, 3, 3, 4)`。

### 8.2 验证中发现并修复的问题

1. **`scripts/superset_setup.py` 数据集 schema 硬编码 `"ecommerce_dw"`**。
   在全新库 `ecommerce_dw_fresh` 上，数据库连接本身正常，但创建 dataset 时
   Superset 去连 `ecommerce_dw`，报
   `Access denied for user 'superset_ro'@'%' to database 'ecommerce_dw'`。
   已改为 `env("MYSQL_DATABASE", "ecommerce_dw")`，PUT / POST 两处都改。
   这是本轮唯一因“全新环境”暴露的代码问题；生产库名恰好等于默认值，所以
   之前未显现。
2. **空元库首启时 `superset-init` 导入 bundle 会校验失败**。bundle 里的
   dataset 引用数据库连接，而首启时连接行尚不存在；脚本按设计
   `|| echo "[warning] import failed"` 继续，随后由
   `scripts/superset_setup.py` 完成首次 bootstrap。全新环境验证确认该路径
   可用，且重复执行 setup 幂等（第二次运行全部走 PUT / 更新分支）。
3. **`fix_database_password.py` 的账号名必须与 `apply_superset_grants.py`
   一致**。后者当前固定创建 `superset_ro`（生产既有账号名），因此全新环境
   的 `MYSQL_RO_USER` 也使用 `superset_ro`；不同 MySQL 实例之间不冲突。
   已用容器内手动执行验证：
   `[fix-db] updated 'ecommerce_dw (Superset RO)' URI host=mysql:3306`。

### 8.3 清理

```bash
docker compose -p ecommerce-fresh ... down -v
```

真实输出：7 个容器全部 `Removed`，4 个命名卷
（`ecommerce-fresh-mysql-data` / `postgres-data` / `airflow-logs` /
`superset-home`）与 `ecommerce-fresh_ecommerce-net` 全部 `Removed`。清理后：

- `docker ps -a --filter name=ecommerce-fresh` 为空；
- `docker volume ls --filter name=ecommerce-fresh` 为空；
- 生产 5 个容器仍 healthy；
- 生产 13 张表行数与 §0 基线逐表一致（`ALL MATCH`）。

## 9. 复现命令

```bash
# 全量测试
python -m pytest
docker compose exec -T airflow-scheduler python -m pytest tests/dag
ruff check . && ruff format --check .

# 索引（幂等；已有库补建）
python scripts/apply_perf_indexes.py --dry-run
python scripts/apply_perf_indexes.py

# 演示（生产只读，独立 *_test schema）
python scripts/demo_phase8.py
python scripts/demo_phase8.py --keep --skip-http

# 全新环境（详细命令见 §8；临时 env 不入库）
docker compose -p ecommerce-fresh --env-file <fresh.env> \
  -f docker-compose.yml -f docker-compose.fresh.yml up -d
```

## 10. 交付边界

- Phase 8 工程线当时未改 README / architecture / 既有设计文档；V1.0 文档化由后续文档提交完成；
- 未把 `.env`、临时 env、数据文件、Docker 卷写入仓库；
- 镜像仍固定 tag（`mysql:8.0.43`、`postgres:16.13`、
  `apache/airflow:2.10.5-python3.11`、`apache/superset:4.1.2`），未使用
  `:latest`；
- 测试只写 `*_test` 或一次性独立 schema；生产库 `ads_publish_log` 保持 12 行，
  未新增测试发布；
- 本轮不 push，只在本地做语义化 commit。

# 运行与故障恢复手册（V1.0）

> 面向日常运维：调度巡检、手动回填、批次失败清理、质量门禁 FAIL、publish 拒绝、
> DAG 重跑、看板刷新、备份/恢复、WSL2 常见问题。部署见 `docs/deployment.md`；
> 设计细节见 `docs/incremental_and_quality.md`、`docs/airflow_design.md`、
> `docs/superset_design.md`。
>
> 约定：容器命令在 WSL 执行；`REPO=/mnt/c/Zproject/data-engineering/ecommerce-data-platform`。
> 为简洁，下文用 `docker compose ...` 表示：
> `wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose ..."`。

## 1. 运行模型

| 入口 | 触发方式 | 作用范围 | 典型耗时 |
| --- | --- | --- | --- |
| Airflow `ecommerce_daily_pipeline` | 每日 04:00 UTC（`0 4 * * *`），`catchup=False`，`max_active_runs=1` | 水位线驱动的新尾巴 + 7 天窗口内迟到 | 小窗口约 11 分钟（实测） |
| `python -m ecommerce_dw.cli incremental` | 手工 | 同上；`--from/--to` 为深修正 | 普通增量 255.41s；深回填 238.83s（实测） |
| `python -m ecommerce_dw.cli quality run` | 手工/Airflow | FULL 或 DATES | 全量约 209.50s（run #7） |
| `python -m ecommerce_dw.cli publish` | 手工/Airflow | FULL 或 DATES，必须引用 PASS run 或现跑门禁 | 秒级 |
| `python scripts/superset_setup.py` | 手工 | 看板/数据集/图表 upsert | 分钟级 |
| `python scripts/demo_phase8.py` | 手工 | 隔离 `*_test` 端到端演示，生产只读 | 约 4.2s |

**幂等原则**：ODS 按文件 SHA-256 + `row_key`；DWD/DWS/ADS 按 scope 删除重插；
增量成功才推水位；稳定态重跑 = 合法 no-op，不新增 `ads_publish_log`。

## 2. 日常巡检

### 2.1 容器与端口

```bash
docker compose ps
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8088/health
python scripts/check_db_health.py
```

预期：mysql/postgres/scheduler/webserver/superset 均 healthy；两个 init 容器
`Exited (0)`；健康检查退出码 0。

### 2.2 Airflow 运行历史

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-runs -d ecommerce_daily_pipeline
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  ecommerce_daily_pipeline <run_id>
```

### 2.3 控制表巡检 SQL

在 MySQL 业务库执行（可用任意客户端；以下为示例）：

```sql
-- 最近 10 次 DAG 运行的规划与收尾
SELECT run_id, dag_run_id, mode, window_start, window_end, dwd_dates, agg_dates,
       status, published, publish_id, quality_run_id, error_summary,
       started_at, finished_at
FROM airflow_pipeline_run
ORDER BY started_at DESC
LIMIT 10;

-- 最近 5 次门禁
SELECT run_id, status, scope_type, dates_csv, rules_total, rules_passed,
       rules_failed, checks_total, started_at, finished_at
FROM quality_check_run
ORDER BY run_id DESC
LIMIT 5;

-- 最近 10 次发布（只增不删）
SELECT publish_id, scope_type, dates_csv, quality_run_id, quality_status, published_at
FROM ads_publish_log
ORDER BY publish_id DESC
LIMIT 10;

-- 水位前沿与日期数
SELECT pipeline, COUNT(*) AS dates, MIN(event_date_utc) AS oldest,
       MAX(event_date_utc) AS newest, MAX(max_ingest_batch) AS max_batch
FROM etl_watermark
GROUP BY pipeline;
```

也可用 CLI：

```bash
python -m ecommerce_dw.cli status
python -m ecommerce_dw.cli quality show
```

### 2.4 水位与发布健康判断

- `etl_watermark` 应覆盖 139 个业务日期；`MAX(event_date_utc)` 为前沿。
- `airflow_pipeline_run.status` 不应长期停留在 `RUNNING`（超过单次运行上限）。
- `quality_check_run` 最新一轮应为 `PASS`；若最新为 `FAIL`，先处理再发布。
- `ads_publish_log` 只增不删；发布号回退/缺失说明有人手工改表，需审计。

## 3. 日常调度与手动回填

### 3.1 稳定态复跑（无新数据）

```bash
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline
```

预期：`register_batch` 规划 `dwd_dates=(none)`；各构建任务打印 no-op 原因并 exit 0；
`quality_gate` 跳过；`publish_success` 输出 `SKIPPED`；`ads_publish_log` 不增长；
`airflow_pipeline_run` 新增 `SUCCESS / SKIPPED` 一行。

### 3.2 Airflow 显式回填（FORCE）

```bash
# 区间
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline \
  --conf '{"from": "2015-09-16", "to": "2015-09-17"}'

# 或显式日期列表
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline \
  --conf '{"dates": "2015-09-16,2015-09-17"}'
```

FORCE 无视水位，重建区间内 ODS 有数据的日期；同一区间重复回填 = 再一次幂等重建
并**再多写一条**发布日志（业务表行数不变，`ads_publish_log` +1）。

### 3.3 CLI 回填（不经 Airflow）

```bash
# 普通增量（水位线驱动）
python -m ecommerce_dw.cli incremental
python -m ecommerce_dw.cli incremental --dry-run      # 只规划

# 深修正（历史区间）
python -m ecommerce_dw.cli incremental --from 2015-09-16 --to 2015-09-16

# 仅重建 DWD 或 DWS/ADS 的指定日期
python -m ecommerce_dw.dwd.cli build --dates 2015-09-16
python -m ecommerce_dw.dws.cli build --dates 2015-09-15,2015-09-16
```

> `incremental` 会自动为 DWS/ADS 补“留存前驱日”（例如重建 09-16 会带上 09-15），
> 这是 `agg_dates = dwd_dates ∪ 留存前驱日` 的设计，不是错误。

### 3.4 回填后的必做核对

```bash
python -m ecommerce_dw.cli verify
python -m ecommerce_dw.dwd.cli verify
python -m ecommerce_dw.cli status
```

- DWD 恒等式：`ods = ok + dup + reject`。
- ADS `verify`：2,094 checks ALL MATCH（全量时）。
- 水位：受影响日期已前推；稳定态复跑应为 no-op。

## 4. 批次失败清理

### 4.1 ODS 批次（`etl_batch_log`）

**症状**：`status=PENDING` 长期不结束、`FAILED`、行数与源文件不符、`validate_ods` 失败。

```bash
python -m ecommerce_dw.ingestion.cli status
python -m ecommerce_dw.ingestion.cli cleanup --batch <batch_id>
python scripts/load_all_ods.py
```

- `cleanup --batch` 删除该批次已写入的 ODS 行并把批次置 `CLEANED`；随后重新装载。
- 同一文件 SHA-256 已 LOADED 时 `load_all_ods.py` 会 skip；若需强制重装，先 cleanup
  再装载（不要直接改 `etl_batch_log`）。
- 真实案例：events 早期 PENDING 批次 #16 清理后 0 行残留；part1 进程被杀后跨进程
  续传，5.6M 已提交行保留、零重写（`docs/ods_design.md` §6.4/§6.5）。
- 装载器对 MySQL 断连自动重连续传；不要因为容器短暂重启就 cleanup。

### 4.2 DWD 构建（`dwd_build_log`）

**症状**：`status=RUNNING` 残留（进程被杀）、`FAILED`、`build` 报“已有未完成构建”。

```bash
python -m ecommerce_dw.dwd.cli status
python -m ecommerce_dw.dwd.cli build --force          # 复位失败/崩溃构建后重建
python -m ecommerce_dw.dwd.cli cleanup --build <build_id>  # 清掉该构建产物并置 CLEANED
python -m ecommerce_dw.dwd.cli verify
```

- `--force` 会丢弃同 scope 的失败/崩溃构建并干净重建；适用于“构建中断、stage 已写一半”。
- 真实案例：全量 build #30 的 staging pass 在 98% 被内存回收中断，同一命令断点续跑
  自动接续完成；按日 build #48 验证 DATES 重建幂等（13,682 行不变）。
- 不要手工删 `dwd_events` 行；用 `cleanup --build` 或 `--force`。

### 4.3 DWS/ADS 构建（`dws_ads_build_log`）

**症状**：`RUNNING` 残留、聚合表缺日期、`build` 报失败构建。

```bash
python -m ecommerce_dw.dws.cli status
python -m ecommerce_dw.dws.cli build --force
python -m ecommerce_dw.dws.cli cleanup --build <build_id>
python -m ecommerce_dw.cli verify
```

- DWS builder 会把崩溃遗留的 `RUNNING` 构建自动 sweep 为 `CLEANED` 并重跑整个 scope
  （见 `src/ecommerce_dw/dws/builder.py`）；`--force` 用于显式复位失败构建。
- 真实案例：一次后台深回填被系统内存压力杀掉，聚合 build #4 已删 scope 行、未全部
  重插；前台重跑同命令，builder 判为崩溃遗留并清为 `CLEANED`，build #5 完成 2 天
  scope，全表行数恢复（`docs/incremental_and_quality.md` §9.6）。

### 4.4 Airflow 控制批次（`airflow_pipeline_run`）

**症状**：DAG 失败后控制行仍 `RUNNING`，或需要手工收尾。

```sql
SELECT run_id, dag_run_id, mode, window_start, window_end, status,
       published, publish_id, quality_run_id, error_summary
FROM airflow_pipeline_run
WHERE status = 'RUNNING'
ORDER BY started_at DESC;
```

```bash
# 手工收尾（控制面操作，不动业务数据）
python -m ecommerce_dw.cli batch-finish --batch <batch_id> --status FAILED \
  --error "manual recovery: <原因>"
```

正常路径由 DAG 的 `on_failure_callback` 写入 `FAILED` 与 `error_summary`；手工
`batch-finish` 仅用于回调未执行/需要人工关闭的残留批次。收尾后再重跑 DAG。

## 5. 质量门禁 FAIL 处置

### 5.1 定位

```bash
python -m ecommerce_dw.cli quality show
python -m ecommerce_dw.cli quality show --run <RUN_ID>
```

或 SQL：

```sql
SELECT rule_key, category, status, checked, failed_rows, detail, duration_ms
FROM quality_check_result
WHERE run_id = <RUN_ID>
ORDER BY status DESC, duration_ms DESC;

SELECT run_id, status, scope_type, dates_csv, rules_total, rules_passed,
       rules_failed, checks_total, started_at, finished_at
FROM quality_check_run
WHERE run_id = <RUN_ID>;
```

门禁是**硬失败**：任一规则 FAIL → `quality_check_run.status='FAIL'` → CLI exit 2 →
`publish` 拒绝。**不要**为了发布去改 `quality_check_run` / `quality_check_result`。

### 5.2 常见规则与处置

| 规则/类别 | 典型原因 | 处置 |
| --- | --- | --- |
| `dwd_identity`（跨层对账） | ODS/DWD 行数不满足 `ODS = DWD + dup + reject` | 重建受影响日期 DWD；检查 reject 是否异常增长 |
| `ods_batch_log_rowcount` / `ods_batch_coverage` | `etl_batch_log.row_count` 与表内实际行数不符；批次缺失/孤儿行 | 用 `ingestion.cli status` 核对；必要时 cleanup 后重装；不要手工改 row_count |
| `ods_event_type_valid` | ODS 出现契约外事件类型（如 `purchase`） | 查来源文件/入库批次；脏数据应从 ODS 清理或走 DWD reject 后重跑 |
| `ods_source_path_unique` / `ods_row_key_unique` | 重复导入/主键冲突 | 检查 `(source_file, source_row_number)` 与 `row_key`；用批次审计定位 |
| `dwd_dedup_key_unique` | DWD 去重失效/重复构建 | 检查 `dwd_build_log` 是否有残留构建；`build --force` 重建 scope |
| ADS 对账类（`ads_*_eq_dwd`） | 聚合口径/构建中断导致 ADS 与 DWD 不一致 | 重建 DWS/ADS 受影响日期；`verify` 复核 |
| 范围类（日期窗口） | 日期越界/部分日混入 | 检查 scope 日期集合；使用完整窗口或显式 DATES |

### 5.3 修复后重跑

```bash
# 修数据（ODS 重装 / DWD 重建 / DWS-ADS 重建）后，重新跑门禁
python -m ecommerce_dw.cli quality run
# 或只对受影响日期
python -m ecommerce_dw.cli quality run --dates 2015-09-16,2015-09-17
```

每次 `quality run` 产生新的 `run_id`；旧 FAIL 记录保留作审计。发布必须引用**新的 PASS**
run（或让 `publish` 现跑门禁）。

> 若确认是规则实现/口径问题而非数据问题，属于代码变更，不在运维手工修数范围；
> 记录规则键、run id、失败样本，转研发处理。

## 6. publish 拒绝处置

`publish` 只在两种情况下写 `ads_publish_log`：引用的 run 存在且 `PASS`，且作用域匹配；
否则 exit 2、**不写任何行**。

| 拒绝原因 | 触发条件 | 处置 |
| --- | --- | --- |
| `run #N does not exist` | `--run` 指向不存在的门禁 | 先 `quality run`，用输出的 run id |
| `status=FAIL (not PASS)` | 引用了 FAIL run | 修复数据后重跑门禁，引用新 PASS run |
| `requested FULL publish but gate run scope_type='DATES'` | FULL 发布背靠 DATES 门禁 | 跑一次 FULL `quality run`（不带 `--dates`），再 `publish --run <FULL_RUN>` |
| `requested DATES ... but gate run scope_type='FULL'` | DATES 发布背靠 FULL 门禁 | 跑同日期集合的 `quality run --dates ...`，再引用它 |
| `dates do not match` | DATES 发布与门禁日期集合不一致（窄/宽/异集合） | 让 `--dates` 与门禁 `dates_csv` **完全一致**（顺序无关，去重排序后比较） |

排查 SQL：

```sql
SELECT run_id, status, scope_type, dates_csv
FROM quality_check_run
WHERE run_id = <RUN_ID>;

SELECT publish_id, scope_type, dates_csv, quality_run_id, published_at
FROM ads_publish_log
ORDER BY publish_id DESC
LIMIT 5;
```

正确示例：

```bash
# FULL：FULL 门禁 -> FULL 发布
python -m ecommerce_dw.cli quality run
python -m ecommerce_dw.cli publish --run <FULL_RUN_ID>

# DATES：同日期集合门禁 -> 同日期集合发布
python -m ecommerce_dw.cli quality run --dates 2015-09-16,2015-09-17
python -m ecommerce_dw.cli publish --dates 2015-09-16,2015-09-17 --run <DATES_RUN_ID>
```

> `ads_publish_log` 只增不删；不要删除/改写历史行（包括 publish_id 5–9 的历史证据发布）。

## 7. DAG 重跑与失败恢复

### 7.1 同逻辑日期重跑

```bash
docker compose exec airflow-scheduler airflow dags trigger ecommerce_daily_pipeline
```

- 同逻辑日期会复用/新建控制批次；若水位已消费且 ODS 无新批次，全程合法 no-op。
- `max_active_runs=1`：上一次 run 未结束时，新触发会排队；先确认没有卡住的 run。

### 7.2 失败任务重跑

```bash
# 查看失败任务
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  ecommerce_daily_pipeline <run_id>

# 清除指定任务状态后重跑（也可在 UI 里 Clear）
docker compose exec airflow-scheduler airflow tasks clear \
  ecommerce_daily_pipeline -t build_dwd -y -d
```

- 构建任务本身幂等；清除后重跑不会重复写业务行。
- 若失败发生在 `quality_gate`，修复数据后清除 `quality_gate` / `publish_success` 重跑。
- 若失败发生在 `register_batch`（确定性任务，`retries=0`），先修输入/环境，再触发新 run。

### 7.3 DAG 暂停/恢复

```bash
docker compose exec airflow-scheduler airflow dags pause ecommerce_daily_pipeline
docker compose exec airflow-scheduler airflow dags unpause ecommerce_daily_pipeline
```

维护窗口、备份、升级前先 pause；完成后 unpause。

### 7.4 重试语义

- 非确定性任务 `retries=2`、`retry_delay=2m`、`execution_timeout=3h`；`start/register_batch/publish_success` 为 `retries=0`。
- `retries` 只兜底瞬态故障，不承担数据正确性；正确性由幂等重建 + 60 条门禁承担。

## 8. 看板刷新与 Superset 运维

### 8.1 数据刷新

```bash
# 发布一个新批次（必须引用 PASS 且作用域匹配的门禁）
python -m ecommerce_dw.cli publish --dates 2015-09-16,2015-09-17 --run <DATES_RUN_ID>
```

- 发布写 `ads_publish_log`；`v_ads_publish_status` 的 `publish_count` +1；商品分析看板
  “累计发布次数” KPI +1。
- Superset 图表直查 ADS/视图；当前未配置结果缓存，发布后刷新页面即可看到变化。
  若后续启用缓存，需在 Superset 数据集/图表层清理对应缓存键。

### 8.2 看板健康检查

```bash
python scripts/superset_evidence.py       # 5 项关键指标 vs 直连 SQL
python scripts/superset_ui_evidence.py    # 8 张截图 + 硬断言（需 Playwright）
```

- `superset_evidence.py` 期望 5 项 ALL MATCH（PV 2,664,218、区间 UV 1,396,937、
  2015-07-26 DAU 17,516、严格转化率 0.008、2015-07-26 订单 101）。
- `superset_ui_evidence.py` 任何图表出现 `Waiting on` / `No results` / `Timeout` /
  `trouble loading` / HTTP 错误 → exit 1；有界轮询，不会无界等待。

### 8.3 看板重建

```bash
# 正常：幂等 upsert + 导出包
python scripts/superset_setup.py

# 空元库/元数据损坏：重建 superset-init 后再 setup
docker compose up -d --force-recreate superset-init && docker compose up -d superset
python scripts/apply_superset_grants.py
python scripts/superset_setup.py
```

- 看板包在 `dashboards/exports/`（charts 21 / dashboards 3 / datasets 12 / databases 1）。
- `superset-init` 的导入是幂等的；重复导入 `dashboard_slices` 不翻倍。
- 只读账号 `superset_ro` 由 `apply_superset_grants.py` 幂等维护；不要给它加写权限。

## 9. 备份与恢复

> 以下为建议流程（命令可执行；本仓库未把备份纳入自动化测试）。生产环境应定期演练恢复。

### 9.1 备份前

```bash
docker compose exec airflow-scheduler airflow dags pause ecommerce_daily_pipeline
```

暂停调度，避免备份期间有构建写入；确认没有 RUNNING 批次。

### 9.2 逻辑备份

```bash
# 业务数仓（MySQL）——容器内读取 MYSQL_* 环境变量
docker compose exec -T mysql sh -c \
  'exec mysqldump -uroot -p"$MYSQL_ROOT_PASSWORD" \
   --single-transaction --routines --triggers --events "$MYSQL_DATABASE"' \
  > ecommerce_dw_$(date +%F).sql

# Airflow 元库（PostgreSQL）
docker compose exec -T postgres sh -c \
  'exec pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' \
  > meta_$(date +%F).dump

# Superset 元库（PostgreSQL，独立库；默认 superset_meta，自定义 SUPERSET_META_DB 时替换）
docker compose exec -T postgres sh -c \
  'exec pg_dump -U "$POSTGRES_USER" -d superset_meta -Fc' \
  > superset_meta_$(date +%F).dump
```

> `postgres` 服务容器只注入 `POSTGRES_*`，不注入 `SUPERSET_META_DB`；若改过 Superset
> 元库名，把上面的 `superset_meta` 换成实际库名（恢复命令同理）。

### 9.3 物理卷快照（可选）

```bash
docker run --rm \
  -v ecommerce-data-platform_mysql_data:/data:ro \
  -v "$PWD":/backup \
  alpine tar czf /backup/mysql_data_$(date +%F).tgz -C /data .
```

PostgreSQL 卷同理替换卷名。卷快照要求容器停止或使用文件系统一致性快照；逻辑备份
更适合跨版本恢复。

### 9.4 恢复

```bash
# 1) 停 Airflow/Superset，避免写入
docker compose stop airflow-scheduler airflow-webserver superset

# 2) 恢复 MySQL（先确认目标库存在；必要时重建空库）
docker compose exec -T mysql sh -c \
  'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE"' \
  < ecommerce_dw_YYYY-MM-DD.sql

# 3) 恢复元库
docker compose exec -T postgres sh -c \
  'exec pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' \
  < meta_YYYY-MM-DD.dump
docker compose exec -T postgres sh -c \
  'exec pg_restore -U "$POSTGRES_USER" -d superset_meta --clean --if-exists' \
  < superset_meta_YYYY-MM-DD.dump

# 4) 起栈并验收
docker compose up -d
python scripts/check_db_health.py
python -m ecommerce_dw.cli status
python -m ecommerce_dw.cli verify
python scripts/demo_phase8.py
docker compose exec airflow-scheduler airflow dags unpause ecommerce_daily_pipeline
```

### 9.5 恢复原则

- **ODS 是源**：若只是 DWD/DWS/ADS 内容异常，优先从 ODS 重建（幂等），不必恢复整库。
- **发布日志只增不删**：恢复时不要删除 `ads_publish_log` 历史行；恢复后审计发布号连续性。
- **恢复后必须重跑门禁**：`quality run` 通过后才能继续 `publish`。
- **建议目标**：每日一次逻辑备份；保留 ≥7 天；备份文件加密并放到仓库外；每季度做一次
  恢复到 fresh 环境的演练（用 `docker-compose.fresh.yml`，不碰生产）。

## 10. WSL2 常见问题

### 10.1 Windows 侧没有 `docker` 命令

本机容器运行时在 WSL2 内，Windows 侧不安装 docker CLI。所有 compose 命令：

```bash
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose ps"
```

若用 Docker Desktop（WSL2 backend），可直接在 Windows 侧执行 `docker compose`。

### 10.2 WSL 重启后容器/引擎未起

```bash
wsl --shutdown
wsl -d Ubuntu24.04 -- bash -lc "sudo systemctl start docker"
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d"
```

容器 `restart: unless-stopped` 会在引擎恢复后自动拉起；若没有，手工 `up -d`。

### 10.3 内存压力/构建被杀

本机曾出现系统内存压力杀掉后台构建（非程序错误）。处置：

- 不要反复重试同一后台命令；先确认 `dwd_build_log` / `dws_ads_build_log` 是否有
  `RUNNING` 残留；
- 前台重跑同 scope（DWS builder 会自动 sweep 崩溃构建；DWD 可用 `--force`）；
- 在 `C:\Users\<user>\.wslconfig` 限制/提高 WSL 内存，或把重负载放到稳定 Linux 主机：

```ini
[wsl2]
memory=8GB
processors=4
```

- 全量 23M 行装载/构建不建议与其它重负载并行。

### 10.4 端口被占用

```bash
# Windows
netstat -ano | findstr ":3306 :5432 :8080 :8088"
```

改 `.env` 的 `MYSQL_PORT / POSTGRES_PORT / AIRFLOW_UI_PORT / SUPERSET_UI_PORT`，再
`docker compose up -d`。改 Superset 端口时同步设 `SUPERSET_UI_URL`。

### 10.5 镜像拉取超时

Docker Hub 直连不通时用镜像源再打回标准名：

```bash
docker pull docker.1ms.run/library/mysql:8.0.43
docker tag  docker.1ms.run/library/mysql:8.0.43 mysql:8.0.43
```

Airflow/Superset 同理（见 `docs/deployment.md` §10.1）。自动拉取失败一次不代表镜像
不存在，先看 `docker compose logs` 与 daemon 的 registry-mirrors。

### 10.6 时间/时区

- 容器 `TZ=UTC`（Airflow/Superset）；MySQL/PostgreSQL 服务容器 `TZ=Asia/Shanghai`，
  但**业务日期全部按 UTC 写入**，不要用本机日期做增量窗口。
- Airflow 调度 `0 4 * * *` 是 UTC；业务日期由 `logical_date`/水位线决定，与机器日期无关。

### 10.7 中文输出乱码

Windows 终端先 `chcp 65001`，或设置 `PYTHONUTF8=1`。

### 10.8 文件权限/挂载

- 业务数据卷在 WSL 内（命名卷），不要手工改 `/var/lib/mysql` 权限。
- 仓库在 `/mnt/c`（9p 挂载）上，I/O 较慢；全量装载/构建耗时受此影响，属预期。
- `.env` 通过 compose 只读挂载进 Airflow 容器；改 `.env` 后需 `docker compose up -d`
  重建容器才生效。

## 11. 故障速查表

| 现象 | 可能原因 | 处置 |
| --- | --- | --- |
| `docker compose ps` 有容器 unhealthy | 端口/凭据/元库未就绪 | 看 `logs <service>`；先确保 postgres/mysql healthy |
| Airflow DAG 不在 `dags list` | DAG import 错误/挂载缺失 | `airflow dags list-import-errors`；检查 `./airflow/dags` 挂载 |
| DAG 任务一直 RUNNING | 上游卡住/任务超时 | 看任务日志；`execution_timeout` 到点失败；清任务重跑 |
| `load_ods` 报 already loaded | SHA-256 命中 | 合法 no-op；需重装先 cleanup 批次 |
| DWD `build` 报已有未完成构建 | RUNNING/FAILED 残留 | `build --force` 或 `cleanup --build` |
| DWS 构建中途被杀 | 内存/VM 重启 | 前台重跑同 scope；builder 自动 sweep |
| `quality run` FAIL | 数据/契约/对账不一致 | `quality show --run` 定位规则；修数据后重跑 |
| `publish` exit 2 | 无 PASS run / 作用域不匹配 | 见 §6；重跑对应 scope 门禁 |
| Superset 图表 `No results` | 数据集 SQL/筛选器/物化表 | 跑 `superset_evidence.py`；必要时 `superset_setup.py` 重建 |
| Superset 登录失败 | admin 未建/元库损坏 | 看 `superset-init` 日志；`--force-recreate superset-init` |
| 看板 KPI 不更新 | 未 publish/缓存 | `publish` 追加日志；刷新页面；检查缓存配置 |
| 连接 MySQL 拒绝 | 端口/凭据/容器未起 | `check_db_health.py`；`docker compose ps`；核对 `.env` |

## 12. 恢复演练清单

- [ ] 暂停 DAG，确认无 RUNNING 批次
- [ ] 备份 MySQL + `meta` + `superset_meta`，记录文件大小/SHA-256
- [ ] 在 fresh 项目（`docker-compose.fresh.yml`）恢复备份，不碰生产
- [ ] 恢复后 `check_db_health.py`、`dwd.cli verify`、`cli verify`、`quality run`
- [ ] 起 Airflow/Superset，确认 DAG 可见、三看板可开
- [ ] `demo_phase8.py` 通过，生产 13 表基线不变
- [ ] 恢复生产调度，记录演练时间与结果

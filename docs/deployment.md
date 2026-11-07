# 部署手册（V1.0 · 从零部署）

> 适用仓库：`C:\Zproject\data-engineering\ecommerce-data-platform`（Windows 11 + WSL2）。
> 本文命令均按本机已验证路径编写：Windows 侧执行 Python/uv，容器命令经
> `wsl -d Ubuntu24.04 -- bash -lc "..."` 进入 WSL 执行。若使用 Docker Desktop
> （WSL2 backend），可去掉 `wsl ...` 前缀，直接在 Git Bash/PowerShell 执行 `docker compose`。
>
> 相关文档：架构见 `docs/architecture.md`，日常运维见 `docs/operations.md`，
> 性能与 fresh 环境实测见 `docs/performance.md`，测试矩阵见 `docs/test_matrix.md`。

## 1. 版本矩阵与交付边界

| 组件 | 固定版本 | 说明 |
| --- | --- | --- |
| Python | 3.11（`>=3.11,<3.12`） | 由 uv 管理 `.venv` |
| MySQL | `mysql:8.0.43` | 业务数仓 `ecommerce_dw` |
| PostgreSQL | `postgres:16.13` | Airflow `meta` + Superset `superset_meta` |
| Airflow | `apache/airflow:2.10.5-python3.11` | 自定义镜像 `ecommerce-airflow:2.10.5-python3.11`，LocalExecutor |
| Superset | `apache/superset:4.1.2` | 自定义镜像 `ecommerce-superset:4.1.2`，补 `pymysql` + `psycopg2-binary` |
| Docker | Engine 29.9.3（本机实测） | WSL2 Ubuntu24.04，systemd + overlayfs |

**不使用 `:latest`**。升级必须在明确窗口内改 tag、重建、回归，见 §9。

## 2. 前置条件

- Windows 11；WSL2 发行版 `Ubuntu24.04`，已启用 systemd，Docker Engine 可用。
- Git、uv（Python 3.11）、约 1 GB 原始 CSV 的磁盘空间，以及 MySQL 数据卷所需空间。
- 能访问 Docker Hub 或已配置镜像加速；本机 Docker Hub 直连不通时走
  `docker.1ms.run` 拉取后打回标准名（见 §10.1）。
- 端口 `3306 / 5432 / 8080 / 8088` 未被占用；冲突时改 `.env` 的
  `MYSQL_PORT / POSTGRES_PORT / AIRFLOW_UI_PORT / SUPERSET_UI_PORT`。
- 原始数据放在 `data/raw/retailrocket/`（该目录被 `.gitignore` 忽略）：
  `events.csv`、`item_properties_part1.csv`、`item_properties_part2.csv`、
  `category_tree.csv`。来源、字节数、SHA-256 见 `docs/dataset.md` §3。

## 3. 获取仓库与 Python 环境

```bash
cd /c/Zproject/data-engineering/ecommerce-data-platform

uv venv --python 3.11 .venv
source .venv/Scripts/activate          # Git Bash
# PowerShell: .\.venv\Scripts\Activate.ps1
uv sync --all-extras
```

> 依赖版本由 `uv.lock` 锁定，换机器可复现。升级依赖用 `uv lock --upgrade` 后重新
> `uv sync`，lock 文件随代码提交，不要手工改。

## 4. `.env` 配置

```bash
cp .env.example .env
```

`.env` 已被 `.gitignore` 忽略，**禁止提交**；`.env.example` 只放占位符。

| 变量 | 必填 | 用途/说明 |
| --- | --- | --- |
| `MYSQL_HOST` / `MYSQL_PORT` | 是 | 宿主机脚本连接 MySQL 的地址/端口；默认 `127.0.0.1:3306` |
| `MYSQL_DATABASE` | 是 | 业务库名，默认 `ecommerce_dw`；MySQL 初始化脚本按此建库 |
| `MYSQL_ROOT_PASSWORD` | 是 | root 口令；建库、集成测试建 `*_test` schema、`apply_superset_grants.py` 需要 |
| `MYSQL_USER` / `MYSQL_PASSWORD` | 是 | 应用账号 `ecom_app`，各 CLI/装载/构建使用 |
| `MYSQL_TEST_DATABASE` | 否 | 集成测试 schema，默认 `ecommerce_dw_test`；**必须以 `_test` 结尾** |
| `POSTGRES_HOST` / `POSTGRES_PORT` | 是 | 元数据库地址/端口；默认 `127.0.0.1:5432` |
| `POSTGRES_DB` | 是 | Airflow 元库，默认 `meta` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | 是 | PostgreSQL 超管；Airflow 元库 + Superset 建库使用 |
| `AIRFLOW_FERNET_KEY` | 是 | Airflow Fernet key（≥32 字节 url-safe base64） |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | 是 | Airflow Web 会话签名密钥 |
| `AIRFLOW_WWW_USER_USERNAME` / `_PASSWORD` / `_EMAIL` | 是 | `airflow-init` 幂等创建的 admin |
| `AIRFLOW_UI_PORT` | 否 | Airflow Web 宿主机端口，默认 `8080` |
| `SUPERSET_UI_PORT` | 否 | Superset Web 宿主机端口，默认 `8088` |
| `SUPERSET_UI_URL` | 否 | `scripts/superset_setup.py` 访问的 API 地址；默认 `http://localhost:8088`，改端口时必须同步 |
| `SUPERSET_META_DB` | 是 | Superset 元库名，默认 `superset_meta`（与 Airflow `meta` 物理隔离） |
| `SUPERSET_META_USER` / `_PASSWORD` | 是 | Superset 元库专属角色 |
| `SUPERSET_SECRET_KEY` | 是 | Superset 会话/Fernet 密钥 |
| `SUPERSET_ADMIN_USERNAME` / `_PASSWORD` / `_EMAIL` | 是 | `superset-init` 幂等创建的 admin |
| `MYSQL_RO_USER` / `MYSQL_RO_PASSWORD` | 是 | 只读业务账号，默认 `superset_ro`；`apply_superset_grants.py` 创建/授权 |
| `SUPERSET_MYSQL_HOST` / `_PORT` | 是 | Superset 容器内访问 MySQL 的地址，默认 `mysql:3306` |

生成密钥：

```bash
python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

> 同一条命令生成 `AIRFLOW_FERNET_KEY`、`AIRFLOW_WEBSERVER_SECRET_KEY`、
> `SUPERSET_SECRET_KEY`（各自独立取值，不要复用）。

## 5. 起栈顺序

### 5.1 先起数据库并等健康

```bash
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d mysql postgres"
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose ps"
python scripts/check_db_health.py
```

预期：`ecommerce-mysql`、`ecommerce-postgres` 均 `healthy`；健康检查脚本退出码 0。

- MySQL 首次在**空数据卷**启动时自动执行 `docker/mysql/init/01_init.sh`，按
  `MYSQL_DATABASE` 建库并固定 `utf8mb4 / utf8mb4_0900_ai_ci`；卷已存在时不会重复执行。
- PostgreSQL 首次启动按 `POSTGRES_DB/USER/PASSWORD` 建库；Airflow 与 Superset 的
  元库在后续 init 容器中创建/迁移。

### 5.2 应用业务库 DDL（顺序固定）

```bash
python scripts/apply_ods_ddl.py          # ODS 3 表 + etl_batch_log
python scripts/apply_dwd_ddl.py          # DWD 4 表 + dwd_build_log
python scripts/apply_dws_ads_ddl.py      # DWS 3 表 + ADS 6 表 + dws_ads_build_log
python scripts/apply_quality_ddl.py      # etl_watermark / quality_check_run / quality_check_result / ads_publish_log
python scripts/apply_perf_indexes.py     # Phase 8 五个性能索引（幂等；已有库补建）
```

所有 DDL 均为 `CREATE TABLE IF NOT EXISTS` / 幂等索引检查，可重复执行；但不做列结构
变更——列变更必须新增迁移脚本，见 §9。

### 5.3 装载 ODS

```bash
python scripts/load_all_ods.py
```

- 顺序：`category_tree` → `events` → `item_properties_part1` → `part2`。
- 同一 `(source_file, file_sha256)` 已 LOADED 的文件自动 skip（幂等）。
- 断点续跑：MySQL 容器重启/连接中断时 loader 自动重连续传。
- 期望行数：`ods_category_tree_raw` 1,669；`ods_events_raw` 2,756,101；
  `ods_item_properties_raw` 20,275,902（part1 10,999,999 + part2 9,275,903）。

### 5.4 构建 DWD

```bash
python -m ecommerce_dw.dwd.cli build
python -m ecommerce_dw.dwd.cli verify
```

期望：`dwd_events` 2,755,641、`dwd_events_duplicate` 460、`dwd_events_reject` 0，
`verify` 输出 `identity=OK`。

### 5.5 构建 DWS / ADS

```bash
python -m ecommerce_dw.dws.cli build
python -m ecommerce_dw.cli verify
```

期望：build #8 口径的 9 张表累计 3,431,622 行；`verify` 2,094 checks ALL MATCH。
全量构建本机实测 555.7s（含构建内不变量校验）。

### 5.6 引导增量水位

```bash
python -m ecommerce_dw.cli incremental --init --to 2015-09-16
python -m ecommerce_dw.cli status
```

期望：登记 137 个业务日期（2015-05-03 ~ 2015-09-16）；下一次普通 `incremental`
接管 09-17 / 09-18。

### 5.7 全量门禁与首次发布

```bash
python -m ecommerce_dw.cli quality run
# 记下输出中的 run #N（必须 PASS）
python -m ecommerce_dw.cli publish --run N
python -m ecommerce_dw.cli quality show --run N
```

期望：60/60 PASS，`checks_total` 89,305,119（行数随数据规模变化），
`publish` 写一条 `ads_publish_log`。门禁 FAIL 或没有 PASS run 时 `publish` 拒绝并 exit 2。

### 5.8 起完整栈（Airflow + Superset）

```bash
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d"
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose ps"
```

启动顺序由 compose 的 `depends_on` 保证：

```text
postgres healthy ─┬─► airflow-init (db migrate + admin) ─► scheduler / webserver
mysql healthy ────┘
postgres healthy ─┬─► superset-init (建 superset_meta + 迁移 + admin + 导入看板) ─► superset
mysql healthy ────┘
```

预期：

```text
ecommerce-mysql              Up (healthy)
ecommerce-postgres           Up (healthy)
ecommerce-airflow-init       Exited (0)
ecommerce-airflow-scheduler  Up (healthy)
ecommerce-airflow-webserver  Up (healthy)
ecommerce-superset-init      Exited (0)
ecommerce-superset           Up (healthy)
```

### 5.9 Superset 只读授权与看板生成

```bash
python scripts/apply_superset_grants.py   # 5 个受控视图 + superset_ro SELECT 授权
python scripts/superset_setup.py          # 幂等 upsert 数据库/数据集/图表/看板并导出
```

- `apply_superset_grants.py` 以 root 连接，幂等创建 5 个视图与 `superset_ro`；打印
  `SHOW GRANTS`（6 张 ADS 表 + 5 视图全 SELECT，无写权限）。
- `superset_setup.py` 需要 Superset Web 已 healthy，默认访问
  `http://localhost:8088`；非默认端口在 `.env` 设 `SUPERSET_UI_URL`。
- 首次在空元库启动时，`superset-init` 导入看板包可能因数据库连接尚未建立而打印
  `[warning] import failed`，随后由 `superset_setup.py` 完成 bootstrap；这是已知且
  可接受路径，见 `docs/performance.md` §8.2。

### 5.10 Phase 9.3 实时作业（Kafka → Flink → `ecommerce_rt`）

前置：5.8 的栈已起（Kafka/Flink healthy），`.env` 有 `RT_APP_PASSWORD`（Flink 容器内由
compose 以 `RT_MYSQL_*` 注入）和 `RT_RECON_RO_PASSWORD`（对账只读账号），`ecommerce_rt`
5 张表已由 `python scripts/apply_rt_ddl.py` 建好。

```bash
bash scripts/download_flink_jars.sh     # 下载固定版本 connector / MySQL 驱动到 docker/flink/lib
bash scripts/build_flink_job.sh         # javac --release 17 + 42 项自检 + build/flink-jobs/ecommerce-rt-job.jar
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose build flink-jobmanager flink-taskmanager"
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d --force-recreate flink-jobmanager flink-taskmanager"
bash scripts/deploy_flink_job.sh        # 提交并打印 JOB_ID
python scripts/verify_rt_vs_dwd.py 2015-07-14 2015-07-16
```

- 作业名 `ecommerce-rt-daily-metrics`，默认 source `ecommerce.events.v1`、DLQ
  `ecommerce.events.v1.dlq`、group `ecommerce-rt-flink-v1`、parallelism 2、起始 offset
  `earliest`。
- `scripts/deploy_flink_job.sh` 支持 `--restore-from file:/opt/flink/checkpoints/<job>/chk-N`
  恢复；恢复后 Flink REST `/jobs/<id>/checkpoints` 的 `counts.restored` 应为 1。
- jar 不进仓库：`docker/flink/lib/*.jar`、`build/` 均已 git 忽略，部署机需先跑下载/构建脚本。
- `verify_rt_vs_dwd.py` 只读 DWD 使用 `MYSQL_*`，只读 RT 使用
  `RT_RECON_RO_USER`（默认 `rt_recon_ro`）/ `RT_RECON_RO_PASSWORD`，不使用 `rt_app`。

## 6. 健康检查与验收

```bash
# 1) 容器状态
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose ps"

# 2) 数据库连通性（Windows 侧，走宿主机映射端口）
python scripts/check_db_health.py

# 3) Airflow
wsl -d Ubuntu24.04 -- bash -lc "curl -fsS http://localhost:8080/health"
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose exec airflow-scheduler airflow dags list"
# 期望看到 ecommerce_daily_pipeline 与 phase6_retry_demo

# 4) Superset
wsl -d Ubuntu24.04 -- bash -lc "curl -fsS http://localhost:8088/health"
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose exec -T superset superset version"
# 期望 4.1.2

# 5) 一键端到端演示（隔离 schema，生产只读）
python scripts/demo_phase8.py
```

`demo_phase8.py` 会：检查四层栈健康 → 快照生产 13 张表行数 → 建独立 `*_test` schema
→ 装载 14 行样例 → 跑 `incremental` → 注入迟到批次并重算 → 60 条门禁 → `publish`
→ `verify` → 复核生产行数不变 → 清理。约 4.2s；`--keep` 保留 schema，`--skip-http`
跳过 Airflow/Superset HTTP 探测。

### 6.1 端口与连接表

| 服务 | Host | Port | 库/用户 |
| --- | --- | ---: | --- |
| MySQL（业务数仓） | 127.0.0.1 | `MYSQL_PORT`（默认 3306） | `MYSQL_DATABASE` / `MYSQL_USER` |
| PostgreSQL（元数据） | 127.0.0.1 | `POSTGRES_PORT`（默认 5432） | `meta` / `POSTGRES_USER`；`superset_meta` / `SUPERSET_META_USER` |
| Airflow Web | localhost | `AIRFLOW_UI_PORT`（默认 8080） | `.env` 的 Airflow admin |
| Superset Web | localhost | `SUPERSET_UI_PORT`（默认 8088） | `.env` 的 Superset admin |

## 7. 全新环境（`docker-compose.fresh.yml`）

用途：用一个**独立 compose 项目、独立容器名、独立命名卷**从空库验证部署，
不触碰生产栈（项目名 `ecommerce-data-platform`、默认端口 3306/5432/8080/8088）。

```bash
# 临时 env 放在仓库外，不入库；示例路径（真实口令自行填写）
# /mnt/c/Zproject/ZClaude/ecommerce-phase8/fresh.env
#   MYSQL_PORT=13306
#   MYSQL_DATABASE=ecommerce_dw_fresh
#   POSTGRES_PORT=15432
#   POSTGRES_DB=meta_fresh
#   AIRFLOW_UI_PORT=18080
#   SUPERSET_UI_PORT=18088
#   SUPERSET_META_DB=superset_meta_fresh
#   ... 其余口令/密钥同 .env

# 1) 只起数据库
docker compose -p ecommerce-fresh \
  --env-file /mnt/c/Zproject/ZClaude/ecommerce-phase8/fresh.env \
  -f docker-compose.yml -f docker-compose.fresh.yml up -d mysql postgres

# 2) 对 fresh 库应用 DDL（宿主机脚本用环境变量覆盖 .env）
#    若 fresh.env 的账号/口令与生产 .env 不同，先在当前 Git Bash 里 export
#    MYSQL_USER / MYSQL_PASSWORD / MYSQL_ROOT_PASSWORD / POSTGRES_USER / POSTGRES_PASSWORD。
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python scripts/apply_ods_ddl.py
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python scripts/apply_dwd_ddl.py
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python scripts/apply_dws_ads_ddl.py
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python scripts/apply_quality_ddl.py

# 3) 装小样例并跑链路（样例路径/命令见 docs/performance.md §8.1）
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python -m ecommerce_dw.cli incremental
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python -m ecommerce_dw.cli quality run
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python -m ecommerce_dw.cli publish --run 1
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python -m ecommerce_dw.cli verify

# 4) 起完整 fresh 栈
docker compose -p ecommerce-fresh \
  --env-file /mnt/c/Zproject/ZClaude/ecommerce-phase8/fresh.env \
  -f docker-compose.yml -f docker-compose.fresh.yml up -d

# 5) fresh 环境只读授权 + 看板（注意 API 端口）
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh python scripts/apply_superset_grants.py
MYSQL_PORT=13306 MYSQL_DATABASE=ecommerce_dw_fresh SUPERSET_UI_URL=http://localhost:18088 \
  python scripts/superset_setup.py

# 6) 验收后彻底清理（仅 fresh 项目！）
docker compose -p ecommerce-fresh \
  --env-file /mnt/c/Zproject/ZClaude/ecommerce-phase8/fresh.env \
  -f docker-compose.yml -f docker-compose.fresh.yml down -v
```

验证结果（本机实测，详见 `docs/performance.md` §8）：7 个容器全部 healthy/Exited(0)；
Airflow `dags list` 可见两个 DAG；Superset 4.1.2；3 个看板 / 15 个数据集；
`superset_ro` 对 fresh 库的 smoke 查询返回 `(2015-09-16, 4, 2, 4)`、`(2015-09-17, 3, 3, 4)`；
`down -v` 后 7 容器、4 卷、网络全部 Removed，生产 5 容器仍 healthy、13 张表基线不变。

> **警告**：`down -v` 会删除数据卷。只对 `-p ecommerce-fresh` 使用；生产项目严禁带 `-v`。

## 8. 首次初始化的幂等性

| 初始化 | 实现 | 重复执行是否安全 |
| --- | --- | --- |
| MySQL 建库 | `docker/mysql/init/01_init.sh`：`CREATE DATABASE IF NOT EXISTS` + `ALTER DATABASE` | 是；但只在空数据卷首次启动时由官方镜像执行 |
| Airflow 元库 | `airflow-init`：`airflow db migrate`（alembic 幂等） | 是 |
| Airflow admin | `airflow users list` 查重后再 `users create` | 是；已存在则 skip |
| Superset 元库/角色 | `superset-init` Step 1：查 `pg_roles` / `pg_database` 后建 | 是 |
| Superset 迁移 | `superset db upgrade` | 是 |
| Superset admin | `superset fab list-users` 查重后建 | 是 |
| 看板导入 | `superset import-dashboards -p bundle -u admin`；失败仅 warning | 是；重复导入不翻倍（实测） |
| 连接口令 | `fix_database_password.py` 用 `set_sqlalchemy_uri()` 重建，口令进 Fernet 列 | 是 |
| 业务 DDL | `CREATE TABLE IF NOT EXISTS` + 幂等索引检查 | 是 |
| ODS 装载 | `(source_file, file_sha256)` 已 LOADED 则 skip | 是 |
| DWD/DWS/ADS 构建 | 同 scope 删除旧行再重插 | 是（同 scope 内容幂等） |
| 水位引导 | `upsert_many` | 是（同日期同批次覆盖） |

## 9. 升级与回滚

### 9.1 升级前

1. 暂停调度：Airflow UI 暂停 `ecommerce_daily_pipeline`，或
   `docker compose exec airflow-scheduler airflow dags pause ecommerce_daily_pipeline`。
2. 备份业务库与两个元库（命令见 `docs/operations.md` §9）。
3. 记录当前 tag、`git rev-parse HEAD`、`ads_publish_log` 最大 `publish_id`。

### 9.2 升级步骤

1. 修改 `docker-compose.yml` / `docker/*/Dockerfile` 中的固定 tag（不要用 `:latest`）。
2. 拉基础镜像并重建：
   ```bash
   docker compose build airflow-init superset-init
   docker compose up -d
   ```
3. `airflow-init` / `superset-init` 会自动执行 alembic 迁移；观察退出码与日志：
   ```bash
   docker compose logs --tail=200 airflow-init
   docker compose logs --tail=200 superset-init
   ```
4. 回归：`docker compose ps`、`python scripts/demo_phase8.py`、`python -m pytest`、
   容器内 `pytest tests/dag`、Superset 三看板打开、`scripts/superset_evidence.py`。
5. 恢复调度：`airflow dags unpause ecommerce_daily_pipeline`。

### 9.3 回滚

- **镜像回滚**：把 tag 改回上一版本，`docker compose up -d`；业务数据卷不动。
- **元库回滚**：若新版本 alembic 迁移不可逆，停栈后用升级前 `pg_dump` 恢复
  `meta` / `superset_meta`，再起旧镜像。
- **业务库回滚**：优先用升级前 `mysqldump` 恢复；若只是数据内容问题，可从 ODS 重新
  全量构建 DWD/DWS/ADS（ODS 是源，构建幂等）。
- **发布日志**：`ads_publish_log` 只增不删；回滚不删除历史发布行。

### 9.4 Schema 变更

`sql/ddl/*.sql` 的 `CREATE TABLE IF NOT EXISTS` **不会**修改已存在表。列/索引变更必须：

1. 新增迁移脚本（例如 `sql/ddl/migrations/YYYYMMDD_xxx.sql`）并纳入评审；
2. 在测试 schema 先验证；
3. 备份后执行；
4. 更新 `docs/data_dictionary.md` 与相关设计文档。

不要手工 `ALTER` 生产表后不留脚本。

## 10. 常见部署问题

### 10.1 镜像拉取超时

```bash
docker pull docker.1ms.run/library/mysql:8.0.43
docker tag  docker.1ms.run/library/mysql:8.0.43 mysql:8.0.43

docker pull docker.1ms.run/apache/airflow:2.10.5-python3.11
docker tag  docker.1ms.run/apache/airflow:2.10.5-python3.11 apache/airflow:2.10.5-python3.11

docker pull docker.1ms.run/apache/superset:4.1.2
docker tag  docker.1ms.run/apache/superset:4.1.2 apache/superset:4.1.2
```

### 10.2 端口冲突

```bash
# Windows
netstat -ano | findstr ":3306 :5432 :8080 :8088"
# 改 .env 的 MYSQL_PORT / POSTGRES_PORT / AIRFLOW_UI_PORT / SUPERSET_UI_PORT 后
docker compose up -d
```

### 10.3 Airflow init 失败

```bash
docker compose logs airflow-init
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt"
```

优先检查 `POSTGRES_PASSWORD` 是否与 compose 注入一致、`meta` 库是否可连。

### 10.4 Superset init 失败

```bash
docker compose logs superset-init
docker compose up -d --force-recreate superset-init && docker compose up -d superset
```

空元库一键重建即上述命令；`superset-init` 可重复执行。若只是看板包导入 warning，
在 Superset healthy 后执行 `python scripts/apply_superset_grants.py` 与
`python scripts/superset_setup.py` 即可。

## 11. 凭据管理

- 唯一凭据入口是宿主 `.env`；compose 注入容器，DAG/CLI 只读环境变量，源码不含口令。
- 不使用 Airflow Connections/Variables 双写凭据；Superset 的业务连接口令由
  `fix_database_password.py` 写入 Fernet 加密列，URI 列只留掩码。
- 生产环境必须替换本地弱口令，改用 Vault/KMS/云 Secrets，并限制 3306/5432/8080/8088
  的访问来源；Superset 前置 TLS/反向代理，替换默认 admin。
- `.env`、`config/config.yaml`、`data/`、临时 fresh env 均不得入库。

## 12. 部署检查清单

- [ ] `.env` 已创建且未被 git 跟踪；密钥为独立随机值
- [ ] `docker compose ps`：mysql/postgres/scheduler/webserver/superset healthy，两个 init Exited(0)
- [ ] `python scripts/check_db_health.py` 退出码 0
- [ ] ODS 三表行数 = 2,756,101 / 20,275,902 / 1,669
- [ ] DWD 恒等式 2,756,101 = 2,755,641 + 460 + 0
- [ ] DWS/ADS 9 表构建完成；`verify` 2,094 checks ALL MATCH
- [ ] 水位 139 行；`ads_publish_log` 至少 1 行且由 PASS 门禁背书
- [ ] Airflow `dags list` 可见 `ecommerce_daily_pipeline`；schedule `0 4 * * *`
- [ ] Superset 4.1.2；3 看板 21 图；`superset_ro` 仅 SELECT
- [ ] `python scripts/demo_phase8.py` 通过，生产 13 表基线不变
- [ ] `python -m pytest` 236 passed / 1 skipped；DAG 契约 5 passed
- [ ] Phase 9.3：`bash scripts/build_flink_job.sh` 输出 `SELFTEST passed=42 failed=0` 与
      `BUILD_FLINK_JOB_OK`；`bash scripts/deploy_flink_job.sh` 打印 JobID；
      `python scripts/verify_rt_vs_dwd.py <from> <to>` 输出 `VERIFY_RT_VS_DWD_OK`

# ecommerce-data-platform

电商用户行为数据仓库与实时分析平台。

**Phase 1（本阶段）**：数据探索与数据契约——全量流式统计、字段画像、指标口径与数据契约。
尚未实现 ETL、数仓建模、Airflow 编排、Superset 看板或实时链路。

目标形态（后续 Phase）：Retailrocket 用户行为数据 → MySQL 四层数仓（ODS / DWD / DWS / ADS）→ Airflow 编排 → Superset 看板；之后扩展 Kafka + Flink 实时链路。

## 当前状态

- **Phase 1 已完成**：全量数据探索（4 个 CSV，约 1GB）结论见
  `notebooks/01_eda.ipynb`（已实际执行）；字段/指标口径/数据契约分别沉淀在
  `docs/data_dictionary.md`、`docs/metric_definitions.md`、`docs/data_contract.md`。
- 可复用流式统计模块在 `src/ecommerce_dw/eda/`（无数据库、只读本地 CSV）。
- 工程骨架 + 两个数据库的容器编排文件（`docker-compose.yml`）。
- 容器运行时为 **WSL2 内的 Docker Engine**（Windows 侧无 `docker` 命令，见「已验证环境」）；MySQL / PostgreSQL 容器已实际启动并通过健康检查。
- 业务数仓（MySQL）与 Airflow/Superset 元数据库（PostgreSQL）严格分离（见 `docs/architecture.md`）。

## 环境要求

- Windows 11（本说明面向 PowerShell；Git Bash 亦可）
- git >= 2.x
- uv（本项目用 uv 管理 Python 3.11 虚拟环境，不使用系统 py 启动器）
- 容器运行时（二选一；本机已验证的是第一种）：
  - **WSL2 内的 Docker Engine**（主路径）：Ubuntu24.04 + systemd + overlayfs；
    Windows 侧没有 `docker` 命令，所有 compose 命令在 WSL 里执行
    （见「Docker（数据库）启停」）。
  - Docker Desktop（WSL2 backend）：另一种可选路径，命令相同，可在 Windows 侧直接执行。

## 初始化步骤

在项目根目录打开 PowerShell：

```powershell
# 1. 创建 Python 3.11 虚拟环境
uv venv --python 3.11 .venv

# 2. 激活虚拟环境（每次新开终端都要执行一次）
.\.venv\Scripts\Activate.ps1

# 3. 安装项目依赖（运行时 + dev，版本由 uv.lock 锁定）
uv sync --all-extras
```

> 若 PowerShell 拒绝执行激活脚本：`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`。
>
> 依赖版本由 `uv.lock`（随仓库提交）锁定，换机器可复现。升级依赖用
> `uv lock --upgrade` 后重新 `uv sync`；lock 文件必须随代码一起提交，不要手工改动。

## 配置

```powershell
# 生成本地配置：按需修改 .env 里的用户名 / 口令（本地开发用弱口令即可）
Copy-Item .env.example .env
```

- `.env` 已被 git 忽略，**禁止提交到仓库**；其中口令仅用于本地开发。
- `config/config.yaml`（复制自 `config/config.example.yaml`）同样被 git 忽略；配置文件不含任何凭据。

## 代码检查与测试

```powershell
# 代码规范检查
ruff check .

# 格式检查（只检查，不改动文件）
ruff format --check .

# 冒烟测试（不依赖数据库）
pytest
```

## Phase 1 产物快速入口

- **EDA notebook**：`notebooks/01_eda.ipynb`（已执行含图表，可 Restart & Run All 复现；
  重新生成 cell 用 `python scripts/build_eda_notebook.py`）。
- **流式统计模块**：`src/ecommerce_dw/eda/`——`scan_csv` 通用画像、
  `summarize_events` / `summarize_item_properties` / `summarize_category_tree` 专项统计。
- **口径文档**：`docs/data_dictionary.md`、`docs/metric_definitions.md`、`docs/data_contract.md`。

## Docker（数据库）启停

> 前提：容器运行时已就绪。本机经实际验证的是 **WSL2 内的 Docker Engine 29.9.3**
> （发行版 `Ubuntu24.04`，systemd 管理）；Windows 侧（PowerShell / Git Bash）**没有**
> `docker` 命令，所有容器命令都在 WSL 里执行。若改用 Docker Desktop（WSL2 backend），
> 下面命令可在 Windows 侧直接执行。

所有 compose 操作统一经 WSL 执行（每次一条命令，目录为 Windows 挂载点）：

```bash
# 启动 MySQL + PostgreSQL（后台）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d"

# 查看状态
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose ps"

# 查看日志（MySQL 首次建库脚本的输出也在这里）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose logs -f mysql"

# 停止（保留数据卷）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose down"

# 停止并删除数据卷（数据会丢，谨慎使用）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose down -v"
```

- 端口、库名、用户名、口令全部在 `.env` 中配置，`docker compose` 自动读取。
- MySQL 首次启动会自动执行 `docker/mysql/init/01_init.sql`（建库 + utf8mb4 字符集）。
- 镜像拉取：Docker Hub 直连不通，daemon 已配置 registry-mirrors
  （daocloud / 1ms.run / dockerproxy.net）。自动拉取曾因 CDN 节点超时失败一次，
  改用 `docker pull docker.1ms.run/library/<image>` 再 `docker tag` 回标准名即成功
  （详见「已验证环境」）。
- **镜像 tag 已核实存在并拉取成功**：`mysql:8.0.43`、`postgres:16.13`
  （详见 `docs/architecture.md` §4）。

## 数据库连接验证

容器启动后，用 Python 脚本分别对两个库执行 `SELECT 1`：

```powershell
python scripts/check_db_health.py
```

> Windows 终端若中文输出乱码，先执行 `chcp 65001`，或运行前设置 `$env:PYTHONUTF8=1`。

任一失败退出码非 0。也可以直接用客户端连接：

| 服务 | Host | Port | 库 | 用户 |
| --- | --- | --- | --- | --- |
| MySQL（业务数仓） | 127.0.0.1 | `MYSQL_PORT` | `MYSQL_DATABASE` | `MYSQL_USER` |
| PostgreSQL（元数据库） | 127.0.0.1 | `POSTGRES_PORT` | `POSTGRES_DB` | `POSTGRES_USER` |

`scripts/check_db_health.py` 会自动从 `.env` 读取以上变量。

## 已验证环境（Docker / 数据库实测记录）

> 以下均为本机**实际执行结果**，供运维与面试引用；环境变化时同步更新本节。

- **运行时**：WSL2 内 Docker Engine **29.9.3**（发行版 Ubuntu24.04，systemd 管理，
  overlayfs 存储驱动）；Windows 侧无 `docker` 命令，compose 一律在 WSL 中执行。
- **拉取方式**：Docker Hub 直连不通；daemon 已配置 registry-mirrors
  （daocloud / 1ms.run / dockerproxy.net）。`docker compose up -d` 自动拉取时曾因
  CDN 节点超时失败一次；改用 `docker pull docker.1ms.run/library/<image>` 再
  `docker tag` 回标准名后成功。
- **镜像**：`mysql:8.0.43` 与 `postgres:16.13` 两个 tag 均已实际拉取成功、确认存在。
- **容器健康**：两容器均为 `healthy`，`RestartCount=0`。
- **连通性**：Windows 侧 `python scripts/check_db_health.py` 退出码 **0**
  （两个库 SELECT 1 均通过，走宿主机发布端口 127.0.0.1）。
- **字符集**：业务库 `ecommerce_dw` 为 **utf8mb4 / utf8mb4_0900_ai_ci**
  （由 `docker/mysql/init/01_init.sql` 自动初始化，无需手工干预）。
- **命名卷**：`ecommerce-data-platform_mysql_data`、
  `ecommerce-data-platform_postgres_data` 已由 compose 创建。

## 目录结构

```text
ecommerce-data-platform/
├─ config/                # 配置文件模板（不含凭据）
├─ data/{raw,staging,sample}/   # 数据目录（数据文件不入库、不提交）
├─ scripts/               # 运维脚本（如 check_db_health.py）
├─ src/ecommerce_dw/      # 项目 Python 包（Phase 0 仅配置解析模块）
├─ sql/{ddl,dml,checks}/  # 数仓建模 SQL（后续 Phase 使用）
├─ airflow/dags/          # Airflow DAG（后续 Phase）
├─ streaming/             # Kafka + Flink 实时链路（后续 Phase）
├─ dashboards/exports/    # 看板与导出（后续 Phase）
├─ tests/                 # 冒烟/单元/集成/数据质量测试
├─ docs/                  # 架构文档
├─ docker/mysql/init/     # MySQL 初始化 SQL
├─ docker-compose.yml
├─ pyproject.toml
├─ .env.example
└─ .gitignore
```

## 已知限制与下一步

- 容器运行时已就绪且 Phase 0 容器相关交付物已实测通过（WSL2 内 Docker Engine，
  见「已验证环境」）；若日后迁到 Docker Desktop 或 CI 环境，以「已验证环境」为基线重新核对。
- `docs/architecture.md` 中包含完整 Roadmap；下一阶段从「Phase 2：ODS 入库与数据批次」开始。

## 安全说明

- `.env`、`config/config.yaml`、`data/` 下的数据文件均已被 .gitignore 忽略。
- 本地弱口令仅供开发联调，生产环境必须换用密钥管理方案。
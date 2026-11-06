# ecommerce-data-platform

电商用户行为数据仓库与实时分析平台。

**Phase 0（本阶段）**：完成工程初始化与骨架搭建。不实现任何 ETL、数仓建模、Airflow 编排、Superset 看板或实时链路。

目标形态（后续 Phase）：Retailrocket 用户行为数据 → MySQL 四层数仓（ODS / DWD / DWS / ADS）→ Airflow 编排 → Superset 看板；之后扩展 Kafka + Flink 实时链路。

## 当前状态

- 工程骨架 + 两个数据库的容器编排文件（`docker-compose.yml`）。
- 本机 Docker Desktop / WSL2 **尚未安装**，容器相关内容未实际启动验证（见「已知限制与下一步」）。
- 业务数仓（MySQL）与 Airflow/Superset 元数据库（PostgreSQL）严格分离（见 `docs/architecture.md`）。

## 环境要求

- Windows 11（本说明面向 PowerShell；Git Bash 亦可）
- git >= 2.x
- uv（本项目用 uv 管理 Python 3.11 虚拟环境，不使用系统 py 启动器）
- （可选，容器相关）Docker Desktop（WSL2 backend）

## 初始化步骤

在项目根目录打开 PowerShell：

```powershell
# 1. 创建 Python 3.11 虚拟环境
uv venv --python 3.11 .venv

# 2. 激活虚拟环境（每次新开终端都要执行一次）
.\.venv\Scripts\Activate.ps1

# 3. 安装项目依赖（运行时 + dev）
uv pip install -e ".[dev]"
```

> 若 PowerShell 拒绝执行激活脚本：`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`。

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

## Docker（数据库）启停

> 前提：已安装并启动 Docker Desktop（含 WSL2 后端）。当前环境未安装，见「已知限制与下一步」。

```powershell
# 启动 MySQL + PostgreSQL（后台）
docker compose up -d

# 查看状态
docker compose ps

# 查看日志（MySQL 首次建库脚本的输出也在这里）
docker compose logs -f mysql

# 停止（保留数据卷）
docker compose down

# 停止并删除数据卷（数据会丢，谨慎使用）
docker compose down -v
```

- 端口、库名、用户名、口令全部在 `.env` 中配置，`docker compose` 自动读取。
- MySQL 首次启动会自动执行 `docker/mysql/init/01_init.sql`（建库 + utf8mb4 字符集）。
- **镜像 tag 首次 pull 前必须核实存在**，若不存在请换同分支最近可用 tag（详见 `docs/architecture.md`）。

## 数据库连接验证

容器启动后，用 Python 脚本分别对两个库执行 `SELECT 1`：

```powershell
python scripts/check_db_health.py
```

任一失败退出码非 0。也可以直接用客户端连接：

| 服务 | Host | Port | 库 | 用户 |
| --- | --- | --- | --- | --- |
| MySQL（业务数仓） | 127.0.0.1 | `MYSQL_PORT` | `MYSQL_DATABASE` | `MYSQL_USER` |
| PostgreSQL（元数据库） | 127.0.0.1 | `POSTGRES_PORT` | `POSTGRES_DB` | `POSTGRES_USER` |

`scripts/check_db_health.py` 会自动从 `.env` 读取以上变量。

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

- Docker Desktop / WSL2 未安装（`docker` 命令不存在），Phase 0 **未**对容器做任何实际验证；安装 Docker 后若 compose 配置有问题，会在首次启动时修正。
- `docs/architecture.md` 中包含完整 Roadmap；下一阶段从「Phase 1：ODS 层数据导入」开始。

## 安全说明

- `.env`、`config/config.yaml`、`data/` 下的数据文件均已被 .gitignore 忽略。
- 本地弱口令仅供开发联调，生产环境必须换用密钥管理方案。
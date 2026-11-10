# Phase 11 — Web UI 设计文档（只读展示 + 本地分析沙箱）

> 实施契约：Web UI SPEC v1（冻结）。本文记录 Web UI 的架构、API 契约、
> `webapp_ro` 权限模型、部署/运行方式与失败排查。前端由独立目录
> `web/frontend/` 实现，本文只描述后端与部署边界。

---

## 1. 定位与非目标

把 ADS 报表、实时 `rt_*` 结果、质量门禁与运维审计整合成一套**只读**界面：
白底、细描边、克制用色的简约风格；单容器同时提供 `/api/*` 与 React SPA。

v1 **不做**：写库、触发 Airflow/Flink 作业、登录体系、移动端专门适配。
后端代码不包含任何 `INSERT` / `UPDATE` / `DELETE` / DDL；运行时账号
`webapp_ro` 仅被授予 `SELECT`，连接会话另设 `READ ONLY` 作为第二道防线。

## 2. 架构与目录

```text
浏览器 ── localhost:8090 ──► ecommerce-web（FastAPI + Uvicorn，容器内 8090）
                               ├─ /api/*   → 只读查询 MySQL
                               ├─ /assets/*→ 前端构建产物
                               └─ 其他路径 → SPA fallback（index.html）
                                        │
                     ecommerce-net      ▼
                               ecommerce-mysql（ecommerce_dw + ecommerce_rt）
                                        ▲
                               webapp_ro（SELECT 白名单）
```

```text
web/
  backend/
    app/
      main.py            # FastAPI 实例、异常处理、SPA 静态托管
      config.py          # MYSQL_* / WEB_DB_* / WEB_STATIC_DIR 读取
      db.py              # PyMySQL DictCursor + 简单连接池（只读会话）
      date_range.py      # 日期默认区间与 date_from <= date_to 校验
      serialization.py   # Decimal/date/NaN/Infinity → JSON 安全值
      routers/
        meta.py          # /api/health /api/meta
        overview.py      # /api/overview
        items.py         # /api/items
        retention.py     # /api/retention
        realtime.py      # /api/realtime
        quality.py       # /api/quality/runs[/{run_id}]
        ops.py           # /api/ops
    requirements.txt     # fastapi / uvicorn / PyMySQL / python-dotenv（锁版本）
    tests/               # pytest，连真实本地库（webapp_ro）
  frontend/              # React 18 + TS + Vite + ECharts（另一实现方负责）
docker/web/Dockerfile    # node:20-alpine 构建 → python:3.11-slim 运行
scripts/create_webapp_ro.py  # 幂等创建只读账号并授权（SPEC §2）
```

## 3. API 契约（照抄 SPEC §3，v1 冻结）

统一前缀 `/api`，JSON，日期参数 `date_from` / `date_to`（`YYYY-MM-DD`，含端点）。
缺省 = 全部可用区间（`ads_traffic_daily` 的 min/max）。所有响应不得返回
NaN/Infinity；无数据的比率返回 `null`，前端显示 "—"。

### 3.1 `GET /api/health`

```json
{"status":"ok","ts":"ISO8601"}
```

### 3.2 `GET /api/meta`

```json
{
  "date_min": "2015-05-03", "date_max": "2015-09-18",
  "full_days": 137, "partial_days": ["2015-05-03", "2015-09-18"],
  "latest_publish": {"publish_id": 13, "scope_type": "DATES", "dates_csv": "...",
                     "quality_status": "PASS", "published_at": "..."},
  "latest_quality": {"run_id": 8, "status": "PASS", "rules_total": 60,
                     "rules_passed": 60, "rules_failed": 0, "checks_total": 76488744},
  "realtime": {"latest_updated_at": "...", "final_days": 138,
               "rows": 139, "reconciliation": {"MATCH": 1781, "DIFF": 0, "SKIP": 26}}
}
```

### 3.3 `GET /api/overview?date_from&date_to`

```json
{
  "range": {"from": "...", "to": "...", "days": 139},
  "kpis": {
    "pv": 2664218, "uv_daily_avg": 10050.0, "dau_daily_avg": 10110.0,
    "orders": 17591, "view_users": 1396937, "cart_users": 37591,
    "buyer_users": 17591,
    "add_to_cart_rate": 0.0269, "conv_strict_rate": 0.0080,
    "conv_global_rate": 0.0084, "d1_retention": 0.0275
  },
  "traffic":  [{"date": "2015-05-04", "pv": 18681, "uv": 10995, "dau": 11019}],
  "rates":    [{"date": "...", "add_to_cart_rate": 0.0266,
                "conv_strict_rate": 0.0079, "conv_global_rate": 0.0081}],
  "funnel":   [{"stage": "view", "users": 1396937},
               {"stage": "cart", "users": 37591},
               {"stage": "buy",  "users": 17591}],
  "top_items": [{"item_id": 461686, "vcnt": 12345, "acnt": 456, "tcnt": 78}],
  "d1_series": [{"date": "...", "d1_rate": 0.0275}]
}
```

口径：

- `pv = SUM(ads_traffic_daily.pv)`；`uv_daily_avg / dau_daily_avg = AVG(uv/dau)`。
- 订单 = `SUM(ads_funnel_daily.metric_value WHERE metric_name='orders')`。
- 加购率/严格转化率/全局转化率 = **加权** `SUM(分子用户)/SUM(view_users)`，
  `add_to_cart_users = cart_users`；严格转化用户用 `conv_strict_users`；
  全局转化用户用 `conv_global_users`（`ads_funnel_daily` 里均有 metric_name）。
- `d1_retention = SUM(retained_users WHERE age_day=1 AND window_status='full') / SUM(cohort_size WHERE age_day=1 AND window_status='full')`。
  `d1_series` / `cohorts` 仍包含全部 partial 点（不删除、不补零），但 partial 行
  **不参与加权**；`window_status` 是 cohort 级字段，取该 cohort 首个 `age_day` 行
  （因此 2015-09-17 的 cohort 标 `full`，其 `age_day=1` 行标 `partial`）。
- `top_items`：区间内 `ads_item_ranking_daily` 按 `transaction_cnt` 聚合 Top 10。
- `d1_series`：按 cohort_date 的 age_day=1 留存率。

> 本实现严格按上述**公式**计算；SPEC 示例中的数值来自完整窗口的
> `ads_funnel_interval` 快照，仅作结构示例，不改变逐日加权公式。

### 3.4 `GET /api/items?date_from&date_to&limit=20`

```json
{
  "items": [{"item_id": 461686, "view_cnt": 1, "add_to_cart_cnt": 2,
             "transaction_cnt": 3, "view_uv": 4, "transaction_uv": 5}],
  "categories": [{"category_id": 123, "view_cnt": 1, "order_cnt": 2,
                  "view_uv": 3, "transaction_uv": 4}],
  "behavior": [{"date": "...", "items_viewed": 1, "items_carted": 2, "items_bought": 3}]
}
```

口径：`items` = 区间 SUM 后按 `transaction_cnt` 降序取 `limit`；
`categories` = `v_ads_category_daily` 区间 SUM 后按 `view_cnt` 降序前 15；
`behavior` 日粒度来自 `ads_item_behavior_daily`。`limit` 取值 1–100。

### 3.5 `GET /api/retention?date_from&date_to&max_age=14`

```json
{
  "cohorts": [{"cohort_date": "2015-05-04", "cohort_size": 11019,
               "window_status": "full",
               "values": [{"age_day": 0, "rate": 1.0, "window_status": "full"},
                          {"age_day": 1, "rate": 0.051, "window_status": "full"}]}],
  "d1_series": [{"date": "...", "d1_rate": 0.0275, "window_status": "full"}],
  "weighted_d1": 0.0275
}
```

`max_age` 默认 14，最大 30。`cohorts[].window_status` 是 cohort 级字段，取该
cohort 首个 age_day（D0）行；`cohorts[].values[].window_status` 与
`d1_series[].window_status` 是该 age_day 自己的行级状态，前端应据此标注
partial 点并统计 D1 完整窗口 cohort 数（生产为 136 full / 3 partial）。
`weighted_d1` 只对 `window_status='full'` 的 age_day=1 cohort 加权
（38370/1393829=0.027528484）；partial 点仍保留在 `cohorts` 与 `d1_series`，
只是不进入加权。生产 `ads_retention_cohort` 目前只物化 age_day 0/1，因此实际
返回每个 cohort 2 个值；接口仍按 `max_age` 截断。

### 3.6 `GET /api/realtime`

```json
{
  "tables": [{"table": "rt_traffic_daily", "rows": 139, "final_rows": 138,
              "latest_date": "2015-09-18", "updated_at": "..."}],
  "reconciliation": {"MATCH": 1781, "DIFF": 0, "SKIP": 26, "checked_at": "..."},
  "compare": [{"date": "...", "metric": "pv", "offline": 11022,
               "realtime": 11022, "status": "MATCH"}],
  "late_events": 0
}
```

`compare`：`rt_reconciliation_daily` 最近 7 个日期、每个日期全部 13 个 metric，
或按 `date_from/date_to` 过滤（可选参数）。

### 3.7 `GET /api/quality/runs?limit=20`

```json
{"runs": [{"run_id": 8, "scope_type": "DATES", "dates_csv": "...",
           "status": "PASS", "rules_total": 60, "rules_passed": 60,
           "rules_failed": 0, "checks_total": 76488744,
           "started_at": "...", "finished_at": "..."}]}
```

`GET /api/quality/runs/{run_id}` 追加
`"rules": [{"rule_key","category","status","checked","failed_rows","detail","duration_ms"}]`
（`detail` 截断到 500 字符）。`limit` 取值 1–100；不存在返回 404。

### 3.8 `GET /api/ops`

```json
{
  "pipeline_runs": [{"run_id": 7, "dag_run_id": "...", "mode": "FORCE",
                     "window_start": "...", "window_end": "...", "status": "SUCCESS",
                     "published": "PUBLISHED", "started_at": "...", "finished_at": "..."}],
  "publish_log": [{"publish_id": 13, "scope_type": "DATES", "dates_csv": "...",
                   "quality_status": "PASS", "published_at": "..."}],
  "watermark": [{"pipeline": "...", "event_date_utc": "...",
                 "max_ingest_batch": 17, "processed_at": "..."}],
  "table_stats": [{"table": "dwd_events", "rows": 2755641}]
}
```

`table_stats` 由后端白名单表名执行 `COUNT(*)`，只允许 ADS/控制/RT 表；
`dwd_events` 等大表如需展示，用 `information_schema.TABLES.TABLE_ROWS` 近似值并标注
"approx"，避免全表 COUNT 拖慢接口。

> 本实现的 `table_stats` 只包含 `webapp_ro` 已授权的 11 张 DW 物理表 + 5 张 RT
> 表（全部精确 `COUNT(*)`，最大约 1.4 万行）。`dwd_events` 不在只读白名单内，
> 因此不展示，也不用近似值冒充精确行数。`pipeline_runs` / `publish_log` 取最新
> 20 行；`watermark` 每条 pipeline 只返回最新水位一行。

## 4. 权限模型（只读）

新建独立账号 `webapp_ro`（口令在 `.env` 的 `WEB_DB_USER` / `WEB_DB_PASSWORD`，
不入库、不进镜像）。初始化脚本 `scripts/create_webapp_ro.py`：

- `CREATE USER IF NOT EXISTS` + `ALTER USER` 同步口令，可重复执行；
- 仅授予 **SELECT**：
  - `ecommerce_dw`：`ads_traffic_daily`、`ads_funnel_daily`、`ads_funnel_interval`、
    `ads_item_ranking_daily`、`ads_item_behavior_daily`、`ads_retention_cohort`、
    `ads_publish_log`、`quality_check_run`、`quality_check_result`、
    `airflow_pipeline_run`、`etl_watermark`、`v_ads_category_daily`、
    `v_ads_item_orders_daily`、`v_ads_funnel_interval`、`v_ads_publish_status`；
  - `ecommerce_rt`：`rt_traffic_daily`、`rt_funnel_daily`、`rt_item_behavior_daily`、
    `rt_reconciliation_daily`、`rt_late_events`；
- 不读取、不暴露 `ecom_app` 凭据；后端配置也**不会**回退到 `MYSQL_USER` /
  `MYSQL_PASSWORD`；
- 连接建立后执行 `SET SESSION TRANSACTION READ ONLY`，即使误写也会被数据库拒绝；
- `SHOW GRANTS` 只允许出现 `GRANT SELECT ...` / `GRANT USAGE ...`（脚本自检）。

验证：

```bash
python scripts/create_webapp_ro.py            # 幂等授权，并打印 SHOW GRANTS
python scripts/create_webapp_ro.py --dry-run  # 只打印 SQL（口令脱敏）
```

## 5. 部署与运行

### 5.1 配置

`.env`（gitignored）新增：

```dotenv
WEB_UI_PORT=8090
WEB_DB_USER=webapp_ro
WEB_DB_PASSWORD=<本地口令>
```

`docker-compose.yml` 的 `web` 服务注入 `MYSQL_HOST=mysql`、`MYSQL_PORT=3306`、
`MYSQL_DATABASE`、`RT_DATABASE`（默认 `ecommerce_rt`）、`WEB_DB_*`、
`WEB_STATIC_DIR=/app/static`；仅 `depends_on: mysql (service_healthy)`，
不影响现有 6 个服务。

### 5.2 启动

```bash
# 首次 / 口令变更后（宿主机、项目 venv）
python scripts/create_webapp_ro.py

# 构建并启动（只新增 web 容器）
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/Zproject/data-engineering/ecommerce-data-platform && docker compose up -d web"
```

访问 `http://localhost:8090`。健康检查：
`GET /api/health`（容器 healthcheck 使用同一地址）。

宿主机直接调试后端（首次先安装 Web 依赖；容器内已自动安装）：

```bash
uv pip install --python .venv/Scripts/python.exe -r web/backend/requirements.txt
cd web/backend
../../.venv/Scripts/python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8090
```

### 5.3 镜像与前端产物

`docker/web/Dockerfile` 为多阶段构建：

1. `node:20-alpine`：`COPY web ./web`，按 `WEB_FRONTEND_MODE` 决定
   - `auto`（默认）：有前端源码时执行 `npm ci && npm run build`；构建失败且存在
     预构建 `dist` 时回退复用（SPEC §6 网络退化路径）；
   - `build`：强制容器内构建，失败即失败；
   - `prebuilt`：直接使用宿主机预构建的 `web/frontend/dist`（离线构建）；
   - `skip`：写占位 `index.html`，只验证后端镜像（前端尚未就绪时）；
2. `python:3.11-slim`：安装 `web/backend/requirements.txt`，
   拷贝后端到 `/app/app`、前端 `dist` 到 `/app/static`，以非 root 用户运行
   `uvicorn app.main:app --host 0.0.0.0 --port 8090`。

前端最终产物路径约定为 `web/frontend/dist`（Vite 默认），镜像内为
`/app/static`。`.dockerignore` 用 `!web/frontend/dist/**` 保留宿主预构建产物：
`auto`/`build` 优先在容器内从源码构建，网络失败时 `auto` 才回退到预构建
`dist`；`prebuilt` 可显式跳过 `npm`。全新检出没有 `dist` 时，`auto` 同样执行
`npm ci && npm run build`。只想先验证后端镜像时用
`WEB_FRONTEND_MODE=skip` 生成占位页。

### 5.4 路由与静态托管

`/api/*` 由路由处理；未知 `/api/*` 返回 404 JSON。其余路径优先命中
`WEB_STATIC_DIR` 下的真实文件，未命中则回落到 `index.html`，支持
`/items`、`/retention`、`/realtime`、`/quality`、`/analyze`、`/model`
直接刷新。前端未构建时返回内置占位页（200），方便后端联调。

`/realtime` 当前不出现在侧栏导航：页面与 `/api/realtime` 保留，但尚未接入新的实时
数据源，只作为内部批流验证视图，从 `/model` 页进入；等实时链路真正落地后再恢复入口。

### 5.5 上传分析沙箱（不写 MySQL）

只读看板之外，`/analyze` 提供轻量的上传分析链路：用户上传 CSV / Excel，服务端探测
列名与样本值给出字段映射，用户确认后进入单 worker 队列，在任务级 DuckDB 中完成
解析、规范化与聚合。原始文件、`normalized.csv`、`analysis.duckdb`、`result.json`
全部落在 `ANALYSIS_DATA_DIR/<job_id>/`，按 TTL（默认 168h）清理；分析全程不接触
MySQL 连接，也不修改既有四层仓库与 139 天基线。

**页面路由**

| 路由 | 作用 |
| --- | --- |
| `/analyze` | 上传、示例数据、最近任务 |
| `/analyze/:jobId` | 字段映射确认、进度与预计剩余时间、失败/取消重试 |
| `/analyze/:jobId/result` | 结果看板、数据质量、来源与字段可用性 |
| `/model` | 四层批流模型与上传沙箱的说明页 |

**API**

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/api/analysis/uploads` | 上传 CSV / XLSX，返回 `awaiting_mapping` 与字段探测 |
| POST | `/api/analysis/sample` | 创建 14 天、275,404 行的示例任务 |
| GET | `/api/analysis/jobs?limit=` | 最近任务列表 |
| GET | `/api/analysis/jobs/{id}` | 任务状态、进度、探测与映射 |
| POST | `/api/analysis/jobs/{id}/start` | 确认映射并启动；`failed` / `cancelled` 可重试 |
| GET | `/api/analysis/jobs/{id}/result` | 完成后的结果快照（未完成返回 409） |
| DELETE | `/api/analysis/jobs/{id}` | 删除任务目录；运行中先请求取消 |

**两层层级模型**：`fact_events` 保存规范化事件（view / add_to_cart / transaction /
other），`agg_daily`、`agg_funnel`、`agg_retention`、`agg_item`、`agg_category`、
`agg_quality` 由它派生。必填映射为用户、商品、事件类型、事件时间；订单号、类目、
金额、商品名可选，缺失时对应指标自动降级而不是中断。

**上限与并发**：单文件 200MB、500 万行、单 worker、队列 20；进度按已处理行数与
rows/sec EWMA 估算，前端倒计时每次轮询用 `eta_seconds` 重新校准。

**完成转场**：任务页看到运行态后完成，跳转带 `reveal` 标记（`location.state` 与
`analysis-reveal:{job_id}` 双通道，后者保证刷新/拦截时仍能触发）。结果页在已渲染的
内容上播放「案卷展开」——两扇门板从合拢向两侧退开，结果内容同步上浮；路由切换本身
用 `document.startViewTransition` 做短交叉淡入，不支持时退回普通导航。
`prefers-reduced-motion` 时不推门，只用一层墨色帷幕短淡出。直接打开已完成任务或刷新
结果页不会重播。

**彩蛋**：`MagpiePet` 全局挂载一只喜鹊，分析页停在绯红面板上随机走动/啄食，离开页面
飞回侧栏鸟窝（品牌图标已改为鸟窝）；回到分析页时随机选择直飞、掉落再飞、绕路再飞三种
落回方式，点击它会飞出去玩一圈再回来。飞行用 Web Animations，页面隐藏时以超时兜底
落位，减少动态模式下不飞行。

## 6. 错误处理与响应约定

- 参数校验：日期格式错误、`limit`/`max_age` 越界、`run_id` 非整数 → 422；
  `date_from > date_to` → 400；不存在的 `run_id` → 404。
- 连接失败（认证、网络、池耗尽）→ 503 `{"detail":"database temporarily unavailable"}`，
  服务端日志带完整堆栈。
- 查询失败 → 500 `{"detail":"database query failed"}`，日志记录 MySQL 错误码。
- 响应数值：`Decimal` 整数值转 `int`，比率转 `float`；`NaN` / `Infinity`
  统一转 `null`；DB 时间按 UTC ISO-8601（naive 时间补 `Z`）。
- 比率分母为 0 / 为空 → `null`，前端显示 "—"。

## 7. 失败排查

| 现象 | 排查 |
| --- | --- |
| `/api/health` 200，业务接口 503 | 连接/认证问题：`docker compose logs web`；检查 `WEB_DB_USER/PASSWORD` 与 MySQL 网络 |
| MySQL `1045 Access denied` | 口令不同步：重跑 `python scripts/create_webapp_ro.py`（会 `ALTER USER`） |
| MySQL `1142 SELECT command denied` | 授权缺失：重跑初始化脚本，确认 `SHOW GRANTS` 覆盖目标表/视图 |
| 接口 500 且日志为 SQL 错误 | 表结构/DDL 变更未同步；对照 `sql/ddl/` 与 §3 字段 |
| 页面空白 / 404 静态资源 | 检查镜像内 `/app/static/index.html`；`WEB_STATIC_DIR` 是否正确；重新构建前端 |
| 前端路由刷新 404 | 确认请求未走 `/api`，且 FastAPI catch-all 已注册在 API 路由之后 |
| 容器 unhealthy | `docker inspect ecommerce-web` + `docker compose logs web`；healthcheck 探测 `localhost:8090/api/health` |
| WSL `docker` 超时 | WSL2 内存压力，重试命令即可；不要重建 WSL 或改目录权限 |

## 8. 测试与验收

```bash
# 新后端测试（真实本地库、只读账号；含已知值断言与只读权限断言）
.venv/Scripts/python.exe -m pytest web/backend/tests -q

# 上传分析沙箱（字段探测 / 规范化 / 管道 / API 重试）
.venv/Scripts/python.exe -m pytest tests/unit/test_analysis_*.py -q

# 现有测试零回归
.venv/Scripts/python.exe -m pytest tests/unit -q
.venv/Scripts/python.exe -m pytest tests/integration -q

# 代码规范（项目 ruff 配置，src 已包含 web/backend）
.venv/Scripts/ruff.exe check web/backend scripts/create_webapp_ro.py
.venv/Scripts/ruff.exe format --check web/backend scripts/create_webapp_ro.py
```

已知值基线（2026-11 生产库）：`2015-09-17` pv=9830、uv=6231、orders=41；
`2015-09-16` pv=11022；质量运行 #8 PASS 60/60；实时对账
MATCH 1781 / DIFF 0 / SKIP 26。

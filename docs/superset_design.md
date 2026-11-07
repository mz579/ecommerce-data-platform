# Phase 7 — Superset 看板设计文档

> 对应 `README.md`「当前状态」的 Phase 7 阶段。本文记录部署拓扑、访问控制、
> 元数据库隔离、可复现部署、三个看板的指标口径与筛选器、数据刷新与新鲜度、
> 验收证据与实测命令，以及反推过程中的被否决策。

---

## 1. 部署拓扑

```text
Windows 11 ── localhost:8088 ──► Superset 4.1.2（容器，ecommerce-superset）
                                     │  ├─ 元数据库：PostgreSQL 独立库 superset_meta
                                     │  └─ 业务连接：MySQL only-read 账号 superset_ro
                                     │        └─ 仅授权 4 张 ADS 应用表 + 5 张受控视图
MySQL（业务数仓 ecommerce_dw） ◄── superset_ro（SELECT 白名单）
PostgreSQL（元数据库 meta + superset_meta） ◄── superset 元数据（隔离于 Airflow meta）
```

- Web 端口：宿主机 `8088`（`.env` 的 `SUPERSET_UI_PORT`）。
- 一次初始化容器 `superset-init`（`docker/airflow/...` 对等模式），退出码 0 即完成；
  `superset` 服务 `depends_on` 它 `service_completed_successfully`。
- 所有容器共享 `ecommerce-net` 网络；业务 MySQL 与 Postgres 的端口只从宿主机映射
  给运维/CLI 使用（`127.0.0.1`），SUPERSET_MYSQL_HOST 在容器内用服务名 `mysql`。

## 2. 访问控制（只读）

- Superset **不直连 `ecom_app`**。业务连接使用独立只读账号 `superset_ro`，
  权限为 **4 张 ADS 应用表 + 5 张受控视图**（`SHOW GRANTS` 实测见 §9.2）。
- 受控视图（`sql/ddl/ads_views/`）：`v_ads_funnel_interval`、
  `v_ads_category_daily`、`v_ads_item_behavior_daily`、`v_ads_item_orders_daily`、
  `v_ads_publish_status`；`ads_publish_log` 控制表不对 `superset_ro` 开放
  （其新鲜度通过只读视图 `v_ads_publish_status` 暴露计数）。
- 口令不落任何代码/文档：只存在于被 `.gitignore` 忽略的 `.env`（与 `.env.example`
  占位 `CHANGE_ME`）。导入用看板包里的数据库 URL 是对导出口令掩码后的形式；
  `docker/superset/init.sh` Step 5 的 `fix_database_password.py` 从 env 重建口令，
  通过模型 `set_sqlalchemy_uri()` 把真实口令写进 Fernet 加密的 `password` 列、
  URI 列只留掩码（见 §9.3 连接测试：URI 无明文口令）。

## 3. 元数据库隔离（superset_meta）

- Superset 元数据落在 PostgreSQL 独立库 `superset_meta`，与 Airflow 元库 `meta`
  **物理隔离**，分别用不同角色（`SUPERSET_META_USER` vs `POSTGRES_USER`）。
- `superset-init` Step 1 幂等创建 DB 与角色（psycopg2，不依赖 psql 客户端）；
  `superset db upgrade` 执行 alembic 迁移到该库。
- 空元数据库一键重建命令：

```bash
docker compose up -d --force-recreate superset-init && docker compose up -d superset
```

## 4. 部署可复现

- 镜像锁 `apache/superset:4.1.2`，最小化 Dockerfile 只补 `pymysql` +
  `psycopg2-binary`（MySQL 驱动缺失是官方镜像唯一缺口）。
- 初始化（`docker/superset/init.sh`，`set -euo pipefail`，可重复执行）：
  1. 幂等建 `superset_meta` 库/角色；
  2. `superset db upgrade`；
  3. 幂等建 admin + `superset init`；
  4. `superset import-dashboards -p <bundle.zip> -u <admin>` 导入看板 JSON
     （4.1.x 该命令强制要求 `-u/--username`，缺省直接报错）；
  5. 用 env 口信重建业务数据库连接（`fix_database_password.py`）。
- 看板包（`dashboards/exports/`，`docker compose` 只读挂载到容器
  `/opt/superset/exports`）：由 `scripts/superset_setup.py` 声明性生成并导出，
  包含 `charts(21) / dashboards(3) / datasets(12) / databases(1) / metadata.yaml`，
  可原样重新导入（见 §9.5 import 幂等实测）。
- 正常开发路径：`python scripts/superset_setup.py`（Windows venv，走 REST API 对
  生效实例 upsert 数据库/数据集/图表/看板并导出包）；全新环境走 init.sh 导入。

## 5. 三个看板（图表 21 张，dashboard_slices = 11 / 5 / 5）

| 看板 | slug | 图表数 | 内容 |
| --- | --- | --- | --- |
| 运营总览 | `ops-overview` | 11 | 口径说明 panel；6 个 KPI（PV/区间UV/订单/加购/严格转化/全局转化）+ D1 留存；PV/UV/DAU 日趋势；转化率双口径 vs 严格口径（线）；加购转化率日趋势；D1 留存曲线 |
| 用户行为 | `user-behavior` | 5 | 口径说明 panel；漏斗（view→atc→buyer）；DAU/新增用户日趋势；新增用户 D1 留存曲线；新增用户留存热力图；用户会话/日漏斗明细表（9 指标） |
| 商品分析 | `product-analysis` | 5 | 口径说明 panel；累计发布次数 KPI（新鲜度）；Top-20 商品排行表（含 add_to_cart_cnt/transaction_cnt 等）；Top-20 区间 PV 条形（dist_bar）；类目 Top-20 区间 PV/订单（dist_bar）；每日 distinct 商品数（浏览/加购/成交） |

每个看板含一个 **Markdown 口径说明 panel**（说明 139 天周期、UTC 时区、
`orders = distinct(transaction_id)`、首尾天 `is_full_day=0` 等）。

## 6. 指标口径（§6 可直接引用）

- **orders = distinct transaction_id**（去重口径全仓一致）。
- **转化率双口径分开展示**：
  - 严格转化 = 成交用户 / 浏览用户；全局转化 = distinct 成交 / distinct 浏览 —
    **全局口径允许 >100%**，因此两口径不合并为一个数字。
  - 运营看板用「双口径趋势 + 两个 KPI」分别呈现，并用维度筛选器
    `metric_name` 控制线系列（默认双选，取消「严格」后只剩全局系列）。
- **D1 留存**：cohort（首访日）用户在次日回访的比例；部分首/末日
  （2015-05-03、2015-09-18）`is_full_day=0` 不做日级留存混淆。
- **时间范围**：全部图表 `event_date_utc` 驱动，筛选器支持按日期维度筛选。

## 7. 筛选器（原生 Native Filters）

每个看板在 `FILTER_ROW` 放置原生筛选器（`filter_select`），组件 id 与
`json_metadata.native_filter_configuration[].id` **完全一致**（`NATIVE_FILTER-PH7-N`），
否则 filter bar 不渲染。

| 看板 | 筛选器 | 目标 |
| --- | --- | --- |
| 运营总览 | 日期范围（`event_date_utc / cohort_date`） | traffic / funnel_conv / funnel_atc / retention |
| 运营总览 | 转化口径（严格 / 全局）默认双选 | funnel_conv.metric_name |
| 用户行为 | 日期范围（`event_date_utc / cohort_date`） | dau_new / retention |
| 商品分析 | 日期范围（`event_date_utc`） | item_behavior |
| 商品分析 | 类目 `category_id` | category_top |

- **默认值**写在 `defaultDataMask`（`extraFormData.filters` + `filterState.value`），
  只写 `controlValues.defaultValue` 前端不生效。
- 变更采用「staged → APPLY FILTERS」提交语义：筛选器改动后需点击
  `APPLY FILTERS` 按钮才触发全部图表重查（4.1.2 的 filter bar 行为）。

## 8. 刷新与新鲜度

- 看板数据来自 ADS 汇总表 + 受控视图，由 Airflow 日任务链更新；
  刷新链路：`python -m ecommerce_dw.cli publish --run 5`（真实跑一次发布，
  在 `ads_publish_log` 新增一行）→ 商品分析看板的「累计发布次数」KPI 立即 +1。
- 每次发布都写 `ads_publish_log` 审计行，看板通过 `v_ads_publish_status`
  只读视图读取计数（不开放控制表本身）。

## 9. 验收证据（实测命令 + 输出摘录）

下文的完整输出以 Repo 内脚本复跑为准；此处给出命令与结论。

### 9.1 服务与版本

```bash
wsl -d Ubuntu24.04 -- bash -lc "cd /mnt/c/... && docker compose ps"
# ecommerce-superset  Up (healthy)
wsl -d Ubuntu24.04 -- bash -lc "docker compose -f .../docker-compose.yml exec -T superset superset version"
# 4.1.2
```

### 9.2 SHOW GRANTS（superset_ro）

`python scripts/apply_superset_grants.py` 幂等重跑后的实际输出（只读白名单，9 个对象）：

```text
== SHOW GRANTS FOR 'superset_ro'@'%' ==
  GRANT USAGE ON *.* TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_funnel_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_item_ranking_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_retention_cohort` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`ads_traffic_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_category_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_funnel_interval` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_item_behavior_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_item_orders_daily` TO `superset_ro`@`%`
  GRANT SELECT ON `ecommerce_dw`.`v_ads_publish_status` TO `superset_ro`@`%`
```

无任何 INSERT/UPDATE/DELETE；`ads_publish_log` 控制表不开放。

### 9.3 数据库连接（无明文口令）

- `scripts/superset_setup.py` 建连接后执行 `test_connection OK`；图表查询全走该连接，
  §9.6 的 ALL MATCH 即真实跑通证明。
- 4.1.2 的 `dbs` 表：`sqlalchemy_uri` 是**不加密**的 `String(1024)` 列，真正加密的是
  独立 `password` 列（Fernet）。因此重建口令必须走模型自带 `set_sqlalchemy_uri()`
  （`fix_database_password.py` Step 5）：真实口令进加密的 `password` 列，URI 列只留
  掩码 `XXXXXXXXXX`。直接赋 `row.sqlalchemy_uri` 会把明文口令落库（验收中实测抓到并
  修正，见 §10）。
- 落库实测（`psql -U superset_meta_app -d superset_meta`）：

```text
 id | database_name            | uri                                                        | pw_len
----+--------------------------+-------------------------------------+--------
  1 | ecommerce_dw (Superset RO) | mysql+pymysql://superset_ro:XXXXXXXXXX@mysql:3306/...  |   24
```

- 导出看板包里的 `databases/*.yaml` 也只带掩码 URI 且 `allow_dml: false`；真实口令
  只存在于被 `.gitignore` 忽略的 `.env`（compose 注入容器）。

### 9.4 三个看板与图表数

`docker compose exec -T postgres psql -U superset_meta_app -d superset_meta -c
"SELECT d.dashboard_title, count(ds.id) FROM dashboards d JOIN dashboard_slices ds ON ds.dashboard_id = d.id GROUP BY d.id ORDER BY d.id"`

结论：11 / 5 / 5。

### 9.5 包导入幂等

对现有实例重跑 `superset import-dashboards -p <bundle> -u admin` 后：
dashboard_slices 数量保持不变（不翻倍）；`fix_database_password.py` 重跑后连接测试 OK。

### 9.6 关键指标一致性（Superset chart/data API vs 直连 SQL，浮点容差）

`python scripts/superset_evidence.py` 实际输出（5 项关键指标 ALL MATCH）：

```text
chart                                   superset             sql    ok
------------------------------------------------------------------------
PV 总量（139 日）                              2664218         2664218  True
区间 UV（view 口径）                            1396937         1396937  True
DAU 2015-07-26                               17516           17516  True
区间严格转化率                                    0.008          0.0080  True
订单数 2015-07-26（distinct txn）               101.0      101.000000  True
```

同一脚本另输出数据层筛选验证：PV 全区间 2,664,218 vs 仅 07-25..07-27 的 86,250
（与直连 SQL `86,250` 一致）；转化率线「仅严格口径」0.007467 与「两口径全选」0.007694
数值不同 —— 严格/全局两口径分开呈现、不会混用。

### 9.7 筛选器 before/after（UI 实测）

`python scripts/superset_ui_evidence.py`（Playwright 真浏览器，每次为新会话，
先做维度筛选再做日期筛选，避免大并发重查波叠加超过前端 60s 超时）。实际输出：

```text
saved ops_overview_default.png (转化口径 before: both series)
saved ops_overview_filter_metric.png (转化口径: strict deselected)
    conv-trend before contains strict=True global=True   # 双系列：global + strict
    conv-trend after  contains strict=False global=True  # 仅剩 global 单系列
saved ops_overview_filter_date.png (date filter after   PV: 32,077)
    date filter value-change evidence: True  (2,664,218 -> 32,077)
saved product_analysis_after_publish.png (freshness KPI: 9)
```

- 维度筛选（转化口径）：默认双系列，取消 `conv_strict_rate` 后 only
  `conv_global_rate` 单系列（UI 截图 + legend 文本双重验证）；
- 日期筛选（event_date_utc / cohort_date）：ops PV `2,664,218 → 32,077`（2015-07-26）。

### 9.8 刷新数据变化

`python -m ecommerce_dw.cli publish --run 5` 一次后商品分析看板「累计发布次数」
8 → 9；前后两张产品页截图对比可见 KPI 变化（§9.9）。发布链路与截至性口径见 §8。

### 9.9 截图（真实渲染）

`dashboards/screenshots/`（6 张，均来自 `superset_ui_evidence.py` /
`superset_screenshots.py` 的真实 Chromium 渲染）：

| 文件 | 内容 |
| --- | --- |
| `ops_overview_default.png` | 运营总览默认态：KPI 行 + 双口径双系列 |
| `ops_overview_filter_date.png` | 日期筛选 2015-07-26 后：PV 32,077 |
| `ops_overview_filter_metric.png` | 转化口径取消「严格」后：仅全局系列 |
| `user_behavior_default.png` | 用户行为默认态 |
| `product_analysis_before_publish.png` | 商品分析刷新前：累计发布 8 |
| `product_analysis_after_publish.png` | 商品分析刷新后：累计发布 9 |

## 10. 被否决的备选方案

| 方案 | 否决理由 |
| --- | --- |
| Superset 直接连 `ecom_app` | 写权限暴露，不满足只读安全边界 |
| 元数据与 Airflow 共用 `meta` 库 | 升级/迁移互相拖累，破坏隔离（架构文档 §6） |
| 看板用 `viz_type: "bar"`（时间序） | 是时间序列图，groupby 分类轴报错；改 `dist_bar` |
| Markdown 写 `meta.text` | 4.1.2 前端读 `meta.code`，写 text 渲染为空占位 |
| `order_by_cols` 直接用列名 | 走 legacy formData 解析，`JSON.parse` 抛错；改空数组 + 数据集 SQL 内排序 |
| 筛滤器组件 id 用 `FILTER-01` | 与 `native_filter_configuration` id 不一致，filter bar 不渲染 |
| API 导入覆盖 | 4.1.x 无 CLI 覆盖语义时用 `import-dashboards` + `-u` 必需参数 |
| 直接给 `Database.sqlalchemy_uri` 赋值重建口令 | 4.1.2 的 URI 列不加密，直接赋值会把明文口令落库（巡检 `dbs` 表实测抓到）；必须走 `set_sqlalchemy_uri()`：口令进 Fernet 加密的 `password` 列、URI 列只存掩码 |

## 11. 相关文件

- `scripts/superset_setup.py` —— 声明式 upsert（DB/数据集/图表/看板/导出）
- `scripts/superset_evidence.py` —— 指标一致性验收
- `scripts/superset_ui_evidence.py` —— 筛选器 before/after UI 验收
- `scripts/superset_screenshots.py` —— 真实渲染截图
- `docker/superset/{Dockerfile,init.sh,superset_config.py,fix_database_password.py}`
- `dashboards/exports/`、`dashboards/screenshots/`
"""One-key build/refresh of the Phase 7 Superset dashboards (idempotent).

This is the single source of truth for the three dashboards. It talks to the
Superset REST API (admin login) and upserts, keyed by deterministic UUIDs:

  1. the read-only MySQL database connection (superset_ro, from .env);
  2. the virtual SQL datasets (SQL as committed here — they only read the four
     ADS tables and the five controlled views, see docs/superset_design.md);
  3. the charts (three dashboards: 运营总览 / 用户行为 / 商品分析);
  4. the dashboards with native filters (date range + dimension filters);
  5. exports the dashboard bundle to ``dashboards/exports/`` so a fresh
     metadata database can be rebuilt by ``superset-init`` (import + password
     fix).

Usage (project root, venv active, superset webserver up on SUPERSET_UI_PORT):

    python scripts/superset_setup.py

Environment (from .env): SUPERSET_UI_URL / SUPERSET_ADMIN_USERNAME /
SUPERSET_ADMIN_PASSWORD / MYSQL_RO_USER / MYSQL_RO_PASSWORD /
MYSQL_DATABASE / SUPERSET_MYSQL_HOST / SUPERSET_MYSQL_PORT.
"""

from __future__ import annotations

import http.cookiejar
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_NAMESPACE = uuid.UUID("3231fc43-d40b-4554-b9f6-9c08aef3a3d6")

CONN_NAME = "ecommerce_dw (Superset RO)"


def env(name: str, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if val is None:
        raise RuntimeError(f"missing env var {name} (check .env / .env.example)")
    return val


# --------------------------------------------------------------------------- #
# REST API helper                                                              #
# --------------------------------------------------------------------------- #


class SupersetClient:
    """Minimal Superset REST API client (bearer + CSRF token, JSON in/out).

    CSRF in Superset 4.x is bound to the Flask session cookie: the token endpoint
    sets the session cookie, so we must persist it (HTTPCookieProcessor) and send
    it back with every request together with the ``X-CSRFToken`` header.
    """

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPRedirectHandler(),
            urllib.request.HTTPCookieProcessor(self.cookies),
        )
        self.token = ""
        self.csrf_token = ""
        self.token = self._login(username, password)
        self.csrf_token = self._fetch_csrf_token()

    def _login(self, username: str, password: str) -> str:
        payload = {
            "username": username,
            "password": password,
            "provider": "db",
            "refresh": True,
        }
        _, body = self.request("POST", "/api/v1/security/login", payload=payload)
        token = body["access_token"]
        if not token:
            raise RuntimeError("login failed: no access_token returned")
        return token

    def _fetch_csrf_token(self) -> str:
        _, body = self.request("GET", "/api/v1/security/csrf_token/")
        token = body.get("result")
        if not token:
            raise RuntimeError("no CSRF token returned")
        return token

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: dict | list | None = None,
        _depth: int = 0,
    ) -> tuple[int, dict | list]:
        url = self.base_url + path
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        # Superset 对所有写操作都校验 CSRF（即便使用 OAuth2 bearer 认证）
        if self.csrf_token:
            headers["X-CSRFToken"] = self.csrf_token
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=60) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            # Flask strict_slashes 会把无尾斜杠路径重定向（POST 也会返回 308）。
            # urllib 自带 HTTPRedirectHandler 不跨方法跟随 307/308，这里手动跟随。
            newurl = exc.headers.get("Location")
            if exc.code in (301, 302, 307, 308) and newurl and _depth < 3:
                next_path = (
                    newurl[len(self.base_url) :] if newurl.startswith(self.base_url) else newurl
                )
                return self.request(
                    method=method, path=next_path, payload=payload, _depth=_depth + 1
                )
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}\n  {detail[:800]}") from exc
        if not body:
            return 0, {}
        return 0, json.loads(body.decode("utf-8"))

    def list_map(self, kind: str, name_field: str) -> dict[str, int]:
        """Return {name_field: id} for an entity kind (for upsert matching)."""
        rison = f"(columns:!(id,uuid,{name_field}),page_size:500)"
        q = urllib.parse.quote(rison, safe="")
        _, body = self.request("GET", f"/api/v1/{kind}/?q={q}")
        result = body.get("result") or []
        return {str(row[name_field]): row["id"] for row in result if row.get(name_field)}


def _uuid(*parts: str) -> uuid.UUID:
    return uuid.uuid5(_NAMESPACE, ":".join(parts))


# --------------------------------------------------------------------------- #
# Identifiers / connection                                                     #
# --------------------------------------------------------------------------- #


def build_uri() -> str:
    user = env("MYSQL_RO_USER")
    password = env("MYSQL_RO_PASSWORD")
    host = env("SUPERSET_MYSQL_HOST", "mysql")
    port = env("SUPERSET_MYSQL_PORT", "3306")
    database = env("MYSQL_DATABASE", "ecommerce_dw")
    return f"mysql+pymysql://{user}:{quote_plus(password)}@{host}:{port}/{database}?charset=utf8mb4"


# --------------------------------------------------------------------------- #
# Dataset definitions (SQL-first; name -> SQL)                                 #
# --------------------------------------------------------------------------- #

TRAFFIC_SQL = "SELECT event_date_utc, pv, uv, dau, is_full_day FROM ads_traffic_daily"
FUNNEL_SQL = "SELECT event_date_utc, metric_name, metric_value, is_full_day\nFROM ads_funnel_daily"
FUNNEL_CONV_SQL = (
    "SELECT event_date_utc, metric_name, metric_value\n"
    "FROM ads_funnel_daily\n"
    "WHERE metric_name IN ('conv_strict_rate', 'conv_global_rate')"
)
# 加购率（口径 A）日趋势专用数据集。注意：SELECT 必须不含 metric_name——
# 转化口径 native filter 目标是 funnel_conv 的 metric_name 列，数据集暴露同名
# 列时该 filter 会泄漏到本查询（WHERE metric_name IN ('conv_*')），导致与
# 下方 metric_name='add_to_cart_rate' 冲突后返回空集（Phase 7 R16 已修复）。
FUNNEL_ATC_SQL = (
    "SELECT event_date_utc, metric_value\n"
    "FROM ads_funnel_daily\n"
    "WHERE metric_name = 'add_to_cart_rate'"
)
RETENTION_SQL = (
    "SELECT cohort_date, age_day, cohort_size, retained_users, retention_rate,"
    " window_status\n"
    "FROM ads_retention_cohort"
)
INTERVAL_FUNNEL_SQL = (
    "SELECT view_users, cart_users, buyer_users, orders, conv_strict_users,"
    " conv_global_users, add_to_cart_rate, conv_strict_rate, conv_global_rate\n"
    "FROM v_ads_funnel_interval"
)
DAU_NEW_SQL = (
    "SELECT t.event_date_utc, t.uv, t.dau,"
    " COALESCE(r.new_visitors, 0) AS new_visitors\n"
    "FROM ads_traffic_daily t\n"
    "LEFT JOIN (\n"
    "  SELECT cohort_date, cohort_size AS new_visitors\n"
    "  FROM ads_retention_cohort WHERE age_day = 0\n"
    ") r ON r.cohort_date = t.event_date_utc"
)
FUNNEL_STEPS_SQL = (
    "SELECT 'view_users' AS step, 1 AS step_order, u.view_users AS value\n"
    "FROM v_ads_funnel_interval u\n"
    "UNION ALL\n"
    "SELECT 'cart_users', 2, u.cart_users FROM v_ads_funnel_interval u\n"
    "UNION ALL\n"
    "SELECT 'buyer_users', 3, u.buyer_users FROM v_ads_funnel_interval u"
)
RANKING_ORDERS_SQL = (
    "SELECT r.event_date_utc, r.rank_no, r.item_id, r.view_cnt,"
    " r.add_to_cart_cnt, r.transaction_cnt,\n"
    "       COALESCE(o.order_cnt, 0) AS order_cnt, r.total_events\n"
    "FROM ads_item_ranking_daily r\n"
    "LEFT JOIN v_ads_item_orders_daily o\n"
    "  ON o.event_date_utc = r.event_date_utc AND o.item_id = r.item_id"
)
TOP_ITEMS_LIVE_SQL = (
    "SELECT r.rank_no, r.item_id, r.view_cnt, r.add_to_cart_cnt,"
    " r.transaction_cnt,\n"
    "       COALESCE(o.order_cnt, 0) AS order_cnt, r.total_events\n"
    "FROM ads_item_ranking_daily r\n"
    "LEFT JOIN v_ads_item_orders_daily o\n"
    "  ON o.event_date_utc = r.event_date_utc AND o.item_id = r.item_id\n"
    "WHERE r.event_date_utc = (\n"
    "  SELECT MAX(x.event_date_utc) FROM ads_item_ranking_daily x\n"
    "  WHERE x.is_full_day = 1\n"
    ") AND r.rank_no <= 20\n"
    "ORDER BY r.rank_no ASC"
)
CATEGORY_DAILY_SQL = (
    "SELECT event_date_utc, category_id, view_cnt, add_to_cart_cnt,"
    " transaction_cnt, view_uv, add_to_cart_uv, transaction_uv, order_cnt\n"
    "FROM v_ads_category_daily"
)
CATEGORY_TOP_SQL = (
    "SELECT c.category_id,\n"
    "       SUM(c.view_cnt) AS pv,\n"
    "       SUM(c.order_cnt) AS orders,\n"
    "       SUM(c.view_uv) AS uv\n"
    "FROM v_ads_category_daily c\n"
    "WHERE c.category_id <> 0\n"
    "GROUP BY c.category_id\n"
    "ORDER BY pv DESC\n"
    "LIMIT 20"
)
ITEM_BEHAVIOR_SQL = (
    "SELECT event_date_utc, items_viewed, items_carted, items_bought\n"
    "FROM v_ads_item_behavior_daily"
)
FUNNEL_DAY_EXAMPLE_SQL = (
    "SELECT event_date_utc, metric_name, metric_value\n"
    "FROM ads_funnel_daily\n"
    "WHERE event_date_utc = (\n"
    "  SELECT MAX(event_date_utc) FROM ads_funnel_daily WHERE is_full_day = 1\n"
    ") AND metric_name IN (\n"
    "  'view_users','cart_users','buyer_users','orders','add_to_cart_rate',\n"
    "  'conv_strict_users','conv_global_users','conv_strict_rate','conv_global_rate'\n"
    ")"
)
PUBLISH_STATUS_SQL = (
    "SELECT publish_count, latest_publish_id, latest_published_at,"
    " latest_scope_type, latest_dates_csv, latest_quality_run_id\n"
    "FROM v_ads_publish_status"
)

DATASETS: dict[str, str] = {
    "traffic": TRAFFIC_SQL,
    "funnel": FUNNEL_SQL,
    "funnel_conv": FUNNEL_CONV_SQL,
    "funnel_atc": FUNNEL_ATC_SQL,
    "retention": RETENTION_SQL,
    "interval_funnel": INTERVAL_FUNNEL_SQL,
    "dau_new": DAU_NEW_SQL,
    "funnel_steps": FUNNEL_STEPS_SQL,
    "ranking_orders": RANKING_ORDERS_SQL,
    "top_items_live": TOP_ITEMS_LIVE_SQL,
    "category_daily": CATEGORY_DAILY_SQL,
    "category_top": CATEGORY_TOP_SQL,
    "item_behavior": ITEM_BEHAVIOR_SQL,
    "funnel_day_example": FUNNEL_DAY_EXAMPLE_SQL,
    "publish_status": PUBLISH_STATUS_SQL,
}

# --------------------------------------------------------------------------- #
# Chart builders                                                               #
# --------------------------------------------------------------------------- #


def metric(sql_expr: str, label: str | None = None) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expr,
        "label": label or sql_expr,
    }


def in_filter(column: str, values: list) -> dict:
    return {
        "expressionType": "SIMPLE",
        "clause": "WHERE",
        "operator": "in",
        "subject": column,
        "comparator": values,
        "isExtra": False,
    }


def eq_filter(column: str, value) -> dict:
    return {
        "expressionType": "SIMPLE",
        "clause": "WHERE",
        "operator": "==",
        "subject": column,
        "comparator": value,
        "isExtra": False,
    }


OPS_MARKDOWN = (
    "## 运营总览 · Operations Overview（Phase 7）\n\n"
    "- **统计时间范围**：2015-05-03 03:00 ~ 2015-09-18 02:59 **UTC**，"
    "共 **139** 个业务日。首尾两日（05-03 / 09-18）为**不完整日**"
    "（`is_full_day=0`），趋势图按实际数据展示、口径说明已标注。\n"
    "- **时区**：全线 UTC；日界 = 世界时零点（`event_date_utc`）。\n"
    "- **关键口径**：UV = 当日发生 `view` 的去重 `visitor_id`；"
    "DAU = 当日**任意事件**的去重 visitor；订单数 = `distinct(transaction_id)`；"
    "加购率（口径 A）= 加购用户数 ÷ 浏览用户数；"
    "**严格转化率** = 交易用户 ∩ 浏览用户 ÷ 浏览用户；"
    "**全局转化率** = 全部交易用户 ÷ 浏览用户（可 **>100%**，属正常）；"
    "留存 = 首次观测 cohort 的 D1 回流；区间口径 = 完整窗口 [05-04, 09-17]。\n"
    "- 事件量 ≠ 订单量：`transaction` 事件行数 ≠ `distinct(transaction_id)`。"
)

USER_MARKDOWN = (
    "## 用户行为 · User Behavior（Phase 7）\n\n"
    "- **统计时间范围**：2015-05-03 03:00 ~ 2015-09-18 02:59 **UTC**"
    "（139 个业务日，首末日不完整）。\n"
    "- **时区**：UTC。\n"
    "- **关键口径**：漏斗为用户级（`view_users → cart_users → buyer_users`，"
    "区间口径，完整窗口）；DAU = 任意事件去重 visitor；新增访客 = 首次观测日"
    "（cohort）规模 `age_day=0`；留存 = 首次观测 cohort 的 D1 回流率"
    "（`window_status='full'` 才参与曲线，partial 不打 0）。\n"
    "- 匿名 Cookie 会重置，留存口径存在系统性高估（局限说明）。"
)

PRODUCT_MARKDOWN = (
    "## 商品分析 · Product Analysis（Phase 7）\n\n"
    "- **统计时间范围**：2015-05-03 03:00 ~ 2015-09-18 02:59 **UTC**"
    "（139 个业务日，首末日不完整）。\n"
    "- **时区**：UTC。\n"
    "- **关键口径**：**订单数 = `distinct(transaction_id)`**（禁止把 "
    "`transaction` 事件行数叫订单数）；商品排行按浏览量排序、订单数旁挂；"
    "类目表现剔除未分类桶（`category_id=0`）；浏览/加购/成交商品数 = 当日 "
    "distinct item，为「每日商品数」口径。\n"
    "- 商品日排行表格与 Top 图默认取**最新完整日**（2015-09-17）。"
)


def kpi(
    metric_def: dict,
    label: str,
    fmt: str,
    subheader: str | None = None,
    filters: list | None = None,
) -> dict:
    """标量 KPI 卡片（viz_type: big_number_total）。

    说明：Superset 4.1.2 的 ``big_number``（带趋势线）在 buildQuery 里强制
    ``is_timeseries: true``——当数据集没有可用的时间列或查询不带时间范围时，
    要么报 “Datetime column not provided”，要么把 KPI 渲染成“最近一天”的值。
    因此标量大盘指标统一改用 ``big_number_total``：它的 buildQuery 只透传
    baseQueryObject，不强制 timeseries；配合 ``time_range: "No filter"`` 得到
    的是全区间标量聚合（口径见 docs/superset_design.md）。
    """
    params = {
        "metric": metric_def,
        "subheader": subheader or label,
        "time_range": "No filter",
        "y_axis_format": fmt,
        "header_font_size": 0.5,
        "subheader_font_size": 0.14,
        "viz_type": "big_number_total",
    }
    if filters:
        params["adhoc_filters"] = filters
    return params


CHARTS: list[dict] = [
    # ---- 运营总览（说明面板用 MARKDOWN 组件，见 _position_json） ----
    {
        "key": "ops_pv",
        "name": "PV 总量（139 日）",
        "viz_type": "big_number_total",
        "dataset": "traffic",
        "params": kpi(
            metric("SUM(pv)", "PV 总量（139 日含部分日）"), "全区间 PV（139 日）", ",.0f"
        ),
    },
    {
        "key": "ops_uv",
        "name": "区间 UV（view 口径）",
        "viz_type": "big_number_total",
        "dataset": "interval_funnel",
        "params": kpi(metric("view_users", "区间 UV"), "区间 UV（完整窗口）", ",.0f"),
    },
    {
        "key": "ops_orders",
        "name": "区间订单数（distinct txn）",
        "viz_type": "big_number_total",
        "dataset": "interval_funnel",
        "params": kpi(metric("orders", "区间订单数"), "区间订单数（完整窗口）", ",.0f"),
    },
    {
        "key": "ops_atc",
        "name": "区间加购率（口径 A）",
        "viz_type": "big_number_total",
        "dataset": "interval_funnel",
        "params": kpi(metric("add_to_cart_rate", "加购率A"), "区间加购率（口径 A）", ".2%"),
    },
    {
        "key": "ops_conv_strict",
        "name": "区间严格转化率",
        "viz_type": "big_number_total",
        "dataset": "interval_funnel",
        "params": kpi(metric("conv_strict_rate", "严格转化率"), "区间严格转化率", ".2%"),
    },
    {
        "key": "ops_conv_global",
        "name": "区间全局转化率",
        "viz_type": "big_number_total",
        "dataset": "interval_funnel",
        "params": kpi(
            metric("conv_global_rate", "全局转化率"), "区间全局转化率（可 >100%）", ".2%"
        ),
    },
    {
        "key": "ops_retention",
        "name": "D1 留存率（加权）",
        "viz_type": "big_number_total",
        "dataset": "retention",
        "params": kpi(
            metric("SUM(retained_users)/NULLIF(SUM(cohort_size),0)", "D1 留存率"),
            "D1 留存率（完整 cohort 加权）",
            ".2%",
            filters=[eq_filter("age_day", 1), eq_filter("window_status", "full")],
        ),
    },
    {
        "key": "ops_traffic_trend",
        "name": "PV / UV / DAU 日趋势（139 日）",
        "viz_type": "line",
        "dataset": "traffic",
        "params": {
            "granularity_sqla": "event_date_utc",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": [
                metric("SUM(pv)", "PV"),
                metric("SUM(uv)", "UV"),
                metric("SUM(dau)", "DAU"),
            ],
            "adhoc_filters": [],
            "groupby": [],
            "row_limit": 10000,
            "line_width": 2,
            "show_legend": True,
            "x_axis_format": "smart_date",
            "y_axis_format": ",.0f",
            "rich_tooltip": True,
            "curve_style": "linear",
            "viz_type": "line",
        },
    },
    {
        "key": "ops_conv_trend",
        "name": "转化率双口径（严格 vs 全局）日趋势",
        "viz_type": "line",
        "dataset": "funnel_conv",
        "params": {
            "granularity_sqla": "event_date_utc",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": [metric("AVG(metric_value)", "转化率")],
            "adhoc_filters": [],
            "groupby": ["metric_name"],
            "row_limit": 10000,
            "line_width": 2,
            "si_scale": True,
            "show_legend": True,
            "x_axis_format": "smart_date",
            "y_axis_format": ".2%",
            "rich_tooltip": True,
            "curve_style": "linear",
            "viz_type": "line",
        },
    },
    {
        "key": "ops_atc_trend",
        "name": "加购率（口径 A）日趋势",
        "viz_type": "line",
        "dataset": "funnel_atc",
        "params": {
            "granularity_sqla": "event_date_utc",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": [metric("AVG(metric_value)", "加购率 A")],
            "adhoc_filters": [],
            "groupby": [],
            "row_limit": 10000,
            "line_width": 2,
            "show_legend": True,
            "x_axis_format": "smart_date",
            "y_axis_format": ".2%",
            "rich_tooltip": True,
            "curve_style": "linear",
            "viz_type": "line",
        },
    },
    {
        "key": "ops_retention_curve",
        "name": "次日留存率 D1 曲线（逐 cohort）",
        "viz_type": "line",
        "dataset": "retention",
        "params": {
            "granularity_sqla": "cohort_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": [metric("AVG(retention_rate)", "D1 留存率")],
            "adhoc_filters": [eq_filter("age_day", 1), eq_filter("window_status", "full")],
            "groupby": [],
            "row_limit": 10000,
            "line_width": 2,
            "show_legend": True,
            "x_axis_format": "smart_date",
            "y_axis_format": ".2%",
            "rich_tooltip": True,
            "viz_type": "line",
        },
    },
    # ---- 用户行为（说明面板用 MARKDOWN 组件） ----
    {
        "key": "user_funnel",
        "name": "用户级漏斗（浏览→加购→成交，区间口径）",
        "viz_type": "funnel",
        "dataset": "funnel_steps",
        "params": {
            "groupby": ["step"],
            "metric": metric("AVG(value)", "用户数"),
            "row_limit": 100,
            "adhoc_filters": [],
            "viz_type": "funnel",
            "show_legend": True,
            "number_format": ",.0f",
        },
    },
    {
        "key": "user_dau_new",
        "name": "DAU 与新增访客（首次观测）日趋势",
        "viz_type": "line",
        "dataset": "dau_new",
        "params": {
            "granularity_sqla": "event_date_utc",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": [metric("SUM(dau)", "DAU"), metric("SUM(new_visitors)", "新增访客")],
            "adhoc_filters": [],
            "groupby": [],
            "row_limit": 10000,
            "line_width": 2,
            "show_legend": True,
            "x_axis_format": "smart_date",
            "y_axis_format": ",.0f",
            "rich_tooltip": True,
            "viz_type": "line",
        },
    },
    {
        "key": "user_retention_curve",
        "name": "留存曲线（按首次观测日 cohort）",
        "viz_type": "line",
        "dataset": "retention",
        "params": {
            "granularity_sqla": "cohort_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": [metric("AVG(retention_rate)", "D1 留存率")],
            "adhoc_filters": [eq_filter("age_day", 1), eq_filter("window_status", "full")],
            "groupby": [],
            "row_limit": 10000,
            "line_width": 2,
            "show_legend": True,
            "x_axis_format": "smart_date",
            "y_axis_format": ".2%",
            "rich_tooltip": True,
            "viz_type": "line",
        },
    },
    {
        "key": "user_retention_heatmap",
        "name": "留存 cohort 热力图（cohort × age_day）",
        "viz_type": "heatmap",
        "dataset": "retention",
        "params": {
            "all_columns_x": "age_day",
            "all_columns_y": "cohort_date",
            "metric": metric("100*SUM(retained_users)/NULLIF(SUM(cohort_size),0)", "留存率 %"),
            "adhoc_filters": [eq_filter("window_status", "full")],
            "row_limit": 1000,
            "show_legend": True,
            "show_values": True,
            "linear_color_scheme": "blue_white_yellow",
            "viz_type": "heatmap",
            "normalize_across": "heatmap",
            "left_margin": 30,
            "right_margin": 20,
            "bottom_margin": 30,
            "top_margin": 40,
        },
    },
    {
        "key": "user_daily_funnel",
        "name": "漏斗日报（最新完整日 9 口径明细表）",
        "viz_type": "table",
        "dataset": "funnel_day_example",
        "params": {
            "granularity_sqla": None,
            "time_range": "No filter",
            "columns": ["metric_name", "metric_value"],
            "metrics": [],
            "groupby": [],
            "order_by_cols": [],
            "row_limit": 100,
            "adhoc_filters": [],
            "viz_type": "table",
            "table_timestamp_format": "smart_date",
            "page_length": 20,
            "include_search": False,
            "show_cell_bars": False,
        },
    },
    # ---- 商品分析（说明面板用 MARKDOWN 组件） ----
    {
        "key": "product_top_table",
        "name": "商品日排行 Top-20（最新完整日，订单数=distinct txn）",
        "viz_type": "table",
        "dataset": "top_items_live",
        "params": {
            "time_range": "No filter",
            "columns": [
                "rank_no",
                "item_id",
                "view_cnt",
                "add_to_cart_cnt",
                "transaction_cnt",
                "order_cnt",
            ],
            # 不能 order_by_cols: ["rank_no"]：4.1.2 前端把 order_by_cols 映射成
            # orderby 并逐项 JSON.parse，纯列名抛 “Found invalid orderby options”。
            # 排序由数据集 SQL（ORDER BY r.rank_no ASC + LIMIT）保证，因此置空。
            "metrics": [],
            "groupby": [],
            "order_by_cols": [],
            "row_limit": 100,
            "adhoc_filters": [],
            "viz_type": "table",
            "table_timestamp_format": "smart_date",
            "page_length": 20,
            "include_search": True,
            "show_cell_bars": True,
        },
    },
    # ---- 商品 Top/类目柱状图：必须用 dist_bar（分类柱），不能用 "bar" ----
    # 4.1.2 的 viz_type "bar" 是时间序列柱状图，buildQuery 强制 is_timeseries:
    # true；数据集没有可直接做时间轴的列时后端报 “Datetime column not
    # provided...”。dist_bar 才是纯分类柱（groupby 做 x 轴，metrics 做柱子）。
    {
        "key": "product_top_bar",
        "name": "商品 Top-20 浏览 PV 与订单数（最新完整日）",
        "viz_type": "dist_bar",
        "dataset": "top_items_live",
        "params": {
            "time_range": "No filter",
            "metrics": [metric("AVG(view_cnt)", "浏览 PV"), metric("AVG(order_cnt)", "订单数")],
            "adhoc_filters": [],
            "groupby": ["item_id"],
            "columns": [],
            "row_limit": 20,
            "order_by_cols": [],
            "viz_type": "dist_bar",
            "color_scheme": "echarts_tropical",
            "show_legend": True,
            "show_values": True,
        },
    },
    {
        "key": "product_category_bar",
        "name": "类目维度表现 Top-20（区间 PV / 订单数）",
        "viz_type": "dist_bar",
        "dataset": "category_top",
        "params": {
            "time_range": "No filter",
            "metrics": [metric("pv", "区间 PV"), metric("orders", "区间订单数")],
            "adhoc_filters": [],
            "groupby": ["category_id"],
            "columns": [],
            "row_limit": 20,
            "order_by_cols": [],
            "viz_type": "dist_bar",
            "color_scheme": "echarts_office",
            "show_legend": True,
            "show_values": True,
        },
    },
    {
        "key": "product_item_behavior",
        "name": "浏览/加购/成交商品数对比（每日 distinct item）",
        "viz_type": "line",
        "dataset": "item_behavior",
        "params": {
            "granularity_sqla": "event_date_utc",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "metrics": [
                metric("SUM(items_viewed)", "浏览商品数"),
                metric("SUM(items_carted)", "加购商品数"),
                metric("SUM(items_bought)", "成交商品数"),
            ],
            "adhoc_filters": [],
            "groupby": [],
            "row_limit": 10000,
            "line_width": 2,
            "show_legend": True,
            "x_axis_format": "smart_date",
            "y_axis_format": ",.0f",
            "rich_tooltip": True,
            "viz_type": "line",
        },
    },
    {
        "key": "product_publish_status",
        "name": "数据发布 / 新鲜度（ads_publish_log 计数）",
        "viz_type": "big_number_total",
        "dataset": "publish_status",
        "params": kpi(metric("publish_count", "发布次数"), "累计发布次数（刷新后更新）", ",.0f"),
    },
]

CHART_KEYS = {chart["key"]: chart for chart in CHARTS}

# --------------------------------------------------------------------------- #
# Dashboards                                                                   #
# --------------------------------------------------------------------------- #


def _kpi_row(specs: list[tuple[str, int]]) -> list[dict]:
    """Layout helper: one row with chart keys and widths (sum <= 12)."""
    return [{"key": key, "width": width} for key, width in specs]


OPS_ROWS = [
    [{"kind": "markdown", "text": OPS_MARKDOWN, "width": 12}],
    _kpi_row(
        [
            ("ops_pv", 2),
            ("ops_uv", 2),
            ("ops_orders", 2),
            ("ops_atc", 2),
            ("ops_conv_strict", 2),
            ("ops_conv_global", 2),
        ]
    ),
    [{"key": "ops_retention", "width": 3}],
    [{"key": "ops_traffic_trend", "width": 12}],
    [{"key": "ops_conv_trend", "width": 6}, {"key": "ops_atc_trend", "width": 6}],
    _kpi_row([("ops_retention_curve", 12)]),
]

USER_ROWS = [
    [{"kind": "markdown", "text": USER_MARKDOWN, "width": 12}],
    [{"key": "user_funnel", "width": 4}],
    [{"key": "user_dau_new", "width": 6}, {"key": "user_retention_curve", "width": 6}],
    [{"key": "user_retention_heatmap", "width": 6}, {"key": "user_daily_funnel", "width": 6}],
]

PRODUCT_ROWS = [
    [{"kind": "markdown", "text": PRODUCT_MARKDOWN, "width": 12}],
    [{"key": "product_publish_status", "width": 3}],
    [{"key": "product_top_table", "width": 6}, {"key": "product_top_bar", "width": 6}],
    [{"key": "product_category_bar", "width": 6}, {"key": "product_item_behavior", "width": 6}],
]

DASHBOARDS = [
    {
        "key": "ops",
        "title": "运营总览 / Operations Overview",
        "slug": "ops-overview",
        "rows": OPS_ROWS,
        "filters": [
            {
                "name": "日期范围（event_date_utc / cohort_date）",
                "filter_type": "filter_select",
                "targets": [
                    ("traffic", "event_date_utc"),
                    ("funnel_conv", "event_date_utc"),
                    ("funnel_atc", "event_date_utc"),
                    ("retention", "cohort_date"),
                ],
            },
            {
                "name": "转化口径（严格 / 全局）",
                "filter_type": "filter_select",
                "targets": [("funnel_conv", "metric_name")],
                "default_value": ["conv_strict_rate", "conv_global_rate"],
            },
        ],
    },
    {
        "key": "user",
        "title": "用户行为 / User Behavior",
        "slug": "user-behavior",
        "rows": USER_ROWS,
        "filters": [
            {
                "name": "日期范围（event_date_utc / cohort_date）",
                "filter_type": "filter_select",
                "targets": [
                    ("dau_new", "event_date_utc"),
                    ("retention", "cohort_date"),
                ],
            },
        ],
    },
    {
        "key": "product",
        "title": "商品分析 / Product Analysis",
        "slug": "product-analysis",
        "rows": PRODUCT_ROWS,
        "filters": [
            {
                "name": "日期范围（event_date_utc）",
                "filter_type": "filter_select",
                "targets": [("item_behavior", "event_date_utc")],
            },
            {
                "name": "类目（category_id）",
                "filter_type": "filter_select",
                "targets": [("category_top", "category_id")],
            },
        ],
    },
]

FULL_HEADER = "ecommerce-data-platform · Phase 7 Superset 看板"


def _position_json(
    header_text: str,
    rows: list[list[dict]],
    chart_ids: dict[str, int],
    filter_ids: list[str],
) -> str:
    components: dict[str, dict] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "children": [],
            "parents": [],
            "meta": {"text": header_text},
        },
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"], "parents": []},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": ["FILTER_ROW", *[f"ROW-{n}" for n in range(len(rows))]],
            "parents": ["ROOT_ID"],
        },
        "FILTER_ROW": {
            "type": "ROW",
            "id": "FILTER_ROW",
            "children": filter_ids,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
    }
    for n, row_items in enumerate(rows):
        row_id = f"ROW-{n}"
        comp_ids: list[str] = []
        for item in row_items:
            if item.get("kind") == "markdown":
                cid = f"MARKDOWN-{n}"
                comp_ids.append(cid)
                components[cid] = {
                    "type": "MARKDOWN",
                    "id": cid,
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", row_id],
                    # 注意：Superset 4.1.2 前端 Markdown 组件读的是 meta.code
                    # （见 fe879...chunk.js：markdownSource = component.meta.code），
                    # 不是 meta.text。写 text 会被写入 DB、导出也带 text，但前端
                    # 只认 code，否则渲染出空的 markdown 占位符。
                    "meta": {
                        "width": item["width"],
                        "height": 50,
                        "code": item["text"],
                        "position": {"x": 0, "y": 0, "width": item["width"], "height": 50},
                    },
                }
                continue
            chart_id = chart_ids[item["key"]]
            cid = f"CHART-{chart_id}"
            comp_ids.append(cid)
            components[cid] = {
                "type": "CHART",
                "id": cid,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": chart_id,
                    "width": item["width"],
                    "height": 50,
                    "sliceName": CHART_KEYS[item["key"]]["name"],
                    "position": {"x": 0, "y": 0, "width": item["width"], "height": 50},
                    "uuid": str(_uuid("chart", item["key"])),
                },
            }
        components[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": comp_ids,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
    for fid in filter_ids:
        components[fid] = {
            "type": "NATIVE_FILTER",
            "id": fid,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "FILTER_ROW"],
            "meta": {
                "width": 12,
                "height": 40,
                "chartId": None,
                "position": {"x": 0, "y": 0, "width": 12, "height": 40},
            },
        }
    return json.dumps(components)


def _json_metadata(filters: list[dict], dataset_ids: dict[str, int]) -> str:
    native = []
    for idx, spec in enumerate(filters):
        fid = f"NATIVE_FILTER-PH7-{idx + 1}"
        targets = [
            {"datasetId": dataset_ids[ds_key], "column": {"name": col}}
            for ds_key, col in spec["targets"]
        ]
        control_values: dict = {
            "multiSelect": True,
            "enableEmptyFilter": False,
            "defaultValue": spec.get("default_value", []),
            "searchAllOptions": True,
            "sortAscending": True,
            "inverseSelection": False,
        }
        # 默认选中值要写到 defaultDataMask（不写 filter bar 前端不应用默认过滤，
        # select 光是 controlValues.defaultValue 不会生效）：
        #   extraFormData.filters 会在查询前合并成 WHERE metric_name IN (...)。
        default_mask: dict = {
            "extraFormData": {},
            "filterState": {},
            "ownState": {},
        }
        if spec.get("default_value"):
            default_mask = {
                "extraFormData": {
                    "filters": [
                        {"col": spec["targets"][0][1], "op": "IN", "val": spec["default_value"]},
                    ],
                },
                "filterState": {
                    "value": spec["default_value"],
                    "label": list(spec["default_value"]),
                },
                "ownState": {},
            }
        native.append(
            {
                "id": fid,
                "name": spec["name"],
                "filterType": "filter_select",
                "cascadeParentIds": [],
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "controlValues": control_values,
                "targets": targets,
                "defaultDataMask": default_mask,
                "metrics": [],
                "adhoc_filters": [],
                "dateFilter": False,
            }
        )
    metadata = {
        "filter_bar_orientation": "vertical",
        "show_native_filters": True,
        "native_filter_configuration": native,
        "expanded_slices": {},
        "refresh_frequency": 0,
        "color_scheme": None,
        "label_colors": {},
        "shared_label_colors": {},
        "timed_refresh_immune_slices": [],
        "chart_configuration": {},
        "cross_filters_enabled": False,
    }
    return json.dumps(metadata)


# --------------------------------------------------------------------------- #
# main                                                                          #
# --------------------------------------------------------------------------- #


def _upsert_database(client: SupersetClient, existing: dict[str, int]) -> int:
    db_payload = {
        "database_name": CONN_NAME,
        "sqlalchemy_uri": build_uri(),
        "configuration_method": "sqlalchemy_form",
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_file_upload": False,
        "allow_run_async": False,
        "extra": json.dumps(
            {
                "metadata_params": {},
                "engine_params": {},
                "metadata_cache_timeout": {},
                "schemas_allowed_for_file_upload": [],
            }
        ),
    }
    if CONN_NAME in existing:
        db_id = existing[CONN_NAME]
        client.request("PUT", f"/api/v1/database/{db_id}", payload=db_payload)
        print(f"[database] updated  #{db_id} {CONN_NAME}")
    else:
        _, body = client.request("POST", "/api/v1/database", payload=db_payload)
        db_id = body["id"]
        print(f"[database] created   #{db_id} {CONN_NAME}")
    # test_connection evidence
    try:
        client.request(
            "POST",
            "/api/v1/database/test_connection",
            payload={"sqlalchemy_uri": build_uri()},
        )
        print("[database] test_connection OK")
    except RuntimeError as exc:
        print(f"[database] test_connection FAILED: {exc} (continue anyway)")
    return int(db_id)


def _prune_stale_columns(client: SupersetClient, ds_id: int, sql: str) -> None:
    """Delete dataset columns that are NOT in the SQL's result projection.

    Phase 7 R16 regression guard: when a SQL dataset's ``SELECT`` lists fewer
    columns than before, Superset 4.1.2 keeps the vanished column in the
    dataset metadata.  The 转化口径 native filter matches a column *by name*
    against every dataset in the dashboard, so a stale ``metric_name`` column
    on ``ds_funnel_atc`` re-leaks the filter's
    ``WHERE metric_name IN ('conv_strict_rate','conv_global_rate')`` onto the
    atc chart query -> outside-the-subquery reference to an unknown column ->
    1054 MySQL error (previously masked as an empty set / No results).
    Dropping any column the current SQL no longer projects closes the hole.

    Column names come from the DB engine's own introspection of the SQL
    ("SELECT * FROM (<sql>) LIMIT 0" -> cursor.description), i.e. the same
    truth Superset uses at dataset POST time, not a string parser.
    """
    import pymysql

    try:
        conn = pymysql.connect(
            host=env("MYSQL_HOST", "127.0.0.1"),
            port=int(env("MYSQL_PORT", "3306")),
            user=env("MYSQL_RO_USER"),
            password=env("MYSQL_RO_PASSWORD"),
            database=env("MYSQL_DATABASE", "ecommerce_dw"),
            charset="utf8mb4",
            connect_timeout=10,
            read_timeout=30,
        )
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM ({sql}) AS _stale LIMIT 0")
            cols = [d[0] for d in cur.description]
        finally:
            conn.close()
    except Exception as exc:  # introspection failure must not break the setup run
        print(f"  [column-sync] introspect failed for ds #{ds_id}: {exc}")
        return
    _, body = client.request("GET", f"/api/v1/dataset/{ds_id}")
    for col in body["result"]["columns"]:
        if col["column_name"] not in cols:
            try:
                client.request("DELETE", f"/api/v1/dataset/{ds_id}/column/{col['id']}")
                print(
                    f"  [column-sync] ds #{ds_id} dropped stale column "
                    f"{col['column_name']!r} (not in SQL projection)"
                )
            except Exception as exc:
                print(f"  [column-sync] ds #{ds_id} could not drop {col['column_name']!r}: {exc}")


def _upsert_datasets(
    client: SupersetClient,
    db_id: int,
    existing: dict[str, int],
) -> dict[str, int]:
    ds_ids: dict[str, int] = {}
    for key, sql in DATASETS.items():
        table_name = f"ds_{key}"
        if table_name in existing:
            ds_id = existing[table_name]
            # PUT 不接受 database 外键（POST 必须带）
            payload = {"schema": "ecommerce_dw", "table_name": table_name, "sql": sql}
            client.request("PUT", f"/api/v1/dataset/{ds_id}", payload=payload)
            print(f"  [dataset] updated #{ds_id} {table_name}")
        else:
            payload = {
                "database": db_id,
                "schema": "ecommerce_dw",
                "table_name": table_name,
                "sql": sql,
            }
            _, body = client.request("POST", "/api/v1/dataset", payload=payload)
            ds_id = body["id"]
            print(f"  [dataset] created #{ds_id} {table_name}")
        ds_ids[key] = int(ds_id)
        _prune_stale_columns(client, int(ds_id), sql)
    return ds_ids


def _upsert_charts(
    client: SupersetClient,
    ds_ids: dict[str, int],
    existing: dict[str, int],
) -> dict[str, int]:
    chart_ids: dict[str, int] = {}
    for spec in CHARTS:
        key = spec["key"]
        ds_id = ds_ids[spec["dataset"]]
        params = dict(spec["params"])
        params["datasource"] = f"{ds_id}__table"
        payload = {
            "datasource_id": ds_id,
            "datasource_type": "table",
            "slice_name": spec["name"],
            "viz_type": spec["viz_type"],
            "params": json.dumps(params),
        }
        if spec["name"] in existing:
            chart_id = existing[spec["name"]]
            client.request("PUT", f"/api/v1/chart/{chart_id}", payload=payload)
            print(f"  [chart]  updated #{chart_id} {key}")
        else:
            _, body = client.request("POST", "/api/v1/chart", payload=payload)
            chart_id = body["id"]
            print(f"  [chart]  created #{chart_id} {key}")
        chart_ids[key] = int(chart_id)
    return chart_ids


def _upsert_dashboards(
    client: SupersetClient,
    chart_ids: dict[str, int],
    ds_ids: dict[str, int],
    existing: dict[str, int],
) -> list[int]:
    dash_ids: list[int] = []
    for spec in DASHBOARDS:
        # 原生筛选器的组件 id 必须与 json_metadata.native_filter_configuration[].id
        # 完全一致（NATIVE_FILTER-PH7-N），否则前端 filter bar 渲染为空。
        filter_ids = [f"NATIVE_FILTER-PH7-{i}" for i in range(1, len(spec["filters"]) + 1)]
        position = _position_json(FULL_HEADER, spec["rows"], chart_ids, filter_ids)
        metadata = _json_metadata(spec["filters"], ds_ids)
        payload = {
            "dashboard_title": spec["title"],
            "slug": spec["slug"],
            "position_json": position,
            "json_metadata": metadata,
            "published": True,
            "owners": [1],
        }
        if spec["title"] in existing:
            dash_id = existing[spec["title"]]
            client.request("PUT", f"/api/v1/dashboard/{dash_id}", payload=payload)
            print(f"  [dashboard] updated #{dash_id} {spec['slug']}")
        else:
            _, body = client.request("POST", "/api/v1/dashboard", payload=payload)
            dash_id = body["id"]
            print(f"  [dashboard] created #{dash_id} {spec['slug']}")
            # published flag can only be set at creation
        dash_ids.append(int(dash_id))
    return dash_ids


def _export_dashboards(client: SupersetClient, dash_ids: list[int]) -> Path:
    out_dir = PROJECT_ROOT / "dashboards" / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = out_dir / f"superset_dashboards_{stamp}.zip"
    q = urllib.parse.quote("[" + ",".join(map(str, dash_ids)) + "]", safe="")
    url = f"{client.base_url}/api/v1/dashboard/export/?q={q}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {client.token}"})
    with client.opener.open(req, timeout=120) as resp:
        content = resp.read()
    out.write_bytes(content)
    print(f"[export] wrote {out.relative_to(PROJECT_ROOT)} ({len(content)} bytes)")
    return out


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    base = env("SUPERSET_UI_URL", "http://localhost:8088")
    client = SupersetClient(
        base, env("SUPERSET_ADMIN_USERNAME", "admin"), env("SUPERSET_ADMIN_PASSWORD")
    )

    print("== upsert database connection ==")
    db_existing = client.list_map("database", "database_name")
    db_id = _upsert_database(client, db_existing)

    print("== upsert datasets ==")
    ds_existing = client.list_map("dataset", "table_name")
    ds_ids = _upsert_datasets(client, db_id, ds_existing)

    print("== upsert charts ==")
    ch_existing = client.list_map("chart", "slice_name")
    chart_ids = _upsert_charts(client, ds_ids, ch_existing)

    print("== upsert dashboards ==")
    d_existing = client.list_map("dashboard", "dashboard_title")
    dash_ids = _upsert_dashboards(client, chart_ids, ds_ids, d_existing)

    print("== export ==")
    _export_dashboards(client, dash_ids)

    print("\nsetup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())

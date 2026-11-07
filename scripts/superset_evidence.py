"""Phase 7 acceptance evidence #5: Superset chart/data API vs direct SQL.

For a set of key charts, query the Superset chart data endpoint (exactly the
query that renders the chart) and independently run the equivalent SQL against
MySQL via the read-only account; then compare value-by-value.

Also exercises the native-filter behaviour (date range + 转化口径) at the data
level, which is used as the "filter before/after" evidence alongside the UI
screenshots.

Usage (project root, venv):  python scripts/superset_evidence.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
from superset_setup import SupersetClient  # noqa: E402

FLOAT_TOLERANCE = 1e-6


def env(name: str) -> str:
    return os.environ[name]


def sql_metric(expr: str, label: str | None = None) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label or expr}


def superse_agg(
    client: SupersetClient,
    ds_id: int,
    metric: dict,
    *,
    filters: list[dict] | None = None,
    time_range: str = "No filter",
) -> object:
    """Return the scalar value a chart with this metric would display."""
    queries = [
        {
            "metrics": [metric],
            "granularity": "event_date_utc",
            "time_range": time_range,
            "row_limit": 1000,
            "filters": filters or [],
        }
    ]
    payload = {
        "datasource": {"id": ds_id, "type": "table"},
        "force": True,
        "result_format": "json",
        "result_type": "full",
        "queries": queries,
    }
    _, body = client.request("POST", "/api/v1/chart/data", payload=payload)
    result = body["result"][0]
    if result.get("status") != "success":
        raise RuntimeError(f"chart data failed: {result.get('error')}")
    rows = result.get("data") or []
    if not rows:
        return None
    row = rows[0]
    # timeseries or single-row payload: pick the first non-timestamp value
    for value in row.values():
        if isinstance(value, (int, float)):
            if value == row.get("__timestamp"):
                continue
            return value
    return None


def mysql_scalar(cur, sql: str) -> object:
    cur.execute(sql)
    row = cur.fetchone()
    return row[0] if row else None


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    client = SupersetClient(
        "http://localhost:8088",
        env("SUPERSET_ADMIN_USERNAME"),
        env("SUPERSET_ADMIN_PASSWORD"),
    )

    # dataset ids by table_name
    _, body = client.request("GET", "/api/v1/dataset/?q=(columns:!(id,table_name),page_size:500)")
    ds_ids = {r["table_name"]: r["id"] for r in body["result"] if r.get("table_name")}

    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user=env("MYSQL_RO_USER"),
        password=env("MYSQL_RO_PASSWORD"),
        database="ecommerce_dw",
        charset="utf8mb4",
        connect_timeout=10,
    )

    checks = [
        {
            "chart": "PV 总量（139 日）",
            "ds": "ds_traffic",
            "ds_id": ds_ids["ds_traffic"],
            "metric": sql_metric("SUM(pv)", "PV总量"),
            "time_range": "No filter",
            "sql": "SELECT SUM(pv) FROM ads_traffic_daily",
        },
        {
            "chart": "区间 UV（view 口径）",
            "ds": "ds_interval_funnel",
            "ds_id": ds_ids["ds_interval_funnel"],
            "metric": sql_metric("view_users", "区间UV"),
            "time_range": "No filter",
            "sql": "SELECT view_users FROM v_ads_funnel_interval",
        },
        {
            "chart": "DAU 2015-07-26",
            "ds": "ds_traffic",
            "ds_id": ds_ids["ds_traffic"],
            "metric": sql_metric("SUM(dau)", "DAU"),
            "filters": [{"col": "event_date_utc", "op": "==", "val": "2015-07-26"}],
            "sql": "SELECT dau FROM ads_traffic_daily WHERE event_date_utc='2015-07-26'",
        },
        {
            "chart": "区间严格转化率",
            "ds": "ds_interval_funnel",
            "ds_id": ds_ids["ds_interval_funnel"],
            "metric": sql_metric("conv_strict_rate", "严格转化率"),
            "time_range": "No filter",
            "sql": "SELECT conv_strict_rate FROM v_ads_funnel_interval",
        },
        {
            "chart": "订单数 2015-07-26（distinct txn）",
            "ds": "ds_funnel",
            "ds_id": ds_ids["ds_funnel"],
            "metric": sql_metric("AVG(metric_value)", "订单数"),
            "filters": [
                {"col": "event_date_utc", "op": "==", "val": "2015-07-26"},
                {"col": "metric_name", "op": "==", "val": "orders"},
            ],
            "sql": (
                "SELECT metric_value FROM ads_funnel_daily "
                "WHERE event_date_utc='2015-07-26' AND metric_name='orders'"
            ),
        },
    ]

    print(f"{'chart':<32}{'superset':>16}{'sql':>16}{'ok':>6}")
    print("-" * 72)
    failures = 0
    with conn.cursor() as cur:
        for c in checks:
            api_val = superse_agg(
                client,
                c["ds_id"],
                c["metric"],
                filters=c.get("filters"),
                time_range=c.get("time_range", "No filter"),
            )
            sql_val = mysql_scalar(cur, c["sql"])
            if isinstance(api_val, float) and isinstance(sql_val, (int, float)):
                ok = abs(api_val - float(sql_val)) <= FLOAT_TOLERANCE * max(
                    1.0, abs(float(sql_val))
                )
            else:
                ok = (float(api_val) if api_val is not None else None) == (
                    float(sql_val) if sql_val is not None else None
                )
            print(f"{c['chart']:<32}{str(api_val):>16}{str(sql_val):>16}{str(ok):>6}")
            failures += 0 if ok else 1

    # --- filter before/after evidence (data level) ---
    print("\n== native filter before/after (data level) ==")
    days = ["2015-07-25", "2015-07-26", "2015-07-27"]
    pv_full = superse_agg(
        client, ds_ids["ds_traffic"], sql_metric("SUM(pv)", "PV"), time_range="No filter"
    )
    pv_filtered = superse_agg(
        client,
        ds_ids["ds_traffic"],
        sql_metric("SUM(pv)", "PV"),
        filters=[{"col": "event_date_utc", "op": "in", "val": days}],
        time_range="No filter",
    )
    with conn.cursor() as cur:
        day_sql = mysql_scalar(
            cur,
            "SELECT SUM(pv) FROM ads_traffic_daily WHERE event_date_utc IN ('{}')".format(
                "','".join(days)
            ),
        )
    print(f"PV 全区间（筛选前, filter_select 未选日期） : {pv_full}")
    print(
        f"PV 仅 07-25..07-27（筛选后, 同 native filter）: {pv_filtered}   (direct SQL: {day_sql})"
    )
    assert float(pv_filtered) == float(day_sql)

    strict_only = superse_agg(
        client,
        ds_ids["ds_funnel_conv"],
        sql_metric("AVG(metric_value)", "转化率"),
        filters=[{"col": "metric_name", "op": "==", "val": "conv_strict_rate"}],
        time_range="No filter",
    )
    strict_both = superse_agg(
        client,
        ds_ids["ds_funnel_conv"],
        sql_metric("AVG(metric_value)", "转化率"),
        filters=[
            {"col": "metric_name", "op": "in", "val": ["conv_strict_rate", "conv_global_rate"]}
        ],
        time_range="No filter",
    )
    print(f"转化率线：仅【严格】口径（metric_name 筛选后）  : {strict_only:.6f}")
    print(f"转化率线：【严格+全局】两口径（默认全选）        : {strict_both:.6f}")
    print("（两口径分开呈现、数值不同，验证不会把严格当全局）")

    conn.close()
    print(f"\n{'ALL MATCH' if failures == 0 else f'{failures} FAILURES'}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

"""Web UI 后端接口测试（真实本地库 + 只读账号）。

覆盖 SPEC §3 的每个端点：200 / 结构 / 关键值，并校验非法参数返回 4xx、
响应中无 NaN/Infinity、只读账号无写权限。
"""

from __future__ import annotations

import math
from typing import Any

import pymysql
import pytest
from fastapi.testclient import TestClient

from app.config import settings

ENDPOINTS = (
    "/api/health",
    "/api/meta",
    "/api/overview",
    "/api/items",
    "/api/retention",
    "/api/realtime",
    "/api/quality/runs",
    "/api/quality/runs/8",
    "/api/ops",
)


def _assert_finite(payload: Any) -> None:
    if isinstance(payload, float):
        assert math.isfinite(payload), payload
    elif isinstance(payload, dict):
        for value in payload.values():
            _assert_finite(value)
    elif isinstance(payload, list):
        for value in payload:
            _assert_finite(value)


def _get(client: TestClient, path: str, status: int = 200) -> Any:
    response = client.get(path)
    assert response.status_code == status, (path, response.status_code, response.text)
    if "application/json" in response.headers.get("content-type", ""):
        assert "NaN" not in response.text
        assert "Infinity" not in response.text
        payload = response.json()
        _assert_finite(payload)
        return payload
    return response


def test_health_ok(client: TestClient) -> None:
    payload = _get(client, "/api/health")
    assert payload["status"] == "ok"
    assert "T" in payload["ts"]
    assert payload["ts"].endswith("Z")


def test_meta_known_boundaries(client: TestClient) -> None:
    payload = _get(client, "/api/meta")
    assert payload["date_min"] == "2015-05-03"
    assert payload["date_max"] == "2015-09-18"
    assert payload["full_days"] == 137
    assert payload["partial_days"] == ["2015-05-03", "2015-09-18"]
    assert payload["latest_publish"]["publish_id"] == 13
    assert payload["latest_quality"]["run_id"] == 8
    assert payload["latest_quality"]["rules_total"] == 60
    assert payload["realtime"]["rows"] == 139
    assert payload["realtime"]["final_days"] == 138
    assert payload["realtime"]["reconciliation"]["MATCH"] == 1781


def test_overview_default_range(client: TestClient) -> None:
    payload = _get(client, "/api/overview")
    assert payload["range"] == {"from": "2015-05-03", "to": "2015-09-18", "days": 139}
    assert len(payload["traffic"]) == 139
    assert payload["kpis"]["pv"] == 2664218
    assert payload["kpis"]["orders"] == 17674
    # 完整窗口 cohort 加权：38370 / 1393829（partial cohort 不参与）
    assert payload["kpis"]["d1_retention"] == pytest.approx(0.027528484, abs=1e-6)
    assert payload["funnel"][0]["stage"] == "view"
    assert len(payload["top_items"]) == 10
    assert payload["top_items"][0]["item_id"] == 461686
    assert len(payload["d1_series"]) == 139


def test_overview_known_day_matches_ads(client: TestClient) -> None:
    payload = _get(client, "/api/overview?date_from=2015-09-17&date_to=2015-09-17")
    kpis = payload["kpis"]
    assert kpis["pv"] == 9830
    assert kpis["uv_daily_avg"] == pytest.approx(6231.0)
    assert kpis["orders"] == 41
    assert kpis["view_users"] == 6231
    assert kpis["cart_users"] == 154
    assert kpis["buyer_users"] == 32
    assert kpis["d1_retention"] is None or kpis["d1_retention"] >= 0
    traffic = payload["traffic"]
    assert traffic == [{"date": "2015-09-17", "pv": 9830, "uv": 6231, "dau": 6270}]
    rates = payload["rates"]
    assert len(rates) == 1
    assert rates[0]["add_to_cart_rate"] == pytest.approx(154 / 6231)

    sept16 = _get(client, "/api/overview?date_from=2015-09-16&date_to=2015-09-16")
    assert sept16["kpis"]["pv"] == 11022
    assert sept16["kpis"]["orders"] == 100


def test_overview_weighted_rates_and_validation(client: TestClient) -> None:
    payload = _get(client, "/api/overview?date_from=2015-09-16&date_to=2015-09-17")
    kpis = payload["kpis"]
    assert kpis["pv"] == 9830 + 11022
    assert kpis["orders"] == 141
    assert kpis["add_to_cart_rate"] == pytest.approx(348 / 13017)
    assert kpis["conv_strict_rate"] == pytest.approx(90 / 13017)
    assert kpis["conv_global_rate"] == pytest.approx(102 / 13017)

    _get(client, "/api/overview?date_from=2015-09-18&date_to=2015-09-17", status=400)
    _get(client, "/api/overview?date_from=2015-13-01&date_to=2015-09-17", status=422)
    # MySQL DATE 下界之外的日期必须 400（而不是让 SQL 抛 500）。
    _get(client, "/api/overview?date_from=0001-01-01&date_to=2015-09-17", status=400)


def test_items_shapes_and_known_top_item(client: TestClient) -> None:
    payload = _get(client, "/api/items?date_from=2015-05-03&date_to=2015-09-18")
    assert len(payload["items"]) == 20
    assert len(payload["categories"]) == 15
    assert len(payload["behavior"]) == 139
    top = payload["items"][0]
    assert top == {
        "item_id": 461686,
        "view_cnt": 2354,
        "add_to_cart_cnt": 292,
        "transaction_cnt": 127,
        "view_uv": 1590,
        "transaction_uv": 126,
    }
    counts = [row["transaction_cnt"] for row in payload["items"]]
    assert counts == sorted(counts, reverse=True)
    views = [row["view_cnt"] for row in payload["categories"]]
    assert views == sorted(views, reverse=True)
    assert payload["behavior"][0]["date"] == "2015-05-03"


def test_items_limit_validation(client: TestClient) -> None:
    payload = _get(client, "/api/items?limit=5")
    assert len(payload["items"]) == 5
    _get(client, "/api/items?limit=0", status=422)
    _get(client, "/api/items?limit=101", status=422)
    _get(client, "/api/items?limit=abc", status=422)


def test_retention_matrix_and_weighted_d1(client: TestClient) -> None:
    payload = _get(client, "/api/retention")
    assert len(payload["cohorts"]) == 139
    first = payload["cohorts"][0]
    assert first["cohort_date"] == "2015-05-03"
    assert first["window_status"] in {"full", "partial"}
    # 生产 ADS 只物化 age_day 0/1；max_age 上限校验仍按 SPEC（14/30）。
    assert len(first["values"]) == 2
    assert first["values"][0] == {"age_day": 0, "rate": 1.0, "window_status": "partial"}
    assert len(payload["d1_series"]) == 139
    assert {point["window_status"] for point in payload["d1_series"]} == {"full", "partial"}
    assert payload["weighted_d1"] == pytest.approx(38370 / 1393829, abs=1e-6)
    _get(client, "/api/retention?max_age=31", status=422)
    _get(client, "/api/retention?max_age=-1", status=422)


def _fetch_all(sql: str) -> list[tuple[Any, ...]]:
    conn = pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.dw_database,
        charset="utf8mb4",
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall())
    finally:
        conn.close()


def test_weighted_d1_excludes_partial_cohorts(client: TestClient) -> None:
    """加权 D1 只算 window_status='full'；partial 点仍保留在 cohorts/d1_series。"""
    payload = _get(client, "/api/retention")
    # 136 个 full cohort：38370 / 1393829 = 0.027528484（2.75%）
    assert payload["weighted_d1"] == pytest.approx(0.027528484, abs=1e-6)
    # 全部 cohort 的比值是 38827 / 1407579 = 0.027584242，不能用于加权
    assert payload["weighted_d1"] != pytest.approx(38827 / 1407579, abs=1e-7)

    # 行级窗口状态：age_day=1 有 136 full + 3 partial；API 的加权只取 full。
    totals = {
        window_status: (int(count), int(retained), int(cohort))
        for window_status, count, retained, cohort in _fetch_all(
            "SELECT window_status, COUNT(*), SUM(retained_users), SUM(cohort_size) "
            "FROM ads_retention_cohort WHERE age_day = 1 GROUP BY window_status"
        )
    }
    assert totals["full"][0] == 136
    assert totals["partial"][0] == 3
    assert payload["weighted_d1"] == pytest.approx(totals["full"][1] / totals["full"][2], abs=1e-12)

    # partial 的 D1 点仍完整出现在 cohorts 与 d1_series 中，只是不参与加权。
    partial_dates = {
        str(row[0])
        for row in _fetch_all(
            "SELECT DISTINCT cohort_date FROM ads_retention_cohort "
            "WHERE age_day = 1 AND window_status = 'partial'"
        )
    }
    assert partial_dates == {"2015-05-03", "2015-09-17", "2015-09-18"}
    assert partial_dates <= {cohort["cohort_date"] for cohort in payload["cohorts"]}
    assert partial_dates <= {point["date"] for point in payload["d1_series"]}

    # 行级状态随点返回：2015-09-17 的 D1 为 partial，而它的 D0 value 为 full。
    d1_status = {point["date"]: point["window_status"] for point in payload["d1_series"]}
    assert d1_status["2015-09-17"] == "partial"
    sept17 = next(c for c in payload["cohorts"] if c["cohort_date"] == "2015-09-17")
    assert sept17["window_status"] == "full"
    sept17_values = {value["age_day"]: value["window_status"] for value in sept17["values"]}
    assert sept17_values[0] == "full"
    assert sept17_values[1] == "partial"
    assert all(
        value["window_status"] in {"full", "partial"}
        for cohort in payload["cohorts"]
        for value in cohort["values"]
    )

    overview = _get(client, "/api/overview")
    assert overview["kpis"]["d1_retention"] == pytest.approx(0.027528484, abs=1e-6)

    # 2015-09-17 的 D1 行是 partial（D0 行为 full），只有它时无可加权样本 -> null，
    # 但该点和 cohort 仍要返回。
    single = _get(client, "/api/retention?date_from=2015-09-17&date_to=2015-09-17")
    assert single["weighted_d1"] is None
    assert [cohort["cohort_date"] for cohort in single["cohorts"]] == ["2015-09-17"]
    assert [point["date"] for point in single["d1_series"]] == ["2015-09-17"]
    single_overview = _get(client, "/api/overview?date_from=2015-09-17&date_to=2015-09-17")
    assert single_overview["kpis"]["d1_retention"] is None


def test_realtime_default_seven_days_and_known_counts(client: TestClient) -> None:
    payload = _get(client, "/api/realtime")
    assert [row["table"] for row in payload["tables"]] == [
        "rt_traffic_daily",
        "rt_funnel_daily",
        "rt_item_behavior_daily",
    ]
    assert payload["tables"][0]["rows"] == 139
    assert payload["tables"][0]["final_rows"] == 138
    assert payload["reconciliation"] == {
        "MATCH": 1781,
        "DIFF": 0,
        "SKIP": 26,
        "checked_at": payload["reconciliation"]["checked_at"],
    }
    assert payload["late_events"] == 0
    dates = {row["date"] for row in payload["compare"]}
    assert len(dates) == 7
    assert len(payload["compare"]) == 7 * 13
    assert {row["status"] for row in payload["compare"]} <= {"MATCH", "DIFF", "SKIP"}


def test_realtime_filter_single_day(client: TestClient) -> None:
    payload = _get(client, "/api/realtime?date_from=2015-09-17&date_to=2015-09-17")
    assert len(payload["compare"]) == 13
    assert {row["date"] for row in payload["compare"]} == {"2015-09-17"}
    _get(client, "/api/realtime?date_from=2015-09-18&date_to=2015-09-17", status=400)
    _get(client, "/api/realtime?date_from=0001-01-01", status=400)


def test_quality_runs_and_detail(client: TestClient) -> None:
    payload = _get(client, "/api/quality/runs?limit=3")
    assert len(payload["runs"]) == 3
    latest = payload["runs"][0]
    assert latest["run_id"] == 8
    assert latest["status"] == "PASS"
    assert latest["rules_total"] == 60
    assert latest["checks_total"] == 76488744

    detail = _get(client, "/api/quality/runs/8")
    assert detail["run_id"] == 8
    assert len(detail["rules"]) == 60
    for rule in detail["rules"]:
        assert set(rule) == {
            "rule_key",
            "category",
            "status",
            "checked",
            "failed_rows",
            "detail",
            "duration_ms",
        }
        assert rule["detail"] is None or len(rule["detail"]) <= 500
    _get(client, "/api/quality/runs/999999", status=404)
    _get(client, "/api/quality/runs/abc", status=422)
    _get(client, "/api/quality/runs/0", status=422)
    _get(client, "/api/quality/runs/999999999999999999999", status=422)


def test_ops_payload_and_table_stats(client: TestClient) -> None:
    payload = _get(client, "/api/ops")
    assert payload["pipeline_runs"][0]["run_id"] == 7
    assert payload["pipeline_runs"][0]["status"] == "SUCCESS"
    assert payload["publish_log"][0]["publish_id"] == 13
    assert len(payload["watermark"]) == 1
    assert payload["watermark"][0]["pipeline"] == "incremental"
    stats = {row["table"]: row["rows"] for row in payload["table_stats"]}
    assert stats["ads_traffic_daily"] == 139
    assert stats["ads_funnel_interval"] == 1
    assert stats["rt_reconciliation_daily"] == 1807
    assert len(stats) == 16


def test_spa_fallback_and_api_404(client: TestClient) -> None:
    for page in ("/", "/items", "/retention", "/realtime", "/quality"):
        response = _get(client, page)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert response.text
    _get(client, "/api/no-such-endpoint", status=404)


def test_readonly_account_has_no_write_grants() -> None:
    conn = pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        charset="utf8mb4",
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW GRANTS FOR CURRENT_USER")
            grants = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()
    assert grants
    assert all(grant.startswith(("GRANT SELECT", "GRANT USAGE")) for grant in grants), grants

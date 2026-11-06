"""Integration tests for the DWS/ADS aggregation build (Phase 4).

Stages a hand-built synthetic ODS dataset into the dedicated ``*_test`` schema
(never production), runs the real Phase-3 DWD build for test-only UTC days in
2099, then runs the DWS/ADS aggregation build and compares *every* output row
against baselines computed by hand and written into this module -- verified
independently of any SQL used by the build statements.

Coverage matrix (each maps to a hand-computed number below):

- same-user multiple views (v1 sees 1001 twice, one UV)
- cart -> transaction (v3 view + add_to_cart + one user / two order ids)
- duplicate ODS event lands in ``dwd_events_duplicate``, never counted
- illegal events (unknown type / missing item / bad timestamp) rejected
- repeated ``transaction_id`` rows dedup to one order (COUNT DISTINCT)
- cross-day visitor (v3 active on D1 and D2) drives retention + per-day PV
- late second-batch event triggers a per-day rebuild (D1 PV 6 -> 7, UV stable)
- unknown "0" visitor sentinel counts PV only (UV / DAU excluded)
- zero-view day yields NULL funnel rates instead of 0
- as-of category joins: latest snapshot wins, future snapshots excluded,
  same-day snapshot included, non-categoryid / non-numeric rows ignored
- idempotency: rebuilding the same scope reproduces identical content
"""

from __future__ import annotations

import csv
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from ecommerce_dw.dwd import builder as dwd_builder
from ecommerce_dw.dwd.transform import date_ms_range
from ecommerce_dw.dws import builder as agg_builder
from ecommerce_dw.dws.build_log import AggBuildLogRepo
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo

_EVENTS_HEADER = ["timestamp", "visitorid", "event", "itemid", "transactionid"]
_PROPS_HEADER = ["timestamp", "itemid", "property", "value"]
_EVENTS_TABLE = "ods_events_raw"
_PROPS_TABLE = "ods_item_properties_raw"
_DWD_TABLES = (
    "dwd_events",
    "dwd_events_duplicate",
    "dwd_events_reject",
    "dwd_events_stage",
)
_AGG_TABLES = (
    "dws_user_daily",
    "dws_item_daily",
    "dws_category_daily",
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_item_ranking_daily",
    "ads_retention_cohort",
)

D1 = date(2099, 1, 1)
D2 = date(2099, 1, 2)
D3 = date(2099, 1, 3)
D4 = date(2099, 1, 4)
D5 = date(2099, 1, 5)
_DAYS = (D1, D2, D3, D4)
_ALL_CSV = "2099-01-01,2099-01-02,2099-01-03,2099-01-04"
# Test-only full window: [D1, D3] keeps D4 as a partial day for the markers.
_WIN = (D1, D3)

_EXPECTED_STATS = {
    "dws_user_daily": 14,
    "dws_item_daily": 14,
    "dws_category_daily": 8,
    "ads_traffic_daily": 4,
    "ads_funnel_daily": 36,
    "ads_item_ranking_daily": 14,
    "ads_retention_cohort": 8,
}


def _d1_start() -> int:
    return date_ms_range(D1)[0]


def _sample_file1_rows() -> list[list[str]]:
    s1, _ = date_ms_range(D1)
    s2, _ = date_ms_range(D2)
    s3, _ = date_ms_range(D3)
    s4, _ = date_ms_range(D4)
    return [
        # ---- D1: 6 visitors, PV=6 / UV=4 ----
        [str(s1 + 1000), "1", "view", "1001", ""],
        [str(s1 + 1001), "1", "view", "1001", ""],  # 同访客多次浏览
        [str(s1 + 1002), "2", "view", "1002", ""],
        [str(s1 + 1003), "2", "view", "1002", ""],
        [str(s1 + 1004), "3", "view", "3003", ""],
        [str(s1 + 1005), "3", "addtocart", "3003", ""],  # 加购 -> 交易
        [str(s1 + 1006), "3", "transaction", "3003", "70001"],
        [str(s1 + 1007), "3", "transaction", "3003", "70002"],
        [str(s1 + 1008), "5", "addtocart", "5050", ""],  # 只加购、从不浏览
        [str(s1 + 1009), "9", "view", "9090", ""],
        [str(s1 + 1010), "9", "transaction", "4004", "70003"],
        [str(s1 + 1011), "10", "addtocart", "5051", ""],  # 只加购、从不浏览
        [str(s1 + 1012), "9", "transaction", "4004", "70003"],  # 重复订单号 -> 去重
        # ---- D2: cross-day return + sentinel + new visitors ----
        [str(s2 + 1000), "3", "view", "3003", ""],  # 跨日回访
        [str(s2 + 1001), "0", "view", "11111", ""],  # 哨兵访客 -> 只增 PV
        [str(s2 + 1002), "5", "addtocart", "5050", ""],
        [str(s2 + 1003), "6", "transaction", "1010", "70021"],  # 无浏览交易用户
        # ---- D3: zero-view day (all funnel rates NULL) ----
        [str(s3 + 1000), "7", "addtocart", "5050", ""],
        [str(s3 + 1001), "11", "addtocart", "1010", ""],
        # ---- D4: cohort tail (window partial) ----
        [str(s4 + 1000), "7", "transaction", "5051", "70031"],
        [str(s4 + 1001), "12", "transaction", "5051", "70041"],
        # ---- exact duplicate of the first row (same dedup key) ----
        [str(s1 + 1000), "1", "view", "1001", ""],
        # ---- illegal events: rejected, never aggregated ----
        [str(s2 + 2000), "8", "purchase", "5051", ""],  # 未知事件类型
        [str(s1 + 2000), "3", "view", "", ""],  # 缺商品
        ["oops", "5", "view", "5051", ""],  # 非整数时间戳
    ]


def _sample_file2_rows() -> list[list[str]]:
    """Late batch: v3's *second* D1 view of item 3003 (arrives later)."""
    s1, _ = date_ms_range(D1)
    return [[str(s1 + 3000), "3", "view", "3003", ""]]


def _sample_prop_rows() -> list[list[str]]:
    s1, _ = date_ms_range(D1)
    s3, _ = date_ms_range(D3)
    s5, _ = date_ms_range(D5)
    before = s1 - 1000  # 2098-12-31 23:59:59.000 (before the window)
    return [
        [str(before), "1001", "categoryid", "501"],
        [str(before), "1002", "categoryid", "502"],
        [str(before), "5050", "categoryid", "501"],
        [str(before), "5051", "categoryid", "502"],
        [str(before), "2001", "categoryid", "601"],
        [str(s3 + 10 * 3600 * 1000), "1010", "categoryid", "503"],  # 同日快照 01-03 10:00
        [str(s5), "3003", "categoryid", "700"],  # 未来快照 01-05 -> 对 3003 全部排除
        [str(before), "1001", "size", "42"],  # 非 categoryid -> 忽略
        [str(before), "1002", "categoryid", "abc"],  # value 非数字 -> 忽略
        [str(s1 - 5000), "1001", "categoryid", "800"],  # 更旧快照 -> 输给 501
    ]


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def _stage_events(cfg, tmp_path: Path, rows: list[list[str]]) -> str:
    source_file = f"events_dws_it_{uuid.uuid4().hex[:10]}.csv"
    src = _write_csv(tmp_path / source_file, _EVENTS_HEADER, rows)
    return loader.load_csv(cfg, src, _EVENTS_TABLE).source_file


def _stage_props(cfg, tmp_path: Path) -> str:
    source_file = f"props_dws_it_{uuid.uuid4().hex[:10]}.csv"
    src = _write_csv(tmp_path / source_file, _PROPS_HEADER, _sample_prop_rows())
    return loader.load_csv(cfg, src, _PROPS_TABLE).source_file


def _rows(cfg, sql: str, params: tuple = ()) -> list[tuple]:
    conn = agg_builder.connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return list(cur.fetchall())
    finally:
        conn.close()


def _scalar(cfg, sql: str, params: tuple = ()):
    return _rows(cfg, sql, params)[0][0]


def _traffic(cfg) -> dict[date, tuple]:
    rows = _rows(
        cfg,
        "SELECT event_date_utc, pv, uv, dau, is_full_day FROM ads_traffic_daily",
    )
    return {d: (pv, uv, dau, full) for d, pv, uv, dau, full in rows}


def _snapshot(cfg) -> list[tuple[str, list[tuple]]]:
    """Content of the seven output tables without the volatile lineage columns."""
    specs = {
        "dws_user_daily": (
            "event_date_utc, visitor_id, visitor_zero, view_cnt, add_to_cart_cnt, "
            "transaction_cnt, order_cnt, active_flag",
            "event_date_utc, visitor_id",
        ),
        "dws_item_daily": (
            "event_date_utc, item_id, view_cnt, add_to_cart_cnt, transaction_cnt, "
            "view_uv, add_to_cart_uv, transaction_uv, order_cnt",
            "event_date_utc, item_id",
        ),
        "dws_category_daily": (
            "event_date_utc, category_id, view_cnt, add_to_cart_cnt, transaction_cnt, "
            "view_uv, add_to_cart_uv, transaction_uv, order_cnt",
            "event_date_utc, category_id",
        ),
        "ads_traffic_daily": (
            "event_date_utc, pv, uv, dau, is_full_day",
            "event_date_utc",
        ),
        "ads_funnel_daily": (
            "event_date_utc, metric_name, metric_value, is_full_day",
            "event_date_utc, metric_name",
        ),
        "ads_item_ranking_daily": (
            "event_date_utc, item_id, rank_no, view_cnt, add_to_cart_cnt, "
            "transaction_cnt, view_uv, add_to_cart_uv, transaction_uv, total_events, "
            "is_full_day",
            "event_date_utc, rank_no",
        ),
        "ads_retention_cohort": (
            "cohort_date, age_day, cohort_size, retained_users, retention_rate, window_status",
            "cohort_date, age_day",
        ),
    }
    out: list[tuple[str, list[tuple]]] = []
    for table, (cols, order) in specs.items():
        rows = _rows(cfg, f"SELECT {cols} FROM `{table}` ORDER BY {order}")
        out.append((table, rows))
    return out


@pytest.fixture(scope="module")
def cfg(mysql_testdb):
    """Use the session-provisioned dedicated test schema (never production)."""
    if not mysql_testdb.database.endswith("_test"):
        raise RuntimeError(
            f"integration tests refuse to run against a non-test schema: "
            f"{mysql_testdb.database!r} does not end with '_test'"
        )
    return mysql_testdb


@pytest.fixture
def tracked(cfg):
    """Track every staged file / build and wipe them after the test.

    Called before the build steps so a mid-run failure still cleans up, and also
    wipes the whole DWS/ADS output (only this module writes those tables in the
    dedicated test schema) plus stale build-log rows for the test scopes.
    """
    events_files: list[str] = []
    props_files: list[str] = []
    dwd_builds: list[int] = []
    agg_builds: list[int] = []

    def _track(
        events: str | list[str] | None = None,
        props: str | None = None,
        dwd_id: int | None = None,
        agg_id: int | None = None,
    ) -> None:
        if events:
            events_files.extend([events] if isinstance(events, str) else events)
        if props:
            props_files.append(props)
        if dwd_id is not None:
            dwd_builds.append(dwd_id)
        if agg_id is not None:
            agg_builds.append(agg_id)

    yield _track

    if cfg is None:
        return
    conn = agg_builder.connect(cfg)
    try:
        cur = conn.cursor()
        for src in events_files:
            cur.execute("DELETE FROM ods_events_raw WHERE source_file = %s", (src,))
            BatchLogRepo(conn).delete_for_file(src)
            for table in _DWD_TABLES:
                cur.execute(f"DELETE FROM `{table}` WHERE source_file = %s", (src,))
        for src in props_files:
            cur.execute("DELETE FROM ods_item_properties_raw WHERE source_file = %s", (src,))
            BatchLogRepo(conn).delete_for_file(src)
        for bid in dwd_builds:
            cur.execute("DELETE FROM dwd_build_log WHERE build_id = %s", (bid,))
        for bid in agg_builds:
            cur.execute("DELETE FROM dws_ads_build_log WHERE build_id = %s", (bid,))
        for table in _AGG_TABLES:
            cur.execute(f"DELETE FROM `{table}`")
        for dates_csv in (_ALL_CSV, "2099-01-01"):
            cur.execute(
                "DELETE FROM dws_ads_build_log WHERE scope_type = 'DATES' AND dates_csv = %s",
                (dates_csv,),
            )
            cur.execute(
                "DELETE FROM dwd_build_log WHERE scope_type = 'DATES' AND dates_csv = %s",
                (dates_csv,),
            )
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# hand-computed baselines (shared by the assert helpers below)                  #
# --------------------------------------------------------------------------- #

_FUNNEL_ORDER = (
    "view_users",
    "cart_users",
    "buyer_users",
    "orders",
    "add_to_cart_rate",
    "conv_strict_users",
    "conv_global_users",
    "conv_strict_rate",
    "conv_global_rate",
)
# day -> (the 9 metric values in _FUNNEL_ORDER, is_full_day)
_FUNNEL_BY_DAY = {
    D1: ((4, 3, 2, 3, Decimal("0.75"), 2, 2, Decimal("0.5"), Decimal("0.5")), 1),
    D2: ((1, 1, 1, 1, Decimal("1"), 0, 1, Decimal("0"), Decimal("1")), 1),
    D3: ((0, 2, 0, 0, None, 0, 0, None, None), 1),
    D4: ((0, 0, 2, 2, None, 0, 2, None, None), 0),
}

_TRAFFIC_BY_DAY = {
    D1: (7, 4, 6, 1),  # PV up from 6 after the late D1 view; UV/DAU unchanged
    D2: (2, 1, 3, 1),  # sentinel local view makes PV=2 while UV(1)/DAU(3) drop it
    D3: (0, 0, 2, 1),  # zero views, two active cart-only visitors
    D4: (0, 0, 2, 0),  # partial day (window ends 01-03)
}

_RETENTION_BY_DAY = {
    (D1, 0): (6, 6, Decimal("1"), "full"),
    (D1, 1): (6, 2, Decimal("0.333333"), "full"),  # v3 + v5 return on D2
    (D2, 0): (1, 1, Decimal("1"), "full"),
    (D2, 1): (1, 0, Decimal("0"), "full"),
    (D3, 0): (2, 2, Decimal("1"), "full"),
    (D3, 1): (2, 1, Decimal("0.5"), "partial"),  # v7 returns, D4 past window end
    (D4, 0): (1, 1, Decimal("1"), "partial"),
    (D4, 1): (1, 0, Decimal("0"), "partial"),
}

_CATEGORY_BY_DAY = {
    # (day, category) -> (view, cart, txn, view_uv, cart_uv, txn_uv, orders)
    (D1, 0): (3, 1, 4, 2, 1, 2, 3),
    (D1, 501): (2, 1, 0, 1, 1, 0, 0),  # 1001 x2 views + 5050 cart
    (D1, 502): (2, 1, 0, 1, 1, 0, 0),  # 1002 x2 views + 5051 cart
    (D2, 0): (2, 0, 1, 1, 0, 1, 1),
    (D2, 501): (0, 1, 0, 0, 1, 0, 0),
    (D3, 501): (0, 1, 0, 0, 1, 0, 0),
    (D3, 503): (0, 1, 0, 0, 1, 0, 0),
    (D4, 502): (0, 0, 2, 0, 0, 2, 2),
}

_RANK_BY_DAY = {
    # (day, rank_no) -> (item_id, view_cnt, add_to_cart_cnt, transaction_cnt)
    (D1, 1): (3003, 2, 1, 2),
    (D1, 2): (1002, 2, 0, 0),
    (D1, 3): (1001, 2, 0, 0),
    (D1, 4): (9090, 1, 0, 0),
    (D1, 5): (5051, 0, 1, 0),
    (D1, 6): (5050, 0, 1, 0),
    (D1, 7): (4004, 0, 0, 2),
    (D2, 1): (11111, 1, 0, 0),
    (D2, 2): (3003, 1, 0, 0),
    (D2, 3): (5050, 0, 1, 0),
    (D2, 4): (1010, 0, 0, 1),
    (D3, 1): (5050, 0, 1, 0),  # cart tie -> item_id DESC
    (D3, 2): (1010, 0, 1, 0),
    (D4, 1): (5051, 0, 0, 2),
}

_USER_SPOTS = {
    # (day, visitor) -> (visitor_zero, view, cart, txn, orders)
    (D1, 1): (0, 2, 0, 0, 0),  # two views -> one UV
    (D1, 3): (0, 2, 1, 2, 2),
    (D1, 9): (0, 1, 0, 2, 1),  # two txn rows, one distinct order id
    (D1, 10): (0, 0, 1, 0, 0),
    (D2, 0): (1, 1, 0, 0, 0),  # unknown bucket
    (D2, 3): (0, 1, 0, 0, 0),
    (D2, 6): (0, 0, 0, 1, 1),
    (D3, 11): (0, 0, 1, 0, 0),
    (D4, 12): (0, 0, 0, 1, 1),
}

_ITEM_SPOTS = {
    # (day, item) -> (view, cart, txn, view_uv, cart_uv, txn_uv, orders)
    (D1, 3003): (2, 1, 2, 1, 1, 1, 2),
    (D1, 4004): (0, 0, 2, 0, 0, 1, 1),  # duplicate order id deduped
    (D2, 11111): (1, 0, 0, 0, 0, 0, 0),  # sentinel view: view_uv stays 0
    (D4, 5051): (0, 0, 2, 0, 0, 2, 2),
}


def _assert_funnel(cfg) -> None:
    rows = _rows(
        cfg,
        "SELECT event_date_utc, metric_name, metric_value, is_full_day "
        "FROM ads_funnel_daily ORDER BY event_date_utc, metric_name",
    )
    actual = {(d, name): (value, full) for d, name, value, full in rows}
    expected = {
        (day, name): (value, full)
        for day, (values, full) in _FUNNEL_BY_DAY.items()
        for name, value in zip(_FUNNEL_ORDER, values, strict=True)
    }
    assert len(rows) == 36
    assert actual == expected


def test_dws_ads_small_sample_from_dwd(cfg, tmp_path, tracked):
    """End-to-end DWD -> DWS -> ADS on the hand-computed sample, incl. the
    late-event per-day rebuild and an idempotent full-scope rebuild."""
    # ---- stage ODS: two event batches + one snapshot file ----
    file1 = _stage_events(cfg, tmp_path, _sample_file1_rows())
    props = _stage_props(cfg, tmp_path)
    tracked(events=[file1], props=props)

    # ---- Phase-3 DWD build (first batch: 21 winners, 1 duplicate, 3 rejects) ----
    b1 = dwd_builder.build_dwd(cfg, dates=list(_DAYS), source_file=file1)
    tracked(dwd_id=b1.build_id)
    assert (
        b1.ods_count,
        b1.ok_rows,
        b1.duplicate_rows,
        b1.reject_rows,
    ) == (25, 21, 1, 3)
    assert (
        _scalar(cfg, "SELECT COUNT(*) FROM dwd_events_duplicate WHERE source_file = %s", (file1,))
        == 1
    )
    assert (
        _scalar(cfg, "SELECT COUNT(*) FROM dwd_events_reject WHERE source_file = %s", (file1,)) == 3
    )
    # sentinel "0" visitor -> NULL + flag, kept as a view row
    assert _rows(
        cfg,
        "SELECT event_type, visitor_id, quality_flags FROM dwd_events WHERE item_id = 11111",
    ) == [("view", None, "visitor_zero")]

    # ---- Build A: first full build of the four days (before the late event) ----
    res_a = agg_builder.build_dws_ads(
        cfg, dates=list(_DAYS), window_start=D1, window_end=D3, top_n=20
    )
    tracked(agg_id=res_a.build_id)
    assert res_a.status == "LOADED"
    assert res_a.dates_csv == _ALL_CSV
    assert res_a.table_stats == _EXPECTED_STATS
    assert _traffic(cfg)[D1] == (6, 4, 6, 1)  # the duplicate view never counted

    report = agg_builder.verify_dws_ads(cfg, dates=list(_DAYS), window_start=D1, window_end=D3)
    assert report.mismatches == ()

    # ---- late second batch: v3's second D1 view of item 3003 ----
    file2 = _stage_events(cfg, tmp_path, _sample_file2_rows())
    tracked(events=file2)
    b2 = dwd_builder.build_dwd(cfg, dates=[D1], source_file=file2)
    tracked(dwd_id=b2.build_id)
    assert (b2.ods_count, b2.ok_rows, b2.duplicate_rows, b2.reject_rows) == (1, 1, 0, 0)

    # ---- Build B: per-day rebuild of D1 only ----
    res_b = agg_builder.build_dws_ads(cfg, dates=[D1], window_start=D1, window_end=D3, top_n=20)
    tracked(agg_id=res_b.build_id)
    assert res_b.table_stats == {
        "dws_user_daily": 6,
        "dws_item_daily": 7,
        "dws_category_daily": 3,
        "ads_traffic_daily": 1,
        "ads_funnel_daily": 9,
        "ads_item_ranking_daily": 7,
        "ads_retention_cohort": 2,
    }
    # ... and the untouched days are unchanged alongside the updated D1.
    assert _traffic(cfg) == _TRAFFIC_BY_DAY

    # ---- final baselines: every output row checked by hand ----
    _assert_funnel(cfg)

    rows = _rows(
        cfg,
        "SELECT cohort_date, age_day, cohort_size, retained_users, retention_rate, "
        "window_status FROM ads_retention_cohort",
    )
    actual = {(d, a): (sz, ret, rate, st) for d, a, sz, ret, rate, st in rows}
    assert len(rows) == 8
    assert actual == _RETENTION_BY_DAY

    rows = _rows(
        cfg,
        "SELECT event_date_utc, category_id, view_cnt, add_to_cart_cnt, "
        "transaction_cnt, view_uv, add_to_cart_uv, transaction_uv, order_cnt "
        "FROM dws_category_daily",
    )
    actual = {
        (d, cat): (v, cart, txn, vuv, cuv, tuv, orders)
        for d, cat, v, cart, txn, vuv, cuv, tuv, orders in rows
    }
    assert len(rows) == 8
    assert actual == _CATEGORY_BY_DAY

    rows = _rows(
        cfg,
        "SELECT event_date_utc, rank_no, item_id, view_cnt, add_to_cart_cnt, "
        "transaction_cnt FROM ads_item_ranking_daily ORDER BY event_date_utc, rank_no",
    )
    actual = {(d, rank): (item, v, cart, txn) for d, rank, item, v, cart, txn in rows}
    assert len(rows) == 14
    assert actual == _RANK_BY_DAY
    # partial-day marker on the D4 ranking row
    assert (
        _scalar(
            cfg,
            "SELECT is_full_day FROM ads_item_ranking_daily WHERE event_date_utc = %s",
            (D4,),
        )
        == 0
    )

    rows = _rows(
        cfg,
        "SELECT event_date_utc, visitor_id, visitor_zero, view_cnt, add_to_cart_cnt, "
        "transaction_cnt, order_cnt FROM dws_user_daily",
    )
    actual = {
        (d, v): (zero, view, cart, txn, orders) for d, v, zero, view, cart, txn, orders in rows
    }
    assert all(actual[k] == v for k, v in _USER_SPOTS.items())

    rows = _rows(
        cfg,
        "SELECT event_date_utc, item_id, view_cnt, add_to_cart_cnt, transaction_cnt, "
        "view_uv, add_to_cart_uv, transaction_uv, order_cnt FROM dws_item_daily",
    )
    actual = {
        (d, item): (view, cart, txn, vuv, cuv, tuv, orders)
        for d, item, view, cart, txn, vuv, cuv, tuv, orders in rows
    }
    assert all(actual[k] == v for k, v in _ITEM_SPOTS.items())

    # ---- independent SQL recomputation of every ADS metric (different code) ----
    report2 = agg_builder.verify_dws_ads(cfg, dates=list(_DAYS), window_start=D1, window_end=D3)
    assert len(report2.checks) > 0
    assert report2.mismatches == ()

    # ---- idempotency: rebuilding the same scope reproduces identical content ----
    before = _snapshot(cfg)
    res_c = agg_builder.build_dws_ads(
        cfg, dates=list(_DAYS), window_start=D1, window_end=D3, top_n=20
    )
    tracked(agg_id=res_c.build_id)
    assert res_c.status == "LOADED"
    assert res_c.table_stats == _EXPECTED_STATS
    assert _snapshot(cfg) == before

    # ---- build-log audit ----
    conn = agg_builder.connect(cfg)
    try:
        repo = AggBuildLogRepo(conn)
        full = repo.find_for_scope("DATES", _ALL_CSV)
        assert [e.status for e in full] == ["LOADED", "LOADED"]
        assert full[0].table_stats == _EXPECTED_STATS
        single = repo.find_for_scope("DATES", "2099-01-01")
        assert [e.status for e in single] == ["LOADED"]
    finally:
        conn.close()

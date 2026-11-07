"""Phase 8 end-to-end metric oracle on a hand-computed miniature dataset.

This module is the *metric correctness baseline* required by the Phase 8 spec.
It stages the tiny fixture in ``tests/fixtures/phase8_manual/`` into the
dedicated ``*_test`` schema (never production) and drives the real production
code path ODS -> DWD -> DWS -> ADS, then re-derives every headline metric with
independent SQL / pure Python and compares against values calculated by hand.

Fixture coverage (D1 = 2015-09-16, D2 = 2015-09-17):

- same user multiple views (v1 views item 101 twice on D1, plus a late third)
- cart then transaction (v2: view -> add_to_cart -> transaction 9001)
- exact duplicate event (row 9 repeats row 1 -> dwd_events_duplicate)
- illegal event (``purchase`` on D1 -> dwd_events_reject)
- cross-day event (v1/v2 appear on D1 and D2; feeds D1 retention)
- late event (batch 2 adds a third D1 view after the first build)
- no-user-id event (visitorid ``0`` -> NULL + visitor_zero flag; PV only)
- transaction without a view (v4 -> conv_global_users 2 vs strict 1)
- zero-cart day with views (D2 add_to_cart_rate is 0, not NULL)
- partial retention tail (D2 cohort age1 is marked ``partial``)

Hand-computed expectations live in the ``_EXPECTED_*`` dicts below. See
``docs/test_matrix.md`` for the full data-to-assertion map.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from ecommerce_dw.dwd import builder as dwd_builder
from ecommerce_dw.dws import builder as agg_builder
from ecommerce_dw.dws.build_log import AggBuildLogRepo
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo

D1 = date(2015, 9, 16)
D2 = date(2015, 9, 17)
_DAYS = (D1, D2)
_WIN = (D1, D2)

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "phase8_manual"
_EVENTS_HEADER = ["timestamp", "visitorid", "event", "itemid", "transactionid"]
_PROPS_HEADER = ["timestamp", "itemid", "property", "value"]
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
    "ads_item_behavior_daily",
    "ads_funnel_interval",
)

# --------------------------------------------------------------------------- #
# hand-computed expectations (calculated from the 15 raw rows on paper)       #
# --------------------------------------------------------------------------- #

# day -> (pv, uv, dau, is_full_day)
_EXPECTED_TRAFFIC = {
    D1: (5, 2, 4, 1),
    D2: (3, 3, 4, 1),
}

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
# day -> 9 metric values in _FUNNEL_ORDER
_EXPECTED_FUNNEL = {
    D1: (2, 2, 2, 2, Decimal("1"), 1, 2, Decimal("0.5"), Decimal("1")),
    D2: (3, 0, 1, 1, Decimal("0"), 0, 1, Decimal("0"), Decimal("0.333333")),
}
# (cohort_date, age_day) -> (cohort_size, retained_users, rate, window_status)
_EXPECTED_RETENTION = {
    (D1, 0): (4, 4, Decimal("1"), "full"),
    (D1, 1): (4, 2, Decimal("0.5"), "full"),  # v1 + v2 return on D2
    (D2, 0): (2, 2, Decimal("1"), "full"),
    (D2, 1): (2, 0, Decimal("0"), "partial"),  # age-1 day (D3) is outside the window
}
# day -> (items_viewed, items_carted, items_bought, is_full_day)
_EXPECTED_ITEM_BEHAVIOR = {
    D1: (3, 2, 2, 1),
    D2: (3, 0, 1, 1),
}
# (day, rank_no) -> (item_id, view_cnt, add_to_cart_cnt, transaction_cnt)
_EXPECTED_RANKING = {
    (D1, 1): (101, 3, 0, 0),
    (D1, 2): (102, 1, 1, 1),
    (D1, 3): (104, 1, 0, 0),
    (D1, 4): (103, 0, 1, 0),
    (D1, 5): (105, 0, 0, 1),
    (D2, 1): (106, 1, 0, 0),
    (D2, 2): (102, 1, 0, 0),
    (D2, 3): (101, 1, 0, 0),
    (D2, 4): (107, 0, 0, 1),
}


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


def _load(cfg, path: Path, table: str) -> str:
    return loader.load_csv(cfg, path, table).source_file


@pytest.fixture(scope="module")
def cfg(mysql_testdb):
    if not mysql_testdb.database.endswith("_test"):
        raise RuntimeError(
            f"integration tests refuse to run against a non-test schema: "
            f"{mysql_testdb.database!r} does not end with '_test'"
        )
    return mysql_testdb


@pytest.fixture
def tracked(cfg):
    events_files: list[str] = []
    dwd_builds: list[int] = []
    agg_builds: list[int] = []

    def _track(events: str | None = None, dwd_id: int | None = None, agg_id: int | None = None):
        if events:
            events_files.append(events)
        if dwd_id is not None:
            dwd_builds.append(dwd_id)
        if agg_id is not None:
            agg_builds.append(agg_id)

    yield _track

    conn = agg_builder.connect(cfg)
    try:
        cur = conn.cursor()
        for src in events_files:
            cur.execute("DELETE FROM ods_events_raw WHERE source_file = %s", (src,))
            BatchLogRepo(conn).delete_for_file(src)
            for table in _DWD_TABLES:
                cur.execute(f"DELETE FROM `{table}` WHERE source_file = %s", (src,))
        for bid in dwd_builds:
            cur.execute("DELETE FROM dwd_build_log WHERE build_id = %s", (bid,))
        for bid in agg_builds:
            cur.execute("DELETE FROM dws_ads_build_log WHERE build_id = %s", (bid,))
        for table in _AGG_TABLES:
            cur.execute(f"DELETE FROM `{table}`")
        conn.commit()
    finally:
        conn.close()


def _snapshot(cfg) -> list[tuple[str, list[tuple]]]:
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
        "ads_traffic_daily": ("event_date_utc, pv, uv, dau, is_full_day", "event_date_utc"),
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
        "ads_item_behavior_daily": (
            "event_date_utc, items_viewed, items_carted, items_bought, is_full_day",
            "event_date_utc",
        ),
    }
    return [
        (table, _rows(cfg, f"SELECT {cols} FROM `{table}` ORDER BY {order}"))
        for table, (cols, order) in specs.items()
    ]


def test_phase8_manual_sample_end_to_end(cfg, tracked):
    """ODS -> DWD -> DWS -> ADS on the hand sample, with independent re-derivation.

    The assertions below intentionally avoid ``verify_dws_ads`` for the headline
    values: those are recomputed independently (SQL aggregates + Python set
    arithmetic) so a bug in the builder's own verifier cannot hide a wrong metric.
    ``verify_dws_ads`` is still asserted as a second, different-shaped gate.
    """
    # ---------------- stage batch 1 (14 ODS rows) + category snapshots -------- #
    b1 = _load(cfg, _FIXTURES / "events_batch1.csv", "ods_events_raw")
    props = _load(cfg, _FIXTURES / "item_properties.csv", "ods_item_properties_raw")
    tracked(events=b1)
    tracked(events=props)

    b1_res = dwd_builder.build_dwd(cfg, dates=list(_DAYS), source_file=b1)
    tracked(dwd_id=b1_res.build_id)
    assert (b1_res.ods_count, b1_res.ok_rows, b1_res.duplicate_rows, b1_res.reject_rows) == (
        14,
        12,
        1,
        1,
    )
    # no-user-id sentinel is preserved as NULL + quality flag, never dropped
    assert _rows(
        cfg,
        "SELECT visitor_id, quality_flags FROM dwd_events WHERE item_id = 104",
    ) == [(None, "visitor_zero")]

    agg1 = agg_builder.build_dws_ads(
        cfg, dates=list(_DAYS), window_start=_WIN[0], window_end=_WIN[1], top_n=100
    )
    tracked(agg_id=agg1.build_id)
    assert agg1.status == "LOADED"
    assert agg1.table_stats == {
        "dws_user_daily": 9,
        "dws_item_daily": 9,
        "dws_category_daily": 9,
        "ads_traffic_daily": 2,
        "ads_funnel_daily": 18,
        "ads_item_ranking_daily": 9,
        "ads_retention_cohort": 4,
        "ads_item_behavior_daily": 2,
    }

    # ---------------- late batch 2: a third D1 view for v1/item 101 ---------- #
    b2 = _load(cfg, _FIXTURES / "events_batch2_late.csv", "ods_events_raw")
    tracked(events=b2)
    b2_res = dwd_builder.build_dwd(cfg, dates=[D1], source_file=b2)
    tracked(dwd_id=b2_res.build_id)
    assert (b2_res.ods_count, b2_res.ok_rows, b2_res.duplicate_rows, b2_res.reject_rows) == (
        1,
        1,
        0,
        0,
    )
    agg2 = agg_builder.build_dws_ads(
        cfg, dates=[D1], window_start=_WIN[0], window_end=_WIN[1], top_n=100
    )
    tracked(agg_id=agg2.build_id)
    assert agg2.status == "LOADED"

    # ---------------- hand-computed table-level expectations ----------------- #
    traffic = {
        d: (pv, uv, dau, full)
        for d, pv, uv, dau, full in _rows(
            cfg, "SELECT event_date_utc, pv, uv, dau, is_full_day FROM ads_traffic_daily"
        )
    }
    assert traffic == _EXPECTED_TRAFFIC

    funnel_rows = _rows(
        cfg,
        "SELECT event_date_utc, metric_name, metric_value, is_full_day FROM ads_funnel_daily",
    )
    funnel = {(d, name): (value, full) for d, name, value, full in funnel_rows}
    assert len(funnel_rows) == 18
    for day, values in _EXPECTED_FUNNEL.items():
        for name, value in zip(_FUNNEL_ORDER, values, strict=True):
            assert funnel[(day, name)] == (value, 1), (day, name)

    retention = {
        (d, age): (size, retained, rate, status)
        for d, age, size, retained, rate, status in _rows(
            cfg,
            "SELECT cohort_date, age_day, cohort_size, retained_users, "
            "retention_rate, window_status FROM ads_retention_cohort",
        )
    }
    assert retention == _EXPECTED_RETENTION

    item_behavior = {
        d: (viewed, carted, bought, full)
        for d, viewed, carted, bought, full in _rows(
            cfg,
            "SELECT event_date_utc, items_viewed, items_carted, items_bought, is_full_day "
            "FROM ads_item_behavior_daily",
        )
    }
    assert item_behavior == _EXPECTED_ITEM_BEHAVIOR

    ranking = {
        (d, rank): (item, view, cart, txn)
        for d, rank, item, view, cart, txn in _rows(
            cfg,
            "SELECT event_date_utc, rank_no, item_id, view_cnt, add_to_cart_cnt, "
            "transaction_cnt FROM ads_item_ranking_daily",
        )
    }
    assert ranking == _EXPECTED_RANKING

    # ---------------- independent re-derivation (not the builder verifier) --- #
    direct_traffic = {
        d: (int(pv), int(uv), int(dau))
        for d, pv, uv, dau in _rows(
            cfg,
            "SELECT event_date_utc, "
            "       SUM(event_type = 'view') AS pv, "
            "       COUNT(DISTINCT CASE WHEN event_type = 'view' THEN visitor_id END) AS uv, "
            "       COUNT(DISTINCT visitor_id) AS dau "
            "FROM dwd_events WHERE event_date_utc IN (%s, %s) GROUP BY event_date_utc",
            (D1, D2),
        )
    }
    assert direct_traffic == {d: (v[0], v[1], v[2]) for d, v in _EXPECTED_TRAFFIC.items()}

    raw = _rows(
        cfg,
        "SELECT event_date_utc, event_type, visitor_id, transaction_id "
        "FROM dwd_events WHERE event_date_utc IN (%s, %s)",
        (D1, D2),
    )
    per_day: dict[date, list[tuple]] = {}
    for day, event_type, visitor, txn in raw:
        per_day.setdefault(day, []).append((event_type, visitor, txn))
    for day, rows in per_day.items():
        views = {v for e, v, _ in rows if e == "view" and v is not None}
        carts = {v for e, v, _ in rows if e == "add_to_cart" and v is not None}
        buyers = {v for e, v, _ in rows if e == "transaction" and v is not None}
        orders = {t for e, _, t in rows if e == "transaction" and t is not None}
        strict = views & buyers
        expected = _EXPECTED_FUNNEL[day]
        assert len(views) == expected[0]
        assert len(carts) == expected[1]
        assert len(buyers) == expected[2]
        assert len(orders) == expected[3]
        assert len(strict) == expected[5]
        assert len(buyers) == expected[6]

    # Retention independently: first-seen day per visitor, next-day active set.
    first_seen: dict[int, date] = {}
    active: dict[date, set[int]] = {}
    for day, _event_type, visitor, _txn in raw:
        if visitor is None:
            continue
        first_seen[visitor] = min(first_seen.get(visitor, day), day)
        active.setdefault(day, set()).add(visitor)
    assert {v for v, d in first_seen.items() if d == D1} == {1, 2, 3, 4}
    assert {v for v, d in first_seen.items() if d == D2} == {5, 9}
    assert active[D1] & active.get(D2, set()) == {1, 2}

    # ---------------- builder's own independent verifier is still clean ------ #
    report = agg_builder.verify_dws_ads(
        cfg, dates=list(_DAYS), window_start=_WIN[0], window_end=_WIN[1]
    )
    assert report.mismatches == ()

    # ---------------- idempotency: same scopes reproduce identical content --- #
    before = _snapshot(cfg)
    agg3 = agg_builder.build_dws_ads(
        cfg, dates=list(_DAYS), window_start=_WIN[0], window_end=_WIN[1], top_n=100
    )
    tracked(agg_id=agg3.build_id)
    assert agg3.table_stats == agg1.table_stats
    assert _snapshot(cfg) == before

    conn = agg_builder.connect(cfg)
    try:
        repo = AggBuildLogRepo(conn)
        assert [e.status for e in repo.find_for_scope("DATES", "2015-09-16,2015-09-17")] == [
            "LOADED",
            "LOADED",
        ]
        assert [e.status for e in repo.find_for_scope("DATES", "2015-09-16")] == ["LOADED"]
    finally:
        conn.close()

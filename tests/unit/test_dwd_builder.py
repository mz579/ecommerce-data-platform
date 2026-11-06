"""Unit tests for DWD builder helpers that do not need a database.

Covers scope serialization, the ODS/DWD WHERE builders, and the stage-insert
parameter shape (the pure pieces of the orchestration).
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from ecommerce_dw.dwd.builder import (
    _DUPLICATE_SELECT_SQL,
    _REJECT_SELECT_SQL,
    _WINNER_SELECT_SQL,
    Scope,
    _dwd_where,
    _ods_where,
    _stage_row_params,
)
from ecommerce_dw.dwd.transform import transform_event

_FULL = Scope(())
_DATES = Scope((date(2015, 5, 3), date(2015, 5, 4)))
_CUSTOM = Scope((date(2015, 5, 3),), source="events_it_x.csv")


def test_scope_serialisation():
    assert _FULL.scope_type == "FULL"
    assert _FULL.dates_csv is None
    assert _DATES.scope_type == "DATES"
    assert _DATES.dates_csv == "2015-05-03,2015-05-04"


def test_ods_where_full_and_dates():
    sql, params = _ods_where(_FULL)
    assert sql == "source_file = %s"
    assert params == ["events.csv"]

    sql, params = _ods_where(_DATES)
    assert sql.startswith("source_file = %s AND (")
    assert params[0] == "events.csv"
    assert len(params) == 5  # source + 2 date ranges * 2 bounds
    # UTC-day range for 2015-05-03 starts at 00:00:00 that day.
    assert params[1] == int(datetime(2015, 5, 3, tzinfo=UTC).timestamp() * 1000)
    assert params[2] == params[1] + 86_400_000


def test_ods_where_honours_custom_source():
    sql, params = _ods_where(_CUSTOM)
    assert params[0] == "events_it_x.csv"
    assert sql.startswith("source_file = %s AND (")


def test_dwd_where_uses_event_date_for_dates_scope():
    sql, params = _dwd_where(_DATES)
    assert "event_date_utc IN (%s, %s)" in sql
    assert params == ["events.csv", date(2015, 5, 3), date(2015, 5, 4)]
    sql_full, params_full = _dwd_where(_FULL)
    assert sql_full == "source_file = %s"
    assert params_full == ["events.csv"]


def test_stage_row_params_shape_and_values():
    outcome = transform_event("1430622004384", "7", "addtocart", "10", "")
    loaded = datetime(2026, 11, 7, 3, 0, 0, tzinfo=UTC).replace(tzinfo=None)
    row = _stage_row_params(
        b"x" * 32,
        "1430622004384",
        "7",
        "addtocart",
        "10",
        "",
        source_row_number=42,
        ingest_batch_id=17,
        outcome=outcome,
        source_file="events.csv",
        build_id=9,
        loaded_at=loaded,
    )
    # Follows _STAGE_COLUMNS order exactly.
    assert row == (
        b"x" * 32,
        "1430622004384",
        "7",
        "addtocart",
        "10",
        "",
        7,
        10,
        "addtocart",  # raw_event_type keeps the original spelling
        "add_to_cart",
        outcome.event_time_utc,
        outcome.event_date_utc,
        None,  # no transaction_id on a non-transaction event
        outcome.dedup_key,
        "ok",
        "OK",
        None,
        None,
        17,
        "events.csv",
        42,
        loaded,
        9,
    )


def test_scope_dedupes_and_sorts_dates():
    scope = Scope((date(2015, 5, 4), date(2015, 5, 3), date(2015, 5, 4)))
    assert scope.dates == (date(2015, 5, 3), date(2015, 5, 4))
    assert scope.dates_csv == "2015-05-03,2015-05-04"


def test_split_statements_are_scoped_to_this_build_id():
    """The split must only classify the current build's stage rows.

    A build whose split treats every stage row as its own swept up an
    interrupted FULL build's un-split rows and corrupted the DWD tables; each
    statement therefore filters ``st.build_id = %s`` (plus its own hardcoded
    ``record_status`` literal and the dedup_key placeholder for the INSERT).
    """
    for template, status_literal in (
        (_WINNER_SELECT_SQL, "WHERE st.record_status = 'OK' AND st.build_id = %s"),
        (_DUPLICATE_SELECT_SQL, "WHERE st.record_status = 'OK' AND st.build_id = %s"),
        (_REJECT_SELECT_SQL, "WHERE st.record_status = 'REJECT' AND st.build_id = %s"),
    ):
        assert status_literal in template
        assert template.count("build_id = %s") == 1
        assert "{cols}" in template  # the INSERT column template is always present

"""Unit tests for the DWS/ADS pure helpers (no database, Phase 4)."""

from __future__ import annotations

from datetime import date

from ecommerce_dw.dws.builder import Scope
from ecommerce_dw.dws.constants import (
    is_full_day,
    retention_d0_range,
    retention_window_status,
)

# --------------------------------------------------------------------------- #
# full-window / partial-day markers                                            #
# --------------------------------------------------------------------------- #


def test_is_full_day_bounds():
    assert is_full_day(date(2015, 5, 4))
    assert is_full_day(date(2015, 9, 17))
    assert not is_full_day(date(2015, 5, 3))  # partial prefix day
    assert not is_full_day(date(2015, 9, 18))  # partial suffix day
    assert not is_full_day(date(2015, 9, 19))  # outside the data


def test_is_full_day_custom_window():
    assert is_full_day(date(2099, 1, 1), date(2099, 1, 1), date(2099, 1, 4))
    assert not is_full_day(date(2098, 12, 31), date(2099, 1, 1), date(2099, 1, 4))


def test_retention_d0_range_derives_from_window():
    # Pinned production window -> D0 in [05-04, 09-16].
    assert retention_d0_range() == (date(2015, 5, 4), date(2015, 9, 16))
    # Custom window -> D0 ends one day before the window end (D+1 must fit).
    start, end = retention_d0_range(date(2099, 1, 1), date(2099, 1, 4))
    assert (start, end) == (date(2099, 1, 1), date(2099, 1, 3))


def test_retention_window_status_full_and_partial():
    full_start, full_end = date(2099, 1, 1), date(2099, 1, 4)
    # Both cohort and return day inside the window -> full.
    assert retention_window_status(date(2099, 1, 2), 1, full_start, full_end) == "full"
    # Cohort is the last eligible D0 (return day = window end) -> full.
    assert retention_window_status(date(2099, 1, 3), 1, full_start, full_end) == "full"
    # Return day beyond the window end -> partial (window tail insufficient).
    assert retention_window_status(date(2099, 1, 4), 1, full_start, full_end) == "partial"
    # Cohort day itself is a partial day (before window start) -> partial.
    assert retention_window_status(date(2098, 12, 31), 0, full_start, full_end) == "partial"


# --------------------------------------------------------------------------- #
# Scope normalization                                                          #
# --------------------------------------------------------------------------- #


def test_scope_full_vs_dates():
    assert Scope(()).scope_type == "FULL"
    assert Scope(()).dates_csv is None
    scope = Scope((date(2099, 1, 2), date(2099, 1, 1), date(2099, 1, 1)))
    assert scope.scope_type == "DATES"
    assert scope.dates_csv == "2099-01-01,2099-01-02"
    assert scope.dates == (date(2099, 1, 1), date(2099, 1, 2))

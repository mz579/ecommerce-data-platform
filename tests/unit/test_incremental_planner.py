"""Unit tests for the incremental planner (pure watermark-vs-current logic)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from ecommerce_dw.incremental.planner import (
    WorkPlan,
    compute_frontier,
    plan_backfill,
    plan_incremental,
)

D = date(2015, 9, 1)


def _d(offset: int) -> date:
    return D + timedelta(days=offset)


def test_workplan_invariants():
    plan = WorkPlan((_d(1), _d(2)), (_d(2),), ())
    assert plan.affected == (_d(1), _d(2))
    assert not plan.is_noop
    assert "2 date(s)" in plan.label
    assert WorkPlan((_d(3),), (_d(3),), ()).affected == (_d(3),)

    with pytest.raises(AssertionError):
        WorkPlan((_d(1),), (_d(2),), ())  # late not in new -> invalid


def test_workplan_noop():
    plan = WorkPlan()
    assert plan.is_noop
    assert plan.label == "no-op"
    plan = WorkPlan(out_of_window_dates=(_d(5),))
    assert not plan.is_noop
    assert "outside window" in plan.label


def test_compute_frontier():
    assert compute_frontier({}, {}) is None
    assert compute_frontier({_d(2): 1}, {_d(3): 1}) == _d(3)
    assert compute_frontier({_d(5): 1}, {}) == _d(5)


def test_fresh_tail_dates_are_new():
    watermark = {_d(0): 10}
    current = {_d(0): 10, _d(1): 11, _d(2): 11}
    plan = plan_incremental(current, watermark, late_window_days=7)
    assert plan.new_dates == (_d(1), _d(2))
    assert plan.late_dates == ()
    assert plan.out_of_window_dates == ()


def test_late_data_inside_window():
    watermark = {_d(1): 1, _d(4): 2}
    current = {_d(1): 2, _d(4): 2}  # D1 got a newer batch
    plan = plan_incremental(current, watermark, late_window_days=7, frontier=_d(4))
    assert plan.new_dates == (_d(1),)
    assert plan.late_dates == (_d(1),)


def test_deep_correction_outside_window():
    watermark = {_d(1): 1, _d(4): 2}
    current = {_d(1): 2, _d(4): 2}
    plan = plan_incremental(current, watermark, late_window_days=3, frontier=_d(4))
    # window = [D4-2, D4]; D1 is older -> reported, not auto-processed
    assert plan.new_dates == ()
    assert plan.out_of_window_dates == (_d(1),)


def test_window_boundary_is_inclusive():
    watermark = {_d(2): 1, _d(4): 2, _d(1): 1}
    current = {_d(1): 2, _d(2): 2, _d(4): 2}
    plan = plan_incremental(current, watermark, late_window_days=3, frontier=_d(4))
    # window = [D2, D4] -> D2 inside (late), D1 outside (deep)
    assert plan.late_dates == (_d(2),)
    assert plan.out_of_window_dates == (_d(1),)


def test_noop_when_watermark_matches_current():
    current = {_d(1): 1, _d(2): 2}
    plan = plan_incremental(current, dict(current), late_window_days=7)
    assert plan.is_noop
    assert plan.new_dates == ()


def test_zero_batch_dates_ignored():
    current = {_d(1): 0, _d(2): 2}
    watermark = {}
    plan = plan_incremental(current, watermark, late_window_days=7)
    assert plan.new_dates == (_d(2),)


def test_late_window_days_validation():
    with pytest.raises(ValueError):
        plan_incremental({}, {}, late_window_days=0)


def test_plan_backfill_forces_range():
    current = {
        _d(1): 1,
        _d(2): 2,
        _d(3): 3,
        _d(10): 3,  # outside the range -> untouched (out-of-window predecessor)
    }
    watermark = {_d(1): 1, _d(2): 2, _d(3): 3}
    plan = plan_backfill(_d(1), _d(3), current, watermark, late_window_days=7)
    assert plan.new_dates == (_d(1), _d(2), _d(3))  # forced regardless of watermark


def test_plan_backfill_skips_absent_dates():
    current = {_d(1): 1, _d(4): 2}
    plan = plan_backfill(_d(1), _d(5), current, {}, late_window_days=7)
    assert plan.new_dates == (_d(1), _d(4))  # gap days have no raw rows


def test_plan_backfill_single_day():
    current = {_d(3): 3}
    plan = plan_backfill(_d(3), _d(3), current, {}, late_window_days=7)
    assert plan.new_dates == (_d(3),)


def test_plan_backfill_raises_on_invalid_range():
    with pytest.raises(ValueError):
        plan_backfill(_d(5), _d(3), {}, {}, late_window_days=7)

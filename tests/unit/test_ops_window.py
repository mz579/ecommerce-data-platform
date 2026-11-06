"""Unit tests for the Airflow window resolver (pure, no IO; DAG-import safe)."""

from __future__ import annotations

from datetime import date

import pytest

from ecommerce_dw.ops.window import (
    MODE_FORCE,
    MODE_TAIL,
    RunWindow,
    WindowError,
    parse_dates_csv,
    resolve_window,
)

D = date(2015, 9, 16)


def test_parse_dates_csv():
    assert parse_dates_csv("2015-09-16,2015-09-17") == (date(2015, 9, 16), date(2015, 9, 17))
    assert parse_dates_csv(" 2015-09-17 ,2015-09-16 ") == (date(2015, 9, 16), date(2015, 9, 17))
    assert parse_dates_csv("2015-09-16") == (D,)
    with pytest.raises(WindowError):
        parse_dates_csv("")
    with pytest.raises(WindowError):
        parse_dates_csv("2015-13-99")


def test_runwindow_dates_csv():
    w = RunWindow(mode=MODE_TAIL, days=(D,))
    assert w.start == D and w.end == D
    assert w.dates_csv == "2015-09-16"
    w2 = RunWindow(mode=MODE_FORCE, days=(date(2015, 9, 16), date(2015, 9, 17)))
    assert w2.dates_csv == "2015-09-16,2015-09-17"
    assert w2.as_dict() == {
        "mode": MODE_FORCE,
        "dates_csv": "2015-09-16,2015-09-17",
        "logical_date": None,
    }
    with pytest.raises(WindowError):
        RunWindow(mode="NOPE", days=(D,))
    with pytest.raises(WindowError):
        RunWindow(mode=MODE_TAIL, days=())


def test_tail_uses_logical_date_first():
    w = resolve_window(logical_date=D, data_interval_start=date(2015, 9, 1))
    assert w.mode == MODE_TAIL
    assert w.days == (D,)
    assert w.logical_date == D


def test_tail_falls_back_to_data_interval_start():
    w = resolve_window(data_interval_start=D)
    assert w.mode == MODE_TAIL
    assert w.days == (D,)
    assert w.logical_date == D


def test_tail_requires_a_date():
    with pytest.raises(WindowError):
        resolve_window()


def test_conf_dates_forces_mode():
    w = resolve_window(logical_date=D, conf={"dates": "2015-09-16,2015-09-17"})
    assert w.mode == MODE_FORCE
    assert w.days == (date(2015, 9, 16), date(2015, 9, 17))
    assert w.logical_date == D  # logical date preserved as metadata


def test_conf_from_to_range():
    w = resolve_window(logical_date=D, conf={"from": "2015-09-16", "to": "2015-09-17"})
    assert w.mode == MODE_FORCE
    assert w.days == (date(2015, 9, 16), date(2015, 9, 17))


def test_conf_from_without_to_raises():
    with pytest.raises(WindowError):
        resolve_window(logical_date=D, conf={"from": "2015-09-16"})
    with pytest.raises(WindowError):
        resolve_window(logical_date=D, conf={"to": "2015-09-16"})


def test_conf_inverted_range_raises():
    with pytest.raises(WindowError):
        resolve_window(logical_date=D, conf={"from": "2015-09-17", "to": "2015-09-16"})


def test_conf_invalid_dates_raise():
    with pytest.raises(WindowError):
        resolve_window(logical_date=D, conf={"dates": "garbage"})
    with pytest.raises(WindowError):
        resolve_window(logical_date=D, conf={"from": "2015-9-1", "to": "2015-09-16"})


def test_conf_priority_dates_over_range():
    w = resolve_window(
        logical_date=D,
        conf={"dates": "2015-09-20", "from": "2015-09-16", "to": "2015-09-17"},
    )
    assert w.mode == MODE_FORCE
    assert w.days == (date(2015, 9, 20),)

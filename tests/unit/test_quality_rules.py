"""Unit tests for the quality-gate rule catalog and its date-filter helper."""

from __future__ import annotations

from datetime import date

import pytest

from ecommerce_dw.quality.models import (
    QUALITY_CATEGORIES,
    RunStatus,
    SqlCountRule,
    date_filter_sql,
)
from ecommerce_dw.quality.rules import build_rule_catalog
from ecommerce_dw.quality.runner import _scope

RULES_PER_CATEGORY = {
    "completeness": 3,
    "uniqueness": 12,
    "validity": 9,
    "range": 12,
    "reconciliation": 24,
}

D1 = date(2099, 1, 1)
D2 = date(2099, 1, 2)


def test_catalog_sizes_and_unique_keys():
    catalog = build_rule_catalog()
    assert len(catalog) == sum(RULES_PER_CATEGORY.values()) == 60
    keys = [r.key for r in catalog]
    assert len(keys) == len(set(keys))
    assert all(r.key for r in catalog)
    assert all(r.description for r in catalog)


@pytest.mark.parametrize("category,expected", RULES_PER_CATEGORY.items())
def test_catalog_category_counts(category, expected):
    catalog = build_rule_catalog()
    assert sum(1 for r in catalog if r.category == category) == expected


def test_catalog_rule_shape():
    for rule in build_rule_catalog():
        assert rule.category in QUALITY_CATEGORIES
        assert rule.severity == "error"  # the gate has no warn-and-continue path
        assert isinstance(rule, SqlCountRule)
        # date_keyed follows date_column, and the injected {date_filter} hook
        # occurs exactly as many times as the runner repeats the date params for.
        hook_count = rule.sql.count("{date_filter}")
        if rule.date_keyed:
            assert rule.date_filter_repeats >= 1
            assert hook_count == rule.date_filter_repeats
        else:
            assert hook_count == 0


def test_catalog_valid_window_override():
    catalog = build_rule_catalog(valid_window=(D1, D2))
    rule = next(r for r in catalog if r.key == "dwd_event_date_in_window")
    assert rule.extra_params == (D1, D2)
    assert "2099-01-01" in rule.description


def test_date_filter_sql():
    assert date_filter_sql("ev_day", None) == ("", ())
    assert date_filter_sql("ev_day", ()) == ("", ())
    filt, params = date_filter_sql("ev_day", (D1, D2))
    assert filt == " AND `ev_day` IN (%s, %s)"
    assert params == (D1, D2)


def test_scope_helper():
    assert _scope(None) == ("FULL", None)
    assert _scope((D2, D1)) == ("DATES", "2099-01-01,2099-01-02")
    assert _scope((D1, D2, D1)) == ("DATES", "2099-01-01,2099-01-02")


def test_run_status_constant():
    assert RunStatus.PASS == "PASS"
    assert RunStatus.FAIL == "FAIL"

"""Unit tests for the quality runner helpers (Phase 7 R16).

``validate_publish_scope`` is a pure function implementing the gate-scope
rule: a FULL publish must be backed by a FULL-scope PASS gate, a DATES publish
by a DATES-scope gate over the identical day set. Positive and negative cases
are both exercised here (pure, no DB needed).
"""

from ecommerce_dw.quality.runner import _scope, validate_publish_scope


def _pairs():
    """(run_scope_type, run_dates_csv, req_scope_type, req_dates_csv, expect_allowed)"""
    return [
        # ---- FULL publish backed by a FULL gate: allowed -------------
        ("FULL", None, "FULL", None, True),
        # ---- FULL publish backed by a DATES gate: refused -----------
        ("DATES", "2015-09-16,2015-09-17", "FULL", None, False),
        # ---- DATES publish backed by identical DATES gate: allowed ---
        (
            "DATES",
            "2015-09-16,2015-09-17",
            "DATES",
            "2015-09-16,2015-09-17",
            True,
        ),
        # ---- DATES publish backed by a wider/narrower/other day set --
        (
            "DATES",
            "2015-09-16,2015-09-17",
            "DATES",
            "2015-09-16",
            False,
        ),
        (
            "DATES",
            "2015-09-16",
            "DATES",
            "2015-09-16,2015-09-17",
            False,
        ),
        (
            "DATES",
            "2015-09-16,2015-09-17",
            "DATES",
            "2015-08-01,2015-08-02",
            False,
        ),
        # ---- DATES publish backed by a FULL gate: refused ------------
        ("FULL", None, "DATES", "2015-09-16", False),
    ]


def test_validate_publish_scope_positive_and_negative():
    allowed = 0
    for run_type, run_csv, req_type, req_csv, expect in _pairs():
        reason = validate_publish_scope(run_type, run_csv, req_type, req_csv)
        if expect:
            allowed += 1
            assert reason is None, (run_type, run_csv, req_type, req_csv)
        else:
            assert reason is not None and "publish refused" in reason, (
                run_type,
                run_csv,
                req_type,
                req_csv,
            )
    assert allowed == 2
    assert len(_pairs()) == 7


def test_validate_publish_scope_messages_are_specific():
    assert "FULL" in validate_publish_scope("DATES", "2015-09-16", "FULL", None)
    reason = validate_publish_scope("DATES", "2015-09-16", "DATES", "2015-09-17")
    assert "2015-09-16" in reason and "2015-09-17" in reason


def test_scope_helper():
    from datetime import date

    assert _scope(None) == ("FULL", None)
    assert _scope(()) == ("FULL", None)
    assert _scope([]) == ("FULL", None)
    # sorted + deduped, ISO format
    assert _scope((date(2015, 9, 17), date(2015, 9, 16), date(2015, 9, 17))) == (
        "DATES",
        "2015-09-16,2015-09-17",
    )

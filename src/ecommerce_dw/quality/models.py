"""Quality-gate data models (Phase 5).

Design points:

- a :class:`CheckRule` is a stateless description of one check; ``run`` executes
  a single SQL statement against the caller-supplied connection and returns a
  :class:`RuleOutcome` with ``checked`` (rows compared) and ``failed_rows``.
  The gate semantics are binary: ``failed_rows > 0`` means FAIL — there is no
  "warn and continue" severity on the path that guards ADS publishing.
- ``date_filter_sql`` injects a parameterized UTC-day filter so that a
  full-library run (``dates=None``) and a per-day scoped run share one rule SQL.
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass
from datetime import date

# Rule categories (five groups required by the phase spec).
QUALITY_CATEGORIES = ("completeness", "uniqueness", "validity", "range", "reconciliation")


class RunStatus:
    """Gate run statuses (stored in ``quality_check_run.status``)."""

    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class RuleOutcome:
    """Result of running one rule."""

    passed: bool
    checked: int = 0
    failed_rows: int = 0
    detail: str | None = None
    duration_ms: float = 0.0


def date_filter_sql(date_column: str | None, dates: tuple[date, ...] | None) -> tuple[str, tuple]:
    """Return ``(" AND <col> IN (%s,..)", (d1,..))`` or ``("", ())`` for full scope."""
    if not dates or not date_column:
        return "", ()
    marks = ", ".join(["%s"] * len(dates))
    return f" AND `{date_column}` IN ({marks})", tuple(dates)


@dataclass(frozen=True)
class CheckRule(abc.ABC):
    """A single gate rule: static metadata + an executable SQL check."""

    key: str
    category: str
    description: str
    date_keyed: bool = False
    severity: str = "error"

    def __post_init__(self) -> None:
        if self.category not in QUALITY_CATEGORIES:
            raise ValueError(f"{self.key}: unknown category {self.category!r}")
        if self.severity != "error":
            raise ValueError(f"{self.key}: only hard-error rules are allowed in the gate")

    @abc.abstractmethod
    def run(self, conn, *, dates: tuple[date, ...] | None) -> RuleOutcome: ...


@dataclass(frozen=True)
class SqlCountRule(CheckRule):
    """Runs a query yielding one row ``(total, bad)`` and passes iff ``bad == 0``.

    ``sql`` accepts a ``{date_filter}`` hook. ``date_filter_repeats`` controls
    how many times the injected day filter must occur inside the query (rules
    joining several date-keyed subqueries need the ``%s`` values repeated once
    per occurrence). Rule-specific parameters (``extra_params``) come first.
    """

    sql: str = ""
    date_column: str | None = None
    date_filter_repeats: int = 1
    extra_params: tuple = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "date_keyed", self.date_column is not None)

    def run(self, conn, *, dates: tuple[date, ...] | None = None) -> RuleOutcome:
        filt, filter_params = date_filter_sql(self.date_column, dates)
        start = time.monotonic()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    self.sql.format(date_filter=filt),
                    (*self.extra_params, *(filter_params * self.date_filter_repeats)),
                )
                row = cur.fetchone()
        except Exception as exc:  # a broken rule must fail the gate, not crash the run
            return RuleOutcome(
                passed=False,
                checked=0,
                failed_rows=1,
                detail=f"rule SQL failed: {type(exc).__name__}: {exc}",
                duration_ms=(time.monotonic() - start) * 1000,
            )
        total = int(row[0] or 0)
        bad = int(row[1] or 0)
        return RuleOutcome(
            passed=bad == 0,
            checked=total,
            failed_rows=bad,
            duration_ms=(time.monotonic() - start) * 1000,
        )

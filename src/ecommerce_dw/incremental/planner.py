"""Pure incremental planning logic (Phase 5).

Compares the two per-date batch maps that drive everything:

- ``watermark_batch``    : ``{business_date -> ingest batch already consumed}``
- ``current_batch_by_date`` : ``{business_date -> max ingest batch in ODS now}``

A date whose current batch exceeds its watermark either **progressed** (fresh
tail data) or received **late** data.  Late data close enough to the frontier
(inside the ``late_window_days`` lookback) is recomputed automatically; dates
further back are reported as ``out_of_window`` and require an explicit
``--from/--to`` backfill - they still enter the same one-key incremental entry.

All functions here are pure / database-free so they can be unit-tested.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class WorkPlan:
    """What the incremental runner needs to do."""

    new_dates: tuple[date, ...] = ()
    late_dates: tuple[date, ...] = ()
    out_of_window_dates: tuple[date, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "new_dates", tuple(sorted(set(self.new_dates))))
        object.__setattr__(self, "late_dates", tuple(sorted(set(self.late_dates))))
        object.__setattr__(
            self, "out_of_window_dates", tuple(sorted(set(self.out_of_window_dates)))
        )
        assert set(self.late_dates) <= set(self.new_dates)

    @property
    def affected(self) -> tuple[date, ...]:
        """Business dates to process in this run (new tail + in-window late)."""
        return self.new_dates

    @property
    def is_noop(self) -> bool:
        return not self.new_dates and not self.out_of_window_dates

    @property
    def label(self) -> str:
        bits = []
        if self.new_dates:
            bits.append(f"{len(self.new_dates)} date(s)")
        if self.late_dates:
            bits.append(f"{len(self.late_dates)} late inside window")
        if self.out_of_window_dates:
            bits.append(f"{len(self.out_of_window_dates)} outside window -> need --from/--to")
        return ", ".join(bits) if bits else "no-op"


def compute_frontier(
    watermark_batch: dict[date, int], current_batch_by_date: dict[date, int]
) -> date | None:
    """The newest known business date (raw data tail, else watermark tail)."""
    known = set(watermark_batch) | set(current_batch_by_date)
    return max(known) if known else None


def plan_incremental(
    current_batch_by_date: dict[date, int],
    watermark_batch: dict[date, int],
    *,
    frontier: date | None = None,
    late_window_days: int = 7,
) -> WorkPlan:
    """Classify each raw-data business date as new / late / outside the window.

    A date without a positive watermark is *new* (never processed).  A processed
    date is *late* when it sits inside ``[frontier - (late_window_days - 1),
    frontier]`` and received a newer batch; deeper corrections are reported as
    ``out_of_window_dates`` so the caller can ask for an explicit backfill.
    """
    if late_window_days < 1:
        raise ValueError("late_window_days must be >= 1")
    if frontier is None:
        frontier = compute_frontier(watermark_batch, current_batch_by_date)
    new_dates: list[date] = []
    late_dates: list[date] = []
    out_of_window_dates: list[date] = []
    window_start = frontier - timedelta(days=late_window_days - 1) if frontier else None
    for day in sorted(current_batch_by_date):
        current_batch = current_batch_by_date[day]
        if current_batch <= 0:
            continue
        recorded = watermark_batch.get(day, 0)
        if current_batch <= recorded:
            continue  # already fully consumed
        if recorded == 0:
            new_dates.append(day)  # never processed: fresh tail / bootstrap gap
        elif window_start is not None and day >= window_start:
            new_dates.append(day)
            late_dates.append(day)
        else:
            out_of_window_dates.append(day)
    return WorkPlan(tuple(new_dates), tuple(late_dates), tuple(out_of_window_dates))


def plan_backfill(
    from_date: date,
    to_date: date,
    current_batch_by_date: dict[date, int],
    watermark_batch: dict[date, int],
    *,
    frontier: date | None = None,
    late_window_days: int = 7,
) -> WorkPlan:
    """Force a range [from_date, to_date] regardless of the current watermark.

    Used for deep corrections and for the DATES-backed historical-correction
    entry: the runner rebuilds DWD and DWS/ADS for these dates through the same
    incremental path and then moves the watermark forward.
    """
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    days = [d for d in _daterange(from_date, to_date) if d in current_batch_by_date]
    # Preserve the late classification for reporting; everything in range is built.
    plan = plan_incremental(
        current_batch_by_date,
        watermark_batch,
        frontier=frontier,
        late_window_days=late_window_days,
    )
    late_in_scope = tuple(sorted(d for d in plan.late_dates if from_date <= d <= to_date))
    return WorkPlan(tuple(days), late_in_scope, ())


def _daterange(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)

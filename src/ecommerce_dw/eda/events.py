"""Streaming statistics for the Retailrocket ``events.csv`` table.

Single-pass scan that streams all rows and accumulates:

- per-event-type counts, with any event outside the documented vocabulary
  collected separately (never silently dropped)
- timestamp range (Unix milliseconds) and invalid-timestamp count
- distinct visitor / item / transaction-id counts
- coverage of ``transactionid``: every ``transaction`` event must carry one,
  and non-transaction rows must not

``daily_event_counts`` is a second, lighter pass that aggregates events per
UTC calendar day for trend charts.

No database is required; only a local CSV file is read.
"""

from __future__ import annotations

import csv
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

VALID_EVENTS: frozenset[str] = frozenset({"view", "addtocart", "transaction"})


@dataclass(frozen=True)
class EventsStats:
    n_rows: int
    event_counts: dict[str, int]
    unknown_events: dict[str, int]  # event values outside the documented set
    ts_min_ms: int
    ts_max_ms: int
    n_invalid_ts: int
    n_visitors: int
    n_items: int
    n_transactionids: int  # distinct non-empty transactionid
    n_transaction_rows: int
    n_txnid_nonempty_rows: int  # rows where transactionid is non-empty
    n_txnid_on_transaction_rows: int  # txnid present AND event == transaction
    n_txnid_reused: int  # txnids appearing on more than one transaction row
    max_txnid_events: int  # max transaction rows sharing one transactionid
    transaction_rows_missing_txnid: int
    txnid_non_transaction_rows: int  # non-empty txnid on a non-transaction row
    n_missing_visitor: int
    n_visitor_zero: int  # visitorid == "0" sentinel rows
    n_missing_item: int
    n_missing_timestamp: int
    n_missing_transactionid: int
    item_ids: frozenset[str]


@dataclass(frozen=True)
class DailyEvents:
    dates: tuple[str, ...]  # ascending ISO calendar dates (UTC)
    counts_by_event: dict[str, tuple[int, ...]]


def summarize_events(path: str | Path, *, valid_events: Iterable[str] | None = None) -> EventsStats:
    """Stream ``events.csv`` and return one-pass statistics (see docstrings)."""
    src = Path(path)
    allowed = frozenset(valid_events) if valid_events is not None else VALID_EVENTS

    event_counts = Counter()
    unknown_events = Counter()
    visitors: set[str] = set()
    items: set[str] = set()
    transactionids: set[str] = set()
    txn_events_per_id: Counter = Counter()  # transaction rows per transactionid

    n_rows = 0
    ts_min: int | None = None
    ts_max: int | None = None
    n_invalid_ts = 0
    n_transaction_rows = 0
    n_txnid_nonempty_rows = 0
    n_txnid_on_transaction_rows = 0
    n_missing_visitor = 0
    n_visitor_zero = 0
    n_missing_item = 0
    n_missing_timestamp = 0
    n_missing_transactionid = 0

    with src.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        _ = next(reader)  # header
        for row in reader:
            if len(row) != 5:  # pragma: no cover - malformed rows are rare
                continue
            ts, visitor, event, item_id, txn_id = row
            n_rows += 1

            if event in allowed:
                event_counts[event] += 1
            else:
                unknown_events[event] += 1
            if event == "transaction":
                n_transaction_rows += 1

            if visitor == "":
                n_missing_visitor += 1
            else:
                visitors.add(visitor)
                if visitor == "0":
                    n_visitor_zero += 1

            if item_id == "":
                n_missing_item += 1
            else:
                items.add(item_id)

            if ts == "":
                n_missing_timestamp += 1
                n_invalid_ts += 1
            else:
                try:
                    ts_val = int(ts)
                except ValueError:
                    n_invalid_ts += 1
                else:
                    if ts_min is None or ts_val < ts_min:
                        ts_min = ts_val
                    if ts_max is None or ts_val > ts_max:
                        ts_max = ts_val

            if txn_id == "":
                n_missing_transactionid += 1
            else:
                n_txnid_nonempty_rows += 1
                transactionids.add(txn_id)
                if event == "transaction":
                    n_txnid_on_transaction_rows += 1
                    txn_events_per_id[txn_id] += 1

    return EventsStats(
        n_rows=n_rows,
        event_counts=dict(event_counts),
        unknown_events=dict(unknown_events),
        ts_min_ms=ts_min if ts_min is not None else 0,
        ts_max_ms=ts_max if ts_max is not None else 0,
        n_invalid_ts=n_invalid_ts,
        n_visitors=len(visitors),
        n_items=len(items),
        n_transactionids=len(transactionids),
        n_transaction_rows=n_transaction_rows,
        n_txnid_nonempty_rows=n_txnid_nonempty_rows,
        n_txnid_on_transaction_rows=n_txnid_on_transaction_rows,
        n_txnid_reused=sum(1 for n in txn_events_per_id.values() if n > 1),
        max_txnid_events=max(txn_events_per_id.values(), default=0),
        transaction_rows_missing_txnid=n_transaction_rows - n_txnid_on_transaction_rows,
        txnid_non_transaction_rows=n_txnid_nonempty_rows - n_txnid_on_transaction_rows,
        n_missing_visitor=n_missing_visitor,
        n_visitor_zero=n_visitor_zero,
        n_missing_item=n_missing_item,
        n_missing_timestamp=n_missing_timestamp,
        n_missing_transactionid=n_missing_transactionid,
        item_ids=frozenset(items),
    )


def daily_event_counts(path: str | Path) -> DailyEvents:
    """Aggregate events per UTC calendar day.

    Returns tuples aligned with ``dates`` so they can be zipped for charts.
    Rows whose timestamp can't be parsed are ignored for the daily series.
    """
    src = Path(path)
    by_date: dict[str, Counter] = {}
    with src.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        _ = next(reader)
        for row in reader:
            if len(row) != 5:
                continue
            try:
                ts_val = int(row[0])
            except ValueError:
                continue
            day = datetime.fromtimestamp(ts_val / 1000, tz=UTC).date().isoformat()
            by_date.setdefault(day, Counter())[row[2]] += 1

    dates = sorted(by_date)
    counts_by_event: dict[str, list[int]] = {}
    for idx, day in enumerate(dates):
        counter = by_date[day]
        for event, n in counter.items():
            series = counts_by_event.setdefault(event, [0] * len(dates))
            series[idx] = n

    return DailyEvents(
        dates=tuple(dates),
        counts_by_event={event: tuple(values) for event, values in counts_by_event.items()},
    )

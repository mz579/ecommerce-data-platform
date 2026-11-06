"""Pure, side-effect-free event transform: raw ODS cells -> standard DWD event.

Everything in this module is a pure function / frozen dataclass: given the five
raw string columns of an ``ods_events_raw`` row it produces a
:class:`TransformOutcome` that is either ``OK`` (canonical dedup channel) or
``REJECT`` (isolation table), plus the flags that qualify it. No I/O and no
dependencies on the database — this is the unit-testable core of the DWD build.

Design points (see ``docs/dwd_design.md``):

- ``event_type`` follows the master plan: ``view -> view``, ``addtocart ->
  add_to_cart``, ``transaction -> transaction``; anything else is rejected with
  ``unknown_event_type``. The raw value is always preserved in ``raw_event_type``.
- Millisecond timestamps become naive-UTC ``datetime`` / ``date`` (DATETIME(3)
  precision); empty / non-integer / <=0 timestamps are rejected.
- ``visitorid == "0"`` is the documented sentinel: ``visitor_id=None`` + flag
  ``visitor_zero`` (soft anomaly, row is kept); empty -> ``missing_visitor``
  reject; non-numeric or out of BIGINT range -> reject, never defaulted to 0.
- a ``transaction`` event *must* carry a non-empty ``transactionid`` and
  non-transaction events *must not* carry one; violations are rejected.
- On REJECT the event time is still recorded whenever the timestamp is valid,
  so the isolation table is as queryable as the detail table.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

# DWD standard event-name mapping (authoritative, master plan 2.2).
EVENT_TYPE_MAP: dict[str, str] = {
    "view": "view",
    "addtocart": "add_to_cart",
    "transaction": "transaction",
}
STANDARD_EVENT_TYPES: frozenset[str] = frozenset(EVENT_TYPE_MAP.values())

# Transform logic version; bump when rules change so a crashed build is never
# silently resumed with mixed logic (see docs/dwd_design.md).
TRANSFORM_VERSION = "1"


# ---- reject reasons (the enum stored in dwd_events_reject.reject_reason) -----
class RejectReason:
    INVALID_TIMESTAMP = "invalid_timestamp"
    MISSING_VISITOR = "missing_visitor"
    INVALID_VISITOR_ID = "invalid_visitor_id"
    MISSING_ITEM = "missing_item"
    INVALID_ITEM_ID = "invalid_item_id"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    MISSING_TXN_ON_TRANSACTION = "missing_txn_on_transaction"
    UNEXPECTED_TXNID = "unexpected_txnid"
    INVALID_TRANSACTION_ID = "invalid_transaction_id"


# ---- quality flags (comma-joined into dwd_events.quality_flags) -------------
FLAG_VISITOR_ZERO = "visitor_zero"
_FLAG_OK = "ok"

# MySQL BIGINT is a signed 64-bit integer -> hard boundary on ID columns.
BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1

_EPOCH_UTC = datetime(1970, 1, 1)
_SECONDS_PER_DAY = 86_400
_MILLIS_PER_DAY = _SECONDS_PER_DAY * 1000


@dataclass(frozen=True)
class TransformOutcome:
    """Result of transforming one raw event row.

    ``status == "OK"`` rows are dedup candidates; ``status == "REJECT"`` rows are
    isolated. Field presence mirrors what the stage table stores.
    """

    status: Literal["OK", "REJECT"]
    event_type: str | None = None
    event_time_utc: datetime | None = None
    event_date_utc: date | None = None
    visitor_id: int | None = None
    item_id: int | None = None
    transaction_id: int | None = None
    dedup_key: str | None = None
    quality_flags: tuple[str, ...] = ()
    reject_reason: str | None = None
    reject_message: str | None = None

    @property
    def is_ok(self) -> bool:
        return self.status == "OK"

    @property
    def quality_flags_csv(self) -> str:
        """Comma-joined flags; ``"ok"`` when there are none."""
        if not self.quality_flags:
            return _FLAG_OK
        return ",".join(self.quality_flags)


# --------------------------------------------------------------------------- #
# small pure parsing helpers (also useful directly from tests)                #
# --------------------------------------------------------------------------- #


def parse_ts_ms(raw: str) -> int:
    """Return the validated millisecond timestamp; raise ``ValueError`` otherwise."""
    text = raw.strip()
    if not text:
        raise ValueError("empty timestamp")
    try:
        ms = int(text)
    except ValueError as exc:
        raise ValueError(f"timestamp is not an integer: {raw!r}") from exc
    if ms <= 0:
        raise ValueError(f"timestamp must be a positive integer, got {raw!r}")
    return ms


def ms_to_utc_datetime(ms: int) -> datetime:
    """Milliseconds since epoch -> naive-UTC datetime (sub-second preserved)."""
    if ms < 0:
        raise ValueError(f"negative millisecond value {ms}")
    try:
        return _EPOCH_UTC + timedelta(milliseconds=ms)
    except OverflowError as exc:
        raise ValueError(f"timestamp out of datetime range: {ms}") from exc


def ms_to_utc_date(ms: int) -> date:
    """Milliseconds since epoch -> UTC calendar date (``ts // 86400000`` day)."""
    return ms_to_utc_datetime(ms).date()


def date_ms_range(d: date) -> tuple[int, int]:
    """Return ``[start_ms, end_ms)`` covering the UTC calendar day ``d``."""
    start_day = (d.toordinal() - _EPOCH_UTC.date().toordinal()) * _SECONDS_PER_DAY * 1000
    return start_day, start_day + _MILLIS_PER_DAY


def parse_bigint(raw: str, *, field: str) -> int:
    """Parse a non-empty whole number that fits a MySQL BIGINT (signed 64-bit).

    Raises ``ValueError`` when the value is empty, non-integer, or out of range.
    """
    text = raw.strip()
    if not text:
        raise ValueError(f"empty {field}")
    try:
        value = int(text)
    except ValueError as exc:
        raise ValueError(f"{field} is not an integer: {raw!r}") from exc
    if value < BIGINT_MIN or value > BIGINT_MAX:
        raise ValueError(f"{field} out of BIGINT range: {raw!r}")
    return value


def make_dedup_key(
    event_time_ms: int,
    visitor_id: int | None,
    event_type: str,
    item_id: int,
    transaction_id: int | None,
) -> str:
    """SHA-256 (hex) of the canonical business key.

    Business key (agreed in Phase 3):
    ``(event_time_utc, visitor_id, event_type, item_id, COALESCE(transaction_id, 0))``.
    NULL visitor_id encodes as the empty component, NULL transaction_id as
    ``"0"`` (matches the COALESCE). Component order is fixed, so equal keys
    always hash equal and unequal keys never collide (barring SHA-256).
    """
    components = (
        str(event_time_ms),
        "" if visitor_id is None else str(visitor_id),
        event_type,
        str(item_id),
        "0" if transaction_id is None else str(transaction_id),
    )
    payload = "\x1f".join(components).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


# --------------------------------------------------------------------------- #
# the transform itself                                                        #
# --------------------------------------------------------------------------- #


def transform_event(
    raw_timestamp: str,
    raw_visitorid: str,
    raw_event: str,
    raw_itemid: str,
    raw_transactionid: str,
) -> TransformOutcome:
    """Transform one row of raw event cells into a standard event outcome.

    The five arguments are exactly ``ods_events_raw``'s ``timestamp, visitorid,
    event, itemid, transactionid`` string columns. The function never raises for
    bad data — it reports through the outcome status / reject_reason instead.
    """
    # 1. timestamp: must be a positive integer (milliseconds).
    try:
        ts_ms = parse_ts_ms(raw_timestamp)
    except ValueError as exc:
        return TransformOutcome(
            status="REJECT", reject_reason=RejectReason.INVALID_TIMESTAMP, reject_message=str(exc)
        )
    try:
        ts_dt = ms_to_utc_datetime(ts_ms)
    except ValueError as exc:
        return TransformOutcome(
            status="REJECT", reject_reason=RejectReason.INVALID_TIMESTAMP, reject_message=str(exc)
        )
    ts_date = ts_dt.date()

    # 2. event: must be in the standard mapping.
    standard = EVENT_TYPE_MAP.get(raw_event)
    if standard is None:
        return TransformOutcome(
            status="REJECT",
            event_type="unknown",
            event_time_utc=ts_dt,
            event_date_utc=ts_date,
            reject_reason=RejectReason.UNKNOWN_EVENT_TYPE,
            reject_message=(
                f"unknown event type {raw_event!r} (expected one of {sorted(EVENT_TYPE_MAP)})"
            ),
        )

    # 3. visitorid: "0" sentinel -> NULL + flag; blank -> reject; else BIGINT.
    flags: list[str] = []
    visitor_text = raw_visitorid.strip()
    if not visitor_text:
        return TransformOutcome(
            status="REJECT",
            event_type=standard,
            event_time_utc=ts_dt,
            event_date_utc=ts_date,
            reject_reason=RejectReason.MISSING_VISITOR,
            reject_message="empty visitorid (behavior events need an actor)",
        )
    if visitor_text == "0":
        visitor_id: int | None = None
        flags.append(FLAG_VISITOR_ZERO)
    else:
        try:
            visitor_id = parse_bigint(visitor_text, field="visitorid")
        except ValueError as exc:
            return TransformOutcome(
                status="REJECT",
                event_type=standard,
                event_time_utc=ts_dt,
                event_date_utc=ts_date,
                reject_reason=RejectReason.INVALID_VISITOR_ID,
                reject_message=str(exc),
            )

    # 4. itemid: required, must be a BIGINT.
    try:
        item_id = parse_bigint(raw_itemid, field="itemid")
    except ValueError as exc:
        reason = (
            RejectReason.MISSING_ITEM if not raw_itemid.strip() else RejectReason.INVALID_ITEM_ID
        )
        return TransformOutcome(
            status="REJECT",
            event_type=standard,
            event_time_utc=ts_dt,
            event_date_utc=ts_date,
            visitor_id=visitor_id,
            reject_reason=reason,
            reject_message=str(exc),
        )

    # 5. transactionid: transaction events must carry it; others must not.
    transaction_id: int | None = None
    if standard == "transaction":
        if not raw_transactionid.strip():
            return TransformOutcome(
                status="REJECT",
                event_type=standard,
                event_time_utc=ts_dt,
                event_date_utc=ts_date,
                visitor_id=visitor_id,
                item_id=item_id,
                reject_reason=RejectReason.MISSING_TXN_ON_TRANSACTION,
                reject_message="transaction event must carry a transactionid",
            )
        try:
            transaction_id = parse_bigint(raw_transactionid, field="transactionid")
        except ValueError as exc:
            return TransformOutcome(
                status="REJECT",
                event_type=standard,
                event_time_utc=ts_dt,
                event_date_utc=ts_date,
                visitor_id=visitor_id,
                item_id=item_id,
                reject_reason=RejectReason.INVALID_TRANSACTION_ID,
                reject_message=str(exc),
            )
    elif raw_transactionid.strip():
        return TransformOutcome(
            status="REJECT",
            event_type=standard,
            event_time_utc=ts_dt,
            event_date_utc=ts_date,
            visitor_id=visitor_id,
            item_id=item_id,
            reject_reason=RejectReason.UNEXPECTED_TXNID,
            reject_message="non-transaction event must not carry a transactionid",
        )

    dedup_key = make_dedup_key(ts_ms, visitor_id, standard, item_id, transaction_id)
    return TransformOutcome(
        status="OK",
        event_type=standard,
        event_time_utc=ts_dt,
        event_date_utc=ts_date,
        visitor_id=visitor_id,
        item_id=item_id,
        transaction_id=transaction_id,
        dedup_key=dedup_key,
        quality_flags=tuple(sorted(flags)),
    )

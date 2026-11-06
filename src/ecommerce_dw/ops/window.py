"""业务日期窗口解析（Phase 6，Airflow DAG 专用）。

本模块是**纯函数模块**：不 import Airflow、不连数据库、不做任何 IO，
因此可以被 DAG 模块在加载期安全导入（Phase 6 硬性要求：DAG import 阶段
禁止耗时 IO / 全表查询）。

调度语义：

- ``TAIL`` 模式（默认）：把「逻辑日期 / 数据区间」当作一个业务日期窗口，
  具体哪些日期需要重建由水位线规划器（``incremental.planner``）在任务运行时
  决定——稳定态（水位已消费、ODS 无新批次）会自然得到「无受影响日期」。
- ``FORCE`` 模式：通过 ``conf`` 显式指定回填区间（``dates`` 或 ``from``/``to``），
  显式重建该区间内 ODS 有数据的日期，与 Phase 5 的 ``incremental --from/--to``
  深修正语义一致。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

MODE_TAIL = "TAIL"
MODE_FORCE = "FORCE"


class WindowError(ValueError):
    """入参无法解析出合法业务日期窗口。"""


def parse_dates_csv(raw: str) -> tuple[date, ...]:
    """把 ``2015-09-16,2015-09-17`` 解析为去重、升序的日期元组。"""
    if not raw or not raw.strip():
        raise WindowError("empty date list")
    seen: dict[date, None] = {}
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            seen[date.fromisoformat(token)] = None
        except ValueError as exc:
            raise WindowError(f"invalid date {token!r} in {raw!r}") from exc
    if not seen:
        raise WindowError(f"no valid dates in {raw!r}")
    return tuple(sorted(seen))


def _daterange(start: date, end: date) -> tuple[date, ...]:
    days: list[date] = []
    day = start
    while day <= end:
        days.append(day)
        day += timedelta(days=1)
    return tuple(days)


@dataclass(frozen=True)
class RunWindow:
    """一次 DAG 运行要覆盖的业务日期窗口。"""

    mode: str
    days: tuple[date, ...]
    logical_date: date | None = None

    def __post_init__(self) -> None:
        if self.mode not in (MODE_TAIL, MODE_FORCE):
            raise WindowError(f"unknown mode {self.mode!r}")
        if not self.days:
            raise WindowError("window has no business dates")

    @property
    def start(self) -> date:
        return self.days[0]

    @property
    def end(self) -> date:
        return self.days[-1]

    @property
    def dates_csv(self) -> str:
        """窗口内每一天，逗号分隔（升序）。"""
        return ",".join(d.isoformat() for d in self.days)

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "dates_csv": self.dates_csv,
            "logical_date": self.logical_date.isoformat() if self.logical_date else None,
        }


def resolve_window(
    *,
    logical_date: date | None = None,
    data_interval_start: date | None = None,
    data_interval_end: date | None = None,
    conf: Mapping[str, Any] | None = None,
) -> RunWindow:
    """把调度参数 + ``conf`` 解析为企业务日期窗口。

    优先级（与 Phase 5 语义一致）：

    1. ``conf["dates"]`` —— 显式日期列表（FORCE）；
    2. ``conf["from"]`` + ``conf["to"]`` —— 显式区间（FORCE）；
    3. 其余 —— TAIL，取 ``logical_date``（回退到 ``data_interval_start``），
       即「只处理这一个业务日期」；这一天的重建与否由水位线规划器决定。
    """
    conf = dict(conf or {})
    dates_raw = conf.get("dates")
    from_raw = conf.get("from")
    to_raw = conf.get("to")

    if dates_raw:
        days = parse_dates_csv(str(dates_raw))
        return RunWindow(mode=MODE_FORCE, days=days, logical_date=logical_date)

    if from_raw is not None or to_raw is not None:
        if not from_raw or not to_raw:
            raise WindowError("conf 'from' and 'to' must be given together")
        try:
            start = date.fromisoformat(str(from_raw))
            end = date.fromisoformat(str(to_raw))
        except ValueError as exc:
            raise WindowError(f"invalid from/to: {from_raw!r} / {to_raw!r}") from exc
        if start > end:
            raise WindowError(f"from ({start}) must be <= to ({end})")
        return RunWindow(mode=MODE_FORCE, days=_daterange(start, end), logical_date=logical_date)

    fallback = logical_date or data_interval_start
    if fallback is None:
        raise WindowError(
            "no window could be resolved: give conf['dates'] / conf['from']+conf['to'], "
            "or run through the scheduler with a logical date / data interval"
        )
    return RunWindow(mode=MODE_TAIL, days=(fallback,), logical_date=fallback)

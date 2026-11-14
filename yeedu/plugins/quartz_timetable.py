from __future__ import annotations

import logging
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import timedelta
from functools import lru_cache
from typing import Any, Dict, FrozenSet, Iterator, Optional, Tuple

import pendulum
from pendulum import DateTime
from cron_descriptor import ExpressionDescriptor, FormatException, Options
from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import Timetable, DagRunInfo, DataInterval, TimeRestriction

MONTH_NAMES = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
               "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
# Quartz weekday numbers: SUN=1 ... SAT=7
DAY_NAMES = {"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7}
MIN_YEAR, MAX_YEAR = 1970, 2199
# How far ahead to look for the next run when the expression has no year field
SEARCH_YEARS = 30
# A due slot is still kept this long after its time (DAG re-parses can land before the scheduler creates the run)
SLOT_GRACE = timedelta(seconds=10)


def _now() -> DateTime:
    return pendulum.now("UTC")


@dataclass(frozen=True)
class _QuartzSpec:
    seconds: Tuple[int, ...]
    minutes: Tuple[int, ...]
    hours: Tuple[int, ...]
    months: Tuple[int, ...]
    years: Optional[FrozenSet[int]]
    # ("any",) | ("all",) | ("values", set) | ("last", offset) | ("last_weekday",) | ("nearest_weekday", day)
    dom: tuple = field(default=("any",))
    # ("any",) | ("all",) | ("values", set) | ("last_of", weekday) | ("nth", weekday, n)
    dow: tuple = field(default=("any",))


def _number(text: str, low: int, high: int, names: Optional[Dict[str, int]], expression: str) -> int:
    value = names.get(text) if names else None
    if value is None:
        if not text.isdigit():
            raise AirflowTimetableInvalid(f"invalid value '{text}' in Quartz cron '{expression}'")
        value = int(text)
    if not low <= value <= high:
        raise AirflowTimetableInvalid(f"value {value} out of range {low}-{high} in Quartz cron '{expression}'")
    return value


def _values(text: str, low: int, high: int, names: Optional[Dict[str, int]], expression: str) -> FrozenSet[int]:
    """Parse a plain field: '*', 'n', 'a-b', lists, and steps ('*/s', '/s', 'n/s', 'a-b/s'). Ranges may wrap."""
    span = high - low + 1
    values = set()
    for part in text.split(","):
        if not part:
            raise AirflowTimetableInvalid(f"empty list item in Quartz cron '{expression}'")
        base, step = part, 1
        if "/" in part:
            base, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) < 1:
                raise AirflowTimetableInvalid(f"invalid step '{part}' in Quartz cron '{expression}'")
            step = int(step_text)
            base = base or "*"
        if base == "*":
            start, end = low, high
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start = _number(start_text, low, high, names, expression)
            end = _number(end_text, low, high, names, expression)
        else:
            start = _number(base, low, high, names, expression)
            # 'n/s' runs from n to the end of the range
            end = high if "/" in part else start
        length = (end - start) % span
        values.update(low + (start - low + offset) % span for offset in range(0, length + 1, step))
    return frozenset(values)


def _day_of_month(text: str, expression: str) -> tuple:
    if text == "?":
        return ("any",)
    if text == "*":
        return ("all",)
    if "," in text and ("L" in text or "W" in text):
        raise AirflowTimetableInvalid(f"L and W cannot be combined with other days in Quartz cron '{expression}'")
    if text == "LW":
        return ("last_weekday",)
    if text == "L":
        return ("last", 0)
    if text.startswith("L-"):
        return ("last", _number(text[2:], 0, 30, None, expression))
    if text.endswith("W"):
        return ("nearest_weekday", _number(text[:-1], 1, 31, None, expression))
    return ("values", _values(text, 1, 31, None, expression))


def _day_of_week(text: str, expression: str) -> tuple:
    if text == "?":
        return ("any",)
    if text == "*":
        return ("all",)
    if "," in text and ("L" in text or "#" in text):
        raise AirflowTimetableInvalid(f"# and L cannot be combined with other days in Quartz cron '{expression}'")
    if text == "L":
        return ("values", frozenset({7}))
    if "#" in text:
        day_text, nth_text = text.split("#", 1)
        return ("nth", _number(day_text, 1, 7, DAY_NAMES, expression), _number(nth_text, 1, 5, None, expression))
    if text.endswith("L"):
        return ("last_of", _number(text[:-1], 1, 7, DAY_NAMES, expression))
    return ("values", _values(text, 1, 7, DAY_NAMES, expression))


@lru_cache(maxsize=256)
def _parse(expression: str) -> _QuartzSpec:
    """Parse a 6-field (sec min hour dom month dow) or 7-field (+ year) Quartz expression."""
    parts = expression.strip().upper().split()
    if len(parts) not in (6, 7):
        raise AirflowTimetableInvalid(f"Quartz cron needs 6 or 7 fields, got {len(parts)}: '{expression}'")
    years = None
    if len(parts) == 7 and parts[6] != "*":
        years = _values(parts[6], MIN_YEAR, MAX_YEAR, None, expression)
    return _QuartzSpec(
        seconds=tuple(sorted(_values(parts[0], 0, 59, None, expression))),
        minutes=tuple(sorted(_values(parts[1], 0, 59, None, expression))),
        hours=tuple(sorted(_values(parts[2], 0, 23, None, expression))),
        months=tuple(sorted(_values(parts[4], 1, 12, MONTH_NAMES, expression))),
        years=years,
        dom=_day_of_month(parts[3], expression),
        dow=_day_of_week(parts[5], expression),
    )


def _quartz_weekday(year: int, month: int, day: int) -> int:
    # isoweekday: MON=1 ... SUN=7  ->  Quartz: SUN=1 ... SAT=7
    return pendulum.date(year, month, day).isoweekday() % 7 + 1


def _days_by_month_rule(rule: tuple, year: int, month: int, last: int) -> Optional[set]:
    kind = rule[0]
    if kind in ("any", "all"):
        return None
    if kind == "values":
        return {day for day in rule[1] if day <= last}
    if kind == "last":
        return {last - rule[1]} if last - rule[1] >= 1 else set()
    if kind == "last_weekday":
        day = last
        while _quartz_weekday(year, month, day) in (1, 7):
            day -= 1
        return {day}
    # nearest_weekday: the closest Mon-Fri to the day, without leaving the month
    day = rule[1]
    if day > last:
        return set()
    weekday = _quartz_weekday(year, month, day)
    if weekday == 7:
        day = day - 1 if day > 1 else day + 2
    elif weekday == 1:
        day = day + 1 if day < last else day - 2
    return {day}


def _days_by_week_rule(rule: tuple, year: int, month: int, last: int) -> Optional[set]:
    kind = rule[0]
    if kind in ("any", "all"):
        return None
    days = [day for day in range(1, last + 1)]
    if kind == "values":
        return {day for day in days if _quartz_weekday(year, month, day) in rule[1]}
    matching = [day for day in days if _quartz_weekday(year, month, day) == rule[1]]
    if kind == "last_of":
        return {matching[-1]}
    # nth: no run in a month without that many of the weekday
    return {matching[rule[2] - 1]} if len(matching) >= rule[2] else set()


def _month_days(spec: _QuartzSpec, year: int, month: int) -> list:
    last = monthrange(year, month)[1]
    by_month = _days_by_month_rule(spec.dom, year, month, last)
    by_week = _days_by_week_rule(spec.dow, year, month, last)
    if by_month is None and by_week is None:
        return list(range(1, last + 1))
    if by_month is None or by_week is None:
        return sorted(by_month if by_week is None else by_week)
    # Quartz requires '?' in one of the day fields; when both are set (older DAGs), a day must match both
    return sorted(by_month & by_week)


def _times(spec: _QuartzSpec, after: Optional[Tuple[int, int, int]]) -> Iterator[Tuple[int, int, int]]:
    """Times of day in order, starting at `after` (inclusive) when given."""
    for hour in spec.hours:
        if after and hour < after[0]:
            continue
        for minute in spec.minutes:
            if after and (hour, minute) < after[:2]:
                continue
            for second in spec.seconds:
                if after and (hour, minute, second) < after:
                    continue
                yield hour, minute, second


@dataclass(frozen=True)
class QuartzTimetable(Timetable):
    """Schedules a DAG with a Quartz cron expression, as Databricks jobs do.

    Supports 6 fields (sec min hour day-of-month month day-of-week) or 7 (+ year), with ?, L, L-n, LW, nW,
    nL, n#k, lists, ranges, wrapping ranges and steps. Weekdays are 1-7 or SUN-SAT (SUN=1).
    A slot that comes due while the previous run is still active, or while the DAG is paused, is skipped.
    """

    cron_expression: str
    tz: str = "Asia/Kolkata"

    def __post_init__(self) -> None:
        # Fails the DAG import on unsupported syntax instead of silently running at the wrong times
        _parse(self.cron_expression)

    @property
    def summary(self) -> str:
        # The Yeedu API returns this as schedule_cron
        return self.cron_expression

    @property
    def description(self) -> str:
        try:
            options = Options()
            options.day_of_week_start_index_zero = False
            return ExpressionDescriptor(self.cron_expression, options).get_description()
        except (FormatException, ValueError, KeyError) as e:
            logging.warning("Failed to describe Quartz cron '%s': %s", self.cron_expression, e)
            return self.cron_expression

    def validate(self) -> None:
        _parse(self.cron_expression)

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        anchor = run_after.in_timezone(self.tz)
        return DataInterval(start=anchor, end=anchor.add(seconds=1))

    def next_dagrun_info(self, *, last_automated_data_interval: Optional[DataInterval], restriction: TimeRestriction) -> Optional[DagRunInfo]:
        now = _now()
        if last_automated_data_interval is not None:
            anchor = last_automated_data_interval.end
        else:
            anchor = (restriction.earliest - timedelta(seconds=1)) if restriction.earliest else now
        if not restriction.catchup:
            # Skip slots that passed while the previous run was active or the DAG was paused,
            # but keep one that passed only moments ago: the scheduler may not have created its run yet.
            anchor = max(anchor, now - SLOT_GRACE)
        if restriction.earliest is not None:
            # Never run before the DAG start_date
            anchor = max(anchor, restriction.earliest - timedelta(seconds=1))

        next_start = self._next_valid_start(anchor)
        if next_start is None:
            return None
        if restriction.latest is not None and next_start > restriction.latest:
            return None
        return DagRunInfo.interval(start=next_start, end=next_start.add(seconds=1))

    def serialize(self) -> Dict[str, Any]:
        return {"cron_expression": self.cron_expression, "tz": self.tz}

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "QuartzTimetable":
        return cls(cron_expression=data["cron_expression"], tz=data.get("tz", "Asia/Kolkata"))

    def _next_valid_start(self, dt: DateTime) -> Optional[DateTime]:
        """First fire time strictly after `dt`, in the timetable's timezone, or None if it never fires again."""
        spec = _parse(self.cron_expression)
        local = dt.in_timezone(self.tz)
        today = (local.year, local.month, local.day)
        last_year = max(spec.years) if spec.years else local.year + SEARCH_YEARS
        for year in range(local.year, last_year + 1):
            if spec.years is not None and year not in spec.years:
                continue
            for month in spec.months:
                if (year, month) < today[:2]:
                    continue
                for day in _month_days(spec, year, month):
                    if (year, month, day) < today:
                        continue
                    after = (local.hour, local.minute, local.second) if (year, month, day) == today else None
                    for hour, minute, second in _times(spec, after):
                        candidate = pendulum.datetime(year, month, day, hour, minute, second, tz=self.tz)
                        if candidate > dt:
                            return candidate
        return None

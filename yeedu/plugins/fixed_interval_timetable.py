from __future__ import annotations

import math
from datetime import timedelta
from typing import Any

from pendulum import DateTime

from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.utils import timezone

UNIT_SECONDS = {"MINUTES": 60, "HOURS": 3600, "DAYS": 86400, "WEEKS": 604800}
# A due slot is still kept this long after its time (DAG re-parses can land before the scheduler creates the run)
SLOT_GRACE = timedelta(seconds=10)


def _now() -> DateTime:
    return timezone.coerce_datetime(timezone.utcnow())


class FixedIntervalTimetable(Timetable):
    """Runs every N minutes/hours/days/weeks from the previous scheduled run, like a Databricks periodic trigger.

    The first run is one interval after the DAG start_date. A slot that comes due while the previous run
    is still active (or while the DAG is paused) is skipped, so runs stay on the same grid.
    """

    def __init__(self, interval: int, unit: str) -> None:
        if int(interval) < 1 or unit not in UNIT_SECONDS:
            raise AirflowTimetableInvalid(f"invalid fixed interval: {interval} {unit}")
        self._interval = int(interval)
        self._unit = unit
        self._seconds = self._interval * UNIT_SECONDS[unit]
        self._delta = timedelta(seconds=self._seconds)

    @property
    def summary(self) -> str:
        # The Yeedu API parses this exact format back into schedule_interval
        return f"every {self._interval} {self._unit}"

    def serialize(self) -> dict[str, Any]:
        return {"interval": self._interval, "unit": self._unit}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        return cls(data["interval"], data["unit"])

    def validate(self) -> None:
        if self._seconds <= 0:
            raise AirflowTimetableInvalid("interval must be positive")

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval.exact(run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        now = _now()
        if last_automated_data_interval is None:
            # Anchor on start_date (not now) so repeated DAG parses give the same first run
            next_time = (restriction.earliest or now) + self._delta
        else:
            next_time = last_automated_data_interval.end + self._delta
        if next_time + SLOT_GRACE < now:
            # Skip the slots that passed while the previous run was active or the DAG was paused.
            # A slot that passed only moments ago is kept: the scheduler may not have created its run yet.
            missed = math.ceil((now - next_time).total_seconds() / self._seconds)
            next_time = next_time + timedelta(seconds=self._seconds * missed)
        if restriction.latest is not None and next_time > restriction.latest:
            return None
        return DagRunInfo.exact(timezone.coerce_datetime(next_time))

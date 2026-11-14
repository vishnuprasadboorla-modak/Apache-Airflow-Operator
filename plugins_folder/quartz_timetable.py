# One-time file for the Airflow plugins folder. Keeps the serialized class path "quartz_timetable.QuartzTimetable"
# stable for existing cron DAGs; the real logic lives in the airflow-yeedu-operator whl, so it never drifts.
from yeedu.plugins.quartz_timetable import QuartzTimetable as _QuartzTimetable


class QuartzTimetable(_QuartzTimetable):
    pass

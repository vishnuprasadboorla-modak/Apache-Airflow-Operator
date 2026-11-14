from airflow.plugins_manager import AirflowPlugin
from yeedu.plugins.fixed_interval_timetable import FixedIntervalTimetable


class FixedIntervalTimetablePlugin(AirflowPlugin):
    name = "fixed_interval_timetable_plugin"
    timetables = [FixedIntervalTimetable]

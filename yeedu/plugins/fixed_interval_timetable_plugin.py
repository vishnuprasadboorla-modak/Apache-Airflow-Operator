from airflow.plugins_manager import AirflowPlugin
from yeedu.plugins.fixed_interval_timetable import FixedIntervalTimetable


# Loaded through the whl entry point. The name differs from any plugins-folder copy, which Airflow loads first.
class FixedIntervalTimetablePlugin(AirflowPlugin):
    name = "yeedu_fixed_interval_timetable_plugin"
    timetables = [FixedIntervalTimetable]

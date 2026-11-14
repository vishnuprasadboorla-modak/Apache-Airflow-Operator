from airflow.plugins_manager import AirflowPlugin
from yeedu.plugins.quartz_timetable import QuartzTimetable


# Loaded through the whl entry point. The name differs from the plugins-folder copy, which Airflow loads first.
class QuartzTimetablePlugin(AirflowPlugin):
    name = "yeedu_quartz_timetable_plugin"
    timetables = [QuartzTimetable]

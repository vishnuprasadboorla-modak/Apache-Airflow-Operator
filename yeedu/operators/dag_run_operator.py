from __future__ import annotations

from typing import Any, Mapping

# from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


# class YeeduDagRunOperator(BaseOperator):
#     """
#     Wrapper around TriggerDagRunOperator that only adds:
#       - loop_input templating
#       - your exact XCom push behavior for loop_input
#     """

#     template_fields = ("loop_input",)

#     def __init__(
#         self,
#         *,
#         trigger_dag_id: str,
#         conf: Mapping[str, Any] | None = None,
#         loop_input: Any = None,
#         wait_for_completion: bool = True,
#         poke_interval: int = 60,
#         reset_dag_run: bool = False,
#         **kwargs,
#     ):
#         super().__init__(**kwargs)
#         self.trigger_dag_id = trigger_dag_id
#         self.conf = dict(conf) if conf else {}
#         self.loop_input = loop_input

#         self.wait_for_completion = wait_for_completion
#         self.poke_interval = poke_interval
#         self.reset_dag_run = reset_dag_run

#     def execute(self, context: Context):
#         # ---- 1) COPY your loop_input XCom push logic (unchanged) ----
#         ti = context["ti"]
#         run_id = ti.run_id
#         map_index = ti.map_index
#         task_id = ti.task_id

#         composite_key = f"{run_id}__{task_id}__{map_index}"

#         value = self.loop_input
#         if not isinstance(value, (str, int, float, dict, list)):
#             value = str(value)

#         ti.xcom_push(key=composite_key, value=value)

#         # ---- 2) Internally call TriggerDagRunOperator ----
#         trigger = TriggerDagRunOperator(
#             # same as wrapper task_id (safe in this inline-execute pattern)
#             task_id=self.task_id,
#             trigger_dag_id=self.trigger_dag_id,
#             conf=self.conf,
#             wait_for_completion=self.wait_for_completion,
#             poke_interval=self.poke_interval,
#             reset_dag_run=self.reset_dag_run,
#         )

#         # IMPORTANT: since we're calling execute() directly, we must render templates ourselves
#         # (TriggerDagRunOperator has templated fields like trigger_dag_id, conf, etc.)
#         # trigger.render_template_fields(context)

#         return trigger.execute(context)

class YeeduDagRunOperator(TriggerDagRunOperator):
    template_fields = tuple(
        f for f in TriggerDagRunOperator.template_fields if f != "conf"
    )
    def __init__(self, *, loop_input=None, **kwargs):
        super().__init__(
            wait_for_completion=True,
            deferrable=True,      # <-- key change
            poke_interval=60,
            **kwargs,
        )
        self.loop_input = loop_input

    def execute(self, context):
        # your XCom push logic (unchanged)
        ti = context["ti"]
        composite_key = f"{ti.run_id}__{ti.task_id}__{ti.map_index}"

        value = self.loop_input
        if not isinstance(value, (str, int, float, dict, list)):
            value = str(value)

        ti.xcom_push(key=composite_key, value=value)

        return super().execute(context)

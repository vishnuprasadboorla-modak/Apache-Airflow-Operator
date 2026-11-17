"""Hook for resolving runtime expressions and managing Yeedu-specific context."""

from __future__ import annotations

import json
import re
from datetime import timedelta
from typing import Any, Dict, List, Callable

from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule

from yeedu.utils.common import (
    resolve_runtime_expr,
    resolve_condition_operand,
    resolve_all_task_params,
    resolve_foreach_input,
    tr,
    retry_delay_from_milliseconds,
    build_job_url,
    pipeline_id_to_dag_id
)
from yeedu.utils.callbacks import (
    dag_success_cb_factory,
    dag_failure_cb_factory,
    dag_start_cb_factory,
    task_success_cb_factory,
    task_failure_cb_factory,
    task_start_cb_factory,
)
from yeedu.utils.metadata import (
    get_job_meta,
    get_job_parameters,
    get_current_task_meta,
    get_workspace_meta
)

# Prefix of the one structured log line the Yeedu task logs API returns as branch_result
BRANCH_RESULT_PREFIX = "Branch result: "

# Condition operators and the symbol shown in task logs
CONDITION_SYMBOLS = {
    "EQUAL_TO": "==", "EQ": "==", "NOT_EQUAL": "!=", "NE": "!=",
    "GREATER_THAN": ">", "LESS_THAN": "<",
    "GREATER_THAN_OR_EQUAL": ">=", "GE": ">=", "LESS_THAN_OR_EQUAL": "<=", "LE": "<=",
}
ORDERING_OPERATORS = ("GREATER_THAN", "LESS_THAN", "GREATER_THAN_OR_EQUAL", "LESS_THAN_OR_EQUAL", "GE", "LE")
NUMBER = re.compile(r"\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*")


def _shown(operand: Any) -> str:
    """An operand as written: references stay as they are, constants are quoted (like Databricks)."""
    text = str(operand)
    return text if "{{" in text else json.dumps(text, ensure_ascii=False)


def _quoted(value: Any) -> str:
    """A resolved value, quoted."""
    return json.dumps(str(value), ensure_ascii=False)


def compare_condition(left_val: Any, op: str, right_val: Any) -> bool:
    """Compares resolved operands; ordering operators need numbers and fail the task otherwise."""
    if op in ORDERING_OPERATORS:
        for side, val in (("left", left_val), ("right", right_val)):
            if not NUMBER.fullmatch(str(val)):
                raise ValueError(f'Numeric operand "{side}" is not a valid number. Actual value: {val}')
    if op == "GREATER_THAN":
        return float(left_val) > float(right_val)
    if op == "LESS_THAN":
        return float(left_val) < float(right_val)
    if op in ("EQUAL_TO", "EQ"):
        return str(left_val) == str(right_val)
    if op in ("NOT_EQUAL", "NE"):
        return str(left_val) != str(right_val)
    if op in ("GREATER_THAN_OR_EQUAL", "GE"):
        return float(left_val) >= float(right_val)
    if op in ("LESS_THAN_OR_EQUAL", "LE"):
        return float(left_val) <= float(right_val)
    return bool(left_val)


class YeeduRuntimeHook(BaseHook):
    """
    Hook for managing Yeedu runtime expressions and context metadata.

    This hook provides methods to resolve runtime expressions and extract
    metadata from Airflow context for use in Yeedu jobs and pipelines.
    """

    def __init__(self):
        """Initialize the YeeduRuntimeHook."""
        super().__init__()

    def resolve_expression(self, expr: Any, context: Dict[str, Any]) -> Any:
        """
        Resolve runtime expressions using Airflow context.

        Args:
            expr: The expression to resolve
            context: The Airflow task context

        Returns:
            The resolved expression
        """
        self.log.info("[YeeduRuntimeHook.resolve_expression] BEFORE: %r", expr)
        result = resolve_runtime_expr(expr, context)
        self.log.info("[YeeduRuntimeHook.resolve_expression] AFTER:  %r → %r", expr, result)
        return result

    def resolve_condition_operand(self, expr: Any, context: Dict[str, Any]) -> Any:
        """Resolve an if/else condition operand; fails when it is a task value that is a list or dict."""
        self.log.info("[YeeduRuntimeHook.resolve_condition_operand] BEFORE: %r", expr)
        result = resolve_condition_operand(expr, context)
        self.log.info("[YeeduRuntimeHook.resolve_condition_operand] AFTER:  %r → %r", expr, result)
        return result

    def _log_branch_result(self, result: Dict[str, Any]) -> None:
        """Logs the structured result that the task logs API returns as branch_result."""
        self.log.info("%s%s", BRANCH_RESULT_PREFIX, json.dumps(result, ensure_ascii=False, default=str))

    def _log_next_tasks(self, targets: List[str], outcome: str) -> None:
        if targets:
            self.log.info("Next tasks: %s", ", ".join(targets))
        else:
            self.log.info("No task runs on the %s outcome.", outcome)

    def evaluate_condition(self, left: Any, op: str, right: Any, true_targets: List[str],
                           false_targets: List[str], context: Dict[str, Any]) -> List[str]:
        """
        Evaluates an if/else condition, logs it like Databricks (original, resolved, outcome)
        and returns the tasks to run next. Errors still fail the task.
        """
        symbol = CONDITION_SYMBOLS.get(op, op)
        result: Dict[str, Any] = {
            "type": "condition",
            "original": {"left": left, "op": op, "right": right},
            "resolved": None,
            "outcome": None,
        }
        self.log.info("Original expression: %s %s %s", _shown(left), symbol, _shown(right))
        try:
            left_val = resolve_condition_operand(left, context)
            right_val = resolve_condition_operand(right, context)
            result["resolved"] = {"left": str(left_val), "right": str(right_val)}
            self.log.info("Resolved expression: %s %s %s", _quoted(left_val), symbol, _quoted(right_val))
            matched = compare_condition(left_val, op, right_val)
        except Exception as e:
            result["error"] = str(e)
            self._log_branch_result(result)
            raise

        result["outcome"] = "true" if matched else "false"
        targets = list(true_targets if matched else false_targets)
        result["next_tasks"] = targets
        self.log.info("Expression outcome: %s", result["outcome"])
        self._log_next_tasks(targets, result["outcome"])
        self._log_branch_result(result)
        return targets

    def evaluate_switch(self, expr: Any, case_targets: Dict[str, List[str]],
                        default_targets: List[str], context: Dict[str, Any]) -> List[str]:
        """
        Evaluates a switch, logs the expression, its resolved value and the matched case,
        and returns the tasks to run next. Errors still fail the task.
        """
        result: Dict[str, Any] = {
            "type": "switch",
            "original": {"expr": expr, "cases": [str(case) for case in case_targets]},
            "resolved": None,
            "outcome": None,
        }
        self.log.info("Expression: %s", _shown(expr))
        try:
            value = resolve_runtime_expr(expr, context)
            result["resolved"] = {"expr": str(value)}
            self.log.info("Resolved value: %s", _quoted(value))
            matched_case = None
            for case_value in case_targets:
                if str(value) == str(resolve_runtime_expr(str(case_value), context)):
                    matched_case = case_value
                    break
        except Exception as e:
            result["error"] = str(e)
            self._log_branch_result(result)
            raise

        if matched_case is None:
            result["outcome"] = "default"
            targets = list(default_targets)
            self.log.info("No case matched, using default.")
        else:
            result["outcome"] = str(matched_case)
            targets = list(case_targets[matched_case])
            self.log.info("Matched case: %s", _quoted(matched_case))
        result["next_tasks"] = targets
        self._log_next_tasks(targets, result["outcome"])
        self._log_branch_result(result)
        return targets

    def get_job_metadata(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract job metadata from Airflow context.

        Args:
            context: The Airflow task context

        Returns:
            Dictionary containing job metadata
        """
        return get_job_meta(context)

    def get_job_parameters(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract job parameters from Airflow context.

        Args:
            context: The Airflow task context

        Returns:
            Dictionary containing job parameters
        """
        return get_job_parameters(context)

    def get_task_metadata(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract current task metadata from Airflow context.

        Args:
            context: The Airflow task context

        Returns:
            Dictionary containing task metadata
        """
        return get_current_task_meta(context)

    def get_workspace_metadata(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract workspace metadata from Airflow context.

        Args:
            context: The Airflow task context

        Returns:
            Dictionary containing workspace metadata
        """
        return get_workspace_meta(context)

    def resolve_parameters(self, params: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve all runtime expressions in a parameters dictionary.
        Handles nested dicts and lists recursively.

        Args:
            params: Dictionary of parameters that may contain expressions
            context: The Airflow task context

        Returns:
            Dictionary with all expressions resolved
        """
        self.log.info("[YeeduRuntimeHook.resolve_parameters] BEFORE: %s", params)
        resolved = resolve_all_task_params(params, context)
        self.log.info("[YeeduRuntimeHook.resolve_parameters] AFTER:  %s", resolved)
        return resolved

    def resolve_foreach_input(
        self,
        expr: str,
        tasks_dict: Dict[str, Any],
        dag_params: Dict[str, Any] | None = None,
        task_params: Dict[str, Any] | None = None,
    ) -> Any:
        """
        Resolve a for_each input expression for .expand(loop_input=...).

        Returns a static list or XComArg depending on the expression:
          - "[1,2,3]" → [1, 2, 3]
          - "{{ tasks.X.values.Y }}" → XComArg(tasks["X"], key="Y")
          - "{{ job.parameters.P }}" → parsed list from dag_params
          - "{{ task.parameters.P }}" → parsed list from task_params

        Args:
            expr: The for_each input expression
            tasks_dict: DAG's tasks dict (task_id → operator)
            dag_params: DAG-level parameters for job.parameters.* resolution
            task_params: Task-level parameters for task.parameters.* resolution
        """
        self.log.debug("[YeeduRuntimeHook.resolve_foreach_input] BEFORE: expr=%r, dag_params=%s, task_params=%s",
                      expr, dag_params, task_params)
        result = resolve_foreach_input(expr, tasks_dict, dag_params, task_params)
        self.log.info("[YeeduRuntimeHook.resolve_foreach_input] AFTER:  expr=%r → %r (type=%s)",
                      expr, result, type(result).__name__)
        return result

    # Utility methods
    @staticmethod
    def trigger_rule(name: str | None) -> TriggerRule:
        """Convert string trigger rule to TriggerRule enum."""
        return tr(name)

    @staticmethod
    def retry_delay_from_ms(milliseconds: int | None) -> timedelta:
        """Convert milliseconds to timedelta for retry delays."""
        return retry_delay_from_milliseconds(milliseconds)

    @staticmethod
    def build_job_url(
        job_id: int,
        job_kind: str,
        workspace_id: int,
        tenant_id: str,
        hostname: str,
        port: int,
        ssl_enabled: str
    ) -> str:
        """Build Yeedu job URL."""
        return build_job_url(job_id, job_kind, workspace_id, tenant_id, hostname, port, ssl_enabled)

    @staticmethod
    def pipeline_to_dag_id(pipeline_id: int | str) -> str:
        """Convert pipeline ID to DAG ID."""
        return pipeline_id_to_dag_id(pipeline_id)

    # Callback factory methods
    @staticmethod
    def dag_success_callback(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
        """Create DAG success callback."""
        return dag_success_cb_factory(recipients)

    @staticmethod
    def dag_failure_callback(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
        """Create DAG failure callback."""
        return dag_failure_cb_factory(recipients)

    @staticmethod
    def dag_start_callback(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
        """Create DAG start callback."""
        return dag_start_cb_factory(recipients)

    @staticmethod
    def task_success_callback(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
        """Create task success callback."""
        return task_success_cb_factory(recipients)

    @staticmethod
    def task_failure_callback(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
        """Create task failure callback."""
        return task_failure_cb_factory(recipients)

    @staticmethod
    def task_start_callback(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
        """Create task start callback."""
        return task_start_cb_factory(recipients)

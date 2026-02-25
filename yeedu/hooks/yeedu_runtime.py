"""Hook for resolving runtime expressions and managing Yeedu-specific context."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, List, Callable

from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule

from yeedu.utils.common import (
    resolve_runtime_expr,
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
        self.log.info("[YeeduRuntimeHook.resolve_foreach_input] BEFORE: expr=%r, dag_params=%s, task_params=%s",
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

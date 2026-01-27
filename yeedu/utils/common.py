"""Common utility functions for Yeedu Airflow integration."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any, Dict

from airflow.utils.trigger_rule import TriggerRule


# Trigger rule mapping
TR_MAP = {
    "All Succeeded": TriggerRule.ALL_SUCCESS,
    "At Least One Succeeded": TriggerRule.ONE_SUCCESS,
    "None Failed": TriggerRule.NONE_FAILED,
    "All Done": TriggerRule.ALL_DONE,
    "At Least One Failed": TriggerRule.ONE_FAILED,
    "All Failed": TriggerRule.ALL_FAILED,
}


def tr(name: str | None) -> TriggerRule:
    """
    Convert a string trigger rule name to an Airflow TriggerRule enum.

    Args:
        name: The trigger rule name as a string, defaults to "All Succeeded"

    Returns:
        The corresponding TriggerRule enum value
    """
    return TR_MAP.get(name or "All Succeeded", TriggerRule.ALL_SUCCESS)


def retry_delay_from_milliseconds(milliseconds: int | None) -> timedelta:
    """
    Convert milliseconds to a timedelta for retry delays.

    Args:
        milliseconds: The delay in milliseconds, defaults to 0

    Returns:
        A timedelta object representing the delay
    """
    milliseconds = milliseconds or 0
    return timedelta(milliseconds=milliseconds)


# Runtime expression resolver
_RUNTIME_TOKEN_RE = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")


def resolve_runtime_expr(expr: Any, context: Dict[str, Any]) -> Any:
    """
    Resolve runtime expressions in strings.

    Supports expressions like:
    - {{ job.id }}, {{ job.parameters.some_key }}
    - {{ task.execution_count }}, {{ tasks.other.values.my_value }}
    - {{ workspace.url }}, etc.

    Args:
        expr: The expression to resolve (can be any type, only strings are processed)
        context: The Airflow task context dictionary

    Returns:
        The resolved expression or the original value if not a string
    """
    if expr is None:
        return None
    if not isinstance(expr, str):
        return expr

    from .metadata import get_job_meta, get_job_parameters, get_current_task_meta, get_workspace_meta

    job_meta = get_job_meta(context)
    job_params = get_job_parameters(context)
    cur_task_meta = get_current_task_meta(context)

    def _eval_token(token: str) -> Any:
        token = token.strip()

        if token.startswith("job.parameters."):
            key = token.split(".", 2)[2]
            return job_params.get(key)

        if token.startswith("job."):
            return _eval_job_token(token, job_meta)

        if token.startswith("task."):
            return _eval_current_task_token(token, cur_task_meta)

        if token.startswith("tasks."):
            return _eval_other_task_token(token, context)

        if token.startswith("workspace."):
            ws_meta = get_workspace_meta(context)
            return _eval_workspace_token(token, ws_meta)

        # unknown token – return raw
        return "{{ " + token + " }}"

    def replacer(match: re.Match) -> str:
        inner = match.group(1)
        val = _eval_token(inner)
        return str(val) if val is not None else ""

    try:
        return _RUNTIME_TOKEN_RE.sub(replacer, expr)
    except Exception:
        return expr


def _eval_job_token(token: str, job_meta: Dict[str, Any]) -> Any:
    """Evaluate job-related tokens."""
    if token == "job.id":
        return job_meta["id"]
    if token == "job.name":
        return job_meta["name"]
    if token == "job.run_id":
        return job_meta["run_id"]

    if token.startswith("job.start_time."):
        field = token.split(".", 2)[2]
        return job_meta["start_time"].get(field)

    if token.startswith("job.trigger.time."):
        field = token.split(".", 3)[3]
        return job_meta["trigger"]["time"].get(field)

    if token == "job.trigger.type":
        return job_meta["trigger"]["type"]

    return None


def _eval_current_task_token(token: str, task_meta: Dict[str, Any]) -> Any:
    """Evaluate current task-related tokens."""
    if token == "task.execution_count":
        return task_meta["execution_count"]
    if token == "task.name":
        return task_meta["name"]
    if token == "task.run_id":
        return task_meta["run_id"]
    return None


def _eval_other_task_token(token: str, context: Dict[str, Any]) -> Any:
    """Evaluate tokens for other tasks (XCom values)."""
    parts = token.split(".")
    if len(parts) != 4:
        return None
    _, task_id, field, key = parts
    if field != "values":
        return None
    ti = context.get("ti")
    if ti:
        return ti.xcom_pull(task_ids=task_id, key=key)
    return None


def _eval_workspace_token(token: str, ws_meta: Dict[str, Any]) -> Any:
    """Evaluate workspace-related tokens."""
    if token == "workspace.id":
        return ws_meta["id"]
    if token == "workspace.url":
        return ws_meta["url"]
    return None


def build_job_url(job_id: int, job_kind: str) -> str:
    """
    Build a Yeedu job URL based on the job ID and type.

    Args:
        job_id: The job ID
        job_kind: The job type ('notebook' or other)

    Returns:
        The formatted job URL
    """
    if job_kind == "notebook":
        return f"yeedu://notebook/{job_id}"
    return f"yeedu://job/{job_id}"


def pipeline_id_to_dag_id(pipeline_id: int | str) -> str:
    """
    Map a Yeedu PIPELINE.ID to an Airflow dag_id.

    Args:
        pipeline_id: The pipeline ID

    Returns:
        The formatted DAG ID
    """
    return f"yeedu_pipeline_{pipeline_id}"

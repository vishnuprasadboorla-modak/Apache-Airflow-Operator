"""Common utility functions for Yeedu Airflow integration."""

from __future__ import annotations

import logging
import re
from datetime import timedelta
from typing import Any, Dict

from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)


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


def resolve_runtime_expr(expr: Any, context: Dict[str, Any] = None) -> Any:
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

    if isinstance(expr, str) and not _RUNTIME_TOKEN_RE.search(expr):
        return expr

    log.info("[resolve_expr] INPUT: %r", expr)

    from .metadata import get_job_meta, get_job_parameters, get_current_task_meta, get_workspace_meta

    job_meta = get_job_meta(context)
    job_params = get_job_parameters(context)
    cur_task_meta = get_current_task_meta(context)

    def _eval_token(token: str) -> Any:
        token = token.strip()

        if token.startswith("job.parameters."):
            key = token.split(".", 2)[2]
            val = job_params.get(key)
            log.info("[resolve_expr]   token=%r → job_params[%r] = %r (type=%s)", token, key, val, type(val).__name__)
            return val

        if token.startswith("job."):
            val = _eval_job_token(token, job_meta)
            log.info("[resolve_expr]   token=%r → %r (type=%s)", token, val, type(val).__name__)
            return val

        if token.startswith("task."):
            val = _eval_current_task_token(token, cur_task_meta)
            log.info("[resolve_expr]   token=%r → %r (type=%s)", token, val, type(val).__name__)
            return val

        if token.startswith("tasks."):
            val = _eval_other_task_token(token, context)
            log.info("[resolve_expr]   token=%r → %r (type=%s)", token, val, type(val).__name__)
            return val

        if token.startswith("workspace."):
            ws_meta = get_workspace_meta(context)
            val = _eval_workspace_token(token, ws_meta)
            log.info("[resolve_expr]   token=%r → %r (type=%s)", token, val, type(val).__name__)
            return val

        if token == "loop_input":
            val = _eval_loop_input(context)
            log.info("[resolve_expr]   token=%r → %r (type=%s)", token, val, type(val).__name__)
            return val

        # unknown token – return raw
        log.info("[resolve_expr]   token=%r → UNKNOWN, returning raw", token)
        return "{{ " + token + " }}"

    def replacer(match: re.Match) -> str:
        inner = match.group(1)
        val = _eval_token(inner)
        return str(val) if val is not None else ""

    try:
        result = _RUNTIME_TOKEN_RE.sub(replacer, expr)
        log.info("[resolve_expr] OUTPUT: %r → %r", expr, result)
        return result
    except Exception as e:
        log.error("[resolve_expr] ERROR resolving %r: %s", expr, e)
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
    """
    Evaluate tokens for other tasks.

    Supported patterns:
      tasks.<name>.values.<key>    → XCom pull (set by dbutils.taskValues.set)
      tasks.<name>.result_state    → Airflow task instance state
      tasks.<name>.run_id          → Airflow task instance run_id
      tasks.<name>.execution_count → Airflow task instance try_number
    """
    from .metadata import get_other_task_instance

    parts = token.split(".")

    # tasks.<name>.values.<key> — 4 parts
    if len(parts) == 4:
        _, task_id, field, key = parts
        if field == "values":
            ti = context.get("ti")
            if ti:
                return ti.xcom_pull(task_ids=task_id, key=key)
        return None

    # tasks.<name>.<field> — 3 parts (result_state, run_id, execution_count)
    if len(parts) == 3:
        _, task_id, field = parts
        other_ti = get_other_task_instance(task_id, context)
        if other_ti is None:
            return None
        if field == "result_state":
            return getattr(other_ti, "state", None)
        if field == "run_id":
            return getattr(other_ti, "run_id", None)
        if field == "execution_count":
            return getattr(other_ti, "try_number", None)
        return None

    return None


def _eval_workspace_token(token: str, ws_meta: Dict[str, Any]) -> Any:
    """Evaluate workspace-related tokens."""
    if token == "workspace.id":
        return ws_meta["id"]
    if token == "workspace.url":
        return ws_meta["url"]
    return None

def resolve_all_task_params(
    params: Dict[str, Any],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Resolve all runtime expressions in a parameters dictionary.

    Walks the dict recursively — resolves strings, recurses into nested
    dicts and lists, passes through everything else unchanged.

    Args:
        params: Dictionary of parameters that may contain {{ expressions }}
        context: The Airflow task context dictionary

    Returns:
        New dictionary with all expressions resolved
    """
    if params is None:
        log.info("[resolve_all_task_params] params is None, returning {}")
        return {}

    log.info("[resolve_all_task_params] INPUT params: %s", params)

    def _resolve_value(value: Any) -> Any:
        if isinstance(value, str):
            return resolve_runtime_expr(value, context)
        if isinstance(value, dict):
            return {k: _resolve_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_resolve_value(item) for item in value]
        return value

    resolved = {key: _resolve_value(val) for key, val in params.items()}
    log.info("[resolve_all_task_params] OUTPUT params: %s", resolved)
    return resolved


def _eval_loop_input(context: Dict[str, Any]) -> Any:
    """
    Evaluate the loop_input token for for_each mapped tasks.
    Pulls the value from XCom using the composite key pattern:
    '{run_id}__{task_id}__{map_index}'.
    """
    ti = context.get("ti")
    if not ti:
        return None
    run_id = ti.run_id
    task_id = ti.task_id
    map_index = getattr(ti, "map_index", -1)
    composite_key = f"{run_id}__{task_id}__{map_index}"
    return ti.xcom_pull(task_ids=task_id, key=composite_key)


def resolve_foreach_input(
    expr: str,
    tasks_dict: Dict[str, Any],
    dag_params: Dict[str, Any] | None = None,
    task_params: Dict[str, Any] | None = None,
) -> Any:
    """
    Resolve a for_each input expression for use with .expand(loop_input=...).

    Called at DAG parse time (not inside a task's execute()).
    Returns either a static list or an XComArg for dynamic expansion.

    Cases:
      1. Static list string like "[1,2,3]"
         → returns [1, 2, 3]

      2. Expression "{{ tasks.<name>.values.<key> }}"
         → returns XComArg(tasks_dict[name], key=key)
         → Airflow resolves the XCom value at runtime

      3. Expression "{{ job.parameters.<param> }}"
         → looks up param default from dag_params, parses as JSON list
         → returns the parsed list

      4. Expression "{{ task.parameters.<param> }}"
         → looks up param from task_params, parses as JSON list
         → returns the parsed list

    Args:
        expr: The for_each input expression string
        tasks_dict: The DAG's tasks dictionary (task_id → operator instance)
        dag_params: DAG-level parameters dict (for job.parameters.* resolution)
        task_params: Task-level parameters dict (for task.parameters.* resolution)

    Returns:
        A list (static) or XComArg (dynamic) suitable for .expand(loop_input=...)
    """
    import json

    expr = expr.strip()
    log.info("[resolve_foreach_input] INPUT: expr=%r", expr)

    # Case 1: No expression markers → static list
    if "{{" not in expr:
        try:
            parsed = json.loads(expr)
            if isinstance(parsed, list):
                log.info("[resolve_foreach_input] Case 1 (static list): %r → %r", expr, parsed)
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        # Single static value → wrap in list
        log.info("[resolve_foreach_input] Case 1 (single static): %r → [%r]", expr, expr)
        return [expr]

    # Extract the token from {{ ... }}
    match = _RUNTIME_TOKEN_RE.match(expr)
    if not match:
        log.info("[resolve_foreach_input] No token match, wrapping: %r → [%r]", expr, expr)
        return [expr]

    token = match.group(1).strip()
    log.info("[resolve_foreach_input] Extracted token: %r", token)

    # Case 2: tasks.<name>.values.<key> → XComArg
    if token.startswith("tasks."):
        parts = token.split(".")
        if len(parts) == 4 and parts[2] == "values":
            from airflow.models.xcom_arg import XComArg

            task_id = parts[1]
            key = parts[3]
            if task_id not in tasks_dict:
                raise ValueError(
                    f"for_each input references task '{task_id}' "
                    f"but it was not found in tasks dict"
                )
            result = XComArg(tasks_dict[task_id], key=key)
            log.info("[resolve_foreach_input] Case 2 (XComArg): task_id=%r, key=%r → %r", task_id, key, result)
            return result

    # Case 3: job.parameters.<param> → resolve from dag_params at parse time
    if token.startswith("job.parameters.") and dag_params:
        param_key = token.split(".", 2)[2]
        value = dag_params.get(param_key)
        log.info("[resolve_foreach_input] Case 3 (job.parameters): param_key=%r, raw_value=%r", param_key, value)
        if value is not None:
            result = _parse_foreach_value(value)
            log.info("[resolve_foreach_input] Case 3 (job.parameters): parsed → %r", result)
            return result

    # Case 4: task.parameters.<param> → resolve from task_params at parse time
    if token.startswith("task.parameters.") and task_params:
        param_key = token.split(".", 2)[2]
        value = task_params.get(param_key)
        log.info("[resolve_foreach_input] Case 4 (task.parameters): param_key=%r, raw_value=%r", param_key, value)
        if value is not None:
            result = _parse_foreach_value(value)
            log.info("[resolve_foreach_input] Case 4 (task.parameters): parsed → %r", result)
            return result

    # Fallback: return as single-element list
    log.info("[resolve_foreach_input] Fallback: %r → [%r]", expr, expr)
    return [expr]


def _parse_foreach_value(value: Any) -> list:
    """Parse a parameter value into a list for for_each expansion."""
    import json

    try:
        parsed = json.loads(str(value))
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    # Non-list value → wrap in list
    return [value]


def build_job_url(
    job_id: int,
    job_kind: str,
    workspace_id: int,
    tenant_id: str,
    hostname: str,
    port: int,
    ssl_enabled: str
) -> str:
    """
    Build the full Yeedu job URL.

    Args:
        job_id: The job/notebook ID.
        job_kind: 'job' or 'notebook'.
        workspace_id: Workspace identifier.
        tenant_id: Tenant UUID.
        hostname: The Yeedu server hostname.
        port: The port number.
        ssl_enabled: Whether to use HTTPS or HTTP

    Returns:
        Full URL: https://{hostname}:{port}/tenant/{tenant_id}/workspace/{workspace_id}/{kind}/{job_id}
    """
    kind = (job_kind or "job").strip().lower()
    protocol = "https" if str(ssl_enabled).lower() == "true" else "http"

    return f"{protocol}://{hostname}:{port}/tenant/{tenant_id}/workspace/{workspace_id}/{kind}/{job_id}"


def pipeline_id_to_dag_id(pipeline_id: int | str) -> str:
    """
    Map a Yeedu PIPELINE.ID to an Airflow dag_id.

    Args:
        pipeline_id: The pipeline ID

    Returns:
        The formatted DAG ID
    """
    return f"yeedu_pipeline_{pipeline_id}"

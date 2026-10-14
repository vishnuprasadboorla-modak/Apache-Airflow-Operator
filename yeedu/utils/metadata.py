"""Metadata extraction utilities for Airflow context."""

from __future__ import annotations

import logging
from datetime import timezone
from typing import Any, Dict, Optional

from airflow.models.dagrun import DagRun

log = logging.getLogger(__name__)

# XCom keys YeeduOperator publishes about its own run, read back by {{ tasks.<name>.run_id / execution_count }}.
# Airflow 3's task SDK cannot read another task instance's id or try number directly.
TASK_RUN_ID_XCOM_KEY = "yeedu_task_run_id"
TASK_EXECUTION_COUNT_XCOM_KEY = "yeedu_task_execution_count"


def _iso_utc_millis(dt) -> str:
    """UTC ISO-8601 with millisecond precision and a Z suffix, e.g. 2026-10-12T07:21:59.116Z (matches the REST API)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{dt.microsecond // 1000:03d}Z"


def _extract_time_fields(dt) -> Dict[str, Any]:
    """Extract all date/time fields from a datetime object."""
    if not dt:
        return {
            "day": None, "hour": None, "is_weekday": None,
            "iso_date": None, "iso_datetime": None, "iso_weekday": None,
            "minute": None, "month": None, "second": None,
            "year": None, "timestamp_ms": None,
        }
    return {
        "day":          dt.day,
        "hour":         dt.hour,
        "is_weekday":   str(dt.isoweekday() in (1, 2, 3, 4, 5)).lower(),
        "iso_date":     dt.date().isoformat(),
        "iso_datetime": _iso_utc_millis(dt),
        "iso_weekday":  dt.isoweekday(),
        "minute":       dt.minute,
        "month":        dt.month,
        "second":       dt.second,
        "year":         dt.year,
        "timestamp_ms": int(dt.timestamp() * 1000),
    }


def _extract_job_id(dag_id: str) -> str | None:
    """Extract pipeline ID from dag_id. dag_id format: '{workspace_id}_{pipeline_id}'."""
    if dag_id and "_" in dag_id:
        return dag_id.split("_", 1)[1]
    return None


def _extract_trigger_type(dag_run) -> str | None:
    """Extract trigger type from dag_run.run_type, uppercased to match Databricks convention."""
    run_type = getattr(dag_run, "run_type", None)
    if run_type:
        # Airflow 3.x: run_type is DagRunType enum, .value gives the plain string
        val = getattr(run_type, "value", str(run_type))
        return str(val).upper()
    return None


def get_job_meta(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract job metadata from Airflow context.

    Args:
        context: The Airflow task context dictionary

    Returns:
        Dictionary containing job metadata
    """
    dag = context.get("dag")
    dag_run = context.get("dag_run")

    dag_id = dag.dag_id if dag else None
    # dag_display_name is set from the pipeline's real name at DAG-generation time
    # (see _dag_header_open.njk); falls back to dag_id for DAGs generated before
    # that, or if it's ever empty.
    dag_display_name = (getattr(dag, "dag_display_name", None) if dag else None) or dag_id

    # start_time = dag_run.start_date  (actual wall-clock time execution began)
    # trigger.time = dag_run.logical_date  (scheduled/logical time the run represents)
    start_date = getattr(dag_run, "start_date", None)
    logical_date = getattr(dag_run, "logical_date", None)

    meta = {
        "id": _extract_job_id(dag_id),
        "name": dag_display_name,
        "run_id": getattr(dag_run, "run_id", None),
        "start_time": _extract_time_fields(start_date),
        "trigger": {
            "time": _extract_time_fields(logical_date),
            "type": _extract_trigger_type(dag_run),
        },
    }
    log.info("[get_job_meta] dag_id=%s, job_id=%s, name=%s, run_id=%s, trigger_type=%s",
             dag_id, meta["id"], meta["name"], meta["run_id"], meta["trigger"]["type"])
    log.info("[get_job_meta] start_time=%s", meta["start_time"])
    return meta


def get_job_parameters(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract job parameters from Airflow context.

    Args:
        context: The Airflow task context dictionary

    Returns:
        Dictionary containing merged DAG params and runtime configuration
    """
    dag = context.get("dag")
    dag_run = context.get("dag_run")

    dag_params = (dag and getattr(dag, "params", {})) or {}
    try:
        dag_params = dict(dag_params)
    except Exception:
        dag_params = {}

    conf = (dag_run and (dag_run.conf or {})) or {}
    merged = dict(dag_params)
    merged.update(conf)
    log.info("[get_job_parameters] dag_params=%s", dag_params)
    log.info("[get_job_parameters] conf_overrides=%s", conf)
    log.info("[get_job_parameters] merged=%s", merged)
    return merged


def _task_instance_id(ti) -> Optional[str]:
    """Task instance id (UUID) as a string, or None. Distinct from the dag run id."""
    ti_id = getattr(ti, "id", None)
    return str(ti_id) if ti_id is not None else None


def get_current_task_meta(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract current task metadata from Airflow context.

    Args:
        context: The Airflow task context dictionary

    Returns:
        Dictionary containing current task metadata
    """
    ti = context.get("ti")
    task = ti.task if ti else None
    meta = {
        "execution_count": getattr(ti, "try_number", None),
        "name":           task.task_id if task else None,
        "run_id":         _task_instance_id(ti),
    }
    log.info("[get_current_task_meta] task_name=%s, run_id=%s, execution_count=%s",
             meta["name"], meta["run_id"], meta["execution_count"])
    return meta

def get_other_task_instance(task_id: str, context: Dict[str, Any]):
    """
    Retrieve the TaskInstance of another task in the same DAG run.

    Args:
        task_id: The ID of the other task
        context: The Airflow task context dictionary

    Returns:
        The TaskInstance object, or None if not found
    """
    dag_run: DagRun | None = context.get("dag_run")
    if dag_run:
        try:
            ti = dag_run.get_task_instance(task_id)
            log.info("[get_other_task_instance] task_id=%s → state=%s, run_id=%s",
                     task_id, getattr(ti, "state", None), getattr(ti, "run_id", None))
            return ti
        except Exception as e:
            log.debug("[get_other_task_instance] task_id=%s → %s", task_id, e)
            return None
    log.warning("[get_other_task_instance] dag_run is None, cannot look up task_id=%s", task_id)
    return None


def _pull_xcom_scalar(ti, task_id: str, key: str):
    """xcom_pull a str/int published by another task; None if missing, mapped (non-scalar) or unreadable."""
    try:
        val = ti.xcom_pull(task_ids=task_id, key=key)
    except Exception as e:
        log.warning("[get_other_task_info] xcom_pull task_id=%s key=%s → %s", task_id, key, e)
        return None
    return val if isinstance(val, (str, int)) else None


def _other_task_state(ti, task_id: str) -> Optional[str]:
    """State of another task in the same dag run, via the Airflow 3 task SDK."""
    try:
        states = ti.get_task_states(dag_id=ti.dag_id, task_ids=[task_id], run_ids=[ti.run_id])
    except Exception as e:
        log.warning("[get_other_task_info] get_task_states task_id=%s → %s", task_id, e)
        return None
    state = (states or {}).get(ti.run_id, {}).get(task_id)
    return str(getattr(state, "value", state)) if state is not None else None


def get_other_task_info(task_id: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Facts about another task in the same dag run, for {{ tasks.<name>.result_state / run_id / execution_count }}.

    state comes from the task SDK; run_id (task instance id) and execution_count come from the XComs
    YeeduOperator publishes (TASK_RUN_ID_XCOM_KEY / TASK_EXECUTION_COUNT_XCOM_KEY), so they are only
    available for Yeedu job/notebook tasks. Falls back to the ORM task instance where one is reachable.
    """
    info = {"state": None, "run_id": None, "execution_count": None}
    ti = context.get("ti")
    if ti is not None:
        info["state"] = _other_task_state(ti, task_id)
        info["run_id"] = _pull_xcom_scalar(ti, task_id, TASK_RUN_ID_XCOM_KEY)
        info["execution_count"] = _pull_xcom_scalar(ti, task_id, TASK_EXECUTION_COUNT_XCOM_KEY)

    if info["state"] is None and info["run_id"] is None and info["execution_count"] is None:
        other_ti = get_other_task_instance(task_id, context)
        if other_ti is not None:
            info["state"] = getattr(other_ti, "state", None)
            info["run_id"] = _task_instance_id(other_ti)
            info["execution_count"] = getattr(other_ti, "try_number", None)

    log.info("[get_other_task_info] task_id=%s → %s", task_id, info)
    return info


def get_workspace_meta(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract workspace metadata from Airflow context.
    Workspace ID is extracted from dag_id format: '{workspace_id}_{pipeline_id}'.

    Args:
        context: The Airflow task context dictionary

    Returns:
        Dictionary containing workspace metadata
    """
    dag = context.get("dag")
    workspace_id = None
    if dag:
        dag_id = dag.dag_id
        workspace_id = dag_id.split("_")[0] if "_" in dag_id else None

    meta = {"id": workspace_id, "url": _build_workspace_url(context, workspace_id)}
    log.info("[get_workspace_meta] workspace_id=%s url=%s", workspace_id, meta["url"])
    return meta


def _build_workspace_url(context: Dict[str, Any], workspace_id: Any) -> Optional[str]:
    """
    Build the workspace runs-page UI URL:
    {scheme}://{host}:5173/tenant/{tenant_id}/workspace/{workspace_id}/runs?pageNumber=1&limit=20

    Taken from the first YeeduOperator found (current task, then the rest of the DAG),
    since every task in a pipeline shares the same host and tenant.
    """
    candidates = []
    task = context.get("task")
    if task is not None:
        candidates.append(task)
    dag = context.get("dag")
    if dag is not None:
        candidates.extend(getattr(dag, "task_dict", {}).values())

    for t in candidates:
        base_url = getattr(t, "base_url", None)
        tenant_id = getattr(t, "tenant_id", None)
        port = getattr(t, "restapi_port", None)
        if not (base_url and tenant_id and port):
            continue
        ui_base = base_url.replace(f":{port}/api/v1/", ":5173").rstrip("/")
        ws_id = workspace_id or getattr(t, "workspace_id", None)
        return f"{ui_base}/tenant/{tenant_id}/workspace/{ws_id}/runs?pageNumber=1&limit=20"
    return None

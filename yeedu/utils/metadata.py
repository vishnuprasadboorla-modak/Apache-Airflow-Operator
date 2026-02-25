"""Metadata extraction utilities for Airflow context."""

from __future__ import annotations

import logging
from typing import Any, Dict

from airflow.models.dagrun import DagRun

log = logging.getLogger(__name__)


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
        "iso_datetime": dt.isoformat(),
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

    # start_time = dag_run.start_date  (actual wall-clock time execution began)
    # trigger.time = dag_run.logical_date  (scheduled/logical time the run represents)
    start_date = getattr(dag_run, "start_date", None)
    logical_date = getattr(dag_run, "logical_date", None)

    meta = {
        "id": _extract_job_id(dag_id),
        "name": dag_id,
        "run_id": getattr(dag_run, "run_id", None),
        "start_time": _extract_time_fields(start_date),
        "trigger": {
            "time": _extract_time_fields(logical_date),
            "type": _extract_trigger_type(dag_run),
        },
    }
    log.info("[get_job_meta] dag_id=%s, job_id=%s, run_id=%s, trigger_type=%s",
             dag_id, meta["id"], meta["run_id"], meta["trigger"]["type"])
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
        "run_id":         getattr(ti, "run_id", None),
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
            log.error("[get_other_task_instance] task_id=%s → ERROR: %s", task_id, e)
            return None
    log.warning("[get_other_task_instance] dag_run is None, cannot look up task_id=%s", task_id)
    return None


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

    meta = {"id": workspace_id, "url": None}
    log.info("[get_workspace_meta] workspace_id=%s", workspace_id)
    return meta

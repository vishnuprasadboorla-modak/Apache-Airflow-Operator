"""Metadata extraction utilities for Airflow context."""

from __future__ import annotations

from typing import Any, Dict


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

    logical_date = getattr(dag_run, "logical_date", None) or getattr(
        dag_run, "start_date", None)
    start = logical_date

    def _bool_weekday(dt):
        return bool(dt and dt.isoweekday() in (1, 2, 3, 4, 5))

    meta = {
        "id": None,
        "name": dag.dag_id if dag else None,
        "run_id": getattr(dag_run, "run_id", None),
        "start_time": {
            "day":          start.day if start else None,
            "hour":         start.hour if start else None,
            "is_weekday":   _bool_weekday(start),
            "iso_date":     start.date().isoformat() if start else None,
            "iso_datetime": start.isoformat() if start else None,
            "iso_weekday":  start.isoweekday() if start else None,
            "minute":       start.minute if start else None,
            "month":        start.month if start else None,
            "second":       start.second if start else None,
        },
        "trigger": {
            "time": {
                "timestamp_ms": None,
                "year": None,
            },
            "type": None,
        },
    }
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
    return {
        "execution_count": getattr(ti, "try_number", None),
        "name":           task.task_id if task else None,
        "run_id":         None,
    }


def get_other_task_meta(task_id: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract metadata for another task from Airflow context.

    Args:
        task_id: The ID of the other task
        context: The Airflow task context dictionary

    Returns:
        Dictionary containing other task metadata
    """
    ti = context.get("ti")
    meta = {"values": {}}
    if ti:
        meta["values"] = {}
    return meta


def get_workspace_meta(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract workspace metadata from Airflow context.

    Args:
        context: The Airflow task context dictionary

    Returns:
        Dictionary containing workspace metadata
    """
    return {"id": None, "url": None}

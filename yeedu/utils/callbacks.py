"""Callback factory functions for DAG and task events."""

from __future__ import annotations

import logging
from typing import List, Callable, Dict, Any

from yeedu.hooks.email_notification import EmailNotificationHook

log = logging.getLogger(__name__)


def dag_success_cb_factory(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
    """
    Factory function to create a DAG success callback.

    Args:
        recipients: List of email addresses to notify

    Returns:
        Callback function for DAG success events
    """
    def _cb(context: Dict[str, Any]) -> None:
        if recipients:
            try:
                EmailNotificationHook().notify_dag(
                    recipients=recipients,
                    dag_id=context["dag"].dag_id,
                    run_id=context["run_id"],
                    status="success",
                    context=context,
                )
            except Exception as e:
                log.error("Email notification failed for DAG success callback: %s", e, exc_info=True)
    return _cb


def dag_failure_cb_factory(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
    """
    Factory function to create a DAG failure callback.

    Args:
        recipients: List of email addresses to notify

    Returns:
        Callback function for DAG failure events
    """
    def _cb(context: Dict[str, Any]) -> None:
        if recipients:
            try:
                EmailNotificationHook().notify_dag(
                    recipients=recipients,
                    dag_id=context["dag"].dag_id,
                    run_id=context["run_id"],
                    status="failed",
                    context=context,
                )
            except Exception as e:
                log.error("Email notification failed for DAG failure callback: %s", e, exc_info=True)
    return _cb


def dag_start_cb_factory(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
    """
    Factory function to create a DAG start callback.

    Args:
        recipients: List of email addresses to notify

    Returns:
        Callback function for DAG start events
    """
    def _cb(context: Dict[str, Any]) -> None:
        if recipients:
            try:
                EmailNotificationHook().notify_dag(
                    recipients=recipients,
                    dag_id=context["dag"].dag_id,
                    run_id=context["run_id"],
                    status="started",
                    context=context,
                )
            except Exception as e:
                log.error("Email notification failed for DAG start callback: %s", e, exc_info=True)
    return _cb


def task_success_cb_factory(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
    """
    Factory function to create a task success callback.

    Args:
        recipients: List of email addresses to notify

    Returns:
        Callback function for task success events
    """
    def _cb(context: Dict[str, Any]) -> None:
        if recipients:
            try:
                EmailNotificationHook().notify_task(
                    recipients=recipients,
                    task_id=context["task_instance"].task_id,
                    run_id=context["run_id"],
                    status="success",
                    context=context,
                )
            except Exception as e:
                log.error("Email notification failed for task success callback: %s", e, exc_info=True)
    return _cb


def task_failure_cb_factory(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
    """
    Factory function to create a task failure callback.

    Args:
        recipients: List of email addresses to notify

    Returns:
        Callback function for task failure events
    """
    def _cb(context: Dict[str, Any]) -> None:
        if recipients:
            try:
                EmailNotificationHook().notify_task(
                    recipients=recipients,
                    task_id=context["task_instance"].task_id,
                    run_id=context["run_id"],
                    status="failed",
                    context=context,
                )
            except Exception as e:
                log.error("Email notification failed for task failure callback: %s", e, exc_info=True)
    return _cb


def task_start_cb_factory(recipients: List[str]) -> Callable[[Dict[str, Any]], None]:
    """
    Factory function to create a task start callback.

    Args:
        recipients: List of email addresses to notify

    Returns:
        Callback function for task start events
    """
    def _cb(context: Dict[str, Any]) -> None:
        if recipients:
            try:
                EmailNotificationHook().notify_task(
                    recipients=recipients,
                    task_id=context["task_instance"].task_id,
                    run_id=context["run_id"],
                    status="started",
                    context=context,
                )
            except Exception as e:
                log.error("Email notification failed for task start callback: %s", e, exc_info=True)
    return _cb

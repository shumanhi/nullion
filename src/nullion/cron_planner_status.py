"""Planner-card previews for cron runs.

Cron execution still needs one reliable agent turn to produce the deliverable.
This module builds the optional task-card surface from the structured planner
without dispatching background mini-agents.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nullion.task_decomposer import TaskDecomposer
from nullion.task_queue import TaskGroup, TaskStatus, make_group_id
from nullion.task_status_format import format_task_status_line, format_task_status_summary


@dataclass(frozen=True, slots=True)
class CronPlannerStatusPreview:
    group: TaskGroup
    planner_summary: str
    subject: str = ""

    @property
    def group_id(self) -> str:
        return self.group.group_id

    @property
    def allowed_tools(self) -> tuple[str, ...]:
        names: set[str] = set()
        for task in self.group.tasks:
            for tool_name in task.allowed_tools or ():
                normalized = str(tool_name or "").strip()
                if normalized:
                    names.add(normalized)
        return tuple(sorted(names))

    def initial_text(self) -> str:
        status_lines = {
            task.task_id: format_task_status_line(
                task,
                status=TaskStatus.RUNNING if not task.dependencies else TaskStatus.PENDING,
            )
            for task in self.group.tasks
        }
        return format_task_status_summary(
            self.group.tasks,
            planner_summary=self.planner_summary,
            subject=self.subject,
            status_lines=status_lines,
            default_status=TaskStatus.PENDING,
        )

    def terminal_text(self, *, success: bool) -> str:
        terminal_status = TaskStatus.COMPLETE if success else TaskStatus.FAILED
        status_lines = {
            task.task_id: format_task_status_line(task, status=terminal_status)
            for task in self.group.tasks
        }
        return format_task_status_summary(
            self.group.tasks,
            planner_summary=self.planner_summary,
            subject=self.subject,
            status_lines=status_lines,
            default_status=terminal_status,
        )


def build_cron_planner_status_preview(
    *,
    model_client: Any,
    user_message: str,
    conversation_id: str,
    principal_id: str,
    tool_registry: Any,
    subject: str = "",
) -> CronPlannerStatusPreview | None:
    """Return a display-only planner card from a validated structured DAG."""
    if model_client is None:
        return None
    tools = [
        str(tool.get("name", ""))
        for tool in (tool_registry.list_tool_definitions() if tool_registry is not None else ())
        if isinstance(tool, dict) and tool.get("name")
    ]
    decomposer = TaskDecomposer(model_client=model_client)
    dag_plan = decomposer.plan_dag(user_message, available_tools=tools)
    if not dag_plan.can_dispatch:
        return None
    group = decomposer.decompose(
        user_message,
        group_id=make_group_id(),
        conversation_id=conversation_id,
        principal_id=principal_id,
        available_tools=tools,
        dag_plan=dag_plan,
    )
    return CronPlannerStatusPreview(
        group=group,
        planner_summary=_planner_summary_from_disposition(
            disposition=dag_plan.disposition,
            task_count=len(group.tasks),
        ),
        subject=subject,
    )


def cron_planner_run_succeeded(result: dict[str, object]) -> bool:
    if result.get("cron_delivery_failed") or result.get("cron_run_failed"):
        return False
    if result.get("reached_iteration_limit") or result.get("raw_tool_payload_blocked"):
        return False
    if result.get("suspended_for_approval"):
        return False
    status = str(result.get("cron_delivery_status") or "").strip()
    return status not in {"failed", "paused_for_approval"}


def _planner_summary_from_disposition(*, disposition: str, task_count: int) -> str:
    label = str(disposition or "").replace("_", " ").title() or "Mission"
    if task_count:
        return f"{label} * {task_count} task{'s' if task_count != 1 else ''}"
    return label


__all__ = [
    "CronPlannerStatusPreview",
    "build_cron_planner_status_preview",
    "cron_planner_run_succeeded",
]

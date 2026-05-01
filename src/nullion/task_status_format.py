"""Shared task-list rendering for mini-agent progress surfaces."""

from __future__ import annotations

from typing import Iterable

from nullion.task_queue import TaskRecord, TaskStatus

TASK_STATUS_PENDING = "☐"
TASK_STATUS_RUNNING = "◐"
TASK_STATUS_COMPLETE = "☑"
TASK_STATUS_FAILED = "✕"
TASK_STATUS_CANCELLED = "⊘"
TASK_STATUS_BLOCKED = "▣"
TASK_STATUS_WAITING_INPUT = "▤"
TASK_STATUS_SUBLIST_PREFIX = ""


def task_status_glyph(status: TaskStatus | str | None) -> str:
    normalized = status.value if isinstance(status, TaskStatus) else str(status or "").strip().lower()
    if normalized == TaskStatus.QUEUED.value:
        return TASK_STATUS_RUNNING
    if normalized == TaskStatus.RUNNING.value:
        return TASK_STATUS_RUNNING
    if normalized == TaskStatus.COMPLETE.value:
        return TASK_STATUS_COMPLETE
    if normalized == TaskStatus.FAILED.value:
        return TASK_STATUS_FAILED
    if normalized == TaskStatus.CANCELLED.value:
        return TASK_STATUS_CANCELLED
    if normalized == TaskStatus.BLOCKED.value:
        return TASK_STATUS_BLOCKED
    if normalized == TaskStatus.WAITING_INPUT.value:
        return TASK_STATUS_WAITING_INPUT
    return TASK_STATUS_PENDING


def format_task_status_line(
    task: TaskRecord,
    *,
    status: TaskStatus | str | None = None,
    detail: str | None = None,
) -> str:
    line = f"{task_status_glyph(task.status if status is None else status)} {task.title}"
    if detail:
        line = f"{line}: {detail}"
    return line


def format_task_status_summary(
    tasks: Iterable[TaskRecord],
    *,
    planner_summary: str = "",
    status_lines: dict[str, str] | None = None,
    default_status: TaskStatus | str | None = None,
) -> str:
    task_list = list(tasks)
    lines: list[str] = []
    if planner_summary:
        lines.append(f"Planner: {planner_summary}")
    count_label = f"{len(task_list)} task{'s' if len(task_list) != 1 else ''}"
    if planner_summary.lower().startswith("parallel mission"):
        lines.append(f"→ Running {count_label} in parallel:")
    else:
        lines.append(f"→ Working on {count_label}:")
    known_lines = status_lines or {}
    for task in task_list:
        fallback = format_task_status_line(
            task,
            status=default_status if default_status is not None else None,
        )
        lines.append(format_task_status_sublist_line(known_lines.get(task.task_id, fallback)))
    return "\n".join(lines)


def format_task_status_sublist_line(line: str) -> str:
    stripped = str(line or "").strip()
    return f"  {stripped}" if stripped else "  "


def format_task_status_activity_detail(
    tasks: Iterable[TaskRecord],
    *,
    status_lines: dict[str, str] | None = None,
) -> str:
    known_lines = status_lines or {}
    return "\n".join(
        format_task_status_sublist_line(known_lines.get(task.task_id, format_task_status_line(task)))
        for task in tasks
    )


__all__ = [
    "TASK_STATUS_CANCELLED",
    "TASK_STATUS_BLOCKED",
    "TASK_STATUS_COMPLETE",
    "TASK_STATUS_FAILED",
    "TASK_STATUS_PENDING",
    "TASK_STATUS_RUNNING",
    "TASK_STATUS_SUBLIST_PREFIX",
    "TASK_STATUS_WAITING_INPUT",
    "format_task_status_activity_detail",
    "format_task_status_line",
    "format_task_status_summary",
    "format_task_status_sublist_line",
    "task_status_glyph",
]

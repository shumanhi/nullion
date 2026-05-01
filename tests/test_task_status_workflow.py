from __future__ import annotations

from datetime import UTC, datetime

from nullion.task_queue import TaskPriority, TaskRecord, TaskStatus
from nullion.task_status_format import (
    TASK_STATUS_BLOCKED,
    TASK_STATUS_COMPLETE,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    format_task_status_activity_detail,
    format_task_status_line,
    format_task_status_summary,
    task_status_glyph,
)


def _task(task_id: str, title: str, status: TaskStatus) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title=title,
        description=title,
        status=status,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
        created_at=datetime.now(UTC),
    )


def test_task_status_summary_puts_arrow_only_on_header_not_each_subtask() -> None:
    tasks = [
        _task("weather", "Fetch NYC weather", TaskStatus.PENDING),
        _task("calendar", "Fetch ACME calendar", TaskStatus.PENDING),
        _task("inbox", "Fetch urgent inbox", TaskStatus.PENDING),
        _task("brief", "Compose morning brief", TaskStatus.BLOCKED),
    ]

    assert format_task_status_summary(tasks) == (
        "→ Working on 4 tasks:\n"
        "  ☐ Fetch NYC weather\n"
        "  ☐ Fetch ACME calendar\n"
        "  ☐ Fetch urgent inbox\n"
        "  ▣ Compose morning brief"
    )


def test_task_status_summary_can_render_initial_checklist_as_all_pending() -> None:
    tasks = [
        _task("find", "Find OpenClaw config files", TaskStatus.QUEUED),
        _task("audit", "Audit OpenClaw configuration", TaskStatus.BLOCKED),
    ]

    assert format_task_status_summary(
        tasks,
        planner_summary="Sequential Mission • 2 tasks",
        default_status=TaskStatus.PENDING,
    ) == (
        "Planner: Sequential Mission • 2 tasks\n"
        "→ Working on 2 tasks:\n"
        "  ☐ Find OpenClaw config files\n"
        "  ☐ Audit OpenClaw configuration"
    )


def test_task_status_glyphs_cover_lifecycle_states() -> None:
    assert task_status_glyph(TaskStatus.PENDING) == TASK_STATUS_PENDING
    assert task_status_glyph(TaskStatus.RUNNING) == TASK_STATUS_RUNNING
    assert task_status_glyph(TaskStatus.COMPLETE) == TASK_STATUS_COMPLETE
    assert task_status_glyph(TaskStatus.FAILED) == TASK_STATUS_FAILED
    assert task_status_glyph(TaskStatus.BLOCKED) == TASK_STATUS_BLOCKED


def test_task_status_line_and_activity_detail_preserve_custom_status_lines() -> None:
    task = _task("brief", "Compose morning brief", TaskStatus.RUNNING)

    assert format_task_status_line(task, detail="drafting") == "◐ Compose morning brief: drafting"
    assert format_task_status_activity_detail([task], status_lines={"brief": "☑ Compose morning brief"}) == (
        "  ☑ Compose morning brief"
    )


def test_queued_task_status_renders_as_running() -> None:
    task = TaskRecord(
        task_id="brief",
        group_id="grp",
        conversation_id="conv",
        principal_id="principal",
        title="Compose morning brief",
        description="Write the brief",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )

    assert format_task_status_line(task) == "◐ Compose morning brief"

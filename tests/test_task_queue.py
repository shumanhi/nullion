from datetime import datetime, timezone

import pytest

from nullion.task_queue import TaskGroup, TaskPriority, TaskRecord, TaskRegistry, TaskStatus


def _task(task_id: str, status: TaskStatus, dependencies: list[str] | None = None) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        group_id="grp-1",
        conversation_id="telegram:1",
        principal_id="telegram_chat",
        title=task_id,
        description=task_id,
        status=status,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=dependencies or [],
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_ready_tasks_for_group_requires_completed_dependencies():
    registry = TaskRegistry()
    producer = _task("producer", TaskStatus.FAILED)
    consumer = _task("consumer", TaskStatus.BLOCKED, dependencies=["producer"])
    await registry.add_group(
        TaskGroup(
            group_id="grp-1",
            conversation_id="telegram:1",
            original_message="do producer then consumer",
            tasks=[producer, consumer],
        )
    )

    assert registry.ready_tasks_for_group("grp-1") == []

    await registry.update_task("producer", status=TaskStatus.COMPLETE)

    assert [task.task_id for task in registry.ready_tasks_for_group("grp-1")] == ["consumer"]

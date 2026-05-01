"""Lightweight scheduler primitives for recurring automation."""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from uuid import uuid4


class ScheduleKind(str, Enum):
    ONCE = "once"
    RECURRING = "recurring"


@dataclass(slots=True)
class ScheduledTask:
    task_id: str
    capsule_id: str
    schedule_kind: ScheduleKind
    interval_minutes: int
    enabled: bool
    last_run_at: datetime | None
    failure_count: int



def create_recurring_task(capsule_id: str, interval_minutes: int) -> ScheduledTask:
    return ScheduledTask(
        task_id=uuid4().hex,
        capsule_id=capsule_id,
        schedule_kind=ScheduleKind.RECURRING,
        interval_minutes=interval_minutes,
        enabled=True,
        last_run_at=None,
        failure_count=0,
    )



def should_run(
    last_run_at: datetime | None,
    now: datetime,
    interval_minutes: int,
) -> bool:
    if last_run_at is None:
        return True

    return (now - last_run_at) >= timedelta(minutes=interval_minutes)



def mark_task_ran(task: ScheduledTask, now: datetime) -> ScheduledTask:
    return replace(task, last_run_at=now, failure_count=0)



def mark_task_failed(task: ScheduledTask) -> ScheduledTask:
    return replace(task, failure_count=task.failure_count + 1)



def disable_task(task: ScheduledTask) -> ScheduledTask:
    return replace(task, enabled=False)

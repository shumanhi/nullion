"""Task queue — data model and in-memory registry for Phase 5 parallel execution.

TaskRecord is the unit of work dispatched to a mini-agent. A group of
TaskRecords that came from the same user message share a group_id and may
declare dependencies on each other, forming a DAG.

TaskRegistry is the in-memory store for all active records. It is the single
source of truth for task state during a conversation; records are also
persisted to RuntimeStore for crash recovery.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Literal
from uuid import uuid4

from nullion.mini_agent_config import mini_agent_timeout_seconds

logger = logging.getLogger(__name__)


# ── Enums ──────────────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING        = "pending"         # created, not yet runnable
    BLOCKED        = "blocked"         # waiting for a dependency
    QUEUED         = "queued"          # runnable, waiting for pool slot
    RUNNING        = "running"         # agent actively executing
    WAITING_INPUT  = "waiting_input"   # paused, needs user response
    COMPLETE       = "complete"
    FAILED         = "failed"
    CANCELLED      = "cancelled"


class TaskPriority(str, Enum):
    URGENT = "urgent"   # user said "asap", "urgent", "now"
    HIGH   = "high"
    NORMAL = "normal"   # default
    LOW    = "low"      # background / speculative


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class TaskResult:
    task_id: str
    status: Literal["success", "failure", "partial"]
    output: str | None = None
    artifacts: list[str] = field(default_factory=list)   # file paths produced
    error: str | None = None
    context_out: object = None                            # published to context bus


@dataclass
class TaskRecord:
    task_id: str
    group_id: str
    conversation_id: str
    principal_id: str
    title: str                         # ≤ 50 chars, shown in status
    description: str                   # full goal sent to mini-agent
    status: TaskStatus
    priority: TaskPriority
    allowed_tools: list[str]           # tool scope for the assigned agent
    dependencies: list[str]            # task_ids that must complete first
    context_key_in: str | None = None  # context bus key to read before starting
    context_key_out: str | None = None # context bus key to write on completion
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    agent_id: str | None = None
    result: TaskResult | None = None
    retry_count: int = 0
    max_retries: int = 2
    timeout_s: float = field(default_factory=mini_agent_timeout_seconds)
    deep_agent_skills: list[str] = field(default_factory=list)
    deep_agent_subagents: list[dict[str, str]] = field(default_factory=list)

    def is_terminal(self) -> bool:
        return self.status in {TaskStatus.COMPLETE, TaskStatus.FAILED, TaskStatus.CANCELLED}

    def is_runnable(self) -> bool:
        return self.status in {TaskStatus.QUEUED, TaskStatus.PENDING}


@dataclass
class TaskGroup:
    group_id: str
    conversation_id: str
    original_message: str
    tasks: list[TaskRecord] = field(default_factory=list)
    planner_metadata: dict[str, object] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def all_terminal(self) -> bool:
        return all(t.is_terminal() for t in self.tasks)

    def any_failed(self) -> bool:
        return any(t.status == TaskStatus.FAILED for t in self.tasks)


# ── Registry ───────────────────────────────────────────────────────────────────

class TaskRegistry:
    """In-memory store for active TaskRecords and TaskGroups.

    All mutation methods are guarded by an asyncio.Lock so they are safe to
    call from concurrent coroutines.
    """

    def __init__(self) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._groups: dict[str, TaskGroup] = {}
        self._lock = asyncio.Lock()

    # ── Write ──────────────────────────────────────────────────────────────

    async def add_task(self, task: TaskRecord) -> None:
        async with self._lock:
            self._tasks[task.task_id] = task
            group = self._groups.get(task.group_id)
            if group is not None and task not in group.tasks:
                group.tasks.append(task)

    async def add_group(self, group: TaskGroup) -> None:
        async with self._lock:
            self._groups[group.group_id] = group
            for task in group.tasks:
                self._tasks[task.task_id] = task

    async def update_task(self, task_id: str, **kwargs) -> TaskRecord | None:
        """Update fields on an existing task. Returns the updated record."""
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            valid_kwargs = {k: v for k, v in kwargs.items() if hasattr(task, k)}
            updated = replace(task, **valid_kwargs)
            self._tasks[task_id] = updated
            group = self._groups.get(updated.group_id)
            if group is not None:
                group.tasks = [
                    updated if existing.task_id == task_id else existing
                    for existing in group.tasks
                ]
            return updated

    # ── Read ───────────────────────────────────────────────────────────────

    def get_task(self, task_id: str) -> TaskRecord | None:
        return self._tasks.get(task_id)

    def get_group(self, group_id: str) -> TaskGroup | None:
        return self._groups.get(group_id)

    def list_by_group(self, group_id: str) -> list[TaskRecord]:
        group = self._groups.get(group_id)
        if group is not None:
            return list(group.tasks)
        return [task for task in self._tasks.values() if task.group_id == group_id]

    def list_by_status(self, *statuses: TaskStatus) -> list[TaskRecord]:
        status_set = set(statuses)
        snapshot = list(self._tasks.values())
        return [t for t in snapshot if t.status in status_set]

    def list_by_conversation(self, conversation_id: str) -> list[TaskRecord]:
        return [t for t in self._tasks.values() if t.conversation_id == conversation_id]

    def ready_tasks_for_group(self, group_id: str) -> list[TaskRecord]:
        """Return tasks in *group_id* whose dependencies are all complete.

        A missing dependency (not found in the group) is treated as *blocking*
        rather than complete.  Fabricating a phantom COMPLETE record would
        allow tasks to run before their real dependency finishes or even
        exists, which can cause data corruption or incorrect results.
        """
        group_tasks = {t.task_id: t for t in self.list_by_group(group_id)}
        ready = []
        for task in group_tasks.values():
            if task.status != TaskStatus.BLOCKED:
                continue
            deps_done = all(
                dep_id in group_tasks and group_tasks[dep_id].status == TaskStatus.COMPLETE
                for dep_id in task.dependencies
            )
            if deps_done:
                ready.append(task)
        return ready

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a single task. Returns True if the task was found and cancellable."""
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False
            if task.is_terminal():
                return False
            task.status = TaskStatus.CANCELLED
            task.completed_at = datetime.now(timezone.utc)
            logger.debug("TaskRegistry: cancelled task %s", task_id)
            return True

    async def cancel_group(self, group_id: str) -> int:
        """Cancel all non-terminal tasks in *group_id*. Returns count cancelled."""
        count = 0
        for task in self.list_by_group(group_id):
            if not task.is_terminal():
                ok = await self.cancel_task(task.task_id)
                if ok:
                    count += 1
        return count

    async def purge_group(self, group_id: str) -> None:
        """Remove all records for *group_id* once it is fully terminal."""
        async with self._lock:
            group = self._groups.pop(group_id, None)
            if group is None:
                return
            for task in group.tasks:
                self._tasks.pop(task.task_id, None)


# ── Factory helpers ────────────────────────────────────────────────────────────

def make_task_id() -> str:
    return f"task-{uuid4().hex[:12]}"


def make_group_id() -> str:
    return f"grp-{uuid4().hex[:12]}"


__all__ = [
    "TaskStatus",
    "TaskPriority",
    "TaskRecord",
    "TaskResult",
    "TaskGroup",
    "TaskRegistry",
    "make_task_id",
    "make_group_id",
]

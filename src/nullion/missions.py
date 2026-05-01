"""Mission-tracking primitives for Project Nullion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class MissionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    WAITING_USER = "waiting_user"
    BLOCKED = "blocked"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class MissionContinuationPolicy(str, Enum):
    MANUAL = "manual"
    AUTO_FINISH = "auto_finish"
    APPROVAL_GATED = "approval_gated"


class MissionTerminalReason(str, Enum):
    COMPLETED = "completed"
    EXECUTION_FAILED = "execution_failed"
    APPROVAL_DENIED = "approval_denied"
    USER_CANCELLED = "user_cancelled"


@dataclass(slots=True)
class MissionStep:
    step_id: str
    title: str
    status: str
    kind: str
    capsule_id: str | None = None
    mini_agent_run_id: str | None = None
    mini_agent_run_ids: tuple[str, ...] = ()
    required_mini_agent_run_ids: tuple[str, ...] = ()
    notes: str | None = None
    delay_seconds: float = 0.0          # pause before executing this step
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MissionChecklistItem:
    item_id: str
    label: str
    required: bool = True
    satisfied: bool = False
    details: str | None = None


@dataclass(slots=True)
class MissionRecord:
    mission_id: str
    owner: str
    title: str
    goal: str
    status: MissionStatus
    continuation_policy: MissionContinuationPolicy
    created_from_capsule_id: str | None = None
    active_capsule_id: str | None = None
    active_step_id: str | None = None
    steps: tuple[MissionStep, ...] = ()
    completion_checklist: tuple[MissionChecklistItem, ...] = ()
    blocked_reason: str | None = None
    waiting_on: str | None = None
    result_summary: str | None = None
    terminal_reason: MissionTerminalReason | None = None
    last_progress_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


__all__ = [
    "MissionChecklistItem",
    "MissionContinuationPolicy",
    "MissionRecord",
    "MissionStatus",
    "MissionStep",
    "MissionTerminalReason",
]

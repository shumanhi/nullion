"""Deterministic Mini-Agent run tracking primitives."""

from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from typing import Any


class MiniAgentRunStatus(str, Enum):
    """Supported Mini-Agent run lifecycle states."""

    PENDING = "pending"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class MiniAgentRun:
    """A single Mini-Agent run record."""

    run_id: str
    capsule_id: str
    mini_agent_type: str
    status: MiniAgentRunStatus
    created_at: datetime
    result_summary: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


_ALLOWED_TRANSITIONS: dict[MiniAgentRunStatus, set[MiniAgentRunStatus]] = {
    MiniAgentRunStatus.PENDING: {MiniAgentRunStatus.RUNNING, MiniAgentRunStatus.CANCELLED},
    MiniAgentRunStatus.RUNNING: {
        MiniAgentRunStatus.WAITING_INPUT,
        MiniAgentRunStatus.COMPLETED,
        MiniAgentRunStatus.FAILED,
        MiniAgentRunStatus.CANCELLED,
    },
    MiniAgentRunStatus.WAITING_INPUT: {
        MiniAgentRunStatus.RUNNING,
        MiniAgentRunStatus.COMPLETED,
        MiniAgentRunStatus.FAILED,
        MiniAgentRunStatus.CANCELLED,
    },
    MiniAgentRunStatus.COMPLETED: set(),
    MiniAgentRunStatus.FAILED: set(),
    MiniAgentRunStatus.CANCELLED: set(),
}


def create_mini_agent_run(
    *,
    run_id: str,
    capsule_id: str,
    mini_agent_type: str,
    created_at: datetime,
    metadata: dict[str, Any] | None = None,
) -> MiniAgentRun:
    """Create a run record in the pending state."""

    return MiniAgentRun(
        run_id=run_id,
        capsule_id=capsule_id,
        mini_agent_type=mini_agent_type,
        status=MiniAgentRunStatus.PENDING,
        created_at=created_at,
        result_summary=None,
        metadata=dict(metadata or {}),
    )


def transition_mini_agent_run_status(
    run: MiniAgentRun,
    new_status: MiniAgentRunStatus,
    *,
    result_summary: str | None = None,
) -> MiniAgentRun:
    """Return a new run record with a validated status transition."""

    if new_status is run.status:
        if result_summary is None or result_summary == run.result_summary:
            return run
        return replace(run, result_summary=result_summary)

    allowed_next = _ALLOWED_TRANSITIONS[run.status]
    if new_status not in allowed_next:
        raise ValueError(
            f"Invalid status transition: {run.status.value} -> {new_status.value}"
        )

    next_summary = run.result_summary if result_summary is None else result_summary
    return replace(run, status=new_status, result_summary=next_summary)


__all__ = [
    "MiniAgentRun",
    "MiniAgentRunStatus",
    "create_mini_agent_run",
    "transition_mini_agent_run_status",
]

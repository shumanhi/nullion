"""Deterministic Mini-Agent run tracking primitives."""

from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum


class MiniAgentRunStatus(str, Enum):
    """Supported Mini-Agent run lifecycle states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(slots=True)
class MiniAgentRun:
    """A single Mini-Agent run record."""

    run_id: str
    capsule_id: str
    mini_agent_type: str
    status: MiniAgentRunStatus
    created_at: datetime
    result_summary: str | None = None


_ALLOWED_TRANSITIONS: dict[MiniAgentRunStatus, set[MiniAgentRunStatus]] = {
    MiniAgentRunStatus.PENDING: {MiniAgentRunStatus.RUNNING},
    MiniAgentRunStatus.RUNNING: {MiniAgentRunStatus.COMPLETED, MiniAgentRunStatus.FAILED},
    MiniAgentRunStatus.COMPLETED: set(),
    MiniAgentRunStatus.FAILED: set(),
}


def create_mini_agent_run(
    *,
    run_id: str,
    capsule_id: str,
    mini_agent_type: str,
    created_at: datetime,
) -> MiniAgentRun:
    """Create a run record in the pending state."""

    return MiniAgentRun(
        run_id=run_id,
        capsule_id=capsule_id,
        mini_agent_type=mini_agent_type,
        status=MiniAgentRunStatus.PENDING,
        created_at=created_at,
        result_summary=None,
    )


def transition_mini_agent_run_status(
    run: MiniAgentRun,
    new_status: MiniAgentRunStatus,
    *,
    result_summary: str | None = None,
) -> MiniAgentRun:
    """Return a new run record with a validated status transition."""

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

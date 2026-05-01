"""Intent Capsule primitives for interrupt-driven sparse cognition."""

from dataclasses import dataclass, replace
from enum import Enum
from uuid import uuid4


class IntentState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(slots=True)
class IntentCapsule:
    capsule_id: str
    owner: str
    goal: str
    state: IntentState
    risk_level: str
    active_mini_agents: list[str]
    pending_approval_id: str | None
    success_criteria: list[str]


def create_intent_capsule(
    owner: str,
    goal: str,
    risk_level: str = "normal",
    success_criteria: list[str] | None = None,
) -> IntentCapsule:
    return IntentCapsule(
        capsule_id=str(uuid4()),
        owner=owner,
        goal=goal,
        state=IntentState.PENDING,
        risk_level=risk_level,
        active_mini_agents=[],
        pending_approval_id=None,
        success_criteria=list(success_criteria or []),
    )


def with_state(
    capsule: IntentCapsule,
    state: IntentState,
    pending_approval_id: str | None = None,
) -> IntentCapsule:
    return replace(capsule, state=state, pending_approval_id=pending_approval_id)


def add_mini_agent(capsule: IntentCapsule, mini_agent_name: str) -> IntentCapsule:
    return replace(
        capsule,
        active_mini_agents=[*capsule.active_mini_agents, mini_agent_name],
    )


def remove_mini_agent(capsule: IntentCapsule, mini_agent_name: str) -> IntentCapsule:
    active_mini_agents = list(capsule.active_mini_agents)
    try:
        active_mini_agents.remove(mini_agent_name)
    except ValueError:
        pass
    return replace(capsule, active_mini_agents=active_mini_agents)


__all__ = [
    "IntentState",
    "IntentCapsule",
    "create_intent_capsule",
    "with_state",
    "add_mini_agent",
    "remove_mini_agent",
]

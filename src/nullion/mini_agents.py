"""Deterministic Mini-Agent launch policy for Project Nullion."""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class MiniAgentLaunchDecision(str, Enum):
    LAUNCH = "launch"
    HOLD = "hold"
    DENY = "deny"


@dataclass(slots=True)
class MiniAgentLaunchPlan:
    decision: MiniAgentLaunchDecision
    reason: str
    mini_agent_type: str | None


def _value_from_route(policy_route: Any, key: str) -> Any:
    if policy_route is None:
        return None
    if isinstance(policy_route, dict):
        return policy_route.get(key)
    return getattr(policy_route, key, None)


def _route_is_explicitly_safe(policy_route: Any) -> bool:
    safe = _value_from_route(policy_route, "safe")
    if safe is None:
        safe = _value_from_route(policy_route, "is_safe")
    return safe is True


def _route_target(policy_route: Any) -> str | None:
    target = _value_from_route(policy_route, "target")
    if target is None:
        return None
    target_value = getattr(target, "value", target)
    if isinstance(target_value, str):
        return target_value.lower()
    return str(target_value).lower()


def _route_reason(policy_route: Any) -> str:
    reason = _value_from_route(policy_route, "reason")
    if reason is None:
        return ""
    return str(reason).strip()


def _route_mini_agent_type(policy_route: Any) -> str:
    agent_type = _value_from_route(policy_route, "mini_agent_type")
    if agent_type is None:
        agent_type = _value_from_route(policy_route, "agent_type")
    text = str(agent_type or "").strip()
    return text or "general"


def _state_value(intent_capsule: Any) -> str:
    state = getattr(intent_capsule, "state", None)
    state_value = getattr(state, "value", state)
    return "" if state_value is None else str(state_value)


def decide_mini_agent_launch(intent_capsule: Any, policy_route: Any = None) -> MiniAgentLaunchPlan:
    """Decide whether to launch a Mini-Agent using deterministic default-deny policy."""
    state = _state_value(intent_capsule)

    if state == "waiting_approval":
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.HOLD,
            reason="intent_state=waiting_approval;decision=hold",
            mini_agent_type=None,
        )

    if state == "blocked":
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.HOLD,
            reason="intent_state=blocked;decision=hold",
            mini_agent_type=None,
        )

    if state == "completed":
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.DENY,
            reason="intent_state=completed;decision=deny",
            mini_agent_type=None,
        )

    if state == "failed":
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.DENY,
            reason="intent_state=failed;decision=deny",
            mini_agent_type=None,
        )

    risk_level = str(getattr(intent_capsule, "risk_level", "")).lower()
    route_safe = _value_from_route(policy_route, "safe")
    if route_safe is False:
        reason = _route_reason(policy_route)
        detail = f";policy_reason={reason}" if reason else ""
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.DENY,
            reason=f"policy_route=safe_false{detail};decision=deny",
            mini_agent_type=None,
        )

    if risk_level == "high" and not _route_is_explicitly_safe(policy_route):
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.HOLD,
            reason="risk_level=high;policy_route=not_explicitly_safe;decision=hold",
            mini_agent_type=None,
        )

    if _route_target(policy_route) == "sentinel":
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.HOLD,
            reason="policy_route_target=sentinel;decision=hold",
            mini_agent_type=None,
        )

    if state in {"pending", "running"} and (risk_level != "high" or _route_is_explicitly_safe(policy_route)):
        route_note = ";policy_route=safe_true" if _route_is_explicitly_safe(policy_route) else ""
        return MiniAgentLaunchPlan(
            decision=MiniAgentLaunchDecision.LAUNCH,
            reason=f"intent_state={state};risk_level={risk_level or 'normal'}{route_note};decision=launch",
            mini_agent_type=_route_mini_agent_type(policy_route),
        )


    return MiniAgentLaunchPlan(
        decision=MiniAgentLaunchDecision.DENY,
        reason=f"intent_state={state or 'unknown'};decision=deny",
        mini_agent_type=None,
    )


__all__ = [
    "MiniAgentLaunchDecision",
    "MiniAgentLaunchPlan",
    "decide_mini_agent_launch",
]

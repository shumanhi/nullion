"""Typed helpers for agent turn budget continuation approvals."""

from __future__ import annotations

import os
from typing import Any


AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND = "agent_turn_limit_extension"
AGENT_TURN_LIMIT_EXTENSION_ACTION = "extend_agent_turn_limit"
AGENT_TURN_LIMIT_EXTENSION_TOOL_NAME = "doctor_extend_agent_turn_limit"
AGENT_TURN_LIMIT_EXTENSION_MULTIPLIERS = (2, 5, 10)
DEFAULT_AGENT_TURN_MAX_ITERATIONS = 48


def default_agent_turn_max_iterations() -> int:
    raw_value = os.environ.get("NULLION_AGENT_TURN_MAX_ITERATIONS", str(DEFAULT_AGENT_TURN_MAX_ITERATIONS)).strip()
    try:
        limit = int(raw_value)
    except ValueError:
        return DEFAULT_AGENT_TURN_MAX_ITERATIONS
    return min(96, max(1, limit))


def limit_extension_mode_for_multiplier(multiplier: int) -> str:
    value = int(multiplier)
    if value not in AGENT_TURN_LIMIT_EXTENSION_MULTIPLIERS:
        raise ValueError(f"unsupported limit extension multiplier: {multiplier}")
    return f"limit_{value}x"


def multiplier_for_limit_extension_mode(mode: object) -> int | None:
    normalized = str(mode or "").strip().lower()
    for multiplier in AGENT_TURN_LIMIT_EXTENSION_MULTIPLIERS:
        if normalized == limit_extension_mode_for_multiplier(multiplier):
            return multiplier
    return None


def multiplier_for_limit_extension_action(action: object) -> int | None:
    normalized = str(action or "").strip().lower()
    for multiplier in AGENT_TURN_LIMIT_EXTENSION_MULTIPLIERS:
        if normalized == f"extend_limit_{multiplier}x":
            return multiplier
    return None


def is_agent_turn_limit_extension_request(approval: Any) -> bool:
    if approval is None:
        return False
    if str(getattr(approval, "request_kind", "") or "") == AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND:
        return True
    context = getattr(approval, "context", None)
    return isinstance(context, dict) and context.get("tool_name") == AGENT_TURN_LIMIT_EXTENSION_TOOL_NAME


def build_agent_turn_limit_extension_context(
    *,
    current_max_iterations: int | None,
    tool_result_count: int,
    conversation_id: str | None,
    requested_extensions: tuple[str, ...] = (),
) -> dict[str, object]:
    base_limit = current_max_iterations or default_agent_turn_max_iterations()
    return {
        "tool_name": AGENT_TURN_LIMIT_EXTENSION_TOOL_NAME,
        "presented_by": "doctor",
        "current_max_iterations": max(1, int(base_limit)),
        "tool_result_count": max(0, int(tool_result_count)),
        "multipliers": list(AGENT_TURN_LIMIT_EXTENSION_MULTIPLIERS),
        "requested_extensions": [str(ext) for ext in requested_extensions if str(ext)],
        "conversation_id": conversation_id or "",
    }


def build_agent_turn_limit_resume_token(*, current_max_iterations: int | None) -> dict[str, object]:
    base_limit = current_max_iterations or default_agent_turn_max_iterations()
    return {
        "reason": AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
        "current_max_iterations": max(1, int(base_limit)),
    }


def set_agent_turn_limit_resume_multiplier(suspended_turn: Any, multiplier: int) -> int:
    value = int(multiplier)
    if value not in AGENT_TURN_LIMIT_EXTENSION_MULTIPLIERS:
        raise ValueError(f"unsupported limit extension multiplier: {multiplier}")
    token = getattr(suspended_turn, "resume_token", None)
    if not isinstance(token, dict):
        token = build_agent_turn_limit_resume_token(current_max_iterations=None)
    base_limit = token.get("current_max_iterations")
    try:
        current_max_iterations = int(base_limit)
    except (TypeError, ValueError):
        current_max_iterations = default_agent_turn_max_iterations()
    max_iterations = max(1, current_max_iterations) * value
    token.update(
        {
            "reason": AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
            "approved_multiplier": value,
            "max_iterations": max_iterations,
        }
    )
    suspended_turn.resume_token = token
    return max_iterations


def max_iterations_from_resume_token(resume_token: object) -> int | None:
    if not isinstance(resume_token, dict):
        return None
    if resume_token.get("reason") != AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND:
        return None
    raw_value = resume_token.get("max_iterations")
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return None
    return max(1, value)


def chosen_multiplier_from_resume_token(resume_token: object) -> int | None:
    if not isinstance(resume_token, dict):
        return None
    raw_value = resume_token.get("approved_multiplier")
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return None
    return value if value in AGENT_TURN_LIMIT_EXTENSION_MULTIPLIERS else None


def store_budget_approval(runtime, *, conversation_id, message, messages_snapshot,
        current_max_iterations, tool_result_count, chat_id=None, request_id=None,
        message_id=None, requested_extensions=(), context=None, resume_state=None):
    """One persistence/approval mechanism for chat and scheduled continuations.

    Execution-specific state is opaque here. It grants budget only, never tool
    or boundary permissions. Publish choices only after the checkpoint commits.
    """
    from dataclasses import replace
    from datetime import UTC, datetime
    from nullion.approvals import create_approval_request, ApprovalStatus
    from nullion.suspended_turns import SuspendedTurn
    approval = create_approval_request(requested_by=conversation_id,
        action=AGENT_TURN_LIMIT_EXTENSION_ACTION, resource=(context or {}).get('cron_id', conversation_id),
        request_kind=AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
        context={**build_agent_turn_limit_extension_context(current_max_iterations=current_max_iterations,
            tool_result_count=tool_result_count, conversation_id=conversation_id,
            requested_extensions=requested_extensions), **(context or {})})
    token = {**build_agent_turn_limit_resume_token(current_max_iterations=current_max_iterations), **(resume_state or {})}
    suspended = SuspendedTurn(approval_id=approval.approval_id, conversation_id=conversation_id,
        chat_id=chat_id, message=message, request_id=request_id, message_id=message_id,
        created_at=datetime.now(UTC), messages_snapshot=messages_snapshot or [
            {'role': 'user', 'content': [{'type': 'text', 'text': str(message).removeprefix('/chat ')}]}],
        pending_tool_calls=[], resume_token=token)
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(suspended)
    try:
        runtime.checkpoint()
    except Exception:
        runtime.store.remove_suspended_turn(approval.approval_id)
        runtime.store.add_approval_request(replace(approval, status=ApprovalStatus.DENIED,
            decision_reason='budget_checkpoint_unavailable'))
        raise
    return approval.approval_id

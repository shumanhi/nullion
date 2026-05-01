"""Doctor action primitives."""

from dataclasses import dataclass, replace
from hashlib import sha1


PENDING = "pending"
IN_PROGRESS = "in_progress"
COMPLETED = "completed"
CANCELLED = "cancelled"
FAILED = "failed"


_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    PENDING: {IN_PROGRESS, CANCELLED, FAILED},
    IN_PROGRESS: {COMPLETED, CANCELLED, FAILED},
    COMPLETED: set(),
    CANCELLED: set(),
    FAILED: set(),
}


@dataclass(slots=True, frozen=True)
class DoctorAction:
    action_id: str
    recommendation_code: str
    status: str
    summary: str
    severity: str
    owner: str
    source_reason: str | None = None
    reason: str | None = None
    error: str | None = None


def stable_action_id(
    *,
    recommendation_code: str,
    summary: str,
    severity: str,
    owner: str,
    route_target: str | None = None,
    route_reason: str | None = None,
) -> str:
    key_parts = [recommendation_code, summary, severity, owner]
    if route_target is not None:
        key_parts.append(route_target)
    if route_reason is not None:
        key_parts.append(route_reason)
    key = "|".join(key_parts)
    digest = sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"act-{digest}"


def create_doctor_action(
    *,
    recommendation_code: str,
    summary: str,
    severity: str,
    owner: str,
    action_id: str | None = None,
    source_reason: str | None = None,
    route_target: str | None = None,
    route_reason: str | None = None,
) -> DoctorAction:
    resolved_id = action_id or stable_action_id(
        recommendation_code=recommendation_code,
        summary=summary,
        severity=severity,
        owner=owner,
        route_target=route_target,
        route_reason=route_reason,
    )
    return DoctorAction(
        action_id=resolved_id,
        recommendation_code=recommendation_code,
        status=PENDING,
        summary=summary,
        severity=severity,
        owner=owner,
        source_reason=source_reason if source_reason is not None else route_reason,
    )


def start_action(action: DoctorAction) -> DoctorAction:
    return transition_action_status(action, IN_PROGRESS)


def complete_action(action: DoctorAction) -> DoctorAction:
    return transition_action_status(action, COMPLETED)


def cancel_action(action: DoctorAction, *, reason: str | None = None) -> DoctorAction:
    return transition_action_status(action, CANCELLED, reason=reason)


def fail_action(action: DoctorAction, *, error: str | None = None) -> DoctorAction:
    return transition_action_status(action, FAILED, error=error)


def transition_action_status(
    action: DoctorAction,
    new_status: str,
    *,
    reason: str | None = None,
    error: str | None = None,
) -> DoctorAction:
    allowed_next = _ALLOWED_TRANSITIONS[action.status]
    if new_status not in allowed_next:
        raise ValueError(f"Invalid status transition: {action.status} -> {new_status}")

    if new_status == CANCELLED and (reason is None or reason.strip() == ""):
        raise ValueError("cancelled actions require a reason")
    if new_status == FAILED and (error is None or error.strip() == ""):
        raise ValueError("failed actions require an error")

    next_reason = action.reason
    next_error = action.error
    if new_status in {IN_PROGRESS, COMPLETED}:
        next_reason = None
        next_error = None
    elif new_status == CANCELLED:
        next_reason = reason
        next_error = None
    elif new_status == FAILED:
        next_error = error

    return replace(action, status=new_status, reason=next_reason, error=next_error)


__all__ = [
    "DoctorAction",
    "PENDING",
    "IN_PROGRESS",
    "COMPLETED",
    "CANCELLED",
    "FAILED",
    "stable_action_id",
    "create_doctor_action",
    "start_action",
    "complete_action",
    "cancel_action",
    "fail_action",
    "transition_action_status",
]

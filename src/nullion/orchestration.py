"""Orchestration foundation for fast intake and policy preflight."""
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from nullion.approvals import ApprovalRequest, create_approval_request
from nullion.audit import AuditRecord, make_audit_record
from nullion.events import Event, make_event
from nullion.policy import AccessRequest, PolicyDecision, evaluate_request


class OrchestrationOutcome(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_APPROVAL = "require_approval"


@dataclass(slots=True)
class OrchestrationResult:
    outcome: OrchestrationOutcome
    event: Event
    audit_record: AuditRecord
    message: str
    approval_request: ApprovalRequest | None = None


def _outcome_from_policy(decision: PolicyDecision) -> OrchestrationOutcome:
    return OrchestrationOutcome(decision.value)


def _message_for_outcome(outcome: OrchestrationOutcome) -> str:
    if outcome is OrchestrationOutcome.ALLOW:
        return "Request allowed"
    if outcome is OrchestrationOutcome.REQUIRE_APPROVAL:
        return "Approval required"
    return "Request denied"


def orchestrate_request(
    actor: str,
    action: str,
    resource: str,
    event_actor: str | None = None,
    allowed_actions: Iterable[str] | None = None,
    approval_actions: Iterable[str] | None = None,
) -> OrchestrationResult:
    """Evaluate an access request and emit both event and audit artifacts."""
    request = AccessRequest(actor=actor, action=action, resource=resource)
    decision = evaluate_request(
        request,
        allowed_actions=allowed_actions,
        approval_actions=approval_actions,
    )
    outcome = _outcome_from_policy(decision)
    message = _message_for_outcome(outcome)
    approval_request = None

    if outcome is OrchestrationOutcome.REQUIRE_APPROVAL:
        approval_request = create_approval_request(
            requested_by=actor,
            action=action,
            resource=resource,
        )

    structured = {
        "actor": actor,
        "action": action,
        "resource": resource,
        "outcome": outcome.value,
    }
    if approval_request is not None:
        structured["approval_id"] = approval_request.approval_id

    event = make_event(
        event_type="access_request.evaluated",
        actor=event_actor or actor,
        payload=structured,
    )
    audit_record = make_audit_record(
        action="access_request.evaluated",
        actor=actor,
        details=structured,
    )

    return OrchestrationResult(
        outcome=outcome,
        event=event,
        audit_record=audit_record,
        message=message,
        approval_request=approval_request,
    )


__all__ = [
    "OrchestrationOutcome",
    "OrchestrationResult",
    "orchestrate_request",
]

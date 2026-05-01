"""Sentinel escalation artifact primitives for Project Nullion."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from hashlib import sha256

from nullion.approvals import ApprovalRequest


class EscalationStatus(str, Enum):
    ESCALATED = "escalated"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


_ALLOWED_TRANSITIONS: dict[EscalationStatus, set[EscalationStatus]] = {
    EscalationStatus.ESCALATED: {EscalationStatus.ACKNOWLEDGED, EscalationStatus.RESOLVED},
    EscalationStatus.ACKNOWLEDGED: {EscalationStatus.RESOLVED},
    EscalationStatus.RESOLVED: set(),
}


@dataclass(slots=True)
class SentinelEscalationArtifact:
    escalation_id: str
    source_signal_reason: str
    severity: str
    status: EscalationStatus
    created_at: datetime
    summary: str
    approval_id: str | None = None


def create_sentinel_escalation(
    target: str,
    reason: str,
    severity: str,
    *,
    created_at: datetime | None = None,
    summary: str | None = None,
) -> SentinelEscalationArtifact:
    normalized_target = target.value if hasattr(target, "value") else str(target)
    generated_summary = summary or f"Sentinel escalation for {normalized_target}: {reason}"
    timestamp = datetime.now(UTC) if created_at is None else created_at
    escalation_id = _make_escalation_id(
        target=normalized_target,
        reason=reason,
        severity=severity,
        created_at=timestamp,
    )

    return SentinelEscalationArtifact(
        escalation_id=escalation_id,
        source_signal_reason=reason,
        severity=severity,
        status=EscalationStatus.ESCALATED,
        created_at=timestamp,
        summary=generated_summary,
    )


def create_escalation_from_signal_route(
    route: object,
    *,
    created_at: datetime | None = None,
    summary: str | None = None,
) -> SentinelEscalationArtifact:
    return create_sentinel_escalation(
        target=_normalize_target(getattr(route, "target")),
        reason=getattr(route, "reason"),
        severity=getattr(route, "severity"),
        created_at=created_at,
        summary=summary,
    )


def acknowledge_escalation(
    artifact: SentinelEscalationArtifact,
) -> SentinelEscalationArtifact:
    return transition_escalation_status(artifact, EscalationStatus.ACKNOWLEDGED)


def resolve_escalation(
    artifact: SentinelEscalationArtifact,
) -> SentinelEscalationArtifact:
    return transition_escalation_status(artifact, EscalationStatus.RESOLVED)


def link_escalation_to_approval(
    artifact: SentinelEscalationArtifact,
    approval: ApprovalRequest,
) -> SentinelEscalationArtifact:
    if approval.resource != artifact.escalation_id:
        raise ValueError("approval resource must match escalation_id")
    return replace(artifact, approval_id=approval.approval_id)


def transition_escalation_status(
    artifact: SentinelEscalationArtifact,
    new_status: EscalationStatus,
) -> SentinelEscalationArtifact:
    allowed_next = _ALLOWED_TRANSITIONS[artifact.status]
    if new_status not in allowed_next:
        raise ValueError(
            f"Invalid escalation transition: {artifact.status.value} -> {new_status.value}"
        )
    return replace(artifact, status=new_status)


def _normalize_target(target: object) -> str:
    return target.value if hasattr(target, "value") else str(target)


def _make_escalation_id(target: str, reason: str, severity: str, *, created_at: datetime) -> str:
    digest = sha256(f"{target}|{reason}|{severity}|{created_at.isoformat()}".encode("utf-8")).hexdigest()
    return f"esc-{digest[:16]}"

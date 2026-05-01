"""Signal routing primitives for Doctor and Sentinel in Project Nullion."""

from dataclasses import dataclass
from enum import Enum

from nullion.health import HealthAlert, HealthIssueType
from nullion.policy import PolicyDecision


class SignalTarget(str, Enum):
    DOCTOR = "doctor"
    SENTINEL = "sentinel"
    NONE = "none"


@dataclass(slots=True)
class SignalRoute:
    target: SignalTarget
    reason: str
    severity: str
    # Human-readable label (populated from alert.message / playbook.summary).
    # Carried through the pipeline so it can be shown in doctor action cards
    # without parsing the mangled reason string.
    summary: str | None = None
    # Raw error text from the probe or request that triggered this signal.
    error: str | None = None
    # Structured Doctor remediation code supplied by health probes/playbooks.
    recommendation_code: str | None = None


_HEALTH_SEVERITY_BY_ISSUE: dict[HealthIssueType, str] = {
    HealthIssueType.ERROR: "critical",
    HealthIssueType.ISSUE: "high",
    HealthIssueType.TIMEOUT: "high",
    HealthIssueType.STALLED: "medium",
    HealthIssueType.DEGRADED: "low",
}


_POLICY_TARGET_BY_DECISION: dict[PolicyDecision, SignalTarget] = {
    PolicyDecision.REQUIRE_APPROVAL: SignalTarget.SENTINEL,
    PolicyDecision.DENY: SignalTarget.SENTINEL,
    PolicyDecision.ALLOW: SignalTarget.NONE,
}


_POLICY_SEVERITY_BY_DECISION: dict[PolicyDecision, str] = {
    PolicyDecision.REQUIRE_APPROVAL: "high",
    PolicyDecision.DENY: "critical",
    PolicyDecision.ALLOW: "info",
}


def route_health_alert(alert: HealthAlert) -> SignalRoute:
    severity = _HEALTH_SEVERITY_BY_ISSUE[alert.issue_type]
    reason = (
        "health_alert routed to doctor"
        f";issue_type={alert.issue_type.value}"
        f";source={alert.source}"
    )
    details = alert.details or {}
    error_str = details.get("error")
    recommendation_code = details.get("recommendation_code")
    return SignalRoute(
        target=SignalTarget.DOCTOR,
        reason=reason,
        severity=severity,
        summary=alert.message or None,
        error=str(error_str).strip() if error_str else None,
        recommendation_code=str(recommendation_code).strip() if recommendation_code else None,
    )


def route_policy_decision(
    decision: PolicyDecision,
    action: str,
    resource: str,
) -> SignalRoute:
    target = _POLICY_TARGET_BY_DECISION[decision]
    severity = _POLICY_SEVERITY_BY_DECISION[decision]
    reason = (
        f"policy={decision.value}"
        f";action={action}"
        f";resource={resource}"
        f";target={target.value}"
    )
    return SignalRoute(target=target, reason=reason, severity=severity)

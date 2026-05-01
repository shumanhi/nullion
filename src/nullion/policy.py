"""Policy and access-request decision primitives for Project Nullion."""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from fnmatch import fnmatch
from typing import Iterable
from urllib.parse import urlparse

_COMMON_MULTILABEL_PUBLIC_SUFFIXES = {
    "co.uk",
    "org.uk",
    "ac.uk",
    "gov.uk",
    "co.jp",
    "com.au",
    "net.au",
    "org.au",
    "com.br",
    "com.cn",
    "com.mx",
    "co.nz",
}


class PolicyDecision(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_APPROVAL = "require_approval"


class BoundaryKind(str, Enum):
    OUTBOUND_NETWORK = "outbound_network"
    INBOUND_NETWORK = "inbound_network"
    ACCOUNT_ACCESS = "account_access"
    FILESYSTEM_ACCESS = "filesystem_access"


class PolicyMode(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    ASK = "ask"


class SentinelMode(str, Enum):
    ALLOW_ALL = "allow_all"
    RISK_BASED = "risk_based"
    ASK_ALL = "ask_all"


@dataclass(frozen=True, slots=True)
class SentinelPolicy:
    mode: SentinelMode = SentinelMode.RISK_BASED
    risk_threshold: int = 4

    @classmethod
    def from_values(cls, *, mode: str | None = None, risk_threshold: int | str | None = None) -> "SentinelPolicy":
        try:
            parsed_mode = SentinelMode(mode or SentinelMode.RISK_BASED.value)
        except ValueError:
            parsed_mode = SentinelMode.RISK_BASED
        try:
            parsed_threshold = int(risk_threshold if risk_threshold is not None else 4)
        except (TypeError, ValueError):
            parsed_threshold = 4
        return cls(mode=parsed_mode, risk_threshold=min(10, max(3, parsed_threshold)))

    def decision_for_risk(self, risk_score: int, *, baseline: PolicyDecision) -> PolicyDecision:
        """Apply the operator's Sentinel mode without weakening hard denies."""
        if baseline is PolicyDecision.ALLOW:
            return PolicyDecision.ALLOW
        if baseline is PolicyDecision.DENY:
            return PolicyDecision.DENY
        if self.mode is SentinelMode.ALLOW_ALL:
            return PolicyDecision.ALLOW
        if self.mode is SentinelMode.ASK_ALL:
            return PolicyDecision.REQUIRE_APPROVAL
        if risk_score >= self.risk_threshold:
            return PolicyDecision.REQUIRE_APPROVAL
        return PolicyDecision.ALLOW


OPERATOR_PERMISSION_PRINCIPAL = "global:operator"
GLOBAL_PERMISSION_PRINCIPAL = OPERATOR_PERMISSION_PRINCIPAL
TOOL_PERMISSION_SCOPE_GLOBAL = "global"
TOOL_PERMISSION_SCOPE_WORKSPACE = "workspace"


def permission_scope_principal(principal_id: str | None = None) -> str:
    """Return the global permission owner for a principal.

    Tool invocations still keep their original principal and workspace metadata
    for audit, resume, and notification routing. Grants and remembered boundary
    decisions are global so an approval from one workspace follows the operator
    across web, Telegram, Slack, Discord, delegated workers, and future adapters.
    """
    del principal_id
    return OPERATOR_PERMISSION_PRINCIPAL


def tool_permission_scope_for_name(tool_name: str | None) -> str:
    """Return the default grant scope for a tool name.

    Tool approval memory is global by default. Workspace isolation is enforced
    by filesystem roots and provider connection resolution, not by hiding tool
    approvals from other sessions or platforms.
    """
    return TOOL_PERMISSION_SCOPE_GLOBAL


def permission_grant_principal(
    principal_id: str | None,
    *,
    permission: str | None = None,
    tool_name: str | None = None,
    permission_scope: str | None = None,
    boundary_kind: BoundaryKind | str | None = None,
) -> str:
    """Return the principal that should own remembered permission state."""
    normalized_scope = str(permission_scope or "").strip().lower()
    if not normalized_scope:
        candidate_tool = tool_name
        if not candidate_tool and isinstance(permission, str):
            if permission.startswith("tool:"):
                candidate_tool = permission.removeprefix("tool:")
            elif permission.startswith("tool."):
                candidate_tool = permission.removeprefix("tool.")
            else:
                return permission_scope_principal(principal_id)
        normalized_scope = tool_permission_scope_for_name(candidate_tool)

    normalized_boundary = str(getattr(boundary_kind, "value", boundary_kind or "")).strip().lower()
    if normalized_boundary == BoundaryKind.FILESYSTEM_ACCESS.value:
        normalized_scope = TOOL_PERMISSION_SCOPE_WORKSPACE

    if normalized_scope == TOOL_PERMISSION_SCOPE_WORKSPACE and normalized_boundary == BoundaryKind.FILESYSTEM_ACCESS.value:
        return permission_scope_principal(principal_id)
    return GLOBAL_PERMISSION_PRINCIPAL


@dataclass(slots=True)
class AccessRequest:
    actor: str
    action: str
    resource: str


@dataclass(slots=True)
class BoundaryFact:
    kind: BoundaryKind
    tool_name: str
    operation: str
    target: str
    attributes: dict[str, str]


@dataclass(slots=True)
class BoundaryPolicyRule:
    rule_id: str
    principal_id: str
    kind: BoundaryKind
    mode: PolicyMode
    selector: str
    created_by: str
    created_at: datetime
    priority: int = 0
    reason: str | None = None
    expires_at: datetime | None = None
    revoked_at: datetime | None = None


@dataclass(slots=True)
class BoundaryPolicyRequest:
    principal_id: str
    boundary: BoundaryFact


def evaluate_request(
    request: AccessRequest,
    allowed_actions: Iterable[str] | None = None,
    approval_actions: Iterable[str] | None = None,
) -> PolicyDecision:
    """Evaluate an access request by action string using default-deny behavior."""
    approval = frozenset(approval_actions or ())
    allowed = frozenset(allowed_actions or ())

    if request.action in approval:
        return PolicyDecision.REQUIRE_APPROVAL
    if request.action in allowed:
        return PolicyDecision.ALLOW
    return PolicyDecision.DENY


def _rule_is_active(rule: BoundaryPolicyRule, *, now: datetime | None = None) -> bool:
    check_time = now or datetime.now(UTC)
    if rule.revoked_at is not None and rule.revoked_at <= check_time:
        return False
    if rule.expires_at is not None and rule.expires_at <= check_time:
        return False
    return True


def normalize_outbound_network_selector(target: str) -> str:
    """Normalize an outbound-network target to the registrable domain policy key."""
    parsed = urlparse(target if "://" in target else f"https://{target}")
    host = (parsed.hostname or target).lower().strip(".")
    return _policy_domain_for_host(host)


def _selector_matches(rule: BoundaryPolicyRule, request: BoundaryPolicyRequest) -> bool:
    if rule.selector == "*":
        return True
    if request.boundary.kind is BoundaryKind.OUTBOUND_NETWORK:
        return normalize_outbound_network_selector(rule.selector) == normalize_outbound_network_selector(
            request.boundary.target
        )
    target = request.boundary.target
    if rule.selector == target:
        return True
    if fnmatch(target, rule.selector):
        return True
    if rule.selector.endswith("/*") and target == rule.selector[:-2]:
        return True
    if _selector_matches_www_family(selector=rule.selector, target=target):
        return True
    return False


def _selector_specificity(rule: BoundaryPolicyRule, request: BoundaryPolicyRequest) -> int:
    if rule.selector == "*":
        return 1
    if request.boundary.kind is BoundaryKind.OUTBOUND_NETWORK:
        return (
            1
            if normalize_outbound_network_selector(rule.selector) == normalize_outbound_network_selector(
                request.boundary.target
            )
            else 0
        )
    target = request.boundary.target
    if rule.selector == target:
        return 2
    if fnmatch(target, rule.selector):
        return 1
    if rule.selector.endswith("/*") and target == rule.selector[:-2]:
        return 1
    if _selector_matches_www_family(selector=rule.selector, target=target):
        return 1
    return 0


def _selector_matches_www_family(*, selector: str, target: str) -> bool:
    selector_base_url = selector[:-2] if selector.endswith("/*") else selector
    parsed_selector = urlparse(selector_base_url)
    parsed_target = urlparse(target)
    if parsed_selector.scheme != parsed_target.scheme:
        return False
    selector_host = (parsed_selector.hostname or "").lower()
    target_host = (parsed_target.hostname or "").lower()
    if not selector_host or not target_host:
        return False
    if _policy_domain_for_host(selector_host) != _policy_domain_for_host(target_host):
        return False
    if selector.endswith("/*"):
        return True
    return parsed_selector.path.rstrip("/") == parsed_target.path.rstrip("/")


def _policy_domain_for_host(host: str) -> str:
    normalized = host.lower().strip(".")
    labels = [label for label in normalized.split(".") if label]
    if len(labels) <= 2:
        return normalized
    suffix = ".".join(labels[-2:])
    if suffix in _COMMON_MULTILABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return suffix


def _rule_precedence_key(rule: BoundaryPolicyRule, request: BoundaryPolicyRequest) -> tuple[int, int, datetime, int, str]:
    specificity = _selector_specificity(rule, request)
    mode_rank = 1 if rule.mode is PolicyMode.DENY else 0
    return specificity, rule.priority, rule.created_at, mode_rank, rule.rule_id


def evaluate_boundary_request(
    request: BoundaryPolicyRequest,
    rules: Iterable[BoundaryPolicyRule],
) -> PolicyDecision:
    address_class = request.boundary.attributes.get("address_class")
    if request.boundary.kind is BoundaryKind.OUTBOUND_NETWORK and address_class in {
        "private",
        "loopback",
        "localhost",
        "link_local",
        "reserved",
    }:
        return PolicyDecision.DENY

    matched_rules = [
        rule
        for rule in rules
        if rule.principal_id == request.principal_id
        and rule.kind is request.boundary.kind
        and _rule_is_active(rule)
        and _selector_specificity(rule, request) > 0
    ]
    if matched_rules:
        selected_rule = max(matched_rules, key=lambda rule: _rule_precedence_key(rule, request))
        if selected_rule.mode is PolicyMode.ALLOW:
            return PolicyDecision.ALLOW
        if selected_rule.mode is PolicyMode.DENY:
            return PolicyDecision.DENY

    if request.boundary.kind is BoundaryKind.OUTBOUND_NETWORK:
        return PolicyDecision.REQUIRE_APPROVAL
    return PolicyDecision.DENY

"""Shared approval decision helpers for chat and web surfaces."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Callable, Iterable

from nullion.approvals import ApprovalRequest, ApprovalStatus
from nullion.policy import BoundaryKind


ApprovalMode = str


@dataclass(frozen=True, slots=True)
class ApprovalDecisionResult:
    approval: ApprovalRequest
    mode: ApprovalMode
    auto_approved_ids: tuple[str, ...] = ()


def approval_status_value(approval: ApprovalRequest) -> str:
    status = getattr(approval, "status", None)
    return str(getattr(status, "value", status)).lower()


def normalize_approval_mode(mode: object, *, allowed: Iterable[str] = ("once", "always", "run")) -> str:
    normalized = str(mode or "once").strip().lower()
    allowed_set = set(allowed)
    if normalized not in allowed_set:
        raise ValueError(f"mode must be {', '.join(repr(value) for value in sorted(allowed_set))}")
    return normalized


def approval_context(approval: ApprovalRequest) -> dict[str, object]:
    return approval.context if isinstance(approval.context, dict) else {}


def approval_tool_permissions(approval: ApprovalRequest) -> list[str]:
    if approval.request_kind == "boundary_policy" or approval.action == "allow_boundary":
        return []
    ctx = approval_context(approval)
    tool_name = ctx.get("tool_name")
    if isinstance(tool_name, str) and tool_name:
        return [f"tool:{tool_name}"]
    if approval.action == "use_tool":
        return [f"tool:{approval.resource}"]
    if approval.action != "allow_boundary":
        return [f"{approval.action}:{approval.resource}"]
    return []


def approval_boundary_kind(approval: ApprovalRequest) -> BoundaryKind | None:
    ctx = approval_context(approval)
    raw_value = ctx.get("boundary_kind")
    if raw_value is None and (approval.request_kind == "boundary_policy" or approval.action == "allow_boundary"):
        raw_value = "outbound_network"
    if not isinstance(raw_value, str) or not raw_value:
        return None
    try:
        return BoundaryKind(raw_value)
    except ValueError:
        return None


def approval_selector(approval: ApprovalRequest, name: str) -> str | None:
    ctx = approval_context(approval)
    selectors = ctx.get("selector_candidates") if isinstance(ctx.get("selector_candidates"), dict) else {}
    value = selectors.get(name)
    if isinstance(value, str) and value:
        return value
    if name == "always_allow":
        return approval_selector(approval, "allow_once")
    if name == "allow_once" and approval.request_kind == "boundary_policy":
        return approval.resource
    return None


def is_outbound_boundary_approval(approval: ApprovalRequest) -> bool:
    return (
        (approval.request_kind == "boundary_policy" or approval.action == "allow_boundary")
        and approval_boundary_kind(approval) is BoundaryKind.OUTBOUND_NETWORK
    )


def approval_decision_reason(*, mode: str, source: str) -> str:
    source_label = str(source or "operator").strip() or "operator"
    if mode == "run":
        return f"Allowed all web domains from {source_label}"
    if mode == "always":
        return f"Always allowed from {source_label}"
    return f"Allowed once from {source_label}"


def approve_request_with_mode(
    runtime: Any,
    approval_id: str,
    *,
    mode: str = "once",
    source: str,
    expires_at: datetime | None = None,
    run_expires_at: datetime | None = None,
    auto_approve_run_boundaries: bool = False,
    allow_redecide_denied: bool = False,
    run_expiry_factory: Callable[[], datetime | None] | None = None,
) -> ApprovalDecisionResult:
    normalized_mode = normalize_approval_mode(mode)
    store = runtime.store
    approval = store.get_approval_request(approval_id)
    if approval is None:
        raise KeyError(approval_id)

    status = approval_status_value(approval)
    if status == ApprovalStatus.DENIED.value:
        if not allow_redecide_denied:
            raise ValueError("This request was already denied.")
        approval = replace(approval, status=ApprovalStatus.PENDING, decided_by=None, decided_at=None, decision_reason=None)
        store.add_approval_request(approval)
    if status == ApprovalStatus.APPROVED.value:
        return ApprovalDecisionResult(approval=approval, mode=normalized_mode)

    boundary_kind = approval_boundary_kind(approval)
    is_run_wide_web = normalized_mode == "run" and boundary_kind is BoundaryKind.OUTBOUND_NETWORK
    decision_expires_at = run_expires_at if is_run_wide_web else expires_at
    if is_run_wide_web and decision_expires_at is None and run_expiry_factory is not None:
        decision_expires_at = run_expiry_factory()

    approved = runtime.approve_approval_request(
        approval.approval_id,
        principal_id=approval.requested_by,
        permissions=approval_tool_permissions(approval),
        boundary_allow_once_selector=(
            "*"
            if is_run_wide_web
            else approval_selector(approval, "allow_once")
            if normalized_mode == "once"
            else None
        ),
        boundary_allow_once_uses=100 if is_run_wide_web else None,
        boundary_always_allow_selector=approval_selector(approval, "always_allow") if normalized_mode == "always" else None,
        boundary_kind=boundary_kind,
        expires_at=decision_expires_at,
        actor="operator",
        reason=approval_decision_reason(mode=normalized_mode, source=source),
    )

    auto_approved_ids: list[str] = []
    if is_run_wide_web and auto_approve_run_boundaries:
        for other in store.list_approval_requests():
            if other.approval_id == approved.approval_id:
                continue
            if approval_status_value(other) != ApprovalStatus.PENDING.value:
                continue
            if not is_outbound_boundary_approval(other):
                continue
            other_boundary_kind = approval_boundary_kind(other)
            runtime.approve_approval_request(
                other.approval_id,
                principal_id=other.requested_by,
                permissions=approval_tool_permissions(other),
                boundary_allow_once_selector="*",
                boundary_allow_once_uses=100,
                boundary_kind=other_boundary_kind,
                expires_at=decision_expires_at,
                actor="operator",
                reason=approval_decision_reason(mode="run", source=f"{source} global web access"),
            )
            auto_approved_ids.append(other.approval_id)

    return ApprovalDecisionResult(
        approval=approved,
        mode=normalized_mode,
        auto_approved_ids=tuple(auto_approved_ids),
    )

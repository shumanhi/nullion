from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from nullion.approval_decisions import approval_tool_permissions, approve_request_with_mode
from nullion.approvals import (
    ApprovalStatus,
    create_approval_request,
    create_boundary_permit,
    create_permission_grant,
    is_boundary_permit_active,
)
from nullion.policy import BoundaryKind
from nullion.runtime_store import RuntimeStore


class RuntimeDouble:
    def __init__(self) -> None:
        self.store = RuntimeStore()

    def approve_approval_request(
        self,
        approval_id: str,
        *,
        principal_id: str,
        permissions: list[str],
        boundary_allow_once_selector: str | None = None,
        boundary_allow_once_uses: int | None = None,
        boundary_always_allow_selector: str | None = None,
        boundary_kind: BoundaryKind | None = None,
        expires_at: datetime | None = None,
        actor: str,
        reason: str,
    ):
        approval = self.store.get_approval_request(approval_id)
        assert approval is not None
        approved = replace(
            approval,
            status=ApprovalStatus.APPROVED,
            decided_by=actor,
            decided_at=datetime.now(UTC),
            decision_reason=reason,
        )
        self.store.add_approval_request(approved)
        for permission in permissions:
            self.store.add_permission_grant(
                create_permission_grant(
                    approval_id=approval_id,
                    principal_id=principal_id,
                    permission=permission,
                    granted_by=actor,
                    expires_at=expires_at,
                )
            )
        selector = boundary_allow_once_selector or boundary_always_allow_selector
        if selector and boundary_kind is not None:
            self.store.add_boundary_permit(
                create_boundary_permit(
                    approval_id=approval_id,
                    principal_id=principal_id,
                    boundary_kind=boundary_kind,
                    selector=selector,
                    granted_by=actor,
                    uses_remaining=boundary_allow_once_uses or 1,
                    expires_at=expires_at,
                )
            )
        return approved


def _web_approval(approval_id_hint: str, target: str):
    approval = create_approval_request(
        requested_by="telegram:123",
        action="allow_boundary",
        resource=target,
        request_kind="boundary_policy",
        context={
            "tool_name": "web_fetch",
            "boundary_kind": "outbound_network",
            "target": target,
            "selector_candidates": {
                "allow_once": target,
                "always_allow": target,
            },
            "hint": approval_id_hint,
        },
    )
    return approval


def test_allow_all_web_approval_creates_run_wide_permit_and_auto_approves_pending_boundaries() -> None:
    runtime = RuntimeDouble()
    first = _web_approval("first", "https://www.bing.com/*")
    second = _web_approval("second", "https://stockanalysis.com/stocks/nflx/")
    runtime.store.add_approval_request(first)
    runtime.store.add_approval_request(second)
    expires_at = datetime.now(UTC) + timedelta(minutes=30)

    result = approve_request_with_mode(
        runtime,
        first.approval_id,
        mode="run",
        source="telegram",
        run_expires_at=expires_at,
        auto_approve_run_boundaries=True,
    )

    assert result.mode == "run"
    assert result.auto_approved_ids == (second.approval_id,)
    permits = runtime.store.list_boundary_permits()
    assert len(permits) == 2
    assert {permit.selector for permit in permits} == {"*"}
    assert all(permit.uses_remaining == 100 for permit in permits)
    assert all(permit.expires_at == expires_at for permit in permits)
    assert all(is_boundary_permit_active(permit) for permit in permits)
    assert runtime.store.list_permission_grants() == []


def test_always_allow_domain_approval_uses_domain_selector_without_auto_approving_other_requests() -> None:
    runtime = RuntimeDouble()
    first = _web_approval("first", "https://www.bing.com/*")
    second = _web_approval("second", "https://example.com/*")
    runtime.store.add_approval_request(first)
    runtime.store.add_approval_request(second)

    result = approve_request_with_mode(runtime, first.approval_id, mode="always", source="web")

    assert result.auto_approved_ids == ()
    permits = runtime.store.list_boundary_permits()
    assert len(permits) == 1
    assert permits[0].selector == "https://www.bing.com/*"
    assert runtime.store.get_approval_request(second.approval_id).status is ApprovalStatus.PENDING
    assert runtime.store.list_permission_grants() == []


def test_allow_once_web_approval_uses_single_use_boundary_permit() -> None:
    runtime = RuntimeDouble()
    approval = _web_approval("once", "https://www.bing.com/*")
    runtime.store.add_approval_request(approval)

    result = approve_request_with_mode(runtime, approval.approval_id, mode="once", source="telegram")

    assert result.mode == "once"
    assert result.auto_approved_ids == ()
    permits = runtime.store.list_boundary_permits()
    assert len(permits) == 1
    assert permits[0].selector == "https://www.bing.com/*"
    assert permits[0].uses_remaining == 1
    assert runtime.store.list_permission_grants() == []


def test_boundary_approvals_never_create_tool_permission_grants() -> None:
    boundary_approval = _web_approval("boundary", "https://example.com/*")
    tool_approval = create_approval_request(
        requested_by="web:operator",
        action="use_tool",
        resource="terminal_exec",
        context={"tool_name": "terminal_exec"},
    )

    assert approval_tool_permissions(boundary_approval) == []
    assert approval_tool_permissions(tool_approval) == ["tool:terminal_exec"]


def test_approved_request_is_idempotent_and_denied_request_requires_explicit_redecide() -> None:
    runtime = RuntimeDouble()
    approval = _web_approval("first", "https://www.bing.com/*")
    runtime.store.add_approval_request(approval)

    first = approve_request_with_mode(runtime, approval.approval_id, mode="always", source="web")
    second = approve_request_with_mode(runtime, approval.approval_id, mode="run", source="web")

    assert first.approval.approval_id == approval.approval_id
    assert second.mode == "run"
    assert len(runtime.store.list_boundary_permits()) == 1

    denied = replace(_web_approval("denied", "https://example.com/*"), status=ApprovalStatus.DENIED)
    runtime.store.add_approval_request(denied)

    try:
        approve_request_with_mode(runtime, denied.approval_id, mode="always", source="web")
    except ValueError as exc:
        assert "already denied" in str(exc)
    else:
        raise AssertionError("denied approval should not be re-decided by default")

    approve_request_with_mode(
        runtime,
        denied.approval_id,
        mode="always",
        source="web",
        allow_redecide_denied=True,
    )
    assert runtime.store.get_approval_request(denied.approval_id).status is ApprovalStatus.APPROVED

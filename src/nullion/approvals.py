"""Approval flow primitives for Project Nullion."""

import copy
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from uuid import uuid4

from nullion.policy import BoundaryKind


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"


@dataclass(slots=True)
class ApprovalRequest:
    approval_id: str
    requested_by: str
    action: str
    resource: str
    status: ApprovalStatus
    created_at: datetime
    request_kind: str = "capability_grant"
    context: dict[str, object] | None = None
    decided_by: str | None = None
    decided_at: datetime | None = None
    decision_reason: str | None = None


@dataclass(slots=True)
class PermissionGrant:
    grant_id: str
    approval_id: str
    principal_id: str
    permission: str
    granted_by: str
    granted_at: datetime
    expires_at: datetime | None = None
    revoked_by: str | None = None
    revoked_at: datetime | None = None
    revoked_reason: str | None = None

    @property
    def capability(self) -> str:
        return self.permission


@dataclass(slots=True)
class BoundaryPermit:
    permit_id: str
    approval_id: str
    principal_id: str
    boundary_kind: BoundaryKind
    selector: str
    granted_by: str
    granted_at: datetime
    uses_remaining: int = 1
    expires_at: datetime | None = None
    revoked_by: str | None = None
    revoked_at: datetime | None = None
    revoked_reason: str | None = None


def create_approval_request(
    requested_by: str,
    action: str,
    resource: str,
    *,
    request_kind: str = "capability_grant",
    context: dict[str, object] | None = None,
) -> ApprovalRequest:
    return ApprovalRequest(
        approval_id=str(uuid4()),
        requested_by=requested_by,
        action=action,
        resource=resource,
        status=ApprovalStatus.PENDING,
        created_at=datetime.now(UTC),
        request_kind=request_kind,
        context=copy.deepcopy(context) if context is not None else None,
    )


def create_permission_grant(
    *,
    approval_id: str,
    principal_id: str,
    permission: str,
    granted_by: str,
    grant_id: str | None = None,
    granted_at: datetime | None = None,
    expires_at: datetime | None = None,
    revoked_by: str | None = None,
    revoked_at: datetime | None = None,
    revoked_reason: str | None = None,
) -> PermissionGrant:
    return PermissionGrant(
        grant_id=grant_id or str(uuid4()),
        approval_id=approval_id,
        principal_id=principal_id,
        permission=permission,
        granted_by=granted_by,
        granted_at=granted_at or datetime.now(UTC),
        expires_at=expires_at,
        revoked_by=revoked_by,
        revoked_at=revoked_at,
        revoked_reason=revoked_reason,
    )


def create_boundary_permit(
    *,
    approval_id: str,
    principal_id: str,
    boundary_kind: BoundaryKind,
    selector: str,
    granted_by: str,
    permit_id: str | None = None,
    granted_at: datetime | None = None,
    uses_remaining: int = 1,
    expires_at: datetime | None = None,
    revoked_by: str | None = None,
    revoked_at: datetime | None = None,
    revoked_reason: str | None = None,
) -> BoundaryPermit:
    return BoundaryPermit(
        permit_id=permit_id or str(uuid4()),
        approval_id=approval_id,
        principal_id=principal_id,
        boundary_kind=boundary_kind,
        selector=selector,
        granted_by=granted_by,
        granted_at=granted_at or datetime.now(UTC),
        uses_remaining=uses_remaining,
        expires_at=expires_at,
        revoked_by=revoked_by,
        revoked_at=revoked_at,
        revoked_reason=revoked_reason,
    )


def _require_pending(request: ApprovalRequest) -> None:
    if request.status != ApprovalStatus.PENDING:
        raise ValueError("approval request must be pending before decision")


def revoke_permission_grant(
    grant: PermissionGrant,
    *,
    revoked_by: str,
    revoked_at: datetime | None = None,
    reason: str | None = None,
) -> PermissionGrant:
    if grant.revoked_at is not None:
        raise ValueError("permission grant is already revoked")
    return replace(
        grant,
        revoked_by=revoked_by,
        revoked_at=revoked_at or datetime.now(UTC),
        revoked_reason=reason,
    )


def is_permission_grant_active(
    grant: PermissionGrant,
    *,
    now: datetime | None = None,
) -> bool:
    check_time = now or datetime.now(UTC)
    if grant.revoked_at is not None and grant.revoked_at <= check_time:
        return False
    if grant.expires_at is not None and grant.expires_at <= check_time:
        return False
    return True


def is_boundary_permit_active(
    permit: BoundaryPermit,
    *,
    now: datetime | None = None,
) -> bool:
    check_time = now or datetime.now(UTC)
    if permit.revoked_at is not None and permit.revoked_at <= check_time:
        return False
    if permit.expires_at is not None and permit.expires_at <= check_time:
        return False
    if permit.uses_remaining <= 0:
        return False
    return True


def consume_boundary_permit(
    permit: BoundaryPermit,
    *,
    consumed_by: str | None = None,
    consumed_at: datetime | None = None,
) -> BoundaryPermit:
    del consumed_by, consumed_at
    if permit.uses_remaining <= 0:
        raise ValueError("boundary permit has no remaining uses")
    return replace(permit, uses_remaining=0)


def approve(
    request: ApprovalRequest,
    *,
    decided_by: str | None = None,
    decided_at: datetime | None = None,
    reason: str | None = None,
) -> ApprovalRequest:
    _require_pending(request)
    return replace(
        request,
        status=ApprovalStatus.APPROVED,
        decided_by=decided_by,
        decided_at=decided_at or datetime.now(UTC),
        decision_reason=reason,
    )


def deny(
    request: ApprovalRequest,
    *,
    decided_by: str | None = None,
    decided_at: datetime | None = None,
    reason: str | None = None,
) -> ApprovalRequest:
    _require_pending(request)
    return replace(
        request,
        status=ApprovalStatus.DENIED,
        decided_by=decided_by,
        decided_at=decided_at or datetime.now(UTC),
        decision_reason=reason,
    )

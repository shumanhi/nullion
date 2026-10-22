from datetime import UTC, datetime

from nullion.approvals import create_boundary_permit, create_permission_grant
from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode
from nullion.runtime_persistence import load_runtime_store, save_runtime_store
from nullion.runtime_store import RuntimeStore


def test_sqlite_merge_preserves_permission_revocations_from_other_process(tmp_path):
    checkpoint = tmp_path / "runtime.db"
    granted_at = datetime(2026, 5, 1, 22, 5, tzinfo=UTC)
    revoked_at = datetime(2026, 5, 1, 22, 22, tzinfo=UTC)

    active = RuntimeStore()
    active.add_permission_grant(
        create_permission_grant(
            approval_id="ap-grant",
            principal_id="global:operator",
            permission="tool:web_fetch",
            granted_by="operator",
            granted_at=granted_at,
            grant_id="grant-1",
        )
    )
    active.add_boundary_permit(
        create_boundary_permit(
            approval_id="ap-permit",
            principal_id="global:operator",
            boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
            selector="cnn.com",
            granted_by="operator",
            granted_at=granted_at,
            uses_remaining=15,
            permit_id="permit-1",
        )
    )
    active.add_boundary_policy_rule(
        BoundaryPolicyRule(
            rule_id="rule-1",
            principal_id="global:operator",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.ALLOW,
            selector="*",
            created_by="operator",
            created_at=granted_at,
        )
    )
    save_runtime_store(active, checkpoint)

    revoked = RuntimeStore()
    revoked.add_permission_grant(
        create_permission_grant(
            approval_id="ap-grant",
            principal_id="global:operator",
            permission="tool:web_fetch",
            granted_by="operator",
            granted_at=granted_at,
            grant_id="grant-1",
            revoked_at=revoked_at,
        )
    )
    revoked.add_boundary_permit(
        create_boundary_permit(
            approval_id="ap-permit",
            principal_id="global:operator",
            boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
            selector="cnn.com",
            granted_by="operator",
            granted_at=granted_at,
            uses_remaining=15,
            permit_id="permit-1",
            revoked_at=revoked_at,
        )
    )
    revoked.add_boundary_policy_rule(
        BoundaryPolicyRule(
            rule_id="rule-1",
            principal_id="global:operator",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.ALLOW,
            selector="*",
            created_by="operator",
            created_at=granted_at,
            revoked_at=revoked_at,
        )
    )
    save_runtime_store(revoked, checkpoint)

    stale_process = RuntimeStore()
    stale_process.add_permission_grant(active.get_permission_grant("grant-1"))
    stale_process.add_boundary_permit(active.get_boundary_permit("permit-1"))
    stale_process.add_boundary_policy_rule(active.get_boundary_policy_rule("rule-1"))
    save_runtime_store(stale_process, checkpoint)

    loaded = load_runtime_store(checkpoint)
    assert loaded.get_permission_grant("grant-1").revoked_at == revoked_at
    assert loaded.get_boundary_permit("permit-1").revoked_at == revoked_at
    assert loaded.get_boundary_policy_rule("rule-1").revoked_at == revoked_at

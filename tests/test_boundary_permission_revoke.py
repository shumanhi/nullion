from datetime import UTC, datetime

from nullion.approvals import create_boundary_permit
from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode
from nullion.runtime import revoke_related_boundary_permission
from nullion.runtime_store import RuntimeStore


def test_revoke_domain_permit_revokes_active_sibling_permits_and_allow_rules():
    store = RuntimeStore()
    granted_at = datetime(2026, 5, 1, 22, 39, tzinfo=UTC)
    cnn_one = create_boundary_permit(
        approval_id="approval-cnn-1",
        principal_id="telegram_chat",
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="cnn.com",
        granted_by="operator",
        granted_at=granted_at,
        permit_id="permit-cnn-1",
    )
    cnn_two = create_boundary_permit(
        approval_id="approval-cnn-2",
        principal_id="telegram_chat",
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="www.cnn.com",
        granted_by="operator",
        granted_at=granted_at,
        permit_id="permit-cnn-2",
    )
    ccn = create_boundary_permit(
        approval_id="approval-ccn",
        principal_id="telegram_chat",
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="ccn.com",
        granted_by="operator",
        granted_at=granted_at,
        permit_id="permit-ccn",
    )
    store.add_boundary_permit(cnn_one)
    store.add_boundary_permit(cnn_two)
    store.add_boundary_permit(ccn)
    store.add_boundary_policy_rule(
        BoundaryPolicyRule(
            rule_id="rule-cnn",
            principal_id="telegram_chat",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.ALLOW,
            selector="https://www.cnn.com/",
            created_by="operator",
            created_at=granted_at,
        )
    )

    revoked = revoke_related_boundary_permission(
        store,
        permission_kind="boundary-permit",
        permission_id="permit-cnn-1",
    )

    assert revoked == 3
    assert store.get_boundary_permit("permit-cnn-1").revoked_at is not None
    assert store.get_boundary_permit("permit-cnn-2").revoked_at is not None
    assert store.get_boundary_policy_rule("rule-cnn").revoked_at is not None
    assert store.get_boundary_permit("permit-ccn").revoked_at is None


def test_revoke_block_rule_does_not_revoke_allow_permits():
    store = RuntimeStore()
    created_at = datetime(2026, 5, 1, 22, 39, tzinfo=UTC)
    store.add_boundary_permit(
        create_boundary_permit(
            approval_id="approval-cnn",
            principal_id="telegram_chat",
            boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
            selector="cnn.com",
            granted_by="operator",
            granted_at=created_at,
            permit_id="permit-cnn",
        )
    )
    store.add_boundary_policy_rule(
        BoundaryPolicyRule(
            rule_id="block-cnn",
            principal_id="telegram_chat",
            kind=BoundaryKind.OUTBOUND_NETWORK,
            mode=PolicyMode.DENY,
            selector="cnn.com",
            created_by="operator",
            created_at=created_at,
        )
    )

    revoked = revoke_related_boundary_permission(
        store,
        permission_kind="boundary-rule",
        permission_id="block-cnn",
    )

    assert revoked == 1
    assert store.get_boundary_policy_rule("block-cnn").revoked_at is not None
    assert store.get_boundary_permit("permit-cnn").revoked_at is None

from datetime import UTC, datetime
from pathlib import Path

from nullion.approval_decisions import approve_request_with_mode
from nullion.approvals import consume_boundary_permit, create_approval_request, create_boundary_permit
from nullion.policy import BoundaryKind
from nullion.runtime import PersistentRuntime
from nullion.runtime_store import RuntimeStore


def test_allow_once_boundary_approval_creates_single_use_permit(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 1, tzinfo=UTC),
    )
    approval = create_approval_request(
        requested_by="global:operator",
        action="allow_boundary",
        resource="cnn.com",
        request_kind="boundary_policy",
        context={
            "boundary_kind": BoundaryKind.OUTBOUND_NETWORK.value,
            "selector_candidates": {
                "allow_once": "cnn.com",
                "always_allow": "cnn.com",
            },
        },
    )
    runtime.store.add_approval_request(approval)

    approve_request_with_mode(runtime, approval.approval_id, mode="once", source="test")

    permits = runtime.store.list_boundary_permits()
    assert len(permits) == 1
    assert permits[0].selector == "cnn.com"
    assert permits[0].uses_remaining == 1


def test_permissions_dashboard_does_not_display_permit_quota_language():
    source = (Path(__file__).parents[1] / "src" / "nullion" / "web_app.py").read_text()
    assert "uses left" not in source


def test_consuming_legacy_multi_use_permit_retires_it_after_one_use():
    permit = create_boundary_permit(
        approval_id="approval-1",
        principal_id="global:operator",
        boundary_kind=BoundaryKind.OUTBOUND_NETWORK,
        selector="cnn.com",
        granted_by="operator",
        uses_remaining=20,
    )

    consumed = consume_boundary_permit(permit)

    assert consumed.uses_remaining == 0

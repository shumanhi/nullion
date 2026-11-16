"""Exact approval commits do not serialize unrelated history or revive decisions."""
from dataclasses import replace
from datetime import UTC, datetime
import sqlite3

from nullion.approvals import ApprovalStatus, create_approval_request
from nullion.runtime import bootstrap_persistent_runtime
from nullion.runtime_persistence import checkpoint_pending_approvals
from nullion.suspended_turns import SuspendedTurn


def seed(runtime):
    approval = create_approval_request(requested_by='telegram_chat', action='use_tool', resource='terminal_exec',
        request_kind='terminal_destructive_action', context={'command': 'rm fixture', 'tool_arguments': {'command': 'rm fixture'}})
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(SuspendedTurn(approval.approval_id, 'telegram:77', '77', '確認',
        'exact-request', '12', datetime.now(UTC),
        messages_snapshot=[{'role': 'user', 'content': '確認'}],
        pending_tool_calls=[{'id': 'exact-invocation', 'name': 'terminal_exec', 'input': {'command': 'rm fixture'}}]))
    return approval


def test_pending_approval_and_exact_continuation_are_atomic_without_full_history(tmp_path, monkeypatch):
    from nullion import runtime_persistence as persistence
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    approval = seed(runtime)
    def forbidden(*a, **kw):
        raise AssertionError('Full runtime history entered an approval commit')
    monkeypatch.setattr(persistence, 'build_runtime_store_payload', forbidden)
    checkpoint_pending_approvals(runtime.store, runtime.checkpoint_path, [approval.approval_id])
    monkeypatch.undo()
    reopened = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert reopened.store.get_approval_request(approval.approval_id) == approval
    assert reopened.store.get_suspended_turn(approval.approval_id) == runtime.store.get_suspended_turn(approval.approval_id)


def test_late_pending_commit_cannot_resurrect_decided_approval_or_continuation(tmp_path):
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    approval = seed(runtime)
    checkpoint_pending_approvals(runtime.store, runtime.checkpoint_path, [approval.approval_id])
    fresh = bootstrap_persistent_runtime(runtime.checkpoint_path)
    fresh.store.add_approval_request(replace(approval, status=ApprovalStatus.DENIED, decided_by='user', decided_at=datetime.now(UTC)))
    fresh.store.remove_suspended_turn(approval.approval_id)
    fresh.checkpoint(force=True)
    checkpoint_pending_approvals(runtime.store, runtime.checkpoint_path, [approval.approval_id])
    reopened = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert reopened.store.get_approval_request(approval.approval_id).status is ApprovalStatus.DENIED
    assert reopened.store.get_suspended_turn(approval.approval_id) is None

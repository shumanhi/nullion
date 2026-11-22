"""Dashboard history caps never hide native pending consent."""
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest


def seed_approval_history(runtime, *, history_count=105, pending_count=101):
    from nullion.approvals import ApprovalStatus, create_approval_request
    start = datetime(2020, 1, 1, tzinfo=UTC)
    history = []
    pending = []
    for index in range(history_count):
        approval = create_approval_request('web:history', 'use_tool', 'opaque_tool')
        approval = replace(approval, status=ApprovalStatus.DENIED,
            created_at=start + timedelta(seconds=index),
            decided_at=(start + timedelta(days=10) if index == 0 else start + timedelta(days=1, seconds=index)),
            decided_by='fixture')
        runtime.store.add_approval_request(approval)
        history.append(approval)
    for _ in range(pending_count):
        approval = create_approval_request('web:pending', 'use_tool', 'opaque_tool')
        runtime.store.add_approval_request(approval)
        pending.append(approval)
    runtime.checkpoint(force=True)
    return history, pending


@pytest.mark.parametrize('history_limit', [0, 2, 100])
def test_status_preserves_all_pending_and_caps_only_recent_decisions(tmp_path, monkeypatch, history_limit):
    from fastapi.testclient import TestClient
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_WEB_STATUS_APPROVALS_LIMIT', str(history_limit))
    monkeypatch.setenv('NULLION_WEB_STATUS_CACHE_TTL_S', '0')
    history, pending = seed_approval_history(runtime)
    payload = TestClient(create_app(runtime, orchestrator=None, registry=ToolRegistry())).get('/api/status?full=1').json()
    shown = payload['approvals']
    assert {a['approval_id'] for a in shown if a['status'] == 'pending'} == {a.approval_id for a in pending}
    shown_history = [a for a in shown if a['status'] != 'pending']
    retained = history_limit or len(history)
    assert len(shown_history) == retained
    assert len(shown) == len(pending) + retained
    if history_limit:
        assert shown_history[0]['approval_id'] == history[0].approval_id
        assert payload['status_meta']['truncated']['approvals'] == {'shown': len(shown), 'total': len(history) + len(pending)}
    else:
        assert 'approvals' not in payload['status_meta']['truncated']
    assert all(runtime.store.get_approval_request(a.approval_id).status.value == 'pending' for a in pending)

"""Verified approval display survives optional UI-correlation write failures."""
from datetime import UTC, datetime
from pathlib import Path
import threading
import time

import pytest


def native_delete_review(tmp_path, monkeypatch, *, binding='old-turn'):
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolExecutor, ToolInvocation, ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path,monkeypatch)
    selected = tmp_path/'選択したファイル.txt'
    selected.write_text('Preserve until the human approves')
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec('terminal_exec','Run the selected command',ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC,True,5),lambda invocation:pytest.fail('Display must never execute a tool'))
    result = ToolExecutor(store=runtime.store,registry=registry).invoke(ToolInvocation(
        'native-delete','terminal_exec','web:review',{'command':"rm -f -- '"+str(selected)+"' 2>/dev/null"}))
    aid = result.output['approval_id']
    runtime.store.add_suspended_turn(SuspendedTurn(approval_id=aid,conversation_id='web:review',chat_id=None,
        message='/chat 確認',request_id='request-native',message_id=None,created_at=datetime.now(UTC),
        mission_id=None,pending_step_idx=None,messages_snapshot=[],pending_tool_calls=[],
        resume_token={'conversation_turn_id':binding}))
    runtime.checkpoint(force=True)
    return runtime, aid, selected


def test_native_delete_review_survives_real_checkpoint_contention_and_skips_unchanged_binding(tmp_path, monkeypatch):
    from nullion.runtime_persistence import _runtime_store_file_lock
    from nullion.runtime_checkpoints import wait_runtime_checkpoints
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.web_app import _web_approval_required_payload
    runtime, aid, selected = native_delete_review(tmp_path,monkeypatch)
    held, release = threading.Event(), threading.Event()
    def writer():
        with _runtime_store_file_lock(runtime.checkpoint_path):
            held.set()
            assert release.wait(10)
    thread = threading.Thread(target=writer)
    thread.start()
    assert held.wait(3)
    conflict = {'approval_id':aid,'is_web_request':True,'request_kind':'boundary_policy',
        'tool_name':'web_fetch','tool_detail':'https://untrusted.example.test/transport-secret',
        'trigger_flow_label':'Unverified transport label'}
    try:
        started = time.perf_counter()
        payload = _web_approval_required_payload(conflict,runtime=runtime,turn_id='native-current-turn')
        elapsed = time.perf_counter()-started
        assert elapsed < .8, elapsed
        assert payload['approval_id']==aid and payload['request_kind']=='terminal_destructive_action'
        assert payload['is_web_request'] is False
        assert selected.name in payload['tool_detail'] and 'transport-secret' not in str(payload)
        assert 'delete' in payload['display_title'].lower()
        assert runtime.store.get_approval_request(aid).status.value=='pending'
        assert runtime.store.get_suspended_turn(aid).resume_token['conversation_turn_id']=='native-current-turn'
        # The same already-bound request must not touch the locked checkpoint.
        started = time.perf_counter()
        again = _web_approval_required_payload(conflict,runtime=runtime,turn_id='native-current-turn')
        assert time.perf_counter()-started < .08
        assert again==payload and selected.is_file()
    finally:
        release.set()
        thread.join(3)
        assert wait_runtime_checkpoints(runtime,timeout=5)
    restored = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert restored.store.get_approval_request(aid).status.value=='pending'
    assert restored.store.get_suspended_turn(aid).resume_token['conversation_turn_id']=='native-current-turn'


@pytest.mark.parametrize('kind', ['email','web'])
def test_verified_approval_scope_overrides_conflicting_transport_fields(tmp_path,monkeypatch,kind):
    from nullion.approvals import create_approval_request
    from nullion.approval_display import approval_display_from_request
    from nullion.web_app import _web_approval_required_payload
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    runtime = isolate(tmp_path,monkeypatch)
    approval = create_approval_request(requested_by='web:review',
        action='use_tool' if kind=='email' else 'allow_boundary',
        resource='email_send' if kind=='email' else 'https://native.example.test',
        request_kind='capability_grant' if kind=='email' else 'boundary_policy',
        context=({'tool_name':'email_send','tool_arguments':{'to':'reader@example.test',
            'subject':'Native reviewed subject','body':'https://example.test is report content'}} if kind=='email' else
            {'boundary_kind':'outbound_network','operation':'http_get','target':'https://native.example.test'}))
    runtime.store.add_approval_request(approval)
    display = approval_display_from_request(approval)
    payload = _web_approval_required_payload({'approval_id':approval.approval_id,
        'request_kind':'stale-kind','tool_name':'stale-tool','tool_detail':'stale-target',
        'is_web_request':not display.is_web_request},runtime=runtime)
    assert payload['request_kind']==approval.request_kind
    assert payload['tool_name']==display.label and payload['tool_detail']==display.detail
    assert payload['is_web_request'] is display.is_web_request

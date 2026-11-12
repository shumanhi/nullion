"""Scheduler authorization resolves references from bounded verified history."""
import json
from types import SimpleNamespace

import pytest

from nullion.runtime_store import RuntimeStore
from nullion.scheduler_deletion_contract import (
    _recent_scheduler_context, scheduler_deletion_failure_reply, scheduler_deletion_guard_result,
)
from nullion.tools import ToolInvocation, ToolResult


def history(store, *, conversation='telegram:202', workspace='workspace_admin', tool='create_cron',
            status='completed', output=None, request='Watch the sensor.', reply='Created sensor watcher.'):
    store.add_conversation_event({'conversation_id': conversation, 'workspace_id': workspace,
        'event_type': 'conversation.chat_turn', 'turn_id': 'created', 'user_message': request,
        'assistant_reply': reply, 'tool_results': [{'tool_name': tool, 'status': status,
            'output': output if output is not None else {'id': 'watch-a'}}]})


@pytest.fixture
def jobs(monkeypatch):
    records = {'watch-a': SimpleNamespace(name='Sensor watcher', workspace_id='workspace_admin'),
               'other': SimpleNamespace(name='Other watcher', workspace_id='workspace_other')}
    monkeypatch.setattr('nullion.crons.get_cron', records.get)
    return records


@pytest.mark.parametrize('prompt', ['Cancel it', 'Cancélalo', 'それを取り消して', 'ألغِه'])
def test_guard_receives_verified_creation_context_in_any_language(jobs, prompt):
    store = RuntimeStore(); history(store)
    class Model:
        def create(self, **kw):
            data = json.loads(kw['messages'][0]['content'][0]['text'])
            assert data['user_request'] == prompt
            assert data['recent_conversation'][0]['scheduler_object_ids'] == ['watch-a']
            assert data['recent_conversation'][0]['assistant_reply'] == 'Created sensor watcher.'
            return {'content': json.dumps({'authorized': True, 'target_id': 'watch-a',
                'user_evidence': prompt, 'target_context_ref': 'recent:0', 'reason': 'authorized'})}
    assert scheduler_deletion_guard_result(ToolInvocation('cancel', 'delete_cron', 'telegram:202', {'id': 'watch-a'}),
        user_message=prompt, model_client=Model(), runtime_store=store, conversation_id='telegram:202') is None


@pytest.mark.parametrize('context_ref', ['invented', 'recent:1', 1, ['recent:0']])
def test_unverified_history_reference_cannot_authorize(jobs, context_ref):
    store = RuntimeStore(); history(store)
    class Model:
        def create(self, **kw):
            return {'content': json.dumps({'authorized': True, 'target_id': 'watch-a',
                'user_evidence': '1', 'target_context_ref': context_ref})}
    result = scheduler_deletion_guard_result(ToolInvocation('cancel', 'delete_cron', 'telegram:202', {'id': 'watch-a'}),
        user_message='1', model_client=Model(), runtime_store=store, conversation_id='telegram:202')
    assert result.output['reason'] == 'scheduler_delete_not_user_authorized'


@pytest.mark.parametrize('variant', ['other-conversation', 'other-workspace', 'failed', 'missing', 'reset'])
def test_context_isolation_and_failed_receipts(jobs, variant):
    store = RuntimeStore()
    history(store, conversation='telegram:303' if variant == 'other-conversation' else 'telegram:202',
        workspace='workspace_other' if variant == 'other-workspace' else 'workspace_admin',
        status='failed' if variant == 'failed' else 'completed',
        output={'id': 'missing'} if variant == 'missing' else None)
    if variant == 'reset':
        store.add_conversation_event({'conversation_id': 'telegram:202', 'event_type': 'conversation.session_reset'})
    assert not any(row['scheduler_object_ids'] for row in _recent_scheduler_context(store, 'telegram:202', 'telegram:202'))


def test_recent_packet_preserves_intervening_subjects_but_not_raw_payloads(jobs):
    store = RuntimeStore()
    history(store, output={'id': 'watch-a', 'task': 'PRIVATE INTERNAL TASK', 'credential': 'SECRET'})
    for i in range(8):
        history(store, tool='file_write', output={'path': '/private/SECRET'},
            request=f'Document revision {i}', reply=f'Revision {i} saved')
    rows = _recent_scheduler_context(store, 'telegram:202', 'telegram:202')
    assert len(rows) == 6
    assert rows[-1]['assistant_reply'] == 'Revision 7 saved'
    assert all(not row['scheduler_object_ids'] for row in rows)
    assert 'SECRET' not in json.dumps(rows)


def test_numbered_clarification_retains_verified_target(jobs):
    store = RuntimeStore()
    history(store, tool='delete_cron', status='failed', output={
        'reason': 'scheduler_delete_not_user_authorized', 'authorization_status': 'ambiguous_target',
        'target_id': 'watch-a'}, request='Cancel that', reply='1. Cancel Sensor watcher.\n2. Keep it running.')
    rows = _recent_scheduler_context(store, 'telegram:202', 'telegram:202')
    assert rows[0]['scheduler_object_ids'] == ['watch-a']
    assert '1. Cancel' in rows[0]['assistant_reply']


def test_guard_failure_has_action_appropriate_fallback_and_no_recovery_widening(jobs):
    from nullion.agent_orchestrator import _missing_scope_action_final_reply, _maybe_widen_scope_after_repeated_tool_failure
    denied = ToolResult('cancel', 'delete_cron', 'failed', {'reason': 'scheduler_delete_not_user_authorized',
        'target_name': 'Sensor watcher', 'authorization_status': 'ambiguous_target'})
    reply = _missing_scope_action_final_reply('scheduler mutation', tool_results=[denied])
    assert 'haven’t canceled anything' in reply and '1. Cancel “Sensor watcher”' in reply and '2. Keep' in reply
    assert 'schedule or task detail' not in reply
    assert _maybe_widen_scope_after_repeated_tool_failure({}, result=denied, tool_registry=object(),
        tool_results=[denied], tool_recovery_scopes_attempted=[]) is None
    assert scheduler_deletion_failure_reply([denied, ToolResult('ok', 'toggle_cron', 'completed', {})]) is None


def test_missing_model_and_store_errors_return_named_safe_failure(jobs):
    invocation = ToolInvocation('cancel', 'delete_cron', 'telegram:202', {'id': 'watch-a'})
    result = scheduler_deletion_guard_result(invocation, user_message='Cancel it', model_client=None)
    assert 'Sensor watcher' in scheduler_deletion_failure_reply([result])
    class Store:
        def list_recent_conversation_events_after_reset(self, *a, **kw):
            raise OSError('fixture storage unavailable')
    result = scheduler_deletion_guard_result(invocation, user_message='Cancel it',
        model_client=SimpleNamespace(create=lambda **kw: pytest.fail('No call after storage failure')),
        runtime_store=Store(), conversation_id='telegram:202')
    assert result.output['reason'] == 'scheduler_delete_authorization_unavailable'


def test_materialized_backend_approval_keeps_canonical_task_and_typed_metadata(tmp_path, monkeypatch):
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolExecutor, build_default_tool_registry, register_cron_tools
    from nullion.chat_operator import _materialize_pending_tool_approval_for_chat
    from nullion.approval_display import approval_display_from_request
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path / 'runtime.db')
    main = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    worker = bootstrap_persistent_runtime(tmp_path / 'worker.db')
    job = crons.add_cron('Sensor watcher', '0 12 * * *', 'Read sensor')
    registry = build_default_tool_registry(workspace_root=tmp_path, allowed_roots=[tmp_path])
    register_cron_tools(registry)
    result = ToolExecutor(store=worker.store, registry=registry).invoke(
        ToolInvocation('cancel', 'delete_cron', 'web:202', {'id': job.id}))
    assert result.output['scheduler_target']['name'] == job.name
    assert not main.store.list_approval_requests()
    # The outer chat view no longer has the scoped tool; recreate from its typed receipt.
    aid = _materialize_pending_tool_approval_for_chat(main, tool_results=[result],
        conversation_id='web:202', chat_id='web:202', prompt='Cancel it', request_id='cancel',
        message_id=None, principal_id='web:202', registry=None)
    approval = main.store.get_approval_request(aid)
    assert approval.context['scheduler_target'] == {'id': job.id, 'name': job.name}
    assert approval.context['tool_side_effect_class'] == 'write'
    display = approval_display_from_request(approval)
    assert job.name in display.detail and 'Delete this scheduled task?' in display.title
    assert crons.get_cron(job.id) is not None

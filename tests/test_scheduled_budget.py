from datetime import UTC, datetime
from dataclasses import replace
from pathlib import Path

import pytest

from nullion import crons
from nullion.agent_orchestrator import TurnResult
from nullion.agent_turn_limits import max_iterations_from_resume_token
from nullion.approval_decisions import approve_request_with_mode
from nullion.cron_execution_tools import CronConnectorScopeDecision
from nullion.runtime import bootstrap_persistent_runtime
from nullion.scheduled_budget import (
    job_binding, store_scheduled_budget_approval, restore_result, resume_scheduled_budget_approval,
)
from nullion.tools import ToolResult


def saved(tmp_path, monkeypatch):
    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_DATA_DIR', str(tmp_path/'data'))
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path/'runtime.db')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    job = crons.CronJob('selected', '確認', '* * * * *', '残りの記録を確認。',
        delivery_channel='web', delivery_target='web:selected')
    crons.save_crons([job])
    row = ToolResult('source-1', 'opaque_read', 'completed', {'value': 7})
    action = ToolResult('write-1', 'opaque_write', 'completed', {'action_receipt': {'object_id': 'kept'}})
    (tmp_path/'prepared.html').write_text('<!doctype html><html><body>Selected value: 7.</body></html>')
    turn = TurnResult('turn-1', 'Selected value: 7.', tool_results=[row, action], reached_iteration_limit=True,
        response_fulfilled=False, work_usage={'model_calls': 4},
        artifacts=[str(tmp_path/'prepared.html')],
        messages_snapshot=[{'role': 'user', 'content': '選択した記録を確認。'}],
        invocation_metadata={'source-1': {'arguments': {'id': 'selected'}, 'tool_use_id': 'source'}},
        completed_invocation_signatures=['write-signature'], completed_action_results={'write-signature': action},
        completed_read_results={'source-signature': row})
    from nullion.agent_orchestrator import _CompletionReviewHandoff
    from nullion.completion_pipeline import capture_recovery
    from nullion.execution_outcome import prepare_answer
    recovery = capture_recovery({'conversation_id': 'web:selected', 'principal_id': 'web:selected',
        'completion_review_count': 2, 'completion_composition_count': 1,
        'completion_review_progress_fingerprints': ['selected-proof']}, job.task)
    turn.completion_review_handoff = _CompletionReviewHandoff({}, None, None, recovery)
    turn.prepared_answer = prepare_answer(job.task, turn.final_text, turn.tool_results, turn.artifacts)
    ident = store_scheduled_budget_approval(runtime, job=job, original_job_binding=job_binding(job),
        conversation_id='web:selected', result=turn, max_iterations=4,
        flow_context={'scheduled_task_run': True, 'browser_session_id': 'saved-tab'},
        connector_scope=CronConnectorScopeDecision(valid=True), tool_names=['opaque_read', 'opaque_write'],
        progress_text='Selected value: 7.')
    return runtime, job, ident


def approve(runtime, ident):
    approve_request_with_mode(runtime, approval_id=ident, mode='limit_2x', source='fixture')
    runtime.checkpoint()
    return runtime.store.get_suspended_turn(ident)


def test_checkpoint_survives_restart_and_retains_actions_reads_and_browser_scope(tmp_path, monkeypatch):
    runtime, job, ident = saved(tmp_path, monkeypatch)
    approve(runtime, ident)
    restarted = bootstrap_persistent_runtime(runtime.checkpoint_path)
    suspended = restarted.store.get_suspended_turn(ident)
    assert max_iterations_from_resume_token(suspended.resume_token) == 8
    checkpoint = suspended.resume_token['scheduled_execution']
    restored = restore_result(checkpoint['result'], suspended.messages_snapshot)
    recovery = restored.completion_review_handoff.recovery_checkpoint
    assert recovery.attempts == 2 and recovery.compositions == 1 and recovery.progress == ('selected-proof',)
    from nullion.execution_outcome import owned_prepared_answer
    assert owned_prepared_answer(restored, job.task) is not None
    assert restored.completed_action_results['write-signature'].output['action_receipt']['object_id'] == 'kept'
    assert restored.completed_read_results['source-signature'].output['value'] == 7
    assert restored.invocation_metadata['source-1']['arguments'] == {'id': 'selected'}
    assert restored.artifacts == [str(tmp_path/'prepared.html')]
    assert Path(restored.artifacts[0]).read_text() == '<!doctype html><html><body>Selected value: 7.</body></html>'
    assert checkpoint['tool_names'] == ['opaque_read', 'opaque_write']
    assert checkpoint['connector_scope']['write_tool_names'] == []
    assert checkpoint['flow_context']['browser_session_id'] == 'saved-tab'
    calls = []
    restarted.scheduled_budget_runner = lambda current, continuation: calls.append(continuation) or {'cron_delivery_status': 'saved'}
    assert resume_scheduled_budget_approval(restarted, suspended) is None
    assert resume_scheduled_budget_approval(restarted, suspended) is None
    assert len(calls) == 1 and calls[0].max_iterations == 8
    assert crons.get_cron(job.id).enabled and crons.get_cron(job.id).schedule == job.schedule
    assert crons.get_cron(job.id).last_result == 'ok'


@pytest.mark.parametrize('mutation', ['task', 'workspace', 'delivery', 'run', 'disabled', 'snapshot', 'evidence', 'budget', 'denied', 'deleted'])
def test_changed_denied_or_superseded_checkpoint_never_executes(tmp_path, monkeypatch, mutation):
    runtime, job, ident = saved(tmp_path, monkeypatch)
    suspended = approve(runtime, ident)
    if mutation == 'task': job.task = 'otro objetivo'
    if mutation == 'workspace': job.workspace_id = 'other'
    if mutation == 'delivery': job.delivery_target = 'web:other'
    if mutation == 'run': job.last_run = datetime.now(UTC).isoformat()
    if mutation == 'disabled': job.enabled = False
    if mutation in {'task', 'workspace', 'delivery', 'run', 'disabled'}: crons.save_crons([job])
    if mutation == 'deleted': crons.save_crons([], allow_empty=True)
    if mutation == 'snapshot': suspended.messages_snapshot.append({'role': 'user', 'content': 'Different scope'})
    if mutation == 'evidence': suspended.resume_token['scheduled_execution']['result']['tool_results'][0]['output']['value'] = 99
    if mutation == 'budget': suspended.resume_token['max_iterations'] = 999
    if mutation == 'denied':
        from nullion.approvals import ApprovalStatus
        runtime.store.add_approval_request(replace(runtime.store.get_approval_request(ident), status=ApprovalStatus.DENIED))
    calls = []
    runtime.scheduled_budget_runner = lambda *a: calls.append(a)
    assert resume_scheduled_budget_approval(runtime, suspended)
    assert not calls


def test_approval_progress_is_public_and_numbered(tmp_path, monkeypatch):
    runtime, _, ident = saved(tmp_path, monkeypatch)
    from nullion.telegram_app import _approval_card_text
    card = _approval_card_text(runtime.store.get_approval_request(ident))
    assert card.count('Selected value: 7.') == 1
    assert all(f'{index}. ' in card for index in range(1, 5))
    assert 'source-1' not in card and 'saved-tab' not in card and 'write-signature' not in card


def test_busy_run_can_retry_without_spending_the_approval(tmp_path, monkeypatch):
    runtime, job, ident = saved(tmp_path, monkeypatch)
    suspended = approve(runtime, ident)
    calls = []
    runtime.scheduled_budget_runner = lambda *a: calls.append(a) or {'cron_delivery_status': 'saved'}
    # A different thread/process owns this lease; do not reenter its thread-local token.
    from nullion.sqlite_connections import sqlite_connection
    from nullion.cron_scheduler_state import expiry, connection
    import os
    with connection(runtime.checkpoint_path) as c:
        c.execute('INSERT INTO cron_execution_leases VALUES (?,?,?)', (job.id, f'manual:{os.getpid()}:fixture', expiry()))
    assert 'already running' in resume_scheduled_budget_approval(runtime, suspended)
    assert not calls
    with sqlite_connection(runtime.checkpoint_path) as c:
        c.execute('DELETE FROM cron_execution_leases')
    assert resume_scheduled_budget_approval(runtime, suspended) is None and len(calls) == 1


def test_checkpoint_failure_never_publishes_a_usable_budget_approval(tmp_path, monkeypatch):
    from nullion.agent_turn_limits import store_budget_approval
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    def fail(self):
        raise OSError('fixture disk failure')
    monkeypatch.setattr(type(runtime), 'checkpoint', fail)
    with pytest.raises(OSError, match='disk failure'):
        store_budget_approval(runtime, conversation_id='web:selected', message='確認。',
            messages_snapshot=[], current_max_iterations=4, tool_result_count=2)
    approvals = runtime.store.list_approval_requests()
    assert len(approvals) == 1 and approvals[0].status.value == 'denied'
    assert runtime.store.get_suspended_turn(approvals[0].approval_id) is None


@pytest.mark.parametrize('result,expected', [
    ({'suspended_for_approval': True}, 'waiting_for_approval'),
    ({'cron_run_cancelled': True}, 'cancelled'),
    ({'execution_outcome': {'status': 'partially_succeeded', 'execution_succeeded': False}}, 'partial_success'),
    ({'cron_delivery_failed': True, 'reason': 'platform_rejected'}, 'error: platform_rejected'),
    ({'execution_outcome': {'execution_succeeded': False}}, 'error: scheduled task incomplete'),
    ({'cron_delivery_partial_success': True}, 'partial_success'),
    ({'execution_outcome': {'execution_succeeded': True}}, 'ok'),
    ({'execution_outcome': 'invalid'}, 'error: invalid execution outcome'),
    ({'reached_iteration_limit': True}, 'error: cron_run_reached_iteration_limit'),
])
def test_all_scheduled_entrypoints_share_one_outcome_policy(result, expected):
    from nullion.cron_scheduler_state import execution_result_label
    assert execution_result_label(result) == expected


def test_manual_tool_reports_budget_approval_instead_of_iteration_failure(tmp_path, monkeypatch):
    from nullion.tools import _build_run_cron_handler, ToolInvocation
    runtime, job, ident = saved(tmp_path, monkeypatch)
    result = _build_run_cron_handler(lambda _: {'suspended_for_approval': True,
        'approval_id': ident, 'reached_iteration_limit': True,
        'execution_outcome': {'execution_succeeded': False},
        'cron_delivery_status': 'paused_for_approval'})(
            ToolInvocation('selected-run', 'run_cron', 'workspace:workspace_admin', {'id': job.id}))
    assert result.status == 'denied'
    assert result.output['reason'] == 'approval_required'
    assert result.output['approval_id'] == ident
    assert crons.get_cron(job.id).last_result == 'waiting_for_approval'


def test_manual_tool_keeps_delivered_partial_work_without_marking_it_failed(tmp_path, monkeypatch):
    from nullion.tools import _build_run_cron_handler, ToolInvocation
    runtime, job, _ = saved(tmp_path, monkeypatch)
    result = _build_run_cron_handler(lambda _: {'reached_iteration_limit': True,
        'execution_outcome': {'status': 'partially_succeeded', 'execution_succeeded': False},
        'cron_delivery_status': 'partial_success'})(
            ToolInvocation('selected-run', 'run_cron', 'workspace:workspace_admin', {'id': job.id}))
    assert result.status == 'completed'
    assert crons.get_cron(job.id).last_result == 'partial_success'

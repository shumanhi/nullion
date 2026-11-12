"""Real SQLite, runner, approval route and Web delivery; external model/data faked."""
import json
import os
import threading
import time

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, run_scheduled, tool_response, text_response, telegram_boundary
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('task', ['選択した{count}件の記録を確認して報告。', 'Revisa los {count} registros seleccionados y resume sus valores.'])
@pytest.mark.parametrize('target_count', [6, 24])
@pytest.mark.parametrize('surface', ['web', 'telegram'])
@pytest.mark.parametrize('with_action', [False, True])
@pytest.mark.parametrize('manual', [False, True])
def test_budget_approval_resumes_same_run_through_web_api_once(tmp_path, monkeypatch, task, record_property, surface, with_action, target_count, manual):
    task = task.format(count=target_count)
    from fastapi.testclient import TestClient
    from nullion import crons, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'absent.env'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_KEY_STORAGE', 'file')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_CRON_AGENT_MAX_ITERATIONS', '2')
    monkeypatch.setenv('NULLION_TELEGRAM_BOT_TOKEN', 'synthetic-token')
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: None)
    runtime = setup_runtime(tmp_path, monkeypatch)
    reads = []
    writes = []
    deliveries, _ = telegram_boundary(monkeypatch)
    resume_started = threading.Event()
    release = threading.Event()
    registry = ToolRegistry()
    def read(inv):
        record = inv.arguments['record']
        reads.append(record)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'text': f'Record {record}: value 7.'})
    registry.register(ToolSpec('browser_extract_text', 'Read one selected record', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, input_schema={'type': 'object',
            'properties': {'record': {'type': 'string'}}, 'required': ['record']}), read)
    if with_action:
        def update(inv):
            writes.append(inv)
            return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'action_receipt': {'object_id': 'saved-setting'}})
        registry.register(ToolSpec('opaque_update', 'Apply the requested setting', ToolRiskLevel.LOW,
            ToolSideEffectClass.WRITE, False, 2, replay_safe_after_completion=True), update)
        task += '\n指定の設定を一度だけ更新。' if task.startswith('選択') else '\nActualiza una vez el ajuste seleccionado.'
    class Provider:
        replay_requested = False
        def create(self, **kw):
            system = kw.get('system') or ''
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [
                        {'requirement': f'Check all {target_count} selected records.', 'requirement_kind': 'fact',
                         'evidence_subject_id': 'selected-records', 'eligible_evidence_tool_names': ['browser_extract_text']}]}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                complete = len(reads) == target_count
                return text_response(json.dumps({'disposition': 'complete' if complete else 'retry',
                    'all_material_requirements_enumerated': True,
                    'unresolved_requirements': [] if complete else ['Check the remaining selected records.'],
                    'retry_tool_names': [] if complete else ['browser_extract_text'],
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': complete,
                        'draft_covered': complete, 'evidence_invocation_ids': [r['invocation_id'] for r in packet['tool_evidence']
                            if r['tool_name'] == 'browser_extract_text' and r['status'] == 'completed']}
                        for row in packet['material_requirement_contract']]}))
            if 'You repair scheduled-task final answers' in system:
                return text_response(json.dumps({'requirements_satisfied': len(reads) == target_count,
                    'source_state': 'available', 'evidence_refs': ['e0'], 'delivery_decision': 'send',
                    'delivery_reason': 'report', 'remaining_work': [] if len(reads) == target_count else ['Check the remaining selected records.'],
                    'report': f'{len(reads)} selected records checked; each has value 7.'}))
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None:
                return protocol
            assert kw.get('tools'), 'A new unscoped setup/model path was introduced'
            if with_action and not writes:
                return tool_response('opaque_update', {})
            if len(reads) == (3 if with_action else 4):
                resume_started.set()
                release.wait(timeout=2)
                if with_action and not self.replay_requested:
                    self.replay_requested = True
                    return tool_response('opaque_update', {})
            if len(reads) < target_count:
                return tool_response('browser_extract_text', {'record': 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'[len(reads)]})
            return text_response(f'All {target_count} selected records were checked. Each has value 7.')
    provider = Provider()
    runtime.model_client = provider
    job = crons.CronJob('budget-case', '確認', '* * * * *', task,
        delivery_channel=surface, delivery_target='web:budget-case' if surface == 'web' else '123')
    crons.save_crons([job])
    origin = 'web:budget-case' if surface == 'web' else 'telegram:123'
    agent_conversation = origin+':background:selected-run' if manual else None
    first = run_scheduled(job, provider, runtime, registry, lambda *a: pytest.fail('No delivery before approval'),
        agent_conversation_id=agent_conversation)
    assert first['suspended_for_approval'] and reads == list('ABC' if with_action else 'ABCD'), {key: value for key, value in first.items()
        if key not in {'tool_results', 'response_presentation', '_cron_retained_text_report'}}
    ident = first['approval_id']
    before = runtime.store.get_suspended_turn(ident).resume_token['scheduled_execution']
    assert before['result']['work_usage']['model_calls'] == 4
    assert before['flow_context']['browser_session_id']
    assert before['conversation_id'] == origin
    assert before['agent_conversation_id'] == (agent_conversation or origin)
    # Start a fresh runtime to prove this is persisted state, not a live agent.
    from nullion.runtime import bootstrap_persistent_runtime
    runtime = bootstrap_persistent_runtime(runtime.checkpoint_path)
    app = web_app.create_app(runtime, AgentOrchestrator(model_client=provider), registry)
    with TestClient(app) as client:
        started = time.perf_counter()
        response = client.post('/api/approve/'+ident, json={'mode': 'limit_2x'})
        elapsed = time.perf_counter() - started
        assert response.status_code == 200, response.text
        record_property('approval_response_seconds', elapsed)
        assert elapsed < 1, 'Approval waited for the scheduled model instead of queuing saved work'
        assert resume_started.wait(1)
        duplicate = client.post('/api/approve/'+ident, json={'mode': 'limit_2x'})
        assert duplicate.json()['already_handled'] is True
        release.set()
        deadline = time.monotonic() + 5
        messages = []
        while time.monotonic() < deadline:
            if surface == 'web':
                history = client.get('/api/chat/history/web:budget-case?sync_runtime=1').json()
                messages = [m for m in history['messages'] if m.get('role') == 'bot' and f'{target_count} selected records' in m.get('text', '')]
            else:
                history = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.chat_turn')
                messages = [{'text': row['assistant_reply']} for row in history
                    if f'{target_count} selected records' in row.get('assistant_reply', '')]
            from nullion.scheduled_budget import _current_job
            current_job = _current_job(runtime, job.id)
            if runtime.store.get_suspended_turn(ident) is None and ((messages and current_job.last_result == 'ok')
                    or target_count == 24 and current_job.last_result == 'waiting_for_approval'):
                break
            time.sleep(.02)
        if target_count == 24:
            pending = [r for r in runtime.store.list_approval_requests() if r.status.value == 'pending'
                and r.request_kind == 'agent_turn_limit_extension']
            assert len(pending) == 1
            next_turn = runtime.store.get_suspended_turn(pending[0].approval_id)
            assert next_turn.resume_token['scheduled_execution']['result']['work_usage']['model_calls'] == 12
            assert next_turn.resume_token['current_max_iterations'] == 8
            assert len(writes) == (1 if with_action else 0)
            return
        assert reads == list('ABCDEF'), response.text
        assert len(messages) == 1, json.dumps({'history': history, 'deliveries': deliveries,
            'suspended': runtime.store.get_suspended_turn(ident) is not None,
            'turns': runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.chat_turn'),
            'events': [{'type': event.event_type, 'payload': event.payload} for event in runtime.store.list_events()[-8:]]}, default=str)
        assert 'value 7' in messages[0]['text']
        repeated = client.post('/api/approve/'+ident, json={'mode': 'limit_2x'})
        assert repeated.status_code == 200 and repeated.json()['already_handled'] is True
        assert reads == list('ABCDEF')
    assert current_job.enabled and current_job.schedule == job.schedule
    assert current_job.last_result == 'ok'
    assert runtime.store.get_suspended_turn(ident) is None
    assert len(writes) == (1 if with_action else 0), 'A retained successful action was executed twice'
    if surface == 'telegram':
        assert len(deliveries) == 1 and f'{target_count} selected records' in deliveries[0]['text']


def test_task_changed_after_queue_delivers_the_terminal_notice_without_executing(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import crons, web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import _manual_cron_background_serial_lock
    from nullion.tools import ToolRegistry
    from tests.test_scheduled_budget import saved
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'absent.env'))
    monkeypatch.setenv('NULLION_KEY_STORAGE', 'file')
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setattr(chat_store, '_store', None)
    monkeypatch.setattr(crons.CronScheduler, 'start', lambda self: None)
    runtime, job, ident = saved(tmp_path, monkeypatch)
    class Provider:
        def create(self, **kw):
            pytest.fail('Superseded work must not start a model or an action')
    app = web_app.create_app(runtime, AgentOrchestrator(model_client=Provider()), ToolRegistry())
    with TestClient(app) as client:
        lock = _manual_cron_background_serial_lock('web:selected')
        with lock:
            response = client.post('/api/approve/'+ident, json={'mode': 'limit_2x'})
            assert response.status_code == 200
            job.task = '別の依頼。'
            crons.save_crons([job])
        deadline = time.monotonic()+5
        notices = []
        while time.monotonic() < deadline:
            history = client.get('/api/chat/history/web:selected?sync_runtime=1').json()
            notices = [row for row in history['messages'] if row.get('role') == 'bot'
                and 'older continuation was not started' in row.get('text', '')]
            if notices:
                break
            time.sleep(.02)
        assert len(notices) == 1, history
        assert crons.get_cron(job.id).enabled

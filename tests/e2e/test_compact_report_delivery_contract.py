"""Real scheduler, review, SQLite and Telegram boundary; provider/network fakes only."""
import json
import os
from pathlib import Path

import pytest

from tests.report_presentation_fixture import report_fixture
from tests.workflow_provider_protocol import workflow_provider_reply
from tests.e2e.test_scheduled_report_completion import (
    setup_runtime, telegram_boundary, run_scheduled, text_response, tool_response,
)

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E only')


@pytest.mark.parametrize('preference', ['要点だけを簡潔に報告してください。', 'Solo los resultados esenciales, de forma breve.'])
def test_saved_report_output_edit_is_used_by_next_run_without_changing_execution(tmp_path, monkeypatch, preference):
    from fastapi.testclient import TestClient
    from nullion import crons, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    recipe = (Path(__file__).resolve().parents[1] / 'fixtures/concise_email_triage_task.txt').read_text().strip()
    execution, _ = recipe.split('Output instructions (current user preference):', 1)
    old_task = execution + '- Include sender, subject, message date, due date, amount or obligation when available, and whether a calendar event was created or skipped.'
    updated_task = recipe + '\n' + preference
    registry = ToolRegistry(); reads = []
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', {'value': 7, 'amount': 19,
            'due_date': '2030-11-07', 'status': 'scheduled', 'posting_confirmed': False})
    registry.register(ToolSpec('record_read', 'Read owned snapshot facts', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        producer_calls = 0
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            user_text = '\n'.join(block.get('text', '') for row in kw['messages'] if row.get('role') == 'user'
                for block in (row['content'] if isinstance(row['content'], list) else [{'text': row['content']}]))
            assert updated_task in user_text
            assert '- Include sender, subject, message date' not in user_text
            self.producer_calls += 1
            if not reads: return tool_response('record_read', {})
            return text_response('Nov 7 — $19 scheduled; posting unconfirmed. Calendar: 0 new events.')
    provider = Provider(); runtime.model_client = provider
    with TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=provider), registry)) as client:
        response = client.post('/api/crons', json={'name': 'Saved report', 'task': old_task,
            'schedule': '0 0 1 1 *', 'enabled': True, 'delivery_channel': 'telegram', 'delivery_target': '123',
            'schedule_timezone': 'UTC'})
        assert response.status_code == 200, response.text
        before = response.json()['job']; ident = before['id']
        # Warm the persisted read before editing to catch stale task reuse.
        assert crons.get_cron(ident).task == old_task
        response = client.patch('/api/crons/' + ident, json={'task': updated_task})
        assert response.status_code == 200, response.text
        after = response.json()['job']
        for key in ['id', 'name', 'schedule', 'enabled', 'schedule_timezone', 'delivery_channel',
                'delivery_target', 'workspace_id', 'artifact_delivery_options', 'execution_window']:
            assert after.get(key) == before.get(key), key
        job = crons.get_cron(ident)
        assert job.task == updated_task and job.task.startswith(execution)
        deliveries, send = telegram_boundary(monkeypatch)
        result = run_scheduled(job, provider, runtime, registry, send)
    # This owned-snapshot probe exercises configuration and delivery, not a
    # completed account scan: presentation changes must not certify missing
    # email/calendar work from the real saved task.
    assert result['response_fulfilled'] is True
    assert provider.producer_calls == 2 and len(reads) == 1
    assert len(deliveries) == 1 and '$19 scheduled; posting unconfirmed' in deliveries[0]['text']


@pytest.mark.parametrize('repair', [False, True])
@pytest.mark.parametrize('task', ['期限、金額、必要な対応と予定表の結果を簡潔に報告。',
                                'Resume vencimientos, importes, acciones y resultados del calendario.'])
def test_seven_item_triage_delivers_outcomes_without_report_wide_explanations(tmp_path, monkeypatch, repair, task):
    from nullion import preferences
    from nullion.chat_text import REPORT_MESSAGE_FORMAT, render_report_message
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from tests.report_presentation_fixture import triage_outcome_fixture
    runtime = setup_runtime(tmp_path, monkeypatch)
    preferences.save_preferences(preferences.Preferences(markdown_style='plain', response_structure='free'))
    message = triage_outcome_fixture(); expected = render_report_message(message)
    context = message['extra_details'][0]
    source = {'value': 7, 'report': message}
    registry = ToolRegistry(); reads = []
    def read(call):
        reads.append(call.invocation_id)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', source)
    registry.register(ToolSpec('record_read', 'Read selected obligations', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        producer_calls = 0
        repairs = 0
        def create(self, **kw):
            system = kw.get('system') or ''
            if system.startswith('You repair scheduled-task final answers'):
                self.repairs += 1
                assert 'root extra_details' in system
                raw = kw['messages'][0]['content'][0]['text']
                evidence = json.JSONDecoder().raw_decode(raw.split('Verified compact tool evidence from this run:\n', 1)[1])[0]
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [r['evidence_id'] for r in evidence if r['status'] == 'completed'],
                    'report_message': message, 'report': expected + '\n\n' + context}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                assert context not in packet['draft_final_response']
                assert '$105.80' in packet['draft_final_response'] and 'Posting unconfirmed' in packet['draft_final_response']
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            self.producer_calls += 1
            if not reads: return tool_response('record_read', {})
            if repair: raise TimeoutError('Synthetic interruption after successful obligation checks')
            return text_response(json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': message,
                'text': expected + '\n\n' + context}))
    deliveries, send = telegram_boundary(monkeypatch); provider = Provider()
    result = run_scheduled(CronJob(id='triage-concision', name='Selected obligations', task=task,
        schedule='0 * * * *', delivery_channel='telegram', delivery_target='123'), provider, runtime, registry, send)
    assert result['response_fulfilled'] is True
    assert len(reads) == 1 and provider.repairs == int(repair)
    assert provider.producer_calls == 2 + int(repair)
    assert len(deliveries) == 1
    text = deliveries[0]['text']
    assert expected in text and context not in text
    assert len(text.split()) < 210
    assert 'Full subject:' not in text
    for section in message['sections']:
        for item in section['items']:
            assert item['title'] in text and item['summary'] in text
            assert all(detail in text for detail in item['required_details'])
    assert text.count(message['shared_details'][0]['text']) == 1


@pytest.mark.parametrize('repair', [False, True])
@pytest.mark.parametrize('task', ['確認した記録の要点、出典、予定表の結果を簡潔に報告。',
                                'Resume los resultados, las fuentes y el calendario de forma concisa.'])
def test_verbose_fallback_is_compiled_to_same_compact_report_before_review_and_delivery(tmp_path, monkeypatch, repair, task):
    from nullion import preferences
    from nullion.chat_text import REPORT_MESSAGE_FORMAT, render_report_message
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    preferences.save_preferences(preferences.Preferences(markdown_style='plain', response_structure='free'))
    deliveries, send = telegram_boundary(monkeypatch)
    message, verbose, source = report_fixture(); source['value'] = 7
    expected = render_report_message(message)
    registry = ToolRegistry(); reads = []
    def read(invocation):
        reads.append(invocation)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', source)
    registry.register(ToolSpec('record_read', 'Read selected source records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        producer_calls = 0
        repair_calls = 0
        reviewed_drafts = []
        def create(self, **kwargs):
            system = kwargs.get('system') or ''
            if system.startswith('You repair scheduled-task final answers'):
                self.repair_calls += 1
                raw = kwargs['messages'][0]['content'][0]['text']
                evidence = json.JSONDecoder().raw_decode(raw.split('Verified compact tool evidence from this run:\n', 1)[1])[0]
                return text_response(json.dumps({'requirements_satisfied': True, 'source_state': 'available',
                    'evidence_refs': [row['evidence_id'] for row in evidence if row['status'] == 'completed'],
                    'report': verbose, 'report_message': message}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                self.reviewed_drafts.append(packet['draft_final_response'])
                assert 'Historical detail' not in packet['draft_final_response']
                assert '$720' in packet['draft_final_response'] and '$760' in packet['draft_final_response']
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            self.producer_calls += 1
            if not reads: return tool_response('record_read', {})
            if repair: raise TimeoutError('Synthetic interruption after completed checks')
            # The provider returns a verbose legacy fallback too. Rendering must
            # select typed compact content, rather than merely trusting prose.
            return text_response(json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': message, 'text': verbose}))
    provider = Provider()
    result = run_scheduled(CronJob(id='compact-regression', name='確認', task=task, schedule='0 * * * *',
        delivery_channel='telegram', delivery_target='123'), provider, runtime, registry, send)
    assert result['response_fulfilled'] is True, result
    # Existing model-transport retry still owns an interrupted producer; the
    # presentation compiler must add neither a source read nor a model pass.
    assert len(reads) == 1 and provider.producer_calls == 2 + int(repair)
    assert provider.repair_calls == int(repair)
    assert len(deliveries) == 1
    text = deliveries[0]['text']
    assert expected in text
    assert len(text) < len(verbose) * .70
    assert text.count(message['shared_details'][0]['text']) == 1
    assert 'Final posting is unconfirmed' in text and 'amount unavailable' in text
    assert 'Historical detail' not in text and 'Full subject:' not in text
    assert 'nullion.report.v1' not in text and 'shared_detail_ids' not in text
    assert not provider.reviewed_drafts
    reviews = runtime.store.list_recent_conversation_events('telegram:123', event_type='conversation.completion_review')
    assert not reviews


def test_structured_detailed_report_preserves_explicit_full_audit_on_real_runner(tmp_path, monkeypatch):
    from nullion import preferences
    from nullion.chat_text import REPORT_MESSAGE_FORMAT
    from nullion.crons import CronJob
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    preferences.save_preferences(preferences.Preferences(markdown_style='plain'))
    deliveries, send = telegram_boundary(monkeypatch)
    message, _, source = report_fixture(); message['detail_level'] = 'detailed'; source['value'] = 7
    registry = ToolRegistry(); reads = []
    def read(call):
        reads.append(call)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', source)
    registry.register(ToolSpec('record_read', 'Read selected source records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not reads: return tool_response('record_read', {})
            return text_response(json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': message}))
    result = run_scheduled(CronJob(id='detailed-regression', name='監査', task='完整保留逐项审计和原始标题。',
        schedule='0 * * * *', delivery_channel='telegram', delivery_target='123'), Provider(), runtime, registry, send)
    assert result['response_fulfilled'] is True, result
    text = '\n'.join(row['text'] for row in deliveries)
    assert 'Historical detail 0' in text and 'Historical detail 9' in text
    assert all(row['expanded_label'] in text for row in message['sources'])
    assert all(context in text for context in message['extra_details'])


@pytest.mark.asyncio
@pytest.mark.parametrize('helper_shape', ['report', 'plain', 'bad_report'])
@pytest.mark.parametrize('child_layout', ['structured', 'content_fallback'])
async def test_synthesis_and_group_keep_shared_report_ownership_through_cosmetic_fallback(tmp_path, monkeypatch, helper_shape, child_layout):
    import asyncio
    from copy import deepcopy
    from nullion import preferences, telegram_app
    from nullion.chat_text import REPORT_MESSAGE_FORMAT, render_report_message
    from nullion.context_bus import ContextBus
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig, ProgressUpdate
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRecord, TaskGroup, TaskRegistry, TaskStatus, TaskPriority
    from nullion.tools import ToolRegistry
    from tests.report_presentation_fixture import triage_outcome_fixture
    runtime = setup_runtime(tmp_path, monkeypatch)
    preferences.save_preferences(preferences.Preferences(markdown_style='plain'))
    message = triage_outcome_fixture(); expected = render_report_message(message)
    original = deepcopy(message)
    if child_layout == 'content_fallback': original['sections'][0]['layout'] = 'unsupported'
    task = TaskRecord('synthesis', 'shared-summary', 'telegram:123', 'telegram:123', '要点',
        '確認済みの義務と必要な対応だけを簡潔に報告。', TaskStatus.QUEUED, TaskPriority.NORMAL, [], [],
        metadata={'scheduled_task_run': True})
    class Provider:
        producer_calls = 0
        helper_calls = 0
        summary_calls = 0
        def create(self, **kw):
            system = kw.get('system') or ''
            if system.startswith('You validate one completed Nullion scheduled-task step'):
                self.helper_calls += 1
                assert 'root extra_details' in system and REPORT_MESSAGE_FORMAT in system
                assert kw['max_tokens'] > 700
                body = {'status': 'success', 'answer': message['extra_details'][0]}
                if helper_shape != 'plain':
                    body['report_message'] = message if helper_shape == 'report' else {'sections': []}
                return text_response(json.dumps(body))
            if 'You are summarizing the results' in system:
                self.summary_calls += 1
                assert REPORT_MESSAGE_FORMAT in system and 'root extra_details' in system
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                assert packet['task_results'][0]['report_presentation'] in {'structured', 'content_fallback'}
                assert packet['report_delivery_context']['delivery_role'] == (
                    'artifact_summary' if packet['prepared_delivery_artifacts'] else 'standalone_report')
                return text_response(json.dumps({'status': 'complete', 'answer': message['extra_details'][0],
                    'report_message': message, 'evaluated_task_ids': ['synthesis'], 'missing_outcomes': []}))
            if any(tool.get('name') == 'select_attachment_format' for tool in kw.get('tools', [])):
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            protocol = workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            self.producer_calls += 1
            return text_response(json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': original}))
    provider = Provider()
    result = await DeepAgentMiniAgentRunner().run(MiniAgentConfig(agent_id='synthesis', task=task,
        can_request_user_input=False), anthropic_client=provider, tool_registry=ToolRegistry(),
        policy_store=runtime.store, approval_store=runtime.store, context_bus=ContextBus(), progress_queue=asyncio.Queue())
    assert result.status == 'success', result
    assert message['extra_details'][0] not in result.output
    assert '$105.80' in result.output and 'Posting unconfirmed' in result.output
    if child_layout == 'structured': assert result.output == expected
    task.result = result; task.status = TaskStatus.COMPLETE
    group = TaskGroup('shared-summary', 'telegram:123', task.description, tasks=[task])
    tasks = TaskRegistry(); await tasks.add_group(group)
    deliveries, _ = telegram_boundary(monkeypatch)
    async def deliver(conversation, text, **kw):
        if kw.get('is_status'): return True
        return await telegram_app._send_operator_telegram_message('fixture', '123', text, principal_id=conversation)
    aggregator = ResultAggregator(deliver_fn=deliver, model_client=provider, task_registry=tasks)
    await aggregator._handle(ProgressUpdate(agent_id='synthesis', task_id='synthesis', group_id='shared-summary', kind='task_complete'))
    assert provider.producer_calls == provider.summary_calls == 1 and provider.helper_calls == 0
    assert len(deliveries) == 1 and expected in deliveries[0]['text']
    assert message['extra_details'][0] not in deliveries[0]['text']
    assert 'outcome_review_status' not in group.planner_metadata


@pytest.mark.asyncio
@pytest.mark.parametrize('malformed_layout', [False, True])
async def test_deep_agent_and_group_summary_preserve_same_compact_contract(tmp_path, monkeypatch, malformed_layout):
    import asyncio
    from nullion import preferences, telegram_app
    from nullion.chat_text import REPORT_MESSAGE_FORMAT, render_report_message
    from nullion.context_bus import ContextBus
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig, ProgressUpdate
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRecord, TaskGroup, TaskRegistry, TaskStatus, TaskPriority
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    preferences.save_preferences(preferences.Preferences(markdown_style='plain'))
    message, verbose, source = report_fixture(); source['value'] = 7
    registry = ToolRegistry(); reads = []
    def read(call):
        reads.append(call)
        return ToolResult(call.invocation_id, call.tool_name, 'completed', source)
    registry.register(ToolSpec('record_read', 'Read selected records', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    task = TaskRecord('source', 'compact-group', 'telegram:123', 'telegram:123', '確認',
        '確認した記録を簡潔に報告。', TaskStatus.QUEUED, TaskPriority.NORMAL, ['record_read'], [],
        metadata={'scheduled_task_run': True})
    class Provider:
        summaries = 0
        def create(self, **kwargs):
            system = kwargs.get('system') or ''
            if 'You are summarizing the results' in system:
                self.summaries += 1
                assert REPORT_MESSAGE_FORMAT in system
                return text_response(json.dumps({'status': 'complete', 'answer': verbose,
                    'report_message': {'sections': []} if malformed_layout else message,
                    'evaluated_task_ids': ['source'], 'missing_outcomes': []}))
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            if not reads: return tool_response('record_read', {})
            return text_response(json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': message, 'text': verbose}))
    provider = Provider()
    result = await DeepAgentMiniAgentRunner().run(MiniAgentConfig(agent_id='report', task=task, can_request_user_input=False),
        anthropic_client=provider, tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store,
        context_bus=ContextBus(), progress_queue=asyncio.Queue())
    assert result.status == 'success', result
    assert 'Historical detail' not in result.output
    assert result.verified_turn_result.response_presentation['report_presentation'] == 'structured'
    task.result = result; task.status = TaskStatus.COMPLETE
    group = TaskGroup('compact-group', 'telegram:123', task.description, tasks=[task])
    tasks = TaskRegistry(); await tasks.add_group(group)
    deliveries, _ = telegram_boundary(monkeypatch)
    async def deliver(conversation, text, **kwargs):
        if kwargs.get('is_status'): return True
        return await telegram_app._send_operator_telegram_message('fixture', '123', text, principal_id=conversation)
    aggregator = ResultAggregator(deliver_fn=deliver, model_client=provider, task_registry=tasks, min_progress_interval_s=0)
    await aggregator._handle(ProgressUpdate(agent_id='report', task_id='source', group_id='compact-group', kind='task_complete'))
    assert provider.summaries == 1 and len(reads) == 1
    assert len(deliveries) == 1
    delivered = deliveries[0]['text']
    if malformed_layout:
        # The transport may normalize source presentation, but a cosmetic
        # schema error must retain every required finding and the plain draft.
        for row in source['records']:
            for detail in [row['title'], row['summary'], *row['required_details']]:
                assert detail in delivered, (detail, delivered)
        assert 'Historical detail' in delivered
    else:
        assert render_report_message(message) in delivered
    if malformed_layout:
        assert group.planner_metadata['report_presentation']['report_presentation'] == 'content_fallback'
        assert 'outcome_review_status' not in group.planner_metadata
    else:
        assert 'Historical detail' not in deliveries[0]['text']
        assert group.planner_metadata['report_presentation']['report_shared_detail_count'] == 1


def test_plain_chat_remains_one_model_call_and_avoids_optional_report_renderer(tmp_path, monkeypatch):
    import time
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_text
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    runtime = setup_runtime(tmp_path, monkeypatch)
    real = chat_text.render_report_envelope; renderer_calls = []
    def slow_optional_renderer(*args, **kwargs):
        renderer_calls.append(True); time.sleep(.2)
        return real(*args, **kwargs)
    monkeypatch.setattr(chat_text, 'render_report_envelope', slow_optional_renderer)
    class Provider:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return text_response('Hello.')
    provider = Provider(); elapsed = []
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=provider), registry=ToolRegistry())) as client:
        for i, prompt in enumerate(('Hello', 'こんにちは', 'Hola', '안녕', 'مرحبا')):
            start = time.monotonic()
            reply = client.post('/api/chat', json={'conversation_id': f'web:plain-{i}', 'text': prompt})
            elapsed.append(time.monotonic() - start)
            assert reply.status_code == 200 and 'Hello.' in reply.json()['text'], reply.text
    assert provider.calls == 5 and renderer_calls == []
    assert sum(elapsed) / len(elapsed) < .2 and max(elapsed) < .4, elapsed


def test_real_json_file_read_binds_ten_findings_and_shares_compact_policy_with_both_reviewers(tmp_path, monkeypatch):
    from nullion import preferences
    from nullion.chat_text import REPORT_MESSAGE_FORMAT
    from nullion.crons import CronJob
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
                               _build_file_read_handler)
    runtime = setup_runtime(tmp_path, monkeypatch)
    preferences.save_preferences(preferences.Preferences(markdown_style='plain'))
    message, _, source = report_fixture()
    path = tmp_path / '記録.json'; path.write_text(json.dumps(source))
    reads = []
    native_read = _build_file_read_handler(workspace_root=tmp_path, include_principal_workspace=False)
    def read(call):
        reads.append(call)
        return native_read(call)
    registry = ToolRegistry()
    registry.register(ToolSpec('file_read', 'Read selected JSON', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        reviews = 0
        plans = 0
        def create(self, **kwargs):
            system = kwargs.get('system') or ''
            if 'material-requirements planner' in system:
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert packet['report_presentation_contract']['format'] == REPORT_MESSAGE_FORMAT
                self.plans += 1
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [
                        {'requirement': row['title'], 'requirement_kind': 'fact',
                         'evidence_subject_id': row['title'], 'eligible_evidence_tool_names': ['file_read']}
                        for row in source['records']]}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert 'optional expansion' in packet['report_presentation_contract']['review_guidance']
                self.reviews += 1
                records = {r['output']['record']['title']: r['invocation_id'] for r in packet['tool_evidence']
                    if isinstance(r.get('output'), dict) and isinstance(r['output'].get('record'), dict)
                    and 'title' in r['output']['record']}
                assert len(records) == 10, packet['tool_evidence']
                return text_response(json.dumps({'disposition': 'complete', 'unresolved_requirements': [],
                    'retry_tool_names': [], 'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': True,
                        'draft_covered': True, 'evidence_invocation_ids': [records[row['requirement']]]}
                        for row in packet['material_requirement_contract']]}))
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None: return protocol
            if not reads: return tool_response('file_read', {'path': str(path)})
            return text_response(json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': message}))
    provider = Provider(); deliveries, send = telegram_boundary(monkeypatch)
    result = run_scheduled(CronJob(id='json-report', name='報告', task='記録を簡潔に報告。', schedule='0 * * * *',
        delivery_channel='telegram', delivery_target='123'), provider, runtime, registry, send)
    assert result['response_fulfilled'] is True, result
    assert len(reads) == 1 and provider.plans == provider.reviews == 0
    assert len(deliveries) == 1 and 'Historical detail' not in deliveries[0]['text']

"""Review infrastructure failure cannot erase a completed report or replay work."""
import json
import os

import pytest

from tests.e2e.test_universal_outcome_review_boundary import ReasoningProvider, runtime_and_registry, text
from tests.e2e.test_scheduled_report_completion import telegram_boundary, run_scheduled

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


class UnavailableReviewProvider(ReasoningProvider):
    def __init__(self, failure, name='opaque_read'):
        super().__init__(name)
        self.failure = failure

    def create(self, **kw):
        system = kw.get('system', '')
        contract = 'material-requirements planner' in system
        review = 'semantic completion reviewer' in system
        if (contract and self.failure == 'contract_timeout') or review:
            if contract:
                self.contract_calls += 1
            else:
                self.review_calls += 1
            if self.failure.endswith('timeout'):
                from nullion.model_clients import ModelClientTimeoutError
                raise ModelClientTimeoutError('Synthetic unavailable reviewer')
            if self.failure == 'invalid_citations':
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                return text({'disposition': 'complete', 'all_material_requirements_enumerated': True,
                    'unresolved_requirements': [], 'retry_tool_names': [], 'material_requirements': [{
                        'requirement_id': row['requirement_id'], 'satisfied': True, 'draft_covered': True,
                        'evidence_invocation_ids': ['not-a-source-receipt']}
                        for row in packet['material_requirement_contract']]})
            return text('{')
        return super().create(**kw)


@pytest.mark.parametrize('failure', ['contract_timeout', 'review_timeout', 'invalid_json', 'invalid_citations'])
@pytest.mark.parametrize('prompt', ['選択した値を報告してください。', 'Informa del valor seleccionado.'])
def test_real_runner_delivers_answer_with_internal_helper_diagnostics(tmp_path, monkeypatch, failure, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'opaque_read')
    provider = UnavailableReviewProvider(failure)
    runner = AgentOrchestrator(model_client=provider)
    args = dict(conversation_id='web:review-warning', principal_id='web:review-warning', user_message=prompt,
        conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    result = runner.run_turn(**args, max_iterations=4)
    assert 'Observed value: 7.' in result.final_text
    assert result.final_text.count('Review warning:') == 0
    assert result.response_fulfilled is True
    assert result.outcome_review_receipt is None
    assert 'outcome_review_status' not in (result.response_presentation or {})
    assert len(calls) == 1 and provider.agent_calls == 2
    before = (provider.review_calls, provider.contract_calls)
    assert runner.review_turn_result(result, **args).final_text == result.final_text
    assert (provider.review_calls, provider.contract_calls) == before


@pytest.mark.parametrize('failure', ['contract_timeout', 'review_timeout', 'invalid_json', 'invalid_citations'])
def test_scheduler_delivers_answer_once_and_records_helper_availability_internally(tmp_path, monkeypatch, failure):
    from nullion.crons import CronJob
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'opaque_read')
    from nullion import crons, chat_store
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', runtime.checkpoint_path)
    monkeypatch.setattr(chat_store, '_store', None)
    provider = UnavailableReviewProvider(failure)
    deliveries, send = telegram_boundary(monkeypatch)
    job = CronJob(id='report-warning', name='確認', schedule='0 * * * *', task='選択した値を報告してください。',
        delivery_channel='telegram', delivery_target='123')
    result = run_scheduled(job, provider, runtime, registry, send)
    assert len(deliveries) == 1
    assert 'Observed value: 7.' in deliveries[0]['text']
    assert deliveries[0]['text'].count('Review warning:') == 0
    assert len(calls) == 1 and provider.agent_calls == 2
    assert not result.get('cron_report_failed') and not result.get('cron_completion_continuations')
    assert result['response_fulfilled'] is True
    assert 'review_status' not in result['execution_outcome']
    assert result['execution_outcome']['status'] == 'succeeded'
    from nullion.runtime_persistence import load_runtime_store
    rows = load_runtime_store(runtime.checkpoint_path).list_conversation_events('telegram:123')
    assert any(row.get('event_type') == 'conversation.cron_run'
        and not row.get('outcome_review_status') and not row['report_failed'] for row in rows)


@pytest.mark.parametrize('failure', ['review_timeout', 'invalid_citations'])
def test_web_ui_keeps_answer_without_editor_availability_boilerplate(tmp_path, monkeypatch, failure):
    from playwright.sync_api import sync_playwright, expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion import web_app, chat_store
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'weather_forecast')
    for name, value in {'NULLION_ENV_FILE': str(tmp_path / 'none.env'), 'NULLION_TASK_PLANNER_FEED_MODE': 'off',
                        'NULLION_CHAT_STARTUP_WARMUP': '0', 'NULLION_ACTIVITY_TRACE_ENABLED': '0'}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(chat_store, '_store', None)
    class Provider(UnavailableReviewProvider):
        def create(self, **kw):
            names = {row['name'] for row in kw.get('tools', [])}
            system = kw.get('system', '')
            if 'select_attachment_format' in names:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'format', 'name': 'select_attachment_format',
                    'input': {'extension': None, 'confidence': 1, 'embedded_media_extensions': []}}]}
            if 'relationship' in system and 'target_index' in system:
                return text({'relationship': 'independent', 'effect': 'none', 'confidence': 1, 'target_index': None})
            if 'Return JSON only for a single already-selected read-only tool' in system:
                return text({'use_direct': False, 'arguments': {}})
            if 'request_tool_scope' in names and 'weather_forecast' not in names:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'scope', 'name': 'request_tool_scope',
                    'input': {'capabilities': ['weather'], 'tool_names': ['weather_forecast']}}]}
            if not names and not ('material-requirements planner' in system or 'semantic completion reviewer' in system):
                return text({})
            return super().create(**kw)
    provider = Provider(failure, 'weather_forecast')
    runtime.model_client = provider
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=provider), registry=registry)
    with _UvicornThread(app, _free_port()) as server, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
        try:
            context = browser.new_context()
            context.add_init_script('localStorage.setItem("nullion_conv_id", "web:warning-ui");')
            page = context.new_page()
            page.goto(server.base_url)
            page.locator('#user-input').fill('/chat 選択した対象の値を教えて。')
            page.locator('#send-btn').click()
            expect(page.locator('#messages')).to_contain_text('Observed value: 7.', timeout=20000)
            expect(page.locator('#messages')).not_to_contain_text('Review warning:', timeout=20000)
            assert page.locator('#messages').inner_text().count('Review warning:') == 0
            assert len(calls) == 1
            rows = runtime.store.list_conversation_events('web:warning-ui')
            turn = next(row for row in reversed(rows) if row.get('event_type') == 'conversation.chat_turn')
            assert 'Observed value: 7.' in turn['assistant_reply']
            assert turn['assistant_reply'].count('Review warning:') == 0
            assert not any(row.get('event_type') == 'conversation.completion_review' for row in rows)
        finally:
            browser.close()




@pytest.mark.asyncio
@pytest.mark.parametrize('failed_sibling', [True, False])
async def test_failed_sibling_preserves_successful_findings_and_independent_file(tmp_path, failed_sibling):
    from nullion.result_aggregator import ResultAggregator, GroupState
    from nullion.task_queue import TaskGroup, TaskRegistry, TaskRecord, TaskResult, TaskStatus, TaskPriority
    ready = tmp_path / 'selected.html'
    ready.write_text('<!doctype html><html><body>Selected value: 7.</body></html>')
    corrupt = tmp_path / 'incomplete.pdf'
    corrupt.write_bytes(b'not a PDF')
    tasks = [TaskRecord('ready', 'g', 'web:g', 'web:g', 'A', 'A', TaskStatus.COMPLETE,
        TaskPriority.NORMAL, [], [], result=TaskResult('ready', 'success', 'Selected value: 7.', artifacts=[str(ready), str(corrupt)])),
        TaskRecord('failed', 'g', 'web:g', 'web:g', 'B', 'B', TaskStatus.FAILED,
        TaskPriority.NORMAL, [], [], result=TaskResult('failed', 'failure', error='Selected second source is unavailable.'))]
    if not failed_sibling:
        tasks = tasks[:1]
    group = TaskGroup('g', 'web:g', '選択した結果を報告してください。', tasks=tasks)
    registry = TaskRegistry()
    await registry.add_group(group)
    wire = []
    async def deliver(conversation, value, **kw):
        if not kw.get('is_status'):
            wire.append(value)
        return True
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, outbox_path=tmp_path / 'outbox.db')
    await aggregator._on_group_complete(GroupState('g', 'web:g', group.original_message), group)
    receipt = aggregator._outbox.get('g')
    assert receipt['artifacts'] == receipt['delivered_artifacts'] == [str(ready)]
    assert receipt['work_complete'] is False and receipt['status'] == 'delivered'
    assert any('Selected value: 7.' in value and 'failed validation' in value for value in wire)
    assert group.planner_metadata['artifact_validation_failed'] is True
    assert ready.read_text() == '<!doctype html><html><body>Selected value: 7.</body></html>'


@pytest.mark.parametrize('status', ['failed', 'denied'])
def test_reviewer_outage_cannot_make_a_failed_source_call_prove_a_claim(tmp_path, monkeypatch, status):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, _, _ = runtime_and_registry(tmp_path, monkeypatch, 'unused')
    registry = ToolRegistry()
    reads = []
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id, inv.tool_name, status, {}, 'Selected source did not return a value.')
    registry.register(ToolSpec('opaque_read', 'Read selected resource', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    from nullion.turn_context_policy import ScopedTurnToolRegistry,TurnToolScopeDecision,build_turn_tool_evidence
    registry=ScopedTurnToolRegistry(registry,
        evidence=build_turn_tool_evidence(user_message='選択した値を報告してください。',conversation_result=None),
        tool_scope_decision=TurnToolScopeDecision(requested_tool_names=('opaque_read',),
            required_tool_names=('opaque_read',),valid=True))
    provider = UnavailableReviewProvider('review_timeout')
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:no-proof',
        principal_id='web:no-proof', user_message='選択した値を報告してください。', conversation_history=[],
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=4)
    assert result.response_fulfilled is False and not result.artifacts
    assert 'Observed value: 7.' not in result.final_text
    assert len(reads) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('dependency_kind', ['direct', 'transitive', 'missing', 'cycle', 'recovery'])
async def test_selected_report_cannot_bypass_broken_structured_dependencies(tmp_path, dependency_kind):
    from nullion.result_aggregator import ResultAggregator, GroupState
    from nullion.task_queue import TaskGroup, TaskRegistry, TaskRecord, TaskResult, TaskStatus, TaskPriority
    report = tmp_path / 'selected.html'
    report.write_text('<!doctype html><html><body>Dependent finding</body></html>')
    source = TaskRecord('source', 'g', 'web:g', 'web:g', 'Source', 'Source', TaskStatus.FAILED,
        TaskPriority.NORMAL, [], [], result=TaskResult('source', 'failure', error='Selected source unavailable.'))
    middle = TaskRecord('middle', 'g', 'web:g', 'web:g', 'Middle', 'Middle', TaskStatus.COMPLETE,
        TaskPriority.NORMAL, [], ['source'], result=TaskResult('middle', 'success'))
    selected = TaskRecord('selected', 'g', 'web:g', 'web:g', 'Report', 'Report', TaskStatus.COMPLETE,
        TaskPriority.NORMAL, [], ['source'], result=TaskResult('selected', 'success', 'Dependent finding', artifacts=[str(report)]))
    if dependency_kind == 'transitive':
        selected.dependencies = ['middle']
    elif dependency_kind == 'missing':
        selected.dependencies = ['absent']
    elif dependency_kind == 'cycle':
        selected.dependencies = ['middle']
        middle.dependencies = ['selected']
    elif dependency_kind == 'recovery':
        selected.dependencies = []
        selected.metadata = {'publication_dependency_ids': ['source']}
    group = TaskGroup('g', 'web:g', '選択した結果を報告してください。', tasks=[source, middle, selected])
    registry = TaskRegistry()
    await registry.add_group(group)
    wire = []
    async def deliver(conversation, value, **kw):
        if not kw.get('is_status'):
            wire.append(value)
        assert not kw.get('is_artifact')
        return True
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, outbox_path=tmp_path / 'outbox.db')
    await aggregator._on_group_complete(GroupState('g', 'web:g', group.original_message), group)
    receipt = aggregator._outbox.get('g')
    assert receipt['artifacts'] == [] and receipt['work_complete'] is False
    assert len(wire) == 1 and 'Dependent finding' not in wire[0]
    assert 'No report was published' in wire[0]

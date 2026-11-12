"""Real graph/SQLite/Web boundary; only the external model and source are faked."""
import json
import os
import statistics
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def text(value):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': value if isinstance(value, str) else json.dumps(value)}]}


class ReasoningProvider:
    """Script the provider protocol, never mock Nullion's review or executor."""
    def __init__(self, name, *, correction=False, needs_input=False):
        self.name = name
        self.correction = correction
        self.needs_input = needs_input
        self.agent_calls = 0
        self.review_calls = 0
        self.contract_calls = 0
        self.systems = []

    def create(self, **kw):
        system = kw.get('system', '')
        if 'material-requirements planner' in system:
            self.contract_calls += 1
            return text({'all_material_requirements_enumerated': True,
                'requested_artifact_extensions': [], 'material_requirements': [{
                'requirement': 'Report the selected resource value', 'requirement_kind': 'fact',
                'evidence_subject_id': 'resource', 'eligible_evidence_tool_names': [self.name]}]})
        if 'semantic completion reviewer' in system:
            self.review_calls += 1
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            assert packet['original_request'] and packet['current_datetime']
            assert 'mistaken premises' in system
            if self.needs_input:
                return text({'disposition': 'needs_user_input', 'unresolved_requirements': ['The selected target has expired.'],
                    'retry_tool_names': [], 'clarification': {'question': 'That target has expired. Which window should I use?', 'options': ['Upcoming dates', 'Keep the original date']}})
            if self.correction and '8' in packet['draft_final_response']:
                return text({'disposition': 'retry', 'unresolved_requirements': ['Use the observed value 7 in the answer.'],
                    'retry_tool_names': []})
            evidence = next(row for row in packet['tool_evidence'] if row['tool_name'] == self.name and row['status'] == 'completed')
            return text({'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': [],
                'all_material_requirements_enumerated': True, 'material_requirements': [{
                'requirement_id': row['requirement_id'], 'satisfied': True, 'draft_covered': True,
                'evidence_invocation_ids': [evidence['invocation_id']]} for row in packet['material_requirement_contract']]})
        from nullion.workflow_reasoning import WORKFLOW_REASONING_INSTRUCTION
        assert WORKFLOW_REASONING_INSTRUCTION in system
        self.systems.append(system)
        self.agent_calls += 1
        if self.agent_calls == 1:
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'read', 'name': self.name, 'input': {}}]}
        return text('Observed value: 8.' if self.correction and self.agent_calls == 2 else 'Observed value: 7.')


def runtime_and_registry(tmp_path, monkeypatch, name):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = ToolRegistry(); calls = []
    def read(inv):
        calls.append(inv)
        return ToolResult(inv.invocation_id, name, 'completed', {'value': 7, 'target': 'resource'})
    registry.register(ToolSpec(name, 'Read selected resource value', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), read)
    return runtime, registry, calls


@pytest.mark.parametrize('name', ['sensor_read', 'inventory_read', 'opaque_adapter'])
@pytest.mark.parametrize('prompt', ['Report the selected resource value.', '選択した対象の値を教えて。', 'Indica el valor seleccionado.'])
def test_successful_calls_still_review_and_correct_the_delivered_answer(tmp_path, monkeypatch, name, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, name)
    provider = ReasoningProvider(name, correction=True)
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:review', principal_id='web:review',
        user_message=prompt, conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=5)
    assert result.final_text == 'Observed value: 7.' and result.response_fulfilled is True
    assert len(calls) == 1 and provider.agent_calls == 3
    assert provider.contract_calls == 1 and provider.review_calls == 2
    assert result.outcome_review_receipt.disposition == 'complete'


def test_unsupported_expiry_opinion_cannot_create_a_clarification_gate(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    provider = ReasoningProvider('resource_read', needs_input=True)
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:review', principal_id='web:review',
        user_message='Vérifie cette date.', conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=4)
    assert result.response_fulfilled is None
    assert result.final_text == 'Observed value: 7.'
    assert not result.response_presentation.get('needs_input')
    assert len(calls) == 1


def test_direct_result_gets_review_once_and_changed_draft_invalidates_it(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator, TurnResult
    from nullion.tools import ToolInvocation
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    provider = ReasoningProvider('resource_read')
    runner = AgentOrchestrator(model_client=provider)
    evidence = registry.invoke(ToolInvocation('direct', 'resource_read', 'web:review', {}))
    result = TurnResult('direct', 'Observed value: 7.', [evidence])
    kwargs = dict(conversation_id='web:review', principal_id='web:review', user_message='Report the selected value.',
        conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    reviewed = runner.review_turn_result(result, **kwargs)
    assert reviewed.response_fulfilled is True and provider.review_calls == 1
    assert runner.review_turn_result(reviewed, **kwargs) is reviewed
    assert provider.review_calls == 1
    reviewed.final_text = 'Changed answer'
    runner.review_turn_result(reviewed, **kwargs)
    assert provider.review_calls == 2 and len(calls) == 1


def test_plain_turns_reason_in_existing_call_without_review_dependency(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.workflow_reasoning import WORKFLOW_REASONING_INSTRUCTION
    import nullion.agent_orchestrator as module
    from nullion.tools import ToolRegistry
    runtime, _, _ = runtime_and_registry(tmp_path, monkeypatch, 'unused')
    calls = []
    class Provider:
        def create(self, **kw):
            calls.append(kw)
            assert WORKFLOW_REASONING_INSTRUCTION in kw['system']
            return text('Bonjour.')
    def slow_forbidden(*args, **kwargs):
        time.sleep(.2)
        raise AssertionError('Plain chat entered an extra outcome model')
    monkeypatch.setattr(module, '_perform_completion_review', slow_forbidden)
    runner = AgentOrchestrator(model_client=Provider()); elapsed = []
    for prompt in ['Bonjour', 'こんにちは', 'Hola', '안녕', 'Hello']:
        started = time.monotonic()
        result = runner.run_turn(conversation_id='web:plain', principal_id='web:plain', user_message=prompt,
            conversation_history=[], tool_registry=ToolRegistry(), policy_store=runtime.store, approval_store=runtime.store)
        elapsed.append(time.monotonic() - started)
        assert result.final_text == 'Bonjour.'
    assert len(calls) == 5 and statistics.mean(elapsed) < .1 and max(elapsed) < .2


@pytest.mark.parametrize('decision', ['clarify', 'revise', 'invalid', 'unavailable', 'proceed'])
@pytest.mark.parametrize('prompt', ['Change the selected resource.', '選択した対象を変更してください。'])
def test_action_preflight_blocks_mistakes_before_any_write(tmp_path, monkeypatch, decision, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    writes = []
    def write(inv):
        writes.append(inv.arguments)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('resource_write', 'Change selected resource', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2), write)
    class Provider(ReasoningProvider):
        preflights = 0
        def create(self, **kw):
            system = kw.get('system', '')
            if 'Workflow action preflight.' in system:
                self.preflights += 1
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                assert packet['original_request'] == prompt
                assert packet['proposed_action']['arguments'] == {'target': 'selected'}
                if decision == 'unavailable':
                    raise TimeoutError('external model timeout')
                if decision == 'invalid':
                    return text({'decision': 'proceed', 'issues': [], 'approval_granted': True})
                if decision == 'clarify':
                    return text({'decision': 'clarify', 'issues': ['Two selected targets conflict.'],
                        'question': 'Which target should I change?', 'options': ['First target', 'Second target']})
                if decision == 'revise':
                    return text({'decision': 'revise', 'issues': ['Use the target already supplied in context.']})
                return text({'decision': 'proceed', 'issues': []})
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Change selected resource', 'requirement_kind': 'action',
                        'evidence_subject_id': 'resource', 'eligible_evidence_tool_names': ['resource_write']}]})
            if 'semantic completion reviewer' in system:
                if decision == 'revise':
                    return text({'disposition': 'blocked', 'unresolved_requirements': ['The proposed target was not corrected.'],
                        'user_facing_failure': 'I stopped before changing the wrong target.', 'retry_tool_names': []})
                self.name = 'resource_write'
                return super().create(**kw)
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'write',
                    'name': 'resource_write', 'input': {'target': 'selected'}}]}
            return text('Observed value: 7.')
    provider = Provider('resource_write')
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:action', principal_id='web:action',
        user_message=prompt, conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=3)
    assert len(writes) == (1 if decision == 'proceed' else 0)
    assert provider.preflights == (2 if decision == 'invalid' else 1)
    if decision == 'revise':
        audit=next(row for row in runtime.store.list_conversation_events('web:action')
            if row.get('event_type')=='conversation.action_review')
        assert audit['issues']==['Use the target already supplied in context.']
    if decision == 'proceed':
        assert result.response_fulfilled is True, json.dumps(runtime.store.list_conversation_events('web:action'), ensure_ascii=False)
    elif decision == 'clarify':
        assert '1. First target\n2. Second target' in result.final_text
        assert result.response_fulfilled is False
        # A direct-result delivery check must preserve the already reviewed
        # clarification rather than launch a new request or repeat the action.
        before = provider.agent_calls
        preserved = AgentOrchestrator(model_client=provider).review_turn_result(result,
            conversation_id='web:action', principal_id='web:action', user_message=prompt,
            conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
        assert preserved is result and provider.agent_calls == before
    elif decision == 'revise':
        assert result.response_fulfilled is not True
        assert result.tool_results[-1].output['executed'] is False
        assert result.tool_results[-1].output['reason'] == 'request_plan_conflict'
    else:
        assert result.response_fulfilled is False


def test_preflight_cannot_grant_an_approval_and_approved_resume_writes_once(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.approvals import approve, create_permission_grant
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    writes = []
    def write(inv):
        writes.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('guarded_write', 'Change a selected resource after consent', ToolRiskLevel.HIGH,
        ToolSideEffectClass.WRITE, True, 2), write)
    class Provider(ReasoningProvider):
        def create(self, **kw):
            if 'Workflow action preflight.' in kw.get('system', ''):
                return text({'decision': 'proceed', 'issues': []})
            if 'material-requirements planner' in kw.get('system', '') or 'semantic completion reviewer' in kw.get('system', ''):
                return super().create(**kw)
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'guarded', 'name': 'guarded_write', 'input': {}}]}
            return text('Observed value: 7.')
    provider = Provider('guarded_write'); runner = AgentOrchestrator(model_client=provider)
    kwargs = dict(conversation_id='web:approval', principal_id='web:approval', user_message='Apply the selected change.',
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=4)
    first = runner.run_turn(**kwargs, conversation_history=[])
    assert first.suspended_for_approval and first.approval_id and not writes
    request = runtime.store.get_approval_request(first.approval_id)
    approved = approve(request, decided_by='fixture')
    runtime.store.add_approval_request(approved)
    runtime.store.add_permission_grant(create_permission_grant(approval_id=first.approval_id, principal_id='web:approval', permission='tool:guarded_write', granted_by='fixture'))
    final = runner.resume_turn(**kwargs, messages_snapshot=runtime.store.get_suspended_turn(first.approval_id).messages_snapshot,
        previous_result=first)
    assert len(writes) == 1 and final.response_fulfilled is True


def test_real_web_ui_renders_corrected_outcome_and_stores_review_evidence(tmp_path, monkeypatch):
    from playwright.sync_api import sync_playwright, expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion import web_app, chat_store
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'weather_forecast')
    for name, value in {'NULLION_ENV_FILE': str(tmp_path / 'none.env'), 'NULLION_TASK_PLANNER_FEED_MODE': 'off',
                        'NULLION_CHAT_STARTUP_WARMUP': '0', 'NULLION_ACTIVITY_TRACE_ENABLED': '0'}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(chat_store, '_store', None)
    class Provider(ReasoningProvider):
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
    provider = Provider('weather_forecast', correction=True)
    runtime.model_client = provider
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=provider), registry=registry)
    with _UvicornThread(app, _free_port()) as server, sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
        try:
            context = browser.new_context()
            context.add_init_script('localStorage.setItem("nullion_conv_id", "web:universal-ui");')
            page = context.new_page(); page.goto(server.base_url)
            page.locator('#user-input').fill('/chat 選択した対象の値を教えて。')
            page.locator('#send-btn').click()
            expect(page.locator('#messages')).to_contain_text('Observed value: 7.', timeout=20000)
            assert 'Observed value: 8.' not in page.locator('#messages').inner_text()
            assert len(calls) == 1 and provider.review_calls >= 2
            events = runtime.store.list_conversation_events('web:universal-ui')
            assert any(row.get('event_type') == 'conversation.completion_review' and row.get('disposition') == 'complete' for row in events)
            turn = next(row for row in reversed(events) if row.get('event_type') == 'conversation.chat_turn')
            assert turn['assistant_reply'] == 'Observed value: 7.'
            assert any(row['tool_name'] == 'weather_forecast' and row['status'] == 'completed' for row in turn['tool_results'])
        finally:
            browser.close()


@pytest.mark.asyncio
async def test_real_deep_agent_uses_shared_outcome_repair_before_publishing_context(tmp_path, monkeypatch):
    import asyncio
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig
    from nullion.context_bus import ContextBus
    from nullion.task_queue import TaskRecord, TaskStatus, TaskPriority
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    provider = ReasoningProvider('resource_read', correction=True)
    task = TaskRecord(task_id='child', group_id='group', conversation_id='web:delegated', principal_id='web:delegated',
        title='Read resource', description='選択した対象の値を教えて。', status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL, allowed_tools=['resource_read'], dependencies=[], context_key_out='observed')
    context = ContextBus()
    result = await DeepAgentMiniAgentRunner().run(MiniAgentConfig('reader', task, timeout_s=30),
        anthropic_client=provider, tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, context_bus=context, progress_queue=asyncio.Queue())
    assert result.status == 'success', result
    assert result.output == 'Observed value: 7.' and len(calls) == 1
    assert context.get('observed', group_id='group') == 'Observed value: 7.'
    assert provider.review_calls == 2
    assert result.verified_turn_result.response_fulfilled is True


@pytest.mark.asyncio
async def test_real_group_detects_missing_outcome_and_continues_with_owned_receipts(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.result_aggregator import ResultAggregator, GroupState
    from nullion.task_queue import TaskRegistry, TaskGroup, TaskRecord, TaskResult, TaskPriority, TaskStatus
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    units = []
    def unit_read(inv):
        units.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 'kg'})
    registry.register(ToolSpec('unit_read', 'Read the selected resource unit', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), unit_read)
    class Provider(ReasoningProvider):
        group_mode = False
        def create(self, **kw):
            system = kw.get('system', '')
            if 'You are summarizing the results' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                return text({'status': 'partial', 'answer': 'The selected value is 7. Its unit is still missing.',
                    'evaluated_task_ids': [row['task_id'] for row in packet['task_results']], 'missing_outcomes': ['Report the unit.'],
                    'assistance_plan':{'kind':'execution_improvement','task_ids':['child'],'tool_names':['unit_read']}})
            if not self.group_mode:
                return super().create(**kw)
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                    'material_requirements': [{'requirement': 'Report the selected value and its unit together.',
                        'requirement_kind': 'fact', 'evidence_subject_id': 'resource',
                        'eligible_evidence_tool_names': ['resource_read', 'unit_read']}]})
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                evidence = [row['invocation_id'] for row in packet['tool_evidence'] if row['tool_name'] in {'resource_read', 'unit_read'} and row['status'] == 'completed']
                assert len(evidence) == 2 and '7 kg' in packet['draft_final_response']
                return text({'disposition': 'complete', 'unresolved_requirements': [], 'retry_tool_names': [],
                    'all_material_requirements_enumerated': True, 'material_requirements': [{
                    'requirement_id': row['requirement_id'], 'satisfied': True, 'draft_covered': True,
                    'evidence_invocation_ids': evidence} for row in packet['material_requirement_contract']]})
            if not units:
                assert 'completed' in json.dumps(kw['messages'])
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'units', 'name': 'unit_read', 'input': {}}]}
            return text('The selected value is 7 kg.')
    provider = Provider('resource_read'); runner = AgentOrchestrator(model_client=provider)
    child = runner.run_turn(conversation_id='web:group', principal_id='web:group', user_message='Report the selected value.',
        conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    task_result = TaskResult('child', 'success', child.final_text)
    task_result.verified_turn_result = child
    task = TaskRecord('child', 'group', 'web:group', 'web:group', 'Read value', 'Report the selected value.',
        TaskStatus.COMPLETE, TaskPriority.NORMAL, ['resource_read', 'unit_read'], [], result=task_result)
    group = TaskGroup('group', 'web:group', '選択した値と単位を一緒に教えて。', tasks=[task])
    task_registry = TaskRegistry(); await task_registry.add_group(group)
    runner._group_execution_contexts = {'group': (registry, runtime.store, runtime.store)}
    provider.group_mode = True
    wire = []
    async def deliver(conversation, message, **kw):
        if not kw.get('is_status'):
            wire.append(message)
        return True
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=task_registry, model_client=provider,
        outbox_path=tmp_path/'outbox.db', recover_outcome=runner._recover_group_outcome)
    await aggregator._on_group_complete(GroupState('group', 'web:group', group.original_message), group)
    assert len(calls) == 1 and len(units) == 1
    assert any('7 kg' in message for message in wire), wire
    assert group.planner_metadata['outcome_review_status'] == 'complete'
    assert group.planner_metadata['delivery_status'] == 'completed'
    await aggregator._on_group_complete(GroupState('group', 'web:group', group.original_message), group)
    assert len(calls) == len(units) == 1


def test_cancellation_during_action_review_never_executes_the_write(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from threading import Event
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    cancelled = Event(); writes = []
    registry.register(ToolSpec('resource_write', 'Change resource', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2),
        lambda inv: writes.append(inv) or ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7}))
    class Provider:
        def create(self, **kw):
            if 'Workflow action preflight.' in kw.get('system', ''):
                cancelled.set()
                return text({'decision': 'proceed', 'issues': []})
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'w', 'name': 'resource_write', 'input': {}}]}
    result = AgentOrchestrator(model_client=Provider()).run_turn(conversation_id='web:cancel', principal_id='web:cancel',
        user_message='Apply the selected change.', conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, cancellation_checker=cancelled.is_set)
    assert not writes and result.final_text == 'Stopped by /stop.'


@pytest.mark.asyncio
async def test_delegated_timeout_during_preflight_cannot_start_a_late_write(tmp_path, monkeypatch):
    import asyncio
    from threading import Event
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner
    from nullion.mini_agent_runner import MiniAgentConfig
    from nullion.context_bus import ContextBus
    from nullion.task_queue import TaskRecord, TaskStatus, TaskPriority
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    started, release, finished = Event(), Event(), Event()
    writes = []
    registry.register(ToolSpec('resource_write', 'Change resource', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2),
        lambda inv: writes.append(inv) or ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7}))
    class Provider:
        def create(self, **kw):
            if 'Workflow action preflight.' in kw.get('system', ''):
                started.set(); release.wait(5); finished.set()
                return text({'decision': 'proceed', 'issues': []})
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'w', 'name': 'resource_write', 'input': {}}]}
    task = TaskRecord('child', 'group', 'web:timeout', 'web:timeout', 'Change resource', 'Apply selected change.',
        TaskStatus.RUNNING, TaskPriority.NORMAL, ['resource_write'], [])
    pending = asyncio.create_task(DeepAgentMiniAgentRunner().run(MiniAgentConfig('writer', task, timeout_s=.3),
        anthropic_client=Provider(), tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, context_bus=ContextBus(), progress_queue=asyncio.Queue()))
    assert await asyncio.to_thread(started.wait, 5)
    result = await asyncio.wait_for(pending, timeout=3)
    assert result.status == 'failure' and not writes
    release.set()
    assert await asyncio.to_thread(finished.wait, 3)
    await asyncio.sleep(.1)
    assert not writes


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['malformed', 'missing_task', 'contradictory', 'clarify'])
async def test_combined_delivery_rejects_invalid_or_incomplete_goal_review(tmp_path, mode):
    from nullion.result_aggregator import ResultAggregator, GroupState
    from nullion.task_queue import TaskGroup, TaskRegistry, TaskRecord, TaskResult, TaskStatus, TaskPriority
    task = TaskRecord('child', 'g', 'web:g', 'web:g', 'Read value', 'Report value', TaskStatus.COMPLETE,
        TaskPriority.NORMAL, [], [], result=TaskResult('child', 'success', 'Value: 7.'))
    group = TaskGroup('g', 'web:g', 'Report value and unit', tasks=[task])
    class Provider:
        def create(self, **kw):
            if 'You are summarizing the results' not in kw.get('system', ''):
                return text({})
            if mode == 'malformed':
                return text('invalid JSON')
            return text({'status': 'needs_user_input' if mode == 'clarify' else 'complete',
                'answer': 'Value: 7.', 'evaluated_task_ids': [] if mode == 'missing_task' else ['child'],
                'missing_outcomes': ['The unit is missing.'] if mode in {'contradictory', 'clarify'} else [],
                **({'question': 'Which unit should I use?', 'options': ['Metric', 'Imperial']} if mode == 'clarify' else {})})
    registry = TaskRegistry(); await registry.add_group(group)
    wire = []
    async def deliver(_conversation, value, **kw):
        if not kw.get('is_status'): wire.append(value)
        return True
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, model_client=Provider(),
        outbox_path=tmp_path/'outbox.db')
    await aggregator._on_group_complete(GroupState('g', 'web:g', group.original_message), group)
    assert group.planner_metadata['delivery_status'] == 'completed'
    assert aggregator._outbox.get('g')['work_complete'] is False
    assert wire
    if mode == 'clarify': assert '1. Metric\n2. Imperial' in wire[-1]
    else:
        assert 'Value: 7.' in wire[-1] and 'Review warning:' not in wire[-1]
        assert group.planner_metadata['outcome_verified'] is False


def test_mission_approval_resume_retains_actual_pending_batch_and_prior_evidence(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.missions import MissionRecord, MissionStep, MissionStatus, MissionContinuationPolicy
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.approvals import approve, create_permission_grant
    from nullion.runtime import bootstrap_persistent_runtime
    runtime, registry, reads = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    writes = []
    def write(inv):
        writes.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7, 'updated': True})
    registry.register(ToolSpec('guarded_write', 'Change resource after approval', ToolRiskLevel.HIGH,
        ToolSideEffectClass.WRITE, True, 2), write)
    class Provider:
        calls = 0
        def create(self, **kw):
            from tests.workflow_provider_protocol import workflow_provider_reply
            reply = workflow_provider_reply(kw)
            if reply is not None: return reply
            self.calls += 1
            if self.calls in {1, 3}:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': str(self.calls),
                    'name': 'resource_read' if self.calls == 1 else 'guarded_write', 'input': {}}]}
            return text('Value: 7. The selected change is complete.' if self.calls >= 4 else 'Value: 7.')
    provider = Provider(); runner = AgentOrchestrator(model_client=provider)
    mission = MissionRecord('mission', 'web:mission', 'Read then change', 'Report the value and apply the selected change.',
        MissionStatus.PENDING, MissionContinuationPolicy.AUTO_FINISH, steps=(
            MissionStep('first', 'Read selected value', 'pending', 'execute'),
            MissionStep('second', 'Apply selected change', 'pending', 'execute')))
    kw = dict(conversation_id='web:mission', principal_id='web:mission', tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, runtime_store=runtime.store)
    first = runner.run_mission(mission=mission, conversation_history=[], **kw)
    assert first.status == 'suspended' and len(reads) == 1 and not writes
    suspended = runtime.store.get_suspended_turn(first.suspended_approval_id)
    assert suspended.messages_snapshot[-1]['role'] == 'assistant'
    assert any(b.get('name') == 'guarded_write' for b in suspended.messages_snapshot[-1]['content'])
    request = runtime.store.get_approval_request(first.suspended_approval_id)
    runtime.store.add_approval_request(approve(request, decided_by='fixture'))
    runtime.store.add_permission_grant(create_permission_grant(approval_id=request.approval_id,
        principal_id='web:mission', permission='tool:guarded_write', granted_by='fixture'))
    runtime.checkpoint(force=True)
    reopened = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    kw.update(policy_store=reopened.store, approval_store=reopened.store, runtime_store=reopened.store)
    final = AgentOrchestrator(model_client=provider).resume_mission(mission_id='mission', **kw)
    assert final.status == 'completed', final
    assert len(reads) == len(writes) == 1
    assert {r.tool_name for r in final.tool_results if r.status == 'completed'} == {'resource_read', 'guarded_write'}


def test_action_preflight_repairs_wrong_target_and_executes_only_corrected_plan(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    writes = []
    def write(inv):
        writes.append(inv.arguments['target'])
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'target': 'selected', 'value': 7})
    registry.register(ToolSpec('resource_write', 'Update selected resource', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2), write)
    class Provider(ReasoningProvider):
        proposals = []
        def create(self, **kw):
            system = kw.get('system', '')
            if 'Workflow action preflight.' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                target = packet['proposed_action']['arguments']['target']
                self.proposals.append(target)
                return text({'decision': 'revise', 'issues': ['Use the selected target from the request.']} if target == 'other'
                    else {'decision': 'proceed', 'issues': []})
            if 'material-requirements planner' in system or 'semantic completion reviewer' in system:
                return super().create(**kw)
            self.agent_calls += 1
            if self.agent_calls < 3:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': str(self.agent_calls),
                    'name': 'resource_write', 'input': {'target': 'other' if self.agent_calls == 1 else 'selected'}}]}
            return text('Observed value: 7.')
    provider = Provider('resource_write')
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:repair-action',
        principal_id='web:repair-action', user_message='選択された対象だけを更新してください。', conversation_history=[],
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    assert result.response_fulfilled is True, result
    assert writes == ['selected'] and provider.proposals == ['other', 'selected']
    assert result.tool_results[0].output['executed'] is False


def test_review_receipt_survives_temporary_tool_scope_and_delivery_bookkeeping(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator, _FocusedRecoveryToolRegistry
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass
    runtime, registry, calls = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    registry.register(ToolSpec('unrelated_read', 'Read another resource', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), lambda invocation: pytest.fail('Unrelated resource was read'))
    provider = ReasoningProvider('resource_read')
    runner = AgentOrchestrator(model_client=provider)
    focused = _FocusedRecoveryToolRegistry(registry, {'resource_read'})
    kw = dict(conversation_id='web:scope', principal_id='web:scope', user_message='Report the selected value.',
        conversation_history=[], policy_store=runtime.store, approval_store=runtime.store)
    result = runner.run_turn(**kw, tool_registry=focused)
    count = (provider.agent_calls, provider.review_calls, provider.contract_calls)
    result.tool_results[-1].output['artifact_delivery_manifest'] = {'version': 1, 'selected_paths': []}
    same = runner.review_turn_result(result, **kw, tool_registry=registry)
    assert same is result and count == (provider.agent_calls, provider.review_calls, provider.contract_calls)
    assert len(calls) == 1


@pytest.mark.parametrize('postcode', ['A1B2C3', '84205', 'Z-α7'])
@pytest.mark.parametrize('matching', [True, False])
def test_weather_chat_refines_parent_locality_using_structured_postal_evidence(tmp_path, monkeypatch, postcode, matching):
    from fastapi.testclient import TestClient
    from urllib.parse import urlparse, parse_qs
    from nullion import chat_store
    from nullion.web_app import create_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, register_weather_forecast_tool
    runtime, _, _ = runtime_and_registry(tmp_path, monkeypatch, 'unused')
    monkeypatch.setattr(chat_store, '_store', None)
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    calls = []
    def source(url, timeout):
        calls.append(url)
        query = parse_qs(urlparse(url).query)
        if 'geocoding-api' in url:
            return {'results': [{'name': 'Parent locality', 'latitude': 1, 'longitude': 2,
                'postcodes': [postcode, 'another-code']}]}
        if 'nominatim' in url:
            assert query['q'] == [postcode]
            return [{'lat': '3', 'lon': '4', 'display_name': 'Selected area ' + postcode,
                'address': {'postcode': postcode if matching else 'different-code', 'country': 'Fixture'}}]
        assert query['latitude'] == ['3.0'] and query['longitude'] == ['4.0']
        return {'current': {'temperature_2m': 52, 'weather_code': 0}, 'daily': {}, 'timezone': 'UTC'}
    registry = register_weather_forecast_tool(ToolRegistry(), json_get=source)
    class Provider:
        def create(self, **kw):
            from tests.workflow_provider_protocol import workflow_provider_reply
            reply = workflow_provider_reply(kw)
            if reply is not None: return reply
            if not calls:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'forecast',
                    'name': 'weather_forecast', 'input': {'location_text': postcode}}]}
            return text('The selected forecast is 52°F.')
    with TestClient(create_app(runtime, AgentOrchestrator(model_client=Provider()), registry)) as api:
        response = api.post('/api/chat', json={'text': '選択した地域の天気：' + postcode, 'conversation_id': 'web:postal'})
        assert response.status_code == 200, response.text
        value = response.json()
    assert len([url for url in calls if 'nominatim' in url]) == 1
    forecast_calls = [url for url in calls if urlparse(url).netloc == 'api.open-meteo.com']
    assert len(forecast_calls) == int(matching)
    turn = next(row for row in reversed(runtime.store.list_conversation_events('web:postal'))
        if row.get('event_type') == 'conversation.chat_turn')
    result = next(row for row in turn['tool_results'] if row['tool_name'] == 'weather_forecast')
    if matching:
        assert result['output']['location']['postal_code'] == postcode
        assert '52' in value['text']
    else:
        assert result['status'] == 'failed' and '52' not in value['text']


@pytest.mark.parametrize('status', ['completed', 'failed'])
@pytest.mark.parametrize('effect', ['write', 'external_write', 'account_write'])
def test_completion_repair_reuses_dispatched_action_even_after_result_failure(tmp_path, monkeypatch, status, effect):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'unused')
    dispatched = []
    def dispatch(inv):
        dispatched.append(inv.invocation_id)
        return ToolResult(inv.invocation_id, 'opaque_dispatch', status, {'action_receipt': {
            'type': 'action_receipt', 'action': 'started', 'object_type': 'operation',
            'summary': 'Operation accepted.', 'object_id': 'selected'}},
            error='Result delivery failed' if status == 'failed' else None)
    registry.register(ToolSpec('opaque_dispatch', 'Dispatch selected operation', ToolRiskLevel.LOW,
        ToolSideEffectClass(effect), False, 2, replay_safe_after_completion=True), dispatch)
    class Provider:
        turns = 0
        reviews = 0
        def create(self, **kw):
            if 'semantic completion reviewer' in str(kw.get('system', '')):
                self.reviews += 1
                return text({'disposition': 'retry' if self.reviews == 1 else 'blocked',
                    'unresolved_requirements': ['The returned result is missing.'],
                    'retry_tool_names': ['opaque_dispatch'] if self.reviews == 1 else [],
                    'user_facing_failure': 'The operation was dispatched, but its result is unavailable.'})
            response = workflow_provider_reply(kw)
            if response is not None: return response
            self.turns += 1
            if self.turns in (1, 3):
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': str(self.turns),
                    'name': 'opaque_dispatch', 'input': {'target': 'selected'}}]}
            return text('Operation accepted.')
    model = Provider()
    result = AgentOrchestrator(model_client=model).run_turn(conversation_id='web:dispatch', principal_id='web:dispatch',
        user_message='選択した操作を一度だけ実行して、結果を伝えて。', conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=5)
    assert len(dispatched) == 1, result
    assert len([row for row in result.tool_results if row.tool_name == 'opaque_dispatch']) == 1
    assert result.response_fulfilled is not True
    assert result.final_text == 'Operation accepted.'
    receipt = result.tool_results[0]
    assert receipt.status == status
    assert receipt.output['action_receipt']['object_id'] == 'selected'
    if status == 'failed':
        assert receipt.error == 'Result delivery failed'


@pytest.mark.parametrize('owner,prompt', [('opaque_worker', 'この条件を90秒監視して通知して。'), ('مراقب', 'راقب هذا الشرط لمدة 90 ثانية وأبلغني.')])
def test_accepted_background_setup_finishes_without_waiting_for_future_match(tmp_path, monkeypatch, owner, prompt):
    import sqlite3
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'unused')
    registry.register(ToolSpec('collection_read', 'Read selected collection', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'completed',
            {'items': [{'value': 7}], 'item_count': 1, 'coverage': {'scope_found': True, 'truncated': False},
             'source_record_collections': [{'path': ['items'], 'change_identity_fields': ['value']}]}))
    with sqlite3.connect(runtime.checkpoint_path) as db:
        db.execute('CREATE TABLE accepted_jobs (id TEXT PRIMARY KEY, state TEXT)')
    writes = []
    def accept(inv):
        writes.append(inv.invocation_id)
        with sqlite3.connect(runtime.checkpoint_path) as db:
            db.execute('INSERT INTO accepted_jobs VALUES (?,?)', ('job-a', 'waiting'))
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {
            'action_receipt': {'type': 'action_receipt', 'action': 'accepted', 'object_type': 'background_job',
                'object_id': 'job-a', 'summary': 'Configured the requested observation.', 'delivery_required': True},
            'deferred_execution': {'version': 1, 'phase': 'accepted', 'owner': owner, 'handle': 'job-a',
                'configuration': {'duration_seconds': 90, 'condition': 'selected-condition'}}, 'state': 'active'})
    registry.register(ToolSpec('opaque_accept', 'Accept a background observation', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2), accept)
    class Provider:
        step = 0
        checks = 0
        def create(self, **kw):
            if 'Workflow action preflight.' in str(kw.get('system', '')):
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                observed = next(row for row in packet['execution_evidence'] if row['tool_name'] == 'collection_read')
                assert observed['output']['item_count'] == 1
                assert observed['output']['coverage'] == {'scope_found': True, 'truncated': False}
                assert observed['output']['source_record_collections'][0]['change_identity_fields'] == ['value']
            if 'material-requirements planner' in str(kw.get('system', '')) or 'semantic completion reviewer' in str(kw.get('system', '')):
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                assert packet['accepted_background_execution'][0]['owner'] == owner
                assert packet['accepted_background_execution'][0]['configuration']['duration_seconds'] == 90
                self.checks += 1
                if 'material-requirements planner' in str(kw.get('system', '')):
                    contract = {'all_material_requirements_enumerated': True, 'requested_artifact_extensions': [],
                        'material_requirements': [
                            {'requirement': 'Configure the selected background task', 'requirement_kind': 'action',
                             'evidence_subject_id': 'setup', 'eligible_evidence_tool_names': ['opaque_accept']},
                            {'requirement': 'Verify source preparation', 'requirement_kind': 'constraint',
                             'delivery_required': False, 'evidence_subject_id': 'source', 'eligible_evidence_tool_names': ['collection_read']}]}
                    for row in contract['material_requirements']:
                        row.setdefault('completion_owner', 'producer')
                        row.setdefault('delivery_required', True)
                        row['artifact_member_subject_ids'] = []
                    from jsonschema import Draft202012Validator
                    Draft202012Validator(packet['material_requirement_response_schema']).validate(contract)
                    return text(contract)
            reply = workflow_provider_reply(kw, action=True)
            if reply is not None:
                if 'semantic completion reviewer' in str(kw.get('system', '')):
                    coverage = json.loads(reply['content'][0]['text'])
                    for row in coverage['material_requirements']:
                        if row['requirement_id'] == 'r2': row['draft_covered'] = False
                    reply['content'][0]['text'] = json.dumps(coverage)
                return reply
            self.step += 1
            if self.step == 1:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'prepare', 'name': 'collection_read', 'input': {}}]}
            if self.step == 2:
                return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'start', 'name': 'opaque_accept', 'input': {}}]}
            return text('Configured the requested observation. The background condition is still pending.')
    model = Provider()
    result = AgentOrchestrator(model_client=model).run_turn(conversation_id='web:background', principal_id='web:background',
        user_message=prompt, conversation_history=[], tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, max_iterations=4)
    assert result.response_fulfilled is True and 'still pending' in result.final_text, result
    assert len(writes) == 1 and model.checks == 2
    assert '7' not in result.final_text
    assert result.final_text.count('Configured the requested observation.') == 1
    assert [row.tool_name for row in result.tool_results] == ['collection_read', 'opaque_accept']
    with sqlite3.connect(runtime.checkpoint_path) as db:
        assert db.execute('SELECT state FROM accepted_jobs').fetchone()[0] == 'waiting'
    contract_event = next(row for row in runtime.store.list_conversation_events('web:background')
        if row.get('event_type') == 'conversation.completion_requirement_contract')
    assert [row['requirement_kind'] for row in contract_event['requirement_rows']] == ['action', 'constraint']
    assert all('requirement' not in row for row in contract_event['requirement_rows'])


def test_direct_shortcut_repair_receives_actual_evidence_without_rereading(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator, TurnResult
    from nullion.tools import ToolInvocation
    runtime, registry, reads = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    observed = registry.invoke(ToolInvocation('actual-read', 'resource_read', 'web:direct-repair', {}))
    class Provider(ReasoningProvider):
        def create(self, **kw):
            if 'semantic completion reviewer' in str(kw.get('system', '')):
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                if '7' not in packet['draft_final_response']:
                    self.review_calls += 1
                    return text({'disposition': 'retry', 'unresolved_requirements': ['Report the observed value.'], 'retry_tool_names': []})
                return super().create(**kw)
            if 'material-requirements planner' in str(kw.get('system', '')):
                return super().create(**kw)
            retained = next(message for message in kw['messages'] if any(
                'request_continuation_tool_evidence' in block.get('text', '') for block in message.get('content', [])))
            assert 'actual-read' in json.dumps(retained) and '7' in json.dumps(retained)
            self.agent_calls += 1
            return text('Observed value: 7.')
    provider = Provider('resource_read')
    result = AgentOrchestrator(model_client=provider).review_turn_result(
        TurnResult('direct', 'Read complete.', [observed]), conversation_id='web:direct-repair',
        principal_id='web:direct-repair', user_message='観測値を教えて。', conversation_history=[],
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
    assert result.response_fulfilled is True and '7' in result.final_text
    assert len(reads) == 1 and provider.agent_calls == 1 and provider.review_calls == 2


@pytest.mark.parametrize('prompt', ['Report the selected resource value.', '選択した対象の値を教えて。'])
def test_recovered_read_plan_does_not_revive_a_blocked_preparatory_step(tmp_path, monkeypatch, prompt):
    from datetime import datetime, timezone
    from nullion.agent_orchestrator import AgentOrchestrator, TurnContinuationEvidence, _BlockedToolRegistry
    from nullion.tools import ToolInvocation, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolScopeDecision, build_turn_tool_evidence
    from nullion.task_frames import (TaskFrame, TaskFrameStatus, TaskFrameOperation, TaskFrameExecutionContract,
        TaskFrameOutputContract, TaskFrameFinishCriteria)
    runtime, registry, reads = runtime_and_registry(tmp_path, monkeypatch, 'resource_read')
    registry.get_spec('resource_read').capability_tags = ('web',)
    registry.register(ToolSpec('browser_navigate', 'Prepare selected source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)),
        lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'failed', {'reason': 'handler_timeout'}, 'Source preparation timed out'))
    cid = 'web:recovered-read'
    failed = registry.invoke(ToolInvocation('preparation', 'browser_navigate', cid, {}))
    scoped = ScopedTurnToolRegistry(registry, evidence=build_turn_tool_evidence(user_message=prompt, conversation_result=None),
        tool_scope_decision=TurnToolScopeDecision(requested_tool_names=('browser_navigate', 'resource_read'),
            required_tool_names=('browser_navigate', 'resource_read'), web_action='live_research', valid=True))
    blocked = _BlockedToolRegistry(scoped, {'browser_navigate'})
    scope, recovered = blocked.apply_scope_request(ToolInvocation('new-plan', 'request_tool_scope', cid,
        {'capabilities': ['web'], 'tool_names': ['resource_read'], 'required_tool_names': ['resource_read']}))
    assert scope.status == 'completed', scope
    assert scope.output['retired_preparatory_read_tools'] == ['browser_navigate']
    observed = recovered.invoke(ToolInvocation('replacement', 'resource_read', cid, {}))
    now = datetime.now(timezone.utc)
    frame = TaskFrame('selected-frame', cid, 'branch', 'parent', None, TaskFrameStatus.ACTIVE,
        TaskFrameOperation.ANSWER_WITH_CONTEXT, None, TaskFrameExecutionContract(), TaskFrameOutputContract(),
        TaskFrameFinishCriteria(required_tool_completion=('browser_navigate', 'resource_read')), prompt, now, now)
    runtime.store.add_task_frame(frame); runtime.store.set_active_task_frame_id(cid, frame.frame_id)
    recovered.turn_continuation_evidence = TurnContinuationEvidence(cid, cid, ('parent',),
        (failed, scope, observed), same_request_retry=True, user_message=prompt)
    class Provider(ReasoningProvider):
        def create(self, **kw):
            if 'material-requirements planner' in str(kw.get('system', '')) or 'semantic completion reviewer' in str(kw.get('system', '')):
                return super().create(**kw)
            self.agent_calls += 1
            return text('Observed value: 7.')
    model = Provider('resource_read')
    result = AgentOrchestrator(model_client=model).run_turn(conversation_id=cid, principal_id=cid,
        user_message=prompt, conversation_history=[], tool_registry=recovered,
        policy_store=runtime.store, approval_store=runtime.store)
    assert result.response_fulfilled is True and '7' in result.final_text, result
    assert len(reads) == 1 and model.agent_calls == 1
    assert result.tool_results[0].status == 'failed'
    runtime.checkpoint(force=True)


def test_invalid_tracker_setup_is_corrected_using_verified_probe_without_retiring_capability(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.live_tracking_tools import register_live_tracker_tools, tracker_store_for_registry
    from nullion.tool_repair_contract import tool_repair_kind
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime, registry, _ = runtime_and_registry(tmp_path, monkeypatch, 'unused')
    registry.live_tracker_store_path = tmp_path/'live-trackers.db'
    from nullion.turn_context_policy import TurnToolScopeDecision
    registry.turn_tool_scope_decision = TurnToolScopeDecision(tracking_mode='live', required_tool_names=('create_live_tracker',), valid=True)
    registry.register(ToolSpec('probe_read', 'Read selected observation', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, input_schema={'type':'object','properties':{'scope':{'type':'string'}}}),
        lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value':7}))
    results=[]
    class Provider:
        step=0
        def create(self, **kw):
            if 'material-requirements planner' in str(kw.get('system','')):
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'Start the requested observation', 'requirement_kind':'action',
                        'evidence_subject_id':'setup','eligible_evidence_tool_names':['create_live_tracker']}]})
            if 'semantic completion reviewer' in str(kw.get('system','')):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                if not any(row['tool_name']=='create_live_tracker' and row['status']=='completed' for row in packet['tool_evidence']):
                    return text({'disposition':'retry','unresolved_requirements':['Correct the prepared probe and start the tracker.'],
                        'retry_tool_names':['create_live_tracker']})
            response=workflow_provider_reply(kw, action=True)
            if response is not None:return response
            self.step+=1
            if self.step==1:
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':'probe_read','input':{'scope':'selected'}}]}
            if self.step in (2,3):
                if self.step==3:
                    failed=next(row for row in results if row.tool_name=='create_live_tracker' and row.status=='failed')
                    assert tool_repair_kind(failed)=='correct_arguments'
                    assert failed.output['executed'] is False
                    assert not tracker_store_for_registry(registry).list('workspace_admin')
                    probe=next(row for row in failed.output['ready_prepared_probes'] if row['tool_name']=='probe_read')
                else:probe={'tool_name':'probe_read','arguments':{'scope':'different'}}
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'create'+str(self.step),'name':'create_live_tracker',
                    'input':{'name':'Observation correction','duration_seconds':60,'interval_seconds':5,
                        'probe':[probe],'matcher':{'kind':'structured','summary':'The selected threshold was reached.','conditions':[
                            {'step':0,'pointer':'/value','op':'greater_than','value':100}]}}}]}
            return text('The requested tracker is active for 60 seconds, checking every 5 seconds.')
    provider=Provider();register_live_tracker_tools(registry,model_client_getter=lambda:provider)
    result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:tracker-arguments',principal_id='web:tracker-arguments',
        user_message='この条件を60秒間、5秒ごとに確認して通知して。',conversation_history=[],tool_registry=registry,
        policy_store=runtime.store,approval_store=runtime.store,tool_result_callback=results.append,max_iterations=6)
    assert result.response_fulfilled is True,(provider.step, [(row.tool_name,row.status,row.error,row.output) for row in result.tool_results])
    creates=[row for row in result.tool_results if row.tool_name=='create_live_tracker']
    assert [row.status for row in creates]==['failed','completed']
    jobs=tracker_store_for_registry(registry).list('workspace_admin');assert len(jobs)==1 and jobs[0]['state']=='active'


@pytest.mark.parametrize('prompt', ['Read my selected account record.', '選択したアカウントの記録を読んで。'])
@pytest.mark.parametrize('contradictory_input', [None, 'source_selection', 'source_resolution'])
def test_review_retains_runtime_validated_read_selection(tmp_path, monkeypatch, prompt, contradictory_input):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime,registry,reads=runtime_and_registry(tmp_path,monkeypatch,'account_record_read')
    selection={'message':'Selected read scope ready', 'source_evidence_verified':True,
        'connector_source_user_requested':True, 'source_selection_required':False,
        'source_resolution_required':False, 'connector_source_evidence':prompt,
        'available_tools':['account_record_read'], 'required_tool_names':['account_record_read']}
    registry.register(ToolSpec('request_tool_scope','Resolve requested read scope',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2),lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'completed',selection))
    class Provider(ReasoningProvider):
        def create(self,**kw):
            system=kw.get('system','')
            if 'material-requirements planner' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                assert packet['source_authorization']['source_evidence_verified'] is True
                assert packet['source_authorization']['source_selection_required'] is False
            if 'semantic completion reviewer' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                contract=packet['required_source_and_action_contract']
                assert contract['source_evidence_verified'] is True
                assert contract['connector_source_user_requested'] is True
                assert contract['source_selection_required'] is False
                scope=next(row for row in packet['tool_evidence'] if row['tool_name']=='request_tool_scope')
                assert scope['output']['source_evidence_verified'] is True
                if contradictory_input and self.review_calls == 0:
                    self.review_calls += 1
                    return text({'disposition':'needs_user_input','retry_tool_names':[],
                        'unresolved_requirements':['Choose the source again.'],
                        'user_facing_failure':'Selected source not authorized.',
                        'clarification':{'question':'Which source?', 'options':['Selected source','Cancel'],
                            'input_kind':contradictory_input}})
                if contradictory_input:
                    assert 'clarification_contradicts_verified_source_contract' in packet['review_protocol_validation_errors']
            if 'material-requirements planner' not in system and 'semantic completion reviewer' not in system and self.agent_calls==0:
                self.agent_calls+=1
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'scope','name':'request_tool_scope','input':{}}]}
            if 'material-requirements planner' not in system and 'semantic completion reviewer' not in system and self.agent_calls==1:
                self.agent_calls+=1
                return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'read','name':self.name,'input':{}}]}
            return super().create(**kw)
    provider=Provider('account_record_read')
    result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:selected',principal_id='web:selected',
        user_message=prompt,conversation_history=[],tool_registry=registry,policy_store=runtime.store,
        approval_store=runtime.store,max_iterations=5)
    assert result.response_fulfilled is True and result.final_text=='Observed value: 7.'
    assert len(reads)==1 and provider.review_calls==1+bool(contradictory_input)


@pytest.mark.parametrize('prompt',['Report the selected record value.','選択した記録の値を教えて。'])
@pytest.mark.parametrize('record_count',[1,6,24])
@pytest.mark.parametrize('invalid_request',[None,'schema','foreign','pointer'])
def test_review_inspects_retained_full_evidence_without_repeating_source_read(tmp_path,monkeypatch,prompt,record_count,invalid_request):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=ToolRegistry();reads=[]
    padding=320 if record_count==24 else 700
    body='A'*padding+'\nReading = 7.\n'+'Z'*padding
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'id':'chosen','summary':'Read complete',
            **({'message':{'id':'chosen','body':body}} if record_count==1 else
               {'messages':[{'id':str(i),'body':body} for i in range(record_count)]})})
    registry.register(ToolSpec('opaque_record_read','Read selected record',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),read)
    class Provider(ReasoningProvider):
        inspections=0
        invalid_sent=False
        review_timeouts=()
        def create(self,**kw):
            if 'semantic completion reviewer' in kw.get('system',''):
                if self.review_timeouts:
                    assert 0 < kw['timeout'] <= self.review_timeouts[-1]
                self.review_timeouts=(*self.review_timeouts, kw['timeout'])
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                evidence=next(row for row in packet['tool_evidence'] if row['tool_name']==self.name)
                if 'additional_evidence' not in packet:
                    assert 'Reading = 7.' not in json.dumps(evidence['output'])
                    if invalid_request and not self.invalid_sent:
                        self.invalid_sent=True
                        request={'invocation_id':evidence['invocation_id'],
                            'json_pointer':'/message/body' if record_count==1 else '/messages/0/body',
                            'offset':0,'length':12000}
                        if invalid_request=='schema':request['length']=12001
                        elif invalid_request=='foreign':request['invocation_id']='foreign-request'
                        else:request['json_pointer']='/unavailable/body'
                        return text({'disposition':'inspect_evidence','evidence_requests':[request]})
                    if invalid_request:
                        assert packet['evidence_inspection_validation_error']['status']=='invalid_evidence_inspection'
                    self.inspections+=1
                    return text({'disposition':'inspect_evidence','evidence_requests':[{
                        'invocation_id':evidence['invocation_id'],'json_pointer':('/message/body' if record_count==1 else '/messages/'+str(i)+'/body'),
                        'offset':0,'length':12000} for i in range(record_count)]})
                assert len(packet['additional_evidence'])==record_count
                from nullion.agent_orchestrator import _completion_review_request_timeout
                assert 0 < kw['timeout'] <= _completion_review_request_timeout(kw['max_tokens'], request_chars=len(json.dumps(packet,ensure_ascii=False)))
                for inspected in packet['additional_evidence']:
                    assert inspected['invocation_id']==evidence['invocation_id']
                    assert inspected['content']==body and inspected['complete'] is True
            return super().create(**kw)
    provider=Provider('opaque_record_read')
    result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:inspect',principal_id='web:inspect',
        user_message=prompt,conversation_history=[],tool_registry=registry,policy_store=runtime.store,
        approval_store=runtime.store,max_iterations=4)
    assert result.response_fulfilled is True and result.final_text=='Observed value: 7.', json.dumps([
        {k:row.get(k) for k in ('event_type','review_error','response_diagnostics','disposition','reason')}
        for row in runtime.store.list_conversation_events('web:inspect')
        if row.get('event_type') in {'conversation.completion_review', 'conversation.completion_requirement_contract'}])
    assert len(reads)==1 and provider.inspections==1 and provider.review_calls==1 and provider.agent_calls==2
    review=next(row for row in runtime.store.list_conversation_events('web:inspect') if row.get('event_type')=='conversation.completion_review')
    assert review['model_attempts']==2+bool(invalid_request)
    assert review['response_diagnostics'][int(bool(invalid_request))]['status']=='evidence_inspected'
    assert review['review_error'] is None


def test_repeated_invalid_inspection_stops_without_new_source_execution(tmp_path,monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    runtime,registry,reads=runtime_and_registry(tmp_path,monkeypatch,'record_read')
    class Provider(ReasoningProvider):
        rejected=0
        def create(self,**kw):
            if 'semantic completion reviewer' in kw.get('system',''):
                self.rejected+=1
                return text({'disposition':'inspect_evidence','evidence_requests':[
                    {'invocation_id':'another-request','json_pointer':''}]})
            return super().create(**kw)
    provider=Provider('record_read')
    result=AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:rejected-inspection',
        principal_id='web:rejected-inspection',user_message='記録の値を確認。',conversation_history=[],
        tool_registry=registry,policy_store=runtime.store,approval_store=runtime.store,max_iterations=5)
    assert result.response_fulfilled is None and not result.artifacts
    assert 'Observed value: 7.' in result.final_text and 'Review warning:' not in result.final_text
    assert len(reads)==1 and provider.agent_calls==2 and provider.rejected==2
    reviews=[r for r in runtime.store.list_conversation_events('web:rejected-inspection')
        if r.get('event_type')=='conversation.completion_review']
    assert reviews[-1]['review_error']=='invalid_evidence_inspection'

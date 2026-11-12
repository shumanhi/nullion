"""Real graph/SQLite and adapter recovery; only external provider is scripted."""
import os
import json
import statistics
import time
from types import SimpleNamespace

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('prompt', ['選択した値を確認してください。', 'Comprueba el valor seleccionado.'])
@pytest.mark.parametrize('surface', ['web', 'telegram', 'slack', 'discord'])
def test_real_producer_checkpoint_prevents_adapter_and_history_restart(tmp_path, monkeypatch, prompt, surface):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import _run_chat_turn_common_retries, _run_chat_turn_saved_history_retry
    from nullion.completion_pipeline import published_completion
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult

    runtime = setup_runtime(tmp_path, monkeypatch)
    import nullion.agent_orchestrator as agent
    review_boundary_calls = []
    actual_review = agent._review_risky_agent_completion
    def observe_review(*args, **kwargs):
        review_boundary_calls.append(kwargs.get("final_text"))
        return actual_review(*args, **kwargs)
    monkeypatch.setattr(agent, '_review_risky_agent_completion', observe_review)
    registry = ToolRegistry()
    reads = []
    def read(invocation):
        reads.append(invocation)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('sensor_read', 'Read selected value', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        producer_calls = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None:
                return protocol
            self.producer_calls += 1
            return tool_response('sensor_read', {}) if not reads else text_response('Observed value: 7.')
    provider = Provider()
    runner = AgentOrchestrator(model_client=provider)
    conversation = surface + ':selected'
    result = runner.run_turn(conversation_id=conversation, principal_id=conversation,
        user_message=prompt, conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store, max_iterations=5)
    assert result.final_text == 'Observed value: 7.' and result.response_fulfilled is True
    assert published_completion(result, prompt) is not None
    assert review_boundary_calls == ['Observed value: 7.']
    calls_before = provider.producer_calls
    def retry(history, active_registry):
        return runner.run_turn(conversation_id=conversation, principal_id=conversation,
            user_message=prompt, conversation_history=history, tool_registry=active_registry,
            policy_store=runtime.store, approval_store=runtime.store, max_iterations=5)
    recovered = _run_chat_turn_common_retries(initial_result=result, base_history=[],
        active_tool_registry=registry, base_tool_registry=registry, runtime=runtime,
        model_client=provider, user_message=prompt, schema_user_message=prompt,
        evidence=SimpleNamespace(), run_turn=retry, conversation_id=conversation, principal_id=conversation)
    history, _, retried = _run_chat_turn_saved_history_retry(initial_result=recovered.result,
        automatic_history_context='Prior unrelated task', allow_saved_history_retry=True,
        build_history_context=lambda: 'Prior unrelated task', base_history=[], active_tool_registry=registry,
        base_tool_registry=registry, turn_tool_evidence=SimpleNamespace(), run_turn=retry)
    assert history is result and not retried
    assert provider.producer_calls == calls_before == 2 and len(reads) == 1
    assert runner.review_turn_result(result, conversation_id=conversation, principal_id=conversation,
        user_message=prompt, conversation_history=[], tool_registry=registry,
        policy_store=runtime.store, approval_store=runtime.store) is result
    assert review_boundary_calls == ['Observed value: 7.']
    assert runtime.store.list_recent_conversation_events(conversation, event_type='conversation.completion_review')


def test_plain_turn_completion_adds_no_model_pass_or_persistence_latency(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    import nullion.workflow_recovery as recovery
    runtime = setup_runtime(tmp_path, monkeypatch)
    def forbidden(*args):
        time.sleep(.3)
        raise AssertionError('Ordinary chat entered a recovery persistence dependency')
    monkeypatch.setattr(recovery, '_checkpoint_store', forbidden)
    class Provider:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return text_response('こんにちは。')
    provider = Provider(); runner = AgentOrchestrator(model_client=provider)
    durations = []
    for index in range(8):
        start = time.monotonic()
        result = runner.run_turn(conversation_id=f'web:plain-{index}', principal_id='web:plain',
            user_message='こんにちは', conversation_history=[], tool_registry=ToolRegistry(),
            policy_store=runtime.store, approval_store=runtime.store)
        durations.append(time.monotonic() - start)
        assert result.final_text == 'こんにちは。' and not result.workflow_checkpoint
    assert provider.calls == 8
    assert statistics.mean(durations) < .3 and sorted(durations)[-1] < 1


@pytest.mark.parametrize('continuation', ['resume', 'scope_retry'])
def test_real_resume_keeps_exhausted_improvement_window_and_completed_reads(tmp_path, monkeypatch, continuation):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); reads = []
    def read(invocation):
        reads.append(invocation)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('sensor_read', 'Read selected value', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    class Provider:
        producer_calls = 0
        def create(self, **kwargs):
            if 'semantic completion reviewer' in (kwargs.get('system') or ''):
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                proof = next(row['invocation_id'] for row in packet['tool_evidence']
                    if row['tool_name'] == 'sensor_read' and row['status'] == 'completed')
                return text_response(json.dumps({'disposition': 'retry',
                    'all_material_requirements_enumerated': True, 'retry_tool_names': ['sensor_read'],
                    'unresolved_requirements': ['Check the second selected value.'],
                    'material_requirements': [{'requirement_id': row['requirement_id'], 'satisfied': False,
                        'draft_covered': False, 'evidence_invocation_ids': [proof]}
                        for row in packet['material_requirement_contract']]}))
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None:
                return protocol
            self.producer_calls += 1
            return tool_response('sensor_read', {}) if not reads else text_response('First observed value: 7.')
    provider = Provider(); runner = AgentOrchestrator(model_client=provider)
    prompt = '選択した値を確認してください。'
    result = runner.run_turn(conversation_id='web:resume', principal_id='web:resume', user_message=prompt,
        conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store,
        max_iterations=10)
    checkpoint = result.completion_review_handoff.recovery_checkpoint
    assert checkpoint.attempts >= 2 and checkpoint.progress
    before = provider.producer_calls
    if continuation == 'resume':
        resumed = runner.resume_turn(conversation_id='web:resume', principal_id='web:resume', user_message=prompt,
            messages_snapshot=[{'role': 'user', 'content': [{'type': 'text', 'text': prompt}]}],
            previous_result=result, tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store,
            max_iterations=10)
    else:
        from nullion.chat_operator import _registry_with_request_retry_evidence
        scoped = _registry_with_request_retry_evidence(registry, result=result,
            conversation_id='web:resume', principal_id='web:resume', user_message=prompt)
        resumed = runner.run_turn(conversation_id='web:resume', principal_id='web:resume', user_message=prompt,
            conversation_history=[], tool_registry=scoped, policy_store=runtime.store,
            approval_store=runtime.store, max_iterations=10)
    assert provider.producer_calls - before == 1
    assert len(reads) == 1 and '7' in resumed.final_text
    assert resumed.completion_review_handoff.recovery_checkpoint.attempts == checkpoint.attempts
    assert resumed.response_fulfilled is not True


@pytest.mark.parametrize('prompt', ['選択した記録を更新してください。', 'Actualiza el registro seleccionado.'])
def test_interrupted_improvement_does_not_restore_action_totals_from_before_a_write(tmp_path, monkeypatch, prompt):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult, _build_file_write_handler
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from datetime import UTC, datetime
    runtime = setup_runtime(tmp_path, monkeypatch)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='write-selected',
        principal_id=GLOBAL_PERMISSION_PRINCIPAL, kind=BoundaryKind.FILESYSTEM_ACCESS,
        mode=PolicyMode.ALLOW, selector='*', created_by='fixture-user', created_at=datetime.now(UTC)))
    registry = ToolRegistry(filesystem_allowed_roots=(tmp_path,))
    path = tmp_path / 'selected.json'
    path.write_text('{"value": 6}')
    reads, writes = [], []
    def read(inv):
        reads.append(inv)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'value': 7})
    handler = _build_file_write_handler(tmp_path, allowed_roots=(tmp_path,))
    def write(inv):
        writes.append(inv)
        return handler(inv)
    registry.register(ToolSpec('sensor_read', 'Read selected value', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    registry.register(ToolSpec('file_write', 'Update selected local record', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2), write)
    class Provider:
        drafted = False
        def create(self, **kw):
            system = kw.get('system') or ''
            if 'material-requirements planner' in system:
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'requested_artifact_extensions': [], 'material_requirements': [{
                        'requirement': 'Update the selected record using the observed value.',
                        'requirement_kind': 'action', 'evidence_subject_id': 'selected',
                        'eligible_evidence_tool_names': ['file_write']}]}))
            if 'semantic completion reviewer' in system:
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                return text_response(json.dumps({'disposition': 'retry',
                    'all_material_requirements_enumerated': True, 'retry_tool_names': ['file_write'],
                    'unresolved_requirements': ['Update the selected record.'],
                    'material_requirements': [{'requirement_id': row['requirement_id'],
                        'satisfied': False, 'draft_covered': False, 'evidence_invocation_ids': []}
                        for row in packet['material_requirement_contract']]}))
            protocol = workflow_provider_reply(kw, completion=False)
            if protocol is not None: return protocol
            if not reads: return tool_response('sensor_read', {})
            if not self.drafted:
                self.drafted = True
                return text_response('Observed value: 7. No file has been changed.')
            if not writes:
                return tool_response('file_write', {'path': str(path), 'content': '{"value": 7}'})
            raise TimeoutError('External producer interrupted after the write')
    result = AgentOrchestrator(model_client=Provider()).run_turn(
        conversation_id='web:write', principal_id='web:write', user_message=prompt,
        conversation_history=[], tool_registry=registry, policy_store=runtime.store,
        approval_store=runtime.store, max_iterations=10,
        tool_flow_context={'planned_action_tool_names': ['file_write']})
    assert json.loads(path.read_text()) == {'value': 7}
    assert len(reads) == len(writes) == 1
    assert 'No file has been changed' not in (result.final_text or '')
    assert result.response_fulfilled is not True


def test_budget_exit_uses_the_same_terminal_owner_without_starting_another_round(tmp_path, monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    import nullion.agent_orchestrator as agent
    from nullion.completion_pipeline import published_completion
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry(); calls = []; reviews = []
    def read(invocation):
        calls.append(invocation)
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {'value': 7})
    registry.register(ToolSpec('sensor_read', 'Read selected value', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), read)
    actual_review = agent._review_risky_agent_completion
    def observe_review(*args, **kwargs):
        reviews.append(kwargs)
        return actual_review(*args, **kwargs)
    monkeypatch.setattr(agent, '_review_risky_agent_completion', observe_review)
    class Provider:
        producer_calls = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs)
            if protocol is not None:
                return protocol
            self.producer_calls += 1
            return tool_response('sensor_read', {})
    provider = Provider()
    result = AgentOrchestrator(model_client=provider).run_turn(conversation_id='web:limit',
        principal_id='web:limit', user_message='確認してください。', conversation_history=[],
        tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store, max_iterations=1)
    assert len(calls) == provider.producer_calls == len(reviews) == 1
    assert result.reached_iteration_limit and published_completion(result, '確認してください。')
    assert result.response_fulfilled is True

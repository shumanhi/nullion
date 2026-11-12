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
    assert review_boundary_calls == []
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
    assert review_boundary_calls == []
    assert not runtime.store.list_recent_conversation_events(conversation, event_type='conversation.completion_review')


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
    assert len(calls) == provider.producer_calls == 1 and not reviews
    assert result.reached_iteration_limit and published_completion(result, '確認してください。')
    assert result.response_fulfilled is False

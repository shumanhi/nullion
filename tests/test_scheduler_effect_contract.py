"""An inspection, an incomplete answer and an object's schema grant no write effects."""
from types import SimpleNamespace
from dataclasses import replace

import pytest

from nullion import chat_operator as chat
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


def registry():
    result = ToolRegistry()
    for name, effect in [('list_crons', ToolSideEffectClass.READ), ('toggle_cron', ToolSideEffectClass.WRITE),
                         ('run_cron', ToolSideEffectClass.WRITE), ('terminal_exec', ToolSideEffectClass.DANGEROUS_EXEC)]:
        result.register(ToolSpec(name, name, ToolRiskLevel.LOW, effect, False, 2),
            lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'completed', {}))
    return result


def scoped(action='inspect'):
    return ScopedTurnToolRegistry(registry(), evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(scheduler_action=action,
            requested_tool_names=('list_crons',), valid=True))


@pytest.mark.parametrize('capability,tool,action', [('scheduler_mutate','toggle_cron','mutate'),
                                                   ('scheduler_run','run_cron','run')])
@pytest.mark.parametrize('explicit_action', [True, False])
def test_inspection_cannot_escalate_through_scope_discovery(capability, tool, action, explicit_action):
    current = scoped()
    arguments = {'capabilities':[capability], 'tool_names':[tool], 'required_tool_names':[tool]}
    if explicit_action: arguments['scheduler_action'] = action
    result, widened = current.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:501',arguments))
    assert result.status == 'denied'
    assert result.output['reason'] == 'turn_effect_not_authorized'
    assert widened is current and not widened.can_invoke_tool(tool)
    assert widened.can_invoke_tool('terminal_exec')  # Existing execution approval policy still applies.


def test_declared_mutation_survives_inspection_then_exact_continuation():
    current = scoped('mutate')
    read, current = current.apply_scope_request(ToolInvocation('read','request_tool_scope','web:501',
        {'capabilities':['scheduler_read'],'tool_names':['list_crons']}))
    assert read.status == 'completed'
    write, current = current.apply_scope_request(ToolInvocation('write','request_tool_scope','web:501',
        {'capabilities':['scheduler_mutate'],'tool_names':['toggle_cron'],'scheduler_toggle_enabled':False}))
    assert write.status == 'completed' and current.can_invoke_tool('toggle_cron')


@pytest.mark.parametrize('prompt', ['Any news about the observer?', '監視の状況は？', '¿Cómo va el seguimiento?'])
@pytest.mark.parametrize('incomplete', [False, True])
def test_mutation_recovery_never_reclassifies_inspection_or_schema(prompt, incomplete, monkeypatch):
    current = scoped()
    initial = SimpleNamespace(final_text="I cannot access the live source; the current observation is unknown.",
        tool_results=[ToolResult('list','list_crons','completed',{'crons':[{'id':'a','enabled':True}]} )],
        artifacts=[], suspended_for_approval=False, response_fulfilled=not incomplete)
    def forbidden(**kwargs):
        raise AssertionError('An inspection must not invoke a mutation classifier or retry')
    monkeypatch.setattr(chat,'build_turn_tool_scope_decision', forbidden)
    result, same, retried = chat._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial, base_history=[], active_tool_registry=current,
        base_tool_registry=current._delegate, model_client=object(), user_message=prompt,
        evidence=TurnToolEvidence(), run_turn=forbidden)
    assert result is initial and same is current and not retried


def test_unscoped_object_metadata_is_not_a_mutation_plan(monkeypatch):
    current = registry()
    initial = SimpleNamespace(final_text='The scheduler mutation tool is not available in this turn.',
        tool_results=[ToolResult('list','list_crons','completed',{'crons':[{'id':'a','enabled':True}]})],
        artifacts=[], suspended_for_approval=False)
    def forbidden(**kwargs): raise AssertionError('No current-turn mutation contract')
    monkeypatch.setattr(chat,'build_turn_tool_scope_decision',forbidden)
    result, _, retried = chat._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial,base_history=[],active_tool_registry=current,model_client=object(),
        user_message='操作 41',evidence=TurnToolEvidence(),run_turn=forbidden)
    assert result is initial and not retried


def test_run_contract_cannot_escalate_to_mutation():
    current = scoped('run')
    result, same = current.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:501',
        {'tool_names':['toggle_cron'], 'required_tool_names':['toggle_cron']}))
    assert result.status == 'denied' and same is current


def test_reconstructed_registry_cannot_replace_contract_with_a_larger_discovery_scope():
    current = scoped()
    decision = replace(current.turn_tool_scope_decision, scheduler_action='mutate',
        requested_tool_names=('list_crons','toggle_cron'))
    recovered = ScopedTurnToolRegistry(current._delegate, evidence=TurnToolEvidence(), tool_scope_decision=decision)
    assert not recovered.can_invoke_tool('toggle_cron')
    denied = recovered.invoke(ToolInvocation('bad','toggle_cron','web:501',{'id':'a','enabled':False}))
    assert denied.status == 'denied'


def test_old_registry_mutation_plan_does_not_override_current_inspection(monkeypatch):
    old = scoped('mutate')
    current = scoped()
    initial = SimpleNamespace(final_text='現在の結果は不明です。', tool_results=[
        ToolResult('list','list_crons','failed',{},error='Source unavailable')],
        artifacts=[], suspended_for_approval=False)
    result, _, retried = chat._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial,base_history=[],active_tool_registry=current,base_tool_registry=old,
        model_client=object(),user_message='1',evidence=TurnToolEvidence(numbered_option_selected=True),
        run_turn=lambda *a: pytest.fail('Prior plan cannot grant a write'))
    assert result is initial and not retried


def test_new_turn_with_explicit_structured_mutation_has_its_own_contract():
    inspected = scoped()
    # A numeric selection is authorized by its current typed continuation plan,
    # never by recognizing words in the numeric reply or a past assistant draft.
    current = ScopedTurnToolRegistry(inspected._delegate,
        evidence=TurnToolEvidence(numbered_option_selected=True, current_user_message='2'),
        tool_scope_decision=TurnToolScopeDecision(scheduler_action='mutate', valid=True))
    result, widened = current.apply_scope_request(ToolInvocation('scope','request_tool_scope','web:501',
        {'capabilities':['scheduler_mutate'],'tool_names':['toggle_cron'],'scheduler_toggle_enabled':False}))
    assert result.status == 'completed' and widened.can_invoke_tool('toggle_cron')
    assert inspected.turn_tool_scope_decision.allowed_scheduler_actions == ('inspect',)

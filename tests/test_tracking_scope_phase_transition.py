import pytest

from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolInvocation
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


def registry():
    base = ToolRegistry()
    for name, tags in [('create_cron', ('scheduler', 'cron')), ('create_live_tracker', ('live_tracking',))]:
        base.register(ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 2,
            capability_tags=tags), lambda inv: pytest.fail('Selecting visibility must not execute a mutation'))
    return ScopedTurnToolRegistry(base, evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(tracking_mode='clarify', valid=True))


@pytest.mark.parametrize('name,mode', [('create_cron', 'recurring'), ('create_live_tracker', 'live')])
def test_exact_registered_creation_plan_resolves_previous_tracking_clarification(name, mode):
    result, scoped = registry().apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'web:選択',
        {'capabilities': ['scheduler_mutate' if mode == 'recurring' else 'live_tracking'],
         'tool_names': [name], 'required_tool_names': [name]}))
    assert result.status == 'completed'
    assert scoped.turn_tool_scope_decision.tracking_mode == mode
    assert name in {row['name'] for row in scoped.list_tool_definitions()}
    assert scoped.can_invoke_tool(name)


@pytest.mark.parametrize('arguments', [
    {'tool_names': ['create_cron'], 'tracking_mode': 'clarify'},
    {'tool_names': ['create_cron', 'create_live_tracker']},
    {'capabilities': ['scheduler_read']},
])
def test_an_unresolved_or_explicitly_requested_choice_does_not_select_creation(arguments):
    result, scoped = registry().apply_scope_request(ToolInvocation('scope', 'request_tool_scope', 'web:選択', arguments))
    assert result.output['reason'] == 'live_tracking_needs_details'
    assert scoped.turn_tool_scope_decision.tracking_mode == 'clarify'
    assert not scoped.can_invoke_tool('create_cron') and not scoped.can_invoke_tool('create_live_tracker')

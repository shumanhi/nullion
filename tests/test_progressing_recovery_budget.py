from copy import deepcopy

import pytest

from nullion.agent_orchestrator import (
    _completion_progress_recovery_update,
    _completion_recovery_progress_signatures,
)
from nullion.tools import ToolResult


def source_state():
    return {'iterations': 8, 'max_iterations': 20, 'completion_recovery_iteration_limit': 8,
        'completion_recovery_tool_names': ['opaque_adapter'],
        'completion_recovery_progress_signatures': [],
        'tool_results': [ToolResult('a', 'opaque_adapter', 'completed', {'value': '記録'})],
        'invocation_metadata': {'a': {'arguments': {'record': '記録'}}}}


def test_productive_window_can_continue_but_cannot_increase_hard_budget():
    state = source_state()
    state['max_iterations'] = 10
    update = _completion_progress_recovery_update(state)
    assert update['completion_recovery_iteration_limit'] == 10
    assert 'max_iterations' not in update
    assert len(update['completion_recovery_progress_signatures']) == 1
    assert '記録' not in repr(update), 'Budget bookkeeping must not retain source arguments'


@pytest.mark.parametrize('change', ['failed', 'denied', 'error', 'negative_output', 'unrelated',
    'duplicate_arguments', 'missing_arguments', 'no_progress', 'below_window', 'hard_limit'])
def test_recovery_cannot_renew_from_stalls_or_unverified_work(change):
    state = deepcopy(source_state())
    result = state['tool_results'][0]
    if change in {'failed', 'denied'}:
        state['tool_results'] = [ToolResult('a', 'opaque_adapter', change, {'value': '記録'})]
    if change == 'error':
        state['tool_results'] = [ToolResult('a', 'opaque_adapter', 'completed', {'value': '記録'}, 'source unavailable')]
    if change == 'negative_output':
        result.output['verified'] = False
    if change == 'unrelated':
        state['completion_recovery_tool_names'] = ['different_adapter']
    if change in {'no_progress', 'duplicate_arguments'}:
        state['completion_recovery_progress_signatures'] = _completion_recovery_progress_signatures(state, ['opaque_adapter'])
        if change == 'duplicate_arguments':
            state['tool_results'].append(ToolResult('new-id', 'opaque_adapter', 'completed', {'value': 'changed prose'}))
            state['invocation_metadata']['new-id'] = {'arguments': {'record': '記録'}}
    if change == 'missing_arguments':
        state['invocation_metadata'].clear()
    if change == 'below_window':
        state['iterations'] = 7
    if change == 'hard_limit':
        state['max_iterations'] = 8
    assert _completion_progress_recovery_update(state) is None


def test_plain_turn_fast_path_does_not_inspect_tool_results():
    class NoScan:
        def __iter__(self):
            raise AssertionError('Plain turn scanned recovery history')
    assert _completion_progress_recovery_update({'tool_results': NoScan()}) is None


def test_initial_recovery_allowance_keeps_existing_sixty_call_ceiling(monkeypatch):
    import nullion.agent_orchestrator as agent
    from nullion.tools import ToolRegistry
    monkeypatch.setattr(agent, '_review_risky_agent_completion', lambda *a, **k:
        agent._CompletionReviewDecision('retry', ('残りの記録',), ('opaque_adapter',)))
    state = {'iterations': 59, 'max_iterations': 60, 'user_message': '残りの記録',
        'conversation_id': 'web:budget', 'principal_id': 'web:budget', 'runtime_store': None,
        'tool_registry': ToolRegistry(), 'messages': [], 'tool_results': [], 'artifacts': [],
        'content': [{'type': 'text', 'text': 'Préparé.'}]}
    update = agent._agent_turn_finalize_node(state)
    assert update['max_iterations'] == 60
    assert _completion_progress_recovery_update({**state, **update, 'iterations': 60}) is None


def test_fixed_scheduled_recovery_budget_is_not_enlarged_by_a_rejected_draft(monkeypatch):
    import nullion.agent_orchestrator as agent
    from nullion.tools import ToolRegistry
    monkeypatch.setattr(agent, '_review_risky_agent_completion', lambda *a, **k:
        agent._CompletionReviewDecision('retry', ('残りの記録',), ('opaque_adapter',)))
    state = {'iterations': 2, 'max_iterations': 3, 'user_message': '残りの記録',
        'conversation_id': 'web:budget', 'principal_id': 'web:budget', 'runtime_store': None,
        'tool_registry': ToolRegistry(), 'messages': [], 'tool_results': [], 'artifacts': [],
        'tool_flow_context': {'producer_iteration_budget_fixed': True},
        'content': [{'type': 'text', 'text': 'Préparé.'}]}
    update = agent._agent_turn_finalize_node(state)
    assert update['max_iterations'] == 3

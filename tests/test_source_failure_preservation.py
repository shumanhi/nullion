"""A failed optional step cannot erase relevance-selected current-turn evidence."""
from types import SimpleNamespace
import asyncio

import pytest

from nullion import agent_orchestrator as agent
from nullion.tools import ToolRegistry, ToolResult


def source_state():
    source = ToolResult('quoted-source', 'source_reader', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'partial',
        'observations': [{'value': 'Option A · 71 USD', 'quote': 'Option A · 71 USD',
                          'source_url': 'https://example.test/offer'}], 'unknowns': ['取消条件']})
    return {'user_message': '価格を比較', 'conversation_id': 'web:another-user',
        'principal_id': 'web:another-user', 'tool_registry': ToolRegistry(),
        'tool_results': [source], 'artifacts': [], 'source_answer_checkpoint': {
            'tool_result_count': 1, 'presentation': {'intro': 'Everything is complete.',
            'points': [{'evidence_ids': ['e0.0'], 'detail_ids': [], 'relevance': 'Requested price'}],
            'limitation_ids': ['u0']}}}


@pytest.mark.parametrize('failure', ['browser', 'model', 'limit', 'missing_action'])
def test_terminal_failure_keeps_verified_answer_and_unfulfilled_state(failure):
    state = source_state()
    if failure == 'browser':
        state['tool_results'].append(ToolResult('later', 'browser_wait_for', 'failed',
            {'reason': 'browser_condition_unmet'}, 'unshown condition'))
    kwargs = {'response_fulfilled': False, 'completion_failure_reply': 'The remaining condition could not be verified.'}
    if failure == 'model': kwargs['model_timed_out'] = True
    if failure == 'limit': kwargs['reached_iteration_limit'] = True
    result = agent._complete_agent_turn(state, final_text='Generic failure.', **kwargs)['result']
    assert 'Option A · 71 USD' in result.final_text
    assert '取消条件' in result.final_text
    assert result.final_text.count('Not verified from this check:') == 1
    assert 'remaining condition' not in result.final_text
    assert result.response_presentation['source_answer_terminal_failure'] == kwargs['completion_failure_reply']
    assert 'Everything is complete' not in result.final_text
    assert result.response_fulfilled is False
    assert result.completion_failure_reply == result.final_text
    assert result.response_presentation['source_report_status'] == 'partial'
    assert not result.artifacts


@pytest.mark.parametrize('protected', ['scheduled', 'artifact', 'approval', 'cancelled', 'invalid_checkpoint', 'unquoted'])
def test_preservation_does_not_bypass_completion_or_consent_contracts(protected):
    state = source_state(); kwargs = {'response_fulfilled': False}
    if protected == 'scheduled': state['tool_flow_context'] = {'scheduled_task_run': True}
    elif protected == 'artifact': state['tool_flow_context'] = {'requires_artifact_delivery': True, 'required_artifact_extensions': ['.pdf']}
    elif protected == 'approval': kwargs['suspended_for_approval'] = True
    elif protected == 'cancelled': state['cancellation_checker'] = lambda: True
    elif protected == 'invalid_checkpoint': state['source_answer_checkpoint']['tool_result_count'] = 20
    else: state['tool_results'][0].output['observations'][0]['quote'] = 'Unrelated text'
    result = agent._complete_agent_turn(state, final_text='Still pending.', **kwargs)['result']
    assert 'Option A' not in result.final_text
    assert result.response_fulfilled is False


def test_schema_followup_cannot_restart_a_terminal_partial_source_answer():
    from nullion.chat_operator import _run_chat_turn_schema_followup_tool_retry
    initial = SimpleNamespace(response_presentation={'source_report_status': 'partial'})
    registry = ToolRegistry()
    def forbidden(*args): pytest.fail('Terminal source answer restarted browsing')
    result, actual, retried = _run_chat_turn_schema_followup_tool_retry(initial_result=initial,
        base_history=[], active_tool_registry=registry, evidence=SimpleNamespace(), run_turn=forbidden)
    assert result is initial and actual is registry and not retried


def test_playwright_url_wait_matches_cdp_globs_without_resource_load_wait():
    from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
    captured = []
    class Page:
        async def wait_for_url(self, matcher, **kw):
            captured.append(kw)
            assert matcher('https://example.test/a/b/c?locale=ja')
            assert not matcher('https://example.test/d/e')
    backend = PlaywrightBackend()
    async def get_page(session): return Page()
    backend._get_page = get_page
    asyncio.run(backend.wait_for('one', None, '*a/b/c*', None, 1))
    assert captured == [{'timeout': 1000, 'wait_until': 'commit'}]


@pytest.mark.parametrize('failure', [
    'Verify each route stop.; Provide visiting details.; Verify food and shopping.',
    '各地点の迂回時間を検証する。買い物の候補を確認する。',
    'Verificar los desvíos y confirmar las tiendas.',
])
@pytest.mark.parametrize('reason_kind', ['review', 'rendered_answer'])
def test_source_disclosure_has_one_owner_even_when_failure_contains_an_entire_answer(failure, reason_kind):
    from nullion.source_observation_contract import render_source_answer
    state = source_state()
    if reason_kind == 'rendered_answer':
        failure = render_source_answer(state['tool_results'], state['source_answer_checkpoint']['presentation'])
    result = agent._complete_agent_turn(state, final_text=failure,
        response_fulfilled=False, completion_failure_reply=failure)['result']
    assert result.final_text.count('Option A · 71 USD') == 1
    assert result.final_text.count('取消条件') == 1
    assert result.final_text.count('Not verified from this check:') == 1
    assert 'Verify each' not in result.final_text and '各地点' not in result.final_text and 'Verificar' not in result.final_text
    assert result.response_fulfilled is False
    assert result.response_presentation['source_answer_terminal_failure'] == failure


def test_preserved_answer_without_selected_unknowns_does_not_publish_review_instructions():
    state = source_state()
    state['tool_results'][0].output['report_status'] = 'completed'
    state['source_answer_checkpoint']['presentation']['limitation_ids'] = []
    result = agent._complete_agent_turn(state, final_text='Verify all outstanding requirements.',
        response_fulfilled=False, completion_failure_reply='Verify all outstanding requirements.')['result']
    assert 'Option A' in result.final_text
    assert result.final_text.endswith("I couldn't verify all requested details from this check.")
    assert 'Verify all' not in result.final_text and not result.response_fulfilled


@pytest.mark.parametrize('disposition', ['needs_user_input', 'needs_approval'])
def test_partial_report_retains_numbered_action_prompt(monkeypatch, disposition):
    from nullion.source_observation_contract import SOURCE_ANSWER_TOOL
    state = source_state()
    state['content'] = [{'type': 'tool_use', 'name': SOURCE_ANSWER_TOOL,
        'input': state['source_answer_checkpoint']['presentation']}]
    decision = agent._CompletionReviewDecision(disposition, ('A necessary detail or pending action is required',))
    monkeypatch.setattr(agent, '_review_risky_agent_completion', lambda *a, **kw: decision)
    result = agent._complete_partial_source_report(state)['result']
    assert result.final_text.count('Option A · 71 USD') == 1
    assert result.final_text.count('Not verified from this check:') == 1
    assert '1.' in result.final_text and '2.' in result.final_text
    assert result.response_fulfilled is False


def test_recorded_route_report_does_not_repeat_its_disclosure_as_a_reviewer_checklist(monkeypatch):
    import json
    from pathlib import Path
    from nullion.source_observation_contract import SOURCE_ANSWER_TOOL
    report = json.loads((Path(__file__).parent / 'fixtures/route_partial_source_report.json').read_text())
    state = source_state()
    state['tool_results'] = [ToolResult('route', 'browser_run_task', 'completed', report)]
    state['completion_review_count'] = agent._COMPLETION_REVIEW_MAX_ATTEMPTS
    presentation = {'intro': '', 'points': [{'evidence_ids': [f'e0.{i}'],
        'detail_ids': [], 'relevance': 'A requested stop on the route'} for i in range(3)],
        'limitation_ids': ['u0', 'u1']}
    state['content'] = [{'type': 'tool_use', 'name': SOURCE_ANSWER_TOOL, 'input': presentation}]
    review = agent._CompletionReviewDecision('retry', (
        'Verify that each recommended stop is along or reasonably close to the specified route.',
        'Provide visiting details and verify food, shopping, and sightseeing experiences.'), ('browser_run_task',))
    monkeypatch.setattr(agent, '_review_risky_agent_completion', lambda *a, **kw: review)
    result = agent._complete_partial_source_report(state)['result']
    assert all(place in result.final_text for place in ['State Line Lookout', 'Saunderskill', 'Minnewaska'])
    assert result.final_text.count('Precise added driving times') == 1
    assert result.final_text.count('Current opening hours') == 1
    assert result.final_text.count('Not verified from this check:') == 1
    assert 'Verify that each' not in result.final_text and 'Provide visiting' not in result.final_text
    assert result.response_fulfilled is False and not result.artifacts

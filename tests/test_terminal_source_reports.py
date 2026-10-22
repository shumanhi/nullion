"""Terminal partial research remains evidence-backed and cannot restart itself."""
from types import SimpleNamespace

import pytest

from nullion import agent_orchestrator as agent
from nullion.source_observation_contract import (
    compact_source_observations, render_source_answer, source_answer_context,
    source_report_status, terminal_source_report_tools,
)
from nullion.tools import ToolResult


def report(*, empty=False, continuation=False):
    return ToolResult('read-1', 'reader', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'blocked' if empty else 'partial',
        'next_action': 'alternative_source' if continuation else 'report',
        'continuation': {'kind': 'alternative_source', 'tool_names': ['reader']} if continuation else None,
        'observations': [] if empty else [{'value': 'Reported figure: 42', 'quote': 'Reported figure: 42',
                                           'source_url': 'https://example.org/source'}],
        'blocker': 'The source requires login; the full text could not be accessed.',
        'unknowns': ['Independent evidence for the reported figure was not obtained.']})


@pytest.mark.parametrize('empty', [False, True])
def test_terminal_report_keeps_cause_and_uncertainty_without_certifying_claims(empty):
    result = report(empty=empty)
    result.output = compact_source_observations(result.output)
    context = source_answer_context([result])
    assert source_report_status([result]) == ('blocked' if empty else 'partial')
    draft = {'intro': '', 'points': [], 'limitation_ids': [x['id'] for x in context['limitations']]}
    text = render_source_answer([result], draft)
    assert 'requires login' in text and 'Independent evidence' in text
    assert '42' not in text  # Irrelevant or unverified source claims need not be presented.
    preserved = agent._complete_agent_turn({'user_message': '検証して', 'tool_results': [result],
        'source_answer_checkpoint': {'presentation': draft, 'tool_result_count': 1}},
        final_text='Generic failure', response_fulfilled=False)['result']
    assert 'requires login' in preserved.final_text and not preserved.response_fulfilled


def test_empty_answer_requires_specific_limitation():
    with pytest.raises(ValueError):
        render_source_answer([report()], {'intro': '', 'points': [], 'limitation_ids': []})


@pytest.mark.parametrize('tools,expected', [(('reader',), 'blocked'), ((), 'blocked'),
                                         (('reader', 'other_reader'), 'retry')])
def test_review_cannot_repeat_terminal_producer_but_can_choose_other_methods(tools, expected):
    state = {'tool_results': [report()]}
    decision = agent._CompletionReviewDecision('retry', ('Unverified outcome',), tools)
    actual = agent._source_report_recovery_decision(state, decision)
    assert actual.disposition == expected
    assert 'reader' not in actual.retry_tool_names
    if expected == 'retry': assert actual.retry_tool_names == ('other_reader',)


def test_explicit_continuation_and_scheduled_contract_are_preserved():
    decision = agent._CompletionReviewDecision('retry', ('Unverified outcome',), ('reader',))
    assert not terminal_source_report_tools([report(continuation=True)])
    assert agent._source_report_recovery_decision({'tool_results': [report(continuation=True)]}, decision) is decision
    state = {'tool_results': [report()], 'tool_flow_context': {'scheduled_task_run': True}}
    assert agent._source_report_recovery_decision(state, decision) is decision


@pytest.mark.parametrize('done,alternative,expected', [(True, False, False), (False, False, True), (True, True, True)])
def test_browser_terminal_blocker_is_not_rewritten_into_automatic_continuation(done, alternative, expected):
    from nullion.plugins.browser_plugin.browser_use_adapter import PageEvidence
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    evidence = PageEvidence(principal_id='other-user', policy=BrowserPolicy())
    evidence.goal_status = 'blocked'; evidence.blocker = 'Source could not be accessed.'
    evidence.next_action = 'alternative_source' if alternative else 'report'
    output = evidence.result(error=None, elapsed=1, steps=2, done=done)
    assert bool(output['continuation']) is expected
    assert output['report_status'] == 'blocked'


def test_terminal_report_cannot_silently_omit_its_blocker_and_fallback_is_specific():
    from nullion.source_observation_contract import partial_source_report_reply
    result = report()
    with pytest.raises(ValueError):
        render_source_answer([result], {'intro': '', 'points': [{'evidence_ids': ['e0.0'],
            'detail_ids': [], 'relevance': 'Requested source statement'}], 'limitation_ids': []})
    text = partial_source_report_reply([result], draft={'invalid': True})
    assert 'requires login' in text and 'Independent evidence' in text
    assert '42' not in text


def test_blocked_answer_tool_has_valid_schema_without_empty_enums():
    from nullion.source_observation_contract import source_answer_tool_definition
    from jsonschema import Draft202012Validator
    spec = source_answer_tool_definition(source_answer_context([report(empty=True)]))
    Draft202012Validator.check_schema(spec['input_schema'])
    Draft202012Validator(spec['input_schema']).validate({'intro': '', 'points': [], 'limitation_ids': ['u0']})


def test_completed_alternative_can_omit_an_earlier_resolved_access_problem():
    complete = ToolResult('read-2', 'other_reader', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'completed',
        'observations': [{'value': 'Verified record: 39', 'quote': 'Verified record: 39',
                          'source_url': 'https://example.net/record'}], 'unknowns': []})
    text = render_source_answer([report(), complete], {'intro': '', 'points': [
        {'evidence_ids': ['e1.0'], 'detail_ids': [], 'relevance': 'Verified independently'}], 'limitation_ids': []})
    assert 'Verified record: 39' in text and 'requires login' not in text


def test_failed_synthesis_without_access_blocker_does_not_select_unrelated_unknowns():
    from nullion.source_observation_contract import partial_source_report_reply
    result = report(); result.output['blocker'] = None
    text = partial_source_report_reply([result], draft={'invalid': True})
    assert len(text) < 150 and '42' not in text and 'Independent evidence' not in text

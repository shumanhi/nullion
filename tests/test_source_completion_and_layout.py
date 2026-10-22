"""Partial source work must progress, and record fields must remain readable."""
import copy
import json
from pathlib import Path

import pytest

from nullion import agent_orchestrator as agent
from nullion.source_observation_contract import source_answer_context, render_source_answer, source_answer_tool_definition
from nullion.tools import ToolRegistry, ToolResult, ToolSpec, ToolSideEffectClass, ToolRiskLevel

REPORT = json.loads((Path(__file__).parent / 'fixtures/train_partial_source_report.json').read_text())


def state():
    registry = ToolRegistry()
    registry.register(ToolSpec('reader', 'Read a public page', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2), lambda call: None)
    return {'tool_results': [ToolResult('outbound', 'reader', 'completed', copy.deepcopy(REPORT))],
        'tool_registry': registry}


def train_presentation(results):
    context = source_answer_context(results)
    row = context['evidence'][1]
    ids = {x['text']: x['id'] for x in row['excerpts']}
    groups = [[ids[x] for x in fields] for fields in (
        ['DEPARTS', '12:50', 'p'], ['ARRIVES', '4:12', 'p'], ['3h 22m'], ['Coach', 'from', '$', '155'])]
    return {'intro': 'NYC → Washington, Thursday, October 22:', 'points': [{
        'evidence_ids': [row['id']], 'title_ids': [ids['Northeast Regional'], ids['1147']],
        'detail_ids': list(dict.fromkeys(i for g in groups for i in g)), 'detail_groups': groups,
        'relevance': 'An outbound option close to the requested departure time'}], 'limitation_ids': ['u0']}


def test_recorded_train_fields_are_labeled_and_include_currency_duration_and_times():
    s = state(); plan = train_presentation(s['tool_results'])
    from jsonschema import Draft202012Validator
    Draft202012Validator(source_answer_tool_definition(source_answer_context(s['tool_results']))['input_schema']).validate(plan)
    text = render_source_answer(s['tool_results'], plan)
    assert '**Northeast Regional 1147**' in text
    assert 'DEPARTS 12:50 p' in text and 'ARRIVES 4:12 p' in text
    assert '3h 22m' in text and 'Coach from $ 155' in text
    assert 'Northeast Regional 12:50 155' not in text


@pytest.mark.parametrize('mutation', [
    lambda p: p['points'][0]['title_ids'].append('e0.0.q0.l0'),
    lambda p: p['points'][0]['detail_groups'].append(['invented']),
    lambda p: p['points'][0]['detail_groups'].pop(),
    lambda p: p['points'][0]['detail_groups'].append(p['points'][0]['detail_groups'][0]),
    lambda p: p['points'][0]['detail_ids'].append([]),
    lambda p: p['points'][0]['detail_groups'][0].reverse(),
])
def test_layout_cannot_mix_records_invent_or_silently_drop_selected_fields(mutation):
    s = state(); plan = train_presentation(s['tool_results']); mutation(plan)
    with pytest.raises(ValueError): render_source_answer(s['tool_results'], plan)


@pytest.mark.parametrize('missing', ['Return options around noon', '正午ごろの復路', 'Opciones de regreso al mediodía'])
def test_partial_without_blocker_can_continue_the_same_read_tool_for_missing_work(missing):
    s = state(); decision = agent._CompletionReviewDecision('retry', (missing,), ('reader',))
    assert agent._source_report_recovery_decision(s, decision) == decision


@pytest.mark.parametrize('reason', ['blocker', 'no_progress', 'regression', 'no_facts', 'write'])
def test_recovery_stops_exhausted_or_non_progressing_work_without_repeating_actions(reason):
    s = state(); result = s['tool_results'][0]
    if reason == 'blocker': result.output['blocker'] = 'The source requires sign-in.'
    elif reason == 'no_facts': result.output['observations'] = []
    elif reason in {'no_progress', 'regression'}:
        newer = copy.deepcopy(result); newer.invocation_id = 'again'
        if reason == 'regression': newer.output['observations'] = newer.output['observations'][:1]
        s['tool_results'].append(newer)
    else:
        registry = ToolRegistry()
        registry.register(ToolSpec('reader', 'Write something', ToolRiskLevel.LOW,
            ToolSideEffectClass.WRITE, False, 2), lambda call: None)
        s['tool_registry'] = registry
    decision = agent._source_report_recovery_decision(s, agent._CompletionReviewDecision('retry', ('Unmet outcome',), ('reader',)))
    assert decision.disposition == 'blocked' and not decision.retry_tool_names


def test_source_file_producer_requires_explicit_self_continuation_metadata():
    s = state(); registry = ToolRegistry()
    registry.register(ToolSpec('reader', 'Capture source evidence', ToolRiskLevel.LOW,
        ToolSideEffectClass.WRITE, False, 2, continuation_tools=('reader',)), lambda call: None)
    s['tool_registry'] = registry
    decision = agent._CompletionReviewDecision('retry', ('未確認の復路',), ('reader',))
    assert agent._source_report_recovery_decision(s, decision) == decision


@pytest.mark.parametrize('outcome', ['completed', 'partial', 'empty', 'blocker'])
def test_completed_same_url_lookup_resolves_continuation_only_with_verified_facts(outcome):
    from nullion.source_observation_contract import pending_source_continuation
    first = state()['tool_results'][0]
    first.output['continuation'] = {'kind': 'alternative_source', 'source_url': 'https://example.com/search', 'tool_names': ['reader']}
    second = ToolResult('return', 'reader', 'completed', {'result_kind': 'source_observations',
        'report_status': 'partial' if outcome == 'partial' else 'completed',
        'page_url': 'https://example.com/search', 'observations': [] if outcome == 'empty' else [{'value': '復路 12:00', 'quote': '復路 12:00'}],
        'blocker': 'The source cannot be accessed' if outcome == 'blocker' else None})
    assert bool(pending_source_continuation([first, second])) is (outcome != 'completed')


def test_browser_recovery_scope_is_runtime_owned_and_includes_original_constraints(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin import browser_use_adapter as adapter, browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolInvocation
    monkeypatch.setattr(adapter, 'browser_use_python', lambda: '/worker/python')
    captured = []
    def worker(task, **kwargs):
        captured.append(task)
        return {'result_kind': 'source_observations', 'report_status': 'completed', 'observations': [], 'artifact_paths': []}
    monkeypatch.setattr(browser_use_worker, 'run_isolated_browser_task', worker)
    handler = adapter.browser_task_handler(lambda: object(), BrowserPolicy())
    for context in [{}, {'user_request': 'One request: outbound and return, Coach only',
                        'completion_review_unresolved_requirements': ['復路の時刻と運賃']}]:
        result = handler(ToolInvocation('call', 'browser_run_task', 'user',
            {'task': 'Look up the remaining direction', 'url': 'https://example.com',
             'remaining_requirements': ['untrusted tool argument']}, flow_context=context))
        assert result.status == 'completed'
    assert captured[0].remaining_requirements == []
    assert captured[1].remaining_requirements == ['復路の時刻と運賃']
    objective = adapter._browser_task_objective(captured[1])
    assert 'Coach only' in objective and '復路の時刻と運賃' in objective
    assert 'Do not restart completed lookups' in objective


@pytest.mark.parametrize('change,can_continue', [('label', False), ('capture', False), ('quote', True), ('source', True), ('control', True)])
def test_progress_uses_verified_record_context_rather_than_bare_values_or_capture_noise(change, can_continue):
    s = state(); older = s['tool_results'][0]; newer = copy.deepcopy(older); newer.invocation_id = 'new'
    record = newer.output['observations'][0]
    if change == 'label': record['label'] = 'A different collection label'
    elif change == 'capture': record['captured_at'] = 'later'; record['evidence_path'] = '/internal/new-capture.png'
    elif change == 'quote': record['quote'] += '\nAdditional verified constraint'
    elif change == 'source': record['source_url'] = 'https://independent.example/record'
    else: record['control_state'] = {'value': '2026-10-25', 'selected': True}
    s['tool_results'].append(newer)
    actual = agent._source_report_recovery_decision(s, agent._CompletionReviewDecision('retry', ('Additional requested constraint',), ('reader',)))
    assert (actual.disposition == 'retry') is can_continue

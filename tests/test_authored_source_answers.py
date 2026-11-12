import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion.source_observation_contract import source_answer_context, render_source_answer, source_answer_review_records
from nullion.tools import ToolResult, ToolRegistry


def context_result():
    output = json.loads((Path(__file__).parent/'fixtures/ticket_context_only_observations.json').read_text())
    return ToolResult('capture', 'browser_run_task', 'completed', output)


def test_background_inventory_is_never_rendered_as_requested_results():
    results = [context_result()]
    context = source_answer_context(results)
    record = next(row for row in context['evidence'] if row['value'].startswith('AMC 84th'))
    limitation = context['limitations'][0]['id']
    answer = {'intro': 'No requested-date screening was verified.', 'answer_status': 'no_verified_results',
        'focus_roles': ['entity'], 'points': [{'evidence_ids': [record['id']], 'detail_ids': [],
            'semantic_role': 'entity', 'request_relation': 'supporting_context', 'relevance': 'Venue location only.'}],
        'limitation_ids': [limitation], 'limitation_summaries': {limitation: 'The requested date was not reached.'}}
    rendered = render_source_answer(results, answer)
    assert 'No requested-date' in rendered and 'requested date was not reached' in rendered
    assert 'AMC 84th' not in rendered and 'Select' not in rendered
    records, _, _ = source_answer_review_records(results, answer)
    assert not records, 'Background must not count as an answered candidate in review'
    answer['points'][0]['request_relation'] = 'result'
    with pytest.raises(ValueError, match='no-result'): render_source_answer(results, answer)


@pytest.mark.parametrize('prompt', ['残りの予定を確認。', 'Comprueba el horario solicitado.'])
def test_one_prepared_answer_survives_review_failure_with_exact_layout(prompt):
    from nullion.execution_outcome import prepare_answer
    from nullion.agent_orchestrator import _preserved_source_answer_on_failure
    results = [context_result()]
    text = 'I could not verify the requested-date screening.\n\nThe calendar did not expose that date.'
    prepared = prepare_answer(prompt, text, results)
    state = {'user_message': prompt, 'tool_registry': ToolRegistry(), 'tool_results': results,
        'source_answer_checkpoint': {'tool_result_count': 1, 'presentation': {}, 'composed_answer': prepared}}
    reply, _ = _preserved_source_answer_on_failure(state, 'Reviewer unavailable')
    assert reply == text and 'Here are the findings' not in reply
    state['user_message'] = 'Different prompt'
    assert _preserved_source_answer_on_failure(state, 'Reviewer unavailable') is None


def test_new_evidence_invalidates_the_prepared_answer_instead_of_reconstructing_it():
    from nullion.execution_outcome import prepare_answer
    from nullion.agent_orchestrator import _preserved_source_answer_on_failure
    results = [context_result()]
    prepared = prepare_answer('確認。', 'Earlier finding.', results)
    state = {'user_message': '確認。', 'tool_registry': ToolRegistry(),
        'tool_results': [*results, ToolResult('new-capture', 'browser_run_task', 'completed', {'changed': True})],
        'source_answer_checkpoint': {'tool_result_count': 1, 'presentation_reviewed': True,
            'presentation': {}, 'composed_answer': prepared}}
    assert _preserved_source_answer_on_failure(state, 'Reviewer unavailable') is None


@pytest.mark.parametrize('disabled,stale,expected', [(False, False, True), (True, False, False), (False, True, False)])
def test_pending_enabled_control_retains_existing_browser_work(disabled, stale, expected):
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_use_adapter import PageEvidence, BrowserReport
    async def read():
        return {'url': 'https://example.test/results', 'text': 'Pick a location',
                'controls': [{'label': 'Choose location', 'disabled': disabled}]}
    evidence = PageEvidence('fixture', BrowserPolicy(), text_capture=read)
    page = SimpleNamespace(url='https://example.test/results', title='Results', dom_state=None, screenshot=None)
    asyncio.run(evidence.capture(page))
    if stale: asyncio.run(evidence.capture(page))
    evidence.retain(BrowserReport(observations=[], unknowns=['Location has not been selected.'],
        goal_status='partial', next_action='report', pending_controls=[{'source_id': 0, 'control_index': 0}]))
    result = evidence.result(error=None, elapsed=1, steps=1, done=True)
    assert bool(result['continuation']) is expected
    if expected:
        assert result['continuation']['pending_controls'] == [{'source_id': 0, 'control_index': 0}]

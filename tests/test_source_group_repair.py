from copy import deepcopy
import jsonschema
import pytest

from tests.source_group_fixture import grouped_source_fixture
from nullion.source_observation_contract import (
    source_answer_context, source_answer_tool_definition, normalize_source_answer_selection,
    render_source_answer, source_answer_repair_context, recover_selected_source_excerpts,
)


@pytest.mark.parametrize('name', ['Trail SUV', '自動車', 'Oferta familiar'])
def test_feedback_preserves_title_and_explains_compatible_roles_without_joining_old_capture(name):
    results, plan = grouped_source_fixture(name)
    with pytest.raises(ValueError) as failure:
        render_source_answer(results, plan)
    feedback = source_answer_repair_context(results, plan, failure.value)
    row = next(r for r in feedback['validation_issues'] if r.get('code') == 'selection_contract' and r['point_index'] == 1)
    assert row['required_fields'] == ['title_ids', 'detail_ids']
    assert row['compatible_title_roles'] == ['attribute', 'context']
    assert row['same_capture_entity_evidence_ids'] == []
    fixed = deepcopy(plan); fixed['points'][1]['semantic_role'] = 'context'
    reply = render_source_answer(results, fixed)
    assert name in reply and '$137' in reply and 'Unlimited miles' in reply
    assert 'Selected offer information' not in reply and 'Choose an option' not in reply


def test_advertised_schema_requires_explicit_group_title():
    results, plan = grouped_source_fixture()
    schema = source_answer_tool_definition(source_answer_context(results), require_semantics=True)['input_schema']
    plan['answer_status'] = 'partial_results'
    for point in plan['points']:
        point.update(request_relation='result', summary='The selected offer has captured terms.')
    jsonschema.validate(plan, schema)
    del plan['points'][1]['title_ids']
    with pytest.raises(jsonschema.ValidationError): jsonschema.validate(plan, schema)


@pytest.mark.parametrize('change', ['same', 'other_evidence', 'explicit_empty', 'ambiguous'])
def test_cosmetic_repair_restores_only_unambiguous_prior_title_for_exact_evidence(change):
    results, before = grouped_source_fixture(); after = deepcopy(before)
    after['points'][1].pop('title_ids'); after['points'][1]['semantic_role'] = 'context'
    if change == 'other_evidence': after['points'][1]['evidence_ids'] = ['unowned', 'other']
    if change == 'explicit_empty': after['points'][1]['title_ids'] = []
    if change == 'ambiguous': before['points'].append(deepcopy(before['points'][1]))
    normalized = normalize_source_answer_selection(after, previous_selection=before)
    if change == 'same':
        assert normalized['points'][1]['title_ids'] == before['points'][1]['title_ids']
        assert 'Trail SUV' in render_source_answer(results, normalized)
    else:
        assert not normalized['points'][1].get('title_ids')
    assert 'title_ids' not in after['points'][1] or change == 'explicit_empty'


def test_partial_recovery_keeps_valid_group_whole_and_specific_limitations():
    from nullion.agent_orchestrator import _preserved_source_answer_on_failure
    results, plan = grouped_source_fixture()
    recovered = recover_selected_source_excerpts(results, plan)
    assert recovered['point_count'] == 2 and recovered['limitation_count'] == 2
    text = recovered['text']
    assert text.count('Trail SUV') == 1 and '$137' in text and 'Unlimited miles' in text
    assert 'Selected offer information' not in text and 'Earlier listing' not in text
    plan['points'][0].update(request_relation='result', summary='The other offer has captured terms.')
    plan['points'][1].update(request_relation='result', semantic_role='context',
        summary='Trail SUV: $137 with unlimited miles.')
    plan['limitation_summaries'] = {row['id']: row['text'] for row in source_answer_context(results)['limitations']}
    reply, metadata = _preserved_source_answer_on_failure({'tool_results': results,
        'source_answer_repair_context': {'previous_selection': plan},
        'source_answer_failure_kind': 'invalid_selection'}, None)
    assert 'safely combined or verified' not in reply
    assert all(value in reply for value in results[0].output['unknowns'])
    assert metadata['fulfillment_satisfied'] is False


def test_recovery_never_silently_joins_different_snapshots():
    results, plan = grouped_source_fixture()
    results[0].output['observations'][2]['captured_at'] = 200
    recovered = recover_selected_source_excerpts(results, plan)
    assert recovered['point_count'] >= 2
    # No resulting point may combine those now independent snapshots.
    assert all(len(row['points'][0]['evidence_ids']) == 1 for row in recovered['selections'])


def test_partial_recovery_does_not_claim_a_relation_from_a_shared_page_alone():
    results, plan = grouped_source_fixture(shared_quote=False)
    recovered = recover_selected_source_excerpts(results, plan)
    assert 'Other captured details' in recovered['text']
    assert all(len(row['points'][0]['evidence_ids']) == 1 for row in recovered['selections'])
    assert '$118' in recovered['text'] and '$137' in recovered['text']


def test_citation_numbers_are_stable_across_grouped_findings_intro_and_recovery():
    results, plan = grouped_source_fixture()
    results[0].output['observations'][0]['source_url'] = 'https://catalog.example/one'
    for row in results[0].output['observations'][1:3]: row['source_url'] = 'https://catalog.example/two'
    valid = deepcopy(plan); valid['points'][1]['semantic_role'] = 'context'
    reply = render_source_answer(results, valid)
    assert '[Source 1](https://catalog.example/one)' in reply
    assert '[Source 2](https://catalog.example/two)' in reply
    assert '[Source 1](https://catalog.example/two)' not in reply
    recovered = recover_selected_source_excerpts(results, plan)
    assert '[Source 2](https://catalog.example/two)' in recovered['text']


def test_source_answer_review_uses_shared_presentation_policy():
    from nullion.agent_orchestrator import _completion_review_request_context
    from nullion.chat_text import REPORT_REVIEW_GUIDANCE
    policy = _completion_review_request_context({'source_answer_checkpoint': {'presentation': {}}})['report_presentation_contract']
    assert policy['review_guidance'] == REPORT_REVIEW_GUIDANCE
    assert 'shared limitation may appear once' in policy['review_guidance']


def test_shared_attribute_deduplication_does_not_cross_cite_another_subjects_page():
    from nullion.tools import ToolResult
    observations = []
    for title, url, stamp in [('First offer', 'https://catalog.example/first', 100),
                              ('Second offer', 'https://catalog.example/second', 200)]:
        for value, role in [(title, 'entity'), ('Unlimited miles', 'attribute')]:
            observations.append({'value': value, 'quote': title+'\nUnlimited miles',
                'semantic_role': role, 'source_url': url, 'captured_at': stamp})
    results = [ToolResult('real-capture', 'opaque_capture', 'completed', {
        'result_kind': 'source_observations', 'report_status': 'completed', 'observations': observations})]
    rows = source_answer_context(results)['evidence']
    shared = next(r for r in rows if r['value'] == 'Unlimited miles')
    plan = {'intro': '', 'focus_roles': ['entity'], 'points': [], 'limitation_ids': []}
    for title in ('First offer', 'Second offer'):
        row = next(r for r in rows if r['value'] == title)
        title_ref = next(e['id'] for e in row['excerpts'] if e['text'] == title)
        detail_ref = next(e['id'] for e in shared['excerpts'] if e['text'] == 'Unlimited miles'
                          and set(e['capture_ids']).intersection(row['capture_ids']))
        plan['points'].append({'evidence_ids': [row['id'], shared['id']], 'title_ids': [title_ref],
            'detail_ids': [detail_ref], 'semantic_role': 'entity', 'relevance': 'Requested offer'})
    text = render_source_answer(results, plan)
    first, second = text.split('\n\n')
    assert 'catalog.example/first' in first and 'catalog.example/second' not in first
    assert 'catalog.example/second' in second and 'catalog.example/first' not in second
    assert '[Source 1]' in first and '[Source 2]' in second

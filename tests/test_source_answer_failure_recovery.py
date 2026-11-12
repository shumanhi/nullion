import pytest

from nullion.source_observation_contract import source_answer_context, recover_selected_source_excerpts
from nullion.tools import ToolResult


def selected_sources(size=2):
    report={'result_kind':'source_observations','report_status':'completed','observations':[
        {'value':f'Subject {i}: {170+i} cm','quote':f'Subject {i}: {170+i} cm',
         'semantic_role':'attribute','source_url':f'https://source{i}.example/profile','captured_at':100+i}
        for i in range(size)]}
    results=[ToolResult('capture','opaque_capture','completed',report)]
    rows=source_answer_context(results)['evidence']
    plan={'intro':'Unsupported introductory assertion','focus_roles':['entity','attribute'],
        'intro_evidence_ids':[], 'limitation_ids':[], 'points':[]}
    for start in range(0,size,8):
        group=rows[start:start+8]
        plan['points'].append({'evidence_ids':[r['id'] for r in group],
            'title_ids':[group[0]['excerpts'][0]['id']],
            'detail_ids':[r['excerpts'][0]['id'] for r in group[1:]],
            'detail_groups':[[r['excerpts'][0]['id']] for r in group[1:]],
            'semantic_role':'entity','relevance':'Requested subject properties'})
    return results,plan


@pytest.mark.parametrize('size',[2,30,80])
def test_recovery_separates_selected_capture_facts_without_inventing_relationships(size):
    results,plan=selected_sources(size)
    recovered=recover_selected_source_excerpts(results,plan)
    assert recovered and recovered['point_count']==size
    for i in range(size):assert f'Subject {i}: {170+i} cm' in recovered['text']
    assert 'Unsupported introductory assertion' not in recovered['text']
    assert 'Subject 0: 171 cm' not in recovered['text']


def test_unselected_inventory_and_unquoted_draft_text_are_not_recovered():
    results,plan=selected_sources(3)
    plan['points'][0]['evidence_ids']=plan['points'][0]['evidence_ids'][:2]
    plan['points'][0]['detail_ids']=[];plan['points'][0]['detail_groups']=[]
    recovered=recover_selected_source_excerpts(results,plan)
    assert 'Subject 2' not in recovered['text']
    plan['points']=[{'evidence_ids':['unknown'],'detail':'Invented detail','semantic_role':'entity','relevance':'wanted'}]
    assert recover_selected_source_excerpts(results,plan) is None


def test_controls_do_not_become_entities_in_partial_recovery():
    results,plan=selected_sources()
    row=results[0].output['observations'][0]
    row.update(value='Main Stage',quote='Main Stage',semantic_role='interface_control')
    recovered=recover_selected_source_excerpts(results,plan)
    assert recovered and 'Main Stage' not in recovered['text'] and 'Subject 1' in recovered['text']


def test_recovery_deduplicates_the_same_fact_from_different_captured_sources():
    results,plan=selected_sources()
    results[0].output['observations'][1].update(value='Subject 0: 170 cm',quote='Subject 0: 170 cm')
    recovered=recover_selected_source_excerpts(results,plan)
    assert recovered['point_count']==1 and recovered['text'].count('Subject 0: 170 cm')==1


@pytest.mark.parametrize('protected',['approval','cancelled'])
def test_recovered_selections_do_not_bypass_consent_or_cancellation(protected):
    from nullion.agent_orchestrator import _complete_agent_turn
    from nullion.tools import ToolRegistry
    results,plan=selected_sources()
    state={'tool_results':results,'source_answer_repair_context':{'previous_selection':plan},
        'source_answer_terminal_failure':True,'tool_registry':ToolRegistry(),
        'conversation_id':'web:recovery','principal_id':'web:recovery','user_message':'一覧'}
    kwargs={'suspended_for_approval':True} if protected=='approval' else {}
    if protected=='cancelled':state['cancellation_checker']=lambda:True
    result=_complete_agent_turn(state,final_text='Pending.',response_fulfilled=False,**kwargs)['result']
    assert 'Subject 0' not in result.final_text


def test_partial_recovery_does_not_publish_literal_inventory_without_an_authored_answer():
    from nullion.agent_orchestrator import _preserved_source_answer_on_failure
    results,plan=selected_sources()
    state={'tool_results':results,'source_answer_repair_context':{'previous_selection':plan},
        'source_answer_semantically_rejected':True}
    assert _preserved_source_answer_on_failure(state,'Timeout') is None


def test_partial_recovery_requires_prior_selection_and_requested_roles():
    results,plan=selected_sources()
    assert recover_selected_source_excerpts(results,None) is None
    plan.pop('focus_roles')
    assert recover_selected_source_excerpts(results,plan) is None


def test_recovery_preserves_selected_limitations_without_adding_unselected_unknowns():
    results,plan=selected_sources()
    results[0].output['report_status']='partial'
    results[0].output['unknowns']=['The remaining subject was unavailable.','Unrelated source metadata.']
    plan['limitation_ids']=['u0','unowned']
    recovered=recover_selected_source_excerpts(results,plan)
    assert 'The remaining subject was unavailable.' in recovered['text']
    assert 'Unrelated source metadata.' not in recovered['text']


def test_artifact_failure_keeps_text_and_scheduled_output_ownership():
    from nullion.agent_orchestrator import _preserved_source_answer_on_failure
    results,plan=selected_sources()
    row = source_answer_context(results)['evidence'][0]
    plan['points'] = [{'evidence_ids': [row['id']], 'detail_ids': [], 'semantic_role': 'attribute',
        'request_relation': 'result', 'summary': 'Subject 0: 170 cm', 'relevance': 'Requested height'}]
    state={'tool_results':results,'source_answer_repair_context':{'previous_selection':plan},
        'source_answer_failure_kind':'model_timeout',
        'completion_requirement_contract_cache':{'requested_artifact_extensions':['.pdf']}}
    reply,metadata=_preserved_source_answer_on_failure(state,'Timeout')
    assert 'Subject 0: 170 cm' in reply and 'The requested file is not ready' in reply
    assert metadata['fulfillment_satisfied'] is False
    assert _preserved_source_answer_on_failure({**state,
        'tool_flow_context':{'scheduled_task_run':True}},'Timeout') is None


def test_invalid_authored_join_is_not_split_into_a_new_fragment_dump():
    results, plan = selected_sources()
    plan['points'][0].update(request_relation='result', summary='An unsupported cross-source relationship.')
    assert recover_selected_source_excerpts(results, plan, authored_only=True) is None


def test_model_timeout_copy_is_distinct_from_an_actual_turn_deadline():
    from nullion.agent_orchestrator import _CompletionReviewDecision
    from nullion.review_publication import render_incomplete_review
    reply=render_incomplete_review(_CompletionReviewDecision('blocked',runtime_failure_code='model_response_timeout'))
    assert reply=='The model timed out while preparing the response.'
    assert render_incomplete_review(_CompletionReviewDecision('blocked',runtime_failure_code='deadline'))!=reply

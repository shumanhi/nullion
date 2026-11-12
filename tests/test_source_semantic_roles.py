"""Semantic source roles supplement literal quote/capture verification."""
import copy
import json
import pytest
from nullion.source_observation_contract import (
    compact_source_observations, source_answer_context, source_answer_tool_definition,
    render_source_answer, source_answer_review_records,
)
from nullion.tools import ToolResult


def source_results():
    observations = [
        ('entity', 'Dracula', 'Name\nDracula'),
        ('attribute', 'October 24, 2026 7:30PM', 'Date\nOctober 24, 2026 7:30PM'),
        ('interface_control', 'Main Stage', 'Select a section\nMain Stage\nNo Tickets Available'),
        ('interface_control', 'Let us choose seats for you', 'Let us choose seats for you\nPurchase Best Available Seating (including Standing Room Only)'),
    ]
    return [ToolResult('capture', 'browser_run_task', 'completed', {
        'result_kind':'source_observations', 'report_status':'partial', 'next_action':'report',
        'observations':[{'label':str(index), 'semantic_role':role,'value':value,'quote':quote,
            'source_url':'https://venue.example/performance', 'captured_at':100} for index,(role,value,quote) in enumerate(observations)],
        'unknowns':['Whether a purchase option yields reservable inventory was not tested.'], 'blocker':None,
    })]


def presentation(results, role='entity', index=0):
    rows=source_answer_context(results)['evidence']; row=rows[index]
    return {'intro':'', 'focus_roles':[role], 'points':[{'evidence_ids':[row['id']],
        'semantic_role':role, 'detail_ids':[], 'relevance':'Requested subject'}], 'limitation_ids':[]}


def test_rejects_control_promoted_to_event_even_when_quote_is_verbatim():
    results=source_results()
    with pytest.raises(ValueError,match='contradicts'):
        render_source_answer(results,presentation(results,index=2))


@pytest.mark.parametrize('role,index',[('interface_control',2),('interface_control',3),('attribute',1),('entity',0)])
def test_requested_controls_properties_and_entities_remain_valid(role,index):
    results=source_results(); plan=presentation(results,role,index)
    assert source_answer_context(results)['evidence'][index]['value'] in render_source_answer(results,plan)


def test_properties_stay_with_the_entity_without_becoming_independent_events():
    results=source_results(); rows=source_answer_context(results)['evidence']; plan=presentation(results)
    point=plan['points'][0];point['evidence_ids'].append(rows[1]['id'])
    point['title_ids']=[rows[0]['excerpts'][1]['id']]
    point['detail_ids']=[rows[1]['excerpts'][1]['id']]
    rendered=render_source_answer(results,plan)
    assert rendered.count('Dracula')==1 and '7:30PM' in rendered
    assert 'Main Stage' not in rendered and 'Not verified' not in rendered
    bound,_,_=source_answer_review_records(results,plan)
    assert len(bound)==1 and bound[0].output['source_record_binding']['title']=='Dracula'


def test_wrong_focus_cannot_list_supporting_controls_as_standalone_items():
    results=source_results(); plan=presentation(results,'interface_control',2);plan['focus_roles']=['entity']
    with pytest.raises(ValueError,match='requested semantic role'):render_source_answer(results,plan)


@pytest.mark.parametrize('title',['Pedal-assist bicycles','Bicicletas de pedaleo asistido','ペダル補助自転車'])
def test_captured_attribute_can_supply_non_entity_background_context(title):
    results=source_results();results[0].output['observations'][1].update(value=title,quote=title+'\nMotor assistance operates while pedaling.')
    assert title in render_source_answer(results,presentation(results,'context',1))


def test_interface_control_cannot_be_relabelled_as_background_fact():
    results=source_results()
    with pytest.raises(ValueError,match='contradicts'):render_source_answer(results,presentation(results,'context',2))


def test_typed_observations_require_explicit_item_roles():
    results=source_results();results[0].output['unknowns']=[];results[0].output['report_status']='completed'
    plan=presentation(results);del plan['points'][0]['semantic_role'];del plan['focus_roles']
    with pytest.raises(ValueError,match='explicit answer item role'):render_source_answer(results,plan)


def test_empty_optional_intro_evidence_is_valid_without_an_intro():
    results=source_results();plan=presentation(results)
    plan['intro_evidence_ids']=[]
    assert 'Dracula' in render_source_answer(results,plan)
    definition=source_answer_tool_definition(source_answer_context(results),require_semantics=True)
    assert definition['input_schema']['properties']['intro_evidence_ids']['minItems']==0


def test_explicit_summary_coverage_avoids_duplicate_conclusion():
    results=source_results();plan=presentation(results);plan.update(intro='Dracula is playing.',intro_evidence_ids=['e0.0'],summary_point_indices=[0])
    rendered=render_source_answer(results,plan)
    assert rendered.count('Dracula')==1 and '- **' not in rendered and 'venue.example' in rendered
    plan['intro_evidence_ids']=['e0.1']
    with pytest.raises(ValueError,match='cite every record'):render_source_answer(results,plan)


@pytest.mark.parametrize('indices',[[True],[1],[-1],[0,0],'0'])
def test_summary_coverage_is_typed_and_bounded(indices):
    results=source_results();plan=presentation(results);plan['summary_point_indices']=indices
    with pytest.raises(ValueError):render_source_answer(results,plan)


def test_role_metadata_survives_compaction_and_duplicate_captures():
    results=source_results(); compact=compact_source_observations(results[0].output)
    assert [r['semantic_role'] for r in compact['observations']]==['entity','attribute','interface_control','interface_control']
    later=copy.deepcopy(results[0].output);later['observations']=later['observations'][:1]
    later['observations'][0].update(semantic_role='interface_control',captured_at=200)
    context=source_answer_context(results+[ToolResult('later','browser_run_task','completed',later)])
    same_value=[r for r in context['evidence'] if r['value']=='Dracula']
    assert len(same_value)==2
    bindings=[b for r in same_value for b in r['semantic_bindings']]
    assert {b['role'] for b in bindings}=={'entity','interface_control'}
    assert len({b['capture_id'] for b in bindings})==2


def test_new_tool_schema_requires_semantic_decisions_but_legacy_checkpoints_are_readable():
    import jsonschema
    results=source_results();context=source_answer_context(results);plan=presentation(results)
    plan['answer_status'] = 'partial_results'
    plan['points'][0]['request_relation'] = 'result'
    plan['points'][0]['summary'] = 'Observed event listing.'
    schema=source_answer_tool_definition(context,require_semantics=True)['input_schema']
    jsonschema.validate(plan,schema)
    del plan['focus_roles']
    with pytest.raises(jsonschema.ValidationError):jsonschema.validate(plan,schema)
    legacy=copy.deepcopy(results[0].output);legacy['unknowns']=[];legacy['report_status']='completed'
    for row in legacy['observations']:row.pop('semantic_role')
    old=[ToolResult('old','browser_run_task','completed',legacy)]
    plan=presentation(old);del plan['focus_roles'];del plan['points'][0]['semantic_role']
    assert 'Dracula' in render_source_answer(old,plan)


@pytest.mark.parametrize('name',['Main Stage','Choose your seats','舞台','選擇座位','Escenario principal'])
def test_entity_names_are_never_filtered_by_words(name):
    results=source_results(); row=results[0].output['observations'][0]
    row.update(value=name,quote='Name\n'+name)
    assert name in render_source_answer(results,presentation(results))


def test_browser_capture_preserves_model_roles_without_deciding_from_text():
    import asyncio
    from types import SimpleNamespace
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserReport,PageEvidence
    async def read():return {'url':'https://venue.example','text':'Choose your seats'}
    evidence=PageEvidence('test',BrowserPolicy(),text_capture=read)
    asyncio.run(evidence.capture(SimpleNamespace(url='https://venue.example',title='Page',dom_state=None,screenshot=None)))
    report=BrowserReport(goal_status='answered',next_action='report',observations=[{
        'label':'作品','value':'Choose your seats','quote':'Choose your seats','semantic_role':'entity'}])
    assert evidence.retain(report)==1
    assert evidence.result(error=None,elapsed=1,steps=1,done=True)['observations'][0]['semantic_role']=='entity'
    report.observations[0].semantic_role='interface_control'
    assert evidence.retain(report)==1
    assert len(evidence.observations)==2



def test_name_attribute_can_identify_its_captured_entity_listing():
    results=source_results();rows=results[0].output['observations']
    rows[0].update(value='Events — Dracula, 7:30PM',quote='Events — Dracula, 7:30PM')
    rows[1].update(value='Dracula',quote='Name\nDracula')
    context=source_answer_context(results)['evidence'];plan=presentation(results)
    plan['points'][0].update(evidence_ids=[context[0]['id'],context[1]['id']],
        title_ids=[context[1]['excerpts'][1]['id']],detail_ids=[context[0]['excerpts'][0]['id']])
    assert '**Dracula**' in render_source_answer(results,plan)
    rows[0].update(value='Another event',quote='Another event')
    with pytest.raises(ValueError,match='contradicts'):render_source_answer(results,plan)


def test_control_cannot_be_smuggled_into_an_entity_title_with_an_entity_name():
    results=source_results();context=source_answer_context(results)['evidence'];plan=presentation(results)
    plan['points'][0].update(evidence_ids=[context[0]['id'],context[2]['id']],
        title_ids=[context[0]['excerpts'][1]['id'],context[2]['excerpts'][1]['id']])
    with pytest.raises(ValueError,match='contradicts'):render_source_answer(results,plan)



def test_reviewed_summary_survives_a_later_missing_requirement_without_raw_fields():
    from nullion import agent_orchestrator as agent
    from nullion.tools import ToolRegistry
    results=source_results();plan=presentation(results)
    plan.update(intro='Dracula is on the schedule.',intro_evidence_ids=['e0.0'],summary_point_indices=[0],limitation_ids=['u0'])
    state={'user_message':'What is happening today?', 'tool_registry':ToolRegistry(), 'tool_results':results,
        'source_answer_checkpoint':{'tool_result_count':1,'presentation':plan,'presentation_reviewed':True}}
    reply,_=agent._preserved_source_answer_on_failure(state,'Another requested detail was unavailable.')
    assert reply.count('Dracula')==1 and reply.startswith('Dracula is on the schedule.')
    assert '- **' not in reply and 'Here are the findings' not in reply
    assert results[0].output['unknowns'][0] in reply


@pytest.mark.parametrize("mode",["noncontiguous","missing"])
def test_group_repair_error_identifies_exact_point_and_excerpt_ids(mode):
    results=source_results();plan=presentation(results)
    context=source_answer_context(results)["evidence"]
    first,second=[row["id"] for row in context[1]["excerpts"][:2]]
    plan["points"][0].pop("detail",None)
    plan["points"][0].update(evidence_ids=[context[0]["id"],context[1]["id"]],
        title_ids=[context[0]["excerpts"][1]["id"]],detail_ids=[first,second],
        detail_groups=[[second,first]] if mode=="noncontiguous" else [[second]])
    with pytest.raises(ValueError) as error:render_source_answer(results,plan)
    message=str(error.value)
    assert '"point_index":0' in message and first in message
    assert ("noncontiguous_groups" if mode=="noncontiguous" else "missing_detail_ids") in message


@pytest.mark.parametrize("reason",["Remove the unsupported draft fields","Elimina las afirmaciones sin pruebas","未確認の記述を削除する"])
def test_internal_review_guidance_is_separate_from_terminal_message(reason):
    from nullion import agent_orchestrator as agent
    payload={"disposition":"blocked","unresolved_requirements":[reason],"retry_tool_names":[],
        "user_facing_failure":"The requested pickup time is after the branch closes."}
    model={"content":[{"type":"text","text":json.dumps(payload)}]}
    decision=agent._parse_completion_review_decision(model,registered_tool_names=set())
    assert payload["user_facing_failure"] not in agent._completion_review_open_task_reply(decision)
    assert decision.user_facing_failure == payload["user_facing_failure"]
    assert reason in agent._completion_review_retry_nudge(decision)
    payload["user_facing_failure"]={"unvalidated":"object"}
    assert agent._parse_completion_review_decision({"content":[{"type":"text","text":json.dumps(payload)}]},registered_tool_names=set()) is None


def test_repair_reports_citation_and_title_role_defects_together():
    from nullion.source_observation_contract import source_answer_repair_context
    results=source_results();plan=presentation(results,index=2)
    plan.update(intro='A proposed summary',intro_evidence_ids=['e0.0'],summary_point_indices=[0])
    with pytest.raises(ValueError) as failure:render_source_answer(results,plan)
    before=copy.deepcopy(plan)
    context=source_answer_repair_context(results,plan,failure.value)
    assert 'missing_intro_evidence_ids' in context['validation_issues'][0]['reason']
    issue=next(row for row in context['validation_issues'] if row.get('point_index')==0)
    assert 'captured_title_roles' in issue['reason'] and 'interface_control' in issue['reason']
    assert context['previous_selection']==before==plan


def test_recorded_event_lineup_has_a_valid_entity_answer_without_promoting_section_labels():
    from pathlib import Path
    from nullion.source_observation_contract import source_answer_repair_context
    report=json.loads((Path(__file__).parent/'fixtures/resident_advisor_source_observations.json').read_text())
    results=[ToolResult('observed','browser_run_task','completed',report)]
    rows=source_answer_context(results)['evidence']
    # Same two independent defects as the production draft: missing summary
    # citations and a source attribute misrepresented as a real-world subject.
    bad={'intro':'The lineup is confirmed.', 'intro_evidence_ids':[rows[5]['id']],
        'focus_roles':['entity'],'summary_point_indices':[0], 'points':[
            {'evidence_ids':[rows[4]['id']], 'detail_ids':[], 'semantic_role':'entity',
             'relevance':'Requested artists'}], 'limitation_ids':[]}
    with pytest.raises(ValueError) as failure:render_source_answer(results,bad)
    feedback=source_answer_repair_context(results,bad,failure.value)
    assert any('missing_intro_evidence_ids' in issue['reason'] for issue in feedback['validation_issues'])
    assert any('captured_title_roles' in issue['reason'] for issue in feedback['validation_issues'])
    entity=rows[9]
    repaired={'intro':'', 'focus_roles':['entity'],'points':[{
        'evidence_ids':[entity['id']], 'title_ids':[entity['excerpts'][0]['id']],
        'detail_ids':[], 'semantic_role':'entity','relevance':'The captured artist lineup'}], 'limitation_ids':[]}
    reply=render_source_answer(results,repaired)
    assert all(name in reply for name in ['Word of Command','Mez Monty','Cyler Says'])
    assert 'LINEUP' not in reply and 'focused answer' not in reply



def test_blocked_source_is_labeled_as_access_without_relabeling_verified_input_unknown():
    report={'result_kind':'source_observations','report_status':'blocked','next_action':'report',
        'blocker':'The comparison page returned an access challenge.','observations':[], 'unknowns':[]}
    results=[ToolResult('blocked','browser_run_task','completed',report)]
    context=source_answer_context(results)
    reply=render_source_answer(results,{'intro':'The supplied source is retained.','focus_roles':['context'],
        'points':[],'limitation_ids':[context['limitations'][0]['id']]})
    assert '**Source access**' in reply and 'access challenge' in reply
    assert '**Not verified' not in reply and '\n\n\n' not in reply


def test_explicit_source_groups_derive_one_consistent_flat_index_for_render_and_review():
    from nullion.source_observation_contract import normalize_source_answer_selection
    results=source_results();rows=source_answer_context(results)['evidence'];plan=presentation(results)
    label,value=[row['id'] for row in rows[1]['excerpts'][:2]]
    plan['points'][0].update(evidence_ids=[rows[0]['id'],rows[1]['id']],title_ids=[rows[0]['excerpts'][1]['id']],
        detail_ids=[value],detail_groups=[[label,value]])
    normalized=normalize_source_answer_selection(plan)
    assert normalized['points'][0]['detail_ids']==[label,value] and plan['points'][0]['detail_ids']==[value]
    reply=render_source_answer(results,plan);assert 'Date October 24' in reply
    bound,_,_=source_answer_review_records(results,plan)
    assert len(bound)==1 and bound[0].output['source_record_binding']['fields']==['Date October 24, 2026 7:30PM']


@pytest.mark.parametrize('invalid',['unknown','cross_capture','control_title','duplicate_group'])
def test_group_index_normalization_never_bypasses_source_capture_or_role_validation(invalid):
    results=source_results();rows=source_answer_context(results)['evidence'];plan=presentation(results)
    label,value=[row['id'] for row in rows[1]['excerpts'][:2]]
    plan['points'][0].update(evidence_ids=[rows[0]['id'],rows[1]['id']],title_ids=[rows[0]['excerpts'][1]['id']],
        detail_ids=[value],detail_groups=[[label,value]])
    if invalid=='unknown':plan['points'][0]['detail_groups'][0].append('not-captured')
    elif invalid=='cross_capture':results[0].output['observations'][1]['captured_at']=200
    elif invalid=='control_title':
        plan['points'][0]['evidence_ids'].append(rows[2]['id']);plan['points'][0]['title_ids']=[rows[2]['excerpts'][1]['id']]
    else:plan['points'][0]['detail_groups'].append([label,value])
    with pytest.raises(ValueError):render_source_answer(results,plan)
    assert source_answer_review_records(results,plan)[0]==[]

"""Equal display names do not identify equal captured records."""
import copy
import pytest
from nullion.tools import ToolResult
from nullion.source_observation_contract import source_answer_context,render_source_answer,source_answer_review_records


def records(carrier='American'):
    values=[f'7:14 AM\n–\n9:05 AM\n{carrier}\n2 hr 51 min\nJFK–ORD\nNonstop\n$249',
            f'10:29 AM\n–\n12:16 PM\n{carrier}\n2 hr 47 min\nJFK–ORD\nNonstop\n$249']
    return [ToolResult('flight-capture','browser_run_task','completed',{
        'result_kind':'source_observations','report_status':'completed','goal_status':'answered',
        'completion_scope':'tool_step','next_action':'report','blocker':None,'unknowns':[],
        'sources':[{'source_url':'https://travel.example/results','captured_at':100,
            'main_document_text':'\n'.join(values)}],
        'observations':[{'label':'Flight option','value':value,'quote':value,'source_index':0,
                         'semantic_role':'entity'} for value in values]})]


def selection(results=None, *, context=None):
    rows=(context if context is not None else source_answer_context(results))['evidence'];points=[]
    for row in rows:
        excerpts=row['excerpts']
        points.append({'evidence_ids':[row['id']],'semantic_role':'entity','relevance':'Requested option',
            'title_ids':[excerpts[3]['id']],
            'detail_ids':[r['id'] for i,r in enumerate(excerpts) if i!=3],
            'detail_groups':[[r['id'] for r in excerpts[:3]],*[ [r['id']] for r in excerpts[4:]]]})
    return {'intro':'','focus_roles':['entity'],'points':points,'limitation_ids':[]}


@pytest.mark.parametrize('carrier',['American','Misma compañía','共通の航空会社'])
def test_distinct_records_can_share_the_same_display_title(carrier):
    source=records(carrier);plan=selection(source)
    rendered=render_source_answer(source,plan,require_semantics=True)
    assert rendered.count('**'+carrier+'**')==2
    assert all(value in rendered for value in ['7:14 AM','9:05 AM','10:29 AM','12:16 PM'])
    bound,fingerprints,_=source_answer_review_records(source,plan)
    assert len(bound)==2 and len(set(fingerprints.values()))==2
    assert len({row.invocation_id for row in bound})==2


def test_identical_presented_records_still_reject_duplicate_points():
    source=records();plan=selection(source);plan['points']=[plan['points'][0],copy.deepcopy(plan['points'][0])]
    with pytest.raises(ValueError,match='Repeated answer item'):
        render_source_answer(source,plan,require_semantics=True)


def test_duplicate_rendering_from_different_captures_still_rejected():
    source=records();duplicate=copy.deepcopy(source[0]);duplicate.invocation_id='second-capture'
    duplicate.output['sources'][0]['captured_at']=200
    for row in duplicate.output['observations']:
        row['value']+='\nOther captured field'
        row['quote']=row['value']
    source.append(duplicate);plan=selection(source)
    assert len(plan['points'])==4
    for point in plan['points'][2:]:
        hidden=point['detail_ids'].pop()
        point['detail_groups']=[group for group in point['detail_groups'] if hidden not in group]
    # Distinct captured records with identical selected content still cannot
    # repeat the same cards. The first two different departures must pass.
    with pytest.raises(ValueError,match='"point_index":2'):
        render_source_answer(source,plan,require_semantics=True)


def test_same_production_with_distinct_showtimes_remains_two_records():
    source=[ToolResult('shows','browser_run_task','completed',{
        'result_kind':'source_observations','report_status':'completed',
        'observations':[
            {'value':'Dracula','quote':'Name\nDracula','semantic_role':'entity'},
            {'value':'2:00 PM','quote':'Time\n2:00 PM','semantic_role':'attribute'},
            {'value':'7:30 PM','quote':'Time\n7:30 PM','semantic_role':'attribute'},
        ]})]
    for row in source[0].output['observations']:
        row.update(source_url='https://venue.example/performances',captured_at=100)
    rows=source_answer_context(source)['evidence'];points=[]
    for row in rows[1:]:
        points.append({'evidence_ids':[rows[0]['id'],row['id']],
            'title_ids':[rows[0]['excerpts'][1]['id']],
            'detail_ids':[row['excerpts'][1]['id']],
            'semantic_role':'entity','relevance':'A distinct captured performance time'})
    plan={'intro':'','focus_roles':['entity'],'points':points,'limitation_ids':[]}
    rendered=render_source_answer(source,plan,require_semantics=True)
    assert rendered.count('**Dracula**')==2 and '2:00 PM' in rendered and '7:30 PM' in rendered
    bound,fingerprints,_=source_answer_review_records(source,plan)
    assert len(bound)==2 and len(set(fingerprints.values()))==2

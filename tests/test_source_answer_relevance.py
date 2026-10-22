import copy
import json
from pathlib import Path
from types import SimpleNamespace
import pytest
from nullion.source_observation_contract import source_answer_context, render_source_answer, partial_source_report_reply

REPORT=json.loads((Path(__file__).parent/'fixtures/clinique_source_observations.json').read_text())

def results(report=REPORT):
    return [SimpleNamespace(status='completed',output=report)]

def focused_answer(report=REPORT):
    context=source_answer_context(results(report))
    rows={r['value']:r for r in context['evidence']}
    chosen=[REPORT['observations'][i]['value'] for i in (6,11,12)]
    detail=[REPORT['observations'][6]['quote'].split('\n')[-1],REPORT['observations'][11]['quote'].split('\n')[-1],REPORT['observations'][12]['quote'].split('\n')[-1]]
    return {'intro':'These are the most relevant options from the captured pages:',
            'points':[{'evidence_ids':[rows[value]['id']], 'detail':excerpt, 'relevance':'Addresses the requested concern'} for value,excerpt in zip(chosen,detail)],
            'limitation_ids':['u0','u1']}


def test_recorded_report_is_deduplicated_before_model_relevance_selection():
    context=source_answer_context(results())
    assert len(context['evidence'])==15
    assert len(context['evidence'][6]['quotes'])==2
    assert all('evidence_path' not in row for row in context['evidence'])
    reply=render_source_answer(results(),focused_answer())
    assert reply.count('**Even Better Clinical™ Dark Spot Clearing Serum**')==1
    assert 'Smart Clinical Repair' not in reply and 'All About Eyes' not in reply
    assert 'Named sunscreen ingredients' not in reply and 'additional findings' not in reply
    assert REPORT['unknowns'][0] in reply and REPORT['unknowns'][1] in reply
    assert REPORT['unknowns'][2] not in reply
    assert len(reply)<1700 and len(reply.splitlines())<12


@pytest.mark.parametrize('missing_field', ['Expected delivery date', '配達予定日', 'موعد التسليم'])
def test_unknown_field_is_disclosed_as_unverified_instead_of_a_dangling_heading(missing_field):
    report = {'result_kind':'source_observations', 'report_status':'partial',
        'observations':[{'value':'Tracking Not Available', 'quote':'Tracking Not Available'}],
        'unknowns':[missing_field]}
    reply = render_source_answer(results(report), {'intro':'',
        'points':[{'evidence_ids':['e0.0'], 'detail':'', 'relevance':'Requested status'}],
        'limitation_ids':['u0']})
    disclosure = reply.split('\n\n')[-1]
    assert disclosure != missing_field
    assert 'Not verified' in disclosure and missing_field in disclosure
    assert 'Tracking Not Available' in reply


@pytest.mark.parametrize('mutation',[lambda p:p['points'][0].update(evidence_ids=['invented']),
    lambda p:p['points'][0].update(detail='Clinically proven for South Asian skin'),
    lambda p:p['points'].append(copy.deepcopy(p['points'][0])),
    lambda p:p.update(limitation_ids=['invented']),
    lambda p:p['points'][0].update(relevance='')])
def test_unsupported_or_duplicate_presentation_is_rejected(mutation):
    p=focused_answer();mutation(p)
    with pytest.raises(ValueError):render_source_answer(results(),p)
    assert 'Moisture Surge' not in partial_source_report_reply(results(),draft=p)


def test_invalid_plain_draft_never_falls_back_to_evidence_inventory():
    text=partial_source_report_reply(results(),draft='Unavailable: https://invented.example/')
    assert 'invented.example' not in text and 'Moisture Surge' not in text
    assert len(text)<150


def test_excerpt_ids_are_verified_without_requiring_the_model_to_copy_source_text():
    context=source_answer_context(results())
    row=context['evidence'][6]
    detail=next(e for e in row['excerpts'] if e['text'].startswith('Brightening serum'))
    plan={'intro':'Relevant option:', 'points':[{'evidence_ids':[row['id']],
        'detail_ids':[detail['id']], 'relevance':'Addresses the specific request'}], 'limitation_ids':['u0']}
    reply=render_source_answer(results(),plan)
    assert detail['text'] in reply
    plan['points'][0]['detail_ids']=[context['evidence'][0]['excerpts'][0]['id']]
    with pytest.raises(ValueError):render_source_answer(results(),plan)


def test_one_point_cannot_attribute_another_product_claim_to_its_title():
    context=source_answer_context(results())
    first,other=context['evidence'][:2]
    plan={'intro':'', 'points':[{'evidence_ids':[first['id'],other['id']],
        'detail_ids':[other['excerpts'][-1]['id']], 'relevance':'Related'}], 'limitation_ids':[]}
    with pytest.raises(ValueError):render_source_answer(results(),plan)


def test_source_summary_keeps_existing_system_memory_and_delivery_context():
    from nullion.agent_orchestrator import _agent_turn_model_node
    from nullion.tools import ToolRegistry,ToolResult
    captured=[]
    class Provider:
        def create(self,**kw):
            captured.append(kw)
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'{}'}]}
    state={'source_report_delivery_ready':True,'orchestrator':SimpleNamespace(model_client=Provider()),
        'user_message':'選んでください','conversation_id':'web:memory-context','iterations':0,'max_iterations':4,
        'tool_registry':ToolRegistry(), 'tool_results':[ToolResult('observed','browser_run_task','completed',REPORT)],
        'messages':[{'role':'system','content':[{'type':'text','text':'Saved preference: concise replies with emojis.'}]},
                    {'role':'system','content':'Keep internal paths out of chat.'}],
        'tool_recovery_scopes_attempted':[]}
    _agent_turn_model_node(state)
    assert 'Saved preference: concise replies with emojis.' in captured[0]['system']
    assert 'Keep internal paths out of chat.' in captured[0]['system']
    assert {tool['name'] for tool in captured[0]['tools']}=={'present_source_answer'}

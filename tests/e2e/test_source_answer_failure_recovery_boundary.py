"""Collected facts survive source-answer syntax failure and model timeout.

The route, graph, SQLite, shared renderer and Telegram SDK are real; only the
external source/model/HTTP boundaries are controlled. No wall-clock timeout wait.
"""
import asyncio
import json
import os

import httpx
import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
from tests.workflow_provider_protocol import workflow_provider_reply,fixture_source_plan,fixture_source_assessment

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


@pytest.mark.parametrize('mode',['timeout','invalid_again','long_verified','role_then_long','reviewed_partial','response_error','no_selection','semantic_rejection'])
@pytest.mark.parametrize('prompt',['一覧にまとめてください。','Presenta los datos solicitados.'])
def test_real_chat_and_telegram_preserve_only_selected_source_facts_after_repair_failure(tmp_path,monkeypatch,mode,prompt):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion import chat_store,web_app
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    runtime=setup_runtime(tmp_path,monkeypatch)
    for key,value in {'NULLION_ENV_FILE':str(tmp_path/'absent.env'),'NULLION_MEMORY_ENABLED':'false',
        'NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_ACTIVITY_TRACE_ENABLED':'false','NULLION_CHAT_STARTUP_WARMUP':'0'}.items():monkeypatch.setenv(key,value)
    monkeypatch.setattr(chat_store,'_store',None)
    for key in ['NULLION_AGENT_MODEL_TIMEOUT_SECONDS','NULLION_ARTIFACT_MODEL_TIMEOUT_SECONDS']:monkeypatch.delenv(key,raising=False)
    report={'result_kind':'source_observations','report_status':'completed','goal_status':'answered','next_action':'report',
        'observations':[{'label':name,'value':value,'quote':value,'semantic_role':role,
            'source_url':url,'captured_at':stamp} for name,value,role,url,stamp in [
                ('Aoi','Aoi: 173 cm','attribute','https://first.example/person',100),
                ('Ren','Ren: 183 cm','attribute','https://second.example/person',200),
                ('Unselected','UNSELECTED INVENTORY','entity','https://unrelated.example/page',300),
                ('Control','Main Stage','interface_control','https://first.example/person',100)]], 'unknowns':[]}
    if mode in {'long_verified','role_then_long','reviewed_partial'}:
        report['observations'][0]['quote']+='\n'+'Optional source catalog '*45
    reads=[];attempts=[];reviews=[]
    network=httpx.Client(transport=httpx.MockTransport(lambda request:httpx.Response(200,json=report)))
    registry=ToolRegistry()
    def read(inv):
        reads.append(inv.invocation_id)
        return ToolResult(inv.invocation_id,inv.tool_name,'completed',network.get('https://capture.example/').json())
    registry.register(ToolSpec('opaque_capture','Read the selected public records',ToolRiskLevel.LOW,
        ToolSideEffectClass.READ,False,2,capability_tags=('web',)),read)
    class Model:
        presentations=0
        def create(self,**kw):
            system=kw.get('system') or '';names={r['name'] for r in kw.get('tools') or ()}
            if 'material-requirements planner' in system:
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text_response(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'),
                    'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'Present requested source facts','requirement_kind':'fact',
                        'evidence_subject_id':'requested-subjects','eligible_evidence_tool_names':['opaque_capture']}]}))
            if 'semantic completion reviewer' in system:
                reviews.append(kw);packet=json.loads(kw['messages'][0]['content'][0]['text'])
                assert mode in {'semantic_rejection','invalid_again','long_verified','role_then_long','reviewed_partial'},'Timed-out recovery started another model review'
                proof=packet['citable_evidence'][0]['invocation_id']
                if mode in {'invalid_again','long_verified','role_then_long','reviewed_partial'}:
                    from nullion.chat_text import REPORT_MESSAGE_FORMAT
                    refs=[r['invocation_id'] for r in packet['citable_evidence']]
                    ids=[r['requirement_id'] for r in packet['material_requirement_contract']]
                    message={'title':'Verified findings','summary':'Requested measurements.',
                        'detail_level':'compact','sources':[],'shared_details':[],
                        'sections':[{'heading':'Measurements','priority':'primary','layout':'list','items':[
                            {'title':name,'summary':value,'required_details':[], 'extra_details':[],
                                'source_ids':[],'shared_detail_ids':[]} for name,value in [('Aoi','Aoi: 173 cm'),('Ren','Ren: 183 cm')]]}]}
                    complete=mode in {'long_verified','role_then_long'}
                    verdict = {**fixture_source_assessment(packet,conclusion='limited',sufficient=True),
                        'disposition':'complete' if complete else 'blocked',
                        # Real providers can attach a correction target to a
                        # partial verdict. Keep that verdict and its findings.
                        'reply_target':'corrected_findings',
                        'source_presentation_valid':True if mode!='invalid_again' else None,
                        'unresolved_requirements':[] if complete else ['The requested combined record could not be verified.'],
                        'all_material_requirements_enumerated':True,
                        'material_requirements':[{'requirement_id':identity,'satisfied':complete,'draft_covered':complete,
                            'evidence_invocation_ids':refs} for identity in ids],
                        'supported_findings':[{'text':{'format':REPORT_MESSAGE_FORMAT,'message':message},
                            'presentation':'paragraph','requirement_ids':ids,'evidence_invocation_ids':refs}]}
                    if complete:
                        verdict['retry_tool_names'] = []
                        from copy import deepcopy
                        from jsonschema import validate, ValidationError
                        validate(verdict, packet['review_response_schema'])
                        # A correction must grade its own selected publication,
                        # including after repairing an invalid original role.
                        bad = deepcopy(verdict); bad['source_presentation_valid'] = None
                        with pytest.raises(ValidationError):
                            validate(bad, packet['review_response_schema'])
                    return text_response(json.dumps(verdict))
                return text_response(json.dumps({**fixture_source_assessment(packet,conclusion='limited',sufficient=False),
                    'disposition':'retry','source_presentation_valid':False,'unresolved_requirements':['Irrelevant selection'],
                    'all_material_requirements_enumerated':True,'material_requirements':[{
                        **r,'satisfied':False,'draft_covered':False,'evidence_invocation_ids':[proof]}
                        for r in packet['material_requirement_contract']]}))
            if 'present_source_answer' in names:
                self.presentations+=1;attempts.append(kw.get('timeout'))
                if mode=='no_selection' or self.presentations>1 and mode not in {'invalid_again','long_verified','role_then_long','reviewed_partial'}:
                    if mode=='response_error':raise RuntimeError('External model transport failed')
                    raise TimeoutError('External answer preparation timed out')
                packet=kw['messages'][0]['content'][0]['text']
                raw=packet.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                rows=json.loads(raw)['evidence'];first,second=rows[:2]
                if mode in {'long_verified','role_then_long','reviewed_partial'}:
                    role='entity' if mode=='role_then_long' and self.presentations==1 else 'attribute'
                    return tool_response('present_source_answer',{'intro':'','focus_roles':['attribute'],
                        'points':[{'semantic_role':role if index==0 else 'attribute','evidence_ids':[row['id']],
                            'title_ids':[row['excerpts'][0]['id']],
                            'detail_ids':[row['excerpts'][1]['id']] if index==0 else [],
                            'relevance':'Requested measurements'} for index,row in enumerate([first,second])],
                        'limitation_ids':[]})
                if mode=='semantic_rejection':
                    point={'semantic_role':'entity','evidence_ids':[rows[2]['id']],
                        'request_relation':'supporting_context','summary':'An unrelated record.',
                        'detail_ids':[],'relevance':'Irrelevant to the original request'}
                else:
                    point={'semantic_role':'entity','evidence_ids':[first['id'],second['id']],
                        'title_ids':[first['excerpts'][0]['id']],'detail_ids':[second['excerpts'][0]['id']],
                        'detail_groups':[[second['excerpts'][0]['id']]],'relevance':'Requested facts'}
                retained = [{'semantic_role':'attribute', 'request_relation':'result',
                    'summary':row['value'], 'evidence_ids':[row['id']], 'detail_ids':[],
                    'relevance':'Requested measurement'} for row in
                    ([first] if mode=='semantic_rejection' else [first,second])]
                return tool_response('present_source_answer',{'intro':'Unsupported draft introduction',
                    'answer_status':'partial_results', 'focus_roles':['entity','attribute'],
                    'points':retained+[point], 'limitation_ids':[]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if 'select_attachment_format' in names:return tool_response('select_attachment_format',{'extension':None,'confidence':1})
            if 'request_tool_scope' in names and 'opaque_capture' not in names:
                return tool_response('request_tool_scope',{'capabilities':['web'],'tool_names':['opaque_capture']})
            if not names:return text_response('{}')
            assert not reads,'Completed source work was repeated'
            return tool_response('opaque_capture',{})
    model=Model()
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as api:
        response=api.post('/api/chat',json={'text':'/chat '+prompt,'conversation_id':'web:answer-repair'})
        assert response.status_code==200,response.text
        data=response.json();reply=data['text']
        history=api.get('/api/chat/history/web%3Aanswer-repair')
        assert history.status_code==200
    network.close()
    assert len(reads)==1 and all(timeout==120 for timeout in attempts)
    assert len(attempts)==(1 if mode in {'long_verified','reviewed_partial'} else 2 if mode in {'no_selection','invalid_again','role_then_long'} else 3)
    assert 'response time limit' not in reply
    assert 'UNSELECTED INVENTORY' not in reply and 'Main Stage' not in reply
    if mode == 'no_selection':
        assert 'Aoi: 173 cm' not in reply and 'Ren: 183 cm' not in reply
    elif mode == 'semantic_rejection':
        assert 'Aoi: 173 cm' in reply,reply
    else:
        assert 'Aoi: 173 cm' in reply and 'Ren: 183 cm' in reply,reply
        assert 'Unsupported draft introduction' not in reply
        if mode in {'long_verified','role_then_long','reviewed_partial'}:
            assert 'https://first.example/person' in reply and 'https://second.example/person' in reply
            assert 'https://unrelated.example/page' not in reply
        if mode in {'invalid_again','long_verified','role_then_long','reviewed_partial'}:
            assert 'Source-backed findings collected so far' not in reply and 'Optional source catalog' not in reply, json.dumps([
                r for r in runtime.store.list_recent_conversation_events('web:answer-repair',limit=100)
                if r.get('event_type') in {'conversation.completion_review','conversation.completion_requirement_contract'}],ensure_ascii=False)
            assert 'Measurements' in reply
        else:assert 'Source-backed findings collected so far' not in reply
    if mode in {'timeout','no_selection','semantic_rejection'}:assert 'timed out' in reply
    turn=next(e for e in reversed(runtime.store.list_conversation_events('web:answer-repair')) if e.get('event_type')=='conversation.chat_turn')
    assert turn['assistant_reply']==reply
    assert len([r for r in turn['tool_results'] if r['tool_name']=='opaque_capture'])==1
    assert (turn.get('response_fulfilled') is True)==(mode in {'long_verified','role_then_long'})
    # The same retained partial findings need no second review without new
    # execution evidence. They stay partial; the assertions above separately
    # prevent that assistance from certifying a missing fact.
    assert len(reviews)==int(mode in {'semantic_rejection','invalid_again','long_verified','role_then_long','reviewed_partial'}), json.dumps([
        r for r in runtime.store.list_recent_conversation_events('web:answer-repair',limit=100)
        if r.get('event_type')=='conversation.completion_review'],ensure_ascii=False)
    wire=TelegramWire();install_wire(monkeypatch,wire)
    assert asyncio.run(_send_operator_telegram_delivery(TOKEN,'77',reply,message_only=True))
    assert len(wire.texts)==1 and 'response time limit' not in wire.texts[0]
    if mode in {'long_verified','role_then_long','reviewed_partial'}:
        assert 'href="https://first.example/person"' in wire.texts[0]
        assert 'href="https://second.example/person"' in wire.texts[0]
        assert 'https://unrelated.example/page' not in wire.texts[0]
    if mode not in {'no_selection','semantic_rejection'}:assert 'Aoi: 173 cm' in wire.texts[0] and 'Ren: 183 cm' in wire.texts[0]

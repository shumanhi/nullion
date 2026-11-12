"""Source meaning/relevance through the real chat route and durable runtime."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import copy
import json
import os
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E only')


def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
def tool(name,value):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':value}]}


@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('prompt',['What is happening here today?','¿Qué hay aquí hoy?','今日はここで何がある？'])
@pytest.mark.parametrize('bad_mode',[None,'control_promoted','irrelevant_control','duplicate_summary','irrelevant_limitation','always_rejected','name_field','partial_scope','empty_intro','noncontiguous_groups','incomplete_groups','reviewed_failure','multi_defect','image_reference','group_index','missing_semantics','repair_semantic_downgrade','attribute_context'])
def test_semantic_selection_repairs_without_restarting_browser(tmp_path,monkeypatch,prompt,surface,bad_mode):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_operator,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.plugins.browser_plugin import register_browser_tools,browser_use_worker
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.telegram_formatting import format_telegram_text
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_KEY_STORAGE','file');monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available',lambda:True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python',lambda:'/external/python')
    report={'result_kind':'source_observations','report_status':'partial','goal_status':'answered',
        'next_action':'report','completion_scope':'tool_step','blocker':None,'validation_errors':[],
        'observations':[
            {'label':'作品','semantic_role':'entity','value':'Dracula','quote':'Name\nDracula'},
            {'label':'日時','semantic_role':'attribute','value':'October 24, 2026 7:30PM','quote':'Date\nOctober 24, 2026 7:30PM'},
            {'label':'区画','semantic_role':'interface_control','value':'Main Stage','quote':'Select a section\nMain Stage\nNo Tickets Available'},
            {'label':'操作','semantic_role':'interface_control','value':'Let us choose seats for you','quote':'Let us choose seats for you\nPurchase Best Available Seating (including Standing Room Only)'},
        ],'unknowns':['Whether a purchase option yields reservable inventory was not tested.']}
    if bad_mode in {'partial_scope','reviewed_failure'}:
        report['unknowns']=['The exact showtime was not exposed in the captured source.']
        report['observations'][1].update(value='24',quote='Calendar day\n24')
    if bad_mode=='name_field':
        report['observations'][0].update(value='Dracula, 7:30PM',quote='Dracula, 7:30PM')
        report['observations'][1].update(value='Dracula',quote='Name\nDracula')
    if bad_mode in {'missing_semantics','repair_semantic_downgrade'}:
        # Legacy adapter observations have no declared roles; the advertised
        # answer schema must still be enforced by the runtime.
        for row in report['observations']: row.pop('semantic_role')
    document='\n'.join(row['quote'] for row in report['observations'])
    for row in report['observations']:
        row.update(source_url='https://venue.example/performance',captured_at=100,main_document_text=document)
    import base64
    image=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jL1sAAAAASUVORK5CYII=')
    source=tmp_path/'reference.png';source.write_bytes(image)
    input_attachments=[{'name':source.name,'path':str(source),'media_type':'image/png'}] if bad_mode=='image_reference' else []
    calls=[]
    def browser(*args,**kw):calls.append(1);return copy.deepcopy(report)
    monkeypatch.setattr(browser_use_worker,'run_isolated_browser_task',browser)
    class Provider:
        presentations=0; main_calls=0;reviews=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names={d['name'] for d in kw.get('tools',[])};system=str(kw.get('system') or '')
            if bad_mode=='image_reference' and (system.startswith(('You are a material-requirements planner','You are a semantic completion reviewer')) or 'present_source_answer' in names):
                media=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict) and b.get('type')=='image']
                assert len(media)==1 and base64.b64decode(media[0]['source']['data'])==image
                assert 'original user-provided source context' in system
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if 'relationship' in system:return text(json.dumps({'relationship':'independent','effect':'none','confidence':1,'target_index':None}))
            if system.startswith('You are a material-requirements planner'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet), 'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Identify today’s actual performance and time','requirement_kind':'fact',
                     'evidence_subject_id':'performance','eligible_evidence_tool_names':['browser_run_task']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1; payload=json.loads(kw['messages'][0]['content'][0]['text'])
                assert payload['source_documents'] and payload['source_answer_selection']
                if bad_mode=='group_index':assert payload['source_answer_selection']['points'][0]['detail_ids']==self.group_ids
                ids=[r['invocation_id'] for r in payload['tool_evidence'] if r['tool_name']=='browser_run_task' and r['status']=='completed']
                assert ids
                valid=bad_mode not in {'always_rejected','reviewed_failure'} and not(bad_mode in {'irrelevant_control','duplicate_summary','irrelevant_limitation'} and self.presentations==1)
                return text(json.dumps({**fixture_source_assessment(payload,
                    conclusion='limited' if bad_mode=='partial_scope' else 'positive', sufficient=bad_mode!='partial_scope'),
                    'disposition':'complete' if valid and bad_mode!='partial_scope' else 'retry','source_presentation_valid':valid,
                    'unresolved_requirements':['The requested showtime is unavailable'] if bad_mode=='partial_scope' else [] if valid else ['Present only the requested performance once; omit unrelated controls and purchase limitations'],
                    'retry_tool_names':[], 'all_material_requirements_enumerated':True,
                    'user_facing_failure':'The captured page lists Dracula, but it does not show a confirmed performance time.' if bad_mode=='reviewed_failure' else None,
                    'supported_findings':[{'text':'The captured page lists Dracula, but it does not show a confirmed performance time.',
                        'evidence_invocation_ids':ids[:1]}] if bad_mode=='reviewed_failure' else [],
                    'material_requirements':[{**r,'satisfied':bad_mode not in {'partial_scope','reviewed_failure'},'draft_covered':bad_mode not in {'partial_scope','reviewed_failure'},'evidence_invocation_ids':ids[:1]}
                        for r in payload['material_requirement_contract']]}))
            if not names and system:return text('{}')
            if 'present_source_answer' in names:
                self.presentations+=1
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                context=json.loads(raw); rows=context['evidence']
                if bad_mode in {'missing_semantics','repair_semantic_downgrade'}:
                    assert all(not r['semantic_bindings'] for r in rows)
                else:
                    assert [r['semantic_bindings'][0]['role'] for r in rows]==['entity','attribute','interface_control','interface_control']
                answer={'intro':'Dracula — October 24, 2026, 7:30 PM.', 'intro_evidence_ids':[rows[0]['id'],rows[1]['id']],
                    'focus_roles':['entity'],'summary_point_indices':[0], 'points':[
                        {'evidence_ids':[rows[0]['id'],rows[1]['id']], 'title_ids':[rows[0]['excerpts'][-1]['id']],
                         'detail_ids':[rows[1]['excerpts'][1]['id']], 'semantic_role':'entity','relevance':'The requested performance and its time'}],
                    'limitation_ids':[]}
                if bad_mode=='attribute_context':
                    answer.update(intro='',intro_evidence_ids=[],summary_point_indices=[],focus_roles=['entity','context'])
                    answer['points'][0].update(evidence_ids=[rows[0]['id']],detail_ids=[])
                    answer['points'].append({'evidence_ids':[rows[1]['id']],'detail_ids':[],'semantic_role':'context','relevance':'Requested timing context'})
                if bad_mode=='group_index':
                    self.group_ids=[row['id'] for row in rows[1]['excerpts'][:2]]
                    answer['points'][0]['detail_groups']=[self.group_ids]
                if bad_mode=='partial_scope':
                    answer.update(intro='Dracula is on the schedule.',limitation_ids=['u0'])
                if bad_mode=='empty_intro':
                    answer.update(intro='',intro_evidence_ids=[],summary_point_indices=[])
                if bad_mode=='name_field':
                    answer['points'][0].update(title_ids=[rows[1]['excerpts'][1]['id']],detail_ids=[rows[0]['excerpts'][0]['id']])
                if self.presentations==1:
                    if bad_mode in {'control_promoted','irrelevant_control','multi_defect'}:
                        role='entity' if bad_mode in {'control_promoted','multi_defect'} else 'interface_control'
                        answer['focus_roles'].append(role) if role not in answer['focus_roles'] else None
                        answer['points'].append({'evidence_ids':[rows[2]['id']],'detail_ids':[],'semantic_role':role,'relevance':'Proposed additional subject'})
                    if bad_mode=='multi_defect':answer['intro_evidence_ids']=[rows[0]['id']]
                    elif bad_mode=='duplicate_summary':answer['summary_point_indices']=[]
                    elif bad_mode=='irrelevant_limitation':answer['limitation_ids']=['u0']
                    elif bad_mode in {'noncontiguous_groups','incomplete_groups'}:
                        first,second=[row['id'] for row in rows[1]['excerpts'][:2]]
                        answer['points'][0].update(detail_ids=[first,second],
                            detail_groups=[[second,first]] if bad_mode=='noncontiguous_groups' else [[second]])
                else:
                    correction=kw['messages'][0]['content'][0]['text']
                    assert 'Correct the previous presentation' in correction
                    if bad_mode=='multi_defect':
                        assert 'missing_intro_evidence_ids' in correction and 'captured_title_roles' in correction
                        assert 'previous_selection' in correction and rows[2]['id'] in correction
                    if bad_mode=='noncontiguous_groups':assert 'noncontiguous_groups' in correction and rows[1]['excerpts'][0]['id'] in correction
                    if bad_mode=='incomplete_groups':assert 'missing_detail_ids' in correction and rows[1]['excerpts'][0]['id'] in correction
                if bad_mode=='repair_semantic_downgrade' and self.presentations==1:
                    from copy import deepcopy
                    answer['points'][0].update(request_relation='result', summary='October 24, 2026, 7:30 PM.')
                    answer['points'].append(deepcopy(answer['points'][0]))
                    answer['points'][0]['detail_ids']=['invalid-excerpt']
                if (bad_mode=='missing_semantics' and self.presentations==1
                        or bad_mode=='repair_semantic_downgrade' and self.presentations==2):
                    answer.pop('focus_roles')
                    for point in answer['points']: point.pop('semantic_role',None)
                return tool('present_source_answer',answer)
            self.main_calls+=1
            if self.main_calls==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],'required_tool_names':['browser_run_task']})
            if self.main_calls==2:return tool('browser_run_task',{'task':prompt,'url':'https://venue.example/performance'})
            raise AssertionError('Composition repair restarted the task or browser')
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    registry=ToolRegistry();register_browser_tools(registry,policy=BrowserPolicy(),model_client_getter=lambda:model)
    runtime.active_tool_registry=registry; orchestrator=AgentOrchestrator(model_client=model)
    if surface=='web':
        with TestClient(web_app.create_app(runtime,orchestrator=orchestrator,registry=registry)) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:semantic','turn_id':'semantic-test','attachments':input_attachments})
            assert response.status_code==200,response.text
            reply=response.json()['text'];conversation='web:semantic'
    else:
        settings=load_settings(env={'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'777','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
        reply=chat_operator._render_chat_turn(runtime,message='/chat '+prompt,chat_id='777',settings=settings,
            model_client=model,agent_orchestrator=orchestrator,append_activity_trace=False,attachments=input_attachments);conversation='telegram:777'
    if bad_mode=='partial_scope':
        assert reply.startswith('Dracula is on the schedule.') and reply.count('Dracula')==1,reply
        assert 'Calendar day' not in reply and 'Here are the findings' not in reply
        assert 'The exact showtime was not exposed' in reply and len(calls)==1 and model.presentations==1
        return
    if bad_mode=='reviewed_failure':
        assert reply.startswith('The captured page lists Dracula, but it does not show a confirmed performance time.'),reply
        assert 'Some requested information remains unconfirmed.' not in reply
        assert len(calls)==1 and model.presentations==2 and 'Remove' not in reply and 'because' not in reply
        return
    if bad_mode in {'always_rejected','repair_semantic_downgrade'}:
        assert len(calls)==1 and model.presentations==2 and 'Dracula' in reply and 'Main Stage' not in reply
        assert 'focused answer' not in reply,reply
        return
    assert reply.count('Dracula')==1 and '7:30PM' in reply.replace(' ',''),reply
    assert 'Main Stage' not in reply and 'choose seats' not in reply and 'Not verified' not in reply
    assert len(calls)==1 and model.presentations==(2 if bad_mode and bad_mode not in {'name_field','empty_intro','image_reference','group_index','attribute_context'} else 1)
    rendered,options=format_telegram_text(reply);assert '<a href=' in rendered and options['parse_mode']=='HTML'
    turns=[e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type')=='conversation.chat_turn']
    assert turns and len([r for r in turns[-1]['tool_results'] if r['tool_name']=='browser_run_task'])==1
    assert len(next(r['output'] for r in turns[-1]['tool_results'] if r['tool_name']=='browser_run_task')['observations'])==4
    if bad_mode == 'control_promoted':
        failures=[e for e in runtime.store.list_conversation_events(conversation)
                  if e.get('event_type')=='conversation.source_answer_validation']
        assert failures and failures[0]['valid'] is False and failures[0]['attempt']==1
        assert 'contradicts' in failures[0]['reason'] and failures[0]['selection']['points']

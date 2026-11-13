"""Real chat delivery retains separate records that share a display title."""
from tests.workflow_provider_protocol import workflow_provider_reply,fixture_source_plan,fixture_source_assessment
import json
import os
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real route and SQLite')

@pytest.mark.parametrize('prompt',['Compare these two departures.','Compara estas dos salidas.','この二つの出発便を比較してください。'])
def test_distinct_departures_with_one_carrier_are_delivered_once_each(tmp_path,monkeypatch,prompt):
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,OPERATOR_PERMISSION_PRINCIPAL
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from tests.test_source_record_identity import records,selection
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START,UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    source=records();calls=[]
    def capture(*args,**kwargs):calls.append(1);return source[0].output
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_available',lambda:True)
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_adapter.browser_use_python',lambda:'/external/python')
    monkeypatch.setattr('nullion.plugins.browser_plugin.browser_use_worker.run_isolated_browser_task',capture)
    class Provider:
        step=0;presentations=0;reviews=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
            def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':str(self.step),'name':name,'input':args}]}
            names={r['name'] for r in kw.get('tools',[])};system=str(kw.get('system') or '')
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if system.startswith('You are a material-requirements planner'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'),'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],'material_requirements':[
                    {'requirement':'Compare both captured departures','requirement_kind':'fact',
                     'evidence_subject_id':'departures','eligible_evidence_tool_names':['browser_run_task']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;payload=json.loads(kw['messages'][0]['content'][0]['text'])
                assert payload['source_answer_selection']['points'] and len(payload['source_answer_selection']['points'])==2
                ids=[r['invocation_id'] for r in payload['tool_evidence'] if r['tool_name']=='browser_run_task' and r['status']=='completed']
                assert ids
                return text(json.dumps({**fixture_source_assessment(payload,conclusion='limited'),'disposition':'complete','source_presentation_valid':True,
                    'unresolved_requirements':[],'all_material_requirements_enumerated':True,
                    'material_requirements':[{**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids}
                        for r in payload['material_requirement_contract']]}))
            if 'present_source_answer' in names:
                self.presentations+=1
                raw=kw['messages'][0]['content'][0]['text'].split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
                return tool('present_source_answer',selection(context=json.loads(raw)))
            if not names:return text('{}')
            self.step+=1
            if self.step==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_run_task'],
                'required_tool_names':['browser_run_task']})
            if self.step==2:return tool('browser_run_task',{'task':prompt,'url':'https://travel.example/results'})
            assert self.step==3
            return text('**American** — 7:14 AM–10:29 AM\n\n**American** — 9:05 AM–12:16 PM')
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture',OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    tools=ToolRegistry();register_browser_tools(tools,policy=BrowserPolicy(),model_client_getter=lambda:model)
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=tools)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:departures','turn_id':'departure-turn'})
        assert response.status_code==200,response.text
        data=response.json();reply=data['text']
        assert reply.count('**American**')==2 and all(time in reply for time in ['7:14 AM','9:05 AM','10:29 AM','12:16 PM']),reply
        assert not data['artifacts'] and model.presentations == 0 and (model.reviews == 0) and (len(calls) == 1)
        history=client.get('/api/chat/history/web:departures',params={'turn_ids':json.dumps(['departure-turn'])}).json()
        assert len(history['terminal_replies'])==1 and history['terminal_replies'][0]['text']==reply,history
    turns=[row for row in runtime.store.list_conversation_events('web:departures') if row.get('event_type')=='conversation.chat_turn']
    assert turns and len([row for row in turns[-1]['tool_results'] if row['tool_name']=='browser_run_task'])==1

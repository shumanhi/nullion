"""A real approval/resume keeps the selected uploaded reference for review."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import base64,json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real approval API/store')


@pytest.mark.parametrize('prompt',['Identify the requested subject from this reference','Identifica el tema de esta referencia','この資料の対象を確認してください'])
@pytest.mark.parametrize('surface',['web','telegram'])
@pytest.mark.parametrize('batch_count',[1,2])
def test_approved_source_read_preserves_reference_in_all_internal_decisions(tmp_path,monkeypatch,prompt,surface,batch_count):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,ToolResult
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'missing.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    image=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jL1sAAAAASUVORK5CYII=')
    path=tmp_path/'reference.png';path.write_bytes(image);reads=[];writes=[]
    def read(invocation):
        reads.append(invocation.arguments.get("index"))
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{
            'result_kind':'source_observations','report_status':'completed','goal_status':'answered','completion_scope':'tool_step',
            'next_action':'report','observations':[{'value':'Dracula','quote':'Dracula','semantic_role':'entity',
                'source_url':'https://reference.example/subject','captured_at':100}], 'unknowns':[],'blocker':None})
    registry=ToolRegistry();registry.register(ToolSpec(name='source_fixture',description='Read a reference subject',
        risk_level=ToolRiskLevel.HIGH,side_effect_class=ToolSideEffectClass.READ,requires_approval=True,
        timeout_seconds=5,capability_tags=('web',),input_schema={'type':'object','properties':{'index':{'type':'integer'}},'additionalProperties':False}),read)
    def write(invocation):
        writes.append(invocation.invocation_id)
        target=tmp_path/'owned-receipt.txt';target.write_text('one write')
        return ToolResult(invocation.invocation_id,invocation.tool_name,'completed',{'message':'Saved owned receipt'})
    registry.register(ToolSpec(name='local_write_fixture',description='Save an owned local receipt',
        risk_level=ToolRiskLevel.LOW,side_effect_class=ToolSideEffectClass.WRITE,requires_approval=False,
        timeout_seconds=5,capability_tags=('web',),input_schema={'type':'object','properties':{},'additionalProperties':False}),write)
    def text(v):return {'stop_reason':'end_turn','content':[{'type':'text','text':v}]}
    def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Provider:
        phase=0;reference_calls=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            system=str(kw.get('system') or '');names={t['name'] for t in kw.get('tools',[])}
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
            if system.startswith(('You are a material-requirements planner','You are a semantic completion reviewer')) or 'present_source_answer' in names:
                media=[b for m in kw['messages'] for b in m.get('content',[]) if isinstance(b,dict) and b.get('type')=='image']
                assert len(media)==1 and base64.b64decode(media[0]['source']['data'])==image
                self.reference_calls+=1
            if system.startswith('You are a material-requirements planner'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'), 'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Identify the requested reference subject','requirement_kind':'fact',
                     'evidence_subject_id':'subject','eligible_evidence_tool_names':['source_fixture']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                p=json.loads(kw['messages'][0]['content'][0]['text']);ids=[r['invocation_id'] for r in p['tool_evidence'] if r['tool_name']=='source_fixture' and r['status']=='completed'];assert ids
                return text(json.dumps({**fixture_source_assessment(p,conclusion='limited'), 'disposition':'complete','source_presentation_valid':True,'all_material_requirements_enumerated':True,
                    'unresolved_requirements':[],'material_requirements':[{**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids[:1]} for r in p['material_requirement_contract']]}))
            if 'present_source_answer' in names:
                spec=next(t for t in kw['tools'] if t['name']=='present_source_answer');identity=spec['input_schema']['properties']['points']['items']['properties']['evidence_ids']['items']['enum'][0]
                return tool('present_source_answer',{'intro':'','focus_roles':['entity'],'points':[{'evidence_ids':[identity],
                    'detail_ids':[],'semantic_role':'entity','relevance':'Requested subject'}],'limitation_ids':[]})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['source_fixture','local_write_fixture'],'required_tool_names':['source_fixture']})
            if self.phase==3:return text('Dracula is the requested performance.')
            assert self.phase==2
            return {'stop_reason':'tool_use','content':([{'type':'tool_use','id':'write-0','name':'local_write_fixture','input':{}}] if batch_count==2 else [])+[{'type':'tool_use','id':'source-0','name':'source_fixture','input':{'index':0}}]}
    from nullion.preferences import save_preferences
    save_preferences({'sentinel_mode':'risk_based'})
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt+' https://reference.example/subject',
            'conversation_id':'web:media-approval','attachments':[{'path':str(path),'name':path.name,'media_type':'image/png'}]})
        assert response.status_code==200,response.text
        pending=response.json();assert pending['type']=='approval_required',pending
        assert not reads and runtime.store.get_suspended_turn(pending['approval_id']) is not None
        session=client.get('/api/session').json()
        outputs=[]
        for index in range(1):
            choices=runtime.store.list_suspended_turns()
            assert len(choices)==1
            approval_id=choices[0].approval_id
            if surface=='web':
                approved=client.post('/api/approve/'+approval_id,json={'mode':'once'},headers={'x-nullion-csrf':session['csrf_token']})
                assert approved.status_code==200,approved.text
                result=approved.json();assert result['ok'],result
                outputs.append(result.get('resumed_text') or '')
            else:
                from nullion.approval_decisions import approve_request_with_mode
                from nullion.chat_operator import resume_approved_telegram_request
                approve_request_with_mode(runtime,approval_id,mode='once',source='test')
                outputs.append(resume_approved_telegram_request(runtime,approval_id=approval_id,chat_id='514132807',
                    agent_orchestrator=runtime.agent_orchestrator if hasattr(runtime,'agent_orchestrator') else AgentOrchestrator(model_client=model)) or '')
            assert reads==[0] and len(writes)==batch_count-1,(reads,writes,outputs)
        assert 'Dracula' in outputs[-1], outputs
        assert not runtime.store.list_suspended_turns()
        assert model.reference_calls == 0
        # Retapping an already consumed approval cannot repeat the read.
        if surface=='web':
            client.post('/api/approve/'+approval_id,json={'mode':'once'},headers={'x-nullion-csrf':session['csrf_token']})
        assert reads==[0] and len(writes)==batch_count-1

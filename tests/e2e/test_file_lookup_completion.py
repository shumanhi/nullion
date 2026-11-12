"""Real chat/persistence and filesystem boundary with a stale generation scope."""
import json
import os
import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('mode',['api_read','api_rejected','graph_rejected'])
@pytest.mark.parametrize('prompt,answer',[
    ('不存在.pdf があるか探して。作成や添付は不要です。','検索した場所には不存在.pdf はありませんでした。'),
    ('Busca inexistente.pdf sin crear ni adjuntar archivos.','No encontré inexistente.pdf en la carpeta consultada.'),
])
def test_rejected_unrequested_generation_does_not_block_scoped_no_match(tmp_path,monkeypatch,prompt,answer,mode):
    rejected_write=mode!="api_read"
    direct=mode=="graph_rejected"
    from fastapi.testclient import TestClient
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_search_handler
    executed=[]
    def text(value):return {'content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}
    def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Model:
        phase=0;reviews=0;plans=0
        drafts=[]
        def create(self,**kw):
            system=kw.get('system','')
            if 'Workflow action preflight.' in system:return text({'decision':'revise','issues':['生成は要求されていません。']})
            if 'material-requirements planner' in system:
                self.plans+=1
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[],
                    'material_requirements':[{'requirement':'指定名の検索結果','requirement_kind':'fact','evidence_subject_id':'lookup','eligible_evidence_tool_names':['file_search']}]})
            if 'semantic completion reviewer' in system:
                self.reviews+=1
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                self.drafts.append(packet.get('draft_final_response'))
                proof=next(r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']=='file_search')
                return text({'disposition':'complete','all_material_requirements_enumerated':True,'unresolved_requirements':[],'retry_tool_names':[],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[proof]} for r in packet['material_requirement_contract']]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if not kw.get("tools"):return text(answer)
            self.phase+=1
            if not direct and self.phase==1:return call('request_tool_scope',{'capabilities':['local_files','local_shell'] if rejected_write else ['local_files'],'tool_names':['file_search','terminal_exec'] if rejected_write else ['file_search'],
                'required_tool_names':['file_search','terminal_exec'] if rejected_write else ['file_search'],'artifact_extensions':[],'source_artifact_delivery_requested':False})
            if self.phase==(1 if direct else 2):return call('file_search',{'pattern':'不存在.pdf' if '不存在' in prompt else 'inexistente.pdf','root':str(tmp_path/'files')})
            if rejected_write and self.phase==(2 if direct else 3):return call('terminal_exec',{'command':'printf unrequested','network_mode':'deny'})
            return text(answer)
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    (tmp_path/'files').mkdir()
    registry=app.state.live_tracker_engine.registry
    registry.register(ToolSpec('file_search','Search filenames',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2),
        _build_file_search_handler(tmp_path/'files',include_principal_workspace=False))
    registry.register(ToolSpec('terminal_exec','Perform an external write',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,2),lambda inv:executed.append(inv))
    if direct:
        from nullion.agent_orchestrator import AgentOrchestrator
        registry.scheduled_source_scope={'required_tool_names':['file_search','terminal_exec']}
        result=AgentOrchestrator(model_client=model).run_turn(conversation_id='web:rejected-write',principal_id='operator',
            user_message=prompt,conversation_history=[],tool_registry=registry,
            policy_store=runtime.store,approval_store=runtime.store,max_iterations=8)
        assert result.response_fulfilled is True and result.final_text==answer,result
        rejected=[r for r in result.tool_results if r.tool_name=='terminal_exec']
        assert len(rejected)==1 and rejected[0].output['executed'] is False
        assert model.plans==1 and model.reviews==1 and model.phase==3 and not executed
    else:
        with TestClient(app) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:file-lookup'})
            assert response.status_code==200 and answer in response.json()['text'],response.text
            assert response.json()['artifacts']==[]
            turn=runtime.store.list_recent_conversation_events('web:file-lookup',event_type='conversation.chat_turn')[-1]
            assert turn['response_fulfilled'] is True,turn
            actions=[r for r in turn['tool_results'] if r['tool_name']=='terminal_exec']
            assert len(actions)==(1 if rejected_write else 0),actions
            assert not actions or actions[0]['output']['executed'] is False
            if model.reviews != 1:
                from pathlib import Path
                target = Path('.e2e-artifacts/scheduled-budget-20261108/lookup-review-events.json')
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps({'drafts':model.drafts,'events':runtime.store.list_conversation_events('web:file-lookup')}, default=str))
            assert model.plans==1 and model.reviews==1 and model.phase==(4 if rejected_write else 3) and not executed, {
                'plans': model.plans, 'reviews': model.reviews, 'phase': model.phase,
                'review_events': runtime.store.list_recent_conversation_events('web:file-lookup', event_type='conversation.completion_review')}


@pytest.mark.parametrize('prompt',['HTMLのレポートを一つ作ってください。','Crea un informe HTML.'])
def test_completed_native_fallback_retires_unexecuted_generator(tmp_path,monkeypatch,prompt):
    """The requested file, rather than a rejected method, owns completion."""
    from fastapi.testclient import TestClient
    from nullion.tools import ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_document_create_handler,_build_file_write_handler
    from nullion.artifacts import artifact_root_for_principal
    def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}
    def call(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Model:
        phase=0;reviews=0
        def create(self,**kw):
            system=kw.get('system','');names={r['name'] for r in kw.get('tools',[])}
            if 'Workflow action preflight.' in system:
                return text({'decision':'revise','issues':['Use the other permitted local producer for this requested presentation.']}
                    if self.phase==2 else {'decision':'proceed','issues':[]})
            if 'material-requirements planner' in system:
                return text({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.html'],
                    'material_requirements':[{'requirement':'Deliver the requested current HTML report','requirement_kind':'deliverable',
                        'evidence_subject_id':'html','eligible_evidence_tool_names':['file_write']}]})
            if 'semantic completion reviewer' in system:
                self.reviews+=1;packet=json.loads(kw['messages'][0]['content'][0]['text'])
                proof=next(r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']=='file_write' and r['status']=='completed')
                return text({'disposition':'complete','all_material_requirements_enumerated':True,'unresolved_requirements':[],'retry_tool_names':[],
                    'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':[proof]}
                        for r in packet['material_requirement_contract']]})
            protocol=workflow_provider_reply(kw,completion=False)
            if protocol is not None:return protocol
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['document_create','file_write'],
                'required_tool_names':['document_create','file_write'],'artifact_extensions':['.html']})
            if self.phase==2:return call('document_create',{'output_path':str(path),'title':'Current report'})
            if self.phase==3:return call('file_write',{'path':str(path),'content':body})
            assert self.phase==4,'A rejected generator was retried after the completed fallback'
            return text('The report is attached.\nMEDIA:'+str(path))
    model=Model();app,runtime,db=setup(tmp_path,monkeypatch,model)
    root=artifact_root_for_principal('web:native-fallback');root.mkdir(parents=True,exist_ok=True)
    path=root/'current.html';body='<!doctype html><html><body><h1>Current measurement: 37</h1><p>Requested current report.</p></body></html>'
    registry=app.state.live_tracker_engine.registry
    for name,handler in [('document_create',_build_document_create_handler(tmp_path,allowed_roots=[tmp_path,root])),
        ('file_write',_build_file_write_handler(tmp_path,allowed_roots=[tmp_path,root]))]:
        registry.register(ToolSpec(name,'Local report producer',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5),handler)
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:native-fallback'})
        assert response.status_code==200,response.text
        data=response.json();assert len(data['artifacts'])==1,data
        assert client.get(data['artifacts'][0]['url']).content==body.encode()
        turn=runtime.store.list_recent_conversation_events('web:native-fallback',event_type='conversation.chat_turn')[-1]
        assert turn['response_fulfilled'] is True,json.dumps({'turn':turn,'events':runtime.store.list_conversation_events('web:native-fallback')},ensure_ascii=False)
        rejected=[r for r in turn['tool_results'] if r['tool_name']=='document_create']
        assert len(rejected)==1 and rejected[0]['output']['executed'] is False
        assert model.phase==4 and model.reviews==1

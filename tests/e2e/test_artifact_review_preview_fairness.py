"""Every selected deliverable reaches content review, including late batch members."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container product boundary E2E')

@pytest.mark.parametrize('prompt',['Deliver these seven separate pages.','Entrega estas siete páginas por separado.','7つのページをそれぞれ別のファイルで渡してください。'])
def test_seven_deliverables_keep_per_file_review_and_download_bytes(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');root=artifact_root_for_principal('web:preview-budget');root.mkdir(parents=True,exist_ok=True)
    files=[root/f'page-{i}.html' for i in range(7)]
    bodies={str(path):f'<html><body><h1>Subject {i}</h1><p>'+('Substantive requested material. '*200)+'</p></body></html>' for i,path in enumerate(files)}
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);writes=[];writer=_build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    registry.register(ToolSpec('file_write','Write final page',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),write)
    class Provider:
        phase=0;reviews=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def tool(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[
                    {'requirement':'Deliver subject '+str(i),'requirement_kind':'deliverable','evidence_subject_id':str(i),'eligible_evidence_tool_names':['file_write']} for i in range(7)]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;data=json.loads(kw['messages'][0]['content'][0]['text']);evidence=data['artifact_evidence']
                assert len(evidence)==7 and all(row.get('content_preview') for row in evidence),[(r['filename'],len(r.get('content_preview',''))) for r in evidence]
                assert sum(len(r['content_preview']) for r in evidence)<=12000
                coverage=[]
                for row in data['material_requirement_contract']:
                    i=int(row['evidence_subject_id']);name=files[i].name
                    preview=next(r for r in evidence if r['filename']==name)
                    assert f'Subject {i}' in preview['content_preview']
                    matches=[r['invocation_id'] for r in data['tool_evidence'] if r.get('output',{}).get('artifact_record_binding',{}).get('filename')==name]
                    assert len(matches)==1
                    coverage.append({**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':matches})
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_artifact_delivery' in names:return tool('select_artifact_delivery',{'status':'selected','selected_ids':['a'+str(i) for i in range(7)],'reason':'Seven separate requested subjects, each with its own final page.'})
            if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return tool('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],'artifact_extensions':['.html']})
            if self.phase==2:return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'write'+str(i),'name':'file_write','input':{'path':str(p),'content':bodies[str(p)]}} for i,p in enumerate(files)]}
            assert self.phase==3,'Completed files were unnecessarily regenerated'
            return text('The seven pages are attached.\n'+'\n'.join('MEDIA:'+str(p) for p in files))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:preview-budget'});assert response.status_code==200,response.text
        assert not model.errors,model.errors
        data=response.json();assert {r['path'] for r in data['artifacts']}==set(bodies),data
        for row in data['artifacts']:assert client.get(row['url']).content==bodies[row['path']].encode()
        assert len(writes)==7 and model.reviews==0
    restored=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    turn=next(r for r in restored.store.list_conversation_events('web:preview-budget') if r.get('event_type')=='conversation.chat_turn')
    assert len([r for r in turn['tool_results'] if r['tool_name']=='file_write'])==7

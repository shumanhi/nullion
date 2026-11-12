"""Real API/history/native file search must not repeat a previous writer."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os,hashlib
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Native API/history/file boundary')

@pytest.mark.parametrize('language',['en','es','ja'])
def test_filename_lookup_after_generation_does_not_rewrite_or_attach(tmp_path,monkeypatch,language):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'home/runtime.db');cid='web:filename-only';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);path=root/'cedar.csv';registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);writes=[];searches=[]
    writer=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path]);searcher=tools._build_file_search_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    def search(inv):r=searcher(inv);searches.append(r);return r
    for name,effect,handler in [('file_write',tools.ToolSideEffectClass.WRITE,write),('file_search',tools.ToolSideEffectClass.READ,search)]:registry.register(tools.ToolSpec(name,name,tools.ToolRiskLevel.LOW,effect,False,5),handler)
    class Provider:
        follow=False;phase=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            names={r['name'] for r in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.follow)+str(self.phase),'name':n,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                return text(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':[] if self.follow else ['.csv'],'material_requirements':[{'requirement':'Return only the existing filename without writing or attaching' if self.follow else 'Create one CSV','requirement_kind':'fact' if self.follow else 'deliverable','evidence_subject_id':'file','eligible_evidence_tool_names':['file_search' if self.follow else 'file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                packet=json.loads(kw['messages'][0]['content'][0]['text']);ids=[r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']==('file_search' if self.follow else 'file_write') and r['status']=='completed'];assert ids
                return text(json.dumps({'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':[{**r,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids} for r in packet['material_requirement_contract']],'unresolved_requirements':[]}))
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':None if self.follow else '.csv','confidence':1,'output_kind':'inline' if self.follow else 'attachment','embedded_media_extensions':[]})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_search' if self.follow else 'file_write'],'required_tool_names':['file_search' if self.follow else 'file_write'],'artifact_extensions':[] if self.follow else ['.csv']})
            if self.phase==2:return call('file_search',{'pattern':'cedar.csv','root':str(root)}) if self.follow else call('file_write',{'path':str(path),'content':'Name,Value\nCedar,37\n'})
            assert self.phase==3,'Read-only follow-up inherited an old generation requirement'
            return text('cedar.csv' if self.follow else 'Ready.\nMEDIA:'+str(path))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        first=client.post('/api/chat',json={'text':'Create cedar.csv with Name,Value and Cedar,37.','conversation_id':cid});assert first.status_code==200 and len(first.json()['artifacts'])==1,first.text
        before=(hashlib.sha256(path.read_bytes()).hexdigest(),path.stat().st_mtime_ns)
        model.follow=True;model.phase=0
        prompt={'en':'Search for the CSV you created. Return its filename only, without writing, copying or attaching anything.','es':'Busca el CSV anterior y devuelve solo su nombre. No escribas, copies ni adjuntes archivos.','ja':'前に作成したCSVを検索してファイル名だけ返してください。書き込み、コピー、添付はしないでください。'}[language]
        second=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});data=second.json();assert not model.errors,model.errors
        assert second.status_code==200 and data['text'].strip()=='cedar.csv' and not data.get('artifacts'),data
        assert len(writes)==1 and len(searches)==1
        assert before==(hashlib.sha256(path.read_bytes()).hexdigest(),path.stat().st_mtime_ns)
        turns=[r for r in runtime.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn'];assert len(turns)==2
        assert any(r['tool_name']=='file_search' and r['status']=='completed' for r in turns[-1]['tool_results'])
        assert not any(r['tool_name']=='file_write' for r in turns[-1]['tool_results'])

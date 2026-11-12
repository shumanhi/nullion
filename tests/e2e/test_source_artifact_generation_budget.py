"""Research tools stay visible without shortening an artifact generation deadline."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os,time,statistics
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real API/store/download boundary')

@pytest.mark.parametrize('language',['en','es','ja'])
def test_mixed_source_artifact_budget_and_plain_turn_latency(tmp_path,monkeypatch,language):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.model_clients import ModelClientTimeoutError
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');monkeypatch.setenv('NULLION_AGENT_MODEL_TIMEOUT_SECONDS','45');monkeypatch.setenv('NULLION_ARTIFACT_MODEL_TIMEOUT_SECONDS','120');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);writes=[];reads=[];inspections=[]
    writer=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    def search(query,limit):reads.append(query);time.sleep(.03);return [{'url':'https://example.invalid/reference','title':'Current reference','snippet':'The current reference value is 37.'}]
    registry.register(tools.ToolSpec('file_write','Write final file',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,10),write)
    reader=tools._build_file_read_handler(tmp_path,allowed_roots=[tmp_path])
    def inspect(inv):r=reader(inv);inspections.append(r);return r
    registry.register(tools.ToolSpec('file_read','Inspect completed artifact',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.READ,False,10),inspect)
    monkeypatch.setenv('NULLION_PROVIDER_BINDINGS','search_plugin=brave_search_provider')
    monkeypatch.setenv('NULLION_BRAVE_SEARCH_API_KEY','local-fixture-not-a-real-key')
    monkeypatch.setattr('nullion.providers.resolve_plugin_provider_kwargs',lambda **kwargs:{'web_searcher':search})
    from nullion.preferences import save_preferences
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode
    from datetime import UTC,datetime
    save_preferences({'outbound_request_mode':'allow_all'})
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-search-consent',principal_id='global:operator',kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='https://www.bing.com/*',created_by='fixture',created_at=datetime.now(UTC)))
    class Provider:
        phase=0;plain=False;path=None;errors=[];deadlines=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except ModelClientTimeoutError:raise
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):return text(json.dumps({'all_material_requirements_enumerated':True,'material_requirements':[{'requirement':'Deliver the source-backed CSV','requirement_kind':'deliverable','evidence_subject_id':'csv','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                data=json.loads(kw['messages'][0]['content'][0]['text']);ids=[r['invocation_id'] for r in data['tool_evidence'] if r.get('output',{}).get('artifact_record_binding',{}).get('filename')==self.path.name]
                assert len(ids)==1
                return text(json.dumps({'disposition':'complete','unresolved_requirements':[],'all_material_requirements_enumerated':True,'material_requirements':[{'requirement_id':r['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids} for r in data['material_requirement_contract']]}))
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text('{}')
            if self.plain:
                self.phase+=1;assert kw['timeout']==45;assert self.phase==1
                return text('4')
            if self.phase==2:
                assert {'web_search','file_write'}.issubset(names),('Source or producer scope was prematurely narrowed',names,self.phase)
                self.deadlines.append(kw['timeout'])
                if kw['timeout']<90:raise ModelClientTimeoutError('Simulated generation exceeds the ordinary chat deadline')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['web_search','file_write','file_read'],'required_tool_names':['web_search','file_write','file_read'],'artifact_extensions':['.csv']})
            if self.phase==2:return call('web_search',{'query':'fixture reference'})
            if self.phase==3:return call('file_write',{'path':str(self.path),'content':'Value\n37\n'})
            if self.phase==4:return call('file_read',{'path':str(self.path)})
            assert self.phase==5,'The same source or producer was unnecessarily restarted'
            return text('Created.\nMEDIA:'+str(self.path))
    model=Provider();runtime.model_client=model;samples=[]
    prompts={'en':'Read the external reference, create a CSV of its value, and inspect it before attaching.','es':'Lee la referencia externa, crea un CSV de su valor e inspecciónalo antes de adjuntarlo.','ja':'外部参照を読み、その値のCSVを作成し、添付する前に内容を確認してください。'}
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        for index in range(3):
            cid='web:budget-'+str(index);root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);model.phase=0;model.plain=False;model.path=root/'values.csv';started=time.monotonic()
            response=client.post('/api/chat',json={'text':prompts[language],'conversation_id':cid});samples.append(time.monotonic()-started)
            data=response.json();assert not model.errors,model.errors
            assert response.status_code==200 and len(data['artifacts'])==1,data
            assert client.get(data['artifacts'][0]['url']).content==b'Value\n37\n'
            assert model.phase==5
            model.phase=0;model.plain=True;started=time.monotonic()
            response=client.post('/api/chat',json={'text':{'en':'What is two plus two?','es':'¿Cuánto es dos más dos?','ja':'2足す2はいくつ？'}[language],'conversation_id':cid+'-plain'});samples.append(time.monotonic()-started)
            assert response.status_code==200 and response.json()['text']=='4' and model.phase==1,response.text
        assert len(reads)==len(writes)==len(inspections)==3 and model.deadlines==[120]*3
    assert statistics.mean(samples)<1.5 and max(samples)<3,samples

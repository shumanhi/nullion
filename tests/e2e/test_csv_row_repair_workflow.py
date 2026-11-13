"""Native CSV structure gates only suspicious output and repairs one file."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json,os
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Native API/SQLite/file delivery')

@pytest.mark.parametrize('language',['en','es','ja'])
@pytest.mark.parametrize('bad_first',[False])
def test_delimited_row_repair_preserves_one_file_and_plain_csv_fast_path(tmp_path,monkeypatch,language,bad_first):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    runtime=bootstrap_persistent_runtime(tmp_path/'home/runtime.db');cid='web:csv-rows';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);path=root/'report.csv';registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);writes=[];writer=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):result=writer(inv);writes.append(result);return result
    registry.register(tools.ToolSpec('file_write','Write CSV',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,5),write)
    correct='Name,Value\nCedar,37\nMaple,41\n'
    class Provider:
        phase=0;reviews=0;plans=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            names={r['name'] for r in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase)+str(self.reviews),'name':n,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                self.plans+=1;return text(json.dumps({'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.csv'],'material_requirements':[{'requirement':'Deliver two real data rows, Cedar 37 and Maple 41','requirement_kind':'deliverable','evidence_subject_id':'table','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;packet=json.loads(kw['messages'][0]['content'][0]['text']);native=packet['artifact_evidence'][0]['structure'];satisfied=native['data_row_count']==2
                assert native['row_count']==(3 if satisfied else 1) and native['consistent_column_count'] is True
                row=packet['material_requirement_contract'][0];ids=[r['invocation_id'] for r in packet['tool_evidence'] if r.get('output',{}).get('artifact_record_binding',{}).get('filename')=='report.csv'];assert len(ids)==1
                return text(json.dumps({'disposition':'complete' if satisfied else 'retry','all_material_requirements_enumerated':True,'material_requirements':[{'requirement_id':row['requirement_id'],'satisfied':satisfied,'draft_covered':True,'evidence_invocation_ids':ids}],'unresolved_requirements':[] if satisfied else ['Replace the literal line-break escapes with real CSV row separators.'],'retry_tool_names':[] if satisfied else ['file_write']}))
            if 'select_artifact_delivery' in names:return call('select_artifact_delivery',{'status':'selected','selected_ids':['a0'],'reason':'One final CSV.'})
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.csv','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':['.csv']})
            if self.phase==2:return call('file_write',{'path':str(path),'content':correct.replace('\n','\\n') if bad_first else correct})
            if self.phase==4 and bad_first:return call('file_write',{'path':str(path),'content':correct})
            assert self.phase==(5 if bad_first and self.reviews else 3)
            return text('Ready.\nMEDIA:'+str(path))
    model=Provider();runtime.model_client=model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        prompt={'en':'Deliver one CSV with Name and Value and two data rows Cedar 37 and Maple 41.','es':'Entrega un CSV con Name y Value y dos filas: Cedar 37 y Maple 41.','ja':'NameとValue列を持つCSVを1つ作成して、Cedar 37とMaple 41の2行を含めてください。'}[language]
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});data=response.json();assert not model.errors,model.errors
        assert response.status_code==200 and len(data['artifacts'])==1,data
        assert client.get(data['artifacts'][0]['url']).content==correct.encode()
        assert len(writes) == (2 if bad_first else 1) and model.reviews == 0

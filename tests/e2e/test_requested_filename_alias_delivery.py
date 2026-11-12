"""The actual API attaches the requested name after an outside-root write."""
from tests.workflow_provider_protocol import workflow_provider_reply
import os,json
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary')

@pytest.mark.parametrize('language',['en','es','ja'])
@pytest.mark.parametrize('extension,content',[('.csv','Name,Value\nCedar,37\n'),('.html','<!doctype html><html><body><h1>Cedar 37</h1><p>Verified final content.</p></body></html>'),('.svg','<svg xmlns="http://www.w3.org/2000/svg" width="240" height="100"><text x="10" y="30">Cedar 37</text></svg>')])
def test_requested_name_survives_materialization_and_download(tmp_path,monkeypatch,language,extension,content):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass,_build_file_write_handler
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');chat_store._store=None
    from nullion.artifacts import nullion_data_home
    filename='cedar_values'+extension;source=nullion_data_home()/('working_values'+extension);writes=[]
    prompt={'en':'Create and attach '+filename+' with Cedar 37.','es':'Crea y adjunta '+filename+' con Cedar 37.','ja':'Cedar 37 の '+filename+' を作成して添付してください。'}[language]
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);writer=_build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
    def write(inv):r=writer(inv);writes.append(r);return r
    registry.register(ToolSpec('file_write','Write file',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,10),write)
    class Provider:
        phase=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def _create(self,**kw):
            names={t['name'] for t in kw.get('tools',[])}
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':extension,'confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_write'],'required_tool_names':['file_write'],'artifact_extensions':[extension]})
            if self.phase==2:return call('file_write',{'path':str(source),'content':content})
            assert self.phase==3,'A naming-only change must not trigger regeneration'
            return text('Created.\nMEDIA:'+str(writes[0].output['artifact_path']))
    model=Provider();runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');runtime.model_client=model
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from datetime import datetime,UTC
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('fixture-write',GLOBAL_PERMISSION_PRINCIPAL,BoundaryKind.FILESYSTEM_ACCESS,PolicyMode.ALLOW,str(tmp_path)+'/*','fixture-user',datetime.now(UTC)))
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response=client.post('/api/chat',json={'text':prompt,'conversation_id':'web:named-artifact'});assert response.status_code==200,response.text
        data=response.json();assert not model.errors,model.errors
        assert len(data['artifacts'])==1,data
        artifact=data['artifacts'][0];assert artifact['name']==filename,data
        assert client.get(artifact['url']).content==content.encode()
        assert len(writes)==1 and writes[0].output['artifact_path']!=artifact['path']
    reopened=bootstrap_persistent_runtime(tmp_path/'runtime.db');turns=[r for r in reopened.store.list_conversation_events('web:named-artifact') if r.get('event_type')=='conversation.chat_turn']
    assert any(r.get('output',{}).get('requested_filename_alias') for r in turns[-1]['tool_results'])
    assert turns[-1]['artifact_paths']==[artifact['path']]

"""API -> native browser -> same HTML repair -> download/history, with bounded recovery."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import json,os,socket,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Container native browser/API acceptance')

@pytest.mark.parametrize('prompt',['Research the rail-accessible trip and create one HTML report.','Investiga la excursión accesible en tren y crea un informe HTML.','電車で行ける旅行を調査してHTMLレポートを1つ作ってください。'])
def test_repair_window_finishes_source_read_and_updates_same_artifact(tmp_path,monkeypatch,prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.plugins.browser_plugin import register_browser_tools
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers()
            self.wfile.write(b'<html><h1>Cedar Preserve</h1><p>Take Route 47 train, then walk 12 minutes from Cedar Station.</p></html>')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Page);threading.Thread(target=server.serve_forever,daemon=True).start()
    url=f'http://cedar.localhost:{server.server_port}/';dns=socket.getaddrinfo
    monkeypatch.setattr(socket,'getaddrinfo',lambda host,*a,**k:dns('127.0.0.1' if host=='cedar.localhost' else host,*a,**k))
    for k,v in {'HOME':str(tmp_path),'NULLION_HOME':str(tmp_path/'home'),'NULLION_ENV_FILE':str(tmp_path/'none.env'),'NULLION_MEMORY_ENABLED':'0','NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_BROWSER_BACKEND':'playwright','NULLION_BROWSER_HEADLESS':'true'}.items():monkeypatch.setenv(k,v)
    chat_store._store=None;runtime=bootstrap_persistent_runtime(tmp_path/'home/runtime.db');cid='web:repair-budget';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);path=root/'cedar.html'
    registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);register_browser_tools(registry)
    handler=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path]);writes=[]
    def write(inv):r=handler(inv);writes.append(r);return r
    registry.register(tools.ToolSpec('file_write','Write file',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,5),write)
    class Provider:
        phase=0;reviews=0;errors=[];checkpoints=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self.run(**kw)
            except Exception as exc:self.errors.append(repr(exc));raise
        def run(self,**kw):
            names={r['name'] for r in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
            def call(n,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':f'{n}-{self.phase}','name':n,'input':args}]}
            if system.startswith('You are a material-requirements planner'):
                packet=json.loads(kw['messages'][0]['content'][0]['text'])
                return text(json.dumps({**fixture_source_plan(packet,scope='evidence_summary'),'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.html'],'material_requirements':[{'requirement':'Verify station access and create one HTML report','requirement_kind':'deliverable','evidence_subject_id':'cedar','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;packet=json.loads(kw['messages'][0]['content'][0]['text']);done=len(writes)==2
                assert all(len(row['invocation_id'])<=40 for row in packet['tool_evidence']), 'Long runtime ids leaked into the reviewer reference protocol'
                ids=[r['invocation_id'] for r in packet['tool_evidence'] if r['tool_name']=='file_write' and r['status']=='completed']
                return text(json.dumps({**fixture_source_assessment(packet,conclusion='limited',sufficient=done),'disposition':'complete' if done else 'retry','all_material_requirements_enumerated':True,'material_requirements':[{**r,'satisfied':done,'draft_covered':done,'evidence_invocation_ids':ids if done else []} for r in packet['material_requirement_contract']],'unresolved_requirements':[] if done else ['Read the opened page to verify station access and update the existing report.'],'retry_tool_names':[] if done else ['browser_extract_text','file_write'],'user_facing_failure':'' if done else 'Station access has not yet been checked.'}))
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.html','confidence':1,'output_kind':'attachment','embedded_media_extensions':[]})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['browser_navigate','browser_extract_text','file_write'],'required_tool_names':['browser_navigate','file_write'],'artifact_extensions':['.html']})
            if self.phase==2:return call('browser_navigate',{'url':url})
            if self.phase==3:return call('file_write',{'path':str(path),'content':'<html><body>Station access not checked yet.</body></html>'})
            if self.phase==4:return text('Draft.\nMEDIA:'+str(path))
            messages=kw['messages'];checkpoint_text=messages[-1]['content'][0]['text']
            assert 'Runtime repair checkpoint:' in checkpoint_text,'Repair window was hidden from the executing model'
            assert 'remaining_model_calls' in checkpoint_text and 'existing_artifact_paths' in checkpoint_text
            assert str(path) in checkpoint_text and 'browser_sessions' in checkpoint_text
            self.checkpoints.append(checkpoint_text)
            if self.phase==5:return call('browser_extract_text',{})
            if self.phase==6:
                assert 'walk 12 minutes' in json.dumps(messages)
                return call('file_write',{'path':str(path),'content':f'<html><body>Cedar Preserve: Route 47 train, then walk 12 minutes from Cedar Station. <a href="{url}">Source</a></body></html>'})
            assert self.phase==7,'Repeated completed research instead of finishing'
            return text('Ready.\nMEDIA:'+str(path))
    model=Provider();runtime.model_client=model;start=time.monotonic()
    try:
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});data=response.json()
            assert not model.errors,model.errors
            assert response.status_code==200 and len(data.get('artifacts',[]))==1,data
            assert len(writes)==2 and model.reviews==3 and len(model.checkpoints)==3
            assert 'walk 12 minutes' in path.read_text()
            artifact=data['artifacts'][0];download=client.get(artifact.get('url') or artifact.get('download_url'));assert download.status_code==200 and b'walk 12 minutes' in download.content
            turns=[r for r in runtime.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn'];assert len(turns)==1
            assert sum(r['tool_name']=='browser_navigate' for r in turns[0]['tool_results'])==1
            assert sum(r['tool_name']=='browser_extract_text' for r in turns[0]['tool_results'])==1
            assert time.monotonic()-start<15
    finally:
        server.shutdown();backend=registry._handlers['browser_navigate'].__self__._backend
        close=getattr(backend,'close',None)
        if close:
            import asyncio
            result=close()
            if hasattr(result,'__await__'):asyncio.run(result)

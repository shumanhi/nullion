"""Real DOM, image receipts, terminal render, chat/store and attachment bytes."""
from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
import base64,io,json,os,random,shlex,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker product boundary E2E')

@pytest.fixture(scope='module')
def review_latency_samples():
    samples=[]
    yield samples
    if samples:
        assert sum(samples)/len(samples)<8,samples
        assert max(samples)<12,samples


@pytest.mark.parametrize('reader_name',['browser_extract_detail','browser_extract_text'])
@pytest.mark.parametrize('prompt',['Research this exhibit and attach an illustrated HTML report.','Investiga esta exposición y adjunta un informe HTML ilustrado.','この展示を調べて、画像付きHTMLレポートを添付してください。'])
def test_native_records_survive_review_and_helper_render_does_not_change_request(tmp_path,monkeypatch,prompt,review_latency_samples,record_property,reader_name):
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry,ToolSpec,ToolRiskLevel,ToolSideEffectClass
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'))
    for k,v in {'NULLION_MEMORY_ENABLED':'0','NULLION_TASK_PLANNER_FEED_MODE':'off','NULLION_BROWSER_BACKEND':'playwright','NULLION_BROWSER_HEADLESS':'true','NULLION_BROWSER_BLOCK_PRIVATE':'false'}.items():monkeypatch.setenv(k,v)
    chat_store._store=None
    image=Image.frombytes('RGB',(300,300),random.Random(88).randbytes(300*300*3));buffer=io.BytesIO();image.save(buffer,'JPEG');body=buffer.getvalue();image_url='https://example.org/exhibit.jpg'
    monkeypatch.setattr(tools,'_fetch_browser_image_binary',lambda url,timeout_seconds:{'source_url':url,'final_url':url,'status_code':200,'content_type':'image/jpeg','data':body})
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/asset.jpg':
                self.send_response(200);self.send_header('Content-Type','image/jpeg');self.end_headers();self.wfile.write(body);return
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(('<html><body><main><h1>任意展示</h1><p>Current exhibit, October 1 through November 30, 2026. Admission 37.</p><img src="'+'data:image/jpeg;base64,'+base64.b64encode(body).decode()+'" alt="Official exhibit image"><a href="'+image_url+'">Image source</a></main></body></html>').encode())
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Page);threading.Thread(target=server.serve_forever,daemon=True).start();url=f'http://localhost:{server.server_port}/exhibit'
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:native-records';root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);report=root/'report.html';render=root/'support.jpg'
    from nullion.policy import BoundaryPolicyRule,BoundaryKind,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from datetime import datetime,UTC
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id='fixture-source',principal_id=GLOBAL_PERMISSION_PRINCIPAL,kind=BoundaryKind.OUTBOUND_NETWORK,mode=PolicyMode.ALLOW,selector='example.org',created_by='fixture-user',created_at=datetime.now(UTC)))
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]);policy=BrowserPolicy();policy.block_private=False;register_browser_tools(registry,policy=policy,model_client_getter=lambda:model)
    collected=[];collector=tools._build_browser_image_collect_handler()
    def collect(inv):
        result=collector(inv);collected.append(result);return result
    for name,effect,fn in [('browser_image_collect',ToolSideEffectClass.WRITE,collect),('file_write',ToolSideEffectClass.WRITE,tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path])),('file_read',ToolSideEffectClass.READ,tools._build_file_read_handler(tmp_path,allowed_roots=[tmp_path])),('terminal_exec',ToolSideEffectClass.DANGEROUS_EXEC,tools._build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))]:
        registry.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,30),fn)
    html='<html><body><h1>任意展示</h1><p>Admission 37. October 1–November 30, 2026.</p><svg width="20" height="20"><circle cx="10" cy="10" r="8"/></svg></body></html>'
    class Model:
        phase=0;plans=0;reviews=0;errors=[]
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            try:return self._create(**kw)
            except Exception as e:self.errors.append(repr(e));raise
        def _create(self,**kw):
            names={x['name'] for x in kw.get('tools',[])};system=str(kw.get('system') or '')
            def text(t):return {'stop_reason':'end_turn','content':[{'type':'text','text':t}]}
            def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
            if system.startswith('You are a material-requirements planner'):
                time.sleep(.3)
                self.plans+=1;assert self.plans==1,'An inspection/tool discovery changed the requested outcomes'
                payload=json.loads(kw['messages'][0]['content'][0]['text']);assert 'artifact_delivery_contract' not in payload,'Support render metadata can bias the independent request plan'
                return text(json.dumps({**fixture_source_plan(payload, scope='evidence_summary'), 'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.html'],'material_requirements':[{'requirement':'Verify exhibit date, admission and source image','requirement_kind':'fact','evidence_subject_id':'exhibit','eligible_evidence_tool_names':[reader_name,'browser_image_collect']},{'requirement':'Deliver the illustrated HTML report','requirement_kind':'deliverable','evidence_subject_id':'report','eligible_evidence_tool_names':['file_write']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews+=1;payload=json.loads(kw['messages'][0]['content'][0]['text']);records=[r for r in payload['tool_evidence'] if r.get('output',{}).get('source_record_binding',{}).get('record_kind')=='structured_source_record']
                detail=[r for r in records if r['tool_name']==reader_name];images=[r for r in records if r['tool_name']=='browser_image_collect']
                assert len(detail)==len(images)==1,'Native detail or source image was lost from bound review evidence'
                assert 'Admission 37' in detail[0]['output']['record']['details' if reader_name=='browser_extract_detail' else 'text'] and images[0]['output']['record']['source_url']==image_url
                assert set(payload['artifact_candidate_filenames'])=={'report.html'},payload['artifact_candidate_filenames']
                if self.reviews==1:return text(json.dumps({**fixture_source_assessment(payload, conclusion='limited'), 'disposition':'retry','retry_tool_names':['file_read'],'unresolved_requirements':['Inspect the prepared HTML content before final delivery.']}))
                assert self.reviews==2
                bound=[r for r in payload['tool_evidence'] if r.get('output',{}).get('artifact_record_binding')]
                artifacts=[r for r in bound if r['output']['artifact_record_binding'].get('delivery_role')!='internal_not_selected']
                internal=[r for r in bound if r['output']['artifact_record_binding'].get('delivery_role')=='internal_not_selected']
                assert len(artifacts)==1 and artifacts[0]['output']['artifact_record_binding']['filename']=='report.html'
                assert len(internal)==1 and internal[0]['output']['artifact_record_binding']['filename']=='support.jpg'
                coverage=[]
                for row in payload['material_requirement_contract']:
                    ids=[detail[0]['invocation_id'],images[0]['invocation_id']] if row['requirement_kind']=='fact' else [artifacts[0]['invocation_id']]
                    coverage.append({**row,'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids})
                return text(json.dumps({**fixture_source_assessment(payload, conclusion='limited'), 'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.html','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['browser_navigate',reader_name,'browser_image_collect','file_write','terminal_exec'],'required_tool_names':['browser_navigate',reader_name,'browser_image_collect','file_write'],'artifact_extensions':['.html','.jpg'],'embedded_media_artifact_extensions':['.html']})
            if self.phase==2:
                assert 'browser_run_task' not in names,'A broad source scope exposed the optional full browser worker'
                return call('browser_navigate',{'url':url})
            if self.phase==3:return call(reader_name,{})
            if self.phase==4:return call('browser_image_collect',{'image_urls':[image_url]})
            if self.phase==5:
                assert collected[0].status=='completed',collected[0].error
                illustrated=html.replace('<svg', '<img src="'+collected[0].output['image_paths'][0]+'"><svg')
                return call('file_write',{'path':str(report),'content':illustrated})
            if self.phase==6:
                script='from PIL import Image; Image.new("RGB",(50,50),"blue").save('+repr(str(render))+')'
                return call('terminal_exec',{'command':'python -c '+shlex.quote(script),'timeout_seconds':10,'network_mode':'deny'})
            if self.phase==7:return text('The requested report is ready.\nMEDIA:'+str(report))
            if self.phase==8:return call('request_tool_scope',{'capabilities':['local_files'],'tool_names':['file_read'],'required_tool_names':['file_read']})
            if self.phase==9:return call('file_read',{'path':str(report)})
            assert self.phase==10,'Completed work restarted'
            return text('The requested report is ready.\nMEDIA:'+str(report))
    model=Model();runtime.model_client=model
    try:
        with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
            started=time.monotonic()
            r=client.post('/api/chat',json={'text':prompt+' '+url,'conversation_id':cid});assert r.status_code==200,r.text;data=r.json()
            elapsed=time.monotonic()-started;review_latency_samples.append(elapsed);record_property('elapsed_ms',round(elapsed*1000,2))
            assert not model.errors,model.errors
            assert [a['path'] for a in data['artifacts']]==[str(report)],data
            downloaded=client.get(data['artifacts'][0]['url']).content
            assert downloaded==report.read_bytes() and b'Admission 37' in downloaded and b'data:image/jpeg;base64,' in downloaded
            assert render.exists() and model.plans==1 and model.reviews==2 and model.phase==10
        reopened=bootstrap_persistent_runtime(tmp_path/'runtime.db');events=reopened.store.list_conversation_events(cid)
        reviews=[r for r in events if r.get('event_type')=='conversation.completion_review'];assert reviews[-1]['disposition']=='complete' and reviews[-1].get('review_error') is None,reviews
        assert len([r for r in events if r.get('event_type')=='conversation.completion_requirement_contract' and r.get('status')=='completed'])==1
    finally:
        registry.run_cleanup_hooks();server.shutdown();server.server_close()

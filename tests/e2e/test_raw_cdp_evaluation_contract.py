"""The raw CDP fallback executes functions and retains real-page source bindings."""
from tests.workflow_provider_protocol import workflow_provider_reply
import asyncio,json,os,socket,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from types import SimpleNamespace
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker native Chromium boundary')


def test_raw_cdp_matches_real_playwright_function_expression_and_argument_results():
    from playwright.async_api import async_playwright
    from nullion.plugins.browser_plugin.backends import cdp_backend as cdp
    async def run():
        with socket.socket() as reserve:reserve.bind(('127.0.0.1',0));port=reserve.getsockname()[1]
        async with async_playwright() as pw:
            browser=await pw.chromium.launch(headless=True,args=['--remote-debugging-port='+str(port)])
            page=await browser.new_page();await page.goto('data:text/html,<meta http-equiv="Content-Security-Policy" content="script-src none"><input id="value" value="37">')
            targets=await asyncio.to_thread(cdp._cdp_targets,'http://127.0.0.1:'+str(port));target=next(t for t in targets if t.get('type')=='page' and t.get('url')==page.url)
            raw=cdp._RawCDPPage(target,cdp_url='http://127.0.0.1:'+str(port));samples=[]
            cases=[('() => location.href',None),('function(value) { return {label:value.label, n:value.n+1}; }',{'label':'情報・información','n':37}),('async value => { await Promise.resolve(); return value.n+1; }',{'n':37}),('({n:37,label:"información"})',None),('selector => document.querySelector(selector).value','#value'),('(arg=7) => arg',None),('window.nativeCounter += 1; window.nativeCounter',None),('(() => {window.nativeCounter += 1; return () => window.nativeCounter;})()',None)]
            try:
                for script,arg in cases:
                    await page.evaluate('window.nativeCounter=0');expected=await page.evaluate(script,arg)
                    await page.evaluate('window.nativeCounter=0');started=time.monotonic();actual=await raw.evaluate(script,arg);samples.append(time.monotonic()-started)
                    assert actual==expected,(script,actual,expected)
                    if 'nativeCounter' in script:assert await page.evaluate('window.nativeCounter')==1
                assert await raw.evaluate('function f(){return 37;}; f()')==37
                await page.evaluate('window.nativeCounter=0')
                with pytest.raises(RuntimeError,match='expected runtime error'):
                    await raw.evaluate('() => {window.nativeCounter += 1; throw new SyntaxError("expected runtime error");}')
                assert await page.evaluate('window.nativeCounter')==1
                with pytest.raises(RuntimeError,match='37'):
                    await raw.evaluate('async () => {throw 37;}')
                assert sum(samples)/len(samples)<.5 and max(samples)<2,samples
            finally:await raw.close();assert not page.is_closed();await browser.close()
    asyncio.run(run())


@pytest.mark.parametrize('language',['en','es','ja'])
@pytest.mark.parametrize('discovery_only',[False,True])
def test_real_api_raw_browser_captures_three_bound_sources_and_delivers_once(tmp_path,monkeypatch,language,discovery_only):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion import web_app,chat_store,tools
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import cdp_backend as cdp
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'));monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0');monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');monkeypatch.setenv('NULLION_BROWSER_BACKEND','cdp');chat_store._store=None
    values=[37,41,53]
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            try:index=int(self.path.strip('/') or '0')
            except ValueError:self.send_error(404);return
            if index not in range(3):self.send_error(404);return
            body=f'<html><body><h1>Record {index}</h1><p>Current value: {values[index]}.</p><p>Current as of 2026-10-25.</p></body></html>'.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);threading.Thread(target=server.serve_forever,daemon=True).start();urls=['http://localhost:'+str(server.server_port)+'/'+str(i) for i in range(3)]
    with socket.socket() as reserve:reserve.bind(('127.0.0.1',0));port=reserve.getsockname()[1]
    class Driver:
        async def stop(self):pass
        async def connect(self,url):raise TimeoutError('Exercise native CDP fallback')
        @property
        def chromium(self):return SimpleNamespace(connect_over_cdp=self.connect)
    class Manager:
        async def __aenter__(self):return Driver()
    monkeypatch.setattr(cdp,'async_playwright',Manager)
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True,args=['--remote-debugging-port='+str(port)]);monkeypatch.setenv('NULLION_BROWSER_CDP_URL','http://127.0.0.1:'+str(port));policy=BrowserPolicy();policy.block_private=False
        registry=tools.ToolRegistry(filesystem_allowed_roots=[tmp_path]);register_browser_tools(registry,policy=policy);runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');cid='web:raw-source:'+language;root=artifact_root_for_principal(cid);root.mkdir(parents=True,exist_ok=True);path=root/'sourced.txt';writes=[];writer=tools._build_file_write_handler(tmp_path,allowed_roots=[tmp_path])
        def write(inv):result=writer(inv);writes.append(result);return result
        registry.register(tools.ToolSpec('file_write','Write final report',tools.ToolRiskLevel.LOW,tools.ToolSideEffectClass.WRITE,False,10),write)
        class Provider:
            phase=0;reviews=0;errors=[]
            def create(self,**kw):
                protocol_reply = workflow_provider_reply(kw, completion=False)
                if protocol_reply is not None: return protocol_reply
                try:return self.run(**kw)
                except Exception as e:self.errors.append(repr(e));raise
            def run(self,**kw):
                from tests.workflow_provider_protocol import fixture_source_plan, fixture_source_assessment
                names={t['name'] for t in kw.get('tools',[])};system=str(kw.get('system') or '')
                def text(s):return {'stop_reason':'end_turn','content':[{'type':'text','text':s}]}
                def call(n,a):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':n+str(self.phase),'name':n,'input':a}]}
                if system.startswith('You are a material-requirements planner'):
                    packet=json.loads(kw['messages'][0]['content'][0]['text'])
                    return text(json.dumps({**fixture_source_plan(packet,scope='restricted'),
                        'all_material_requirements_enumerated':True,'requested_artifact_extensions':['.txt'],'material_requirements':[*[{'requirement':'Verify current source '+str(i),'requirement_kind':'fact','evidence_subject_id':'source'+str(i),'eligible_evidence_tool_names':['browser_navigate' if discovery_only else 'browser_extract_text']} for i in range(3)],{'requirement':'Deliver the three-value report','requirement_kind':'deliverable','evidence_subject_id':'report','eligible_evidence_tool_names':['file_write']}]}))
                if system.startswith('You are a semantic completion reviewer'):
                    self.reviews+=1;packet=json.loads(kw['messages'][0]['content'][0]['text']);coverage=[]
                    for row in packet['material_requirement_contract']:
                        subject=row['evidence_subject_id']
                        if subject=='report':ids=[r['invocation_id'] for r in packet['tool_evidence'] if r.get('output',{}).get('artifact_record_binding',{}).get('filename')=='sourced.txt']
                        else:
                            assert 'browser_extract_text' in row['eligible_evidence_tool_names'],row
                            i=int(subject[-1]);records=[r for r in packet['tool_evidence'] if r.get('output',{}).get('result_kind')=='structured_source_record' and r['output'].get('record',{}).get('url')==urls[i]]
                            assert len(records)==1,(urls[i], [{'id':r.get('invocation_id'),'tool':r.get('tool_name'),'kind':r.get('output',{}).get('result_kind'),'record':r.get('output',{}).get('record'), 'shape':list(r.get('output',{}))} for r in packet['tool_evidence']])
                            assert str(values[i]) in records[0]['output']['record']['text'] and '2026-10-25' in records[0]['output']['record']['text']
                            ids=[r['invocation_id'] for r in records]
                        assert len(ids)==1
                        coverage.append({'requirement_id':row['requirement_id'],'satisfied':True,'draft_covered':True,'evidence_invocation_ids':ids})
                    return text(json.dumps({**fixture_source_assessment(packet),
                        'disposition':'complete','all_material_requirements_enumerated':True,'material_requirements':coverage,'unresolved_requirements':[]}))
                if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.txt','confidence':1})
                if not names:return text('{}')
                self.phase+=1
                if self.phase==1:return call('request_tool_scope',{'capabilities':['web','local_files'],'tool_names':['browser_navigate','browser_extract_text','file_write'],'required_tool_names':['browser_navigate','browser_extract_text','file_write'],'artifact_extensions':['.txt']})
                if self.phase in [2,4,6]:return call('browser_navigate',{'url':urls[(self.phase-2)//2]})
                if self.phase in [3,5,7]:return call('browser_extract_text',{})
                if self.phase==8:return call('file_write',{'path':str(path),'content':'Current values: 37, 41, 53.\n'})
                assert self.phase==9,'A source-binding failure repeated research or creation'
                return text('The sourced report is ready.\nMEDIA:'+str(path))
        model=Provider();runtime.model_client=model
        try:
            with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
                prompt={'en':'Verify each current source and attach one text report: ','es':'Verifica cada fuente actual y adjunta un solo informe de texto: ','ja':'各情報源の現在の値を確認して、1つのテキスト報告書を添付してください: '}[language]+', '.join(urls)
                started=time.monotonic();response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});elapsed=time.monotonic()-started;data=response.json();assert not model.errors,model.errors
                assert response.status_code==200 and len(data['artifacts'])==1,data
                assert client.get(data['artifacts'][0]['url']).content==b'Current values: 37, 41, 53.\n'
                assert elapsed < 15 and model.reviews == 0 and (len(writes) == 1), (elapsed, model.reviews, len(writes), [{k: e[k] for k in ('event_type', 'validation_errors', 'review_error', 'status') if k in e} for e in runtime.store.list_conversation_events(cid) if e.get('event_type') in {'conversation.completion_requirement_contract', 'conversation.completion_review'}])
                turn=next(e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn');extracts=[r for r in turn['tool_results'] if r['tool_name']=='browser_extract_text'];assert len(extracts)==3 and {r['output']['page_url'] for r in extracts}==set(urls)
        finally:browser.close();server.shutdown();server.server_close()


@pytest.mark.parametrize('redirect', [False, True])
def test_raw_navigation_ignores_old_document_events_and_records_actual_redirect(redirect):
    """Real delayed navigation must own both document readiness and provenance."""
    from playwright.async_api import async_playwright
    from nullion.plugins.browser_plugin.backends import cdp_backend as cdp
    class Source(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            time.sleep(.35)  # Previously slow dependency: headers/document commit.
            if self.path == '/redirect':
                self.send_response(302);self.send_header('Location','/final');self.end_headers();return
            body='<html><body>情報 37 — información 41</body></html>'.encode()
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(body)));self.end_headers()
            try:self.wfile.write(body)
            except BrokenPipeError:pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);threading.Thread(target=server.serve_forever,daemon=True).start()
    async def run():
        with socket.socket() as reserve:reserve.bind(('127.0.0.1',0));port=reserve.getsockname()[1]
        async with async_playwright() as pw:
            browser=await pw.chromium.launch(headless=True,args=['--remote-debugging-port='+str(port)])
            page=await browser.new_page();base='http://127.0.0.1:'+str(server.server_port)
            target=next(t for t in await asyncio.to_thread(cdp._cdp_targets,'http://127.0.0.1:'+str(port)) if t.get('type')=='page')
            raw=cdp._RawCDPPage(target)
            try:
                await raw._ensure_page_enabled()
                # A previous document event is an external protocol fact, never
                # proof that this requested navigation has finished.
                raw._client._event_backlog['Page.domContentEventFired']=[{'timestamp':0}]
                raw._client._event_backlog['Page.loadEventFired']=[{'timestamp':0}]
                started=time.monotonic();await raw.goto(base+('/redirect' if redirect else '/slow'))
                assert await raw.inner_text('body') == '情報 37 — información 41'
                assert raw.url == base+('/final' if redirect else '/slow')
                assert time.monotonic()-started < 5
                fragment = raw.url + '#facts'
                started=time.monotonic();await raw.goto(fragment)
                assert raw.url == fragment and await raw.inner_text('body') == '情報 37 — información 41'
                assert time.monotonic()-started < 2
            finally:await raw.close();await browser.close()
    try:asyncio.run(run())
    finally:server.shutdown();server.server_close()

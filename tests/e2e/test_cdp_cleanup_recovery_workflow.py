"""Real chat/SQLite/Chromium after failed external Playwright connection cleanup."""
from tests.workflow_provider_protocol import workflow_provider_reply
import asyncio,json,os,socket,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from types import SimpleNamespace
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker real browser boundary')

def test_hung_connection_cleanup_falls_back_and_preserves_borrowed_tab(tmp_path,monkeypatch):
    from fastapi.testclient import TestClient
    from playwright.sync_api import sync_playwright
    from nullion import web_app,chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import cdp_backend as cdp
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    monkeypatch.setenv('HOME',str(tmp_path));monkeypatch.setenv('NULLION_HOME',str(tmp_path/'home'))
    monkeypatch.setenv('NULLION_ENV_FILE',str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED','0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE','off');monkeypatch.setenv('NULLION_BROWSER_BACKEND','cdp');chat_store._store=None
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html');self.end_headers()
            self.wfile.write(b'<html><body><h1>Verified transport recovery</h1><p>Price $37; stock 12</p></body></html>')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Page);threading.Thread(target=server.serve_forever,daemon=True).start()
    url='http://localhost:'+str(server.server_port)+'/'
    with socket.socket() as reserve:reserve.bind(('127.0.0.1',0));port=reserve.getsockname()[1]
    cleaned=[]
    class FailedDriver:
        async def stop(self):
            try:await asyncio.sleep(60)
            finally:cleaned.append(True)
        async def connect(self,url):raise TimeoutError('External Playwright connection did not initialize')
        @property
        def chromium(self):return SimpleNamespace(connect_over_cdp=self.connect)
    class Manager:
        async def __aenter__(self):return FailedDriver()
    monkeypatch.setattr(cdp,'async_playwright',Manager)
    with sync_playwright() as pw:
        browser=pw.chromium.launch(headless=True,args=['--remote-debugging-port='+str(port)])
        borrowed=browser.new_page();borrowed.goto('data:text/html,<title>Borrowed user page</title><p>Do not change</p>')
        monkeypatch.setenv('NULLION_BROWSER_CDP_URL','http://127.0.0.1:'+str(port))
        policy=BrowserPolicy();policy.block_private=False
        registry=ToolRegistry();register_browser_tools(registry,policy=policy)
        runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
        def text(value):return {'stop_reason':'end_turn','content':[{'type':'text','text':value}]}
        def tool(name,args):return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
        class Model:
            phase=0
            def create(self,**kw):
                protocol_reply = workflow_provider_reply(kw, completion=True)
                if protocol_reply is not None: return protocol_reply
                names={t['name'] for t in kw.get('tools',[])}
                if 'select_attachment_format' in names:return tool('select_attachment_format',{'extension':None,'confidence':1})
                if not names:return text('{}')
                self.phase+=1
                if self.phase==1:return tool('request_tool_scope',{'capabilities':['web'],'tool_names':['browser_navigate','browser_extract_text'],'required_tool_names':['browser_navigate','browser_extract_text']})
                if self.phase==2:return tool('browser_navigate',{'url':url})
                if self.phase==3:return tool('browser_extract_text',{})
                assert self.phase==4
                assert 'Price $37; stock 12' in json.dumps(kw['messages'])
                return text('Price $37; stock 12. '+url)
        samples=[]
        try:
            for i,prompt in enumerate(['Read the price and stock.','価格と在庫を教えて。','Lee el precio y las existencias.']):
                model=Model();runtime.model_client=model
                with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
                    cid='web:transport-recovery:'+str(i);start=time.monotonic()
                    response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});samples.append(time.monotonic()-start)
                    assert response.status_code==200,response.text
                    assert 'Price $37; stock 12' in response.json()['text'] and not response.json()['artifacts'],response.json()
                    turn=next(e for e in runtime.store.list_conversation_events(cid) if e.get('event_type')=='conversation.chat_turn')
                    tools=[r for r in turn['tool_results'] if r['tool_name'].startswith('browser_')]
                    assert [r['tool_name'] for r in tools]==['browser_navigate','browser_extract_text']
                    assert all(r['status']=='completed' for r in tools)
            assert sum(samples)/len(samples)<5 and max(samples)<8,samples
            assert cleaned==[True]
            assert not borrowed.is_closed() and borrowed.title()=='Borrowed user page'
        finally:browser.close();server.shutdown();server.server_close()

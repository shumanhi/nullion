"""Native browser reads retain source proof through real API/SQLite history."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')


def test_large_native_listing_retains_linked_facts_without_research_replay(tmp_path,monkeypatch,record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.web_app import create_app
    from nullion.tools import ToolRegistry
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.preferences import save_preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    from tests.e2e.live_public_followup_assertions import assert_public_followup_outcome
    from nullion.startup_maintenance import _compact_tool_output
    runtime=isolate(tmp_path,monkeypatch);save_preferences({'outbound_request_mode':'allow_all'})
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright');monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false');monkeypatch.setenv('NULLION_CODE_MODE','off')
    hits=[];target='/product/37?variant=blue&opaque='+'A37'*60
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/listing':hits.append(self.path)
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers()
            links=''.join(f'<a href="/item/{i}?opaque={"Q"*120}">Item {i}</a>' for i in range(199))
            self.wfile.write(('<main><h1>照明 37 / Lámpara 37 — 988</h1><a href="'+target+'">照明 37 / Lámpara 37</a>'+links+'<p>'+'evidence '*7000+'</p></main>').encode())
        def log_message(self,*args):pass
    site=ThreadingHTTPServer(('127.0.0.1',0),Site);threading.Thread(target=site.serve_forever,daemon=True).start()
    base=f'http://localhost:{site.server_port}';url=base+target
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            if not kw.get('tools'):return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web'],
                'tool_names':['browser_read_pages'],'required_tool_names':['browser_read_pages']})
            if self.phase==2:return call('browser_read_pages',{'pages':[{'url':base+'/listing','read_kind':'text','max_text_chars':50000}]})
            assert self.phase==3,'Native page history triggered another research/model round'
            return text('照明 37 / Lámpara 37 — 988\n'+url)
    model=Model();registry=ToolRegistry();register_browser_tools(registry,model_client_getter=lambda:model)
    samples=[]
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            for index,prompt in enumerate(['価格とリンクを確認してください。','Confirma el precio y el enlace.']):
                model.phase=0;cid=f'web:history-provenance:{index}';started=time.perf_counter()
                response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
                samples.append(time.perf_counter()-started)
                assert response.status_code==200,response.text
                turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
                proof=assert_public_followup_outcome(response=response.json(),turns=[turn],reviews=[])
                assert any(row.get('evidence_kind')=='native_listing_link' for row in proof['observed_sources'])
                reads=[r for r in turn['tool_results'] if r['tool_name']=='browser_read_pages']
                assert len(reads)==1 and len(hits)==index+1 and model.phase==3
                out=reads[0]['output'];doc=out['documents'][0]
                assert '988' in doc['text'] and doc['links'][0]['url']==url
                assert doc['history_projection']['omitted_link_count']>0
                assert _compact_tool_output('browser_read_pages',out)==out
        average=sum(samples)/len(samples);maximum=max(samples)
        record_property('native_history_average_seconds',average);record_property('native_history_max_seconds',maximum)
        assert average<5 and maximum<8,samples
    finally:registry.run_cleanup_hooks();site.shutdown();site.server_close()

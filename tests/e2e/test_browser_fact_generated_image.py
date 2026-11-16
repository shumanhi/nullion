"""Real API, browser facts, native image producer and downloaded PNG; only provider art is faked."""
import json,os,threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import pytest
pytestmark=pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E')!='1',reason='Docker E2E')

@pytest.mark.parametrize('prompt',['価格と在庫を図にしてください。','Representa el precio y las existencias.'])
def test_browser_cover_metadata_does_not_block_requested_new_image(tmp_path,monkeypatch,prompt):
    from datetime import datetime,UTC
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion.web_app import create_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import BoundaryKind,BoundaryPolicyRule,PolicyMode,GLOBAL_PERMISSION_PRINCIPAL
    from nullion.preferences import save_preferences
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry,register_media_plugin
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate,text,call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime=isolate(tmp_path,monkeypatch);save_preferences({'outbound_request_mode':'allow_all'})
    monkeypatch.setenv('NULLION_IMAGE_GENERATE_ENABLED','true')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright');monkeypatch.setenv('NULLION_BROWSER_HEADLESS','true')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE','false')
    for kind in (BoundaryKind.OUTBOUND_NETWORK,BoundaryKind.FILESYSTEM_ACCESS):
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(str(kind),GLOBAL_PERMISSION_PRINCIPAL,kind,PolicyMode.ALLOW,'*','fixture',datetime.now(UTC)))
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(b'<main><article><h2><a href="/book-detail">Verified book</a></h2><p>Price $51.77; stock 22.</p><img width="120" height="80" alt="Verified book" src="https://source.example/cover.jpg"></article></main>')
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Source);threading.Thread(target=server.serve_forever,daemon=True).start()
    url=f'http://localhost:{server.server_port}/book';cid='web:fact-visual';target=artifact_root_for_principal(cid)/'new-visual.png';generated=[]
    def generate(description,path,size):
        assert '51.77' in description and '22' in description
        Path(path).parent.mkdir(parents=True,exist_ok=True);Image.new('RGB',(320,240),'navy').save(path)
        generated.append(path);return {'artifact_path':path,'artifact_paths':[path],'fallback_used':False}
    registry=ToolRegistry(filesystem_allowed_roots=[tmp_path,target.parent]);register_media_plugin(registry,image_generator=generate)
    class Model:
        phase=0
        def create(self,**kw):
            protocol=workflow_provider_reply(kw,completion=True)
            if protocol is not None:return protocol
            names={r['name'] for r in kw.get('tools',[])}
            if 'select_attachment_format' in names:return call('select_attachment_format',{'extension':'.png','confidence':1})
            if not names:return text('{}')
            self.phase+=1
            if self.phase==1:return call('request_tool_scope',{'capabilities':['web','local_files'],
                'tool_names':['browser_read_pages','image_generate'],'required_tool_names':['browser_read_pages','image_generate'],
                'artifact_extensions':['.png'],'source_artifact_delivery_requested':False})
            if self.phase==2:return call('browser_read_pages',{'pages':[{'url':url,'read_kind':'items'}]})
            if self.phase==3:
                wire=json.dumps(kw['messages']);assert '51.77' in wire and '22' in wire
                return call('image_generate',{'prompt':'Price 51.77; stock 22.','output_path':str(target)})
            assert self.phase==4
            return text('Ready.\nMEDIA:'+str(target))
    model=Model();register_browser_tools(registry,model_client_getter=lambda:model)
    try:
        with TestClient(create_app(runtime,AgentOrchestrator(model_client=model),registry)) as client:
            response=client.post('/api/chat',json={'text':prompt,'conversation_id':cid});data=response.json()
            assert response.status_code==200 and len(data['artifacts'])==1,data
            raw=client.get(data['artifacts'][0]['url']);assert raw.content.startswith(b'\x89PNG\r\n\x1a\n')
            assert len(generated)==1
            turn=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')[-1]
            from nullion.response_fulfillment_contract import _browser_page_context_used
            from nullion.tools import ToolResult
            reads=[r for r in turn['tool_results'] if r['tool_name']=='browser_read_pages']
            assert any(_browser_page_context_used(ToolResult(r['invocation_id'],r['tool_name'],r['status'],r['output'])) for r in reads),reads
            assert any(r['tool_name']=='image_generate' and r['status']=='completed' for r in turn['tool_results'])
            assert not any(r['tool_name']=='browser_image_collect' for r in turn['tool_results'])
    finally:registry.run_cleanup_hooks();server.shutdown();server.server_close()

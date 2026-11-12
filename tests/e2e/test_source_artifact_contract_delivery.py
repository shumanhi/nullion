"""Real API, Chromium capture, SQLite and HTTP artifact bytes; only provider replies are scripted."""
import json
import os
from pathlib import Path
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from tests.e2e.test_tracker_status_delivery import setup
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('prompt,wants_file', [
    ('表示中のページのスクリーンショットを送ってください。', True),
    ('أرسل صورة للصفحة الحالية.', True),
    ('Describe la página sin adjuntar la captura de investigación.', False),
])
def test_source_capture_material_contract_controls_real_api_delivery(tmp_path, monkeypatch, prompt, wants_file):
    import io
    import socket
    from PIL import Image
    from fastapi.testclient import TestClient
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import auto_backend

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header('Content-Type', 'text/html'); self.end_headers()
            self.wfile.write(b'<html><body style="background:#136abd"><h1>Current cart</h1><p>Selected item: Cedar</p></body></html>')
        def log_message(self, *args): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://capture.localhost:{server.server_port}/'
    original = socket.getaddrinfo
    monkeypatch.setattr(socket, 'getaddrinfo', lambda host, *a, **kw: original('127.0.0.1' if host=='capture.localhost' else host, *a, **kw))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path/'workspaces'))
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'auto')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'false')
    monkeypatch.setenv('NULLION_TASK_DECOMPOSITION_ENABLED', '0')
    monkeypatch.setattr(auto_backend, '_is_cdp_reachable', lambda: False)
    monkeypatch.setattr(auto_backend, '_reuse_preferred_cdp_if_available', lambda: False)
    monkeypatch.setattr(auto_backend, '_find_chrome', lambda: None)
    def text(value): return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps(value) if isinstance(value,dict) else value}]}
    def call(name, args): return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':name,'name':name,'input':args}]}
    class Model:
        opening=True; phase=0; plans=0
        def create(self, **kw):
            system=kw.get('system',''); names={r['name'] for r in kw.get('tools',[])}
            if 'material-requirements planner' in system:
                self.plans += 1
                return text({'all_material_requirements_enumerated':True,
                    'requested_artifact_extensions':['.png'] if wants_file and not self.opening else [],
                    'material_requirements':[{'requirement':prompt,'requirement_kind':'action' if self.opening else 'deliverable' if wants_file else 'fact',
                        'evidence_subject_id':'.png' if wants_file and not self.opening else 'page',
                        'eligible_evidence_tool_names':['browser_navigate'] if self.opening else ['browser_screenshot']}]})
            protocol=workflow_provider_reply(kw, completion=True)
            if protocol is not None: return protocol
            if 'select_attachment_format' in names: return call('select_attachment_format', {'extension':None,'confidence':1})
            if not names: return text({})
            self.phase += 1
            if self.phase == 1: return call('request_tool_scope', {'capabilities':['web'],
                'tool_names':['browser_navigate'] if self.opening else ['browser_screenshot'],
                'required_tool_names':['browser_navigate'] if self.opening else ['browser_screenshot'],
                'artifact_extensions':[] if self.opening else ['.png'], 'source_artifact_delivery_requested':not self.opening, 'source_artifact_request_evidence':prompt if not self.opening else ''})
            if self.phase == 2: return call('browser_navigate', {'url':url}) if self.opening else call('browser_screenshot', {})
            # Deliberately no MEDIA directive or path: material contract owns delivery.
            return text('The cart is open.' if self.opening else 'Current cart: Cedar.')
    model=Model(); app,runtime,db=setup(tmp_path,monkeypatch,model)
    registry=app.state.live_tracker_engine.registry
    register_browser_tools(registry)
    backend=registry._handlers['browser_navigate'].__self__._backend
    capture_handler=registry._handlers['browser_screenshot']
    def source_capture(invocation):
        result=capture_handler(invocation)
        if wants_file and isinstance(result.output,dict):
            for descriptor in result.output.get('artifact_descriptors',[]):
                descriptor['role']='source'
        return result
    # Exercise a valid source-role receipt from the actual Chromium capture,
    # matching the live incident rather than relying on an earlier scope hint.
    registry._handlers['browser_screenshot']=source_capture
    try:
        with TestClient(app) as client:
            cid='web:source-capture'
            opened=client.post('/api/chat', json={'text':f'Open this cart and keep it open: {url}', 'conversation_id':cid})
            assert opened.status_code==200,opened.text
            model.opening=False; model.phase=0; before=model.plans
            response=client.post('/api/chat', json={'text':prompt,'conversation_id':cid})
            assert response.status_code==200,response.text
            payload=response.json()
            turns=runtime.store.list_recent_conversation_events(cid,event_type='conversation.chat_turn')
            turn=turns[-1]
            assert turn['response_fulfilled'] is True, json.dumps({'payload':payload,'events':runtime.store.list_conversation_events(cid)}, default=str)
            assert model.plans==before+1, 'Reuse the same independent planner for final review'
            captures=[r for r in turn['tool_results'] if r['tool_name']=='browser_screenshot']
            assert len(captures)==1 and captures[0]['status']=='completed',turn
            navigation=next(r for r in turns[0]['tool_results'] if r['tool_name']=='browser_navigate')
            assert captures[0]['output']['session_id']==navigation['output']['session_id']
            assert captures[0]['output']['page_url']==url
            attachments=payload.get('artifacts') or []
            assert len(attachments)==(1 if wants_file else 0), json.dumps({'payload':payload,'events':runtime.store.list_conversation_events(cid)},default=str)
            if wants_file:
                delivered=client.get(attachments[0]['url'])
                assert delivered.status_code==200
                assert delivered.content==Path(captures[0]['output']['artifact_path']).read_bytes()
                image=Image.open(io.BytesIO(delivered.content)); assert image.format=='PNG'
                assert any(lo!=hi for lo,hi in image.convert('RGB').getextrema())
                assert 'attach the requested file' not in payload['text']
    finally:
        from nullion.plugins.browser_plugin.browser_tools import _run
        _run(backend.shutdown());server.shutdown();server.server_close()

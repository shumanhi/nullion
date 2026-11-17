"""Native browser snapshots and read fallback through real chat and Code Mode."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def test_large_cart_snapshot_and_script_scope_keep_reads_fast(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            # Force a large, realistic native snapshot without a giant content
            # field. Link URLs are opaque and must survive projection intact.
            links = ''.join(f'<a aria-label="Referencia {i}" href="/item/{i}?opaque={"x"*350}">Artículo {i}</a>' for i in range(180))
            content = '<main id="basket">Manzanas — $7; Café — $12; Total $19.</main>'
            if self.path.startswith('/cart'): content = '<a id="details" href="/basket">Abrir cesta</a>'+links
            body = ('<html><body>'+content+'</body></html>').encode()
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers(); self.wfile.write(body)
        def log_message(self, *_): pass
    site = ThreadingHTTPServer(('127.0.0.1', 0), Site)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    url = f'http://localhost:{site.server_port}/cart'
    class Model:
        phase = 0
        plain = False
        def create(self, **kw):
            protocol = workflow_provider_reply(kw)
            if protocol is not None: return protocol
            if not kw.get('tools'): return text('{}')
            if self.plain: return text('24')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['web'], 'tool_names': ['browser_run_js']})
            if self.phase == 2:
                enum = next(d for d in kw['tools'] if d['name'] == 'code_mode')['input_schema']['properties']['tool_names']['items']['enum']
                assert {'browser_navigate','browser_snapshot','browser_extract_text'} <= set(enum)
                # No JS is required: the scope must retain the safe native reads.
                return call('code_mode', {'tool_names': ['browser_navigate','browser_snapshot','browser_extract_text'], 'code':
                    "r=call_tool('browser_navigate', {'url':"+repr(url)+"})\n"
                    "s=r['output']['session_id']\n"
                    "snapshot=call_tool('browser_snapshot', {'session_id':s,'max_elements':180})\n"
                    "assert len(snapshot['output']['snapshot']['elements']) == 180\n"
                    "target=[e['href'] for e in snapshot['output']['snapshot']['elements'] if e.get('text') == 'Abrir cesta'][0]\n"
                    "call_tool('browser_navigate', {'session_id':s,'url':target})\n"
                    "call_tool('browser_extract_text', {'session_id':s,'selector':'#basket','ready_selector':'#basket','include_links':False})['output']['text']"})
            assert self.phase == 3, 'A clipped snapshot or scope repair added another model round'
            assert 'Manzanas' in json.dumps(kw['messages'],ensure_ascii=False)
            return text('Manzanas: $7. Café: $12. Total: $19.')
    model = Model(); registry = ToolRegistry(); register_browser_tools(registry, policy=BrowserPolicy())
    try:
        with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
            samples=[]
            for index, prompt in enumerate(['カートの中身は？', '¿Qué hay en mi cesta?', 'Que contient mon panier ?']):
                model.phase=0; start=time.monotonic(); cid=f'web:cart-recovery-{index}'
                result=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
                samples.append(time.monotonic()-start)
                assert result.status_code==200 and 'Total: $19' in result.json()['text'],result.text
                turn=next(r for r in reversed(runtime.store.list_conversation_events(cid)) if r.get('event_type')=='conversation.chat_turn')
                assert turn['response_fulfilled'] is True
                assert model.phase == 3
                assert not any(r['tool_name']=='inspect_retained_evidence' for r in turn['tool_results'])
                assert all(r['status']=='completed' for r in turn['tool_results'])
            assert sum(samples)/len(samples)<10 and max(samples)<15,samples
            record_property('cart_average_seconds',sum(samples)/len(samples))
            record_property('cart_max_seconds',max(samples))
            model.plain=True; start=time.monotonic()
            result=client.post('/api/chat',json={'text':'4 × 6','conversation_id':'web:plain-after-cart'})
            assert result.json()['text']=='24' and time.monotonic()-start<2
    finally:
        registry.run_cleanup_hooks(); site.shutdown(); site.server_close()

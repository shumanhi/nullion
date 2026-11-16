"""Real API/runtime/browser; external site and model are deterministic fixtures."""
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('backend', ['playwright', 'auto'])
def test_native_preflight_and_following_reads_retain_task_session(tmp_path, monkeypatch, backend,
                                                               source_summary_model_reply, record_property):
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    from datetime import UTC, datetime
    from nullion.preferences import save_preferences
    from nullion.policy import BoundaryKind, BoundaryPolicyRule, GLOBAL_PERMISSION_PRINCIPAL, PolicyMode
    save_preferences({'outbound_request_mode':'allow_all'})
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule('source-read', GLOBAL_PERMISSION_PRINCIPAL,
        BoundaryKind.OUTBOUND_NETWORK, PolicyMode.ALLOW, '*', 'fixture', datetime.now(UTC)))
    protected_reads = []
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/favicon.ico':
                self.send_response(204); self.end_headers(); return
            if self.path == '/entry':
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Set-Cookie', 'source_session=retained; Path=/')
                self.end_headers(); self.wfile.write(b'<h1>Source entry</h1>'); return
            retained = 'source_session=retained' in self.headers.get('Cookie', '')
            protected_reads.append(retained)
            if not retained:
                time.sleep(2)  # Repeated session setup is both slow and wrong.
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            body = ('<main id="results"><article><h2><a href="/product">Verified option</a></h2>'
                    '<p>$17</p></article></main>') if retained else '<h1>Session setup required</h1>'
            self.wfile.write(body.encode())
        def log_message(self, *args): pass
    site = ThreadingHTTPServer(('127.0.0.1', 0), Site)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    base = f'http://localhost:{site.server_port}'
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', backend)
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    # Declare the external worker interpreter; native preflight must prevent
    # launching it, so an accidental adaptive call fails this provider fixture.
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', sys.executable)
    class Model:
        phase = 0
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            summary = source_summary_model_reply(kwargs)
            if summary is not None: return summary
            if not kwargs.get('tools'): return text('{}')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities':['web'],
                    'tool_names':['browser_run_task', 'browser_read_pages'],
                    'required_tool_names':['browser_run_task']})
            if self.phase == 2:
                return call('browser_run_task', {'url':base+'/entry', 'task':'確認してください'})
            if self.phase == 3:
                return call('browser_read_pages', {'independent_pages':True, 'pages':[
                    {'url':base+'/a', 'ready_selector':'#results'},
                    {'url':base+'/b', 'ready_selector':'#results'}]})
            assert self.phase == 4, 'Unexpected extra model/inner browser loop'
            assert 'Verified option' in json.dumps(kwargs['messages'])
            return text('Verified option: $17. '+base+'/product')
    model = Model(); registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda:model)
    samples = []
    try:
        with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
            for index, prompt in enumerate(['確認してください', 'Comprueba la opción', 'Vérifiez cette option']):
                model.phase = 0; started = time.monotonic()
                response = client.post('/api/chat', json={'text':prompt, 'conversation_id':f'web:session-reuse-{index}'})
                samples.append(time.monotonic()-started)
                evidence = [r for event in runtime.store.list_conversation_events(f'web:session-reuse-{index}')
                            if event.get('event_type')=='conversation.chat_turn' for r in event.get('tool_results', [])]
                assert response.status_code == 200 and '$17' in response.json()['text'], json.dumps({
                    'reply':response.text, 'cookies':protected_reads, 'results':[
                        {'tool':r['tool_name'], 'status':r['status'], 'error':r.get('error'),
                         'session':r.get('output', {}).get('session_id'),
                         'reason':r.get('output', {}).get('reason'),
                         'pages':r.get('output', {}).get('pages')} for r in evidence]})
                assert base+'/product' in response.json()['text'] and not response.json()['artifacts']
                turns = [r for r in runtime.store.list_conversation_events(f'web:session-reuse-{index}')
                         if r.get('event_type') == 'conversation.chat_turn']
                results = turns[-1]['tool_results']
                preflight = next(r['output'] for r in results if r['tool_name']=='browser_run_task')
                assert preflight['strategy']['adaptive_browser_started'] is False
                batch = next(r['output'] for r in results if r['tool_name']=='browser_read_pages')
                assert batch['completed_pages'] == 2 and model.phase == 4
        assert len(protected_reads) == 6 and all(protected_reads), protected_reads
        assert sum(samples)/len(samples) < 3 and max(samples) < 4, samples
        record_property('mean_session_lookup_seconds', sum(samples)/len(samples))
        record_property('max_session_lookup_seconds', max(samples))
    finally:
        registry.run_cleanup_hooks()
        site.shutdown(); site.server_close()

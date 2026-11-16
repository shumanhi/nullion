"""Real chat route, SQLite receipts, Monty and Chromium; fake only the model/site."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('mode', ['off', 'read_only', 'on'])
def test_gate_preserves_browser_research_and_plain_chat(tmp_path, monkeypatch, mode,
        source_summary_model_reply, record_property):
    from fastapi.testclient import TestClient
    from nullion import code_mode
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from nullion.preferences import save_preferences
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text, call
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    save_preferences({'outbound_request_mode': 'allow_all'})
    monkeypatch.setenv('NULLION_CODE_MODE', mode)
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    requests = []
    class Site(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(200); self.send_header('Content-Type', 'text/html'); self.end_headers()
            self.wfile.write(b'<main id="results">Verified item costs $17.</main>')
        def log_message(self, *args): pass
    site = ThreadingHTTPServer(('127.0.0.1', 0), Site)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    base = f'http://localhost:{site.server_port}'
    class Model:
        phase = 0
        plain = False
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            summary = source_summary_model_reply(kwargs)
            if summary is not None: return summary
            if not kwargs.get('tools'): return text('{}')
            if self.plain: return text('こんにちは。')
            self.phase += 1
            if self.phase == 1:
                return call('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['browser_read_pages'], 'required_tool_names': ['browser_read_pages']})
            if self.phase == 2:
                available = {d['name'] for d in kwargs['tools']}
                assert (code_mode.TOOL_NAME in available) == (mode != 'off')
                args = {'independent_pages': True, 'pages': [
                    {'url': base+suffix, 'read_kind': 'text', 'ready_selector': '#results'}
                    for suffix in ['/a', '/b']]}
                if mode == 'off': return call('browser_read_pages', args)
                return call(code_mode.TOOL_NAME, {'tool_names': ['browser_read_pages'],
                    'code': "pages = " + repr(args['pages']) + "\nrows = [call_tool('browser_read_pages', {'pages': [p]}) for p in pages]\n"
                        + ("" if self.omit_final_expression else "[r['output'] for r in rows]")})
            assert self.phase == 3, 'Extra model call or inner browser agent loop'
            assert '$17' in json.dumps(kwargs['messages'])
            return text('Verified item: $17. '+base+'/a')
    model = Model(); registry = ToolRegistry()
    register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
    try:
        with TestClient(create_app(runtime, AgentOrchestrator(model_client=model), registry)) as client:
            samples = []
            for index, prompt in enumerate(['確認してください', 'Comparez les données', 'Comprueba los datos']):
                model.phase = 0
                model.omit_final_expression = index == 1
                started = time.monotonic()
                conversation = f'web:code-mode-{index}'
                response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
                samples.append(time.monotonic()-started)
                assert response.status_code == 200 and '$17' in response.json()['text'], (response.text, list(runtime.store.list_conversation_events(conversation)))
                results = [r for e in runtime.store.list_conversation_events(conversation)
                    if e.get('event_type') == 'conversation.chat_turn' for r in e.get('tool_results', [])]
                assert any(r['tool_name'] == 'browser_read_pages' and r['status'] == 'completed' for r in results)
                composed = [r for r in results if r['tool_name'] == code_mode.TOOL_NAME]
                assert bool(composed) == (mode != 'off')
                if composed:
                    assert composed[-1]['status'] == 'completed', composed
                    assert len(composed[-1]['output']['reads']) == 2
                    assert composed[-1]['output']['reads'][0]['invocation_id']
                    if model.omit_final_expression:
                        observations = composed[-1]['output']['value']['observations']
                        assert len(observations) == 2 and '$17' in json.dumps(observations)
                        assert all(row['inspection_reference']['json_pointer'] == '' for row in observations)
                        assert len([r for r in results if r['tool_name'] == 'browser_read_pages']) == 2
            assert '/a' in requests and '/b' in requests
            assert sum(samples)/len(samples) < 8 and max(samples) < 12, samples
            record_property('mean_browser_seconds', sum(samples)/len(samples))
            # A slow optional execution dependency must never enter plain chat.
            def slow_execution(*args, **kwargs):
                time.sleep(3)
                raise AssertionError('Code execution entered ordinary chat')
            monkeypatch.setattr('pydantic_monty.Monty', slow_execution)
            model.plain = True
            samples = []
            previous = len(requests)
            for index in range(3):
                started = time.monotonic()
                response = client.post('/api/chat', json={'text': 'こんにちは', 'conversation_id': f'web:plain-code-{index}'})
                samples.append(time.monotonic()-started)
                assert response.status_code == 200 and response.json()['text'] == 'こんにちは。', response.text
            assert len(requests) == previous
            assert sum(samples)/len(samples) < 1 and max(samples) < 2, samples
            record_property('mean_plain_seconds', sum(samples)/len(samples))
    finally:
        registry.run_cleanup_hooks()
        site.shutdown(); site.server_close()

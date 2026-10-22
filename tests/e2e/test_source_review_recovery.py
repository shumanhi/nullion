"""Real chat route, SQLite, graph and Telegram delivery; external provider fakes only."""
from __future__ import annotations
import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

from test_scheduled_report_completion import setup_runtime, text_response, tool_response
from test_source_and_status_delivery_contracts import source_presentation_response

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


@pytest.mark.parametrize('prompt', ['Compare exact transport and lodging prices.', '交通と宿の正確な価格を比較して。', 'Compara precios exactos de transporte y alojamiento.'])
@pytest.mark.parametrize('mode', ['timeout', 'invalid_review', 'later_wait_failure', 'same_batch_wait_failure', 'blocked_requirements'])
def test_chat_preserves_completed_findings_through_review_and_browser_failure(tmp_path, monkeypatch, record_property, prompt, mode):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult
    from nullion.telegram_app import _deliver_reply
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    runtime = setup_runtime(tmp_path, monkeypatch); chat_store._store = None
    calls = []
    registry = ToolRegistry()
    def read(call):
        subject = call.arguments['subject']; calls.append(subject)
        value = 'Option A · 71 USD' if subject == 'transport' else 'Property B · 240 USD · 0.3 km'
        return ToolResult(call.invocation_id, call.tool_name, 'completed', {
            'result_kind': 'source_observations', 'report_status': 'partial',
            'observations': [{'value': value, 'quote': value, 'source_url': 'https://example.test/'+subject}],
            'unknowns': ['Cancellation terms not displayed']})
    def wait(call):
        calls.append('wait')
        return ToolResult(call.invocation_id, call.tool_name, 'failed', {
            'reason': 'browser_condition_unmet', 'page_state_preserved': True,
            'recovery': {'kind': 'inspect_page', 'retry_tool_name': call.tool_name,
                         'required_tool_names': ['browser_snapshot']}}, 'Timeout 100ms exceeded')
    registry.register(ToolSpec('lookup_source', 'Read a remote source', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), read)
    registry.register(ToolSpec('browser_wait_for', 'Wait for a remote page condition', ToolRiskLevel.LOW,
        ToolSideEffectClass.READ, False, 2, capability_tags=('web',)), wait)
    class Model:
        steps = 0; reviews = 0
        def create(self, **kw):
            names = {t['name'] for t in kw.get('tools', [])}; system = kw.get('system', '')
            if system.startswith('You are a material-requirements planner'):
                return text_response(json.dumps({'all_material_requirements_enumerated': True,
                    'material_requirements': [{'requirement': 'Verify both prices and final availability',
                    'requirement_kind': 'fact', 'evidence_subject_id': 'comparison',
                    'eligible_evidence_tool_names': ['lookup_source']}]}))
            if system.startswith('You are a semantic completion reviewer'):
                self.reviews += 1
                if mode in {'timeout', 'same_batch_wait_failure'}: raise TimeoutError('External review provider timed out')
                if mode == 'invalid_review': return text_response('not structured data')
                if mode == 'blocked_requirements':
                    return text_response(json.dumps({'disposition': 'blocked',
                        'unresolved_requirements': ['Verify the exact price.; Provide cancellation terms.; Confirm final availability.'],
                        'retry_tool_names': []}))
                return text_response(json.dumps({'disposition': 'retry',
                    'unresolved_requirements': ['Final availability remains unverified'],
                    'retry_tool_names': ['browser_wait_for']}))
            if names == {'present_source_answer'}: return source_presentation_response(kw)
            if not names: return text_response('{}')
            if 'select_attachment_format' in names:
                return tool_response('select_attachment_format', {'extension': None, 'confidence': 1})
            self.steps += 1
            if self.steps == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web'],
                    'tool_names': ['lookup_source', 'browser_wait_for'], 'required_tool_names': ['lookup_source']})
            if self.steps == 2:
                content = [{'type': 'tool_use', 'id': subject, 'name': 'lookup_source',
                    'input': {'subject': subject}} for subject in ('transport', 'lodging')]
                if mode == 'same_batch_wait_failure':
                    content.append({'type': 'tool_use', 'id': 'wait', 'name': 'browser_wait_for',
                        'input': {'text': 'Unshown condition', 'timeout': 0.1}})
                return {'stop_reason': 'tool_use', 'content': content}
            assert mode == 'later_wait_failure', (mode, self.steps, names)
            if self.steps == 3: return tool_response('browser_wait_for', {'text': 'Condition not displayed', 'timeout': 0.1})
            return text_response('The remaining browser condition could not be verified.')
    model = Model(); runtime.model_client = model
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    conversation = 'web:new-comparison'
    started = time.perf_counter()
    with TestClient(app) as client:
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': conversation})
        assert response.status_code == 200, response.text
        payload = response.json()
        assert '71 USD' in payload['text'] and '240 USD' in payload['text'], payload
        assert 'Cancellation terms' in payload['text']
        assert payload['text'].count('Not verified from this check:') == 1, payload
        assert payload['text'].count('Cancellation terms not displayed') == 1, payload
        assert 'Verify the exact price' not in payload['text'] and 'Provide cancellation' not in payload['text'], payload
        assert "I couldn't complete this request because" not in payload['text'], payload
        assert not payload['artifacts'] and 'MEDIA:' not in payload['text']
        events = [e for e in runtime.store.list_conversation_events(conversation) if e.get('event_type') == 'conversation.chat_turn']
        assert events[-1]['assistant_reply'] == payload['text']
        assert len([t for t in events[-1]['tool_results'] if t['tool_name'] == 'lookup_source']) == 2
        restored = client.get('/api/chat/history/'+conversation,
            params={'turn_ids': json.dumps([events[-1]['turn_id']])})
        assert restored.status_code == 200
        assert restored.json()['terminal_replies'][0]['text'] == payload['text']
    elapsed = time.perf_counter() - started
    record_property('elapsed_seconds', elapsed)
    assert calls == ['transport', 'lodging'] + (['wait'] if mode in {'later_wait_failure', 'same_batch_wait_failure'} else [])
    assert model.reviews <= (3 if mode == 'later_wait_failure' else 1)
    assert elapsed < 8, elapsed
    wire = []
    class Message:
        chat = SimpleNamespace(id=733); message_id = 19
        async def reply_text(self, text, **kw):
            wire.append((text, kw)); return SimpleNamespace(message_id=20)
    asyncio.run(_deliver_reply(Message(), payload['text'], principal_id='telegram:733'))
    assert '71 USD' in wire[0][0] and '240 USD' in wire[0][0]
    assert len(wire) == 1 and wire[0][0].count('Not verified from this check:') == 1
    assert 'Verify the exact price' not in wire[0][0]
    assert wire[0][1]['parse_mode'] == 'HTML'


def test_real_browser_url_wait_and_separate_selected_values(tmp_path, monkeypatch, record_property):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
    from nullion.plugins.browser_plugin.browser_tools import _browser_assert_page_state_script
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers()
            self.wfile.write('<html><body><input aria-label="開始" value="2031-04-06"><input aria-label="終了" value="2031-04-09"><button onclick="history.replaceState(null,\'\',\'/offers/one/two\')">確認</button></body></html>'.encode())
        def log_message(self, *args): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    backend = PlaywrightBackend()
    async def run():
        try:
            await backend.navigate('controls', f'http://127.0.0.1:{server.server_port}/')
            await backend.click('controls', 'button')
            started = time.perf_counter()
            await backend.wait_for('controls', None, '*offers/one/two*', None, 0.5)
            elapsed = time.perf_counter()-started
            record_property('url_wait_seconds', elapsed)
            assert elapsed < 0.5
            page = await backend._get_page('controls')
            assert '2031-04-06 – 09' not in await page.inner_text('body')
            with pytest.raises(Exception):
                await backend.wait_for('controls', None, None, '2031-04-06 – 09', 0.1)
            state = await page.evaluate(_browser_assert_page_state_script(
                required=['2031-04-06', '2031-04-09'], forbidden=[], selector=None))
            assert state['ok'], state
        finally:
            await backend.shutdown()
    try: asyncio.run(run())
    finally: server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_plain_api_latency_stays_independent_of_slow_optional_review(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0'); monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    runtime = setup_runtime(tmp_path, monkeypatch); chat_store._store = None
    class Model:
        calls = 0
        def create(self, **kw):
            if kw.get('system', '').startswith(('You are a semantic completion reviewer', 'You are a material-requirements planner')):
                time.sleep(1); raise AssertionError('Optional slow review entered plain chat')
            self.calls += 1
            return text_response('Hello.')
    model = Model(); samples = []
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=ToolRegistry())) as client:
        for number, prompt in enumerate(('Hello', 'こんにちは', 'Hola', 'مرحبا', 'Bonjour')):
            started = time.perf_counter()
            response = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:plain-{number}'})
            samples.append(time.perf_counter()-started)
            assert response.status_code == 200 and response.json()['text'] == 'Hello.', response.text
    record_property('average_seconds', sum(samples)/len(samples)); record_property('p95_seconds', max(samples))
    assert model.calls == 5
    assert sum(samples)/len(samples) < 0.5 and max(samples) < 1, samples

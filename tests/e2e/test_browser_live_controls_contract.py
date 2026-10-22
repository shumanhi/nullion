"""Real browser interaction, selected controls, API persistence and delivery."""
from __future__ import annotations

import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='container E2E only')


def test_dynamic_times_prices_and_stock_keep_live_control_evidence(tmp_path, monkeypatch, record_property, source_summary_model_reply):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    worker = os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if not worker:
        pytest.skip('isolated Browser Use dependency required')
    monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', worker)
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_USE_SANDBOX', 'false')
    monkeypatch.delenv('NULLION_BROWSER_USE_CDP_URL', raising=False)
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *a, **kw: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *a, **kw: None)

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            if self.path == '/frame':
                self.wfile.write(b'<html><body>Embedded booking controls</body></html>'); return
            self.wfile.write('''<html><body>
                <input aria-label="日付" value="2026-10-10">
                <select aria-label="人数" style="display:none"><option value="2" selected>2</option><option value="3">3</option></select>
                <select aria-label="時刻" style="display:none"><option value="19:00" selected>7:00 PM</option><option value="18:00">6:00 PM</option><option value="17:00" disabled>5:00 PM</option></select>
                <button onclick="document.getElementById('results').hidden=false;history.replaceState(null,'','?state='+('x'.repeat(6000)))">確認</button>
                <section id="results" hidden><button>19:30</button><p>$84</p><p>12 units</p></section><input type="password" value="fixture-private-value">
                <iframe name="dynamic-frame"></iframe><script>document.querySelector('iframe').contentWindow.location.replace('FRAME_URL')</script>
                </body></html>'''.replace('FRAME_URL', f'http://127.0.0.1:{self.server.server_port}/frame').encode())
        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://localhost:{server.server_port}/product.html'
    samples = []
    try:
        for number, (prompt, label, value) in enumerate((
            ('予約できる時間と人数を調べて', '予約時間', '19:30'),
            ('Compara el precio y la fecha seleccionada', 'Precio', '$84'),
            ('تحقق من المخزون والإعدادات المختارة', 'المخزون', '12 units'),
        )):
            prompt = f'{prompt} {url}'
            report = {'goal_status': 'answered', 'next_action': 'report', 'unknowns': [], 'blocker': None,
                'observations': [{'label': label, 'value': value, 'quote': value},
                    {'label': '日付', 'value': '2026-10-10', 'quote': '2026-10-10', 'control_index': 0},
                    {'label': '人数', 'value': '2', 'quote': '2', 'control_index': 1}]}
            class Model:
                model = 'controlled-external-provider'
                browser_calls = 0
                outer_calls = 0
                def create(self, **kwargs):
                    texts = [text for m in kwargs['messages'] for text in ([m['content']] if isinstance(m.get('content'), str) else [part.get('text', '') for part in m.get('content', []) if part.get('type') == 'text'])]
                    schema_messages = [text for text in texts if text.startswith('Return valid JSON matching this schema: ')]
                    def reply(text):
                        return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': text}]}
                    if schema_messages:
                        self.browser_calls += 1
                        if self.browser_calls == 1:
                            dom = '\n'.join(texts)
                            match = re.search(r'\[(\d+)\]<button[^>]*>\s*確認', dom)
                            assert match, dom[-16000:]
                            action = {'click': {'index': int(match.group(1))}}
                        elif self.browser_calls == 2:
                            action = {'read_page_state': {}}
                        elif self.browser_calls == 3:
                            assert '2026-10-10' in '\n'.join(texts)
                            assert 'fixture-private-value' not in '\n'.join(texts)
                            assert '"selected": true' in '\n'.join(texts)
                            assert '"visible": false' in '\n'.join(texts)
                            assert '"frames": [{' in '\n'.join(texts)
                            assert f'http://127.0.0.1:{server.server_port}/frame' in '\n'.join(texts)
                            action = {'record_observations': {**report, 'observations': [{**row, 'source_url': url + '?state=x'} for row in report['observations']]}}
                        elif self.browser_calls == 4:
                            assert 'source_not_captured' in '\n'.join(texts)
                            action = {'record_observations': report}
                        elif self.browser_calls == 5:
                            action = {'select_captured_option': {'source_id': 0, 'control_index': 2, 'option_value': '18:00'}}
                        elif self.browser_calls == 6:
                            assert 'Page changed. Refresh read_page_state.' in '\n'.join(texts)
                            source_id = int(re.findall(r'"source_id":\s*(\d+)', '\n'.join(texts))[-1])
                            action = {'select_captured_option': {'source_id': source_id, 'control_index': 2, 'option_value': '18:00'}}
                        elif self.browser_calls == 7:
                            assert '"selected_value": "18:00"' in '\n'.join(texts)
                            report['observations'].append({'label': '時刻', 'value': '18:00', 'quote': '18:00', 'control_index': 2})
                            action = {'record_observations': report}
                        else:
                            assert self.browser_calls == 8
                            action = {'done': {'data': report, 'success': True}}
                        schema = json.loads(schema_messages[-1].split(': ', 1)[1])
                        payload = {'evaluation_previous_goal': 'Observed current controls', 'memory': 'Use live values',
                            'next_goal': 'Record current result', 'action': [action]}
                        if 'thinking' in schema['properties']:
                            payload['thinking'] = 'Use captured page evidence'
                        return reply(json.dumps(payload, ensure_ascii=False))
                    reviewed = source_summary_model_reply(kwargs)
                    if reviewed is not None:
                        if str(kwargs.get('system', '')).startswith('You are a semantic completion reviewer'):
                            evidence = json.loads(kwargs['messages'][0]['content'][0]['text'])['tool_evidence']
                            browser = next(row['output'] for row in evidence if row['tool_name'] == 'browser_run_task')
                            assert any(row['value'] == value for row in browser['observations'])
                            assert any(row.get('control_state') for row in browser['observations'])
                        return reviewed
                    if not kwargs.get('tools'):
                        return reply(json.dumps({'web_action': 'browser_interaction', 'requested_tool_names': ['browser_run_task'], 'required_tool_names': ['browser_run_task'], 'confidence': 1}))
                    self.outer_calls += 1
                    if any(t['name']=='present_source_answer' for t in kwargs.get('tools',[])):
                        from test_source_and_status_delivery_contracts import source_presentation_response
                        return source_presentation_response(kwargs)
                    if self.outer_calls == 1:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'scope', 'name': 'request_tool_scope',
                            'input': {'capabilities': ['web'], 'tool_names': ['browser_run_task'],
                                'required_tool_names': ['browser_run_task'], 'artifact_extensions': ['.png']}}]}
                    if self.outer_calls == 2:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'browse', 'name': 'browser_run_task',
                            'input': {'task': prompt, 'url': url, 'max_steps': 10}}]}
                    assert self.outer_calls == 3
                    return reply(f'{label}: {value}. 日付: 2026-10-10. 人数: 2.')

            runtime = bootstrap_persistent_runtime(tmp_path / f'{number}.db')
            model = Model()
            runtime.model_client = model
            registry = ToolRegistry()
            register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: model)
            app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
            started = time.monotonic()
            with TestClient(app) as client:
                response = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:controls-{number}'})
            samples.append(time.monotonic() - started)
            assert response.status_code == 200, response.text
            assert value in response.json()['text'] and '2026-10-10' in response.json()['text'], response.json()
            assert response.json()['artifacts'] == [], response.json()
            turns = [e for e in runtime.store.list_conversation_events(f'web:controls-{number}') if e.get('event_type') == 'conversation.chat_turn']
            output = next(result['output'] for result in turns[-1]['tool_results'] if result['tool_name'] == 'browser_run_task')
            assert output['rejected_observations'] == 3, output
            assert output['rejection_counts'] == {'source_not_captured': 3}, output
            assert output['report_status'] == 'completed', output
            assert {row['value'] for row in output['observations']} == {value, '2026-10-10', '2', '18:00'}, output
            assert len(output['page_url']) > 6000 and len(json.dumps(output)) < 12000, output
            assert all(descriptor['role'] == 'source' for descriptor in output['artifact_descriptors'])
            assert model.browser_calls == 8 and model.outer_calls == 3
        record_property('dynamic_browser_average_seconds', sum(samples) / len(samples))
        assert sum(samples) / len(samples) < 25, samples
        assert max(samples) < 35, samples
    finally:
        server.shutdown()
        server.server_close()

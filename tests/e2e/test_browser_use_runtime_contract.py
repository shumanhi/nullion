"""Real Browser Use -> browser -> API -> SQLite and PNG delivery in Docker."""
from __future__ import annotations

import json
import os
import threading
import time
import sys
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='container E2E only')


def response(value):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': value}]}


def source_presentation_response(kwargs):
    from nullion.prompt_injection import UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START, UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END
    message=kwargs['messages'][0]['content'][0]['text']
    raw=message.split(UNTRUSTED_TOOL_OUTPUT_BOUNDARY_START+'\n',1)[1].split('\n'+UNTRUSTED_TOOL_OUTPUT_BOUNDARY_END,1)[0]
    context=json.loads(raw)
    draft={'intro':'', 'points':[{'evidence_ids':[row['id']],
        'detail':'\n'.join(row['value'].splitlines()[1:]),'relevance':'Answers the requested comparison'} for row in context['evidence'][:7]],
        'limitation_ids':[row['id'] for row in context['limitations'][:3]]}
    return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'present','name':'present_source_answer','input':draft}]}


@pytest.mark.parametrize("isolated", [False, True])
@pytest.mark.parametrize("retry_summary", [False, True])
def test_visual_browser_task_delivers_observations_and_png_through_real_chat(tmp_path, monkeypatch, record_property, isolated, retry_summary, source_summary_model_reply):
    if not isolated:
        pytest.importorskip('browser_use')
    if isolated and os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON'):
        worker_home = tmp_path / 'installed-home'
        worker_home.mkdir()
        (worker_home / 'browser-use-venv').symlink_to(Path(os.environ['NULLION_BROWSER_USE_TEST_PYTHON']).parent.parent, target_is_directory=True)
        monkeypatch.setenv('NULLION_HOME', str(worker_home))
        monkeypatch.delenv('NULLION_BROWSER_USE_PYTHON', raising=False)
    elif isolated:
        worker_env = tmp_path / 'worker-env'
        subprocess.run([sys.executable, '-m', 'venv', '--without-pip', str(worker_env)], check=True)
        monkeypatch.setenv('NULLION_BROWSER_USE_PYTHON', str(worker_env / 'bin/python'))
    from fastapi.testclient import TestClient
    from PIL import Image
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'<html><body style="background:#102b42;color:white;font-size:24px;min-height:2400px"><h1>Rental results</h1><article><h2>Sedan A</h2><p>$80 Total</p></article><article><h2>Sedan B</h2><p>$88 Total</p></article></body></html>')
        def log_message(self, *_): pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://localhost:{server.server_port}/?context=' + 'x' * 700
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_USE_SANDBOX', 'false')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('ANONYMIZED_TELEMETRY', 'false')
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *a, **kw: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *a, **kw: None)
    report = {'goal_status': 'partial', 'next_action': 'report', 'observations': [
        {'label': 'Sedan A total', 'value': '$80', 'source_url': url, 'quote': '$80 Total'},
        {'label': 'Sedan B total', 'value': '$88', 'source_url': url, 'quote': '$88 Total'}], 'unknowns': ['Fuel type', 'After-hours return'], 'blocker': None}
    report['observations'].extend({**report['observations'][0], 'label': f'比較項目 {index}'} for index in range(28))
    assert len(json.dumps(report)) > 12_000
    samples = []
    try:
        for index, prompt in enumerate(['価格を比較して', 'Compara los precios', 'قارن الأسعار']):
            class Model:
                model = 'external-fake-provider'
                outer_calls = 0
                browser_calls = 0
                def create(self, **kwargs):
                    schema_messages = [m for m in kwargs['messages'] if isinstance(m.get('content'), str) and m['content'].startswith('Return valid JSON matching this schema: ')]
                    if schema_messages:
                        self.browser_calls += 1
                        assert kwargs['max_tokens'] >= 8192
                        assert any(isinstance(m.get('content'), list) and any(p.get('type') == 'image' for p in m['content']) for m in kwargs['messages'])
                        schema = json.loads(schema_messages[-1]['content'].split(': ', 1)[1])
                        actions = ({'record_observations': report} if self.browser_calls == 1 else {'scroll': {'down': True, 'pages': 0.8}} if self.browser_calls == 2 else {'done': {'data': report, 'success': True}})
                        value = {'evaluation_previous_goal': 'Page captured', 'memory': 'Observed both prices', 'next_goal': 'Return partial findings', 'action': [actions]}
                        if 'thinking' in schema.get('properties', {}): value['thinking'] = 'Use captured evidence'
                        # Fill optional planning fields only when present in the versioned schema.
                        for key in ('current_plan_item', 'plan_update'):
                            if key in schema.get('properties', {}): value[key] = None
                        return response(json.dumps(value))
                    tools = {tool['name'] for tool in kwargs.get('tools', [])}
                    reviewed=source_summary_model_reply(kwargs)
                    if reviewed is not None: return reviewed
                    if not tools:
                        return response(json.dumps({'web_action': 'browser_interaction', 'requested_tool_names': ['browser_run_task'], 'required_tool_names': ['browser_run_task'], 'requested_artifact_extensions': ['.png'], 'confidence': 1}))
                    if 'select_attachment_format' in tools:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'format', 'name': 'select_attachment_format', 'input': {'extension': '.png', 'confidence': 1}}]}
                    self.outer_calls += 1
                    if 'present_source_answer' in tools:
                        if retry_summary and self.outer_calls == 3:
                            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'retry','name':'browser_run_task','input':{'task':prompt,'url':url}}]}
                        return source_presentation_response(kwargs)
                    if self.outer_calls == 1:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'scope', 'name': 'request_tool_scope', 'input': {'capabilities': ['web'], 'tool_names': ['browser_run_task'], 'required_tool_names': ['browser_run_task'], 'artifact_extensions': ['.png']}}]}
                    if self.outer_calls == 2:
                        assert 'browser_run_task' in tools
                        visual = next(tool for tool in kwargs['tools'] if tool['name'] == 'browser_run_task')
                        assert 'PNG screenshots' in visual['description']
                        assert 'isolated browser session' in visual['description']
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'browse', 'name': 'browser_run_task', 'input': {'task': prompt, 'url': url, 'max_steps': 4}}]}
                    assert self.outer_calls == 3, 'No redundant verification/model retry after evidence delivery'
                    payload = json.dumps(kwargs['messages'])
                    assert 'partial' in payload, payload
                    assert '$80' in payload and '$88' in payload
                    # The tool owns the PNG; the model supplies only a user-facing answer.
                    if retry_summary:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'unnecessary-retry', 'name': 'browser_run_task', 'input': {'task': prompt, 'url': url}}]}
                    return response('Sedan A: $80 total. Sedan B: $88 total. Fuel and return policy unknown.')
            model = Model()
            runtime = bootstrap_persistent_runtime(tmp_path / f'runtime-{index}.db')
            runtime.model_client = model
            registry = ToolRegistry()
            register_browser_tools(registry, policy=BrowserPolicy(), model_client_getter=lambda: runtime.model_client)
            runtime.active_tool_registry = registry
            conversation = f'web:browser-use-{index}'
            app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
            start = time.monotonic()
            with TestClient(app) as client:
                result = client.post('/api/chat', json={'text': f'{prompt} {url} .png', 'conversation_id': conversation})
            samples.append(time.monotonic() - start)
            assert result.status_code == 200, result.text
            assert len(result.json()['artifacts']) >= 2, result.json()
            assert '$80' in result.json()['text'] and '$88' in result.json()['text'], json.dumps({'response_text': result.json()['text'], 'outer_calls': model.outer_calls, 'turns': [{k: e.get(k) for k in ('event_type', 'assistant_reply', 'reply')} for e in runtime.store.list_conversation_events(conversation)]})
            assert 'Unshown or unresolved' not in result.json()['text']
            assert 'After-hours return' in result.json()['text']
            assert 'task is still open' not in result.json()['text']
            assert 'not every requested part could be verified' not in result.json()['text']
            turns = [event for event in runtime.store.list_conversation_events(conversation) if event.get('event_type') == 'conversation.chat_turn']
            outputs = [tool['output'] for tool in turns[-1]['tool_results'] if tool['tool_name'] == 'browser_run_task']
            assert len(outputs) == 1 and outputs[0]['report_status'] == 'partial', turns[-1]
            assert len(outputs[0]['observations']) == len({row['quote'] for row in report['observations']}), 'Relabeling an identical quote must not create more findings'
            assert len(json.dumps(outputs[0], ensure_ascii=False)) < 12_000
            assert outputs[0]['sources'] and 'preview' not in outputs[0]
            paths = outputs[0]['artifact_paths']
            assert len(paths) >= 2 and all(Image.open(path).format == 'PNG' for path in paths)
            # Verify real platform preparation sees the captured image bytes.
            delivered = prepare_reply_for_platform_delivery(turns[-1]['assistant_reply'], principal_id='web_user')
            assert delivered.attachments, result.json()
            assert all(Path(attachment).read_bytes().startswith(b'\x89PNG') for attachment in delivered.attachments)
            assert model.browser_calls == 3 and model.outer_calls == (4 if retry_summary else 3)
        record_property('browser_use_average_seconds', sum(samples) / len(samples))
        assert sum(samples) / len(samples) < 20
        assert max(samples) < 30
    finally:
        server.shutdown()
        server.server_close()


def test_plain_chat_never_enters_slow_optional_browser_agent(tmp_path, monkeypatch, record_property):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin import browser_use_adapter
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    async def slow_dependency(*args, **kwargs):
        time.sleep(3)
        pytest.fail('Plain chat loaded the optional browser agent')
    monkeypatch.setattr(browser_use_adapter, 'run_browser_task', slow_dependency)
    monkeypatch.setattr(browser_use_adapter, 'browser_use_available', lambda: True)
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *a, **kw: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *a, **kw: None)
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            return response('Hello.')
    model = Model()
    runtime = bootstrap_persistent_runtime(tmp_path / 'plain.db')
    registry = ToolRegistry()
    register_browser_tools(registry, model_client_getter=lambda: model)
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)
    times = []
    with TestClient(app) as client:
        for index, prompt in enumerate(['Hello', 'こんにちは', 'Hola', 'مرحبا', 'Bonjour']):
            start = time.monotonic()
            result = client.post('/api/chat', json={'text': prompt, 'conversation_id': f'web:plain-{index}'})
            times.append(time.monotonic() - start)
            assert result.status_code == 200 and 'Hello.' in result.json()['text']
    assert model.calls == len(times)
    record_property('plain_average_seconds', sum(times) / len(times))
    record_property('plain_p95_seconds', sorted(times)[-1])
    assert sum(times) / len(times) < 0.8 and max(times) < 1.5


def test_remote_browser_resource_lease_excludes_overlapping_workers(tmp_path, monkeypatch):
    pytest.importorskip('portalocker')
    from nullion.plugins.browser_plugin.browser_use_adapter import cdp_resource_lease
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    script = 'from nullion.plugins.browser_plugin.browser_use_adapter import cdp_resource_lease\nwith cdp_resource_lease("http://127.0.0.1:19222"): print("acquired")'
    env = {**os.environ, 'PYTHONPATH': os.environ['NULLION_APP_REPO'] + '/src'}
    with cdp_resource_lease('http://127.0.0.1:19222'):
        busy = subprocess.run([sys.executable, '-c', script], env=env, capture_output=True, text=True, timeout=15)
        assert busy.returncode != 0 and 'acquired' not in busy.stdout
    free = subprocess.run([sys.executable, '-c', script], env=env, capture_output=True, text=True, timeout=15)
    assert free.returncode == 0 and free.stdout.strip() == 'acquired', free.stderr


def test_isolated_browser_can_follow_an_alternative_public_origin(tmp_path, monkeypatch):
    """Exercise the real Browser Use navigation policy, faking only its model."""
    worker_python = os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if not worker_python:
        pytest.skip('isolated Browser Use runtime required')
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask
    from nullion.plugins.browser_plugin.browser_use_worker import run_isolated_browser_task
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(b'<html><body><h1>Available at 19:30</h1></body></html>')

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    initial = f'http://localhost:{server.server_port}/'
    alternative = f'http://127.0.0.1:{server.server_port}/'
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.delenv('NULLION_BROWSER_ALLOWED_DOMAINS', raising=False)
    monkeypatch.delenv('NULLION_BROWSER_USE_CDP_URL', raising=False)
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_USE_SANDBOX', 'false')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('ANONYMIZED_TELEMETRY', 'false')
    report = {'goal_status': 'answered', 'observations': [{'label': 'Slot', 'value': '19:30', 'quote': 'Available at 19:30',
                               'source_url': alternative}], 'unknowns': [], 'next_action': 'report'}

    class Model:
        model = 'external-fake-provider'
        calls = 0

        def create(self, **kwargs):
            self.calls += 1
            schema_message = next(m for m in kwargs['messages'] if isinstance(m.get('content'), str)
                                  and m['content'].startswith('Return valid JSON matching this schema: '))
            schema = json.loads(schema_message['content'].split(': ', 1)[1])
            action = {'navigate': {'url': alternative}} if self.calls == 1 else {'done': {'data': report, 'success': True}}
            value = {'evaluation_previous_goal': 'Inspect another source', 'memory': '',
                     'next_goal': 'Capture the displayed slot', 'action': [action]}
            if 'thinking' in schema.get('properties', {}):
                value['thinking'] = 'Read the current page'
            for key in ('current_plan_item', 'plan_update'):
                if key in schema.get('properties', {}):
                    value[key] = None
            return response(json.dumps(value))

    try:
        result = run_isolated_browser_task(BrowserTask(task='別の情報源を調べて', url=initial, max_steps=3),
            client=Model(), principal_id='other-user', policy=BrowserPolicy(), timeout=60, python=worker_python)
        assert result['page_url'] == alternative, result
        assert result['observations'] and result['observations'][0]['value'] == '19:30', result
        assert all(d['role'] == 'source' for d in result['artifact_descriptors']), result
    finally:
        server.shutdown()
        server.server_close()

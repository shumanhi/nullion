"""Real Browser Use -> browser -> API -> SQLite and PNG delivery in Docker."""
from __future__ import annotations
from tests.browser_model_protocol import browser_schema_messages

from tests.workflow_provider_protocol import workflow_provider_reply
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


from tests.source_answer_fixture import source_presentation_response


# Mode recovery is exercised through real numeric user choice in
# test_browser_recovery_choice; this SDK matrix uses an explicit deployment mode.
@pytest.mark.parametrize("retry_summary,isolated,direct_evidence,carry_prior,query_deep_page",
    [(retry,isolated,direct,False,False) for retry in (False,) for isolated in (False,True) for direct in (False,True)]
    +[(retry,isolated,True,True,False) for retry in (False,) for isolated in (False,True)]
    +[(False,isolated,True,False,True) for isolated in (False,True)])
def test_visual_browser_task_delivers_observations_and_png_through_real_chat(tmp_path, monkeypatch, record_property, isolated, retry_summary, source_summary_model_reply, direct_evidence, carry_prior, query_deep_page, answered_record=False):
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

    main_page_cookies = []
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith('/?context='):
                main_page_cookies.append(self.headers.get('Cookie',''))
            if self.path.startswith('/blocked'):
                self.send_response(403);self.end_headers();self.wfile.write(b'Access blocked for this source');return
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            if self.path.startswith('/?context='):
                cookie = 'source-session=; Max-Age=0; Path=/' if len(main_page_cookies) == 2 else 'source-session=existing; Max-Age=3600; Path=/'
                self.send_header('Set-Cookie', cookie)
            self.end_headers()
            filler="".join("<div>Unrelated row "+str(i)+"</div>" for i in range(1000)) if query_deep_page else ""
            self.wfile.write(('<html><body style="background:#102b42;color:white;font-size:24px;min-height:2400px"><h1>Rental results</h1>'+filler+'<article><h2>Sedan A</h2><p>$80 Total</p></article><article><h2>Sedan B</h2><p>$88 Total</p></article></body></html>').encode())
        def log_message(self, *_): pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://localhost:{server.server_port}/?context=' + 'x' * 700
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_USE_SANDBOX', 'false')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('ANONYMIZED_TELEMETRY', 'false')
    monkeypatch.setattr(web_app, '_schedule_web_builder_reflection', lambda *a, **kw: None)
    monkeypatch.setattr(web_app, '_schedule_web_no_tool_memory_capture', lambda *a, **kw: None)
    report = {'goal_status': 'partial', 'next_action': 'report', 'observations': [
        {'label': 'Sedan A total', 'value': '$80', 'source_url': url, 'quote': '$80 Total'},
        {'label': 'Sedan B total', 'value': '$88', 'source_url': url, 'quote': '$88 Total'}], 'unknowns': ['Fuel type', 'After-hours return'], 'blocker': None}
    if answered_record:report['goal_status']='answered'
    report['observations'].extend({**report['observations'][0], 'label': f'比較項目 {index}'} for index in range(28))
    assert len(json.dumps(report)) > 12_000
    samples = []
    try:
        for index, prompt in enumerate(['価格を比較して', 'Compara los precios', 'قارن الأسعار']):
            request_visual = bool(direct_evidence and not carry_prior and not query_deep_page and index == 0)
            class Model:
                model = 'external-fake-provider'
                outer_calls = 0
                browser_calls = 0
                def create(self, **kwargs):
                    protocol_reply = workflow_provider_reply(kwargs, completion=True)
                    if protocol_reply is not None: return protocol_reply
                    schema_messages = browser_schema_messages(kwargs)
                    if schema_messages:
                        self.browser_calls += 1
                        assert kwargs['max_tokens'] >= 8192
                        has_image = any(isinstance(m.get('content'), list) and any(p.get('type') == 'image' for p in m['content']) for m in kwargs['messages'])
                        assert has_image == bool(request_visual and self.browser_calls > 1), 'Images belong to a structured screenshot request, not every decision'
                        schema = json.loads(schema_messages[-1]['content'].split(': ', 1)[1])
                        current_capture=next((m['content'] for m in kwargs['messages'] if isinstance(m.get('content'),str) and m['content'].startswith('Current captured page evidence')), '')
                        blocked_step=carry_prior and '/blocked' in current_capture
                        if carry_prior and not blocked_step:
                            handed_off=json.dumps(kwargs['messages'],ensure_ascii=False)
                            assert 'Prior source step context' in handed_off and 'HTTP 403' in handed_off
                        if direct_evidence and not blocked_step and not query_deep_page:
                            captured_messages = [m['content'] for m in kwargs['messages'] if isinstance(m.get('content'), str) and m['content'].startswith('Current captured page evidence')]
                            assert captured_messages and '$80 Total' in captured_messages[-1]
                            assert 'untrusted source data' in captured_messages[-1]
                        if answered_record and self.browser_calls>1:
                            time.sleep(3)  # Formerly repeated inference after a validated terminal record.
                            raise AssertionError('A validated answered/report record must end the worker')
                        actions = ({'done': {'data': report, 'success': True}} if direct_evidence else {'record_observations': report} if self.browser_calls == 1 else {'scroll': {'down': True, 'pages': 0.8}} if self.browser_calls == 2 else {'done': {'data': report, 'success': True}})
                        if request_visual and self.browser_calls == 1:
                            actions = {'screenshot':{}}
                        if query_deep_page:
                            if self.browser_calls==1:
                                assert '$80 Total' not in current_capture
                                actions={'read_page_state':{'query':'Sedan A'}}
                            else:
                                assert self.browser_calls==2
                                captured_search=json.dumps(kwargs['messages'])
                                assert 'matched_line_indices' in captured_search and '$80 Total' in captured_search and '$88 Total' in captured_search
                                actions={'done':{'data':report,'success':True}}
                        if blocked_step:actions={'done':{'data':{'goal_status':'blocked','next_action':'alternative_source','observations':[], 'unknowns':['First source could not be read'],'blocker':'HTTP 403 from first source'},'success':False}}
                        value = {'evaluation_previous_goal': 'Page captured', 'memory': 'Observed both prices', 'next_goal': 'Return partial findings', 'action': [actions]}
                        if 'thinking' in schema.get('properties', {}): value['thinking'] = 'Use captured evidence'
                        # Fill optional planning fields only when present in the versioned schema.
                        for key in ('current_plan_item', 'plan_update'):
                            if key in schema.get('properties', {}): value[key] = None
                        assert [tool['name'] for tool in kwargs['tools']] == ['browser_decision']
                        assert not any(isinstance(m.get('content'),str) and m['content'].startswith('Return valid JSON matching this schema: ') for m in kwargs['messages'])
                        return {'stop_reason':'tool_use','content':[{'type':'tool_use','name':'browser_decision','id':'decision-'+str(self.browser_calls),'input':value}]}
                    tools = {tool['name'] for tool in kwargs.get('tools', [])}
                    reviewed=source_summary_model_reply(kwargs)
                    if reviewed is not None: return reviewed
                    if not tools:
                        return response(json.dumps({'web_action': 'browser_interaction', 'requested_tool_names': ['browser_run_task'], 'required_tool_names': ['browser_run_task'], 'requested_artifact_extensions': [] if carry_prior else ['.png'], 'confidence': 1}))
                    if 'select_attachment_format' in tools:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'format', 'name': 'select_attachment_format', 'input': {'extension': '.png', 'confidence': 1}}]}
                    self.outer_calls += 1
                    if 'present_source_answer' in tools:
                        if retry_summary and self.outer_calls == 3:
                            return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'retry','name':'browser_run_task','input':{'task':prompt,'url':url}}]}
                        return source_presentation_response(kwargs)
                    if self.outer_calls == 1:
                        selected = ['browser_run_task']
                        if carry_prior:
                            selected = ['browser_read_pages', *selected]
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'scope', 'name': 'request_tool_scope', 'input': {'capabilities': ['web'], 'tool_names': selected, 'required_tool_names': selected, 'artifact_extensions': [] if carry_prior else ['.png'], 'source_artifact_delivery_requested': not carry_prior, 'source_artifact_request_evidence': '' if carry_prior else f'{prompt} {url} .png'}}]}
                    if self.outer_calls == 2:
                        assert 'browser_run_task' in tools
                        visual = next(tool for tool in kwargs['tools'] if tool['name'] == 'browser_run_task')
                        assert 'PNG screenshots' in visual['description']
                        assert 'existing login' in visual['description']
                        return {'stop_reason': 'tool_use', 'content': [
                            *([{'type':'tool_use','id':'native-first','name':'browser_read_pages','input':{
                                'independent_pages':True,'pages':[{'url':url,'read_kind':'text'},
                                {'url':f'http://localhost:{server.server_port}/blocked','read_kind':'text'}]}}] if carry_prior else []),
                            *([{'type':'tool_use','id':'first-source','name':'browser_run_task','input':{'task':'Read the first source and report any access blocker.','url':f'http://localhost:{server.server_port}/blocked','max_steps':2,
                                'execution_kind':'adaptive_interaction','interaction_reason':'Inspect the native access failure with the selected visual controller.'}}] if carry_prior else []),
                            {'type': 'tool_use', 'id': 'browse', 'name': 'browser_run_task', 'input': {'task': prompt, 'url': url, 'max_steps': 4,
                                **({'execution_kind':'adaptive_interaction','interaction_reason':'Visually compare the observed options after native preflight.'} if carry_prior else {})}}]}
                    assert self.outer_calls == 3, f'No redundant verification/model retry after evidence delivery: {self.outer_calls}'
                    payload = json.dumps(kwargs['messages'])
                    assert 'partial' in payload, payload
                    assert '$80' in payload and '$88' in payload, {'has80':'$80' in payload,'has88':'$88' in payload,'deep_page':query_deep_page}
                    # The tool owns the PNG; the model supplies only a user-facing answer.
                    if retry_summary:
                        return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': 'unnecessary-retry', 'name': 'browser_run_task', 'input': {'task': prompt, 'url': url}}]}
                    from tests.source_answer_fixture import native_png_directives
                    return response('Sedan A: $80 total. Sedan B: $88 total. Fuel type and After-hours return unknown.' + ('' if carry_prior else native_png_directives(kwargs, latest_only=True)))
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
                payload = {'text': f'{prompt} {url}'+('' if carry_prior else ' .png'), 'conversation_id': conversation}
                if isolated and (direct_evidence or answered_record) and not carry_prior and not query_deep_page and index == 0:
                    import httpx
                    events = []
                    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'true')
                    with client.websocket_connect('/ws/chat') as socket:
                        socket.send_json({**payload, 'turn_id':'measured-browser', 'activity_trace':True})
                        while True:
                            event = socket.receive_json(); events.append(event)
                            if event['type'] == 'done': break
                    if answered_record:
                        assert model.browser_calls==1, f'Worker made {model.browser_calls} decisions after an answered/report handoff'
                    progress = [event for event in events if event.get('id') == 'source-progress']
                    assert progress and progress[0]['detail'] == 'Pages checked: 1 · Findings: 0', events
                    assert progress[0]['activity_phase']=='reading_pages'
                    if answered_record:
                        assert any(e.get('activity_phase')=='finishing_source_review' for e in progress),progress
                    assert events.index(progress[0]) < next(i for i,e in enumerate(events) if e['type'] == 'chunk')
                    result = httpx.Response(200, json={'text': ''.join(e.get('text','') for e in events if e['type'] == 'chunk'),
                        'artifacts':events[-1].get('artifacts',[])})
                else:
                    result = client.post('/api/chat', json=payload)
            samples.append(time.monotonic() - start)
            if isolated and direct_evidence and not carry_prior and not query_deep_page and index == 1:
                saved = list((tmp_path / 'workspaces').rglob('storage-state.json'))
                assert saved and all(not json.loads(path.read_text()).get('cookies') for path in saved), f'Sign-out snapshot retained a cookie; request cookies: {main_page_cookies}'
            assert result.status_code == 200, result.text
            assert (not result.json()['artifacts']) if carry_prior else len(result.json()['artifacts']) == 1, result.json()
            assert '$80' in result.json()['text'] and '$88' in result.json()['text'], json.dumps({'response_text': result.json()['text'], 'outer_calls': model.outer_calls, 'turns': [{k: e.get(k) for k in ('event_type', 'assistant_reply', 'reply', 'error', 'tool_results')} for e in runtime.store.list_conversation_events(conversation)]})
            assert 'Unshown or unresolved' not in result.json()['text']
            assert 'After-hours return' in result.json()['text']
            assert 'task is still open' not in result.json()['text']
            assert 'not every requested part could be verified' not in result.json()['text']
            turns = [event for event in runtime.store.list_conversation_events(conversation) if event.get('event_type') == 'conversation.chat_turn']
            outputs = [tool['output'] for tool in turns[-1]['tool_results'] if tool['tool_name'] == 'browser_run_task']
            last_native = [tool for tool in turns[-1]['tool_results'] if tool['tool_name'] == 'browser_run_task'][-1]
            receipt = (last_native.get('response_requirements') or {}).get('_native_completed_reads')
            if carry_prior:
                assert receipt and receipt['read_only'] is True and receipt['invocation_id']==last_native['invocation_id']
                assert 'browser_extract_text' in receipt['tool_names']
            else:
                assert not receipt, 'A delivered PNG is a write, so the composite must not inherit read-only authority'
            assert len(outputs) == (2 if carry_prior else 1), turns[-1]
            if carry_prior:assert outputs[0]['report_status']=='blocked'
            outputs=outputs[-1:]
            assert outputs[0]['report_status']=='partial',outputs
            if direct_evidence and not carry_prior and not query_deep_page:
                assert outputs[0]['captured_document_count'] <= 3 + int(request_visual), 'Do not recapture around every SDK inference'
            assert len(outputs[0]['observations']) == len({row['quote'] for row in report['observations']}), 'Relabeling an identical quote must not create more findings'
            assert len(json.dumps(outputs[0], ensure_ascii=False)) < 12_000
            assert outputs[0]['sources'] and 'preview' not in outputs[0]
            paths = outputs[0]['artifact_paths']
            assert len(paths) >= 1 and all(Image.open(path).format == 'PNG' for path in paths)
            # Verify real platform preparation sees the captured image bytes.
            delivered = prepare_reply_for_platform_delivery(turns[-1]['assistant_reply'], principal_id='web_user')
            assert (not delivered.attachments) if carry_prior else delivered.attachments, result.json()
            assert all(Path(attachment).read_bytes().startswith(b'\x89PNG') for attachment in delivered.attachments)
            if query_deep_page:assert 'read_page_state' in outputs[0]['execution_audit']['action_names']
            assert model.browser_calls == (1 if answered_record else 2 if carry_prior or query_deep_page else 1 + int(request_visual) if direct_evidence else 1) and model.outer_calls == (4 if retry_summary else 3)
            timings = [e for e in runtime.store.list_conversation_events(conversation)
                if e.get('event_type') == 'conversation.tool_timing' and e.get('tool_name') == 'browser_run_task']
            assert timings and timings[-1]['performance']['model_calls'] >= 1
            assert timings[-1]['performance']['model_ms'] >= 0 and timings[-1]['performance']['capture_ms'] >= 0
        if isolated and direct_evidence and not carry_prior and not query_deep_page:
            assert len(main_page_cookies) >= 3
            assert 'source-session=existing' not in main_page_cookies[0]
            assert 'source-session=existing' in main_page_cookies[1], 'Workspace-owned state must survive separate SDK worker processes'
            assert 'source-session=existing' not in main_page_cookies[2], f'Sign-out must not resurrect a cookie from the previous snapshot: {main_page_cookies}'
            snapshots = list((tmp_path / 'workspaces').rglob('storage-state.json'))
            assert snapshots and all(path.stat().st_mode & 0o077 == 0 for path in snapshots)
        record_property('browser_use_average_seconds', sum(samples) / len(samples))
        assert sum(samples) / len(samples) < 20
        assert max(samples) < 30
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize('isolated',[False,True])
def test_answered_record_handoff_ends_worker_without_redundant_inference(tmp_path,monkeypatch,
        record_property,source_summary_model_reply,isolated):
    test_visual_browser_task_delivers_observations_and_png_through_real_chat(tmp_path,monkeypatch,
        record_property,isolated,False,source_summary_model_reply,False,False,False,answered_record=True)


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
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
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


@pytest.mark.parametrize("crowded", [False, True, 'stalled'])
def test_isolated_browser_can_follow_an_alternative_public_origin(tmp_path, monkeypatch, crowded):
    """Exercise the real Browser Use navigation policy, faking only its model."""
    worker_python = os.environ.get('NULLION_BROWSER_USE_TEST_PYTHON')
    if not worker_python:
        pytest.skip('isolated Browser Use runtime required')
    from nullion.plugins.browser_plugin.browser_use_adapter import BrowserTask
    from nullion.plugins.browser_plugin.browser_use_worker import run_isolated_browser_task
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy

    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            if crowded=='stalled' and self.path in {'/json/version','/json/list'}:
                value=[] if self.path=='/json/list' else {'webSocketDebuggerUrl':cdp_socket_url}
                self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers()
                self.wfile.write(json.dumps(value).encode());return
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
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_USE_SANDBOX', 'false')
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('ANONYMIZED_TELEMETRY', 'false')
    report = {'goal_status': 'answered', 'observations': [{'label': 'Slot', 'value': '19:30', 'quote': 'Available at 19:30',
                               'source_url': alternative}], 'unknowns': [], 'next_action': 'report'}

    class Model:
        model = 'external-fake-provider'
        calls = 0

        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            self.calls += 1
            schema_message = browser_schema_messages(kwargs)[0]
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

    from contextlib import ExitStack
    owned_resources = ExitStack()
    pages = []
    if crowded is True:
        import socket
        from playwright.sync_api import sync_playwright
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            cdp_port = listener.getsockname()[1]
        playwright = owned_resources.enter_context(sync_playwright())
        browser = playwright.chromium.launch(headless=True, args=[f'--remote-debugging-port={cdp_port}'])
        owned_resources.callback(browser.close)
        context = browser.new_context()
        pages = [context.new_page() for _ in range(55)]
        monkeypatch.setenv('NULLION_BROWSER_USE_CDP_URL', f'http://127.0.0.1:{cdp_port}')
    elif crowded=='stalled':
        from websockets.sync.server import serve
        def unresponsive(connection):
            try:
                for _ in connection:pass
            except Exception:pass
        cdp_server=owned_resources.enter_context(serve(unresponsive,'127.0.0.1',0))
        threading.Thread(target=cdp_server.serve_forever,daemon=True).start()
        cdp_socket_url=f'ws://127.0.0.1:{cdp_server.socket.getsockname()[1]}/devtools/browser/stalled'
        monkeypatch.setenv('NULLION_BROWSER_USE_CDP_URL',f'http://127.0.0.1:{server.server_port}')
    try:
        result = run_isolated_browser_task(BrowserTask(task='別の情報源を調べて', url=initial, max_steps=3),
            client=Model(), principal_id='other-user', policy=BrowserPolicy(), timeout=60, python=worker_python)
        if result.get('page_url')!=alternative:print(json.dumps(result,ensure_ascii=False))
        assert result['page_url'] == alternative, result
        assert result['observations'] and result['observations'][0]['value'] == '19:30', result
        assert all(d['role'] == 'source' for d in result['artifact_descriptors']), result
        if crowded is True:
            assert result['browser_session_recovery'] == {'kind':'isolated_capacity_fallback',
                'shared_browser_modified':False, 'shared_session_reused':False}
            assert len(context.pages) == 55
            assert all(not page.is_closed() and page.url == 'about:blank' for page in pages)
        elif crowded=='stalled':
            assert result['browser_session_recovery'] == {'kind':'isolated_startup_fallback',
                'configured_endpoint_changed':False, 'shared_session_reused':False}
    finally:
        owned_resources.close()
        server.shutdown()
        server.server_close()


@pytest.fixture(autouse=True)
def explicit_independent_sdk_fixture(monkeypatch):
    # These anonymous-provider fixtures exercise the independent SDK mode.
    # Managed login preservation has its own real shared-CDP/cookie contract.
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.delenv('NULLION_BROWSER_CDP_URL', raising=False)

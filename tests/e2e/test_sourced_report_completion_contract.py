"""Sourced reports retain browser work, complete comparisons, and deliver readable PDFs."""
from __future__ import annotations

import html
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E only')


def response(text):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': text}]}


def model_evidence(messages):
    records = []
    names = {b.get('id'): b.get('name') for m in messages for b in m.get('content', []) if isinstance(b, dict) and b.get('type') == 'tool_use'}
    for message in messages:
        for block in message.get('content', []):
            if isinstance(block, dict) and block.get('type') == 'tool_result':
                for part in block.get('content', []):
                    try:
                        record = json.loads(part.get('text', ''))
                        record['tool_name'] = names.get(block.get('tool_use_id'))
                        records.append(record)
                    except ValueError:
                        pass
    return records


@pytest.mark.parametrize('prompt', [
    'Compare all three AAA–BBB direct travel windows and attach report.pdf.',
    'Compara las tres ventanas AAA–BBB sin escalas y adjunta report.pdf.',
])
def test_real_browser_report_recovers_missing_comparisons_and_pdf_arguments(tmp_path, monkeypatch, prompt, record_property):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.browser_tools import _run
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, _build_pdf_create_handler
    from pypdf import PdfReader

    queries = []
    windows = ['2026-10-14/2026-10-18', '2026-10-21/2026-10-25', '2026-10-28/2026-11-01']
    costs = [210, 180, 240]
    class Page(BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            route = query.get('route', [''])[0]
            window = query.get('window', [''])[0]
            mode = query.get('mode', [''])[0]
            record = '<h1>Explore destinations</h1><p>Suggested trips for unrelated dates.</p>'
            if route == 'AAA-BBB' and window in windows and mode == 'direct':
                queries.append((route, window, mode))
                record = f'<article id="result">WINDOW {window}; AAA-BBB; direct; Carrier Q; 09:00–12:00; USD {costs[windows.index(window)]}</article>'
            form = '<form><input id="route" name="route"><input id="window" name="window"><input id="mode" name="mode"><button id="search">Search</button></form>'
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(f'<html><body>{form}{record}</body></html>'.encode())
        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Page)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://research.localhost:{server.server_port}/?lang=en#route=ignored;direct=1'
    source_link = f'{url};currency=USD;party=1'
    dns = socket.getaddrinfo
    monkeypatch.setattr(socket, 'getaddrinfo', lambda host, *args, **kw: dns('127.0.0.1' if host == 'research.localhost' else host, *args, **kw))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_HEADLESS', 'true')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    register_browser_tools(registry)
    registry.register(ToolSpec('pdf_create', 'Create PDF report', ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 30),
        _build_pdf_create_handler(tmp_path, include_principal_workspace=False))
    backend = registry._handlers['browser_navigate'].__self__._backend
    pdf = artifact_root_for_principal('operator') / 'report.pdf'

    class Model:
        step = 0
        review_calls = 0
        planner_calls = 0
        def create(self, **kwargs):
            system = str(kwargs.get('system', ''))
            if system.startswith('You are a material-requirements planner'):
                self.planner_calls += 1
                requirements = [{'requirement': f'Compare direct AAA-BBB {window} with carrier, schedule and cost', 'requirement_kind': 'comparison_side',
                    'evidence_subject_id': f'window-{i}', 'eligible_evidence_tool_names': ['browser_extract_text']} for i, window in enumerate(windows)]
                requirements.append({'requirement': 'Deliver a readable PDF with all comparisons and the cheapest option', 'requirement_kind': 'deliverable',
                    'evidence_subject_id': 'report', 'eligible_evidence_tool_names': ['pdf_create']})
                return response(json.dumps({'all_material_requirements_enumerated': True, 'material_requirements': requirements}))
            if system.startswith('You are a semantic completion reviewer'):
                self.review_calls += 1
                payload = json.loads(kwargs['messages'][0]['content'][0]['text'])
                evidence = payload['tool_evidence']
                matching = [r for r in evidence if r.get('tool_name') == 'browser_extract_text' and 'WINDOW ' in json.dumps(r.get('output'))]
                if len(matching) < 3:
                    assert 'unavailable' in json.dumps(payload['artifact_evidence']).lower()
                    return response(json.dumps({'disposition': 'retry', 'unresolved_requirements': ['Matching records for every comparison are missing'],
                        'retry_tool_names': ['browser_snapshot']}))
                completed_pdf = [r for r in evidence if r.get('tool_name') == 'pdf_create' and r.get('status') == 'completed'][-1]
                material = []
                for i, row in enumerate(payload['material_requirement_contract']):
                    record = matching[i] if i < 3 else completed_pdf
                    material.append({**row, 'satisfied': True, 'draft_covered': True, 'evidence_invocation_ids': [record['invocation_id']]})
                return response(json.dumps({'disposition': 'complete', 'all_material_requirements_enumerated': True,
                    'material_requirements': material, 'unresolved_requirements': [], 'retry_tool_names': []}))
            names = {t['name'] for t in kwargs.get('tools', [])}
            self.step += 1
            assert {'browser_type_field', 'browser_click_element', 'browser_extract_text', 'pdf_create'} <= names, 'Research disappeared after raw page text'
            if self.step == 1:
                calls = [('browser_navigate', {'url': url}), ('browser_extract_text', {})]
            elif self.step == 2:
                calls = [('pdf_create', {'output_path': str(pdf), 'text_pages': ['# Comparison\n\nPrices unavailable.'], 'title': 'Comparison'})]
            elif self.step == 3:
                return response(f'Done — attached report.pdf.\nMEDIA:{pdf}')
            elif self.step == 4:
                assert self.review_calls == 1, json.dumps({'planner': self.planner_calls, 'history': kwargs['messages']})
                calls = [('browser_snapshot', {})]
            elif 5 <= self.step <= 7:
                window = windows[self.step - 5]
                calls = [('browser_type_field', {'selector': '#route', 'text': 'AAA-BBB'}),
                    ('browser_type_field', {'selector': '#window', 'text': window}),
                    ('browser_type_field', {'selector': '#mode', 'text': 'direct'}),
                    ('browser_click_element', {'selector': '#search'}), ('browser_extract_text', {})]
            elif self.step == 8:
                calls = [('pdf_create', {'title': 'Comparison'})]
            elif self.step == 9:
                assert 'pdf_create requires at least one' in json.dumps(kwargs['messages'])
                records = model_evidence(kwargs['messages'])
                matching = [r for r in records if r.get('tool_name') == 'browser_extract_text' and 'WINDOW ' in json.dumps(r.get('output'))]
                assert len(matching) == 3
                body = '# Direct route comparison\n\n## Requested windows\n\n| Window | Cost |\n| --- | --- |\n'
                body += ''.join(f'| **{window}** | USD {cost} |\n' for window, cost in zip(windows, costs))
                body += '\n' + '\n\n'.join(r['output']['text'] for r in matching)
                body += f'\n\n*Cheapest:* {windows[1]} — USD 180\n\n[Open source]({source_link})'
                calls = [('pdf_create', {'output_path': str(pdf), 'text_pages': [body], 'title': 'Comparison'})]
            elif self.step == 10:
                return response(f'Compared all three windows. Cheapest: {windows[1]}, USD 180.\nMEDIA:{pdf}')
            else:
                raise AssertionError(f'Unexpected recovery loop {self.step}')
            return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': f't-{self.step}-{i}', 'name': name, 'input': args}
                for i, (name, args) in enumerate(calls)]}

    model = Model()
    started = time.perf_counter()
    try:
        result = AgentOrchestrator(model_client=model).run_turn(conversation_id='telegram:report-user', principal_id='operator',
            user_message=prompt + ' ' + url, conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store,
            max_iterations=18, tool_flow_context={'requires_artifact_delivery': True, 'required_artifact_extensions': ['.pdf']})
        assert not result.reached_iteration_limit and not result.suspended_for_approval, result.final_text
        assert result.response_fulfilled is True, result.final_text
        assert model.review_calls == 2 and model.step == 10
        assert queries == [('AAA-BBB', window, 'direct') for window in windows]
        reader = PdfReader(pdf)
        text = '\n'.join(page.extract_text() for page in reader.pages)
        assert all(window in text for window in windows)
        assert all(f'USD {cost}' in text for cost in costs)
        assert 'Carrier Q' in text and '09:00' in text and 'Cheapest:' in text
        assert 'unavailable' not in text.lower() and '**' not in text and '##' not in text
        uris = [a.get_object().get('/A', {}).get('/URI') for p in reader.pages for a in p.get('/Annots', [])]
        assert source_link in uris
        delivered = prepare_reply_for_platform_delivery(result.final_text, principal_id='operator')
        assert pdf in delivered.attachments
        assert delivered.attachments[0].read_bytes() == pdf.read_bytes()
        assert 'Cheapest' in delivered.text and 'unavailable' not in delivered.text.lower()
        failures = [r for r in result.tool_results if r.tool_name == 'pdf_create' and r.status == 'failed']
        assert len(failures) == 1 and failures[0].output['reason'] == 'invalid_tool_arguments'
        events = runtime.store.list_conversation_events('telegram:report-user')
        reviews = [e for e in events if e.get('event_type') == 'conversation.completion_review']
        assert [e['disposition'] for e in reviews] == ['retry', 'complete']
        record_property('elapsed_seconds', time.perf_counter() - started)
    finally:
        server.shutdown()
        server.server_close()
        for session in list(backend._pages):
            _run(backend.close_session(session))


def test_optional_research_and_pdf_tools_keep_plain_turns_on_fast_path(tmp_path, monkeypatch, record_property):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry = ToolRegistry()
    def unused_tool(call):
        raise AssertionError('Plain chat must not start optional research')
    for name in ['browser_navigate', 'browser_extract_text', 'pdf_create', 'weather_forecast']:
        registry.register(ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5), unused_tool)
    elapsed = []
    for i, prompt in enumerate(['Hello', 'Hola', 'こんにちは', 'Thanks', 'Gracias', 'ありがとう']):
        class Model:
            calls = 0
            def create(self, **kwargs):
                self.calls += 1
                if self.calls > 1 or not kwargs.get('tools'):
                    time.sleep(2)  # Previously slow review must stay outside ordinary chat.
                    raise AssertionError('Unexpected classifier or completion review')
                return response('Hello.')
        model = Model()
        runtime = bootstrap_persistent_runtime(tmp_path / f'plain-{i}.db')
        started = time.perf_counter()
        result = AgentOrchestrator(model_client=model).run_turn(conversation_id=f'web:plain-{i}', principal_id='operator',
            user_message=prompt, conversation_history=[], tool_registry=registry, policy_store=runtime.store, approval_store=runtime.store)
        elapsed.append(time.perf_counter() - started)
        assert result.final_text == 'Hello.' and not result.tool_results
        assert model.calls == 1
    average = sum(elapsed) / len(elapsed)
    p95 = sorted(elapsed)[-1]
    record_property('plain_average_seconds', average)
    record_property('plain_p95_seconds', p95)
    assert average < 0.5 and p95 < 1.5

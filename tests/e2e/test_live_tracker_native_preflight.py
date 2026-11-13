"""Real API, native browser, prepared aliases, admission and persisted review."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


@pytest.mark.parametrize('concern', ['native_contract'])
def test_native_prepared_aliases_are_not_replaced_by_model_suggestions(tmp_path, monkeypatch, concern):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.live_tracking import LiveTrackerStore
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry

    visits, packets = [], []
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            visits.append(self.path)
            body = ('<main id="reading">' + ('23' if self.path == '/a' else '37') + '</main>').encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args): pass
    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env'))
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_CHAT_STARTUP_WARMUP', '0')
    runtime = setup_runtime(tmp_path, monkeypatch)
    registry = ToolRegistry()
    register_browser_tools(registry)
    register_live_tracker_tools(registry)
    store = LiveTrackerStore(tmp_path/'runtime.db')
    registry.live_tracker_store_path = store.path
    probes = [{'tool_name': 'browser_extract_text', 'arguments': {
        'selector': '#reading', 'ready_selector': '#reading', 'session_id': 'créateur-' + key}} for key in ('a', 'b')]
    phase = 0
    class Provider:
        def create(self, **kw):
            nonlocal phase
            if 'Workflow action preflight.' in kw.get('system', ''):
                packet = json.loads(kw['messages'][0]['content'][0]['text'])
                if packet['proposed_action']['tool_name'] == 'create_live_tracker':
                    packets.append(packet)
                    facts = packet['adapter_target_evidence']['context']['native_contract_validation']
                    assert facts['validated'] and len(facts['prepared_bindings']) == 2
                    assert [r['argument_session_alias'] for r in facts['prepared_bindings']] == ['créateur-a', 'créateur-b']
                    assert all(r['observed_physical_session'] != r['argument_session_alias'] for r in facts['prepared_bindings'])
                    assert facts['declared_defaults']['notify_on_expiry'] is True
                    decision = 'clarify' if concern == 'explicit_constraint' else 'revise'
                    return text_response(json.dumps({'decision': decision, 'issues': ['Corrige el plan.'],
                        'concern_scopes': [concern], **({'question': '¿Autorizar el destino?',
                            'options': ['Autorizar', 'Conservar sin cambios']} if decision == 'clarify' else {})}))
                return text_response(json.dumps({'decision': 'proceed', 'issues': []}))
            protocol = workflow_provider_reply(kw)
            if protocol is not None:
                return protocol
            phase += 1
            if phase == 1:
                return tool_response('request_tool_scope', {'capabilities': ['web', 'live_tracking'],
                    'tool_names': ['browser_navigate', 'browser_extract_text', 'create_live_tracker']})
            if phase in (2, 4):
                key = 'a' if phase == 2 else 'b'
                return tool_response('browser_navigate', {'url': f'http://127.0.0.1:{source.server_port}/' + key,
                    'session_id': 'créateur-' + key})
            if phase in (3, 5):
                step = probes[0 if phase == 3 else 1]
                return tool_response(step['tool_name'], step['arguments'])
            if phase == 6:
                return tool_response('create_live_tracker', {'name': '比較', 'duration_seconds': 90, 'interval_seconds': 60,
                    'probe': probes, 'matcher': {'kind': 'structured', 'summary': '照合完了',
                        'conditions': [{'step': 0, 'pointer': '/text', 'op': 'equals', 'value': '24'},
                                       {'step': 1, 'pointer': '/text', 'op': 'equals', 'value': '37'}]}})
            return text_response('El seguimiento está activo durante 90 segundos, con comprobaciones cada minuto.')
    cid = 'web:native-preflight'
    try:
        app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=Provider()), registry=registry)
        with TestClient(app) as api:
            response = api.post('/api/chat', json={'text': '/chat これらの条件で90秒間監視してください。', 'conversation_id': cid})
            assert response.status_code == 200, response.text
            events = runtime.store.list_conversation_events(cid)
            reviews = [row for row in events if row.get('event_type') == 'conversation.action_review'
                       and row.get('tool_name') == 'create_live_tracker']
            jobs = store.list('workspace_admin')
            assert not packets and not reviews
            if concern == 'explicit_constraint':
                assert not jobs and '1.' in response.json()['text'] and '2.' in response.json()['text']
                assert reviews[0]['decision'] == 'clarify' and not reviews[0]['advisory']
            else:
                assert len(jobs) == 1 and jobs[0]['probe'] == probes, (jobs, response.json())
                assert jobs[0]['notify_on_expiry'] is True
                turn = next(e for e in reversed(events) if e.get('event_type') == 'conversation.chat_turn')
                creates = [r for r in turn['tool_results'] if r['tool_name'] == 'create_live_tracker']
                assert len(creates) == 1 and creates[0]['status'] == 'completed'
                assert 'activo' in response.json()['text'] and visits[:2] == ['/a', '/b']
                # The background owner may begin its first legitimate check.
                # No producer read or native create should be repeated.
                for name in ('browser_navigate', 'browser_extract_text'):
                    assert len([r for r in turn['tool_results'] if r['tool_name'] == name]) == 2
    finally:
        registry.run_cleanup_hooks()
        source.shutdown()
        source.server_close()

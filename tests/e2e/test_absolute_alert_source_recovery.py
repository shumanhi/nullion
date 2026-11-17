"""Real chat, browser, durable tracker and worker after an oversized source read."""
import json
import os
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from tests.e2e.test_live_tracker_workflow import live_fixture, text, use, wait_for
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')

@pytest.mark.parametrize('prompt', ['サービスが復旧したら通知。30分ごとに24時間確認。',
                                   'Avísame cuando se restablezca el servicio; cada 30 minutos durante 24 horas.'])
def test_absolute_alert_recovers_large_link_index_and_delivers_only_confirmation(live_fixture, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.live_tracking_tools import register_live_tracker_tools
    state = SimpleNamespace(restored=False, phase=0)
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            status = 'Servicio restablecido' if state.restored else 'Interrupción en curso'
            links = ''.join(f'<a href="/reference/{i}?opaque={"x"*150}">Referencia {i}</a>' for i in range(160))
            body = f'<main><h1>Estado del servicio</h1><p>{status}</p>{links}</main>'.encode()
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers(); self.wfile.write(body)
        def log_message(self, *_): pass
    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{source.server_port}/'
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    f = live_fixture
    register_browser_tools(f.registry)
    probe = {'tool_name': 'browser_extract_text', 'arguments': {'ready_selector': 'main', 'selector': 'main'}}
    def provider(**kw):
        if kw.get('system', '').startswith('Evaluate only the supplied current observations'):
            assert 'a complete relevant source that reports an ongoing problem' in kw['system']
            packet = json.loads(kw['messages'][0]['content'][0]['text']); out = packet['observations'][0]
            assert 'links' not in out and out['page_url'] == url
            if 'Servicio restablecido' in out['text']:
                return text(json.dumps({'status': 'matched', 'summary': 'Servicio restablecido — '+url,
                    'evidence': [{'step': 0, 'pointer': '/text', 'kind': 'quote', 'value': 'Servicio restablecido'}]}))
            return text(json.dumps({'status': 'no_match'}))
        reply = workflow_provider_reply(kw)
        if reply is not None: return reply
        state.phase += 1
        if state.phase == 1:
            return use('request_tool_scope', {'capabilities': ['web', 'live_tracking'], 'tracking_mode': 'live',
                'tool_names': ['browser_navigate', 'browser_extract_text', 'create_live_tracker']})
        if state.phase == 2: return use('browser_navigate', {'url': url})
        if state.phase in (3, 5): return use(probe['tool_name'], probe['arguments'])
        if state.phase in (4, 6):
            return use('create_live_tracker', {'name': 'Recuperación', 'duration_seconds': 86400,
                'interval_seconds': 1800, 'stop_after_match': True, 'probe': [probe],
                'matcher': {'kind': 'semantic', 'history_mode': 'records',
                            'instruction': 'Avisar solo cuando la página confirme que el servicio se ha restablecido.'}})
        return text('Seguimiento activo.')
    # Select the lossless text projection only after the first admission fails.
    original = provider
    def recovering(**kw):
        if state.phase == 4 and kw.get('tools'):
            probe['arguments']['include_links'] = False
        return original(**kw)
    f.model.create = recovering
    register_live_tracker_tools(f.registry, model_client_getter=lambda: f.model)
    app = create_app(f.runtime, f.orchestrator, f.registry)
    try:
        with TestClient(app) as api:
            cid = 'web:absolute-recovery'
            response = api.post('/api/chat', json={'text': prompt+' '+url, 'conversation_id': cid})
            assert response.status_code == 200, response.text
            store = app.state.live_tracker_store; jobs = store.list('workspace_admin')
            assert len(jobs) == 1, response.json()
            job = jobs[0]; assert job['interval_seconds'] == 1800
            turn = next(r for r in reversed(f.runtime.store.list_conversation_events(cid)) if r.get('event_type') == 'conversation.chat_turn')
            creates = [r for r in turn['tool_results'] if r['tool_name'] == 'create_live_tracker']
            assert [r['status'] for r in creates] == ['failed', 'completed'], creates
            failure = creates[0]['output']; assert failure['validation']['code'] == 'observation_too_large'
            schema = failure['recovery']['argument_constraints']['probe_input_schemas']['browser_extract_text']
            assert schema['properties']['include_links']['type'] == 'boolean'
            assert turn['response_fulfilled'] is True
            wait_for(lambda: store.get(job['id'])['checks'] >= 1, timeout=20)
            saved = store.get(job['id']); assert saved['last_outcome'] == 'no_match'
            state.restored = True
            due = datetime.fromisoformat(saved['next_check_at'])+timedelta(seconds=1)
            claimed, token = store.claim(4, now=due)
            app.state.live_tracker_engine.check_once(claimed, token)
            wait_for(lambda: store.get(job['id'])['notification_status'] == 'sent', timeout=20)
            from nullion.chat_store import get_chat_store
            alerts = [m for m in get_chat_store().load_messages(cid) if (m.get('metadata') or {}).get('source') == 'live_tracker']
            assert len(alerts) == 1 and 'Servicio restablecido' in alerts[0]['text'] and url in alerts[0]['text']
    finally:
        f.registry.run_cleanup_hooks(); source.shutdown(); source.server_close()

"""Actual Web setup, browser reads, SQLite polling and notification delivery."""
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading

import pytest

from tests.e2e.test_scheduled_report_completion import setup_runtime, text_response, tool_response
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker workflow')


@pytest.mark.parametrize('condition,label', [('Notify when the vote total changes; ignore the clock.', 'Votes='),
    ('時刻の表示を無視し、票数の合計が変わったら通知してください。', '票数=')])
@pytest.mark.parametrize('preview_selector', ['body', 'a'])
def test_incomplete_preview_is_repaired_before_tracker_admission_and_changes_alert_after_restart(tmp_path, monkeypatch, condition, label, preview_selector):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.live_tracking import LiveTrackerEngine, utcnow
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.tools import ToolRegistry
    source = {'votes': 10, 'clock': 1}; visits = []
    class Server(BaseHTTPRequestHandler):
        def do_GET(self):
            visits.append(self.path)
            body = ''.join(f'<a href="/menu/{i}">Menu {i}</a>' for i in range(60))
            body += '<main id="results"><h1>Election sample</h1>' + '<p>Context</p>' * 40
            body += '</main><script>setTimeout(()=>document.querySelector("#results").insertAdjacentHTML("beforeend",'
            body += json.dumps(f'<p>{label}{source["votes"]}; Clock={source["clock"]}</p>') + '),250);</script>'
            self.send_response(200);self.send_header('Content-Type', 'text/html; charset=utf-8');self.end_headers();self.wfile.write(body.encode())
        def log_message(self, *args): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Server)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{server.server_port}/'
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright');monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path/'none.env'));monkeypatch.setenv('NULLION_MEMORY_ENABLED', 'false')
    runtime = setup_runtime(tmp_path, monkeypatch);registry = ToolRegistry();register_browser_tools(registry)
    phase = 0; evaluations = []
    class Provider:
        def create(self, **kw):
            nonlocal phase
            if str(kw.get('system', '')).startswith('Evaluate only the supplied current observations'):
                p = json.loads(kw['messages'][0]['content'][0]['text']);evaluations.append(p)
                value = p['observations'][0]['text']; history = p['snapshot_history']
                if history['baseline_established'] and label+'11' in value:
                    return text_response(json.dumps({'status':'matched', 'summary':'Vote total increased from 10 to 11.',
                        'evidence':[{'step':0, 'pointer':'/text', 'kind':'quote', 'value':label+'11'}]}))
                return text_response('{"status":"no_match"}')
            protocol = workflow_provider_reply(kw)
            if protocol is not None:return protocol
            phase += 1
            if phase == 1:return tool_response('request_tool_scope', {'capabilities':['web','scheduler'],
                'tool_names':['browser_navigate','browser_find','browser_extract_text','create_live_tracker']})
            if phase == 2:return tool_response('browser_navigate', {'url':url})
            if phase == 3:return tool_response('browser_find', {'selector':preview_selector})
            if phase == 5:return tool_response('browser_extract_text', {'selector':'#results', 'ready_text':label})
            if phase in (4, 6):
                return tool_response('create_live_tracker', {'name':'票数の変化', 'stop_after_match':True,'duration_seconds':3600,
                    'interval_seconds':5, 'source_refresh':'navigate', 'probe':[{
                        'tool_name':'browser_find' if phase == 4 else 'browser_extract_text',
                        'arguments':({'selector':preview_selector} if phase == 4 else
                            {'selector':'#results', 'ready_text':label})}],
                    'matcher':{'kind':'semantic','history_mode':'snapshot','instruction':condition}})
            return text_response('The tracker is monitoring changes. The initial result is its baseline.')
    provider = Provider();register_live_tracker_tools(registry, model_client_getter=lambda:provider)
    registry.live_tracker_store_path = tmp_path/'runtime.db'
    app = web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=provider), registry=registry)
    try:
        with TestClient(app) as api:
            app.state.live_tracker_engine.stop()
            response = api.post('/api/chat', json={'text':'/chat '+condition, 'conversation_id':'web:comparison'})
            assert response.status_code == 200, response.text
            jobs = app.state.live_tracker_store.list('workspace_admin');assert len(jobs) == 1
            job = jobs[0];assert job['comparison_snapshot']['outputs'][0]['text'].endswith(label+'10; Clock=1')
            turns = [r for r in runtime.store.list_conversation_events('web:comparison') if r.get('event_type') == 'conversation.chat_turn']
            attempts = [r for r in turns[-1]['tool_results'] if r['tool_name'] == 'create_live_tracker']
            assert [r['status'] for r in attempts] == ['failed','completed']
            assert attempts[0]['output']['validation']['code'] == 'incomplete_observation'
            assert len(evaluations) == 1, 'The invalid preview must not consume an evaluator call'
            # Recreate the worker between checks: persisted snapshot and portable
            # source setup must survive the creator's transient browser scope.
            for votes in [10, 11]:
                source.update(votes=votes, clock=source['clock']+1)
                engine = LiveTrackerEngine(app.state.live_tracker_store, registry=registry,
                    runtime_store=runtime.store, model_client=provider, deliver=app.state.live_tracker_engine.deliver)
                current, token = app.state.live_tracker_store.claim(4, now=utcnow()+timedelta(seconds=10))
                engine.check_once(current, token)
                current = app.state.live_tracker_store.get(job['id'])
                assert current['state'] == ('active' if votes == 10 else 'matched'), json.dumps({'job':current,'evaluations':evaluations},ensure_ascii=False)
            assert evaluations[-1]['snapshot_history']['previous_observations'][0]['text'].endswith(label+'10; Clock=2')
            assert len(visits) >= 3
            notices = app.state.live_tracker_store.claim_notice()
            assert notices, current
            engine._send(notices)
            history = api.get('/api/chat/history/web:comparison').json()
            assert any('Vote total increased from 10 to 11' in r.get('text','') for r in history['messages']), history
    finally:
        registry.run_cleanup_hooks();server.shutdown();server.server_close()

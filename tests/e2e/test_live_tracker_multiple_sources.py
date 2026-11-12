"""A two-page tracker keeps both live DOMs and delivers their combined result."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker browser workflow')


def test_two_streaming_sources_keep_their_own_tabs_across_checks(tmp_path, monkeypatch):
    from playwright.sync_api import sync_playwright, expect
    from nullion import chat_store, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.live_tracking import LiveTrackerStore
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_ENV_FILE', str(tmp_path / 'missing.env'))
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    chat_store._store = None
    values, loads = {'a': 'Á=23', 'b': '乙=37'}, []
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            key = self.path.rsplit('/', 1)[-1]
            if self.path.startswith('/data/'):
                body = values[key]
            else:
                loads.append(key)
                body = '<main id="reading-' + key + '">' + values[key] + '</main>'
                body += '<script>setInterval(async()=>{document.getElementById("reading-' + key + '")'
                body += '.textContent=await(await fetch("/data/' + key + '",{cache:"no-store"})).text()},100);</script>'
            data = body.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def log_message(self, *args): pass
    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    creator, worker = ToolRegistry(), ToolRegistry()
    register_browser_tools(creator)
    register_browser_tools(worker)
    register_live_tracker_tools(creator)
    store = LiveTrackerStore(tmp_path / 'runtime.db')
    creator.live_tracker_store_path = store.path
    owner = 'web:source-pair'
    context = {'conversation_id': owner, 'browser_session_scope': 'creator'}
    probe, prepared = [], {}
    try:
        for key in ('a', 'b'):
            nav = creator.invoke(ToolInvocation('open-' + key, 'browser_navigate', owner,
                {'url': f'http://127.0.0.1:{source.server_port}/' + key, 'session_id': 'seed-' + key}, flow_context=context))
            assert nav.status == 'completed', nav
            step = {'tool_name': 'browser_extract_text', 'arguments': {
                'selector': '#reading-' + key, 'ready_selector': '#reading-' + key, 'session_id': 'seed-' + key}}
            read = creator.invoke(ToolInvocation('read-' + key, step['tool_name'], owner, step['arguments'], flow_context=context))
            assert read.status == 'completed' and read.output['text'] == values[key], read
            probe.append(step)
            prepared[json.dumps(step, sort_keys=True)] = read.output
        created = creator.invoke(ToolInvocation('create-pair', 'create_live_tracker', owner, {
            'name': '比較', 'duration_seconds': 90, 'interval_seconds': 5, 'source_refresh': 'reuse',
            'stop_after_match': True, 'probe': probe, 'matcher': {'kind': 'structured', 'summary': '照合完了',
                'conditions': [{'step': 0, 'pointer': '/text', 'op': 'equals', 'value': 'Á=24'},
                               {'step': 1, 'pointer': '/text', 'op': 'equals', 'value': '乙=37'}]}},
            flow_context={**context, 'prepared_observations': prepared}))
        assert created.status == 'completed', created
        ident = created.output['id']
        store.confirm_start_receipt(ident, store.get(ident)['expires_at'])
        app = web_app.create_app(runtime, AgentOrchestrator(model_client=object()), worker)
        with _UvicornThread(app, _free_port()) as server, sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
            try:
                ctx = browser.new_context()
                ctx.add_init_script('localStorage.setItem("nullion_conv_id", ' + json.dumps(owner) + ');')
                page = ctx.new_page()
                page.goto(server.base_url)
                deadline = time.monotonic() + 20
                while store.get(ident)['checks'] < 2:
                    assert time.monotonic() < deadline, store.get(ident)
                    page.wait_for_timeout(100)
                assert store.get(ident)['consecutive_errors'] == 0, store.get(ident)
                assert store.get(ident)['last_outcome'] == 'no_match', store.get(ident)
                assert sorted(loads) == ['a', 'a', 'b', 'b'], loads
                values['a'] = 'Á=24'
                expect(page.locator('#messages')).to_contain_text('照合完了', timeout=20000)
                deadline = time.monotonic() + 5
                while store.get(ident)['notification_status'] != 'sent':
                    assert time.monotonic() < deadline, store.get(ident)
                    page.wait_for_timeout(100)
                assert store.get(ident)['state'] == 'matched'
                assert sorted(loads) == ['a', 'a', 'b', 'b'], loads
                history = chat_store.get_chat_store().load_messages(owner)
                alerts = [row for row in history if '照合完了' in row.get('text', '')]
                assert len(alerts) == 1 and 'Á=24' in alerts[0]['text'] and '乙=37' in alerts[0]['text'], alerts
            finally:
                browser.close()
    finally:
        creator.run_cleanup_hooks()
        worker.run_cleanup_hooks()
        source.shutdown()
        source.server_close()

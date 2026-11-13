"""Actual Web route, rendered browser source, tracker worker and SQLite alert."""
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
from types import SimpleNamespace

import pytest

from tests.e2e.test_live_tracker_workflow import live_fixture, text, use, wait_for
from tests.workflow_provider_protocol import workflow_provider_reply

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container E2E')


def objects(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from objects(child)
    elif isinstance(value, str):
        start, end = value.find('{'), value.rfind('}')
        if start >= 0 and end > start:
            try:
                decoded = json.loads(value[start:end+1])
            except ValueError:
                return
            yield from objects(decoded)


@pytest.mark.parametrize('label,prompt', [('見出し', '15分ごとに4時間、このページの新しい記事を通知してください。'),
    ('Titular', 'Avísame de nuevos artículos de esta página cada 15 minutos durante 4 horas.')])
def test_missing_scope_and_invalid_review_recover_to_a_working_fifteen_minute_tracker(live_fixture, monkeypatch, label, prompt):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion import crons
    f = live_fixture
    state = SimpleNamespace(fresh=False, phase=0, reviews=0, selector=None, probe=None, source_reads=0)
    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/image.svg':
                self.send_response(200); self.send_header('Content-Type', 'image/svg+xml'); self.end_headers()
                self.wfile.write(b'<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96"><rect width="96" height="96"/></svg>')
                return
            state.source_reads += 1
            nav = ''.join(f'<a href="/menu/{i}">Menu {i}</a>' for i in range(142))
            cards = ''.join(f'<div><a class="visual" href="/entry/{i}"><img width="96" height="96" src="/image.svg">Credit {i}</a>'
                f'<a href="/entry/{i}"><span class="entry-name">{label} {i}</span></a></div>'
                for i in range(56 if state.fresh else 55))
            self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers()
            self.wfile.write(f'<header><nav>{nav}</nav></header><div><h2>{label}</h2>{cards}</div>'.encode())
        def log_message(self, *_):
            pass
    source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{source.server_port}/'
    monkeypatch.setenv('NULLION_BROWSER_BACKEND', 'playwright')
    monkeypatch.setenv('NULLION_BROWSER_BLOCK_PRIVATE', 'false')
    register_browser_tools(f.registry)
    def create_tracker():
        return use('create_live_tracker', {'name': 'Selected new records', 'stop_after_match':False, 'interval_seconds': 900,
            'duration_seconds': 14400, 'source_refresh': 'navigate', 'probe': state.probe,
            'matcher': {'kind': 'semantic', 'instruction': 'Notify on newly appearing record URLs after the baseline; ignore existing records.'}})
    def provider(**kw):
        system = kw.get('system', '')
        if 'Workflow action preflight.' in system:
            state.reviews += 1
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            assert not f.registry.live_tracker_store_path is None
            from nullion.live_tracking import LiveTrackerStore
            assert LiveTrackerStore(f.registry.live_tracker_store_path).list('workspace_admin') == []
            if state.reviews == 1:
                assert packet['proposed_action']['arguments']['probe'][0]['arguments']['max_items'] == 40
                return text(json.dumps({'decision': 'revise', 'issues': [
                    'The prepared observation is truncated. Select and verify a complete relevant source scope before saving it.']}))
            if state.reviews == 2:
                return text('{')
            assert packet['proposed_action']['arguments']['interval_seconds'] == 900
            assert packet['protocol_validation_errors'] == ['invalid_json']
            return text(json.dumps({'decision': 'proceed', 'issues': [], 'question': None, 'options': None}))
        if system.startswith('Evaluate only the supplied current observations'):
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            out = packet['observations'][0]
            assert out['coverage']['truncated'] is False and 55 <= len(out['items']) <= 56
            assert all(row['title'].startswith(label) for row in out['items'])
            fresh = next((r for r in packet['observation_history']['records'] if r['status'] == 'new'), None)
            if fresh:
                item = out['items'][int(fresh['pointer'].rsplit('/', 1)[1])]
                return text(json.dumps({'status': 'matched', 'summary': item['title']+' — '+item['canonical_url'],
                    'evidence': [{'step': 0, 'pointer': fresh['pointer']+'/canonical_url', 'value': item['canonical_url']}]}))
            return text(json.dumps({'status': 'no_match', 'checked_scopes': [
                {'step': 0, 'section_id': row['id']} for row in out['coverage']['sections']]
                or [{'step': 0, 'section_id': None}]}))
        reply = workflow_provider_reply(kw)
        if reply is not None:
            return reply
        if not kw.get('tools'):
            return text('{}')
        state.phase += 1
        if state.phase == 1:
            return use('request_tool_scope', {'capabilities': ['web', 'live_tracking'], 'tracking_mode': 'live',
                'tool_names': ['browser_navigate', 'browser_extract_items', 'create_live_tracker']})
        if state.phase == 2:
            return use('browser_navigate', {'url': url})
        if state.phase == 3:
            return use('browser_extract_items', {'selector': 'main#not-present'})
        if state.phase == 4:
            hints = [row for obj in objects(kw['messages']) for row in obj.get('available_scopes', [])]
            assert any('entry-name' in row['selector'] for row in hints), hints
            return use('browser_extract_items', {'max_items': 40, 'fields': ['title', 'canonical_url']})
        if state.phase == 5:
            hints = [row for obj in objects(kw['messages']) for row in obj.get('available_scopes', [])]
            state.selector = next(row['selector'] for row in hints if row['visible_link_count'] == 55
                and all(sample.startswith(label) for sample in row['sample_text']))
            state.probe = [{'tool_name': 'browser_extract_items', 'arguments': {
                'max_items': 40, 'fields': ['title', 'canonical_url']}}]
            return create_tracker()
        if state.phase == 6:
            state.probe = [{'tool_name': 'browser_extract_items', 'arguments': {
                'selector': state.selector, 'max_items': 100, 'fields': ['title', 'canonical_url']}}]
            return use('browser_extract_items', state.probe[0]['arguments'])
        if state.phase == 7:
            return create_tracker()
        return text('Watching the selected page.')
    f.model.create = provider
    app = create_app(f.runtime, f.orchestrator, f.registry)
    try:
        with TestClient(app) as client:
            cid = 'web:scope-recovery'
            response = client.post('/api/chat', json={'text': prompt+' '+url, 'conversation_id': cid})
            assert response.status_code == 200, response.text
            turns_now = [r for r in f.runtime.store.list_conversation_events(cid) if r.get('event_type') == 'conversation.chat_turn']
            assert 'Live tracker' in response.json()['text'], (response.json(), [r for t in turns_now
                for r in t.get('tool_results', []) if r['tool_name'] == 'create_live_tracker'])
            store = app.state.live_tracker_store
            jobs = store.list('workspace_admin')
            assert len(jobs) == 1 and jobs[0]['interval_seconds'] == 900
            job = jobs[0]
            setup_agent_calls = state.phase
            assert (datetime.fromisoformat(job['expires_at'])-datetime.fromisoformat(job['created_at'])).total_seconds() == 14400
            assert crons.list_crons() == [] and state.reviews == 0
            wait_for(lambda: store.get(job['id'])['checks'] >= 1, timeout=20)
            saved = store.get(job['id'])
            assert saved['last_outcome'] == 'no_match' and saved['observation_baseline_set']
            due = datetime.fromisoformat(saved['next_check_at'])
            assert store.claim(4, now=due-timedelta(seconds=1)) is None
            state.fresh = True
            claimed, token = store.claim(4, now=due+timedelta(seconds=1))
            app.state.live_tracker_engine.check_once(claimed, token)
            wait_for(lambda: store.get(job['id'])['notification_status'] == 'sent', timeout=12)
            assert store.get(job['id'])['state'] == 'active'
            assert store.get(job['id'])['stop_after_match'] is False
            from nullion.chat_store import get_chat_store
            alerts = [m for m in get_chat_store().load_messages(cid) if (m.get('metadata') or {}).get('source') == 'live_tracker']
            assert len(alerts) == 1 and url+'entry/55' in alerts[0]['text']
            assert 'Continue tracking' not in alerts[0]['text']
            turns = [r for r in f.runtime.store.list_conversation_events(cid) if r.get('event_type') == 'conversation.chat_turn']
            results = turns[-1]['tool_results']
            assert any(r['tool_name'] == 'browser_extract_items' and
                (r.get('output') or {}).get('coverage', {}).get('candidate_count') == 197 and
                (r.get('output') or {}).get('coverage', {}).get('returned_count') == 40 for r in results), results
            assert any(r['tool_name'] == 'browser_extract_items' and r['status'] == 'failed' for r in results)
            assert sum(r['tool_name'] == 'create_live_tracker' and r['status'] == 'completed' for r in results) == 1
            reviews = [r for r in f.runtime.store.list_conversation_events(cid) if r.get('event_type') == 'conversation.action_review']
            assert reviews == []
            assert state.phase == setup_agent_calls, 'Polling must not restart the chat agent'
    finally:
        f.registry.run_cleanup_hooks()
        source.shutdown(); source.server_close()

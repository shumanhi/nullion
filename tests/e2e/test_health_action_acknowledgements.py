"""Health status acknowledgements through SQLite, Telegram HTTP and Web UI."""
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
from types import SimpleNamespace
from urllib.parse import parse_qs

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker product boundary')


def seed(runtime, action_id='alert-1', *, summary='要確認', code='investigate_stall', status='pending'):
    runtime.store.add_doctor_action({
        'action_id': action_id, 'action_type': 'investigate', 'owner': 'doctor',
        'recommendation_code': code, 'status': status, 'summary': summary,
        'severity': 'medium', 'reason': 'source=runtime;detail=/private/internal.log',
        'source_reason': 'source=runtime;conversation_id=private:123;detail=/private/internal.log',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'updated_at': datetime.now(timezone.utc).isoformat(),
    })
    return action_id


@contextmanager
def telegram_boundary():
    calls = []
    class Telegram(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
            if 'application/json' in self.headers.get('Content-Type', ''):
                payload = json.loads(body)
            else:
                payload = {k: v[0] for k, v in parse_qs(body.decode()).items()}
            method = self.path.rsplit('/', 1)[-1]
            calls.append((method, payload))
            result = True
            if method == 'getMe':
                result = {'id': 700, 'is_bot': True, 'first_name': 'Test', 'username': 'test_bot'}
            elif method in {'editMessageText', 'sendMessage'}:
                result = {'message_id': int(payload.get('message_id', len(calls)+900)), 'date': 1,
                    'chat': {'id': 42, 'type': 'private'}, 'text': payload['text']}
            response = json.dumps({'ok': True, 'result': result}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response)))
            self.end_headers()
            self.wfile.write(response)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Telegram)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/bot', calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize('command,expected_status,expected_text', [
    ('complete', 'completed', 'Doctor alert marked as resolved.'),
    ('cancel', 'cancelled', 'Doctor alert dismissed.'),
    ('start', 'in_progress', 'Doctor alert marked as in progress.'),
])
def test_real_telegram_callback_edits_doctor_card_with_scoped_status(tmp_path, monkeypatch, command, expected_status, expected_text):
    from telegram import Bot, Update
    from nullion import telegram_app
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    action_id = seed(runtime)
    settings = NullionSettings(telegram=TelegramSettings(operator_chat_id='42', chat_enabled=True))
    service = telegram_app.ChatOperatorService(runtime, '123456:synthetic', '42', settings)
    with telegram_boundary() as (base_url, calls):
        async def scenario():
            async with Bot('123456:synthetic', base_url=base_url) as bot:
                payload = {'update_id': 1, 'callback_query': {
                    'id': 'callback-1', 'chat_instance': 'test',
                    'from': {'id': 42, 'is_bot': False, 'first_name': 'Operator'},
                    'data': telegram_app._build_callback_data(kind='doctor', action=command, record_id=action_id),
                    'message': {'message_id': 21282, 'date': 1, 'chat': {'id': 42, 'type': 'private'},
                        'from': {'id': 700, 'is_bot': True, 'first_name': 'Test'},
                        'text': 'Doctor: workflow needs review',
                        'reply_to_message': {'message_id': 21281, 'date': 1, 'chat': {'id': 42, 'type': 'private'},
                            'text': '調査をお願いします', 'from': {'id': 42, 'is_bot': False, 'first_name': 'Operator'}}},
                }}
                await service.on_callback_query(Update.de_json(payload, bot), SimpleNamespace(bot=bot))
                if command in {'complete', 'cancel'}:
                    payload['update_id'] = 2
                    payload['callback_query']['id'] = 'callback-2'
                    await service.on_callback_query(Update.de_json(payload, bot), SimpleNamespace(bot=bot))
        asyncio.run(scenario())
    edits = [payload for method, payload in calls if method == 'editMessageText']
    assert edits and edits[0]['text'].startswith(expected_text)
    assert all(str(edit['message_id']) == '21282' for edit in edits)
    assert all('Action completed.' != edit['text'] for edit in edits)
    assert all('/private/' not in edit['text'] and 'private:123' not in edit['text'] for edit in edits)
    if command in {'complete', 'cancel'}:
        assert 'already' in edits[-1]['text']
    assert runtime.store.get_doctor_action(action_id)['status'] == expected_status
    runtime.checkpoint(force=True)
    stored = load_runtime_store(tmp_path/'runtime.db')
    assert stored.get_doctor_action(action_id)['status'] == expected_status


@pytest.mark.parametrize('command,status,phrase', [
    ('complete', 'completed', 'marked as resolved'), ('dismiss', 'cancelled', 'alert dismissed'),
    ('start', 'in_progress', 'marked as in progress'),
])
def test_web_health_status_result_matches_real_store_and_operator_command(tmp_path, monkeypatch, command, status, phrase):
    from fastapi.testclient import TestClient
    from nullion.operator_commands import handle_operator_command
    from tests.e2e.test_web_surface_contracts import _web_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    app, runtime, _ = _web_app(tmp_path)
    seed(runtime, 'web-alert')
    seed(runtime, 'chat-alert', summary='تحتاج إلى مراجعة')
    with TestClient(app) as client:
        result = client.post(f'/api/doctor/web-alert/{command}').json()
        assert result['ok'] is True
        assert phrase in result['message']
        assert result['effect'] == 'alert_status'
        assert result['repair_verified'] is False
        assert '/private/' not in result['message']
        assert runtime.store.get_doctor_action('web-alert')['status'] == status
        text = handle_operator_command(runtime, f'/doctor {command} chat-alert')
        assert phrase in text
        assert runtime.store.get_doctor_action('chat-alert')['status'] == status
        if command != 'start':
            repeated = client.post(f'/api/doctor/web-alert/{command}').json()
            assert 'already' in repeated['message']
            assert 'No new action was run' in repeated['message']


def test_web_repair_request_does_not_resolve_alert_and_cannot_be_enabled_by_prose(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import web_app
    from tests.e2e.test_web_surface_contracts import _web_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    app, runtime, _ = _web_app(tmp_path)
    requests = []
    monkeypatch.setattr(web_app, '_restart_chat_services', lambda: requests.append(True) or 'Restart request accepted')
    with TestClient(app) as client:
        seed(runtime, 'valid', code='telegram_bot_unreachable', summary='接続を確認')
        seed(runtime, 'prose', code='unknown', summary='Telegram typing indicator stuck')
        runtime.checkpoint(force=True)
        status = client.get('/api/status?full=1').json()
        indexed = {item['action_id']: item for item in status['doctor_actions']}
        assert 'valid' in indexed, {'visible': indexed, 'stored': runtime.store.get_doctor_action('valid'), 'status_keys': list(status)}
        assert indexed['valid']['can_try_fix'] is True
        assert indexed['prose']['can_try_fix'] is False
        denied = client.post('/api/doctor/prose/repair')
        assert denied.status_code == 409
        assert requests == []
        result = client.post('/api/doctor/valid/repair').json()
        assert result['effect'] == 'operation_requested'
        assert result['repair_verified'] is False
        assert 'not been verified' in result['message']
        assert runtime.store.get_doctor_action('valid')['status'] == 'in_progress'
        client.post('/api/doctor/valid/complete')
        client.post('/api/doctor/valid/repair')
        assert requests == [True]


@pytest.mark.parametrize('command', ['doctor:unknown', 'doctor:restart_plugin', 'doctor:disable_plugin', 'doctor:pause_chat'])
def test_web_guidance_never_closes_alert_or_claims_execution(tmp_path, monkeypatch, command):
    from fastapi.testclient import TestClient
    from tests.e2e.test_web_surface_contracts import _web_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    app, runtime, _ = _web_app(tmp_path)
    seed(runtime)
    with TestClient(app) as client:
        response = client.post('/api/doctor/alert-1/'+command)
        result = response.json()
        assert response.status_code == (400 if command == 'doctor:unknown' else 200)
        assert result['repair_verified'] is False
        assert result['effect'] in {'unsupported', 'guidance'}
        assert runtime.store.get_doctor_action('alert-1')['status'] == 'pending'


def test_web_ui_resolution_notice_keeps_doctor_context(tmp_path, monkeypatch):
    from playwright.sync_api import expect
    from tests.e2e.test_web_surface_contracts import _web_app, _UvicornThread, _free_port, _open_page

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    app, runtime, _ = _web_app(tmp_path)
    seed(runtime, summary='要確認')
    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.evaluate('refreshDashboard()')
            page.locator('#attention-chip').click()
            expect(page.locator('#attention-body .control-card')).to_have_count(1)
            page.locator('#attention-body button', has_text='Mark resolved').click()
            expect(page.locator('#messages')).to_contain_text('Doctor alert marked as resolved.')
            expect(page.locator('#messages')).to_contain_text('does not verify a repair')
            expect(page.locator('#messages')).not_to_contain_text('Action completed.')
            expect(page.locator('#attention-body .control-card')).to_have_count(0)
            assert runtime.store.get_doctor_action('alert-1')['status'] == 'completed'
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_sentinel_status_receipts_are_explicit_and_match_persisted_state(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.sentinel_escalations import create_sentinel_escalation
    from tests.e2e.test_web_surface_contracts import _web_app

    monkeypatch.setenv('NULLION_HOME', str(tmp_path/'home'))
    app, runtime, _ = _web_app(tmp_path)
    escalation = create_sentinel_escalation('doctor', 'source=test', 'high', summary='要確認')
    runtime.store.add_sentinel_escalation(escalation)
    with TestClient(app) as client:
        url = f'/api/sentinel/{escalation.escalation_id}'
        reviewed = client.post(url+'/acknowledge').json()
        assert 'Sentinel alert marked as reviewed' in reviewed['message']
        assert reviewed['repair_verified'] is False
        resolved = client.post(url+'/resolve').json()
        assert 'Sentinel alert marked as resolved' in resolved['message']
        assert 'does not verify a repair' in resolved['message']
        repeated = client.post(url+'/resolve').json()
        assert 'already resolved' in repeated['message']
        assert runtime.store.get_sentinel_escalation(escalation.escalation_id).status.value == 'resolved'

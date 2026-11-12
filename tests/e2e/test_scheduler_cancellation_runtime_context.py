"""Actual chat ingress, scheduler tools, SQLite and approval execution.

Only the external model and Telegram transport are deterministic boundaries.
"""
from tests.workflow_provider_protocol import workflow_provider_reply
import asyncio
import json
import os
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


def text(value):
    return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': value}]}


def use(name, arguments):
    return {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use', 'id': name, 'name': name, 'input': arguments}]}


def fixture(tmp_path, monkeypatch):
    from nullion import crons, chat_store
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import build_default_tool_registry, register_cron_tools
    from nullion.agent_orchestrator import AgentOrchestrator
    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', 'false')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'false')
    chat_store._store = None
    monkeypatch.setattr(crons, '_RUNTIME_DB_PATH', tmp_path / 'runtime.db')
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    registry = build_default_tool_registry(workspace_root=tmp_path, allowed_roots=[tmp_path])
    register_cron_tools(registry)
    runtime.active_tool_registry = registry

    class Model:
        phase = 'create'
        invoked = False
        authorization = 'authorized'
        guard_packets = []
        calls = []
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names = {row['name'] for row in kw.get('tools', [])}
            self.calls.append((self.phase, sorted(names)))
            if not names:
                if '"scheduler_action":' in str(kw.get('system') or ''):
                    action = {'create': 'create_cron', 'cancel': 'delete_cron', 'pause': 'toggle_cron'}[self.phase]
                    return text(json.dumps({'scheduler_action': 'mutate', 'requested_tool_names': [action],
                        'required_tool_names': [action], 'confidence': 1.0}))
                try:
                    packet = json.loads(kw['messages'][0]['content'][0]['text'])
                except (ValueError, KeyError, TypeError):
                    return text('{}')
                if packet.get('candidate_action') == 'delete_cron':
                    self.guard_packets.append(packet)
                    job = crons.list_crons()[0]
                    supporting = [r for r in packet.get('recent_conversation', []) if job.id in r['scheduler_object_ids']]
                    return text(json.dumps({'authorized': bool(supporting) and self.authorization == 'authorized', 'target_id': job.id,
                        'user_evidence': packet['user_request'], 'target_context_ref': supporting[-1]['ref'] if supporting else None,
                        'reason': self.authorization if supporting else 'ambiguous_target'}))
                return text('{}')
            action = {'create': 'create_cron', 'cancel': 'delete_cron', 'pause': 'toggle_cron'}[self.phase]
            if not self.invoked:
                if action not in names:
                    return use('request_tool_scope', {'capabilities': ['scheduler_mutate'], 'tool_names': [action],
                        'required_tool_names': [action], 'scheduler_action': 'mutate'})
                self.invoked = True
                if action == 'create_cron':
                    return use(action, {'name': 'Sensor watcher', 'schedule': '0 12 * * *',
                        'task': 'Check the synthetic sensor reading.', 'enabled': False})
                job = crons.list_crons()[0]
                return use(action, {'id': job.id, **({'enabled': False} if action == 'toggle_cron' else {})})
            if self.phase == 'create':
                return text('Created Sensor watcher; it is paused.')
            return text('Sensor watcher is paused.' if self.phase == 'pause' else 'Sensor watcher was canceled.')
    model = Model()
    return SimpleNamespace(runtime=runtime, registry=registry, model=model,
        orchestrator=AgentOrchestrator(model_client=model))


@pytest.mark.parametrize('followup', ['Cancel it', 'Cancélalo', 'それを取り消して'])
def test_web_create_then_pronoun_cancel_keeps_target_through_real_approval(tmp_path, monkeypatch, followup):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion import crons
    f = fixture(tmp_path, monkeypatch)
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        response = client.post('/api/chat', json={'text': 'Create a paused sensor watcher every day at noon.',
            'conversation_id': 'web:cancel-context', 'turn_id': 'created'})
        assert response.status_code == 200, response.text
        jobs = crons.list_crons(); assert len(jobs) == 1, (response.json(), f.model.calls)
        job = jobs[0]
        f.model.phase, f.model.invoked = 'cancel', False
        response = client.post('/api/chat', json={'text': followup, 'conversation_id': 'web:cancel-context', 'turn_id': 'cancel'})
        assert response.status_code == 200, response.text
        pending = [a for a in f.runtime.store.list_approval_requests() if a.status.value == 'pending']
        assert len(pending) == 1, (response.json(), f.model.guard_packets, f.model.calls)
        assert crons.get_cron(job.id) is not None
        from nullion.approval_display import approval_display_from_request
        display = approval_display_from_request(pending[0])
        assert job.name in display.detail and job.id in display.detail
        assert f.model.guard_packets[-1]['recent_conversation'][0]['scheduler_object_ids'] == [job.id]
        approved = client.post('/api/approve/' + pending[0].approval_id, json={'mode': 'once'})
        assert approved.status_code == 200, approved.text
        assert crons.get_cron(job.id) is None, approved.json()
        assert 'missing schedule' not in response.text.lower()
        f.runtime.checkpoint(force=True)
        from nullion.runtime_persistence import load_runtime_store
        assert load_runtime_store(tmp_path / 'runtime.db').get_approval_request(pending[0].approval_id).status.value == 'approved'


@pytest.mark.parametrize('action,followup', [('cancel', 'Cancel it'), ('cancel', 'ألغِه'), ('pause', 'Páusalo')])
def test_telegram_completed_turn_followup_targets_created_job(tmp_path, monkeypatch, action, followup):
    from nullion import telegram_app, crons
    from nullion.config import load_settings
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker
    f = fixture(tmp_path, monkeypatch)
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN': '123456:abcdefghijklmnopqrstuvwxyz',
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID': '202', 'NULLION_TELEGRAM_CHAT_ENABLED': 'true'})
    service = telegram_app.ChatOperatorService(runtime=f.runtime, settings=settings,
        bot_token=settings.telegram.bot_token, operator_chat_id='202', model_client=f.model,
        agent_orchestrator=f.orchestrator)
    service._turn_dispatch_tracker = AsyncTurnDispatchTracker()
    wire = []
    class Sent:
        def __init__(self, content):
            self.text, self.message_id = content, 1000 + len(wire)
        async def edit_text(self, text, **kw):
            wire.append(text); self.text = text; return self
        async def delete(self):
            pass
    class Message:
        caption = None
        reply_to_message = None
        from_user = SimpleNamespace(id=202, first_name='Example', last_name='', username='example')
        def __init__(self, value, mid):
            self.text, self.message_id = value, mid
            self.chat = SimpleNamespace(id=202, send_action=self.send_action)
        async def send_action(self, action):
            pass
        async def reply_text(self, text, **kw):
            wire.append(text); return Sent(text)
    async def run():
        await service.on_message(SimpleNamespace(update_id=11,
            message=Message('Create a paused sensor watcher every day at noon.', 11)), SimpleNamespace(bot=None))
        job = crons.list_crons()[0]
        # Real pause mutation must change state, rather than merely returning a list.
        if action == 'pause':
            crons.update_cron(job.id, enabled=True)
        f.model.phase, f.model.invoked = action, False
        await service.on_message(SimpleNamespace(update_id=12, message=Message(followup, 12)), SimpleNamespace(bot=None))
        if action == 'cancel':
            pending = [a for a in f.runtime.store.list_approval_requests() if a.status.value == 'pending']
            assert len(pending) == 1, (wire, f.model.guard_packets, f.model.calls)
            assert any(job.name in message and 'Delete' in message for message in wire), wire
            assert crons.get_cron(job.id) is not None
            assert f.model.guard_packets[-1]['recent_conversation'][0]['scheduler_object_ids'] == [job.id]
        else:
            assert crons.get_cron(job.id).enabled is False, (wire, f.model.calls)
            assert any(job.name in message and 'paused' in message for message in wire), wire
        assert not any('missing schedule or task detail' in message for message in wire), wire
        turns = [e for e in f.runtime.store.list_conversation_events('telegram:202') if e['event_type'] == 'conversation.chat_turn']
        assert len(turns) == (1 if action == 'cancel' else 2)
        assert any(r['tool_name'] == 'create_cron' and r['status'] == 'completed' for r in turns[0]['tool_results'])
    asyncio.run(run())


@pytest.mark.parametrize('authorization,prompt', [('ambiguous_target', 'Cancel that one'),
    ('not_requested', 'El informe no cambió')])
def test_real_web_guard_denial_is_accurate_and_numbered_clarification_can_resume(tmp_path, monkeypatch, authorization, prompt):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion import crons
    f = fixture(tmp_path, monkeypatch)
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        created = client.post('/api/chat', json={'text': 'Create a paused sensor watcher every day at noon.',
            'conversation_id': 'web:clarify-cancel'})
        assert created.status_code == 200
        job = crons.list_crons()[0]
        f.model.phase, f.model.invoked, f.model.authorization = 'cancel', False, authorization
        response = client.post('/api/chat', json={'text': prompt, 'conversation_id': 'web:clarify-cancel'})
        assert response.status_code == 200
        assert not f.runtime.store.list_approval_requests(), response.json()
        assert crons.get_cron(job.id) is not None
        reply = response.json().get('reply') or response.json().get('text')
        assert 'Sensor watcher' in reply, response.json()
        assert 'schedule or task detail' not in reply and 'was canceled' not in reply, reply
        assert len(f.model.guard_packets) == 1, 'An intentional denial must not retry deletion'
        if authorization == 'ambiguous_target':
            assert '1. Cancel' in reply and '2. Keep it running' in reply
            f.model.invoked, f.model.authorization = False, 'authorized'
            numeric = client.post('/api/chat', json={'text': '1', 'conversation_id': 'web:clarify-cancel'})
            assert numeric.status_code == 200
            pending = [a for a in f.runtime.store.list_approval_requests() if a.status.value == 'pending']
            assert len(pending) == 1, numeric.json()
            supporting = f.model.guard_packets[-1]['recent_conversation'][-1]
            assert supporting['scheduler_object_ids'] == [job.id], supporting
            assert '1. Cancel' in supporting['assistant_reply']


def test_browser_cancellation_card_uses_shared_title_and_approval_deletes_exact_job(tmp_path, monkeypatch):
    from playwright.sync_api import expect
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from nullion.web_app import create_app
    from nullion import crons
    f = fixture(tmp_path, monkeypatch)
    with _UvicornThread(create_app(f.runtime, f.orchestrator, f.registry), _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator('#user-input').fill('Create a paused sensor watcher every day at noon.')
            page.locator('#user-input').press('Enter')
            expect(page.locator('#messages')).to_contain_text('Created Sensor watcher; it is paused.', timeout=10000)
            job = crons.list_crons()[0]
            f.model.phase, f.model.invoked = 'cancel', False
            page.locator('#user-input').fill('Cancélalo')
            page.locator('#user-input').press('Enter')
            card = page.locator('#messages .approval-bubble')
            expect(card).to_contain_text('Delete this scheduled task?', timeout=10000)
            expect(card).to_contain_text('Scheduled task: Sensor watcher')
            expect(card).to_contain_text('Approve once to delete it')
            assert 'Allow file access?' not in card.inner_text()
            card.get_by_role('button', name='Allow once', exact=True).click()
            expect(page.locator('#messages')).to_contain_text('deleted', timeout=10000)
            assert crons.get_cron(job.id) is None
            assert not errors
        finally:
            browser.close(); playwright.stop()

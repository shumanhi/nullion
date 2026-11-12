"""Accepted text bytes with a lost response must not replay at another owner.

Real SDK calls, Nullion delivery adapters, workflow runners and SQLite persist;
only external transports and configured target metadata are simulated.
"""
import asyncio
from datetime import UTC, datetime, timedelta
import json
import os
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from tests.e2e.test_delivery_acknowledgement_boundary import (
    TOKEN, TelegramWire, install_wire, prepared,
)

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('surface', ['slack', 'discord'])
@pytest.mark.parametrize('reply', ['確認した結果です。', 'Resultado confirmado.'])
def test_plain_format_fallback_never_replays_an_accepted_message(tmp_path, monkeypatch, surface, reply):
    from nullion.delivery_acknowledgement import UnconfirmedDelivery
    from nullion.messaging_adapters import list_platform_delivery_receipts
    prepared(tmp_path, monkeypatch)
    accepted = []
    if surface == 'slack':
        from slack_sdk.web import async_client
        from nullion.slack_app import send_slack_platform_delivery
        async def post(**kw):
            accepted.append(kw['text'])
            raise httpx.ReadTimeout('Response lost after acceptance')
        monkeypatch.setattr(async_client, 'AsyncWebClient', lambda **kw: SimpleNamespace(chat_postMessage=post))
        send = send_slack_platform_delivery(bot_token='synthetic', channel='fixture', text=reply)
    else:
        from nullion.discord_app import send_discord_platform_delivery
        async def handle(request):
            accepted.append(json.loads(await request.aread())['content'])
            raise httpx.ReadTimeout('Response lost after acceptance', request=request)
        native = httpx.AsyncClient
        monkeypatch.setattr(httpx, 'AsyncClient', lambda **kw: native(**kw, transport=httpx.MockTransport(handle)))
        send = send_discord_platform_delivery(bot_token='synthetic', channel_id='77', text=reply)
    with pytest.raises(UnconfirmedDelivery):
        asyncio.run(send)
    assert accepted == [reply]
    receipts = list_platform_delivery_receipts()
    assert receipts[0]['status'] == 'unknown' and not receipts[0]['text_delivered']


def test_uncertain_stream_edit_does_not_create_another_final_message(tmp_path, monkeypatch):
    from nullion.slack_app import _SlackTextDeltaStreamer
    from nullion.messaging_adapters import execute_platform_reply_delivery
    prepared(tmp_path, monkeypatch)
    accepted = []; new = []
    async def update(**kw):
        accepted.append(kw)
        raise httpx.ReadTimeout('Edit accepted, response lost')
    async def send(text):
        new.append(text)
        return True
    async def run():
        streamer = _SlackTextDeltaStreamer(loop=asyncio.get_running_loop(),
            client=SimpleNamespace(chat_update=update), channel='fixture', ts='123')
        return await execute_platform_reply_delivery('確認結果', channel='slack', target_id='fixture',
            send_text=send, finalize_stream=streamer.finish)
    result = asyncio.run(run())
    assert len(accepted) == 1 and accepted[0]['text'] == '確認結果' and not new
    assert result.unconfirmed is not None and result.receipt.status == 'unknown'


def test_live_tracker_unknown_notice_stays_terminal_across_database_reopen(tmp_path, monkeypatch):
    from nullion.live_tracking import LiveTrackerStore, LiveTrackerEngine
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    runtime, _ = prepared(tmp_path, monkeypatch)
    wire = TelegramWire('unknown_notice'); install_wire(monkeypatch, wire)
    now = datetime.now(UTC)
    store = LiveTrackerStore(tmp_path/'runtime.db')
    job = store.create(name='確認', principal_id='telegram:77', conversation_id='telegram:77',
        workspace_id='workspace_admin', probe=[], matcher={}, interval_seconds=60,
        expires_at=now+timedelta(minutes=5), request_key='fixture', now=now)
    with store.connection() as db:
        store._notice(db, job, 'blocked', '確認結果', now)
    notice = store.claim_notice(now)
    engine = LiveTrackerEngine(store, registry=SimpleNamespace(), runtime_store=runtime.store,
        deliver=lambda j, text, ident: asyncio.run(_send_operator_telegram_delivery(TOKEN, '77', text, message_only=True)))
    engine._send(notice)
    reopened = LiveTrackerStore(tmp_path/'runtime.db')
    assert reopened.claim_notice(now+timedelta(minutes=1)) is None
    assert reopened.get(job['id'])['notification_status'] == 'unknown'
    with reopened.connection() as db:
        assert db.execute('SELECT state,attempts FROM live_tracker_notifications').fetchone()[:] == ('unknown', 1)
    assert wire.texts == ['確認結果']


@pytest.mark.parametrize('fanout', [False, True])
def test_due_reminder_unknown_ack_does_not_fallback_or_rollback_claim(tmp_path, monkeypatch, fanout):
    from nullion import reminder_delivery
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    from nullion.users import MessagingDeliveryTarget
    runtime, _ = prepared(tmp_path, monkeypatch)
    wire = TelegramWire('unknown_notice'); install_wire(monkeypatch, wire)
    settings = NullionSettings(telegram=TelegramSettings(bot_token=TOKEN))
    targets = (MessagingDeliveryTarget(channel='telegram', target_id='77', principal_id='telegram:77',
        workspace_id='workspace_admin', user_id='fixture', display_name='Fixture'),) if fanout else ()
    monkeypatch.setattr(reminder_delivery, 'messaging_delivery_targets_for_workspace', lambda *a, **kw: targets)
    now = datetime.now(UTC)
    task = runtime.schedule_reminder(chat_id='77', text='確認してください', due_at=now-timedelta(seconds=1))
    fallback = []
    async def origin(chat_id, text):
        fallback.append(chat_id)
        return await _send_operator_telegram_delivery(TOKEN, chat_id, text, message_only=True)
    def run(rt, when):
        return asyncio.run(reminder_delivery.deliver_due_reminders_once(rt, send=origin, now=when, settings=settings))
    assert run(runtime, now) == 0
    restarted = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert run(restarted, now+timedelta(minutes=1)) == 0
    assert len(wire.texts) == 1 and fallback == ([] if fanout else ['77'])
    assert not restarted.store.get_scheduled_task(task.task_id).enabled
    events = [e for e in restarted.store.events if e.event_type == 'reminder.delivery.unknown']
    assert len(events) == 1 and events[0].payload['task_id'] == task.task_id


def test_doctor_notice_unknown_ack_is_not_replayed_next_tick(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import config, web_app
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.crons import load_crons
    from tests.e2e.test_tracker_status_delivery import setup, add, record
    wire = TelegramWire('unknown_notice'); install_wire(monkeypatch, wire)
    settings = NullionSettings(doctor_enabled=False, telegram=TelegramSettings(bot_token=TOKEN))
    monkeypatch.setattr(config, 'load_settings', lambda *a, **kw: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *a, **kw: settings)
    app, runtime, db = setup(tmp_path, monkeypatch)
    now = datetime.now(UTC)
    with TestClient(app) as client:
        job = add(client, name='確認', channel='telegram', target='77')
        for i in range(12): record(db, job, 'incomplete', now-timedelta(seconds=120-i*5))
        jobs = load_crons(refresh_next_runs=False)
        assert app.state.tracker_status_tick(jobs, now) == 0
        assert app.state.tracker_status_tick(jobs, now+timedelta(minutes=6)) == 0
        with sqlite3.connect(db) as conn:
            payloads = [json.loads(row[0]) for row in conn.execute('SELECT payload FROM cron_tracker_notices')]
        assert len(payloads) == 1 and payloads[0]['delivery_status'] == 'unknown'
    assert len(wire.texts) == 1


def test_acknowledged_live_alert_is_not_retried_when_local_history_write_fails(tmp_path, monkeypatch):
    from nullion import config, web_app, chat_store
    from nullion.config import NullionSettings, TelegramSettings
    from tests.e2e.test_tracker_status_delivery import setup
    wire = TelegramWire(); install_wire(monkeypatch, wire)
    settings = NullionSettings(doctor_enabled=False, telegram=TelegramSettings(bot_token=TOKEN))
    monkeypatch.setattr(config, 'load_settings', lambda *a, **kw: settings)
    monkeypatch.setattr(web_app, 'load_settings', lambda *a, **kw: settings)
    app, runtime, db = setup(tmp_path, monkeypatch)
    engine = app.state.live_tracker_engine
    now = datetime.now(UTC)
    job = engine.store.create(name='確認', principal_id='telegram:77', conversation_id='telegram:77',
        workspace_id='workspace_admin', probe=[], matcher={}, interval_seconds=60,
        expires_at=now+timedelta(minutes=5), request_key='fixture', now=now)
    with engine.store.connection() as conn:
        engine.store._notice(conn, job, 'blocked', '確認結果', now)
    history = chat_store.get_chat_store()
    def fail(*a, **kw): raise OSError('Synthetic local history write failure')
    monkeypatch.setattr(history, 'save_message', fail)
    engine._send(engine.store.claim_notice(now))
    assert engine.store.get(job['id'])['notification_status'] == 'sent'
    assert engine.store.claim_notice(now+timedelta(minutes=1)) is None
    assert len(wire.texts) == 1


def test_unknown_failure_notice_does_not_replay_on_outbox_recovery(tmp_path, monkeypatch):
    from nullion.result_aggregator import ResultAggregator
    from nullion.task_queue import TaskRegistry
    from nullion.telegram_entrypoint import _send_operator_telegram_delivery
    runtime, paths = prepared(tmp_path, monkeypatch)
    wire = TelegramWire('unknown_notice'); install_wire(monkeypatch, wire)
    async def deliver(conversation, text, **kw):
        if kw.get('is_artifact'): return False  # Explicit pre-send rejection.
        return await _send_operator_telegram_delivery(TOKEN, '77', text, message_only=True)
    def owner():
        return ResultAggregator(deliver_fn=deliver, task_registry=TaskRegistry(), outbox_path=tmp_path/'runtime.db')
    first = owner()
    first._outbox.enqueue('fixture', {'conversation_id':'telegram:77', 'summary':'確認結果',
        'artifacts':[str(paths[0])], 'delivered_artifacts':[], 'text_delivered':False,
        'work_complete':True, 'status':'pending', 'attempts':0, 'retry_at':0})
    asyncio.run(first._deliver_outbox('fixture'))
    second = owner()
    asyncio.run(second._deliver_outbox('fixture'))
    assert len(wire.texts) == 1
    record = second._outbox.get('fixture')
    assert record['failure_notice_attempted'] and record['notice_status'] == 'unknown'
    assert record['status'] == 'failed'  # The explicitly unsent file can still retry.


@pytest.mark.parametrize('streaming', [False, True])
def test_telegram_reply_accepted_before_timeout_is_not_retried(tmp_path, monkeypatch, streaming):
    from telegram import Bot, Chat, Message
    from nullion.telegram_transport import build_telegram_bot
    from nullion.telegram_app import _deliver_reply
    from nullion.chat_streaming import ChatStreamingMode
    from nullion.messaging_adapters import list_platform_delivery_receipts
    prepared(tmp_path, monkeypatch)
    wire = TelegramWire('unknown_notice'); install_wire(monkeypatch, wire)
    reply = '確認結果をお知らせします。' * (30 if streaming else 1)
    async def run():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            message = Message(message_id=1, date=datetime.now(UTC), chat=Chat(id=77, type='private'), text='確認')
            message.set_bot(bot)
            await _deliver_reply(message, reply, quote_reply=False,
                streaming_mode=ChatStreamingMode.MESSAGE_EDITS if streaming else ChatStreamingMode.FINAL_ONLY)
    asyncio.run(run())
    assert len(wire.texts) == 2  # One attempted result and one separate status notice.
    assert 'not confirmed' in wire.texts[1]
    assert list_platform_delivery_receipts()[0]['status'] == 'unknown'


def test_telegram_draft_creation_unknown_ack_is_retained_for_finalization(tmp_path, monkeypatch):
    from telegram import Bot, Chat, Message
    from nullion.telegram_transport import build_telegram_bot
    from nullion.telegram_app import _TelegramTextDeltaStreamer
    from nullion.delivery_acknowledgement import UnconfirmedDelivery
    prepared(tmp_path, monkeypatch)
    wire = TelegramWire('unknown_notice'); install_wire(monkeypatch, wire)
    async def run():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            message = Message(message_id=1, date=datetime.now(UTC), chat=Chat(id=77, type='private'), text='確認')
            message.set_bot(bot)
            streamer = _TelegramTextDeltaStreamer(loop=asyncio.get_running_loop(), message=message)
            with pytest.raises(UnconfirmedDelivery): await streamer._send_or_edit('確認結果')
            with pytest.raises(UnconfirmedDelivery): await streamer.finish('最終確認結果')
    asyncio.run(run())
    assert wire.texts == ['確認結果']


def test_telegram_chat_handler_preserves_unknown_stream_creation_instead_of_new_reply(tmp_path, monkeypatch):
    from telegram import Bot, Chat, Message, User, Update
    from nullion.telegram_transport import build_telegram_bot
    from nullion.telegram_app import ChatOperatorService
    from nullion.config import load_settings
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from nullion.messaging_adapters import list_platform_delivery_receipts
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime, _ = prepared(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', 'false')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'false')
    wire = TelegramWire('unknown_notice'); install_wire(monkeypatch, wire)
    native = wire.handle
    async def handle(request):
        if request.url.path.endswith('/sendChatAction'):
            return httpx.Response(200, json={'ok':True, 'result':True})
        return await native(request)
    wire.handle = handle
    class Model:
        calls = 0
        def create(self, **kw):
            protocol = workflow_provider_reply(kw, reasoned=True)
            if protocol is not None: return protocol
            self.calls += 1
            callback = kw.get('text_delta_callback')
            if callback: callback('確認結果')
            return {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':'最終確認結果です。'}]}
    model = Model()
    runtime.active_tool_registry = ToolRegistry()
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77', 'NULLION_TELEGRAM_CHAT_ENABLED':'true',
        'NULLION_TELEGRAM_CHAT_STREAMING_ENABLED':'true'})
    service = ChatOperatorService(runtime=runtime, bot_token=TOKEN, operator_chat_id='77', settings=settings,
        model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    async def run():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            message = Message(message_id=1, date=datetime.now(UTC), chat=Chat(id=77, type='private'),
                from_user=User(id=77, first_name='Fixture', is_bot=False), text='短く説明してください。')
            message.set_bot(bot)
            await service.on_message(Update(update_id=700, message=message), SimpleNamespace(bot=bot))
    asyncio.run(run())
    assert model.calls > 0
    assert wire.texts == ['最終確認結果です。', 'The platform has not confirmed the remaining delivery. It may still arrive. '
        'I will not resend automatically because that could create duplicates.']
    assert list_platform_delivery_receipts()[0]['status'] == 'unknown'

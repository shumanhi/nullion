"""Real ingress, SDK delivery and SQLite; slow external inference/storage fixtures."""
import asyncio
from datetime import UTC, datetime
import json
import os
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.mark.parametrize('numeric', [False, True])
def test_telegram_completed_replies_do_not_pollute_screenshot_followup_during_cleanup(tmp_path, monkeypatch, numeric):
    from telegram import Bot, Chat, Message, Update, User
    from nullion import telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.telegram_transport import build_telegram_bot
    from nullion.tools import ToolRegistry
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch)
    runtime.active_tool_registry = ToolRegistry()
    started_cleanup, release_cleanup = threading.Event(), threading.Event()
    first_delivered = threading.Event()
    real_checkpoint = type(runtime).checkpoint
    def checkpoint(self, *args, **kwargs):
        if self is runtime and first_delivered.is_set():
            started_cleanup.set()
            assert release_cleanup.wait(8)
        return real_checkpoint(self, *args, **kwargs)
    monkeypatch.setattr(type(runtime), 'checkpoint', checkpoint)
    wire = TelegramWire(); original = wire.handle
    typing = []
    async def handle(request):
        if request.url.path.endswith('/sendChatAction'):
            typing.append(time.monotonic())
            return httpx.Response(200, json={'ok':True,'result':True})
        result = await original(request)
        if request.url.path.endswith('/sendMessage') and len(wire.texts) == 1:
            first_delivered.set()
        return result
    wire.handle = handle; install_wire(monkeypatch, wire)
    class Model:
        step = 0
        def create(self, **kwargs):
            encoded = json.dumps(kwargs.get('messages', []), ensure_ascii=False)
            if 'allowed_target_indexes' in encoded:
                packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
                assert packet.get('target_context_selected'), 'Finished requests entered concurrent admission'
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None: return protocol
            self.step += 1
            if self.step == 1: return text('Hello.')
            if self.step == 2: return text('Ticket board: 6:00 PM, Premium Format XL.' +
                ('\n1. Capturar pantalla\n2. Terminar' if numeric else ''))
            assert 'Ticket board: 6:00 PM' in encoded, encoded
            return text('Screenshot request received for the ticket board at 6:00 PM.')
    model = Model()
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings, bot_token=TOKEN,
        operator_chat_id='77', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    async def replay():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            def update(index, value):
                message = Message(message_id=index, date=datetime.now(UTC), chat=Chat(77,'private'),
                    from_user=User(77,'Fixture',False), text=value)
                message.set_bot(bot); message.chat.set_bot(bot)
                return Update(update_id=index, message=message)
            tasks = []
            try:
                for index, value in enumerate(['Hi', 'Find the ticket board for 6 PM', '1' if numeric else 'Send me screenshot'], 1):
                    tasks.append(asyncio.create_task(service.on_message(update(index, value), SimpleNamespace(bot=bot))))
                    deadline = time.monotonic() + 5
                    while len(wire.texts) < index:
                        assert time.monotonic() < deadline, wire.texts
                        await asyncio.sleep(.01)
                    if index == 1:
                        assert await asyncio.to_thread(started_cleanup.wait, 2)
                    assert await service._turn_dispatch_tracker.active_turn_ids('telegram:77') == ()
                    assert not any('Which request' in s or 'database is locked' in s for s in wire.texts)
                from nullion.runtime_checkpoints import _CHECKPOINT_INFLIGHT
                assert id(runtime) in _CHECKPOINT_INFLIGHT, 'The slow snapshot fixture must still be active'
                assert tasks[0].done(), 'Delivered turns must not wait for the background snapshot'
            finally:
                release_cleanup.set(); await asyncio.gather(*tasks)
    asyncio.run(replay())
    from nullion.runtime_checkpoints import wait_runtime_checkpoints
    assert wait_runtime_checkpoints(runtime, timeout=5)
    assert len(wire.texts) == 3 and 'ticket board at 6:00 PM' in wire.texts[-1], wire.texts
    from nullion.runtime_persistence import load_runtime_store
    restored = load_runtime_store(runtime.checkpoint_path)
    assert len([r for r in restored.list_conversation_events('telegram:77') if r['event_type']=='conversation.chat_turn']) == 3
    assert not service._turn_dispatch_tracker._done_callbacks


def test_real_telegram_typing_precedes_slow_concurrent_admission_and_fast_answers_have_no_extra_notice(tmp_path, monkeypatch):
    from telegram import Bot, Chat, Message, Update, User
    from nullion import telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.telegram_transport import build_telegram_bot
    from nullion.tools import ToolRegistry
    from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, TelegramWire, install_wire
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate, text
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime = isolate(tmp_path, monkeypatch); runtime.active_tool_registry = ToolRegistry()
    wire = TelegramWire(); original = wire.handle; first_actions = []
    async def handle(request):
        if request.url.path.endswith('/sendChatAction'):
            first_actions.append(time.monotonic())
            return httpx.Response(200, json={'ok':True,'result':True})
        return await original(request)
    wire.handle = handle; install_wire(monkeypatch, wire)
    class Model:
        def create(self, **kwargs):
            if 'allowed_target_indexes' in json.dumps(kwargs.get('messages', [])):
                time.sleep(.7)
                return text(json.dumps({'relationship':'separate','effect':'continue','confidence':1,'target_index':None}))
            protocol = workflow_provider_reply(kwargs, completion=True)
            return protocol if protocol is not None else text('Hello.')
    model = Model(); settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings, bot_token=TOKEN,
        operator_chat_id='77', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    async def replay():
        async with build_telegram_bot(Bot,TOKEN) as bot:
            active = await service._turn_dispatch_tracker.register('telegram:77','別の作業',turn_id='running')
            waits = []
            try:
                for index, value in enumerate(['Bonjour','こんにちは','Hola','नमस्ते'],1):
                    first_actions.clear(); started = time.monotonic()
                    message = Message(message_id=index,date=datetime.now(UTC),chat=Chat(77,'private'),
                        from_user=User(77,'Fixture',False),text=value)
                    message.set_bot(bot); message.chat.set_bot(bot)
                    await service.on_message(Update(update_id=index,message=message),SimpleNamespace(bot=bot))
                    assert first_actions
                    waits.append(first_actions[0] - started)
                assert sum(waits)/len(waits) < .2 and max(waits) < .4, waits
            finally: await active.finish()
    asyncio.run(replay())
    assert wire.texts == ['Hello.']*4, wire.texts


def test_busy_secondary_history_does_not_replace_real_http_clarification(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime_persistence import _runtime_store_file_lock, load_runtime_store
    from nullion.web_app import create_app
    from tests.e2e.test_inflight_amendment_delivery import _runtime_fixture
    from tests.e2e.test_quoted_reply_review_and_cleanup import text
    from tests.workflow_provider_protocol import workflow_provider_reply
    fixture = _runtime_fixture(tmp_path,monkeypatch,'unused')
    class Model:
        def create(self, **kwargs):
            if 'allowed_target_indexes' in json.dumps(kwargs.get('messages', [])):
                return text(json.dumps({'relationship':'ambiguous','effect':'continue','confidence':1}))
            protocol = workflow_provider_reply(kwargs,completion=True)
            return protocol if protocol is not None else fixture.model.create(**kwargs)
    app = create_app(fixture.runtime,AgentOrchestrator(model_client=Model()),fixture.registry)
    ready, release = threading.Event(), threading.Event()
    def hold():
        with _runtime_store_file_lock(fixture.runtime.checkpoint_path):
            ready.set(); release.wait(8)
    cid = 'web:contention'
    with TestClient(app) as client, ThreadPoolExecutor() as pool:
        running = pool.submit(client.post,'/api/chat',json={'text':'Inspect the current board','conversation_id':cid,'turn_id':'running'})
        holder = None
        try:
            assert fixture.entered.wait(5)
            holder = threading.Thread(target=hold); holder.start(); assert ready.wait(2)
            started = time.monotonic()
            response = client.post('/api/chat',json={'text':'修正して','conversation_id':cid,'turn_id':'question'})
            assert time.monotonic() - started < 1.5
            assert response.status_code == 200 and 'Which request' in response.json()['text'], response.text
            assert 'database is locked' not in response.text
        finally:
            release.set(); fixture.release.set()
            if holder: holder.join(2)
            running.result(timeout=10)
    deadline = time.monotonic()+5
    while True:
        restored = load_runtime_store(fixture.runtime.checkpoint_path)
        if any(r.get('turn_id')=='question' and r['event_type']=='conversation.chat_turn' for r in restored.list_conversation_events(cid)):break
        assert time.monotonic()<deadline
        time.sleep(.03)

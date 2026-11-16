"""Measured real platform delivery under slow external transport boundaries."""
import asyncio
from datetime import UTC, datetime
import json
import os
import time
from urllib.parse import parse_qs

import httpx
import pytest

from tests.e2e.test_delivery_acknowledgement_boundary import TOKEN, install_wire, prepared

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E")


@pytest.mark.parametrize("language", ["確認結果", "Resultado confirmado"])
def test_telegram_streaming_does_not_block_producer_or_queue_obsolete_edits(tmp_path, monkeypatch, language):
    from telegram import Bot, Chat, Message
    from nullion.telegram_app import _TelegramTextDeltaStreamer
    from nullion.telegram_transport import build_telegram_bot
    prepared(tmp_path, monkeypatch)
    class Wire:
        errors = []
        def __init__(self): self.calls = []; self.text = ""
        async def handle(self, request):
            method = request.url.path.rsplit("/", 1)[-1]
            if method == "getMe":
                return httpx.Response(200, json={"ok":True,"result":{"id":12345,"is_bot":True,"first_name":"Fixture"}})
            assert method in {"sendMessage", "editMessageText"}
            fields = parse_qs((await request.aread()).decode())
            self.calls.append((method, fields["text"][0]))
            await asyncio.sleep(.04)  # Only the external API is slow.
            self.text = fields["text"][0]
            return httpx.Response(200, json={"ok":True,"result":{"message_id":100,"date":1,
                "chat":{"id":77,"type":"private"},"text":self.text}})
    wire = Wire(); install_wire(monkeypatch, wire)
    samples = []
    async def run():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            for index in range(4):
                message = Message(message_id=index+1, date=datetime.now(UTC), chat=Chat(77,"private"), text=language)
                message.set_bot(bot)
                streamer = _TelegramTextDeltaStreamer(loop=asyncio.get_running_loop(), message=message)
                await asyncio.to_thread(streamer.emit, language)
                await asyncio.sleep(.01)  # Let the single initial send own its receipt.
                started = time.perf_counter()
                for _ in range(80):
                    await asyncio.to_thread(streamer.emit, " · " + language)
                producer_time = time.perf_counter()-started
                assert producer_time < .15, "Token reading must not wait for network edits"
                assert await streamer.finish("**"+language+"**\n- A: 7\n- B: 8")
                samples.append(time.perf_counter()-started)
                assert wire.text == "<b>"+language+"</b>\n• A: 7\n• B: 8"
    asyncio.run(run())
    assert len(wire.calls) <= 12, "One current draft and one final edit, not eighty network edits"
    assert len([call for call in wire.calls if call[0] == "sendMessage"]) == 4
    assert sum(samples)/len(samples) < .25 and max(samples) < .4


def test_completed_telegram_answer_is_delivered_at_once(tmp_path, monkeypatch):
    from telegram import Bot, Chat, Message
    from nullion.telegram_app import _deliver_reply
    from nullion.telegram_transport import build_telegram_bot
    from nullion.chat_streaming import ChatStreamingMode
    from tests.e2e.test_delivery_acknowledgement_boundary import TelegramWire
    prepared(tmp_path, monkeypatch)
    wire = TelegramWire(); install_wire(monkeypatch, wire)
    answer = "Verified result: " + "Readable completed answer. " * 70
    async def run():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            message = Message(message_id=1,date=datetime.now(UTC),chat=Chat(77,"private"),text="確認")
            message.set_bot(bot)
            await _deliver_reply(message, answer, streaming_mode=ChatStreamingMode.MESSAGE_EDITS)
    asyncio.run(run())
    assert wire.texts == [answer.strip()]


def test_web_shows_wait_state_promptly_then_complete_readable_reply(tmp_path, monkeypatch):
    from playwright.sync_api import expect
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    runtime = isolate(tmp_path, monkeypatch)
    answer = "Confirmed options\n\n" + "\n".join(f"- Option {index}: available" for index in range(1,41))
    class Model:
        calls = 0
        def create(self, **kwargs):
            self.calls += 1
            time.sleep(.3)  # Only the external inference boundary is slow.
            return {"stop_reason":"end_turn", "content":[{"type":"text","text":answer}]}
    model = Model(); samples = []
    app = create_app(runtime, AgentOrchestrator(model_client=model), ToolRegistry())
    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            for index, prompt in enumerate(["Bonjour", "こんにちは", "Hola"]):
                page.locator('#user-input').fill(prompt)
                started = time.perf_counter()
                page.locator('#send-btn').click()
                expect(page.locator('#messages .typing-bubble, #messages .status-bubble')).to_have_count(1, timeout=700)
                assert time.perf_counter()-started < .7
                completed = page.locator('#messages .msg.bot .bubble').filter(has_text='Option 40: available')
                expect(completed).to_have_count(index+1, timeout=2500)
                expect(completed.last.locator('li')).to_have_count(40)
                samples.append(time.perf_counter()-started)
                page.wait_for_function('() => _activeSendTurnIds.size === 0', timeout=2000)
            assert not errors, errors
        finally:
            browser.close(); playwright.stop()
    assert model.calls == 3
    assert sum(samples)/len(samples) < 1.5 and max(samples) < 2.5
    turns = [e for e in runtime.store.list_conversation_events() if e.get('event_type') == 'conversation.chat_turn']
    assert len(turns) == 3 and all('Option 40: available' in e['assistant_reply'] for e in turns)


def test_buffered_model_answer_uses_one_formatted_telegram_send(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from telegram import Bot, Chat, Message, User, Update
    from nullion.telegram_app import ChatOperatorService
    from nullion.telegram_transport import build_telegram_bot
    from nullion.config import load_settings
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry
    from tests.e2e.test_delivery_acknowledgement_boundary import TelegramWire
    from tests.workflow_provider_protocol import workflow_provider_reply
    runtime, _ = prepared(tmp_path, monkeypatch)
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', 'false')
    monkeypatch.setenv('NULLION_TASK_PLANNER_FEED_MODE', 'off')
    monkeypatch.setenv('NULLION_ACTIVITY_TRACE_ENABLED', 'false')
    wire = TelegramWire(); original = wire.handle
    async def handle(request):
        if request.url.path.endswith('/sendChatAction'):
            return httpx.Response(200, json={'ok':True, 'result':True})
        return await original(request)
    wire.handle = handle; install_wire(monkeypatch, wire)
    class Model:
        def create(self, **kwargs):
            protocol = workflow_provider_reply(kwargs, reasoned=True)
            if protocol is not None: return protocol
            return {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':'**確認結果**\n- A: 7\n- B: 8'}]}
    model = Model(); runtime.active_tool_registry = ToolRegistry()
    settings = load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':TOKEN,
        'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'77', 'NULLION_TELEGRAM_CHAT_ENABLED':'true',
        'NULLION_TELEGRAM_CHAT_STREAMING_ENABLED':'true'})
    service = ChatOperatorService(runtime=runtime, bot_token=TOKEN, operator_chat_id='77', settings=settings,
        model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    async def run():
        async with build_telegram_bot(Bot, TOKEN) as bot:
            message = Message(message_id=1, date=datetime.now(UTC), chat=Chat(77,'private'),
                from_user=User(77,'Fixture',False), text='短く説明してください。')
            message.set_bot(bot)
            await service.on_message(Update(update_id=1,message=message), SimpleNamespace(bot=bot))
    asyncio.run(run())
    assert wire.texts == ['<b>確認結果</b>\n• A: 7\n• B: 8']

"""Real Telegram ingress/model loop/persistence; only provider and Telegram IO faked."""
from __future__ import annotations

import asyncio
import base64
import json
import os
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="opt-in E2E")
PIXEL = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jXioAAAAASUVORK5CYII=")


@pytest.mark.parametrize("prompt", ["What happened to this?", "¿Qué pasó con esto?", "これはどうなった？"])
@pytest.mark.parametrize("restart_receipt", [False, True])
@pytest.mark.parametrize("caption", [None, "Check why this account was blocked"])
def test_quoted_unanswered_image_reaches_model_instead_of_old_topic(tmp_path, monkeypatch, prompt, restart_receipt, caption):
    from nullion import chat_store, telegram_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "false")
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "false")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    monkeypatch.setattr(chat_store, "_store", None)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_conversation_event({"event_type": "conversation.chat_turn", "conversation_id": "telegram:123",
        "turn_id": "old-topic", "user_message": "Tell me about Spot On", "assistant_reply": "Spot On is registered in Paris."})
    runtime.checkpoint()
    downloaded = []
    class File:
        async def download_as_bytearray(self):
            downloaded.append(True)
            return bytearray(PIXEL)
    class Photo:
        file_size = len(PIXEL)
        async def get_file(self):
            return File()
    sent = []
    class Message:
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada", is_bot=False)
        def __init__(self, mid, *, text=None, caption=None, photo=(), reply=None):
            self.message_id, self.text, self.caption = mid, text, caption
            self.photo, self.reply_to_message = photo, reply
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
        async def send_action(self, action):
            pass
        async def reply_text(self, text, **kwargs):
            sent.append((self.message_id, text))
            return SimpleNamespace(message_id=100 + len(sent))
    original = Message(41, caption=caption, photo=[Photo()])
    if restart_receipt:
        attachments = asyncio.run(telegram_app._download_telegram_attachments(original, SimpleNamespace(bot=None), settings=None))
        telegram_app._checkpoint_telegram_attachment_receipt(runtime, conversation_id="telegram:123",
            message_id="telegram-message:123:41", request_id="telegram-update:1", text=original.caption, attachments=attachments)
        runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    requests = []
    class Model:
        def create(self, **kwargs):
            if [tool.get("name") for tool in kwargs.get("tools", [])] == ["select_attachment_format"]:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "format",
                    "name": "select_attachment_format", "input": {"extension": None, "output_kind": "inline",
                        "embedded_media_extensions": [], "confidence": 1}}]}
            if not kwargs.get("tools") and "timeout" not in kwargs:
                return {"content": [{"type": "text", "text": "{}"}]}
            requests.append(kwargs)
            encoded = json.dumps(kwargs.get("messages", []), ensure_ascii=False)
            # This is the failed boundary: the actual primary-model request
            # must carry original bytes and exclude unrelated conversation.
            assert base64.b64encode(PIXEL).decode() in encoded
            assert "Spot On is registered" not in encoded
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "The attached account is Mandir Shri Govinddevji. Its block needs investigation."}]}
    model = Model()
    settings = load_settings(env={"NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    service = telegram_app.ChatOperatorService(runtime=runtime, settings=settings, bot_token=settings.telegram.bot_token,
        operator_chat_id="123", model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model))
    # No completed or registered original turn exists, matching interrupted prod.
    followup = Message(42, text=prompt, reply=original)
    asyncio.run(service.on_message(SimpleNamespace(update_id=2, message=followup), SimpleNamespace(bot=None)))
    assert len(requests) == 1
    assert downloaded == [True]
    assert any(mid == 42 and "Mandir Shri Govinddevji" in text for mid, text in sent)
    persisted = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    replies = persisted.store.list_recent_conversation_events("telegram:123", event_type="conversation.chat_turn")
    assert replies[-1]["user_message"] == prompt
    assert "Mandir Shri Govinddevji" in replies[-1]["assistant_reply"]

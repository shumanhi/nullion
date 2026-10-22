import json
import sys
import types
from types import SimpleNamespace

import pytest

from nullion.telegram_entrypoint import _send_operator_telegram_delivery
from nullion.telegram_app import _TELEGRAM_MESSAGE_CHUNK_SIZE, _reply_text_in_chunks_with_plain_fallback
from nullion.telegram_formatting import format_telegram_text


class FakeTelegramBot:
    calls = []

    def __init__(self, token):
        self.token = token

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def send_message(self, chat_id, text, **kwargs):
        self.calls.append(("send_message", chat_id, text, kwargs))
        return SimpleNamespace(message_id=3001)

    async def send_document(self, chat_id, document, **kwargs):
        self.calls.append(("send_document", chat_id, getattr(document, "name", ""), kwargs))
        return SimpleNamespace(message_id=4001)


@pytest.mark.asyncio
async def test_operator_telegram_delivery_formats_markdown_for_cron(monkeypatch, tmp_path):
    FakeTelegramBot.calls = []
    fake_telegram = types.SimpleNamespace(Bot=FakeTelegramBot)
    monkeypatch.setitem(sys.modules, "telegram", fake_telegram)
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))

    ok = await _send_operator_telegram_delivery(
        "token",
        "chat-1",
        "Scheduled task: Watch\n\n**Result:** No new mentions. Check `site:reddit.com`.",
        suppress_link_preview=True,
    )

    assert ok is True
    assert FakeTelegramBot.calls == [
        (
            "send_message",
            "chat-1",
            "⏰ <b>SCHEDULED TASK: Watch</b>\n\n<b>Result:</b> No new mentions. Check <code>site:reddit.com</code>.",
            {"disable_web_page_preview": True, "parse_mode": "HTML"},
        )
    ]
    receipts = [
        json.loads(line)
        for line in (tmp_path / "delivery_receipts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert receipts[-1]["message_id"] == "3001"


@pytest.mark.asyncio
async def test_operator_telegram_delivery_sends_long_urls_as_clickable_links(monkeypatch, tmp_path):
    FakeTelegramBot.calls = []
    fake_telegram = types.SimpleNamespace(Bot=FakeTelegramBot)
    monkeypatch.setitem(sys.modules, "telegram", fake_telegram)
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    url = (
        "https://dddcaa.r.tsp1-brevo.net/tr/cl/"
        "kAEx2mZXu4DuX7VOOs8gQ-gUM4GVcGTElobJJBsfoPigRNwMNVE4mm"
    )

    ok = await _send_operator_telegram_delivery(
        "token",
        "chat-1",
        f"**Online Payment Site**\n{url}",
        suppress_link_preview=True,
    )

    assert ok is True
    assert FakeTelegramBot.calls == [
        (
            "send_message",
            "chat-1",
            (
                "<b>Online Payment Site</b>\n"
                f'<a href="{url}">Open link (dddcaa.r.tsp1-brevo.net)</a>'
            ),
            {"disable_web_page_preview": True, "parse_mode": "HTML"},
        )
    ]
    receipts = [
        json.loads(line)
        for line in (tmp_path / "delivery_receipts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert receipts[-1]["message_id"] == "3001"


@pytest.mark.asyncio
async def test_operator_telegram_delivery_chunks_long_formatted_cron_text(monkeypatch, tmp_path):
    FakeTelegramBot.calls = []
    fake_telegram = types.SimpleNamespace(Bot=FakeTelegramBot)
    monkeypatch.setitem(sys.modules, "telegram", fake_telegram)
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    text = "\n\n".join(f"**Finding {index}:** " + ("verified evidence " * 35) for index in range(18))

    ok = await _send_operator_telegram_delivery(
        "token",
        "chat-1",
        text,
        suppress_link_preview=True,
    )

    calls = [call for call in FakeTelegramBot.calls if call[0] == "send_message"]
    assert ok is True
    assert len(calls) > 1
    assert all(len(call[2]) <= _TELEGRAM_MESSAGE_CHUNK_SIZE for call in calls)
    assert all(call[3].get("parse_mode") == "HTML" for call in calls)
    assert all(call[2].count("<b>") == call[2].count("</b>") for call in calls)
    receipts = [
        json.loads(line)
        for line in (tmp_path / "delivery_receipts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert receipts[-1]["message_id"] == "3001"


@pytest.mark.asyncio
async def test_operator_telegram_delivery_treats_partial_attachment_delivery_as_delivered(
    monkeypatch,
    tmp_path,
) -> None:
    FakeTelegramBot.calls = []
    fake_telegram = types.SimpleNamespace(Bot=FakeTelegramBot)
    monkeypatch.setitem(sys.modules, "telegram", fake_telegram)
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    output_root = tmp_path / "outputs"
    output_root.mkdir()
    artifact = output_root / "nullion-artifact-report.html"
    missing = output_root / "nullion-artifact-missing.html"
    artifact.write_text("<html><body>" + ("report" * 20) + "</body></html>", encoding="utf-8")

    ok = await _send_operator_telegram_delivery(
        "token",
        "chat-1",
        f"Done.\nMEDIA:{artifact}\nMEDIA:{missing}",
        suppress_link_preview=True,
        request_id="unit-telegram-delivery",
    )

    assert ok is True
    assert [call[0] for call in FakeTelegramBot.calls] == ["send_document"]
    receipts = [
        json.loads(line)
        for line in (tmp_path / "delivery_receipts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert receipts[-1]["status"] == "partial"
    assert receipts[-1]["attachment_count"] == 1
    assert receipts[-1]["unavailable_attachment_count"] == 1
    assert receipts[-1]["message_id"] == "4001"
    assert receipts[-1]["request_id"] == "unit-telegram-delivery"


class FakeReplyMessage:
    def __init__(self):
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append((text, kwargs))


@pytest.mark.asyncio
async def test_long_telegram_replies_keep_formatting_per_chunk_without_visible_tags():
    message = FakeReplyMessage()
    paragraph = "**Finding:** public mention surfaced with details. "
    plain = paragraph * ((_TELEGRAM_MESSAGE_CHUNK_SIZE // len(paragraph)) + 3)
    formatted, kwargs = format_telegram_text(plain)

    await _reply_text_in_chunks_with_plain_fallback(
        message,
        formatted,
        plain,
        do_quote=True,
        **kwargs,
    )

    assert len(message.calls) > 1
    for text, call_kwargs in message.calls:
        assert len(text) <= _TELEGRAM_MESSAGE_CHUNK_SIZE
        if "<b>" in text:
            assert call_kwargs.get("parse_mode") == "HTML"
        else:
            assert "<b>" not in text and "</b>" not in text

from types import SimpleNamespace

import asyncio
import pytest

from nullion import discord_app
from nullion.discord_app import _send_discord_chunks_with_plain_fallback, handle_discord_message
from nullion.config import NullionSettings, SlackSettings
from nullion import messaging_adapters
from nullion import slack_app
from nullion.slack_app import (
    handle_slack_message,
    _format_slack_reply,
    _send_slack_callable_with_plain_fallback,
    _slack_plain_format_fallback_text,
    _slack_reply_chunks,
)


def test_slack_formatter_strips_external_html_before_mrkdwn():
    formatted = _format_slack_reply(
        "<b>Result:</b> **No new mentions** from [Nullion](https://www.nullion.ai/)."
    )

    assert "<b>" not in formatted
    assert "</b>" not in formatted
    assert "Result: *No new mentions*" in formatted
    assert "<https://www.nullion.ai/|Nullion>" in formatted


def test_slack_formatter_preserves_native_quote_blocks():
    formatted = _format_slack_reply("Body:\n> A puppy wakes\n> With bright eyes")

    assert formatted == "Body:\n> A puppy wakes\n> With bright eyes"


def test_slack_formatter_normalizes_quote_fenced_code_blocks():
    formatted = _format_slack_reply("'''svg\n<svg></svg>\n'''")

    assert formatted == "```svg\n<svg></svg>\n```"


def test_slack_chunks_keep_plain_fallback_clean():
    chunks = _slack_reply_chunks("<b>Result:</b> **No new mentions**", limit=100)

    assert chunks == [("Result: *No new mentions*", "Result: **No new mentions**")]
    fallback = _slack_plain_format_fallback_text(chunks[0][1])
    assert "<b>" not in fallback
    assert "</b>" not in fallback


@pytest.mark.asyncio
async def test_slack_callback_fallback_does_not_show_external_html():
    calls = []

    async def sender(text):
        calls.append(text)
        if len(calls) == 1:
            raise RuntimeError("formatted send failed")

    formatted, plain = _slack_reply_chunks("<b>Result:</b> **No new mentions**")[0]
    await _send_slack_callable_with_plain_fallback(sender, formatted_text=formatted, plain_text=plain)

    assert len(calls) == 2
    assert "<b>" not in calls[1]
    assert "</b>" not in calls[1]
    assert "Result: **No new mentions**" in calls[1]


class FakeTurnRegistration:
    def __init__(self, *, superseded: bool = False):
        self.waited = False
        self.finished = False
        self.superseded = superseded
        self.decision = SimpleNamespace(
            dependency_turn_ids=(),
            disposition=None,
            reason="test_dispatch_decision",
        )

    async def wait_for_dependencies(self):
        self.waited = True

    async def finish(self):
        self.finished = True

    async def is_superseded(self):
        return self.superseded


class FakeDispatchTracker:
    def __init__(self, *, superseded: bool = False):
        self.registration = FakeTurnRegistration(superseded=superseded)
        self.calls = []

    async def register(self, conversation_id, text, **kwargs):
        self.calls.append((conversation_id, text, kwargs))
        return self.registration


@pytest.mark.asyncio
async def test_slack_message_uses_dispatch_model_and_sends_plain_reply(monkeypatch):
    tracker = FakeDispatchTracker()
    async def no_attachments(*args, **kwargs):
        return []

    monkeypatch.setattr(slack_app, "GLOBAL_TURN_DISPATCH_TRACKER", tracker)
    monkeypatch.setattr(slack_app, "_download_slack_attachments", no_attachments)
    monkeypatch.setattr(
        slack_app,
        "handle_messaging_ingress_result",
        lambda service, ingress: SimpleNamespace(reply="Plain Slack reply", delivery_contract=None),
    )

    service = SimpleNamespace(model_client=object())
    settings = NullionSettings(slack=SlackSettings(operator_user_id="U1"))
    sent = []

    async def say(text):
        sent.append(text)

    await handle_slack_message(
        service,
        settings,
        event={"user": "U1", "text": "非英語の依頼", "channel": "C1", "event_ts": "1.0"},
        say=say,
    )

    assert sent == ["Plain Slack reply"]
    assert tracker.calls[0][0] == "slack:U1"
    assert tracker.calls[0][1] == "非英語の依頼"
    assert tracker.calls[0][2]["model_client"] is service.model_client
    assert tracker.registration.waited
    assert tracker.registration.finished


@pytest.mark.asyncio
async def test_slack_message_forwards_dispatch_decision_to_runtime_bridge(monkeypatch):
    tracker = FakeDispatchTracker()
    captured = []

    async def no_attachments(*args, **kwargs):
        return []

    def fake_handle_result(service, ingress, *, turn_dispatch_decision=None):
        captured.append(turn_dispatch_decision)
        return SimpleNamespace(reply="Plain Slack reply", delivery_contract=None)

    monkeypatch.setattr(slack_app, "GLOBAL_TURN_DISPATCH_TRACKER", tracker)
    monkeypatch.setattr(slack_app, "_download_slack_attachments", no_attachments)
    monkeypatch.setattr(slack_app, "handle_messaging_ingress_result", fake_handle_result)

    service = SimpleNamespace(model_client=object())
    settings = NullionSettings(slack=SlackSettings(operator_user_id="U1"))
    sent = []

    async def say(text):
        sent.append(text)

    await handle_slack_message(
        service,
        settings,
        event={"user": "U1", "text": "¿Eso requiere atención ahora?", "channel": "C1", "event_ts": "1.0"},
        say=say,
    )

    assert sent == ["Plain Slack reply"]
    assert captured == [tracker.registration.decision]


@pytest.mark.asyncio
async def test_slack_message_does_not_emit_working_ack_from_tool_activity(monkeypatch):
    tracker = FakeDispatchTracker()

    async def no_attachments(*args, **kwargs):
        return []

    def fake_handle_result(service, ingress, *, activity_callback=None, **kwargs):
        assert ingress.operator_chat_id == "slack:U1"
        assert kwargs["turn_dispatch_decision"] is tracker.registration.decision
        activity_callback({"id": "tool-email_search", "tool_name": "email_search"})
        return SimpleNamespace(reply="Email summary", delivery_contract=None)

    monkeypatch.setattr(slack_app, "GLOBAL_TURN_DISPATCH_TRACKER", tracker)
    monkeypatch.setattr(slack_app, "_download_slack_attachments", no_attachments)
    monkeypatch.setattr(slack_app, "handle_messaging_ingress_result", fake_handle_result)

    service = SimpleNamespace(model_client=object())
    settings = NullionSettings(slack=SlackSettings(operator_user_id="U1"))
    sent = []

    async def say(text):
        sent.append(text)
        return {"ok": True, "channel": "C1", "ts": str(len(sent))}

    await handle_slack_message(
        service,
        settings,
        event={"user": "U1", "text": "structured connector request", "channel": "C1", "event_ts": "1.0"},
        say=say,
        client=object(),
    )
    await asyncio.sleep(0.01)

    assert sent == ["Email summary"]


@pytest.mark.asyncio
async def test_slack_message_suppresses_delivery_when_active_turn_was_revised(monkeypatch):
    tracker = FakeDispatchTracker(superseded=True)

    async def no_attachments(*args, **kwargs):
        return []

    monkeypatch.setattr(slack_app, "GLOBAL_TURN_DISPATCH_TRACKER", tracker)
    monkeypatch.setattr(slack_app, "_download_slack_attachments", no_attachments)
    monkeypatch.setattr(
        slack_app,
        "handle_messaging_ingress_result",
        lambda service, ingress: SimpleNamespace(reply="Stale active turn reply", delivery_contract=None),
    )

    service = SimpleNamespace(model_client=object())
    settings = NullionSettings(slack=SlackSettings(operator_user_id="U1"))
    sent = []

    async def say(text):
        sent.append(text)

    await handle_slack_message(
        service,
        settings,
        event={"user": "U1", "text": "非英語の依頼", "channel": "C1", "event_ts": "1.0"},
        say=say,
    )

    assert sent == []
    assert tracker.registration.waited
    assert tracker.registration.finished


@pytest.mark.asyncio
async def test_discord_message_forwards_dispatch_decision_to_runtime_bridge(monkeypatch):
    tracker = FakeDispatchTracker()
    captured = []

    async def no_attachments(*args, **kwargs):
        return []

    def fake_handle_result(service, ingress, *, turn_dispatch_decision=None):
        captured.append(turn_dispatch_decision)
        return SimpleNamespace(reply="Plain Discord reply", delivery_contract=None)

    monkeypatch.setattr(discord_app, "GLOBAL_TURN_DISPATCH_TRACKER", tracker)
    monkeypatch.setattr(discord_app, "_download_discord_attachments", no_attachments)
    monkeypatch.setattr(discord_app, "require_authorized_ingress", lambda ingress, settings: True)
    monkeypatch.setattr(discord_app, "handle_messaging_ingress_result", fake_handle_result)
    monkeypatch.setattr(messaging_adapters, "record_platform_delivery_receipt", lambda receipt: None)

    channel = FakeDiscordChannel()
    channel.id = "DCHAN"
    message = SimpleNamespace(
        author=SimpleNamespace(id="D1", bot=False),
        content="¿Eso requiere atención ahora?",
        id="DMSG",
        channel=channel,
        attachments=[],
    )
    service = SimpleNamespace(model_client=object())

    await handle_discord_message(service, NullionSettings(), message)

    assert channel.sent == ["Plain Discord reply"]
    assert captured == [tracker.registration.decision]
    assert tracker.registration.waited
    assert tracker.registration.finished


@pytest.mark.asyncio
async def test_discord_message_does_not_emit_working_ack_from_tool_activity(monkeypatch):
    tracker = FakeDispatchTracker()

    async def no_attachments(*args, **kwargs):
        return []

    def fake_handle_result(service, ingress, *, activity_callback=None, **kwargs):
        assert ingress.operator_chat_id == "discord:D1"
        assert kwargs["turn_dispatch_decision"] is tracker.registration.decision
        activity_callback({"id": "tool-email_read", "tool_name": "email_read"})
        return SimpleNamespace(reply="Full email body summary", delivery_contract=None)

    monkeypatch.setattr(discord_app, "GLOBAL_TURN_DISPATCH_TRACKER", tracker)
    monkeypatch.setattr(discord_app, "_download_discord_attachments", no_attachments)
    monkeypatch.setattr(discord_app, "require_authorized_ingress", lambda ingress, settings: True)
    monkeypatch.setattr(discord_app, "handle_messaging_ingress_result", fake_handle_result)
    monkeypatch.setattr(messaging_adapters, "record_platform_delivery_receipt", lambda receipt: None)

    channel = FakeDiscordChannel()
    channel.id = "DCHAN"
    message = SimpleNamespace(
        author=SimpleNamespace(id="D1", bot=False),
        content="structured connector request",
        id="DMSG",
        channel=channel,
        attachments=[],
    )
    service = SimpleNamespace(model_client=object())

    await handle_discord_message(service, NullionSettings(), message)

    assert channel.sent == ["Full email body summary"]
    assert tracker.registration.finished


class FakeDiscordChannel:
    def __init__(self, *, fail_first: bool = False):
        self.fail_first = fail_first
        self.attempts = []
        self.sent = []

    async def send(self, text):
        self.attempts.append(text)
        if self.fail_first:
            self.fail_first = False
            raise RuntimeError("formatted send failed")
        self.sent.append(text)


@pytest.mark.asyncio
async def test_discord_chunks_strip_external_html():
    channel = FakeDiscordChannel()

    await _send_discord_chunks_with_plain_fallback(
        channel,
        "<b>Result:</b> **No new mentions** " * 120,
        limit=1900,
    )

    assert len(channel.sent) > 1
    assert all("<b>" not in text and "</b>" not in text for text in channel.sent)


@pytest.mark.asyncio
async def test_discord_plain_fallback_does_not_show_external_html():
    channel = FakeDiscordChannel(fail_first=True)

    await _send_discord_chunks_with_plain_fallback(
        channel,
        "<b>Result:</b> **No new mentions**",
        limit=1900,
    )

    assert len(channel.sent) == 1
    assert "<b>" not in channel.sent[0]
    assert "</b>" not in channel.sent[0]
    assert "Result: **No new mentions**" in channel.sent[0]


@pytest.mark.asyncio
async def test_discord_chunks_preserve_native_quote_blocks():
    channel = FakeDiscordChannel()

    await _send_discord_chunks_with_plain_fallback(
        channel,
        "Body:\n> A puppy wakes\n> With bright eyes",
        limit=1900,
    )

    assert channel.sent == ["Body:\n> A puppy wakes\n> With bright eyes"]


@pytest.mark.asyncio
async def test_discord_chunks_normalize_quote_fenced_code_blocks():
    channel = FakeDiscordChannel()

    await _send_discord_chunks_with_plain_fallback(channel, "'''svg\n<svg></svg>\n'''", limit=1900)

    assert channel.sent == ["```svg\n<svg></svg>\n```"]

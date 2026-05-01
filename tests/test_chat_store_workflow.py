from __future__ import annotations

import sqlite3

import pytest
from cryptography.fernet import Fernet

from nullion.chat_store import ChatStore, _channel_label_for_conversation, _make_title


@pytest.fixture()
def chat_store(tmp_path, monkeypatch) -> ChatStore:
    key = Fernet.generate_key()
    monkeypatch.setattr(ChatStore, "_load_or_create_key", lambda self: key)
    monkeypatch.setattr(ChatStore, "_alternate_decryption_keys", lambda self, primary_key: [])
    return ChatStore(tmp_path / "chat.db", key_path=tmp_path / "chat.key")


def test_chat_store_saves_encrypted_messages_and_metadata(chat_store: ChatStore) -> None:
    first_id = chat_store.save_message(
        "web:one",
        "user",
        "please remember api_key=sk-abcdefghijklmnopqrstuvwxyz",
        metadata={"attachments": [{"path": "/tmp/a.txt"}]},
    )
    second_id = chat_store.save_message("web:one", "bot", "Done", is_error=True)

    assert first_id > 0
    assert second_id > first_id
    assert chat_store.message_count("web:one") == 2

    messages = chat_store.load_messages("web:one")
    assert [message["role"] for message in messages] == ["user", "bot"]
    assert "sk-" not in messages[0]["text"]
    assert messages[0]["metadata"] == {"attachments": [{"path": "/tmp/a.txt"}]}
    assert messages[1]["is_error"] == 1

    conversation = chat_store.get_conversation("web:one")
    assert conversation is not None
    assert conversation["title"]
    assert conversation["status"] == "active"

    with sqlite3.connect(chat_store._path) as conn:
        raw = conn.execute("SELECT text, metadata FROM messages WHERE id = ?", (first_id,)).fetchone()
    assert raw[0].startswith("enc:v1:")
    assert raw[1].startswith("enc:v1:")


def test_chat_store_lists_channels_dates_and_archives(chat_store: ChatStore) -> None:
    chat_store.save_message("telegram:123", "user", "hello", channel="telegram:123", channel_label="Telegram")
    chat_store.save_message("telegram:123", "bot", "hi", channel="telegram:123", channel_label="Telegram")
    chat_store.save_message("slack:abc", "user", "hello", channel="slack:abc", channel_label="Slack")

    channels = chat_store.list_channels()
    assert {channel["channel"] for channel in channels} == {"telegram:123", "slack:abc"}

    month = chat_store.get_conversation("telegram:123")["created_at"][:7]
    day = chat_store.get_conversation("telegram:123")["created_at"][:10]
    assert chat_store.calendar_days("telegram:123", month)[day] == 2
    assert chat_store.list_conversations_for_channel_date("telegram:123", day)[0]["message_count"] == 2

    assert chat_store.archive_conversation("telegram:123") is True
    assert chat_store.archive_conversation("telegram:123") is False
    assert chat_store.list_conversations("archived", channel="telegram:123")[0]["id"] == "telegram:123"

    assert chat_store.delete_conversations_for_channel_date("slack:abc", day) == 1
    assert chat_store.get_conversation("slack:abc") is None
    assert chat_store.clear_conversation("telegram:123") is True
    assert chat_store.load_messages("telegram:123") == []
    assert chat_store.get_conversation("telegram:123")["status"] == "cleared"
    assert chat_store.delete_conversation_permanently("telegram:123") is True
    assert chat_store.delete_conversation_permanently("telegram:123") is False


def test_chat_store_imports_runtime_turns_without_duplicates(chat_store: ChatStore) -> None:
    turns = [
        {"conversation_id": "web", "user_message": "ignored", "assistant_reply": "ignored"},
        {"conversation_id": "telegram:42", "created_at": "2026-01-01T00:00:00+00:00", "user_message": "hi", "assistant_reply": "hello"},
        {"conversation_id": "telegram:42", "created_at": "2026-01-01T00:00:01+00:00", "user_message": "hi", "assistant_reply": "hello"},
        {"conversation_id": "discord:77", "created_at": "2026-01-02T00:00:00+00:00", "user_message": "", "assistant_reply": "pong"},
    ]

    assert chat_store.import_runtime_chat_turns(turns) == 3
    assert chat_store.import_runtime_chat_turns(turns) == 0
    assert [message["text"] for message in chat_store.load_messages("telegram:42")] == ["hi", "hello"]
    assert chat_store.get_conversation("discord:77")["channel_label"] == "Discord · 77"


def test_chat_store_rejects_empty_messages_and_bad_calendar_month(chat_store: ChatStore) -> None:
    chat_store.ensure_conversation("custom", channel="custom", channel_label="Custom")

    assert chat_store.save_message("custom", "user", "   ") == -1
    assert chat_store.message_count("custom") == 0
    with pytest.raises(ValueError):
        chat_store.calendar_days("custom", "2026-1")


def test_chat_store_helpers_cover_titles_and_channel_labels() -> None:
    assert _make_title("  short title  ") == "short title"
    long = "word " * 30
    assert len(_make_title(long)) <= 61
    assert _make_title(long).endswith("…")
    assert _channel_label_for_conversation("web") == "Web"
    assert _channel_label_for_conversation("web:abc") == "Web"
    assert _channel_label_for_conversation("telegram:123") == "Telegram · 123"
    assert _channel_label_for_conversation("slack:T1") == "Slack · T1"
    assert _channel_label_for_conversation("discord:D1") == "Discord · D1"
    assert _channel_label_for_conversation("custom") == "custom"

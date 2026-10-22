from __future__ import annotations

import logging
import sqlite3
from datetime import timezone
from zoneinfo import ZoneInfo

import pytest
from cryptography.fernet import Fernet

import nullion.chat_store as chat_store_module
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


def test_chat_store_hides_internal_scheduled_task_context(chat_store: ChatStore) -> None:
    internal_prompt = (
        "[Manual scheduled task run: Nullion monitor] "
        "This is runtime execution context, not a user-authored chat message."
    )
    chat_store.save_message("web:one", "user", "real user request")
    chat_store.save_message("web:one", "user", internal_prompt)
    chat_store.save_message("web:one", "bot", "Cron started")

    assert [message["text"] for message in chat_store.load_messages("web:one")] == [
        "real user request",
        "Cron started",
    ]

    imported = chat_store.import_runtime_chat_turns(
        [
            {
                "conversation_id": "web:two",
                "created_at": "2026-05-07T06:26:11+00:00",
                "user_message": internal_prompt,
                "assistant_reply": "Planner started",
            }
        ]
    )

    assert imported == 1
    assert chat_store.load_messages("web:two") == [
        {
            "id": 4,
            "role": "bot",
            "text": "Planner started",
            "metadata": {},
            "is_error": 0,
            "created_at": "2026-05-07T06:26:11+00:00",
        }
    ]


def test_chat_store_lists_channels_dates_and_archives(chat_store: ChatStore, monkeypatch) -> None:
    monkeypatch.setattr(chat_store_module, "_local_history_timezone", lambda: timezone.utc)
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


def test_chat_store_indexes_and_timing_logs(chat_store: ChatStore, monkeypatch, caplog) -> None:
    monkeypatch.setattr(chat_store_module, "_local_history_timezone", lambda: timezone.utc)
    monkeypatch.setattr(chat_store_module, "CHAT_SQLITE_MEASURE_ENABLED", True)
    monkeypatch.setattr(chat_store_module, "CHAT_SQLITE_SLOW_MS", 0)

    with caplog.at_level(logging.WARNING, logger="nullion.chat_store"):
        chat_store.save_message("telegram:123", "user", "hello", channel="telegram:123", channel_label="Telegram")
        chat_store.save_message("telegram:123", "bot", "hi", channel="telegram:123", channel_label="Telegram")
        month = chat_store.get_conversation("telegram:123")["created_at"][:7]
        day = chat_store.get_conversation("telegram:123")["created_at"][:10]
        assert chat_store.calendar_days("telegram:123", month)[day] == 2
        assert chat_store.list_conversations_for_channel_date("telegram:123", day)[0]["message_count"] == 2
        assert chat_store.load_messages("telegram:123")

    assert "chat SQLite save_message took" in caplog.text
    assert "chat SQLite calendar_days took" in caplog.text
    assert "chat SQLite list_conversations_for_channel_date took" in caplog.text
    assert "chat SQLite load_messages took" in caplog.text

    with sqlite3.connect(chat_store._path) as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            )
        }
        calendar_plan = "\n".join(
            row[3]
            for row in conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT date(m.created_at) AS day, COUNT(m.id) AS msg_count
                   FROM messages m
                   JOIN conversations c ON c.id = m.conversation_id
                   WHERE c.channel = ? AND m.created_at >= ? AND m.created_at < ?
                   GROUP BY day""",
                ("telegram:123", f"{month}-01", "2026-02-01"),
            )
        )
        day_plan = "\n".join(
            row[3]
            for row in conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT c.id, m.created_at
                   FROM conversations c
                   JOIN messages m ON m.conversation_id = c.id
                   WHERE c.id NOT LIKE 'web:live-stage-contract:%'
                     AND c.channel = ?
                     AND m.created_at >= ? AND m.created_at < ?
                   ORDER BY m.created_at ASC""",
                ("telegram:123", day, "2026-02-01"),
            )
        )

    assert "idx_messages_conv_id" in indexes
    assert "idx_messages_created_conv" in indexes
    assert "idx_conversations_status_channel_last" in indexes
    assert "USING" in calendar_plan
    assert "created_at>?" in calendar_plan
    assert "created_at<?" in calendar_plan
    assert "USING" in day_plan
    assert "created_at>?" in day_plan
    assert "created_at<?" in day_plan


def test_chat_store_imports_runtime_turns_without_duplicates(chat_store: ChatStore) -> None:
    turns = [
        {"conversation_id": "web", "user_message": "ignored", "assistant_reply": "ignored"},
        {"conversation_id": "telegram:42", "created_at": "2026-01-01T00:00:00+00:00", "user_message": "hi", "assistant_reply": "hello"},
        {"conversation_id": "telegram:42", "created_at": "2026-01-01T00:00:01+00:00", "user_message": "hi", "assistant_reply": "hello"},
        {"conversation_id": "discord:77", "created_at": "2026-01-02T00:00:00+00:00", "user_message": "", "assistant_reply": "pong"},
    ]

    assert chat_store.import_runtime_chat_turns(turns) == 5
    assert chat_store.import_runtime_chat_turns(turns) == 0
    assert [message["text"] for message in chat_store.load_messages("telegram:42")] == ["hi", "hello", "hi", "hello"]
    assert chat_store.get_conversation("discord:77")["channel_label"] == "Discord · 77"


def test_chat_store_history_uses_local_dates_and_hides_internal_web_probes(
    chat_store: ChatStore,
    monkeypatch,
) -> None:
    monkeypatch.setattr(chat_store_module, "_local_history_timezone", lambda: ZoneInfo("America/New_York"))
    turns = [
        {
            "conversation_id": "web:local-evening",
            "created_at": "2026-05-16T01:30:00+00:00",
            "user_message": "late local request",
            "assistant_reply": "late local reply",
        },
        {
            "conversation_id": "web:live-stage-contract:simple:probe:0",
            "created_at": "2026-05-16T01:35:00+00:00",
            "user_message": "internal probe",
            "assistant_reply": "probe reply",
        },
    ]

    assert chat_store.import_runtime_chat_turns(turns) == 2

    channels = chat_store.list_channels()
    assert channels == [
        {
            "channel": "web",
            "channel_label": "Web",
            "conversation_count": 1,
            "last_message_at": "2026-05-16T01:30:00+00:00",
        }
    ]
    assert chat_store.calendar_days("web", "2026-05") == {"2026-05-15": 2}
    assert chat_store.list_conversations_for_channel_date("web", "2026-05-16") == []
    day_conversations = chat_store.list_conversations_for_channel_date("web", "2026-05-15")
    assert [(item["id"], item["message_count"]) for item in day_conversations] == [
        ("web:local-evening", 2)
    ]
    assert [message["text"] for message in chat_store.load_messages("web:local-evening", local_date="2026-05-15")] == [
        "late local request",
        "late local reply",
    ]
    assert chat_store.load_messages("web:local-evening", local_date="2026-05-16") == []


def test_chat_store_dedupes_backend_import_followed_by_client_bot_save(chat_store: ChatStore) -> None:
    reply = "Hi Himan!\n\nFun fact: Bananas are berries."
    chat_store.import_runtime_chat_turns(
        [
            {
                "conversation_id": "web:reload",
                "created_at": "2026-05-04T01:26:25.102523+00:00",
                "user_message": "hi",
                "assistant_reply": reply,
            }
        ]
    )
    existing_bot_id = chat_store.load_messages("web:reload")[-1]["id"]

    saved_id = chat_store.save_message(
        "web:reload",
        "bot",
        reply,
        metadata={
            "artifacts": [
                {
                    "name": "screenshot.png",
                    "url": "/api/artifacts/abc123",
                    "media_type": "image/png",
                }
            ],
            "activity": [{"id": "respond", "label": "Writing response", "status": "done"}],
        },
    )

    messages = chat_store.load_messages("web:reload")
    assert saved_id == existing_bot_id
    assert [(message["role"], message["text"]) for message in messages] == [("user", "hi"), ("bot", reply)]
    assert messages[-1]["metadata"] == {
        "artifacts": [
            {
                "name": "screenshot.png",
                "url": "/api/artifacts/abc123",
                "media_type": "image/png",
            }
        ],
        "activity": [{"id": "respond", "label": "Writing response", "status": "done"}],
    }


def test_chat_store_imports_runtime_tool_artifacts_as_bot_metadata(chat_store: ChatStore, tmp_path) -> None:
    artifact = tmp_path / "email_preview.html"
    artifact.write_text("<html><body>preview</body></html>", encoding="utf-8")
    reply = "Done — preview it before sending."
    turn = {
        "conversation_id": "web:reload",
        "created_at": "2026-05-04T01:26:25.102523+00:00",
        "user_message": "make html email",
        "assistant_reply": reply,
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(artifact)},
            }
        ],
    }

    assert chat_store.import_runtime_chat_turns([turn]) == 2
    messages = chat_store.load_messages("web:reload")
    assert messages[-1]["metadata"] == {"artifacts": [{"path": str(artifact)}]}

    with sqlite3.connect(chat_store._path) as conn:
        conn.execute(
            "UPDATE messages SET metadata = NULL WHERE conversation_id = ? AND role = ?",
            ("web:reload", "bot"),
        )

    assert chat_store.import_runtime_chat_turns([turn]) == 0
    messages = chat_store.load_messages("web:reload")
    assert messages[-1]["metadata"] == {"artifacts": [{"path": str(artifact)}]}


def test_chat_store_load_hides_existing_adjacent_duplicate_bot_reply(chat_store: ChatStore) -> None:
    reply = "Here’s the screenshot of **cnn.com**:\n\n`artifacts/screenshot.png`"
    chat_store.save_message("web:dup", "user", "take a screenshot")
    chat_store.save_message("web:dup", "bot", reply)
    metadata = {
        "artifacts": [{"name": "screenshot.png", "url": "/api/artifacts/abc123", "media_type": "image/png"}],
        "activity": [{"id": "artifacts", "label": "Preparing artifacts", "status": "done"}],
    }
    metadata_text = chat_store._encrypt_text(
        chat_store_module.json.dumps(metadata, separators=(",", ":"), sort_keys=True)
    )
    with sqlite3.connect(chat_store._path) as conn:
        conn.execute(
            "INSERT INTO messages (conversation_id, role, text, metadata, is_error, created_at) "
            "VALUES (?, ?, ?, ?, 0, ?)",
            (
                "web:dup",
                "bot",
                chat_store._encrypt_text(reply),
                metadata_text,
                "2026-05-06T12:10:24+00:00",
            ),
        )

    messages = chat_store.load_messages("web:dup")

    assert [(message["role"], message["text"]) for message in messages] == [
        ("user", "take a screenshot"),
        ("bot", reply),
    ]
    assert messages[-1]["metadata"] == metadata


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

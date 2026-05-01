from __future__ import annotations

from types import SimpleNamespace

from nullion.config import NullionSettings, TelegramSettings
from nullion.users import NullionUser, UserRegistry


def test_gateway_lifecycle_events_are_persisted_bounded_and_filterable(tmp_path, monkeypatch) -> None:
    from nullion import gateway_notifications as gateway

    monkeypatch.setattr(gateway, "_STATE_DIR", tmp_path)
    monkeypatch.setattr(gateway, "_EVENTS_PATH", tmp_path / "events.json")
    monkeypatch.setattr(gateway, "_RESTART_MARKER_PATH", tmp_path / "restart.json")
    monkeypatch.setattr(gateway, "_MAX_EVENTS", 3)

    first = gateway.record_gateway_lifecycle_event("starting", "Gateway starting")
    second = gateway.record_gateway_lifecycle_event("ready", "Gateway ready")
    third = gateway.record_gateway_lifecycle_event("restarting", "Gateway restarting")
    fourth = gateway.record_gateway_lifecycle_event("online", "Gateway online")

    events = gateway.list_gateway_lifecycle_events()
    assert [event.event_id for event in events] == [second.event_id, third.event_id, fourth.event_id]
    assert gateway.list_gateway_lifecycle_events(since_id=second.event_id) == [third, fourth]
    assert [event.event_id for event in gateway.list_gateway_lifecycle_events(since_id=first.event_id)] == [
        second.event_id,
        third.event_id,
        fourth.event_id,
    ]

    gateway._EVENTS_PATH.write_text("{not json", encoding="utf-8")
    assert gateway.list_gateway_lifecycle_events() == []


def test_gateway_notifications_collect_operator_and_active_member_chat_ids(monkeypatch) -> None:
    from nullion import gateway_notifications as gateway

    settings = NullionSettings(telegram=TelegramSettings(operator_chat_id=" 123 ", bot_token="token", chat_enabled=True))
    registry = UserRegistry(
        users=[
            NullionUser("active", "Active", telegram_chat_id="456"),
            NullionUser("duplicate", "Duplicate", telegram_chat_id="123"),
            NullionUser("inactive", "Inactive", telegram_chat_id="789", active=False),
            NullionUser("blank", "Blank", telegram_chat_id=" "),
        ]
    )
    monkeypatch.setattr(gateway, "load_user_registry", lambda settings: registry)

    assert gateway._telegram_chat_ids(settings) == ("123", "456")

    monkeypatch.setattr(
        gateway,
        "load_user_registry",
        lambda settings: (_ for _ in ()).throw(RuntimeError("registry unavailable")),
    )
    assert gateway._telegram_chat_ids(settings) == ("123",)


def test_notify_telegram_gateway_event_honors_settings_and_sync_delivery(monkeypatch) -> None:
    from nullion import gateway_notifications as gateway

    sent: list[tuple[str, str, str]] = []
    monkeypatch.setattr(gateway, "_telegram_chat_ids", lambda settings: ("chat-1", "chat-2"))
    monkeypatch.setattr(gateway, "_send_telegram_message", lambda token, chat_id, text: sent.append((token, chat_id, text)))

    disabled = NullionSettings(telegram=TelegramSettings(bot_token="token", chat_enabled=False))
    gateway.notify_telegram_gateway_event("ignored", settings=disabled, async_delivery=False)
    assert sent == []

    enabled = NullionSettings(telegram=TelegramSettings(bot_token="token", chat_enabled=True))
    gateway.notify_telegram_gateway_event("Gateway online", settings=enabled, async_delivery=False)
    assert sent == [("token", "chat-1", "Gateway online"), ("token", "chat-2", "Gateway online")]


def test_notify_telegram_gateway_event_loads_default_env_file(tmp_path, monkeypatch) -> None:
    from nullion import gateway_notifications as gateway

    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                'NULLION_TELEGRAM_BOT_TOKEN="env-token"',
                'NULLION_TELEGRAM_OPERATOR_CHAT_ID="env-chat"',
                "NULLION_TELEGRAM_CHAT_ENABLED=true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env_path.chmod(0o600)

    sent: list[tuple[str, str, str]] = []
    monkeypatch.setattr(gateway, "_STATE_DIR", tmp_path)
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", raising=False)
    monkeypatch.delenv("NULLION_ENV_FILE", raising=False)
    monkeypatch.setattr(gateway, "load_user_registry", lambda settings: UserRegistry())
    monkeypatch.setattr(gateway, "_send_telegram_message", lambda token, chat_id, text: sent.append((token, chat_id, text)))

    gateway.notify_telegram_gateway_event("Gateway restarting", async_delivery=False)

    assert sent == [("env-token", "env-chat", "Gateway restarting")]


def test_gateway_restart_marker_records_and_completes_once(tmp_path, monkeypatch) -> None:
    from nullion import gateway_notifications as gateway

    monkeypatch.setattr(gateway, "_STATE_DIR", tmp_path)
    monkeypatch.setattr(gateway, "_EVENTS_PATH", tmp_path / "events.json")
    monkeypatch.setattr(gateway, "_RESTART_MARKER_PATH", tmp_path / "restart.json")
    delivered: list[str] = []
    monkeypatch.setattr(gateway, "notify_telegram_gateway_event", lambda text, **kwargs: delivered.append(text))

    restart = gateway.begin_gateway_restart(async_delivery=False)

    assert restart.kind == "restarting"
    assert gateway._RESTART_MARKER_PATH.exists()
    assert "restarting" in delivered[0]

    completed = gateway.complete_gateway_restart_if_needed()
    assert completed is not None
    assert completed.kind == "online"
    assert not gateway._RESTART_MARKER_PATH.exists()
    assert "back online" in delivered[1]
    assert gateway.complete_gateway_restart_if_needed() is None


def test_send_telegram_message_posts_expected_request(monkeypatch) -> None:
    from nullion import gateway_notifications as gateway

    captured: dict[str, object] = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"ok"

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["data"] = request.data.decode("utf-8")
        captured["method"] = request.get_method()
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(gateway.urllib.request, "urlopen", fake_urlopen)

    gateway._send_telegram_message("bot-token", "chat id", "hello world")

    assert captured == {
        "url": "https://api.telegram.org/botbot-token/sendMessage",
        "data": "chat_id=chat+id&text=hello+world",
        "method": "POST",
        "timeout": 8,
    }

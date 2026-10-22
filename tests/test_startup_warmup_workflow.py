from __future__ import annotations

import asyncio
import sys
from types import ModuleType
from types import SimpleNamespace


def test_chat_startup_warmup_primes_context_without_chat_turns(tmp_path, monkeypatch) -> None:
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.startup_warmup import run_chat_startup_warmup
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(crons, "list_crons", lambda *args, **kwargs: [])
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])

    result = run_chat_startup_warmup(
        runtime,
        registry=registry,
        settings=SimpleNamespace(enabled_skill_packs=()),
        surface="test",
    )

    assert result.ok
    assert "chat_history_store" in result.warmed
    assert "connector_context" in result.warmed
    assert "chat_context" in result.warmed
    assert "tool_scope" in result.warmed
    assert "cron_metadata" in result.warmed
    events = runtime.store.list_conversation_events()
    assert any(event.get("event_type") == "system.startup_warmup" for event in events)
    assert not any(event.get("event_type") == "conversation.chat_turn" for event in events)


def test_chat_startup_warmup_primes_surface_principals_and_context_warmers(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator, crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.startup_warmup import run_chat_startup_warmup
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(crons, "list_crons", lambda *args, **kwargs: [])
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    delivery_principals: list[str | None] = []
    context_calls: list[str] = []

    monkeypatch.setattr(
        chat_operator,
        "_chat_capability_inventory_prompt",
        lambda *args, **kwargs: "",
    )
    monkeypatch.setattr(
        chat_operator,
        "_enabled_skill_pack_index_prompt",
        lambda *args, **kwargs: "",
    )

    def fake_delivery_contract(runtime, *, principal_id=None, compact_message_only=False):  # noqa: ANN001
        del compact_message_only
        delivery_principals.append(principal_id)
        return ""

    monkeypatch.setattr(chat_operator, "_chat_delivery_contract_prompt", fake_delivery_contract)

    result = run_chat_startup_warmup(
        runtime,
        registry=registry,
        settings=SimpleNamespace(enabled_skill_packs=()),
        surface="telegram",
        principal_ids=("telegram_chat", "telegram_chat", "", None),
        context_warmers=(("first_turn_context", lambda: context_calls.append("first_turn_context")),),
    )

    assert result.ok
    assert delivery_principals == [None, "telegram_chat"]
    assert context_calls == ["first_turn_context"]
    assert "context:first_turn_context" in result.warmed


def test_chat_startup_warmup_scheduler_is_nonblocking_and_once_per_surface(tmp_path, monkeypatch) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    import nullion.startup_warmup as startup_warmup

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    calls: list[str] = []

    def fake_run_chat_startup_warmup(runtime, **kwargs):  # noqa: ANN001
        calls.append(kwargs["surface"])
        assert kwargs["principal_ids"] == ("telegram_chat",)
        assert kwargs["context_warmers"] == ()
        return startup_warmup.StartupWarmupResult(
            surface=kwargs["surface"],
            ok=True,
            elapsed_ms=0.1,
            warmed=("fake",),
        )

    monkeypatch.setattr(startup_warmup, "run_chat_startup_warmup", fake_run_chat_startup_warmup)

    first = startup_warmup.schedule_chat_startup_warmup(runtime, surface="telegram", principal_ids=("telegram_chat",))
    second = startup_warmup.schedule_chat_startup_warmup(runtime, surface="telegram")

    assert first is not None
    assert second is None
    first.join(timeout=2)
    assert not first.is_alive()
    assert calls == ["telegram"]


def test_slack_and_discord_adapters_schedule_startup_warmup(monkeypatch) -> None:
    from nullion import discord_app, slack_app, startup_warmup

    scheduled: list[tuple[str, tuple[str, ...]]] = []
    service = SimpleNamespace(runtime=object(), tool_registry=object(), agent_orchestrator=None)

    monkeypatch.setattr(
        startup_warmup,
        "schedule_chat_startup_warmup",
        lambda runtime, **kwargs: scheduled.append((str(kwargs["surface"]), tuple(kwargs.get("principal_ids") or ()))),
    )
    monkeypatch.setattr(
        slack_app,
        "load_settings",
        lambda env_path=None: SimpleNamespace(
            slack=SimpleNamespace(enabled=True, bot_token="xoxb-test", app_token="xapp-test", signing_secret="secret"),
        ),
    )
    monkeypatch.setattr(
        discord_app,
        "load_settings",
        lambda env_path=None: SimpleNamespace(
            discord=SimpleNamespace(enabled=True, bot_token="discord-test", operator_user_id="2001"),
        ),
    )

    slack_bolt = ModuleType("slack_bolt")
    slack_adapter = ModuleType("slack_bolt.adapter")
    slack_socket_mode = ModuleType("slack_bolt.adapter.socket_mode")
    slack_async_handler = ModuleType("slack_bolt.adapter.socket_mode.async_handler")
    slack_async_app_module = ModuleType("slack_bolt.async_app")

    class FakeAsyncSocketModeHandler:
        def __init__(self, app, token):  # noqa: ANN001
            self.app = app
            self.token = token

        async def start_async(self) -> None:
            return None

    class FakeAsyncApp:
        client = object()

        def __init__(self, **kwargs):  # noqa: ANN001
            self.kwargs = kwargs

        def event(self, name):  # noqa: ANN001
            return lambda fn: fn

        def command(self, name):  # noqa: ANN001
            return lambda fn: fn

    slack_async_handler.AsyncSocketModeHandler = FakeAsyncSocketModeHandler
    slack_async_app_module.AsyncApp = FakeAsyncApp
    monkeypatch.setitem(sys.modules, "slack_bolt", slack_bolt)
    monkeypatch.setitem(sys.modules, "slack_bolt.adapter", slack_adapter)
    monkeypatch.setitem(sys.modules, "slack_bolt.adapter.socket_mode", slack_socket_mode)
    monkeypatch.setitem(sys.modules, "slack_bolt.adapter.socket_mode.async_handler", slack_async_handler)
    monkeypatch.setitem(sys.modules, "slack_bolt.async_app", slack_async_app_module)

    discord_module = ModuleType("discord")

    class FakeIntents:
        message_content = False

        @classmethod
        def default(cls):
            return cls()

    class FakeDiscordClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed = True
            return False

        def __init__(self, *, intents):  # noqa: ANN001
            self.intents = intents

        async def start(self, token: str) -> None:
            self.token = token

    discord_module.Intents = FakeIntents
    discord_module.Client = FakeDiscordClient
    monkeypatch.setitem(sys.modules, "discord", discord_module)

    asyncio.run(slack_app.run_slack_app(service_builder=lambda **kwargs: service))
    asyncio.run(discord_app.run_discord_app(service_builder=lambda **kwargs: service))

    assert scheduled == [("slack", ("telegram_chat",)), ("discord", ("telegram_chat",))]

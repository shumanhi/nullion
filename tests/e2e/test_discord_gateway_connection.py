"""Actual Discord SDK HTTP/WebSocket, Nullion runtime and SQLite. No Discord account.
Only the external Discord endpoints and external model are simulated.
"""

import asyncio
import json
import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1", reason="container E2E"
)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "invalid_token", "disallowed_intent"])
async def test_discord_sdk_connection_and_authorized_chat(
    tmp_path, monkeypatch, failure
):
    import discord
    from aiohttp import WSMsgType, web
    from discord.gateway import DiscordWebSocket
    from discord.http import Route
    from nullion import chat_store, discord_app, users
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import load_settings
    from nullion.messaging_adapters import MessagingAdapterConfigurationError
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import ChatOperatorService
    from nullion.tools import ToolRegistry
    from yarl import URL

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "home" / "users.json")
    monkeypatch.setattr(chat_store, "_store", None)
    env = tmp_path / ".env"
    env.write_text(
        "NULLION_DISCORD_ENABLED=true\nNULLION_DISCORD_BOT_TOKEN=fixture-token\nNULLION_DISCORD_OPERATOR_USER_ID=2001\n"
    )
    env.chmod(0o600)
    settings = load_settings(env_path=env)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    runtime.active_tool_registry = registry

    class Model:
        def __init__(self):
            self.calls = 0
            self.relationship_calls = 0
            self.requests = []

        def create(self, **kwargs):
            if "allowed_target_indexes" in json.dumps(kwargs.get("messages", [])):
                self.relationship_calls += 1
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "relationship": "separate",
                                    "effect": "continue",
                                    "confidence": 1,
                                    "target_index": None,
                                    "target_is_explicit": False,
                                }
                            ),
                        }
                    ]
                }
            self.calls += 1
            self.requests.append(
                {
                    "tools": [t["name"] for t in kwargs.get("tools", [])],
                    "system": str(kwargs.get("system", ""))[:100],
                    "max_tokens": kwargs.get("max_tokens"),
                    "timeout": kwargs.get("timeout"),
                }
            )
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "56"}],
            }

    model = Model()
    service = ChatOperatorService(
        runtime=runtime,
        settings=settings,
        bot_token="unused",
        operator_chat_id=None,
        model_client=model,
        agent_orchestrator=AgentOrchestrator(model_client=model),
    )
    bot = {
        "id": "999",
        "username": "FixtureBot",
        "discriminator": "0001",
        "avatar": None,
        "bot": True,
    }
    owner = {
        "id": "2001",
        "username": "FixtureUser",
        "discriminator": "0001",
        "avatar": None,
        "bot": False,
    }
    seen = []
    wire = []
    finished = asyncio.Event()
    received = asyncio.Queue()
    identifies = []
    elapsed = []

    def message(mid, author, content):
        return {
            "id": str(mid),
            "channel_id": "555",
            "author": author,
            "content": content,
            "timestamp": "2026-10-13T16:00:00+00:00",
            "edited_timestamp": None,
            "tts": False,
            "mention_everyone": False,
            "mentions": [],
            "mention_roles": [],
            "attachments": [],
            "embeds": [],
            "pinned": False,
            "type": 0,
        }

    def json_response(value, **kwargs):
        return web.Response(
            body=json.dumps(value).encode(), content_type="application/json", **kwargs
        )

    async def api(request):
        seen.append((request.method, request.path))
        assert request.headers.get("Authorization") == "Bot fixture-token"
        if failure == "invalid_token":
            return json_response(
                {"message": "401: Unauthorized", "code": 0}, status=401
            )
        if request.path.endswith("/users/@me"):
            return json_response(bot)
        if request.path.endswith("/oauth2/applications/@me"):
            return json_response(
                {
                    "id": "999",
                    "name": "Fixture",
                    "description": "",
                    "icon": None,
                    "bot_public": False,
                    "bot_require_code_grant": False,
                    "owner": owner,
                    "verify_key": "",
                    "flags": 1 << 19,
                }
            )
        if request.path.endswith("/typing"):
            return web.Response(status=204)
        if request.path.endswith("/messages") and request.method == "POST":
            payload = await request.json()
            wire.append(payload["content"])
            await received.put(payload["content"])
            return json_response(message(9000 + len(wire), bot, payload["content"]))
        raise AssertionError((request.method, request.path))

    async def gateway(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json({"op": 10, "d": {"heartbeat_interval": 45000}})

        async def exercise():
            await ws.send_json(
                {
                    "op": 0,
                    "s": 1,
                    "t": "READY",
                    "d": {
                        "v": 10,
                        "user": bot,
                        "guilds": [],
                        "session_id": "fixture-session",
                        "resume_gateway_url": str(base + "/gateway"),
                        "application": {"id": "999", "flags": 1 << 19},
                    },
                }
            )
            await asyncio.wait_for(clients[0].wait_until_ready(), 5)
            deadline = time.perf_counter() + 5
            while (
                not hasattr(clients[0], "_nullion_reminder_task")
                and time.perf_counter() < deadline
            ):
                await asyncio.sleep(0.001)
            assert hasattr(clients[0], "_nullion_reminder_task")
            unknown = {**owner, "id": "2002"}
            await ws.send_json(
                {
                    "op": 0,
                    "s": 2,
                    "t": "MESSAGE_CREATE",
                    "d": message(5002, unknown, "7 × 8"),
                }
            )
            assert "not authorized" in await asyncio.wait_for(received.get(), 10)
            for index, prompt in enumerate(
                ["7 × 8", "7 × 8 の答えだけ", "Solo 7 × 8"], start=3
            ):
                start = time.perf_counter()
                await ws.send_json(
                    {
                        "op": 0,
                        "s": index,
                        "t": "MESSAGE_CREATE",
                        "d": message(5000 + index, owner, prompt),
                    }
                )
                assert await asyncio.wait_for(received.get(), 10) == "56"
                deadline = time.perf_counter() + 2
                while (
                    await discord_app.GLOBAL_TURN_DISPATCH_TRACKER.active_turn_ids(
                        "discord:2001"
                    )
                    and time.perf_counter() < deadline
                ):
                    await asyncio.sleep(0.001)
                assert (
                    not await discord_app.GLOBAL_TURN_DISPATCH_TRACKER.active_turn_ids(
                        "discord:2001"
                    )
                )
                elapsed.append(time.perf_counter() - start)
            finished.set()

        exercise_task = None
        try:
            async for packet in ws:
                if packet.type != WSMsgType.TEXT:
                    continue
                payload = json.loads(packet.data)
                if payload["op"] == 1:
                    await ws.send_json({"op": 11, "d": None})
                if payload["op"] == 2:
                    identifies.append(payload["d"])
                    if failure == "disallowed_intent":
                        await ws.close(code=4014)
                        break
                    exercise_task = asyncio.create_task(exercise())
        finally:
            if exercise_task:
                exercise_task.cancel()
                await asyncio.gather(exercise_task, return_exceptions=True)
        return ws

    app = web.Application()
    app.router.add_get("/gateway", gateway)
    app.router.add_route("*", "/{path:.*}", api)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
    monkeypatch.setattr(Route, "BASE", base)
    monkeypatch.setattr(DiscordWebSocket, "DEFAULT_GATEWAY", URL(base + "/gateway"))
    clients = []
    original_init = discord.Client.__init__

    def capture_client(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        clients.append(self)

    monkeypatch.setattr(discord.Client, "__init__", capture_client)
    task = asyncio.create_task(
        discord_app.run_discord_app(
            checkpoint_path=tmp_path / "runtime.db",
            env_path=env,
            service_builder=lambda **kwargs: service,
        )
    )
    try:
        if failure:
            with pytest.raises(
                MessagingAdapterConfigurationError,
                match="rejected the bot token"
                if failure == "invalid_token"
                else "Message Content intent",
            ):
                await asyncio.wait_for(task, 15)
        else:
            await asyncio.wait_for(finished.wait(), 20)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert model.calls == 3, model.requests
            events = [
                e
                for e in runtime.store.list_conversation_events("discord:2001")
                if e.get("event_type") == "conversation.chat_turn"
            ]
            assert len(events) == 3 and all(
                e["assistant_reply"] == "56" for e in events
            )
            receipts = list(
                (tmp_path / "home").rglob("delivery_receipts.jsonl")
            ) + list((tmp_path / "nullion-data").rglob("delivery_receipts.jsonl"))
            assert receipts
            rows = [json.loads(l) for p in receipts for l in p.read_text().splitlines()]
            assert (
                len(
                    [
                        r
                        for r in rows
                        if r["channel"] == "discord"
                        and r["status"] == "succeeded"
                        and r["target_id"] == "555"
                    ]
                )
                == 3
            )
            assert sum(elapsed) / len(elapsed) < 1.5 and max(elapsed) < 2.5, elapsed
        assert clients[0].is_closed()
        if failure is None:
            assert clients[0]._nullion_reminder_task.done()
        assert clients[0].http._HTTPClient__session.closed
        if failure != "invalid_token":
            assert identifies[0]["intents"] & (1 << 15)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await runner.cleanup()


def test_discord_settings_api_requires_an_authorized_user_and_preserves_token(
    tmp_path, monkeypatch
):
    from fastapi.testclient import TestClient
    from nullion import users, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "home" / "users.json")
    for key in ("NULLION_DISCORD_BOT_TOKEN", "NULLION_DISCORD_OPERATOR_USER_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("NULLION_DISCORD_ENABLED", "false")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(runtime, orchestrator=None, registry=ToolRegistry())
    with TestClient(app) as client:
        r = client.post(
            "/api/config",
            json={"discord_enabled": True, "discord_bot_token": "fixture-token"},
        )
        assert r.status_code == 200, r.text
        data = client.get("/api/config").json()
        assert (
            data["discord_bot_token_set"]
            and not data["discord_authorized_user_configured"]
        )
        status = next(s for s in data["chat_services"] if s["name"] == "discord")
        assert not status["configured"] and status["status"] == "Needs authorized user"
        for identity in ("@someone", 0, False, str(2**64), "9" * 5000):
            r = client.post(
                "/api/config",
                json={
                    "discord_operator_user_id": identity,
                    "discord_bot_token": "should-not-save",
                },
            )
            assert r.status_code == 400
            assert os.environ["NULLION_DISCORD_BOT_TOKEN"] == "fixture-token"
            assert "should-not-save" not in (tmp_path / ".env").read_text()
        assert (
            client.post(
                "/api/config", json={"discord_operator_user_id": "2001"}
            ).status_code
            == 200
        )
        data = client.get("/api/config").json()
        assert (
            data["discord_operator_user_id"] == "2001"
            and data["discord_authorized_user_configured"]
        )
        assert "fixture-token" not in json.dumps(data)
        assert users.is_authorized_messaging_identity(
            "discord", "2001", web_app.load_settings()
        )

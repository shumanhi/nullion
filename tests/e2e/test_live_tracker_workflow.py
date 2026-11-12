"""Live tracking through real chat, SQLite, workers, browser and alert delivery.

The model and a synthetic external observation endpoint are the only fakes.
"""

from tests.workflow_provider_protocol import workflow_provider_reply, fixture_action_acknowledgement
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
from types import SimpleNamespace
from urllib.request import urlopen

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E"
)


def text(value):
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": value}]}


def use(name, args):
    # These pre-existing scenarios exercise first-match contracts explicitly.
    # Continuous defaults and repeated changes have their own route/worker E2E.
    if name == 'create_live_tracker':
        args = {'stop_after_match': True, **args}
    return {
        "stop_reason": "tool_use",
        "content": [{"type": "tool_use", "id": name, "name": name, "input": args}],
    }


@pytest.fixture
def live_fixture(tmp_path, monkeypatch):
    from nullion import chat_store, crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolRegistry,
        ToolSpec,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolResult,
        register_cron_tools,
    )
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.agent_orchestrator import AgentOrchestrator

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "false")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "false")
    chat_store._store = None
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    state = SimpleNamespace(
        available=False,
        slot=None,
        reads=0,
        hold=False,
        entered=threading.Event(),
        release=threading.Event(),
        extra={},
    )

    class Source(BaseHTTPRequestHandler):
        def do_GET(self):
            state.reads += 1
            if state.hold:
                state.entered.set()
                assert state.release.wait(20)
            body = json.dumps(
                {"available": state.available, "slot": state.slot, **state.extra}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
    threading.Thread(target=source.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{source.server_port}/observation"
    registry = ToolRegistry()

    def read(inv):
        with urlopen(inv.arguments["url"], timeout=15) as response:
            output = json.load(response)
        return ToolResult(inv.invocation_id, inv.tool_name, "completed", output)

    registry.register(
        ToolSpec(
            "observation_read",
            "Read a narrow public observation",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            15,
            capability_tags=("public_web",),
        ),
        read,
    )
    register_cron_tools(registry)
    register_live_tracker_tools(registry)
    runtime.active_tool_registry = registry

    class Model:
        mode = "live"
        prepared = False
        created = False
        semantic = False
        quote_evidence = False
        hold_on_create = False
        duration = 90
        bad_pointer = False
        bad_attempted = False
        final_delay = 0
        agent_calls = 0
        semantic_calls = 0
        classifier_calls = 0

        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names = {r["name"] for r in kw.get("tools", [])}
            system = str(kw.get("system") or "")
            if not names:
                if system.startswith("Evaluate only the supplied current observations"):
                    self.semantic_calls += 1
                    packet = json.loads(kw["messages"][0]["content"][0]["text"])
                    output = packet["observations"][0]
                    return text(
                        json.dumps(
                            {
                                "status": (
                                    "matched" if output["available"] else "no_match"
                                ),
                                "summary": f"Slot {output['slot']} is available.",
                                "evidence": [{"step": 0, "pointer": "/content", "kind": "quote",
                                              "value": json.dumps({"slot": output["slot"]})[1:-1]}] if self.quote_evidence else [
                                    {
                                        "step": 0,
                                        "pointer": "/slot",
                                        "value": output["slot"],
                                    }
                                ],
                            }
                        )
                    )
                if '"tracking_mode":' in system:
                    self.classifier_calls += 1
                    return text(
                        json.dumps(
                            {
                                "tracking_mode": (
                                    self.mode
                                    if self.mode != "missing_duration"
                                    else "live"
                                ),
                                "requested_tool_names": (
                                    ["create_live_tracker", "observation_read"]
                                    if self.mode == "live"
                                    else []
                                ),
                                "confidence": 1,
                            }
                        )
                    )
                return text("{}")
            self.agent_calls += 1
            if self.mode == "plain":
                return text("Hello.")
            if self.mode == "clarify":
                if self.agent_calls % 2:
                    return use(
                        "request_tool_scope",
                        {"capabilities": ["live_tracking"], "tracking_mode": "clarify"},
                    )
                return text("Choose the tracking lifetime.")
            action = (
                "create_cron" if self.mode == "recurring" else "create_live_tracker"
            )
            if action not in names or (self.mode != 'recurring' and not self.prepared and 'observation_read' not in names):
                return use(
                    "request_tool_scope",
                    {
                        "capabilities": (
                            ["scheduler_mutate"]
                            if self.mode == "recurring"
                            else ["live_tracking", "web"]
                        ),
                        "tracking_mode": (
                            "recurring" if self.mode == "recurring" else "live"
                        ),
                        "tool_names": (
                            [action]
                            if self.mode == "recurring"
                            else [action, "observation_read"]
                        ),
                        "required_tool_names": [action],
                        **(
                            {"scheduler_action": "mutate"}
                            if self.mode == "recurring"
                            else {}
                        ),
                    },
                )
            if self.mode == "recurring":
                if not self.created:
                    self.created = True
                    return use(
                        action,
                        {
                            "name": "Daily sensor report",
                            "schedule": "0 12 * * *",
                            "task": "Summarize the daily sensor reading.",
                            "enabled": False,
                        },
                    )
                return text(fixture_action_acknowledgement(kw) or "The recurring schedule is saved.")
            if not self.prepared and "observation_read" in names:
                self.prepared = True
                return use("observation_read", {"url": url})
            if not self.created:
                self.created = True
                if self.hold_on_create:
                    state.entered.clear()
                    state.hold = True
                condition = (
                    {
                        "kind": "semantic",
                        "instruction": "Alert only once the requested slot is actually available and include the slot label.",
                    }
                    if self.semantic
                    else {
                        "kind": "structured",
                        "conditions": [
                            {
                                "step": 0,
                                "pointer": "/available",
                                "op": "equals",
                                "value": True,
                            },
                            {
                                "step": 0,
                                "pointer": "/slot",
                                "op": "exists",
                                "label": "Slot",
                            },
                        ],
                        "summary": "The requested slot is available.",
                    }
                )
                if self.bad_pointer and not self.bad_attempted:
                    self.bad_attempted = True
                    self.created = False
                    condition["conditions"][0]["pointer"] = "/output/available"
                return use(
                    action,
                    {
                        "name": "Cedar slot watch",
                        "stop_after_match": True,
                        "interval_seconds": 5,
                        **(
                            {}
                            if self.mode == "missing_duration"
                            else {"duration_seconds": self.duration}
                        ),
                        "probe": [
                            {"tool_name": "observation_read", "arguments": {"url": url}}
                        ],
                        "matcher": condition,
                    },
                )
            time.sleep(self.final_delay)
            return text(fixture_action_acknowledgement(kw) or "Tracking request processed.")

    model = Model()
    f = SimpleNamespace(
        runtime=runtime,
        registry=registry,
        model=model,
        source=state,
        url=url,
        orchestrator=AgentOrchestrator(model_client=model),
        tmp_path=tmp_path,
    )
    yield f
    state.release.set()
    source.shutdown()
    source.server_close()


def wait_for(predicate, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    assert predicate()


@pytest.mark.parametrize(
    "prompt",
    [
        "Watch the slot for 90 seconds and alert as soon as it is assigned.",
        "Vigila la plaza durante 90 segundos y avísame en cuanto esté asignada.",
        "90秒間だけ枠を確認して、割り当てられたらすぐ知らせてください。",
    ],
)
def test_real_chat_and_worker_repeat_prepared_probe_then_alert_once(
    live_fixture, prompt
):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion import crons
    from nullion.chat_store import get_chat_store

    f = live_fixture
    conversation = "web:live-source"
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        response = client.post(
            "/api/chat", json={"text": prompt, "conversation_id": conversation}
        )
        assert response.status_code == 200, response.text
        jobs = client.get("/api/live-trackers").json()["trackers"]
        assert len(jobs) == 1, (response.json(), f.model.agent_calls)
        job = jobs[0]
        assert job["state"] == "active" and job["interval_seconds"] == 5
        assert (
            crons.list_crons() == []
        ), "A live observation must never create a recurring cron"
        assert "Live tracker" in response.json()["text"]
        turn = next(e for e in reversed(f.runtime.store.list_conversation_events(conversation))
                    if e.get('event_type') == 'conversation.chat_turn')
        accepted = next(row['output']['deferred_execution'] for row in turn['tool_results']
                        if row['tool_name'] == 'create_live_tracker' and row['status'] == 'completed')
        assert accepted['handle'] == job['id'] and accepted['configuration']['duration_seconds'] == 90
        assert accepted['configuration']['accepted_at'] == job['created_at']
        agent_calls = f.model.agent_calls
        wait_for(
            lambda: client.get("/api/live-trackers").json()["trackers"][0]["checks"]
            >= 1
        )
        assert f.model.agent_calls == agent_calls and f.model.semantic_calls == 0
        f.source.available = True
        f.source.slot = "B7"
        wait_for(
            lambda: client.get("/api/live-trackers").json()["trackers"][0]["state"]
            == "matched"
        )
        wait_for(
            lambda: client.get("/api/live-trackers").json()["trackers"][0][
                "notification_status"
            ]
            == "sent"
        )
        messages = get_chat_store().load_messages(conversation)
        alerts = [
            m
            for m in messages
            if (m.get("metadata") or {}).get("source") == "live_tracker"
        ]
        assert (
            len(alerts) == 1
            and "B7" in alerts[0]["text"]
            and "Tracking has stopped" in alerts[0]["text"]
        ), alerts
        assert "probe" not in alerts[0]["text"] and '{"' not in alerts[0]["text"]
        assert f.model.agent_calls == agent_calls and f.model.semantic_calls == 0
        current = client.get("/api/live-trackers").json()["trackers"][0]
        assert current["checks"] >= 2 and "probe" not in current
        f.runtime.checkpoint(force=True)
        from nullion.live_tracking import LiveTrackerStore

        assert (
            LiveTrackerStore(f.tmp_path / "runtime.db").get(job["id"])["state"]
            == "matched"
        )
        assert any(
            e.event_type == "live_tracker.notification.sent"
            for e in f.runtime.store.list_events()
        )


def test_browser_live_alert_and_stop_during_inflight_check(live_fixture):
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import (
        _UvicornThread,
        _free_port,
        _open_page,
    )
    from playwright.sync_api import expect

    f = live_fixture
    f.model.hold_on_create = True
    app = create_app(f.runtime, f.orchestrator, f.registry)
    with _UvicornThread(app, _free_port()) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#user-input").fill(
                "Watch the slot for 90 seconds and alert me once it is available."
            )
            page.locator("#user-input").press("Enter")
            expect(page.locator("#messages")).to_contain_text(
                "Live tracker", timeout=12000
            )
            assert f.source.entered.wait(8), "Prepared probe never started"
            row = page.locator("#live-trackers-dash [data-live-tracker-id]")
            expect(row).to_contain_text("Cedar slot watch", timeout=12000)
            expect(row).to_contain_text("Every 5 seconds")
            row.get_by_role("button", name="Stop", exact=True).click()
            expect(row).to_contain_text("Stopped", timeout=10000)
            f.source.available = True
            f.source.slot = "B7"
            f.source.release.set()
            time.sleep(0.3)
            assert (
                app.state.live_tracker_store.list("workspace_admin")[0]["state"]
                == "stopped"
            )
            assert (
                "The requested slot is available."
                not in page.locator("#messages").inner_text()
            )
            assert not errors
        finally:
            f.source.release.set()
            browser.close()
            playwright.stop()


def test_real_browser_receives_background_match_without_another_chat_turn(live_fixture):
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import (
        _UvicornThread,
        _free_port,
        _open_page,
    )
    from playwright.sync_api import expect

    f = live_fixture
    with _UvicornThread(
        create_app(f.runtime, f.orchestrator, f.registry), _free_port()
    ) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#user-input").fill(
                "Observe the slot for 90 seconds. Tell me once it is available."
            )
            page.locator("#user-input").press("Enter")
            expect(page.locator("#messages")).to_contain_text(
                "Live tracker", timeout=12000
            )
            f.source.available = True
            f.source.slot = "B7"
            expect(page.locator("#messages")).to_contain_text("Slot: B7", timeout=15000)
            expect(page.locator("#live-trackers-dash")).to_contain_text(
                "Match found", timeout=10000
            )
            assert not errors
        finally:
            browser.close()
            playwright.stop()


@pytest.mark.parametrize(
    "mode,prompt,fragment",
    [
        (
            "clarify",
            "Keep watching this and tell me when it changes",
            "1. Live tracking",
        ),
        ("clarify", "Sigue vigilándolo y avísame si cambia", "2. Recurring schedule"),
        (
            "missing_duration",
            "Track this urgently until it is available",
            "1. 30 minutes",
        ),
    ],
)
def test_unclear_tracking_intent_or_lifetime_asks_before_creating(
    live_fixture, mode, prompt, fragment
):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion import crons

    f = live_fixture
    f.model.mode = mode
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        response = client.post(
            "/api/chat", json={"text": prompt, "conversation_id": "web:clarification"}
        )
        assert response.status_code == 200, response.text
        assert fragment in response.json()["text"], response.json()
        assert f.model.agent_calls <= (3 if mode == "missing_duration" else 1)
        assert client.get("/api/live-trackers").json()["trackers"] == []
        assert crons.list_crons() == []


def test_recurring_task_stays_a_cron_and_plain_chat_keeps_fast_path(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion import crons

    f = live_fixture
    f.model.mode = "plain"
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        elapsed = []
        for index, prompt in enumerate(["Hello", "Hola", "こんにちは", "مرحبا"]):
            before = f.model.agent_calls
            start = time.perf_counter()
            reply = client.post(
                "/api/chat",
                json={"text": prompt, "conversation_id": f"web:plain-{index}"},
            )
            elapsed.append(time.perf_counter() - start)
            assert (
                reply.json()["text"] == "Hello." and f.model.agent_calls == before + 1
            )
        assert (
            f.model.classifier_calls == 0
            and sum(elapsed) / len(elapsed) < 0.4
            and max(elapsed) < 0.6
        ), elapsed
        f.model.mode = "recurring"
        result = client.post(
            "/api/chat",
            json={
                "text": "Informe diario del sensor a mediodía. Déjalo desactivado.",
                "conversation_id": "web:regular",
            },
        )
        assert (
            result.status_code == 200 and "Cron created" in result.json()["text"]
        ), result.json()
        assert (
            len(crons.list_crons()) == 1
            and client.get("/api/live-trackers").json()["trackers"] == []
        )


def test_semantic_probe_uses_bounded_judge_without_replanning(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app

    f = live_fixture
    f.model.semantic = True
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        result = client.post(
            "/api/chat",
            json={
                "text": "Observe this slot for 90 seconds and alert when assigned.",
                "conversation_id": "web:semantic",
            },
        )
        assert result.status_code == 200
        calls = f.model.agent_calls
        f.source.available = True
        f.source.slot = "B7"
        wait_for(
            lambda: client.get("/api/live-trackers").json()["trackers"][0][
                "notification_status"
            ]
            == "sent"
        )
        assert f.model.semantic_calls >= 1 and f.model.agent_calls == calls


def test_web_settings_are_persistent_and_invalid_requests_are_rejected(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.live_tracking import LiveTrackerStore

    f = live_fixture
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        assert (
            client.put(
                "/api/live-trackers/settings", json={"max_parallel_jobs": 2}
            ).status_code
            == 200
        )
        assert client.get("/api/live-trackers").json()["max_parallel_jobs"] == 2
        assert LiveTrackerStore(f.tmp_path / "runtime.db").capacity() == 2
        for invalid in [[], None, 3, "oops"]:
            assert (
                client.put("/api/live-trackers/settings", json=invalid).status_code
                == 400
            )
            assert client.post("/api/live-trackers", json=invalid).status_code == 400
        assert client.post("/api/live-trackers/unknown/stop").status_code == 404


def test_numbered_duration_followup_retains_clarification_context(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app

    f = live_fixture
    f.model.mode = "missing_duration"
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        first = client.post(
            "/api/chat",
            json={
                "text": "Vigila esta plaza y avísame cuando esté libre.",
                "conversation_id": "web:duration",
            },
        )
        assert "1. 30 minutes" in first.json()["text"]
        original = f.model.create
        packets = []

        def followup(**kwargs):
            packets.append(kwargs)
            reply = original(**kwargs)
            return reply

        f.model.create = followup
        f.model.mode = "live"
        f.model.created = False
        f.model.prepared = False
        f.model.duration = 3600
        second = client.post(
            "/api/chat", json={"text": "2", "conversation_id": "web:duration"}
        )
        if "Live tracker" not in second.json()["text"]:
            from pathlib import Path
            target=Path('.e2e-artifacts/scheduled-budget-20261108/duration-followup-events.json');target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(json.dumps(f.runtime.store.list_conversation_events('web:duration'),default=str))
        assert "Live tracker" in second.json()["text"], second.json()
        assert any(
            "1. 30 minutes" in json.dumps(p.get("messages", [])) for p in packets
        )
        jobs = client.get("/api/live-trackers").json()["trackers"]
        assert len(jobs) == 1
        from datetime import datetime

        assert (
            datetime.fromisoformat(jobs[0]["expires_at"])
            - datetime.fromisoformat(jobs[0]["created_at"])
        ).total_seconds() == 3600


def test_telegram_handler_creates_tracker_and_background_transport_delivers_once(
    live_fixture, monkeypatch
):
    import asyncio
    from nullion import telegram_app, web_app
    from nullion.config import load_settings
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker
    from fastapi.testclient import TestClient

    f = live_fixture
    monkeypatch.setenv(
        "NULLION_TELEGRAM_BOT_TOKEN", "123456:abcdefghijklmnopqrstuvwxyz"
    )
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "202",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    service = telegram_app.ChatOperatorService(
        runtime=f.runtime,
        settings=settings,
        bot_token=settings.telegram.bot_token,
        operator_chat_id="202",
        model_client=f.model,
        agent_orchestrator=f.orchestrator,
    )
    service._turn_dispatch_tracker = AsyncTurnDispatchTracker()
    wire = []
    alerts = []

    class Sent:
        message_id = 1000

        async def edit_text(self, text, **kwargs):
            wire.append(text)
            return self

        async def delete(self):
            pass

    class Message:
        text = "Vigila la plaza durante 90 segundos y avísame en cuanto esté asignada."
        message_id = 11
        caption = None
        reply_to_message = None
        from_user = SimpleNamespace(
            id=202, first_name="Example", last_name="", username="example"
        )
        chat = SimpleNamespace(id=202)

        async def reply_text(self, text, **kwargs):
            wire.append(text)
            return Sent()

    async def delivered(token, target, text, **kwargs):
        alerts.append((target, text, kwargs))
        return True

    monkeypatch.setattr(
        "nullion.telegram_entrypoint._send_operator_telegram_delivery", delivered
    )
    with TestClient(
        web_app.create_app(f.runtime, f.orchestrator, f.registry)
    ) as client:
        asyncio.run(
            service.on_message(
                SimpleNamespace(update_id=11, message=Message()),
                SimpleNamespace(bot=None),
            )
        )
        jobs = client.get("/api/live-trackers").json()["trackers"]
        assert len(jobs) == 1 and any("Live tracker" in text for text in wire), (
            wire,
            f.runtime.store.list_conversation_events("telegram:202"),
        )
        assert (
            client.app.state.live_tracker_store.get(jobs[0]["id"])["conversation_id"]
            == "telegram:202"
        )
        calls = f.model.agent_calls
        f.source.available = True
        f.source.slot = "B7"
        wait_for(lambda: len(alerts) == 1)
        assert (
            alerts[0][0] == "202"
            and "B7" in alerts[0][1]
            and alerts[0][2]["message_only"] is True
            and alerts[0][2]["request_id"] == jobs[0]["id"] + ":matched"
        )
        wait_for(
            lambda: client.get("/api/live-trackers").json()["trackers"][0][
                "notification_status"
            ]
            == "sent"
        )
        assert f.model.agent_calls == calls


def test_model_repairs_invalid_prepared_condition_before_any_tracker_is_started(
    live_fixture,
):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app

    f = live_fixture
    f.model.bad_pointer = True
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        reply = client.post(
            "/api/chat",
            json={
                "text": "90秒間だけ枠を確認して、割り当てられたら知らせてください。",
                "conversation_id": "web:repair-condition",
            },
        )
        jobs = client.get("/api/live-trackers").json()["trackers"]
        assert len(jobs) == 1 and "Live tracker" in reply.json()["text"], reply.json()
        saved = f.runtime.store.list_conversation_events("web:repair-condition")
        turn = next(
            row for row in saved if row["event_type"] == "conversation.chat_turn"
        )
        attempts = [
            r for r in turn["tool_results"] if r["tool_name"] == "create_live_tracker"
        ]
        assert [r["status"] for r in attempts] == ["failed", "completed"]
        job = client.app.state.live_tracker_store.get(jobs[0]["id"])
        assert job["matcher"]["conditions"][0]["pointer"] == "/available"


def test_immediate_match_waits_for_setup_receipt_before_background_alert(live_fixture):
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import (
        _UvicornThread,
        _free_port,
        _open_page,
    )
    from playwright.sync_api import expect

    f = live_fixture
    f.source.available = True
    f.source.slot = "B7"
    f.model.final_delay = 1.5
    with _UvicornThread(
        create_app(f.runtime, f.orchestrator, f.registry), _free_port()
    ) as server:
        playwright, browser, page, errors = _open_page(server.base_url)
        try:
            page.locator("#user-input").fill(
                "Watch this for 90 seconds every 5 seconds."
            )
            page.locator("#user-input").press("Enter")
            expect(page.locator("#messages")).to_contain_text(
                "Tracking has stopped.", timeout=15000
            )
            visible = page.locator("#messages").inner_text()
            assert visible.index(
                "Live tracker “Cedar slot watch” is active."
            ) < visible.index("Live tracker: Cedar slot watch"), visible
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_one_structured_request_confirms_both_distinct_trackers(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app

    f = live_fixture
    original = f.model.create
    first_args = None
    second_created = False

    def multiple(**kwargs):
        nonlocal first_args, second_created
        names = {d["name"] for d in kwargs.get("tools", [])}
        if first_args and not second_created and "create_live_tracker" in names:
            second_created = True
            return use(
                "create_live_tracker", {**first_args, "name": "Willow slot watch"}
            )
        reply = original(**kwargs)
        for block in reply.get("content", []):
            if (
                block.get("type") == "tool_use"
                and block.get("name") == "create_live_tracker"
            ):
                first_args = dict(block["input"])
        return reply

    f.model.create = multiple
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        reply = client.post(
            "/api/chat",
            json={
                "text": "Vigila las dos plazas conocidas durante 90 segundos.",
                "conversation_id": "web:two-observations",
            },
        )
        assert (
            "Cedar slot watch" in reply.json()["text"]
            and "Willow slot watch" in reply.json()["text"]
        ), reply.json()
        assert len(client.get("/api/live-trackers").json()["trackers"]) == 2


def test_live_tracker_keeps_prepared_browser_page_across_checks(live_fixture, monkeypatch):
    """Real Chromium keeps the prepared page and receives a later assignment."""
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.live_tracking import LiveTrackerStore

    f = live_fixture
    state = SimpleNamespace(title="pending", loads=0)

    class PageSource(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/state":
                body = state.title.encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)
                return
            state.loads += 1
            body = b'''<title>pending</title><script>sessionStorage.setItem('prepared','yes');
            setInterval(async()=>{document.title=await(await fetch('/state')).text()},100)</script>'''
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), PageSource)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "playwright")
    register_browser_tools(f.registry)
    phase = 0
    session = None

    def model(**kwargs):
        nonlocal phase, session
        protocol_reply = workflow_provider_reply(kwargs)
        if protocol_reply is not None: return protocol_reply
        names = {d["name"] for d in kwargs.get("tools", [])}
        if not names:
            return text("{}")
        phase += 1
        if phase == 1:
            return use("request_tool_scope", {
                "tracking_mode": "live", "capabilities": ["live_tracking", "browser"],
                "tool_names": ["browser_navigate", "browser_snapshot", "create_live_tracker"],
                "required_tool_names": ["browser_navigate", "browser_snapshot", "create_live_tracker"],
                "reason": "Prepare one bounded live source and reuse its current page.",
            })
        if phase == 2:
            return use("browser_navigate", {"url": f"http://127.0.0.1:{server.server_port}/", "session_id": "source-a"})
        if phase == 3:
            return use("browser_snapshot", {"session_id": "source-a"})
        if phase == 4:
            return use("create_live_tracker", {
                "name": "割り当ての観測", "duration_seconds": 90, "interval_seconds": 5,
                "probe": [{"tool_name": "browser_snapshot", "arguments": {"session_id": "source-a"}}],
                "matcher": {"kind": "structured", "summary": "Assigned resource is ready.", "conditions": [{
                    "step": 0, "pointer": "/snapshot/title", "op": "equals", "value": "slot-7", "label": "Assignment",
                }]},
            })
        return text(fixture_action_acknowledgement(kwargs) or "Watching.")

    f.model.create = model
    try:
        with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
            reply = client.post("/api/chat", json={"text": "このページの割り当てを90秒だけ監視してください。", "conversation_id": "web:stateful-observation"})
            assert "Live tracker" in reply.json()["text"], reply.json()
            store = LiveTrackerStore(f.tmp_path / "runtime.db")
            deadline = time.monotonic() + 25
            job = store.list("workspace_admin")[0]
            while time.monotonic() < deadline:
                job = store.get(job["id"])
                if job["checks"] >= 2:
                    break
                time.sleep(.1)
            assert job["checks"] >= 2 and job["last_outcome"] == "no_match", job
            state.title = "slot-7"
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                job = store.get(job["id"])
                if job["notification_status"] == "sent":
                    break
                time.sleep(.1)
            assert job["state"] == "matched" and job["notification_status"] == "sent", job
            assert state.loads == 1, "Polling reopened the page instead of reusing the prepared tab"
            assert phase == 5, "Polling restarted the agent/model workflow"
    finally:
        f.registry.run_cleanup_hooks()
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.parametrize("selection,hours", [("1",1),("2",2),("3",4),("4",0),("２",2)])
def test_expiry_notice_numeric_choice_through_real_chat(live_fixture, selection, hours):
    from datetime import timedelta, datetime
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.live_tracking import utcnow
    from nullion.chat_store import get_chat_store

    f = live_fixture
    conversation = "web:expiry-choice"
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        response = client.post("/api/chat", json={"text":"枠を90秒確認してください。", "conversation_id":conversation})
        assert response.status_code == 200
        store = client.app.state.live_tracker_store
        engine = client.app.state.live_tracker_engine
        job = store.list("workspace_admin")[0]
        engine.stop()
        store.expire(datetime.fromisoformat(job["expires_at"]) + timedelta(seconds=1))
        notice = store.claim_notice(datetime.fromisoformat(job["expires_at"]) + timedelta(seconds=1))
        # Exercise the production platform delivery callback, not a helper copy.
        assert engine.deliver(store.get(job["id"]), notice["text"], notice["id"])
        history = get_chat_store().load_messages(conversation)
        expiry = history[-1]
        assert expiry["metadata"]["live_tracker_extension_choice"]
        assert "1. Continue for 1 more hour" in expiry["text"]
        calls = f.model.agent_calls + f.model.classifier_calls
        start = utcnow()
        response = client.post("/api/chat", json={"text":selection,"conversation_id":conversation})
        assert response.status_code == 200, response.text
        updated = store.get(job["id"])
        assert len(store.list("workspace_admin")) == 1
        assert f.model.agent_calls + f.model.classifier_calls == calls, "A numbered UI choice must not need a model"
        assert updated["interval_seconds"] == job["interval_seconds"]
        if hours:
            assert "active again until" in response.json()["text"]
            assert updated["state"] == "active"
            assert abs((datetime.fromisoformat(updated["expires_at"]) - start).total_seconds() - hours*3600) < 5
            assert engine.ready_to_deliver(updated), "Extension receipt must unlock later alerts"
        else:
            assert "stay stopped" in response.json()["text"] and updated["state"] == "stopped"
        # Explicit replies to the already-consumed expiry are idempotent even
        # after unrelated messages; the exact same window cannot be extended twice.
        anchored = {"reply_to_message_id":"expiry-1", "reply_to_from_bot":True,"reply_to_text":expiry["text"]}
        from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
        repeated = run_platform_chat_request(f.runtime, PlatformChatRequest(
            platform="web", text="1", chat_id=conversation, conversation_id=conversation,
            request_id="repeat-choice", model_client=f.model, agent_orchestrator=f.orchestrator,
            reply_context=anchored,
        ))
        assert "already handled" in repeated.text
        assert store.get(job["id"])["expires_at"] == updated["expires_at"]


def test_expiry_choice_ignores_unrelated_and_other_chat_numbers(live_fixture):
    from datetime import timedelta, datetime
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.chat_store import get_chat_store
    from nullion.live_tracking_choices import expiry_choice_reply

    f = live_fixture
    chat = "web:maple"
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        client.post("/api/chat",json={"text":"Observe for 90 seconds.","conversation_id":chat})
        store=client.app.state.live_tracker_store
        engine=client.app.state.live_tracker_engine
        engine.stop()
        job=store.list("workspace_admin")[0]
        future=datetime.fromisoformat(job["expires_at"])+timedelta(seconds=1)
        store.expire(future)
        notice=store.claim_notice(future)
        assert engine.deliver(store.get(job["id"]),notice["text"],notice["id"])
        assert expiry_choice_reply(f.runtime,prompt="1",conversation_id="web:birch",principal_id="web:birch") is None
        history=get_chat_store()
        history.save_message(chat,"bot","別の質問です。\n1. 赤\n2. 青")
        assert expiry_choice_reply(f.runtime,prompt="1",conversation_id=chat,principal_id=chat) is None
        assert store.get(job["id"])["state"] == "expired"


def test_browser_receives_expiry_and_extends_same_tracker(live_fixture):
    from datetime import datetime, timedelta
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from playwright.sync_api import expect

    f=live_fixture
    app=create_app(f.runtime,f.orchestrator,f.registry)
    with _UvicornThread(app,_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator("#user-input").fill("Vigila la plaza durante 90 segundos.")
            page.locator("#user-input").press("Enter")
            expect(page.locator("#messages")).to_contain_text("Live tracker", timeout=12000)
            engine=app.state.live_tracker_engine
            engine.stop()
            store=app.state.live_tracker_store
            job=store.list("workspace_admin")[0]
            future=datetime.fromisoformat(job["expires_at"])+timedelta(seconds=1)
            store.expire(future)
            notice=store.claim_notice(future)
            # The durable chat receipt must survive a best-effort runtime-event
            # checkpoint failure. Hold a real competing SQLite write transaction
            # instead of waiting for incidental scheduler contention.
            import sqlite3
            with sqlite3.connect(f.runtime.checkpoint_path) as busy:
                busy.execute("BEGIN IMMEDIATE")
                assert engine.deliver(store.get(job["id"]),notice["text"],notice["id"])
                busy.rollback()
            expect(page.locator("#messages")).to_contain_text("Continue for 2 more hours",timeout=10000)
            start=time.monotonic()
            page.locator("#user-input").fill("2")
            page.locator("#user-input").press("Enter")
            expect(page.locator("#messages")).to_contain_text("active again until",timeout=6000)
            assert time.monotonic()-start < 6
            updated=store.get(job["id"])
            assert updated["state"]=="active" and updated["extension_count"]==1
            assert len(store.list("workspace_admin"))==1
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_two_expiries_require_reply_anchor_and_select_exact_tracker(live_fixture):
    from datetime import datetime, timedelta
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.chat_store import get_chat_store
    from nullion.messaging_turn_graph import PlatformChatRequest, run_platform_chat_request
    f=live_fixture
    chat="web:two-expiries"
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        client.post("/api/chat",json={"text":"90秒だけ観測してください。","conversation_id":chat})
        store=client.app.state.live_tracker_store
        engine=client.app.state.live_tracker_engine
        engine.stop()
        first=store.list("workspace_admin")[0]
        second=store.create(name="Birch watch",principal_id=first["principal_id"],conversation_id=chat,
            workspace_id=first["workspace_id"],probe=first["probe"],matcher=first["matcher"],
            interval_seconds=first["interval_seconds"],expires_at=datetime.fromisoformat(first["expires_at"]),request_key="second-watch")
        future=datetime.fromisoformat(first["expires_at"])+timedelta(seconds=1)
        store.expire(future)
        notices=[]
        for _ in range(2):
            notice=store.claim_notice(future)
            assert engine.deliver(store.get(notice["tracker_id"]),notice["text"],notice["id"])
            store.finish_notice(notice,True,future)
            notices.append(notice)
        reply=client.post("/api/chat",json={"text":"1","conversation_id":chat}).json()["text"]
        assert "More than one tracker" in reply
        assert all(j["state"]=="expired" for j in store.list("workspace_admin"))
        history=get_chat_store().load_messages(chat)
        expiry=next(m for m in history if (m.get("metadata") or {}).get("live_tracker_id")==first["id"])
        reply=run_platform_chat_request(f.runtime,PlatformChatRequest(
            platform="web",text="2",chat_id=chat,conversation_id=chat,request_id="select-first",
            reply_context={"reply_to_from_bot":True,"reply_to_message_id":"first-expiry","reply_to_text":expiry["text"]},
            model_client=f.model,agent_orchestrator=f.orchestrator))
        assert "active again until" in reply.text
        assert store.get(first["id"])["state"]=="active"
        assert store.get(second["id"])["state"]=="expired"


@pytest.mark.parametrize("option,expected", [("1","active again until"),("4","stay stopped")])
def test_telegram_expiry_choice_runs_real_handler_once(live_fixture, monkeypatch, option, expected):
    import asyncio
    from datetime import timedelta
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion import telegram_app, telegram_entrypoint
    from nullion.config import load_settings
    from nullion.live_tracking import utcnow
    f=live_fixture
    wire=[]
    async def send_expiry(token,target,text,**kwargs):
        wire.append(text)
        return True  # External Telegram transport boundary only.
    monkeypatch.setattr(telegram_entrypoint,"_send_operator_telegram_delivery",send_expiry)
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN","123456:abcdefghijklmnopqrstuvwxyz")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID","123")
    settings=load_settings(env={"NULLION_TELEGRAM_BOT_TOKEN":"123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID":"123","NULLION_TELEGRAM_CHAT_ENABLED":"true"})
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        store=client.app.state.live_tracker_store
        engine=client.app.state.live_tracker_engine
        engine.stop()
        now=utcnow()
        job=store.create(name="Aspen observation",principal_id="telegram:123",conversation_id="telegram:123",
            workspace_id="workspace_admin",probe=[{"tool_name":"observation_read","arguments":{"url":"https://example.test/observation"}}],
            matcher={"kind":"structured","conditions":[{"step":0,"pointer":"/available","op":"equals","value":True}]},
            interval_seconds=5,expires_at=now+timedelta(seconds=30),request_key="telegram-expiry",now=now)
        store.expire(now+timedelta(seconds=31))
        notice=store.claim_notice(now+timedelta(seconds=31))
        assert engine.deliver(store.get(job["id"]),notice["text"],notice["id"])
        class Sent:
            message_id=501
            async def edit_text(self,text,**kwargs):return self
            async def delete(self):pass
        class Message:
            text=option
            message_id=502
            caption=None
            from_user=SimpleNamespace(id=123,first_name="Example",last_name="",username="example")
            reply_to_message=SimpleNamespace(message_id=501,text=notice["text"],caption=None,from_user=SimpleNamespace(id=999,is_bot=True))
            async def send_action(self,action):pass
            async def reply_text(self,text,**kwargs):
                wire.append(text)
                return Sent()
        message=Message()
        message.chat=SimpleNamespace(id=123,send_action=message.send_action)
        service=telegram_app.ChatOperatorService(runtime=f.runtime,settings=settings,
            bot_token=settings.telegram.bot_token,operator_chat_id="123",model_client=f.model,agent_orchestrator=f.orchestrator)
        update=SimpleNamespace(update_id=502,message=message)
        async def deliver():
            await service.on_message(update,SimpleNamespace(bot=None))
            await service.on_message(update,SimpleNamespace(bot=None))
        asyncio.run(deliver())
        assert len([s for s in wire if expected in s])==1,wire
        assert len(store.list("workspace_admin"))==1
        assert store.get(job["id"])["state"]==("active" if option=="1" else "stopped")
        assert f.model.agent_calls==0 and f.model.classifier_calls==0


def test_silent_tracker_expiry_ack_survives_busy_chat(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.live_tracking import utcnow
    f=live_fixture
    chat="web:busy-expiry"
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        client.post("/api/chat",json={"text":"Observe the slot for 90 seconds.","conversation_id":chat})
        store=client.app.state.live_tracker_store
        job=store.list("workspace_admin")[0]
        assert not job["require_start_receipt"]
        for i in range(40):
            f.runtime.store.add_conversation_event({"event_type":"conversation.chat_turn","conversation_id":chat,
                "created_at":utcnow().isoformat(),"turn_id":f"busy-{i}","user_message":"別件", "assistant_reply":"完了", "tool_results":[]})
        assert client.app.state.live_tracker_engine.ready_to_deliver(store.get(job["id"]))


@pytest.mark.parametrize("selection,expected", [("3", "active again until"), ("4", "stay stopped")])
def test_committed_expiry_choice_survives_real_sqlite_writer(live_fixture, monkeypatch, selection, expected):
    import sqlite3
    from datetime import datetime, timedelta
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.runtime import bootstrap_persistent_runtime
    f=live_fixture
    chat="web:contended-choice"
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        client.post("/api/chat",json={"text":"90秒だけ観測してください。","conversation_id":chat})
        store=client.app.state.live_tracker_store
        engine=client.app.state.live_tracker_engine
        engine.stop()
        original=store.list("workspace_admin")[0]
        future=datetime.fromisoformat(original["expires_at"])+timedelta(seconds=1)
        store.expire(future)
        notice=store.claim_notice(future)
        assert engine.deliver(store.get(original["id"]),notice["text"],notice["id"])
        store.finish_notice(notice,True,future)
        committed=threading.Event()
        locked=threading.Event()
        release=threading.Event()
        real_connect=sqlite3.connect

        def writer():
            assert committed.wait(10)
            with real_connect(str(store.path),timeout=3) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("UPDATE runtime_meta SET value=value WHERE key='format_version'")
                locked.set()
                assert release.wait(25)

        class ObservedConnection(sqlite3.Connection):
            def __init__(self,*args,**kwargs):
                super().__init__(*args,**kwargs)
                self.choice_changed=False
                def trace(sql):
                    if sql.startswith("UPDATE live_tracker_choices SET selected="):
                        self.choice_changed=True
                self.set_trace_callback(trace)
            def __exit__(self,*args):
                result=super().__exit__(*args)  # Real SQLite commit first.
                if self.choice_changed:
                    committed.set()
                    assert locked.wait(5), "External writer did not acquire its real lock"
                return result

        def observed_connect(database,*args,**kwargs):
            if str(database)==str(store.path):
                kwargs.setdefault("factory",ObservedConnection)
            return real_connect(database,*args,**kwargs)

        worker=threading.Thread(target=writer,daemon=True)
        worker.start()
        calls=f.model.agent_calls+f.model.classifier_calls
        monkeypatch.setattr(sqlite3,"connect",observed_connect)
        started=time.monotonic()
        try:
            response=client.post("/api/chat",json={"text":selection,"conversation_id":chat})
            elapsed=time.monotonic()-started
            assert response.status_code==200,response.text
            assert expected in response.json()["text"],response.json()
            assert "database is locked" not in response.json()["text"]
            assert elapsed < 2, f"Secondary persistence blocked the receipt for {elapsed:.2f}s"
            assert f.model.agent_calls+f.model.classifier_calls==calls
        finally:
            release.set()
            worker.join(timeout=5)
            monkeypatch.setattr(sqlite3,"connect",real_connect)
        assert not worker.is_alive()
        job=store.get(original["id"])
        assert job["state"]==("active" if selection=="3" else "stopped")
        assert len(store.list("workspace_admin"))==1
        if selection=="3":
            assert job["extension_count"]==1
            # A fresh runtime has no secondary extended chat event. The
            # committed choice must still permit later live notifications.
            fresh=bootstrap_persistent_runtime(f.tmp_path/"runtime.db")
            fresh_app=create_app(fresh,f.orchestrator,f.registry)
            assert fresh_app.state.live_tracker_engine.ready_to_deliver(job)
            store.expire(datetime.fromisoformat(job["expires_at"])+timedelta(seconds=1))
            expired=store.get(job["id"])
            assert expired["extension_offer_id"] != expired["window_extension_choice_id"]
            assert fresh_app.state.live_tracker_engine.ready_to_deliver(expired), "New expiry menus must preserve the current window's durable authorization"
        # Once the writer is gone the actual bounded log can be flushed.
        f.runtime.checkpoint_conversation_events(timeout_seconds=2)
        with real_connect(str(store.path)) as connection:
            rows=connection.execute("SELECT payload FROM conversation_events WHERE collection='conversation_events'").fetchall()
        assert any(json.loads(p).get("user_message")==selection and expected in json.loads(p).get("assistant_reply","") for p, in rows)


def test_busy_before_choice_keeps_option_retryable(live_fixture):
    import sqlite3
    from datetime import datetime,timedelta
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    f=live_fixture
    chat="web:busy-before-choice"
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        client.post("/api/chat",json={"text":"Observe for 90 seconds.","conversation_id":chat})
        store=client.app.state.live_tracker_store
        engine=client.app.state.live_tracker_engine
        engine.stop()
        job=store.list("workspace_admin")[0]
        future=datetime.fromisoformat(job["expires_at"])+timedelta(seconds=1)
        store.expire(future)
        notice=store.claim_notice(future)
        assert engine.deliver(store.get(job["id"]),notice["text"],notice["id"])
        store.finish_notice(notice,True,future)
        lock=sqlite3.connect(str(store.path))
        lock.execute("BEGIN IMMEDIATE")
        lock.execute("UPDATE runtime_meta SET value=value WHERE key='format_version'")
        try:
            response=client.post("/api/chat",json={"text":"2","conversation_id":chat})
            assert response.status_code==200
            assert "same number again" in response.json()["text"]
            assert "database is locked" not in response.json()["text"]
        finally:
            lock.rollback();lock.close()
        assert store.get(job["id"])["state"]=="expired"
        offer=store.get(job["id"])["extension_offer_id"]
        assert store.extension_offer(offer)["selected"] is None
        response=client.post("/api/chat",json={"text":"2","conversation_id":chat})
        assert "active again until" in response.json()["text"]
        assert store.get(job["id"])["extension_count"]==1


def test_real_web_pause_cause_repair_resume_and_match(live_fixture, monkeypatch):
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from playwright.sync_api import expect
    f=live_fixture
    f.model.semantic=True
    monkeypatch.setenv('NULLION_WEB_STATUS_CACHE_TTL_S','60')
    app=create_app(f.runtime,f.orchestrator,f.registry)
    with _UvicornThread(app,_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#user-input').fill('راقب اللوحة لمدة 90 ثانية وأبلغني عند تعيين الرصيف')
            page.locator('#user-input').press('Enter')
            expect(page.locator('#messages')).to_contain_text('Live tracker',timeout=15000)
            wait_for(lambda: app.state.live_tracker_store.list("workspace_admin")[0]['last_outcome']=='no_match')
            f.source.extra={'details':'z'*20000}
            row=page.locator('#live-trackers-dash [data-live-tracker-id]')
            expect(row).to_contain_text('Paused',timeout=45000)
            expect(row).to_contain_text('18,000-character live-check limit')
            expect(row).to_contain_text('How to fix it:')
            expect(page.locator('#messages')).to_contain_text('Ask me to narrow the saved source read',timeout=15000)
            paused=app.state.live_tracker_store.list("workspace_admin")[0]
            assert paused['consecutive_errors']==3 and paused['last_problem']['code']=='observation_too_large'
            before=f.source.reads
            time.sleep(1)
            assert f.source.reads==before
            f.source.extra={}
            row.get_by_role('button',name='Resume',exact=True).click()
            expect(row).to_contain_text('No match yet',timeout=15000)
            assert app.state.live_tracker_store.list("workspace_admin")[0]['last_problem'] is None
            f.source.available=True
            f.source.slot='B7'
            expect(page.locator('#messages')).to_contain_text('Slot B7 is available.',timeout=15000)
            expect(row).to_contain_text('Match found',timeout=15000)
            with app.state.live_tracker_store.connection() as db:
                rows=[dict(r) for r in db.execute('SELECT kind,state,text FROM live_tracker_notifications')]
            assert sum(r['kind']=='blocked' and r['state']=='sent' for r in rows)==1
            assert sum(r['kind']=='matched' and r['state']=='sent' for r in rows)==1
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_real_chat_semantic_tracker_accepts_prepared_14kb_observation(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    f=live_fixture
    f.model.semantic=True
    f.model.quote_evidence=True
    f.source.extra={'content':json.dumps({'slot':None,'page_details':'a'*15000})}
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        r=client.post('/api/chat',json={'text':'Vigila esta plaza durante 90 segundos','conversation_id':'web:large-observation'})
        assert r.status_code==200 and 'Live tracker' in r.json()['text']
        wait_for(lambda: client.get('/api/live-trackers').json()['trackers'][0]['checks']>=3,timeout=25)
        job=client.get('/api/live-trackers').json()['trackers'][0]
        assert job['state']=='active' and job['last_outcome']=='no_match' and job['last_problem'] is None
        assert f.model.semantic_calls>=3
        f.source.available=True
        f.source.slot='L2'
        f.source.extra={'content':json.dumps({'slot':'L2','page_details':'a'*15000})}
        wait_for(lambda: client.get('/api/live-trackers').json()['trackers'][0]['state']=='matched')
        job=client.get('/api/live-trackers').json()['trackers'][0]
        assert job['state']=='matched'


def test_real_web_reconnect_recovers_expiry_notice_once_without_reload(live_fixture):
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from playwright.sync_api import expect
    f=live_fixture
    f.model.duration=30
    app=create_app(f.runtime,f.orchestrator,f.registry)
    with _UvicornThread(app,_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#user-input').fill('راقب اللوحة لمدة 30 ثانية')
            page.locator('#user-input').press('Enter')
            expect(page.locator('#messages')).to_contain_text('Live tracker',timeout=15000)
            expect(page.locator('#messages .typing-bubble, #messages .status-bubble')).to_have_count(0)
            # Only the real browser transport is disconnected. The application,
            # worker, clock, SQLite and durable history continue normally.
            page.context.set_offline(True)
            page.evaluate('ws.close()')
            wait_for(lambda: app.state.live_tracker_store.list('workspace_admin')[0]['state']=='expired',timeout=40)
            wait_for(lambda: app.state.live_tracker_store.list('workspace_admin')[0]['notification_status']=='sent')
            assert 'Live tracker expired:' not in page.locator('#messages').inner_text()
            page.context.set_offline(False)
            # Reconnect uses an existing bounded exponential backoff of up to
            # 30 seconds. Recovery must happen without a reload once it opens.
            expect(page.locator('#messages')).to_contain_text('Live tracker expired:',timeout=35000)
            notices=page.locator('#messages .bubble[data-live-notice-id]')
            expect(notices).to_have_count(1)
            page.evaluate('ws.close()')
            expect(page.get_by_text('Connected',exact=True)).to_be_visible(timeout=15000)
            time.sleep(1)
            expect(notices).to_have_count(1)
            with app.state.live_tracker_store.connection() as db:
                assert db.execute("SELECT COUNT(*) FROM live_tracker_notifications WHERE kind='expired' AND state='sent'").fetchone()[0]==1
        finally:
            page.context.set_offline(False)
            browser.close()
            playwright.stop()


def test_real_browser_connection_failure_pauses_with_specific_repair_notice(live_fixture,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.plugins.browser_plugin import register_browser_tools
    from tests.e2e.test_web_surface_contracts import _free_port
    from nullion.chat_store import get_chat_store
    f=live_fixture
    port=_free_port()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','cdp')
    monkeypatch.setenv('NULLION_BROWSER_CDP_URL',f'http://127.0.0.1:{port}')
    register_browser_tools(f.registry)
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        response=client.post('/api/live-trackers',json={
            'name':'Browser connection watch','duration_seconds':90,'interval_seconds':5,
            'conversation_id':'web:browser-health',
            'probe':[{'tool_name':'browser_navigate','arguments':{'url':'https://example.com'}}],
            'matcher':{'kind':'semantic','instruction':'観測'},
        })
        assert response.status_code==200,response.json()
        wait_for(lambda: client.get('/api/live-trackers').json()['trackers'][0]['state']=='blocked',timeout=35)
        job=client.get('/api/live-trackers').json()['trackers'][0]
        assert job['checks']==3 and job['last_problem']['code']=='browser_connection_unavailable'
        wait_for(lambda: client.get('/api/live-trackers').json()['trackers'][0]['notification_status']=='sent')
        messages=get_chat_store().load_messages('web:browser-health')
        notice=next(m for m in messages if (m.get('metadata') or {}).get('live_tracker_id')==job['id'])
        assert 'browser connection' in notice['text'] and 'reconnect the browser' in notice['text']
        assert 'Connection refused' not in notice['text'] and str(port) not in notice['text']
        assert f.model.semantic_calls==0


@pytest.mark.parametrize('option', ['1','4'])
@pytest.mark.parametrize('same_name', [False,True])
def test_two_expired_trackers_support_numbered_target_selection_and_busy_retry(live_fixture,option,same_name):
    from datetime import datetime,timedelta
    import sqlite3
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    f=live_fixture
    chat='web:multiple-expiry-choices'
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        client.post('/api/chat',json={'text':'90秒だけ観測してください','conversation_id':chat})
        store=client.app.state.live_tracker_store
        engine=client.app.state.live_tracker_engine
        engine.stop()
        first=store.list('workspace_admin')[0]
        second=store.create(name=first['name'] if same_name else '新しい観測',principal_id=first['principal_id'],conversation_id=chat,
            workspace_id=first['workspace_id'],probe=first['probe'],matcher=first['matcher'],
            interval_seconds=5,expires_at=datetime.fromisoformat(first['expires_at']),request_key='other-expiry')
        future=datetime.fromisoformat(first['expires_at'])+timedelta(seconds=1)
        store.expire(future)
        for _ in range(2):
            notice=store.claim_notice(future)
            assert engine.deliver(store.get(notice['tracker_id']),notice['text'],notice['id'])
            store.finish_notice(notice,True,future)
        reply=client.post('/api/chat',json={'text':option,'conversation_id':chat}).json()['text']
        assert 'Reply with its number' in reply and '1.' in reply and '2.' in reply
        event=f.runtime.store.list_recent_conversation_events(chat,event_type='conversation.chat_turn',limit=1)[0]
        choices=event['tool_results'][0]['output']['tracker_choices']
        chosen=store.extension_offer(choices[1]['choice_id'])['tracker_id']
        untouched=first['id'] if chosen==second['id'] else second['id']
        lock=sqlite3.connect(str(store.path));lock.execute('BEGIN IMMEDIATE')
        try:
            reply=client.post('/api/chat',json={'text':'2','conversation_id':chat}).json()['text']
            assert 'Storage is busy' in reply and 'Reply with its number' in reply
        finally:
            lock.rollback();lock.close()
        assert all(j['state']=='expired' for j in store.list('workspace_admin'))
        reply=client.post('/api/chat',json={'text':'2','conversation_id':chat}).json()['text']
        assert ('active again until' if option=='1' else 'stay stopped') in reply
        assert store.get(chosen)['state']==('active' if option=='1' else 'stopped')
        assert store.get(untouched)['state']=='expired'
        assert (store.get(chosen).get('extension_count') or 0)==(1 if option=='1' else 0)


def test_legacy_ambiguous_expiry_reply_upgrades_to_numbered_target_selection(live_fixture):
    from datetime import datetime,timedelta,timezone
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.chat_store import get_chat_store
    f=live_fixture;chat='web:legacy-expiry-choice'
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        client.post('/api/chat',json={'text':'90秒だけ観測してください','conversation_id':chat})
        store=client.app.state.live_tracker_store;engine=client.app.state.live_tracker_engine;engine.stop()
        first=store.list('workspace_admin')[0]
        store.create(name='別の観測',principal_id=first['principal_id'],conversation_id=chat,
            workspace_id=first['workspace_id'],probe=first['probe'],matcher=first['matcher'],interval_seconds=5,
            expires_at=datetime.fromisoformat(first['expires_at']),request_key='legacy-other')
        future=datetime.fromisoformat(first['expires_at'])+timedelta(seconds=1);store.expire(future)
        for _ in range(2):
            notice=store.claim_notice(future);assert engine.deliver(store.get(notice['tracker_id']),notice['text'],notice['id'])
            store.finish_notice(notice,True,future)
        # The old version persisted only a typed ambiguity reason and the
        # original numeric action; no target-selection fields existed yet.
        old_reply='複数の観測が終了しています。'
        f.runtime.store.add_conversation_event({'event_type':'conversation.chat_turn','conversation_id':chat,
            'created_at':datetime.now(timezone.utc).isoformat(),'turn_id':'old-ambiguity','user_message':'4',
            'assistant_reply':old_reply,'tool_results':[{'tool_name':'resume_live_tracker','status':'failed',
                'output':{'reason':'live_tracker_choice_ambiguous'}}]})
        get_chat_store().save_message(chat,'bot',old_reply,metadata={'runtime_turn_id':'old-ambiguity'})
        reply=client.post('/api/chat',json={'text':'1','conversation_id':chat}).json()['text']
        assert 'Reply with its number' in reply and 'option 4' in reply
        assert all(j['state']=='expired' for j in store.list('workspace_admin'))
        reply=client.post('/api/chat',json={'text':'1','conversation_id':chat}).json()['text']
        assert 'stay stopped' in reply
        assert sorted(j['state'] for j in store.list('workspace_admin'))==['expired','stopped']


def test_real_web_evaluator_survives_high_chat_profile_and_slow_response(live_fixture):
    """Real Web/worker/store; only the external evaluator transport is simulated."""
    from dataclasses import dataclass
    from nullion.model_clients import ModelClientTimeoutError
    from nullion.web_app import create_app
    from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port, _open_page
    from playwright.sync_api import expect
    f=live_fixture
    f.model.semantic=True
    f.model.duration=180
    observed=[]
    delays=[]
    controls={'fail':False}
    @dataclass
    class ProfiledProvider:
        reasoning_effort: str='high'
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            if str(kw.get('system') or '').startswith('Evaluate only the supplied current observations'):
                observed.append((self.reasoning_effort,kw['max_tokens'],kw['timeout']))
                if controls['fail']:
                    raise ModelClientTimeoutError('private transport detail')
                # Previously the inherited high effort consumed every output
                # token, leaving no decision. This rejects the actual old path.
                if self.reasoning_effort=='high' or kw['max_tokens']<1024:
                    return {'content':[], 'completion':{'status':'incomplete','reason':'max_output_tokens'}}
                delay=16 if not delays else .05
                start=time.monotonic()
                time.sleep(min(delay,kw['timeout']))
                delays.append(time.monotonic()-start)
                if delay>=kw['timeout']:
                    raise ModelClientTimeoutError('deadline')
            return f.model.create(**kw)
    provider=ProfiledProvider()
    from nullion.agent_orchestrator import AgentOrchestrator
    f.orchestrator=AgentOrchestrator(model_client=provider)
    app=create_app(f.runtime,f.orchestrator,f.registry)
    with _UvicornThread(app,_free_port()) as server:
        playwright,browser,page,errors=_open_page(server.base_url)
        try:
            page.locator('#user-input').fill('راقب اللوحة لمدة ثلاث دقائق وأبلغني عند تعيين الرصيف')
            page.locator('#user-input').press('Enter')
            expect(page.locator('#messages')).to_contain_text('Live tracker',timeout=20000)
            store=app.state.live_tracker_store
            # Slow optional evaluation must not enter ordinary chat's hot path.
            from urllib.request import Request
            f.model.mode='plain'
            plain_times=[]
            try:
                for n in range(4):
                    start=time.monotonic()
                    request=Request(server.base_url+'/api/chat',method='POST',
                                    headers={'Content-Type':'application/json'},
                                    data=json.dumps({'text':'こんにちは','conversation_id':f'web:plain-{n}'}).encode())
                    with urlopen(request,timeout=5) as response:
                        reply=json.load(response)
                    plain_times.append(time.monotonic()-start)
                    assert reply['text']=='Hello.'
            finally:
                f.model.mode='live'
            assert sum(plain_times)/len(plain_times)<1.5 and max(plain_times)<3
            wait_for(lambda: store.list('workspace_admin')[0]['checks']>=4,timeout=45)
            job=store.list('workspace_admin')[0]
            assert job['state']=='active' and job['consecutive_errors']==0
            assert job['last_outcome']=='no_match' and job['last_evaluation']['status']=='no_match'
            row=page.locator('#live-trackers-dash [data-live-tracker-id]')
            expect(row).to_contain_text('No match yet',timeout=10000)
            assert provider.reasoning_effort=='high'
            assert all(effort=='low' and tokens>=2048 and budget==30 for effort,tokens,budget in observed)
            assert 15<delays[0]<18 and sum(delays)/len(delays)<5
            assert sorted(delays)[-1]<18
            controls['fail']=True
            expect(row).to_contain_text('Paused',timeout=45000)
            paused=store.get(job['id'])
            assert paused['consecutive_errors']==3
            expect(page.locator('#messages')).to_contain_text('within 30 seconds',timeout=10000)
            expect(page.locator('#messages')).to_contain_text('inspect the evaluator timing',timeout=10000)
            assert 'private transport detail' not in page.locator('#messages').inner_text()
            controls['fail']=False
            row.get_by_role('button',name='Resume',exact=True).click()
            wait_for(lambda: store.get(job['id'])['last_outcome']=='no_match',timeout=10)
            expect(row).to_contain_text('No match yet',timeout=35000)
            f.source.available=True
            f.source.slot='B7'
            expect(row).to_contain_text('Match found',timeout=15000)
            expect(page.locator('#messages')).to_contain_text('Slot B7 is available.',timeout=10000)
            assert store.get(job['id'])['expires_at']==job['expires_at']
            with store.connection() as db:
                notices=[dict(r) for r in db.execute('SELECT kind,state FROM live_tracker_notifications')]
            assert sum(r['kind']=='blocked' and r['state']=='sent' for r in notices)==1
            assert sum(r['kind']=='matched' and r['state']=='sent' for r in notices)==1
            assert not errors
        finally:
            browser.close()
            playwright.stop()


def test_model_settings_repair_reaches_existing_live_tracker(live_fixture,monkeypatch):
    from dataclasses import dataclass
    from fastapi.testclient import TestClient
    from nullion import model_clients
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.model_clients import ModelClientTimeoutError
    from nullion.web_app import create_app
    from nullion.chat_store import get_chat_store
    f=live_fixture
    f.model.semantic=True
    seen=[]
    controls={'broken':False}
    @dataclass
    class Provider:
        model:str='old-connection'
        reasoning_effort:str='high'
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            if str(kw.get('system') or '').startswith('Evaluate only the supplied current observations'):
                seen.append((self.model,self.reasoning_effort))
                if self.model=='old-connection' and controls['broken']:
                    raise ModelClientTimeoutError('private old connection')
            return f.model.create(**kw)
    parent=Provider()
    new_parent=Provider('repaired-connection')
    monkeypatch.setattr(model_clients,'build_model_client_from_settings',lambda settings:new_parent)
    f.orchestrator=AgentOrchestrator(model_client=parent)
    chat='web:model-repair'
    monkeypatch.setenv('NULLION_ADMIN_FORCED_MODEL','')
    monkeypatch.setenv('NULLION_ADMIN_FORCED_PROVIDER','')
    app=create_app(f.runtime,f.orchestrator,f.registry)
    with TestClient(app) as client:
        # Get the existing source URL through a normal user turn and registered
        # tools, retaining real setup validation and persistence.
        response=client.post('/api/chat',json={'text':'راقب اللوحة لثلاث دقائق','conversation_id':chat})
        assert response.status_code==200,response.text
        controls['broken']=True  # Connection fails after valid setup, not before it.
        store=app.state.live_tracker_store
        wait_for(lambda: store.list('workspace_admin')[0]['consecutive_errors']>=1)
        before=store.list('workspace_admin')[0]
        result=client.post('/api/config/model-force',json={'model_name':'repaired-connection','model_provider':'codex'})
        assert result.status_code==200,result.text
        wait_for(lambda: store.get(before['id'])['last_outcome']=='no_match',timeout=15)
        repaired=store.get(before['id'])
        assert repaired['expires_at']==before['expires_at'] and repaired['consecutive_errors']==0
        assert repaired['last_problem'] is None
        assert ('repaired-connection','low') in seen
        assert parent.reasoning_effort=='high' and new_parent.reasoning_effort=='high'
        assert f.orchestrator.model_client is new_parent
        f.source.available=True
        f.source.slot='B7'
        wait_for(lambda: store.get(before['id'])['state']=='matched',timeout=15)
        wait_for(lambda: store.get(before['id'])['notification_status']=='sent')
        messages=get_chat_store().load_messages(chat)
        assert any('Slot B7 is available.' in row['text'] for row in messages)
        assert len(store.list('workspace_admin'))==1


def test_existing_tracker_works_after_switch_to_nonreasoning_adapter(live_fixture,monkeypatch):
    from fastapi.testclient import TestClient
    from nullion import model_clients
    from nullion.model_clients import OpenAIChatCompletionsModelClient
    from nullion.web_app import create_app
    f=live_fixture
    f.model.semantic=True
    seen=[]
    def complete(**kw):
        assert 'reasoning_effort' not in kw
        content=kw['messages'][-1]['content']
        packet=json.loads(content if isinstance(content,str) else ''.join(b['text'] for b in content if b['type']=='text'))
        output=packet['observations'][0]
        seen.append(output)
        answer={'status':'matched' if output['available'] else 'no_match',
                'summary':f"Slot {output['slot']} is available.",
                'evidence':[{'step':0,'pointer':'/slot','value':output['slot']}]}
        return SimpleNamespace(choices=[SimpleNamespace(finish_reason='stop',message=SimpleNamespace(content=json.dumps(answer),tool_calls=[]))])
    transport=SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=complete)))
    adapter=OpenAIChatCompletionsModelClient(client=transport,model='legacy-model',provider='openai')
    monkeypatch.setattr(model_clients,'build_model_client_from_settings',lambda settings:adapter)
    monkeypatch.setenv('NULLION_ADMIN_FORCED_MODEL','')
    monkeypatch.setenv('NULLION_ADMIN_FORCED_PROVIDER','')
    app=create_app(f.runtime,f.orchestrator,f.registry)
    with TestClient(app) as client:
        response=client.post('/api/chat',json={'text':'راقب اللوحة لمدة دقيقة ونصف','conversation_id':'web:legacy-adapter'})
        assert response.status_code==200,response.text
        store=app.state.live_tracker_store
        wait_for(lambda: len(store.list('workspace_admin'))==1)
        before=store.list('workspace_admin')[0]
        result=client.post('/api/config/model-force',json={'model_name':'legacy-model','model_provider':'openai'})
        assert result.status_code==200 and not result.json().get('live_swap_error'),result.text
        wait_for(lambda: len(seen)>=3,timeout=25)
        job=store.get(before['id'])
        assert job['state']=='active' and job['consecutive_errors']==0
        assert job['last_outcome']=='no_match' and adapter.reasoning_effort is None
        f.source.available=True
        f.source.slot='B7'
        wait_for(lambda: store.get(before['id'])['state']=='matched',timeout=12)
        wait_for(lambda: store.get(before['id'])['notification_status']=='sent')
        assert store.get(before['id'])['expires_at']==before['expires_at']
        with store.connection() as db:
            assert db.execute("select count(*) from live_tracker_notifications where kind='matched' and state='sent'").fetchone()[0]==1


@pytest.mark.parametrize('prompt', ['Set it again', '再開して', 'Vuelve a activarlo'])
def test_expired_tracker_resume_menu_numeric_renewal_and_alert(live_fixture, prompt):
    from datetime import datetime, timedelta
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.chat_store import get_chat_store
    f = live_fixture
    chat = 'web:renew-window'
    with TestClient(create_app(f.runtime, f.orchestrator, f.registry)) as client:
        client.post('/api/chat', json={'text':'Observe this source for 90 seconds.', 'conversation_id':chat})
        store = client.app.state.live_tracker_store
        engine = client.app.state.live_tracker_engine
        engine.stop()
        job = store.list('workspace_admin')[0]
        # Expiry remains deliverable even if the secondary setup receipt was lost.
        store.expire(datetime.fromisoformat(job['expires_at'])+timedelta(seconds=1))
        expired = store.get(job['id'])
        expired['require_start_receipt'] = True
        with store.connection() as db:
            store._save(db, expired)
        assert engine.ready_to_deliver(expired)
        # An unrelated background menu must not steal a subsequent numeric
        # answer to the exact tracker selected by this tool-generated menu.
        other=store.create(name='Other observation',principal_id=job['principal_id'],conversation_id=chat,
            workspace_id=job['workspace_id'],probe=job['probe'],matcher=job['matcher'],interval_seconds=5,
            expires_at=datetime.fromisoformat(job['expires_at']),request_key='other-window')
        store.expire(datetime.fromisoformat(job['expires_at'])+timedelta(seconds=1))
        for pending in [store.get(job['id']),store.get(other['id'])]:
            offer=store.extension_offer(pending['extension_offer_id'])
            assert engine.deliver(pending,offer['text'],offer['notice_id'])
        phase = 0
        def resume_model(**kw):
            nonlocal phase
            protocol_reply = workflow_provider_reply(kw)
            if protocol_reply is not None: return protocol_reply
            if not kw.get('tools'): return text('{}')
            phase += 1
            if phase == 1:
                return use('request_tool_scope', {'capabilities':['live_tracking'], 'tracking_mode':'live',
                    'tool_names':['resume_live_tracker'], 'required_tool_names':['resume_live_tracker'],
                    'reason':'Continue the selected saved observation.'})
            if phase == 2:
                return use('resume_live_tracker', {'id':job['id']})
            return text('Processed.')
        f.model.create = resume_model
        reply = client.post('/api/chat', json={'text':prompt,'conversation_id':chat})
        assert '1. Continue for 1 more hour.' in reply.json()['text'], reply.json()
        assert "required step failed" not in reply.json()['text']
        # The menu came from a tool result, even if the background expiration
        # message was never seen. A numeric answer must still consume it.
        chosen = client.post('/api/chat', json={'text':'1','conversation_id':chat})
        assert 'active again' in chosen.json()['text'], chosen.json()
        active = store.get(job['id'])
        assert active['state']=='active' and len(store.list('workspace_admin'))==2
        assert store.get(other['id'])['state']=='expired'
        assert active['probe']==job['probe'] and active['extension_count']==1
        f.source.available=True; f.source.slot='C9'
        from nullion.live_tracking import LiveTrackerEngine
        restarted_engine = LiveTrackerEngine(store, registry=f.registry, runtime_store=f.runtime.store,
                                             deliver=engine.deliver, model_client=f.model)
        claimed, token=store.claim(1)
        restarted_engine.check_once(claimed,token)
        notice=store.claim_notice()
        assert notice['kind']=='matched'
        assert engine.deliver(store.get(job['id']),notice['text'],notice['id'])
        store.finish_notice(notice,True)
        alerts=[m for m in get_chat_store().load_messages(chat) if (m.get('metadata') or {}).get('source')=='live_tracker']
        matched=[m for m in alerts if 'C9' in m['text']]
        assert len(matched)==1


@pytest.mark.parametrize('interval,first,followup,lifetime',[
    (60,'Alert me when the public source changes. First ask which change.', 'Every minute','For one hour'),
    (300,'Avísame cuando cambie la fuente pública. Primero pregunta qué cambio.', 'Cada cinco minutos','Durante una hora'),
    (60,'公開情報の変化を通知してください。先に対象を質問してください。','毎分','1時間だけ'),
])
def test_no_tool_clarification_followup_sees_real_limits_and_creates_saved_interval(
        live_fixture,interval,first,followup,lifetime):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    f=live_fixture;original=f.model.create;mode=['choice'];seen=[]
    f.model.duration=3600
    def provider(**kw):
        names={row['name'] for row in kw.get('tools',[])}
        if not names:return original(**kw)
        if mode[0]=='choice':return text('1. Any new result\n2. A specific result')
        if mode[0]=='interval':
            contexts=[block.get('text','') for message in kw['messages']
                      for block in (message.get('content') if isinstance(message.get('content'),list) else [])
                      if isinstance(block,dict) and block.get('type')=='text']
            compact=next((line.splitlines()[1] for line in contexts
                          if line.startswith('Compact registered capability index')),None)
            facts=(json.loads(compact).get('tool_contracts',[]) if compact else [])
            if not facts:
                full=next((line.split('Registered tool contracts: ',1)[1].splitlines()[0] for line in contexts
                           if 'Registered tool contracts: ' in line),None)
                facts=json.loads(full) if full else []
            contract=next((row for row in facts if row['tool_name']=='create_live_tracker'),None)
            if not contract:return text('The shortest supported check is every 15 minutes.')
            seen.append(contract)
            assert contract['parameters']['interval_seconds']['minimum']<=interval<=contract['parameters']['interval_seconds']['maximum']
            # Metadata can answer the cadence question without prematurely
            # invoking a creator before the user supplies a lifetime.
            return text('How long should I keep tracking?\n1. 30 minutes\n2. One hour\n3. Two hours')
        reply=original(**kw)
        for block in reply.get('content',[]):
            if block.get('name')=='create_live_tracker':block['input']['interval_seconds']=interval
        return reply
    f.model.create=provider;cid='web:cadence-followup'
    with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
        choice=client.post('/api/chat',json={'text':first,'conversation_id':cid})
        assert choice.status_code==200 and '1.' in choice.json()['text'],choice.text
        mode[0]='interval'
        response=client.post('/api/chat',json={'text':followup,'conversation_id':cid})
        assert response.status_code==200,response.text
        assert '15 minutes' not in response.json()['text'],response.json()
        assert 'How long' in response.json()['text'] and '1.' in response.json()['text'],response.json()
        assert seen,'No authoritative discovery contract reached the follow-up model'
        assert client.get('/api/live-trackers').json()['trackers']==[]
        mode[0]='create'
        created=client.post('/api/chat',json={'text':lifetime,'conversation_id':cid})
        assert created.status_code==200,created.text
        jobs=client.get('/api/live-trackers').json()['trackers']
        assert len(jobs)==1 and jobs[0]['interval_seconds']==interval,jobs
        assert jobs[0]['state']=='active'
        events=[r for r in f.runtime.store.list_conversation_events(cid) if r.get('event_type')=='conversation.chat_turn']
        assert any(r['tool_name']=='create_live_tracker' and r['status']=='completed' for r in events[-1]['tool_results'])
        assert not any(r['tool_name']=='create_cron' for e in events for r in e.get('tool_results',[]))
        client.post('/api/live-trackers/'+jobs[0]['id']+'/stop')


@pytest.mark.parametrize('prompt',['Alert me for new articles temporarily.','新しい記事だけ一時的に通知してください。'])
def test_browser_setup_repairs_scope_large_read_and_condition_then_delivers_new_article(live_fixture,monkeypatch,prompt):
    """Failed region -> oversized body -> precise repair -> real poll and one alert."""
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.chat_store import get_chat_store
    f=live_fixture
    source=SimpleNamespace(title='Original article',loads=0)
    class News(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/state':
                body=source.title.encode();kind='text/plain'
            else:
                source.loads+=1
                body=('''<main><div>'''+('a'*10000)+'''</div><section id="news"><h2>記事</h2>
                <article><a href="/news/article">Original article</a></article></section></main>
                <script>setInterval(async()=>{document.querySelector('article a').textContent=await(await fetch('/state')).text()},100)</script>''').encode()
                kind='text/html'
            self.send_response(200);self.send_header('Content-Type',kind);self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),News)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    monkeypatch.setenv('NULLION_BROWSER_BACKEND','playwright');register_browser_tools(f.registry)
    phase=0;receipts=[];failures=[];last_results=[]
    url=f'http://127.0.0.1:{server.server_port}/'
    broad={'tool_name':'browser_extract_text','arguments':{}}
    narrow={'tool_name':'browser_extract_items','arguments':{'selector':'#news a','max_items':5,'fields':['title','canonical_url']}}
    def create(probe,step=0):
        return use('create_live_tracker',{'name':'Prepared article watch','duration_seconds':90,'interval_seconds':5,
            # This fixture explicitly streams updates into its existing DOM.
            # Exercise the opt-in reuse contract alongside static-page defaults.
            'source_refresh':'reuse',
            'probe':[probe],'matcher':{'kind':'structured','summary':'New article published.','conditions':[
                {'step':step,'pointer':'/items/0/title','op':'changed','label':'Headline'},
                {'step':0,'pointer':'/items/0/canonical_url','op':'exists','label':'Link'}]}})
    def model(**kw):
        nonlocal phase
        protocol_reply = workflow_provider_reply(kw)
        if protocol_reply is not None: return protocol_reply
        names={row['name'] for row in kw.get('tools',[])}
        if not names:return text('{}')
        phase+=1
        blocks=[b for m in kw['messages'] for b in (m.get('content') if isinstance(m.get('content'),list) else []) if isinstance(b,dict) and b.get('type')=='tool_result']
        latest='\n'.join(c.get('text','') for b in blocks[-1:] for c in b.get('content',[]) if isinstance(c,dict))
        if phase==1:return use('request_tool_scope',{'tracking_mode':'live','capabilities':['live_tracking','web'],
            'tool_names':['browser_navigate','browser_extract_items','browser_extract_text','create_live_tracker'],
            'required_tool_names':['create_live_tracker']})
        if phase==2:return use('browser_navigate',{'url':url})
        if phase==3:return use('browser_extract_items',{'selector':'#missing'})
        if phase==4:
            assert 'create_live_tracker' in names,'Source repair discarded the live-tracker scope'
            assert 'repair_precondition' in latest
            return use('browser_extract_text',{})
        if phase==5:
            receipt=json.loads(latest.split('Live tracking source preparation (runtime-verified):\n',1)[1])
            assert receipt['status']=='needs_narrower_source';receipts.append(receipt)
            return create(broad,1)
        if phase==6:
            assert 'invalid_observation_step' in latest;failures.append('invalid_observation_step')
            return create(broad)
        if phase==7:
            assert 'observation_too_large' in latest;failures.append('observation_too_large')
            return create(narrow)
        if phase==8:
            assert 'unprepared_probe' in latest;failures.append('unprepared_probe')
            return use(narrow['tool_name'],narrow['arguments'])
        if phase in {9,11}:
            receipt=json.loads(latest.split('Live tracking source preparation (runtime-verified):\n',1)[1])
            assert receipt['status']=='ready' and receipt['probe']==narrow;receipts.append(receipt)
            output=json.loads(latest.split('\n\nLive tracking',1)[0])['output']
            assert 'image_url' not in output['items'][0] and 'url' not in output['items'][0]
            assert output['source_record_collections'][0]['change_identity_fields']==['canonical_url']
            if phase==9:return use('browser_extract_items',{'selector':'#another-missing-region'})
            return create(receipt['probe'])
        if phase==10:
            assert 'repair_precondition' in latest and 'create_live_tracker' in names,latest
            return use(narrow['tool_name'],narrow['arguments'])
        last_results.append(latest)
        return text('Watching for a new article.')
    f.model.create=model;cid='web:source-preparation-repair'
    try:
        with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
            reply=client.post('/api/chat',json={'text':prompt,'conversation_id':cid})
            assert reply.status_code==200 and 'Live tracker' in reply.json()['text'],(reply.json(),last_results)
            jobs=client.get('/api/live-trackers').json()['trackers'];assert len(jobs)==1,jobs
            setup_calls=phase
            store=client.app.state.live_tracker_store
            wait_for(lambda:store.get(jobs[0]['id'])['checks']>=1)
            assert store.get(jobs[0]['id'])['state']=='active'
            source.title='New published article'
            wait_for(lambda:store.get(jobs[0]['id'])['notification_status']=='sent',timeout=20)
            job=store.get(jobs[0]['id']);assert job['state']=='matched' and job['consecutive_errors']==0
            assert source.loads==2 and phase==setup_calls,'Polling restarted the page or agent instead of reusing the worker tab'
            alerts=[m for m in get_chat_store().load_messages(cid) if (m.get('metadata') or {}).get('source')=='live_tracker' and 'New article published.' in m.get('text','')]
            assert len(alerts)==1 and 'New published article' in alerts[0]['text'] and '/article' in alerts[0]['text']
            assert failures==['invalid_observation_step','observation_too_large','unprepared_probe']
            assert [r['status'] for r in receipts]==['needs_narrower_source','ready','ready']
    finally:
        f.registry.run_cleanup_hooks();server.shutdown()


@pytest.mark.parametrize('followup',['3 , check every 5 mins','3、5分ごとに確認して'])
def test_telegram_numeric_lifetime_and_interval_followup_creates_one_tracker(live_fixture,monkeypatch,followup):
    import asyncio
    from datetime import datetime
    from fastapi.testclient import TestClient
    from nullion import telegram_app,web_app
    from nullion.config import load_settings
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker
    f=live_fixture;original=f.model.create;setup=[False];wire=[]
    f.model.duration=21600
    def provider(**kw):
        if not kw.get('tools'):
            for message in kw.get('messages',[]):
                for block in message.get('content',[]):
                    if not isinstance(block,dict):continue
                    try:packet=json.loads(block.get('text',''))
                    except (ValueError,TypeError):continue
                    if isinstance(packet,dict) and 'allowed_relationships' in packet:
                        return text(json.dumps({'relationship':'follow_up','effect':'revise','confidence':1,'target_index':0,'target_is_explicit':True}))
        if not setup[0] and kw.get('tools'):return text('How long should I monitor?\n1. 30 minutes\n2. 1 hour\n3. 6 hours')
        response=original(**kw)
        for block in response.get('content',[]):
            if block.get('name')=='create_live_tracker':block['input']['interval_seconds']=300
        return response
    f.model.create=provider
    token='123456:abcdefghijklmnopqrstuvwxyz'
    monkeypatch.setenv('NULLION_TELEGRAM_BOT_TOKEN',token)
    settings=load_settings(env={'NULLION_TELEGRAM_BOT_TOKEN':token,'NULLION_TELEGRAM_OPERATOR_CHAT_ID':'202','NULLION_TELEGRAM_CHAT_ENABLED':'true'})
    service=telegram_app.ChatOperatorService(runtime=f.runtime,settings=settings,bot_token=token,operator_chat_id='202',model_client=f.model,agent_orchestrator=f.orchestrator)
    service._turn_dispatch_tracker=AsyncTurnDispatchTracker()
    class Sent:
        message_id=1000
        async def edit_text(self,value,**kw):wire.append(value);return self
        async def delete(self):pass
    class Message:
        caption=None;reply_to_message=None
        from_user=SimpleNamespace(id=202,first_name='Example',last_name='',username='example')
        chat=SimpleNamespace(id=202)
        def __init__(self,value,ident):self.text=value;self.message_id=ident
        async def reply_text(self,value,**kw):wire.append(value);return Sent()
    with TestClient(web_app.create_app(f.runtime,f.orchestrator,f.registry)) as client:
        async def conversation():
            await service.on_message(SimpleNamespace(update_id=71,message=Message('Alert me for a new article; ask me how long first.',71)),SimpleNamespace(bot=None))
            assert any('3. 6 hours' in value for value in wire),wire
            assert client.get('/api/live-trackers').json()['trackers']==[]
            setup[0]=True
            await service.on_message(SimpleNamespace(update_id=72,message=Message(followup,72)),SimpleNamespace(bot=None))
        asyncio.run(conversation())
        jobs=client.get('/api/live-trackers').json()['trackers']
        assert len(jobs)==1 and jobs[0]['state']=='active' and jobs[0]['interval_seconds']==300,(jobs,wire)
        assert (datetime.fromisoformat(jobs[0]['expires_at'])-datetime.fromisoformat(jobs[0]['created_at'])).total_seconds()==21600
        assert any('Live tracker' in value for value in wire),wire
        turn=next(row for row in f.runtime.store.list_conversation_events('telegram:202') if row.get('user_message')==followup and row.get('event_type')=='conversation.chat_turn')
        assert turn['parent_turn_id'] and any(r['tool_name']=='create_live_tracker' and r['status']=='completed' for r in turn['tool_results']),turn
        client.post('/api/live-trackers/'+jobs[0]['id']+'/stop')


def test_semantic_new_article_history_survives_edits_and_worker_restart(live_fixture):
    from fastapi.testclient import TestClient
    from nullion.web_app import create_app
    from nullion.live_tracking import LiveTrackerEngine
    from nullion.chat_store import get_chat_store
    f=live_fixture;original=f.model.create;histories=[]
    f.source.extra={'result_kind':'browser_item_collection','items':[{'title':'Original article','url':'https://example.test/news/one'}],
        'coverage':{'scope_found':True,'truncated':False,'sections':[]},
        'source_record_collections':[{'path':['items'],'identity_fields':['title','url'],'change_identity_fields':['url']}]}
    def provider(**kw):
        if not kw.get('tools'):
            for message in kw.get('messages',[]):
                for block in message.get('content',[]):
                    if not isinstance(block,dict):continue
                    try:packet=json.loads(block.get('text',''))
                    except (ValueError,TypeError):continue
                    if not isinstance(packet,dict) or 'observations' not in packet:continue
                    history=packet.get('observation_history');assert history is not None,'Polling forgot earlier observed identities'
                    histories.append(history)
                    fresh=next((r for r in history['records'] if r['status']=='new'),None)
                    if fresh is None:return text(json.dumps({'status':'no_match','checked_scopes':[{'step':0,'section_id':None}]}))
                    index=int(fresh['pointer'].rsplit('/',1)[1]);item=packet['observations'][0]['items'][index]
                    return text(json.dumps({'status':'matched','summary':item['title']+' — '+item['url'],
                        'evidence':[{'step':0,'pointer':fresh['pointer']+'/url','value':item['url']}]}))
        response=original(**kw)
        for block in response.get('content',[]):
            if block.get('name')=='create_live_tracker':block['input']['matcher']={'kind':'semantic',
                'instruction':'Alert only for a newly appearing article URL after the baseline; ignore known URLs and headline edits.'}
        return response
    f.model.create=provider;cid='web:new-article-history';restarted=None
    try:
        with TestClient(create_app(f.runtime,f.orchestrator,f.registry)) as client:
            response=client.post('/api/chat',json={'text':'新しい記事のURLだけ90秒間通知してください。','conversation_id':cid})
            assert 'Live tracker' in response.json()['text'], json.dumps({'response':response.json(),
                'events':[{k:e[k] for k in ('event_type','reason','status','tool_name','tool_results','draft_reply') if k in e}
                    for e in f.runtime.store.list_conversation_events(cid)
                    if e.get('event_type') not in {'conversation.model_timing','conversation.tool_scope_runtime'}]},ensure_ascii=False)
            store=client.app.state.live_tracker_store;job=store.list('workspace_admin')[0]
            calls=f.model.agent_calls
            wait_for(lambda:store.get(job['id'])['checks']>=1)
            assert store.get(job['id'])['observation_baseline_set'] and len(store.get(job['id'])['seen_observation_keys'])==1
            f.source.extra['items'][0]['title']='Edited existing headline'
            wait_for(lambda:store.get(job['id'])['checks']>=2)
            assert store.get(job['id'])['state']=='active' and store.get(job['id'])['last_outcome']=='no_match'
            old=client.app.state.live_tracker_engine;old.stop()
            restarted=LiveTrackerEngine(store,registry=f.registry,runtime_store=f.runtime.store,deliver=old.deliver,model_client=f.model)
            f.source.extra['items'].append({'title':'A newly published article','url':'https://example.test/news/two'})
            restarted.start()
            wait_for(lambda:store.get(job['id'])['notification_status']=='sent',timeout=20)
            saved=store.get(job['id'])
            assert saved['state']=='matched' and len(saved['seen_observation_keys'])==2
            assert saved['last_evaluation']['observation_history']['new_records']==1
            assert any(r['status']=='seen_before' for h in histories for r in h['records'])
            assert f.model.agent_calls==calls,'Polling restarted the agent rather than its prepared observation'
            alerts=[m for m in get_chat_store().load_messages(cid) if (m.get('metadata') or {}).get('source')=='live_tracker' and 'A newly published article' in m.get('text','')]
            assert len(alerts)==1 and 'https://example.test/news/two' in alerts[0]['text'],alerts
    finally:
        if restarted:restarted.stop()

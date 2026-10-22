from __future__ import annotations

import threading
import time
from types import SimpleNamespace

from fastapi.testclient import TestClient


def test_web_verbose_streaming_parallel_followup_and_planner_feed_contract(tmp_path, monkeypatch) -> None:
    from nullion import platform_chat, web_app
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    starts: list[str] = []

    def fake_route_turn_dispatch_with_context(text, *, active_turn_ids=(), **kwargs):  # noqa: ANN001, ANN003
        if text == "follow-up":
            assert "turn:slow" in active_turn_ids
            return TurnDispatchDecision(
                policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
                dependency_turn_ids=("turn:slow",),
                disposition=ConversationTurnDisposition.CONTINUE,
                reason="test_structured_follow_up",
            )
        return TurnDispatchDecision(policy=TurnDispatchPolicy.PARALLEL)

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        user_text = request.text
        starts.append(str(user_text))
        activity_callback = request.activity_callback
        if activity_callback is not None:
            activity_callback({"id": f"agent-{user_text}", "label": "Agent", "status": "running", "detail": str(user_text)})
        if user_text == "slow":
            time.sleep(0.25)
            return platform_chat.PlatformChatResponse(text="slow done " * 24)
        if user_text == "fast":
            return platform_chat.PlatformChatResponse(text="fast done " * 24)
        if user_text == "planner":
            return platform_chat.PlatformChatResponse(
                text="Planner: Parallel Mission\n- Gather data\n- Summarize",
                mini_agent_dispatch=True,
                task_group_id="group-planner",
            )
        if user_text == "follow-up":
            assert "slow" in starts
            return platform_chat.PlatformChatResponse(text="follow-up after slow")
        raise AssertionError(f"unexpected text: {user_text}")

    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_ENABLED", "true")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    monkeypatch.setattr(
        "nullion.turn_dispatch_graph.route_turn_dispatch_with_context",
        fake_route_turn_dispatch_with_context,
    )

    app = web_app.create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=object(), registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            for text, turn_id in [
                ("slow", "turn:slow"),
                ("fast", "turn:fast"),
                ("planner", "turn:planner"),
                ("follow-up", "turn:follow"),
            ]:
                websocket.send_json(
                    {
                        "text": text,
                        "conversation_id": "web:verbose-stream",
                        "turn_id": turn_id,
                        "activity_trace": True,
                        "stream": True,
                    }
                )

            events = []
            done_turns = set()
            while done_turns != {"turn:slow", "turn:fast", "turn:planner", "turn:follow"}:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    done_turns.add(event["turn_id"])

    assert "fast" in starts
    assert "slow" in starts
    assert starts.index("fast") < starts.index("follow-up")
    assert starts.index("slow") < starts.index("follow-up")

    fast_chunk_index = next(
        index for index, event in enumerate(events) if event.get("turn_id") == "turn:fast" and event.get("type") == "chunk"
    )
    slow_done_index = next(
        index for index, event in enumerate(events) if event.get("turn_id") == "turn:slow" and event.get("type") == "done"
    )
    follow_chunk_index = next(
        index for index, event in enumerate(events) if event.get("turn_id") == "turn:follow" and event.get("type") == "chunk"
    )

    assert fast_chunk_index < slow_done_index
    assert slow_done_index < follow_chunk_index
    assert any(
        event.get("turn_id") == "turn:fast"
        and event.get("type") == "activity"
        and event.get("id") == "agent-fast"
        and event.get("detail") == "fast"
        for event in events
    )
    assert any(
        event.get("turn_id") == "turn:planner"
        and event.get("type") == "task_status"
        and event.get("status_kind") == "task_summary"
        and event.get("group_id") == "group-planner"
        and "Planner: Parallel Mission" in event.get("text", "")
        for event in events
    )
    assert not any(event.get("turn_id") == "turn:planner" and event.get("type") == "chunk" for event in events)
    planner_done = next(event for event in events if event.get("turn_id") == "turn:planner" and event.get("type") == "done")
    assert planner_done.get("text", "") == ""


def test_websocket_deferred_cron_background_owner_suppresses_early_foreground_card(tmp_path, monkeypatch) -> None:
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    class Orchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[
                    ToolResult(
                        invocation_id="cron-tool",
                        tool_name="run_cron",
                        status="completed",
                        output={
                            "delivery_status": "deferred",
                            "mini_agent_dispatch": True,
                            "task_group_id": "cron-background-owned",
                            "planner_status_owned_by_background": True,
                            "result": {
                                "text": (
                                    "Manual scheduled task run started. "
                                    "The result will be delivered when ready."
                                )
                            },
                        },
                    )
                ],
                final_text="Manual scheduled task run started. The result will be delivered when ready.",
                artifacts=[],
                thinking_text="",
            )

    monkeypatch.setattr(web_app, "_try_dispatch_web_mini_agents", lambda **kwargs: None)
    monkeypatch.setattr(web_app, "_orchestrator_for_admin_forced_model", lambda orchestrator, runtime: orchestrator)
    monkeypatch.setattr(web_app, "_build_web_skill_hint", lambda runtime, user_text: None)
    monkeypatch.setattr(web_app, "_recent_web_tool_context_prompt", lambda runtime, conv_id: None)
    monkeypatch.setattr(
        web_app,
        "_try_web_builder_reflection",
        lambda *args, **kwargs: SimpleNamespace(detail="Skipped: test", learned_skill_titles=()),
    )
    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        web_app,
        "_enforce_web_response_fulfillment",
        lambda runtime, **kwargs: (kwargs["reply"], True),
    )
    app = web_app.create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=Orchestrator(), registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({
                "text": "trigger cron",
                "conversation_id": "web:cron-background-owned",
                "turn_id": "turn:cron",
                "stream": False,
            })

            events = []
            while True:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    break

    assert not any(
        event.get("type") == "task_status"
        and "Manual scheduled task run started" in str(event.get("text") or "")
        for event in events
    )
    assert not any(event.get("type") == "chunk" for event in events)


def test_websocket_deferred_cron_background_owner_suppresses_final_foreground_card(tmp_path, monkeypatch) -> None:
    from nullion import web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    class Orchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[
                    ToolResult(
                        invocation_id="cron-tool",
                        tool_name="run_cron",
                        status="completed",
                        output={
                            "delivery_status": "deferred",
                            "mini_agent_dispatch": True,
                            "task_group_id": "cron-background-owned",
                            "planner_status_owned_by_background": True,
                            "result": {
                                "text": (
                                    "Manual scheduled task run started. "
                                    "The result will be delivered when ready."
                                )
                            },
                        },
                    )
                ],
                final_text="Manual scheduled task run started. The result will be delivered when ready.",
                artifacts=[],
                thinking_text="",
            )

    monkeypatch.setattr(web_app, "_try_dispatch_web_mini_agents", lambda **kwargs: None)
    monkeypatch.setattr(web_app, "_orchestrator_for_admin_forced_model", lambda orchestrator, runtime: orchestrator)
    monkeypatch.setattr(web_app, "_build_web_skill_hint", lambda runtime, user_text: None)
    monkeypatch.setattr(web_app, "_recent_web_tool_context_prompt", lambda runtime, conv_id: None)
    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        web_app,
        "_enforce_web_response_fulfillment",
        lambda runtime, **kwargs: (kwargs["reply"], True),
    )
    monkeypatch.setattr(web_app, "_feature_enabled", lambda name, default=False: False)
    app = web_app.create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=Orchestrator(), registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({
                "text": "trigger cron",
                "conversation_id": "web:cron-background-owned",
                "turn_id": "turn:cron",
                "stream": False,
            })

            events = []
            while True:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    break

    assert not any(
        event.get("type") == "task_status"
        and "Manual scheduled task run started" in str(event.get("text") or "")
        for event in events
    )
    assert not any(event.get("type") == "chunk" for event in events)


def test_websocket_manual_cron_emits_foreground_fallback_card_before_suppression(tmp_path, monkeypatch) -> None:
    from nullion import crons, web_app
    from nullion.runtime import bootstrap_persistent_runtime, invoke_tool_with_boundary_policy
    from nullion.tools import ToolInvocation, ToolRegistry
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    cron_job = crons.add_cron(
        "Web Foreground Cron Card Smoke",
        "0 0 1 1 *",
        "Reply with the smoke-test completion line.",
        workspace_id="workspace_admin",
        delivery_channel="web",
        delivery_target="web:web-cron-card-smoke",
        enabled=False,
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()

    class Orchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            user_message = str(kwargs.get("user_message") or "")
            if user_message.startswith("Trigger"):
                result = invoke_tool_with_boundary_policy(
                    runtime.store,
                    ToolInvocation(
                        invocation_id="web-cron-card-smoke",
                        tool_name="run_cron",
                        principal_id=str(kwargs.get("principal_id") or "web:web-cron-card-smoke"),
                        arguments={"id": cron_job.id},
                        capsule_id=None,
                    ),
                    registry=registry,
                )
                return SimpleNamespace(
                    suspended_for_approval=False,
                    approval_id=None,
                    tool_results=[result],
                    final_text=str(result.output.get("result_text") or "Started."),
                    artifacts=[],
                    thinking_text="",
                )
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[],
                final_text="Web cron smoke completed.",
                artifacts=[],
                thinking_text="",
            )

    monkeypatch.setattr(web_app, "_try_dispatch_web_mini_agents", lambda **kwargs: None)
    monkeypatch.setattr(web_app, "_orchestrator_for_admin_forced_model", lambda orchestrator, runtime: orchestrator)
    monkeypatch.setattr(web_app, "_build_web_skill_hint", lambda runtime, user_text: None)
    monkeypatch.setattr(web_app, "_recent_web_tool_context_prompt", lambda runtime, conv_id: None)
    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        web_app,
        "_enforce_web_response_fulfillment",
        lambda runtime, **kwargs: (kwargs["reply"], True),
    )
    app = web_app.create_app(runtime, orchestrator=Orchestrator(), registry=registry)

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({
                "text": "Trigger Web Foreground Cron Card Smoke",
                "conversation_id": "web:web-cron-card-smoke",
                "turn_id": "turn:web-cron-card-smoke",
                "activity_trace": True,
                "stream": False,
            })

            events = []
            while True:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    break

    status_events = [event for event in events if event.get("type") == "task_status"]
    assert any(event.get("group_id", "").startswith(f"grp-{cron_job.id}-") for event in status_events)
    assert any("⏰ SCHEDULED TASK:" in str(event.get("text") or "") for event in status_events)
    assert any(
        "Working on 1 task" in str(event.get("text") or "")
        and "Web Foreground Cron Card Smoke" in str(event.get("text") or "")
        for event in status_events
    )
    assert not any(event.get("type") == "chunk" for event in events)


def test_websocket_background_tasks_off_runs_sequential_without_dispatch_model(tmp_path, monkeypatch) -> None:
    from nullion import platform_chat, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    starts: list[str] = []

    def forbidden_dispatch(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("sequential chat should not call turn dispatch model")

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        user_text = request.text
        starts.append(str(user_text))
        if user_text == "first":
            time.sleep(0.15)
        return platform_chat.PlatformChatResponse(text=f"{user_text} done")

    monkeypatch.setenv("NULLION_BACKGROUND_TASKS_ENABLED", "false")
    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    monkeypatch.setattr(
        "nullion.turn_dispatch_graph.route_turn_dispatch_with_context",
        forbidden_dispatch,
    )

    app = web_app.create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=object(), registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({"text": "first", "conversation_id": "web:sequential", "turn_id": "turn:first"})
            websocket.send_json({"text": "second", "conversation_id": "web:sequential", "turn_id": "turn:second"})

            events = []
            done_turns = set()
            while done_turns != {"turn:first", "turn:second"}:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    done_turns.add(event["turn_id"])

    first_done_index = next(index for index, event in enumerate(events) if event.get("turn_id") == "turn:first" and event.get("type") == "done")
    second_chunk_index = next(index for index, event in enumerate(events) if event.get("turn_id") == "turn:second" and event.get("type") == "chunk")

    assert starts == ["first", "second"]
    assert first_done_index < second_chunk_index


def test_websocket_done_omits_reply_text_after_reply_already_sent(tmp_path, monkeypatch) -> None:
    from nullion import platform_chat, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        user_text = request.text
        activity_callback = request.activity_callback
        assert activity_callback is not None
        activity_callback({"id": "tool-weather", "label": "Weather", "status": "running"})
        return platform_chat.PlatformChatResponse(
            text=f"{user_text} done",
            reply_already_sent=True,
        )

    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")
    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)

    app = web_app.create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=object(), registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "weather",
                    "conversation_id": "web:verbose-off-final-only",
                    "turn_id": "turn:quiet",
                    "activity_trace": False,
                    "stream": False,
                }
            )

            events = []
            while not any(event.get("type") == "done" and event.get("turn_id") == "turn:quiet" for event in events):
                events.append(websocket.receive_json())

    assert not any(event.get("type") == "activity" for event in events)
    assert not any(event.get("type") == "chunk" for event in events)
    done = next(event for event in events if event.get("type") == "done")
    assert done["text"] == ""


def test_websocket_stop_cancels_active_session_turns_without_waiting(tmp_path, monkeypatch) -> None:
    from nullion import platform_chat, web_app
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    started = threading.Event()
    starts: list[str] = []

    class FakeOrchestrator:
        model_client = None

        async def cancel_conversation(self, conversation_id: str) -> int:
            assert conversation_id == "web:stop-session"
            return 2

    def fake_route_turn_dispatch_with_context(text, *, active_turn_ids=(), **kwargs):  # noqa: ANN001, ANN003
        assert text != "/stop"
        return TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL,
            disposition=ConversationTurnDisposition.INDEPENDENT,
        )

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        user_text = request.text
        starts.append(str(user_text))
        started.set()
        time.sleep(0.25)
        return platform_chat.PlatformChatResponse(text="slow done")

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    monkeypatch.setattr(
        "nullion.turn_dispatch_graph.route_turn_dispatch_with_context",
        fake_route_turn_dispatch_with_context,
    )

    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=FakeOrchestrator(),
        registry=ToolRegistry(),
    )

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "slow",
                    "conversation_id": "web:stop-session",
                    "turn_id": "turn:slow",
                    "stream": True,
                }
            )
            assert started.wait(timeout=1)
            websocket.send_json(
                {
                    "text": "/stop",
                    "conversation_id": "web:stop-session",
                    "turn_id": "turn:stop",
                    "stream": True,
                }
            )

            events = []
            while not any(event.get("type") == "done" and event.get("turn_id") == "turn:stop" for event in events):
                events.append(websocket.receive_json())

    assert starts == ["slow"]
    stop_event = next(event for event in events if event.get("type") == "session_stopped")
    assert stop_event["cancelled_turn_ids"] == ["turn:slow"]
    assert stop_event["conversation_id"] == "web:stop-session"
    assert any(
        event.get("turn_id") == "turn:stop"
        and event.get("type") == "chunk"
        and event.get("text") == "Stopped 3 active tasks in this session."
        for event in events
    )

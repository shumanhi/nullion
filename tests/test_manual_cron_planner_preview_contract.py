from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace


class _StructuredPlannerModel:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "disposition": "parallel_mission",
                            "needs_clarification": False,
                            "clarification_question": None,
                            "routing_evidence": ["model_structured_plan"],
                            "tasks": [
                                {
                                    "title": "Collect metrics",
                                    "description": "Gather the source metrics for the report.",
                                    "tool_scope": [],
                                    "priority": "normal",
                                    "dependencies": [],
                                    "context_key_in": None,
                                    "context_key_out": "metrics",
                                    "required_inputs": [],
                                    "can_start": True,
                                },
                                {
                                    "title": "Monitor public signals",
                                    "description": "Gather independent public signals.",
                                    "tool_scope": [],
                                    "priority": "normal",
                                    "dependencies": [],
                                    "context_key_in": None,
                                    "context_key_out": "signals",
                                    "required_inputs": [],
                                    "can_start": True,
                                },
                                {
                                    "title": "Deliver report",
                                    "description": "Create the final report after collection finishes.",
                                    "tool_scope": [],
                                    "priority": "normal",
                                    "dependencies": [0, 1],
                                    "context_key_in": "metrics",
                                    "context_key_out": None,
                                    "required_inputs": [],
                                    "can_start": True,
                                },
                            ],
                        }
                    ),
                }
            ]
        }


class _TimeoutPlannerModel:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        raise RuntimeError("The read operation timed out")


class _CronRunOrchestrator:
    def __init__(self, model_client) -> None:  # noqa: ANN001
        self.model_client = model_client
        self.calls: list[dict[str, object]] = []

    def run_turn(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        return SimpleNamespace(
            suspended_for_approval=False,
            approval_id=None,
            tool_results=[],
            final_text="Planner-backed cron report ready.",
            artifacts=[],
            thinking_text="",
        )


def test_web_manual_cron_run_emits_real_planner_preview_status(tmp_path, monkeypatch) -> None:
    from nullion import chat_store, cron_planner_status, crons, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setattr(chat_store, "_store", None)
    cron_planner_status.clear_cron_planner_preview_caches()
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")

    job = crons.CronJob(
        id="cron-preview-real",
        name="Planner preview cron",
        schedule="0 * * * *",
        task="Build the scheduled report from structured runtime inputs.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="100001",
        enabled=False,
    )
    crons.save_crons([job])

    gateway_payloads: list[dict[str, object]] = []

    class FakeWebSocket:
        async def send_text(self, text: str) -> None:
            gateway_payloads.append(json.loads(text))

    def run_threadsafe(coro, loop):  # noqa: ANN001
        _ = loop
        asyncio.run(coro)
        return SimpleNamespace(result=lambda: None)

    planner_model = _StructuredPlannerModel()
    orchestrator = _CronRunOrchestrator(planner_model)
    registry = ToolRegistry()
    monkeypatch.setattr(web_app, "_WEB_GATEWAY_CLIENTS", {FakeWebSocket()})
    monkeypatch.setattr(web_app, "_WEB_DELIVERY_LOOP", SimpleNamespace(is_closed=lambda: False))
    monkeypatch.setattr(web_app.asyncio, "run_coroutine_threadsafe", run_threadsafe)

    web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=orchestrator,
        registry=registry,
    )

    result = registry.invoke(
        ToolInvocation(
            "inv-preview-real",
            "run_cron",
            "web:conversation-preview",
            {"id": job.id},
        )
    )

    assert result.status == "completed", result
    assert result.output["delivery_status"] == "deferred"
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not orchestrator.calls:
        time.sleep(0.01)
    assert orchestrator.calls
    status_payloads = []
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        status_payloads = [
            payload
            for payload in gateway_payloads
            if payload.get("type") == "task_status" and payload.get("group_id")
        ]
        kinds = [str(payload.get("status_kind") or "") for payload in status_payloads]
        if kinds.count("task_summary") >= 2:
            break
        time.sleep(0.01)
    kinds = [str(payload.get("status_kind") or "") for payload in status_payloads]
    assert kinds.count("task_summary") >= 2
    assert all(payload.get("conversation_id") == "web:conversation-preview" for payload in status_payloads)
    summaries = [str(payload.get("text") or "") for payload in status_payloads if payload.get("status_kind") == "task_summary"]

    assert any(text.startswith("⏰ SCHEDULED TASK") for text in summaries)
    assert all("Planning execution" not in text for text in summaries)
    assert any("For: Planner preview cron" in text for text in summaries)


def test_web_manual_cron_run_uses_clean_fallback_planner_summary_on_timeout(tmp_path, monkeypatch) -> None:
    from nullion import chat_store, cron_planner_status, crons, web_app
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setattr(chat_store, "_store", None)
    cron_planner_status.clear_cron_planner_preview_caches()
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")

    job = crons.CronJob(
        id="cron-preview-fallback",
        name="Planner timeout cron",
        schedule="0 * * * *",
        task="Build the scheduled report from structured runtime inputs.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="100001",
        enabled=False,
    )
    crons.save_crons([job])

    gateway_payloads: list[dict[str, object]] = []

    class FakeWebSocket:
        async def send_text(self, text: str) -> None:
            gateway_payloads.append(json.loads(text))

    def run_threadsafe(coro, loop):  # noqa: ANN001
        _ = loop
        asyncio.run(coro)
        return SimpleNamespace(result=lambda: None)

    registry = ToolRegistry()
    monkeypatch.setattr(web_app, "_WEB_GATEWAY_CLIENTS", {FakeWebSocket()})
    monkeypatch.setattr(web_app, "_WEB_DELIVERY_LOOP", SimpleNamespace(is_closed=lambda: False))
    monkeypatch.setattr(web_app.asyncio, "run_coroutine_threadsafe", run_threadsafe)

    web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=_CronRunOrchestrator(_TimeoutPlannerModel()),
        registry=registry,
    )

    result = registry.invoke(
        ToolInvocation(
            "inv-preview-fallback",
            "run_cron",
            "web:conversation-preview",
            {"id": job.id},
        )
    )

    assert result.status == "completed"
    status_payloads = []
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        status_payloads = [
            payload
            for payload in gateway_payloads
            if payload.get("type") == "task_status" and payload.get("group_id")
            and "Planner timeout cron" in str(payload.get("text") or "")
        ]
        if status_payloads:
            break
        time.sleep(0.01)
    assert status_payloads, "fallback planner preview should still emit a planner card"
    first_summary = str(status_payloads[0]["text"])
    assert "⏰ SCHEDULED TASK" in first_summary
    assert "Planning execution" not in first_summary


def test_cron_planner_preview_persistent_cache_survives_memory_clear(tmp_path, monkeypatch) -> None:
    from nullion import cron_planner_status
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_CRON_PLANNER_PREVIEW_CACHE_TTL_SECONDS", "3600")
    monkeypatch.setenv("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED", "1")
    monkeypatch.setenv(
        "NULLION_CRON_PLANNER_PREVIEW_CACHE_PATH",
        str(tmp_path / "home" / ".nullion" / "cron_planner_preview_cache.json"),
    )
    cron_planner_status._PREVIEW_CACHE.clear()
    cron_planner_status._PERSIST_CACHE_LOADED = False

    model = _StructuredPlannerModel()
    preview = cron_planner_status.build_cron_planner_status_preview(
        model_client=model,
        user_message="Daily Pre-Market Stock Research\nFetch and summarize market inputs",
        conversation_id="web:persist-cache-1",
        principal_id="web:persist-cache-1",
        tool_registry=ToolRegistry(),
        subject="Daily Pre-Market Stock Research",
    )
    assert preview is not None
    assert model.calls

    persisted = tmp_path / "home" / ".nullion" / "cron_planner_preview_cache.json"
    assert persisted.exists()

    cron_planner_status._PREVIEW_CACHE.clear()
    cron_planner_status._PERSIST_CACHE_LOADED = False

    timeout_model = _TimeoutPlannerModel()
    cached = cron_planner_status.build_cron_planner_status_preview(
        model_client=timeout_model,
        user_message="Daily Pre-Market Stock Research\nFetch and summarize market inputs",
        conversation_id="web:persist-cache-2",
        principal_id="web:persist-cache-2",
        tool_registry=ToolRegistry(),
        subject="Daily Pre-Market Stock Research",
    )
    assert cached is not None
    assert not hasattr(timeout_model, "calls") or not getattr(timeout_model, "calls", None)


def test_cron_planner_preview_defaults_to_metadata_fallback_without_model_call(monkeypatch) -> None:
    from nullion import cron_planner_status
    from nullion.tools import ToolRegistry

    monkeypatch.delenv("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED", raising=False)
    cron_planner_status._PREVIEW_CACHE.clear()
    cron_planner_status._PERSIST_CACHE_LOADED = False

    model = _TimeoutPlannerModel()
    preview = cron_planner_status.build_cron_planner_status_preview(
        model_client=model,
        user_message="Daily report\nCollect data and write a report",
        conversation_id="telegram:cron-preview-fallback",
        principal_id="telegram:cron-preview-fallback",
        tool_registry=ToolRegistry(),
        subject="Daily report",
    )

    assert preview is not None
    assert not model.calls
    assert preview.planner_summary == "Scheduled task"


def test_cron_planner_preview_cache_matches_normalized_subject_and_task(tmp_path, monkeypatch) -> None:
    from nullion import cron_planner_status
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_CRON_PLANNER_PREVIEW_CACHE_TTL_SECONDS", "3600")
    monkeypatch.setenv("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED", "1")
    cron_planner_status._PREVIEW_CACHE.clear()
    cron_planner_status._PERSIST_CACHE_LOADED = False

    model = _StructuredPlannerModel()
    cached = cron_planner_status.build_cron_planner_status_preview(
        model_client=model,
        user_message="Nullion social + GitHub traffic monitor no empty graphs\nCollect metrics and build report",
        conversation_id="web:norm-1",
        principal_id="web:norm-1",
        tool_registry=ToolRegistry(),
        subject="Nullion social + GitHub traffic monitor no empty graphs",
    )
    assert cached is not None
    assert model.calls

    timeout_model = _TimeoutPlannerModel()
    hit = cron_planner_status.build_cron_planner_status_preview(
        model_client=timeout_model,
        user_message="  Nullion social + GitHub traffic monitor no empty graphs  \n\n  Collect metrics and build report  ",
        conversation_id="web:norm-2",
        principal_id="web:norm-2",
        tool_registry=ToolRegistry(),
        subject="Nullion social + GitHub traffic monitor no empty graphs",
        cache_only=True,
    )
    assert hit is not None

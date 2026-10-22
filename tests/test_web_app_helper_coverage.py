from __future__ import annotations

import json
import logging
import inspect
import sys
import threading
import time
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion import web_app
from nullion.approvals import TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND, create_approval_request
from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.runtime import create_skill_result
from nullion.runtime_store import RuntimeStore
from nullion.skills import SkillRecord
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
)
from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


@pytest.mark.parametrize("prompt", ["hello", "こんにちは", "مرحبا"])
def test_web_plain_reply_delivery_does_not_classify_attachment_format(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    format_calls = []

    class Model:
        def create(self, **kwargs):
            if any(tool.get("name") == "select_attachment_format" for tool in kwargs.get("tools", [])):
                format_calls.append(kwargs)
            return {"content": [{"type": "text", "text": '{"disposition":"independent","confidence":0.99}'}]}

    class Orchestrator:
        model_client = Model()

        def run_turn(self, **kwargs):
            return SimpleNamespace(final_text="42", tool_results=[], artifacts=[], suspended_for_approval=False, reached_iteration_limit=False)

    monkeypatch.setattr(web_app, "_schedule_web_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_schedule_web_no_tool_memory_capture", lambda *args, **kwargs: None)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = web_app.create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))
    with TestClient(app) as client:
        response = client.post("/api/chat", json={"text": prompt, "conversation_id": "web:no-artifact-format"})
    assert response.status_code == 200
    assert response.json()["text"] == "42"
    assert response.json().get("artifacts", []) == []
    assert format_calls == []


def _cron_preview_task(task_id: str, title: str, *, dependencies: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        task_id=task_id,
        title=title,
        description=title,
        allowed_tools=[],
        dependencies=dependencies or [],
        context_key_in=None,
        context_key_out=None,
        metadata={},
        priority=None,
    )


def test_web_compact_reply_after_numbered_prompt_has_structured_followup_evidence() -> None:
    class StructuredDispatchModel:
        def create(self, **kwargs):
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"relationship":"follow_up","effect":"continue",'
                            '"target_index":0,"confidence":0.93}'
                        ),
                    }
                ]
            }

    thread = [
        {
            "user": "Add the identified items to my cart.",
            "assistant": (
                "I can keep going by:\n"
                "1. Searching the merchant name\n"
                "2. Opening the item page\n"
                "Reply 1 and I will continue."
            ),
        }
    ]

    assert web_app._web_thread_has_numbered_choice_followup_evidence(thread, "sí")
    classifier, reason = web_app._web_ambiguity_classifier(
        thread,
        model_client=StructuredDispatchModel(),
        structured_followup_evidence=True,
        allow_plain_compact_classification=True,
    )

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    ctx = SimpleNamespace(active_branch_exists=True)
    assert classifier("sí", ctx) is ConversationTurnDisposition.CONTINUE


def test_web_recent_account_tool_result_is_structured_followup_evidence() -> None:
    from nullion.chat_operator import _recent_conversation_has_structured_tool_evidence

    store = RuntimeStore()
    store.add_conversation_event(
        {
            "conversation_id": "web:email-thread",
            "event_type": "conversation.chat_turn",
            "created_at": datetime.now(UTC).isoformat(),
            "user_message": "Revisa los mensajes sin leer.",
            "assistant_reply": "Encontré dos mensajes.",
            "tool_results": [
                {
                    "tool_name": "email_search",
                    "status": "completed",
                    "output": {
                        "body_requires_tool": "email_read",
                        "next_tool_for_body": "email_read",
                        "results": [{"id": "msg-2", "subject": "Factura"}],
                    },
                }
            ],
        }
    )
    runtime = SimpleNamespace(store=store)

    assert _recent_conversation_has_structured_tool_evidence(
        runtime,
        conversation_id="web:email-thread",
        limit=1,
    )
    assert web_app._recent_web_tool_scopes_for_context(runtime, "web:email-thread") == ("connector",)


class FakeStore:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.runtime_events: list[object] = []
        self.suspended: list[object] = []
        self.active_frame_id: str | None = None
        self.frames: dict[str, object] = {}
        self.task_frames = self.frames
        self.active_task_frames: dict[str, str] = {}
        self.approvals: dict[str, object] = {}
        self.doctor_actions: dict[str, dict] = {}
        self.mini_agent_runs: dict[str, object] = {}

    def add_conversation_event(self, event: dict) -> None:
        self.events.append(event)

    def add_event(self, event) -> None:
        self.runtime_events.append(event)

    def list_events(self) -> list[object]:
        return list(self.runtime_events)

    def list_conversation_events(self, conversation_id: str) -> list[dict]:
        return [event for event in self.events if event.get("conversation_id") == conversation_id]

    def set_active_task_frame_id(self, conversation_id: str, frame_id: str | None) -> None:
        self.active_frame_id = frame_id
        if frame_id is None:
            self.active_task_frames.pop(conversation_id, None)
        else:
            self.active_task_frames[conversation_id] = frame_id

    def get_active_task_frame_id(self, conversation_id: str) -> str | None:
        return self.active_task_frames.get(conversation_id) or self.active_frame_id

    def get_task_frame(self, frame_id: str | None):
        return self.frames.get(frame_id or "")

    def add_task_frame(self, frame) -> None:
        self.frames[frame.frame_id] = frame

    def remove_task_frame(self, frame_id: str) -> bool:
        removed = self.frames.pop(frame_id, None) is not None
        for conversation_id, active_frame_id in list(self.active_task_frames.items()):
            if active_frame_id == frame_id:
                self.active_task_frames.pop(conversation_id, None)
        if self.active_frame_id == frame_id:
            self.active_frame_id = None
        return removed

    def add_suspended_turn(self, turn) -> None:
        self.suspended.append(turn)

    def get_approval_request(self, approval_id: str):
        return self.approvals.get(approval_id)

    def list_approval_requests(self) -> list[object]:
        return list(self.approvals.values())

    def list_suspended_turns(self) -> list[object]:
        return list(self.suspended)

    def add_doctor_action(self, action: dict) -> None:
        action = dict(action)
        action.setdefault("created_at", datetime.now(UTC).isoformat())
        action.setdefault("updated_at", action["created_at"])
        self.doctor_actions[action["action_id"]] = action

    def get_doctor_action(self, action_id: str):
        action = self.doctor_actions.get(action_id)
        return dict(action) if action is not None else None

    def list_doctor_actions(self) -> list[dict]:
        return [dict(action) for action in self.doctor_actions.values()]

    def remove_doctor_action(self, action_id: str) -> bool:
        return self.doctor_actions.pop(action_id, None) is not None

    def add_mini_agent_run(self, run) -> None:
        self.mini_agent_runs[run.run_id] = run

    def list_mini_agent_runs(self) -> list[object]:
        return list(self.mini_agent_runs.values())

    def remove_mini_agent_run(self, run_id: str) -> bool:
        return self.mini_agent_runs.pop(run_id, None) is not None


def test_scheduled_task_registry_bypasses_ordinary_turn_tool_scoping() -> None:
    class ScheduledRegistry:
        scheduled_task_execution_registry = True

        def list_specs(self):  # pragma: no cover - must not be called
            raise AssertionError("scheduled cron registries should not be re-scoped")

    registry = ScheduledRegistry()

    selected = web_app._turn_tool_registry_for_evidence(
        registry,
        evidence=SimpleNamespace(),
        model_client=object(),
        user_message="[Scheduled task: tracker] Check public status.",
        skip_tool_scope_decision=False,
    )

    assert selected is registry


class FakeRuntime:
    def __init__(self, store: FakeStore | None = None) -> None:
        self.store = store
        self.checkpoints = 0
        self.health_issues: list[dict] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def report_health_issue(self, **kwargs) -> None:
        self.health_issues.append(kwargs)


def _wait_for_checkpoints(runtime: FakeRuntime, expected: int, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if runtime.checkpoints >= expected:
            return
        time.sleep(0.01)


def test_web_stable_context_history_prefix_is_cached(monkeypatch) -> None:
    from nullion import builder_capabilities, config, connections, runtime_config, system_context, web_research_policy
    from nullion import skill_pack_installer

    class Registry:
        version = 1

        def list_tool_definitions(self):
            return [
                {
                    "name": f"tool_{self.version}",
                    "description": "demo",
                    "capability_tags": [],
                    "side_effect_class": "read",
                    "risk_level": "low",
                    "requires_approval": False,
                }
            ]

    calls = {
        "snapshot": 0,
        "config": 0,
        "connections": 0,
        "skills": 0,
        "index": 0,
        "research": 0,
        "dependencies": 0,
    }

    def fake_snapshot(**kwargs):  # noqa: ANN003
        calls["snapshot"] += 1
        return object()

    def fake_config(**kwargs):  # noqa: ANN003
        calls["config"] += 1
        return "Runtime config"

    def fake_connections(**kwargs):  # noqa: ANN003
        calls["connections"] += 1
        return f"Connections {kwargs['principal_id']}"

    def fake_skills(enabled):  # noqa: ANN001
        calls["skills"] += 1
        return "Skill packs " + ",".join(enabled)

    def fake_index(enabled, **kwargs):  # noqa: ANN001, ANN003
        calls["index"] += 1
        return "Skill index " + ",".join(enabled)

    def fake_research(**kwargs):  # noqa: ANN003
        calls["research"] += 1
        return "Web research"

    def fake_dependencies(runtime):  # noqa: ANN001
        calls["dependencies"] += 1
        return "Dependencies"

    monkeypatch.setattr(system_context, "build_system_context_snapshot", fake_snapshot)
    monkeypatch.setattr(system_context, "format_system_context_for_prompt", lambda snapshot: "Capabilities")
    monkeypatch.setattr(runtime_config, "format_runtime_config_for_prompt", fake_config)
    monkeypatch.setattr(connections, "format_workspace_connections_for_prompt", fake_connections)
    monkeypatch.setattr(skill_pack_installer, "format_compact_enabled_skill_packs_for_prompt", fake_skills)
    monkeypatch.setattr(skill_pack_installer, "format_cached_enabled_skill_pack_index_for_prompt", fake_index)
    monkeypatch.setattr(web_research_policy, "format_web_research_guidance", fake_research)
    monkeypatch.setattr(builder_capabilities, "format_installed_dependency_context", fake_dependencies)
    monkeypatch.setattr(
        config,
        "load_settings",
        lambda: SimpleNamespace(
            enabled_plugins=(),
            provider_bindings=(),
            enabled_skill_packs=("local/demo",),
            workspace_root="",
            allowed_roots=(),
            web_session_allow_duration="session",
            model=SimpleNamespace(
                provider="codex",
                openai_base_url="",
                openai_model="gpt-5",
                reasoning_effort="low",
                openai_api_key=None,
                codex_refresh_token="token",
                anthropic_api_key=None,
                anthropic_model="",
            ),
        ),
    )
    monkeypatch.setattr(web_app, "tool_registry_allows_connector_context", lambda registry: True)
    monkeypatch.setattr(web_app, "tool_registry_allows_skill_pack_context", lambda registry: True)
    monkeypatch.setattr(web_app, "tool_registry_allows_skill_pack_prompt_context", lambda registry: False)
    monkeypatch.setattr(web_app, "skill_pack_access_prompt", lambda enabled, principal_id: "Skill access")
    monkeypatch.setattr(web_app, "_web_prompt_env_signature", lambda: ())
    monkeypatch.setattr(web_app, "_web_context_file_signatures", lambda: ())
    monkeypatch.setattr(web_app, "_web_enabled_skill_pack_signature", lambda enabled: tuple(enabled))
    monkeypatch.setattr(
        web_app,
        "active_connector_provider_context_snapshot",
        lambda: (
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Example API Gateway",
                "permission_mode": "write",
                "structured_tools": ("connector_request",),
                "skill_pack_id": "example/api-gateway-skill",
                "active_app_ids": ("example-drive",),
                "reference_path_count": 1,
                "reference_paths_sample": ("references/example-drive/README.md",),
            },
        ),
    )

    runtime = FakeRuntime()
    orchestrator = SimpleNamespace(model_client=SimpleNamespace(model="gpt-5", provider="codex"))
    registry = Registry()

    try:
        web_app._clear_web_stable_context_cache()
        first = web_app._web_stable_context_history_prefix(
            runtime=runtime,
            principal_id="web:cache",
            turn_orchestrator=orchestrator,
            turn_tool_registry=registry,
        )
        second = web_app._web_stable_context_history_prefix(
            runtime=runtime,
            principal_id="web:cache",
            turn_orchestrator=orchestrator,
            turn_tool_registry=registry,
        )
        assert first == second
        assert first is not second
        prompt_text = json.dumps(first, default=str)
        assert "Associated connector skill context:" in prompt_text
        assert "active_app_ids=[example-drive]" in prompt_text
        assert "Builder can create or save a reusable skill or connector workflow" in prompt_text
        assert calls == {
            "snapshot": 1,
            "config": 1,
            "connections": 1,
            "skills": 0,
            "index": 1,
            "research": 1,
            "dependencies": 1,
        }

        registry.version = 2
        web_app._web_stable_context_history_prefix(
            runtime=runtime,
            principal_id="web:cache",
            turn_orchestrator=orchestrator,
            turn_tool_registry=registry,
        )
        assert calls["snapshot"] == 2

        web_app._clear_web_stable_context_cache(reason="test")
        web_app._web_stable_context_history_prefix(
            runtime=runtime,
            principal_id="web:cache",
            turn_orchestrator=orchestrator,
            turn_tool_registry=registry,
        )
        assert calls["snapshot"] == 3
    finally:
        web_app._clear_web_stable_context_cache()


def test_web_stable_context_file_signature_excludes_runtime_db(monkeypatch, tmp_path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    for name in (".env", "credentials.json", "runtime.db", "connections.json", "users.json"):
        (home / name).write_text("x", encoding="utf-8")

    monkeypatch.setenv("NULLION_HOME", str(home))
    monkeypatch.delenv("NULLION_ENV_FILE", raising=False)

    signed_paths = {Path(path).name for path, _mtime, _size in web_app._web_context_file_signatures()}

    assert {".env", "credentials.json", "connections.json", "users.json"}.issubset(signed_paths)
    assert "runtime.db" not in signed_paths


def test_basic_web_app_helpers_cover_edge_shapes(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NULLION_WORKSPACE_ROOT", raising=False)
    assert web_app._default_workspace_root() == str(tmp_path)
    assert web_app._configured_workspace_root() == tmp_path.resolve()
    assert web_app._truthy_env("yes") is True
    assert web_app._truthy_env(" false ") is False
    assert web_app._split_model_entries([" a ", "a", "", "b"]) == ["a", "b"]
    assert web_app._split_model_entries("a,b\na") == ["a", "b"]
    assert web_app._primary_model_entry(" first,second") == "first"

    monkeypatch.setattr(web_app.shutil, "which", lambda name: name if name == "found" else None)
    assert web_app._which_local_tool("found") == "found"
    assert web_app._which_local_tool("missing") == ""

    monkeypatch.delenv("NULLION_CODEX_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("CODEX_REFRESH_TOKEN", "env-refresh")
    provider, token = web_app._model_provider_and_codex_token_for_config(
        "openai",
        oai_key="oauth-token",
        creds={"provider": "codex", "api_key": "stored-access"},
        stored_keys={"codex": "stored-key"},
    )
    assert provider == "codex"
    assert token == "env-refresh"


def test_open_workspace_folder_api_opens_configured_root(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    opened: list[Path] = []

    def fake_open(path: Path) -> None:
        opened.append(path)

    storage_root = tmp_path / "storage"
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(storage_root))
    monkeypatch.setattr(web_app, "_open_local_directory", fake_open)

    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=None,
        registry=ToolRegistry(),
    )
    response = TestClient(app).post("/api/workspace/open")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["path"] == str(storage_root.resolve() / "workspace_admin")
    assert opened == [storage_root.resolve() / "workspace_admin"]


def test_connection_imap_settings_endpoint_redacts_password(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_HOST", "imap.gmail.com")
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_PORT", "993")
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_SMTP_PORT", "587")
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_USERNAME", "example-user@example.com")
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_PASSWORD", "secret")

    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=None,
        registry=ToolRegistry(),
    )
    response = TestClient(app).post(
        "/api/connections/imap-settings",
        json={"credential_ref": "FIXTURE_ACCOUNT"},
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["imap_host"] == "imap.gmail.com"
    assert payload["username"] == "example-user@example.com"
    assert payload["password_set"] is True
    assert "password" not in payload


def test_connection_imap_test_endpoint_reports_login_and_mailbox(monkeypatch, tmp_path) -> None:
    import imaplib

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime

    class FakeImap:
        def __init__(self, host, port, timeout=None):  # noqa: ANN001
            self.host = host
            self.port = port
            self.timeout = timeout
            self.logged_out = False

        def login(self, username, password):  # noqa: ANN001
            assert (username, password) == ("example-user@example.com", "secret")

        def select(self, mailbox, readonly=True):  # noqa: ANN001
            assert mailbox == "INBOX"
            assert readonly is True
            return "OK", [b"1"]

        def logout(self):
            self.logged_out = True

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_HOST", "imap.gmail.com")
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_USERNAME", "example-user@example.com")
    monkeypatch.setenv("NULLION_IMAP_FIXTURE_ACCOUNT_PASSWORD", "secret")
    monkeypatch.setattr(imaplib, "IMAP4_SSL", FakeImap)

    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=None,
        registry=ToolRegistry(),
    )
    response = TestClient(app).post(
        "/api/connections/test-imap",
        json={"provider_id": "imap_smtp_provider", "credential_ref": "FIXTURE_ACCOUNT"},
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert "imap.gmail.com:993" in payload["summary"]
    assert "example-user@example.com" in payload["summary"]


def test_web_chat_slash_prefixed_paths_route_as_normal_messages(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime

    seen: list[str] = []

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        del runtime_arg
        seen.append(request.text)
        return platform_chat.PlatformChatResponse(text=f"normal:{request.text}")

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=None,
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    with TestClient(app) as client:
        path_response = client.post(
            "/api/chat",
            json={"text": "/Users/example/Downloads/synthetic-chat-path.txt", "conversation_id": "web:helper"},
        )
        unicode_response = client.post(
            "/api/chat",
            json={"text": "/tmp/資料.txt", "conversation_id": "web:helper"},
        )
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "/Users/example/Downloads/synthetic-chat-path.txt",
                    "conversation_id": "web:helper",
                    "turn_id": "turn:path",
                    "stream": False,
                }
            )
            events: list[dict[str, object]] = []
            while True:
                event = websocket.receive_json()
                events.append(event)
                if event.get("type") == "done":
                    break

    assert path_response.json()["text"] == "normal:/Users/example/Downloads/synthetic-chat-path.txt"
    assert unicode_response.json()["text"] == "normal:/tmp/資料.txt"
    assert {"turn_id": "turn:path", "type": "chunk", "text": "normal:/Users/example/Downloads/synthetic-chat-path.txt"} in events
    assert seen == [
        "/Users/example/Downloads/synthetic-chat-path.txt",
        "/tmp/資料.txt",
        "/Users/example/Downloads/synthetic-chat-path.txt",
    ]


def test_web_chat_http_deferred_background_cron_keeps_visible_receipt(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        del runtime_arg, request
        return platform_chat.PlatformChatResponse(
            text="Manual scheduled task run started. The result will be delivered when ready."
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=None,
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    response = TestClient(app).post(
        "/api/chat",
        json={"text": "Trigger background cron", "conversation_id": "web:helper"},
    )

    assert response.status_code == 200
    assert response.json()["text"] == (
        "Manual scheduled task run started. The result will be delivered when ready."
    )


def test_web_chat_platform_artifact_candidates_scrub_suppressed_helper(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.workspace_storage import workspace_storage_roots_for_principal

    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    roots = workspace_storage_roots_for_principal("web:helper")
    html = roots.artifacts / "train_accessible_outdoor_day_trips.html"
    svg = roots.artifacts / "train_accessible_day_trips_summary.svg"
    html.write_text(
        "<html><body><h1>Day trips</h1><p>Verified itinerary content with images.</p></body></html>",
        encoding="utf-8",
    )
    svg.write_text("<svg><text>helper</text></svg>", encoding="utf-8")

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        del runtime_arg, request
        return platform_chat.PlatformChatResponse(
            text=(
                "Created the requested HTML file and.svg file:\n"
                f"- `{html.name}`\n"
                f"- `{svg.name}`"
            ),
            artifact_paths=[str(html)],
            artifact_candidate_paths=[str(html), str(svg)],
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=None,
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    response = TestClient(app).post(
        "/api/chat",
        json={"text": "build the travel report", "conversation_id": "web:helper"},
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["text"] == "Done — attached the requested HTML file."
    assert payload["artifacts"][0]["name"] == html.name
    assert svg.name not in payload["text"]
    assert "SVG" not in payload["text"]


def test_web_chat_slash_prefixed_path_hides_file_read_from_orchestrator(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.agent_orchestrator import TurnResult
    from nullion.runtime import bootstrap_persistent_runtime

    seen_tools: list[set[str]] = []

    class FakeOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            tool_names = {definition["name"] for definition in kwargs["tool_registry"].list_tool_definitions()}
            seen_tools.append(tool_names)
            assert "file_read" not in tool_names
            return TurnResult(
                turn_id="turn-path",
                final_text="received as plain text",
                tool_results=[],
            )

    app = web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=FakeOrchestrator(),
        registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]),
    )

    response = TestClient(app).post(
        "/api/chat",
        json={"text": "/Users/example/Downloads/synthetic-chat-path.txt", "conversation_id": "web:helper"},
    )

    assert response.json()["type"] == "message"
    assert response.json()["text"] == "received as plain text"
    assert seen_tools and "file_read" not in seen_tools[0]


def test_web_memory_context_reinforces_only_selected_memories(monkeypatch) -> None:
    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin

    owner = memory_owner_for_web_admin()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    hot = UserMemoryEntry(
        entry_id=f"{owner}:hot",
        owner=owner,
        kind=UserMemoryKind.FACT,
        key="hot",
        value="remembered often",
        created_at=now,
        updated_at=now,
        use_count=2,
        use_score=6.0,
        last_used_at=now,
    )
    cold = UserMemoryEntry(
        entry_id=f"{owner}:cold",
        owner=owner,
        kind=UserMemoryKind.FACT,
        key="cold",
        value="not recalled",
        created_at=now + timedelta(days=1),
        updated_at=now + timedelta(days=1),
    )

    class MemoryStore:
        def __init__(self) -> None:
            self.entries = {hot.entry_id: hot, cold.entry_id: cold}

        def list_user_memory_entries(self):
            return list(self.entries.values())

        def add_user_memory_entry(self, entry) -> None:
            self.entries[entry.entry_id] = entry

    monkeypatch.setenv("NULLION_MEMORY_LONG_TERM_LIMIT", "0")
    monkeypatch.setenv("NULLION_MEMORY_MID_TERM_LIMIT", "0")
    monkeypatch.setenv("NULLION_MEMORY_SHORT_TERM_LIMIT", "1")
    runtime = FakeRuntime(MemoryStore())

    text = web_app._web_memory_context(runtime)

    assert "Hot: remembered often" in text
    assert "Cold: not recalled" not in text
    assert runtime.store.entries[hot.entry_id].use_score == 7.0
    assert runtime.store.entries[hot.entry_id].use_count == 3
    assert runtime.store.entries[cold.entry_id].use_score == 0.0
    assert runtime.checkpoints == 1


def test_web_memory_context_can_skip_reinforcement_for_cron(monkeypatch) -> None:
    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_workspace

    owner = memory_owner_for_workspace("workspace_ops")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    entry = UserMemoryEntry(
        entry_id=f"{owner}:watcher_scope",
        owner=owner,
        kind=UserMemoryKind.FACT,
        key="watcher_scope",
        value="watcher uses repo status",
        created_at=now,
        updated_at=now,
        use_count=2,
        use_score=4.0,
        last_used_at=now,
    )

    class MemoryStore:
        def __init__(self) -> None:
            self.entries = {entry.entry_id: entry}

        def list_user_memory_entries(self):
            return list(self.entries.values())

        def add_user_memory_entry(self, updated) -> None:
            self.entries[updated.entry_id] = updated

    runtime = FakeRuntime(MemoryStore())
    text = web_app._web_memory_context(runtime, owner=owner, reinforce=False)

    assert "Watcher Scope: watcher uses repo status" in text
    assert runtime.store.entries[entry.entry_id].use_score == 4.0
    assert runtime.store.entries[entry.entry_id].use_count == 2
    assert runtime.checkpoints == 0


def test_manual_cron_run_uses_job_workspace_memory_without_reinforcing(monkeypatch, tmp_path) -> None:
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry

    job = crons.CronJob(
        id="cron-workspace",
        name="Workspace watcher",
        schedule="*/5 * * * *",
        task="check the workspace",
        workspace_id="workspace_ops",
        delivery_channel="web",
        delivery_target="web:operator",
    )
    captured: dict[str, object] = {}
    completed = threading.Event()

    def fake_run_cron_delivery_workflow(cron_job, *, label, callbacks):  # noqa: ANN001
        return callbacks.run_agent_turn(cron_job, "web:operator")

    class FakeOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            completed.set()
            return SimpleNamespace(
                final_text="ok",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    monkeypatch.setattr(crons, "load_crons", lambda: [])
    monkeypatch.setattr(crons, "get_cron", lambda cron_id: job if cron_id == job.id else None)
    monkeypatch.setattr(web_app, "run_cron_delivery_workflow", fake_run_cron_delivery_workflow)

    registry = ToolRegistry()
    web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=FakeOrchestrator(),
        registry=registry,
    )
    result = registry.invoke(
        ToolInvocation(
            invocation_id="invoke-1",
            tool_name="run_cron",
            principal_id="workspace:workspace_ops",
            arguments={"id": job.id},
        )
    )

    assert result.status == "completed"
    assert completed.wait(2)
    assert captured["conversation_id"] == "web:operator"
    assert captured["principal_id"] == "web:operator"
    assert "Scheduled task" in captured["user_message"]
    flow_context = captured["tool_flow_context"]
    assert flow_context["scheduled_task_run"] is True
    assert flow_context["cron_id"] == job.id
    assert flow_context["cron_name"] == job.name


def test_manual_web_cron_run_targets_originating_web_conversation(monkeypatch, tmp_path) -> None:
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry

    job = crons.CronJob(
        id="cron-web-origin",
        name="Origin scoped watcher",
        schedule="*/5 * * * *",
        task="run the report",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="100001",
    )
    captured: dict[str, object] = {}
    completed = threading.Event()

    def fake_run_cron_delivery_workflow(cron_job, *, label, callbacks):  # noqa: ANN001
        captured["delivery_channel"] = callbacks.effective_channel(cron_job)
        captured["delivery_target"] = callbacks.delivery_target(cron_job, captured["delivery_channel"])
        result = callbacks.run_agent_turn(cron_job, captured["delivery_target"])
        result["cron_delivery_status"] = "deferred"
        return result

    class FakeOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            completed.set()
            return SimpleNamespace(
                final_text="Cron report ready.",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    monkeypatch.setattr(crons, "load_crons", lambda: [])
    monkeypatch.setattr(crons, "get_cron", lambda cron_id: job if cron_id == job.id else None)
    monkeypatch.setattr(web_app, "run_cron_delivery_workflow", fake_run_cron_delivery_workflow)

    registry = ToolRegistry()
    web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=FakeOrchestrator(),
        registry=registry,
    )
    result = registry.invoke(
        ToolInvocation(
            invocation_id="invoke-origin",
            tool_name="run_cron",
            principal_id="web:chat-origin",
            arguments={"id": job.id},
        )
    )

    assert result.status == "completed"
    assert completed.wait(2)
    assert captured["delivery_channel"] == "web"
    assert captured["delivery_target"] == "web:chat-origin"
    assert captured["conversation_id"] == "web:chat-origin"
    assert captured["principal_id"] == "web:chat-origin"
    assert result.output.get("mini_agent_dispatch") is None
    assert result.output["task_group_id"].startswith(f"grp-{job.id}-")
    assert "⏰ SCHEDULED TASK: Origin scoped watcher" in result.output["progress_status_text"]
    assert "◐ Origin scoped watcher" in result.output["progress_status_text"]


def test_web_app_publishes_registry_for_tools_operator_command(tmp_path) -> None:
    from nullion.operator_commands import handle_operator_command
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            "weather_forecast",
            "Check local weather",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            5,
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"ok": True},
        ),
    )

    web_app.create_app(runtime, orchestrator=None, registry=registry)

    rendered = handle_operator_command(runtime, "/tools")
    assert "No tools are loaded" not in rendered
    assert "Ready to use:" in rendered
    assert "Weather forecast" in rendered


@pytest.mark.parametrize(
    ("capability", "provider", "model", "expected"),
    [
        ("audio_transcribe", "openai", "whisper-1", True),
        ("audio_transcribe", "anthropic", "claude", False),
        ("image_ocr", "anthropic", "claude-sonnet", True),
        ("image_generate", "openai", "gpt-image-1", True),
        ("image_generate", "custom", "anything", True),
        ("video_input", "openai", "gpt-4o-video", True),
        ("video_input", "custom", "vision-vl", True),
        ("unknown", "openai", "gpt-5", False),
        ("image_ocr", "", "gpt-4o", False),
    ],
)
def test_media_support_matrix(capability: str, provider: str, model: str, expected: bool) -> None:
    assert web_app._media_model_supports(capability, provider, model) is expected


def test_media_model_normalization_and_options_require_explicit_media_models() -> None:
    media_models = {
        "openai": [
            "gpt-4o",
            {"name": "gpt-image-1", "capabilities": ["image_output", "image_output"]},
            {"model": ""},
            object(),
        ],
        "bad": "not-list",
    }
    normalized = web_app._normalize_media_models(media_models)
    assert normalized == {
        "openai": [
            {"model": "gpt-4o", "capabilities": []},
            {"model": "gpt-image-1", "capabilities": ["image_output"]},
        ]
    }
    assert web_app._media_capability_keys("video_input") == {"video", "video_input", "video_analysis"}
    assert web_app._media_selection_supported(
        "image_ocr",
        provider="custom",
        model="declared-only",
        media_models={"custom": [{"model": "declared-only", "capabilities": ["vision"]}]},
    )
    assert web_app._media_model_options(
        "image_ocr",
        provider_models={"openai": "gpt-4o,gpt-4o-mini"},
        media_models={},
        providers_enabled={"openai": True},
        media_providers_enabled={"openai": True},
        providers_configured={"openai": True},
        active_provider="openai",
        active_model="gpt-4o",
    ) == []
    assert web_app._media_model_options(
        "image_ocr",
        provider_models={"openai": "gpt-4o,gpt-4o-mini"},
        media_models={"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={"openai": True},
        providers_configured={"openai": True},
        active_provider="openai",
        active_model="gpt-4o",
    ) == [{"provider": "openai", "model": "gpt-4o", "label": "openai · gpt-4o"}]
    assert web_app._media_model_options(
        "image_generate",
        provider_models={},
        media_models={"openai": [{"model": "custom-image-capable-model", "capabilities": ["image_output"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={},
        providers_configured={"openai": True},
    ) == [{"provider": "openai", "model": "custom-image-capable-model", "label": "openai · custom-image-capable-model"}]
    assert web_app._media_capability_supported("image_input", "openai", "gpt-4o")
    assert web_app._media_capability_supported("audio_input", "openai", "whisper-1")
    assert web_app._media_capability_supported("image_output", "openai", "gpt-image-1")
    assert web_app._media_capability_supported("video_input", "openai", "gpt-4o")
    assert web_app._media_capability_supported("other", "openai", "gpt-4o") is False
    assert web_app._media_model_declares_capability(
        {"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        provider="openai",
        model="missing",
        capability="image_ocr",
    ) is False
    assert web_app._normalize_media_models("bad") == {}
    assert web_app._media_capability_keys("image_generate") == {"image_output", "image_generation", "image_generate"}
    assert web_app._media_model_options(
        "image_ocr",
        provider_models={"openai": "gpt-4o"},
        media_models={"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={"openai": True},
        providers_configured={"openai": False},
    ) == []


def test_log_buffer_and_runtime_history_sync(tmp_path) -> None:
    web_app._LOG_BUFFER.clear()
    handler = web_app._WebUILogBufferHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    record = logging.LogRecord("test.logger", logging.INFO, __file__, 1, "secret sk-test", (), None)
    handler.emit(record)
    assert web_app._logs_payload("memory")["entry_count"] == 1

    checkpoint = tmp_path / "runtime-store.json"
    checkpoint.write_text(
        '{"conversation_events":[{"event_type":"conversation.chat_turn","user_message":"hi"}]}',
        encoding="utf-8",
    )
    imported: list[list[dict]] = []
    runtime = SimpleNamespace(checkpoint_path=checkpoint)
    store = SimpleNamespace(import_runtime_chat_turns=lambda turns: imported.append(turns))
    web_app._sync_runtime_chat_history_to_store(runtime, store)
    web_app._sync_runtime_chat_history_to_store(runtime, store)
    assert imported == [[{"event_type": "conversation.chat_turn", "user_message": "hi"}]]

    missing_runtime = SimpleNamespace(checkpoint_path=tmp_path / "missing.json")
    web_app._sync_runtime_chat_history_to_store(missing_runtime, store)
    no_events = tmp_path / "no-events.json"
    no_events.write_text("{}", encoding="utf-8")
    web_app._sync_runtime_chat_history_to_store(SimpleNamespace(checkpoint_path=no_events), store)


def test_runtime_history_sync_reads_sqlite_chat_turns_without_full_runtime_load(tmp_path, monkeypatch) -> None:
    from nullion.runtime import bootstrap_persistent_runtime

    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.store.add_conversation_event(
        {
            "conversation_id": "web:fast-history",
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 28, tzinfo=UTC).isoformat(),
            "user_message": "hello history",
            "assistant_reply": "hello back",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": "web:fast-history",
            "event_type": "conversation.memory_capture",
            "created_at": datetime(2026, 5, 28, 0, 0, 1, tzinfo=UTC).isoformat(),
            "status": "skipped",
        }
    )
    runtime.checkpoint()
    imported: list[list[dict]] = []
    store = SimpleNamespace(import_runtime_chat_turns=lambda turns: imported.append(turns))
    monkeypatch.setattr(web_app, "load_runtime_store", lambda _path: (_ for _ in ()).throw(AssertionError("full load")))

    web_app._sync_runtime_chat_history_to_store(SimpleNamespace(checkpoint_path=checkpoint), store)

    assert len(imported) == 1
    assert [turn["event_type"] for turn in imported[0]] == ["conversation.chat_turn"]
    assert imported[0][0]["user_message"] == "hello history"


def test_permission_and_session_duration_helpers(monkeypatch) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert web_app._permission_memory_expires_at("forever", now=now) is None
    assert web_app._permission_memory_expires_at("15m", now=now) == now + timedelta(minutes=15)
    assert web_app._permission_memory_expires_at("30m", now=now) == now + timedelta(minutes=30)
    assert web_app._permission_memory_expires_at("1h", now=now) == now + timedelta(hours=1)
    assert web_app._permission_memory_expires_at("2h", now=now) == now + timedelta(hours=2)
    assert web_app._permission_memory_expires_at("4h", now=now) == now + timedelta(hours=4)
    assert web_app._permission_memory_expires_at("today", now=now) == now + timedelta(days=1)
    assert web_app._permission_memory_expires_at("30d", now=now) == now + timedelta(days=30)
    assert web_app._permission_memory_expires_at("week", now=now) == now + timedelta(days=7)
    assert web_app._permission_memory_expires_at("mystery", now=now) == now + timedelta(days=7)
    monkeypatch.setenv("NULLION_WEB_SESSION_ALLOW_DURATION", "15m")
    assert web_app._web_session_allow_duration_value() == "15m"
    assert "15" in web_app._web_session_allow_duration_label()
    assert web_app._web_session_allow_expires_at(now=now) == now + timedelta(minutes=15)


def test_chat_media_hydration_and_artifact_notice(monkeypatch, tmp_path) -> None:
    existing = tmp_path / "image.png"
    existing.write_text("x", encoding="utf-8")
    missing = tmp_path / "missing.png"

    def fake_descriptors(runtime, paths, *, principal_id=None):
        return [{"path": str(existing), "url": "/api/artifacts/id"}] if str(existing) in paths else []

    monkeypatch.setattr(web_app, "_web_artifact_descriptors", fake_descriptors)
    metadata = {
        "artifacts": [{"path": str(existing), "name": "image"}],
        "attachments": [{"path": str(missing), "name": "missing", "url": "stale"}],
    }
    hydrated = web_app._hydrate_chat_history_media(
        object(),
        [{"role": "bot", "text": "ok", "metadata": metadata}],
        principal_id="web:operator",
    )
    assert hydrated[0]["artifacts"][0]["url"] == "/api/artifacts/id"
    assert hydrated[0]["attachments"][0]["missing"] is True
    assert "did not offer it as a download" in web_app._web_artifact_delivery_notice("Done", ["x"], [])
    assert web_app._web_artifact_delivery_notice(f"Done\nMEDIA:{existing}", [str(existing)], [{"path": str(existing)}]) == "Done"
    listing_result = ToolResult(
        invocation_id="search",
        tool_name="file_search",
        status="completed",
        output={"matches": [str(existing)]},
    )
    assert web_app._web_artifact_delivery_notice(
        web_app._web_local_discovery_results_reply([str(existing)]),
        [str(existing)],
        [{"path": str(existing), "name": existing.name}],
        tool_results=[listing_result],
    ) == "Done — attached the requested image."
    assert web_app._strip_web_media_directive_lines(f"Done\nMEDIA\n{existing}") == "Done"
    hydrated_from_text = web_app._hydrate_chat_history_media(
        object(),
        [{"role": "bot", "text": f"Open this\nMEDIA:{existing}", "metadata": {}}],
        principal_id="web:operator",
    )
    assert hydrated_from_text[0]["text"] == "Open this"
    assert hydrated_from_text[0]["artifacts"][0]["url"] == "/api/artifacts/id"
    assert web_app._strip_web_media_directive_lines(
        f"Done\n**File:** [image.png](sandbox:artifacts/{existing.name})"
    ) == "Done"
    assert web_app._web_screenshot_reply("https://example.com") == "Done — captured a screenshot of https://example.com."
    failure = SimpleNamespace(url="https://example.com", error="")
    assert "did not complete" in web_app._web_screenshot_failure_reply(failure)


def test_web_artifact_notice_summarizes_multi_artifact_payloads(tmp_path) -> None:
    workbook = tmp_path / "report.xlsx"
    csv = tmp_path / "report.csv"
    pdf = tmp_path / "report.pdf"
    payloads = [
        {"path": str(workbook), "name": workbook.name},
        {"path": str(csv), "name": csv.name},
        {"path": str(pdf), "name": pdf.name},
    ]

    notice = web_app._web_artifact_delivery_notice(
        "Done - attached the requested XLSX workbook.",
        [str(workbook), str(csv), str(pdf)],
        payloads,
    )

    assert notice.startswith("Created and attached 3 artifacts:")
    assert "XLSX workbook" in notice
    assert "CSV file" in notice
    assert "PDF" in notice
    assert "report.xlsx" in notice
    assert "Done - attached" not in notice


def test_web_artifact_receipt_replaces_stale_suppressed_helper_text(tmp_path) -> None:
    html = tmp_path / "train_accessible_outdoor_day_trips.html"
    svg = tmp_path / "train_accessible_day_trips_summary.svg"
    html.write_text("<html><body>Day trips</body></html>", encoding="utf-8")
    svg.write_text("<svg><text>helper</text></svg>", encoding="utf-8")
    stale_text = (
        "Created the requested HTML file and.svg file:\n"
        f"- `{html.name}`\n"
        f"- `{svg.name}`"
    )

    cleaned = web_app.strip_unselected_artifact_references(
        stale_text,
        selected_paths=[str(html)],
        candidate_paths=[str(html), str(svg)],
    )

    assert cleaned is None
    assert web_app._web_artifact_delivery_receipt([{"path": str(html), "name": html.name}]) == (
        "Done — attached the requested HTML file."
    )


def test_web_delivery_artifacts_honor_scoped_html_over_reply_svg(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    html = principal_root / "train_accessible_outdoor_day_trips.html"
    svg = principal_root / "train_accessible_day_trips_summary.svg"
    html.write_text(
        "<html><body><h1>Trips</h1><img src='data:image/png;base64,AAAA'></body></html>",
        encoding="utf-8",
    )
    svg.write_text("<svg><text>support summary</text></svg>", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create the requested self-contained HTML deliverable.",
        reply=(
            "Created the requested HTML file and SVG support file:\n"
            f"- {html}\n"
            f"- {svg}"
        ),
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".html"],
                    "embedded_media_artifact_extensions": [".html"],
                },
            ),
            ToolResult("html", "file_write", "completed", {"path": str(html), "artifact_path": str(html)}),
            ToolResult("svg", "file_write", "completed", {"path": str(svg), "artifact_path": str(svg)}),
        ],
        principal_id="web:1",
        required_attachment_extensions=(".html",),
    )

    assert paths == [str(html)]


def test_web_completed_requested_artifacts_require_every_extension(tmp_path) -> None:
    workbook = tmp_path / "report.xlsx"
    csv = tmp_path / "report.csv"
    workbook.write_bytes(b"PK\x03\x04workbook")
    csv.write_text("name,value\nA,1\n", encoding="utf-8")
    results = [
        ToolResult(
            "sheet",
            "spreadsheet_create",
            "completed",
            {"artifact_paths": [str(workbook), str(csv)]},
        )
    ]

    assert web_app._web_completed_requested_artifact_paths_from_tool_results(
        results,
        requested_extensions=(".xlsx", ".csv", ".pdf"),
    ) == []


def test_web_bare_terminal_stdout_listing_is_discovery_not_delivery() -> None:
    result = ToolResult(
        "terminal",
        "terminal_exec",
        "completed",
        {"stdout": "stage_file_search_delivery_guard.pdf\n", "stderr": "", "exit_code": 0},
    )

    assert web_app._web_terminal_stdout_is_path_listing(result.output)
    assert web_app._web_tool_results_are_local_discovery_listing([result])


def test_web_delivery_artifact_paths_keep_scoped_multi_format_package_when_format_guess_is_single(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    workbook = principal_root / "package.xlsx"
    csv_file = principal_root / "package.csv"
    docx = principal_root / "package.docx"
    pptx = principal_root / "package.pptx"
    pdf = principal_root / "package.pdf"
    html = principal_root / "package.html"
    source_zip = principal_root / "source.zip"
    receipt = principal_root / "receipt.txt"
    workbook.write_bytes(b"PK\x03\x04workbook")
    csv_file.write_text("name,value\nA,1\n", encoding="utf-8")
    docx.write_bytes(b"PK\x03\x04docx")
    pptx.write_bytes(b"PK\x03\x04pptx")
    pdf.write_bytes(b"%PDF-1.4\n")
    html.write_text(
        "<!doctype html><html><body><h1>Package</h1><p>Structured package delivery fixture with enough body text.</p></body></html>",
        encoding="utf-8",
    )
    source_zip.write_bytes(b"PK\x03\x04source")
    receipt.write_text("ok", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )
    tool_results = [
        ToolResult(
            "scope",
            "request_tool_scope",
            "completed",
            {"artifact_extensions": [".xlsx", ".csv", ".docx", ".pptx", ".pdf", ".html"]},
        ),
        ToolResult(
            "download",
            "file_download",
            "completed",
            {
                "artifact_path": str(source_zip),
                "artifact_paths": [str(source_zip)],
                "artifact_descriptors": [{"path": str(source_zip), "role": "source", "kind": "download"}],
            },
        ),
        ToolResult(
            "sheet",
            "spreadsheet_create",
            "completed",
            {
                "artifact_path": str(workbook),
                "artifact_paths": [str(workbook)],
                "artifact_descriptors": [{"path": str(workbook), "role": "deliverable", "kind": "workbook"}],
            },
        ),
        ToolResult("csv", "file_write", "completed", {"path": str(csv_file), "artifact_path": str(csv_file)}),
        ToolResult("docx", "document_create", "completed", {"artifact_path": str(docx)}),
        ToolResult("pptx", "presentation_create", "completed", {"artifact_path": str(pptx)}),
        ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(pdf)}),
        ToolResult("html", "html_create", "completed", {"artifact_path": str(html)}),
        ToolResult("receipt", "file_write", "completed", {"path": str(receipt), "artifact_path": str(receipt)}),
    ]

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create the requested artifact package.",
        reply="Created the package.",
        tool_results=tool_results,
        principal_id="web:1",
        requested_extension=".xlsx",
        required_attachment_extensions=(".xlsx", ".csv", ".docx", ".pptx", ".pdf", ".html"),
    )

    assert paths == [str(workbook), str(csv_file), str(docx), str(pptx), str(pdf), str(html)]


def test_web_fetch_attachment_helpers(monkeypatch, tmp_path) -> None:
    completed = ToolResult(
        invocation_id="1",
        tool_name="web_fetch",
        status="completed",
        output={"url": "https://example.com/a", "body": "<html><head></head><body><script>x()</script>Hello</body></html>"},
    )
    failed = ToolResult(invocation_id="2", tool_name="web_fetch", status="failed", output={"body": "no"})
    assert web_app._requested_web_attachment_extension("https://example.com output=page.html") == ".html"
    assert web_app._requested_web_attachment_extension("example.com output=output.txt") == ".txt"
    assert web_app._requested_web_attachment_extension("example.com artifact=report.txt") == ".txt"
    assert web_app._requested_web_attachment_extension("fetch https://example.com") is None
    assert web_app._requested_web_attachment_extension("send example.com") is None
    assert web_app._requested_web_attachment_extension("hello") is None
    assert web_app._latest_completed_web_tool_result([failed, completed], tool_name="web_fetch") is completed
    assert web_app._latest_completed_web_tool_result([failed], tool_name="web_fetch") is None
    assert web_app._web_fetch_body_for_attachment(completed, extension=".txt").startswith("<html")
    assert web_app._web_fetch_body_for_attachment(
        ToolResult(invocation_id="3", tool_name="web_fetch", status="completed", output={"html": "<p>x</p>"}),
        extension=".html",
    ) == "<p>x</p>"
    assert web_app._web_fetch_body_for_attachment(
        ToolResult(invocation_id="4", tool_name="web_fetch", status="completed", output={}),
        extension=".txt",
    ) is None
    terminal = ToolResult(
        invocation_id="5",
        tool_name="terminal_exec",
        status="completed",
        output={"stdout": "curl output\n", "stderr": "", "exit_code": 0},
    )
    assert web_app._terminal_exec_output_for_attachment(terminal, extension=".txt") == "curl output\n"
    assert web_app._terminal_exec_output_for_attachment(
        ToolResult(
            invocation_id="6",
            tool_name="terminal_exec",
            status="completed",
            output={"stdout": "", "stderr": "warning\n", "exit_code": 0},
        ),
        extension=".txt",
    ) == "warning\n"
    safe_html = web_app._viewable_static_html("<html><body><script>x()</script>Hi</body></html>", source_url="https://example.com")
    assert "<script" not in safe_html
    assert '<base href="https://example.com">' in safe_html
    assert "<pre>" in web_app._viewable_static_html("plain < text")

    paths = web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt="fetch https://example.com page.html",
        tool_results=[completed],
    )
    assert len(paths) == 1
    html_path = Path(paths[0])
    assert html_path.suffix == ".html"
    assert html_path.is_relative_to(tmp_path / "nullion-data")
    assert "scripts disabled" in html_path.read_text(encoding="utf-8")
    paths = web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt="curl google.com and save output.txt",
        tool_results=[terminal],
    )
    assert len(paths) == 1
    txt_path = Path(paths[0])
    assert txt_path.suffix == ".txt"
    assert txt_path.is_relative_to(tmp_path / "nullion-data")
    assert txt_path.read_text(encoding="utf-8") == "curl output\n"
    assert web_app._materialize_fetch_artifact_for_web(object(), prompt="hello", tool_results=[completed]) == []
    assert web_app._materialize_fetch_artifact_for_web(object(), prompt="save https://example.com page.html", tool_results=[failed]) == []
    assert web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt="save https://example.com page.html",
        tool_results=[ToolResult(invocation_id="5", tool_name="web_fetch", status="completed", output={})],
    ) == []


def test_materialize_fetch_artifact_reuses_existing_requested_pdf(monkeypatch, tmp_path) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    existing_pdf = artifact_root / "launch_artifact_boundary_smoke_test.pdf"
    existing_pdf.write_bytes(b"%PDF-1.4\n%existing\n")

    def fail_fetch_artifact(*args, **kwargs):
        raise AssertionError("fetch-artifact fallback should not run when a matching PDF already exists")

    monkeypatch.setattr(web_app, "run_fetch_artifact_workflow", fail_fetch_artifact)

    paths = web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt="Create a simple pdf file with the words launch artifact boundary smoke test.",
        tool_results=[
            ToolResult(
                invocation_id="pdf-1",
                tool_name="pdf_create",
                status="completed",
                output={"artifact_path": str(existing_pdf), "artifact_paths": [str(existing_pdf)]},
            )
        ],
        requested_extension=".pdf",
    )

    assert paths == [str(existing_pdf)]


def test_web_artifact_descriptors_cover_deduplication(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, media_root):
        root.mkdir()
    artifact = runtime_root / "artifact.txt"
    artifact.write_text("hello", encoding="utf-8")
    duplicate = principal_root / "artifact.txt"
    duplicate.write_text("hello", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=tmp_path / "files", media=media_root),
    )
    payloads = web_app._web_artifact_descriptors(object(), [str(artifact), str(duplicate)], principal_id="web")
    assert len(payloads) == 2
    assert payloads[0]["url"].startswith("/api/artifacts/")


def test_web_artifact_descriptors_derive_workspace_artifact_root_from_absolute_path(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    wrong_workspace_base = tmp_path / "wrong-home" / "workspaces"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root, wrong_workspace_base):
        root.mkdir(parents=True)
    artifact = (
        tmp_path
        / "stage-home"
        / "workspaces"
        / "workspace_admin"
        / "artifacts"
        / "stage_archive_transform_field_notes.xlsx"
    )
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"PK\x03\x04fake workbook")

    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(web_app, "workspace_storage_base", lambda: wrong_workspace_base)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    payloads = web_app._web_artifact_descriptors(object(), [str(artifact)], principal_id="web:1")

    assert len(payloads) == 1
    assert payloads[0]["name"] == "stage_archive_transform_field_notes.xlsx"
    assert payloads[0]["path"] == str(artifact)


def test_web_recover_completed_requested_artifact_from_tool_results(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    workbook = runtime_root / "image_manifest.xlsx"
    plain_workbook = runtime_root / "plain_manifest.xlsx"
    with zipfile.ZipFile(workbook, "w") as package:
        package.writestr("xl/workbook.xml", "<workbook/>")
        package.writestr("xl/media/image1.png", b"\x89PNG\r\nmedia")
    with zipfile.ZipFile(plain_workbook, "w") as package:
        package.writestr("xl/workbook.xml", "<workbook/>")

    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    tool_results = [
        ToolResult(
            invocation_id="scope",
            tool_name="request_tool_scope",
            status="completed",
            output={
                "artifact_extensions": [".xlsx"],
                "embedded_media_artifact_extensions": [".xlsx"],
            },
        ),
        ToolResult(
            invocation_id="sheet",
            tool_name="spreadsheet_create",
            status="completed",
            output={"artifact_path": str(workbook), "artifact_paths": [str(workbook)]},
        ),
        ToolResult(
            invocation_id="plain",
            tool_name="spreadsheet_create",
            status="completed",
            output={"artifact_path": str(plain_workbook), "artifact_paths": [str(plain_workbook)]},
        ),
    ]

    paths, artifacts = web_app._web_recover_completed_requested_artifacts(
        object(),
        principal_id="web:1",
        tool_results=tool_results,
        required_attachment_extensions=(),
        required_embedded_media_extensions=(),
    )

    assert paths == [str(workbook)]
    assert artifacts[0]["name"] == "image_manifest.xlsx"
    assert artifacts[0]["url"].startswith("/api/artifacts/")


def test_web_recovered_artifact_still_requires_complete_extension_set(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    docx_path = runtime_root / "partial_pack.docx"
    pdf_path = runtime_root / "partial_pack.pdf"
    with zipfile.ZipFile(docx_path, "w") as package:
        package.writestr("word/document.xml", "<document/>")
    pdf_path.write_bytes(b"%PDF-1.4\n%partial\n")

    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    scope_result = ToolResult(
        invocation_id="scope",
        tool_name="request_tool_scope",
        status="completed",
        output={"artifact_extensions": [".docx", ".pdf"]},
    )
    docx_result = ToolResult(
        invocation_id="docx",
        tool_name="document_create",
        status="completed",
        output={"artifact_path": str(docx_path), "artifact_paths": [str(docx_path)]},
    )
    runtime = SimpleNamespace(store=RuntimeStore())

    paths, artifacts, text, fulfilled = web_app._web_apply_recovered_completed_requested_artifacts(
        runtime,
        conversation_id="web:1",
        user_message="Create a DOCX and PDF pack.",
        final_text="Created the document.",
        tool_results=[scope_result, docx_result],
        required_attachment_extensions=(),
        required_embedded_media_extensions=(),
    )

    assert paths == [str(docx_path)]
    assert [artifact["name"] for artifact in artifacts] == ["partial_pack.docx"]
    assert fulfilled is False
    assert "couldn't attach all of the requested files" in text

    pdf_result = ToolResult(
        invocation_id="pdf",
        tool_name="pdf_create",
        status="completed",
        output={"artifact_path": str(pdf_path), "artifact_paths": [str(pdf_path)]},
    )
    paths, artifacts, text, fulfilled = web_app._web_apply_recovered_completed_requested_artifacts(
        runtime,
        conversation_id="web:1",
        user_message="Create a DOCX and PDF pack.",
        final_text="Created the package.",
        tool_results=[scope_result, docx_result, pdf_result],
        required_attachment_extensions=(),
        required_embedded_media_extensions=(),
    )

    assert paths == [str(docx_path), str(pdf_path)]
    assert {artifact["name"] for artifact in artifacts} == {"partial_pack.docx", "partial_pack.pdf"}
    assert fulfilled is True


def test_web_delivery_artifact_paths_include_plain_pdf_reply_path(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    txt_artifact = principal_root / "fresh_news_briefing_2026-05-01.txt"
    pdf_artifact = principal_root / "fresh_news_briefing_2026-05-01.pdf"
    workspace_file = files_root / "cnn_headlines.txt"
    txt_artifact.write_text("staging text", encoding="utf-8")
    pdf_artifact.write_bytes(b"%PDF-1.4\n%test\n")
    workspace_file.write_text("headline", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="and then attach report.pdf",
        reply=f"Download / attachment path:\n{pdf_artifact}",
        tool_results=[
            ToolResult(
                invocation_id="write-txt",
                tool_name="file_write",
                status="completed",
                output={"path": str(txt_artifact)},
            )
        ],
        principal_id="web:1",
    )

    assert paths == [str(pdf_artifact)]

    html_artifact = principal_root / "fresh_news_briefing_2026-05-01.html"
    image_artifact = principal_root / "fresh_news_briefing_2026-05-01.png"
    html_artifact.write_text("<html>draft</html>", encoding="utf-8")
    image_artifact.write_bytes(b"\x89PNG\r\n")
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="make a polished deliverable",
        reply="Created the requested file.",
        artifact_paths=[str(html_artifact), str(image_artifact), str(pdf_artifact)],
        principal_id="web:1",
        requested_extension=".pdf",
    ) == [str(pdf_artifact)]

    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="[Manual scheduled task run: Peak Design shipment tracker] Track shipment status.",
        reply="I created the PDF attachment successfully:\nPDF: peak_design_shipment_update.pdf",
        artifact_paths=[str(pdf_artifact)],
        principal_id="web:1",
    ) == [str(pdf_artifact)]

    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="attach report.txt",
        reply=f"ARTIFACT:{workspace_file}",
        principal_id="web:1",
    ) == [str(workspace_file)]

    listing_fixture = principal_root / "stage_file_search_delivery_guard.pdf"
    listing_fixture.write_bytes(b"%PDF-1.4\n% existing fixture\n")
    listing_results = [
        ToolResult(
            invocation_id="find-1",
            tool_name="terminal_exec",
            status="completed",
            output={"stdout": f"{listing_fixture}\n", "stderr": ""},
        )
    ]
    assert web_app._materialize_fetch_artifact_for_web(
        object(),
        prompt=f"Find the pdf file named {listing_fixture.name} and do not attach it.",
        tool_results=listing_results,
        principal_id="web:1",
        requested_extension=".pdf",
    ) == []
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt=f"Find the pdf file named {listing_fixture.name} and do not attach it.",
        reply=f"I found 1 file:\n- {listing_fixture.name}",
        tool_results=listing_results,
        principal_id="web:1",
        requested_extension=".pdf",
    ) == []

    screenshot_artifact = principal_root / "screenshot-internal.png"
    screenshot_artifact.write_bytes(b"\x89PNG\r\n")
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="find movie listings for tomorrow",
        reply="Here are the movie listings I found.",
        tool_results=[
            ToolResult(
                invocation_id="shot",
                tool_name="browser_screenshot",
                status="completed",
                output={
                    "path": str(screenshot_artifact),
                    "artifact_path": str(screenshot_artifact),
                    "artifact_paths": [str(screenshot_artifact)],
                },
            ),
            ToolResult(
                invocation_id="extract",
                tool_name="browser_extract_text",
                status="completed",
                output={"text": "Movie listings and showtimes."},
            ),
        ],
        principal_id="web:1",
    ) == []
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="stage screenshot artifact request",
        reply="Screenshot captured.",
        tool_results=[
            ToolResult(
                invocation_id="shot",
                tool_name="browser_screenshot",
                status="completed",
                output={
                    "path": str(screenshot_artifact),
                    "artifact_path": str(screenshot_artifact),
                    "artifact_paths": [str(screenshot_artifact)],
                },
            )
        ],
        principal_id="web:1",
    ) == [str(screenshot_artifact)]

    described_workbook = principal_root / "stage_archive_transform_field_notes.xlsx"
    described_source_zip = principal_root / "field-notes-images.zip"
    described_workbook.write_bytes(b"PK\x03\x04fake workbook")
    described_source_zip.write_bytes(b"PK\x03\x04fake zip")
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="structured archive transform",
        reply="Created the requested spreadsheet.",
        tool_results=[
            ToolResult(
                invocation_id="extract",
                tool_name="archive_extract",
                status="completed",
                output={
                    "artifact_descriptors": [
                        {
                            "path": str(described_source_zip),
                            "role": "source",
                            "extension": ".zip",
                        }
                    ],
                },
            ),
            ToolResult(
                invocation_id="xlsx",
                tool_name="spreadsheet_create",
                status="completed",
                output={
                    "artifact_path": str(described_workbook),
                    "artifact_descriptors": [
                        {
                            "path": str(described_workbook),
                            "role": "deliverable",
                            "extension": ".xlsx",
                        }
                    ],
                },
            ),
        ],
        principal_id="web:1",
    ) == [str(described_workbook)]
    assert web_app._web_preferred_deliverable_artifact_paths(
        object(),
        principal_id="web:1",
        tool_results=[
            ToolResult(
                invocation_id="extract",
                tool_name="archive_extract",
                status="completed",
                output={
                    "artifact_descriptors": [
                        {
                            "path": str(described_source_zip),
                            "role": "source",
                            "extension": ".zip",
                        }
                    ],
                },
            ),
            ToolResult(
                invocation_id="xlsx",
                tool_name="spreadsheet_create",
                status="completed",
                output={
                    "artifact_path": str(described_workbook),
                    "artifact_descriptors": [
                        {
                            "path": str(described_workbook),
                            "role": "deliverable",
                            "extension": ".xlsx",
                        }
                    ],
                },
            ),
        ],
    ) == [str(described_workbook)]

    note = principal_root / "hermes_model_update_note.txt"
    note.write_text("Updated model.default to gpt-5.4.\n", encoding="utf-8")
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="structured config edit continuation",
        reply=f"Done -- attached the requested text file.\n\nMEDIA:{note}",
        tool_results=[
            ToolResult(
                invocation_id="scope",
                tool_name="request_tool_scope",
                status="completed",
                output={"artifact_extensions": [], "embedded_media_artifact_extensions": []},
            ),
            ToolResult(
                invocation_id="write-note",
                tool_name="file_write",
                status="completed",
                output={"path": str(note)},
            ),
        ],
        principal_id="web:1",
    ) == []

    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="structured artifact request",
        reply=f"Done.\n\nMEDIA:{note}",
        tool_results=[
            ToolResult(
                invocation_id="scope",
                tool_name="request_tool_scope",
                status="completed",
                output={"artifact_extensions": [".txt"]},
            ),
            ToolResult(
                invocation_id="write-note",
                tool_name="file_write",
                status="completed",
                output={"path": str(note)},
            ),
        ],
        principal_id="web:1",
    ) == [str(note)]

    admin_artifacts = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    web_artifacts = tmp_path / "workspaces" / "web_1" / "artifacts"
    admin_artifacts.mkdir(parents=True)
    web_artifacts.mkdir(parents=True)
    workbook = admin_artifacts / "comparison.xlsx"
    csv_file = admin_artifacts / "comparison.csv"
    pdf_file = admin_artifacts / "summary.pdf"
    workbook.write_bytes(b"PK\x03\x04fake workbook")
    csv_file.write_text("name,value\nA,1\n", encoding="utf-8")
    pdf_file.write_bytes(b"%PDF-1.4\n")
    monkeypatch.setattr(web_app, "workspace_storage_base", lambda: tmp_path / "workspaces")
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: web_artifacts)

    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create a workbook, CSV, and PDF.",
        reply=(
            "I couldn't create the requested file with `spreadsheet create`: "
            "This turn requires the spreadsheet artifact to contain embedded image/screenshot media."
        ),
        tool_results=[
            ToolResult(
                invocation_id="scope",
                tool_name="request_tool_scope",
                status="completed",
                output={"artifact_extensions": [".xlsx", ".csv", ".pdf"]},
            ),
            ToolResult(
                invocation_id="xlsx",
                tool_name="spreadsheet_create",
                status="completed",
                output={"artifact_path": str(workbook)},
            ),
            ToolResult(
                invocation_id="csv",
                tool_name="file_write",
                status="completed",
                output={"path": str(csv_file)},
            ),
            ToolResult(
                invocation_id="pdf",
                tool_name="pdf_create",
                status="completed",
                output={"artifact_path": str(pdf_file)},
            ),
        ],
        principal_id="web:1",
        required_attachment_extensions=(".xlsx", ".csv", ".pdf"),
    ) == [str(workbook), str(csv_file), str(pdf_file)]

    html_file = admin_artifacts / "stage_latency_probe.html"
    scoped_csv_file = admin_artifacts / "stage_latency_probe.csv"
    scoped_pdf_file = admin_artifacts / "stage_latency_probe_note.pdf"
    html_file.write_text(
        "<!doctype html><html><head><title>Stage latency</title></head>"
        "<body><h1>Stage latency</h1><p>Artifact delivery proof.</p></body></html>",
        encoding="utf-8",
    )
    scoped_csv_file.write_text("name,value\nAlpha,1\n", encoding="utf-8")
    scoped_pdf_file.write_bytes(b"%PDF-1.4\n%stage latency\n")
    scoped_tool_results = [
        ToolResult(
            invocation_id="scope-csv-html",
            tool_name="request_tool_scope",
            status="completed",
            output={"artifact_extensions": [".csv", ".html"]},
        ),
        ToolResult(
            invocation_id="pdf",
            tool_name="pdf_create",
            status="completed",
            output={
                "artifact_path": str(scoped_pdf_file),
                "artifact_paths": [str(scoped_pdf_file)],
                "path": str(scoped_pdf_file),
            },
        ),
        ToolResult(
            invocation_id="csv",
            tool_name="file_write",
            status="completed",
            output={
                "artifact_path": str(scoped_csv_file),
                "artifact_paths": [str(scoped_csv_file)],
                "path": str(scoped_csv_file),
            },
        ),
        ToolResult(
            invocation_id="html",
            tool_name="file_write",
            status="completed",
            output={
                "artifact_path": str(html_file),
                "artifact_paths": [str(html_file)],
                "path": str(html_file),
            },
        ),
    ]
    scoped_reply = (
        "Done — attached the requested files.\n\n"
        f"MEDIA:{html_file}\n"
        f"MEDIA:{scoped_csv_file}\n"
        f"MEDIA:{scoped_pdf_file}"
    )
    scoped_paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create a CSV, an HTML summary, and a PDF note. Attach all three.",
        reply=scoped_reply,
        tool_results=scoped_tool_results,
        principal_id="web:1",
        required_attachment_extensions=(".csv", ".html"),
    )

    assert scoped_paths == [str(html_file), str(scoped_csv_file), str(scoped_pdf_file)]
    scoped_artifacts = web_app._web_artifact_descriptors(object(), scoped_paths, principal_id="web:1")
    scoped_notice = web_app._web_artifact_delivery_notice(
        scoped_reply,
        scoped_paths,
        scoped_artifacts,
        tool_results=scoped_tool_results,
    )
    final_text, fulfilled = web_app._enforce_web_response_fulfillment(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        conversation_id="web:1",
        user_message="Create a CSV, an HTML summary, and a PDF note. Attach all three.",
        reply=scoped_notice,
        tool_results=scoped_tool_results,
        artifact_paths=scoped_paths,
        artifact_count=len(scoped_artifacts),
        required_attachment_extensions=(".csv", ".html"),
    )

    assert fulfilled is True
    assert "MEDIA:" not in final_text
    assert "couldn't attach" not in final_text


def test_web_delivery_keeps_explicit_sandbox_html_companion(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    poster_pdf = principal_root / "stage_format_qa_night_market_poster.pdf"
    memo_pdf = principal_root / "stage_format_qa_night_market_memo.pdf"
    poster_html = principal_root / "stage_format_qa_night_market_poster.html"
    poster_pdf.write_bytes(b"%PDF-1.4\n%poster\n")
    memo_pdf.write_bytes(b"%PDF-1.4\n%memo\n")
    poster_html.write_text(
        "<!doctype html><html><body><h1>STAGE FORMAT QA</h1></body></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    prompt = (
        "Create a polished visual design package. Deliver a poster PDF, a memo PDF, "
        "and a self-contained HTML version."
    )
    reply = (
        "Files created:\n"
        "1. [Poster PDF](sandbox:stage_format_qa_night_market_poster.pdf)\n"
        "2. [Memo PDF](sandbox:stage_format_qa_night_market_memo.pdf)\n"
        "3. [HTML poster](sandbox:stage_format_qa_night_market_poster.html)"
    )
    tool_results = [
        ToolResult(
            invocation_id="poster-pdf",
            tool_name="pdf_create",
            status="completed",
            output={"artifact_path": str(poster_pdf), "artifact_paths": [str(poster_pdf)]},
        ),
        ToolResult(
            invocation_id="memo-pdf",
            tool_name="pdf_create",
            status="completed",
            output={"artifact_path": str(memo_pdf), "artifact_paths": [str(memo_pdf)]},
        ),
        ToolResult(
            invocation_id="html-write",
            tool_name="file_write",
            status="completed",
            output={"path": str(poster_html)},
        ),
    ]

    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt=prompt,
        reply=reply,
        tool_results=tool_results,
        principal_id="web:1",
    ) == [str(poster_pdf), str(memo_pdf), str(poster_html)]
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt=prompt,
        reply=reply,
        tool_results=tool_results,
        principal_id="web:1",
        requested_extension=".pdf",
    ) == [str(poster_pdf), str(memo_pdf)]


def test_web_delivery_artifact_paths_prefers_email_attachment_over_confirmation(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    sent_image = principal_root / "dog_for_email.png"
    confirmation = principal_root / "email_delivery_confirmation_2026-05-15_2111.txt"
    sent_image.write_bytes(b"\x89PNG\r\nemail-image")
    confirmation.write_text("Email sent to recipient@example.com.", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    tool_results = [
        ToolResult(
            invocation_id="email-1",
            tool_name="email_send",
            status="completed",
            output={
                "to": ["recipient@example.com"],
                "subject": "Hi",
                "attachment_paths": [str(sent_image)],
                "provider_id": "maton:gmail",
                "status_code": 200,
                "json": {"id": "msg-1"},
            },
        ),
        ToolResult(
            invocation_id="confirm-1",
            tool_name="file_write",
            status="completed",
            output={"path": str(confirmation)},
        ),
    ]
    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Send an email to recipient@example.com tell him hi and send an image of a dog",
        reply=f"Created and attached the delivery confirmation file.\n\nMEDIA:{confirmation}",
        tool_results=tool_results,
        principal_id="web:1",
    )

    assert paths == [str(sent_image)]
    notice = web_app._web_artifact_delivery_notice(
        f"Created and attached the delivery confirmation file.\n\nMEDIA:{confirmation}",
        paths,
        [{"path": str(sent_image), "name": sent_image.name, "media_type": "image/png"}],
        tool_results=tool_results,
    )
    assert "✉️ Email sent to recipient@example.com with subject \"Hi\"" in notice
    assert "status 200" not in notice
    assert "message id" not in notice
    assert "msg-1" not in notice
    assert "Done — attached the requested image." in notice
    assert "confirmation" not in notice.lower()
    assert "MEDIA:" not in notice


def test_web_delivery_artifact_paths_keeps_multi_format_email_attachments_when_format_guess_is_single(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    attachments = [
        principal_root / "stage_qa_test_dataset.csv",
        principal_root / "stage_qa_test_workbook.xlsx",
        principal_root / "stage_qa_test_summary.pdf",
        principal_root / "stage_qa_test_package.html",
    ]
    attachments[0].write_text("name,value\nAlpha,1\n", encoding="utf-8")
    attachments[1].write_bytes(b"PK\x03\x04xlsx")
    attachments[2].write_bytes(b"%PDF-1.4\n%stage qa\n")
    attachments[3].write_text(
        "<!doctype html><html><body><h1>STAGE QA TEST</h1><p>Complex package attachment.</p></body></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    tool_results = [
        ToolResult(
            invocation_id="email-1",
            tool_name="email_send",
            status="completed",
            output={
                "to": ["recipient@example.com"],
                "subject": "Nullion stage deep QA",
                "attachment_paths": [str(path) for path in attachments],
            },
        ),
    ]

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt=(
            "Create a stage QA email package and send it to recipient@example.com. "
            "Attach a generated CSV, XLSX workbook, PDF summary, and one HTML file."
        ),
        reply='Email sent to recipient@example.com with subject "Nullion stage deep QA".',
        tool_results=tool_results,
        principal_id="web:1",
        requested_extension=".csv",
    )

    assert paths == [str(path) for path in attachments]
    payloads = [{"path": str(path), "name": path.name, "media_type": ""} for path in attachments]
    notice = web_app._web_artifact_delivery_notice(
        'Email sent to recipient@example.com with subject "Nullion stage deep QA".',
        paths,
        payloads,
        tool_results=tool_results,
    )
    assert "Email sent to recipient@example.com" in notice
    assert "Done — attached the requested attachments." in notice
    assert "text file" not in notice


def test_web_delivery_artifact_paths_keeps_email_attachments_authoritative_over_source_media(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    source_image = principal_root / "stage_multi_tool_qa_met_image.jpg"
    workbook = principal_root / "stage_multi_tool_qa_culture_itinerary_benchmark.xlsx"
    pdf = principal_root / "stage_multi_tool_qa_culture_itinerary_summary.pdf"
    confirmation = principal_root / "email_delivery_confirmation_2026-06-16_1945.txt"
    source_image.write_bytes(b"\xff\xd8\xffsource")
    workbook.write_bytes(b"PK\x03\x04xlsx")
    pdf.write_bytes(b"%PDF-1.4\n%stage qa\n")
    confirmation.write_text("Email sent to recipient@example.com.", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt=(
            "Send the itinerary benchmark email to recipient@example.com. "
            "Attach the PDF summary and XLSX workbook."
        ),
        reply='Email sent to recipient@example.com with subject "Nullion stage multi tool QA".',
        tool_results=[
            ToolResult(
                "source-image",
                "file_download",
                "completed",
                {
                    "artifact_path": str(source_image),
                    "artifact_paths": [str(source_image)],
                    "artifact_descriptors": [
                        {"path": str(source_image), "role": "source"}
                    ],
                },
            ),
            ToolResult(
                "workbook",
                "spreadsheet_create",
                "completed",
                {
                    "artifact_path": str(workbook),
                    "artifact_paths": [str(workbook)],
                    "artifact_descriptors": [
                        {"path": str(workbook), "role": "deliverable"}
                    ],
                },
            ),
            ToolResult(
                "pdf",
                "pdf_create",
                "completed",
                {
                    "artifact_path": str(pdf),
                    "artifact_paths": [str(pdf)],
                    "artifact_descriptors": [
                        {"path": str(pdf), "role": "deliverable"}
                    ],
                },
            ),
            ToolResult(
                "email",
                "email_send",
                "completed",
                {
                    "to": ["recipient@example.com"],
                    "subject": "Nullion stage multi tool QA",
                    "attachment_paths": [str(workbook), str(pdf)],
                },
            ),
            ToolResult("confirmation", "file_write", "completed", {"path": str(confirmation)}),
        ],
        principal_id="web:1",
        required_attachment_extensions=(".xlsx", ".pdf"),
    )

    assert paths == [str(workbook), str(pdf)]


def test_web_delivery_artifact_paths_suppresses_email_read_attachment_siblings(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    read_pdf = principal_root / "stage_multi_tool_qa_culture_itinerary_executive_-7df7e18ff4e8.pdf"
    stale_sibling = principal_root / "stage_multi_tool_qa_culture_itinerary_executive_summary_resend.pdf"
    read_pdf.write_bytes(b"%PDF-1.4\n% attachment read source\n")
    stale_sibling.write_bytes(b"%PDF-1.4\n% stale source sibling\n")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Anything urgent in my email overnight",
        reply=f"Done — attached the requested PDF.\n\nMEDIA:{stale_sibling}",
        tool_results=[
            ToolResult(
                "search",
                "email_search",
                "completed",
                {"items": [{"id": "msg-1", "subject": "Stage QA culture itinerary"}]},
            ),
            ToolResult(
                "read",
                "email_read",
                "completed",
                {
                    "message": {
                        "id": "msg-1",
                        "subject": "Stage QA culture itinerary",
                        "attachments": [
                            {"filename": read_pdf.name, "attachmentId": "pdf-1"},
                            {"filename": stale_sibling.name, "attachmentId": "pdf-2"},
                        ],
                        "next_tool_for_attachment": "email_attachment_read",
                    }
                },
            ),
            ToolResult(
                "attachment",
                "email_attachment_read",
                "completed",
                {
                    "artifact_path": str(read_pdf),
                    "artifact_paths": [str(read_pdf)],
                },
            ),
        ],
        artifact_paths=[str(stale_sibling)],
        principal_id="web:email-urgent",
    )

    assert paths == []


def test_web_delivery_replaces_plain_email_workbook_with_recent_media_workbook(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    plain_workbook = principal_root / "culture_itinerary_benchmark.xlsx"
    media_workbook = principal_root / "culture_itinerary_benchmark_workbook.xlsx"
    pdf = principal_root / "culture_itinerary_executive_summary.pdf"
    with zipfile.ZipFile(plain_workbook, "w") as package:
        package.writestr("xl/workbook.xml", "<workbook/>")
    with zipfile.ZipFile(media_workbook, "w") as package:
        package.writestr("xl/workbook.xml", "<workbook/>")
        package.writestr("xl/media/image1.png", b"\x89PNG\r\nmedia")
    pdf.write_bytes(b"%PDF-1.4\n%stage qa\n")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )
    store = RuntimeStore()
    conversation_id = "web:stage-media-email"
    store.add_conversation_event(
        {
            "event_type": "conversation.chat_turn",
            "conversation_id": conversation_id,
            "tool_results": [
                {
                    "tool_name": "spreadsheet_create",
                    "status": "completed",
                    "output": {
                        "artifact_path": str(media_workbook),
                        "artifact_paths": [str(media_workbook)],
                        "embedded_images": [str(media_root / "source.png")],
                    },
                }
            ],
        }
    )
    runtime = SimpleNamespace(store=store)
    tool_results = [
        ToolResult(
            "scope",
            "request_tool_scope",
            "completed",
            {
                "artifact_extensions": [".xlsx", ".pdf"],
                "embedded_media_artifact_extensions": [".xlsx"],
            },
        ),
        ToolResult(
            "email",
            "email_send",
            "completed",
            {
                "to": ["recipient@example.com"],
                "subject": "Nullion stage multi tool QA",
                "attachment_paths": [str(plain_workbook), str(pdf)],
            },
        ),
    ]

    paths = web_app._web_delivery_artifact_paths(
        runtime,
        prompt="Send the existing media workbook and PDF summary.",
        reply='Email sent to recipient@example.com with subject "Nullion stage multi tool QA".',
        tool_results=tool_results,
        principal_id=conversation_id,
        required_attachment_extensions=(".xlsx", ".pdf"),
    )

    assert paths == [str(media_workbook), str(pdf)]


def test_web_delivery_uses_required_extensions_and_filters_sidecars(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    xlsx_path = principal_root / "stage_required_workbook.xlsx"
    csv_path = principal_root / "stage_required_rows.csv"
    pdf_path = principal_root / "stage_required_summary.pdf"
    html_path = principal_root / "stage_required_page.html"
    sidecar_path = principal_root / "stage_required_notes.txt"
    xlsx_path.write_bytes(b"PK\x03\x04xlsx")
    csv_path.write_text("name,value\nAlpha,1\n", encoding="utf-8")
    pdf_path.write_bytes(b"%PDF-1.4\n%summary\n")
    html_path.write_text(
        "<!doctype html><html><body><h1>Required package</h1></body></html>",
        encoding="utf-8",
    )
    sidecar_path.write_text("internal package note", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    tool_results = [
        ToolResult(
            invocation_id="scope",
            tool_name="request_tool_scope",
            status="completed",
            output={"artifact_extensions": [".xlsx", ".csv", ".pdf", ".html"]},
        ),
        ToolResult(
            invocation_id="xlsx",
            tool_name="spreadsheet_create",
            status="completed",
            output={"artifact_path": str(xlsx_path), "artifact_paths": [str(xlsx_path)]},
        ),
        ToolResult(
            invocation_id="csv",
            tool_name="spreadsheet_create",
            status="completed",
            output={"path": str(csv_path)},
        ),
        ToolResult(
            invocation_id="pdf",
            tool_name="pdf_create",
            status="completed",
            output={"artifact_path": str(pdf_path), "artifact_paths": [str(pdf_path)]},
        ),
        ToolResult(
            invocation_id="html",
            tool_name="file_write",
            status="completed",
            output={"path": str(html_path)},
        ),
        ToolResult(
            invocation_id="notes",
            tool_name="file_write",
            status="completed",
            output={"path": str(sidecar_path)},
        ),
    ]

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create a workbook, CSV export, PDF summary, and HTML page.",
        reply="Created the requested artifact package.",
        tool_results=tool_results,
        principal_id="web:1",
        required_attachment_extensions=(".xlsx", ".csv", ".pdf", ".html"),
    )

    assert paths == [str(xlsx_path), str(csv_path), str(pdf_path), str(html_path)]


def test_web_delivery_keeps_non_media_files_in_mixed_embedded_media_package(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    csv_path = principal_root / "stage_random_website_qa_media_archive_dataset.csv"
    linked_html = principal_root / "stage_random_website_qa_linked_report.html"
    embedded_html = principal_root / "stage_random_website_qa_embedded_report.html"
    pdf_path = principal_root / "stage_random_website_qa_media_report.pdf"
    package_json = principal_root / "stage_random_website_qa_media_archive_package.json"
    status_json = principal_root / "stage_random_website_qa_media_archive_status.json"
    status_text = principal_root / "stage_random_website_qa_media_archive_status.txt"
    source_image = media_root / "poster.png"
    csv_path.write_text("name,type\nAudio,Collection\n", encoding="utf-8")
    source_image.write_bytes(b"\x89PNG\r\n\x1a\nstage")
    linked_html.write_text(
        "<!doctype html><html><body><h1>STAGE RANDOM WEBSITE QA</h1></body></html>",
        encoding="utf-8",
    )
    embedded_html.write_text(
        "<!doctype html><html><body><h1>STAGE RANDOM WEBSITE QA</h1><img src=\"data:image/png;base64,AAAA\"></body></html>",
        encoding="utf-8",
    )
    pdf_path.write_bytes(b"%PDF-1.4\n%stage random website qa\n")
    package_json.write_text('{"sidecar": true}', encoding="utf-8")
    status_json.write_text('{"status": "ok"}', encoding="utf-8")
    status_text.write_text("internal status", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    reply = "\n".join(
        [
            "Done - attached the requested artifact package:",
            f"MEDIA:{embedded_html}",
            f"MEDIA:{pdf_path}",
            f"MEDIA:{linked_html}",
            f"MEDIA:{csv_path}",
            f"MEDIA:{package_json}",
            f"MEDIA:{status_json}",
            f"MEDIA:{status_text}",
        ]
    )
    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt=(
            "Deliver exactly these final artifacts: a CSV dataset, a linked HTML report, "
            "a self-contained HTML report with embedded image assets, and a PDF media report."
        ),
        reply=reply,
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".csv", ".html", ".pdf"],
                    "embedded_media_artifact_extensions": [".html", ".pdf"],
                },
            ),
            ToolResult("csv", "file_write", "completed", {"path": str(csv_path)}),
            ToolResult("linked", "file_write", "completed", {"path": str(linked_html)}),
            ToolResult("embedded", "file_write", "completed", {"path": str(embedded_html)}),
            ToolResult(
                "pdf",
                "pdf_create",
                "completed",
                {"artifact_path": str(pdf_path), "source_image_paths": [str(source_image)]},
            ),
            ToolResult("json", "file_write", "completed", {"path": str(package_json)}),
            ToolResult("status-json", "file_write", "completed", {"path": str(status_json)}),
            ToolResult("status-text", "file_write", "completed", {"path": str(status_text)}),
        ],
        principal_id="web:1",
        required_attachment_extensions=(".csv", ".html", ".pdf"),
    )

    assert paths == [str(embedded_html), str(pdf_path), str(linked_html), str(csv_path)]


def test_web_delivery_uses_final_scoped_package_not_late_support_files(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    docx_path = principal_root / "stage_mixed_embedded_media_pack.docx"
    xlsx_path = principal_root / "stage_mixed_embedded_media_pack.xlsx"
    csv_path = principal_root / "stage_mixed_embedded_media_pack.csv"
    pdf_path = principal_root / "stage_mixed_embedded_media_pack.pdf"
    html_path = principal_root / "stage_mixed_embedded_media_pack.html"
    status_json = principal_root / "stage_mixed_embedded_media_pack_status.json"
    status_text = principal_root / "stage_mixed_embedded_media_pack_status.txt"
    preview_png = principal_root / "stage_mixed_embedded_media_pack_preview.png"
    alternate_docx = principal_root / "stage_mixed_embedded_media_pack_report.docx"
    alternate_xlsx = principal_root / "stage_mixed_embedded_media_pack_workbook.xlsx"
    alternate_csv = principal_root / "stage_mixed_embedded_media_pack_export.csv"
    alternate_pdf = principal_root / "stage_mixed_embedded_media_pack_summary.pdf"
    alternate_html = principal_root / "stage_mixed_embedded_media_pack_report.html"
    for path in (docx_path, xlsx_path):
        path.write_bytes(b"PK\x03\x04")
    for path in (alternate_docx, alternate_xlsx):
        path.write_bytes(b"PK\x03\x04")
    csv_path.write_text("name,value\nqa,1\n", encoding="utf-8")
    alternate_csv.write_text("name,value\nqa-alt,1\n", encoding="utf-8")
    pdf_path.write_bytes(b"%PDF-1.4\n")
    alternate_pdf.write_bytes(b"%PDF-1.4\n%alternate\n")
    html_path.write_text(
        "<html><body><h1>Stage mixed embedded media pack</h1><img src='data:image/png;base64,AAAA'></body></html>",
        encoding="utf-8",
    )
    alternate_html.write_text("<html><body><h1>Alternate report</h1></body></html>", encoding="utf-8")
    status_json.write_text('{"status":"ok"}', encoding="utf-8")
    status_text.write_text("internal status", encoding="utf-8")
    preview_png.write_bytes(b"\x89PNG\r\n\x1a\n")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create the requested final artifact package with embedded media in the workbook.",
        reply=(
            "Created the requested package.\n"
            f"MEDIA:{docx_path}\n"
            f"MEDIA:{xlsx_path}\n"
            f"MEDIA:{csv_path}\n"
            f"MEDIA:{pdf_path}\n"
            f"MEDIA:{html_path}\n"
            f"MEDIA:{alternate_docx}\n"
            f"MEDIA:{alternate_xlsx}\n"
            f"MEDIA:{alternate_csv}\n"
            f"MEDIA:{alternate_pdf}\n"
            f"MEDIA:{alternate_html}\n"
            f"MEDIA:{status_json}\n"
            f"MEDIA:{status_text}\n"
            f"MEDIA:{preview_png}"
        ),
        tool_results=[
            ToolResult("preview-scope", "request_tool_scope", "completed", {"artifact_extensions": [".png"]}),
            ToolResult(
                "final-scope",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".docx", ".xlsx", ".csv", ".pdf", ".html"],
                    "embedded_media_artifact_extensions": [".xlsx"],
                },
            ),
            ToolResult("docx", "document_create", "completed", {"artifact_path": str(docx_path)}),
            ToolResult(
                "xlsx",
                "spreadsheet_create",
                "completed",
                {"artifact_path": str(xlsx_path), "embedded_images": [str(preview_png)]},
            ),
            ToolResult("csv", "file_write", "completed", {"path": str(csv_path), "artifact_path": str(csv_path)}),
            ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(pdf_path)}),
            ToolResult("html", "file_write", "completed", {"path": str(html_path), "artifact_path": str(html_path)}),
            ToolResult("alt-docx", "document_create", "completed", {"artifact_path": str(alternate_docx)}),
            ToolResult(
                "alt-xlsx",
                "spreadsheet_create",
                "completed",
                {"artifact_path": str(alternate_xlsx), "embedded_images": [str(preview_png)]},
            ),
            ToolResult("alt-csv", "file_write", "completed", {"path": str(alternate_csv), "artifact_path": str(alternate_csv)}),
            ToolResult("alt-pdf", "pdf_create", "completed", {"artifact_path": str(alternate_pdf)}),
            ToolResult("alt-html", "file_write", "completed", {"path": str(alternate_html), "artifact_path": str(alternate_html)}),
            ToolResult(
                "support-scope",
                "request_tool_scope",
                "completed",
                {"artifact_extensions": [".json", ".txt", ".png"]},
            ),
            ToolResult("json", "file_write", "completed", {"path": str(status_json), "artifact_path": str(status_json)}),
            ToolResult("txt", "file_write", "completed", {"path": str(status_text), "artifact_path": str(status_text)}),
            ToolResult("png", "file_write", "completed", {"path": str(preview_png), "artifact_path": str(preview_png)}),
        ],
        artifact_paths=[
            str(docx_path),
            str(xlsx_path),
            str(csv_path),
            str(pdf_path),
            str(html_path),
            str(alternate_docx),
            str(alternate_xlsx),
            str(alternate_csv),
            str(alternate_pdf),
            str(alternate_html),
            str(status_json),
            str(status_text),
            str(preview_png),
        ],
        principal_id="web:1",
    )

    assert paths == [str(docx_path), str(xlsx_path), str(csv_path), str(pdf_path), str(html_path)]


def test_web_delivery_infers_package_extensions_from_completed_artifact_tools_without_scope(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    docx_path = principal_root / "stage_mixed_embedded_media_pack_report.docx"
    xlsx_path = principal_root / "stage_mixed_embedded_media_pack_workbook.xlsx"
    csv_path = principal_root / "stage_mixed_embedded_media_pack_export.csv"
    pdf_path = principal_root / "stage_mixed_embedded_media_pack_summary.pdf"
    html_path = principal_root / "stage_mixed_embedded_media_pack_report.html"
    image_path = principal_root / "stage_mixed_embedded_media_pack_embedded_image.png"
    json_path = principal_root / "stage_mixed_embedded_media_pack_attachment.json"
    text_path = principal_root / "stage_mixed_embedded_media_pack_attachment.txt"
    for path in (docx_path, xlsx_path):
        path.write_bytes(b"PK\x03\x04")
    csv_path.write_text("name,value\nqa,1\n", encoding="utf-8")
    pdf_path.write_bytes(b"%PDF-1.4\n")
    html_path.write_text(
        "<html><body><h1>STAGE MIXED EMBEDDED QA</h1><p>Verified final HTML artifact.</p></body></html>",
        encoding="utf-8",
    )
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    json_path.write_text('{"support": true}', encoding="utf-8")
    text_path.write_text("support notes", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    reply = "\n".join(
        [
            "Done - attached the requested artifact package:",
            f"MEDIA:{docx_path}",
            f"MEDIA:{xlsx_path}",
            f"MEDIA:{csv_path}",
            f"MEDIA:{pdf_path}",
            f"MEDIA:{html_path}",
            f"MEDIA:{image_path}",
            f"MEDIA:{json_path}",
            f"MEDIA:{text_path}",
        ]
    )
    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create a test-only mixed artifact package and attach the final files.",
        reply=reply,
        tool_results=[
            ToolResult("docx", "document_create", "completed", {"artifact_path": str(docx_path)}),
            ToolResult("csv", "file_write", "completed", {"path": str(csv_path), "artifact_path": str(csv_path)}),
            ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(pdf_path)}),
            ToolResult("html", "file_write", "completed", {"path": str(html_path), "artifact_path": str(html_path)}),
            ToolResult("image", "terminal_exec", "completed", {"artifact_paths": [str(image_path), str(xlsx_path)]}),
            ToolResult("json", "file_write", "completed", {"path": str(json_path), "artifact_path": str(json_path)}),
            ToolResult("txt", "file_write", "completed", {"path": str(text_path), "artifact_path": str(text_path)}),
        ],
        artifact_paths=[
            str(docx_path),
            str(xlsx_path),
            str(csv_path),
            str(pdf_path),
            str(html_path),
            str(image_path),
            str(json_path),
            str(text_path),
        ],
        principal_id="web:1",
    )

    assert paths == [str(docx_path), str(xlsx_path), str(csv_path), str(pdf_path), str(html_path)]


def test_web_delivery_drops_media_required_suffix_when_no_artifact_proves_media(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    csv_path = principal_root / "stage_random_website_qa_media_archive_dataset.csv"
    plain_html = principal_root / "stage_random_website_qa_embedded_report.html"
    plain_pdf = principal_root / "stage_random_website_qa_media_report.pdf"
    csv_path.write_text("name,type\nAudio,Collection\n", encoding="utf-8")
    plain_html.write_text(
        "<!doctype html><html><body><h1>STAGE RANDOM WEBSITE QA</h1></body></html>",
        encoding="utf-8",
    )
    plain_pdf.write_bytes(b"%PDF-1.4\n%stage random website qa\n")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Create a CSV, self-contained HTML with embedded image assets, and PDF media report.",
        reply="Created the requested package.",
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".csv", ".html", ".pdf"],
                    "embedded_media_artifact_extensions": [".html", ".pdf"],
                },
            ),
            ToolResult("csv", "file_write", "completed", {"path": str(csv_path)}),
            ToolResult("html", "file_write", "completed", {"path": str(plain_html)}),
            ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(plain_pdf)}),
        ],
        principal_id="web:1",
        required_attachment_extensions=(".csv", ".html", ".pdf"),
    )

    assert paths == [str(csv_path)]


def test_web_delivery_artifact_paths_recovers_file_write_artifact_after_structured_artifact(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    xlsx_path = principal_root / "stage_archive_fallback_chain.xlsx"
    csv_path = principal_root / "stage_archive_extension_summary.csv"
    notes_path = principal_root / "stage_archive_notes.txt"
    xlsx_path.write_bytes(b"PK\x03\x04xlsx")
    csv_path.write_text("extension,count\n.txt,2\n", encoding="utf-8")
    notes_path.write_text("internal notes", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt=(
            "Using the extracted archive context from this same chat, create a CSV named "
            "stage_archive_extension_summary.csv listing file extension and count. "
            "Attach only the CSV; do not attach source files or sidecars."
        ),
        reply="Created the CSV.",
        tool_results=[
            ToolResult(
                "xlsx",
                "spreadsheet_create",
                "completed",
                {"artifact_path": str(xlsx_path), "artifact_paths": [str(xlsx_path)]},
            ),
            ToolResult("csv", "file_write", "completed", {"path": str(csv_path)}),
            ToolResult("notes", "file_write", "completed", {"path": str(notes_path)}),
        ],
        principal_id="web:1",
    )

    assert paths == [str(csv_path)]


def test_web_delivery_refines_source_extensions_with_deliverable_descriptor(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    workbook = principal_root / "stage_archive_transform_field_notes.xlsx"
    source_zip = upload_root / "field-notes-images.zip"
    sidecar_csv = upload_root / "field-notes-images-extracted_manifest.csv"
    sidecar_json = upload_root / "field-notes-images-extracted_manifest.json"
    image = files_root / "observations" / "alpha-sun.png"
    image.parent.mkdir()
    workbook.write_bytes(b"PK\x03\x04fake workbook")
    source_zip.write_bytes(b"PK\x03\x04fake zip")
    sidecar_csv.write_text("name,path\nalpha,alpha-sun.png\n", encoding="utf-8")
    sidecar_json.write_text("[]", encoding="utf-8")
    image.write_bytes(b"\x89PNG\r\n")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )
    tool_results = [
        ToolResult(
            invocation_id="extract",
            tool_name="archive_extract",
            status="completed",
            output={
                "archive_path": str(source_zip),
                "artifact_descriptors": [
                    {"path": str(source_zip), "role": "source", "extension": ".zip"},
                    {"path": str(sidecar_csv), "role": "intermediate", "extension": ".csv"},
                    {"path": str(sidecar_json), "role": "intermediate", "extension": ".json"},
                ],
                "embedded_images": [str(image)],
            },
        ),
        ToolResult(
            invocation_id="xlsx",
            tool_name="spreadsheet_create",
            status="completed",
            output={
                "artifact_path": str(workbook),
                "artifact_paths": [str(workbook)],
                "artifact_descriptors": [
                    {"path": str(workbook), "role": "deliverable", "extension": ".xlsx"}
                ],
                "embedded_images": [str(image)],
                "skipped_images": [],
            },
        ),
    ]

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="archivo de salida stage_archive_transform_field_notes.xlsx; fuente field-notes-images.zip",
        reply="Created the requested spreadsheet.",
        tool_results=tool_results,
        principal_id="web:1",
        requested_extension=".zip",
        required_attachment_extensions=(".xlsx", ".png", ".csv", ".json", ".zip"),
    )

    assert paths == [str(workbook)]
    assert web_app._web_filter_artifact_paths_to_reply_paths(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        reply="Done — attached the requested spreadsheet.",
        artifact_paths=[str(workbook), str(image)],
        principal_id="web:1",
        required_attachment_extensions=(".xlsx",),
        prompt="Produce one structured workbook artifact.",
    ) == [str(workbook)]
    final_text, fulfilled = web_app._enforce_web_response_fulfillment(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        conversation_id="web:1",
        user_message="archivo de salida stage_archive_transform_field_notes.xlsx; fuente field-notes-images.zip",
        reply="I couldn't attach all of the requested files. The task is still open.",
        tool_results=tool_results,
        artifact_paths=paths,
        artifact_count=1,
        required_attachment_extensions=(".xlsx", ".png", ".csv", ".json", ".zip"),
        required_embedded_media_extensions=(".xlsx",),
    )

    assert fulfilled
    assert "couldn't attach" not in final_text
    assert f"MEDIA:{workbook}" in final_text


def test_web_required_attachment_extension_merge_preserves_repeated_explicit_formats() -> None:
    tool_results = [
        ToolResult(
            invocation_id="scope",
            tool_name="request_tool_scope",
            status="completed",
            output={"artifact_extensions": [".pdf", ".html"]},
        ),
        ToolResult(
            invocation_id="pdf",
            tool_name="pdf_create",
            status="completed",
            output={
                "artifact_path": "/workspace/artifacts/poster.pdf",
                "artifact_descriptors": [
                    {
                        "path": "/workspace/artifacts/poster.pdf",
                        "role": "deliverable",
                        "extension": ".pdf",
                    }
                ],
            },
        ),
        ToolResult(
            invocation_id="html",
            tool_name="file_write",
            status="completed",
            output={
                "path": "/workspace/artifacts/poster.html",
                "artifact_descriptors": [
                    {
                        "path": "/workspace/artifacts/poster.html",
                        "role": "deliverable",
                        "extension": ".html",
                    }
                ],
            },
        ),
    ]

    assert web_app._web_merge_required_attachment_extensions(
        (".pdf", ".pdf", ".html"),
        tool_results,
    ) == (".pdf", ".pdf", ".html")
    assert web_app._web_refine_required_attachment_extensions_for_deliverables(
        (".pdf", ".pdf", ".html"),
        tool_results,
    ) == (".pdf", ".pdf", ".html")


def test_web_file_search_listing_sidecar_does_not_attach_discovered_pdf(tmp_path) -> None:
    discovered_pdf = tmp_path / "stage_file_search_delivery_guard.pdf"
    sidecar = tmp_path / "stage_file_search_delivery_guard_listing.txt"
    discovered_pdf.write_bytes(b"%PDF-1.4\n% discovered, not produced\n")
    sidecar.write_text(discovered_pdf.name, encoding="utf-8")
    tool_results = [
        ToolResult(
            invocation_id="search",
            tool_name="file_search",
            status="completed",
            output={
                "matches": [str(discovered_pdf)],
                "match_details": [{"path": str(discovered_pdf), "match_type": "filename"}],
            },
        ),
        ToolResult(
            invocation_id="write",
            tool_name="file_write",
            status="completed",
            output={"path": str(sidecar), "artifact_path": str(sidecar), "artifact_paths": [str(sidecar)]},
        ),
        ToolResult(
            invocation_id="patch",
            tool_name="file_patch",
            status="completed",
            output={"path": str(sidecar)},
        ),
    ]

    assert web_app._web_local_discovery_paths_from_tool_results(tool_results) == (str(discovered_pdf),)
    assert web_app._web_tool_results_are_local_discovery_or_sidecar(
        tool_results,
        required_extensions=(".pdf",),
    )
    assert web_app._web_refine_required_attachment_extensions_for_deliverables(
        (".pdf",),
        tool_results,
    ) == ()
    assert web_app._web_refine_requested_extension_for_deliverables(".pdf", tool_results) is None

    reply = web_app._web_local_discovery_results_reply(
        web_app._web_local_discovery_paths_from_tool_results(tool_results)
    )

    assert discovered_pdf.name in reply
    assert str(discovered_pdf) not in reply
    assert web_app._web_local_discovery_reply_for_tool_results(
        tool_results,
        required_extensions=(".pdf",),
    ) == f"I found 1 file:\n- {discovered_pdf.name}"
    assert web_app._web_delivery_artifact_paths(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        prompt=(
            f"Find the pdf file named {discovered_pdf.name} in my workspace and reply with "
            "a concise filename-only listing. Do not attach, upload, or send the file."
        ),
        reply=reply,
        tool_results=tool_results,
        artifact_paths=[str(discovered_pdf)],
        principal_id="web:file-search-listing",
        requested_extension=".pdf",
        required_attachment_extensions=(".pdf",),
    ) == []


def test_web_delivery_attaches_typed_direct_existing_artifact_from_file_search(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal-artifacts"
    uploads_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, uploads_root, files_root, media_root):
        root.mkdir()
    requested = principal_root / "stage_requested_delivery_artifact.xlsx"
    requested.write_bytes(b"PK\x03\x04sheet")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: uploads_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )
    tool_results = [
        ToolResult(
            invocation_id="search",
            tool_name="file_search",
            status="completed",
            output={
                "action": "deliver",
                "direct_existing_artifact": True,
                "matches": [
                    {
                        "artifact": True,
                        "extension": ".xlsx",
                        "name": requested.name,
                        "path": str(requested),
                    }
                ],
            },
        )
    ]

    assert web_app._web_delivery_artifact_paths(
        SimpleNamespace(checkpoint_path=runtime_root / "runtime.db", store=RuntimeStore()),
        prompt=(
            f"Entrega el archivo exacto {requested.name} como archivo descargable."
        ),
        reply="Done - attached the requested spreadsheet.",
        tool_results=tool_results,
        artifact_paths=[],
        principal_id="web:direct-existing-artifact",
    ) == [str(requested)]


def test_web_filter_keeps_required_artifact_when_reply_mentions_unrelated_paths(tmp_path) -> None:
    artifact = tmp_path / "stage_followup_reuse_values.xlsx"
    artifact.write_bytes(b"PK\x03\x04sheet")
    unrelated = tmp_path / "stage_followup_source_values.csv"
    unrelated.write_text("stale\n", encoding="utf-8")

    filtered = web_app._web_filter_artifact_paths_to_reply_paths(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        reply=f"I found 2 files:\n- {unrelated.name}\n- old_report.pdf",
        artifact_paths=[str(artifact)],
        principal_id="web:artifact-filter",
        required_attachment_extensions=(".xlsx",),
    )

    assert filtered == [str(artifact)]


def test_web_filter_drops_reply_mentioned_helper_media_when_required_artifact_is_narrow(tmp_path) -> None:
    workbook = tmp_path / "stage_artifact_followup_retry_guard_265b3f8bd1.xlsx"
    helper_image = tmp_path / "alpha-sun.png"
    workbook.write_bytes(b"PK\x03\x04sheet")
    helper_image.write_bytes(b"\x89PNG\r\n\x1a\n")

    filtered = web_app._web_filter_artifact_paths_to_reply_paths(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        prompt=(
            "Create a new XLSX workbook named stage_artifact_followup_retry_guard_265b3f8bd1.xlsx. "
            "Attach only stage_artifact_followup_retry_guard_265b3f8bd1.xlsx."
        ),
        reply=(
            "Done - attached the requested files.\n\n"
            f"MEDIA:{workbook}\n"
            f"MEDIA:{helper_image}"
        ),
        artifact_paths=[str(workbook), str(helper_image)],
        principal_id="web:artifact-filter",
        required_attachment_extensions=(".xlsx",),
    )

    assert filtered == [str(workbook)]


def test_web_delivery_preserves_complete_explicit_filename_package_after_reply_filter(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    poster_pdf = principal_root / "community_night_market_poster_STAGE_FORMAT_QA.pdf"
    memo_pdf = principal_root / "community_night_market_memo_STAGE_FORMAT_QA.pdf"
    poster_html = principal_root / "community_night_market_poster_STAGE_FORMAT_QA.html"
    extra_pdf = principal_root / "requested_artifact_last_mile.pdf"
    poster_pdf.write_bytes(b"%PDF-1.4\n%poster\n")
    memo_pdf.write_bytes(b"%PDF-1.4\n%memo\n")
    poster_html.write_text(
        "<!doctype html><html><body><h1>Community night market poster</h1>"
        "<p>Stage format QA deliverable with enough content to be a valid HTML artifact.</p>"
        "</body></html>",
        encoding="utf-8",
    )
    extra_pdf.write_bytes(b"%PDF-1.4\n%extra\n")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )
    prompt = (
        "Create exactly these files: "
        "community_night_market_poster_STAGE_FORMAT_QA.pdf, "
        "community_night_market_memo_STAGE_FORMAT_QA.pdf, and "
        "community_night_market_poster_STAGE_FORMAT_QA.html."
    )
    tool_results = [
        ToolResult(
            "scope",
            "request_tool_scope",
            "completed",
            {"artifact_extensions": [".pdf", ".pdf", ".html"]},
        ),
        ToolResult(
            "poster-html",
            "file_write",
            "completed",
            {"path": str(poster_html), "artifact_path": str(poster_html)},
        ),
        ToolResult("poster-pdf", "pdf_create", "completed", {"artifact_path": str(poster_pdf)}),
        ToolResult("memo-pdf", "pdf_create", "completed", {"artifact_path": str(memo_pdf)}),
        ToolResult("extra-pdf", "pdf_create", "completed", {"artifact_path": str(extra_pdf)}),
    ]
    all_reply_paths = "\n".join(
        f"MEDIA:{path}" for path in (poster_pdf, memo_pdf, poster_html, extra_pdf)
    )

    delivered = web_app._web_delivery_artifact_paths(
        object(),
        prompt=prompt,
        reply=f"Created files.\n{all_reply_paths}",
        tool_results=tool_results,
        artifact_paths=[str(extra_pdf), str(poster_pdf), str(memo_pdf), str(poster_html)],
        principal_id="web:format-qa",
        required_attachment_extensions=(".pdf", ".pdf", ".html"),
    )

    assert delivered == [str(poster_pdf), str(memo_pdf), str(poster_html)]

    filtered = web_app._web_filter_artifact_paths_to_reply_paths(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        prompt=prompt,
        reply=f"Done.\nMEDIA:{poster_pdf}",
        artifact_paths=delivered,
        principal_id="web:format-qa",
        required_attachment_extensions=(".pdf", ".pdf", ".html"),
    )

    assert filtered == delivered


def test_web_local_discovery_reply_does_not_replace_created_required_artifact(tmp_path) -> None:
    created = tmp_path / "stage_archive_extension_summary.csv"
    created.write_text("extension,count\n.txt,2\n", encoding="utf-8")
    discovered = tmp_path / "old_source_file.csv"
    discovered.write_text("old\n", encoding="utf-8")
    tool_results = [
        ToolResult(
            "search",
            "file_search",
            "completed",
            {
                "matches": [str(discovered), str(created)],
                "match_details": [
                    {"path": str(discovered), "match_type": "filename"},
                    {"path": str(created), "match_type": "filename"},
                ],
            },
        ),
        ToolResult(
            "exec",
            "terminal_exec",
            "completed",
            {
                "stdout": f"created {created}\n",
                "stderr": "",
                "exit_code": 0,
                "artifact_paths": [str(created)],
            },
        ),
    ]

    assert web_app._web_local_discovery_reply_for_tool_results(
        tool_results,
        required_extensions=(".csv",),
    ) is None


def test_web_response_fulfillment_defers_scheduler_future_media_requirements(tmp_path) -> None:
    final_text, fulfilled = web_app._enforce_web_response_fulfillment(
        SimpleNamespace(checkpoint_path=tmp_path / "runtime.db", store=RuntimeStore()),
        conversation_id="web:scheduler-future-artifacts",
        user_message="Set up a scheduled report that creates spreadsheet and PDF artifacts.",
        reply="Cron created.",
        tool_results=[
            ToolResult(
                invocation_id="scope-1",
                tool_name="request_tool_scope",
                status="completed",
                output={
                    "artifact_extensions": [".xlsx", ".pdf"],
                    "embedded_media_artifact_extensions": [".xlsx"],
                },
            ),
            ToolResult(
                invocation_id="cron-1",
                tool_name="create_cron",
                status="completed",
                output={"message": "Cron created."},
            ),
        ],
        required_attachment_extensions=(".xlsx", ".pdf"),
        required_embedded_media_extensions=(".xlsx",),
    )

    assert fulfilled
    assert final_text == "Cron created."


def test_web_delivery_artifact_paths_keeps_completed_image_generate_artifact(monkeypatch, tmp_path) -> None:
    runtime_root = tmp_path / "runtime"
    principal_root = tmp_path / "principal"
    upload_root = tmp_path / "uploads"
    files_root = tmp_path / "files"
    media_root = tmp_path / "media"
    for root in (runtime_root, principal_root, upload_root, files_root, media_root):
        root.mkdir()
    generated_svg = principal_root / "monkey_lake_house.svg"
    generated_svg.write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")
    monkeypatch.setattr(web_app, "artifact_root_for_runtime", lambda runtime: runtime_root)
    monkeypatch.setattr(web_app, "artifact_root_for_principal", lambda principal_id: principal_root)
    monkeypatch.setattr(web_app, "messaging_upload_root", lambda: upload_root)
    monkeypatch.setattr(
        web_app,
        "workspace_storage_roots_for_principal",
        lambda principal_id: SimpleNamespace(files=files_root, media=media_root),
    )

    paths = web_app._web_delivery_artifact_paths(
        object(),
        prompt="Generate an image of a monkey going to a lake house",
        reply=f"Generated the image and saved it here:\n\n`{generated_svg.name}`",
        tool_results=[
            ToolResult(
                invocation_id="img-1",
                tool_name="image_generate",
                status="completed",
                output={
                    "provider": "local_svg_fallback",
                    "path": str(generated_svg),
                    "output_path": str(generated_svg.with_suffix('.png')),
                },
            ),
        ],
        principal_id="web:1",
    )

    assert paths == [str(generated_svg)]
    assert web_app._web_delivery_artifact_paths(
        object(),
        prompt="Generate an image of a monkey going to a lake house",
        reply=f"Generated the image and saved it here:\n\n`{generated_svg.name}`",
        tool_results=[
            ToolResult(
                invocation_id="img-2",
                tool_name="image_generate",
                status="completed",
                output={
                    "provider": "local_svg_fallback",
                    "path": str(generated_svg),
                    "output_path": str(generated_svg.with_suffix('.png')),
                },
            ),
        ],
        principal_id="web:1",
        requested_extension=".png",
    ) == [str(generated_svg)]


def test_web_completed_email_send_reply_uses_clean_summary() -> None:
    reply = web_app._web_normalize_completed_email_send_reply(
        (
            "Email sent successfully to **recipient@example.com**.\n\n"
            "Subject: **Release check**  \n"
            "Message ID: `19e329d7a87a278c`"
        ),
        [
            ToolResult(
                invocation_id="email-1",
                tool_name="email_send",
                status="completed",
                output={
                    "to": ["recipient@example.com"],
                    "subject": "Release check",
                    "id": "19e329d7a87a278c",
                },
            )
        ],
    )

    assert reply == '✉️ Email sent to recipient@example.com with subject "Release check".'
    assert "Message ID" not in reply
    assert "19e329d7a87a278c" not in reply


def test_artifact_lookup_download_path_and_logs(monkeypatch, tmp_path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    file_path = root / "one.txt"
    file_path.write_text("hello\nsecret", encoding="utf-8")
    descriptor = web_app.artifact_descriptor_for_path(file_path, artifact_root=root)
    assert descriptor is not None
    monkeypatch.setattr(web_app, "_web_artifact_roots", lambda runtime: [root])
    web_app._WEB_ARTIFACTS[descriptor.artifact_id] = file_path
    assert web_app._web_artifact_path_for_id(object(), descriptor.artifact_id) == file_path
    assert web_app._web_artifact_descriptor_for_path(object(), file_path).artifact_id == descriptor.artifact_id
    web_app._WEB_ARTIFACTS.pop(descriptor.artifact_id, None)
    assert web_app._web_artifact_path_for_id(object(), descriptor.artifact_id) == file_path
    assert web_app._web_artifact_path_for_id(object(), "missing") is None
    assert web_app._web_artifact_descriptor_for_path(object(), tmp_path / "outside.txt") is None

    stale_id = descriptor.artifact_id
    file_path.write_text("hello\nsecret\nrewritten", encoding="utf-8")
    rewritten_descriptor = web_app.artifact_descriptor_for_path(file_path, artifact_root=root)
    assert rewritten_descriptor is not None
    assert rewritten_descriptor.artifact_id != stale_id
    web_app._WEB_ARTIFACTS.pop(stale_id, None)
    runtime = SimpleNamespace(
        store=SimpleNamespace(
            list_recent_conversation_events=lambda conversation_id, event_type=None, limit=None: [
                {
                    "event_type": "conversation.chat_turn",
                    "artifacts": [
                        {
                            "id": stale_id,
                            "name": file_path.name,
                            "path": str(file_path),
                            "url": f"/api/artifacts/{stale_id}",
                        }
                    ],
                }
            ]
        )
    )
    assert web_app._web_artifact_path_for_id(runtime, stale_id) == file_path

    downloads = tmp_path / "downloads"
    first = web_app._unique_download_path("../report.txt", downloads_dir=downloads)
    first.write_text("exists", encoding="utf-8")
    assert web_app._unique_download_path("../report.txt", downloads_dir=downloads).name == "report (1).txt"
    for idx in range(1, 1000):
        (downloads / f"report ({idx}).txt").write_text("exists", encoding="utf-8")
    with pytest.raises(RuntimeError):
        web_app._unique_download_path("../report.txt", downloads_dir=downloads)

    log_dir = tmp_path / ".nullion" / "logs"
    log_dir.mkdir(parents=True)
    log_file = log_dir / "nullion.log"
    log_file.write_text("line1\nline2", encoding="utf-8")
    monkeypatch.setattr(web_app.Path, "home", lambda: tmp_path)
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / ".nullion"))
    assert web_app._known_log_files() == [log_file]
    assert web_app._tail_text(log_file, max_bytes=5)["truncated"] is True
    assert web_app._logs_payload("nullion.log")["ok"] is True
    assert web_app._logs_payload("missing.log")["ok"] is False


def test_reporting_doctor_and_chat_service_helpers(monkeypatch) -> None:
    runtime = FakeRuntime()
    web_app._report_web_client_issue(runtime, issue_type="nonsense", message="", details={"x": "y"})
    assert runtime.health_issues[0]["source"] == "web_app"

    assert web_app._doctor_action_can_try_fix({"summary": "Telegram typing indicator stuck"})
    assert not web_app._doctor_action_can_try_fix({"summary": "unrelated"})
    with pytest.raises(ValueError):
        web_app._try_doctor_fix({"summary": "unrelated"})

    monkeypatch.delenv("NULLION_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.setenv("NULLION_SLACK_ENABLED", "true")
    monkeypatch.setenv("NULLION_SLACK_BOT_TOKEN", "x")
    monkeypatch.setenv("NULLION_SLACK_APP_TOKEN", "y")
    monkeypatch.setenv("NULLION_DISCORD_ENABLED", "true")
    monkeypatch.delenv("NULLION_DISCORD_BOT_TOKEN", raising=False)
    assert web_app._chat_service_enabled_from_env("slack") is True
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "chat")
    assert web_app._chat_service_configured_from_env("telegram") == (True, "Configured")
    monkeypatch.setenv("NULLION_DISCORD_BOT_TOKEN", "discord")
    monkeypatch.setenv("NULLION_DISCORD_OPERATOR_USER_ID", "2001")
    assert web_app._chat_service_configured_from_env("discord") == (True, "Configured")
    monkeypatch.delenv("NULLION_DISCORD_BOT_TOKEN", raising=False)
    assert web_app._chat_service_configured_from_env("discord") == (False, "Needs token")
    monkeypatch.setattr(web_app, "_launchd_status_for_labels", lambda labels: (None, None))
    monkeypatch.setattr(web_app, "_process_running_for_command", lambda command: command == "nullion-slack")
    services = web_app._chat_services_status_payload()
    assert {service["name"]: service["state"] for service in services}["slack"] == "live"


def test_message_history_error_and_activity_helpers(monkeypatch) -> None:
    messages = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": [{"type": "text", "text": "old"}]},
        {"role": "assistant", "content": "Approval required for tool", "metadata": {"approval_notice": True}},
        {"role": "user", "content": "new"},
    ]
    assert web_app._text_from_message_content(messages[1]["content"]) == "old"
    assert web_app._text_from_message_content(123) == ""
    assert web_app._text_from_message_content([{"type": "image", "text": "ignored"}]) == ""
    assert web_app._last_user_text_from_snapshot(messages) == "new"
    assert web_app._last_user_text_from_snapshot([]) is None
    assert web_app._last_user_text_from_snapshot([{"role": "assistant", "content": "x"}]) is None
    assert web_app._resume_history_from_snapshot(messages) == messages[:1]
    assert web_app._resume_history_from_snapshot([]) == []
    assert web_app._is_stale_approval_notice({"role": "user", "content": "approval required"}) is False
    assert web_app._is_stale_approval_notice({"role": "assistant", "content": "Tool approval requested: x"}) is False
    assert web_app._is_stale_approval_notice({"role": "assistant", "content": "x", "metadata": {"approval_notice": True}})
    label, detail, is_web = web_app._approval_display_from_request(None)
    assert label and isinstance(detail, str) and is_web is False
    assert web_app._approval_trigger_flow_label_from_request(None) is None
    assert web_app._web_approval_required_payload(
        {
            "approval_id": "approval-1",
            "tool_name": "fetch a web page",
            "tool_detail": "https://example.com",
            "trigger_flow_label": None,
            "is_web_request": True,
        }
    ) == {
        "type": "approval_required",
        "text": "Nullion paused before continuing. Review the approval card to continue or deny the action.",
        "approval_id": "approval-1",
        "tool_name": "fetch a web page",
        "tool_detail": "https://example.com",
        "trigger_flow_label": "",
        "is_web_request": True,
        "request_kind": "",
        "web_session_allow_label": web_app._web_session_allow_duration_label(),
    }

    destructive_approval = create_approval_request(
        requested_by="web:e2e",
        action="use_tool",
        resource="terminal_exec",
        request_kind=TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND,
        context={
            "tool_name": "terminal_exec",
            "command": "rm -f /workspace/runtime.db.corrupt-20260610",
            "destructive_target_count": 1,
            "destructive_targets": [{"target": "/workspace/runtime.db.corrupt-20260610"}],
        },
    )
    approved_text = web_app._approval_confirmation_text_for_web(
        destructive_approval,
        action="approve",
        mode="once",
    )
    denied_text = web_app._approval_confirmation_text_for_web(destructive_approval, action="deny")
    assert approved_text.startswith("Delete approved once. Continuing...")
    assert "Command:\n`rm -f /workspace/runtime.db.corrupt-20260610`" in approved_text
    assert "- /workspace/runtime.db.corrupt-20260610" in approved_text
    assert denied_text.startswith("Denied. Nullion will wait for your next instruction.")
    assert "About to delete 1 path" in denied_text

    assert web_app._short_error_text(ValueError("")) == "ValueError"
    long_error = ValueError("x" * 300)
    assert web_app._short_error_text(long_error).endswith("...")
    routed = ValueError(
        "Error code: 404 - {'error': {'message': 'No allowed providers are available', "
        "'metadata': {'available_providers': ['a'], 'requested_providers': ['b']}}}"
    )
    assert "restricted to providers" in web_app._short_error_text(routed)
    credit_hint = ValueError("No endpoints found metadata': {'available_providers': ['a']}")
    assert "Check credits" in web_app._short_error_text(credit_hint)
    assert web_app._short_error_text(ValueError("plain")) == "plain"
    assert web_app._is_new_command("/new@bot now")
    assert not web_app._is_new_command("/new@")
    assert web_app._web_task_frame_summary("  hello   world ") == "hello world"
    assert web_app._web_task_frame_summary("", limit=10) == "Web chat turn"
    assert web_app._web_task_frame_summary("abcdefghijk", limit=8) == "abcde..."

    store = FakeStore()
    runtime = FakeRuntime(store)
    store.set_active_task_frame_id("conv", "frame-1")
    web_app._record_web_conversation_reset(runtime, "conv")
    assert runtime.checkpoints == 1
    assert store.get_active_task_frame_id("conv") is None
    web_app._remember_web_chat_turn(
        runtime,
        conversation_id="conv",
        user_message="u",
        assistant_reply="a",
        tool_results=[ToolResult(invocation_id="1", tool_name="tool", status="completed", output={"api_key": "secret"})],
    )
    assert web_app._web_chat_events_after_latest_reset(runtime, "conv")[0]["assistant_reply"] == "a"
    assert web_app._web_chat_events_after_latest_reset(SimpleNamespace(store=None), "conv") == []
    assert web_app._web_chat_history_from_store(runtime, "conv") == [
        {"role": "user", "content": [{"type": "text", "text": "u"}]},
        {"role": "assistant", "content": [{"type": "text", "text": "a"}]},
    ]
    store.add_conversation_event({"conversation_id": "conv", "event_type": "conversation.chat_turn", "user_message": "only user"})
    store.add_conversation_event({
        "conversation_id": "conv",
        "event_type": "conversation.chat_turn",
        "user_message": "[Manual scheduled task run: Hidden] internal cron prompt",
        "assistant_reply": "Cron started",
    })
    assert web_app._web_chat_history_from_store(SimpleNamespace(store=None), "conv") == []
    history = web_app._web_chat_history_from_store(runtime, "conv")
    assert all("Manual scheduled task run" not in item["content"][0]["text"] for item in history)
    assert web_app._is_internal_scheduled_task_context("[Scheduled task: Hidden] internal cron prompt")
    assert web_app._is_internal_scheduled_task_context("[Manual scheduled task run: Hidden] internal cron prompt")
    connector_registry = SimpleNamespace(
        list_specs=lambda: [
            SimpleNamespace(name="delete_cron", capability_tags=("scheduler", "cron")),
        ]
    )
    assert web_app._web_chat_history_turn_limit(
        SimpleNamespace(turn=SimpleNamespace(parent_turn_id=None, disposition="independent")),
        connector_registry,
    ) == 6
    assert web_app._web_chat_history_turn_limit(
        SimpleNamespace(turn=SimpleNamespace(parent_turn_id="turn-0", disposition="independent")),
        connector_registry,
    ) == 8
    assert not web_app._should_schedule_web_no_tool_memory_capture(
        owner="workspace:workspace_admin",
        user_message="[Scheduled task: Hidden] internal cron prompt",
        tool_results=[],
    )
    web_app._WEB_NO_TOOL_MEMORY_CAPTURE_LAST.clear()
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "true")
    monkeypatch.setenv("NULLION_MEMORY_NO_TOOL_CAPTURE_ENABLED", "true")
    monkeypatch.delenv("NULLION_MEMORY_NO_TOOL_MIN_INTERVAL_SECONDS", raising=False)
    assert web_app._should_schedule_web_no_tool_memory_capture(
        owner="workspace:workspace_admin",
        user_message="durable preference should be considered",
        tool_results=[],
        rate_limit_key="memory-test",
    )
    assert web_app._should_schedule_web_no_tool_memory_capture(
        owner="workspace:workspace_admin",
        user_message="another durable preference should also be considered",
        tool_results=[],
        rate_limit_key="memory-test",
    )
    assert "Historical, timestamped tool outcomes" in web_app._recent_web_tool_context_prompt(runtime, "conv")
    assert web_app._recent_web_tool_context_prompt(SimpleNamespace(store=None), "conv") is None
    assert web_app._recent_web_tool_context_prompt(SimpleNamespace(store=FakeStore()), "conv") is None
    assert web_app._compact_web_tool_results_for_context(None) == []
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "off")
    assert web_app._web_memory_context(runtime) is None

    tool_results = [
        ToolResult(invocation_id="1", tool_name="web_fetch", status="completed", output={"url": "https://example.com/a"}),
        ToolResult(invocation_id="2", tool_name="tool", status="failed", output="plain", error="boom"),
        ToolResult(invocation_id="3", tool_name="tool", status="pending", output={}),
        ToolResult(invocation_id="4", tool_name="tool", status="denied", output={"reason": "policy"}),
    ]
    events = web_app._web_activity_events_for_tool_results(tool_results)
    assert [event["status"] for event in events if event["id"].startswith("tool-")] == ["done", "failed", "done", "blocked"]
    assert web_app._web_activity_events_for_tool_results([]) == []
    monkeypatch.setattr(web_app, "is_untrusted_tool_name", lambda name: False)
    assert web_app._web_tool_result_detail(ToolResult("5", "web_search", "completed", {"summary": "hidden"})) is None
    assert web_app._web_tool_result_detail(ToolResult("6", "tool", "completed", {"url": "https://example.com/a"})) == "example.com"
    assert web_app._web_tool_result_detail(ToolResult("7", "tool", "completed", {"summary": "sum"})) == "sum"
    assert web_app._web_tool_result_detail(ToolResult("8", "tool", "completed", "plain text")) == "plain text"
    assert web_app._web_tool_result_detail(ToolResult("9", "tool", "completed", {})) is None

    emitted: list[dict[str, str]] = []
    web_app._emit_activity(None, "none", "None")
    web_app._emit_activity(emitted.append, "x", "Label", "done", "detail")
    web_app._emit_skill_usage_activity(emitted.append, ["Skill A"])
    web_app._emit_skill_usage_activity(emitted.append, [])
    web_app._emit_activity(lambda event: (_ for _ in ()).throw(RuntimeError("boom")), "x", "Label")
    assert emitted[0]["detail"] == "detail"
    assert emitted[-1]["id"] == "skill"
    assert web_app._mini_agent_activity_detail(SimpleNamespace(task_status_detail="ready"), 2) == "ready"
    assert isinstance(web_app._mini_agent_activity_detail(SimpleNamespace(task_titles=["A", "B"]), 2), str)


def test_web_visible_text_sanitizes_raw_browser_extract_dump() -> None:
    raw_text = (
        "Skip to main content\n"
        "Featured Shows\n"
        "Buena Vista Social Club\n"
        "from $63.20\n"
        "Hamilton\n"
        "from $152.32\n"
        "Back to Top\n"
        "Privacy Policy\n"
    )

    visible = web_app._web_http_visible_text(
        {
            "text": raw_text,
            "_delivery_user_message": "Any tickets for Broadway shows tonight?",
            "_delivery_tool_results": [
                ToolResult("extract", "browser_extract_text", "completed", {"text": raw_text}),
            ],
        }
    )

    assert "Skip to main content" not in visible
    assert "Back to Top" not in visible
    assert "- Buena Vista Social Club: from $63.20" in visible
    assert "- Hamilton: from $152.32" in visible


def test_web_visible_text_withholds_short_browser_ui_dump() -> None:
    raw_text = (
        "Best\n\n"
        "26 min\n\n"
        "1h 4m\n\n"
        "2h 49m\n\n"
        "52 min\n\n"
        "Add destination\n\n"
        "Search along the route...\n\n"
        "Gas\n\n"
        "EV charging\n\n"
        "Hotels\n\n"
        "Options"
    )

    visible = web_app._web_http_visible_text(
        {
            "text": raw_text,
            "_delivery_user_message": "Yes",
            "_delivery_tool_results": [
                ToolResult(
                    "nav",
                    "browser_navigate",
                    "completed",
                    {"result": "Navigated to https://maps.example.test/route (status 200)"},
                ),
                ToolResult("extract", "browser_extract_text", "completed", {"text": raw_text}),
            ],
        }
    )

    assert visible == raw_text
    assert "Live-search blocker" not in visible


def test_web_agent_turn_budget_uses_browser_baseline_for_browser_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "12")
    monkeypatch.setenv("NULLION_BROWSER_AGENT_TURN_MAX_ITERATIONS", "24")

    browser_registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            web_action="browser_interaction",
            requested_tool_names=(),
            required_tool_names=(),
        )
    )
    plain_registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            web_action="none",
            requested_tool_names=(),
            required_tool_names=(),
        )
    )

    assert web_app._web_agent_turn_max_iterations(browser_registry) == 24
    assert web_app._web_agent_turn_max_iterations(plain_registry) == 12


def test_web_mini_agent_dispatch_uses_model_structured_dag(monkeypatch) -> None:
    class FakeDagPlanner:
        def __init__(self, *, model_client):
            self.model_client = model_client

        def plan_dag(self, message, *, available_tools):
            return SimpleNamespace(
                can_dispatch=True,
                tasks=[
                    SimpleNamespace(title="Collect source A"),
                    SimpleNamespace(title="Collect source B"),
                ],
            )

    captured: dict[str, object] = {}

    class DispatchingOrchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                dispatched=True,
                task_count=2,
                acknowledgment="delegated",
                planner_summary="Parallel Mission",
            )

    monkeypatch.setattr(web_app, "_feature_enabled", lambda name, default=True: True)
    monkeypatch.setattr(web_app, "TaskDecomposer", FakeDagPlanner)

    result = web_app._try_dispatch_web_mini_agents(
        orchestrator=DispatchingOrchestrator(),
        conversation_id="web:structured",
        principal_id="web:structured",
        user_message="資料 A と資料 B を調べて比較してください",
        tool_registry=ToolRegistry(),
        runtime=SimpleNamespace(store=object()),
        has_attachments=False,
    )

    assert result is not None
    assert result.acknowledgment == "delegated"
    assert result.task_titles == ["Collect source A", "Collect source B"]
    assert captured["dag_plan"].can_dispatch is True
    assert isinstance(captured["available_tools"], list)
    assert captured["single_task_fast_path"] is False


def test_web_mini_agent_dispatch_skips_small_structured_artifact_workflow(monkeypatch) -> None:
    class FakeDagPlanner:
        def __init__(self, *, model_client):
            self.model_client = model_client

        def plan_dag(self, message, *, available_tools):
            return SimpleNamespace(
                can_dispatch=True,
                disposition="sequential_mission",
                tasks=[
                    SimpleNamespace(title="Collect source", tool_scope=["web_search"], dep_indices=[]),
                    SimpleNamespace(title="Create artifact", tool_scope=["pdf_create"], dep_indices=[0]),
                ],
            )

    class DispatchingOrchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            raise AssertionError("small artifact workflow should stay in direct turn")

    monkeypatch.setattr(web_app, "_feature_enabled", lambda name, default=True: True)
    monkeypatch.setattr(web_app, "TaskDecomposer", FakeDagPlanner)

    assert web_app._try_dispatch_web_mini_agents(
        orchestrator=DispatchingOrchestrator(),
        conversation_id="web:artifact",
        principal_id="web:artifact",
        user_message="structured artifact request",
        tool_registry=ToolRegistry(),
        runtime=SimpleNamespace(store=object()),
        has_attachments=False,
        requested_extensions=(".pdf",),
    ) is None


def test_web_mini_agent_dispatch_honors_explicit_planner_single_task(monkeypatch) -> None:
    from nullion.turn_context_policy import TurnToolEvidence

    class FakeDagPlanner:
        def __init__(self, *, model_client):
            self.model_client = model_client

        def plan_dag(self, message, *, available_tools):
            return SimpleNamespace(
                can_dispatch=False,
                can_dispatch_when_requested=True,
                tasks=[SimpleNamespace(title="Build requested PDF")],
            )

    captured: dict[str, object] = {}
    captured_materialize: dict[str, object] = {}

    class DispatchingOrchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                dispatched=True,
                task_count=1,
                acknowledgment=(
                    "Planner: Single Turn • 1 task\n"
                    "For: explicit planner\n"
                    "→ Working on 1 task:\n"
                    "  ☐ Build requested PDF"
                ),
                planner_summary="Single Turn • 1 task",
            )

    monkeypatch.setattr(web_app, "_feature_enabled", lambda name, default=True: True)
    monkeypatch.setattr(web_app, "TaskDecomposer", FakeDagPlanner)
    monkeypatch.setattr(
        web_app,
        "materialize_mini_agent_tool_scope_registry",
        lambda registry, **kwargs: (captured_materialize.update(kwargs) or registry),
    )
    evidence = TurnToolEvidence(requested_extensions=(".xlsx",))

    assert web_app._try_dispatch_web_mini_agents(
        orchestrator=DispatchingOrchestrator(),
        conversation_id="web:planner",
        principal_id="web:planner",
        user_message="explicit planner",
        tool_registry=ToolRegistry(),
        runtime=SimpleNamespace(store=object()),
        has_attachments=False,
        evidence=evidence,
    ) is None

    result = web_app._try_dispatch_web_mini_agents(
        orchestrator=DispatchingOrchestrator(),
        conversation_id="web:planner",
        principal_id="web:planner",
        user_message="explicit planner",
        tool_registry=ToolRegistry(),
        runtime=SimpleNamespace(store=object()),
        has_attachments=False,
        force_dispatch=True,
        evidence=evidence,
    )

    assert result is not None
    assert result.task_titles == ["Build requested PDF"]
    assert captured["dag_plan"].can_dispatch_when_requested is True
    assert captured["single_task_fast_path"] is False
    assert captured_materialize["evidence"] is evidence
    assert captured_materialize["planner_requested"] is True


def test_manual_web_cron_uses_shared_single_agent_runner(tmp_path, monkeypatch) -> None:
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    job = crons.CronJob(
        id="cron-single-fast",
        name="Single fast cron",
        schedule="0 * * * *",
        task="Check one account state and summarize the result.",
        workspace_id="workspace_admin",
        delivery_channel="web",
        delivery_target="web:operator",
    )
    crons.save_crons([job])
    completed = threading.Event()
    captured: dict[str, object] = {}

    class FakeOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            completed.set()
            return SimpleNamespace(
                final_text="single cron done",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

        def dispatch_request_sync(self, **kwargs):  # noqa: ANN003
            raise AssertionError("manual cron should stay on the single-agent lane")


    registry = ToolRegistry()
    web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=FakeOrchestrator(),
        registry=registry,
    )

    result = registry.invoke(ToolInvocation("inv-run", "run_cron", "web:single-fast", {"id": job.id}))

    assert result.status == "completed"
    assert result.output["cron_delivery_status"] == "deferred"
    assert completed.wait(2)
    assert captured["conversation_id"] == "web:single-fast"
    assert captured["principal_id"] == "web:single-fast"
    flow_context = captured["tool_flow_context"]
    assert flow_context["scheduled_task_run"] is True
    assert "isolated_browser_session" not in flow_context
    assert flow_context["browser_session_scope"] == flow_context["browser_session_id"]
    assert flow_context["browser_session_scope"].startswith("cron-cron-single-fast-")
    assert "mini_agent_dispatch" not in result.output
    assert result.output["task_group_id"].startswith(f"grp-{job.id}-")
    assert "⏰ SCHEDULED TASK: Single fast cron" in result.output["progress_status_text"]
    assert "◐ Single fast cron" in result.output["progress_status_text"]


def test_manual_web_cron_ignores_multi_task_preview_and_stays_single_agent(tmp_path, monkeypatch) -> None:
    from nullion import crons
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    job = crons.CronJob(
        id="cron-multi-deep",
        name="Multi deep cron",
        schedule="0 * * * *",
        task="Create a two-step scheduled report.",
        workspace_id="workspace_admin",
        delivery_channel="web",
        delivery_target="web:operator",
    )
    crons.save_crons([job])
    completed = threading.Event()
    captured: dict[str, object] = {}

    class FakeOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            completed.set()
            return SimpleNamespace(
                final_text="multi-step cron done",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

        def dispatch_request_sync(self, **kwargs):  # noqa: ANN003
            raise AssertionError("cron must not use DeepAgent dispatch")

    registry = ToolRegistry()
    web_app.create_app(
        bootstrap_persistent_runtime(tmp_path / "runtime.db"),
        orchestrator=FakeOrchestrator(),
        registry=registry,
    )

    result = registry.invoke(ToolInvocation("inv-run", "run_cron", "web:multi-deep", {"id": job.id}))

    assert result.status == "completed"
    assert result.output["cron_delivery_status"] == "deferred"
    assert completed.wait(2)
    assert captured["conversation_id"] == "web:multi-deep"
    assert "mini_agent_dispatch" not in result.output
    assert result.output["task_group_id"].startswith(f"grp-{job.id}-")
    assert "⏰ SCHEDULED TASK: Multi deep cron" in result.output["progress_status_text"]
    assert "◐ Multi deep cron" in result.output["progress_status_text"]


def test_web_attachment_extension_preflight_uses_literal_evidence_without_model(monkeypatch) -> None:
    calls: list[object | None] = []

    def fake_plan_attachment_format(text, *, model_client=None):  # noqa: ANN001
        calls.append(model_client)
        return SimpleNamespace(extension=".pdf" if model_client is None else ".xlsx")

    monkeypatch.delenv("NULLION_WEB_ATTACHMENT_FORMAT_MODEL_PLANNING_ENABLED", raising=False)
    monkeypatch.setattr(web_app, "plan_attachment_format", fake_plan_attachment_format)

    assert web_app._requested_web_turn_attachment_extension("artifact.pdf", model_client=object()) == ".pdf"
    assert web_app._requested_web_turn_attachment_extension("/Users/example/Downloads/file.pdf", model_client=object()) is None
    assert calls == [None]


def test_web_attachment_extension_preflight_does_not_model_plan_for_plain_text(monkeypatch) -> None:
    calls: list[object | None] = []
    model_client = object()

    def fake_plan_attachment_format(text, *, model_client=None):  # noqa: ANN001
        calls.append(model_client)
        if model_client is None:
            return SimpleNamespace(extension=None)
        return SimpleNamespace(extension=f".{str(text or '').split()[-1]}")

    monkeypatch.setenv("NULLION_WEB_ATTACHMENT_FORMAT_MODEL_PLANNING_ENABLED", "true")
    monkeypatch.setattr(web_app, "plan_attachment_format", fake_plan_attachment_format)

    assert web_app._requested_web_turn_attachment_extension("quick hello", model_client=model_client, allow_model_planning=True) is None
    assert web_app._requested_web_turn_attachment_extension("create pdf", model_client=model_client, allow_model_planning=True) == ".pdf"
    assert web_app._requested_web_turn_attachment_extension(
        "quick hello",
        model_client=model_client,
        allow_model_planning=True,
        require_attachment_token_for_model_planning=False,
    ) == ".hello"
    assert calls == [None, None, model_client, None, model_client]


def test_web_attachment_extension_preflight_allows_structured_url_signal(monkeypatch) -> None:
    calls: list[object | None] = []
    model_client = object()

    def fake_plan_attachment_format(text, *, model_client=None):  # noqa: ANN001
        calls.append(model_client)
        if model_client is None:
            return SimpleNamespace(extension=None)
        return SimpleNamespace(extension=".xlsx")

    monkeypatch.setattr(web_app, "plan_attachment_format", fake_plan_attachment_format)

    assert web_app._requested_web_turn_attachment_extension(
        "Build the requested sheet from https://example.test/catalog",
        model_client=model_client,
        allow_model_planning=True,
    ) == ".xlsx"
    assert calls == [None, model_client]


def test_web_turn_skip_tool_scope_decision_uses_structured_evidence_only(monkeypatch) -> None:
    evidence = SimpleNamespace(
        has_url_target=False,
        has_attachments=False,
        artifact_requested=False,
        context_linked=False,
    )

    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    ) is True

    monkeypatch.setenv("NULLION_WEB_TOOL_SCOPE_DECISION_ENABLED", "false")
    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    ) is False


def test_web_turn_skip_tool_scope_decision_rejects_structured_turns() -> None:
    evidence = SimpleNamespace(
        has_url_target=True,
        has_attachments=False,
        artifact_requested=False,
        context_linked=False,
    )

    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    ) is False


def test_web_turn_skip_tool_scope_decision_does_not_branch_on_arbitrary_prompt_shape() -> None:
    evidence = SimpleNamespace(
        has_url_target=False,
        has_attachments=False,
        artifact_requested=False,
        context_linked=False,
    )

    assert "user_message" in inspect.signature(web_app._web_turn_skip_tool_scope_decision).parameters
    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        user_message="ordinary unmatched prompt",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    ) is True


def test_web_turn_skip_tool_scope_decision_uses_cached_active_connector_app_ids(monkeypatch) -> None:
    from nullion import turn_context_policy

    evidence = SimpleNamespace(
        has_url_target=False,
        has_attachments=False,
        artifact_requested=False,
        context_linked=False,
    )
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        lambda *args, **kwargs: ({"active_app_ids": ("example-drive",)},),
    )

    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        user_message="acceso a example-drive?",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    ) is False


def test_web_turn_skip_tool_scope_decision_uses_overrides() -> None:
    evidence = SimpleNamespace(
        has_url_target=False,
        has_attachments=False,
        artifact_requested=False,
        context_linked=False,
    )
    dispatch_context = SimpleNamespace(dependency_turn_ids=["previous-turn"])

    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        config_action="set_config",
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    ) is False
    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        config_action=None,
        allow_mini_agents=True,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    ) is False
    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=True,
        turn_dispatch_decision=None,
    ) is False
    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        user_message="hi",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=dispatch_context,
    ) is False


def test_web_task_frame_start_checkpoint_respects_feature_flag(monkeypatch) -> None:
    import nullion.runtime

    def fake_process_conversation_message(
        store,  # noqa: ARG001
        conversation_id: str,  # noqa: ARG001
        user_message: str,  # noqa: ARG001
        request_id: str | None = None,
        message_id: str | None = None,
        turn_id: str | None = None,
        **kwargs,  # noqa: ARG001
    ) -> SimpleNamespace:
        _ = (request_id, message_id, kwargs)
        turn = SimpleNamespace(
            turn_id=turn_id or "override-turn",
            branch_id="branch-start",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        return SimpleNamespace(turn=turn)

    monkeypatch.setattr(nullion.runtime, "process_conversation_message", fake_process_conversation_message)

    store = FakeStore()
    runtime = FakeRuntime(store)
    with_store = web_app._start_web_task_frame(
        runtime,
        conversation_id="web:checkpoint",
        user_text="hello",
        turn_id="turn-hello",
    )
    _wait_for_checkpoints(runtime, 1)
    assert runtime.checkpoints == 1
    assert with_store[0] is not None

    monkeypatch.setenv("NULLION_WEB_TASK_FRAME_START_CHECKPOINT_ENABLED", "false")
    store = FakeStore()
    runtime = FakeRuntime(store)
    disabled = web_app._start_web_task_frame(
        runtime,
        conversation_id="web:checkpoint",
        user_text="hello",
        turn_id="turn-hello",
    )
    assert runtime.checkpoints == 0
    assert disabled[0] is not None


def test_web_task_frame_finish_checkpoint_respects_feature_flag(monkeypatch) -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="web:checkpoint",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(requires_attempt=True),
        summary="checkpointed",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_activity_turn_id="turn-1",
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:checkpoint", frame.frame_id)

    web_app._finish_web_task_frame(
        runtime,
        conversation_id="web:checkpoint",
        frame_id="frame-1",
        status=TaskFrameStatus.COMPLETED,
        completion_turn_id="turn-2",
    )
    finished = store.get_task_frame("frame-1")
    assert finished is not None
    assert finished.status is TaskFrameStatus.COMPLETED
    assert runtime.checkpoints == 1
    assert store.get_active_task_frame_id("web:checkpoint") is None

    monkeypatch.setenv("NULLION_WEB_TASK_FRAME_FINISH_CHECKPOINT_ENABLED", "false")
    disabled_store = FakeStore()
    disabled_runtime = FakeRuntime(disabled_store)
    disabled_frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="web:checkpoint",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(requires_attempt=True),
        summary="checkpointed",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_activity_turn_id="turn-1",
    )
    disabled_store.add_task_frame(disabled_frame)
    disabled_store.set_active_task_frame_id("web:checkpoint", disabled_frame.frame_id)
    web_app._finish_web_task_frame(
        disabled_runtime,
        conversation_id="web:checkpoint",
        frame_id="frame-1",
        status=TaskFrameStatus.COMPLETED,
        completion_turn_id="turn-2",
    )
    assert disabled_runtime.checkpoints == 0


def test_web_task_frame_finish_falls_back_to_active_frame() -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-active",
        conversation_id="web:checkpoint",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(requires_attempt=True),
        summary="checkpointed",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_activity_turn_id="turn-1",
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:checkpoint", frame.frame_id)

    web_app._finish_web_task_frame(
        runtime,
        conversation_id="web:checkpoint",
        frame_id="frame-stale",
        status=TaskFrameStatus.COMPLETED,
        completion_turn_id="turn-2",
    )

    finished = store.get_task_frame("frame-active")
    assert finished is not None
    assert finished.status is TaskFrameStatus.COMPLETED
    assert finished.completion_turn_id == "turn-2"
    assert store.get_active_task_frame_id("web:checkpoint") is None


def test_web_task_frame_finish_writes_terminal_state_to_checkpoint(tmp_path, monkeypatch) -> None:
    import json
    import sqlite3

    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    frame = TaskFrame(
        frame_id="frame-persist",
        conversation_id="web:checkpoint",
        branch_id="branch-persist",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(requires_attempt=True),
        summary="checkpointed",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        last_activity_turn_id="turn-1",
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("web:checkpoint", frame.frame_id)
    runtime.checkpoint(force=True)
    monkeypatch.setattr(web_app, "_schedule_web_checkpoint", lambda *args, **kwargs: None)

    web_app._finish_web_task_frame(
        runtime,
        conversation_id="web:checkpoint",
        frame_id="frame-persist",
        status=TaskFrameStatus.COMPLETED,
        completion_turn_id="turn-2",
    )

    with sqlite3.connect(str(runtime.checkpoint_path)) as conn:
        row = conn.execute(
            "SELECT payload FROM tasks_missions WHERE collection = ? AND item_key = ?",
            ("task_frames", "branch-persist"),
        ).fetchone()
        active = conn.execute(
            "SELECT COUNT(*) FROM tasks_missions WHERE collection = ? AND item_key = ?",
            ("active_task_frames", "web:checkpoint"),
        ).fetchone()[0]
    assert row is not None
    payload = json.loads(row[0])
    assert payload["status"] == "completed"
    assert payload["completion_turn_id"] == "turn-2"
    assert active == 0


def test_remember_web_chat_turn_checkpoint_respects_feature_flag(monkeypatch) -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    web_app._remember_web_chat_turn(
        runtime,
        conversation_id="conv-checkpoint",
        user_message="u",
        assistant_reply="a",
    )
    assert runtime.checkpoints == 1
    assert runtime.store.events[-1]["event_type"] == "conversation.chat_turn"
    assert str(runtime.store.events[-1]["turn_id"]).startswith("turn-web-")

    monkeypatch.setenv("NULLION_WEB_CHAT_TURN_CHECKPOINT_ENABLED", "false")
    checkpoint_disabled_runtime = FakeRuntime(FakeStore())
    web_app._remember_web_chat_turn(
        checkpoint_disabled_runtime,
        conversation_id="conv-checkpoint",
        user_message="u",
        assistant_reply="a",
    )
    assert checkpoint_disabled_runtime.checkpoints == 0


def test_remember_web_chat_turn_records_stable_turn_id(monkeypatch) -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_web")

    web_app._remember_web_chat_turn(
        runtime,
        conversation_id="conv",
        conversation_turn_id="turn-1",
        user_message="u",
        assistant_reply="a",
    )
    web_app._remember_web_chat_turn(
        runtime,
        conversation_id="conv",
        conversation_turn_id="turn-2",
        user_message="u2",
        assistant_reply="a2",
    )
    events = [event for event in runtime.store.events if event.get("event_type") == "conversation.chat_turn"]

    assert [events[0]["turn_id"], events[1]["turn_id"]] == ["turn-1", "turn-2"]
    assert [events[0]["workspace_id"], events[1]["workspace_id"]] == ["workspace_web", "workspace_web"]


def test_web_interactive_fast_profile_uses_low_reasoning_for_lightweight_turn(monkeypatch) -> None:
    from dataclasses import dataclass

    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.turn_context_policy import build_turn_tool_evidence

    @dataclass(slots=True)
    class Client:
        model: str = "gpt-slower"
        reasoning_effort: str | None = "medium"

    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            allow_web_tools=False,
            allow_scheduler_tools=False,
            allow_connector_tools=False,
            allow_skill_pack_tools=False,
        )
    )
    evidence = build_turn_tool_evidence(user_message="status ping", conversation_result=None)
    orchestrator = AgentOrchestrator(model_client=Client())

    monkeypatch.delenv("NULLION_WEB_INTERACTIVE_FAST_MODEL", raising=False)
    monkeypatch.setenv("NULLION_WEB_INTERACTIVE_FAST_REASONING_EFFORT", "low")

    assert web_app._web_turn_fast_profile_candidate(
        evidence=evidence,
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    )
    profiled = web_app._orchestrator_with_interactive_fast_profile(
        orchestrator,
        enabled=True,
        tool_registry=registry,
    )

    assert profiled is not orchestrator
    assert profiled.model_client.reasoning_effort == "low"
    assert orchestrator.model_client.reasoning_effort == "medium"


def test_web_interactive_fast_profile_ignores_active_connector_inventory_for_plain_turn(monkeypatch) -> None:
    from nullion import turn_context_policy
    from nullion.turn_context_policy import build_turn_tool_evidence

    evidence = build_turn_tool_evidence(user_message="hi", conversation_result=None)

    monkeypatch.setattr(turn_context_policy, "_runtime_has_active_connector_provider", lambda: True)
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: (_ for _ in ()).throw(AssertionError("plain turns must not load connector inventory")),
    )

    assert web_app._web_turn_fast_profile_candidate(
        evidence=evidence,
        user_message="hi",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    )
    assert web_app._web_turn_skip_tool_scope_decision(
        evidence=evidence,
        user_message="hi",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    )


def test_web_interactive_fast_profile_stays_on_full_client_for_special_tool_plan() -> None:
    from dataclasses import dataclass

    from nullion.agent_orchestrator import AgentOrchestrator

    @dataclass(slots=True)
    class Client:
        model: str = "gpt-slower"
        reasoning_effort: str | None = "medium"

    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            allow_web_tools=True,
            allow_scheduler_tools=False,
            allow_connector_tools=False,
            allow_skill_pack_tools=False,
        )
    )
    orchestrator = AgentOrchestrator(model_client=Client())

    assert web_app._orchestrator_with_interactive_fast_profile(
        orchestrator,
        enabled=True,
        tool_registry=registry,
    ) is orchestrator


def test_dead_task_frame_cleanup_cancels_and_records_doctor_item() -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-dead",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="stuck approval task",
        created_at=datetime.now(UTC) - timedelta(minutes=5),
        updated_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)

    cleaned = web_app._cleanup_dead_task_frames(runtime)

    assert cleaned == [
        {
            "frame_id": "frame-dead",
            "status": "cancelled",
            "summary": "stuck approval task",
            "message": "Killed task frame frame-dead.",
        }
    ]
    assert store.get_task_frame("frame-dead").status is TaskFrameStatus.CANCELLED
    assert store.get_active_task_frame_id("web:operator") is None
    action = store.get_doctor_action("act-dead-task-frame-dead")
    assert action["status"] == "completed"
    assert action["recommendation_code"] == "cleanup_dead_task_frame"
    assert "dead_task_frame" in action["source_reason"]
    assert runtime.checkpoints == 1


def test_dead_task_frame_cleanup_leaves_fresh_waiting_approval_alone() -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-fresh",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="fresh approval wait",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)

    assert web_app._cleanup_dead_task_frames(runtime) == []
    assert store.get_task_frame("frame-fresh").status is TaskFrameStatus.WAITING_APPROVAL
    assert store.get_active_task_frame_id("web:operator") == "frame-fresh"
    assert store.doctor_actions == {}


def test_dead_task_frame_cleanup_leaves_live_pending_approval_alone() -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-live",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="live approval wait",
        created_at=datetime.now(UTC) - timedelta(minutes=5),
        updated_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)
    store.approvals["ap-live"] = SimpleNamespace(
        approval_id="ap-live",
        requested_by="web:operator",
        status=SimpleNamespace(value="pending"),
        context={},
    )

    assert web_app._cleanup_dead_task_frames(runtime) == []
    assert store.get_task_frame("frame-live").status is TaskFrameStatus.WAITING_APPROVAL
    assert store.get_active_task_frame_id("web:operator") == "frame-live"
    assert store.doctor_actions == {}


def test_dead_task_frame_cleanup_cancels_stale_web_running_frame(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_STALE_WEB_TASK_FRAME_SECONDS", "120")
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-stale",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="stale web task",
        created_at=datetime.now(UTC) - timedelta(minutes=10),
        updated_at=datetime.now(UTC) - timedelta(minutes=10),
        last_activity_turn_id="turn",
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)

    cleaned = web_app._cleanup_dead_task_frames(runtime)

    assert cleaned == [
        {
            "frame_id": "frame-stale",
            "status": "cancelled",
            "summary": "stale web task",
            "message": "Killed task frame frame-stale.",
        }
    ]
    assert store.get_task_frame("frame-stale").status is TaskFrameStatus.CANCELLED
    assert store.get_active_task_frame_id("web:operator") is None
    action = store.get_doctor_action("act-dead-task-frame-stale")
    assert "stale-frame timeout" in action["source_reason"]


def test_dead_task_frame_cleanup_cancels_stale_legacy_live_web_frame(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_STALE_WEB_TASK_FRAME_SECONDS", "120")
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-legacy-live-web",
        conversation_id="live-prod-web",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="stale legacy web task",
        created_at=datetime.now(UTC) - timedelta(minutes=10),
        updated_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("live-prod-web", frame.frame_id)

    cleaned = web_app._cleanup_dead_task_frames(runtime)

    assert cleaned == [
        {
            "frame_id": "frame-legacy-live-web",
            "status": "cancelled",
            "summary": "stale legacy web task",
            "message": "Killed task frame frame-legacy-live-web.",
        }
    ]
    assert store.get_task_frame("frame-legacy-live-web").status is TaskFrameStatus.CANCELLED
    assert store.get_active_task_frame_id("live-prod-web") is None
    action = store.get_doctor_action("act-dead-task-frame-legacy-live-web")
    assert "stale-frame timeout" in action["source_reason"]


def test_dead_task_frame_cleanup_leaves_fresh_web_running_frame(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_STALE_WEB_TASK_FRAME_SECONDS", "120")
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-fresh-running",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.RUNNING,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="fresh web task",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        last_activity_turn_id="turn",
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)

    assert web_app._cleanup_dead_task_frames(runtime) == []
    assert store.get_task_frame("frame-fresh-running").status is TaskFrameStatus.RUNNING
    assert store.get_active_task_frame_id("web:operator") == "frame-fresh-running"
    assert store.doctor_actions == {}


def test_dead_task_frame_cleanup_cancels_stale_waiting_input_frame(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_STALE_WEB_TASK_FRAME_SECONDS", "120")
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="frame-waiting-input",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_INPUT,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="stale waiting input task",
        created_at=datetime.now(UTC) - timedelta(minutes=10),
        updated_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:operator", frame.frame_id)

    cleaned = web_app._cleanup_dead_task_frames(runtime)

    assert cleaned[0]["frame_id"] == "frame-waiting-input"
    assert store.get_task_frame("frame-waiting-input").status is TaskFrameStatus.CANCELLED
    assert store.get_active_task_frame_id("web:operator") is None


def test_dead_task_frame_cleanup_cancels_stale_planner_clarification_waiting_input(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_STALE_WAITING_INPUT_TASK_FRAME_SECONDS", "120")
    store = FakeStore()
    runtime = FakeRuntime(store)
    frame = TaskFrame(
        frame_id="planner-clarification-deadbeef",
        conversation_id="telegram:100001",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_INPUT,
        operation=TaskFrameOperation.GENERATE_ARTIFACT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="Need trip destination details",
        metadata={"planner_waiting_for_clarification": True},
        created_at=datetime.now(UTC) - timedelta(minutes=10),
        updated_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("telegram:100001", frame.frame_id)

    cleaned = web_app._cleanup_dead_task_frames(runtime)

    assert cleaned[0]["frame_id"] == "planner-clarification-deadbeef"
    assert store.get_task_frame("planner-clarification-deadbeef").status is TaskFrameStatus.CANCELLED
    assert store.get_active_task_frame_id("telegram:100001") is None


def test_active_cron_delivery_tasks_uses_latest_non_terminal_events(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_DASHBOARD_ACTIVE_CRON_DELIVERY_MAX_AGE_SECONDS", "7200")
    now = datetime(2026, 5, 27, 12, 30, tzinfo=UTC)
    store = FakeStore()
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.started",
            created_at=now - timedelta(hours=3),
            payload={"cron_id": "old-cron", "cron_name": "Old cron", "delivery_channel": "telegram"},
        )
    )
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.started",
            created_at=now - timedelta(minutes=3),
            payload={"cron_id": "sent-cron", "cron_name": "Sent cron", "delivery_channel": "telegram"},
        )
    )
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.sent",
            created_at=now - timedelta(minutes=2),
            payload={"cron_id": "sent-cron", "cron_name": "Sent cron", "delivery_channel": "telegram"},
        )
    )
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.agent_preflight",
            created_at=now - timedelta(minutes=4),
            payload={"cron_id": "running-cron", "cron_name": "Running cron", "delivery_channel": "telegram"},
        )
    )
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.blocked",
            created_at=now - timedelta(minutes=1),
            payload={"cron_id": "blocked-cron", "cron_name": "Blocked cron", "delivery_channel": "telegram"},
        )
    )

    active = web_app._active_cron_delivery_tasks(store, now=now)
    by_id = {item["cron_id"]: item for item in active}

    assert "running-cron" in by_id
    assert "blocked-cron" in by_id
    assert "sent-cron" not in by_id
    assert "old-cron" not in by_id
    assert by_id["running-cron"]["status"] == "running"
    assert by_id["blocked-cron"]["status"] == "blocked"


def test_active_cron_delivery_tasks_drops_default_stale_non_terminal_events(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_DASHBOARD_ACTIVE_CRON_DELIVERY_MAX_AGE_SECONDS", raising=False)
    now = datetime(2026, 5, 27, 12, 30, tzinfo=UTC)
    store = FakeStore()
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.agent_preflight",
            created_at=now - timedelta(minutes=31),
            payload={"cron_id": "stale-cron", "cron_name": "Stale cron", "delivery_channel": "telegram"},
        )
    )
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.agent_preflight",
            created_at=now - timedelta(minutes=29),
            payload={"cron_id": "running-cron", "cron_name": "Running cron", "delivery_channel": "telegram"},
        )
    )

    active = web_app._active_cron_delivery_tasks(store, now=now)
    by_id = {item["cron_id"]: item for item in active}

    assert "stale-cron" not in by_id
    assert by_id["running-cron"]["status"] == "running"


def test_run_web_server_bounds_graceful_shutdown(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(app, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(web_app, "_build_runtime", lambda: (object(), object(), object()))
    monkeypatch.setattr(web_app, "create_app", lambda runtime, orchestrator, registry: object())
    monkeypatch.setattr("uvicorn.run", fake_run)

    web_app._run_web_server(SimpleNamespace(host="127.0.0.1", port=8742, reload=False))

    assert calls == [
        {
            "host": "127.0.0.1",
            "port": 8742,
            "reload": False,
            "timeout_graceful_shutdown": 5,
        }
    ]


def test_cron_delivery_task_kill_endpoint_cancels_and_records_terminal_event(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    now = datetime.now(UTC)
    store = FakeStore()
    runtime = FakeRuntime(store)
    runtime.checkpoint_path = tmp_path / "runtime.db"
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.agent_preflight",
            created_at=now,
            payload={
                "cron_id": "running-cron",
                "cron_name": "Running cron",
                "delivery_channel": "telegram",
                "delivery_target": "100001",
                "conversation_id": "telegram:100001",
            },
        )
    )
    calls = []

    def fake_cancel_manual_cron_background_run(**kwargs):  # noqa: ANN003
        calls.append(kwargs)
        return 1

    monkeypatch.setattr(web_app, "cancel_manual_cron_background_run", fake_cancel_manual_cron_background_run)
    app = web_app.create_app(runtime, orchestrator=None, registry=ToolRegistry())

    response = TestClient(app).post("/api/tasks/cron-delivery/cron-delivery:running-cron/kill")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["cron_id"] == "running-cron"
    assert data["status"] == "cancelled"
    assert calls == [{"task_group_id": "cron-delivery:running-cron", "cron_id": "running-cron"}]
    assert store.runtime_events[-1].event_type == "cron.delivery.cancelled"
    active_after_cancel = web_app._active_cron_delivery_tasks(store, now=datetime.now(UTC))
    assert all(item["cron_id"] != "running-cron" for item in active_after_cancel)


def test_dashboard_cleanup_prunes_old_and_duplicate_records(monkeypatch) -> None:
    from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus

    monkeypatch.setenv("NULLION_DOCTOR_ACTION_HISTORY_RETENTION_SECONDS", "60")
    monkeypatch.setenv("NULLION_DOCTOR_OPEN_RETENTION_SECONDS", "3600")
    monkeypatch.setenv("NULLION_TASK_HISTORY_RETENTION_SECONDS", "60")
    monkeypatch.setenv("NULLION_MINI_AGENT_HISTORY_RETENTION_SECONDS", "60")
    now = datetime(2026, 5, 7, 12, 0, tzinfo=UTC)
    old = (now - timedelta(hours=2)).isoformat()
    newest = (now - timedelta(minutes=5)).isoformat()
    store = FakeStore()
    runtime = FakeRuntime(store)
    base_action = {
        "owner": "doctor",
        "status": "pending",
        "action_type": "inspect",
        "recommendation_code": "investigate_stall",
        "summary": "Stalled workflow detected",
        "severity": "medium",
        "reason": None,
        "source_reason": "source=web;issue_type=stalled",
        "error": None,
    }
    store.add_doctor_action({**base_action, "action_id": "act-new", "created_at": newest, "updated_at": newest})
    store.add_doctor_action({**base_action, "action_id": "act-duplicate", "created_at": old, "updated_at": old})
    store.add_doctor_action({
        **base_action,
        "action_id": "act-closed",
        "status": "completed",
        "created_at": old,
        "updated_at": old,
    })
    closed_frame = TaskFrame(
        frame_id="frame-closed",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.CANCELLED,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="closed task",
        created_at=now - timedelta(hours=2),
        updated_at=now - timedelta(hours=2),
    )
    store.add_task_frame(closed_frame)
    store.set_active_task_frame_id("web:operator", closed_frame.frame_id)
    store.add_mini_agent_run(MiniAgentRun("run-closed", "group", "research", MiniAgentRunStatus.FAILED, now - timedelta(hours=2), "done"))

    counts = web_app._cleanup_old_dashboard_records(runtime, now=now)

    assert counts == {"doctor_actions": 2, "task_frames": 1, "mini_agent_runs": 1}
    assert store.get_doctor_action("act-new") is not None
    assert store.get_doctor_action("act-duplicate") is None
    assert store.get_doctor_action("act-closed") is None
    assert store.get_task_frame("frame-closed") is None
    assert store.get_active_task_frame_id("web:operator") is None
    assert store.list_mini_agent_runs() == []
    assert runtime.checkpoints == 1


def test_suspended_turn_config_feature_and_credentials_helpers(monkeypatch, tmp_path) -> None:
    store = FakeStore()
    runtime = FakeRuntime(store)
    web_app._store_web_screenshot_suspended_turn(
        runtime,
        approval_id="approval-1",
        conversation_id="conv",
        user_text="screenshot https://example.com",
    )
    assert store.suspended[0].approval_id == "approval-1"
    assert runtime.checkpoints == 1

    monkeypatch.delenv("NULLION_MEMORY_ENABLED", raising=False)
    assert web_app._feature_enabled("NULLION_MEMORY_ENABLED") is True
    monkeypatch.delenv("NULLION_SMART_CLEANUP_ENABLED", raising=False)
    assert web_app._smart_cleanup_enabled() is True
    monkeypatch.setenv("NULLION_SMART_CLEANUP_ENABLED", "off")
    assert web_app._smart_cleanup_enabled() is False
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "off")
    assert web_app._feature_enabled("NULLION_MEMORY_ENABLED") is False


    web_app._remember_web_explicit_memory(runtime, user_message="remember this")

    with monkeypatch.context() as browser_patch:
        browser_patch.setenv("NULLION_BROWSER_ENABLED", "false")
        assert web_app._resolve_browser_backend() is None
        browser_patch.setenv("NULLION_BROWSER_ENABLED", "true")
        browser_patch.setattr("nullion.auth.load_stored_credentials", lambda: {}, raising=False)
        browser_patch.setenv("NULLION_BROWSER_BACKEND", "cdp")
        assert web_app._resolve_browser_backend() == "cdp"
        browser_patch.delenv("NULLION_BROWSER_BACKEND", raising=False)
        browser_patch.setenv("NULLION_PLUGINS", "search,browser")
        assert web_app._resolve_browser_backend() == "auto"
        browser_patch.delenv("NULLION_PLUGINS", raising=False)
        browser_patch.setenv("NULLION_ENABLED_PLUGINS", "search_plugin,browser_plugin,workspace_plugin")
        browser_patch.setenv("NULLION_BROWSER_PREFERRED", "brave")
        assert web_app._resolve_browser_backend() == "auto"

    monkeypatch.delenv("NULLION_MODEL_PROVIDER", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("NULLION_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert web_app._env_model_provider_configured() is False
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "openai")
    assert web_app._env_model_provider_configured() is True
    monkeypatch.setattr(web_app, "_build_openai_client", lambda: "openai-client")
    monkeypatch.setattr(web_app, "_build_anthropic_client", lambda: "anthropic-client")
    assert web_app._build_model_client_from_env() == "openai-client"
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "anthropic")
    assert web_app._build_model_client_from_env() == "anthropic-client"
    monkeypatch.delenv("NULLION_MODEL_PROVIDER", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("NULLION_OPENAI_API_KEY", "sk-nullion")
    assert web_app._env_model_provider_configured() is True
    assert web_app._build_model_client_from_env() == "openai-client"
    monkeypatch.delenv("NULLION_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        web_app._build_model_client_from_env()
    monkeypatch.setattr(sys, "argv", [])
    assert web_app._current_process_restart_command() == [sys.executable, "-m", "nullion.web_app"]
    monkeypatch.setattr(sys, "argv", ["-m", "nullion.web_app"])
    assert web_app._current_process_restart_command() == [sys.executable, "-m", "nullion.web_app"]
    monkeypatch.setattr(sys, "argv", ["nullion-web", "--port", "1"])
    assert web_app._current_process_restart_command() == [sys.executable, "nullion-web", "--port", "1"]
    assert web_app._codex_reauth_command() == [sys.executable, "-u", "-m", "nullion.auth", "--reauth", "codex"]

    creds_path = tmp_path / "credentials.json"
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", creds_path)
    web_app._write_credentials_json({"provider": "codex"})
    assert web_app._read_credentials_json() == {"provider": "codex"}
    creds_path.write_text("{bad", encoding="utf-8")
    assert web_app._read_credentials_json() == {"provider": "codex"}

    env_path = tmp_path / ".env"
    env_path.write_text("# comment\nA=\"old\"\nB=keep\n", encoding="utf-8")
    web_app._write_env_updates(env_path, {"A": "new", "C": "added"})
    assert env_path.read_text(encoding="utf-8") == '# comment\nA="new"\nB=keep\nC="added"\n'
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / "nested" / ".env"))
    assert web_app._find_env_path() == tmp_path / "nested" / ".env"
    monkeypatch.delenv("NULLION_ENV_FILE", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app.os.path, "expanduser", lambda path: str(tmp_path / path.removeprefix("~/")))
    assert web_app._find_env_path() == web_app.Path(".env")
    env_path.unlink()
    default_env = web_app._find_env_path()
    assert default_env == tmp_path / ".nullion" / ".env"


def test_doctor_data_cleanup_merges_builder_duplicates_without_age_retention(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_DATA_RETENTION_DAYS", raising=False)
    store = RuntimeStore()
    first = create_skill_result(
        store,
        title="Trigger named cron",
        summary="Captured from: Trigger one cron",
        trigger="Use when the user wants to manually run a named cron.",
        steps=["List crons"],
        tags=["cron"],
        actor="runtime",
    )
    duplicate = create_skill_result(
        store,
        title="Trigger named cron",
        summary="Captured from: Trigger another cron",
        trigger="Use when the user wants to manually run a named cron.",
        steps=["Run selected cron"],
        tags=["scheduled"],
        actor="runtime",
    )
    store.skills["legacy-duplicate"] = SkillRecord(
        skill_id="legacy-duplicate",
        title="Trigger named cron",
        summary="Captured from: Trigger another cron again",
        trigger="Use when the user wants to manually run a named cron.",
        steps=["Run selected cron"],
        tags=["legacy"],
        created_at=datetime.now(UTC) - timedelta(days=30),
        updated_at=datetime.now(UTC) - timedelta(days=30),
    )
    store.add_conversation_event({
        "conversation_id": "web:operator",
        "event_type": "conversation.chat_turn",
        "created_at": (datetime.now(UTC) - timedelta(days=365)).isoformat(),
        "user_message": "old",
        "assistant_reply": "old",
    })
    checkpoints = {"count": 0}
    runtime = SimpleNamespace(store=store, checkpoint=lambda: checkpoints.__setitem__("count", checkpoints["count"] + 1))

    counts = web_app._run_doctor_data_cleanup(runtime, now=datetime.now(UTC))

    assert first.created is True
    assert duplicate.created is False
    assert counts["skills"] == 1
    assert counts["chat_events"] == 0
    assert len(store.skills) == 1
    assert "legacy" in next(iter(store.skills.values())).tags
    assert len(store.conversation_events) == 1


def test_doctor_data_cleanup_respects_explicit_retention_days(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_DATA_RETENTION_DAYS", "30")
    monkeypatch.delenv("NULLION_DATA_RETENTION_TYPES", raising=False)
    store = RuntimeStore()
    now = datetime.now(UTC)
    store.add_conversation_event({
        "conversation_id": "web:operator",
        "event_type": "conversation.chat_turn",
        "created_at": (now - timedelta(days=31)).isoformat(),
        "user_message": "old",
        "assistant_reply": "old",
    })
    store.add_conversation_event({
        "conversation_id": "web:operator",
        "event_type": "conversation.chat_turn",
        "created_at": (now - timedelta(days=1)).isoformat(),
        "user_message": "new",
        "assistant_reply": "new",
    })
    checkpoints = {"count": 0}
    runtime = SimpleNamespace(store=store, checkpoint=lambda: checkpoints.__setitem__("count", checkpoints["count"] + 1))

    counts = web_app._run_doctor_data_cleanup(runtime, now=now)

    assert counts["chat_events"] == 1
    assert len(store.conversation_events) == 1
    assert store.conversation_events[0]["user_message"] == "new"
    assert checkpoints["count"] == 1


def test_doctor_data_cleanup_respects_selected_retention_types(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_DATA_RETENTION_DAYS", "30")
    monkeypatch.setenv("NULLION_DATA_RETENTION_TYPES", "builder")
    store = RuntimeStore()
    now = datetime.now(UTC)
    store.add_conversation_event({
        "conversation_id": "web:operator",
        "event_type": "conversation.chat_turn",
        "created_at": (now - timedelta(days=90)).isoformat(),
        "user_message": "old",
        "assistant_reply": "old",
    })
    runtime = SimpleNamespace(store=store, checkpoint=lambda: None)

    counts = web_app._run_doctor_data_cleanup(runtime, now=now)

    assert counts["chat_events"] == 0
    assert len(store.conversation_events) == 1


def test_turn_memory_capture_event_is_persisted_for_operator_visibility() -> None:
    from nullion.builder_memory import BuilderMemoryResult, record_turn_memory_capture_event

    store = RuntimeStore()
    checkpoints = {"count": 0}
    runtime = SimpleNamespace(
        store=store,
        checkpoint=lambda: checkpoints.__setitem__("count", checkpoints["count"] + 1),
    )

    record_turn_memory_capture_event(
        runtime,
        conversation_id="web:operator",
        owner="workspace:workspace_admin",
        source="web_no_tool_turn",
        result=BuilderMemoryResult(written=1),
    )

    [event] = store.list_conversation_events("web:operator")
    assert event["event_type"] == "conversation.memory_capture"
    assert event["owner"] == "workspace:workspace_admin"
    assert event["status"] == "written"
    assert event["written"] == 1
    assert checkpoints["count"] == 1


@pytest.mark.parametrize("prompt", ["Remember this detail", "この好みを覚えてください", "Recuerda esta preferencia"])
def test_web_explicit_memory_requires_structured_receipt_without_a_timer(monkeypatch, prompt):
    def forbidden(*args, **kwargs):
        raise AssertionError("Plain chat started an implicit memory writer")
    monkeypatch.setattr(threading, "Timer", forbidden)
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "1")
    runtime = SimpleNamespace(store=RuntimeStore(), checkpoint=lambda **kwargs: None)
    before = time.perf_counter()
    web_app._remember_web_explicit_memory(runtime, user_message=prompt, owner="workspace:workspace_admin")
    assert (time.perf_counter() - before) * 1000 < 250
    assert runtime.store.list_user_memory_entries() == []


def test_web_no_tool_model_memory_capture_is_opt_in(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "1")
    monkeypatch.delenv("NULLION_MEMORY_NO_TOOL_CAPTURE_ENABLED", raising=False)

    assert not web_app._should_schedule_web_no_tool_memory_capture(
        owner="workspace:workspace_admin",
        user_message="This is an ordinary chat turn with enough text.",
        tool_results=[],
    )


def test_completed_selected_artifact_is_preserved_instead_of_fetch_fallback(tmp_path,monkeypatch):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    path=artifact_root_for_principal('web:selected')/'report.txt';path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(b'Final selected content: 23')
    def never_render(*args,**kwargs):raise AssertionError('A completed output must not be replaced by a fallback')
    monkeypatch.setattr(web_app,'run_fetch_artifact_workflow',never_render)
    assert web_app._materialize_fetch_artifact_for_web(runtime,prompt='添付して。',requested_extension='.txt',
        principal_id='web:selected',existing_artifact_paths=[str(path)],
        tool_results=[ToolResult('fetch','web_fetch','completed',{'body':'Raw source content: 17'})])==[]
    assert path.read_bytes()==b'Final selected content: 23'

from __future__ import annotations

import asyncio
import base64
import json
import queue
import threading
import time
import zipfile
from dataclasses import replace
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run real workflow e2e tests",
)


def _web_app(tmp_path: Path):
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry
    from nullion.web_app import create_app

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    app = create_app(runtime, orchestrator=None, registry=registry)
    return app


def _cron_visible_text(text: str | None) -> str | None:
    if text is None:
        return None
    return f"⏰ {text}"


def test_profile_preferences_users_cron_and_oauth_workflow_use_isolated_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import crons, preferences, users, web_app

    home = tmp_path / "home"
    env_file = tmp_path / ".env"
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.setenv("CODEX_REFRESH_TOKEN", "refresh-e2e")
    monkeypatch.setenv("CODEX_OAUTH_TOKEN", "access-e2e")
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "users.json")
    monkeypatch.setattr(preferences, "_PREFS_PATH", tmp_path / "preferences.json")
    monkeypatch.setattr(preferences, "_PROFILE_PATH", tmp_path / "profile.json")
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)
    monkeypatch.setattr(web_app, "_DEFAULT_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app = _web_app(tmp_path)
    with TestClient(app) as client:
        profile_response = client.post(
            "/api/profile",
            json={"name": "E2E Operator", "email": "operator@example.test", "ignored": "nope"},
        )
        profile = client.get("/api/profile").json()

        preferences_response = client.post(
            "/api/preferences",
            json={"tone": "professional", "show_reasoning": True, "timezone": "UTC", "unknown": "ignored"},
        )
        saved_preferences = client.get("/api/preferences").json()

        users_response = client.post(
            "/api/users",
            json={
                "multi_user_enabled": True,
                "users": [
                    {
                        "user_id": "member_e2e",
                        "display_name": "Workflow Member",
                        "role": "member",
                        "messaging_channel": "slack",
                        "messaging_user_id": "U123",
                    }
                ],
            },
        )
        user_registry = client.get("/api/users").json()

        create_cron_response = client.post(
            "/api/crons",
            json={
                "name": "E2E scheduled workflow",
                "schedule": "*/15 * * * *",
                "task": "Run the isolated e2e workflow",
                "workspace_id": "workspace_admin",
                "delivery_channel": "web",
            },
        )
        created_job = create_cron_response.json()["job"]
        cron_id = created_job["id"]
        patch_cron_response = client.patch(f"/api/crons/{cron_id}", json={"enabled": False})
        listed_crons = client.get("/api/crons").json()
        delete_cron_response = client.delete(f"/api/crons/{cron_id}")
        crons_after_delete = client.get("/api/crons").json()

        oauth_reauth_response = client.post("/api/config/oauth/reauth", json={"provider": "codex"})
        oauth_disconnect_response = client.post("/api/config/oauth/disconnect", json={"provider": "codex"})

    assert profile_response.status_code == 200
    assert profile_response.json() == {"ok": True}
    assert profile == {"name": "E2E Operator", "email": "operator@example.test"}

    assert preferences_response.status_code == 200
    assert preferences_response.json() == {"ok": True}
    assert saved_preferences["tone"] == "professional"
    assert saved_preferences["show_reasoning"] is True
    assert saved_preferences["timezone"] == "UTC"
    assert "unknown" not in saved_preferences

    assert users_response.status_code == 200
    assert users_response.json()["ok"] is True
    assert user_registry["multi_user_enabled"] is True
    assert any(user["user_id"] == "member_e2e" and user["messaging_channel"] == "slack" for user in user_registry["users"])

    assert create_cron_response.status_code == 200
    assert created_job["delivery_channel"] == "web"
    assert created_job["delivery_target"] == "web:operator"
    assert patch_cron_response.status_code == 200
    assert patch_cron_response.json()["job"]["enabled"] is False
    assert any(job["id"] == cron_id for job in listed_crons)
    assert delete_cron_response.status_code == 200
    assert delete_cron_response.json() == {"ok": True}
    assert all(job["id"] != cron_id for job in crons_after_delete)

    assert oauth_reauth_response.status_code == 200
    assert oauth_reauth_response.json()["stream_url"] == "/api/config/oauth/reauth/stream?provider=codex"
    assert oauth_disconnect_response.status_code == 200
    assert isinstance(oauth_disconnect_response.json()["removed"], list)
    assert "CODEX_REFRESH_TOKEN" not in os.environ
    assert "CODEX_OAUTH_TOKEN" not in os.environ


def test_reminder_tool_lifecycle_and_workspace_delivery_rules_use_isolated_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient

    from nullion import crons, reminder_delivery, users, web_app
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry
    from nullion.web_app import create_app

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "users.json")
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)
    monkeypatch.setattr(web_app, "_DEFAULT_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))
    with TestClient(app) as client:
        users_response = client.post(
            "/api/users",
            json={
                "multi_user_enabled": True,
                "users": [
                    {
                        "user_id": "member-reminder",
                        "display_name": "Reminder Member",
                        "role": "member",
                        "workspace_id": "workspace_reminders",
                        "messaging_channel": "telegram",
                        "messaging_user_id": "member-reminder",
                    }
                ],
            },
        )
        users_response.raise_for_status()
        monkeypatch.setattr(
            users,
            "messaging_delivery_targets_for_workspace",
            lambda workspace_id, *, settings=None: (
                users.MessagingDeliveryTarget(
                    channel="telegram",
                    target_id="member-reminder",
                    principal_id="user:member-reminder",
                    workspace_id=workspace_id,
                    user_id="member-reminder",
                    display_name="Reminder Member",
                ),
            ),
        )

    now = datetime.now(UTC) - timedelta(minutes=1)
    task = runtime.schedule_reminder(
        chat_id="member-reminder",
        text="Follow up with lead",
        due_at=now,
    )

    sent: list[tuple[str, str]] = []

    async def fake_workspace_send(target, text: str, *, settings: NullionSettings | None) -> bool:  # noqa: ANN001
        sent.append((target.channel, target.target_id))
        return True

    async def fake_origin_send(chat_id: str, text: str) -> bool:
        sent.append(("origin", chat_id))
        return True

    monkeypatch.setattr(
        reminder_delivery,
        "_send_workspace_reminder_target",
        fake_workspace_send,
    )

    delivered = asyncio.run(
        reminder_delivery.deliver_due_reminders_once(
            runtime,
            send=fake_origin_send,
            now=now,
            settings=NullionSettings(telegram=TelegramSettings(bot_token="tg-token")),
        )
    )

    delivered_task = runtime.store.get_scheduled_task(task.task_id)
    delivered_reminder = runtime.store.get_reminder(task.task_id)

    assert delivered == 1
    assert delivered_task is not None and delivered_task.enabled is False
    assert delivered_reminder is not None and delivered_reminder.delivered_at == now
    assert sent == [("telegram", "member-reminder")]


def test_cron_workspace_crud_rules_and_isolation_are_exercised_in_e2e(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient

    from nullion import crons, users, web_app

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setattr(users, "_USERS_PATH", tmp_path / "users.json")
    monkeypatch.setattr(web_app, "_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)
    monkeypatch.setattr(web_app, "_DEFAULT_CREDENTIALS_PATH", tmp_path / "credentials.json", raising=False)

    app = _web_app(tmp_path)

    with TestClient(app) as client:
        create_admin_cron_response = client.post(
            "/api/crons",
            json={
                "name": "Admin E2E Cron",
                "schedule": "*/15 * * * *",
                "task": "Report admin ops status",
                "workspace_id": "workspace_admin",
                "delivery_channel": "web",
            },
        )
        create_workspace_cron_response = client.post(
            "/api/crons",
            json={
                "name": "Workspace E2E Cron",
                "schedule": "*/15 * * * *",
                "task": "Report workspace ops status",
                "workspace_id": "workspace_reminders",
                "delivery_channel": "web",
            },
        )
        assert create_admin_cron_response.status_code == 200
        assert create_workspace_cron_response.status_code == 200

        admin_cron = create_admin_cron_response.json()["job"]
        workspace_cron = create_workspace_cron_response.json()["job"]

        listed_before = client.get("/api/crons").json()
        listed_ids = {item["id"] for item in listed_before}
        assert admin_cron["id"] in listed_ids
        assert workspace_cron["id"] in listed_ids

        patch_response = client.patch(
            f"/api/crons/{workspace_cron['id']}",
            json={
                "name": "Workspace E2E Cron v2",
                "enabled": False,
            },
        )
        delete_response = client.delete(f"/api/crons/{workspace_cron['id']}")
        listed_after = client.get("/api/crons").json()

        listed_after_ids = {item["id"] for item in listed_after}
        assert patch_response.status_code == 200
        assert patch_response.json()["job"]["name"] == "Workspace E2E Cron v2"
        assert patch_response.json()["job"]["enabled"] is False
        assert delete_response.status_code == 200
        assert workspace_cron["id"] not in listed_after_ids
        assert admin_cron["id"] in listed_after_ids


def test_cron_scheduler_execution_runs_due_jobs_with_fake_platform_sender_and_workspace_routing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons, users
    from nullion.cron_delivery import (
        CronRunDeliveryCallbacks,
        cron_delivery_target,
        effective_cron_delivery_channel,
        record_cron_delivery_chat_turn,
        run_cron_delivery_workflow,
    )
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    workspace_targets = (
        users.MessagingDeliveryTarget(
            channel="telegram",
            target_id="member-reminder",
            principal_id="user:member-reminder",
            workspace_id="workspace_reminders",
            user_id="member-reminder",
            display_name="Reminder Member",
        ),
    )

    settings = NullionSettings(telegram=TelegramSettings(bot_token="tg-token"))
    runtime = bootstrap_persistent_runtime(tmp_path / "delivery-history-runtime.db")
    events: list[dict[str, object]] = []

    def _delivery_target(job, channel: str) -> str:
        direct_target = cron_delivery_target(job, channel, settings=settings)
        if direct_target:
            return direct_target
        for workspace_target in users.messaging_delivery_targets_for_workspace(job.workspace_id, settings=settings):
            if workspace_target.channel == channel and workspace_target.target_id:
                return workspace_target.target_id
        return ""

    sent: list[tuple[str, str]] = []

    def _run_agent_turn(job, conv_id: str):  # noqa: ARG001
        return {"text": f"scheduled output: {job.task}"}

    def _record_event(event_type: str, job, channel: str, target: str, conv_id: str, **extra: object) -> None:
        events.append({"event_type": event_type, "job_id": job.id, "workspace_id": job.workspace_id, **extra})

    def _save_web_delivery(*_args, **_kwargs) -> bool:
        return True

    def _send_platform(job, channel: str, text: str) -> bool:
        sent.append((channel, _delivery_target(job, channel)))
        return True

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: effective_cron_delivery_channel(job, settings=settings),
        delivery_target=_delivery_target,
        run_agent_turn=_run_agent_turn,
        record_event=_record_event,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=_save_web_delivery,
        send_platform_delivery=_send_platform,
        record_chat_turn=lambda job, conv_id, channel, target, text: record_cron_delivery_chat_turn(
            runtime.store,
            job,
            conversation_id=conv_id,
            delivery_channel=channel,
            delivery_target=target,
            delivered_text=text,
        ),
    )

    monkeypatch.setattr(
        users,
        "messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings=None: workspace_targets,
    )

    job = crons.add_cron(
        name="Cron with explicit workspace routing",
        schedule="*/15 * * * *",
        task="Run synthetic workspace cron workflow",
        workspace_id="workspace_reminders",
        delivery_channel="telegram",
        delivery_target="member-reminder",
    )
    loaded_jobs = crons.load_crons()
    assert loaded_jobs, "cron job was not persisted"
    loaded_jobs[0].next_run = (datetime.now(UTC) - timedelta(minutes=1)).replace(second=0, microsecond=0).isoformat()
    crons.save_crons(loaded_jobs)

    scheduler = crons.CronScheduler(
        fire_fn=lambda cron_job: run_cron_delivery_workflow(
            cron_job,
            label="Scheduled task",
            callbacks=callbacks,
        ),
        tick_interval=0.01,
    )
    scheduler._tick()
    from cron_scheduler_helpers import finish_scheduler
    finish_scheduler(scheduler)

    assert ("telegram", "member-reminder") in sent
    assert any(event["event_type"] == "cron.delivery.sent" for event in events)
    turns = runtime.store.list_conversation_chat_turns("telegram:member-reminder", limit=1)
    assert turns == [
        {
            "user": (
                "[Scheduled task delivery context] task=Cron with explicit workspace routing; "
                "channel=telegram; artifacts=none"
            ),
            "assistant": "⏰ **SCHEDULED TASK: Cron with explicit workspace routing**\n\nscheduled output: Run synthetic workspace cron workflow",
        }
    ]
    updated_job = crons.get_cron(job.id)
    assert updated_job is not None
    assert updated_job.last_result == "ok"


def test_cron_scheduler_generates_pdf_and_delivers_single_clean_attachment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from PIL import Image
    from pypdf import PdfReader

    from nullion import crons
    from nullion.cron_delivery import CronRunDeliveryCallbacks, run_cron_delivery_workflow
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.tools import ToolInvocation, _build_pdf_create_handler
    from nullion.workspace_storage import workspace_storage_roots_for_principal
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "nullion-data"))
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")

    workspace_roots = workspace_storage_roots_for_principal("telegram_chat")
    workspace = workspace_roots.files
    screenshot = workspace / "cnn.png"
    Image.new("RGB", (160, 90), "navy").save(screenshot)
    pdf_path = workspace_roots.artifacts / "cnn_screenshot.pdf"
    pdf_create = _build_pdf_create_handler(workspace_root=workspace_roots.root, include_principal_workspace=False)

    sent_deliveries = []

    def _run_agent_turn(_job, _conv_id: str):
        created = pdf_create(
            ToolInvocation(
                "e2e-pdf",
                "pdf_create",
                "operator",
                {
                    "output_path": str(pdf_path),
                    "image_paths": [str(screenshot)],
                    "title": "CNN screenshot",
                },
            )
        )
        assert created.status == "completed"
        return {
            "final_text": "CNN screenshot PDF created successfully.\nMEDIA",
            "tool_results": [created],
        }

    def _send_platform(_job, channel: str, text: str) -> bool:
        delivery = prepare_reply_for_platform_delivery(text, principal_id="telegram:100001")
        sent_deliveries.append((channel, text, delivery))
        return True

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: "telegram",
        delivery_target=lambda _job, _channel: "100001",
        run_agent_turn=_run_agent_turn,
        record_event=lambda *_args, **_kwargs: None,
        block_reason=lambda _result, _text, _artifacts: None,
        save_web_delivery=lambda *_args, **_kwargs: False,
        send_platform_delivery=_send_platform,
    )

    job = crons.add_cron(
        name="CNN screenshot PDF",
        schedule="0 * * * *",
        task="Structured cron PDF delivery",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="100001",
    )
    jobs = crons.load_crons()
    jobs[0].next_run = (datetime.now(UTC) - timedelta(minutes=1)).replace(second=0, microsecond=0).isoformat()
    crons.save_crons(jobs)

    scheduler = crons.CronScheduler(
        fire_fn=lambda cron_job: run_cron_delivery_workflow(
            cron_job,
            label="Scheduled task",
            callbacks=callbacks,
        ),
        tick_interval=0.01,
    )
    scheduler._tick()
    from cron_scheduler_helpers import finish_scheduler
    finish_scheduler(scheduler)

    assert len(sent_deliveries) == 1
    channel, sent_text, delivery = sent_deliveries[0]
    assert channel == "telegram"
    assert sent_text == f"⏰ **SCHEDULED TASK: CNN screenshot PDF**\n\nCNN screenshot PDF created successfully.\n\nMEDIA:{pdf_path}"
    assert delivery.text == "⏰ **SCHEDULED TASK: CNN screenshot PDF**\n\nCNN screenshot PDF created successfully."
    assert delivery.attachments == (pdf_path,)
    assert delivery.media_directive_count == 1
    assert pdf_path.read_bytes().startswith(b"%PDF")
    assert len(PdfReader(str(pdf_path)).pages) == 1
    for principal_id in ("telegram_chat", "user:slack-member", "user:discord-member"):
        platform_delivery = prepare_reply_for_platform_delivery(
            sent_text,
            principal_id=principal_id,
        )
        assert platform_delivery.text == "⏰ **SCHEDULED TASK: CNN screenshot PDF**\n\nCNN screenshot PDF created successfully."
        assert platform_delivery.attachments == (pdf_path,)
        assert platform_delivery.unavailable_attachment_count == 0
    stored = crons.get_cron(job.id)
    assert stored is not None
    assert stored.last_result == "ok"


@pytest.mark.parametrize("requested", [False, True])
def test_telegram_screenshot_request_e2e_creates_and_uploads_real_png_attachment(
    tmp_path: Path,
    monkeypatch,
    requested: bool,
) -> None:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_adapters import delivery_contract_for_turn, prepare_reply_for_platform_delivery
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import auto_backend
    from nullion.plugins.browser_plugin.browser_session import BrowserScreenshotResult
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import _deliver_reply
    from nullion.telegram_turn_graph import plan_telegram_post_run_delivery
    from nullion.tools import ToolInvocation, ToolRegistry

    prompt = "Send a screenshot of https://adobe.com" if requested else "https://adobe.com"

    from io import BytesIO
    from PIL import Image
    png = BytesIO()
    Image.new("RGB", (32, 32), "blue").save(png, format="PNG")
    screenshot_bytes = png.getvalue()

    class DeterministicBrowserBackend:
        BACKEND_NAME = "auto"

        def __init__(self) -> None:
            self.task_session_id: str | None = None

        async def navigate(self, session_id: str, url: str) -> str:
            assert session_id.startswith("task-")
            assert session_id != "adobe-session"
            self.task_session_id = session_id
            assert url == "https://adobe.com"
            return "navigated"

        async def screenshot(self, session_id: str, mode: str = "auto") -> BrowserScreenshotResult:
            assert session_id == self.task_session_id
            return BrowserScreenshotResult(
                data=screenshot_bytes,
                mode="viewport",
                requested_mode=mode,
                page_url="https://adobe.com",
                page_title="Adobe",
                viewport_width=1280,
                viewport_height=720,
                document_width=1280,
                document_height=720,
                is_clipped=False,
            )

        async def close_session(self, session_id: str) -> None:
            assert session_id == self.task_session_id

    class StructuredPlanModel:
        def __init__(self) -> None:
            self.agent_calls = 0

        def create(self, **kwargs):
            tool_names = {tool["name"] for tool in kwargs.get("tools") or () if isinstance(tool, dict)}
            if "select_attachment_format" in tool_names:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "format-1",
                            "name": "select_attachment_format",
                            "input": {"extension": ".png", "confidence": 1.0},
                        }
                    ],
                }
            if int(kwargs.get("max_tokens") or 0) == 96 and not tool_names:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "requested_outcome": "unspecified",
                                    "web_action": "browser_interaction",
                                    "scheduler_action": "none",
                                    "scheduler_toggle_enabled": None,
                                    "scheduler_selection_policy": "none",
                                    "scheduler_target_scope": "none",
                                    "skill_pack_action": "none",
                                    "connector_app_ids": [],
                                    "connector_source_user_requested": False,
                                    "requested_tool_names": ["browser_navigate", "browser_screenshot"],
                                    "required_tool_names": [],
                                    "requested_artifact_extensions": [".png"],
                                    "excluded_artifact_extensions": [],
                                    "required_embedded_media_extensions": [], "source_artifact_delivery_requested": requested, "source_artifact_request_evidence": "Send a screenshot" if requested else "",
                                    "confidence": 0.99,
                                }
                            ),
                        }
                    ]
                }
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "navigate-1",
                            "name": "browser_navigate",
                            "input": {"url": "https://adobe.com", "session_id": "adobe-session"},
                        }
                    ],
                }
            if self.agent_calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "screenshot-1",
                            "name": "browser_screenshot",
                            "input": {"session_id": "adobe-session"},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "Screenshot captured."}],
            }

    class TelegramMessage:
        text = prompt

        def __init__(self) -> None:
            self.documents: list[tuple[bytes, dict[str, object]]] = []
            self.text_replies: list[tuple[str, dict[str, object]]] = []

        async def reply_document(self, document, **kwargs) -> None:
            self.documents.append((document.read(), dict(kwargs)))

        async def reply_text(self, text: str, **kwargs) -> None:
            self.text_replies.append((text, dict(kwargs)))

    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    monkeypatch.setattr(auto_backend, "AutoBackend", DeterministicBrowserBackend)

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    register_browser_tools(registry)
    runtime.active_tool_registry = registry
    model = StructuredPlanModel()
    settings = NullionSettings(
        telegram=TelegramSettings(
            bot_token="123456:abcdefghijklmnopqrstuvwxyz",
            operator_chat_id="100001",
            chat_enabled=True,
        )
    )
    monkeypatch.setattr("nullion.chat_operator._try_builder_reflection", lambda *args, **kwargs: None)

    reply = handle_chat_operator_message(
        runtime,
        prompt,
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=AgentOrchestrator(model_client=model),
        append_activity_trace=False,
        allow_mini_agents=False,
    )
    assert reply is not None
    if not requested:
        assert "MEDIA:" not in reply
        assert "Opened" in reply
        plan = plan_telegram_post_run_delivery(text_for_ack=prompt, reply=reply, stream_final_reply=True,
            streaming_mode="stream", final_only_streaming_mode="final", runtime=runtime, conversation_id="telegram:100001")
        message = TelegramMessage()
        asyncio.run(_deliver_reply(message, reply, principal_id="telegram_chat", delivery_contract=plan.delivery_contract))
        assert message.documents == []
        assert len(message.text_replies) == 1
        assert "Opened" in message.text_replies[0][0]
        assert model.agent_calls == 0
        return
    assert "MEDIA:" in reply
    assert "not done yet" not in reply.lower()
    assert "still needs" not in reply.lower()

    contract = delivery_contract_for_turn(
        prompt,
        reply=reply,
        required_attachment_extensions=(".png",),
    )
    delivery = prepare_reply_for_platform_delivery(
        reply,
        principal_id="telegram_chat",
        delivery_contract=contract,
    )
    assert delivery.text == "Done — attached the requested screenshot."
    assert len(delivery.attachments) == 1
    screenshot = delivery.attachments[0]
    assert screenshot.suffix == ".png"
    assert screenshot.read_bytes() == screenshot_bytes
    assert delivery.unavailable_attachment_count == 0

    plan = plan_telegram_post_run_delivery(
        text_for_ack=prompt,
        reply=reply,
        stream_final_reply=True,
        streaming_mode="stream",
        final_only_streaming_mode="final",
        runtime=runtime,
        conversation_id="telegram:100001",
    )
    message = TelegramMessage()
    asyncio.run(
        _deliver_reply(
            message,
            reply,
            principal_id="telegram_chat",
            delivery_contract=plan.delivery_contract,
        )
    )
    assert message.text_replies == []
    assert len(message.documents) == 1
    uploaded_bytes, upload_kwargs = message.documents[0]
    assert uploaded_bytes == screenshot_bytes
    assert upload_kwargs["caption"] == "Done — attached the requested screenshot."


def test_telegram_browser_text_answer_survives_an_optional_failed_assertion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    product_evidence = (
        "Dermaharmony 5% Sulfur and 2% Salicylic Acid Liquid Castile Soap\n"
        "Liquid wash, not a bar\n"
        "ACTIVE INGREDIENTS: 5% Sulfur and 2% Salicylic Acid\n"
        "Price: $9.99\n"
        "URL: https://example.test/product"
    )
    invoked_tools: list[str] = []

    class Model:
        def __init__(self) -> None:
            self.agent_calls = 0
            self.no_tool_calls = 0
            self.review_calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            system = str(kwargs.get("system") or "")
            if int(kwargs.get("max_tokens") or 0) == 96 and not kwargs.get("tools"):
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "requested_outcome": "unspecified",
                                    "web_action": "browser_interaction",
                                    "scheduler_action": "none",
                                    "scheduler_toggle_enabled": None,
                                    "scheduler_selection_policy": "none",
                                    "scheduler_target_scope": "none",
                                    "skill_pack_action": "none",
                                    "connector_app_ids": [],
                                    "connector_source_user_requested": False,
                                    "requested_tool_names": [
                                        "browser_assert_page_state",
                                        "browser_extract_text",
                                    ],
                                    "required_tool_names": [],
                                    "requested_artifact_extensions": [],
                                    "excluded_artifact_extensions": [],
                                    "required_embedded_media_extensions": [],
                                    "confidence": 0.99,
                                }
                            ),
                        }
                    ]
                }
            if system.startswith("You are a material-requirements planner"):
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement": (
                                                "a non-bar Amazon product with verified sulfur content, price, "
                                                "and product link"
                                            ),
                                            "requirement_kind": "constraint",
                                            "evidence_subject_id": "requested-amazon-product",
                                            "eligible_evidence_tool_names": [],
                                        }
                                    ],
                                }
                            ),
                        }
                    ]
                }
            if system.startswith("You are a semantic completion reviewer"):
                self.review_calls += 1
                review_payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
                contract_row = review_payload["material_requirement_contract"][0]
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "disposition": "retry",
                                    "unresolved_requirements": [
                                        "the requested product is not supported by verified terminal browser evidence"
                                    ],
                                    "retry_tool_names": [],
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement_id": contract_row["requirement_id"],
                                            "requirement": contract_row["requirement"],
                                            "requirement_kind": contract_row["requirement_kind"],
                                            "satisfied": False,
                                            "draft_covered": False,
                                            "evidence_invocation_ids": [],
                                        }
                                    ],
                                }
                            ),
                        }
                    ]
                }
            if not kwargs.get("tools"):
                self.no_tool_calls += 1
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": "I need the browser tools to verify a matching product.",
                        }
                    ],
                }
            visible = {definition["name"] for definition in kwargs["tools"]}
            if "request_tool_scope" in visible and "browser_extract_text" not in visible:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "scope-browser",
                    "name": "request_tool_scope", "input": {"capabilities": ["web"],
                    "tool_names": ["browser_assert_page_state", "browser_extract_text"]}}]}
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "assert-product",
                            "name": "browser_assert_page_state",
                            "input": {"required": ["Dermaharmony", "sulfur"]},
                        }
                    ],
                }
            if self.agent_calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "extract-product",
                            "name": "browser_extract_text",
                            "input": {"selector": "#product-detail"},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "I found a matching non-bar option: Dermaharmony Liquid Castile Soap contains "
                            "5% sulfur and 2% salicylic acid. It was listed at $9.99. "
                            "https://example.test/product"
                        ),
                    }
                ],
            }

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            "browser_assert_page_state",
            "Verify rendered browser state.",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            5,
            capability_tags=("web", "browser"),
        ),
        lambda invocation: (
            invoked_tools.append(invocation.tool_name)
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "verified": False,
                    "missing_required": ["Dermaharmony", "sulfur"],
                    "result": {"ok": False, "missing": [{"expected": "Dermaharmony"}]},
                },
            )
        ),
    )
    registry.register(
        ToolSpec(
            "browser_extract_text",
            "Extract rendered browser text.",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            5,
            capability_tags=("web", "browser"),
        ),
        lambda invocation: (
            invoked_tools.append(invocation.tool_name)
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"selector": "#product-detail", "text": product_evidence},
            )
        ),
    )
    runtime.active_tool_registry = registry
    model = Model()
    monkeypatch.setattr("nullion.chat_operator._try_builder_reflection", lambda *args, **kwargs: None)

    reply = handle_chat_operator_message(
        runtime,
        "Can you find me a non-bar sulfur soap that actually has sulfur on amazon?",
        chat_id="100001",
        settings=NullionSettings(
            telegram=TelegramSettings(chat_enabled=True, operator_chat_id="100001")
        ),
        model_client=model,
        agent_orchestrator=AgentOrchestrator(model_client=model),
        append_activity_trace=False,
        allow_mini_agents=False,
    )

    chat_turns = [
        event
        for event in runtime.store.list_conversation_events("telegram:100001")
        if event.get("event_type") == "conversation.chat_turn"
    ]
    assert invoked_tools == ["browser_assert_page_state", "browser_extract_text"], {
        "reply": reply,
        "agent_calls": model.agent_calls,
        "no_tool_calls": model.no_tool_calls,
        "review_calls": model.review_calls,
        "tool_results": chat_turns[-1].get("tool_results") if chat_turns else None,
    }
    assert model.review_calls == 0
    assert "Dermaharmony" in reply
    assert "5% sulfur" in reply
    assert "$9.99" in reply
    assert "https://example.test/product" in reply
    assert "Live-search blocker" not in reply
    assert chat_turns
    persisted_tool_names = [item["tool_name"] for item in chat_turns[-1]["tool_results"]]
    assert persisted_tool_names[-2:] == ["browser_assert_page_state", "browser_extract_text"]


def test_telegram_browser_detail_answer_does_not_trigger_terminal_revalidation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    target_url = (
        "https://www.amazon.com/DermaHarmony-Sulfur-Salicylic-Liquid-Castile/"
        "dp/B0897CFK16?th=1"
    )
    anchored_target_url = f"{target_url}#skippedLink"
    target_title = (
        "Dermaharmony 5% Sulfur and 2% Salicylic Acid Liquid Castile Soap "
        "(4 Fl Oz Bottle)"
    )

    preview = json.dumps(
        {
            "items": [
                {
                    "title": "Tgideas Sulfur Soap Body Wash",
                    "compact_text": "Liquid sulfur soap body wash",
                    "price_text": "$19.99",
                    "rating": "4.4",
                    "url": "https://www.amazon.com/dp/TGIDEAS-LIQUID",
                },
                {
                    "title": "Erbaid Sulfur Soap Bar - Sulfur Face & Body Wash",
                    "compact_text": "Solid sulfur soap bar for face and body",
                    "price_text": "$5.99",
                    "rating": "4.1",
                    "url": "https://www.amazon.com/dp/ERBAID-BAR",
                },
                {
                    "title": "Bee & Flower Sulfur Soap Liquid Shampoo & Body Wash",
                    "compact_text": "Liquid shampoo and body wash with natural sulfur extract",
                    "price_text": "$14.59",
                    "rating": "4.3",
                    "url": "https://www.amazon.com/dp/BEE-FLOWER-LIQUID",
                },
                {
                    "title": "Dermaharmony 5% Sulfur Liquid Castile Soap",
                    "compact_text": "Liquid Castile soap with 5% sulfur and 2% salicylic acid",
                    "price_text": "$9.99",
                    "rating": "4.2",
                    "url": "https://www.amazon.com/dp/DERMAHARMONY-LIQUID",
                },
            ]
        }
    )
    preview = preview[:-2] + ', {"title": "incomplete transport record"'
    invoked_tools: list[str] = []
    terminal_mutation_invocation_ids: list[str] = []

    class Model:
        def __init__(self) -> None:
            self.agent_calls = 0
            self.review_calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            system = str(kwargs.get("system") or "")
            if int(kwargs.get("max_tokens") or 0) == 96 and not kwargs.get("tools"):
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "requested_outcome": "unspecified",
                                    "web_action": "browser_interaction",
                                    "scheduler_action": "none",
                                    "scheduler_toggle_enabled": None,
                                    "scheduler_selection_policy": "none",
                                    "scheduler_target_scope": "none",
                                    "skill_pack_action": "none",
                                    "connector_app_ids": [],
                                    "connector_source_user_requested": False,
                                    "requested_tool_names": [
                                        "browser_extract_items",
                                        "browser_extract_detail",
                                        "browser_navigate",
                                        "browser_run_js",
                                        "browser_assert_page_state",
                                        "browser_snapshot",
                                        "browser_click_element",
                                        "browser_type_field",
                                    ],
                                    "required_tool_names": [],
                                    "requested_artifact_extensions": [],
                                    "excluded_artifact_extensions": [],
                                    "required_embedded_media_extensions": [],
                                    "confidence": 0.99,
                                }
                            ),
                        }
                    ]
                }
            if system.startswith("You are a material-requirements planner"):
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement": (
                                                "a non-bar Amazon product with verified sulfur content, price, "
                                                "availability, and product link"
                                            ),
                                            "requirement_kind": "constraint",
                                            "evidence_subject_id": "requested-amazon-product",
                                            "eligible_evidence_tool_names": ["browser_extract_detail"],
                                        }
                                    ],
                                }
                            ),
                        }
                    ]
                }
            if system.startswith("You are a semantic completion reviewer"):
                self.review_calls += 1
                review_payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
                contract_row = review_payload["material_requirement_contract"][0]
                evidence_ids = [
                    str(record.get("invocation_id") or "")
                    for record in review_payload["tool_evidence"]
                    if record.get("tool_name") == "browser_extract_detail"
                    and record.get("status") == "completed"
                    and str(record.get("invocation_id") or "")
                ]
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "disposition": "complete",
                                    "unresolved_requirements": [],
                                    "retry_tool_names": [],
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement_id": contract_row["requirement_id"],
                                            "requirement": contract_row["requirement"],
                                            "requirement_kind": contract_row["requirement_kind"],
                                            "satisfied": True,
                                            "draft_covered": True,
                                            "evidence_invocation_ids": evidence_ids[-1:],
                                        }
                                    ],
                                }
                            ),
                        }
                    ]
                }
            if not kwargs.get("tools"):
                return {
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "I need browser access to verify products."}],
                }
            visible = {definition["name"] for definition in kwargs["tools"]}
            if "request_tool_scope" in visible and "browser_extract_items" not in visible:
                return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "scope-browser",
                    "name": "request_tool_scope", "input": {"capabilities": ["web"],
                    "tool_names": ["browser_extract_items", "browser_extract_detail", "browser_navigate", "browser_run_js",
                                   "browser_assert_page_state", "browser_snapshot", "browser_click_element", "browser_type_field"]}}]}
            sequence = [
                ("browser_extract_items", {"selector": "main"}),
                ("browser_navigate", {"url": target_url}),
                ("browser_run_js", {"script": "return window.searchResults"}),
                (
                    "browser_assert_page_state",
                    {"selector": "body", "required": ["5% Sulfur"], "forbidden": ["Bar"]},
                ),
                (
                    "browser_assert_page_state",
                    {
                        "selector": "#centerCol",
                        "required": [target_title, "5% Sulfur"],
                        "forbidden": ["Bar", "Soap Bar"],
                    },
                ),
                ("browser_snapshot", {}),
                ("browser_click_element", {"role": "link", "name": "Main content"}),
                ("browser_extract_detail", {"selector": "#centerCol"}),
                (
                    "browser_type_field",
                    {"label": "Search Amazon", "text": "non-bar sulfur soap"},
                ),
            ]
            if self.agent_calls < len(sequence):
                tool_name, tool_input = sequence[self.agent_calls]
                self.agent_calls += 1
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"live-{self.agent_calls}",
                            "name": tool_name,
                            "input": tool_input,
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": f"{target_title} — $9.99; In Stock\n{anchored_target_url}",
                    }
                ],
            }

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()

    def register(
        tool_name: str,
        handler,  # noqa: ANN001
        *,
        side_effect: ToolSideEffectClass = ToolSideEffectClass.READ,
    ) -> None:
        registry.register(
            ToolSpec(
                tool_name,
                f"Run {tool_name}.",
                ToolRiskLevel.LOW,
                side_effect,
                False,
                5,
                capability_tags=("web", "browser"),
            ),
            handler,
        )

    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools

    catalog_records = [
        {
            "title": "Jericho Dead Sea Sulfur Soap - Vegan Face & Body Bar",
            "compact_text": "Solid sulfur soap bar",
            "price": "$8.99",
            "url": "https://www.amazon.com/dp/JERICHO-BAR",
        },
        {
            "title": "VALITIC Kojic Acid Dark Spot Remover Soap Bars",
            "compact_text": "Soap bars",
            "price": "$14.99",
            "url": "https://www.amazon.com/dp/VALITIC-BARS-ONE",
        },
        {
            "title": "Bee & Flower Sulfur Soap Liquid Shampoo & Body Wash",
            "compact_text": "Liquid shampoo with sulfur extract",
            "price": "$14.59",
            "url": "https://www.amazon.com/dp/BEE-FLOWER-LIQUID",
        },
    ]

    class DeterministicBrowserBackend:
        BACKEND_NAME = "cdp"

        def __init__(self) -> None:
            self.current_url = "https://www.amazon.com/s?k=liquid+sulfur+soap"

        async def navigate(self, session_id: str, url: str) -> str:
            self.current_url = url
            return f"Navigated to {url} (status 200)"

        async def run_js(self, session_id: str, script: str):  # noqa: ANN001
            if "forbidden_found" in script and "const args" in script:
                serialized_args = script.split("const args = ", 1)[1].split(";", 1)[0]
                args = json.loads(serialized_args)
                selector = args.get("selector")
                required = list(args.get("required") or [])
                forbidden = list(args.get("forbidden") or [])
                verified = selector == "#centerCol"
                forbidden_found = (
                    []
                    if verified
                    else [
                        {"expected": label, "match": "Sulfur Soap Bar"}
                        for label in forbidden
                    ]
                )
                return {
                    "ok": verified,
                    "url": self.current_url,
                    "title": f"Amazon.com : {target_title} : Beauty & Personal Care",
                    "selector": selector,
                    "required": [
                        {"expected": label, "match": target_title}
                        for label in required
                    ],
                    "forbidden": [
                        {"expected": label, "match": "" if verified else "Sulfur Soap Bar"}
                        for label in forbidden
                    ],
                    "missing": [],
                    "forbidden_found": forbidden_found,
                    "candidates": [target_title],
                }
            if "source_index" in script and "numeric_snippets" in script:
                return {
                    "url": self.current_url,
                    "title": "Amazon search",
                    "items": catalog_records,
                    "item_count": len(catalog_records),
                }
            if "ambiguous_detail_scope" in script and "record_scope" in script:
                return {
                    "ok": True,
                    "selector": "#centerCol",
                    "record_scope": "selected_root",
                    "page": {
                        "url": self.current_url,
                        "title": f"Amazon.com : {target_title} : Beauty & Personal Care",
                    },
                    "record": {
                        "availability": "In Stock",
                        "price": "$9.99",
                        "title": target_title,
                        "details": "5% sulfur liquid Castile soap in a 4 fl oz bottle.",
                        "url": self.current_url,
                    },
                }
            if "element_count" in script and "__nullionBrowserElementSeq" in script:
                return {
                    "url": self.current_url,
                    "title": target_title,
                    "active_element_id": "",
                    "element_count": 1,
                    "elements": [
                        {
                            "element_id": "n-main",
                            "role": "link",
                            "name": "Main content",
                            "text": "Main content",
                            "visible": True,
                            "disabled": False,
                            "editable": False,
                        }
                    ],
                }
            if "window.currentProductDetail" in script:
                return {
                    "availability": "In Stock",
                    "bodySample": "Customers also bought Sulfur Bar Soap",
                    "price": "$9.99",
                    "productTitle": target_title,
                    "title": f"Amazon.com : {target_title} : Beauty & Personal Care",
                    "url": self.current_url,
                }
            if "window.searchResults" in script:
                return catalog_records
            return {"ok": True, "url": self.current_url}

        async def click_element(self, session_id: str, target: dict[str, object]) -> None:
            self.current_url = anchored_target_url

        async def click(self, session_id: str, selector: str) -> None:
            self.current_url = anchored_target_url

        async def type_field(
            self,
            session_id: str,
            target: dict[str, object],
            text: str,
        ) -> None:
            return None

        async def extract_text(self, session_id: str, selector: str | None) -> str:
            return target_title

        async def close_session(self, session_id: str) -> None:
            return None

    browser_tools = BrowserTools(
        backend=DeterministicBrowserBackend(),
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )

    def register_browser_tool(tool_name: str) -> None:
        method = getattr(browser_tools, tool_name)

        def handler(invocation):  # noqa: ANN001
            invoked_tools.append(invocation.tool_name)
            if invocation.tool_name == "browser_type_field":
                terminal_mutation_invocation_ids.append(invocation.invocation_id)
            return method(invocation)

        register(
            tool_name,
            handler,
            side_effect=(
                ToolSideEffectClass.WRITE
                if tool_name in {"browser_click_element", "browser_type_field"}
                else ToolSideEffectClass.READ
            ),
        )

    for browser_tool_name in (
        "browser_extract_items",
        "browser_extract_detail",
        "browser_navigate",
        "browser_run_js",
        "browser_assert_page_state",
        "browser_snapshot",
        "browser_click_element",
        "browser_type_field",
    ):
        register_browser_tool(browser_tool_name)
    runtime.active_tool_registry = registry
    model = Model()
    monkeypatch.setattr("nullion.chat_operator._try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "11")

    reply = handle_chat_operator_message(
        runtime,
        "Can you find me a non-bar sulfur soap that actually has sulfur on amazon?",
        chat_id="100001",
        settings=NullionSettings(
            telegram=TelegramSettings(chat_enabled=True, operator_chat_id="100001")
        ),
        model_client=model,
        agent_orchestrator=AgentOrchestrator(model_client=model),
        append_activity_trace=False,
        allow_mini_agents=False,
    )

    chat_turns = [
        event
        for event in runtime.store.list_conversation_events("telegram:100001")
        if event.get("event_type") == "conversation.chat_turn"
    ]
    assert invoked_tools == [
        "browser_extract_items",
        "browser_navigate",
        "browser_run_js",
        "browser_assert_page_state",
        "browser_assert_page_state",
        "browser_snapshot",
        "browser_click_element",
        "browser_extract_detail",
        "browser_type_field",
    ], {
        "reply": reply,
        "agent_calls": model.agent_calls,
        "review_calls": model.review_calls,
        "chat_turns": chat_turns,
    }
    assert target_title in reply
    assert "$9.99" in reply
    assert "In Stock" in reply
    assert anchored_target_url in reply
    assert "Amazon.com :" not in reply
    assert "Amazon Autos" not in reply
    assert "Customers also bought" not in reply
    assert "Jericho Dead Sea Sulfur Soap" not in reply
    assert "VALITIC" not in reply
    assert "Tgideas Sulfur Soap Body Wash" not in reply
    assert "Bee & Flower Sulfur Soap Liquid Shampoo & Body Wash" not in reply
    assert "Erbaid Sulfur Soap Bar" not in reply
    assert "Related listing" not in reply
    assert "concentrated sulfur soap bar" not in reply
    assert "Live-search blocker" not in reply
    assert '"items"' not in reply
    assert reply.count(anchored_target_url) == 1
    assert chat_turns
    assert chat_turns[-1]["assistant_reply"] == reply
    persisted_tool_names = [item["tool_name"] for item in chat_turns[-1]["tool_results"]]
    assert persisted_tool_names == ["request_tool_scope", *invoked_tools]
    persisted_assertions = [
        item["output"]["verified"]
        for item in chat_turns[-1]["tool_results"]
        if item["tool_name"] == "browser_assert_page_state"
    ]
    assert persisted_assertions == [False, True]
    final_browser_read = [
        item
        for item in chat_turns[-1]["tool_results"]
        if item["tool_name"] == "browser_extract_detail"
    ][-1]
    terminal_form_action = next(
        item
        for item in chat_turns[-1]["tool_results"]
        if item["tool_name"] == "browser_type_field"
    )
    assert terminal_form_action["output"]["session_id"] == final_browser_read["output"]["session_id"]
    assert len(terminal_mutation_invocation_ids) == 1
    assert "terminal_revalidation" not in final_browser_read["output"]
    assert model.review_calls == 0
    click_result = next(
        item
        for item in chat_turns[-1]["tool_results"]
        if item["tool_name"] == "browser_click_element"
    )
    assert click_result["output"]["action_receipt"]["type"] == "action_receipt"
    typed_assertion = final_browser_read["output"]["page_state_assertion"]
    assert typed_assertion["verified"] is True
    assert typed_assertion["session_id"] == final_browser_read["output"]["session_id"]
    assert typed_assertion["contract"] == {
        "required": [target_title, "5% Sulfur"],
        "forbidden": ["Bar", "Soap Bar"],
        "selector": "#centerCol",
    }
    assert typed_assertion["result"]["url"] == anchored_target_url


def test_agent_browser_turns_run_concurrently_with_isolated_task_sessions() -> None:
    from concurrent.futures import ThreadPoolExecutor

    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    class BarrierBackend:
        BACKEND_NAME = "cdp"

        def __init__(self) -> None:
            self._all_navigations_entered: asyncio.Event | None = None
            self._navigation_waiters: set[str] = set()
            self.overlapped_navigation = False
            self.pages: dict[str, dict[str, str]] = {}

        async def navigate(self, session_id: str, url: str) -> dict[str, str]:
            if self._all_navigations_entered is None:
                self._all_navigations_entered = asyncio.Event()
            if self._navigation_waiters:
                self.overlapped_navigation = True
            self._navigation_waiters.add(session_id)
            if len(self._navigation_waiters) == 2:
                self._all_navigations_entered.set()
            try:
                await asyncio.wait_for(self._all_navigations_entered.wait(), timeout=3.0)
            finally:
                self._navigation_waiters.discard(session_id)
            page = {"url": url, "data": f"payload:{url}"}
            self.pages[session_id] = page
            return dict(page)

        async def run_js(self, session_id: str, script: str) -> dict[str, str]:
            assert script == "return window.__taskData"
            return dict(self.pages[session_id])

        async def close_session(self, session_id: str) -> None:
            return None

    class BrowserTaskModel:
        def __init__(self, target_url: str) -> None:
            self.target_url = target_url
            self.agent_calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            system = str(kwargs.get("system") or "")
            if system.startswith("You are a semantic completion reviewer"):
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "disposition": "complete",
                                    "unresolved_requirements": [],
                                    "retry_tool_names": [],
                                }
                            ),
                        }
                    ]
                }
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"navigate-{self.target_url}",
                            "name": "browser_navigate",
                            "input": {"url": self.target_url},
                        }
                    ],
                }
            if self.agent_calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"read-{self.target_url}",
                            "name": "browser_run_js",
                            "input": {"script": "return window.__taskData"},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": f"Read {self.target_url}."}],
            }

    backend = BarrierBackend()
    browser_tools = BrowserTools(
        backend=backend,
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )
    registry = ToolRegistry()
    registry.register_cleanup_hook(browser_tools.close_tracked_sessions)
    registry.register(
        ToolSpec(
            "browser_navigate",
            "Navigate to the task page.",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            8,
            input_schema={
                "type": "object",
                "properties": {"url": {"type": "string"}},
                "required": ["url"],
            },
            capability_tags=("web", "browser"),
            continuation_tools=("browser_run_js",),
        ),
        browser_tools.browser_navigate,
    )
    registry.register(
        ToolSpec(
            "browser_run_js",
            "Read deterministic data from the task page.",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            8,
            input_schema={
                "type": "object",
                "properties": {"script": {"type": "string"}},
                "required": ["script"],
            },
            capability_tags=("web", "browser"),
        ),
        browser_tools.browser_run_js,
    )

    targets = ("https://alpha.example/task", "https://beta.example/task")
    start_gate = threading.Barrier(3)

    def run_browser_turn(target_url: str):  # noqa: ANN202
        model = BrowserTaskModel(target_url)
        orchestrator = AgentOrchestrator(model_client=model)
        start_gate.wait(timeout=5)
        return orchestrator.run_turn(
            conversation_id=f"browser-concurrency:{target_url}",
            principal_id="web:shared-browser-operator",
            user_message=f"Open {target_url} and read its task data.",
            conversation_history=[],
            tool_registry=registry,
            policy_store=None,
            approval_store=None,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_browser_turn, target_url) for target_url in targets]
        start_gate.wait(timeout=5)
        results = [future.result(timeout=15) for future in futures]

    assert backend.overlapped_navigation is True
    result_sessions: list[str] = []
    for target_url, result in zip(targets, results, strict=True):
        navigate_result = next(item for item in result.tool_results if item.tool_name == "browser_navigate")
        read_result = next(item for item in result.tool_results if item.tool_name == "browser_run_js")
        assert navigate_result.status == "completed"
        assert read_result.status == "completed"
        session_id = str(navigate_result.output["session_id"])
        result_sessions.append(session_id)
        assert session_id.startswith("task-")
        assert read_result.output["session_id"] == session_id
        expected_page = {"url": target_url, "data": f"payload:{target_url}"}
        assert navigate_result.output["result"] == expected_page
        assert read_result.output["result"] == expected_page
        assert backend.pages[session_id] == expected_page

    assert len(set(result_sessions)) == 2


def test_successful_browser_turns_avoid_completion_review_latency() -> None:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    class Model:
        def __init__(self) -> None:
            self.review_calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            system = str(kwargs.get("system") or "")
            if system.startswith("You are a semantic completion reviewer"):
                self.review_calls += 1
                time.sleep(1.25)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": '{"disposition":"complete","unresolved_requirements":[],"retry_tool_names":[]}',
                        }
                    ]
                }
            recent_messages = json.dumps((kwargs.get("messages") or [])[-3:], default=str)
            if "Fast browser result" in recent_messages:
                return {
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "Fast browser result: item available for $12.00."}],
                }
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": f"extract-{time.perf_counter_ns()}",
                        "name": "browser_extract_text",
                        "input": {"selector": "#result"},
                    }
                ],
            }

    registry = ToolRegistry()
    invocations: list[str] = []
    registry.register(
        ToolSpec(
            "browser_extract_text",
            "Extract rendered browser text.",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            5,
            capability_tags=("web", "browser"),
        ),
        lambda invocation: (
            invocations.append(invocation.tool_name)
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"selector": "#result", "text": "Fast browser result: item available for $12.00."},
            )
        ),
    )
    model = Model()
    orchestrator = AgentOrchestrator(model_client=model)

    elapsed_ms: list[float] = []
    for index in range(5):
        started = time.perf_counter()
        result = orchestrator.run_turn(
            conversation_id=f"browser-latency-{index}",
            principal_id=f"browser-latency-{index}",
            user_message="Prüfe das gerenderte Ergebnis und nenne den aktuellen Artikel.",
            conversation_history=[],
            tool_registry=registry,
            policy_store=None,
            approval_store=None,
        )
        elapsed_ms.append((time.perf_counter() - started) * 1000)
        assert result.final_text == "Fast browser result: item available for $12.00.", {
            "index": index,
            "reply": result.final_text,
            "review_calls": model.review_calls,
            "invocations": invocations,
        }

    average_ms = sum(elapsed_ms) / len(elapsed_ms)
    p95_ms = max(elapsed_ms)
    assert model.review_calls == 0
    assert invocations == ["browser_extract_text"] * 5
    assert average_ms < 650, f"successful browser average latency regressed: avg={average_ms:.1f}ms samples={elapsed_ms!r}"
    assert p95_ms < 1200, f"successful browser p95 latency regressed: p95={p95_ms:.1f}ms samples={elapsed_ms!r}"


def test_telegram_screenshot_request_e2e_recovers_from_cdp_context_management_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import websockets

    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_adapters import delivery_contract_for_turn, prepare_reply_for_platform_delivery
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import cdp_backend
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.telegram_app import _deliver_reply
    from nullion.telegram_turn_graph import plan_telegram_post_run_delivery
    from nullion.tools import ToolInvocation, ToolRegistry

    from io import BytesIO
    from PIL import Image
    image_buffer = BytesIO()
    Image.new("RGB", (800, 600), "blue").save(image_buffer, format="PNG")
    screenshot_png = image_buffer.getvalue()
    received_methods: list[str] = []

    class RawDevToolsServer:
        def __init__(self) -> None:
            self.loop = asyncio.new_event_loop()
            self.ready: queue.Queue[tuple[int, object]] = queue.Queue()
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.server = None

        async def _handler(self, websocket) -> None:
            async for raw_message in websocket:
                message = json.loads(raw_message)
                method = message["method"]
                received_methods.append(method)
                result: dict[str, object] = {}
                if method == "Page.getLayoutMetrics":
                    result = {
                        "layoutViewport": {"clientWidth": 800, "clientHeight": 600},
                        "contentSize": {"width": 800, "height": 600},
                    }
                elif method == "Page.captureScreenshot":
                    result = {"data": base64.b64encode(screenshot_png).decode("ascii")}
                await websocket.send(json.dumps({"id": message["id"], "result": result}))
                if method == "Page.navigate":
                    await websocket.send(json.dumps({"method": "Page.domContentEventFired", "params": {}}))

        async def _start(self) -> None:
            self.server = await websockets.serve(self._handler, "127.0.0.1", 0)
            port = self.server.sockets[0].getsockname()[1]
            self.ready.put((port, self.server))

        def _run(self) -> None:
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._start())
            self.loop.run_forever()

        def start(self) -> int:
            self.thread.start()
            port, _server = self.ready.get(timeout=5)
            return port

        def close(self) -> None:
            if self.server is not None:
                async def _stop() -> None:
                    self.server.close()
                    await self.server.wait_closed()

                asyncio.run_coroutine_threadsafe(_stop(), self.loop).result(timeout=5)
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=5)
            self.loop.close()

    class Chromium:
        async def connect_over_cdp(self, _url):
            raise RuntimeError(
                "BrowserType.connect_over_cdp: Protocol error "
                "(Browser.setDownloadBehavior): Browser context management is not supported"
            )

    class PlaywrightContext:
        async def __aenter__(self):
            return SimpleNamespace(chromium=Chromium())

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class StructuredPlanModel:
        def __init__(self) -> None:
            self.agent_calls = 0

        def create(self, **kwargs):
            tool_names = {tool["name"] for tool in kwargs.get("tools") or () if isinstance(tool, dict)}
            if int(kwargs.get("max_tokens", 0)) == 96 and not tool_names:
                return {"content": [{"type": "text", "text": json.dumps({
                    "requested_outcome": "unspecified", "web_action": "browser_interaction",
                    "scheduler_action": "none", "skill_pack_action": "none", "connector_app_ids": [],
                    "requested_tool_names": ["browser_navigate", "browser_screenshot"],
                    "required_tool_names": [], "requested_artifact_extensions": [".png"],
                    "excluded_artifact_extensions": [], "required_embedded_media_extensions": [], "source_artifact_delivery_requested": True, "source_artifact_request_evidence": "Send a screenshot",
                    "confidence": 0.99,
                })}]}
            if "select_attachment_format" in tool_names:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "format-1",
                            "name": "select_attachment_format",
                            "input": {"extension": ".png", "confidence": 1.0},
                        }
                    ],
                }
            self.agent_calls += 1
            if self.agent_calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "navigate-1",
                            "name": "browser_navigate",
                            "input": {"url": "https://cnn.com", "session_id": "telegram-shot"},
                        }
                    ],
                }
            if self.agent_calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "screenshot-1",
                            "name": "browser_screenshot",
                            "input": {"session_id": "telegram-shot"},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "Screenshot captured."}],
            }

    class TelegramMessage:
        text = "Send a screenshot of https://cnn.com"

        def __init__(self) -> None:
            self.documents: list[tuple[bytes, dict[str, object]]] = []
            self.text_replies: list[tuple[str, dict[str, object]]] = []

        async def reply_document(self, document, **kwargs) -> None:
            self.documents.append((document.read(), dict(kwargs)))

        async def reply_text(self, text: str, **kwargs) -> None:
            self.text_replies.append((text, dict(kwargs)))

    server = RawDevToolsServer()
    registry = ToolRegistry()
    try:
        port = server.start()
        websocket_url = f"ws://127.0.0.1:{port}/devtools/page/1"
        monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
        monkeypatch.setenv("NULLION_BROWSER_BACKEND", "cdp")
        monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://127.0.0.1:9222")
        monkeypatch.setattr(cdp_backend, "async_playwright", lambda: PlaywrightContext())
        monkeypatch.setattr(
            cdp_backend,
            "_async_cdp_targets",
            lambda _url: asyncio.sleep(0, result=[]),
        )
        monkeypatch.setattr(
            cdp_backend,
            "_async_new_cdp_target",
            lambda _url: asyncio.sleep(
                0,
                result={
                    "id": "target-1",
                    "type": "page",
                    "url": "about:blank",
                    "webSocketDebuggerUrl": websocket_url,
                },
            ),
        )

        runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
        register_browser_tools(registry)
        runtime.active_tool_registry = registry
        navigate_result = registry.invoke(
            ToolInvocation(
                invocation_id="navigate-registered-tool",
                tool_name="browser_navigate",
                principal_id="telegram_chat",
                arguments={"url": "https://cnn.com", "session_id": "telegram-shot"},
                capsule_id="telegram-cdp-regression",
            )
        )
        assert navigate_result.status == "completed"
        registered_session_id = str(navigate_result.output["session_id"])
        assert registered_session_id.startswith("task-")
        assert registered_session_id != "telegram-shot"
        model = StructuredPlanModel()
        settings = NullionSettings(
            telegram=TelegramSettings(
                bot_token="123456:abcdefghijklmnopqrstuvwxyz",
                operator_chat_id="100001",
                chat_enabled=True,
            )
        )
        monkeypatch.setattr("nullion.chat_operator._try_builder_reflection", lambda *args, **kwargs: None)

        reply = handle_chat_operator_message(
            runtime,
            "Send a screenshot of https://cnn.com",
            chat_id="100001",
            settings=settings,
            model_client=model,
            agent_orchestrator=AgentOrchestrator(model_client=model),
            append_activity_trace=False,
            allow_mini_agents=False,
        )
        assert reply is not None
        assert "MEDIA:" in reply
        assert "Navigation failed" not in reply
        assert "Screenshot failed" not in reply

        contract = delivery_contract_for_turn(
            "Send a screenshot of https://cnn.com",
            reply=reply,
            required_attachment_extensions=(".png",),
        )
        delivery = prepare_reply_for_platform_delivery(
            reply,
            principal_id="telegram_chat",
            delivery_contract=contract,
        )
        assert delivery.text == "Done — attached the requested screenshot."
        assert len(delivery.attachments) == 1
        assert delivery.attachments[0].read_bytes() == screenshot_png

        plan = plan_telegram_post_run_delivery(
            text_for_ack="Send a screenshot of https://cnn.com",
            reply=reply,
            stream_final_reply=True,
            streaming_mode="stream",
            final_only_streaming_mode="final",
            runtime=runtime,
            conversation_id="telegram:100001",
        )
        message = TelegramMessage()
        asyncio.run(
            _deliver_reply(
                message,
                reply,
                principal_id="telegram_chat",
                delivery_contract=plan.delivery_contract,
            )
        )
        assert message.text_replies == []
        assert len(message.documents) == 1
        uploaded_bytes, upload_kwargs = message.documents[0]
        assert uploaded_bytes == screenshot_png
        assert upload_kwargs["caption"] == "Done — attached the requested screenshot."
        assert "Page.navigate" in received_methods
        assert "Page.captureScreenshot" in received_methods
        assert "Browser.setDownloadBehavior" not in received_methods
    finally:
        registry.run_cleanup_hooks()
        server.close()


def test_cron_scheduler_execution_uses_workspace_derived_targets_when_delivery_target_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons, users
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.cron_delivery import CronRunDeliveryCallbacks, effective_cron_delivery_channel, cron_delivery_target, run_cron_delivery_workflow

    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    workspace_targets = (
        users.MessagingDeliveryTarget(
            channel="telegram",
            target_id="workspace-member-telegram",
            principal_id="user:member-reminder",
            workspace_id="workspace_reminders",
            user_id="member-reminder",
            display_name="Workspace Member",
        ),
    )

    settings = NullionSettings(telegram=TelegramSettings(bot_token="tg-token"))
    resolved_targets: list[tuple[str, str]] = []

    def _delivery_target(job, channel: str) -> str:
        direct_target = cron_delivery_target(job, channel, settings=settings)
        if direct_target:
            return direct_target
        for candidate in users.messaging_delivery_targets_for_workspace(job.workspace_id, settings=settings):
            if candidate.channel == channel and candidate.target_id:
                return candidate.target_id
        return ""

    def _run_agent_turn(job, conv_id: str):
        return {"text": f"scheduled output: {job.task}"}

    def _record_event(_event_type: str, _job, _channel: str, _target: str, _conversation_id: str, **_extra: object) -> None:
        pass

    def _save_web_delivery(*_args, **_kwargs) -> bool:
        return True

    def _send_platform(job, channel: str, text: str) -> bool:
        resolved_targets.append((channel, _delivery_target(job, channel), text[:10]))
        return True

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: effective_cron_delivery_channel(job, settings=settings),
        delivery_target=_delivery_target,
        run_agent_turn=_run_agent_turn,
        record_event=_record_event,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=_save_web_delivery,
        send_platform_delivery=_send_platform,
    )

    monkeypatch.setattr(users, "messaging_delivery_targets_for_workspace", lambda workspace_id, *, settings=None: workspace_targets)

    job = crons.add_cron(
        name="Cron with implicit workspace target",
        schedule="*/15 * * * *",
        task="Run workspace-derived target cron",
        workspace_id="workspace_reminders",
        delivery_channel="telegram",
        delivery_target="",
    )
    jobs = crons.load_crons()
    jobs[0].next_run = (datetime.now(UTC) - timedelta(minutes=1)).replace(second=0, microsecond=0).isoformat()
    crons.save_crons(jobs)

    scheduler = crons.CronScheduler(
        fire_fn=lambda cron_job: run_cron_delivery_workflow(
            cron_job,
            label="Scheduled task",
            callbacks=callbacks,
        ),
        tick_interval=0.01,
    )
    scheduler._tick()
    from cron_scheduler_helpers import finish_scheduler
    finish_scheduler(scheduler)

    assert resolved_targets == [
        (
            "telegram",
            "workspace-member-telegram",
            "⏰ **SCHEDU",
        )
    ]

    updated_job = crons.get_cron(job.id)
    assert updated_job is not None
    assert updated_job.last_result == "ok"


def test_cron_delivery_contract_e2e_matrix_covers_text_file_both_silent_and_default(tmp_path: Path) -> None:
    from nullion.cron_delivery import (
        CronRunDeliveryCallbacks,
        DEFAULT_CRON_NO_OUTPUT_MESSAGE,
        cron_structured_result_block_reason,
        run_cron_delivery_workflow,
        scheduled_task_delivery_text,
    )
    from nullion.crons import CronJob
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.response_sanitizer import safe_raw_tool_payload_replacement, sanitize_user_visible_reply
    from nullion.tools import ToolResult
    from nullion.workspace_storage import workspace_storage_roots_for_principal

    report = tmp_path / "nullion-artifact-cron-contract-report.json"
    report.write_text('{"status":"ready"}', encoding="utf-8")
    principal_id = "telegram:contract-chat"
    html_report = workspace_storage_roots_for_principal(principal_id).artifacts / "cron-contract-report.html"
    html_report.parent.mkdir(parents=True, exist_ok=True)
    html_report.write_text(
        "<!doctype html><html><head><title>Cron contract</title></head>"
        "<body><main><h1>Cron contract report</h1><p>Ready.</p></main></body></html>",
        encoding="utf-8",
    )
    previous_html_report = (
        workspace_storage_roots_for_principal(principal_id).artifacts
        / "cron-contract-report-previous.html"
    )
    previous_html_report.write_text(
        "<!doctype html><html><head><title>Previous cron contract</title></head>"
        "<body><main><h1>Previous report</h1></main></body></html>",
        encoding="utf-8",
    )
    internal_state = tmp_path / "nullion-artifact-cron-contract-state.json"
    internal_state.write_text('{"checkpoint":"internal"}', encoding="utf-8")
    monitor_checkpoint = workspace_storage_roots_for_principal(principal_id).artifacts / "pr_monitor_checkpoint-run.json"
    monitor_checkpoint.write_text('{"state":"unchanged"}', encoding="utf-8")
    verification_screenshot = workspace_storage_roots_for_principal(principal_id).artifacts / "verification-screenshot.png"
    verification_screenshot.write_bytes(b"\x89PNG\r\nverification")
    bundle_workbook = workspace_storage_roots_for_principal(principal_id).artifacts / "random-site-stress.xlsx"
    bundle_document = workspace_storage_roots_for_principal(principal_id).artifacts / "random-site-stress.docx"
    bundle_deck = workspace_storage_roots_for_principal(principal_id).artifacts / "random-site-stress.pptx"
    bundle_pdf = workspace_storage_roots_for_principal(principal_id).artifacts / "random-site-stress.pdf"
    bundle_html = workspace_storage_roots_for_principal(principal_id).artifacts / "random-site-stress.html"
    bundle_csv = workspace_storage_roots_for_principal(principal_id).artifacts / "random-site-stress.csv"
    for path, required_member in (
        (bundle_workbook, "xl/workbook.xml"),
        (bundle_document, "word/document.xml"),
        (bundle_deck, "ppt/presentation.xml"),
    ):
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("[Content_Types].xml", "<Types />")
            archive.writestr(required_member, "<xml />")
    bundle_pdf.write_bytes(b"%PDF-1.4\n%cron\n")
    bundle_csv.write_text("name,url\nExample,https://example.com\n", encoding="utf-8")
    bundle_html.write_text(
        "<!doctype html><html><head><title>Random site stress</title></head>"
        "<body><main><h1>Random site stress</h1><p>Ready.</p></main></body></html>",
        encoding="utf-8",
    )
    connector_output = {
        "messages": [
            {
                "id": "msg-1",
                "title": "Account notice",
                "source": "billing-connector",
                "summary": "Review your account notice.",
            }
        ]
    }
    connector_fallback_text = safe_raw_tool_payload_replacement(
        tool_results=[ToolResult("connector-1", "connector_request", "completed", connector_output)],
        source="agent",
    )
    cron_contract_task = "Structured cron delivery contract exercise"
    web_search_output = {
        "query": "CRML MSFT OKLO mid market stock price quote news",
        "results": [
            {
                "title": "Netflix: NFLX Stock Price Quote & News - Robinhood",
                "url": "https://robinhood.com/stocks/NFLX/",
                "snippet": "Advertisement Recent share performance and business snapshot Netflix (NFLX) has drawn attention.",
            },
            {
                "title": "Critical Metals Corp. (CRML) Stock Price, News, Quote & History",
                "url": "https://finance.yahoo.com/quote/CRML/",
                "snippet": "Trailing total returns as of 6/15/2026, which may include dividends.",
            },
            {
                "title": "Oklo: OKLO Stock Price Quote & News - Robinhood",
                "url": "https://robinhood.com/stocks/OKLO/",
                "snippet": "As of 2026-06-16, Oklo stock has fluctuated between $58.02 and $61.95.",
            },
        ],
    }
    web_search_leads_fallback = sanitize_user_visible_reply(
        user_message=cron_contract_task,
        reply="The scheduled task report is ready.",
        tool_results=[ToolResult("search-1", "web_search", "completed", web_search_output)],
    )
    internal_reference_text = "# Connector Routing Reference\n\n" + ("Use these internal endpoint notes. " * 20)

    cases = [
        {
            "name": "text-only",
            "agent_result": {"text": "Status ready"},
            "status": "sent",
            "sent_text": "Status ready",
            "delivery_text": "Status ready",
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "file-only",
            "agent_result": {"text": f"MEDIA:{report}"},
            "status": "sent",
            "sent_text": f"MEDIA:{report}",
            "delivery_text": None,
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (report,),
        },
        {
            "name": "text-and-file",
            "agent_result": {"text": f"Status ready\nMEDIA:{report}"},
            "status": "sent",
            "sent_text": f"Status ready\nMEDIA:{report}",
            "delivery_text": "Status ready",
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (report,),
        },
        {
            "name": "structured-tool-artifact",
            "agent_result": {
                "final_text": "Resumen listo",
                "tool_results": [
                    {
                        "tool_name": "pdf_create",
                        "status": "completed",
                        "output": {"artifact_path": str(report)},
                    }
                ],
            },
            "status": "sent",
            "sent_text": f"Resumen listo\n\nMEDIA:{report}",
            "delivery_text": "Resumen listo",
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (report,),
        },
        {
            "name": "structured-artifact-bundle-text-mentions-only-one-output",
            "agent_result": {
                "final_text": f"Generated and verified artifact bundle.\n\nMEDIA:{bundle_pdf}",
                "tool_results": [
                    {
                        "tool_name": "spreadsheet_create",
                        "status": "completed",
                        "output": {"artifact_path": str(bundle_workbook)},
                    },
                    {
                        "tool_name": "document_create",
                        "status": "completed",
                        "output": {"artifact_path": str(bundle_document)},
                    },
                    {
                        "tool_name": "presentation_create",
                        "status": "completed",
                        "output": {"artifact_path": str(bundle_deck)},
                    },
                    {
                        "tool_name": "pdf_create",
                        "status": "completed",
                        "output": {"artifact_path": str(bundle_pdf)},
                    },
                    {
                        "tool_name": "file_write",
                        "status": "completed",
                        "output": {"path": str(bundle_html), "bytes_written": bundle_html.stat().st_size},
                    },
                    {
                        "tool_name": "file_write",
                        "status": "completed",
                        "output": {"path": str(bundle_csv), "bytes_written": bundle_csv.stat().st_size},
                    },
                ],
            },
            "status": "sent",
            "sent_text": (
                "Generated and verified artifact bundle.\n\n"
                f"MEDIA:{bundle_pdf}\n"
                f"MEDIA:{bundle_workbook}\n"
                f"MEDIA:{bundle_document}\n"
                f"MEDIA:{bundle_deck}\n"
                f"MEDIA:{bundle_html}\n"
                f"MEDIA:{bundle_csv}"
            ),
            "delivery_text": "Generated and verified artifact bundle.",
            "attachment_count": 6,
            "media_count": 6,
            "attachments": (
                bundle_pdf,
                bundle_workbook,
                bundle_document,
                bundle_deck,
                bundle_html,
                bundle_csv,
            ),
        },
        {
            "name": "file-write-artifact-path",
            "agent_result": {
                "final_text": "Cron HTML report ready",
                "tool_results": [
                    {
                        "tool_name": "file_write",
                        "status": "completed",
                        "output": {"path": str(html_report), "bytes_written": html_report.stat().st_size},
                    }
                ],
            },
            "status": "sent",
            "sent_text": f"Cron HTML report ready\n\nMEDIA:{html_report}",
            "delivery_text": "Cron HTML report ready",
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (html_report,),
        },
        {
            "name": "plain-report-artifact-path",
            "agent_result": {
                "final_text": "Cron HTML report generated.\nReport: artifacts/cron-contract-report.html",
                "tool_results": [
                    {
                        "tool_name": "terminal_exec",
                        "status": "completed",
                        "output": {"returncode": 0},
                    }
                ],
            },
            "status": "sent",
            "sent_text": f"Cron HTML report generated.\n\nMEDIA:{html_report}",
            "delivery_text": "Cron HTML report generated.",
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (html_report,),
        },
        {
            "name": "file-read-previous-report-file-write-current-report",
            "agent_result": {
                "final_text": "Cron HTML report refreshed",
                "tool_results": [
                    {
                        "tool_name": "file_read",
                        "status": "completed",
                        "output": {
                            "path": str(previous_html_report),
                            "content": previous_html_report.read_text(encoding="utf-8"),
                        },
                    },
                    {
                        "tool_name": "file_write",
                        "status": "completed",
                        "output": {"path": str(html_report), "bytes_written": html_report.stat().st_size},
                    },
                    {
                        "tool_name": "file_read",
                        "status": "completed",
                        "output": {
                            "path": str(html_report),
                            "content": html_report.read_text(encoding="utf-8"),
                        },
                    },
                ],
            },
            "status": "sent",
            "sent_text": f"Cron HTML report refreshed\n\nMEDIA:{html_report}",
            "delivery_text": "Cron HTML report refreshed",
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (html_report,),
        },
        {
            "name": "html-report-with-browser-verification-screenshot",
            "agent_result": {
                "final_text": "Cron HTML report generated and verified readable.",
                "tool_results": [
                    {
                        "tool_name": "file_write",
                        "status": "completed",
                        "output": {"path": str(html_report), "bytes_written": html_report.stat().st_size},
                    },
                    {
                        "tool_name": "browser_screenshot",
                        "status": "completed",
                        "output": {
                            "path": str(verification_screenshot),
                            "artifact_path": str(verification_screenshot),
                            "artifact_paths": [str(verification_screenshot)],
                        },
                    },
                ],
            },
            "status": "sent",
            "sent_text": f"Cron HTML report generated and verified readable.\n\nMEDIA:{html_report}",
            "delivery_text": "Cron HTML report generated and verified readable.",
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (html_report,),
        },
        {
            "name": "split-artifact-directive-normalized",
            "agent_result": {
                "final_text": f"Status ready\n\nMEDIA\n{report}",
                "tool_results": [
                    {
                        "tool_name": "pdf_create",
                        "status": "completed",
                        "output": {"artifact_paths": [str(report)]},
                    }
                ],
            },
            "status": "sent",
            "sent_text": f"Status ready\n\nMEDIA:{report}",
            "delivery_text": "Status ready",
            "attachment_count": 1,
            "media_count": 1,
            "attachments": (report,),
        },
        {
            "name": "internal-artifact-is-not-attached",
            "agent_result": {"text": "State refreshed", "artifacts": [{"path": str(internal_state)}]},
            "status": "sent",
            "sent_text": "State refreshed",
            "delivery_text": "State refreshed",
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "bare-json-checkpoint-path-is-not-attached",
            "agent_result": {
                "text": f"Nothing changed.\nCurrent monitor state was persisted at artifacts/{monitor_checkpoint.name}.",
                "artifacts": [{"path": str(monitor_checkpoint)}],
            },
            "status": "sent",
            "sent_text": "Nothing changed.",
            "delivery_text": "Nothing changed.",
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "explicit-json-checkpoint-media-is-not-attached",
            "agent_result": {
                "text": f"Nothing changed.\n\nMEDIA:{monitor_checkpoint}",
                "artifacts": [{"path": str(monitor_checkpoint)}],
            },
            "status": "sent",
            "sent_text": "Nothing changed.",
            "delivery_text": "Nothing changed.",
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "default-no-output-message",
            "agent_result": {"text": DEFAULT_CRON_NO_OUTPUT_MESSAGE},
            "status": "sent",
            "sent_text": DEFAULT_CRON_NO_OUTPUT_MESSAGE,
            "delivery_text": DEFAULT_CRON_NO_OUTPUT_MESSAGE,
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "silent-no-change",
            "agent_result": {},
            "status": "silent",
            "sent_text": None,
            "delivery_text": None,
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "raw-connector-payload-fallback",
            "agent_result": {
                "final_text": "I checked connector data and received 1 structured item.",
                "raw_tool_payload_blocked": True,
                "tool_results": [
                    {
                        "tool_name": "connector_request",
                        "status": "completed",
                        "output": {"messages": [{"id": "msg-1", "title": "Deadline"}]},
                    }
                ],
            },
            "status": "failed",
            "reason": "cron_run_raw_tool_payload",
            "sent_text": None,
            "delivery_text": None,
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "model-repeated-connector-sanitizer-fallback",
            "agent_result": {
                "final_text": connector_fallback_text,
                "raw_tool_payload_blocked": True,
                "tool_results": [
                    {
                        "tool_name": "connector_request",
                        "status": "completed",
                        "output": connector_output,
                    }
                ],
            },
            "status": "failed",
            "reason": "cron_run_raw_tool_payload",
            "sent_text": None,
            "delivery_text": None,
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "web-search-leads-fallback",
            "agent_result": {
                "final_text": web_search_leads_fallback,
                "cron_task": cron_contract_task,
                "tool_results": [
                    {
                        "tool_name": "web_search",
                        "status": "completed",
                        "output": web_search_output,
                    }
                ],
            },
            "status": "failed",
            "reason": "cron_run_raw_tool_payload",
            "sent_text": None,
            "delivery_text": None,
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "internal-reference-tool-output-leak",
            "agent_result": {
                "final_text": internal_reference_text,
                "tool_results": [
                    {
                        "tool_name": "skill_pack_read",
                        "status": "completed",
                        "output": {
                            "path": "references/google-mail/README.md",
                            "text": internal_reference_text,
                        },
                    }
                ],
            },
            "status": "failed",
            "reason": "cron_run_internal_tool_output_leaked",
            "sent_text": None,
            "delivery_text": None,
            "attachment_count": 0,
            "media_count": 0,
        },
        {
            "name": "raw-non-connector-payload-fallback",
            "agent_result": {
                "final_text": "I blocked a raw structured payload from agent output before sending it here.",
                "raw_tool_payload_blocked": True,
                "tool_results": [
                    {
                        "tool_name": "file_read",
                        "status": "completed",
                        "output": {"path": "/tmp/internal-state.json"},
                    }
                ],
            },
            "status": "failed",
            "reason": "cron_run_raw_tool_payload",
            "sent_text": None,
            "delivery_text": None,
            "attachment_count": 0,
            "media_count": 0,
        },
    ]

    for case in cases:
        sent: list[str] = []
        events: list[str] = []
        cleared: list[str] = []
        job = CronJob(
            id=f"cron-{case['name']}",
            name=f"Cron contract {case['name']}",
            schedule="*/15 * * * *",
            task=cron_contract_task,
            delivery_channel="telegram",
            delivery_target="contract-chat",
        )
        callbacks = CronRunDeliveryCallbacks(
            effective_channel=lambda _job: "telegram",
            delivery_target=lambda _job, _channel: "contract-chat",
            run_agent_turn=lambda _job, _conv_id, result=case["agent_result"]: result,
            record_event=lambda event_type, *_args, **_kwargs: events.append(event_type),
            block_reason=lambda result, text, artifacts: cron_structured_result_block_reason(
                result,
                artifacts,
                text=text,
            ),
            save_web_delivery=lambda *_args, **_kwargs: False,
            send_platform_delivery=lambda _job, _channel, text: sent.append(text) or True,
            start_background_delivery=lambda _conv_id, _job: None,
            clear_background_delivery=lambda conv_id: cleared.append(conv_id),
        )

        result = run_cron_delivery_workflow(job, label="Scheduled task", callbacks=callbacks)

        assert result["cron_delivery_status"] == case["status"], case["name"]
        if case["status"] == "silent":
            assert sent == [], case["name"]
            assert "cron.delivery.silent" in events
            assert cleared == [principal_id], case["name"]
            continue
        if case["status"] == "failed":
            assert sent == [], case["name"]
            assert "cron.delivery.failed" in events
            assert result["reason"] == case["reason"], case["name"]
            assert cleared == [principal_id], case["name"]
            continue

        assert sent == [scheduled_task_delivery_text(job, case["sent_text"])], case["name"]
        assert "cron.delivery.sent" in events
        assert "cron.delivery.repaired" not in events
        assert cleared == [principal_id], case["name"]
        delivery = prepare_reply_for_platform_delivery(sent[0], principal_id=principal_id)
        expected_delivery_text = (
            scheduled_task_delivery_text(job, case["delivery_text"])
            if case["delivery_text"] is not None
            else "⏰ **SCHEDULED TASK: " + job.name + "**"
        )
        assert delivery.text == expected_delivery_text, case["name"]
        assert len(delivery.attachments) == case["attachment_count"], case["name"]
        assert delivery.media_directive_count == case["media_count"], case["name"]
        if case["attachment_count"]:
            assert delivery.attachments == case["attachments"], case["name"]


def test_manual_background_cron_run_does_not_surface_json_media_attachment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.tools import ToolInvocation, _build_list_crons_handler, _build_run_cron_handler
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")

    tracker = tmp_path / "nullion-artifact-payment_reminder_tracker.json"
    tracker.write_text('{"items":[]}', encoding="utf-8")
    job = crons.add_cron(
        name="Morning payment and urgent email tracker",
        schedule="0 9 * * *",
        task=f"Maintain {tmp_path / 'files' / 'payment_reminder_tracker.json'} and notify only on meaningful changes.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="100001",
    )
    listed = _build_list_crons_handler()(ToolInvocation("inv-list-crons", "list_crons", "telegram_chat", {}))

    assert listed.status == "completed"
    assert listed.output["crons"][0]["task"] == job.task
    assert "payment_reminder_tracker" not in listed.output["message"]
    assert "task:" not in listed.output["message"]

    def _runner(_job):
        return {
            "final_text": (
                "Estado guardado\n\n"
                f"MEDIA:{tracker}\n\n"
                "Activity\n"
                "✓ file_read — payment_reminder_tracker.json\n"
                "⊗ connector_request — URL can't contain control characters\n"
                "✓ file_write — payment_reminder_tracker.json\n"
                "✨ Learned a new skill: Track payment reminders"
            ),
            "artifacts": [{"path": str(tracker)}],
            "artifact_paths": [{"path": str(tracker)}],
        }

    result = _build_run_cron_handler(_runner)(
        ToolInvocation("inv-run-cron", "run_cron", "telegram_chat", {"id": job.id})
    )

    assert result.status == "completed"
    assert result.output["result_text"] == "Cron ran successfully; no output was produced."
    assert result.output["foreground_auto_attach_created_artifacts"] is False
    assert "artifact_paths" not in result.output
    serialized_result = json.dumps(result.output, ensure_ascii=False)
    assert "MEDIA:" not in serialized_result
    assert "payment_reminder_tracker" not in serialized_result
    assert "Activity" not in serialized_result
    assert "URL can't contain" not in serialized_result
    assert "Learned a new skill" not in serialized_result
    assert "artifact_paths" not in result.output["result"]
    assert "artifacts" not in result.output["result"]

    delivery = prepare_reply_for_platform_delivery(
        result.output["result_text"],
        principal_id="telegram:100001",
    )
    assert delivery.attachments == ()
    assert delivery.media_directive_count == 0

    stored = crons.get_cron(job.id)
    assert stored is not None
    assert "MEDIA:" not in (stored.last_result or "")


@pytest.mark.parametrize("channel", ["telegram", "slack", "discord", "web"])
def test_cron_raw_connector_fallback_is_blocked_for_every_delivery_channel(channel: str) -> None:
    from nullion.cron_delivery import (
        CronRunDeliveryCallbacks,
        cron_structured_result_block_reason,
        run_cron_delivery_workflow,
    )
    from nullion.crons import CronJob

    connector_results = [
        {
            "tool_name": "connector_request",
            "status": "completed",
            "output": {"messages": [{"id": "msg-a"}, {"id": "msg-b"}]},
        },
        {
            "tool_name": "connector_request",
            "status": "completed",
            "output": {
                "id": "msg-a",
                "title": "Renewal decision needed",
                "source": "housing-connector",
                "summary": "Please review the renewal document.",
            },
        },
        {
            "tool_name": "connector_request",
            "status": "completed",
            "output": {
                "id": "msg-b",
                "title": "Payment scheduled",
                "source": "payments-connector",
                "summary": "Your payment has been scheduled.",
            },
        },
    ]
    agent_result = {
        "final_text": "Connector result summary:\n- subject: Payment scheduled",
        "raw_tool_payload_blocked": True,
        "tool_results": connector_results,
    }
    sent: list[tuple[str, str]] = []
    saved: list[tuple[str, str]] = []
    events: list[str] = []
    job = CronJob(
        id=f"cron-raw-repair-{channel}",
        name="Informe de correo programado",
        schedule="0 8 * * *",
        task="Use connector_request and return a readable report.",
        delivery_channel=channel,
        delivery_target=f"{channel}-target",
    )

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: channel,
        delivery_target=lambda _job, _channel: f"{channel}-target",
        run_agent_turn=lambda _job, _conv_id: agent_result,
        record_event=lambda event_type, *_args, **_kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: cron_structured_result_block_reason(
            result,
            artifacts,
            text=text,
        ),
        save_web_delivery=lambda _job, _conv_id, text, _artifacts, _result: saved.append((channel, text)) or True,
        send_platform_delivery=lambda _job, delivery_channel, text: sent.append((delivery_channel, text)) or True,
        start_background_delivery=lambda _conv_id, _job: None,
        clear_background_delivery=lambda _conv_id: None,
    )

    result = run_cron_delivery_workflow(job, label="Tarea programada", callbacks=callbacks)

    assert "cron.delivery.repaired" not in events
    assert "cron.delivery.failed" in events
    assert result.get("cron_delivery_repaired") is not True
    assert result["cron_delivery_status"] == "failed"
    assert result["reason"] == "cron_run_raw_tool_payload"
    assert saved == []
    assert sent == []


@pytest.mark.parametrize("channel", ["telegram", "slack", "discord", "web"])
def test_cron_account_identifier_leak_is_blocked_for_every_delivery_channel(channel: str) -> None:
    from nullion.cron_delivery import (
        CronRunDeliveryCallbacks,
        cron_structured_result_block_reason,
        run_cron_delivery_workflow,
    )
    from nullion.crons import CronJob

    internal_id = "19e4e04ed4b961f3"
    agent_result = {
        "final_text": (
            "Recordatorio enviado 2026-06-16: tarjeta American Express 15007; "
            f"identificador interno {internal_id}."
        ),
        "tool_results": [
            {
                "tool_name": "email_search",
                "status": "completed",
                "output": {
                    "messages": [
                        {
                            "id": internal_id,
                            "subject": "Important Notice: Your May 2026 Statement",
                            "from": "American Express",
                            "snippet": "Your statement is ready.",
                        }
                    ]
                },
            }
        ],
    }
    sent: list[tuple[str, str]] = []
    saved: list[tuple[str, str]] = []
    events: list[str] = []
    job = CronJob(
        id=f"cron-account-id-leak-{channel}",
        name="Cron account identifier leak contract",
        schedule="0 8 * * *",
        task="Use account tools and return a user-facing report.",
        delivery_channel=channel,
        delivery_target=f"{channel}-target",
    )

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: channel,
        delivery_target=lambda _job, _channel: f"{channel}-target",
        run_agent_turn=lambda _job, _conv_id: agent_result,
        record_event=lambda event_type, *_args, **_kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: cron_structured_result_block_reason(
            result,
            artifacts,
            text=text,
        ),
        save_web_delivery=lambda _job, _conv_id, text, _artifacts, _result: saved.append((channel, text)) or True,
        send_platform_delivery=lambda _job, delivery_channel, text: sent.append((delivery_channel, text)) or True,
        start_background_delivery=lambda _conv_id, _job: None,
        clear_background_delivery=lambda _conv_id: None,
    )

    result = run_cron_delivery_workflow(job, label="Tarea programada", callbacks=callbacks)

    assert "cron.delivery.failed" in events
    assert result["cron_delivery_status"] == "failed"
    assert result["reason"] == "cron_run_raw_tool_payload"
    assert result["cron_sensitive_tool_identifier_withheld"] is True
    assert saved == []
    assert sent == []


@pytest.mark.parametrize("channel", ["telegram", "slack", "discord", "web"])
def test_cron_internal_reference_output_is_blocked_for_every_delivery_channel(channel: str) -> None:
    from nullion.cron_delivery import (
        CronRunDeliveryCallbacks,
        cron_structured_result_block_reason,
        run_cron_delivery_workflow,
    )
    from nullion.crons import CronJob

    reference_text = "# Connector Routing Reference\n\n" + ("Use these internal endpoint notes. " * 20)
    agent_result = {
        "final_text": reference_text,
        "tool_results": [
            {
                "tool_name": "skill_pack_read",
                "status": "completed",
                "output": {
                    "path": "references/google-mail/README.md",
                    "text": reference_text,
                },
            }
        ],
    }
    sent: list[tuple[str, str]] = []
    saved: list[tuple[str, str]] = []
    events: list[str] = []
    job = CronJob(
        id=f"cron-reference-leak-{channel}",
        name="Cron reference leak contract",
        schedule="0 8 * * *",
        task="Use available tools and return a report.",
        delivery_channel=channel,
        delivery_target=f"{channel}-target",
    )

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: channel,
        delivery_target=lambda _job, _channel: f"{channel}-target",
        run_agent_turn=lambda _job, _conv_id: agent_result,
        record_event=lambda event_type, *_args, **_kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: cron_structured_result_block_reason(
            result,
            artifacts,
            text=text,
        ),
        save_web_delivery=lambda _job, _conv_id, text, _artifacts, _result: saved.append((channel, text)) or True,
        send_platform_delivery=lambda _job, delivery_channel, text: sent.append((delivery_channel, text)) or True,
        start_background_delivery=lambda _conv_id, _job: None,
        clear_background_delivery=lambda _conv_id: None,
    )

    result = run_cron_delivery_workflow(job, label="Tarea programada", callbacks=callbacks)

    assert "cron.delivery.failed" in events
    assert result["cron_delivery_status"] == "failed"
    assert result["reason"] == "cron_run_internal_tool_output_leaked"
    assert saved == []
    assert sent == []


def test_manual_cron_raw_connector_block_does_not_surface_connector_details(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons
    from nullion.response_sanitizer import safe_raw_tool_payload_replacement
    from nullion.tools import ToolInvocation, ToolResult, _build_run_cron_handler
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")

    connector_output = {
        "messages": [
            {
                "title": "Account notice",
                "source": "billing-connector",
                "summary": "Review your account notice.",
            }
        ]
    }
    blocked_text = safe_raw_tool_payload_replacement(
        tool_results=[ToolResult("connector-1", "connector_request", "completed", connector_output)],
        source="agent",
    )
    job = crons.add_cron(
        name="Connector-backed scheduled report",
        schedule="0 8,13,18 * * *",
        task="Use connector_request and produce a concise report.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="100001",
    )

    def _runner(_job):
        return {
            "cron_delivery_failed": True,
            "cron_run_failed": True,
            "cron_delivery_status": "failed",
            "raw_tool_payload_blocked": True,
            "reason": "cron_run_raw_tool_payload",
            "text": blocked_text,
            "tool_results": [
                {
                    "tool_name": "connector_request",
                    "status": "completed",
                    "output": connector_output,
                }
            ],
        }

    result = _build_run_cron_handler(_runner)(
        ToolInvocation("inv-run-cron", "run_cron", "telegram_chat", {"id": job.id})
    )

    assert result.status == "failed"
    assert result.output["reason"] == "cron_run_raw_tool_payload"
    assert result.output["result_text"] == (
        "Cron run was blocked because it produced raw structured tool output "
        "instead of a deliverable report."
    )
    serialized = json.dumps(result.output, ensure_ascii=False)
    assert "billing-connector" not in serialized
    assert "Account notice" not in serialized
    assert "Review your account notice" not in serialized


def test_telegram_registered_run_cron_uses_originating_chat_and_planner_without_outer_wrapper(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons, telegram_app, telegram_entrypoint
    from nullion.tools import ToolInvocation, ToolRegistry

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "123456:telegram-token")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "999")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")

    job = crons.CronJob(
        id="cron-telegram-origin-feed",
        name="Telegram origin feed cron",
        schedule="0 * * * *",
        task="Create a planner-backed HTML report.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="999",
    )
    crons.save_crons([job])

    registry = ToolRegistry()
    captured: dict[str, object] = {"deliveries": [], "send_targets": []}
    completed = threading.Event()
    sent = threading.Event()
    terminal_status_sent = threading.Event()

    class FakePlannerPreview:
        group_id = "grp-telegram-origin-preview"
        allowed_tools = ("terminal_exec",)
        summary = "Parallel Mission"
        group = SimpleNamespace(
            planner_metadata={"planner": "model_dag", "disposition": "parallel_mission"},
            tasks=[
                SimpleNamespace(
                    task_id="collect",
                    title="Collect content",
                    description="Gather report content.",
                    allowed_tools=[],
                    dependencies=[],
                    context_key_in=None,
                    context_key_out="content",
                    metadata={},
                    priority=None,
                ),
                SimpleNamespace(
                    task_id="report",
                    title="Build report",
                    description="Create the report from gathered content.",
                    allowed_tools=[],
                    dependencies=["collect"],
                    context_key_in="content",
                    context_key_out="report",
                    metadata={},
                    priority=None,
                ),
            ],
        )

        def initial_text(self) -> str:
            return "PLANNER\nFor: Telegram origin feed cron\n2 tasks"

        def terminal_text(self, *, success: bool) -> str:
            captured["preview_terminal_success"] = success
            return "PLANNER\nFor: Telegram origin feed cron\nDone"

        def with_group_id(self, group_id: str):
            self.group_id = group_id
            return self

    def deliver_fn(conversation_id: str, text: str, **kwargs):  # noqa: ANN003
        captured["deliveries"].append({"conversation_id": conversation_id, "text": text, **kwargs})

    class FakeOrchestrator:
        model_client = object()
        _deliver_fn = staticmethod(deliver_fn)

        def dispatch_request_sync(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("manual cron should not dispatch to mini agents")

        def run_turn(self, **kwargs):  # noqa: ANN003
            captured["run_turn"] = kwargs
            completed.set()
            return SimpleNamespace(
                final_text="Telegram origin report finished.",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    def service_factory(runtime, *, settings):  # noqa: ANN001
        _ = runtime
        return SimpleNamespace(
            settings=settings,
            model_client=object(),
            agent_orchestrator=FakeOrchestrator(),
            refresh_live_configuration=lambda: None,
        )

    async def fake_send_operator_telegram_delivery(bot_token, target, message, **kwargs):  # noqa: ANN001, ANN003
        captured["send_targets"].append(target)
        captured["sent_message"] = message
        captured["sent_kwargs"] = kwargs
        sent.set()
        return True

    async def fake_status_delivery(*_args, **_kwargs):  # noqa: ANN002, ANN003
        if sent.is_set():
            terminal_status_sent.set()
        return True

    monkeypatch.setattr(telegram_entrypoint, "_terminal_executor_backend_from_settings", lambda settings: object())
    monkeypatch.setattr(telegram_entrypoint, "create_plugin_tool_registry", lambda **kwargs: registry)
    monkeypatch.setattr(telegram_entrypoint, "_validate_plugin_bootstrap_settings", lambda settings: {})
    monkeypatch.setattr(telegram_entrypoint, "_register_enabled_plugins", lambda **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "register_media_plugin", lambda *args, **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "register_reminder_tools", lambda *args, **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "_resolve_browser_backend", lambda: None)
    monkeypatch.setattr(telegram_entrypoint, "default_browser_backend_for_web_research", lambda settings: None)
    monkeypatch.setattr(telegram_entrypoint, "_send_operator_telegram_delivery", fake_send_operator_telegram_delivery)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", fake_status_delivery)

    telegram_entrypoint._build_runtime_service_from_settings(
        checkpoint_path=tmp_path / "runtime.db",
        service_factory=service_factory,
    )

    result = registry.invoke(
        ToolInvocation(
            "inv-telegram-origin",
            "run_cron",
            "telegram:100001",
            {"id": job.id},
        )
    )

    assert result.status == "completed"
    assert result.output["delivery_status"] == "deferred"
    assert result.output["status_delivered"] is False
    assert completed.wait(2)
    assert "mini_agent_dispatch" not in result.output
    assert result.output["task_group_id"].startswith("manual-cron-")
    assert "◐ Running" in result.output["progress_status_text"]
    assert captured["run_turn"]["conversation_id"] == "telegram:100001"
    assert captured["run_turn"]["principal_id"] == "telegram:100001"
    assert captured["deliveries"] == []
    assert sent.wait(2)
    assert captured["send_targets"] == ["100001"]
    assert terminal_status_sent.wait(2)


def test_telegram_cron_hides_connector_tool_when_scope_decision_denies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons, cron_delivery, telegram_app, telegram_entrypoint
    from nullion.cron_execution_tools import CronConnectorScopeDecision
    from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "123456:telegram-token")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "999")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")

    job = crons.CronJob(
        id="cron-telegram-public-feed",
        name="Public report cron",
        schedule="0 * * * *",
        task="Construye un informe HTML publico con datos de mercado desde URLs publicas verificadas.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="999",
    )
    crons.save_crons([job])

    registry = ToolRegistry()
    captured: dict[str, object] = {"deliveries": [], "send_targets": []}
    completed = threading.Event()
    sent = threading.Event()
    terminal_status_sent = threading.Event()
    empty_schema = {"type": "object", "properties": {}}

    def _complete(invocation: ToolInvocation) -> ToolResult:
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {})

    registry.register(
        ToolSpec(
            "terminal_exec",
            "Run shell",
            ToolRiskLevel.HIGH,
            ToolSideEffectClass.DANGEROUS_EXEC,
            False,
            10,
            input_schema=empty_schema,
        ),
        _complete,
    )
    registry.register(
        ToolSpec(
            "connector_request",
            "Connector gateway",
            ToolRiskLevel.HIGH,
            ToolSideEffectClass.ACCOUNT_WRITE,
            False,
            10,
            input_schema=empty_schema,
            capability_tags=("connector",),
        ),
        _complete,
    )

    def deliver_fn(conversation_id: str, text: str, **kwargs):  # noqa: ANN003
        captured["deliveries"].append({"conversation_id": conversation_id, "text": text, **kwargs})

    class FakeOrchestrator:
        model_client = object()
        _deliver_fn = staticmethod(deliver_fn)

        def dispatch_request_sync(self, **kwargs):  # noqa: ANN003
            raise AssertionError("cron must not use DeepAgent dispatch")

        def run_turn(self, **kwargs):  # noqa: ANN003
            tool_registry = kwargs["tool_registry"]
            captured["run_tool_names"] = [
                definition["name"] for definition in tool_registry.list_tool_definitions()
            ]
            denied = tool_registry.invoke(
                ToolInvocation(
                    "tool-public-connector",
                    "connector_request",
                    kwargs["principal_id"],
                    {
                        "provider_id": "skill_pack_connector_maton_ai_api_gateway_skill",
                        "url": "https://query1.finance.yahoo.com/v8/finance/chart/NVDA",
                    },
                )
            )
            captured["denied_connector_reason"] = denied.output.get("reason")
            captured["denied_connector_suppressed"] = denied.output.get("suppress_activity")
            completed.set()
            return SimpleNamespace(
                final_text="Public cron finished without connector.",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    def service_factory(runtime, *, settings):  # noqa: ANN001
        _ = runtime
        return SimpleNamespace(
            settings=settings,
            model_client=object(),
            agent_orchestrator=FakeOrchestrator(),
            refresh_live_configuration=lambda: None,
        )

    async def fake_send_operator_telegram_delivery(bot_token, target, message, **kwargs):  # noqa: ANN001, ANN003
        captured["send_targets"].append(target)
        captured["sent_message"] = message
        captured["sent_kwargs"] = kwargs
        sent.set()
        return True

    async def fake_status_delivery(*_args, **_kwargs):  # noqa: ANN002, ANN003
        if sent.is_set():
            terminal_status_sent.set()
        return True

    monkeypatch.setattr(telegram_entrypoint, "_terminal_executor_backend_from_settings", lambda settings: object())
    monkeypatch.setattr(telegram_entrypoint, "create_plugin_tool_registry", lambda **kwargs: registry)
    monkeypatch.setattr(telegram_entrypoint, "_validate_plugin_bootstrap_settings", lambda settings: {})
    monkeypatch.setattr(telegram_entrypoint, "_register_enabled_plugins", lambda **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "register_media_plugin", lambda *args, **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "register_reminder_tools", lambda *args, **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "_resolve_browser_backend", lambda: None)
    monkeypatch.setattr(telegram_entrypoint, "default_browser_backend_for_web_research", lambda settings: None)
    monkeypatch.setattr(telegram_entrypoint, "_send_operator_telegram_delivery", fake_send_operator_telegram_delivery)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", fake_status_delivery)

    telegram_entrypoint._build_runtime_service_from_settings(
        checkpoint_path=tmp_path / "runtime.db",
        service_factory=service_factory,
    )

    result = registry.invoke(
        ToolInvocation(
            "inv-public-cron",
            "run_cron",
            "telegram:100001",
            {"id": job.id},
        )
    )

    assert result.status == "completed"
    assert result.output["delivery_status"] == "deferred"
    assert completed.wait(2)
    assert captured["run_tool_names"] == ["terminal_exec"]
    assert captured["denied_connector_reason"] == "cron_execution_connector_scope_denied"
    assert captured["denied_connector_suppressed"] is True
    # A completed model call is not terminal delivery. Keep the transport
    # boundary fakes installed until the actual background run has finished.
    assert sent.wait(2)
    assert terminal_status_sent.wait(2)
    assert captured["send_targets"] == ["100001"]
    deadline = time.monotonic() + 2
    while True:
        with cron_delivery._MANUAL_CRON_BACKGROUND_RUNS_LOCK:
            pending = result.output["task_group_id"] in cron_delivery._MANUAL_CRON_BACKGROUND_RUNS_BY_CONVERSATION.get("telegram:100001", {})
        if not pending or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    assert not pending


@pytest.mark.parametrize(
    ("channel", "target", "principal_id"),
    [
        ("telegram", "100001", "telegram:100001"),
        ("slack", "C123", "slack:C123"),
        ("discord", "987654321", "discord:987654321"),
    ],
)
def test_cron_html_artifact_delivery_crosses_each_messaging_platform_boundary(
    tmp_path: Path,
    channel: str,
    target: str,
    principal_id: str,
) -> None:
    from nullion.cron_delivery import (
        CronRunDeliveryCallbacks,
        cron_structured_result_block_reason,
        run_cron_delivery_workflow,
    )
    from nullion.crons import CronJob
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.workspace_storage import workspace_storage_roots_for_principal

    _ = tmp_path
    report = workspace_storage_roots_for_principal(principal_id).artifacts / f"{channel}-report.html"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        "<!doctype html><html><body><main><h1>Traffic report</h1><p>Ready.</p></main></body></html>",
        encoding="utf-8",
    )
    sent: list[dict[str, str]] = []
    events: list[str] = []
    job = CronJob(
        id=f"cron-{channel}-html-report",
        name=f"{channel.title()} HTML report cron",
        schedule="0 * * * *",
        task="Create the HTML report artifact.",
        workspace_id="workspace_admin",
        delivery_channel=channel,
        delivery_target=target,
    )

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda _job: channel,
        delivery_target=lambda _job, _channel: target,
        run_agent_turn=lambda _job, _conv_id: {
            "final_text": "HTML report ready.",
            "tool_results": [
                {
                    "tool_name": "file_write",
                    "status": "completed",
                    "output": {"path": str(report), "bytes_written": report.stat().st_size},
                }
            ],
            "artifacts": [{"path": str(report)}],
        },
        record_event=lambda event_type, *_args, **_kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: cron_structured_result_block_reason(
            result,
            artifacts,
            text=text,
        ),
        save_web_delivery=lambda *_args, **_kwargs: False,
        send_platform_delivery=lambda _job, delivery_channel, delivery_target, text: (
            sent.append({"channel": delivery_channel, "target": delivery_target, "text": text}) or True
        ),
        start_background_delivery=lambda *_args, **_kwargs: None,
        clear_background_delivery=lambda *_args, **_kwargs: None,
    )

    result = run_cron_delivery_workflow(job, label="Manual scheduled task run", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert events == ["cron.delivery.started", "cron.delivery.sent"]
    assert sent == [
        {
            "channel": channel,
            "target": target,
            "text": f"⏰ **MANUAL SCHEDULED TASK RUN: {job.name}**\n\nHTML report ready.\n\nMEDIA:{report}",
        }
    ]
    delivery = prepare_reply_for_platform_delivery(sent[0]["text"], principal_id=principal_id)
    assert delivery.text == f"⏰ **MANUAL SCHEDULED TASK RUN: {job.name}**\n\nHTML report ready."
    assert delivery.attachments == (report,)
    assert delivery.media_directive_count == 1


def test_cron_planner_status_preview_uses_model_structured_plan_without_prompt_keywords(monkeypatch) -> None:
    from nullion.cron_planner_status import build_cron_planner_status_preview

    monkeypatch.setenv("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED", "1")
    from uuid import uuid4
    cache_buster = uuid4().hex
    captured: dict[str, object] = {}

    class FakeModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            captured["system"] = kwargs.get("system")
            captured["messages"] = kwargs.get("messages")
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
                                        "title": "Recolectar metricas",
                                        "description": "Gather public repository counters.",
                                        "tool_scope": ["terminal_exec"],
                                        "priority": "normal",
                                        "dependencies": [],
                                        "context_key_in": None,
                                        "context_key_out": "github_metrics",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "Buscar menciones publicas",
                                        "description": "Gather public awareness mentions.",
                                        "tool_scope": ["web_search"],
                                        "priority": "normal",
                                        "dependencies": [],
                                        "context_key_in": None,
                                        "context_key_out": "public_mentions",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "Generar reporte HTML",
                                        "description": "Create the report after collection is complete.",
                                        "tool_scope": ["file_write"],
                                        "priority": "normal",
                                        "dependencies": [0, 1],
                                        "context_key_in": "github_metrics",
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

    class FakeToolRegistry:
        def list_tool_definitions(self):
            return [{"name": "terminal_exec"}, {"name": "web_search"}, {"name": "file_write"}]

    preview = build_cron_planner_status_preview(
        model_client=FakeModelClient(),
        user_message=f"Necesito este informe en HTML para el monitor programado. ref={cache_buster}",
        conversation_id="telegram:100001",
        principal_id="telegram:100001",
        tool_registry=FakeToolRegistry(),
        subject="Monitor programado de trafico",
    )

    assert preview is not None
    assert captured["system"]
    initial = preview.initial_text()
    assert "Planner: Parallel Mission * 3 tasks" in initial
    assert "For: Monitor programado de trafico" in initial
    assert "◐ Recolectar metricas" in initial
    assert "◐ Buscar menciones publicas" in initial
    assert "☐ Generar reporte HTML" in initial
    terminal = preview.terminal_text(success=True)
    assert "☑ Recolectar metricas" in terminal
    assert "☑ Buscar menciones publicas" in terminal
    assert "☑ Generar reporte HTML" in terminal


def test_cron_planner_status_preview_omits_runtime_delivery_only_terminal_steps(monkeypatch) -> None:
    from nullion.cron_planner_status import build_cron_planner_status_preview
    from uuid import uuid4

    monkeypatch.setenv("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED", "1")
    captured: dict[str, object] = {}
    cache_buster = uuid4().hex

    class FakeModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            captured["messages"] = kwargs.get("messages")
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "disposition": "sequential_mission",
                                "needs_clarification": False,
                                "clarification_question": None,
                                "routing_evidence": ["model_structured_plan"],
                                "tasks": [
                                    {
                                        "title": "データ収集",
                                        "description": "Collect verified connector data.",
                                        "tool_scope": ["connector_request"],
                                        "priority": "normal",
                                        "dependencies": [],
                                        "context_key_in": None,
                                        "context_key_out": "verified_data",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "確認内容の整理",
                                        "description": "Synthesize verified obligations.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [0],
                                        "context_key_in": "verified_data",
                                        "context_key_out": "verified_obligations",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "表示文を整える",
                                        "description": "Prepare the platform-facing text.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [1],
                                        "context_key_in": "verified_obligations",
                                        "context_key_out": None,
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "通知先へ送る",
                                        "description": "Send via the configured platform.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [2],
                                        "context_key_in": None,
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

    class FakeToolRegistry:
        def list_tool_definitions(self):
            return [{"name": "connector_request"}]

    preview = build_cron_planner_status_preview(
        model_client=FakeModelClient(),
        user_message=f"定期ジョブの確認を実行する。runtime_delivery_strip_e2e_{cache_buster}",
        conversation_id="telegram:100001",
        principal_id="telegram:100001",
        tool_registry=FakeToolRegistry(),
        subject=f"定期確認 runtime_delivery_strip_e2e_{cache_buster}",
    )

    assert preview is not None
    prompt_text = captured["messages"][0]["content"][0]["text"]
    assert "external_platform_delivery_managed_by_runtime=true" in prompt_text
    assert [task.title for task in preview.group.tasks] == ["データ収集", "確認内容の整理"]
    assert "表示文を整える" not in preview.initial_text()
    assert "通知先へ送る" not in preview.initial_text()


def test_manual_cron_raw_payload_failure_ends_chat_turn_before_connector_reentry() -> None:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    connector_payload = {
        "messages": [
            {
                "title": "Account notice",
                "source": "billing-connector",
                "summary": "Review your account notice.",
            }
        ]
    }
    invoked_tools: list[str] = []

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tu-run-cron",
                            "name": "run_cron",
                            "input": {"id": "c4cb766a"},
                        }
                    ],
                }
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tu-connector",
                        "name": "connector_request",
                        "input": {},
                    }
                ],
            }

    def _run_cron(invocation):
        invoked_tools.append(invocation.tool_name)
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "failed",
            {
                "id": "c4cb766a",
                "name": "Connector-backed scheduled report",
                "reason": "cron_run_raw_tool_payload",
                "result_text": (
                    "Cron run was blocked because it produced raw structured tool output "
                    "instead of a deliverable report."
                ),
            },
            "Cron run produced raw structured tool output instead of a deliverable report.",
        )

    def _connector_request(invocation):
        invoked_tools.append(invocation.tool_name)
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", connector_payload)

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="run_cron",
            description="Run a scheduled task by id",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {"id": {"type": "string"}}},
        ),
        _run_cron,
    )
    registry.register(
        ToolSpec(
            name="connector_request",
            description="Call a connector",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}},
        ),
        _connector_request,
    )
    client = Client()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:100001",
        principal_id="telegram_chat",
        user_message="activar tarea",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert client.calls == 1
    assert invoked_tools == ["run_cron"]
    assert result.final_text is not None
    assert "delivery was blocked" in result.final_text
    assert "billing-connector" not in result.final_text
    assert "Account notice" not in result.final_text


def test_builder_runtime_workflow_can_store_and_accept_a_proposal(tmp_path: Path) -> None:
    from nullion.builder import BuilderDecisionType, BuilderProposal
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Synthesize recurring workflow",
        summary="This workflow repeats enough to become a reusable skill.",
        confidence=0.93,
        approval_mode="skill",
        suggested_skill_title="Recurring Workflow",
        suggested_trigger="When the same workflow repeats every week.",
        suggested_steps=(
            "Collect the input signal",
            "Execute the workflow checklist",
            "Persist a reusable follow-up pattern",
        ),
        suggested_tags=("e2e", "builder", "workflow"),
    )

    stored = runtime.store_builder_proposal(
        proposal,
        proposal_id="e2e-builder-proposal",
        actor="runtime",
    )
    pending = runtime.list_pending_builder_proposals()

    assert stored.proposal_id == "e2e-builder-proposal"
    assert stored.status == "pending"
    assert len(pending) == 1
    assert pending[0].proposal_id == stored.proposal_id

    skill = runtime.accept_stored_builder_skill_proposal(stored.proposal_id, actor="runtime")
    accepted = runtime.get_builder_proposal(stored.proposal_id)
    assert accepted is not None
    assert accepted.status == "accepted"
    assert accepted.accepted_skill_id == skill.skill_id
    assert skill.title == "Recurring Workflow"
    assert any(item.skill_id == skill.skill_id for item in runtime.list_skills())


def test_builder_runtime_duplicate_auto_skill_is_not_relearned(tmp_path: Path) -> None:
    from nullion.builder import BuilderDecisionType, BuilderProposal
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    first = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Trigger named cron",
        summary="Captured from: Trigger my email cron",
        confidence=0.86,
        approval_mode="skill",
        suggested_skill_title="Trigger named cron",
        suggested_trigger="Use when the user wants to manually run a specific cron job by name or purpose.",
        suggested_steps=("List crons", "Run the selected cron"),
        suggested_tags=("cron",),
    )
    stored = runtime.store_builder_proposal(first, actor="builder_reflector")
    accepted = runtime.accept_stored_builder_skill_proposal_result(stored.proposal_id, actor="builder_auto")
    duplicate = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Trigger named cron",
        summary="Captured from: Trigger the mid market cron too",
        confidence=0.86,
        approval_mode="skill",
        suggested_skill_title="Trigger named cron",
        suggested_trigger="Use when the user wants to manually trigger a scheduled cron job by name or description.",
        suggested_steps=("List available cron jobs", "Run the identified cron"),
        suggested_tags=("scheduled-jobs",),
    )

    duplicate_record = runtime.store_builder_proposal(duplicate, actor="builder_reflector")

    assert accepted.created is True
    assert duplicate_record.status == "accepted"
    assert duplicate_record.accepted_skill_id == accepted.skill.skill_id
    assert runtime.get_builder_proposal(duplicate_record.proposal_id) is None
    assert len(runtime.list_skills()) == 1
    assert runtime.list_pending_builder_proposals() == []


def test_user_memory_workflow_is_stored_and_mutated_with_runtime_store_contracts(tmp_path: Path) -> None:
    from nullion.memory import (
        UserMemoryKind,
        memory_owner_for_web_admin,
        remember_text_fact,
    )
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = memory_owner_for_web_admin()

    captured_entry = remember_text_fact(
        runtime.store,
        owner=owner,
        key="summary_style",
        value="direct, concise summaries for recurring updates",
        source="e2e",
        kind=UserMemoryKind.PREFERENCE,
    )
    assert captured_entry.owner == owner
    assert runtime.store.get_user_memory_entry(captured_entry.entry_id) is not None

    preference = remember_text_fact(
        runtime.store,
        owner=owner,
        key="timezone",
        value="America/New_York",
        source="e2e",
        kind=UserMemoryKind.PREFERENCE,
    )
    stored_preference = runtime.store.get_user_memory_entry(preference.entry_id)
    assert stored_preference is not None
    assert stored_preference.kind is UserMemoryKind.PREFERENCE
    assert stored_preference.value == "America/New_York"

    runtime.store.update_user_memory_entry(replace(stored_preference, value="America/Chicago"))
    assert runtime.store.get_user_memory_entry(stored_preference.entry_id) is not None
    assert runtime.store.get_user_memory_entry(stored_preference.entry_id).value == "America/Chicago"

    preferences = runtime.store.list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert any(item.entry_id == preference.entry_id for item in preferences)

    assert runtime.store.remove_user_memory_entry(captured_entry.entry_id) is True
    assert runtime.store.get_user_memory_entry(captured_entry.entry_id) is None


def test_task_frame_runtime_workflow_can_add_list_activate_and_complete_frames(tmp_path: Path) -> None:
    from nullion.memory import memory_owner_for_web_admin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_frames import (
        TaskFrame,
        TaskFrameExecutionContract,
        TaskFrameFinishCriteria,
        TaskFrameOperation,
        TaskFrameOutputContract,
        TaskFrameStatus,
        TaskFrameTarget,
    )

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = memory_owner_for_web_admin()
    now = datetime.now(UTC)

    frame = TaskFrame(
        frame_id="frame-e2e",
        conversation_id="conversation-e2e",
        branch_id="branch-e2e",
        source_turn_id="turn-e2e",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.FETCH_RESOURCE,
        target=TaskFrameTarget(kind="owner", value=owner),
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="Collect recurring status update context",
        created_at=now,
        updated_at=now,
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id(frame.conversation_id, frame.frame_id)

    conversation_frames = runtime.store.list_task_frames("conversation-e2e")
    assert len(conversation_frames) == 1
    assert conversation_frames[0].frame_id == frame.frame_id
    assert runtime.store.get_active_task_frame_id("conversation-e2e") == frame.frame_id

    completed_frame = replace(
        frame,
        status=TaskFrameStatus.COMPLETED,
        completion_turn_id="turn-e2e-complete",
        last_activity_turn_id="turn-e2e-complete",
        updated_at=now + timedelta(minutes=1),
    )
    runtime.store.add_task_frame(completed_frame)
    reloaded = runtime.store.get_task_frame("frame-e2e")
    assert reloaded is not None
    assert reloaded.status is TaskFrameStatus.COMPLETED
    assert reloaded.completion_turn_id == "turn-e2e-complete"

    runtime.store.set_active_task_frame_id("conversation-e2e", None)
    assert runtime.store.get_active_task_frame_id("conversation-e2e") is None


def test_mini_agent_workflow_runs_and_transitions_are_persisted_and_reconciled(tmp_path: Path) -> None:
    from nullion.intent import IntentCapsule, IntentState
    from nullion.mini_agent_runs import MiniAgentRunStatus
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_capsule(
        IntentCapsule(
            capsule_id="capsule-e2e",
            owner="owner-e2e",
            goal="Collect and summarize status",
            state=IntentState.PENDING,
            risk_level="normal",
            active_mini_agents=[],
            pending_approval_id=None,
            success_criteria=["done"],
        )
    )

    run = runtime.start_mini_agent_run(
        run_id="run-e2e",
        capsule_id="capsule-e2e",
        mini_agent_type="planner",
        created_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    active_run = runtime.store.get_mini_agent_run("run-e2e")
    assert run is not None
    assert active_run is not None
    assert active_run.status is MiniAgentRunStatus.PENDING
    assert "planner" in runtime.store.get_capsule("capsule-e2e").active_mini_agents

    runtime.mark_mini_agent_run_running("run-e2e")
    runtime.complete_mini_agent_run("run-e2e", result_summary="completed planning pass")

    completed = runtime.store.get_mini_agent_run("run-e2e")
    assert completed is not None
    assert completed.status is MiniAgentRunStatus.COMPLETED
    assert completed.result_summary == "completed planning pass"
    assert "planner" not in runtime.store.get_capsule("capsule-e2e").active_mini_agents

    stale_run = runtime.start_mini_agent_run(
        run_id="run-stale-e2e",
        capsule_id="capsule-e2e",
        mini_agent_type="auditor",
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    stale_with_running_status = replace(
        stale_run,
        status=MiniAgentRunStatus.RUNNING,
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    runtime.store.add_mini_agent_run(stale_with_running_status)
    stale_waiting_run = runtime.start_mini_agent_run(
        run_id="run-waiting-input-e2e",
        capsule_id="capsule-e2e",
        mini_agent_type="reviewer",
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    runtime.store.add_mini_agent_run(replace(stale_waiting_run, status=MiniAgentRunStatus.WAITING_INPUT))
    repaired = runtime.reconcile_stale_mini_agent_runs(now=datetime.now(UTC), stale_after=timedelta(minutes=1))
    assert {item.run_id for item in repaired} >= {"run-stale-e2e", "run-waiting-input-e2e"}

    stale_final = runtime.store.get_mini_agent_run("run-stale-e2e")
    assert stale_final is not None
    assert stale_final.status is MiniAgentRunStatus.FAILED
    waiting_final = runtime.store.get_mini_agent_run("run-waiting-input-e2e")
    assert waiting_final is not None
    assert waiting_final.status is MiniAgentRunStatus.FAILED


def test_planner_artifact_recovery_cannot_hide_unfinished_source_research(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def run_scenario() -> None:
        await _run_planner_unfinished_source_scenario(tmp_path, monkeypatch)

    asyncio.run(run_scenario())


def test_planner_download_task_gets_local_inspection_scope_and_delivers_artifact(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def run_scenario() -> None:
        from nullion.agent_orchestrator import AgentOrchestrator
        from nullion.artifacts import artifact_root_for_principal
        import nullion.mini_agent_runner as mini_agent_runner
        from nullion.runtime import bootstrap_persistent_runtime
        from nullion.task_decomposer import DagPlan, DecomposedTask
        from nullion.task_queue import TaskPriority, TaskResult
        from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec

        monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.05")
        monkeypatch.setenv("NULLION_PLANNER_MINI_AGENT_TIMEOUT_SECONDS", "120")

        runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
        deliveries: list[dict[str, object]] = []
        runs: list[dict[str, object]] = []

        class FakeMiniAgentRunner:
            async def run(self, config, **_kwargs):  # noqa: ANN001, ANN003
                runs.append({
                    "title": config.task.title,
                    "allowed_tools": tuple(config.task.allowed_tools),
                    "context_in": config.context_in,
                })
                if config.task.title == "Collect downloaded data":
                    return TaskResult(
                        task_id=config.task.task_id,
                        status="success",
                        output="Downloaded and inspected the local source artifact.",
                        context_out={"row_count": 3},
                    )
                if config.task.title == "Verify artifact":
                    return TaskResult(
                        task_id=config.task.task_id,
                        status="success",
                        output="Verified the requested spreadsheet artifact.",
                        context_out=config.context_in,
                    )
                if config.task.title == "Deliver receipt":
                    return TaskResult(
                        task_id=config.task.task_id,
                        status="success",
                        output="Spreadsheet delivery receipt is ready.",
                        context_out=config.context_in,
                    )
                artifact = artifact_root_for_principal("telegram_chat") / "planner_download_scope.xlsx"
                artifact.parent.mkdir(parents=True, exist_ok=True)
                from openpyxl import Workbook

                workbook = Workbook()
                worksheet = workbook.active
                worksheet.append(["row_count"])
                worksheet.append([3])
                workbook.save(artifact)
                return TaskResult(
                    task_id=config.task.task_id,
                    status="success",
                    output=(
                        "Created the requested spreadsheet.\n\n"
                        "Saved file:\n"
                        "`planner_download_scope.xlsx`"
                    ),
                )

        monkeypatch.setattr(mini_agent_runner, "MiniAgentRunner", FakeMiniAgentRunner)

        registry = ToolRegistry()
        for tool_name in (
            "file_download",
            "file_read",
            "workspace_summary",
            "file_search",
            "archive_extract",
            "spreadsheet_create",
        ):
            registry.register(
                ToolSpec(
                    name=tool_name,
                    description=f"{tool_name} test tool",
                    risk_level=ToolRiskLevel.LOW,
                    side_effect_class=ToolSideEffectClass.READ,
                    requires_approval=False,
                    timeout_seconds=5,
                    input_schema={"type": "object", "properties": {}},
                ),
                lambda invocation: None,
            )

        plan = DagPlan(
            disposition="sequential_mission",
            routing_evidence=["model_structured_plan"],
            tasks=[
                DecomposedTask(
                    title="Collect downloaded data",
                    description="Download a structured source artifact and inspect it locally.",
                    tool_scope=["file_download"],
                    priority=TaskPriority.NORMAL,
                    dep_indices=[],
                    context_key_in=None,
                    context_key_out="source-data",
                ),
                DecomposedTask(
                    title="Create spreadsheet",
                    description="Create the requested spreadsheet from the inspected source data.",
                    tool_scope=["spreadsheet_create"],
                    priority=TaskPriority.NORMAL,
                    dep_indices=[0],
                    context_key_in="source-data",
                    context_key_out=None,
                ),
            ],
        )

        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator.set_deliver_fn(
            lambda conversation_id, text, **kwargs: deliveries.append({
                "conversation_id": conversation_id,
                "text": text,
                **kwargs,
            })
        )
        try:
            await orchestrator.dispatch_request(
                conversation_id="telegram:100001",
                principal_id="telegram_chat",
                user_message="structured workbook request",
                tool_registry=registry,
                policy_store=runtime.store,
                approval_store=runtime.store,
                available_tools=[spec.name for spec in registry.list_specs()],
                single_task_fast_path=False,
                dag_plan=plan,
                requires_artifact_delivery=True,
                required_artifact_kind="xlsx",
            )

            for _ in range(60):
                if any(delivery.get("is_artifact") for delivery in deliveries):
                    break
                await asyncio.sleep(0.1)
        finally:
            await orchestrator.shutdown_dispatcher()

        assert [run["title"] for run in runs] == [
            "Collect downloaded data",
            "Create spreadsheet",
            "Verify artifact",
            "Deliver receipt",
        ]
        assert set(runs[0]["allowed_tools"]) >= {
            "file_download",
            "file_read",
            "workspace_summary",
            "file_search",
            "archive_extract",
        }
        assert runs[1]["context_in"] == {"row_count": 3}
        artifact_deliveries = [delivery for delivery in deliveries if delivery.get("is_artifact")]
        assert len(artifact_deliveries) == 1
        assert Path(str(artifact_deliveries[0]["text"])).name == "planner_download_scope.xlsx"

    asyncio.run(run_scenario())


async def _run_planner_unfinished_source_scenario(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.artifacts import artifact_root_for_principal
    from nullion.mini_agent_runs import MiniAgentRunStatus
    import nullion.mini_agent_runner as mini_agent_runner
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_decomposer import DagPlan, DecomposedTask
    from nullion.task_queue import TaskPriority, TaskResult
    from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.1")
    monkeypatch.setenv("NULLION_PLANNER_MINI_AGENT_TIMEOUT_SECONDS", "240")
    monkeypatch.setenv("NULLION_PLANNER_DEPENDENCY_RECOVERY_ATTEMPTS", "1")

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    deliveries: list[dict[str, object]] = []
    runs: list[dict[str, object]] = []

    class FakeMiniAgentRunner:
        async def run(self, config, **_kwargs):  # noqa: ANN001, ANN003
            runs.append({
                "title": config.task.title,
                "retry_count": config.task.retry_count,
                "allowed_tools": tuple(config.task.allowed_tools),
                "description": config.task.description,
                "timeout_s": config.timeout_s,
            })
            if config.task.title == "Research source data":
                return TaskResult(
                    task_id=config.task.task_id,
                    status="failure",
                    error="Timed out after 180s without reaching a terminal state.",
                )
            artifact = artifact_root_for_principal("telegram_chat") / "nyc_promotional_recovered.pdf"
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.write_bytes(b"%PDF-1.4\n%planner recovery e2e\n")
            return TaskResult(
                task_id=config.task.task_id,
                status="success",
                output=(
                    "Recovered and created the requested PDF.\n\n"
                    "Saved file:\n"
                    "`nyc_promotional_recovered.pdf`"
                ),
            )

    monkeypatch.setattr(mini_agent_runner, "MiniAgentRunner", FakeMiniAgentRunner)

    registry = ToolRegistry()
    for tool_name in ("browser_navigate", "browser_extract_text", "pdf_create"):
        registry.register(
            ToolSpec(
                name=tool_name,
                description=f"{tool_name} test tool",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: None,
        )

    plan = DagPlan(
        disposition="sequential_mission",
        routing_evidence=["model_structured_plan"],
        tasks=[
            DecomposedTask(
                title="Research source data",
                description="Collect verified source material.",
                tool_scope=["browser_navigate", "browser_extract_text"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="research",
            ),
            DecomposedTask(
                title="Create PDF",
                description="Create the requested PDF from research.",
                tool_scope=["pdf_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[0],
                context_key_in="research",
                context_key_out=None,
            ),
        ],
    )

    orchestrator = AgentOrchestrator(model_client=object())
    orchestrator.set_deliver_fn(
        lambda conversation_id, text, **kwargs: deliveries.append({
            "conversation_id": conversation_id,
            "text": text,
            **kwargs,
        })
    )
    try:
        result = await orchestrator.dispatch_request(
            conversation_id="telegram:100001",
            principal_id="telegram_chat",
            user_message="solicitud de folleto",
            tool_registry=registry,
            policy_store=runtime.store,
            approval_store=runtime.store,
            available_tools=["browser_navigate", "browser_extract_text", "pdf_create"],
            single_task_fast_path=False,
            dag_plan=plan,
        )

        assert result.status_delivered is True
        for _ in range(60):
            if any("No report was published." in str(delivery.get("text") or "") for delivery in deliveries):
                break
            await asyncio.sleep(0.1)
    finally:
        await orchestrator.shutdown_dispatcher()

    artifact_deliveries = [delivery for delivery in deliveries if delivery.get("is_artifact")]
    # The recovery runner only writes a file; it never completes the failed
    # research. File existence cannot certify the required source work.
    assert artifact_deliveries == []
    final_results = [str(delivery.get("text") or "") for delivery in deliveries
                     if not delivery.get("is_status")]
    assert any("No report was published." in text for text in final_results)
    assert all("Recovered and created" not in text for text in final_results)
    assert [run["title"] for run in runs] == ["Research source data", "Create PDF"]
    recovered_run = runs[-1]
    assert recovered_run["retry_count"] == 1
    assert set(recovered_run["allowed_tools"]) == {"pdf_create", "browser_navigate", "browser_extract_text"}
    assert "Dependency recovery context" in str(recovered_run["description"])
    assert recovered_run["timeout_s"] == 240
    persisted_statuses = [run.status for run in runtime.store.list_mini_agent_runs()]
    assert MiniAgentRunStatus.FAILED in persisted_statuses
    assert MiniAgentRunStatus.COMPLETED in persisted_statuses


def test_planner_research_scratch_html_is_not_delivered_as_attachment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def run_scenario() -> None:
        from nullion.agent_orchestrator import AgentOrchestrator
        from nullion.artifacts import artifact_root_for_principal
        import nullion.mini_agent_runner as mini_agent_runner
        from nullion.runtime import bootstrap_persistent_runtime
        from nullion.task_decomposer import DagPlan, DecomposedTask
        from nullion.task_queue import TaskPriority, TaskResult
        from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec

        monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
        runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
        deliveries: list[dict[str, object]] = []

        class FakeMiniAgentRunner:
            async def run(self, config, **_kwargs):  # noqa: ANN001, ANN003
                if config.task.title == "Fetch BBC homepage":
                    html_path = artifact_root_for_principal("telegram_chat") / "bbc_homepage_current.html"
                    html_path.parent.mkdir(parents=True, exist_ok=True)
                    html_path.write_text("<!doctype html><html><body>BBC</body></html>", encoding="utf-8")
                    return TaskResult(
                        task_id=config.task.task_id,
                        status="success",
                        output="Retrieved BBC homepage. Raw HTML was saved at artifacts/bbc_homepage_current.html.",
                    )
                return TaskResult(
                    task_id=config.task.task_id,
                    status="success",
                    output="Compared CNN and BBC homepage results.",
                )

        monkeypatch.setattr(mini_agent_runner, "MiniAgentRunner", FakeMiniAgentRunner)

        registry = ToolRegistry()
        registry.register(
            ToolSpec(
                name="terminal_exec",
                description="terminal test tool",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: None,
        )
        plan = DagPlan(
            disposition="parallel_mission",
            routing_evidence=["model_structured_plan"],
            tasks=[
                DecomposedTask(
                    title="Fetch BBC homepage",
                    description="Fetch current BBC homepage evidence.",
                    tool_scope=["terminal_exec"],
                    priority=TaskPriority.NORMAL,
                    dep_indices=[],
                    context_key_in=None,
                    context_key_out="bbc",
                ),
                DecomposedTask(
                    title="Compare homepages",
                    description="Compare gathered homepage evidence.",
                    tool_scope=[],
                    priority=TaskPriority.NORMAL,
                    dep_indices=[0],
                    context_key_in="bbc",
                    context_key_out=None,
                ),
            ],
        )

        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator.set_deliver_fn(
            lambda conversation_id, text, **kwargs: deliveries.append({
                "conversation_id": conversation_id,
                "text": text,
                **kwargs,
            })
        )
        try:
            result = await orchestrator.dispatch_request(
                conversation_id="telegram:100001",
                principal_id="telegram_chat",
                user_message="compare results from cnn.com and bbc.com home page",
                tool_registry=registry,
                policy_store=runtime.store,
                approval_store=runtime.store,
                available_tools=["terminal_exec"],
                single_task_fast_path=False,
                dag_plan=plan,
            )

            assert result.status_delivered is True
            for _ in range(60):
                if any(
                    not delivery.get("is_status")
                    and not delivery.get("is_artifact")
                    and str(delivery.get("text") or "").startswith("Result for")
                    for delivery in deliveries
                ):
                    break
                await asyncio.sleep(0.1)
        finally:
            await orchestrator.shutdown_dispatcher()

        assert not [delivery for delivery in deliveries if delivery.get("is_artifact")]
        assert any(
            str(delivery.get("text") or "").startswith(
                'Result for "compare results from cnn.com and bbc.com home page":'
            )
            for delivery in deliveries
        )

    asyncio.run(run_scenario())


def test_deep_agent_relocates_external_pdf_artifact_into_downloadable_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def run_scenario() -> None:
        await _run_deep_agent_external_pdf_relocation_scenario(tmp_path, monkeypatch)

    asyncio.run(run_scenario())


async def _run_deep_agent_external_pdf_relocation_scenario(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import sys

    import nullion.deep_agent_runner as deep_agent_runner
    from nullion.artifacts import artifact_root_for_principal, path_is_within
    from nullion.task_queue import TaskResult
    from nullion.tools import ToolResult

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    external_pdf = tmp_path / "outside-download-root" / "nyc_poster.pdf"
    external_pdf.parent.mkdir(parents=True)
    external_pdf.write_bytes(b"%PDF-1.4\n%outside artifact e2e\n")

    class FakeDeepAgent:
        async def ainvoke(self, _payload, config=None):  # noqa: ANN001
            return {
                "final": (
                    "I attempted to generate the PDF, but the PDF file could not be verified "
                    "after creation, so I can't safely claim it is available."
                )
            }

    monkeypatch.setitem(
        sys.modules,
        "deepagents",
        SimpleNamespace(create_deep_agent=lambda **_kwargs: FakeDeepAgent()),
    )

    def fake_langchain_tools(_registry, **kwargs):  # noqa: ANN001, ANN003
        callback = kwargs["tool_result_callback"]
        callback(
            ToolResult(
                invocation_id="tool-outside-pdf",
                tool_name="terminal_exec",
                status="completed",
                output={"artifact_path": str(external_pdf), "artifact_paths": [str(external_pdf)]},
                error=None,
            )
        )
        return []

    monkeypatch.setattr(deep_agent_runner, "nullion_tools_as_langchain_tools", fake_langchain_tools)
    monkeypatch.setattr(deep_agent_runner, "_deep_agent_meta_tools", lambda *_args, **_kwargs: [])

    task = SimpleNamespace(
        task_id="task-external-pdf",
        group_id="grp-external-pdf",
        principal_id="telegram_chat",
        title="Create promotional PDF",
        description="Create the requested PDF artifact.",
        allowed_tools=["terminal_exec", "pdf_create"],
        context_key_in=None,
        context_key_out=None,
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "pdf"},
    )
    config = SimpleNamespace(
        task=task,
        agent_id="agent-external-pdf",
        context_in=None,
        max_iterations=3,
        max_continuations=0,
        can_request_user_input=False,
    )
    result = await deep_agent_runner.DeepAgentMiniAgentRunner()._run_inner(
        config,
        anthropic_client=object(),
        tool_registry=object(),
        policy_store=object(),
        context_bus=SimpleNamespace(get=lambda *_args, **_kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert isinstance(result, TaskResult)
    assert result.status == "success"
    assert len(result.artifacts) == 1
    delivered_pdf = Path(result.artifacts[0])
    artifact_root = artifact_root_for_principal("telegram_chat")
    assert path_is_within(delivered_pdf, artifact_root)
    assert delivered_pdf != external_pdf
    assert delivered_pdf.suffix == ".pdf"
    assert delivered_pdf.read_bytes() == external_pdf.read_bytes()
    assert result.output == f"Created and verified downloadable artifact: {delivered_pdf.name}"


def test_telegram_planner_pdf_request_dispatches_once_and_requires_confirmed_status_before_suppressing_ack(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.chat_operator import handle_chat_operator_message
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_decomposer import DagPlan, DecomposedTask, TaskDecomposer
    from nullion.task_queue import TaskPriority
    from nullion.telegram_app import _should_suppress_planner_status_ack
    from nullion.tools import ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="pdf_create",
            description="Create a PDF",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: None,
    )
    runtime.active_tool_registry = registry

    plan = DagPlan(
        disposition="sequential_mission",
        routing_evidence=["model_structured_plan"],
        tasks=[
            DecomposedTask(
                title="Collect source material",
                description="Collect source material.",
                tool_scope=[],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="source",
            ),
            DecomposedTask(
                title="Create PDF",
                description="Create the PDF deliverable.",
                tool_scope=["pdf_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[0],
                context_key_in="source",
                context_key_out=None,
            ),
        ],
    )

    monkeypatch.setattr(TaskDecomposer, "plan_dag", lambda self, *_args, **_kwargs: plan)

    class AttachmentFormatModel:
        def create(self, **kwargs):  # noqa: ANN003
            tools = kwargs.get("tools") or []
            if tools and tools[0].get("name") == "select_attachment_format":
                return {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "select_attachment_format",
                            "input": {"extension": ".pdf", "confidence": 0.99},
                        }
                    ]
                }
            return {"content": [{"type": "text", "text": "ok"}]}

    class FakeOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def dispatch_request_sync(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            return SimpleNamespace(
                dispatched=True,
                acknowledgment="Structured planner acknowledgment",
                task_count=2,
                planner_summary="Sequential Mission • 2 tasks",
                task_titles=("Collect source material", "Create PDF"),
                task_status_detail="  ☐ Collect source material\n  ☐ Create PDF",
                status_delivered=False,
            )

    orchestrator = FakeOrchestrator()
    reply = handle_chat_operator_message(
        runtime,
        "/planner créer un document promotionnel.pdf",
        chat_id="100001",
        settings=NullionSettings(telegram=TelegramSettings(chat_enabled=True, operator_chat_id="100001")),
        model_client=AttachmentFormatModel(),
        agent_orchestrator=orchestrator,
        activity_callback=lambda _event: None,
        append_activity_trace=False,
    )

    assert reply == "Structured planner acknowledgment"
    assert len(orchestrator.calls) == 1
    assert orchestrator.calls[0]["single_task_fast_path"] is False
    assert orchestrator.calls[0]["requires_artifact_delivery"] is True
    assert orchestrator.calls[0]["required_artifact_kind"] == "pdf"
    assert not _should_suppress_planner_status_ack(
        runtime,
        chat_id="100001",
        reply=reply,
        status_stream_delivered=False,
    )
    chat_turns = [
        event
        for event in runtime.store.list_conversation_events("telegram:100001")
        if event.get("event_type") == "conversation.chat_turn"
    ]
    assert chat_turns[-1]["tool_results"][1]["output"]["status_delivered"] is True
    assert chat_turns[-1]["tool_results"][1]["output"]["status_delivery_confirmed"] is False


def test_status_mini_agent_reconciliation_scope_protects_other_surface_runs(tmp_path: Path) -> None:
    from dataclasses import replace

    from nullion.intent import IntentCapsule, IntentState
    from nullion.mini_agent_runs import MiniAgentRunStatus
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    for capsule_id in ("web-owned", "telegram-owned"):
        runtime.store.add_capsule(
            IntentCapsule(
                capsule_id=capsule_id,
                owner="owner-e2e",
                goal="Run delegated work",
                state=IntentState.PENDING,
                risk_level="normal",
                active_mini_agents=[],
                pending_approval_id=None,
                success_criteria=["done"],
            )
        )
    web_run = runtime.start_mini_agent_run(
        run_id="run-web-stale",
        capsule_id="web-owned",
        mini_agent_type="Web owned",
        created_at=datetime.now(UTC) - timedelta(hours=2),
        metadata={"conversation_id": "web:status"},
    )
    telegram_run = runtime.start_mini_agent_run(
        run_id="run-telegram-live",
        capsule_id="telegram-owned",
        mini_agent_type="Telegram owned",
        created_at=datetime.now(UTC) - timedelta(hours=2),
        metadata={"conversation_id": "telegram:123"},
    )
    runtime.store.add_mini_agent_run(replace(web_run, status=MiniAgentRunStatus.RUNNING))
    runtime.store.add_mini_agent_run(replace(telegram_run, status=MiniAgentRunStatus.RUNNING))

    repaired = runtime.reconcile_stale_mini_agent_runs(
        now=datetime.now(UTC),
        stale_after=timedelta(minutes=1),
        live_run_ids=set(),
        live_group_ids={"web-owned"},
        owned_conversation_prefixes=("web:",),
    )

    assert repaired == []
    assert runtime.store.get_mini_agent_run("run-web-stale").status is MiniAgentRunStatus.RUNNING
    assert runtime.store.get_mini_agent_run("run-telegram-live").status is MiniAgentRunStatus.RUNNING


def test_empty_live_group_reconciliation_repairs_persisted_only_runs(tmp_path: Path) -> None:
    from dataclasses import replace

    from nullion.mini_agent_runs import MiniAgentRunStatus
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    stale_run = runtime.start_mini_agent_run(
        run_id="run-persisted-only",
        capsule_id="orphaned-group",
        mini_agent_type="Persisted only",
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    runtime.store.add_mini_agent_run(replace(stale_run, status=MiniAgentRunStatus.RUNNING))

    repaired = runtime.reconcile_stale_mini_agent_runs(
        now=datetime.now(UTC),
        stale_after=timedelta(minutes=1),
        live_run_ids=set(),
        live_group_ids=set(),
    )

    assert [item.run_id for item in repaired] == ["run-persisted-only"]
    assert runtime.store.get_mini_agent_run("run-persisted-only").status is MiniAgentRunStatus.FAILED


def test_scheduled_background_run_reconciliation_repairs_orphaned_run(tmp_path: Path) -> None:
    from dataclasses import replace

    from nullion.mini_agent_runs import MiniAgentRunStatus
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    scheduled_run = runtime.start_mini_agent_run(
        run_id="run-scheduled-background",
        capsule_id="scheduled-group",
        mini_agent_type="Deliver report",
        created_at=datetime.now(UTC) - timedelta(hours=2),
        metadata={"scheduled_task_run": True, "deep_agent_profiles": ["scheduled_job"]},
    )
    runtime.store.add_mini_agent_run(replace(scheduled_run, status=MiniAgentRunStatus.RUNNING))
    ordinary_run = runtime.start_mini_agent_run(
        run_id="run-ordinary-stale",
        capsule_id="ordinary-group",
        mini_agent_type="Ordinary task",
        created_at=datetime.now(UTC) - timedelta(hours=2),
    )
    runtime.store.add_mini_agent_run(replace(ordinary_run, status=MiniAgentRunStatus.RUNNING))

    repaired = runtime.reconcile_stale_mini_agent_runs(
        now=datetime.now(UTC),
        stale_after=timedelta(minutes=1),
        live_run_ids=set(),
        live_group_ids=set(),
    )

    assert [item.run_id for item in repaired] == ["run-scheduled-background", "run-ordinary-stale"]
    assert runtime.store.get_mini_agent_run("run-ordinary-stale").status is MiniAgentRunStatus.FAILED
    assert runtime.store.get_mini_agent_run("run-scheduled-background").status is MiniAgentRunStatus.FAILED
    assert "scheduled background Mini-Agent" in (runtime.store.get_mini_agent_run("run-scheduled-background").result_summary or "")

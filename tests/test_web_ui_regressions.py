from __future__ import annotations

import inspect
import json
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
import time
from types import SimpleNamespace

import nullion.web_app as web_app
from nullion.web_app import (
    _HTML,
    _approval_display_from_turn_result,
    create_app,
    _filter_supported_media_models,
    _invalid_media_model_capabilities,
    _media_model_options,
    _media_model_supports,
    _media_selection_supported,
    _normalize_media_models,
)

_SOURCE = Path(web_app.__file__).read_text(encoding="utf-8")


def test_assistant_avatar_is_served_from_app_package(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).get("/assets/nullion-assistant-avatar.svg")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("image/svg+xml")
    assert b"<svg" in response.content
    assert '"website" / "assets" / "nullion-assistant-avatar.svg"' not in _SOURCE


def test_web_http_chat_uses_shared_platform_gateway(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    calls = []
    workspace_base = tmp_path / "workspaces"
    artifact_root = workspace_base / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    artifact_path = artifact_root / "shared_gateway_report.pdf"
    artifact_path.write_bytes(b"%PDF-1.4\n%shared gateway\n")
    monkeypatch.setattr(web_app, "workspace_storage_base", lambda: workspace_base)

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        calls.append((runtime_arg, request))
        return platform_chat.PlatformChatResponse(
            text="shared gateway reply",
            artifact_paths=[str(artifact_path)],
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)

    app = create_app(runtime, orchestrator=SimpleNamespace(model_client=object()), registry=ToolRegistry())
    response = TestClient(app).post(
        "/api/chat",
        json={"conversation_id": "browser-1", "text": "hello from web"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["text"] == "shared gateway reply"
    assert body["artifacts"][0]["name"] == "shared_gateway_report.pdf"
    assert body["artifacts"][0]["url"].startswith("/api/artifacts/")
    assert calls
    runtime_arg, request = calls[0]
    assert runtime_arg is runtime
    assert request.platform == "web"
    assert request.conversation_id == "browser-1"
    assert request.text == "hello from web"


def test_web_http_chat_filters_shared_payload_to_final_media_receipt(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    workspace_base = tmp_path / "workspaces"
    artifact_root = workspace_base / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    html_path = artifact_root / "train_accessible_day_trips.html"
    side_svg_path = artifact_root / "train_accessible_day_trips_summary.svg"
    html_path.write_text("<html><body>report</body></html>", encoding="utf-8")
    side_svg_path.write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")
    monkeypatch.setattr(web_app, "workspace_storage_base", lambda: workspace_base)
    monkeypatch.setattr(
        web_app,
        "_web_artifact_descriptors",
        lambda runtime, paths, principal_id=None: [
            {"path": str(path), "name": Path(str(path)).name, "url": f"/api/artifacts/{Path(str(path)).name}"}
            for path in paths
        ],
    )

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        return platform_chat.PlatformChatResponse(
            text=f"Done - attached the requested HTML.\n\nMEDIA:{html_path}",
            artifact_paths=[str(html_path), str(side_svg_path)],
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)

    app = create_app(runtime, orchestrator=SimpleNamespace(model_client=object()), registry=ToolRegistry())
    response = TestClient(app).post(
        "/api/chat",
        json={"conversation_id": "browser-1", "text": "create the HTML report with real images"},
    )

    assert response.status_code == 200
    body = response.json()
    assert [artifact["name"] for artifact in body["artifacts"]] == [html_path.name]
    assert "MEDIA:" not in body["text"]
    assert side_svg_path.name not in body["text"]


def test_web_http_chat_rewrites_multi_artifact_sidecar_dump(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    workspace_base = tmp_path / "workspaces"
    artifact_root = workspace_base / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    csv_path = artifact_root / "stage_random_media_archive_dataset.csv"
    linked_html = artifact_root / "stage_random_media_archive_linked_report.html"
    embedded_html = artifact_root / "stage_random_media_archive_embedded_report.html"
    pdf_path = artifact_root / "stage_random_media_archive_pdf_report.pdf"
    json_path = artifact_root / "stage_random_website_qa_media_archive_manifest.json"
    csv_path.write_text("name,type\nSTAGE RANDOM WEBSITE QA,fixture\n", encoding="utf-8")
    linked_html.write_text(
        "<!doctype html><html><body><h1>STAGE RANDOM WEBSITE QA</h1>"
        "<p>This linked report has enough body text to qualify as a deliverable artifact.</p></body></html>",
        encoding="utf-8",
    )
    embedded_html.write_text(
        "<!doctype html><html><body><h1>STAGE RANDOM WEBSITE QA</h1>"
        "<p>This self-contained report has enough body text to qualify as a deliverable artifact.</p></body></html>",
        encoding="utf-8",
    )
    pdf_path.write_bytes(b"%PDF-1.4\n% STAGE RANDOM WEBSITE QA\n")
    json_path.write_text('{"sidecar": true}', encoding="utf-8")
    monkeypatch.setattr(web_app, "workspace_storage_base", lambda: workspace_base)

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        return platform_chat.PlatformChatResponse(
            text=(
                "Done - attached the requested artifact package:\n"
                f"- {csv_path.name}\n"
                f"- {linked_html.name}\n"
                f"- {embedded_html.name}\n"
                f"- {pdf_path.name}\n"
                f"- {json_path.name}\n\n"
                f"MEDIA:{csv_path}\n"
                f"MEDIA:{linked_html}\n"
                f"MEDIA:{embedded_html}\n"
                f"MEDIA:{pdf_path}\n"
                f"MEDIA:{json_path}"
            ),
            artifact_paths=[
                str(csv_path),
                str(linked_html),
                str(embedded_html),
                str(pdf_path),
                str(json_path),
            ],
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)

    app = create_app(runtime, orchestrator=SimpleNamespace(model_client=object()), registry=ToolRegistry())
    response = TestClient(app).post(
        "/api/chat",
        json={"conversation_id": "browser-1", "text": "create the random website artifact package"},
    )

    assert response.status_code == 200
    body = response.json()
    names = [artifact["name"] for artifact in body["artifacts"]]
    assert names == [csv_path.name, linked_html.name, embedded_html.name, pdf_path.name]
    assert json_path.name not in body["text"]
    assert "JSON" not in body["text"]


def test_shared_artifact_delivery_keeps_requested_package_and_drops_helper_media(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.artifacts import artifact_output_descriptor
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    csv_path = artifact_root / "stage_random_website_qa_media_archive_dataset.csv"
    linked_html = artifact_root / "stage_random_website_qa_linked_report.html"
    embedded_html = artifact_root / "stage_random_website_qa_self_contained_report.html"
    pdf_path = artifact_root / "stage_random_website_qa_media_report.pdf"
    json_path = artifact_root / "stage_random_website_qa_manifest.json"
    helper_png = artifact_root / "stage_random_website_qa_embedded_media_image.png"
    csv_path.write_text("name,type\nSTAGE RANDOM WEBSITE QA,fixture\n", encoding="utf-8")
    for html_path in (linked_html, embedded_html):
        html_path.write_text(
            "<!doctype html><html><body><h1>STAGE RANDOM WEBSITE QA</h1>"
            "<p>This report is long enough to qualify as a deliverable artifact.</p></body></html>",
            encoding="utf-8",
        )
    pdf_path.write_bytes(b"%PDF-1.4\n% STAGE RANDOM WEBSITE QA\n")
    json_path.write_text('{"internal": true}', encoding="utf-8")
    helper_png.write_bytes(b"\x89PNG\r\n\x1a\nhelper image bytes")
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda _runtime: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda _principal_id: artifact_root)
    all_paths = [csv_path, linked_html, embedded_html, pdf_path, json_path, helper_png]
    tool_results = [
        ToolResult(
            f"tool-{index}",
            "file_write" if path.suffix.lower() != ".pdf" else "pdf_create",
            "completed",
            {
                "artifact_path": str(path),
                "artifact_paths": [str(path)],
                "artifact_descriptors": [artifact_output_descriptor(path)],
            },
        )
        for index, path in enumerate(all_paths)
    ]

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "Created CSV, HTML, PDF, JSON, and images.\n"
            f"MEDIA:{csv_path}\nMEDIA:{linked_html}\nMEDIA:{embedded_html}\n"
            f"MEDIA:{pdf_path}\nMEDIA:{json_path}\nMEDIA:{helper_png}"
        ),
        artifact_paths=[str(path) for path in all_paths],
        prompt="opaque artifact package request",
        principal_id="workspace_admin",
        tool_results=tool_results,
        required_attachment_extensions=(".csv", ".html", ".pdf"),
    )

    media_names = [Path(line.removeprefix("MEDIA:")).name for line in reply.splitlines() if line.startswith("MEDIA:")]
    assert media_names == [csv_path.name, linked_html.name, embedded_html.name, pdf_path.name]
    assert json_path.name not in reply
    assert helper_png.name not in reply


def test_platform_chat_extracts_media_artifact_paths(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator, platform_chat

    workspace_root = tmp_path / "workspace"
    artifact_root = workspace_root / "artifacts"
    artifact_root.mkdir(parents=True)
    artifact_path = artifact_root / "shared_gateway_report.pdf"
    artifact_path.write_bytes(b"%PDF-1.4\n%shared gateway\n")
    monkeypatch.setenv("NULLION_WORKSPACE_ROOT", str(workspace_root))
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: f"Done — attached the requested file.\n\nMEDIA:{artifact_path}",
    )

    response = platform_chat.run_platform_chat_request(
        object(),
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create a PDF and attach it.",
        ),
    )

    assert (response.text or "").startswith("Done — attached the requested file.")
    assert "MEDIA:" not in (response.text or "")
    assert response.artifact_paths == [str(artifact_path)]


def test_platform_chat_scrubs_orphan_approval_marker(monkeypatch) -> None:
    from nullion import chat_operator, platform_chat

    class Store:
        def get_approval_request(self, approval_id):
            return None

        def get_suspended_turn(self, approval_id):
            return None

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            return []

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "Tool approval requested: stale-approval-id",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Send the prepared email.",
        ),
    )

    assert response.suspended_for_approval is False
    assert response.approval_id is None
    assert "stale-approval-id" not in (response.text or "")
    assert "No changes were made" in (response.text or "")


def test_platform_chat_preserves_suspended_turn_approval_marker(monkeypatch) -> None:
    from nullion import chat_operator, platform_chat

    class Store:
        def get_approval_request(self, approval_id):
            return None

        def get_suspended_turn(self, approval_id):
            if approval_id == "turn-budget-approval-id":
                return SimpleNamespace(approval_id=approval_id)
            return None

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            return []

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "Tool approval requested: turn-budget-approval-id\n\n"
            "Doctor recommends more tool budget"
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Finish the prepared email package.",
        ),
    )

    assert response.suspended_for_approval is True
    assert response.approval_id == "turn-budget-approval-id"
    assert "turn-budget-approval-id" in (response.text or "")


def test_chat_operator_recovers_actionable_approval_from_suspended_turn_state(tmp_path) -> None:
    from nullion import chat_operator
    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    approval = create_approval_request(
        requested_by="web:live-stage-contract:email",
        action="use_tool",
        resource="email_send",
        context={"tool_name": "email_send", "tool_detail": "recipient@example.com"},
    )
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="web:live-stage-contract:email",
            chat_id="live-stage-contract:email",
            message="/chat Send a short test email.",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
        )
    )

    reply, live = chat_operator._approval_marker_reply_for_turn_result(
        runtime,
        SimpleNamespace(approval_id=None),
        conversation_id="web:live-stage-contract:email",
        chat_id="live-stage-contract:email",
    )

    assert live is True
    assert reply == f"Tool approval requested: {approval.approval_id}"


def test_chat_operator_materializes_backend_tool_approval_before_fulfillment(tmp_path) -> None:
    from nullion import chat_operator
    from nullion.approvals import ApprovalStatus
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    tool_result = ToolResult(
        "email-send-1",
        "email_send",
        "denied",
        {
            "approval_id": "email-approval-from-backend",
            "reason": "approval_required",
            "requires_approval": True,
        },
        "Approval required for tool: email_send",
    )

    approval_id = chat_operator._materialize_pending_tool_approval_for_chat(
        runtime,
        tool_results=[tool_result],
        conversation_id="web:live-stage-contract:email-package",
        chat_id="web:live-stage-contract:email-package",
        prompt="Create attachments and send the package.",
        request_id="req-1",
        message_id="msg-1",
        principal_id="web:live-stage-contract:email-package",
        registry=None,
    )

    assert approval_id == "email-approval-from-backend"
    approval = runtime.store.get_approval_request(approval_id)
    assert approval is not None
    assert approval.status is ApprovalStatus.PENDING
    assert approval.resource == "email_send"
    assert "Create attachments" in str((approval.context or {}).get("tool_description") or "")
    suspended = runtime.store.get_suspended_turn(approval_id)
    assert suspended is not None
    assert suspended.message == "/chat Create attachments and send the package."


def test_chat_operator_materializes_tool_arguments_from_approval_required_result(tmp_path) -> None:
    from nullion import chat_operator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    arguments = {
        "to": ["recipient@example.com"],
        "subject": "Stage QA",
        "body": "Test body",
        "attachment_paths": [str(tmp_path / "stage.csv"), str(tmp_path / "stage.xlsx")],
    }
    tool_result = ToolResult(
        "email-send-1",
        "email_send",
        "denied",
        {
            "approval_id": "email-approval-with-arguments",
            "reason": "approval_required",
            "requires_approval": True,
            "tool_arguments": arguments,
        },
        "Approval required for tool: email_send",
    )

    approval_id = chat_operator._materialize_pending_tool_approval_for_chat(
        runtime,
        tool_results=[tool_result],
        conversation_id="web:live-stage-contract:email-package",
        chat_id="web:live-stage-contract:email-package",
        prompt="Create attachments and send the package.",
        request_id="req-1",
        message_id="msg-1",
        principal_id="web:live-stage-contract:email-package",
        registry=None,
    )

    approval = runtime.store.get_approval_request(approval_id)
    assert approval is not None
    assert approval.context["tool_arguments"] == arguments
    assert "tool_description" not in approval.context


def test_chat_operator_executes_approved_pending_tool_call_without_replaying_prompt(tmp_path) -> None:
    from nullion import chat_operator
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    calls = []
    arguments = {
        "to": ["recipient@example.com"],
        "subject": "Stage QA",
        "body": "Test body",
        "attachment_paths": [str(tmp_path / "stage.csv")],
    }
    (tmp_path / "stage.csv").write_text("name,value\nstage,ok\n", encoding="utf-8")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_send",
            description="Send an email.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=30,
        ),
        lambda invocation: calls.append(dict(invocation.arguments))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "to": invocation.arguments.get("to"),
                "subject": invocation.arguments.get("subject"),
                "attachment_paths": invocation.arguments.get("attachment_paths"),
            },
        ),
    )
    runtime.active_tool_registry = registry
    approval = create_approval_request(
        requested_by="web:live-stage-contract:email-package",
        action="use_tool",
        resource="email_send",
        context={
            "tool_name": "email_send",
            "requires_approval": True,
            "tool_arguments": arguments,
        },
    )
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="web:live-stage-contract:email-package",
            chat_id="web:live-stage-contract:email-package",
            message="/chat Create attachments and send the package.",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
            messages_snapshot=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "Create attachments and send the package."}],
                }
            ],
            pending_tool_calls=[
                {
                    "invocation_id": "email-send-1",
                    "tool_name": "email_send",
                    "status": "denied",
                    "output": {
                        "approval_id": approval.approval_id,
                        "reason": "approval_required",
                        "requires_approval": True,
                        "tool_arguments": arguments,
                    },
                    "error": "Approval required for tool: email_send",
                }
            ],
        )
    )
    approve_request_with_mode(runtime, approval.approval_id, mode="once", source="test")

    result = chat_operator.execute_approved_pending_tool_call(
        runtime,
        runtime.store.get_suspended_turn(approval.approval_id),
        tool_registry=registry,
    )

    assert result is not None
    assert result.tool_name == "email_send"
    assert result.status == "completed"
    assert calls == [arguments]


def test_chat_operator_executes_approved_tool_call_from_context_without_suspended_turn(tmp_path) -> None:
    from nullion import chat_operator
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    calls = []
    arguments = {
        "to": ["recipient@example.com"],
        "subject": "Stage QA context",
        "body": "Test body",
        "attachment_paths": [str(tmp_path / "stage.csv")],
    }
    (tmp_path / "stage.csv").write_text("name,value\nstage,ok\n", encoding="utf-8")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_send",
            description="Send an email.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=30,
        ),
        lambda invocation: calls.append(dict(invocation.arguments))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "to": invocation.arguments.get("to"),
                "subject": invocation.arguments.get("subject"),
                "attachment_paths": invocation.arguments.get("attachment_paths"),
            },
        ),
    )
    runtime.active_tool_registry = registry
    approval = create_approval_request(
        requested_by="web:live-stage-contract:email-package",
        action="use_tool",
        resource="email_send",
        context={
            "tool_name": "email_send",
            "requires_approval": True,
            "tool_arguments": arguments,
            "trigger_flow": {
                "capsule_id": "turn-stage",
                "invocation_id": "email-send-context",
                "principal_id": "web:live-stage-contract:email-package",
            },
        },
    )
    runtime.store.add_approval_request(approval)
    approve_request_with_mode(runtime, approval.approval_id, mode="once", source="test")

    result = chat_operator.execute_approved_tool_call_from_approval_context(
        runtime,
        approval.approval_id,
        tool_registry=registry,
    )

    assert result is not None
    assert result.tool_name == "email_send"
    assert result.status == "completed"
    assert calls == [arguments]


def test_web_resume_executes_approved_tool_context_before_prompt_replay(tmp_path) -> None:
    from nullion import web_app
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    calls = []
    arguments = {
        "to": ["recipient@example.com"],
        "subject": "Stage QA suspended context",
        "body": "Test body",
        "attachment_paths": [str(tmp_path / "stage.csv")],
    }
    (tmp_path / "stage.csv").write_text("name,value\nstage,ok\n", encoding="utf-8")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_send",
            description="Send an email.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=30,
        ),
        lambda invocation: calls.append(dict(invocation.arguments))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "to": invocation.arguments.get("to"),
                "subject": invocation.arguments.get("subject"),
                "attachment_paths": invocation.arguments.get("attachment_paths"),
            },
        ),
    )
    approval = create_approval_request(
        requested_by="web:live-stage-contract:email-package",
        action="use_tool",
        resource="email_send",
        context={
            "tool_name": "email_send",
            "requires_approval": True,
            "tool_arguments": arguments,
            "trigger_flow": {
                "capsule_id": "turn-stage",
                "invocation_id": "email-send-context",
                "principal_id": "web:live-stage-contract:email-package",
            },
        },
    )
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="web:live-stage-contract:email-package",
            chat_id="web:live-stage-contract:email-package",
            message="/chat Create attachments and send the package.",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
            messages_snapshot=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "Create attachments and send the package."}],
                }
            ],
            pending_tool_calls=[],
        )
    )
    approve_request_with_mode(runtime, approval.approval_id, mode="once", source="test")

    class ReplayWouldFail:
        def run_turn(self, *args, **kwargs):  # pragma: no cover - should not be reached
            raise AssertionError("approval resume replayed the prompt instead of executing tool context")

    payload = web_app._resume_web_turn_from_snapshot(
        runtime,
        approval_id=approval.approval_id,
        orchestrator=ReplayWouldFail(),
        registry=registry,
    )

    assert payload is not None
    assert payload["type"] == "message"
    assert "Stage QA suspended context" in payload["text"]
    assert calls == [arguments]
    assert runtime.store.get_suspended_turn(approval.approval_id) is None


def test_web_approval_route_executes_tool_context_before_suspended_replay(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    calls = []
    arguments = {
        "to": ["recipient@example.com"],
        "subject": "Stage QA approval route",
        "body": "Test body",
        "attachment_paths": [str(tmp_path / "stage.csv")],
    }
    (tmp_path / "stage.csv").write_text("name,value\nstage,ok\n", encoding="utf-8")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_send",
            description="Send an email.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=30,
        ),
        lambda invocation: calls.append(dict(invocation.arguments))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "to": invocation.arguments.get("to"),
                "subject": invocation.arguments.get("subject"),
                "attachment_paths": invocation.arguments.get("attachment_paths"),
            },
        ),
    )
    approval = create_approval_request(
        requested_by="web:live-stage-contract:email-package",
        action="use_tool",
        resource="email_send",
        context={
            "tool_name": "email_send",
            "requires_approval": True,
            "tool_arguments": arguments,
            "trigger_flow": {
                "capsule_id": "turn-stage",
                "invocation_id": "email-send-context",
                "principal_id": "web:live-stage-contract:email-package",
            },
        },
    )
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="web:live-stage-contract:email-package",
            chat_id="web:live-stage-contract:email-package",
            message="/chat Create attachments and send the package.",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
            messages_snapshot=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "Create attachments and send the package."}],
                }
            ],
            pending_tool_calls=[],
        )
    )

    class ReplayWouldFail:
        def run_turn(self, *args, **kwargs):  # pragma: no cover - should not be reached
            raise AssertionError("approval route replayed the prompt instead of executing tool context")

    app = create_app(runtime, orchestrator=ReplayWouldFail(), registry=registry)
    response = TestClient(app).post(f"/api/approve/{approval.approval_id}", json={"mode": "once"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert "Stage QA approval route" in payload["resume"]["text"]
    assert calls == [arguments]
    assert runtime.store.get_suspended_turn(approval.approval_id) is None


def test_web_approval_route_preserves_preapproval_artifact_evidence(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:live-stage-contract:email-artifact-package"
    artifact_path = tmp_path / "stage_qa_test_data.csv"
    artifact_path.write_text("name,value\nstage,ok\n", encoding="utf-8")
    monkeypatch.setattr(
        web_app,
        "_web_artifact_descriptors",
        lambda runtime, paths, principal_id=None: [
            {"path": str(path), "name": Path(str(path)).name, "url": f"/api/artifacts/{Path(str(path)).name}"}
            for path in paths
        ],
    )

    calls = []
    arguments = {
        "to": ["recipient@example.com"],
        "subject": "Stage QA approval package",
        "body": "Test body",
        "attachment_paths": [str(artifact_path)],
    }
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_send",
            description="Send an email.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=True,
            timeout_seconds=30,
        ),
        lambda invocation: calls.append(dict(invocation.arguments))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "to": invocation.arguments.get("to"),
                "subject": invocation.arguments.get("subject"),
                "attachment_paths": invocation.arguments.get("attachment_paths"),
            },
        ),
    )
    approval = create_approval_request(
        requested_by=conversation_id,
        action="use_tool",
        resource="email_send",
        context={
            "tool_name": "email_send",
            "requires_approval": True,
            "tool_arguments": arguments,
            "trigger_flow": {
                "capsule_id": "turn-stage",
                "invocation_id": "email-send-context",
                "principal_id": conversation_id,
            },
        },
    )
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id=conversation_id,
            chat_id=conversation_id,
            message="/chat Create attachments and send the package.",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
            messages_snapshot=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "Create attachments and send the package."}],
                }
            ],
            pending_tool_calls=[
                {
                    "invocation_id": "file-write-1",
                    "tool_name": "file_write",
                    "status": "completed",
                    "output": {
                        "path": str(artifact_path),
                        "artifact_paths": [str(artifact_path)],
                    },
                    "error": None,
                },
                {
                    "invocation_id": "email-send-context",
                    "tool_name": "email_send",
                    "status": "denied",
                    "output": {
                        "approval_id": approval.approval_id,
                        "reason": "approval_required",
                        "requires_approval": True,
                        "tool_arguments": arguments,
                    },
                    "error": "Approval required for tool: email_send",
                },
            ],
        )
    )

    app = create_app(runtime, orchestrator=SimpleNamespace(model_client=object()), registry=registry)
    response = TestClient(app).post(f"/api/approve/{approval.approval_id}", json={"mode": "once"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert [artifact["name"] for artifact in payload["resume"]["artifacts"]] == [artifact_path.name]
    assert calls == [arguments]
    events = runtime.store.list_recent_conversation_events(conversation_id, event_type="conversation.chat_turn")
    latest = events[-1]
    tool_names = [result["tool_name"] for result in latest["tool_results"]]
    assert tool_names[:2] == ["file_write", "email_send"]
    assert "artifact_delivery" in tool_names


def test_web_direct_email_resume_delivers_actual_email_attachments(tmp_path, monkeypatch) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    stale_workbook = tmp_path / "stage_multi_tool_qa_culture_itinerary_benchmark_resend.xlsx"
    emailed_workbook = tmp_path / "stage_multi_tool_qa_culture_itinerary_benchmark.xlsx"
    emailed_pdf = tmp_path / "stage_multi_tool_qa_culture_itinerary_executive_summary_resend.pdf"
    stale_workbook.write_bytes(b"PK\x03\x04 stale workbook")
    emailed_workbook.write_bytes(b"PK\x03\x04 media workbook")
    emailed_pdf.write_bytes(b"%PDF-1.4\n% emailed pdf\n")
    monkeypatch.setattr(
        web_app,
        "_web_artifact_descriptors",
        lambda runtime, paths, principal_id=None: [
            {"path": str(path), "name": Path(str(path)).name, "url": f"/api/artifacts/{Path(str(path)).name}"}
            for path in paths
        ],
    )
    prior_tool_results = [
        ToolResult(
            "sheet",
            "spreadsheet_create",
            "completed",
            {
                "artifact_path": str(stale_workbook),
                "artifact_paths": [str(stale_workbook)],
            },
        ),
        ToolResult(
            "pdf",
            "pdf_create",
            "completed",
            {
                "artifact_path": str(emailed_pdf),
                "artifact_paths": [str(emailed_pdf)],
            },
        ),
    ]
    email_result = ToolResult(
        "email",
        "email_send",
        "completed",
        {
            "to": ["recipient@example.com"],
            "subject": "Nullion stage multi tool QA",
            "attachment_paths": [str(emailed_pdf), str(emailed_workbook)],
        },
    )

    payload = web_app._web_direct_tool_result_resume_payload(
        runtime,
        conversation_id="web:live-stage-contract:email-artifact-package",
        user_text="Send the PDF and XLSX by email.",
        tool_result=email_result,
        prior_tool_results=prior_tool_results,
    )

    names = [artifact["name"] for artifact in payload["artifacts"]]
    assert names == [
        "stage_multi_tool_qa_culture_itinerary_executive_summary_resend.pdf",
        "stage_multi_tool_qa_culture_itinerary_benchmark.xlsx",
    ]
    assert "stage_multi_tool_qa_culture_itinerary_benchmark_resend.xlsx" not in names
    assert "stage_multi_tool_qa_culture_itinerary_benchmark_resend.xlsx" not in payload["text"]
    assert "Done — attached the requested attachments." in payload["text"]
    events = runtime.store.list_recent_conversation_events(
        "web:live-stage-contract:email-artifact-package",
        event_type="conversation.chat_turn",
        limit=1,
    )
    assert events
    assert [artifact["name"] for artifact in events[0]["artifacts"]] == names


def test_platform_chat_prefers_canonical_stored_turn_reply(monkeypatch) -> None:
    from nullion import chat_operator, platform_chat

    request_text = "Find live availability from the browser."
    canonical_reply = (
        "Live-search blocker: the browser/search tools did not produce "
        "verified result records from this run."
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "user_message": request_text,
                    "assistant_reply": canonical_reply,
                    "tool_results": [],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "Source-backed points\n\n- Static marketing page text",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text=request_text,
        ),
    )

    assert response.text == canonical_reply


def test_platform_chat_recovers_browser_extract_evidence_over_stored_blocker(monkeypatch) -> None:
    from nullion import chat_operator, platform_chat

    request_text = "Can you research Citi Bike e-bike helmet requirements for adults?"
    stored_reply = "Citi Bike recommends wearing a helmet and following local age requirements."
    extract_text = (
        "Citi Bike riding tips\n"
        "Wear a helmet\n"
        "It's law in NJ for riders under 17\n"
        "Keep it buckled and wear it level\n"
        "Our next-gen ebike has pedal assist.\n"
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "user_message": request_text,
                    "assistant_reply": stored_reply,
                    "tool_results": [
                        {
                            "invocation_id": "extract-1",
                            "tool_name": "browser_extract_text",
                            "status": "completed",
                            "output": {"text": extract_text},
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: stored_reply,
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text=request_text,
        ),
    )

    assert response.text is not None
    assert response.text == stored_reply
    assert "Live-search blocker" not in response.text
    assert "helmet" in response.text
    assert response.artifact_paths == []


def test_web_approval_payload_hydrates_from_runtime_store(tmp_path) -> None:
    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    approval = create_approval_request(
        requested_by="web:operator",
        action="use_tool",
        resource="email_send",
        request_kind="capability_grant",
        context={
            "tool_name": "email_send",
            "tool_arguments": {
                "to": ["recipient@example.com"],
                "subject": "Stage approval",
                "body": "Test body",
            },
        },
    )
    runtime.store.add_approval_request(approval)

    payload = web_app._web_approval_required_payload(
        {"approval_id": approval.approval_id, "is_web_request": True},
        default_tool_name="tool",
        runtime=runtime,
    )

    assert payload["approval_id"] == approval.approval_id
    assert payload["tool_name"] == "send an email"
    assert "recipient@example.com" in payload["tool_detail"]
    assert "Stage approval" in payload["tool_detail"]
    assert payload["request_kind"] == "capability_grant"
    assert payload["text"]
    assert "Review email" in payload["text"]


def test_platform_chat_scrubs_unselected_supporting_artifact_names(monkeypatch, tmp_path) -> None:
    from nullion import chat_operator, platform_chat

    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    screenshot = artifact_root / "supporting-screenshot.png"
    docx = artifact_root / "final-report.docx"
    xlsx = artifact_root / "final-workbook.xlsx"
    csv = artifact_root / "final-export.csv"
    pdf = artifact_root / "final-summary.pdf"
    html = artifact_root / "final-report.html"
    for path in (screenshot, docx, xlsx, csv, pdf, html):
        path.write_bytes(b"artifact")

    class Store:
        def get_approval_request(self, approval_id):
            return None

        def get_suspended_turn(self, approval_id):
            return None

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            return [
                {
                    "event_type": "conversation.chat_turn",
                    "conversation_id": conversation_id,
                    "user_message": "Create a mixed package with final document, workbook, CSV, PDF, and HTML files.",
                    "assistant_reply": (
                        "Created the requested files:\n"
                        f"- `{screenshot.name}`\n"
                        f"- `{docx.name}`"
                    ),
                    "tool_results": [
                        {
                            "tool_name": "browser_screenshot",
                            "status": "completed",
                            "output": {"path": str(screenshot)},
                        },
                        {
                            "tool_name": "document_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(docx),
                                "artifact_descriptors": [
                                    {"path": str(docx), "role": "deliverable", "media_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
                                ],
                            },
                        },
                        {
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(xlsx),
                                "embedded_screenshots": [str(screenshot)],
                                "artifact_descriptors": [
                                    {"path": str(xlsx), "role": "deliverable", "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
                                ],
                            },
                        },
                        {
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(csv),
                                "artifact_descriptors": [
                                    {"path": str(csv), "role": "deliverable", "media_type": "text/csv"}
                                ],
                            },
                        },
                        {
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(pdf),
                                "source_screenshot_paths": [str(screenshot)],
                                "artifact_descriptors": [
                                    {"path": str(pdf), "role": "deliverable", "media_type": "application/pdf"}
                                ],
                            },
                        },
                        {
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(html),
                                "embedded_html_images": [{"path": str(screenshot)}],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "I couldn't attach all of the requested files. The task is still open.",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="web:mixed-package",
            text="Create a mixed package with final document, workbook, CSV, PDF, and HTML files.",
        ),
    )

    assert response.artifact_paths == [str(docx), str(xlsx), str(csv), str(pdf), str(html)]
    assert screenshot.name not in (response.text or "")
    assert str(screenshot) not in response.artifact_paths


def test_platform_chat_parses_approval_marker_before_activity_trace(monkeypatch) -> None:
    from nullion import chat_operator, platform_chat

    class Store:
        def get_approval_request(self, approval_id):
            if approval_id == "pending-approval-id":
                return SimpleNamespace(status=SimpleNamespace(value="pending"))
            return None

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            return []

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "Tool approval requested: pending-approval-id\n\n"
            "Activity\n"
            "- email_send"
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Send the prepared email.",
        ),
    )

    assert response.suspended_for_approval is True
    assert response.approval_id == "pending-approval-id"


def test_platform_chat_preserves_string_pending_approval_marker(monkeypatch) -> None:
    from nullion import chat_operator, platform_chat

    class Store:
        def get_approval_request(self, approval_id):
            if approval_id == "string-pending-approval-id":
                return SimpleNamespace(status="pending")
            return None

        def get_suspended_turn(self, approval_id):
            return None

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            return []

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "Tool approval requested: string-pending-approval-id",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Send the prepared email.",
        ),
    )

    assert response.suspended_for_approval is True
    assert response.approval_id == "string-pending-approval-id"


def test_platform_chat_recovers_current_turn_pending_approval_from_orphan_reply(monkeypatch) -> None:
    from datetime import UTC, datetime

    from nullion import chat_operator, platform_chat
    from nullion.approval_markers import ORPHAN_APPROVAL_REQUEST_REPLY

    class Store:
        def __init__(self) -> None:
            self._approval = SimpleNamespace(
                approval_id="current-email-approval-id",
                requested_by="web:email-approval-contract",
                status="pending",
                created_at=datetime.now(UTC),
                context={"conversation_id": "web:email-approval-contract"},
            )

        def get_approval_request(self, approval_id):
            if approval_id == self._approval.approval_id:
                return self._approval
            return None

        def get_suspended_turn(self, approval_id):
            return None

        def list_approval_requests(self):
            return [self._approval]

        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            return []

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: ORPHAN_APPROVAL_REQUEST_REPLY,
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="email-approval-contract",
            text="Send the prepared email.",
        ),
    )

    assert response.suspended_for_approval is True
    assert response.approval_id == "current-email-approval-id"
    assert response.text == "Tool approval requested: current-email-approval-id"


def test_platform_chat_does_not_reuse_previous_turn_artifacts_for_followup(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    previous_pdf = tmp_path / "previous_report.pdf"
    previous_xlsx = tmp_path / "previous_workbook.xlsx"
    previous_pdf.write_bytes(b"%PDF-1.4\n")
    previous_xlsx.write_bytes(b"PK\x03\x04xlsx")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "user_message": "Create the artifact package.",
                    "assistant_reply": (
                        "Done - attached the requested artifacts.\n\n"
                        f"MEDIA:{previous_pdf}\nMEDIA:{previous_xlsx}"
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "make-pdf",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {"path": str(previous_pdf), "artifact_path": str(previous_pdf)},
                        },
                        {
                            "invocation_id": "make-xlsx",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {"path": str(previous_xlsx), "artifact_path": str(previous_xlsx)},
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "Email sent to recipient@example.com with the selected attachments.",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Send recipient@example.com the selected files from the previous result.",
        ),
    )

    assert response.text == "Email sent to recipient@example.com with the selected attachments."
    assert response.artifact_paths == []
    assert previous_pdf.name not in response.text
    assert previous_xlsx.name not in response.text


def test_platform_chat_filters_side_artifact_for_embedded_media_contract(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    html_path = tmp_path / "train_accessible_day_trips.html"
    side_svg_path = tmp_path / "train_accessible_day_trips_summary.svg"
    html_path.write_text("<html><body><img src='photo.jpg'>report</body></html>", encoding="utf-8")
    side_svg_path.write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": (
                        "Done - attached the requested artifact package.\n\n"
                        f"MEDIA:{html_path}\nMEDIA:{side_svg_path}"
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "scope-html",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {
                                "artifact_extensions": [".html"],
                                "embedded_media_artifact_extensions": [".html"],
                            },
                        },
                        {
                            "invocation_id": "write-html",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(html_path),
                                "artifact_path": str(html_path),
                                "artifact_paths": [str(html_path)],
                                "artifact_descriptors": [{"path": str(html_path), "role": "artifact"}],
                                "source_image_paths": [str(tmp_path / "source-image.jpg")],
                            },
                        },
                        {
                            "invocation_id": "scope-svg",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {"artifact_extensions": [".svg"]},
                        },
                        {
                            "invocation_id": "write-svg",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(side_svg_path),
                                "artifact_path": str(side_svg_path),
                                "artifact_paths": [str(side_svg_path)],
                                "artifact_descriptors": [{"path": str(side_svg_path), "role": "artifact"}],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "Done - attached the requested artifact package.\n\n"
            f"MEDIA:{html_path}\nMEDIA:{side_svg_path}"
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create one self-contained HTML report with real images embedded in the HTML.",
        ),
    )

    assert response.artifact_paths == [str(html_path)]
    assert side_svg_path.name not in (response.text or "")


def test_platform_chat_preserves_mixed_artifact_package_with_embedded_media_contract(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    csv_path = tmp_path / "culture_itinerary_benchmark.csv"
    xlsx_path = tmp_path / "culture_itinerary_benchmark.xlsx"
    docx_path = tmp_path / "culture_itinerary_benchmark.docx"
    pdf_path = tmp_path / "culture_itinerary_executive_summary.pdf"
    html_path = tmp_path / "culture_itinerary_benchmark_embedded.html"
    visual_source_html = tmp_path / "culture_itinerary_visual_source.html"
    source_image = tmp_path / "source-image.jpg"
    source_svg = tmp_path / "culture_itinerary_source.svg"
    source_png = tmp_path / "culture_itinerary_source.png"
    screenshot_png = tmp_path / "screenshot-cart.png"
    csv_path.write_text("name,value\nMuseum,1\n", encoding="utf-8")
    xlsx_path.write_bytes(b"PK\x03\x04xlsx")
    docx_path.write_bytes(b"PK\x03\x04docx")
    pdf_path.write_bytes(b"%PDF-1.4\n")
    html_path.write_text(
        "<html><body><h1>Culture itinerary benchmark</h1><img src='source-image.jpg'>"
        "<p>This report has enough visible planning text to be the final HTML deliverable.</p></body></html>",
        encoding="utf-8",
    )
    visual_source_html.write_text("<html><body><canvas></canvas>QA</body></html>", encoding="utf-8")
    source_image.write_bytes(b"\xff\xd8\xff")
    source_svg.write_text("<svg xmlns='http://www.w3.org/2000/svg'><circle cx='5' cy='5' r='5'/></svg>", encoding="utf-8")
    source_png.write_bytes(b"\x89PNG\r\n\x1a\n")
    screenshot_png.write_bytes(b"\x89PNG\r\n\x1a\n")
    artifact_paths = [str(csv_path), str(xlsx_path), str(docx_path), str(pdf_path), str(html_path)]
    reply = "Done - attached the requested artifact package.\n\n" + "\n".join(
        f"MEDIA:{path}"
        for path in (*artifact_paths, str(visual_source_html), str(source_svg), str(source_png), str(screenshot_png))
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": reply,
                    "tool_results": [
                        {
                            "invocation_id": "scope-package",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {
                                "artifact_extensions": [".csv", ".xlsx", ".docx", ".pdf", ".html"],
                                "embedded_media_artifact_extensions": [".xlsx"],
                            },
                        },
                        {
                            "invocation_id": "write-csv",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(csv_path),
                                "artifact_path": str(csv_path),
                                "artifact_paths": [str(csv_path)],
                            },
                        },
                        {
                            "invocation_id": "make-xlsx",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "path": str(xlsx_path),
                                "artifact_path": str(xlsx_path),
                                "artifact_paths": [str(xlsx_path)],
                                "source_image_paths": [str(source_image), str(source_svg)],
                            },
                        },
                        {
                            "invocation_id": "scope-png-source",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {
                                "artifact_extensions": [".csv", ".xlsx", ".docx", ".pdf", ".html", ".svg", ".png"],
                                "embedded_media_artifact_extensions": [".xlsx"],
                            },
                        },
                        {
                            "invocation_id": "write-source-svg",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(source_svg),
                                "artifact_path": str(source_svg),
                                "artifact_paths": [str(source_svg)],
                            },
                        },
                        {
                            "invocation_id": "download-source-png",
                            "tool_name": "file_download",
                            "status": "completed",
                            "output": {
                                "path": str(source_png),
                                "artifact_path": str(source_png),
                                "artifact_paths": [str(source_png)],
                                "artifact_descriptors": [{"path": str(source_png), "role": "source"}],
                            },
                        },
                        {
                            "invocation_id": "screenshot-source",
                            "tool_name": "browser_screenshot",
                            "status": "completed",
                            "output": {
                                "path": str(screenshot_png),
                                "artifact_path": str(screenshot_png),
                                "artifact_paths": [str(screenshot_png)],
                            },
                        },
                        {
                            "invocation_id": "make-docx",
                            "tool_name": "document_create",
                            "status": "completed",
                            "output": {"path": str(docx_path), "artifact_path": str(docx_path)},
                        },
                        {
                            "invocation_id": "make-pdf",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {"path": str(pdf_path), "artifact_path": str(pdf_path)},
                        },
                        {
                            "invocation_id": "write-html",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {"path": str(html_path), "artifact_path": str(html_path)},
                        },
                        {
                            "invocation_id": "write-visual-source-html",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(visual_source_html),
                                "artifact_path": str(visual_source_html),
                                "artifact_paths": [str(visual_source_html)],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(chat_operator, "handle_chat_operator_message", lambda *args, **kwargs: reply)

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create the structured artifact package described by the current plan.",
        ),
    )

    assert response.artifact_paths == artifact_paths
    for path in artifact_paths:
        assert Path(path).name in (response.text or "")
    assert source_svg.name not in (response.text or "")
    assert source_png.name not in (response.text or "")
    assert screenshot_png.name not in (response.text or "")
    assert visual_source_html.name not in (response.text or "")


def test_platform_chat_suppresses_last_mile_helper_pdf_when_regular_pdf_exists(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    workbook_path = tmp_path / "location_comparison.xlsx"
    csv_path = tmp_path / "location_comparison.csv"
    summary_pdf = tmp_path / "executive_summary.pdf"
    helper_pdf = tmp_path / "requested_artifact_pdf-helper.pdf"
    workbook_path.write_bytes(b"PK\x03\x04xlsx")
    csv_path.write_text("name,value\nRiverside,1\n", encoding="utf-8")
    summary_pdf.write_bytes(b"%PDF-1.4\nsummary")
    helper_pdf.write_bytes(b"%PDF-1.4\nhelper")
    reply = "Done - attached the requested files.\n\n" + "\n".join(
        f"MEDIA:{path}" for path in (workbook_path, csv_path, summary_pdf, helper_pdf)
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": reply,
                    "tool_results": [
                        {
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {"artifact_extensions": [".xlsx", ".csv", ".pdf"]},
                        },
                        {
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(workbook_path),
                                "artifact_paths": [str(workbook_path)],
                                "artifact_descriptors": [{"path": str(workbook_path), "role": "deliverable"}],
                            },
                        },
                        {
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(csv_path),
                                "artifact_paths": [str(csv_path)],
                                "artifact_descriptors": [{"path": str(csv_path), "role": "deliverable"}],
                            },
                        },
                        {
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(summary_pdf),
                                "artifact_paths": [str(summary_pdf)],
                            },
                        },
                        {
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(helper_pdf),
                                "artifact_paths": [str(helper_pdf)],
                                "last_mile_materialized": True,
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(chat_operator, "handle_chat_operator_message", lambda *args, **kwargs: reply)

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create the requested workbook, CSV, and PDF package.",
        ),
    )

    assert response.artifact_paths == [str(workbook_path), str(csv_path), str(summary_pdf)]
    assert helper_pdf.name not in (response.text or "")


def test_platform_chat_preserves_same_suffix_package_companions_when_one_artifact_has_media(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    poster_pdf = tmp_path / "community-night-market-poster.pdf"
    memo_pdf = tmp_path / "community-night-market-formal-memo.pdf"
    poster_html = tmp_path / "community-night-market-poster.html"
    source_image = tmp_path / "night-market-photo.png"
    source_image.write_bytes(b"\x89PNG\r\n\x1a\n")
    poster_pdf.write_bytes(b"%PDF-1.4\n")
    memo_pdf.write_bytes(b"%PDF-1.4\n")
    poster_html.write_text(
        "<html><body><h1>STAGE FORMAT QA</h1><img src='night-market-photo.png'></body></html>",
        encoding="utf-8",
    )
    artifact_paths = [str(poster_pdf), str(memo_pdf), str(poster_html)]
    reply = "Done - attached the requested artifact package.\n\n" + "\n".join(
        f"MEDIA:{path}" for path in artifact_paths
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": reply,
                    "tool_results": [
                        {
                            "invocation_id": "scope-package",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {
                                "artifact_extensions": [".pdf", ".html"],
                                "embedded_media_artifact_extensions": [".pdf", ".html"],
                            },
                        },
                        {
                            "invocation_id": "make-poster",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {
                                "path": str(poster_pdf),
                                "artifact_path": str(poster_pdf),
                                "artifact_paths": [str(poster_pdf)],
                                "source_image_paths": [str(source_image)],
                            },
                        },
                        {
                            "invocation_id": "make-memo",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {
                                "path": str(memo_pdf),
                                "artifact_path": str(memo_pdf),
                                "artifact_paths": [str(memo_pdf)],
                            },
                        },
                        {
                            "invocation_id": "write-html",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(poster_html),
                                "artifact_path": str(poster_html),
                                "artifact_paths": [str(poster_html)],
                                "embedded_html_images": [{"path": str(source_image)}],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(chat_operator, "handle_chat_operator_message", lambda *args, **kwargs: reply)

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create the poster PDF, formal memo PDF, and self-contained HTML poster.",
        ),
    )

    assert response.artifact_paths == artifact_paths


def test_chat_operator_does_not_complete_partial_media_required_artifact_pack(tmp_path) -> None:
    from nullion import chat_operator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    poster_pdf = tmp_path / "night_market_poster_stage_format_qa.pdf"
    memo_pdf = tmp_path / "night_market_formal_memo_stage_format_qa.pdf"
    svg_only_html = tmp_path / "night_market_poster_stage_format_qa.html"
    source_image = tmp_path / "night-market-photo.png"
    poster_pdf.write_bytes(b"%PDF-1.4\n")
    memo_pdf.write_bytes(b"%PDF-1.4\n")
    svg_only_html.write_text(
        "<html><body><h1>STAGE FORMAT QA</h1><svg></svg></body></html>",
        encoding="utf-8",
    )
    source_image.write_bytes(b"\x89PNG\r\n\x1a\n")
    result = SimpleNamespace(
        final_text=(
            "Created the requested PDF and HTML file:\n"
            f"- {poster_pdf.name}\n"
            f"- {memo_pdf.name}\n"
            f"- {svg_only_html.name}"
        ),
        artifacts=[str(poster_pdf), str(memo_pdf), str(svg_only_html)],
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".pdf", ".html"],
                    "embedded_media_artifact_extensions": [".pdf", ".html"],
                },
            ),
            ToolResult(
                "poster",
                "pdf_create",
                "completed",
                {
                    "artifact_path": str(poster_pdf),
                    "artifact_paths": [str(poster_pdf)],
                    "source_image_paths": [str(source_image)],
                },
            ),
            ToolResult(
                "memo",
                "pdf_create",
                "completed",
                {
                    "artifact_path": str(memo_pdf),
                    "artifact_paths": [str(memo_pdf)],
                    "source_image_paths": [str(source_image)],
                },
            ),
            ToolResult(
                "html",
                "file_write",
                "completed",
                {
                    "path": str(svg_only_html),
                    "artifact_path": str(svg_only_html),
                    "artifact_paths": [str(svg_only_html)],
                },
            ),
        ],
    )

    assert chat_operator._completed_requested_artifact_paths_for_turn_result(
        runtime,
        result=result,
        prompt="Create a poster PDF, memo PDF, and self-contained HTML with real images.",
        principal_id=None,
        required_attachment_extensions=(".pdf", ".html"),
        required_embedded_media_extensions=(".pdf", ".html"),
    ) == ()


def test_platform_chat_blocks_raw_tool_request_markup_without_tool_results(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    raw_tool_request = (
        '<tool_request>{"tool":"request_tool_scope","data":{"capabilities":["browser"],'
        '"tool_names":["browser_navigate"]}}</tool_request>'
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [{"assistant_reply": raw_tool_request, "tool_results": []}]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: raw_tool_request,
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Use browser tools for this live lookup.",
        ),
    )

    assert "<tool_request>" not in (response.text or "")
    assert "blocked a raw structured payload" in (response.text or "")
    assert response.artifact_paths == []


def test_web_artifact_payloads_are_filtered_to_final_media_receipt(tmp_path, monkeypatch) -> None:
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    html = tmp_path / "train_accessible_outdoor_day_trips.html"
    side_svg = tmp_path / "train_accessible_outdoor_day_trips_report.svg"
    html.write_text("<html><body>report</body></html>", encoding="utf-8")
    side_svg.write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")

    filtered = web_app._web_filter_artifact_paths_to_reply_paths(
        runtime,
        reply=f"Done — attached the requested HTML.\n\nMEDIA:{html}",
        artifact_paths=[str(html), str(side_svg)],
        principal_id="web:artifact-filter",
    )

    assert filtered == [str(html)]


def test_web_delivery_prefers_embedded_media_contract_over_side_artifact(
    tmp_path, monkeypatch,
) -> None:
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolResult

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    html = tmp_path / "train_accessible_outdoor_day_trips.html"
    side_svg = tmp_path / "train_accessible_outdoor_day_trips_summary.svg"
    html.write_text("<html><body>report</body></html>", encoding="utf-8")
    side_svg.write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")
    monkeypatch.setattr(
        web_app,
        "_web_artifact_descriptors",
        lambda runtime, paths, principal_id=None: [
            {"path": str(path), "name": Path(str(path)).name}
            for path in paths
        ],
    )
    tool_results = [
        ToolResult(
            invocation_id="scope-html",
            tool_name="request_tool_scope",
            status="completed",
            output={
                "artifact_extensions": [".html"],
                "embedded_media_artifact_extensions": [".html"],
            },
        ),
        ToolResult(
            invocation_id="write-html",
            tool_name="file_write",
            status="completed",
            output={
                "path": str(html),
                "artifact_descriptors": [{"path": str(html), "role": "artifact"}],
            },
        ),
        ToolResult(
            invocation_id="scope-svg",
            tool_name="request_tool_scope",
            status="completed",
            output={"artifact_extensions": [".svg"]},
        ),
        ToolResult(
            invocation_id="write-svg",
            tool_name="file_write",
            status="completed",
            output={
                "path": str(side_svg),
                "artifact_descriptors": [{"path": str(side_svg), "role": "artifact"}],
            },
        ),
    ]

    delivered = web_app._web_delivery_artifact_paths(
        runtime,
        prompt="Create one self-contained HTML report with local/source raster images embedded in the HTML.",
        reply=f"Done — attached the requested artifact package.\n\nMEDIA:{side_svg}",
        tool_results=tool_results,
        artifact_paths=[str(side_svg), str(html)],
        principal_id="web:artifact-filter",
    )

    assert delivered == [str(html)]


def test_platform_chat_does_not_attach_terminal_discovered_search_target(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    artifact_path = tmp_path / "stage_file_search_delivery_guard_1234.pdf"
    artifact_path.write_bytes(b"%PDF-1.4\n%search target\n")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "I found 1 file:\n- stage_file_search_delivery_guard_1234.pdf",
                    "tool_results": [
                        {
                            "invocation_id": "term-1",
                            "tool_name": "terminal_exec",
                            "status": "completed",
                            "output": {"stdout": f"{artifact_path}\n", "stderr": ""},
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "I found 1 file:\n- stage_file_search_delivery_guard_1234.pdf",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text=(
                "Find the pdf file named stage_file_search_delivery_guard_1234.pdf in my workspace "
                "and reply with a concise filename-only listing. Do not attach, upload, or send the file."
            ),
        ),
    )

    assert response.text == "I found 1 file:\n- stage_file_search_delivery_guard_1234.pdf"
    assert response.artifact_paths == []


def test_platform_chat_prefers_browser_text_over_unrequested_screenshot(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    screenshot_path = tmp_path / "github-prs.png"
    screenshot_path.write_bytes(b"png")
    extracted_text = (
        "Pull requests: sample-team/sample-app\n"
        "0 Open\n"
        "29 Closed\n"
        "There aren't any open pull requests."
    )

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": f"Done — attached the requested screenshot.\n\nMEDIA:{screenshot_path}",
                    "tool_results": [
                        {
                            "invocation_id": "extract-1",
                            "tool_name": "browser_extract_text",
                            "status": "completed",
                            "output": {"text": extracted_text},
                        },
                        {
                            "invocation_id": "shot-1",
                            "tool_name": "browser_screenshot",
                            "status": "completed",
                            "output": {
                                "path": str(screenshot_path),
                                "artifact_path": str(screenshot_path),
                                "artifact_paths": [str(screenshot_path)],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: f"Done — attached the requested screenshot.\n\nMEDIA:{screenshot_path}",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Check open pull requests for https://github.com/sample-team/sample-app",
        ),
    )

    assert "Pull requests: sample-team/sample-app" in response.text
    assert "0 Open" in response.text
    assert response.artifact_paths == []


def test_web_remember_chat_turn_persists_sanitized_browser_evidence_reply() -> None:
    class Store:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def add_conversation_event(self, event: dict[str, object]) -> None:
            self.events.append(event)

    store = Store()
    runtime = SimpleNamespace(store=store)

    web_app._remember_web_chat_turn(
        runtime,
        conversation_id="web:browser-evidence",
        user_message="Can you research NYC Citi Bike e-bike helmet requirements for adults?",
        assistant_reply=(
            "The page lists helmet requirements and a 15 MPH speed limit."
        ),
        tool_results=[
            {
                "invocation_id": "extract",
                "tool_name": "browser_extract_text",
                "status": "completed",
                "output": {
                    "url": "https://www.nyc.gov/html/dot/html/bicyclists/ebikes.shtml",
                    "text": (
                        "NYC Resources\n"
                        "Electric Bicycles & More\n"
                        "Do I need to wear a helmet? | Recommended (Yes, if working) | Recommended | Yes\n"
                        "The speed limit for e-bike and pedal assist bicycle riders is 15MPH.\n"
                    ),
                },
            }
        ],
    )

    assert store.events
    reply = str(store.events[0]["assistant_reply"])
    assert reply == "The page lists helmet requirements and a 15 MPH speed limit."
    assert "Live-search blocker" not in reply
    assert "helmet" in reply


def test_platform_chat_recovers_artifacts_from_stored_turn_when_text_loses_media(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    artifact_path = tmp_path / "night_market_memo.pdf"
    artifact_path.write_bytes(b"%PDF-1.4\n%memo\n")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "Created the requested PDF:\n- night_market_memo.pdf",
                    "tool_results": [
                        {
                            "invocation_id": "pdf-1",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {"artifact_path": str(artifact_path)},
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "I couldn't attach all of the requested files. The task is still open.",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create a poster PDF and memo PDF.",
        ),
    )

    assert "couldn't attach" not in (response.text or "")
    assert "MEDIA:" not in (response.text or "")
    assert response.artifact_paths == [str(artifact_path)]


def test_platform_chat_recovers_artifacts_from_stored_reply_media_lines(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    artifact_path = tmp_path / "stage_chat_memory_recall.csv"
    artifact_path.write_text("Field,Value\ncheckpoint,ORCHID-7442-LUMEN\n", encoding="utf-8")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": f"Done — attached the requested CSV file.\n\nMEDIA:{artifact_path}",
                    "tool_results": [],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "I couldn't attach the requested file. The task is still open.",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Attach only stage_chat_memory_recall.csv.",
        ),
    )

    assert "couldn't attach" not in (response.text or "")
    assert "MEDIA:" not in (response.text or "")
    assert response.artifact_paths == [str(artifact_path)]


def test_platform_chat_does_not_attach_stored_local_discovery_paths(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    discovered_path = tmp_path / "existing_invoice.pdf"
    discovered_path.write_bytes(b"%PDF-1.4\n%existing file\n")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "I found 1 file:\n- existing_invoice.pdf",
                    "tool_results": [
                        {
                            "invocation_id": "search-1",
                            "tool_name": "terminal_exec",
                            "status": "completed",
                            "output": {
                                "stdout": f"{discovered_path}\n",
                                "stderr": "",
                                "artifacts": [str(discovered_path)],
                            },
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "I found 1 file:\n- existing_invoice.pdf",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Find existing_invoice.pdf and do not attach it.",
        ),
    )

    assert response.text == "I found 1 file:\n- existing_invoice.pdf"
    assert response.artifact_paths == []


def test_platform_chat_scope_artifact_extension_does_not_make_file_search_delivery(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    discovered_path = tmp_path / "workspace_summary_report.pdf"
    discovered_path.write_bytes(b"%PDF-1.4\n%existing file\n")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": (
                        "Done — attached the requested files.\n\n"
                        f"MEDIA:{discovered_path}"
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "search-1",
                            "tool_name": "file_search",
                            "status": "completed",
                            "output": {
                                "matches": [str(discovered_path)],
                                "match_details": [{"path": str(discovered_path), "match_type": "filename"}],
                            },
                        },
                        {
                            "invocation_id": "scope-1",
                            "tool_name": "request_tool_scope",
                            "status": "completed",
                            "output": {
                                "available_tools": ["file_read"],
                                "artifact_extensions": [".pdf"],
                                "scope_requested": True,
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "Done — attached the requested files.\n\n"
            f"MEDIA:{discovered_path}"
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Find workspace_summary_report.pdf and show me where it is.",
        ),
    )

    assert response.text == "I found 1 file:\n- workspace_summary_report.pdf"
    assert response.artifact_paths == []


def test_platform_chat_attaches_direct_existing_artifact_delivery(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    delivered_path = tmp_path / "stage_requested_file_delivery.xlsx"
    delivered_path.write_bytes(b"PK\x03\x04 workbook")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": (
                        "Done — attached the requested spreadsheet.\n\n"
                        f"MEDIA:{delivered_path}"
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "search-1",
                            "tool_name": "file_search",
                            "status": "completed",
                            "output": {
                                "action": "deliver",
                                "count": 1,
                                "direct_existing_artifact": True,
                                "matches": [str(delivered_path)],
                            },
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: f"I found 1 file:\n- {delivered_path.name}",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Send me the requested spreadsheet file.",
        ),
    )

    assert "I found 1 file" not in (response.text or "")
    assert "MEDIA:" not in (response.text or "")
    assert response.artifact_paths == [str(delivered_path)]


def test_platform_chat_rewrites_bad_media_reply_for_terminal_filename_listing(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    discovered_path = tmp_path / "stage_file_search_delivery_guard.pdf"
    discovered_path.write_bytes(b"%PDF-1.4\n%existing file\n")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": (
                        "Done — attached the requested PDF.\n\n"
                        f"MEDIA:{discovered_path}"
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "search-1",
                            "tool_name": "terminal_exec",
                            "status": "completed",
                            "output": {
                                "stdout": f"{discovered_path.name}\n",
                                "stderr": "",
                            },
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "Done — attached the requested PDF.\n\n"
            f"MEDIA:{discovered_path}"
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text=f"Find {discovered_path.name} in my workspace.",
        ),
    )

    assert response.text == f"I found 1 file:\n- {discovered_path.name}"
    assert "MEDIA:" not in (response.text or "")
    assert response.artifact_paths == []


def test_platform_chat_suppresses_transient_media_for_local_discovery_sidecar(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    discovered_path = tmp_path / "existing_invoice.pdf"
    listing_path = tmp_path / "existing_invoice_listing.txt"
    discovered_path.write_bytes(b"%PDF-1.4\n%existing file\n")
    listing_path.write_text("existing_invoice.pdf\n", encoding="utf-8")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "I found 1 file:\n- existing_invoice.pdf",
                    "tool_results": [
                        {
                            "invocation_id": "search-1",
                            "tool_name": "file_search",
                            "status": "completed",
                            "output": {
                                "matches": [str(discovered_path)],
                                "match_details": [{"path": str(discovered_path), "match_type": "filename"}],
                            },
                        },
                        {
                            "invocation_id": "sidecar-1",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(listing_path),
                                "artifact_path": str(listing_path),
                                "artifact_paths": [str(listing_path)],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "I found 1 file:\n- existing_invoice.pdf"
            f"\n\nMEDIA:{discovered_path}\nMEDIA:{listing_path}"
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Find existing_invoice.pdf and do not attach it.",
        ),
    )

    assert response.text == "I found 1 file:\n- existing_invoice.pdf"
    assert response.artifact_paths == []


def test_platform_chat_delivers_requested_file_write_after_local_discovery(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    discovered_path = tmp_path / "old_archive.zip"
    generated_path = tmp_path / "stage_archive_extension_summary.csv"
    discovered_path.write_bytes(b"PK\x03\x04 archive")
    generated_path.write_text("extension,count\n.html,3\n.csv,2\n", encoding="utf-8")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": (
                        "I found 1 file:\n"
                        "- old_archive.zip"
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "search-1",
                            "tool_name": "file_search",
                            "status": "completed",
                            "output": {
                                "matches": [str(discovered_path)],
                                "match_details": [{"path": str(discovered_path), "match_type": "filename"}],
                            },
                        },
                        {
                            "invocation_id": "write-1",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(generated_path),
                                "artifact_path": str(generated_path),
                                "artifact_paths": [str(generated_path)],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "I found 1 file:\n- old_archive.zip",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text=(
                "Create a file named stage_archive_extension_summary.csv from the current archive context. "
                "Attach only the CSV."
            ),
        ),
    )

    assert response.text == "Done - attached the requested CSV."
    assert response.artifact_paths == [str(generated_path)]


def test_platform_chat_prefers_stored_deliverable_over_transient_sidecar_media(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    workbook_path = tmp_path / "final_report.xlsx"
    csv_sidecar_path = tmp_path / "final_report.csv"
    workbook_path.write_bytes(b"PK\x03\x04 workbook")
    csv_sidecar_path.write_text("row,value\n", encoding="utf-8")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "Created the requested workbook:\n- final_report.xlsx",
                    "tool_results": [
                        {
                            "invocation_id": "xlsx-1",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(workbook_path),
                                "artifact_paths": [str(workbook_path)],
                                "artifact_descriptors": [
                                    {
                                        "path": str(workbook_path),
                                        "role": "deliverable",
                                        "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    }
                                ],
                            },
                        },
                        {
                            "invocation_id": "sidecar-1",
                            "tool_name": "terminal_exec",
                            "status": "completed",
                            "output": {
                                "stdout": f"{csv_sidecar_path}\n",
                                "stderr": "",
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: f"Done — attached the requested CSV.\n\nMEDIA:{csv_sidecar_path}",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create final_report.xlsx and attach only the workbook.",
        ),
    )

    assert response.text == "Done - attached the requested XLSX workbook."
    assert response.artifact_paths == [str(workbook_path)]


def test_platform_chat_does_not_promote_stored_json_sidecar_to_requested_artifact(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    workbook_path = tmp_path / "stage_archive_manifest_rebuilt.xlsx"
    csv_sidecar_path = tmp_path / "stage_archive_manifest_rebuilt.csv"
    json_sidecar_path = tmp_path / "stage_archive_manifest_rebuilt.json"
    workbook_path.write_bytes(b"PK\x03\x04 workbook")
    csv_sidecar_path.write_text("name,size\n", encoding="utf-8")
    json_sidecar_path.write_text("{}", encoding="utf-8")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": (
                        "Done - attached the requested artifacts: XLSX workbook, CSV, JSON.\n"
                        "Files: stage_archive_manifest_rebuilt.xlsx, "
                        "stage_archive_manifest_rebuilt.csv, stage_archive_manifest_rebuilt.json."
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "xlsx-1",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_paths": [
                                    str(workbook_path),
                                    str(csv_sidecar_path),
                                    str(json_sidecar_path),
                                ],
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "Done - attached the requested artifacts: XLSX workbook, CSV, JSON.\n"
            "Files: stage_archive_manifest_rebuilt.xlsx, "
            "stage_archive_manifest_rebuilt.csv, stage_archive_manifest_rebuilt.json."
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create stage_archive_manifest_rebuilt.xlsx and attach only that workbook.",
        ),
    )

    assert response.artifact_paths == [str(workbook_path)]
    assert "stage_archive_manifest_rebuilt.xlsx" in (response.text or "")
    assert "JSON" not in (response.text or "")
    assert "CSV" not in (response.text or "")
    assert "stage_archive_manifest_rebuilt.csv" not in (response.text or "")
    assert "stage_archive_manifest_rebuilt.json" not in (response.text or "")


def test_platform_chat_replaces_attachment_failure_when_stored_deliverable_exists(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    workbook_path = tmp_path / "final_report.xlsx"
    workbook_path.write_bytes(b"PK\x03\x04 workbook")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "I couldn't attach all of the requested files. The task is still open.",
                    "tool_results": [
                        {
                            "invocation_id": "xlsx-1",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(workbook_path),
                                "artifact_paths": [str(workbook_path)],
                                "artifact_descriptors": [
                                    {
                                        "path": str(workbook_path),
                                        "role": "deliverable",
                                        "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    }
                                ],
                            },
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "I couldn't attach all of the requested files. The task is still open.",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create final_report.xlsx and attach only the workbook.",
        ),
    )

    assert response.text == "Done - attached the requested XLSX workbook."
    assert response.artifact_paths == [str(workbook_path)]


def test_platform_chat_suppresses_unrequested_local_discovery_cleanup_artifacts(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    searched_pdf = tmp_path / "stage_file_search_delivery_guard.pdf"
    summary_pdf = tmp_path / "file_read_workspace_summary_delivery.pdf"
    searched_pdf.write_bytes(b"%PDF-1.4 searched")
    summary_pdf.write_bytes(b"%PDF-1.4 summary")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:file-search-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": (
                        "Done — attached the requested files.\n\n"
                        f"MEDIA:{summary_pdf}\n"
                        f"MEDIA:{searched_pdf}"
                    ),
                    "tool_results": [
                        {
                            "invocation_id": "search-1",
                            "tool_name": "file_search",
                            "status": "completed",
                            "output": {"matches": [str(searched_pdf)]},
                        },
                        {
                            "invocation_id": "read-1",
                            "tool_name": "file_read",
                            "status": "failed",
                            "output": {"path": str(searched_pdf), "type": "pdf"},
                            "error": "Could not extract PDF text",
                        },
                        {
                            "invocation_id": "pdf-1",
                            "tool_name": "pdf_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(summary_pdf),
                                "artifact_paths": [str(summary_pdf)],
                            },
                        },
                        {
                            "invocation_id": "write-1",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {
                                "path": str(searched_pdf),
                                "artifact_path": str(searched_pdf),
                                "requested_filename_alias": True,
                                "source_path": str(summary_pdf),
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: "Done — attached the requested files.",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="file-search-1",
            text="Show me matching local files named stage_file_search_delivery_guard.pdf.",
        ),
    )

    assert response.artifact_paths == []
    assert response.text == "I found 1 file:\n- stage_file_search_delivery_guard.pdf"


def test_chat_reply_sanitizer_suppresses_generated_file_search_alias_media(
    tmp_path,
) -> None:
    from nullion import chat_operator
    from nullion.tools import ToolResult

    searched_pdf = tmp_path / "stage_file_search_delivery_guard.pdf"
    listing_pdf = tmp_path / "stage_file_search_delivery_guard_listing.pdf"
    searched_pdf.write_bytes(b"%PDF-1.4 searched")
    listing_pdf.write_bytes(b"%PDF-1.4 listing")
    tool_results = [
        ToolResult(
            "search-1",
            "file_search",
            "completed",
            {"matches": [str(searched_pdf)]},
        ),
        ToolResult(
            "pdf-1",
            "pdf_create",
            "completed",
            {"path": str(listing_pdf), "artifact_path": str(listing_pdf), "artifact_paths": [str(listing_pdf)]},
        ),
        ToolResult(
            "write-1",
            "file_write",
            "completed",
            {
                "path": str(searched_pdf),
                "artifact_path": str(searched_pdf),
                "artifact_paths": [str(searched_pdf)],
                "requested_filename_alias": True,
                "source_path": str(listing_pdf),
            },
        ),
    ]

    reply = chat_operator._sanitize_chat_reply(
        prompt=f"Find the pdf file named {searched_pdf.name} in my workspace.",
        reply=f"Done — attached the requested PDF.\n\nMEDIA:{searched_pdf}",
        tool_results=tool_results,
    )

    assert reply == "I found 1 file:\n- stage_file_search_delivery_guard.pdf"


def test_platform_chat_preserves_incomplete_reply_when_artifact_package_still_needs_approval(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    workbook_path = tmp_path / "stage_qa_test_workbook.xlsx"
    report_path = tmp_path / "stage_qa_test_report.html"
    for path in (workbook_path, report_path):
        path.write_bytes(b"artifact")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "I couldn't attach all of the requested files. The task is still open.",
                    "tool_results": [
                        {
                            "invocation_id": "xlsx-1",
                            "tool_name": "spreadsheet_create",
                            "status": "completed",
                            "output": {
                                "artifact_path": str(workbook_path),
                                "artifact_descriptors": [
                                    {
                                        "path": str(workbook_path),
                                        "role": "deliverable",
                                        "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    }
                                ],
                            },
                        },
                        {
                            "invocation_id": "html-1",
                            "tool_name": "file_write",
                            "status": "completed",
                            "output": {"path": str(report_path)},
                        },
                        {
                            "invocation_id": "email-1",
                            "tool_name": "email_send",
                            "status": "denied",
                            "output": {
                                "approval_id": "approval-email-1",
                                "reason": "approval_required",
                                "requires_approval": True,
                            },
                        },
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: (
            "Done - attached the requested artifacts: XLSX workbook, HTML file."
        ),
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create the files and email them to recipient@example.com.",
        ),
    )

    assert (response.text or "").startswith(
        "I couldn't attach all of the requested files. The task is still open."
    )
    assert "Done - attached" not in (response.text or "")
    assert response.artifact_paths == [str(workbook_path), str(report_path)]


def test_platform_chat_keeps_requested_terminal_output_path(
    tmp_path, monkeypatch,
) -> None:
    from nullion import chat_operator, platform_chat

    workbook_path = tmp_path / "terminal_created.xlsx"
    workbook_path.write_bytes(b"PK\x03\x04 workbook")

    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "web:browser-1"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "assistant_reply": "Created terminal_created.xlsx",
                    "tool_results": [
                        {
                            "invocation_id": "exec-1",
                            "tool_name": "terminal_exec",
                            "status": "completed",
                            "output": {
                                "stdout": f"{workbook_path}\n",
                                "stderr": "",
                            },
                        }
                    ],
                }
            ]

    runtime = SimpleNamespace(store=Store())
    monkeypatch.setattr(
        chat_operator,
        "handle_chat_operator_message",
        lambda *args, **kwargs: f"Done — attached the requested workbook.\n\nMEDIA:{workbook_path}",
    )

    response = platform_chat.run_platform_chat_request(
        runtime,
        platform_chat.PlatformChatRequest(
            platform="web",
            conversation_id="browser-1",
            text="Create terminal_created.xlsx and attach only that workbook.",
        ),
    )

    assert response.text == "Done — attached the requested workbook."
    assert response.artifact_paths == [str(workbook_path)]


def test_platform_chat_delivery_contract_preserves_requested_extension_counts() -> None:
    from nullion import platform_chat

    extensions = platform_chat._required_attachment_extensions_for_platform_delivery(
        "Create a poster .pdf, memo .pdf, and .html version.",
        ["/tmp/poster.pdf", "/tmp/poster.html"],
    )

    assert extensions == (".pdf", ".pdf", ".html")


def test_config_api_defaults_smart_cleanup_on(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.delenv("NULLION_SMART_CLEANUP_ENABLED", raising=False)
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    payload = TestClient(app).get("/api/config").json()

    assert payload["smart_cleanup_enabled"] is True


def test_profile_save_preserves_existing_values_when_blank_form_has_not_loaded(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / ".nullion"))
    profile_path = tmp_path / ".nullion" / "profile.json"
    profile_path.parent.mkdir(parents=True)
    profile_path.write_text(
        json.dumps(
            {
                "name": "Himan",
                "email": "himan@example.test",
                "phone": "",
                "address": "10024",
                "notes": "Use my profile location for weather.",
            }
        ),
        encoding="utf-8",
    )
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    response = client.post(
        "/api/profile",
        json={"name": "", "email": "", "phone": "", "address": "", "notes": ""},
    )

    assert response.status_code == 200
    assert client.get("/api/profile").json() == {
        "name": "Himan",
        "email": "himan@example.test",
        "phone": "",
        "address": "10024",
        "notes": "Use my profile location for weather.",
    }


def test_profile_save_allows_explicit_clear_after_profile_loaded(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / ".nullion"))
    profile_path = tmp_path / ".nullion" / "profile.json"
    profile_path.parent.mkdir(parents=True)
    profile_path.write_text(
        json.dumps(
            {
                "name": "Himan",
                "email": "himan@example.test",
                "phone": "",
                "address": "10024",
                "notes": "Use my profile location for weather.",
            }
        ),
        encoding="utf-8",
    )
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    response = client.post(
        "/api/profile",
        json={"name": "", "email": "", "phone": "", "address": "", "notes": "", "_allow_clear": True},
    )

    assert response.status_code == 200
    assert client.get("/api/profile").json() == {
        "name": "",
        "email": "",
        "phone": "",
        "address": "",
        "notes": "",
    }


def test_capability_dependency_ui_uses_install_request_language() -> None:
    assert "Ask Builder" not in _HTML
    assert "Library dependencies" not in _HTML
    assert "cfg-dependency-id" not in _HTML
    assert "cfg-dependency-github-url" not in _HTML
    assert "renderCapabilityDependencies" in _HTML
    assert "Request install" in _HTML
    assert "function uninstallSkillPackFromSettings" in _HTML
    assert "/api/skill-packs/uninstall" in _HTML
    assert "function uninstallCapabilityDependency" in _HTML
    assert "/api/capabilities/dependencies/uninstall" in _HTML
    assert "function restoreCapabilityDependency" in _HTML
    assert "/api/capabilities/dependencies/restore" in _HTML
    assert "available_to_agent" in _SOURCE
    assert "Review & install" in _HTML
    assert "capability-dependency-option" in _HTML
    assert "capability-dependency-action" in _HTML
    assert ".capability-dependency-foot a" in _HTML
    assert "capability-custom-primary" in _HTML
    assert "cfg-custom-python-source" in _HTML
    assert "PyPI package" in _HTML
    assert "GitHub repo" in _HTML
    assert "Advanced details" in _HTML
    assert "#9cc8ff" in _HTML
    assert "/api/capabilities/custom-dependency/propose" in _HTML
    assert "Core packages are installed with Nullion and kept current by updates" in _HTML
    assert "usage note, optional" in _HTML
    assert "prefillCustomCapabilityDependencyMetadata" in _HTML
    assert "/api/capabilities/dependency-metadata" in _HTML
    assert "cfg-custom-python-requirement" not in _HTML
    assert "cfg-custom-python-github\"" not in _HTML


def test_builder_proposal_status_and_web_cards_explain_acceptance_benefit(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.builder import (
        BuilderDecisionType,
        BuilderProposal,
        BuilderProposalRecord,
        builder_proposal_acceptance_benefit,
    )
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Artifact workflow",
        summary="Save the artifact workflow.",
        confidence=0.88,
        approval_mode="skill",
    )
    runtime.store.add_builder_proposal(
        BuilderProposalRecord(
            proposal_id="prop-benefit",
            proposal=proposal,
            status="pending",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    status = TestClient(app).get("/api/status?full=1").json()

    [item] = [entry for entry in status["builder_proposals"] if entry["proposal_id"] == "prop-benefit"]
    assert item["acceptance_benefit"] == builder_proposal_acceptance_benefit(proposal)
    assert "create or update files/artifacts" in item["acceptance_benefit"]
    assert "const acceptanceBenefit = p.acceptance_benefit || '';" in _HTML
    assert "builder-benefit" in _HTML


def test_lite_status_reconciles_stale_platform_mini_agent_runs(tmp_path) -> None:
    from dataclasses import replace

    from fastapi.testclient import TestClient

    from nullion.mini_agent_runs import MiniAgentRunStatus
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    for run_id, conversation_id in (
        ("run-stale-web", "web:live-stage-contract:stale"),
        ("run-stale-telegram", "telegram:100001"),
    ):
        run = runtime.start_mini_agent_run(
            run_id=run_id,
            capsule_id=f"{run_id}-group",
            mini_agent_type="Stale task",
            created_at=datetime.now(UTC) - timedelta(hours=2),
            metadata={"conversation_id": conversation_id},
        )
        runtime.store.add_mini_agent_run(replace(run, status=MiniAgentRunStatus.RUNNING))
    runtime.checkpoint()

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    payload = TestClient(app).get("/api/status").json()

    assert payload["health"]["counts"]["active_mini_agent_runs"] == 0
    assert runtime.store.get_mini_agent_run("run-stale-web").status is MiniAgentRunStatus.FAILED
    assert runtime.store.get_mini_agent_run("run-stale-telegram").status is MiniAgentRunStatus.FAILED


def test_skill_pack_uninstall_api_removes_pack_and_agent_prompt(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import skill_pack_installer as installer
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    source = tmp_path / "source"
    skill_dir = source / "skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text("# Local Skill\nUse local-only docs.", encoding="utf-8")
    pack_root = tmp_path / "packs"
    env_file = tmp_path / ".env"
    monkeypatch.setenv("NULLION_SKILL_PACK_DIR", str(pack_root))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.setenv("NULLION_ENABLED_SKILL_PACKS", "nullion/web-research, local/demo")
    monkeypatch.setenv("NULLION_SKILL_PACK_ACCESS_ENABLED", "true")
    installer.install_skill_pack(str(source), pack_id="local/demo", root=pack_root)

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/skill-packs/uninstall", json={"pack_id": "local/demo"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["removed"] is True
    assert payload["enabled_skill_packs"] == "nullion/web-research"
    assert installer.get_installed_skill_pack("local/demo", root=pack_root) is None
    assert "local/demo" not in installer.format_enabled_skill_packs_for_prompt(["local/demo"], root=pack_root)
    assert "NULLION_ENABLED_SKILL_PACKS=\"nullion/web-research\"" in env_file.read_text(encoding="utf-8")


def test_skill_pack_install_api_uses_runtime_home_when_pack_dir_unset(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    source = tmp_path / "source"
    skill_dir = source / "skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text("# Local Skill\nUse local-only docs.", encoding="utf-8")
    env_file = tmp_path / ".env"
    runtime_home = tmp_path / "stage-home"
    monkeypatch.delenv("NULLION_SKILL_PACK_DIR", raising=False)
    monkeypatch.delenv("NULLION_DATA_DIR", raising=False)
    monkeypatch.setenv("NULLION_HOME", str(runtime_home))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.setenv("NULLION_ENABLED_SKILL_PACKS", "nullion/web-research")

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post(
        "/api/skill-packs/install",
        json={"source": str(source), "pack_id": "local/demo", "enable": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pack"]["path"] == str(runtime_home / "skill-packs" / "local" / "demo")
    assert (runtime_home / "skill-packs" / "local" / "demo" / "nullion-skill-pack.json").exists()
    assert "NULLION_ENABLED_SKILL_PACKS=\"nullion/web-research, local/demo\"" in env_file.read_text(encoding="utf-8")


def test_capability_dependency_uninstall_marks_agent_unavailable(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import builder_capabilities
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    env_file = tmp_path / ".env"
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.delenv("NULLION_DISABLED_CAPABILITY_DEPENDENCIES", raising=False)

    def fake_uninstall(dependency_id: str) -> dict[str, object]:
        assert dependency_id == "pandas"
        return {
            "ok": True,
            "dependency_id": "pandas",
            "package": "pandas",
            "returncode": 0,
            "installed": False,
            "installed_version": None,
            "stdout": "",
            "stderr": "",
        }

    real_status = builder_capabilities.capability_dependency_status

    def fake_status(dependency):
        if getattr(dependency, "package", "") == "pandas":
            return SimpleNamespace(
                dependency_id="pandas",
                package="pandas",
                import_name="pandas",
                installed=True,
                installed_version="9.9.9",
                summary="DataFrame analysis.",
                usage_note="DataFrame analysis.",
                docs_url=None,
                github_url=None,
                license=None,
                install_requirement="pandas>=2.2,<3",
            )
        return real_status(dependency)

    monkeypatch.setattr(builder_capabilities, "uninstall_capability_dependency", fake_uninstall)
    monkeypatch.setattr(builder_capabilities, "capability_dependency_status", fake_status)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    response = client.post("/api/capabilities/dependencies/uninstall", json={"dependency_id": "pandas"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert os.environ["NULLION_DISABLED_CAPABILITY_DEPENDENCIES"] == "pandas"
    config_payload = client.get("/api/config").json()
    pandas = next(item for item in config_payload["capability_dependencies"] if item["dependency_id"] == "pandas")
    assert pandas["disabled"] is True
    assert pandas["available_to_agent"] is False
    assert "pandas" not in builder_capabilities.format_installed_dependency_context(runtime)

    restored = client.post("/api/capabilities/dependencies/restore", json={"dependency_id": "pandas"})
    assert restored.status_code == 200
    assert os.environ.get("NULLION_DISABLED_CAPABILITY_DEPENDENCIES") is None


def test_config_api_lists_setup_python_packages(tmp_path) -> None:
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    payload = TestClient(app).get("/api/config").json()
    ids = {item["dependency_id"] for item in payload["capability_dependencies"]}

    assert {"pandas", "openpyxl", "pillow", "pypdf", "python-docx", "python-pptx", "soundfile"} <= ids


def test_capability_dependency_metadata_endpoint_prefills_short_editable_defaults(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    def fake_metadata(package: str) -> dict[str, object]:
        assert package == "pandas"
        return {
            "ok": True,
            "package": "pandas",
            "import_name": "pandas",
            "summary": "Data analysis toolkit.",
            "license": "BSD-3-Clause",
            "docs_url": "https://pandas.pydata.org/docs/",
            "source_url": "https://github.com/pandas-dev/pandas",
        }

    monkeypatch.setattr(web_app, "_dependency_metadata_from_pypi", fake_metadata)
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).get("/api/capabilities/dependency-metadata?source=pypi&package=pandas")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "package": "pandas",
        "import_name": "pandas",
        "summary": "Data analysis toolkit.",
        "license": "BSD-3-Clause",
        "docs_url": "https://pandas.pydata.org/docs/",
        "source_url": "https://github.com/pandas-dev/pandas",
    }


def test_chat_upload_limit_is_configurable_and_reports_oversize(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_MAX_UPLOAD_MB", "1")
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())

    response = TestClient(app).post(
        "/api/upload",
        files={"file": ("clip.mov", b"x" * (1024 * 1024 + 1), "video/quicktime")},
    )

    assert response.status_code == 413
    assert response.json() == {"ok": False, "error": "File exceeds maximum size of 1 MB."}


def test_saved_media_models_default_enabled_without_legacy_toggle_flag() -> None:
    options = _media_model_options(
        "image_ocr",
        provider_models={},
        media_models={"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={},
        providers_configured={"openai": True},
    )

    assert options == [{"provider": "openai", "model": "gpt-4o", "label": "openai · gpt-4o"}]


def test_media_provider_toggle_exposes_models_when_explicitly_enabled() -> None:
    options = _media_model_options(
        "image_ocr",
        provider_models={},
        media_models={"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={"openai": True},
        providers_configured={"openai": True},
    )

    assert options == [{"provider": "openai", "model": "gpt-4o", "label": "openai · gpt-4o"}]


def test_media_model_javascript_uses_explicit_media_inventory_only() -> None:
    start = _HTML.index("function mediaModelOptionsForCapability(capability)")
    end = _HTML.index("function updateMediaModelCapabilityFeedback()", start)
    body = _HTML[start:end]

    assert "_providerMediaModels" in body
    assert "_providerModels" not in body
    assert "Connected provider/model:" not in body


def test_chat_provider_models_do_not_populate_media_options() -> None:
    disabled_options = _media_model_options(
        "image_ocr",
        provider_models={"anthropic": "claude-sonnet-4-6"},
        media_models={},
        providers_enabled={"anthropic": True},
        media_providers_enabled={},
        providers_configured={"anthropic": True},
    )
    enabled_options = _media_model_options(
        "image_ocr",
        provider_models={"anthropic": "claude-sonnet-4-6"},
        media_models={},
        providers_enabled={"anthropic": True},
        media_providers_enabled={"anthropic": True},
        providers_configured={"anthropic": True},
    )

    assert disabled_options == []
    assert enabled_options == []


def test_media_provider_enabled_javascript_defaults_saved_models_on() -> None:
    assert "function mediaProviderEnabled(provider)" in _HTML
    assert "return _mediaProvidersEnabled[provider] !== false;" in _HTML
    assert "return providerHasMediaModels(provider);" in _HTML


def test_header_shows_current_version_tag() -> None:
    match = re.search(
        r'<span class="version-tag" title="Current Nullion version">([^<]+)</span>',
        _HTML,
    )
    assert match is not None
    assert match.group(1).startswith("v")
    assert "{{NULLION_VERSION}}" not in _HTML


def test_header_exposes_workspace_folder_button() -> None:
    assert 'id="workspace-folder-btn"' in _HTML
    assert 'aria-label="Open workspace folder"' in _HTML
    assert '<svg class="ni" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">' in _HTML
    assert "#browser-btn .ni,\n  #workspace-folder-btn .ni { width:26px; height:26px; }" in _HTML
    assert "#browser-btn,\n  #workspace-folder-btn {\n    width: 34px;\n    height: 34px;" in _HTML
    assert "async function openWorkspaceFolder()" in _HTML
    assert "fetch('/api/workspace/open', { method: 'POST' })" in _HTML


def test_memory_chips_show_value_without_internal_key_prefix() -> None:
    start = _HTML.index("function memoryChipHtml(m, full = false)")
    end = _HTML.index("async function deleteMemoryItem", start)
    body = _HTML[start:end]

    assert ".dash-section.memory-section { padding: 14px 20px 18px; }" in _HTML
    assert ".memory-section #memory-list { padding: 2px 4px 6px; }" in _HTML
    assert ".memory-chip-list { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-start; padding: 4px 4px 6px; }" in _HTML
    assert "max-width: min(100%, 260px)" in _HTML
    assert "font-size: 10.5px; font-weight: 720; line-height: 1.1; min-height: 26px;" in _HTML
    assert "width: 17px; height: 17px;" in _HTML
    assert "el.innerHTML = `${visible}${more}`;" in _HTML
    assert 'View all memories' in _HTML
    assert "const displayText = value ?" in body
    assert '${escHtml(displayText)}' in body
    assert '${escHtml(key)}${labelValue' not in body
    assert "const title = displayText;" in body
    assert "memoryPreviewHtml(visibleMemory.slice(0, previewCount))" in _HTML


def test_memory_full_list_uses_score_weighted_map_with_delete_controls() -> None:
    assert ".full-list-modal.memory-modal { max-width: 780px; width: min(780px, 96vw); }" in _HTML
    assert "memory-brain-node score-${level}" in _HTML
    assert "padding: 6px 6px 6px 12px" in _HTML
    assert "memory-link-svg" in _HTML
    assert "memory-link-tier" in _HTML
    assert "memory-link-score" in _HTML
    assert "memory-link-fresh" in _HTML
    assert "memory-map-legend" in _HTML
    assert "memory-legend-gradient" in _HTML
    assert "linear-gradient(90deg" in _HTML
    assert "function memoryMapHtml(items)" in _HTML
    assert "function memoryNodeLayout(memories, maxScore)" in _HTML
    assert "function memoryConnectionSvg(layout, links)" in _HTML
    assert "function memoryConnectionLines(nodes)" in _HTML
    assert "function memoryBrainNodeHtml(node)" in _HTML
    assert "body.innerHTML = memoryMapHtml(items);" in _HTML
    assert "memoryConnectionSvg(layout, links)" in _HTML
    assert '<svg class="memory-link-svg"' in _HTML
    assert 'aria-label="Memory score color scale"' in _HTML
    assert "More permanent</span>" in _HTML
    assert "Recent</span>" in _HTML
    assert "memoryScoreLabel(score)" not in _HTML
    assert "memoryKindLabel(item.kind)" in _HTML
    assert 'class="memory-node-delete"' in _HTML
    assert "deleteMemoryItem(decodeURIComponent" in _HTML
    assert 'data-tooltip="${escAttr(title)}"' in _HTML


def test_settings_expose_memory_bucket_caps() -> None:
    assert 'id="cfg-memory-smart-cleanup"' in _HTML
    assert 'id="cfg-memory-long-term-limit"' in _HTML
    assert 'id="cfg-memory-mid-term-limit"' in _HTML
    assert 'id="cfg-memory-short-term-limit"' in _HTML
    assert "<label>Mid-term</label>" in _HTML
    assert "<label>Short-term</label>" in _HTML
    assert "cfg.memory_smart_cleanup === true" in _HTML
    assert "cfg.memory_long_term_limit ?? 25" in _HTML
    assert "cfg.memory_mid_term_limit ?? 15" in _HTML
    assert "cfg.memory_short_term_limit ?? 10" in _HTML
    assert "memory_long_term_limit: positiveNumberInputValue('cfg-memory-long-term-limit', 25)" in _HTML
    assert "memory_mid_term_limit: positiveNumberInputValue('cfg-memory-mid-term-limit', 15)" in _HTML
    assert "memory_short_term_limit: positiveNumberInputValue('cfg-memory-short-term-limit', 10)" in _HTML
    assert "memory_smart_cleanup: document.getElementById('cfg-memory-smart-cleanup').checked" in _HTML
    assert "Maximum saved memories per workspace. Recalled memories are kept longer." in _HTML


def test_builder_memory_full_card_and_actions_are_rendered() -> None:
    assert "Agent memory is full" in _SOURCE
    assert "String(p.decision_type || '') === 'memory_full'" in _HTML
    assert "async function increaseMemoryLimits(btn)" in _HTML
    assert "async function runSmartMemoryCleanup(btn)" in _HTML
    assert "More saved memory can increase token usage" in _HTML
    assert "Builder will remove lower-strength memories first" in _HTML
    assert "/api/memory/increase-limits" in _SOURCE
    assert "/api/memory/smart-cleanup" in _SOURCE


def test_memory_delete_all_rerenders_open_memory_modal() -> None:
    assert "function updateLocalMemoryList(nextMemory)" in _HTML
    assert "_lastDashboardData = { ...(_lastDashboardData || {}), memory };" in _HTML
    assert "updateLocalMemoryList([]);" in _HTML
    assert "updateLocalMemoryList(current.filter(item => String(item?.entry_id || '') !== id));" in _HTML
    assert "if (_currentFullListKey === 'memory') renderFullListPage('memory');" in _HTML
    assert "if (!items.length) {" in _HTML
    assert "body.innerHTML = '<div class=\"empty\"><strong>Memory quiet</strong>Facts, preferences, and project context will collect here.</div>';" in _HTML


def test_web_builder_reflection_skips_scheduler_only_tool_turns() -> None:
    from nullion.tools import ToolResult

    result = web_app._try_web_builder_reflection(
        SimpleNamespace(),
        SimpleNamespace(model_client=object()),
        user_message="request from chat",
        assistant_reply="Ran cron.",
        tool_results=[
            ToolResult(
                invocation_id="tool-1",
                tool_name="run_cron",
                status="completed",
                output={"delivery_status": "saved"},
            )
        ],
        conversation_id="web:test",
        memory_owner="workspace:workspace_admin",
    )

    assert result.learned_skill_titles == []
    assert result.detail == "skipped: operational scheduler tools"


def test_web_config_persists_memory_bucket_caps(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    env_path = tmp_path / ".env"
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.delenv("NULLION_MEMORY_LONG_TERM_LIMIT", raising=False)
    monkeypatch.delenv("NULLION_MEMORY_MID_TERM_LIMIT", raising=False)
    monkeypatch.delenv("NULLION_MEMORY_SHORT_TERM_LIMIT", raising=False)
    monkeypatch.delenv("NULLION_MEMORY_SMART_CLEANUP_ENABLED", raising=False)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)
    response = client.post(
        "/api/config",
        json={
            "memory_long_term_limit": 7,
            "memory_mid_term_limit": 9,
            "memory_short_term_limit": 2,
            "memory_smart_cleanup": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert web_app.os.environ["NULLION_MEMORY_LONG_TERM_LIMIT"] == "7"
    assert web_app.os.environ["NULLION_MEMORY_MID_TERM_LIMIT"] == "9"
    assert web_app.os.environ["NULLION_MEMORY_SHORT_TERM_LIMIT"] == "2"
    assert web_app.os.environ["NULLION_MEMORY_SMART_CLEANUP_ENABLED"] == "true"
    payload = client.get("/api/config").json()
    assert payload["memory_smart_cleanup"] is True
    assert payload["memory_long_term_limit"] == 7
    assert payload["memory_mid_term_limit"] == 9
    assert payload["memory_short_term_limit"] == 2


def test_config_rejects_zero_memory_limits(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    env_path = tmp_path / ".env"
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    response = client.post(
        "/api/config",
        json={
            "memory_long_term_limit": 0,
            "memory_mid_term_limit": 0,
            "memory_short_term_limit": 0,
        },
    )

    assert response.status_code == 400
    assert "memory_long_term_limit must be at least 1" in response.json()["error"]


def test_web_config_refreshes_live_settings_and_agent_registry(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    env_path = tmp_path / ".env"
    workspace_root = tmp_path / "workspace-live"
    extra_root = tmp_path / "extra-live"
    workspace_root.mkdir()
    extra_root.mkdir()
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.delenv("NULLION_OPERATOR_NAME", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", raising=False)
    monkeypatch.delenv("NULLION_WORKSPACE_ROOT", raising=False)
    monkeypatch.delenv("NULLION_ALLOWED_ROOTS", raising=False)

    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path / "old-root"])
    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=registry)
    client = TestClient(app)

    response = client.post(
        "/api/config",
        json={
            "operator_name": "Live Settings User",
            "tg_chat_id": "424242",
            "workspace_root": str(workspace_root),
            "allowed_roots": str(extra_root),
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    live_settings = app.state.nullion_app_settings
    assert live_settings.telegram.operator_chat_id == "424242"
    assert live_settings.workspace_root == str(workspace_root)
    assert live_settings.allowed_roots == (str(extra_root),)
    assert Path(workspace_root).resolve() in registry.filesystem_allowed_roots()
    assert Path(extra_root).resolve() in registry.filesystem_allowed_roots()
    assert all(root.name != "old-root" for root in registry.filesystem_allowed_roots())


def test_web_config_persists_browser_backend_to_env_and_credentials(monkeypatch, tmp_path) -> None:
    import copy

    from fastapi.testclient import TestClient

    from nullion import web_app
    import nullion.plugins.browser_plugin as browser_plugin
    from nullion.tools import ToolRegistry

    env_path = tmp_path / ".env"
    env_path.write_text('NULLION_BROWSER_BACKEND="auto"\n', encoding="utf-8")
    saved_credentials = {"browser_backend": "auto"}
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    monkeypatch.setattr(web_app, "_read_credentials_json", lambda: copy.deepcopy(saved_credentials))

    def write_credentials(creds):
        saved_credentials.clear()
        saved_credentials.update(copy.deepcopy(creds))

    monkeypatch.setattr(web_app, "_write_credentials_json", write_credentials)
    monkeypatch.setattr(browser_plugin, "register_browser_tools", lambda registry, **kwargs: None)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    response = client.post("/api/config", json={"browser_backend": "playwright"})

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert saved_credentials["browser_backend"] == "playwright"
    assert web_app.os.environ["NULLION_BROWSER_BACKEND"] == "playwright"
    assert 'NULLION_BROWSER_BACKEND="playwright"' in env_path.read_text(encoding="utf-8")
    assert client.get("/api/config").json()["browser_backend"] == "playwright"


def test_web_config_preserves_saved_provider_token_when_key_field_is_blank(monkeypatch, tmp_path) -> None:
    import base64
    import copy

    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    def jwt(payload: dict[str, object]) -> str:
        def enc(data: dict[str, object]) -> str:
            raw = json.dumps(data, separators=(",", ":")).encode()
            return base64.urlsafe_b64encode(raw).decode().rstrip("=")

        return f"{enc({'alg': 'none'})}.{enc(payload)}.sig"

    env_path = tmp_path / ".env"
    codex_access_token = jwt({"https://api.openai.com/auth": {"chatgpt_account_id": "acct-test"}, "exp": 9_999_999_999})
    saved_credentials = {
        "provider": "codex",
        "api_key": codex_access_token,
        "refresh_token": "codex-refresh-token",
        "keys": {
            "codex": codex_access_token,
            "openrouter": "sk-openrouter-token",
        },
        "model": "gpt-5.5",
        "models": {
            "codex": "gpt-5.5",
            "openrouter": "qwen/qwen3.6-flash",
        },
    }
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setattr(web_app, "_read_credentials_json", lambda: copy.deepcopy(saved_credentials))

    def write_credentials(creds):
        saved_credentials.clear()
        saved_credentials.update(copy.deepcopy(creds))

    monkeypatch.setattr(web_app, "_write_credentials_json", write_credentials)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post(
        "/api/config",
        json={
            "model_provider": "codex",
            "api_key": "",
            "model_name": "gpt-5.5",
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert saved_credentials["api_key"] == codex_access_token
    assert saved_credentials["keys"]["codex"] == codex_access_token
    assert saved_credentials["refresh_token"] == "codex-refresh-token"
    env_text = env_path.read_text(encoding="utf-8")
    assert f'NULLION_OPENAI_API_KEY="{codex_access_token}"' in env_text
    assert f'OPENAI_API_KEY="{codex_access_token}"' in env_text


def test_open_agent_browser_uses_open_without_navigation_when_no_url(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        ToolRegistry,
        ToolResult,
        ToolRiskLevel,
        ToolSideEffectClass,
        ToolSpec,
    )

    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    calls: list[tuple[str, dict[str, object]]] = []
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.mark_plugin_installed("browser_plugin")

    def browser_open(invocation):
        calls.append(("browser_open", dict(invocation.arguments)))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"session_id": "workspace-alpha", "result": "opened"},
        )

    def browser_navigate(invocation):
        calls.append(("browser_navigate", dict(invocation.arguments)))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"session_id": "workspace-alpha", "result": "navigated"},
        )

    registry.register(
        ToolSpec("browser_open", "Open", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 10),
        browser_open,
    )
    registry.register(
        ToolSpec("browser_navigate", "Navigate", ToolRiskLevel.MEDIUM, ToolSideEffectClass.READ, False, 30),
        browser_navigate,
    )
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=registry)
    client = TestClient(app)

    opened = client.post("/api/browser/open", json={})
    navigated = client.post("/api/browser/open", json={"url": "https://example.com"})

    assert opened.status_code == 200
    assert opened.json()["ok"] is True
    assert opened.json()["result"]["session_id"] == "workspace-alpha"
    assert navigated.status_code == 200
    assert calls == [
        ("browser_open", {"session_id": "default"}),
        ("browser_navigate", {"session_id": "default", "url": "https://example.com"}),
    ]


def test_live_browser_refresh_tracks_all_registered_browser_tools() -> None:
    from nullion import web_app

    expected = {
        "browser_open",
        "browser_navigate",
        "browser_click",
        "browser_click_element",
        "browser_click_id",
        "browser_type",
        "browser_type_field",
        "browser_type_id",
        "browser_select_combobox",
        "browser_snapshot",
        "browser_extract_text",
        "browser_screenshot",
        "browser_scroll",
        "browser_wait_for",
        "browser_assert_page_state",
        "browser_find",
        "browser_run_js",
        "browser_close",
    }

    assert expected <= set(web_app._BROWSER_TOOL_NAMES)


def test_web_status_surfaces_memory_full_card_and_cleanup_actions(monkeypatch, tmp_path) -> None:
    from datetime import UTC, datetime

    from fastapi.testclient import TestClient
    from nullion import web_app
    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    env_path = tmp_path / ".env"
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setenv("NULLION_MEMORY_LONG_TERM_LIMIT", "2")
    monkeypatch.setenv("NULLION_MEMORY_MID_TERM_LIMIT", "10")
    monkeypatch.setenv("NULLION_MEMORY_SHORT_TERM_LIMIT", "10")
    monkeypatch.setenv("NULLION_MEMORY_SMART_CLEANUP_ENABLED", "false")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = memory_owner_for_web_admin()
    now = datetime.now(UTC)
    for key, score in (("keep", 8.0), ("drop", 0.0)):
        runtime.store.add_user_memory_entry(
            UserMemoryEntry(
                entry_id=f"{owner}:{key}",
                owner=owner,
                kind=UserMemoryKind.PREFERENCE,
                key=key,
                value=f"{key} memory",
                created_at=now,
                updated_at=now,
                use_score=score,
                use_count=int(score),
            )
        )
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    status = client.get("/api/status?full=1").json()
    [proposal] = [item for item in status["builder_proposals"] if item["decision_type"] == "memory_full"]
    assert proposal["title"] == "Agent memory is full"
    assert "increase token usage" in proposal["summary"]
    assert "Enable it so Builder can remove lower-strength memories automatically" in proposal["summary"]
    assert proposal["actions"]["can_increase"] is True
    assert proposal["actions"]["suggest_enable_smart_cleanup"] is True

    increased = client.post("/api/memory/increase-limits").json()
    assert increased["ok"] is True
    assert increased["updated"]["NULLION_MEMORY_LONG_TERM_LIMIT"] == "7"

    monkeypatch.setenv("NULLION_MEMORY_LONG_TERM_LIMIT", "2")
    cleaned = client.post("/api/memory/smart-cleanup").json()
    assert cleaned["ok"] is True
    assert cleaned["removed"] == 1
    remaining = runtime.store.list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert [entry.key for entry in remaining] == ["keep"]
    refreshed = client.get("/api/status?full=1").json()
    assert not any(item["decision_type"] == "memory_full" for item in refreshed["builder_proposals"])


def test_web_auto_skill_background_flow_auto_accepts_by_default() -> None:
    assert "async def analyze_conversation_for_skills(conv_id: str, auto_accept: bool = True)" in _SOURCE
    assert "if (data.ok && data.auto_accepted && data.accepted && data.accepted.length)" in _HTML
    assert "function _showAutoSkillAcceptedBanner(skills)" in _HTML


def test_task_status_card_ignores_progress_notes() -> None:
    assert "const statusKind = String(data.status_kind || 'task_summary');" in _HTML
    assert "if (statusKind !== 'task_summary') return;" in _HTML
    assert "if (data.conversation_id && data.conversation_id !== conversationId) return;" in _HTML
    assert 'if status_kind != "task_summary" and not activity_trace_enabled():' in inspect.getsource(create_app)
    assert "status_text = compact_tool_activity_text(" in inspect.getsource(create_app)


def test_task_status_card_marks_terminal_list_as_finalizing() -> None:
    assert "const rowStates = [];" in _HTML
    assert "const allTerminal = rowStates.length > 0 && rowStates.every(state => ['complete', 'failed', 'cancelled'].includes(state));" in _HTML
    assert "? 'Finalizing results' : titles[0];" in _HTML


def test_web_background_message_reuses_active_turn_bubble_for_ungrouped_replies() -> None:
    assert "function correlatedTurnIdForBackgroundReply(data)" in _HTML
    assert "const groupId = String(data.group_id || '').trim();" in _HTML
    assert "return _turnIdByTaskGroup.get(groupId) || null;" in _HTML
    assert "String(bubble.dataset.rawText || '').trim() === backgroundText" in _HTML
    assert "if (data.group_id) {" in _HTML
    assert "if (!wasRecentlyDisplayedBotReply(data.text || '', artifacts)) addMessage('bot', data.text || '', false, { artifacts });" in _HTML
    assert "const groupId = String(data.group_id || '');" in _HTML
    assert "const groupTurnId = _turnIdByTaskGroup.get(groupId) || data.turn_id || null;" in _HTML
    assert "Boolean(groupTurnId && _botTurnBubbles.has(groupTurnId))" in _HTML
    assert "} else {\n            const bubble = renderServerDeliveredBackgroundReply(data) || existingServerDeliveredReply(data.text || '');" in _HTML
    assert "if (!bubble) addMessage('bot', data.text || '', false, { artifacts });\n          }" in _HTML


def test_web_server_delivered_background_reply_skips_frontend_history_resave() -> None:
    start = _HTML.index("function renderServerDeliveredBackgroundReply(data)")
    end = _HTML.index("function typingIndicatorHtml", start)
    body = _HTML[start:end]

    assert "_messageMetadataByTurn.set(turnId, { artifacts: artifacts || [] });" in body
    assert "_skipSaveByTurn.set(turnId, true);" in body
    assert "const finalized = finalizeBotMsg(text, false, turnId);" in body
    assert "finishTurnUi(turnId);" in body
    assert "addMessage('bot'" not in body


def test_grouped_mini_agent_activity_updates_existing_task_card_instead_of_new_bubble() -> None:
    start = _HTML.index("function updateMiniAgentActivityForGroup(groupId, turnId = null)")
    end = _HTML.index("function updateMiniAgentTaskActivity(data)", start)
    body = _HTML[start:end]

    assert "const { wrap, items } = groupedActivityTarget(groupId, effectiveTurnId);" in body
    assert "if (wrap && wrap.isConnected) {" in body
    assert "updateRunActivityInWrap(wrap, event, items || new Map());" in body
    assert "currentActivityItems" not in body
    assert "if (effectiveTurnId && _botTurnBubbles.has(effectiveTurnId)) {" in body
    assert body.index("updateRunActivityInWrap(wrap, event, items || new Map());") < body.index("updateRunActivity(event);")


def test_task_status_card_drops_stale_activity_when_group_changes() -> None:
    start = _HTML.index("function updateTaskStatusCard(data)")
    end = _HTML.index("function activityEnabledForTurn", start)
    body = _HTML[start:end]

    assert "const keepGroupActivity = existingGroupId === groupId;" in body
    assert "const staleActivity = detachRunActivity(bubble);" in body
    assert "if (staleActivity) staleActivity.remove();" in body
    assert "const mergedText = keepGroupActivity ? mergeTaskStatusText(previousText, text) : String(text || '');" in body


def test_web_builder_notices_use_grouped_structured_marker() -> None:
    notice = web_app._append_web_builder_notice("Done", ["Track payment reminders", "Run requested cron"])

    assert notice == "Done\n\n::builder-skill::Track payment reminders\n::builder-skill::Run requested cron"
    assert "function renderBuilderNotice(skills)" in _HTML
    assert "const BUILDER_SKILL_MARKER = '::builder-skill::';" in _HTML
    assert '<div class="builder-notice">' in _HTML
    assert '<span class="builder-notice-icon">${NI.wrench()}</span>' in _HTML
    assert ".builder-notice {\n    margin-top: 13px;" in _HTML
    assert "font-size: 11px;" in _HTML
    assert "Builder learned a new skill:" not in inspect.getsource(web_app._append_web_builder_notice)


def test_resolved_approval_failure_state_wraps_without_squashing_title() -> None:
    assert ".approval-bubble.approval-resolved .approval-title { font-size: 12px; white-space: nowrap; }" in _HTML
    assert ".approval-bubble.approval-resolved .approval-state.visible" in _HTML
    assert "white-space: normal; overflow-wrap: anywhere;" in _HTML
    assert ".approval-bubble.approval-resolved .approval-state.visible { font-size: 12px; white-space: nowrap; }" not in _HTML


def test_decision_history_header_keeps_status_badge_on_one_line() -> None:
    assert "grid-template-columns: 24px minmax(0, 1fr) max-content" in _HTML
    assert ".decision-title {\n    min-width: 0;" in _HTML
    assert "letter-spacing: 0.06em; white-space: nowrap; border-radius: 999px;" in _HTML


def test_permission_and_decision_cards_render_targets_as_code_blocks() -> None:
    assert "function targetCodeBlockHtml(value, label = 'Target', extraClass = '')" in _HTML
    assert "Web access · ${target}" in _HTML
    assert "Web access · ${item.domain}" in _HTML
    assert "Domain access:" not in _HTML
    assert "target-code-block" in _HTML
    assert "target-code-value" in _HTML
    assert "${targetCodeBlockHtml(item.target, 'URL', 'control-code-block')}" in _HTML
    assert "${decisionDetailHtml(a)}" in _HTML
    assert ".control-code-block { margin: 8px 0 0 32px; }" in _HTML
    assert ".control-meta { color: var(--muted); font-size: 11px; line-height: 1.4; margin: 7px 0 0 32px;" in _HTML
    assert "function dashboardTimeLabel(raw)" in _HTML
    assert "const meta = `Allowed by Allow all${tool}`;" in _HTML
    assert "${whenLabel ? `<div class=\"control-time\">${escHtml(whenLabel)}</div>` : ''}" in _HTML


def test_delivery_receipts_panel_and_api(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    receipts = [
        {
            "channel": "telegram",
            "target_id": "123",
            "status": "failed",
            "attachment_count": 0,
            "attachment_required": True,
            "unavailable_attachment_count": 1,
            "error": "artifact_unavailable",
            "created_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    calls: list[dict[str, object]] = []

    def fake_receipts(*, limit=20, status=None, path=None):
        calls.append({"limit": limit, "status": status, "path": path})
        return receipts

    monkeypatch.setattr(web_app, "list_platform_delivery_receipts", fake_receipts)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).get("/api/deliveries?status=failed&limit=5")

    assert response.status_code == 200
    assert response.json()["deliveries"] == receipts
    assert calls[-1]["status"] == "failed"
    assert calls[-1]["limit"] == 5
    assert TestClient(app).get("/api/deliveries?status=nope").status_code == 400


def test_web_config_save_uses_langgraph_workflow() -> None:
    source = inspect.getsource(create_app)
    module_source = inspect.getsource(web_app)

    assert "class _WebConfigSaveState" in source
    assert "_compiled_web_config_save_graph" in source
    assert "StateGraph(_WebConfigSaveState)" in source
    assert "class _WebConfigModelTestState" in source
    assert "_compiled_web_config_model_test_graph" in source
    assert "StateGraph(_WebConfigModelTestState)" in source
    assert "class _WebConfigIntentState" in module_source
    assert "_compiled_web_config_intent_graph" in module_source
    assert "StateGraph(_WebConfigIntentState)" in module_source
    config_handler_source = inspect.getsource(web_app._handle_web_config_request)
    assert "what model" not in config_handler_source
    assert "which provider" not in config_handler_source


def test_web_config_request_uses_structured_action_or_model_plan(monkeypatch) -> None:
    import nullion.runtime_config as runtime_config

    monkeypatch.setattr(
        runtime_config,
        "current_runtime_config",
        lambda model_client=None: SimpleNamespace(
            provider="openai",
            model="gpt-test",
            web_access=True,
            browser_enabled=False,
            memory_enabled=True,
        ),
    )

    class ModelClient:
        def __init__(self, action: str, confidence: float) -> None:
            self.action = action
            self.confidence = confidence
            self.calls = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_web_config_action",
                        "input": {"action": self.action, "confidence": self.confidence},
                    }
                ]
            }

    explicit_client = ModelClient("none", 0.0)
    explicit = web_app._handle_web_config_request(
        "ordinary text",
        runtime=SimpleNamespace(),
        orchestrator=SimpleNamespace(model_client=explicit_client),
        config_action={"type": "runtime_config_summary"},
    )
    assert explicit.startswith("Current model config:")
    assert explicit_client.calls == []

    disabled_client = ModelClient("show_runtime_config", 0.91)
    disabled = web_app._handle_web_config_request(
        "¿qué configuración está activa?",
        runtime=SimpleNamespace(),
        orchestrator=SimpleNamespace(model_client=disabled_client),
    )
    assert disabled is None
    assert disabled_client.calls == []

    monkeypatch.setenv("NULLION_WEB_CONFIG_TEXT_CLASSIFIER_ENABLED", "1")
    classified_client = ModelClient("show_runtime_config", 0.91)
    classified = web_app._handle_web_config_request(
        "¿qué configuración está activa?",
        runtime=SimpleNamespace(),
        orchestrator=SimpleNamespace(model_client=classified_client),
    )
    assert "- Provider: openai" in classified
    assert classified_client.calls
    assert "what model" not in classified_client.calls[0]["system"].lower()

    low_confidence = web_app._handle_web_config_request(
        "plain chat",
        runtime=SimpleNamespace(),
        orchestrator=SimpleNamespace(model_client=ModelClient("show_runtime_config", 0.2)),
    )
    assert low_confidence is None


def test_web_history_restores_activity_from_metadata_not_visible_text() -> None:
    assert "metadata.activity" in _HTML
    assert "restoreRunActivityEvents" in _HTML
    assert "Activity\\\\s+Live" not in _HTML


def test_all_history_uses_local_dates_and_day_scoped_message_loads() -> None:
    assert "function _localDateKey(date)" in _HTML
    assert "const todayStr    = _localDateKey(new Date());" in _HTML
    assert "dateStr ? '?date=' + encodeURIComponent(dateStr) : ''" in _HTML
    assert "const todayStr    = new Date().toISOString().slice(0, 10);" not in _HTML


def test_web_dashboard_refresh_and_profile_placeholders_are_release_safe() -> None:
    assert 'placeholder="Your name"' in _HTML
    assert 'placeholder="jane@example.com"' not in _HTML
    assert 'placeholder="+1 555 000 0000"' not in _HTML
    assert "setInterval(() => fetch('/api/crons')" in _HTML
    assert "120000" in _HTML
    assert "setInterval(refreshDashboard, 30000);" in _HTML
    status_stream_source = inspect.getsource(create_app)
    assert "await asyncio.sleep(15.0)" in status_stream_source
    assert "status_changed" not in inspect.getsource(web_app.create_app).split("@app.get(\"/api/status/stream\")", 1)[1].split("@app", 1)[0]


def test_web_cron_lists_use_human_schedule_labels() -> None:
    assert "function cronScheduleLabel(job)" in _HTML
    assert "${escHtml(cronScheduleLabel(j))}" in _HTML
    assert "<code>${escHtml(j.schedule)}</code>" not in _HTML


def test_config_save_clears_disabled_provider_model_without_switching_active_chat(monkeypatch, tmp_path) -> None:
    import copy

    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    env_path = tmp_path / ".env"
    monkeypatch.setattr(web_app, "_find_env_path", lambda: env_path)
    monkeypatch.setenv("NULLION_MODEL_PROVIDER", "openrouter")
    monkeypatch.setenv("NULLION_MODEL", "qwen/qwen3.6-flash")
    monkeypatch.setenv("NULLION_OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
    saved_credentials = {
        "provider": "openrouter",
        "model": "qwen/qwen3.6-flash",
        "base_url": "https://openrouter.ai/api/v1",
        "models": {
            "gemini": "gpt-5.5",
            "openrouter": "qwen/qwen3.6-flash",
        },
        "providers_enabled": {
            "gemini": False,
            "openrouter": True,
        },
    }

    monkeypatch.setattr(web_app, "_read_credentials_json", lambda: copy.deepcopy(saved_credentials))

    def write_credentials(creds):
        saved_credentials.clear()
        saved_credentials.update(copy.deepcopy(creds))

    monkeypatch.setattr(web_app, "_write_credentials_json", write_credentials)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post(
        "/api/config",
        json={
            "model_provider": "gemini",
            "model_name": "",
            "model_provider_enabled": False,
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    saved = copy.deepcopy(saved_credentials)
    assert saved["provider"] == "openrouter"
    assert saved["model"] == "qwen/qwen3.6-flash"
    assert saved["base_url"] == "https://openrouter.ai/api/v1"
    assert saved["models"] == {"openrouter": "qwen/qwen3.6-flash"}
    assert saved["providers_enabled"]["gemini"] is False
    assert env_path.exists() is False
    assert web_app.os.environ["NULLION_MODEL_PROVIDER"] == "openrouter"
    assert web_app.os.environ["NULLION_MODEL"] == "qwen/qwen3.6-flash"
    assert web_app.os.environ["NULLION_OPENAI_BASE_URL"] == "https://openrouter.ai/api/v1"


def test_approval_card_javascript_rejects_internal_placeholder_details() -> None:
    assert "const placeholders = new Set([" in _HTML
    assert "'approval required'" in _HTML
    assert "Command details were not provided by the runtime." in _HTML


def test_web_allow_all_copy_is_unambiguous_about_domains() -> None:
    assert "Allow all web domains" in _HTML
    assert "Allow requests to any web domain for this scope" in _HTML
    assert "All web domains" in _HTML
    assert "Allow globally" not in _HTML
    assert "Global allow" not in _HTML


def test_connection_form_rejects_env_var_names_in_base_url_field() -> None:
    assert "Connector base URL must start with http:// or https://." in _HTML
    assert "Put env var names in the credential reference field." in _HTML
    assert 'env_name.endswith("_BASE_URL")' in inspect.getsource(create_app)
    assert "must be an http:// or https:// URL" in inspect.getsource(create_app)


def test_users_tab_exposes_member_edit_and_remove_controls() -> None:
    assert "Workspace ID" in _HTML
    assert "Messaging identity" in _HTML
    assert "Block terminal execution" in _HTML
    assert "function setUserWorkspaceId(index, value)" in _HTML
    assert "function setUserBlockedTools(index, value)" not in _HTML
    assert "function setUserTerminalBlocked(index, blocked)" in _HTML
    assert "function removeUserMember(index)" in _HTML
    assert "Member removed. Save changes to apply." in _HTML


def test_connections_provider_dropdown_uses_skill_auth_metadata() -> None:
    assert "Choose the account, email bridge, or connector gateway" in _HTML
    assert "Generic connector / MCP gateway" in _HTML
    assert "let skillAuthProviders = [];" in _HTML
    assert "renderConnectionProviderOptions(cfg.installed_skill_packs, cfg.skill_auth_providers);" in _HTML
    assert "function registerSkillAuthProviders(authProviders)" in _HTML
    assert "const uniqueProviders = [];" in _HTML
    assert '<option value="maton_connector_provider">' not in _HTML
    assert "DEFAULT_CONNECTION_PROVIDER_IDS" not in _HTML


def test_connections_ui_generic_connector_is_workspace_scoped_and_editable() -> None:
    assert "Use this for an MCP bridge, API gateway, automation API, webhook bridge" in _HTML
    assert "defaultCredentialRefForProvider(providerId, workspaceId, credentialScope, label = '')" in _HTML
    assert "return `NULLION_${workspaceKey}_${prefix}_API_KEY`;" in _HTML
    assert "providerInfo.kind === 'connector'" in _HTML
    assert "Saved on this workspace connection so each workspace can use its own gateway endpoint." in _HTML
    assert "provider_profile: (isCustomApi || isConnector)" in _HTML
    assert "_env_updates: Object.keys(envUpdates).length ? envUpdates : (existing?._env_updates || null)" in _HTML
    assert "editProviderInfo.kind === 'connector'" in _HTML
    assert "Check connector setup" in _HTML
    assert "maton_connector_provider" not in _HTML
    assert "Test Maton" not in _HTML
    assert "if (apiBaseUrl && providerInfo.baseEnv && !isCustomApi) envUpdates[providerInfo.baseEnv]" not in _HTML


def test_connection_setup_helpers_use_clear_provider_specific_copy() -> None:
    assert "Enter only that profile name here." in _HTML
    assert "Use this only when your bridge implements Nullion email endpoints" in _HTML
    assert "For a general API gateway, MCP bridge, automation tool, or non-email service" in _HTML
    assert "Use this for a mailbox that exposes IMAP for reading and SMTP for sending." in _HTML
    assert "Pick the workspace that should own this connector credential." in _HTML
    assert "Pick the workspace that should own this provider credential." in _HTML
    assert "broader connector skills should use one of the connector options below" not in _HTML


def test_generic_connector_setup_check_validates_reference_without_network(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_ADMIN_ACME_CRM_API_KEY", "secret-token")
    monkeypatch.setattr(
        "nullion.web_app._find_provider_connection_from_payload",
        lambda body, provider_id: SimpleNamespace(
            provider_id=provider_id,
            provider_profile="https://gateway.example.test",
            credential_ref="NULLION_WORKSPACE_ADMIN_ACME_CRM_API_KEY",
        ),
    )

    result = web_app._provider_connection_test_details(
        {
            "provider_id": "custom_connector_provider",
            "provider_profile": "https://gateway.example.test",
            "credential_ref": "NULLION_WORKSPACE_ADMIN_ACME_CRM_API_KEY",
        }
    )

    assert result["ok"] is True
    assert result["provider_id"] == "custom_connector_provider"
    assert result["base_url"] == "https://gateway.example.test"
    assert result["credential_ref"] == "NULLION_WORKSPACE_ADMIN_ACME_CRM_API_KEY"
    assert "Connector setup looks ready" in result["summary"]


def test_connections_ui_requires_confirmation_for_shared_admin_credentials() -> None:
    assert 'id="new-connection-scope"' in _HTML
    assert "Admin shared across workspaces" in _HTML
    assert "Share this admin credential?" in _HTML
    assert "workspaceId = 'workspace_admin';" in _HTML
    assert "credential_scope: credentialScope" in _HTML
    assert "Shared by admin" in _HTML
    assert "confirmAction({" in _HTML


def test_connections_ui_allows_changing_connector_permissions() -> None:
    assert 'id="new-connection-permission-mode"' in _HTML
    assert "Read-only requests" in _HTML
    assert "Read + write requests" in _HTML
    assert "permission_mode: permissionEl && permissionEl.value === 'write' ? 'write' : 'read'" in _HTML
    assert "async function setWorkspaceConnectionPermission(index, mode)" in _HTML
    assert "Allow connector writes?" in _HTML
    assert "window.confirm" not in _HTML


def test_connections_ui_can_edit_and_test_imap_connections() -> None:
    assert 'id="connection-test-btn"' in _HTML
    assert 'id="connection-submit-btn"' in _HTML
    assert "function updateConnectionFormActions()" in _HTML
    assert "async function editWorkspaceConnection(index)" in _HTML
    assert "function cancelConnectionEdit()" in _HTML
    assert "async function testImapConnection(index = null)" in _HTML
    assert "/api/connections/imap-settings" in _HTML
    assert "/api/connections/test-imap" in _HTML
    assert "Update connection" in _HTML
    assert "Test IMAP" in _HTML


def test_settings_save_errors_name_the_failed_endpoint() -> None:
    assert "friendlyFetchFailure('Preferences save', '/api/preferences'" in _HTML
    assert "friendlyFetchFailure('Connections save', '/api/connections'" in _HTML
    assert "friendlyFetchFailure('Settings save', '/api/config'" in _HTML
    assert "could not reach ${url}" in _HTML


def test_web_restart_sends_gateway_restart_notice_before_exiting(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import desktop_entrypoint, gateway_notifications, web_app
    from nullion.tools import ToolRegistry

    calls: list[dict[str, object]] = []

    class Event:
        def to_dict(self):
            return {
                "event_id": "restart-1",
                "kind": "restarting",
                "text": "Nullion gateway is restarting",
                "created_at": "2026-04-30T12:00:00Z",
            }

    def fake_begin_gateway_restart(**kwargs):
        calls.append(kwargs)
        return Event()

    monkeypatch.setattr(gateway_notifications, "begin_gateway_restart", fake_begin_gateway_restart)
    monkeypatch.setattr(desktop_entrypoint, "schedule_desktop_reload", lambda **kwargs: None)
    monkeypatch.setattr(web_app, "_restart_non_web_services", lambda: "chat restarted")
    monkeypatch.setattr(web_app, "_schedule_process_restart", lambda: None)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/restart")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["gateway_event"]["kind"] == "restarting"
    assert calls and calls[0]["async_delivery"] is False


def test_chat_services_restart_sends_gateway_restart_notice(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import gateway_notifications, web_app
    from nullion.tools import ToolRegistry

    calls: list[dict[str, object]] = []

    class Event:
        def to_dict(self):
            return {
                "event_id": "chat-restart-1",
                "kind": "restarting",
                "text": "Nullion gateway is restarting",
                "created_at": "2026-04-30T12:00:00Z",
            }

    def fake_begin_gateway_restart(**kwargs):
        calls.append(kwargs)
        return Event()

    monkeypatch.setattr(gateway_notifications, "begin_gateway_restart", fake_begin_gateway_restart)
    monkeypatch.setattr(web_app, "_restart_chat_services", lambda **kwargs: "chat restarted")
    monkeypatch.setattr(web_app, "_chat_services_status_payload", lambda: [])

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/chat-services/restart")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert calls and calls[0]["async_delivery"] is False


def test_update_restart_shows_restart_notice_without_waiting_for_websocket() -> None:
    assert "function showGatewayNotice(event)" in _HTML
    assert "shownGatewayEventIds.has(eventId)" in _HTML
    assert "localStorage.setItem('nullion_gateway_event_id', eventId);" in _HTML
    assert "} else if (data.type === 'gateway_notice') {\n        showGatewayNotice(data);" in _HTML
    assert "if (payload.gateway_event) showGatewayNotice(payload.gateway_event);" in _HTML
    local_notice = "showGatewayNotice({kind: 'restarting', text: '🟡 Nulliøn gateway is restarting. Chat may pause for a moment.'});"
    assert local_notice in _HTML
    assert _HTML.index(local_notice) < _HTML.index("const response = await fetch('/api/restart', { method: 'POST' });")


def test_settings_close_warns_about_unsaved_changes() -> None:
    assert "function settingsStateSnapshot()" in _HTML
    assert "function settingsHaveUnsavedChanges()" in _HTML
    assert "async function closeSettings()" in _HTML
    assert "Discard unsaved settings?" in _HTML
    assert "You have unsaved settings changes. Close Settings and lose those changes?" in _HTML
    assert "confirmText: 'Discard changes'" in _HTML
    assert "cancelText: 'Keep editing'" in _HTML
    assert "if (!discard) return;" in _HTML


def test_settings_dirty_state_is_tracked_and_reset_after_save() -> None:
    assert "let settingsBaselineSnapshot = '';" in _HTML
    assert "let settingsDirtyExplicit = false;" in _HTML
    assert "let settingsSnapshotReady = false;" in _HTML
    assert "refreshSettingsBaseline();" in _HTML
    assert "await refreshSettingsBaseline({force: true});" in _HTML
    assert "addEventListener('input', markSettingsDirty, true)" in _HTML
    assert "addEventListener('change', markSettingsDirty, true)" in _HTML
    assert "event.target.closest('.pref-chip')" in _HTML
    assert "markSettingsDirty();" in _HTML


def test_doctor_diagnose_button_posts_and_refreshes_existing_status_surfaces() -> None:
    assert "async function runDoctorDiagnose()" in _HTML
    assert "fetch('/api/doctor/diagnose', { method: 'POST' })" in _HTML
    assert "await loadBuilderDoctorSettingsSummary({force: true});" in _HTML
    assert "await refreshDashboard();" in _HTML
    assert "pollStatus" not in _HTML
    assert "API('/api/doctor/diagnose', { method: 'POST' })" not in _HTML


def test_doctor_action_results_are_visible_in_chat_not_only_card_metadata() -> None:
    update_source = _HTML.split("async function updateDoctorItem", 1)[1].split("function doctorItemHtml", 1)[0]
    assert "addAssistantNotice(action === 'repair' ? 'Safe fix applied' : 'Doctor action result'" in update_source
    assert "kind === 'doctor'" in update_source
    assert "action === 'repair'" in update_source


def test_builder_doctor_settings_use_lightweight_summary_endpoint() -> None:
    summary_source = _HTML.split("async function loadBuilderDoctorSettingsSummary", 1)[1].split("async function", 1)[0]
    assert "API('/api/settings/summary')" in summary_source
    assert "API('/api/status?full=1')" not in summary_source
    assert "options.force !== true" in summary_source
    assert "loadBuilderDoctorSettingsSummary({force: true})" in _HTML


def test_settings_save_and_lazy_tabs_do_not_force_full_status_on_open() -> None:
    assert "let preferencesLoaded = false;" in _HTML
    assert "let profileSettingsLoaded = false;" in _HTML
    assert "let usersTabLoaded = false;" in _HTML
    assert "let connectionsTabLoaded = false;" in _HTML
    assert "loadConfig({skipSettingsApplyIfDirty: !force})" in _HTML
    assert "if (name === 'preferences') loadPreferences();" in _HTML
    assert "loadConnectionsTab();" in _HTML
    assert "const prefsSave = preferencesLoaded ? savePreferences()" in _HTML
    assert "const usersSave = usersTabLoaded ? saveUsers()" in _HTML
    assert "const connectionsSave = connectionsTabLoaded ? saveConnections()" in _HTML
    assert "API('/api/config/header')" in _HTML
    assert "_lastHeaderConfigPayload = null;" in _HTML
    assert "clear_media_selections: Array.from(_clearMediaSelections)" in _HTML


def test_doctor_settings_expose_smart_cleanup_default_toggle() -> None:
    assert 'id="cfg-smart-cleanup-enabled" checked' in _HTML
    assert "cfg.smart_cleanup_enabled !== false" in _HTML
    assert "smart_cleanup_enabled: document.getElementById('cfg-smart-cleanup-enabled').checked" in _HTML
    assert "Automatically remove invalid Doctor records, merge duplicate Builder learnings" in _HTML


def test_doctor_settings_expose_data_cleanup_controls() -> None:
    assert "Stored data retention" in _HTML
    assert "Chat history" in _HTML
    assert "Builder learnings" in _HTML
    assert "Doctor logs" in _HTML
    assert 'value="chat" checked onchange="updateDataRetentionTypeCards()"' in _HTML
    assert "checked disabled" not in _HTML
    assert '<option value="">Never</option>' in _HTML
    assert '<option value="15">15 days</option>' in _HTML
    assert '<option value="60">60 days</option>' in _HTML
    assert "data_retention_types: dataRetentionTypesPayloadValue()" in _HTML
    assert "async function runDoctorCleanupNow()" in _HTML
    assert "data_retention_days: dataRetentionDaysPayloadValue()" in _HTML
    assert "body: JSON.stringify(payload)" in _HTML
    assert "Cleanup complete" in _HTML
    assert "bytes_freed" in _HTML
    assert 'class="doctor-action-row"' in _HTML
    assert "Clean up now" in _HTML
    assert "Enable auto clean" in _HTML


def test_doctor_cleanup_endpoint_uses_visible_retention_payload(tmp_path, monkeypatch) -> None:
    from datetime import UTC, datetime, timedelta

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.delenv("NULLION_DATA_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("NULLION_DATA_RETENTION_TYPES", raising=False)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_conversation_event({
        "conversation_id": "web:operator",
        "event_type": "conversation.chat_turn",
        "created_at": (datetime.now(UTC) - timedelta(days=8)).isoformat(),
        "user_message": "old",
        "assistant_reply": "old",
    })
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())

    payload = TestClient(app).post(
        "/api/doctor/cleanup",
        json={"data_retention_days": 7, "data_retention_types": ["chat"]},
    ).json()

    assert payload["ok"] is True
    assert payload["removed"]["chat_events"] == 1
    assert payload["applied_retention_days"] == 7
    assert payload["applied_retention_types"] == ["chat"]
    assert "before_storage" in payload
    assert "bytes_freed" in payload
    assert runtime.store.conversation_events == []
    assert os.environ.get("NULLION_DATA_RETENTION_DAYS") is None


def test_web_verbose_mode_has_no_planner_card_toggle() -> None:
    assert 'id="verbose-mode-btn"' in _HTML
    assert 'id="verbose-mode-label">Verbose: Off</span>' in _HTML
    assert 'function cycleVerboseMode()' in _HTML
    assert "const next = current === 'on' ? 'off' : 'on';" in _HTML
    assert 'id="cfg-verbose-enabled"' in _HTML
    assert "activity_trace:   document.getElementById('cfg-verbose-enabled').checked" in _HTML
    assert "body: JSON.stringify({ activity_trace: cfg.activity_trace })" in _HTML
    assert 'id="cfg-verbose-mode"' not in _HTML
    assert 'id="planner-feed-btn"' not in _HTML
    assert 'id="cfg-task-planner-feed-enabled"' not in _HTML
    assert 'id="cfg-task-planner-feed-mode"' not in _HTML
    assert 'id="activity-trace-toggle"' not in _HTML


def test_web_verbose_mode_invalidates_cached_header_config_after_save() -> None:
    assert "function invalidateConfigCaches()" in _HTML
    assert "async function loadHeaderConfig(options = {})" in _HTML
    assert "fetchHeaderConfig({force: options.force === true})" in _HTML
    verbose_block = _HTML[_HTML.index("async function setVerboseMode(mode)") : _HTML.index("async function cycleVerboseMode()")]
    assert "invalidateConfigCaches();" in verbose_block
    assert "await loadHeaderConfig({force: true});" in verbose_block
    activity_block = _HTML[_HTML.index("async function toggleActivityTrace(force = null)") : _HTML.index("async function launchAgentBrowser()")]
    assert "invalidateConfigCaches();" in activity_block
    assert "await loadHeaderConfig({force: true});" in activity_block


def test_web_mini_agent_final_delivery_broadcasts_with_task_planner_mode(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.tools import ToolRegistry

    class CapturingOrchestrator:
        deliver_fn = None

        def set_deliver_fn(self, deliver_fn):
            self.deliver_fn = deliver_fn

    class FakeChatStore:
        def __init__(self) -> None:
            self.saved_messages = []

        def save_message(self, *args, **kwargs):  # noqa: ANN002, ANN003
            self.saved_messages.append((args, kwargs))
            return len(self.saved_messages)

    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    fake_store = FakeChatStore()
    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: fake_store)
    orchestrator = CapturingOrchestrator()
    app = create_app(SimpleNamespace(store=None), orchestrator=orchestrator, registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            assert orchestrator.deliver_fn is not None
            orchestrator.deliver_fn("web:operator", "Final delegated answer")

            data = websocket.receive_json()

    assert data["type"] == "background_message"
    assert data["conversation_id"] == "web:operator"
    assert data["text"] == "Final delegated answer"
    assert fake_store.saved_messages == [
        (("web:operator", "bot", "Final delegated answer"), {"is_error": False})
    ]


def test_web_mini_agent_artifact_delivery_broadcasts_download_card(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    class CapturingOrchestrator:
        deliver_fn = None

        def set_deliver_fn(self, deliver_fn):
            self.deliver_fn = deliver_fn

    class FakeChatStore:
        def __init__(self) -> None:
            self.saved_messages = []

        def save_message(self, *args, **kwargs):  # noqa: ANN002, ANN003
            self.saved_messages.append((args, kwargs))
            return len(self.saved_messages)

    artifact = {
        "path": "/tmp/report.pdf",
        "url": "/api/artifacts/report",
        "name": "report.pdf",
        "media_type": "application/pdf",
    }
    fake_store = FakeChatStore()
    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: fake_store)
    monkeypatch.setattr(web_app, "_web_artifact_descriptors", lambda runtime, paths, principal_id=None: [artifact])
    orchestrator = CapturingOrchestrator()
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=orchestrator, registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            assert orchestrator.deliver_fn is not None
            orchestrator.deliver_fn("web:operator", "/tmp/report.pdf", is_artifact=True)

            data = websocket.receive_json()

    assert data["type"] == "background_message"
    assert data["conversation_id"] == "web:operator"
    assert data["text"] == "Attached the requested file."
    assert data["artifacts"] == [artifact]
    assert fake_store.saved_messages == [
        (
            ("web:operator", "bot", "Attached the requested file."),
            {"is_error": False, "metadata": {"artifacts": [artifact]}},
        )
    ]
    events = list(runtime.store.list_recent_conversation_events("web:operator", event_type="conversation.chat_turn", limit=5))
    assert events
    assert events[-1]["assistant_reply"] == "Attached the requested file."
    assert events[-1]["artifacts"] == [artifact]
    delivery_results = [
        result
        for result in events[-1]["tool_results"]
        if result.get("tool_name") == "artifact_delivery"
    ]
    assert delivery_results
    assert delivery_results[-1]["status"] == "completed"
    assert delivery_results[-1]["output"]["artifact_path"] == "/tmp/report.pdf"


def test_web_mini_agent_artifact_delivery_refuses_invalid_paths(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.tools import ToolRegistry

    class CapturingOrchestrator:
        deliver_fn = None

        def set_deliver_fn(self, deliver_fn):
            self.deliver_fn = deliver_fn

    class FakeChatStore:
        def __init__(self) -> None:
            self.saved_messages = []

        def save_message(self, *args, **kwargs):  # noqa: ANN002, ANN003
            self.saved_messages.append((args, kwargs))
            return len(self.saved_messages)

    fake_store = FakeChatStore()
    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: fake_store)
    monkeypatch.setattr(web_app, "_web_artifact_descriptors", lambda runtime, paths, principal_id=None: [])
    orchestrator = CapturingOrchestrator()
    app = create_app(SimpleNamespace(store=None), orchestrator=orchestrator, registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            assert orchestrator.deliver_fn is not None
            orchestrator.deliver_fn("web:operator", "/tmp/report.pdf", is_artifact=True)

            data = websocket.receive_json()

    assert data["type"] == "background_message"
    assert data["conversation_id"] == "web:operator"
    assert "couldn't attach `report.pdf`" in data["text"]
    assert "downloadable workspace" in data["text"]
    assert "/tmp/report.pdf" not in data["text"]
    assert "artifacts" not in data
    assert fake_store.saved_messages == [
        (
            ("web:operator", "bot", data["text"]),
            {"is_error": True},
        )
    ]


def test_web_download_click_does_not_bubble_into_chat_controls() -> None:
    assert "event.stopPropagation();" in _HTML
    assert "renderServerDeliveredBackgroundReply(data)" in _HTML


def test_delegated_runs_do_not_mark_writing_response_done_early() -> None:
    assert 'if not result.get("mini_agent_dispatch"):' in _SOURCE
    dispatch_branch = _SOURCE[
        _SOURCE.index('if dispatch_result is not None and getattr(dispatch_result, "dispatched", True):'):
        _SOURCE.index('try:\n        streaming_safe = (')
    ]
    assert '"mini-agents", "Mini-Agents", "running"' in dispatch_branch
    assert '"respond", "Writing response"' not in dispatch_branch


def test_task_status_cards_use_structured_checklist_renderer() -> None:
    assert "function renderTaskStatusText(text)" in _HTML
    assert "function mergeTaskStatusText(previousText, nextText)" in _HTML
    assert "taskStatusRank(previousGlyph) > taskStatusRank(row.glyph)" in _HTML
    assert "bubble.innerHTML = renderTaskStatusText(mergedText);" in _HTML
    assert ".task-status-row {\n    display: grid; grid-template-columns: 14px minmax(0, 1fr); gap: 8px; align-items: start;\n    margin-left: 18px;" in _HTML
    assert ".task-status-icon {\n    width: 12px; height: 12px;" in _HTML
    assert "const states = {" in _HTML
    assert "'☐': {cls: 'pending', symbol: '☐'}" in _HTML
    assert "'▣': {cls: 'pending', symbol: '☐'}" in _HTML
    assert "symbol: '!'" not in _HTML
    assert "symbol: ''" not in _HTML
    assert "'☑': {cls: 'complete', symbol: '☑'}" in _HTML


def test_task_status_cards_use_compact_trace_typography() -> None:
    assert ".task-status-card {\n    display: flex; flex-direction: column; gap: 6px;\n    font: 12px/1.35 -apple-system" in _HTML
    assert "task-status-title-icon" in _HTML
    assert "color: var(--green); font-size: 12px" in _HTML
    assert "color: var(--text); font-size: 12px; font-weight: 600; line-height: 1.35;" in _HTML
    assert "const titleIcon = allTerminal ? '✓' : '◐';" in _HTML
    assert ".task-status-list {\n    display: flex; flex-direction: column; gap: 2px;" in _HTML
    assert "color: var(--muted); font-size: 11px; line-height: 1.35;" in _HTML
    assert ".task-status-icon {\n    width: 12px; height: 12px;" in _HTML
    assert "'✕': {cls: 'failed', symbol: '✕'}" in _HTML
    assert "'◐': {cls: 'running', symbol: '◐'}" in _HTML


def test_composer_attachment_control_lives_after_textarea() -> None:
    input_row = _HTML[_HTML.index('<div id="input-row">') : _HTML.index('<div id="attachments-bar"')]

    assert input_row.index('id="composer-clear-btn"') < input_row.index('id="user-input"')
    assert input_row.index('id="user-input"') < input_row.index('id="attach-btn"')
    assert input_row.index('id="attach-btn"') < input_row.index('id="composer-mode"')
    assert 'id="user-input" placeholder="Ask Nullion, paste or drag an image, or type / for commands…" rows="1" autofocus' in input_row
    assert '#input-row {\n    padding: 12px 16px; border-top: 1px solid var(--border);\n    display: grid; grid-template-columns: auto auto minmax(0, 1fr) auto auto auto;' in _HTML


def test_composer_accepts_clipboard_images_and_refocuses() -> None:
    assert "function focusComposer({ force = false } = {})" in _HTML
    assert "function addPendingFiles(files)" in _HTML
    assert "window.addEventListener('DOMContentLoaded', () => setTimeout(() => focusComposer(), 0));" in _HTML
    assert "window.addEventListener('focus', () => setTimeout(() => focusComposer(), 0));" in _HTML
    assert "document.getElementById('user-input').addEventListener('paste', (event) => {" in _HTML
    assert "item.kind !== 'file' || !String(item.type || '').startsWith('image/')" in _HTML
    assert "`clipboard-image-${new Date().toISOString().replace(/[:.]/g, '-')}.${extension}`" in _HTML
    assert "addPendingFiles(pastedFiles);" in _HTML
    assert "const chatDropZone = document.getElementById('chat-panel');" in _HTML
    assert "chatDropZone.addEventListener('drop', (event) => {" in _HTML
    assert "const files = Array.from(event.dataTransfer?.files || []);" in _HTML
    assert "addPendingFiles(files);" in _HTML
    assert "#input-wrap.drag-over #input-row" in _HTML
    assert '<div id="drop-overlay" aria-hidden="true">Drop file to attach</div>' in _HTML
    assert "#drop-overlay {\n    position: fixed;" in _HTML
    assert "#chat-panel.drag-over #drop-overlay { display: flex; }" in _HTML
    assert "const dragHasFiles = (dataTransfer) => {" in _HTML
    assert "Array.from(dataTransfer.types || []).includes('Files')" in _HTML
    assert "const rect = chatDropZone.getBoundingClientRect();" in _HTML
    assert "overlay.style.height = `${Math.max(0, Math.round(rect.height - 32))}px`;" in _HTML
    assert "focusComposer({ force: true });" in _HTML
    assert "Upload failed:" in _HTML
    assert "Attachment upload failed." in _HTML


def test_settings_expose_mini_agent_iteration_and_continuation_limits() -> None:
    source = inspect.getsource(create_app)

    assert 'id="cfg-mini-agent-max-iterations"' in _HTML
    assert 'id="cfg-mini-agent-max-continuations"' in _HTML
    assert "cfg.mini_agent_max_iterations || 24" in _HTML
    assert "mini_agent_max_iterations: Number(document.getElementById('cfg-mini-agent-max-iterations').value || 24)" in _HTML
    assert "cfg.mini_agent_max_continuations ?? 1" in _HTML
    assert "mini_agent_max_continuations: Number(document.getElementById('cfg-mini-agent-max-continuations').value || 1)" in _HTML
    assert '"mini_agent_max_continuations": mini_agent_continuations' in source
    assert '("mini_agent_max_continuations", "NULLION_MINI_AGENT_MAX_CONTINUATIONS", 0)' in source


def test_set_default_chat_model_action_lives_with_chat_model_default_strip() -> None:
    button = '<button class="card-btn secondary" type="button" onclick="forceModelToAllSessions()">Set default</button>'
    default_row = '<div class="model-default-row">'
    default_label = '<span class="ams-label">Current:</span>'
    test_button = 'id="model-test-btn"'

    assert _HTML.count(button) == 1
    assert _HTML.index(default_row) < _HTML.index(button) < _HTML.index(default_label)
    assert _HTML.index(button) < _HTML.index(default_label)
    assert _HTML.index(button) > _HTML.index('id="cfg-model-name"')
    assert _HTML.index(test_button) < _HTML.index(button)


def test_set_default_chat_model_endpoint_uses_live_hot_swap_helper() -> None:
    source = inspect.getsource(create_app)
    force_start = source.index('@app.post("/api/config/model-force")')
    force_end = source.index('@app.delete("/api/config/model-force")', force_start)
    force_body = source[force_start:force_end]
    clear_end = source.index('@app.post("/api/config/oauth/disconnect")', force_end)
    clear_body = source[force_end:clear_end]

    assert "def _hot_swap_live_model_client(" in source
    assert 'reason="settings save"' in source
    assert 'reason="default model change"' in force_body
    assert "provider=model_provider or None" in force_body
    assert "model_name=model_name" in force_body
    assert 'reason="default model clear"' in clear_body


def test_header_model_switcher_only_lists_enabled_provider_models() -> None:
    start = _HTML.index("function renderHeaderConfig(cfg)")
    end = _HTML.index("async function switchHeaderProvider()", start)
    body = _HTML[start:end]

    provider_ids_block = body[body.index("const providerIds = Array.from(new Set(["):body.index("])).filter(Boolean);")]
    assert "...Object.keys(providerModels)" in provider_ids_block
    assert "...Object.keys(providersEnabled)" in provider_ids_block
    assert "...Object.keys(providersConfigured)" not in provider_ids_block
    assert "const providerIsEnabled = id => hasEnabledFlag(id)" in body
    assert "? providersEnabled[id] === true" in body
    assert "const providerHasModels = id =>" in body
    assert "splitModelEntries(providerModels[id]).length > 0" in body
    assert "providerIsEnabled(id) && providerHasModels(id) && providerIsUsable(id)" in body


def test_web_approval_card_uses_explicit_web_flag_for_id_only_details() -> None:
    assert "Boolean(data.is_web_request)" in _HTML
    assert "Boolean(msg.is_web_request)" in _HTML
    assert "Boolean(resume.is_web_request)" in _HTML
    assert (
        "function approvalActionsHtml(approvalId, toolName, detail, "
        "sessionAllowLabel = 'for all workspaces', forceWeb = false, requestKind = '')"
    ) in _HTML
    assert "const isWeb = forceWeb || approvalIsWebRequest(toolName, detail);" in _HTML
    assert "haystack.includes('fetch a web page')" in _HTML
    assert "haystack.includes('allow web access')" in _HTML


def test_web_approval_display_payload_marks_web_tool_results_even_without_url_detail() -> None:
    class Runtime:
        store = None

    class Result:
        approval_id = "f9da193f-0000-0000-0000-000000000000"
        tool_results = []

    from nullion.tools import ToolResult

    Result.tool_results = [
        ToolResult(
            invocation_id="inv-web",
            tool_name="web_fetch",
            status="denied",
            output={
                "reason": "approval_required",
                "boundary_kind": "outbound_network",
                "target": "https://www.google.com/",
            },
        )
    ]

    label, detail, trigger, is_web_request = _approval_display_from_turn_result(Runtime(), Result())

    assert label == "fetch a web page"
    assert detail == "https://www.google.com/"
    assert trigger is None
    assert is_web_request is True


def test_web_approval_cards_show_tool_context_and_stale_clicks_do_not_say_denied() -> None:
    assert "Tool: ${escHtml(tool)}" in _HTML
    assert "data.stale" in _HTML
    assert "Approval expired" in _HTML
    assert "That approval is no longer pending. I refreshed approvals." in _HTML


def test_web_approval_resume_returns_new_structured_approval_when_delegated_task_pauses_again(tmp_path) -> None:
    from datetime import UTC, datetime

    from fastapi.testclient import TestClient

    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    approval = create_approval_request(
        requested_by="web:operator",
        action="use_tool",
        resource="web_fetch",
        request_kind="tool",
        context={"tool_name": "web_fetch"},
    )
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="web:operator",
            chat_id="web:operator",
            message="open the approved URL",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
            task_id="delegated-task",
        )
    )

    class Orchestrator:
        def resume_paused_task_sync(self, **kwargs):  # noqa: ANN003
            assert kwargs["task_id"] == "delegated-task"
            return SimpleNamespace(
                status="partial",
                output="Approval required before the delegated task can continue.",
                resume_token={"approval_id": "approval-next"},
            )

    app = create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry())
    response = TestClient(app).post(f"/api/approve/{approval.approval_id}", json={"mode": "once"})

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["resume"] == {
        "type": "approval_required",
        "approval_id": "approval-next",
        "tool_name": "delegated task",
        "tool_detail": "Approval required before the delegated task can continue.",
        "is_web_request": True,
        "text": "Approval required before the delegated task can continue.",
    }
    assert runtime.store.get_suspended_turn(approval.approval_id) is None


def test_web_approval_resumes_suspended_turn_when_approval_row_is_gone(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    approval_id = "approval-row-already-consumed"
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval_id,
            conversation_id="web:operator",
            chat_id="web:operator",
            message="send the approved email package",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
            messages_snapshot=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "send the approved email package"}],
                }
            ],
        )
    )

    class Orchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            assert kwargs["conversation_id"] == "web:operator"
            return SimpleNamespace(
                final_text="Resumed and sent the approved email.",
                tool_results=[],
                artifacts=[],
                reached_iteration_limit=False,
                suspended_for_approval=False,
                turn_id="turn-resumed-after-consumed-approval",
            )

    app = create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry())
    response = TestClient(app).post(f"/api/approve/{approval_id}", json={"mode": "once"})

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["already_handled"] is True
    assert data["resume"]["text"] == "Resumed and sent the approved email."
    assert runtime.store.get_suspended_turn(approval_id) is None


def test_task_cards_expose_kill_controls_and_doctor_cleanup_copy() -> None:
    assert "async function killTask(kind, id, title)" in _HTML
    assert "function killTaskFromButton(button)" in _HTML
    assert "/api/tasks/frame/${encodeURIComponent(id)}/kill" in _HTML
    assert "/api/tasks/mini-agent/${encodeURIComponent(id)}/kill" in _HTML
    assert "Stop this task and clear it from active work." in _HTML
    assert "pendingCardActions" in _HTML
    assert "aria-busy=\"true\"" in _HTML
    assert "alert(`Could not kill task:" not in _HTML
    assert "alert(`Update failed:" not in _HTML
    assert ".task-card {\n    display: grid;" in _HTML
    assert "function formatElapsedSince(raw, now = Date.now())" in _HTML
    assert 'class="task-elapsed" data-elapsed-since=' in _HTML
    assert "setInterval(updateElapsedCounters, 1000);" in _HTML
    assert "taskCardMetaHtml(t)" in _HTML
    assert "taskActionButtonHtml(t.killKind, t.id, t.title)" in _HTML
    assert "const killKind = isMiniAgent ? 'mini-agent' : (isCronDelivery ? 'cron-delivery' : 'frame');" in _HTML
    assert "taskActionButtonHtml(killKind, id, title)" in _HTML
    assert "/api/tasks/cron-delivery/${encodeURIComponent(id)}/kill" in _HTML
    assert 'data-task-title="${escAttr(title)}"' in _HTML
    assert "onclick=\"killTaskFromButton(this)\"" in _HTML
    assert "onclick=\"killTask('${escAttr(kind)}','${escAttr(id)}','${escAttr(title)}')\"" not in _HTML
    assert '<div class="control-actions"><button class="mini-btn danger" title="Stop this task and clear it from active work."' not in _HTML
    assert "cleanup_dead_task_frame" in _HTML
    assert "Doctor cleared the stale task frame" in _HTML


def test_task_frame_kill_endpoint_cancels_active_frame(tmp_path) -> None:
    from datetime import UTC, datetime

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_frames import (
        TaskFrame,
        TaskFrameExecutionContract,
        TaskFrameFinishCriteria,
        TaskFrameOperation,
        TaskFrameOutputContract,
        TaskFrameStatus,
    )
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    frame = TaskFrame(
        frame_id="frame-kill",
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
        summary="dead task",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("web:operator", "frame-kill")

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/tasks/frame/frame-kill/kill")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert runtime.store.get_task_frame("frame-kill").status is TaskFrameStatus.CANCELLED
    assert runtime.store.get_active_task_frame_id("web:operator") is None

    second = TestClient(app).post("/api/tasks/frame/frame-kill/kill")

    assert second.status_code == 200
    assert second.json()["ok"] is True
    assert second.json()["message"] == "Task frame is already finished."


def test_mini_agent_kill_endpoint_delegates_to_orchestrator(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    class Orchestrator:
        def __init__(self) -> None:
            self.cancelled: list[str] = []

        async def cancel_task(self, task_id: str) -> bool:
            self.cancelled.append(task_id)
            return task_id == "task-1"

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    orchestrator = Orchestrator()
    app = create_app(runtime, orchestrator=orchestrator, registry=ToolRegistry())

    response = TestClient(app).post("/api/tasks/mini-agent/task-1/kill")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert orchestrator.cancelled == ["task-1"]


def test_mini_agent_kill_endpoint_handles_persisted_only_and_stale_runs(tmp_path) -> None:
    from datetime import UTC, datetime

    from fastapi.testclient import TestClient

    from nullion.mini_agent_runs import MiniAgentRunStatus
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.start_mini_agent_run(
        run_id="task-persisted",
        capsule_id="group-persisted",
        mini_agent_type="Persisted only",
        created_at=datetime.now(UTC),
    )
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())

    response = TestClient(app).post("/api/tasks/mini-agent/task-persisted/kill")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    run = runtime.store.get_mini_agent_run("task-persisted")
    assert run is not None
    assert run.status is MiniAgentRunStatus.FAILED
    assert run.result_summary == "Killed from web UI."

    second = TestClient(app).post("/api/tasks/mini-agent/task-persisted/kill")

    assert second.status_code == 200
    assert second.json()["ok"] is True
    assert second.json()["message"] == "Mini-Agent task is already finished."


def test_doctor_update_endpoint_is_idempotent_for_stale_clicks(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_doctor_action(
        {
            "action_id": "act-double-click",
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
    )
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    first = client.post("/api/doctor/act-double-click/dismiss")
    second = client.post("/api/doctor/act-double-click/dismiss")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["ok"] is True
    assert second.json()["ok"] is True
    assert runtime.store.get_doctor_action("act-double-click")["status"] == "cancelled"


def test_doctor_inspect_endpoint_returns_visible_playbook_result(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_doctor_action(
        {
            "action_id": "act-web-inspect",
            "owner": "doctor",
            "status": "pending",
            "action_type": "inspect",
            "recommendation_code": "investigate_stall",
            "summary": "Workflow timed out",
            "severity": "high",
            "reason": None,
            "source_reason": "source=web;issue_type=timeout;detail=Read operation timed out",
            "error": None,
        }
    )
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    response = client.post("/api/doctor/act-web-inspect/doctor%3Ainspect_run")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["item"]["status"] == "in_progress"
    assert "Doctor run inspection" in data["message"]
    assert "Read operation timed out" in data["message"]


def test_status_refresh_cleans_dead_waiting_approval_frame_into_doctor_history(tmp_path) -> None:
    from datetime import UTC, datetime, timedelta

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_frames import (
        TaskFrame,
        TaskFrameExecutionContract,
        TaskFrameFinishCriteria,
        TaskFrameOperation,
        TaskFrameOutputContract,
        TaskFrameStatus,
    )
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
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
        summary="orphaned approval wait",
        created_at=datetime.now(UTC) - timedelta(minutes=5),
        updated_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("web:operator", frame.frame_id)

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    data = TestClient(app).get("/api/status?full=1").json()

    assert runtime.store.get_task_frame("frame-dead").status is TaskFrameStatus.CANCELLED
    assert runtime.store.get_active_task_frame_id("web:operator") is None
    assert any(
        item["recommendation_code"] == "cleanup_dead_task_frame"
        and item["status"] == "completed"
        and "orphaned approval wait" in item["summary"]
        for item in data["doctor_actions"]
    )


def test_status_refresh_respects_smart_cleanup_toggle(tmp_path, monkeypatch) -> None:
    from datetime import UTC, datetime, timedelta

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_frames import (
        TaskFrame,
        TaskFrameExecutionContract,
        TaskFrameFinishCriteria,
        TaskFrameOperation,
        TaskFrameOutputContract,
        TaskFrameStatus,
    )
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_SMART_CLEANUP_ENABLED", "false")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
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
        summary="orphaned approval wait",
        created_at=datetime.now(UTC) - timedelta(minutes=5),
        updated_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("web:operator", frame.frame_id)

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    data = TestClient(app).get("/api/status?full=1").json()

    assert runtime.store.get_task_frame("frame-dead").status is TaskFrameStatus.WAITING_APPROVAL
    assert runtime.store.get_active_task_frame_id("web:operator") == "frame-dead"
    assert not any(item["recommendation_code"] == "cleanup_dead_task_frame" for item in data["doctor_actions"])


def test_status_endpoint_reuses_short_lived_cache(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_WEB_STATUS_CACHE_TTL_S", "30")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    first = client.get("/api/status?full=1")
    second = client.get("/api/status?full=1")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.headers["X-Nullion-Status-Cache"] == "miss"
    assert second.headers["X-Nullion-Status-Cache"] == "hit"
    assert second.json()["status_meta"]["cache_ttl_s"] == 30.0


def test_status_endpoint_compacts_dashboard_mini_agent_payload(tmp_path, monkeypatch) -> None:
    from datetime import timedelta

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_WEB_STATUS_CACHE_TTL_S", "0")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    now = datetime.now(UTC) - timedelta(minutes=5)
    for index in range(60):
        run_id = f"done-{index:02d}"
        runtime.start_mini_agent_run(
            run_id=run_id,
            capsule_id=f"group-{index}",
            mini_agent_type="Completed worker",
            created_at=now + timedelta(seconds=index),
        )
        runtime.complete_mini_agent_run(run_id, result_summary="completed " + ("x" * 1000))
    runtime.start_mini_agent_run(
        run_id="running-old",
        capsule_id="group-running",
        mini_agent_type="Running worker",
        created_at=now - timedelta(days=3),
    )
    runtime.mark_mini_agent_run_running("running-old")
    runtime.start_mini_agent_run(
        run_id="failed-old",
        capsule_id="group-failed",
        mini_agent_type="Failed worker",
        created_at=now + timedelta(seconds=61),
    )
    runtime.fail_mini_agent_run("failed-old", result_summary="failed " + ("y" * 1000))

    payload = TestClient(create_app(runtime, orchestrator=None, registry=ToolRegistry())).get("/api/status?full=1").json()
    task_ids = [item["task_id"] for item in payload["mini_agent_tasks"]]

    assert len(payload["mini_agent_tasks"]) == 50
    assert "running-old" in task_ids
    assert "failed-old" in task_ids
    failed = next(item for item in payload["mini_agent_tasks"] if item["task_id"] == "failed-old")
    assert "[truncated for dashboard status payload]" in failed["result_summary"]
    assert payload["status_meta"]["limits"]["mini_agent_tasks"] == 50
    assert payload["status_meta"]["truncated"]["mini_agent_tasks"] == {"shown": 50, "total": 62}


def test_chat_history_endpoint_does_not_sync_runtime_history_by_default(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    import nullion.chat_store as chat_store_module
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    chat_store_module._store = None
    calls: list[str] = []
    monkeypatch.setattr(web_app, "_sync_runtime_chat_history_to_store", lambda _runtime, _store: calls.append("sync"))

    client = TestClient(create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry()))

    assert client.get("/api/chat/history/web:empty").status_code == 200
    assert calls == []

    assert client.get("/api/chat/history/web:empty?sync_runtime=1").status_code == 200
    assert calls == ["sync"]


def test_all_channel_history_endpoints_do_not_sync_runtime_history_by_default(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    import nullion.chat_store as chat_store_module
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "nullion-home"))
    chat_store_module._store = None
    calls: list[str] = []
    monkeypatch.setattr(web_app, "_sync_runtime_chat_history_to_store", lambda _runtime, _store: calls.append("sync"))

    client = TestClient(create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry()))

    assert client.get("/api/history/channels").status_code == 200
    assert client.get("/api/history/calendar/web?month=2026-05").status_code == 200
    assert client.get("/api/history/conversations/web/2026-05-28").status_code == 200
    assert calls == []

    assert client.get("/api/history/channels?sync_runtime=1").status_code == 200
    assert client.get("/api/history/calendar/web?month=2026-05&sync_runtime=1").status_code == 200
    assert client.get("/api/history/conversations/web/2026-05-28?sync_runtime=1").status_code == 200
    assert calls == ["sync", "sync", "sync"]


def test_web_restore_loads_latest_active_conversation_before_history() -> None:
    source = _HTML
    assert "async function _fetchLatestActiveWebConversation()" in source
    assert "const latestConv = await _fetchLatestActiveWebConversation();" in source
    assert source.index("const latestConv = await _fetchLatestActiveWebConversation();") < source.index(
        "const data = await _fetchConversationHistory(conversationId);"
    )
    assert "String(data.conversation.status).toLowerCase() !== 'active'" in source


def test_web_attachment_history_saves_structured_text_without_display_summary() -> None:
    source = _HTML
    assert "history_text: messageText" in source
    assert "historyText = effectiveMetadata.history_text;" in source
    assert "delete historyMetadata.history_text;" in source
    assert "_chatSaveMsg('user', historyText, false, historyMetadata);" in source


def test_settings_summary_endpoint_returns_compact_counts(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).get("/api/settings/summary")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"builder", "doctor", "health", "storage"}
    assert payload["builder"]["proposals"] == 0
    assert payload["doctor"]["actions"] == 0
    assert "builder_proposals" not in payload
    assert "latency_breakdowns" not in payload


def test_config_header_endpoint_returns_lightweight_header_payload(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).get("/api/config/header")

    assert response.status_code == 200
    payload = response.json()
    assert "model_provider" in payload
    assert "provider_models" in payload
    assert "providers_configured" in payload
    assert "skill_pack_catalog" not in payload
    assert "installed_skill_packs" not in payload


def test_config_post_can_disable_media_selection_without_forgetting_saved_model(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_PROVIDER", "openai")
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_MODEL", "whisper-1")
    monkeypatch.setenv("NULLION_AUDIO_TRANSCRIBE_ENABLED", "true")

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    response = client.post(
        "/api/config",
        json={
            "model_provider": "openai",
            "model_name": "gpt-4o",
            "audio_transcribe_enabled": False,
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert os.environ["NULLION_AUDIO_TRANSCRIBE_ENABLED"] == "false"
    assert os.environ["NULLION_AUDIO_TRANSCRIBE_PROVIDER"] == "openai"
    assert os.environ["NULLION_AUDIO_TRANSCRIBE_MODEL"] == "whisper-1"

    cleared = client.post(
        "/api/config",
        json={
            "model_provider": "openai",
            "model_name": "gpt-4o",
            "audio_transcribe_enabled": False,
            "audio_transcribe_provider": "",
            "audio_transcribe_model": "",
            "clear_media_selections": ["audio_transcribe_provider", "audio_transcribe_model"],
        },
    )

    assert cleared.status_code == 200
    assert cleared.json()["ok"] is True
    assert os.environ["NULLION_AUDIO_TRANSCRIBE_ENABLED"] == "false"
    assert "NULLION_AUDIO_TRANSCRIBE_PROVIDER" not in os.environ
    assert "NULLION_AUDIO_TRANSCRIBE_MODEL" not in os.environ


def test_status_endpoint_uses_cheap_fingerprint_until_ttl_expires(tmp_path, monkeypatch) -> None:
    from datetime import UTC, datetime

    from fastapi.testclient import TestClient

    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_WEB_STATUS_CACHE_TTL_S", "30")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = memory_owner_for_web_admin()
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    first = client.get("/api/status?full=1")
    second = client.get("/api/status?full=1")

    runtime.store.add_user_memory_entry(
        UserMemoryEntry(
            entry_id=f"{owner}:favorite_color",
            owner=owner,
            kind=UserMemoryKind.PREFERENCE,
            key="favorite_color",
            value="The user's favorite color is green.",
            source="background-test",
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
    )
    cached_after_memory_change = client.get("/api/status?full=1")
    monkeypatch.setenv("NULLION_WEB_STATUS_CACHE_TTL_S", "0")
    refreshed = client.get("/api/status?full=1")

    assert first.headers["X-Nullion-Status-Cache"] == "miss"
    assert second.headers["X-Nullion-Status-Cache"] == "hit"
    assert cached_after_memory_change.headers["X-Nullion-Status-Cache"] == "hit"
    assert refreshed.headers["X-Nullion-Status-Cache"] == "miss"
    assert [item["value"] for item in refreshed.json()["memory"]] == ["The user's favorite color is green."]


def test_status_checkpoint_sync_ignores_append_only_chat_event_changes(tmp_path, monkeypatch) -> None:
    import json
    import sqlite3
    from datetime import UTC, datetime

    from fastapi.testclient import TestClient

    import nullion.runtime_persistence as runtime_persistence
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_WEB_STATUS_CACHE_TTL_S", "0")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.checkpoint()
    client = TestClient(create_app(runtime, orchestrator=None, registry=ToolRegistry()))

    assert client.get("/api/status?full=1").status_code == 200

    def fail_dashboard_reload(*_args, **_kwargs):
        raise AssertionError("dashboard status should not reload runtime rows for append-only chat events")

    monkeypatch.setattr(runtime_persistence, "_load_runtime_store_sqlite_collections", fail_dashboard_reload)
    with sqlite3.connect(str(runtime.checkpoint_path)) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO conversation_events
               (collection, item_key, payload, updated_at)
               VALUES (?, ?, ?, ?)""",
            (
                "conversation_events",
                "chat-event-only",
                json.dumps(
                    {
                        "event_id": "chat-event-only",
                        "event_type": "conversation.chat_turn",
                        "conversation_id": "telegram:test",
                        "created_at": datetime.now(UTC).isoformat(),
                    }
                ),
                datetime.now(UTC).isoformat(),
            ),
        )

    response = client.get("/api/status?full=1")

    assert response.status_code == 200
    assert response.headers["X-Nullion-Status-Cache"] == "miss"


def test_status_endpoint_bounds_memory_payload(tmp_path, monkeypatch) -> None:
    from datetime import UTC, datetime, timedelta

    from fastapi.testclient import TestClient

    from nullion.memory import UserMemoryEntry, UserMemoryKind, memory_owner_for_web_admin
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv("NULLION_WEB_STATUS_CACHE_TTL_S", "0")
    monkeypatch.setenv("NULLION_WEB_STATUS_MEMORY_LIMIT", "2")
    monkeypatch.setenv("NULLION_WEB_STATUS_MEMORY_VALUE_TEXT_LIMIT", "12")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = memory_owner_for_web_admin()
    now = datetime(2026, 5, 8, tzinfo=UTC)
    for index in range(3):
        runtime.store.add_user_memory_entry(
            UserMemoryEntry(
                entry_id=f"{owner}:favorite-color-{index}",
                owner=owner,
                kind=UserMemoryKind.FACT,
                key=f"favorite-color-{index}",
                value="green " * 20,
                source="test",
                created_at=now + timedelta(seconds=index),
                updated_at=now + timedelta(seconds=index),
            )
        )

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    payload = TestClient(app).get("/api/status?full=1").json()

    assert len(payload["memory"]) == 2
    assert payload["memory"][0]["value_truncated"] is True
    assert payload["memory"][0]["value"].startswith("green green ")
    assert payload["status_meta"]["limits"]["memory"] == 2


def test_doctor_diagnose_endpoint_returns_report(tmp_path) -> None:
    from types import SimpleNamespace

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    class Orchestrator:
        def get_status(self):
            return [SimpleNamespace(task_id="mini-live")]

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry())

    response = TestClient(app).post("/api/doctor/diagnose")
    data = response.json()

    assert response.status_code == 200
    assert data["ok"] is True
    assert isinstance(data["report"], dict)
    assert data["report"]["summary"]


def test_status_refresh_does_not_replace_store_while_web_turn_is_active(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.checkpoint()
    approval = create_approval_request(
        requested_by="web:test",
        action="allow_boundary",
        resource="https://example.com/*",
        request_kind="boundary_policy",
    )
    runtime.store.add_approval_request(approval)
    runtime.last_checkpoint_fingerprint = "force-refresh"

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    with app.state.nullion_runtime_turn_guard():
        response = client.get("/api/status?full=1")

    assert response.status_code == 200
    assert runtime.store.get_approval_request(approval.approval_id) is not None

    client.get("/api/status?full=1")
    assert runtime.store.get_approval_request(approval.approval_id) is not None


def test_restart_endpoint_reports_chat_service_failure_after_gateway_notice_without_live_restart(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    scheduled: list[bool] = []

    def fail_chat_services():
        raise ValueError("Telegram token missing")

    monkeypatch.setattr(web_app, "_restart_non_web_services", fail_chat_services)
    monkeypatch.setattr(web_app, "_schedule_process_restart", lambda: scheduled.append(True))

    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/restart")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["gateway_event"]["kind"] == "restarting"
    assert data["chat_services_error"] == "Telegram token missing"
    assert scheduled == [True]


def test_model_provider_test_control_lives_next_to_chat_model_input() -> None:
    model_input_pos = _HTML.index('id="cfg-model-name"')
    add_input_pos = _HTML.index('id="cfg-chat-model-name"')
    list_pos = _HTML.index('id="chat-model-list"')
    test_button_pos = _HTML.index('id="model-test-btn"')
    feedback_pos = _HTML.index('id="model-test-feedback"')
    media_section_pos = _HTML.index('id="media-model-section-title"')

    assert model_input_pos < add_input_pos < test_button_pos < list_pos < feedback_pos < media_section_pos
    assert "provider-test-row" not in _HTML


def test_chat_model_add_uses_media_model_inventory_pattern() -> None:
    assert '<input type="text" id="cfg-chat-model-name"' in _HTML
    assert '<button class="card-btn secondary" type="button" id="model-add-btn" onclick="addChatModelRow()">Add</button>' in _HTML
    assert 'class="chat-model-item media-model-item"' in _HTML
    assert "data-model-name=\"${escAttr(model)}\"" in _HTML
    assert "No chat models added for this provider." in _HTML
    assert "handleChatModelInputKey(event)" in _HTML
    assert "Rows save top-to-bottom as the comma-separated fallback order; the first model is active." in _HTML


def test_chat_model_rows_have_per_model_test_controls() -> None:
    assert 'id="chat-model-list"' in _HTML
    assert "function renderChatModelRows(modelValue)" in _HTML
    assert 'onclick="makeChatModelDefault(${index})">Make default</button>' in _HTML
    assert 'class="media-default-btn chat-model-test-btn" type="button" onclick="testModelConnection(${index})">Test</button>' in _HTML
    assert 'class="chat-model-remove-btn" type="button" title="Remove chat model" onclick="removeChatModelRow(${index})">×</button>' in _HTML
    assert "function syncChatModelRowsToHidden()" in _HTML


def test_chat_model_default_promotion_reorders_saved_model_rows() -> None:
    assert "function makeChatModelDefault(index)" in _HTML
    assert "const [model] = values.splice(index, 1);" in _HTML
    assert "values.unshift(model);" in _HTML
    assert "renderChatModelRows(values);" in _HTML
    assert "moved to the top of the fallback order. Save settings to persist." in _HTML
    assert "`Fallback ${index}`" in _HTML


def test_empty_chat_model_rows_clear_hidden_fallback_value() -> None:
    assert "No chat models added for this provider." in _HTML
    assert "if (hidden) hidden.value = '';" in _HTML
    assert "if (provider) _providerModels[provider] = '';" in _HTML


def test_model_api_key_input_enables_selected_chat_provider() -> None:
    assert 'id="cfg-api-key" placeholder="sk-..." autocomplete="off" oninput="onModelApiKeyInput()"' in _HTML
    assert "function onModelApiKeyInput()" in _HTML
    assert "_providersEnabled[provider] = true;" in _HTML
    assert "function providerHasMediaAccess(provider)" in _HTML
    assert "provider === selectedProvider && !!apiKey && !apiKey.startsWith" in _HTML
    assert "refreshMediaHelperOptionsFromInventory();" in _HTML


def test_media_model_add_control_lives_next_to_media_model_input() -> None:
    media_input_pos = _HTML.index('id="cfg-media-model-name"')
    add_button = '<button class="card-btn secondary" type="button" onclick="addMediaModel()">Add</button>'
    type_select_pos = _HTML.index('id="cfg-media-model-type"')
    feedback_pos = _HTML.index('id="media-model-feedback"')

    assert _HTML.count(add_button) == 1
    assert media_input_pos < _HTML.index(add_button) < type_select_pos < feedback_pos


def test_web_chat_allows_parallel_active_turns() -> None:
    assert "let _activeSendTurnIds = new Set();" in _HTML
    assert "let _activeTurnTimers = new Map();" in _HTML
    assert "function chatTurnInFlight()" in _HTML
    assert "function beginTurnUi(turnId, text)" in _HTML
    assert "function finishTurnUi(turnId = null)" in _HTML
    assert "_activeSendTurnIds.add(id);" in _HTML
    assert "_activeSendTurnIds.delete(turnId);" in _HTML
    assert "async function sendMessage() {\n  if (chatTurnInFlight())" not in _HTML
    assert "sendMessage = async function() {\n  if (chatTurnInFlight())" not in _HTML
    assert "beginTurnUi(turnId, displayText);" in _HTML
    assert "finishTurnUi(data.turn_id || null);" in _HTML


def test_chat_restore_does_not_rerender_after_user_interaction() -> None:
    assert "let _chatUserInteractedSinceLoad = false;" in _HTML
    assert "_chatUserInteractedSinceLoad = true;" in _HTML
    assert "if (_chatUserInteractedSinceLoad) {" in _HTML
    assert "document.getElementById('send-btn').addEventListener('click', () => sendMessage());" in _HTML


def test_websocket_chat_dispatches_turns_concurrently() -> None:
    source = Path(web_app.__file__).read_text(encoding="utf-8")
    assert "active_turn_tasks: set[asyncio.Task] = set()" in source
    assert "task = asyncio.create_task(process_chat_payload(payload, dependency_tasks, dispatch_decision, latency_recorder))" in source
    assert "send_lock = asyncio.Lock()" in source
    assert "from nullion.turn_dispatch_graph import route_turn_dispatch_with_context" in source
    assert "route_turn_dispatch_with_context," in source
    assert "turn_dispatch_decision=dispatch_decision" in source


def test_websocket_chat_can_finish_later_message_first(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    def fake_run_platform_chat_request(runtime, request):  # noqa: ANN001, ANN202
        _ = runtime
        if "slow" in str(request.text):
            time.sleep(0.2)
        return platform_chat.PlatformChatResponse(text=f"{request.text} done")

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({"text": "slow", "conversation_id": "web:test", "turn_id": "turn:slow", "stream": False})
            websocket.send_json({"text": "fast", "conversation_id": "web:test", "turn_id": "turn:fast", "stream": False})

            first = websocket.receive_json()
            assert first == {"turn_id": "turn:fast", "type": "chunk", "text": "fast done"}

            seen_done = set()
            while seen_done != {"turn:fast", "turn:slow"}:
                event = websocket.receive_json()
                if event["type"] == "done":
                    seen_done.add(event["turn_id"])


def test_websocket_platform_gateway_preserves_surface_turn_id(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import platform_chat
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    calls = []

    def fake_run_platform_chat_request(runtime, request):  # noqa: ANN001, ANN202
        _ = runtime
        calls.append(request)
        return platform_chat.PlatformChatResponse(text="ok")

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({
                "text": "preserve this turn id",
                "conversation_id": "web:turn-id",
                "turn_id": "turn:surface-id",
                "stream": False,
            })
            assert websocket.receive_json() == {"turn_id": "turn:surface-id", "type": "chunk", "text": "ok"}
            while True:
                event = websocket.receive_json()
                if event["type"] == "done":
                    assert event["turn_id"] == "turn:surface-id"
                    break

    assert len(calls) == 1
    assert calls[0].turn_id == "turn:surface-id"
    assert calls[0].conversation_ingress_id == "turn:surface-id"


def test_web_saved_history_prompt_includes_previous_turn_context(tmp_path) -> None:
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:history-reference"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 13, 41, tzinfo=UTC).isoformat(),
            "turn_id": "turn-route",
            "user_message": "Princeton Junction to New York Penn Station",
            "assistant_reply": "Northeast Corridor #7840 arrives at New York Penn Station at 3:34 PM.",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 15, 5, tzinfo=UTC).isoformat(),
            "turn_id": "turn-reference",
            "user_message": "Do you know that NJ Transit train we were talking about?",
            "assistant_reply": "I do not have the earlier details visible here.",
        }
    )

    prompt = web_app._automatic_web_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="NJ Transit train",
        visible_turns=[],
    )

    assert prompt is not None
    assert "previous_turn" in prompt
    assert "Princeton Junction" in prompt
    assert "3:34 PM" in prompt


def test_web_saved_history_prompt_does_not_fall_back_to_unrelated_recent_turns(tmp_path) -> None:
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:history-reference"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 13, 41, tzinfo=UTC).isoformat(),
            "turn_id": "turn-pr-context",
            "user_message": "PR 28",
            "assistant_reply": "PR 28 is waiting on the Windows installer check.",
        }
    )

    prompt = web_app._automatic_web_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="zz-no-match",
        visible_turns=[],
    )

    assert prompt is None


def test_web_saved_history_prompt_adds_recent_context_to_direct_matches(tmp_path) -> None:
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:history-reference"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 13, 41, tzinfo=UTC).isoformat(),
            "turn_id": "turn-direct-match",
            "user_message": "alpha",
            "assistant_reply": "Need more structured target data.",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 13, 42, tzinfo=UTC).isoformat(),
            "turn_id": "turn-recent-target",
            "user_message": "target-42",
            "assistant_reply": "Use local_shell for target-42 checks.",
        }
    )

    prompt = web_app._automatic_web_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="alpha",
        visible_turns=[],
    )

    assert prompt is not None
    assert "Need more structured target data" in prompt
    assert "target-42" in prompt
    assert "Use local_shell" in prompt


def test_web_saved_history_prompt_orders_recent_candidates_first(tmp_path) -> None:
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:history-reference"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 13, 41, tzinfo=UTC).isoformat(),
            "turn_id": "turn-stale",
            "user_message": "beta item update status beta item update",
            "assistant_reply": "Need more structured target data.",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 13, 42, tzinfo=UTC).isoformat(),
            "turn_id": "turn-latest",
            "user_message": "find beta",
            "assistant_reply": "beta item resolved as catalog target 4821 and source link ready.",
        }
    )

    prompt = web_app._automatic_web_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="beta item update?",
        visible_turns=[],
    )

    assert prompt is not None
    assert prompt.index("beta item resolved as catalog target 4821") < prompt.index("Need more structured target data")


def test_web_saved_history_prompt_prefers_adjacent_reply_over_later_side_turn(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:history-reference"
    matches = [
        {
            "created_at": datetime(2026, 5, 27, 13, 42, tzinfo=UTC).isoformat(),
            "context": "following_turn",
            "match_score": 17,
            "user_message": "target item side note",
            "assistant_reply": "Need separate runtime details for the side path.",
        },
        {
            "created_at": datetime(2026, 5, 25, 13, 41, tzinfo=UTC).isoformat(),
            "context": "following_turn",
            "match_score": 18,
            "assistant_reply": "target item resolved as catalog target 4821 and source link ready.",
        },
    ]

    class HistoryRegistry:
        def invoke(self, invocation):
            output = {"matches": matches if invocation.arguments.get("query") else [], "searched_turn_count": len(matches)}
            return SimpleNamespace(status="completed", output=output)

    monkeypatch.setattr(chat_operator, "with_conversation_history_tool", lambda *args, **kwargs: HistoryRegistry())

    prompt = web_app._automatic_web_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="target item update?",
        visible_turns=[],
    )

    assert prompt is not None
    assert prompt.index("target item resolved as catalog target 4821") < prompt.index("Need separate runtime details")
    assert "Highest-priority candidate" in prompt
    assert "ground the answer in these saved facts first" in prompt
    reference_message = web_app._web_saved_history_reference_context_message(prompt)
    assert reference_message is not None
    assert reference_message["role"] == "assistant"
    assert "target item resolved as catalog target 4821" in reference_message["content"][0]["text"]


def test_web_saved_history_prompt_surfaces_recent_structured_targets_over_generic_match(tmp_path) -> None:
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:history-reference"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 27, 14, 8, tzinfo=UTC).isoformat(),
            "turn_id": "turn-generic-clarification",
            "user_message": "estado?",
            "assistant_reply": "Please send the target details.",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 27, 14, 15, tzinfo=UTC).isoformat(),
            "turn_id": "turn-url-target",
            "user_message": "mira esto",
            "assistant_reply": (
                "Estado del target listo. Link: https://records.example.test/workspaces/release-7/items/28"
            ),
        }
    )
    for index in range(12):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 27, 14, 16 + index, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-filler-{index}",
                "user_message": f"relleno {index}",
                "assistant_reply": f"contexto ordinario {index}",
            }
        )

    prompt = web_app._automatic_web_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="estado?",
        visible_turns=[],
    )

    assert prompt is not None
    assert "Structured saved reference candidates" in prompt
    assert '"type": "url"' in prompt
    assert '"domain": "records.example.test"' in prompt
    assert "https://records.example.test/workspaces/release-7/items/28" in prompt
    reference_message = web_app._web_saved_history_reference_context_message(prompt)
    assert reference_message is not None
    reference_text = reference_message["content"][0]["text"]
    assert "https://records.example.test/workspaces/release-7/items/28" in reference_text
    assert "Please send the target details" not in reference_text
    assert "verify fresh/current status with live tools" in reference_text


def test_web_saved_history_uses_shared_operator_prompt_and_fallback_contract(tmp_path) -> None:
    from nullion import chat_operator
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    conversation_id = "web:shared-history-contract"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 29, 10, 0, tzinfo=UTC).isoformat(),
            "turn_id": "turn-badge-packet",
            "user_message": (
                "[Scheduled task delivery context] task=conference badge packet courier tracker; "
                "channel=slack; artifacts=none"
            ),
            "assistant_reply": (
                "Courier packet BPK-441 is out for delivery to the west lobby desk. "
                "Last scan was 2026-05-29 08:42."
            ),
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 29, 10, 5, tzinfo=UTC).isoformat(),
            "turn_id": "turn-unrelated-maintenance",
            "user_message": (
                "[Scheduled task delivery context] task=maintenance window readiness tracker; "
                "channel=telegram; artifacts=none"
            ),
            "assistant_reply": "Maintenance window MW-19 is ready.",
        }
    )

    web_prompt = web_app._automatic_web_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="where is the conference badge packet now?",
        visible_turns=[],
    )
    shared_prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="where is the conference badge packet now?",
        visible_turns=[],
    )

    assert web_prompt == shared_prompt
    assert web_app._web_saved_history_reference_context_message(web_prompt) == chat_operator._saved_history_reference_context_message(web_prompt)

    result = SimpleNamespace(
        final_text=(
            "Which packet do you mean?\n"
            "1. Send a photo\n"
            "2. Send a tracking link"
        )
    )
    assert chat_operator._apply_saved_history_structured_target_fallback(result, web_prompt) is True
    assert "conference badge packet courier tracker" in result.final_text
    assert "BPK-441" in result.final_text


def test_web_saved_history_reference_context_does_not_select_multiple_targets() -> None:
    prompt = "\n".join(
        [
            "Automatic saved-chat lookup for this turn found candidate prior turns.",
            "Structured saved reference candidates (ranked by relevance and recency):",
            'Target 1: {"domain": "records.example.test", "type": "url", "url": "https://records.example.test/a"}',
            'Target 2: {"domain": "records.example.test", "type": "url", "url": "https://records.example.test/b"}',
            "These targets come from structured runtime evidence such as URLs.",
            "Candidate prior turns:",
        ]
    )

    assert web_app._web_saved_history_reference_context_message(prompt) is None


def test_websocket_chat_streams_tool_activity_before_final_reply(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    tool_result = ToolResult(
        invocation_id="tool-1",
        tool_name="web_fetch",
        status="completed",
        output={"url": "https://example.com", "status_code": 200},
    )

    class Orchestrator:
        model_client = object()

        def run_turn(self, *, tool_result_callback=None, **kwargs):  # noqa: ANN001, ANN003
            assert tool_result_callback is not None
            tool_result_callback(tool_result)
            time.sleep(0.15)
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[tool_result],
                final_text="Fetched.",
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
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=Orchestrator(), registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({
                "text": "https://example.com",
                "conversation_id": "web:test",
                "turn_id": "turn:stream-tools",
                "activity_trace": True,
                "stream": False,
            })

            saw_live_tool_activity = False
            for _ in range(20):
                event = websocket.receive_json()
                assert event["type"] != "chunk"
                if (
                    event["type"] == "activity"
                    and event.get("id") == "orchestrate"
                    and "web_fetch" in event.get("detail", "")
                ):
                    saw_live_tool_activity = True
                    break

            assert saw_live_tool_activity


def test_websocket_tool_use_decision_does_not_send_working_notice_before_final_reply(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult

    tool_result = ToolResult(
        invocation_id="tool-1",
        tool_name="weather_forecast",
        status="completed",
        output={"summary": "Mild"},
    )

    class Orchestrator:
        model_client = object()

        def run_turn(self, *, foreground_event_callback=None, tool_result_callback=None, **kwargs):  # noqa: ANN001, ANN003
            assert foreground_event_callback is not None
            assert tool_result_callback is not None
            foreground_event_callback({"id": "foreground-working-ack", "label": "Preparing work", "status": "running"})
            tool_result_callback(tool_result)
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[tool_result],
                final_text="Weather checked.",
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
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=Orchestrator(), registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({
                "text": "neutral live tool request",
                "conversation_id": "web:test",
                "turn_id": "turn:foreground-tool-ack",
                "activity_trace": True,
                "stream": False,
            })

            for _ in range(50):
                event = websocket.receive_json()
                assert event["type"] != "working_ack"
                if event["type"] == "chunk":
                    assert event["text"].startswith("Weather checked.")
                    break
            else:
                raise AssertionError("final reply was not sent")


def test_web_working_notice_path_is_removed() -> None:
    assert "function replaceProvisionalTypingWithNotice(title, body, turnId = null)" in _HTML
    assert "function assistantNoticeHtml(title, body)" in _HTML
    assert "let _finishedTurnIds = new Set();" in _HTML
    assert "WEB_WORKING_ACK_TEXT" not in _HTML
    assert "function latestBotBubble()" in _HTML
    assert "function findBotBubbleByTurnId(turnId)" in _HTML
    assert "bubble.dataset.statusText = String(body || '');" in _HTML
    assert "data.type === 'working_ack'" not in _HTML
    assert "removeTransientWorkingAckBubble" not in _HTML
    assert "removeTransientWorkingAckBubbles" not in _HTML
    assert "const bubble = turnId ? (_botTurnBubbles.get(turnId) || findBotBubbleByTurnId(turnId)) : latestBotBubble();" in _HTML
    assert "if (turnId && _finishedTurnIds.has(turnId)) return { handled: true, bubble: null };" in _HTML
    assert "_finishedTurnIds.add(turnId);" in _HTML
    assert "if (isVisibleAnswerBubble(bubble)) return { handled: true, bubble: null };" in _HTML
    assert "function updateBubbleWithNotice(bubble, title, body, turnId = null)" in _HTML
    assert '"type": "working_ack"' not in _SOURCE
    socket_handler = _HTML[_HTML.index("socket.onmessage = (event) => {") : _HTML.index("} else if (data.type === 'thinking')", _HTML.index("socket.onmessage = (event) => {"))]
    assert "noticeKind === 'foreground_working_ack'" in socket_handler
    assert "replaceProvisionalTyping: noticeKind === 'foreground_working_ack'" in socket_handler


def test_web_done_event_recovers_missing_terminal_reply_from_runtime_history() -> None:
    done_handler = _HTML[_HTML.index("} else if (data.type === 'done') {") : _HTML.index("} else if (data.type === 'error')", _HTML.index("} else if (data.type === 'done') {"))]
    assert "refreshConversationAfterMissingTerminalReply(0, renderedConversationMessageCount());" in done_handler
    assert "function refreshConversationAfterMissingTerminalReply(attempt = 0, minCount = null)" in _HTML
    assert "?sync_runtime=1" in _HTML
    assert "cleanText !== '(no reply)'" in _HTML


def test_web_idle_dashboard_recovers_missing_terminal_reply_from_runtime_history() -> None:
    assert "let _pendingTerminalReplyRecoveries = new Map();" in _HTML
    assert "let _pendingTerminalReplyRecoveryKickTimer = null;" in _HTML
    assert "function rememberPendingTerminalReply(turnId)" in _HTML
    assert "rememberPendingTerminalReply(id);" in _HTML
    assert "schedulePendingTerminalReplyRecovery();" in _HTML
    assert "function schedulePendingTerminalReplyRecovery(delayMs = 2500)" in _HTML
    assert "function pendingTerminalReplyRecoveryBaseline()" in _HTML
    assert "function pendingTerminalPlaceholderCount()" in _HTML
    assert "function dashboardItemIsOpen(item)" in _HTML
    assert "function recoverMissingTerminalReplyFromIdleDashboard(data = {})" in _HTML
    assert "attempt < 90" in _HTML
    assert "renderedConversationMessageCount() - pendingTerminalPlaceholderCount()" in _HTML
    assert "refreshConversationAfterMissingTerminalReply(0, baseline);" in _HTML
    assert "clearPendingTerminalReply();" in _HTML
    assert "finishTurnUi();" in _HTML
    assert "recoverMissingTerminalReplyFromIdleDashboard(data);" in _HTML


def test_finalize_bot_message_does_not_reenable_send_button() -> None:
    start = _HTML.index("function finalizeBotMsg(")
    end = _HTML.index("function approvalToolIcon(", start)
    body = _HTML[start:end]

    assert "disabled = false" not in body
    assert "setSendButtonDisabled(false)" not in body
    assert "finishTurnUi(" not in body


def test_web_renderer_turns_markdown_blockquotes_into_readable_quote_blocks() -> None:
    assert ".message-quote" in _HTML
    assert "line.match(/^\\s*&gt;\\s?(.*)$/)" in _HTML
    assert '<blockquote class="message-quote">' in _HTML


def test_approval_resume_updates_original_activity_trace() -> None:
    assert "let _activityElByApproval = new Map();" in _HTML
    assert "function rememberApprovalActivity(approvalId, turnId = null)" in _HTML
    assert "function updateApprovalRunActivity(approvalId, event)" in _HTML
    assert "rememberApprovalActivity(data.approval_id, data.turn_id || null);" in _HTML
    assert "rememberApprovalActivity(msg.approval_id, turnId);" in _HTML
    assert "handleApprovalResume(data, approvalId);" in _HTML
    assert "activity.forEach(event => updateApprovalRunActivity(activityApprovalId, event));" in _HTML


def test_model_provider_test_uses_refreshed_csrf_and_only_connection_rows() -> None:
    assert "async function refreshLocalCsrfToken()" in _HTML
    assert "const res = await nativeFetch('/api/session'" in _HTML
    assert "return nativeFetch(input, {...nextInit, headers: retryHeaders});" in _HTML
    assert "id=\"mtr-media-" not in _HTML
    assert "Enter at least one chat model for the selected provider." in _HTML


def test_media_model_normalization_dedupes_and_drops_invalid_records() -> None:
    assert _normalize_media_models(
        {
            "openai": [
                {"model": "gpt-4o", "capabilities": ["image_input", "image_input"]},
                {"model": "gpt-4o", "capabilities": ["audio_input"]},
                {"name": "gpt-image-1", "capabilities": ["image_output"]},
                {"capabilities": ["image_input"]},
            ],
            "": [{"model": "ignored"}],
            "gemini": "not-a-list",
        }
    ) == {
        "openai": [
            {"model": "gpt-4o", "capabilities": ["image_input"]},
            {"model": "gpt-image-1", "capabilities": ["image_output"]},
        ]
    }


def test_invalid_media_model_capabilities_are_reported_before_save() -> None:
    errors = _invalid_media_model_capabilities(
        {
            "codex": [
                {"model": "gpt-5.5", "capabilities": ["audio_input"]},
                {"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]},
                {"model": "gpt-5.5", "capabilities": ["unknown_media"]},
            ],
            "openai": [{"model": "gpt-4o", "capabilities": []}],
        }
    )

    assert "codex · gpt-5.5 cannot use Codex OAuth for audio_input" in errors
    assert "codex · gpt-5.5 has unknown model type unknown_media" in errors
    assert "openai · gpt-4o needs a model type" in errors


def test_media_models_are_provider_scoped() -> None:
    assert _media_model_supports("image_generate", "codex", "gemini-3.1-flash-image-preview") is False
    assert _media_model_supports("image_generate", "gemini", "gemini-3.1-flash-image-preview") is True
    assert _filter_supported_media_models(
        {
            "codex": [{"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]}],
            "gemini": [{"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]}],
        }
    ) == {
        "codex": [{"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]}],
        "gemini": [{"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]}]
    }


def test_media_model_javascript_uses_structured_capability_selection() -> None:
    assert "function mediaCapabilityIsValid(provider, model, cap)" in _HTML
    assert "function mediaModelMatchesProvider(provider, model)" not in _HTML
    assert "belongs under a different provider" not in _HTML


def test_settings_load_refreshes_media_helper_options_after_hydration() -> None:
    assert "model-backed helper suggestions may not appear until the next click" in _HTML
    assert "refreshMediaHelperOptionsFromInventory();" in _HTML


def test_media_selection_rejects_partial_selection_and_codex_audio_even_if_declared() -> None:
    media_models = {"codex": [{"model": "gpt-5.5", "capabilities": ["audio_input", "image_input"]}]}

    assert _media_selection_supported("image_ocr", provider="", model="", media_models=media_models) is True
    assert _media_selection_supported("image_ocr", provider="codex", model="", media_models=media_models) is False
    assert _media_selection_supported("audio_transcribe", provider="codex", model="gpt-5.5", media_models=media_models) is False
    assert _media_selection_supported("image_ocr", provider="codex", model="gpt-5.5", media_models=media_models) is True


def test_terminal_recovery_route_requires_exact_turn_and_preserves_artifact_bytes(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from nullion.artifacts import artifact_root_for_principal
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    monkeypatch.setenv('NULLION_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path / 'workspaces'))
    runtime = bootstrap_persistent_runtime(tmp_path / 'runtime.db')
    conv = 'web:recovery-artifact'
    image = artifact_root_for_principal(conv) / 'capture.png'
    image.parent.mkdir(parents=True, exist_ok=True)
    image.write_bytes(b'\x89PNG\r\n\x1a\nrecovery-bytes')
    runtime.store.add_conversation_event({'conversation_id': conv, 'event_type': 'conversation.chat_turn',
        'turn_id': 'completed-turn', 'assistant_reply': 'Attached the requested capture. token=synthetic-secret-value',
        'tool_results': [{'tool_name': 'browser_screenshot', 'status': 'completed', 'output': {'artifact_path': str(image)}}]})
    client = TestClient(create_app(runtime, orchestrator=None, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path])))
    assert client.get(f'/api/chat/history/{conv}', params={'turn_ids': '["other-turn"]'}).json()['terminal_replies'] == []
    reply = client.get(f'/api/chat/history/{conv}', params={'turn_ids': '["completed-turn"]'}).json()['terminal_replies'][0]
    assert reply['turn_id'] == 'completed-turn'
    assert 'synthetic-secret-value' not in reply['text'] and '[redacted]' in reply['text']
    assert len(reply['artifacts']) == 1
    assert client.get(reply['artifacts'][0]['url']).content == image.read_bytes()
    for invalid in ('not-json', '{}', '[1]', '[""]', json.dumps(['id'] * 51)):
        assert client.get(f'/api/chat/history/{conv}', params={'turn_ids': invalid}).status_code == 400

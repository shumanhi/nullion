from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion.approvals import ApprovalRequest, ApprovalStatus, PermissionGrant
from nullion import chat_operator, operator_commands, telegram_app
from nullion.builder import BuilderDecisionType, BuilderProposal, BuilderProposalRecord
from nullion.config import load_settings
from nullion.memory import UserMemoryEntry, UserMemoryKind
from nullion.runtime import bootstrap_persistent_runtime
from nullion.suspended_turns import SuspendedTurn
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)
from nullion.tools import ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


class EventStore:
    def __init__(self) -> None:
        self.events: dict[str, list[dict]] = {}
        self.approvals: list[ApprovalRequest] = []
        self.grants: list[PermissionGrant] = []
        self.actions: list[dict] = []

    def add_conversation_event(self, event: dict) -> None:
        self.events.setdefault(event["conversation_id"], []).append(event)

    def list_conversation_events(self, conversation_id: str) -> list[dict]:
        return list(self.events.get(conversation_id, []))

    def list_approval_requests(self) -> list[ApprovalRequest]:
        return list(self.approvals)

    def get_approval_request(self, approval_id: str):
        return next((approval for approval in self.approvals if approval.approval_id == approval_id), None)

    def list_permission_grants(self) -> list[PermissionGrant]:
        return list(self.grants)

    def get_permission_grant(self, grant_id: str):
        return next((grant for grant in self.grants if grant.grant_id == grant_id), None)

    def list_doctor_actions(self) -> list[dict]:
        return list(self.actions)

    def get_doctor_action(self, action_id: str):
        return next((action for action in self.actions if action.get("action_id") == action_id), None)


class Runtime:
    def __init__(self) -> None:
        self.store = EventStore()
        self.checkpoints = 0
        self.denied: list[str] = []
        self.revoked: list[str] = []
        self.started_at = datetime(2026, 1, 1, tzinfo=UTC)

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def list_pending_builder_proposals(self):
        return []

    def deny_approval_request(self, approval_id: str, *, actor: str):
        self.denied.append(f"{actor}:{approval_id}")

    def revoke_permission_grant(self, grant_id: str, *, actor: str):
        self.revoked.append(f"{actor}:{grant_id}")

    def render_status_for_telegram(self, *, active_only=False):
        return "active" if active_only else "status"


def _approval(approval_id: str, *, status: ApprovalStatus = ApprovalStatus.PENDING, context: dict | None = None) -> ApprovalRequest:
    return ApprovalRequest(
        approval_id=approval_id,
        requested_by="telegram:1",
        action="use_tool",
        resource="web_fetch",
        status=status,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        context=context,
    )


def _grant(grant_id: str, *, expires_at=None, revoked_at=None) -> PermissionGrant:
    return PermissionGrant(
        grant_id=grant_id,
        approval_id="ap",
        principal_id="telegram:1",
        permission="tool:web_fetch",
        granted_by="operator",
        granted_at=datetime(2026, 1, 1, tzinfo=UTC),
        expires_at=expires_at,
        revoked_at=revoked_at,
    )


def test_chat_operator_text_and_session_helpers(monkeypatch) -> None:
    runtime = Runtime()
    monkeypatch.delenv("NULLION_TELEGRAM_CHAT_STREAMING_ENABLED", raising=False)

    assert chat_operator._normalize_local_intent_text(" Approve it!! ") == "approve it"
    assert chat_operator._word_tokens("File: report-v2.xlsx!") == ("file", "report", "v2", "xlsx")
    assert chat_operator._is_explicit_approval_reply("please approve") is True
    assert chat_operator._contains_file_reference("Saved to /tmp/report.pdf") is True
    assert chat_operator._contains_file_reference("Look in /tmp/artifacts") is False
    assert chat_operator._contains_file_reference("Version 1.2 is not a file") is False
    assert chat_operator._requested_attachment_extension("make a word document") == ".docx"
    assert chat_operator._requested_attachment_extension("send as text file") == ".txt"
    assert chat_operator._requested_attachment_extension("no attachment") is None
    assert chat_operator._classify_chat_backend_issue_type("timed out waiting").value == "timeout"
    assert chat_operator._classify_chat_backend_issue_type("boom").value == "error"

    assert chat_operator.activity_trace_status_text_for_chat(runtime, chat_id="1") in {"on", "off"}
    chat_operator.set_activity_trace_enabled_for_chat(runtime, chat_id="1", enabled=True)
    assert chat_operator.activity_trace_enabled_for_chat(runtime, chat_id="1") is True
    chat_operator.set_thinking_display_enabled_for_chat(runtime, chat_id="1", enabled=True)
    assert chat_operator.thinking_display_status_text_for_chat(runtime, chat_id="1") == "on"
    chat_operator.set_chat_streaming_enabled_for_chat(runtime, chat_id="1", enabled=False)
    assert chat_operator.chat_streaming_status_text_for_chat(runtime, chat_id="1") == "off"
    runtime.store.add_conversation_event({"conversation_id": "telegram:1", "event_type": "conversation.session_reset"})
    assert chat_operator._session_activity_trace_setting(runtime, chat_id="1") is None

    assert chat_operator._handle_streaming_command_for_chat(runtime, "/streaming on", chat_id="1") == "Chat streaming is on."
    assert chat_operator._handle_streaming_command_for_chat(None, "/streaming status", chat_id="1").startswith("Chat streaming is on")
    assert chat_operator._handle_streaming_command_for_chat(runtime, "/streaming maybe", chat_id="1").startswith("Usage")
    assert chat_operator._handle_thinking_command_for_chat(runtime, "/thinking off", chat_id="1") == "Thinking display is off."
    assert chat_operator._handle_thinking_command_for_chat(runtime, "/thinking maybe", chat_id="1").startswith("Usage")
    with pytest.raises(ValueError):
        chat_operator.set_verbose_mode_for_chat(runtime, chat_id="1", mode="loud")
    assert runtime.checkpoints >= 4


def test_chat_operator_conversation_memory_approval_and_artifact_helpers(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "chat-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123"})
    thread = [
        {"user": "make a file", "assistant": "Saved https://example.com/report"},
        {"user": "thanks", "assistant": "Okay."},
    ]

    assert chat_operator._chat_prompt_for_message("/chat hello there") == "hello there"
    assert chat_operator._chat_prompt_for_message("/status") is None
    assert chat_operator._local_chat_reply_body("thanks") == "Anytime."
    assert chat_operator._local_chat_reply_body("ok") == "Okay."
    assert chat_operator._local_chat_reply_body("good night") == "Good night \U0001f319"
    assert chat_operator._build_conversation_context(thread).startswith("User: make a file")
    assert chat_operator._messaging_channel_and_identity_for_chat("slack:U1") == ("slack", "U1")
    assert chat_operator._conversation_id_for_chat(None) == "telegram:default"
    assert chat_operator._principal_id_for_chat("123", settings) == "telegram_chat"
    assert chat_operator._previous_user_message(thread) == "thanks"
    assert chat_operator._previous_assistant_message(thread) == "Saved https://example.com/report"
    assert chat_operator._looks_like_short_ambiguous_follow_up("send it") is True
    assert chat_operator._assistant_reply_referencable_artifact_reason("Saved `/tmp/report.txt`") == "inline_code"
    fallback, reason = chat_operator._chat_ambiguity_fallback(thread, "send it")
    assert reason == "url"
    assert fallback("send it", True) is chat_operator.ConversationTurnDisposition.CONTINUE

    approval = ApprovalRequest(
        approval_id="ap-chat",
        requested_by="telegram:123",
        action="use_tool",
        resource="web_fetch",
        status=ApprovalStatus.PENDING,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        context={},
    )
    runtime.store.add_approval_request(approval)
    assert chat_operator._auto_approval_command_for_message(runtime, "approve", chat_id="123") == "/approve ap-chat"
    assert chat_operator._approval_id_from_command("/approve ap-chat") == "ap-chat"
    chat_operator._remember_chat_turn(runtime, chat_id="123", user_message="go", assistant_reply="Tool approval requested: ap-chat")
    assert chat_operator._source_prompt_for_approval(runtime, chat_id="123", approval_id="ap-chat") == "go"
    assert chat_operator._is_approval_pending_message({"role": "assistant", "content": "Tool approval requested: ap-chat"}) is True
    assert chat_operator._is_telegram_resume_principal("telegram:123") is True

    media_context = chat_operator._build_conversation_context(
        [{"user": "why so many files?", "assistant": f"Done.\nMEDIA:{tmp_path / 'old.pdf'}"}]
    )
    assert media_context is not None
    assert "MEDIA:" not in media_context

    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "1")
    chat_operator._remember_explicit_memory(runtime, chat_id="123", settings=settings, prompt="Remember I like vim")
    assert "vim" in (chat_operator._memory_context_for_chat(runtime, chat_id="123", settings=settings) or "").lower()

    assert chat_operator._artifact_delivery_label(
        [SimpleNamespace(name="capture.png", media_type="image/png")],
        tool_results=[ToolResult("shot", "browser_screenshot", "completed", {})],
    ) == "screenshot"
    assert chat_operator._artifact_delivery_label([SimpleNamespace(name="photo.png", media_type="image/png")]) == "image"
    result = ToolResult(
        invocation_id="i1",
        tool_name="file_write",
        status="completed",
        output={"path": str(tmp_path / "report.txt"), "artifact_paths": [str(tmp_path / "image.png")]},
    )
    assert chat_operator._artifact_paths_from_tool_results([result]) == [
        str(tmp_path / "report.txt"),
        str(tmp_path / "image.png"),
    ]
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    artifact = artifact_root / "report.txt"
    artifact.write_text("coverage artifact", encoding="utf-8")
    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda rt: artifact_root)
    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply="Result: report.txt",
        artifact_paths=[],
        prompt="frobnicate however tomorrow says it",
        tool_results=[],
    )
    assert "MEDIA:" not in reply
    assert chat_operator._reply_has_deliverable_media(runtime, reply) is False
    assert chat_operator._clean_undeliverable_media_reply(runtime, "hello\nMEDIA:/outside/missing.txt") == "hello"

    pdf_artifact = artifact_root / "report.pdf"
    pdf_artifact.write_bytes(b"%PDF-1.4\n%test\n")
    pdf_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply="I created the actual PDF now and saved it here:",
        artifact_paths=[],
        prompt="That's the pdf",
        tool_results=[ToolResult("pdf", "file_write", "completed", {"path": str(pdf_artifact)})],
    )
    assert "Done \u2014 attached the requested PDF." in pdf_reply
    assert f"MEDIA:{pdf_artifact}" in pdf_reply


def test_chat_operator_render_turn_orchestrator_and_fallback_paths(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "render-runtime.db")
    events: list[dict] = []

    tool_result = ToolResult(
        invocation_id="tool-1",
        tool_name="web_fetch",
        status="completed",
        output={"url": "https://example.com", "summary": "Fetched"},
    )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: "Enabled skill packs: testing")
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: SimpleNamespace(title="Coverage Skill", titles=("Coverage Skill",), prompt="Use the coverage skill."))
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(
        chat_operator,
        "TaskPlanner",
        lambda: SimpleNamespace(
            build_execution_plan=lambda **kwargs: SimpleNamespace(
                mission=SimpleNamespace(steps=()),
                can_dispatch_mini_agents=False,
                can_run_mission=False,
            )
        ),
    )

    class Orchestrator:
        model_client = object()

        def run_turn(self, *, tool_result_callback=None, **kwargs):
            if tool_result_callback is not None:
                tool_result_callback(tool_result)
            return SimpleNamespace(
                tool_results=[tool_result],
                suspended_for_approval=False,
                approval_id=None,
                final_text="Model answer",
                artifacts=[],
                thinking_text="short thought",
            )

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat fetch the page",
        chat_id="123",
        settings=settings,
        agent_orchestrator=Orchestrator(),
        activity_callback=events.append,
    )
    assert "Model answer" in reply
    assert any(event.get("id") == "skill" for event in events)
    assert any(event.get("label") == "web_fetch" for event in events)
    assert chat_operator._recent_tool_context_prompt(runtime, "telegram:123")

    fallback_runtime = bootstrap_persistent_runtime(tmp_path / "fallback-runtime.db")

    def fake_generate_chat_reply(
        *,
        message,
        conversation_context=None,
        memory_context=None,
        active_tool_registry=None,
        policy_store=None,
        approval_store=None,
        conversation_id=None,
        principal_id=None,
        live_tool_result_recorder=None,
        live_information_resolution_recorder=None,
    ):
        if live_tool_result_recorder is not None:
            live_tool_result_recorder(tool_result)
        if live_information_resolution_recorder is not None:
            live_information_resolution_recorder("resolved live info")
        return f"fallback for {message}"

    monkeypatch.setattr(chat_operator, "generate_chat_reply", fake_generate_chat_reply)
    fallback_reply = chat_operator._render_chat_turn(
        fallback_runtime,
        message="/chat answer directly",
        chat_id="123",
        settings=settings,
        agent_orchestrator=None,
        append_activity_trace=False,
    )
    assert "fallback for answer directly" in fallback_reply

    monkeypatch.setattr(chat_operator, "handle_operator_command", lambda *args, **kwargs: "Approved.")
    assert chat_operator.handle_chat_operator_message(
        fallback_runtime,
        "/new",
        chat_id="123",
        settings=settings,
    ) == "Starting fresh."
    assert chat_operator.handle_chat_operator_message(
        fallback_runtime,
        "hello chat",
        chat_id="123",
        settings=settings,
        agent_orchestrator=None,
        append_activity_trace=False,
    )


def test_chat_operator_resume_materialization_and_command_paths(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "resume-runtime.db")
    approval = ApprovalRequest(
        approval_id="ap-resume",
        requested_by="telegram:123",
        action="use_tool",
        resource="web_fetch",
        status=ApprovalStatus.APPROVED,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        context={},
    )
    runtime.store.add_approval_request(approval)
    suspended = SuspendedTurn(
        approval_id="ap-resume",
        conversation_id="telegram:123",
        chat_id="123",
        message="/chat fetch",
        request_id="req",
        message_id="msg",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        messages_snapshot=[
            {"role": "user", "content": [{"type": "text", "text": "old"}]},
            {"role": "assistant", "content": "Tool approval requested: old"},
            {"role": "user", "content": [{"type": "text", "text": "fetch"}]},
        ],
    )
    runtime.store.add_suspended_turn(suspended)
    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_append_chat_artifacts_to_reply", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])

    class ResumeOrchestrator:
        model_client = object()

        def resume_turn(self, **kwargs):
            assert kwargs["user_message"] == "fetch"
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                final_text="resumed answer",
                artifacts=[],
                tool_results=[ToolResult("i", "web_fetch", "completed", {"url": "https://example.com"})],
            )

    assert "resumed answer" in chat_operator.resume_approved_telegram_request(
        runtime,
        approval_id="ap-resume",
        chat_id="123",
        agent_orchestrator=ResumeOrchestrator(),
    )

    fetch_result = ToolResult("fetch", "web_fetch", "completed", {"body": "page body", "html": "<html>ok</html>"})
    assert chat_operator._latest_completed_tool_result([ToolResult("x", "other", "completed", {}), fetch_result], tool_name="web_fetch") is fetch_result
    assert chat_operator._materializable_fetch_body(fetch_result, extension=".txt") == "page body"
    assert chat_operator._materializable_fetch_body(ToolResult("html", "web_fetch", "completed", {"html": "<html>ok</html>"}), extension=".html") == "<html>ok</html>"
    monkeypatch.setattr(chat_operator, "_reply_has_deliverable_media", lambda *args, **kwargs: False)
    monkeypatch.setattr(chat_operator, "artifact_path_for_generated_workspace_file", lambda principal_id, suffix: tmp_path / f"artifact{suffix}", raising=False)
    monkeypatch.setattr(chat_operator, "invoke_tool_with_boundary_policy", lambda store, invocation, registry: ToolResult("write", "file_write", "completed", {"path": invocation.arguments["path"]}))
    monkeypatch.setattr(type(runtime), "active_tool_registry", property(lambda self: object()))
    materialized = chat_operator._maybe_materialize_requested_fetch_attachment(
        runtime,
        prompt="send as text file",
        reply="done",
        tool_results=[fetch_result],
    )
    assert "MEDIA:" in materialized

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    assert chat_operator.handle_chat_operator_message(runtime, "/verbose status", chat_id="123", settings=settings).startswith("Verbose mode")
    assert chat_operator.handle_chat_operator_message(runtime, "/streaming off", chat_id="123", settings=settings) == "Chat streaming is off."
    assert chat_operator.handle_chat_operator_message(runtime, "/thinking status", chat_id="123", settings=settings).startswith("Thinking display")
    monkeypatch.setattr(chat_operator, "_render_telegram_health", lambda runtime: "health ok")
    assert chat_operator.handle_chat_operator_message(runtime, "/health", chat_id="123", settings=settings) == "health ok"


def test_chat_operator_task_frame_and_activity_helpers(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "task-frame-runtime.db")
    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="telegram:123",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.FETCH_RESOURCE,
        target=TaskFrameTarget(kind="url", value="https://example.com", normalized_value="https://example.com/"),
        execution=TaskFrameExecutionContract(preferred_tool_family="terminal_exec"),
        output=TaskFrameOutputContract(artifact_kind="html"),
        finish=TaskFrameFinishCriteria(requires_artifact_delivery=True, required_artifact_kind="html"),
        summary="Fetch example",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("telegram:123", "frame-1")

    assert chat_operator._prompt_from_fetch_task_frame(target=frame.target, output=frame.output, execution=frame.execution) == (
        "Fetch https://example.com/ using curl and send me as an html file"
    )
    assert chat_operator._task_frame_output_phrase("docx") == "and send me as a Word document"
    assert chat_operator._active_task_frame_prompt(runtime, chat_id="123").startswith("Fetch https://example.com/")
    assert chat_operator._effective_prompt_from_task_frame(
        runtime,
        prompt="continue",
        conversation_result=SimpleNamespace(
            active_task_frame_id="frame-1",
            task_frame_continuation=SimpleNamespace(
                mode=SimpleNamespace(value="continue"),
                target=None,
                output=TaskFrameOutputContract(artifact_kind="txt"),
                execution=None,
            ),
        ),
    ).endswith("as a text file")
    monkeypatch.setattr(chat_operator, "_reply_has_deliverable_media", lambda *args, **kwargs: False)
    assert "still needs a html attachment" in chat_operator._incomplete_artifact_delivery_reply(
        runtime,
        conversation_id="telegram:123",
        reply="not done",
    )
    assert chat_operator._should_include_conversation_context(
        SimpleNamespace(turn=SimpleNamespace(parent_turn_id="turn-0"), task_frame_continuation=None),
        [{"user": "u", "assistant": "a"}],
    ) is True
    assert chat_operator._mission_step_titles(SimpleNamespace(steps=[SimpleNamespace(title=" One "), SimpleNamespace(title="")])) == ["One"]
    assert chat_operator._mini_agent_activity_detail(SimpleNamespace(task_status_detail=" detail\n", task_titles=("A",)), 1) == " detail"
    summary = "\u2192 \u2610 Gather data\n\u2611 Summarize\nplain"
    assert chat_operator._task_titles_from_status_summary(summary) == ["Gather data", "Summarize"]
    assert chat_operator._should_show_reply_label(local_reply=None, prior_user_message="hello") is True


def test_chat_operator_compound_backend_and_error_paths(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "compound-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    result = runtime.process_conversation_message(
        conversation_id="telegram:123",
        chat_id="123",
        user_message="first and second",
    )
    monkeypatch.setattr(chat_operator, "split_compound_intent", lambda prompt: ["first", "second"])
    monkeypatch.setattr(
        chat_operator,
        "_generate_backend_reply",
        lambda runtime, **kwargs: f"reply {kwargs['prompt']}",
    )
    monkeypatch.setattr(chat_operator, "_append_chat_artifacts_to_reply", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "render_chat_response_for_telegram", lambda contract: contract.draft.text)
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)
    compound = chat_operator._execute_compound_chat_turn(
        runtime,
        chat_id="123",
        prompt="first and second",
        conversation_context="User: before\nAssistant: ok",
        memory_context=None,
        conversation_result=result,
        append_activity_trace=False,
    )
    assert "reply first" in compound and "reply second" in compound

    def unavailable(*args, **kwargs):
        raise chat_operator.ChatBackendUnavailableError("quota exceeded")

    monkeypatch.setattr(chat_operator, "split_compound_intent", lambda prompt: [prompt])
    monkeypatch.setattr(chat_operator, "_generate_backend_reply", unavailable)
    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    health_issues: list[dict] = []
    monkeypatch.setattr(type(runtime), "report_health_issue", lambda self, **kwargs: health_issues.append(kwargs))
    assert chat_operator._render_chat_turn(
        runtime,
        message="/chat fail please",
        chat_id="123",
        settings=settings,
        agent_orchestrator=None,
    ) == "Nullion chat backend is unavailable right now."
    assert health_issues


def test_chat_operator_image_and_mission_render_paths(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "image-runtime.db")
    artifact = tmp_path / ".nullion-artifacts" / "image.png"
    artifact.parent.mkdir()
    artifact.write_bytes(b"png-data")
    common_patches = [
        (chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None),
        (chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None),
        (chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"]),
        (chat_operator, "artifact_root_for_principal", lambda principal_id: artifact.parent),
        (chat_operator, "artifact_root_for_runtime", lambda runtime: artifact.parent),
        (chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None),
    ]
    for obj, name, value in common_patches:
        monkeypatch.setattr(obj, name, value)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(
            kind="image",
            image_result=SimpleNamespace(),
            needs_approval=False,
            approval_id=None,
            completed=True,
            artifact_paths=[str(artifact)],
            tool_results=[ToolResult("img", "image_generate", "completed", {"path": str(artifact)})],
            error=None,
        ),
    )
    image_reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat make an image",
        chat_id="123",
        settings=settings,
    )
    assert "MEDIA:" in image_reply

    mission_runtime = bootstrap_persistent_runtime(tmp_path / "mission-runtime.db")
    mission_artifact = tmp_path / ".nullion-artifacts" / "mission.txt"
    mission_artifact.write_text("mission", encoding="utf-8")
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name: name == "NULLION_TASK_DECOMPOSITION_ENABLED")
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, message: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(
        chat_operator,
        "TaskPlanner",
        lambda: SimpleNamespace(
            build_execution_plan=lambda **kwargs: SimpleNamespace(
                mission=SimpleNamespace(steps=[SimpleNamespace(title="Fetch"), SimpleNamespace(title="Summarize")]),
                can_dispatch_mini_agents=False,
                can_run_mission=True,
            )
        ),
    )

    class MissionOrchestrator:
        model_client = object()

        def run_mission(self, **kwargs):
            return SimpleNamespace(
                status="completed",
                suspended_approval_id=None,
                final_summary="Mission complete",
                artifacts=[str(mission_artifact)],
                tool_results=[ToolResult("mission", "web_fetch", "completed", {"url": "https://example.com"})],
            )

    mission_reply = chat_operator._render_chat_turn(
        mission_runtime,
        message="/chat do a two step task",
        chat_id="123",
        settings=settings,
        agent_orchestrator=MissionOrchestrator(),
        append_activity_trace=False,
    )
    assert "Mission complete" in mission_reply or "MEDIA:" in mission_reply


def test_chat_operator_delegated_ack_stays_quiet_and_does_not_trigger_builder(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "delegated-runtime.db")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name: True)
    monkeypatch.setattr(
        chat_operator,
        "_try_builder_reflection",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("builder should not run for delegated ack")),
    )
    monkeypatch.setattr(
        chat_operator,
        "TaskPlanner",
        lambda: SimpleNamespace(
            build_execution_plan=lambda **kwargs: SimpleNamespace(
                mission=SimpleNamespace(steps=[SimpleNamespace(title="Find"), SimpleNamespace(title="Audit")]),
                can_dispatch_mini_agents=True,
                can_run_mission=True,
            )
        ),
    )

    class DelegatingOrchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            return SimpleNamespace(
                dispatched=True,
                task_count=2,
                acknowledgment=(
                    "Planner: Sequential Mission • 2 tasks\n"
                    "→ Working on 2 tasks:\n"
                    "  ☐ Find OpenClaw config files\n"
                    "  ☐ Audit OpenClaw configuration"
                ),
                planner_summary="Sequential Mission • 2 tasks",
                task_titles=["Find OpenClaw config files", "Audit OpenClaw configuration"],
                task_status_detail=(
                    "  ☐ Find OpenClaw config files\n"
                    "  ☐ Audit OpenClaw configuration"
                ),
            )

        def run_turn(self, **kwargs):
            raise AssertionError("delegated path should not run a direct turn")

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat audit openclaw config",
        chat_id="123",
        settings=settings,
        agent_orchestrator=DelegatingOrchestrator(),
        append_activity_trace=False,
    )

    assert "Planner: Sequential Mission • 2 tasks" in reply
    assert "☐ Find OpenClaw config files" in reply
    assert "Learned a new skill" not in reply


def test_chat_operator_remaining_helper_branches(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "helpers-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    assert chat_operator.build_instant_ack(runtime, "", chat_id="123", settings=settings) is None
    disabled = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "false"})
    assert chat_operator.build_instant_ack(runtime, "/chat hello", chat_id="123", settings=disabled) is None
    assert chat_operator.build_instant_ack(runtime, "/chat hello", chat_id="999", settings=settings) is None
    assert chat_operator.build_instant_ack(runtime, "/chat thanks", chat_id="123", settings=settings) is None

    assert chat_operator._is_approval_pending_message({
        "role": "assistant",
        "content": [{"type": "text", "text": "Tool approval requested: ap"}],
    }) is True
    assert chat_operator._is_approval_pending_message({
        "role": "assistant",
        "content": [{"type": "tool_use", "id": "tool"}],
    }) is False
    assert chat_operator._is_approval_pending_message({"role": "assistant", "content": 42}) is False

    registry = object()
    monkeypatch.setattr(type(runtime), "active_tool_registry", property(lambda self: registry))
    monkeypatch.setattr(
        "nullion.system_context.build_system_context_snapshot",
        lambda tool_registry=None: SimpleNamespace(snapshot=True),
    )
    monkeypatch.setattr("nullion.system_context.format_system_context_for_prompt", lambda snapshot: "tool inventory")
    assert "Live capability inventory" in (chat_operator._chat_capability_inventory_prompt(runtime) or "")
    monkeypatch.setattr("nullion.system_context.format_system_context_for_prompt", lambda snapshot: "")
    assert chat_operator._chat_capability_inventory_prompt(runtime) is None

    chat_operator._NEWLY_LEARNED_SKILLS[runtime] = ["One", "Two"]
    assert "Learned 2 new skills" in (chat_operator._pop_learned_skills_notification(runtime) or "")
    assert chat_operator._append_builder_proposal_nudge(runtime, "reply") == "reply"
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Auto skill",
        summary="Learn it",
        confidence=0.8,
        approval_mode="skill",
    )
    class AutoRuntime:
        def store_builder_proposal(self, proposal, actor):
            return SimpleNamespace(status="pending", proposal_id="prop-auto")

        def accept_stored_builder_skill_proposal(self, proposal_id, actor):
            return SimpleNamespace(title="Auto skill", skill_id="skill-auto")

    auto_runtime = AutoRuntime()
    chat_operator._auto_accept_proposal(auto_runtime, proposal, source="test")
    assert chat_operator._NEWLY_LEARNED_SKILLS[auto_runtime] == ["Auto skill"]

    monkeypatch.setattr(chat_operator, "_render_chat_turn", lambda *args, **kwargs: "chat rendered")
    assert chat_operator.handle_chat_operator_message(runtime, "hello", chat_id="123", settings=settings) == "chat rendered"
    pending = ApprovalRequest(
        approval_id="ap-auto",
        requested_by="telegram:123",
        action="use_tool",
        resource="web_fetch",
        status=ApprovalStatus.PENDING,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        context={},
    )
    runtime.store.add_approval_request(pending)
    monkeypatch.setattr(chat_operator, "handle_operator_command", lambda *args, **kwargs: "Approved request ap-auto.")
    monkeypatch.setattr(chat_operator, "resume_approved_telegram_request", lambda *args, **kwargs: "resumed")
    assert "Continuing the approved request" in chat_operator.handle_chat_operator_message(
        runtime,
        "approve",
        chat_id="123",
        settings=settings,
    )


def test_telegram_basic_helpers_cards_and_doctor_copy(monkeypatch) -> None:
    user = SimpleNamespace(first_name="Ada", last_name="Lovelace", username="ada")
    message = SimpleNamespace(
        text=None,
        caption="caption",
        message_id=7,
        from_user=user,
        chat=SimpleNamespace(id=123),
    )
    update = SimpleNamespace(update_id=99)

    assert telegram_app._get_telegram_channel_label(message, "123") == "Telegram \u00b7 Ada Lovelace"
    assert telegram_app._message_text_or_caption(message) == "caption"
    assert telegram_app._telegram_request_id(update) == "telegram-update:99"
    assert telegram_app._telegram_message_id(message=message, chat_id="123") == "telegram-message:123:7"
    assert telegram_app._ingress_dedupe_key(request_id=None, message_id="m") == "m"
    assert telegram_app._principal_id_for_telegram_message(message, None) == "telegram_chat"
    assert telegram_app._attachments_include_video([{"name": "clip.mp4"}]) is True
    assert telegram_app._split_telegram_message_chunks("a\n\nb", limit=3) == ["a", "b"]
    assert telegram_app._is_valid_telegram_bot_token("123456:abcdefghijklmnopqrstuvwxyz") is True
    assert "browser challenge" in telegram_app._operator_visible_error(RuntimeError("<html>Cloudflare challenge")).lower()
    assert "network client needs to reconnect" in telegram_app._operator_visible_error(RuntimeError("event loop is closed"))

    data = telegram_app._build_callback_data(kind="approval", action="allow_once", record_id="ap-1")
    assert telegram_app._parse_callback_data(data) == ("approval", "allow_once", "ap-1")
    assert telegram_app._parse_callback_data("bad") is None
    assert telegram_app._approval_title("terminal_exec") == "💻 Run this command?"
    assert telegram_app._approval_target_host("https://www.example.com/path") == "example.com"
    assert telegram_app._approval_is_web_request("web_fetch", "https://example.com") is True
    assert "Allow all web domains" in telegram_app._approval_copy_for("web_fetch", "https://example.com")
    assert telegram_app._should_disable_web_preview("\u2705 Approval inbox\n\nNone") is True
    assert telegram_app._activity_icon("running") == "\u2192"
    assert telegram_app._activity_icon("other") == "\u2022"

    action = {
        "action_id": "doctor-1234567890",
        "status": "pending",
        "severity": "high",
        "summary": "Routed health issue",
        "reason": "source=telegram_chat;issue_type=timeout;stage=model;detail=Timed out",
        "recommendation_code": "investigate_timeout",
    }
    assert telegram_app._doctor_reason_fields(action)["detail"] == "Timed out"
    assert telegram_app._doctor_title_text(action) == "Workflow timed out"
    assert "timeout threshold" in telegram_app._doctor_diagnosis_text(action)
    assert "Inspect recent run activity" in telegram_app._doctor_suggestion_text(action)
    assert "Doctor: Workflow timed out" in telegram_app._doctor_card_text(action)
    assert telegram_app._extract_safe_alternatives("I can't do that\n- Summarize the policy\n1. Draft a safe note") == [
        "Summarize the policy",
        "Draft a safe note",
    ]
    assert telegram_app._extract_safe_alternatives("Sure, I can help") == []


def test_telegram_cards_and_decision_actions(tmp_path, monkeypatch) -> None:
    runtime = Runtime()
    service = SimpleNamespace(runtime=runtime, model_client=None)

    assert isinstance(telegram_app.build_telegram_bot_commands(), list)
    assert telegram_app._get_telegram_channel_label(None, "123") == "Telegram \u00b7 123"
    assert telegram_app._telegram_attachment_caption_kwargs(None) == (None, {}, False)
    long_caption, kwargs, too_long = telegram_app._telegram_attachment_caption_kwargs("x" * 1200)
    assert long_caption is None and kwargs == {} and too_long is True

    assert telegram_app._approval_card_actions(_approval("ap-web", context={"tool_name": "web_fetch", "tool_input": {"url": "https://example.com"}}))[0][1] == "allow_session"
    assert telegram_app._approval_detail("", "", "abcdef1234") == "requested action \u00b7 request abcdef12"
    boundary_approval = ApprovalRequest(
        approval_id="ap-web",
        requested_by="telegram:1",
        action="allow_boundary",
        resource="https://example.com",
        status=ApprovalStatus.PENDING,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        request_kind="boundary_policy",
        context={"boundary_kind": "outbound_network", "target": "https://example.com"},
    )
    boundary_card = telegram_app._approval_card_text(boundary_approval)
    assert boundary_card.startswith("🛡️ Allow web access?")
    assert "Requested URL: `example.com`" in boundary_card
    assert "`https://example.com`" in boundary_card
    assert telegram_app._doctor_detail_text({"reason": "source=telegram;issue_type=quota;detail=Exceeded your current quota"}) == "Exceeded your current quota \u2014 telegram \u00b7 quota"
    assert telegram_app._doctor_title_text({"reason": "detail=chat backend failed"}) == "Chat backend unavailable"
    assert telegram_app._doctor_suggestion_text({"recommendation_code": "repair_missing_capsule_reference"}) == "Repair the schedule by selecting a valid capsule or remove the stale scheduled task."

    monkeypatch.setattr(telegram_app, "handle_operator_command", lambda runtime, command, **kwargs: f"handled {command}")
    assert telegram_app._execute_decision_action(service, kind="proposal", action="accept", record_id="prop-1") == (
        "Accepted",
        "handled /accept-proposal prop-1",
    )
    assert telegram_app._execute_decision_action(service, kind="nav", action="show", record_id="commands")[0] == "\u2139\ufe0f"

    runtime.store.approvals = [_approval("ap-deny")]
    assert telegram_app._execute_decision_action(service, kind="approval", action="deny", record_id="ap-deny") == (
        "Denied web access",
        "🚫 🛡️ Denied web access: `web_fetch`. I'll stop here.",
    )
    assert runtime.denied == ["operator:ap-deny"]

    runtime.start_doctor_action = lambda action_id: {"action_id": action_id, "status": "running", "summary": "Started"}
    runtime.complete_doctor_action = lambda action_id: {"action_id": action_id, "status": "completed"}
    runtime.cancel_doctor_action = lambda action_id, reason: {"action_id": action_id, "status": "cancelled", "reason": reason}
    assert telegram_app._execute_decision_action(service, kind="doctor", action="start", record_id="doc-1")[0] == "Started"
    assert telegram_app._execute_decision_action(service, kind="doctor", action="complete", record_id="doc-1") == ("Completed", "Action completed.")
    assert telegram_app._execute_decision_action(service, kind="doctor", action="cancel", record_id="doc-1") == ("Dismissed", "Action dismissed.")

    assert telegram_app._execute_decision_action(service, kind="setting", action="set", record_id="verbose_full", chat_id="123")[0] == "Verbose full"
    assert telegram_app._execute_decision_action(service, kind="setting", action="set", record_id="streaming_on", chat_id="123") == (
        "Streaming on",
        "Chat streaming is on.",
    )
    monkeypatch.setattr(telegram_app, "chat_model_option_for_token", lambda token, **kwargs: {"provider": "openai", "model": "gpt-test"})
    assert telegram_app._execute_decision_action(service, kind="model", action="select", record_id="openai.0") == (
        "Model selected",
        "handled /model openai gpt-test",
    )

    runtime.store.scheduled_tasks = {"rem-1": object()}
    runtime.store.remove_reminder = lambda task_id: True
    runtime.store.get_reminder = lambda task_id: SimpleNamespace(text="stretch")
    assert telegram_app._execute_decision_action(service, kind="reminder", action="cancel", record_id="rem-1") == (
        "Cancelled",
        "\u23f0 Reminder cancelled.",
    )
    assert telegram_app._execute_decision_action(service, kind="reminder", action="edit_time", record_id="rem-1")[0] == "Edit"

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123"})
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
        model_client=SimpleNamespace(name="old"),
    )
    monkeypatch.setattr("nullion.model_clients.clone_model_client_with_model", lambda client, model_name: SimpleNamespace(name=model_name))
    monkeypatch.setattr("nullion.model_clients.build_model_client_from_settings", lambda settings: SimpleNamespace(name=settings.model.openai_model))
    monkeypatch.setattr("nullion.agent_orchestrator.AgentOrchestrator", lambda model_client: SimpleNamespace(model_client=model_client))
    svc.swap_model_client("gpt-new")
    assert svc.model_client.name == "gpt-new"
    svc.swap_provider_model_client("openai", "gpt-provider")
    assert svc.model_client.name == "gpt-provider"

    monkeypatch.setenv("NULLION_VIDEO_INPUT_ENABLED", "true")
    monkeypatch.setenv("NULLION_VIDEO_INPUT_PROVIDER", "openai")
    monkeypatch.setenv("NULLION_VIDEO_INPUT_MODEL", "gpt-video")
    monkeypatch.setattr("nullion.providers._media_settings_for_model", lambda provider, model: SimpleNamespace(model=SimpleNamespace(openai_model=model)))
    monkeypatch.setattr(telegram_app, "build_model_client_from_settings", lambda settings: SimpleNamespace(name=settings.model.openai_model))
    monkeypatch.setattr(telegram_app, "AgentOrchestrator", lambda model_client: SimpleNamespace(model_client=model_client))
    media_client, media_orchestrator = svc._media_model_for_attachments([{"name": "clip.mp4"}])
    assert media_client.name == "gpt-video"
    assert media_orchestrator.model_client is media_client


def test_telegram_decision_snapshot_cards_and_delivery_edges(tmp_path, monkeypatch) -> None:
    runtime = Runtime()
    before = telegram_app.DecisionSnapshot(
        pending_approval_ids=frozenset(),
        pending_builder_proposal_ids=frozenset(),
        pending_doctor_action_ids=frozenset(),
    )
    runtime.store.approvals = [_approval("ap-new")]
    assert telegram_app._capture_decision_snapshot(runtime).pending_approval_ids == frozenset({"ap-new"})
    approval_card = telegram_app._new_decision_card(runtime, before)
    assert approval_card is not None and "access" in approval_card.text.lower()
    assert telegram_app._existing_pending_approval_card(runtime, "Tool approval requested: ap-new") is not None
    assert telegram_app._existing_pending_approval_card(runtime, "Approval required before Nullion can continue.") is not None

    runtime.store.approvals = []
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Coverage helper",
        summary="Add tests.",
        confidence=0.9,
        approval_mode="skill",
    )
    runtime.list_pending_builder_proposals = lambda: [
        BuilderProposalRecord(
            proposal_id="prop-new",
            proposal=proposal,
            status="pending",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    proposal_card = telegram_app._new_decision_card(runtime, before)
    assert proposal_card is not None and "Coverage helper" in proposal_card.text

    runtime.list_pending_builder_proposals = lambda: []
    runtime.store.actions = [{"action_id": "doc-new", "status": telegram_app.DOCTOR_ACTION_PENDING, "severity": "low", "summary": "Check health"}]
    doctor_card = telegram_app._new_decision_card(runtime, before)
    assert doctor_card is not None and doctor_card.supplemental is True

    runtime.store.reminders = {
        "rem-new": SimpleNamespace(task_id="rem-new", text="stand up", due_at=datetime(2026, 1, 1, tzinfo=UTC))
    }
    runtime.store.get_reminder = lambda task_id: runtime.store.reminders.get(task_id)
    reminder_card = telegram_app._check_new_reminder_card(runtime, frozenset())
    assert reminder_card is not None and "stand up" in reminder_card.text

    alternatives = telegram_app._extract_safe_alternatives("I won't do that\n- Write a summary")
    markup = telegram_app._build_suggestion_markup(alternatives)
    if telegram_app.InlineKeyboardButton is not None:
        assert markup is not None

    class EditOnly:
        def __init__(self) -> None:
            self.edits = []

        async def edit_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    message = EditOnly()
    monkeypatch.setattr(
        telegram_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, principal_id=None, allow_attachments=True: SimpleNamespace(
            text=reply,
            attachments=(),
        ),
    )
    asyncio.run(telegram_app._deliver_reply(message, "edited reply"))
    assert message.edits[-1][0] == "edited reply"

    class BadRequest(Exception):
        pass

    class FormattingFailureMessage:
        def __init__(self) -> None:
            self.replies = []

        async def reply_text(self, text, **kwargs):
            self.replies.append((text, kwargs))
            if kwargs.get("parse_mode") == "HTML":
                raise BadRequest("Can't parse entities: unmatched end tag")

    formatting_failure = FormattingFailureMessage()
    asyncio.run(telegram_app._deliver_reply(formatting_failure, "Use **bold**"))
    assert len(formatting_failure.replies) == 2
    assert formatting_failure.replies[0][1]["parse_mode"] == "HTML"
    assert "parse_mode" not in formatting_failure.replies[1][1]
    assert formatting_failure.replies[1][0].startswith("Telegram could not render the formatted reply")
    assert "```text\nUse **bold**\n```" in formatting_failure.replies[1][0]


def test_telegram_card_error_and_run_polling_helpers(tmp_path, monkeypatch) -> None:
    runtime = Runtime()
    monkeypatch.setattr("nullion.runtime_config.current_runtime_config", lambda model_client=None: SimpleNamespace(provider="openai", model="gpt"))
    monkeypatch.setattr(telegram_app, "chat_model_options", lambda **kwargs: [
        {"provider": "openai", "model": "gpt-" + "x" * 60},
        {"provider": "anthropic", "model": "claude"},
    ])
    monkeypatch.setattr("nullion.operator_commands._render_models", lambda runtime: "models text")
    models_card = telegram_app._build_models_card(runtime)
    if telegram_app.InlineKeyboardButton is not None:
        assert models_card is not None and models_card.text == "models text"
    monkeypatch.setattr(telegram_app, "chat_model_options", lambda **kwargs: [])
    assert telegram_app._build_models_card(runtime) is None

    assert telegram_app._approval_title("security permission") == "🛡️ Allow external access?"
    assert telegram_app._approval_title("send_email") == "✉️ Send this message?"
    assert telegram_app._approval_title("package_install") == "📦 Install package?"
    assert telegram_app._approval_target_host("not a url with spaces")

    class FailureMessage:
        def __init__(self) -> None:
            self.replies = []
            self.edits = []

        async def reply_text(self, text, **kwargs):
            self.replies.append((text, kwargs))

        async def edit_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    asyncio.run(telegram_app._send_telegram_delivery_failure(FailureMessage(), SimpleNamespace(text="bad", attachments=()), do_quote=False))
    edit_only = SimpleNamespace(edits=[], edit_text=lambda text, **kwargs: edit_only.edits.append((text, kwargs)))

    async def async_edit(text, **kwargs):
        edit_only.edits.append((text, kwargs))

    edit_only.edit_text = async_edit
    asyncio.run(telegram_app._send_telegram_delivery_failure(edit_only, SimpleNamespace(text="bad", attachments=()), do_quote=False))
    with pytest.raises(AttributeError):
        asyncio.run(telegram_app._send_telegram_delivery_failure(object(), SimpleNamespace(text="bad", attachments=()), do_quote=False))

    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    deliver_holder = {}
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
        model_client=object(),
        agent_orchestrator=SimpleNamespace(set_deliver_fn=lambda fn: deliver_holder.setdefault("fn", fn)),
    )
    sent_operator_messages = []
    status_calls = []

    async def fake_send_operator_message(bot_token, chat_id, text, **kwargs):
        sent_operator_messages.append((chat_id, text, kwargs))
        return None

    async def fake_send_or_edit_status(bot, status_messages, **kwargs):
        status_calls.append(kwargs)

    monkeypatch.setattr(telegram_app, "_send_operator_telegram_message", fake_send_operator_message, raising=False)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_status_message", fake_send_or_edit_status, raising=False)
    monkeypatch.setattr(type(svc), "register_handlers", lambda self, app: setattr(app, "registered", True))
    monkeypatch.setattr(type(svc), "_build_health_monitor", lambda self, app: SimpleNamespace(start=lambda: None, stop=lambda: None))

    class App:
        def __init__(self) -> None:
            self.bot = SimpleNamespace(send_message=lambda *args, **kwargs: None)
            self.ran = False

        def add_handler(self, handler):
            pass

        def post_init(self, fn):
            self.post_init_fn = fn

        def post_shutdown(self, fn):
            self.post_shutdown_fn = fn

        def run_polling(self, **kwargs):
            self.ran = True
            self.kwargs = kwargs

    app = App()
    returned = svc.run_polling(application=app)
    assert returned is app and app.ran is True
    deliver_holder["fn"]("telegram:123", "progress", is_status=True, group_id="g", status_kind="progress_note")
    deliver_holder["fn"]("telegram:123", "status", is_status=True, group_id="g", status_kind="task_summary")
    deliver_holder["fn"]("telegram:123", "hello")
    deliver_holder["fn"]("telegram:123", "/tmp/report.pdf", is_artifact=True)
    assert not any(call.get("text") == "progress" for call in status_calls)
    assert ("123", "hello", {"principal_id": "telegram_chat"}) in sent_operator_messages
    assert ("123", "MEDIA:/tmp/report.pdf", {"principal_id": "telegram_chat"}) in sent_operator_messages


def test_telegram_refresh_busy_ack_and_registration_helpers(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "refresh-runtime.db")
    assert telegram_app._refresh_runtime_from_checkpoint(SimpleNamespace(checkpoint_path=None)) is False
    assert telegram_app._refresh_runtime_from_checkpoint(SimpleNamespace(checkpoint_path=tmp_path / "missing.json")) is False
    checkpoint = tmp_path / "runtime.json"
    checkpoint.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(telegram_app, "load_runtime_store", lambda path: SimpleNamespace(loaded=True))
    monkeypatch.setattr(telegram_app, "render_runtime_store_payload_json", lambda store: "payload")
    holder = SimpleNamespace(checkpoint_path=checkpoint, store=None, last_checkpoint_fingerprint=None)
    assert telegram_app._refresh_runtime_from_checkpoint(holder) is True
    assert holder.last_checkpoint_fingerprint == "payload"
    monkeypatch.setattr(telegram_app, "load_runtime_store", lambda path: (_ for _ in ()).throw(RuntimeError("bad")))
    assert telegram_app._refresh_runtime_from_checkpoint(holder) is False

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    assert telegram_app._busy_chat_ack_text(None, chat_id="123", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("/status", chat_id="123", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("thanks", chat_id="123", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("do useful work", chat_id="999", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("do useful work", chat_id="123", settings=settings)
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    waiting_ack = telegram_app._busy_chat_ack_text(
        "add images too",
        chat_id="123",
        settings=settings,
        dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
            dependency_turn_ids=("turn:first",),
        ),
    )
    assert waiting_ack is not None
    assert "parallel" not in waiting_ack.lower()

    class Bot:
        def __init__(self) -> None:
            self.commands = None

        async def set_my_commands(self, commands):
            self.commands = commands

    bot = Bot()
    assert asyncio.run(telegram_app.register_telegram_bot_commands(bot)) is True
    assert bot.commands
    original_bot_command = telegram_app.BotCommand
    monkeypatch.setattr(telegram_app, "BotCommand", None)
    assert telegram_app.build_telegram_bot_commands() == []
    assert asyncio.run(telegram_app.register_telegram_bot_commands(bot)) is False
    monkeypatch.setattr(telegram_app, "BotCommand", original_bot_command)


def test_operator_command_rendering_approval_and_grant_helpers(monkeypatch) -> None:
    runtime = Runtime()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    pending = _approval("ap-pending", context={"tool_name": "web_fetch", "tool_input": {"url": "https://example.com"}})
    approved = _approval("ap-approved", status=ApprovalStatus.APPROVED)
    runtime.store.approvals = [approved, pending]
    active = _grant("grant-active", expires_at=now + timedelta(days=1))
    expired = _grant("grant-expired", expires_at=now - timedelta(days=1))
    revoked = _grant("grant-revoked", revoked_at=now)
    runtime.store.grants = [revoked, expired, active]

    assert operator_commands.operator_command_catalog()
    assert any(command == "help" for command, _description in operator_commands.telegram_bot_command_menu())
    assert operator_commands.normalize_operator_command_head("/help@NullionBot") == "/help"
    assert operator_commands._humanize_operator_label("needs_review") == "Needs review"
    lines = ["Header", ""]
    assert operator_commands._append_next(lines, "Do it")[-1] == "  Do it"
    assert operator_commands._display_ref("Item", 2) == "2. Item"
    assert operator_commands._format_uptime_duration(3661) == "1h 1m 1s"
    assert operator_commands._normalize_mention_suffix("abc@bot") == "abc"
    assert operator_commands._operator_timestamp_label("2026-01-01T00:00:00+00:00")
    assert operator_commands._event_timestamp_label({"created_at": "raw"}) == "raw"
    assert "Uptime:" in operator_commands._render_uptime(runtime, now=now + timedelta(seconds=61))
    assert "Nullion" in operator_commands._render_version(runtime)
    assert operator_commands._backup_human_label({"name": "missing-runtime-store.json.bak.2"}).startswith("missing-runtime-store")

    assert operator_commands._approval_permissions_for_request(pending) == ["tool:web_fetch"]
    assert "web_fetch" in operator_commands._approval_list_line(pending)
    assert operator_commands._parse_filter_tokens(["status=pending"]) == {"status": "pending"}
    assert operator_commands._parse_filter_tokens(["bad"]) is None
    assert operator_commands._grant_state(active, now=now) == "active"
    assert operator_commands._grant_state(expired, now=now) == "expired"
    assert operator_commands._grant_state(revoked, now=now) == "revoked"
    assert operator_commands._resolve_approval_token(runtime, "1") == pending
    assert operator_commands._resolve_approval_token(runtime, "ap-approved") == approved
    assert operator_commands._resolve_approval_token(runtime, "missing") is None
    assert operator_commands._resolve_grant_token(runtime, "1") == active
    assert operator_commands._resolve_grant_token(runtime, "grant-revoked") == revoked
    assert "Approval inbox" in operator_commands._render_approvals(runtime)
    assert "No approval requests matched" in operator_commands._render_approvals(runtime, ["status=denied"])
    assert "Approval request" in operator_commands._render_approval(runtime, "ap-pending")
    assert operator_commands._render_approval(runtime, None).startswith("Usage")
    assert operator_commands._deny_request(runtime, "ap-pending") == "Denied request ap-pending."
    assert runtime.denied == ["operator:ap-pending"]
    assert "Permission grants" in operator_commands._render_grants(runtime, ["status=all"])
    assert "Permission grant" in operator_commands._render_grant(runtime, "grant-active")
    assert operator_commands._revoke_grant(runtime, "grant-active") == "Revoked grant grant-active."
    assert runtime.revoked == ["operator:grant-active"]
    assert operator_commands._parse_builder_proposal_page(None) == 1
    assert operator_commands._parse_builder_proposal_page(["page=2"]) == 2
    assert operator_commands._parse_builder_proposal_page(["0"]) is None


def test_operator_commands_cover_builder_memory_models_and_misc_routes(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    settings = load_settings(env={"NULLION_ENABLED_SKILL_PACKS": "nullion/web-research"})
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Ship coverage workflow",
        summary="Capture the coverage workflow.",
        confidence=0.84,
        approval_mode="skill",
        suggested_skill_title="Coverage Workflow",
        suggested_trigger="When raising coverage",
        suggested_steps=("Run coverage", "Add tests"),
        suggested_tags=("coverage",),
    )
    runtime.store.add_builder_proposal(
        BuilderProposalRecord(
            proposal_id="prop-1",
            proposal=proposal,
            status="pending",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    memory = UserMemoryEntry(
        entry_id="mem-1",
        owner="workspace:workspace_admin",
        kind=UserMemoryKind.PREFERENCE,
        key="editor",
        value="vim",
        source="test",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    runtime.store.add_user_memory_entry(memory)
    monkeypatch.setattr(type(runtime), "list_backups", lambda self: [{"generation": 0, "name": "runtime-store.json.bak", "modified_at": 0}])
    monkeypatch.setattr(type(runtime), "latest_restore_metadata", lambda self: {"source": "backup", "generation": 0})
    monkeypatch.setattr(type(runtime), "restore_from_backup", lambda self, generation=0: None)
    registry = SimpleNamespace(
        list_specs=lambda: [
            ToolSpec("web_fetch", "Fetch", ToolRiskLevel.MEDIUM, ToolSideEffectClass.READ, True, 10),
            ToolSpec("web_search", "Search", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 10),
        ],
        list_installed_plugins=lambda: ["browser_plugin"],
    )
    monkeypatch.setattr(type(runtime), "active_tool_registry", property(lambda self: registry))

    monkeypatch.setattr(operator_commands, "_render_codebase", lambda rt: "codebase summary")
    monkeypatch.setattr(operator_commands, "_render_system_context", lambda rt: "system context")
    monkeypatch.setattr(operator_commands, "_runtime_settings", lambda rt: settings)
    monkeypatch.setattr(operator_commands, "_run_update_flow", lambda **kwargs: SimpleNamespace(success=True, from_version="1", to_version="2", steps=[]))
    monkeypatch.setattr(operator_commands, "_do_restart", lambda *args, **kwargs: "restart")
    monkeypatch.setattr(operator_commands, "current_runtime_config", lambda model_client=None: SimpleNamespace(provider="openai", model="gpt", admin_forced_model=None, memory_enabled=True, web_access=True, browser_enabled=True, file_access=True, terminal_enabled=True, telegram_configured=True))
    monkeypatch.setattr(operator_commands, "_read_credentials", lambda: {"provider": "openai", "keys": {"openai": "sk"}, "models": {"openai": "gpt,gpt-mini"}})
    monkeypatch.setattr(operator_commands, "_write_credentials", lambda creds: None)
    monkeypatch.setattr(operator_commands, "persist_model_name", lambda model: None)

    assert "Builder proposals" in operator_commands.handle_operator_command(runtime, "/proposals")
    assert "Ship coverage workflow" in operator_commands.handle_operator_command(runtime, "/proposal 1")
    assert "Accepted Builder proposal" in operator_commands.handle_operator_command(runtime, "/accept-proposal prop-1")
    runtime.store.add_builder_proposal(replace(runtime.store.get_builder_proposal("prop-1"), status="pending", accepted_skill_id=None, resolved_at=None))
    assert "Rejected Builder proposal" in operator_commands.handle_operator_command(runtime, "/reject-proposal prop-1")
    runtime.store.add_builder_proposal(replace(runtime.store.get_builder_proposal("prop-1"), status="pending", resolved_at=None))
    assert "Archived Builder proposal" in operator_commands.handle_operator_command(runtime, "/archive-proposal prop-1")
    assert "Saved skills" in operator_commands.handle_operator_command(runtime, "/skills")
    assert "Skill history" in operator_commands.handle_operator_command(runtime, "/skill-history 1")
    assert "Updated skill" in operator_commands.handle_operator_command(runtime, "/update-skill 1 title Better Coverage")
    assert "Reverted skill" in operator_commands.handle_operator_command(runtime, "/revert-skill 1 1")
    assert "Memory" in operator_commands.handle_operator_command(runtime, "/memory", memory_owner="workspace:workspace_admin")
    assert "Deleted memory item" in operator_commands.handle_operator_command(runtime, "/memory delete mem-1", memory_owner="workspace:workspace_admin")
    assert "models" in operator_commands.handle_operator_command(runtime, "/models").lower()
    assert "Switched" in operator_commands.handle_operator_command(runtime, "/model openai gpt-mini", service=SimpleNamespace(swap_provider_model_client=lambda provider, model: None))
    assert "Backups" in operator_commands.handle_operator_command(runtime, "/backups")
    assert "Restored latest backup" in operator_commands.handle_operator_command(runtime, "/restore")
    assert "Plugin catalog" in operator_commands.handle_operator_command(runtime, "/plugins available")
    assert "Browser" in operator_commands.handle_operator_command(runtime, "/plugin browser_plugin")
    assert "Skill pack catalog" in operator_commands.handle_operator_command(runtime, "/skill-packs available")
    assert "Web Research" in operator_commands.handle_operator_command(runtime, "/skill-pack nullion/web-research")
    assert "What I can do" in operator_commands.handle_operator_command(runtime, "/tools")
    assert operator_commands.handle_operator_command(runtime, "/system-context") == "system context"
    assert operator_commands.handle_operator_command(runtime, "/codebase") == "codebase summary"
    monkeypatch.setattr(
        operator_commands,
        "list_platform_delivery_receipts",
        lambda limit=10, status=None: [
            {
                "channel": "telegram",
                "target_id": "123",
                "status": status or "succeeded",
                "attachment_count": 0,
                "attachment_required": True,
                "unavailable_attachment_count": 1,
                "error": "artifact_unavailable",
                "created_at": "2026-01-01T00:00:00+00:00",
            }
        ],
    )
    assert "Delivery receipts" in operator_commands.handle_operator_command(runtime, "/deliveries")
    assert "telegram:123" in operator_commands.handle_operator_command(runtime, "/deliveries all")
    assert "Updated successfully" in operator_commands.handle_operator_command(runtime, "/update --force")
    assert operator_commands.handle_operator_command(runtime, "/ping") == "Pong."
    assert operator_commands.handle_operator_command(runtime, "/unknown").startswith("Unknown command.")


def test_operator_commands_status_doctor_readme_and_reminder_paths(tmp_path, monkeypatch) -> None:
    runtime = Runtime()
    action = {
        "action_id": "doc-1",
        "severity": "high",
        "action_type": "repair",
        "status": "pending",
        "summary": "Fix stalled task",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:01:00+00:00",
        "details": "details",
    }
    runtime.store.actions = [action]
    snapshot = {
        "counts": {
            "running_capsules": 2,
            "pending_doctor_actions": 1,
            "open_sentinel_escalations": 1,
        },
        "capsules": [{"state": "blocked"}],
        "missions": [{"status": "blocked"}, {"status": "running"}],
        "doctor_actions": [action],
        "permission_grants": [{"grant_state": "active"}],
    }
    monkeypatch.setattr(operator_commands, "build_runtime_status_snapshot", lambda store: snapshot)
    monkeypatch.setattr(operator_commands, "compute_approval_pressure", lambda snapshot: {"pending_approval_requests": 1})
    monkeypatch.setattr(operator_commands, "current_runtime_config", lambda model_client=None: SimpleNamespace(provider="openai", model="gpt", memory_enabled=True, web_access=True, browser_enabled=False, file_access=True, terminal_enabled=False, telegram_configured=True))

    assert "Approvals: 1 pending" in operator_commands._render_status(runtime)
    assert "Some tasks are blocked" in operator_commands._render_health(runtime)
    assert "Fix stalled task" in operator_commands._render_doctor(runtime)
    assert "Doctor action" in operator_commands._render_doctor_action(runtime, "1")
    runtime.start_doctor_action = lambda action_id: {"status": "running"}
    runtime.complete_doctor_action = lambda action_id: {"status": "completed"}
    runtime.cancel_doctor_action = lambda action_id, reason: {"status": "cancelled"}
    assert "Started Doctor action doc-1" in operator_commands._doctor_action_cmd(runtime, "start", "1")
    assert "Completed Doctor action doc-1" in operator_commands._doctor_action_cmd(runtime, "complete", "doc-1")
    assert "Dismissed Doctor action doc-1" in operator_commands._doctor_action_cmd(runtime, "dismiss", "doc-1")
    assert operator_commands._doctor_action_cmd(runtime, "start", None).startswith("Usage")
    monkeypatch.setattr(operator_commands, "execute_doctor_playbook_command", lambda *args, **kwargs: SimpleNamespace(message="playbook ran"))
    assert operator_commands._doctor_run_cmd(runtime, "1", "doctor:repair") == "playbook ran"
    assert operator_commands._doctor_run_cmd(runtime, None, None).startswith("Usage")

    runtime.store.remove_reminder = lambda task_id: task_id == "rem-1"
    runtime.store.scheduled_tasks = {"rem-1": object()}
    assert operator_commands._reminder_cmd(runtime, ["/reminder"]).startswith("Usage")
    assert operator_commands._reminder_cmd(runtime, ["/reminder", "cancel", "rem-1"]) == "Cancelled reminder rem-1."
    assert operator_commands._reminder_cmd(runtime, ["/reminder", "cancel", "missing"]) == "Reminder not found: missing"

    readme = tmp_path / "README.md"
    readme.write_text(
        "# Project Nullion\n\nA local assistant.\n\n## Goals\n- Reliable chat\n\n## Initial focus\n- Tests\n\n## Other\n- Ignore\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(operator_commands, "_README_PATH", readme)
    assert operator_commands._readme_project_context() == ("A local assistant.", ("Reliable chat",), ("Tests",))

    registry = SimpleNamespace(
        list_specs=lambda: [
            SimpleNamespace(name="web_fetch", availability="available", requires_approval=True),
            SimpleNamespace(name="file_read", availability="available", requires_approval=False),
            SimpleNamespace(name="missing_tool", availability="unavailable", requires_approval=False),
        ],
        list_installed_plugins=lambda: ["browser_plugin"],
    )
    runtime.active_tool_registry = registry
    monkeypatch.setattr(
        operator_commands,
        "build_system_context_snapshot",
        lambda tool_registry=None: SimpleNamespace(available_tools=registry.list_specs(), installed_plugins=("browser_plugin",)),
    )
    tools_text = operator_commands._render_tools(runtime)
    assert "Ready to use" in tools_text and "Available with your approval" in tools_text and "Not available" in tools_text
    assert "Connected:" in operator_commands._render_plugins(runtime, [])


def test_operator_commands_route_and_dense_list_paths(tmp_path, monkeypatch) -> None:
    runtime = Runtime()
    approvals = [
        _approval(f"ap-{index}", context={"tool_name": "web_fetch", "tool_input": {"url": f"https://example.com/{index}"}})
        for index in range(operator_commands._DENSE_LIST_LIMIT + 2)
    ]
    runtime.store.approvals = approvals
    dense = operator_commands._render_approvals(runtime, ["status=all"])
    assert "... and 2 more approval requests." in dense
    monkeypatch.setattr(operator_commands, "approve_request_with_mode", lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr("nullion.approval_decisions.is_outbound_boundary_approval", lambda approval: False)
    monkeypatch.setattr(
        "nullion.approval_decisions.approve_request_with_mode",
        lambda runtime, approval_id, **kwargs: SimpleNamespace(approval=runtime.store.get_approval_request(approval_id)),
    )
    monkeypatch.setattr("nullion.approval_decisions.approval_tool_permissions", lambda approval: ["tool:web_fetch"])
    assert "Approved request" in operator_commands._approve_request(runtime, "1")
    assert operator_commands._approve_request(runtime, None).startswith("Usage")
    assert "not found" in operator_commands._approve_request(runtime, "missing")

    settings = load_settings(env={"NULLION_ENABLED_SKILL_PACKS": "nullion/web-research"})
    runtime.settings = settings
    assert "Enabled:" in operator_commands._render_skill_packs(runtime, [])
    assert operator_commands._render_skill_packs(runtime, ["bad"]).startswith("Usage")
    assert "Unknown skill pack" in operator_commands._render_skill_pack_detail("missing-pack")

    monkeypatch.setattr(operator_commands, "_render_doctor", lambda runtime: "doctor list")
    monkeypatch.setattr(operator_commands, "_run_doctor_diagnose", lambda runtime: "diagnosis")
    monkeypatch.setattr(operator_commands, "_render_doctor_action", lambda runtime, token: f"doctor {token}")
    monkeypatch.setattr(operator_commands, "_doctor_action_cmd", lambda runtime, action, token: f"{action} {token}")
    monkeypatch.setattr(operator_commands, "_doctor_run_cmd", lambda runtime, token, command: f"run {token} {command}")
    assert operator_commands.handle_operator_command(runtime, "/doctor") == "doctor list"
    assert operator_commands.handle_operator_command(runtime, "/doctor diagnose") == "diagnosis"
    assert operator_commands.handle_operator_command(runtime, "/doctor 1") == "doctor 1"
    assert operator_commands.handle_operator_command(runtime, "/doctor start 1") == "start 1"
    assert operator_commands.handle_operator_command(runtime, "/doctor run 1 doctor:repair") == "run 1 doctor:repair"
    assert operator_commands.handle_operator_command(runtime, "/doctor too many args here now").startswith("Usage")
    assert operator_commands.handle_operator_command(runtime, "/status a b") == operator_commands._EXTRA_STATUS_ARGS
    assert operator_commands.handle_operator_command(runtime, "/status active") == "active"
    runtime.store.get_capsule = lambda capsule_id: None
    assert operator_commands.handle_operator_command(runtime, "/status missing") == "Capsule not found: missing"


@pytest.mark.asyncio
async def test_telegram_delivery_download_streamer_and_service_helpers(tmp_path, monkeypatch) -> None:
    class Message:
        def __init__(self) -> None:
            self.text = "/chat hello"
            self.caption = None
            self.chat = SimpleNamespace(id=123)
            self.replies: list[tuple[str, dict]] = []
            self.documents: list[dict] = []
            self.edits: list[tuple[str, dict]] = []

        async def reply_text(self, text, **kwargs):
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=44, edit_text=self.edit_text)

        async def edit_text(self, text, **kwargs):
            self.edits.append((text, kwargs))
            return SimpleNamespace(message_id=44)

        async def reply_document(self, document, **kwargs):
            self.documents.append({"data": document.read(), "kwargs": kwargs})

    message = Message()
    await telegram_app._reply_text_in_chunks(message, "one two three four", do_quote=True)
    assert message.replies[-1][1]["do_quote"] is True
    assert await telegram_app._reply_text_with_streaming_edits(message, "x" * 250, do_quote=False) is True

    attachment = tmp_path / "artifact.txt"
    attachment.write_text("artifact", encoding="utf-8")
    monkeypatch.setattr(
        telegram_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, principal_id=None, allow_attachments=True: SimpleNamespace(
            text="caption",
            attachments=(attachment,),
        ),
    )
    await telegram_app._deliver_reply(message, "MEDIA:" + str(attachment), principal_id="telegram_chat")
    assert message.documents[-1]["data"] == b"artifact"

    monkeypatch.setattr(
        telegram_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, principal_id=None, allow_attachments=True: SimpleNamespace(
            text=reply,
            attachments=(),
        ),
    )
    await telegram_app._deliver_reply(
        message,
        "hello",
        decision_card=telegram_app.DecisionCard(text="card `x < y`", reply_markup=object()),
    )
    assert message.replies[-1][0].startswith("card")
    assert "<code>x &lt; y</code>" in message.replies[-1][0]
    assert message.replies[-1][1]["parse_mode"] == "HTML"

    async def fail_streaming_edits(*args, **kwargs):
        raise RuntimeError("streaming failed")

    monkeypatch.setattr(telegram_app, "_reply_text_with_streaming_edits", fail_streaming_edits)
    await telegram_app._deliver_reply(
        message,
        "fallback after streaming failure",
        streaming_mode=telegram_app.ChatStreamingMode.MESSAGE_EDITS,
    )
    assert message.replies[-1][0] == "fallback after streaming failure"

    await telegram_app._send_callback_follow_up(message, "callback reply")
    assert message.replies[-1][0] == "callback reply"

    class FileByteArray:
        async def download_as_bytearray(self):
            return bytearray(b"bytes")

    class FileMemory:
        async def download_to_memory(self, *, out):
            out.write(b"memory")

    assert await telegram_app._telegram_file_bytes(FileByteArray()) == b"bytes"
    assert await telegram_app._telegram_file_bytes(FileMemory()) == b"memory"
    assert await telegram_app._telegram_file_bytes(object()) is None

    async def get_byte_file():
        return FileByteArray()

    async def get_memory_file(file_id=None):
        return FileMemory()

    file_ref = SimpleNamespace(get_file=get_byte_file)
    got = await telegram_app._telegram_get_file(file_ref, SimpleNamespace(bot=None))
    assert isinstance(got, FileByteArray)
    context = SimpleNamespace(bot=SimpleNamespace(get_file=get_memory_file))
    assert isinstance(await telegram_app._telegram_get_file(SimpleNamespace(file_id="f"), context), FileMemory)

    saved: list[dict] = []
    monkeypatch.setattr(telegram_app, "save_messaging_attachment", lambda **kwargs: saved.append(kwargs) or {"path": "/tmp/a", "media_type": kwargs["media_type"], "name": kwargs["filename"]})
    download_message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        photo=[SimpleNamespace(get_file=get_byte_file)],
        audio=SimpleNamespace(file_name="a.mp3", mime_type="audio/mpeg", get_file=get_byte_file),
        voice=None,
        video=None,
        document=SimpleNamespace(file_name="doc.txt", mime_type="text/plain", get_file=get_byte_file),
    )
    attachments = await telegram_app._download_telegram_attachments(download_message, SimpleNamespace(bot=None), settings=None)
    assert len(attachments) == 3
    assert saved[0]["principal_id"] == "telegram_chat"

    health_runtime = SimpleNamespace(issues=[], report_health_issue=lambda **kwargs: health_runtime.issues.append(kwargs))
    async def ok_action(action):
        return None

    await telegram_app._send_typing_indicator(SimpleNamespace(chat=SimpleNamespace(id=1, send_action=ok_action)), runtime=health_runtime, text="hi")

    async def failing_action(action):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await telegram_app._send_typing_indicator(SimpleNamespace(chat=SimpleNamespace(id=1, send_action=failing_action)), runtime=health_runtime, text="hi")
    assert health_runtime.issues
    assert telegram_app._should_quote_reply("/chat hi") is False
    assert telegram_app._should_quote_reply("plain") is True

    streamer = telegram_app._TelegramActivityStreamer(message)
    await streamer._update({"id": "orchestrate", "label": "Run", "status": "running", "detail": "Tools:\n  web"})
    await streamer._update({"id": "tool-1", "label": "web_fetch", "status": "done", "detail": "example.com"})
    assert "Activity" in streamer._render()
    assert streamer._detail_is_activity_sublist("\u2192 task") is True

    def handle_update_with_activity(update, *, attachments=None, activity_callback=None, append_activity_trace=True):
        return {
            "attachments": attachments,
            "activity_callback": activity_callback,
            "append_activity_trace": append_activity_trace,
        }

    service = SimpleNamespace(handle_update=handle_update_with_activity)
    assert telegram_app._call_handle_update_with_activity(service, object(), attachments=[{"x": "y"}])["attachments"] == [{"x": "y"}]

    class Bot:
        def __init__(self) -> None:
            self.sent = []
            self.edited = []

        async def send_message(self, chat_id, text, **kwargs):
            self.sent.append((chat_id, text, kwargs))
            return SimpleNamespace(message_id=55)

        async def edit_message_text(self, **kwargs):
            self.edited.append(kwargs)

    bot = Bot()
    status_messages: dict[tuple[str, str], int] = {}
    await telegram_app._send_or_edit_telegram_status_message(bot, status_messages, chat_id="123", group_id="g", text="Working")
    await telegram_app._send_or_edit_telegram_status_message(bot, status_messages, chat_id="123", group_id="g", text="Done")
    assert status_messages[("123", "g")] == 55
    assert bot.edited

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-runtime.db")
    svc = telegram_app.ChatOperatorService(runtime=runtime, bot_token="123456:abcdefghijklmnopqrstuvwxyz", operator_chat_id="123", settings=settings)
    monkeypatch.setattr(telegram_app, "handle_chat_operator_message", lambda *args, **kwargs: "handled")
    assert svc.handle_update(SimpleNamespace(update_id=5, message=message)) == "handled"
    assert svc.handle_update(SimpleNamespace(message=SimpleNamespace(chat=SimpleNamespace(id=123), text=None, caption=None))) is None
    assert svc.handle_update(SimpleNamespace(message=SimpleNamespace(chat=SimpleNamespace(id=123), text=None, caption=None)), attachments=[{"path": "/tmp/a.txt"}]) == "handled"
    assert svc._media_model_for_attachments([{"name": "clip.mp4"}]) == (svc.model_client, svc.agent_orchestrator)
    svc.swap_model_client("gpt-test")

    first_run_settings = load_settings(env={"NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    first_run = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id=None,
        settings=first_run_settings,
    )
    monkeypatch.setattr("nullion.web_app._find_env_path", lambda: None)
    setup_message = Message()
    assert await first_run._maybe_do_first_run_setup(setup_message, "999") is True
    assert first_run.operator_chat_id == "999"
    assert await first_run._maybe_do_first_run_setup(setup_message, "999") is False

    class ReminderBot:
        def __init__(self) -> None:
            self.messages = []

        async def send_message(self, chat_id, text, **kwargs):
            self.messages.append((chat_id, text, kwargs))

    await svc.deliver_due_reminders(bot=ReminderBot(), now=datetime(2026, 1, 1, tzinfo=UTC))


@pytest.mark.asyncio
async def test_telegram_on_message_callback_and_service_builders(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-flow.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict] = []

    class Message:
        def __init__(self, text="/chat hello") -> None:
            self.text = text
            self.caption = None
            self.message_id = 10
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)
            self.replies = []
            self.edits = []

        async def send_action(self, action):
            return None

        async def reply_text(self, text, **kwargs):
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88, edit_text=self.edit_text)

        async def edit_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    async def no_download(message, context, settings=None):
        return []

    async def no_typing(message, runtime, text):
        return None

    async def stop_typing(task):
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):
        deliveries.append({"message": message, "reply": reply, **kwargs})

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_build_help_menu_card", lambda: telegram_app.DecisionCard("help card", object()))
    monkeypatch.setattr(telegram_app, "_build_models_card", lambda runtime: telegram_app.DecisionCard("models card", object()))
    monkeypatch.setattr(telegram_app, "_build_verbose_settings_card", lambda runtime, chat_id=None: telegram_app.DecisionCard("verbose card", object()))
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", lambda service, update, **kwargs: "I can't do that\n- Safer summary")

    await svc.on_message(SimpleNamespace(update_id=1, message=Message("/help")), SimpleNamespace(bot=None))
    await svc.on_message(SimpleNamespace(update_id=2, message=Message("/models")), SimpleNamespace(bot=None))
    await svc.on_message(SimpleNamespace(update_id=3, message=Message("/verbose")), SimpleNamespace(bot=None))
    await svc.on_message(SimpleNamespace(update_id=4, message=Message("hello")), SimpleNamespace(bot=None))
    assert [item["reply"] for item in deliveries[:3]] == ["help card", "models card", "verbose card"]
    assert deliveries[-1]["additional_markup"] is not None or telegram_app.InlineKeyboardButton is None
    assert svc._seen_ingress_ids
    await svc.on_message(SimpleNamespace(update_id=4, message=Message("duplicate")), SimpleNamespace(bot=None))

    class Callback:
        def __init__(self, data, message=None, from_user_id=123) -> None:
            self.data = data
            self.message = message
            self.from_user = SimpleNamespace(id=from_user_id)
            self.answers = []

        async def answer(self, text, **kwargs):
            self.answers.append((text, kwargs))

    callback_message = Message("/callback")
    async def capture_callback_follow_up(message, reply, **kwargs):
        deliveries.append({"reply": reply, "follow_up": True, **kwargs})

    monkeypatch.setattr(telegram_app, "_send_callback_follow_up", capture_callback_follow_up)
    monkeypatch.setattr("nullion.chat_operator.handle_chat_operator_message", lambda *args, **kwargs: "suggestion handled")
    suggestion = Callback(telegram_app._build_callback_data(kind="suggestion", action="send", record_id="Safer summary"), callback_message)
    await svc.on_callback_query(SimpleNamespace(callback_query=suggestion), SimpleNamespace())
    assert suggestion.answers[-1][0] == "On it!"

    monkeypatch.setattr(telegram_app, "_execute_decision_action", lambda *args, **kwargs: ("Done", "decision reply"))
    decision = Callback(telegram_app._build_callback_data(kind="proposal", action="accept", record_id="prop-1"), callback_message)
    await svc.on_callback_query(SimpleNamespace(callback_query=decision), SimpleNamespace())
    assert decision.answers[-1][0] == "Done"
    assert callback_message.edits

    runtime.store.add_approval_request(
        ApprovalRequest(
            approval_id="ap-resume",
            requested_by="telegram:123",
            action="use_tool",
            resource="web_fetch",
            status=ApprovalStatus.APPROVED,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            context={},
        )
    )
    runtime.store.add_approval_request(_approval("ap-next"))
    monkeypatch.setattr(telegram_app, "_execute_decision_action", lambda *args, **kwargs: ("Approved", "approved reply"))
    monkeypatch.setattr(telegram_app, "resume_approved_telegram_request", lambda *args, **kwargs: "Tool approval requested: ap-next")
    approval_callback = Callback(
        telegram_app._build_callback_data(kind="approval", action="allow_once", record_id="ap-resume"),
        callback_message,
    )
    await svc.on_callback_query(SimpleNamespace(callback_query=approval_callback), SimpleNamespace())
    assert any(item.get("decision_card") is not None for item in deliveries)

    monkeypatch.setattr(telegram_app, "resume_approved_telegram_request", lambda *args, **kwargs: None)
    approval_callback_no_output = Callback(
        telegram_app._build_callback_data(kind="approval", action="approve", record_id="ap-resume"),
        callback_message,
    )
    await svc.on_callback_query(SimpleNamespace(callback_query=approval_callback_no_output), SimpleNamespace())
    assert any("resend your message" in item.get("reply", "") for item in deliveries)

    unauthorized = Callback(telegram_app._build_callback_data(kind="proposal", action="accept", record_id="prop-1"), callback_message, from_user_id=999)
    await svc.on_callback_query(SimpleNamespace(callback_query=unauthorized), SimpleNamespace())
    assert unauthorized.answers[-1][0] == "Unauthorized"
    bad = Callback("bad", callback_message)
    await svc.on_callback_query(SimpleNamespace(callback_query=bad), SimpleNamespace())
    assert bad.answers[-1][0] == "Unknown action"

    monkeypatch.setattr(telegram_app, "build_model_client_from_settings", lambda settings: SimpleNamespace(name="client"))
    monkeypatch.setattr(telegram_app, "AgentOrchestrator", lambda model_client: SimpleNamespace(model_client=model_client))
    built = telegram_app.build_telegram_operator_service(runtime, settings=settings)
    assert built.operator_chat_id == "123"
    built_msg = telegram_app.build_messaging_operator_service(runtime, settings=settings)
    assert built_msg.bot_token == ""
    with pytest.raises(telegram_app.MissingTelegramBotTokenError):
        telegram_app.build_telegram_operator_service(runtime, settings=load_settings(env={"NULLION_TELEGRAM_BOT_TOKEN": ""}))

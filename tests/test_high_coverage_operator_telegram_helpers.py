from __future__ import annotations

import asyncio
import base64
import inspect
import json
import os
import sqlite3
import threading
import zipfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from nullion.approvals import ApprovalRequest, ApprovalStatus, PermissionGrant
from nullion import chat_operator, messaging_adapters, operator_commands, runtime_persistence, telegram_app, telegram_transport
from nullion.builder import (
    BuilderDecisionType,
    BuilderProposal,
    BuilderProposalRecord,
)
from nullion.config import load_settings
from nullion.chat_text import make_markdown_tables_chat_readable
from nullion.memory import UserMemoryEntry, UserMemoryKind
import nullion.runtime as runtime_module
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
    extract_url_target,
)
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


def test_telegram_chat_handler_uses_shared_platform_gateway(monkeypatch, tmp_path: Path) -> None:
    from nullion import platform_chat

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    service = telegram_app.ChatOperatorService(runtime, "token", "123", settings)
    calls = []

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        calls.append((runtime_arg, request))
        return platform_chat.PlatformChatResponse(text="shared telegram reply")

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)

    dispatch_decision = SimpleNamespace(turn_id="telegram-update:opaque-turn")
    response = service.handle_text_request(
        text="hello",
        chat_id="123",
        request_id="telegram-update:fallback",
        turn_dispatch_decision=dispatch_decision,
    )

    assert response.text == "shared telegram reply"
    assert calls
    runtime_arg, request = calls[0]
    assert runtime_arg is runtime
    assert request.platform == "telegram"
    assert request.chat_id == "123"
    assert request.text == "hello"
    assert request.turn_id == "telegram-update:opaque-turn"


def test_telegram_chat_handler_preserves_shared_gateway_artifacts_for_delivery(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from nullion import platform_chat

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    service = telegram_app.ChatOperatorService(runtime, "token", "123", settings)
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    artifact = artifact_root / "dropbox_images_split.xlsx"
    artifact.write_bytes(b"fake workbook bytes")

    def fake_run_platform_chat_request(runtime_arg, request):  # noqa: ANN001
        assert runtime_arg is runtime
        assert request.platform == "telegram"
        return platform_chat.PlatformChatResponse(
            text="Done — attached the requested XLSX.",
            artifact_paths=[str(artifact)],
        )

    monkeypatch.setattr(platform_chat, "run_platform_chat_request", fake_run_platform_chat_request)

    response = service.handle_text_request(text="Generate the spreadsheet.", chat_id="123")

    assert response.text == "Done — attached the requested XLSX."
    assert response.artifact_paths == [str(artifact)]
    contract = messaging_adapters.delivery_contract_for_turn(
        "Generate the spreadsheet.",
        reply=response.text,
        artifact_paths=response.artifact_paths,
        requires_attachment_delivery=True,
    )
    delivery = messaging_adapters.prepare_reply_for_platform_delivery(
        response.text,
        principal_id="telegram:123",
        delivery_contract=contract,
        artifact_paths=response.artifact_paths,
    )
    assert delivery.attachments == (artifact,)
    assert delivery.unavailable_attachment_count == 0


def test_compacted_single_line_markdown_table_renders_as_chat_list() -> None:
    raw = (
        "| Event | NYC-area note | | |---|---| | | Carly Rae Jepsen | Pop concert/event in the NYC area | "
        "| | Cole Escola | Comedy/theater event in the NYC area |"
    )

    rendered = make_markdown_tables_chat_readable(raw)

    assert rendered == (
        "- Carly Rae Jepsen: Pop concert/event in the NYC area\n"
        "- Cole Escola: Comedy/theater event in the NYC area"
    )


def test_plain_no_tool_direct_model_reply_uses_compact_no_tool_prompt() -> None:
    calls: list[dict[str, object]] = []

    class Model:
        def create(self, **kwargs):  # noqa: ANN001
            calls.append(kwargs)
            return {"content": [{"type": "text", "text": "Compact plain reply"}]}

    reply = chat_operator._plain_no_tool_direct_model_reply(
        model_client=Model(),
        prompt="neutral plain turn",
        memory_context="Preference: keep it brief.",
        conversation_context=None,
        capability_context=(
            "Live capability inventory:\n"
            'directive before any user-facing explanation: <tool_request>{"tool":"request_tool_scope"}</tool_request>'
        ),
    )

    assert reply == "Compact plain reply"
    assert calls
    assert calls[0]["tools"] == []
    assert "Chat delivery contract (message-only turn)" in calls[0]["system"]
    assert "explicitly asks for a no-tool response" in calls[0]["system"]
    assert "No tools are available for this first-pass reply" in calls[0]["system"]
    assert "Structured capability inventory:" in calls[0]["system"]
    assert "<tool_request>" in calls[0]["system"]
    assert calls[0]["messages"] == [
        {"role": "user", "content": [{"type": "text", "text": "neutral plain turn"}]}
    ]


def test_planner_dispatch_artifact_extensions_prefer_visible_final_artifact_tool() -> None:
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(requested_artifact_extensions=(".png", ".xlsx")),
        list_specs=lambda: [SimpleNamespace(name="spreadsheet_create")],
        list_tool_definitions=lambda: [{"name": "spreadsheet_create"}],
    )

    assert chat_operator._planner_dispatch_artifact_extensions(registry, ()) == (".xlsx",)


def test_planner_dispatch_artifact_extensions_derive_final_extension_from_single_visible_artifact_tool() -> None:
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(requested_artifact_extensions=(".png",)),
        list_specs=lambda: [SimpleNamespace(name="spreadsheet_create")],
        list_tool_definitions=lambda: [{"name": "spreadsheet_create"}],
    )

    assert chat_operator._planner_dispatch_artifact_extensions(registry, ()) == (".xlsx",)


def test_browser_navigation_continuations_are_not_mandatory_retry_metadata() -> None:
    from nullion.tools import register_browser_plugin

    registry = ToolRegistry(plugin_registration_allowed=True)
    register_browser_plugin(registry, browser_navigator=lambda _url: {"result": "ok"})

    definitions = {definition["name"]: definition for definition in registry.list_tool_definitions()}

    assert "continuation_tools" not in definitions["browser_navigate"]


def test_saved_history_scope_does_not_widen_independent_turn_to_prior_tool_family() -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("browser_open", ()),
        ("browser_navigate", ()),
        ("web_fetch", ()),
        ("calendar_list", ("calendar", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = TurnToolEvidence(saved_history_available=True)
    active_registry = chat_operator.ScopedTurnToolRegistry(registry, evidence=evidence)
    history_context = "\n".join(
        [
            "Recent runtime-evidence-backed saved turns (ranked by relevance and recency):",
            "Evidence 1.",
            "Tools: browser_open, browser_navigate, web_fetch",
            "User: prior unrelated browser task",
        ]
    )

    widened = chat_operator._augment_tool_registry_from_saved_history_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        history_context=history_context,
    )

    assert widened is active_registry
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert names == {"request_tool_scope"}

    followup_widened = chat_operator._augment_tool_registry_from_saved_history_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        history_context=history_context,
        allow_unlinked_context_scope=True,
    )

    followup_names = {definition["name"] for definition in followup_widened.list_tool_definitions()}
    assert "browser_open" in followup_names
    assert "request_tool_scope" in followup_names


def test_planner_dispatch_artifact_extensions_prefer_explicit_requested_attachment_over_handoff_scope() -> None:
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(requested_artifact_extensions=(".json",)),
        list_specs=lambda: [SimpleNamespace(name="file_write"), SimpleNamespace(name="spreadsheet_create")],
        list_tool_definitions=lambda: [{"name": "file_write"}, {"name": "spreadsheet_create"}],
    )

    assert chat_operator._planner_dispatch_artifact_extensions(
        registry,
        (".xlsx",),
        available_tool_names=("file_write", "spreadsheet_create"),
    ) == (".xlsx",)


def test_planner_dispatch_artifact_extensions_use_dispatch_tool_subset_for_final_kind() -> None:
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(requested_artifact_extensions=(".png",)),
        list_specs=lambda: [
            SimpleNamespace(name="document_create"),
            SimpleNamespace(name="presentation_create"),
            SimpleNamespace(name="spreadsheet_create"),
        ],
        list_tool_definitions=lambda: [
            {"name": "document_create"},
            {"name": "presentation_create"},
            {"name": "spreadsheet_create"},
        ],
    )

    assert chat_operator._planner_dispatch_artifact_extensions(
        registry,
        (),
        available_tool_names=("archive_extract", "spreadsheet_create"),
    ) == (".xlsx",)


def test_planner_dispatch_artifact_extensions_derive_from_dispatch_tool_without_extension_candidates() -> None:
    assert chat_operator._planner_dispatch_artifact_extensions(
        None,
        (),
        available_tool_names=("archive_extract", "spreadsheet_create"),
    ) == (".xlsx",)


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


def test_numeric_source_selection_retries_visible_account_read_tools() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_search",
            description="Search email.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    result = SimpleNamespace(final_text="I can use email records, but the tool is not available.", tool_results=[])

    assert chat_operator._turn_result_should_retry_with_visible_account_read_tools(
        result,
        registry,
        numbered_option_selected=True,
    )
    assert not chat_operator._turn_result_should_retry_with_visible_account_read_tools(
        result,
        registry,
        numbered_option_selected=False,
    )


def test_numeric_source_selection_links_completed_numbered_prompt(tmp_path, monkeypatch) -> None:
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "completed-numbered-source-runtime.db")
    conversation_id = "telegram:123"
    previous_assistant = (
        "I can check, but please choose which authorized source you want me to read first.\n\n"
        "1. Email records\n"
        "2. Calendar records\n"
        "3. Another connected account/source, if available"
    )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class NumberedOptionOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.calls: list[dict] = []

        def run_turn(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text=previous_assistant if len(self.calls) == 1 else "I checked email records.",
                artifacts=[],
            )

    orchestrator = NumberedOptionOrchestrator()
    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Did I make a payment to Acme this week?",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    first_turn_id = runtime.store.get_conversation_head(conversation_id)["active_turn_id"]
    runtime.store.set_conversation_head(conversation_id, active_branch_id=None, active_turn_id=None)

    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat 1",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        turn_dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL,
            disposition=chat_operator.ConversationTurnDisposition.INDEPENDENT,
            reason="no_active_turn",
        ),
    )
    follow_up_turn_id = runtime.store.get_conversation_head(conversation_id)["active_turn_id"]
    follow_up_turn = runtime.store.get_conversation_turn(follow_up_turn_id)

    assert first == previous_assistant
    assert second == "I checked email records."
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id == first_turn_id
    assert follow_up_turn.disposition_reason == "dispatch:numbered_option_reply"


def test_numeric_connector_option_reply_does_not_get_replaced_by_builder_fallback(tmp_path, monkeypatch) -> None:
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "numbered-connector-option-runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scope_request",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    runtime.active_tool_registry = registry
    conversation_id = "telegram:123"
    previous_assistant = (
        "Choose one source to continue:\n\n"
        "1. Google Analytics Data\n"
        "2. Local CSV export"
    )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_active_connector_app_id_providers_for_message",
        lambda: ({"active_app_ids": ("google-analytics",)},),
    )

    class NumberedConnectorOptionOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.calls: list[dict] = []

        def run_turn(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text=previous_assistant if len(self.calls) == 1 else "I checked the selected source.",
                artifacts=[],
            )

    orchestrator = NumberedConnectorOptionOrchestrator()
    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Pick a source for this stage check.",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    first_turn_id = runtime.store.get_conversation_head(conversation_id)["active_turn_id"]
    runtime.store.set_conversation_head(conversation_id, active_branch_id=None, active_turn_id=None)

    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat 1",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        turn_dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL,
            disposition=chat_operator.ConversationTurnDisposition.INDEPENDENT,
            reason="no_active_turn",
        ),
    )
    follow_up_turn_id = runtime.store.get_conversation_head(conversation_id)["active_turn_id"]
    follow_up_turn = runtime.store.get_conversation_turn(follow_up_turn_id)

    assert first == previous_assistant
    assert second == "I checked the selected source."
    assert "Builder suggestion" not in second
    assert runtime.list_pending_builder_proposals() == []
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id == first_turn_id
    assert follow_up_turn.disposition_reason == "dispatch:numbered_option_reply"


def test_source_selection_scope_result_wins_over_local_fallback_text() -> None:
    results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {
                "source_selection_required": True,
                "available_sources": [
                    {
                        "display_name": "Example connector",
                        "active_app_ids": ["google-mail", "google-calendar"],
                        "read_tools": ["calendar_list", "email_read", "email_search"],
                        "permission_mode": "read",
                    }
                ],
            },
        ),
        ToolResult("exec-1", "terminal_exec", "completed", {"stdout": "noop"}),
    ]

    reply = chat_operator._account_source_selection_reply_from_scope_results(results)

    assert reply is not None
    assert "please choose which authorized source" in reply
    assert "1. Example connector: Email records" in reply
    assert "2. Example connector: Calendar records" in reply
    assert "noop" not in reply


def test_source_selection_scope_result_wins_over_accidental_account_read() -> None:
    results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {
                "source_selection_required": True,
                "available_sources": [
                    {
                        "display_name": "Example connector",
                        "active_app_ids": ["google-mail", "google-calendar"],
                        "read_tools": ["calendar_list", "email_read", "email_search"],
                        "permission_mode": "read",
                    }
                ],
            },
        ),
        ToolResult(
            "email-1",
            "email_search",
            "completed",
            {
                "results": [
                    {
                        "subject": "Leaked stage search result",
                        "from": "sender@example.com",
                        "snippet": "This should not be rendered before source selection.",
                    }
                ]
            },
        ),
    ]

    reply = chat_operator._account_source_selection_reply_from_scope_results(results)

    assert reply is not None
    assert "please choose which authorized source" in reply
    assert "1. Example connector: Email records" in reply
    assert "2. Example connector: Calendar records" in reply
    assert "Leaked stage search result" not in reply


def test_single_account_source_scope_emits_consent_choices_without_retry_context() -> None:
    results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {
                "source_selection_required": True,
                "available_sources": [
                    {
                        "display_name": "Google Mail",
                        "active_app_ids": ["google-mail"],
                        "read_tools": ["email_search", "email_read", "email_attachment_read"],
                        "permission_mode": "read",
                    }
                ],
            },
        )
    ]

    assert "2. Provide another source" in chat_operator._account_source_selection_reply_from_scope_results(results)
    assert chat_operator._account_source_selection_scope_retry_context(results) is None


def test_source_selection_scope_result_stops_common_retry_chain() -> None:
    initial_result = SimpleNamespace(
        final_text="I should be replaced by the source picker.",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "source_selection_required": True,
                    "available_sources": [
                        {
                            "display_name": "Example connector",
                            "read_tools": ["email_search", "calendar_list"],
                            "permission_mode": "read",
                        }
                    ],
                },
            )
        ],
    )

    def fail_run_turn(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("source selection should not retry into fallback tools")

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=ToolRegistry(),
        base_tool_registry=ToolRegistry(),
        model_client=object(),
        user_message="status prompt",
        evidence=None,
        run_turn=fail_run_turn,
        include_local_system_fallback=True,
    )

    assert result.result is initial_result
    assert result.retried_account_read
    assert not result.retried_local_system_fallback


def test_scope_only_plain_reply_bypasses_common_retry_chain() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="scope",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        final_text="Hi Himan - how can I help?",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[],
    )

    def fail_run_turn(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain completed replies must not enter common retry tools")

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Hi",
        evidence=TurnToolEvidence(),
        run_turn=fail_run_turn,
        include_local_system_fallback=True,
    )

    assert result.result is initial_result
    assert not result.retried_model_tool_request
    assert not result.retried_no_tool_scope_decision
    assert not result.retried_missing_tool_scope
    assert not result.retried_local_system_fallback


def test_deferred_run_cron_receipt_stops_common_retry_chain() -> None:
    initial_result = SimpleNamespace(
        final_text="Manual scheduled task run started.",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "run-1",
                "run_cron",
                "completed",
                {
                    "delivery_status": "deferred",
                    "cron_delivery_status": "deferred",
                    "message": (
                        "Manual scheduled task run started: Morning Report. "
                        "The result will be delivered to this chat when ready."
                    ),
                },
            )
        ],
    )

    def fail_run_turn(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("deferred run_cron receipt should not enter common retries")

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=ToolRegistry(),
        base_tool_registry=ToolRegistry(),
        model_client=object(),
        user_message="run the scheduler item",
        evidence=None,
        run_turn=fail_run_turn,
        include_local_system_fallback=True,
    )

    assert result.result is initial_result
    assert not result.retried_model_tool_request
    assert not result.retried_scheduler_run_scope
    assert not result.retried_scheduler_run_fallback
    assert not result.retried_local_system_fallback


def test_source_selection_scope_result_stops_before_exact_account_read_without_selection(monkeypatch) -> None:
    active_registry = ToolRegistry()
    base_registry = ToolRegistry()
    for registry in (active_registry, base_registry):
        registry.register(
            ToolSpec(
                name="request_tool_scope",
                description="scope",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    for name in ("calendar_list", "email_search"):
        base_registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("connector", "account_read"),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    initial_result = SimpleNamespace(
        final_text="I can check, but please choose which source.",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "source_selection_required": True,
                    "available_sources": [
                        {
                            "display_name": "Example connector",
                            "read_tools": ["calendar_list", "email_search"],
                            "permission_mode": "read",
                        }
                    ],
                },
            )
        ],
    )

    decision_calls: list[dict] = []

    def choose_calendar_scope(**kwargs):  # noqa: ANN003
        decision_calls.append(kwargs)
        return chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_source_user_requested=True,
            requested_tool_names=("calendar_list",),
            required_tool_names=("calendar_list",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", choose_calendar_scope)

    def run_turn(history, registry):  # noqa: ANN001
        raise AssertionError("source selection must stop before account reads")

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "Check my calendar"}]}],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="Check my calendar",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_account_read
    assert result.result is initial_result
    assert decision_calls == []


def test_source_selection_scope_result_does_not_read_email_family_without_selection(monkeypatch) -> None:
    active_registry = ToolRegistry()
    base_registry = ToolRegistry()
    for registry in (active_registry, base_registry):
        registry.register(
            ToolSpec(
                name="request_tool_scope",
                description="scope",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    for name in ("email_search", "email_read", "calendar_list"):
        base_registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("connector", "account_read"),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    initial_result = SimpleNamespace(
        final_text="I can check, but please choose which authorized source.",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "source_selection_required": True,
                    "available_sources": [
                        {
                            "display_name": "Example connector",
                            "active_app_ids": ["google-mail", "google-calendar"],
                            "read_tools": ["email_search", "email_read", "calendar_list"],
                            "permission_mode": "read",
                        }
                    ],
                },
            )
        ],
    )

    def choose_email_scope(**kwargs):  # noqa: ANN003
        raise AssertionError("source selection must stop before scope reclassification")
        return chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_app_ids=("google-mail",),
            connector_source_user_requested=True,
            requested_tool_names=("email_search", "email_read"),
            required_tool_names=("email_search",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", choose_email_scope)
    visible_tool_sets: list[set[str]] = []

    def run_turn(history, registry):  # noqa: ANN001
        raise AssertionError("source selection must stop before account reads")

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "Check the connected source"}]}],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="Check the connected source",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_account_read
    assert visible_tool_sets == []
    assert result.result is initial_result


def test_source_selection_single_account_source_stops_before_account_tools(monkeypatch) -> None:
    active_registry = ToolRegistry()
    base_registry = ToolRegistry()
    for registry in (active_registry, base_registry):
        registry.register(
            ToolSpec(
                name="request_tool_scope",
                description="scope",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    for name in ("email_search", "email_read", "email_attachment_read", "calendar_list", "browser_navigate"):
        base_registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("connector", "account_read") if name != "browser_navigate" else ("web",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    initial_result = SimpleNamespace(
        final_text="I can check, but please choose which authorized source.",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "source_selection_required": True,
                    "available_sources": [
                        {
                            "display_name": "Structured account gateway",
                            "active_app_ids": ["google-mail", "google-calendar"],
                            "read_tools": ["calendar_list", "email_attachment_read", "email_read", "email_search"],
                            "permission_mode": "read",
                        }
                    ],
                },
            )
        ],
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(valid=True, confidence=0.52),
    )
    visible_tool_sets: list[set[str]] = []

    def run_turn(history, registry):  # noqa: ANN001
        raise AssertionError("source selection must stop before account reads")

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "structured account read"}]}],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="structured account read",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_account_read
    assert visible_tool_sets == []
    assert result.result is initial_result


def test_account_read_retry_includes_source_selection_scope_context(monkeypatch) -> None:
    active_registry = ToolRegistry()
    base_registry = ToolRegistry()
    for registry in (active_registry, base_registry):
        registry.register(
            ToolSpec(
                name="request_tool_scope",
                description="scope",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    for name in ("calendar_list", "email_search"):
        base_registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("connector", "account_read"),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    initial_result = SimpleNamespace(
        final_text="I can check, but please choose which source.",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "source_selection_required": True,
                    "available_sources": [
                        {
                            "display_name": "Example connector",
                            "active_app_ids": ["google-calendar"],
                            "read_tools": ["calendar_list", "email_search"],
                            "permission_mode": "read",
                        }
                    ],
                },
            )
        ],
    )

    decision_calls: list[dict] = []

    def choose_calendar_scope(**kwargs):  # noqa: ANN003
        decision_calls.append(kwargs)
        return chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_source_user_requested=True,
            requested_tool_names=("calendar_list",),
            required_tool_names=("calendar_list",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", choose_calendar_scope)

    def run_turn(history, registry):  # noqa: ANN001
        visible = {
            str(definition.get("name") or "")
            for definition in registry.list_tool_definitions()
        }
        assert "calendar_list" in visible
        return SimpleNamespace(
            final_text="Calendar checked.",
            artifacts=[],
            suspended_for_approval=False,
            tool_results=[ToolResult("cal-1", "calendar_list", "completed", {"events": []})],
        )

    result, _registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "Structured numbered source selection"}]}],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="Structured numbered source selection",
        evidence=TurnToolEvidence(numbered_option_selected=True),
        run_turn=run_turn,
        numbered_option_selected=True,
    )

    assert retried
    assert result.final_text == "Calendar checked."
    assert decision_calls
    assert "Structured authorized account sources" in decision_calls[0]["assistant_no_tool_draft"]
    assert "calendar_list" in decision_calls[0]["assistant_no_tool_draft"]


def test_failed_browser_plus_file_search_result_allows_exact_account_read_recovery(monkeypatch) -> None:
    active_registry = ToolRegistry()
    base_registry = ToolRegistry()
    for registry in (active_registry, base_registry):
        registry.register(
            ToolSpec(
                name="request_tool_scope",
                description="scope",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    for name in ("calendar_list", "file_search", "browser_assert_page_state"):
        base_registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("connector", "account_read") if name == "calendar_list" else ("local_files",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    initial_result = SimpleNamespace(
        final_text="I found 5 files:\n- .env\n- connections.json",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult("browser-1", "browser_assert_page_state", "failed", {}),
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "capabilities": ["web"],
                    "available_tools": ["file_search"],
                    "source_selection_required": False,
                },
            ),
            ToolResult("files-1", "file_search", "completed", {"matches": [".env", "connections.json"]}),
        ],
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_source_user_requested=True,
            requested_tool_names=("calendar_list",),
            required_tool_names=("calendar_list",),
            confidence=1.0,
            valid=True,
        ),
    )

    def run_turn(history, registry):  # noqa: ANN001
        visible = {
            str(definition.get("name") or "")
            for definition in registry.list_tool_definitions()
        }
        assert visible == {"calendar_list"}
        return SimpleNamespace(
            final_text="Calendar checked.",
            artifacts=[],
            suspended_for_approval=False,
            tool_results=[ToolResult("cal-1", "calendar_list", "completed", {"events": []})],
        )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "Whats in my calendar for this weekend?"}]}],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="Whats in my calendar for this weekend?",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_account_read
    assert result.result.final_text == "Calendar checked."
    assert [tool.tool_name for tool in result.result.tool_results][-1] == "calendar_list"


def test_current_cron_reference_gate_uses_runtime_metadata_not_english(monkeypatch) -> None:
    from nullion import connections, crons

    seen: dict[str, object] = {}

    monkeypatch.setattr(connections, "workspace_id_for_principal", lambda principal_id: "workspace_admin")

    def fake_list_crons(*, workspace_id=None, refresh_next_runs=True):  # noqa: ANN001
        seen["workspace_id"] = workspace_id
        seen["refresh_next_runs"] = refresh_next_runs
        return [
            SimpleNamespace(id="abc12345", name="Stage live scheduler manual run 599d86af28"),
            SimpleNamespace(id="def67890", name="Another scheduled task"),
        ]

    monkeypatch.setattr(crons, "list_crons", fake_list_crons)

    assert chat_operator._user_turn_mentions_current_cron_reference(
        "ejecuta ahora Stage live scheduler manual run 599d86af28",
        principal_id="telegram_chat",
    )
    assert not chat_operator._user_turn_mentions_current_cron_reference(
        "ejecuta ahora una tarea diferente",
        principal_id="telegram_chat",
    )
    assert seen == {"workspace_id": "workspace_admin", "refresh_next_runs": False}


def test_render_chat_turn_current_cron_reference_forces_scheduler_scope(monkeypatch, tmp_path: Path) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    for name in ("request_tool_scope", "list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler",) if name != "request_tool_scope" else ("tool_scope",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(type(runtime), "active_tool_registry", property(lambda self: registry))
    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_user_turn_mentions_current_cron_reference",
        lambda user_message, *, principal_id: "Stage live scheduler manual run 599d86af28" in user_message,
    )
    decisions: list[dict[str, object]] = []

    def fake_build_turn_tool_scope_decision(**kwargs):  # noqa: ANN003
        decisions.append(kwargs)
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_turn_tool_scope_decision)

    class Orchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            visible = {
                str(definition.get("name") or "")
                for definition in kwargs["tool_registry"].list_tool_definitions()
            }
            assert {"list_crons", "run_cron"}.issubset(visible)
            return SimpleNamespace(
                tool_results=[ToolResult("run-1", "run_cron", "completed", {"message": "started"})],
                suspended_for_approval=False,
                approval_id=None,
                final_text="Manual scheduled task run started.",
                artifacts=[],
            )

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat ejecuta Stage live scheduler manual run 599d86af28",
        chat_id="123",
        settings=settings,
        agent_orchestrator=Orchestrator(),
        append_activity_trace=False,
    )

    assert "started" in reply.lower()
    assert decisions
    assert decisions[-1]["force_model_decision"] is True


def test_request_scope_visible_skips_pre_model_account_source_shortcut(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "source-shortcut-runtime.db")
    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("email_search", ("email", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(type(runtime), "active_tool_registry", property(lambda self: registry))
    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class Orchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.calls = 0

        def run_turn(self, **kwargs):
            self.calls += 1
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text="model path answer",
                artifacts=[],
            )

    orchestrator = Orchestrator()

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat test-only status prompt",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert reply == "model path answer"
    assert orchestrator.calls == 1


def test_plain_independent_turn_skips_live_tool_context_and_fulfillment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "plain-fast-runtime.db")
    runtime.active_tool_registry = ToolRegistry()

    def fail_plain_context(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain independent turns must not build live tool/capability context")

    def fail_plain_fulfillment(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain independent turns must not evaluate deliverable fulfillment")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", fail_plain_context)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_index_prompt", fail_plain_context)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", fail_plain_context)
    monkeypatch.setattr(chat_operator, "_builder_route_hints_prompt", fail_plain_context)
    monkeypatch.setattr(chat_operator, "evaluate_response_execution_outcome", fail_plain_fulfillment)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_schedule_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class PlainOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.history: list[dict[str, object]] = []

        def run_turn(self, **kwargs):  # noqa: ANN003
            self.history = list(kwargs["conversation_history"])
            tool_names = {
                str(tool.get("name") or "")
                for tool in kwargs["tool_registry"].list_tool_definitions()
                if isinstance(tool, dict)
            }
            assert tool_names <= {"request_tool_scope", chat_operator.CHAT_HISTORY_SEARCH_TOOL_NAME, "memory_search"}
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text="plain reply",
                artifacts=[],
            )

    orchestrator = PlainOrchestrator()
    activity_events: list[dict[str, object]] = []

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Hi",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        activity_callback=activity_events.append,
        append_activity_trace=False,
    )

    assert reply == "plain reply"
    assert any(event.get("id") == "prepare" for event in activity_events)
    assert all(
        "Live capability inventory" not in str(block)
        for message in orchestrator.history
        for block in message.get("content", [])
    )


def test_unrequested_diagnostic_log_media_is_suppressed(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    log_path = root / "codex-startup.log"
    log_path.write_text("startup failed\n", encoding="utf-8")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done.\n\nMEDIA:{log_path}",
        artifact_paths=[],
        prompt="current status",
        principal_id="telegram:1",
        required_attachment_extensions=(),
    )

    assert reply == "Done."
    assert "MEDIA:" not in reply
    assert str(log_path) not in reply

    requested = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done.\n\nMEDIA:{log_path}",
        artifact_paths=[],
        prompt="status.log",
        principal_id="telegram:1",
        required_attachment_extensions=(".log",),
    )

    assert f"MEDIA:{log_path}" in requested


def test_same_stem_text_sidecar_is_not_attached_with_primary_artifact(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    pdf_path = root / "email_deadline_triage_2026-05-04.pdf"
    txt_path = root / "email_deadline_triage_2026-05-04.txt"
    pdf_path.write_bytes(b"%PDF-1.4\n%test\n")
    txt_path.write_text("plain text sidecar\n", encoding="utf-8")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "I found your email tracker files. Attaching the PDF version here:\n\n"
            f"MEDIA:{pdf_path}\n\n"
            "I also found the text version if you want it:\n\n"
            f"MEDIA:{txt_path}"
        ),
        artifact_paths=[],
        prompt="and my email tracker",
        principal_id="telegram:1",
        required_attachment_extensions=(),
    )

    assert reply == f"Done — attached the requested PDF.\n\nMEDIA:{pdf_path}"
    assert str(txt_path) not in reply

    requested_text = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done.\n\nMEDIA:{pdf_path}\nMEDIA:{txt_path}",
        artifact_paths=[],
        prompt="email_deadline_triage_2026-05-04.txt",
        principal_id="telegram:1",
        required_attachment_extensions=(".txt",),
    )

    assert requested_text == f"Done — attached the requested text file.\n\nMEDIA:{txt_path}"


def test_mixed_image_and_workbook_delivery_prefers_primary_non_image_artifact(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    workbook = root / "topaz_jewelry_styles.xlsx"
    image_a = root / "01_topaz_ring.png"
    image_b = root / "02_topaz_earrings.png"
    workbook.write_bytes(b"PK\x03\x04xlsx bytes")
    image_a.write_bytes(b"\x89PNG\r\n")
    image_b.write_bytes(b"\x89PNG\r\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done.\nMEDIA:{image_a}\nMEDIA:{image_b}\nMEDIA:{workbook}",
        artifact_paths=[],
        prompt="Search top 10 topez jwellery styles and create an excel File with images inside.",
        principal_id="telegram:1",
        required_attachment_extensions=(),
    )

    assert reply == f"Done — attached the requested spreadsheet.\n\nMEDIA:{workbook}"
    assert str(image_a) not in reply
    assert str(image_b) not in reply


def test_requested_workbook_delivery_filters_same_turn_extra_media_paths(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    workbook = root / "stage_archive_transform_field_notes.xlsx"
    screenshot = root / "screenshot-1b8113a59f13.png"
    source_image = root / "alpha-sun.png"
    workbook.write_bytes(b"PK\x03\x04xlsx bytes")
    screenshot.write_bytes(b"\x89PNG\r\n")
    source_image.write_bytes(b"\x89PNG\r\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "Done — attached the requested files.\n\n"
            f"MEDIA:{workbook}\n"
            f"MEDIA:{screenshot}\n"
            f"MEDIA:{source_image}"
        ),
        artifact_paths=[str(workbook), str(screenshot), str(source_image)],
        prompt=(
            "Create a new XLSX workbook named stage_archive_transform_field_notes.xlsx. "
            "Attach only stage_archive_transform_field_notes.xlsx; do not attach screenshots or source files."
        ),
        principal_id="telegram:1",
        tool_results=[
            ToolResult(
                "xlsx-1",
                "spreadsheet_create",
                "completed",
                {
                    "artifact_descriptors": [
                        {
                            "extension": ".xlsx",
                            "path": str(workbook),
                            "role": "deliverable",
                        }
                    ],
                    "artifact_path": str(workbook),
                    "artifact_paths": [str(workbook)],
                },
            ),
            ToolResult(
                "png-1",
                "browser_screenshot",
                "completed",
                {
                    "artifact_descriptors": [
                        {
                            "extension": ".png",
                            "path": str(screenshot),
                            "role": "deliverable",
                        }
                    ],
                    "artifact_path": str(screenshot),
                    "artifact_paths": [str(screenshot)],
                },
            ),
            ToolResult(
                "png-2",
                "file_write",
                "completed",
                {
                    "artifact_descriptors": [
                        {
                            "extension": ".png",
                            "path": str(source_image),
                            "role": "deliverable",
                        }
                    ],
                    "artifact_path": str(source_image),
                    "artifact_paths": [str(source_image)],
                },
            ),
        ],
        required_attachment_extensions=(".xlsx",),
    )

    assert reply == f"Done — attached the requested spreadsheet.\n\nMEDIA:{workbook}"
    assert str(screenshot) not in reply
    assert str(source_image) not in reply


def test_requested_workbook_delivery_replaces_wrong_reply_media_with_workbook(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    workbook = root / "stage_archive_transform_field_notes.xlsx"
    source_image = root / "alpha-sun.png"
    workbook.write_bytes(b"PK\x03\x04xlsx bytes")
    source_image.write_bytes(b"\x89PNG\r\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "Done — attached the requested files.\n\n"
            f"MEDIA:{source_image}"
        ),
        artifact_paths=[str(workbook), str(source_image)],
        prompt="Create a new XLSX workbook named stage_archive_transform_field_notes.xlsx.",
        principal_id="telegram:1",
        tool_results=[
            ToolResult(
                "xlsx-1",
                "spreadsheet_create",
                "completed",
                {
                    "artifact_descriptors": [
                        {
                            "extension": ".xlsx",
                            "path": str(workbook),
                            "role": "deliverable",
                        }
                    ],
                    "artifact_path": str(workbook),
                    "artifact_paths": [str(workbook)],
                },
            ),
            ToolResult(
                "png-1",
                "file_write",
                "completed",
                {
                    "artifact_descriptors": [
                        {
                            "extension": ".png",
                            "path": str(source_image),
                            "role": "deliverable",
                        }
                    ],
                    "artifact_path": str(source_image),
                    "artifact_paths": [str(source_image)],
                },
            ),
        ],
        required_attachment_extensions=(".xlsx",),
    )

    assert reply == f"Done — attached the requested spreadsheet.\n\nMEDIA:{workbook}"
    assert str(source_image) not in reply


def test_completed_requested_workbook_lock_ignores_later_png_side_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    workbook = root / "stage_archive_transform_field_notes.xlsx"
    screenshot = root / "stage_archive_transform_field_notes.png"
    source_image = root / "alpha-sun.png"
    with zipfile.ZipFile(workbook, "w") as package:
        package.writestr("[Content_Types].xml", "<Types />")
        package.writestr("xl/media/image1.png", b"\x89PNG\r\n")
    screenshot.write_bytes(b"\x89PNG\r\n")
    source_image.write_bytes(b"\x89PNG\r\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    result = SimpleNamespace(
        final_text=f"Done — attached the requested files.\n\nMEDIA:{source_image}",
        artifacts=[str(workbook), str(screenshot), str(source_image)],
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".xlsx"],
                    "embedded_media_artifact_extensions": [".xlsx"],
                    "available_tools": ["archive_extract", "spreadsheet_create", "file_write"],
                },
            ),
            ToolResult(
                "xlsx-1",
                "spreadsheet_create",
                "completed",
                {
                    "artifact_descriptors": [
                        {
                            "extension": ".xlsx",
                            "path": str(workbook),
                            "role": "deliverable",
                        }
                    ],
                    "artifact_path": str(workbook),
                    "artifact_paths": [str(workbook)],
                    "embedded_images": [str(source_image)],
                },
            ),
            ToolResult(
                "png-1",
                "file_write",
                "completed",
                {
                    "artifact_descriptors": [
                        {
                            "extension": ".png",
                            "path": str(source_image),
                            "role": "deliverable",
                        }
                    ],
                    "artifact_path": str(source_image),
                    "artifact_paths": [str(source_image)],
                },
            ),
        ],
    )

    completed_paths = chat_operator._completed_requested_artifact_paths_for_turn_result(
        runtime,
        result=result,
        prompt=(
            "Create a new XLSX workbook named stage_archive_transform_field_notes.xlsx. "
            "Attach only stage_archive_transform_field_notes.xlsx."
        ),
        principal_id="telegram:1",
        required_attachment_extensions=(".xlsx",),
        required_embedded_media_extensions=(".xlsx",),
    )

    assert completed_paths == (str(workbook),)


def test_archive_extract_images_feed_last_mile_workbook_with_filename_fields(
    monkeypatch,
    tmp_path: Path,
) -> None:
    load_workbook = pytest.importorskip("openpyxl").load_workbook
    root = tmp_path / "artifacts"
    root.mkdir()
    png_bytes = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
    )
    image_paths = []
    for name in ("alpha-sun.png", "beta-moonlight.png", "gamma-river.png"):
        path = root / name
        path.write_bytes(png_bytes)
        image_paths.append(path)

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)

    archive_result = ToolResult(
        "archive-1",
        "archive_extract",
        "completed",
        {
            "entries": [
                {
                    "name": path.name,
                    "path": str(path),
                    "bytes": path.stat().st_size,
                    "media_type": "image/png",
                }
                for path in image_paths
            ],
            "entry_count": len(image_paths),
            "artifact_paths": [],
        },
    )

    collected_paths = chat_operator._current_turn_collected_media_paths([archive_result])
    assert collected_paths == tuple(str(path) for path in image_paths)

    results, paths = chat_operator._materialize_last_mile_spreadsheet_artifacts(
        principal_id="telegram:1",
        prompt="Create a workbook from these extracted image records.",
        reply="",
        missing_extensions=(".xlsx",),
        collected_media_paths=collected_paths,
        tool_results=[archive_result],
    )

    assert [result.tool_name for result in results] == ["spreadsheet_create"]
    assert len(paths) == 1
    workbook_path = Path(paths[0])
    assert workbook_path.suffix == ".xlsx"
    with zipfile.ZipFile(workbook_path) as package:
        assert any(member.startswith("xl/media/") for member in package.namelist())
    workbook = load_workbook(workbook_path)
    try:
        sheet = workbook.active
        values = {
            str(cell.value)
            for row in sheet.iter_rows(values_only=False)
            for cell in row
            if cell.value is not None
        }
    finally:
        workbook.close()
    assert "Original filename" in values
    assert "Uppercase extension" in values
    assert "Reversed stem" in values
    assert "alpha-sun.png" in values
    assert "nus-ahpla" in values
    assert "thgilnoom-ateb" in values
    assert "revir-ammag" in values
    assert "PNG" in values


def test_browser_collected_source_images_do_not_leak_from_document_reply(monkeypatch, tmp_path: Path) -> None:
    from nullion.artifacts import artifact_output_descriptor
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    root = tmp_path / "artifacts"
    root.mkdir()
    image_paths = []
    for name in (
        "sarabeths_uws-29648250b803.png",
        "manhattan_diner-90ea0e913fb5.jpg",
        "zabars_cafe-e0155aea440c.jpg",
    ):
        path = root / name
        path.write_bytes(b"\x89PNG\r\nsource-image")
        image_paths.append(path)
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    leaked_reply = "\n".join(
        [
            "Created it.",
            "",
            "Here's the Google Doc with the breakfast options and restaurant images:",
            "https://docs.google.com/document/d/doc-id/edit",
            "",
            "I included:",
            "- Good Enough to Eat",
            "- Sarabeth's Upper West Side",
            *(f"MEDIA:{path}" for path in image_paths),
        ]
    )
    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=leaked_reply,
        artifact_paths=[],
        prompt="Can u create a google docs with all that information and restaurant images",
        principal_id="telegram:1",
        tool_results=[
            ToolResult(
                "collect",
                "browser_image_collect",
                "completed",
                {
                    "image_paths": [str(path) for path in image_paths],
                    "artifact_paths": [str(path) for path in image_paths],
                    "artifact_descriptors": [
                        artifact_output_descriptor(path, role="source", kind="browser_image")
                        for path in image_paths
                    ],
                },
            ),
        ],
        required_attachment_extensions=(),
    )
    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:1")

    assert "https://docs.google.com/document/d/doc-id/edit" in reply
    assert "MEDIA:" not in reply
    assert delivery.attachments == ()
    for path in image_paths:
        assert str(path) not in reply


def test_processed_email_attachment_source_is_not_auto_delivered(monkeypatch, tmp_path: Path) -> None:
    from nullion.artifacts import artifact_output_descriptor
    from nullion import messaging_adapters

    root = tmp_path / "artifacts"
    root.mkdir()
    pdf_path = root / "i-751-checklist-a4cd82baf217.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n% source checklist\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)
    monkeypatch.setattr(messaging_adapters, "artifact_root_for_principal", lambda principal_id: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done -- attached the requested PDF.\n\nMEDIA:{pdf_path}",
        artifact_paths=[str(pdf_path)],
        prompt="match the email attachment list against the Google Docs",
        principal_id="telegram:1",
        tool_results=[
            ToolResult(
                "search",
                "email_search",
                "completed",
                {"items": [{"id": "msg-1", "subject": "I-751 matter"}]},
            ),
            ToolResult(
                "email",
                "email_read",
                "completed",
                {
                    "message": {
                        "id": "msg-1",
                        "subject": "I-751 matter",
                        "body": "Please provide the documents listed in the attached checklist.",
                    }
                },
            ),
            ToolResult(
                "attachment",
                "email_attachment_read",
                "completed",
                {
                    "artifact_path": str(pdf_path),
                    "artifact_paths": [str(pdf_path)],
                    "artifact_descriptors": [
                        artifact_output_descriptor(pdf_path, role="deliverable", kind="email_attachment")
                    ],
                },
            ),
        ],
        required_attachment_extensions=(),
    )
    delivery = messaging_adapters.prepare_reply_for_platform_delivery(reply, principal_id="telegram:1")

    assert "MEDIA:" not in reply
    assert str(pdf_path) not in reply
    assert delivery.attachments == ()


def test_direct_email_attachment_read_still_delivers_attachment(monkeypatch, tmp_path: Path) -> None:
    from nullion.artifacts import artifact_output_descriptor
    from nullion import messaging_adapters

    root = tmp_path / "artifacts"
    root.mkdir()
    pdf_path = root / "i-751-checklist-a4cd82baf217.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n% source checklist\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)
    monkeypatch.setattr(messaging_adapters, "artifact_root_for_principal", lambda principal_id: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done -- attached the requested PDF.\n\nMEDIA:{pdf_path}",
        artifact_paths=[str(pdf_path)],
        prompt="show me the attachment from the email",
        principal_id="telegram:1",
        tool_results=[
            ToolResult(
                "attachment",
                "email_attachment_read",
                "completed",
                {
                    "artifact_path": str(pdf_path),
                    "artifact_paths": [str(pdf_path)],
                    "artifact_descriptors": [
                        artifact_output_descriptor(pdf_path, role="deliverable", kind="email_attachment")
                    ],
                },
            ),
        ],
        required_attachment_extensions=(),
    )
    delivery = messaging_adapters.prepare_reply_for_platform_delivery(reply, principal_id="telegram:1")

    assert f"MEDIA:{pdf_path}" in reply
    assert delivery.attachments == (pdf_path,)


def test_model_media_directive_keeps_other_tool_result_artifacts(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    csv_path = root / "qa_stage_alpha_beta_gamma.csv"
    pdf_path = root / "qa_stage_alpha_beta_gamma_summary.pdf"
    csv_path.write_text("name,value\nAlpha,1\nBeta,2\nGamma,3\n", encoding="utf-8")
    pdf_path.write_bytes(b"%PDF-1.4\n%test\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done — attached the requested artifact.\n\nMEDIA:{csv_path}",
        artifact_paths=[],
        prompt="create a tiny CSV artifact with 3 rows, then create a PDF summary from that CSV; attach both files",
        principal_id="telegram:1",
        tool_results=[
            ToolResult("csv", "file_write", "completed", {"path": str(csv_path)}),
            ToolResult(
                "pdf",
                "pdf_create",
                "completed",
                {"artifact_path": str(pdf_path), "artifact_paths": [str(pdf_path)]},
            ),
        ],
        required_attachment_extensions=(".pdf",),
    )

    assert f"MEDIA:{csv_path}" in reply
    assert f"MEDIA:{pdf_path}" in reply


def test_email_send_receipt_attaches_sent_image_not_confirmation_sidecar(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    sent_image = root / "dog_for_email.png"
    sent_image.write_bytes(b"\x89PNG\r\nemail-image")
    confirmation = root / "email_dog_delivery_confirmation_2026-05-15.txt"
    confirmation.write_text("Email sent to recipient@example.com with subject Hi.", encoding="utf-8")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Created and attached the confirmation file.\n\nMEDIA:{confirmation}",
        artifact_paths=[],
        prompt="Send an email to recipient@example.com tell him hi and send an image of a dog",
        principal_id="telegram:1",
        tool_results=[
            ToolResult(
                "email-1",
                "email_send",
                "completed",
                {
                    "to": ["recipient@example.com"],
                    "subject": "Hi",
                    "provider_id": "maton-google-mail",
                    "status_code": 200,
                    "json": {"id": "msg-1"},
                    "attachment_paths": [str(sent_image)],
                },
            ),
            ToolResult("confirm-1", "file_write", "completed", {"path": str(confirmation)}),
        ],
        required_attachment_extensions=(".pdf",),
    )

    assert "✉️ Email sent to recipient@example.com" in reply
    assert "status 200" not in reply
    assert "message id" not in reply
    assert "msg-1" not in reply
    assert "Done — attached the requested image." in reply
    assert f"MEDIA:{sent_image}" in reply
    assert str(confirmation) not in reply
    assert "email_delivery_confirmation.txt" not in reply


def test_completed_email_summary_replaces_transport_heavy_model_receipt() -> None:
    reply = chat_operator._prepend_completed_email_send_summaries(
        (
            'The test email was sent successfully to **recipient@example.com** '
            'with subject "Release check" (status 200, message id 19e328c0462cb896).\n\n'
            "**Message ID:** 19e328c0462cb896"
        ),
        [
            ToolResult(
                "email-1",
                "email_send",
                "completed",
                {"to": ["recipient@example.com"], "subject": "Release check", "id": "19e328c0462cb896"},
            )
        ],
    )

    assert reply == '✉️ Email sent to recipient@example.com with subject "Release check".'
    assert "status 200" not in reply
    assert "message id" not in reply.lower()
    assert "19e328c0462cb896" not in reply


def test_file_write_decodes_base64_image_artifact(tmp_path: Path) -> None:
    from nullion import tools

    output_path = tmp_path / "badge.png"
    handler = tools._build_file_write_handler(workspace_root=tmp_path)
    result = handler(
        ToolInvocation(
            invocation_id="write-image",
            tool_name="file_write",
            principal_id="test",
            arguments={
                "path": str(output_path),
                "content": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=",
            },
        )
    )

    assert result.status == "completed"
    assert result.output["binary_image_decoded"] is True
    assert output_path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    assert output_path.stat().st_size < len(str(result.output["path"])) + 200


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


def test_chat_memory_capture_persists_durable_email_confirmation_preference(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_MEMORY_NO_TOOL_CAPTURE_ENABLED", "1")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = "workspace:workspace_admin"

    class Model:
        def create(self, **kwargs):  # noqa: ANN001
            payload = json.loads(kwargs["messages"][1]["content"][0]["text"])
            assert payload["turn"]["user_message"] == "Always confirm emails before sending."
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "entries": [
                                    {
                                        "key": "email_send_confirmation_preference",
                                        "value": "Always ask for confirmation before sending emails.",
                                        "kind": "preference",
                                        "content_type": "durable_memory",
                                        "confidence": 0.99,
                                    }
                                ]
                            }
                        ),
                    }
                ]
            }

    import nullion.builder_background as builder_background

    monkeypatch.setattr(
        builder_background,
        "schedule_builder_background_task",
        lambda _label, worker: (worker() or True),
    )

    chat_operator._schedule_chat_turn_memory_capture(
        runtime,
        SimpleNamespace(model_client=Model()),
        owner=owner,
        user_message="Always confirm emails before sending.",
        assistant_reply="Got it. I will confirm before sending emails.",
        tool_results=[],
    )

    [entry] = runtime.store.list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert entry.owner == owner
    assert entry.key == "email_send_confirmation_preference"
    assert entry.value == "Always ask for confirmation before sending emails."


def test_chat_memory_capture_falls_back_when_narrow_extractor_is_empty(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_MEMORY_NO_TOOL_CAPTURE_ENABLED", "1")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = "workspace:workspace_admin"

    class EmptyModel:
        def create(self, **_kwargs):  # noqa: ANN001
            return {"content": [{"type": "text", "text": "{\"entries\": []}"}]}

    import nullion.builder_background as builder_background
    import nullion.builder_memory as builder_memory

    def fake_manage_turn_memory(runtime_arg, _model, **kwargs):  # noqa: ANN001, ANN003
        assert runtime_arg is runtime
        assert kwargs["owner"] == owner
        runtime.store.add_user_memory_entry(
            UserMemoryEntry(
                entry_id=f"{owner}:email_confirmation_preference",
                key="email_confirmation_preference",
                value="Always confirm before sending emails.",
                kind=UserMemoryKind.PREFERENCE,
                owner=owner,
                source="builder_memory",
            )
        )
        runtime.checkpoint()
        return builder_memory.BuilderMemoryResult(written=1)

    monkeypatch.setattr(
        builder_background,
        "schedule_builder_background_task",
        lambda _label, worker: (worker() or True),
    )
    monkeypatch.setattr(builder_memory, "manage_turn_memory", fake_manage_turn_memory)

    chat_operator._schedule_chat_turn_memory_capture(
        runtime,
        SimpleNamespace(model_client=EmptyModel()),
        owner=owner,
        user_message="Always confirm before sending emails.",
        assistant_reply="Understood. I will confirm before sending any email.",
        tool_results=[],
    )

    [entry] = runtime.store.list_user_memory_entries(kind=UserMemoryKind.PREFERENCE)
    assert entry.key == "email_confirmation_preference"
    assert entry.value == "Always confirm before sending emails."


def test_chat_memory_context_reinforces_selected_entries(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "1")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    owner = "workspace:workspace_admin"
    runtime.store.add_user_memory_entry(
        UserMemoryEntry(
            entry_id=f"{owner}:reply_style",
            owner=owner,
            kind=UserMemoryKind.PREFERENCE,
            key="reply_style",
            value="Use more emojis for time-sensitive, well-structured replies.",
            source="test",
        )
    )

    text = chat_operator._memory_context_for_chat(runtime, chat_id="123", settings=None)

    assert text == "Reply Style: Use more emojis for time-sensitive, well-structured replies."
    reinforced = runtime.store.get_user_memory_entry(f"{owner}:reply_style")
    assert reinforced is not None
    assert reinforced.use_count == 1
    assert reinforced.use_score > 0
    assert reinforced.last_used_at is not None


def test_render_chat_turn_skips_foreground_memory_reinforcement(monkeypatch, tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    calls: list[dict] = []

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    def fake_memory_context(*args, **kwargs):  # noqa: ANN001
        calls.append(kwargs)
        return "Memory: keep replies compact"

    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", fake_memory_context)

    class TextOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.calls: list[dict] = []

        def run_turn(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text="ok",
                artifacts=[],
            )

    orchestrator = TextOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat hello",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert reply == "ok"
    assert calls
    assert calls[0]["reinforce"] is False
    history_text = "\n".join(
        block.get("text", "")
        for message in orchestrator.calls[0]["conversation_history"]
        for block in message.get("content", [])
        if isinstance(block, dict)
    )
    assert "Known user memory:\nMemory: keep replies compact" in history_text


def test_requested_attachment_extensions_use_named_output_filename_extension() -> None:
    class Planner:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            self.calls += 1
            assert kwargs["tools"][0]["name"] == "select_attachment_format"
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {
                            "extension": ".pdf",
                            "embedded_media_extensions": [],
                        },
                    }
                ]
            }

    prompt = (
        "Using the workbook you just created, create a PDF named "
        "stage_followup_reuse_summary.pdf that states the total and the top region. "
        "Attach only the PDF."
    )

    planner = Planner()
    assert chat_operator._requested_attachment_extensions(
        prompt,
        model_client=planner,
        allow_model_planning=False,
    ) == (".pdf",)
    assert planner.calls == 1


def test_chat_operator_text_and_session_helpers(monkeypatch) -> None:
    runtime = Runtime()
    monkeypatch.delenv("NULLION_TELEGRAM_CHAT_STREAMING_ENABLED", raising=False)

    assert chat_operator._normalize_local_intent_text(" Approve it!! ") == "approve it"
    assert chat_operator._word_tokens("File: report-v2.xlsx!") == ("file", "report", "v2", "xlsx")
    assert chat_operator._is_explicit_approval_reply("please approve") is False
    assert chat_operator._is_internal_scheduled_task_context("[Scheduled task: Daily weather] run") is True
    assert chat_operator._is_internal_scheduled_task_context("[Manual scheduled task run: Daily weather] run") is True
    assert chat_operator._is_internal_scheduled_task_context("Daily weather") is False
    assert chat_operator._assistant_reply_referencable_artifact_reason("Saved to /tmp/report.pdf") is None
    assert chat_operator._requested_attachment_extension("report.docx") == ".docx"
    assert chat_operator._requested_attachment_extension("report.txt") == ".txt"
    assert chat_operator._requested_attachment_extension("no attachment") is None
    assert "user_message" in inspect.signature(chat_operator._messaging_turn_skip_tool_scope_decision).parameters
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
    assert chat_operator._session_activity_trace_setting(runtime, chat_id="1") is True
    assert chat_operator._handle_streaming_command_for_chat(runtime, "/streaming on", chat_id="1") == "Chat streaming is on."
    assert chat_operator._handle_streaming_command_for_chat(None, "/streaming status", chat_id="1").startswith("Chat streaming is on")
    assert chat_operator._handle_streaming_command_for_chat(runtime, "/streaming maybe", chat_id="1").startswith("Usage")
    assert chat_operator._handle_thinking_command_for_chat(runtime, "/thinking off", chat_id="1") == "Thinking display is off."
    assert chat_operator._handle_thinking_command_for_chat(runtime, "/thinking maybe", chat_id="1").startswith("Usage")
    with pytest.raises(ValueError):
        chat_operator.set_verbose_mode_for_chat(runtime, chat_id="1", mode="loud")
    assert runtime.checkpoints >= 4


def test_chat_session_settings_persist_independently_after_restart(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("NULLION_TELEGRAM_CHAT_STREAMING_ENABLED", raising=False)
    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)

    chat_operator.set_verbose_mode_for_chat(runtime, chat_id="1", mode="on")
    chat_operator.set_chat_streaming_enabled_for_chat(runtime, chat_id="1", enabled=False)
    chat_operator.set_thinking_display_enabled_for_chat(runtime, chat_id="1", enabled=True)

    restarted = bootstrap_persistent_runtime(checkpoint)

    assert chat_operator.verbose_mode_status_text_for_chat(restarted, chat_id="1") == "on"
    assert chat_operator.chat_streaming_status_text_for_chat(restarted, chat_id="1") == "off"
    assert chat_operator.thinking_display_status_text_for_chat(restarted, chat_id="1") == "on"
    session_events = [
        event
        for event in restarted.store.list_conversation_events("telegram:1")
        if event.get("event_type") == "conversation.session_settings"
    ]
    assert len(session_events) == 3


def test_chat_session_settings_read_from_index_without_full_event_scan(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("NULLION_TELEGRAM_CHAT_STREAMING_ENABLED", raising=False)
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    chat_operator.set_verbose_mode_for_chat(runtime, chat_id="1", mode="on")
    chat_operator.set_chat_streaming_enabled_for_chat(runtime, chat_id="1", enabled=False)
    chat_operator.set_thinking_display_enabled_for_chat(runtime, chat_id="1", enabled=True)

    def fail_full_event_scan(*args, **kwargs):
        raise AssertionError("chat setting status should use the session setting index")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(type(runtime.store), "list_conversation_events", fail_full_event_scan)

        assert chat_operator.verbose_mode_status_text_for_chat(runtime, chat_id="1") == "on"
        assert chat_operator.chat_streaming_status_text_for_chat(runtime, chat_id="1") == "off"
        assert chat_operator.thinking_display_status_text_for_chat(runtime, chat_id="1") == "on"


def test_chat_session_display_settings_survive_conversation_reset(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "true")
    monkeypatch.setenv("NULLION_TELEGRAM_CHAT_STREAMING_ENABLED", "true")
    checkpoint = tmp_path / "runtime.db"
    runtime = bootstrap_persistent_runtime(checkpoint)

    chat_operator.set_verbose_mode_for_chat(runtime, chat_id="1", mode="off")
    chat_operator.set_chat_streaming_enabled_for_chat(runtime, chat_id="1", enabled=False)
    chat_operator.set_thinking_display_enabled_for_chat(runtime, chat_id="1", enabled=True)

    runtime.store.add_conversation_event({"conversation_id": "telegram:1", "event_type": "conversation.session_reset"})
    runtime.checkpoint()
    restarted = bootstrap_persistent_runtime(checkpoint)

    assert chat_operator.verbose_mode_status_text_for_chat(restarted, chat_id="1") == "off"
    assert chat_operator.chat_streaming_status_text_for_chat(restarted, chat_id="1") == "off"
    assert chat_operator.thinking_display_status_text_for_chat(restarted, chat_id="1") == "on"


def test_chat_operator_builder_uses_structured_disable_flag() -> None:
    before = len(chat_operator._TURN_SIGNAL_WINDOW)
    chat_operator._try_builder_reflection(
        SimpleNamespace(),
        SimpleNamespace(model_client=object()),
        user_message="Prepare a weather report.",
        assistant_reply="done",
        tool_names=["weather_lookup", "web_fetch"],
        tool_error_count=0,
        outcome=chat_operator.TurnOutcome.SUCCESS,
        conversation_id="telegram:1",
        memory_owner="workspace:workspace_admin",
        tool_results=[ToolResult("1", "weather_lookup", "completed", {"forecast": "rain"})],
        builder_learning_enabled=False,
    )
    assert len(chat_operator._TURN_SIGNAL_WINDOW) == before


def test_chat_operator_builder_skips_scheduler_only_tool_turns() -> None:
    before = len(chat_operator._TURN_SIGNAL_WINDOW)
    chat_operator._try_builder_reflection(
        SimpleNamespace(),
        SimpleNamespace(model_client=object()),
        user_message="operator-triggered scheduler action",
        assistant_reply="Ran cron.",
        tool_names=["list_crons", "run_cron"],
        tool_error_count=0,
        outcome=chat_operator.TurnOutcome.SUCCESS,
        conversation_id="telegram:1",
        memory_owner="workspace:workspace_admin",
        tool_results=[ToolResult("1", "run_cron", "completed", {"delivery_status": "sent"})],
    )
    assert len(chat_operator._TURN_SIGNAL_WINDOW) == before


def test_chat_operator_conversation_memory_approval_and_artifact_helpers(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "chat-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123"})
    thread = [
        {"user": "make a file", "assistant": "Saved https://example.com/report"},
        {"user": "thanks", "assistant": "Okay."},
    ]

    assert chat_operator._chat_prompt_for_message("/chat hello there") == "hello there"
    assert chat_operator._chat_prompt_for_message("/status") is None
    assert chat_operator._local_chat_reply_body("thanks") is None
    assert chat_operator._local_chat_reply_body("ok") is None
    assert chat_operator._local_chat_reply_body("good night") is None
    assert chat_operator._build_conversation_context(thread).startswith("User: make a file")
    assert chat_operator._messaging_channel_and_identity_for_chat("slack:U1") == ("slack", "U1")
    assert chat_operator._conversation_id_for_chat(None) == "telegram:default"
    assert chat_operator._principal_id_for_chat("123", settings) == "telegram_chat"
    assert chat_operator._previous_user_message(thread) == "thanks"
    assert chat_operator._previous_assistant_message(thread) == "Okay."
    assert chat_operator._assistant_reply_referencable_artifact_reason("Saved `/tmp/report.txt`") is None
    conversation_id = chat_operator._conversation_id_for_chat("123")
    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id=conversation_id,
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.GENERATE_ARTIFACT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(artifact_kind="pdf"),
        finish=TaskFrameFinishCriteria(requires_artifact_delivery=True, required_artifact_kind="pdf"),
        summary="generate artifact",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id(conversation_id, frame.frame_id)
    fallback, reason = chat_operator._chat_ambiguity_fallback(runtime, chat_id="123", prompt="ส่งให้หน่อย")
    assert reason == "task_frame_artifact_contract"
    assert fallback("ส่งให้หน่อย", True) is None

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
    assert chat_operator._auto_approval_command_for_message(runtime, "approve", chat_id="123") is None
    assert chat_operator._approval_id_from_command("/approve ap-chat") == "ap-chat"
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_member")
    chat_operator._remember_chat_turn(runtime, chat_id="123", user_message="go", assistant_reply="Tool approval requested: ap-chat")
    assert runtime.store.list_conversation_events("telegram:123")[-1]["workspace_id"] == "workspace_member"
    assert chat_operator._source_prompt_for_approval(runtime, chat_id="123", approval_id="ap-chat") == "go"
    assert chat_operator._is_approval_pending_message({"role": "assistant", "content": "Tool approval requested: ap-chat"}) is True
    assert chat_operator._is_telegram_resume_principal("telegram:123") is True

    media_context = chat_operator._build_conversation_context(
        [{"user": "why so many files?", "assistant": f"Done.\nMEDIA:{tmp_path / 'old.pdf'}"}]
    )
    assert media_context is not None
    assert "MEDIA:" not in media_context

    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "1")
    monkeypatch.setenv("NULLION_CHAT_EXPLICIT_MEMORY_SYNC_CAPTURE", "1")
    chat_operator._remember_explicit_memory(runtime, chat_id="123", settings=settings, prompt="Remember I like vim")
    assert chat_operator._memory_context_for_chat(runtime, chat_id="123", settings=settings) is None
    from nullion.memory_tools import MemoryToolRegistry
    memory_owner = chat_operator._memory_owner_for_chat("123", settings)
    memory_registry = MemoryToolRegistry(ToolRegistry(), runtime=runtime, workspace_id=memory_owner.removeprefix("workspace:"),
        conversation_id="telegram:123", user_message="Remember I like vim")
    receipt = memory_registry.invoke(ToolInvocation("save-editor", "memory_save", "telegram:123", {
        "key": "memory", "value": "I like vim", "kind": "fact", "source_quote": "Remember I like vim"}))
    assert receipt.status == "completed" and receipt.output["persisted"] is True
    assert chat_operator._memory_context_for_chat(runtime, chat_id="123", settings=settings) == "Memory: I like vim"

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
    source_zip = str(tmp_path / "Archive.zip")
    manifest_xlsx = str(tmp_path / "Archive_manifest.xlsx")
    manifest_csv = str(tmp_path / "Archive_manifest.csv")
    descriptor_results = [
        ToolResult(
            invocation_id="download",
            tool_name="file_download",
            status="completed",
            output={
                "artifact_path": source_zip,
                "artifact_paths": [source_zip],
                "artifact_descriptors": [{"path": source_zip, "role": "source", "kind": "download"}],
            },
        ),
        ToolResult(
            invocation_id="extract",
            tool_name="archive_extract",
            status="completed",
            output={
                "artifact_path": manifest_xlsx,
                "artifact_paths": [manifest_xlsx],
                "artifact_descriptors": [
                    {"path": source_zip, "role": "source", "kind": "archive"},
                    {"path": manifest_xlsx, "role": "deliverable", "kind": "archive_manifest"},
                    {"path": manifest_csv, "role": "intermediate", "kind": "archive_manifest_sidecar"},
                ],
            },
        ),
    ]
    assert chat_operator._artifact_paths_from_tool_results(descriptor_results) == [manifest_xlsx]
    assert chat_operator._should_suppress_foreground_reply([result]) is False
    assert chat_operator._should_suppress_foreground_reply([
        ToolResult(
            invocation_id="cron",
            tool_name="run_cron",
            status="completed",
            output={"foreground_reply_suppressed": True},
        )
    ]) is True
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
    assert reply == "Result: report.txt"

    explicit_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"MEDIA:{artifact}",
        artifact_paths=[],
        prompt="frobnicate however tomorrow says it",
        tool_results=[],
    )
    assert f"MEDIA:{artifact}" in explicit_reply
    assert chat_operator._reply_has_deliverable_media(runtime, explicit_reply) is True
    assert chat_operator._clean_undeliverable_media_reply(runtime, "hello\nMEDIA:/outside/missing.txt") == "hello"

    pdf_artifact = artifact_root / "report.pdf"
    pdf_artifact.write_bytes(b"%PDF-1.4\n%test\n")
    pdf_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply="I created the actual PDF now and saved it here:",
        artifact_paths=[],
        prompt="That's the pdf",
        tool_results=[ToolResult("pdf", "file_write", "completed", {"path": str(pdf_artifact)})],
        required_attachment_extensions=(".pdf",),
    )
    assert "Done \u2014 attached the requested PDF." in pdf_reply
    assert f"MEDIA:{pdf_artifact}" in pdf_reply

    embedded_pdf_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply="Captured the requested page.",
        artifact_paths=[str(pdf_artifact)],
        prompt="neutral structured browser capture",
        tool_results=[],
        required_attachment_extensions=(),
        required_embedded_media_extensions=(".pdf",),
    )
    assert f"MEDIA:{pdf_artifact}" in embedded_pdf_reply

    workbook_artifact = artifact_root / "stage_archive_transform_field_notes.xlsx"
    workbook_artifact.write_bytes(b"PK\x03\x04structured workbook")
    workbook_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply="Created the requested spreadsheet:\n- `stage_archive_transform_field_notes.xlsx`",
        artifact_paths=[],
        prompt="neutral requested .xlsx attachment",
        tool_results=[
            ToolResult(
                "sheet",
                "spreadsheet_create",
                "completed",
                {"path": str(workbook_artifact), "rows": 3, "columns": 5},
            )
        ],
        required_attachment_extensions=(".xlsx",),
    )
    assert "Created the requested spreadsheet" in workbook_reply
    assert f"MEDIA:{workbook_artifact}" in workbook_reply

    poster_html = artifact_root / "community_night_market_poster_STAGE_FORMAT_QA.html"
    poster_pdf = artifact_root / "community_night_market_poster_STAGE_FORMAT_QA.pdf"
    memo_pdf = artifact_root / "community_night_market_memo_STAGE_FORMAT_QA.pdf"
    stale_pdf = artifact_root / "community_night_market_old_draft.pdf"
    poster_html.write_text("<html>STAGE FORMAT QA</html>", encoding="utf-8")
    for path in (poster_pdf, memo_pdf, stale_pdf):
        path.write_bytes(b"%PDF-1.4\n%test\n" + path.name.encode("utf-8"))
    package_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "I found 4 files:\n"
            f"- {poster_html.name}\n- {poster_pdf.name}\n- {memo_pdf.name}\n- {stale_pdf.name}"
        ),
        artifact_paths=[str(poster_html), str(poster_pdf), str(memo_pdf)],
        prompt=(
            f"Create {poster_html.name}, {poster_pdf.name}, and {memo_pdf.name}. "
            "Do not attach old drafts."
        ),
        tool_results=[
            ToolResult(
                str(index),
                "file_write",
                "completed",
                {
                    "artifact_path": str(path),
                    "artifact_paths": [str(path)],
                    "artifact_descriptors": [{"path": str(path), "role": "deliverable"}],
                },
            )
            for index, path in enumerate((poster_html, poster_pdf, memo_pdf), start=1)
        ],
        required_attachment_extensions=(".html", ".pdf", ".pdf"),
    )
    assert package_reply.startswith("Done \u2014 attached the requested files.")
    assert stale_pdf.name not in package_reply
    assert package_reply.count("MEDIA:") >= 2


def test_explicit_filename_extension_file_search_match_delivers_requested_artifact(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    workbook_artifact = artifact_root / "stage_requested_file_delivery_contract.xlsx"
    workbook_artifact.write_bytes(b"PK\x03\x04structured workbook")
    runtime = Runtime()
    runtime.store.get_active_task_frame_id = lambda conversation_id: None

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda rt: artifact_root)

    no_contract_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "I ran the local command.\n\n```text\n"
            "- exists: True;\n"
            "- filename: stage_requested_file_delivery_contract.xlsx;\n"
            "```"
        ),
        artifact_paths=[],
        prompt=(
            "Adjunta el archivo exacto llamado stage_requested_file_delivery_contract.xlsx "
            "como archivo descargable."
        ),
        tool_results=[
            ToolResult(
                "search-files",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(workbook_artifact)}],
                    "matches": [str(workbook_artifact)],
                    "search_contents": False,
                    "truncated": False,
                },
            )
        ],
        required_attachment_extensions=(),
    )

    assert "MEDIA:" not in no_contract_reply
    assert str(workbook_artifact) not in no_contract_reply
    assert "stage_requested_file_delivery_contract.xlsx" in no_contract_reply

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "I ran the local command.\n\n```text\n"
            "- exists: True;\n"
            "- filename: stage_requested_file_delivery_contract.xlsx;\n"
            "```"
        ),
        artifact_paths=[],
        prompt=(
            "Adjunta el archivo exacto llamado stage_requested_file_delivery_contract.xlsx "
            "como archivo descargable."
        ),
        tool_results=[
            ToolResult(
                "search-files",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(workbook_artifact)}],
                    "matches": [str(workbook_artifact)],
                    "search_contents": False,
                    "truncated": False,
                },
            )
        ],
        required_attachment_extensions=(".xlsx",),
    )

    assert "MEDIA:" in reply
    assert f"MEDIA:{workbook_artifact}" in reply
    assert str(workbook_artifact) not in reply.split("MEDIA:", 1)[0]

    verified_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "I ran the local command.\n\n```text\n"
            "- exists: True;\n"
            "- filename: stage_requested_file_delivery_contract.xlsx;\n"
            "- size_bytes: 1943;\n"
            "```"
        ),
        artifact_paths=[],
        prompt=(
            "Adjunta el archivo exacto llamado stage_requested_file_delivery_contract.xlsx "
            "como archivo descargable. No respondas con una lista de archivos ni con una ruta local."
        ),
        tool_results=[
            ToolResult(
                "search-files",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(workbook_artifact)}],
                    "matches": [str(workbook_artifact)],
                    "search_contents": False,
                    "truncated": True,
                },
            ),
            ToolResult(
                "verify-file",
                "terminal_exec",
                "completed",
                {
                    "exit_code": 0,
                    "stdout": (
                        "exists: True\n"
                        "filename: stage_requested_file_delivery_contract.xlsx\n"
                        "size_bytes: 1943\n"
                    ),
                    "stderr": "",
                },
            ),
        ],
        required_attachment_extensions=(".xlsx",),
    )

    assert "MEDIA:" in verified_reply
    assert f"MEDIA:{workbook_artifact}" in verified_reply
    assert "I ran the local command" not in verified_reply


def test_local_file_discovery_results_are_not_uploaded_as_attachments(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    found_pdf = artifact_root / "non_english_fixture.pdf"
    found_pdf.write_bytes(b"%PDF-1.4\n%found\n")
    inventory = artifact_root / "pdf_inventory.txt"
    inventory.write_text("PDF inventory\n\n- artifacts\n- uploads\n", encoding="utf-8")
    runtime = Runtime()
    runtime.store.get_active_task_frame_id = lambda conversation_id: None

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda rt: artifact_root)

    discovery_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done.\n\nMEDIA:{found_pdf}",
        artifact_paths=[],
        prompt="42 .pdf",
        tool_results=[
            ToolResult(
                "find-pdf",
                "terminal_exec",
                "completed",
                {"exit_code": 0, "stdout": f"{found_pdf}\n"},
            )
        ],
        required_attachment_extensions=(".pdf",),
    )

    assert discovery_reply == f"I found 1 file:\n- {found_pdf.name}"
    assert "MEDIA:" not in discovery_reply

    sidecar_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done -- attached the requested text file.\n\nMEDIA:{inventory}",
        artifact_paths=[str(inventory)],
        prompt="Find all the pdf files on my computer",
        tool_results=[
            ToolResult(
                "find-pdf",
                "terminal_exec",
                "completed",
                {"exit_code": 0, "stdout": f"{found_pdf}\n"},
            ),
            ToolResult(
                "inventory",
                "file_write",
                "completed",
                {"path": str(inventory), "bytes_written": inventory.stat().st_size},
            ),
        ],
        required_attachment_extensions=(),
    )

    assert sidecar_reply == f"I found 1 file:\n- {found_pdf.name}"
    assert str(inventory) not in sidecar_reply
    assert "MEDIA:" not in sidecar_reply

    model_media_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done -- attached the requested files.\n\nMEDIA:{found_pdf}\nMEDIA:{inventory}",
        artifact_paths=[],
        prompt="find local artifacts",
        tool_results=[
            ToolResult(
                "search-files",
                "file_search",
                "completed",
                {"matches": [{"path": str(found_pdf)}, {"path": str(inventory)}]},
            )
        ],
        required_attachment_extensions=(),
    )

    assert model_media_reply == f"I found 2 files:\n- {found_pdf.name}\n- {inventory.name}"
    assert "MEDIA:" not in model_media_reply
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    delivery = prepare_reply_for_platform_delivery(model_media_reply, principal_id="telegram:test")
    assert delivery.attachments == ()

    preview_payload = json.dumps(
        {
            "match_details": [
                {"match_type": "filename", "path": str(found_pdf)},
                {"match_type": "filename", "path": str(inventory)},
            ],
            "matches": [],
            "truncated": True,
        },
        sort_keys=True,
    )
    compressed_file_search_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"I found the PDFs.\n\nMEDIA:{found_pdf}\nMEDIA:{inventory}",
        artifact_paths=[],
        prompt="structured file listing request",
        tool_results=[
            ToolResult(
                "search-files-preview",
                "file_search",
                "completed",
                {"truncated": True, "original_chars": len(preview_payload), "preview": preview_payload},
            )
        ],
        required_attachment_extensions=(".pdf",),
    )

    assert compressed_file_search_reply == f"I found 2 files:\n- {found_pdf.name}\n- {inventory.name}"
    assert "MEDIA:" not in compressed_file_search_reply
    compressed_delivery = prepare_reply_for_platform_delivery(
        compressed_file_search_reply,
        principal_id="telegram:test",
    )
    assert compressed_delivery.attachments == ()

    scoped_file_search_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done — attached the requested PDF.\n\nMEDIA:{found_pdf}",
        artifact_paths=(),
        prompt="find the PDF file named non_english_fixture.pdf in my workspace and list it only",
        principal_id="telegram:test",
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "available_tools": ["file_patch", "file_read", "file_search", "file_write", "workspace_summary"],
                    "artifact_extensions": [],
                    "embedded_media_artifact_extensions": [],
                },
            ),
            ToolResult(
                "search-files-live-shape",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(found_pdf)}],
                    "matches": [str(found_pdf)],
                    "search_contents": False,
                    "truncated": False,
                },
            ),
        ],
        required_attachment_extensions=(),
    )
    enforced_file_search_reply = chat_operator._enforce_chat_response_fulfillment(
        runtime,
        conversation_id="telegram:test",
        prompt="find the PDF file named non_english_fixture.pdf in my workspace and list it only",
        reply=scoped_file_search_reply,
        principal_id="telegram:test",
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "available_tools": ["file_patch", "file_read", "file_search", "file_write", "workspace_summary"],
                    "artifact_extensions": [],
                    "embedded_media_artifact_extensions": [],
                },
            ),
            ToolResult(
                "search-files-live-shape",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(found_pdf)}],
                    "matches": [str(found_pdf)],
                    "search_contents": False,
                    "truncated": False,
                },
            ),
        ],
        required_attachment_extensions=(),
    )
    scoped_delivery = prepare_reply_for_platform_delivery(
        enforced_file_search_reply,
        principal_id="telegram:test",
    )
    assert enforced_file_search_reply == f"I found 1 file:\n- {found_pdf.name}"
    assert "MEDIA:" not in enforced_file_search_reply
    assert scoped_delivery.attachments == ()
    assert chat_operator._missing_required_attachment_extensions_for_repair(
        runtime,
        prompt="structured file listing request",
        principal_id="telegram:test",
        required_attachment_extensions=(".pdf",),
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "available_tools": ["file_read", "file_search", "workspace_summary"],
                    "artifact_extensions": [".pdf"],
                },
            ),
            ToolResult(
                "search-files-live-shape",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(found_pdf)}],
                    "matches": [str(found_pdf)],
                    "search_contents": False,
                    "truncated": False,
                },
            ),
        ],
        artifact_paths=(),
    ) == ()

    cron_created_reply = chat_operator._enforce_chat_response_fulfillment(
        runtime,
        conversation_id="telegram:test",
        prompt="Set up a scheduled report that will create spreadsheet and PDF artifacts when it runs.",
        reply="Cron created.",
        principal_id="telegram:test",
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "available_tools": ["create_cron", "spreadsheet_create", "pdf_create"],
                    "artifact_extensions": [".xlsx", ".pdf"],
                    "embedded_media_artifact_extensions": [".xlsx"],
                },
            ),
            ToolResult(
                "cron",
                "create_cron",
                "completed",
                {"message": "Cron created."},
            ),
        ],
        required_attachment_extensions=(".xlsx", ".pdf"),
        required_embedded_media_extensions=(".xlsx",),
    )
    assert cron_created_reply == "Cron created."

    stale_context_file_search_reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply="I couldn't attach the requested file. The task is still open.",
        artifact_paths=(),
        prompt="find the PDF file named non_english_fixture.pdf in my workspace and list it only",
        principal_id="telegram:test",
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "available_tools": ["file_patch", "file_read", "file_search", "file_write", "workspace_summary"],
                    "artifact_extensions": [],
                    "embedded_media_artifact_extensions": [],
                },
            ),
            ToolResult(
                "search-files-live-shape",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(found_pdf)}],
                    "matches": [str(found_pdf)],
                    "search_contents": False,
                    "truncated": False,
                },
            ),
        ],
        required_attachment_extensions=(".pdf",),
    )
    assert stale_context_file_search_reply == f"I found 1 file:\n- {found_pdf.name}"
    sanitized_stale_context_reply = chat_operator._sanitize_chat_reply(
        prompt="find the PDF file named non_english_fixture.pdf in my workspace and list it only",
        reply="I couldn't attach the requested file. The task is still open.",
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "available_tools": ["file_patch", "file_read", "file_search", "file_write", "workspace_summary"],
                    "artifact_extensions": [],
                    "embedded_media_artifact_extensions": [],
                },
            ),
            ToolResult(
                "search-files-live-shape",
                "file_search",
                "completed",
                {
                    "match_details": [{"match_type": "filename", "path": str(found_pdf)}],
                    "matches": [str(found_pdf)],
                    "search_contents": False,
                    "truncated": False,
                },
            ),
        ],
        source="agent",
    )
    assert sanitized_stale_context_reply == f"I found 1 file:\n- {found_pdf.name}"


def test_exact_requested_filename_from_terminal_stdout_is_attached(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    workbook = artifact_root / "stage_requested_file_delivery_contract.xlsx"
    workbook.write_bytes(b"PK\x03\x04fake workbook")
    runtime = Runtime()
    runtime.store.get_active_task_frame_id = lambda conversation_id: None

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda rt: artifact_root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply="I found 1 file:\n- stage_requested_file_delivery_contract.xlsx",
        artifact_paths=[],
        prompt=(
            "Adjunta el archivo exacto llamado stage_requested_file_delivery_contract.xlsx "
            "como archivo descargable. No respondas con una lista de archivos ni con una ruta local."
        ),
        tool_results=[
            ToolResult(
                "find-workbook",
                "terminal_exec",
                "completed",
                {"exit_code": 0, "stdout": f"{workbook}\n", "stderr": ""},
            )
        ],
        required_attachment_extensions=(),
    )

    assert reply == f"Done — attached the requested spreadsheet.\n\nMEDIA:{workbook.resolve()}"


def test_visible_file_write_text_artifact_is_attached_even_with_discovery(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    found_pdf_dir = tmp_path / "Downloads"
    found_pdf_dir.mkdir()
    found_pdf = found_pdf_dir / "statement.pdf"
    found_pdf.write_bytes(b"%PDF-1.4\n%found\n")
    inventory = artifact_root / "pdf_files_found.txt"
    inventory.write_text(str(found_pdf) + "\n", encoding="utf-8")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda rt: artifact_root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "I checked the text file and it is ready.\n\n"
            "**Artifact:** `pdf_files_found.txt`\n"
            "**Path:** `artifacts/pdf_files_found.txt`"
        ),
        artifact_paths=[str(inventory)],
        prompt="structured artifact request",
        tool_results=[
            ToolResult(
                "find-pdf",
                "terminal_exec",
                "completed",
                {"exit_code": 0, "stdout": f"{found_pdf}\n"},
            ),
            ToolResult(
                "inventory",
                "file_write",
                "completed",
                {"path": str(inventory), "bytes_written": inventory.stat().st_size},
            ),
        ],
        required_attachment_extensions=(),
    )

    assert f"MEDIA:{inventory}" in reply
    assert str(found_pdf) not in reply


def test_visible_terminal_created_text_artifact_named_for_source_files_is_attached(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    inventory = artifact_root / "pdf_files_found.txt"
    inventory.write_text("PDF inventory\n", encoding="utf-8")
    stale_primary = artifact_root / "pdf_files_found.pdf"
    stale_primary.write_bytes(b"%PDF-1.4\n%stale\n")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda rt: artifact_root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=(
            "I searched the folders and created the text file.\n\n"
            "**File:** `pdf_files_found.txt`\n"
            "**Saved at:** `artifacts/pdf_files_found.txt`"
        ),
        artifact_paths=[str(inventory)],
        prompt="structured artifact request",
        tool_results=[
            ToolResult(
                "find-pdf",
                "terminal_exec",
                "completed",
                {"exit_code": 0, "stdout": f"Wrote results to {inventory}\n"},
            )
        ],
        required_attachment_extensions=(),
    )

    assert f"MEDIA:{inventory}" in reply
    assert f"MEDIA:{stale_primary}" not in reply


def test_file_read_artifact_prevents_media_from_file_contents(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    inventory = artifact_root / "pdf_files_found.txt"
    old_pdf = artifact_root / "old_report.pdf"
    old_pdf.write_bytes(b"%PDF-1.4\n%old\n")
    inventory.write_text(str(old_pdf) + "\n", encoding="utf-8")
    runtime = Runtime()

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda rt: artifact_root)

    reply = chat_operator._append_chat_artifacts_to_reply(
        runtime,
        reply=f"Done — attached the requested files.\n\nMEDIA:{old_pdf}",
        artifact_paths=[],
        prompt="structured artifact request",
        tool_results=[
            ToolResult(
                "read-inventory",
                "file_read",
                "completed",
                {"path": str(inventory), "content": inventory.read_text(encoding="utf-8")},
            )
        ],
        required_attachment_extensions=(),
    )

    assert f"MEDIA:{inventory}" in reply
    assert f"MEDIA:{old_pdf}" not in reply
    assert "attached the requested text file" in reply


def test_chat_operator_foreground_delivery_uses_only_current_turn_artifacts(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "scoped-delivery-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    artifact_root = tmp_path / ".nullion-artifacts"
    artifact_root.mkdir()
    foreign_artifact = artifact_root / "foreign.png"
    foreign_artifact.write_bytes(b"foreign-image")
    current_artifact = artifact_root / "current.png"
    current_artifact.write_bytes(b"current-image")
    current_report = artifact_root / "current-report.txt"
    current_report.write_text("current report", encoding="utf-8")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: artifact_root)
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class TextOnlyOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                final_text="structured answer",
                artifacts=[],
                tool_results=[ToolResult("fetch", "web_fetch", "completed", {"url": "https://example.test/data"})],
            )

    text_reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat これは現在の情報だけ確認して",
        chat_id="123",
        settings=settings,
        agent_orchestrator=TextOnlyOrchestrator(),
        append_activity_trace=False,
    )

    assert "MEDIA:" not in text_reply
    assert str(foreign_artifact) not in text_reply

    class ArtifactOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                final_text="structured artifact ready",
                artifacts=[str(current_report)],
                tool_results=[ToolResult("artifact", "file_write", "completed", {"path": str(current_report)})],
            )

    artifact_reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat これを添付して",
        chat_id="123",
        settings=settings,
        agent_orchestrator=ArtifactOrchestrator(),
        append_activity_trace=False,
    )

    assert f"MEDIA:{current_report}" in artifact_reply
    assert str(foreign_artifact) not in artifact_reply

    class DiagnosticScreenshotOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                final_text=f"Done \u2014 attached the requested screenshot.\n\nMEDIA:{current_artifact}",
                artifacts=[],
                tool_results=[
                    ToolResult(
                        "shot",
                        "browser_screenshot",
                        "completed",
                        {
                            "path": str(current_artifact),
                            "artifact_path": str(current_artifact),
                            "artifact_paths": [str(current_artifact)],
                        },
                    ),
                    ToolResult(
                        "extract",
                        "browser_extract_text",
                        "completed",
                        {"text": "Movie showtimes from the page"},
                    ),
                ],
            )

    screenshot_reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat これを調べて",
        chat_id="123",
        settings=settings,
        agent_orchestrator=DiagnosticScreenshotOrchestrator(),
        append_activity_trace=False,
    )

    assert screenshot_reply == "Movie showtimes from the page"
    assert "MEDIA:" not in screenshot_reply
    assert str(current_artifact) not in screenshot_reply

    class PromotedDiagnosticScreenshotOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                final_text="I could not find a clean price in the visible page text.",
                artifacts=[str(current_artifact)],
                tool_results=[
                    ToolResult(
                        "shot",
                        "browser_screenshot",
                        "completed",
                        {
                            "path": str(current_artifact),
                            "artifact_path": str(current_artifact),
                            "artifact_paths": [str(current_artifact)],
                        },
                    ),
                    ToolResult(
                        "extract",
                        "browser_extract_text",
                        "completed",
                        {"text": "No visible prices were found on the page"},
                    ),
                ],
            )

    promoted_screenshot_reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat 料金だけ調べて",
        chat_id="123",
        settings=settings,
        agent_orchestrator=PromotedDiagnosticScreenshotOrchestrator(),
        append_activity_trace=False,
    )

    assert promoted_screenshot_reply == "No visible prices were found on the page"
    assert "MEDIA:" not in promoted_screenshot_reply
    assert str(current_artifact) not in promoted_screenshot_reply


def test_chat_operator_artifact_followup_does_not_reattach_stale_existing_file(
    tmp_path,
    monkeypatch,
) -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    runtime = bootstrap_persistent_runtime(tmp_path / "artifact-followup-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "nullion_public_awareness_github_traffic_monitor.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<html><body>" + ("traffic " * 20) + "</body></html>", encoding="utf-8")

    chat_operator._remember_chat_turn(
        runtime,
        chat_id="123",
        user_message="structured cron run",
        assistant_reply=f"Run complete.\nMEDIA {artifact.name}",
    )
    parent_turn_id = runtime.store.get_conversation_head("telegram:123")["active_turn_id"]
    decision = TurnDispatchDecision(
        policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
        dependency_turn_ids=(parent_turn_id,),
        disposition=ConversationTurnDisposition.CONTINUE,
        reason="model_structured_follow_up",
    )

    class NormalOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):
            return SimpleNamespace(final_text="Normal turn handled.", tool_results=[])

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: chat_operator.ArtifactWorkflowResult("none", "not_matched"),
    )
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    before_files = sorted(path.name for path in artifact.parent.iterdir())
    events: list[dict] = []

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat وصل الملف السابق مرة أخرى",
        chat_id="123",
        settings=settings,
        agent_orchestrator=NormalOrchestrator(),
        activity_callback=events.append,
        append_activity_trace=False,
        turn_dispatch_decision=decision,
    )

    assert reply == "Normal turn handled."
    assert "MEDIA:" not in reply
    assert artifact.name not in reply
    assert sorted(path.name for path in artifact.parent.iterdir()) == before_files
    assert "Reusing existing artifact" not in {event["label"] for event in events}


def test_remember_chat_turn_persists_sanitized_browser_evidence_reply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    runtime = bootstrap_persistent_runtime(tmp_path / "browser-evidence-runtime.db")
    prompt = "Can you research NYC Citi Bike e-bike helmet requirements for adults?"

    chat_operator._remember_chat_turn(
        runtime,
        chat_id="123",
        user_message=prompt,
        assistant_reply=(
            "Citi Bike recommends a helmet. Local age requirements vary."
        ),
        tool_results=[
            ToolResult(
                "extract-citi-bike",
                "browser_extract_text",
                "completed",
                {
                    "url": "https://www.nyc.gov/html/dot/html/bicyclists/ebikes.shtml",
                    "text": (
                        "NYC Resources\n"
                        "Do I need to wear a helmet? | Recommended (Yes, if working) "
                        "Required for 16-17 year olds. All riders and passengers under 18 must wear a helmet. "
                        "Citi Bike electric bicycles follow NYC e-bike rules."
                    ),
                },
            )
        ],
    )

    events = list(
        runtime.store.list_recent_conversation_events(
            "telegram:123",
            event_type="conversation.chat_turn",
            limit=1,
        )
    )
    assert events
    persisted_reply = events[0]["assistant_reply"]
    assert persisted_reply == "Citi Bike recommends a helmet. Local age requirements vary."
    assert "helmet" in persisted_reply
    assert "Citi Bike" in persisted_reply
    assert "I opened the page, but" not in persisted_reply


def test_render_chat_turn_delivers_sanitized_browser_evidence_over_cron_receipt_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    runtime = bootstrap_persistent_runtime(tmp_path / "browser-cron-receipt-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    blocker = (
        "Live-search blocker: the browser interaction needed to finish the request did not complete, "
        "so I do not have verified result records from this run.\n\n"
        "Requested task: Get me payment portal for olrp"
    )

    class BrowserCronReceiptOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                final_text="OLRP payment portal. Tenant Login: https://kmrm.appfolio.com/connect/users/sign_in",
                suspended_for_approval=False,
                approval_id=None,
                artifacts=[],
                tool_results=[
                    ToolResult(
                        "cron-receipt",
                        "run_cron",
                        "completed",
                        {"message": blocker, "delivery_status": "sent"},
                    ),
                    ToolResult(
                        "items",
                        "browser_extract_items",
                        "completed",
                        {
                            "session_id": "portal-session",
                            "page": {
                                "url": "https://kmrm.appfolio.com/connect/users/sign_in",
                                "title": "OLRP payment portal",
                            },
                            "items": [
                                {
                                    "title": "OLRP payment portal",
                                    "compact_text": "OLRP payment portal for tenant rent account login",
                                    "link_text": "Tenant Login",
                                    "url": "https://kmrm.appfolio.com/connect/users/sign_in",
                                    "fields": {"Portal": "Tenant Login"},
                                    "actions": [
                                        {
                                            "label": "Tenant Login",
                                            "url": "https://kmrm.appfolio.com/connect/users/sign_in",
                                        }
                                    ],
                                }
                            ],
                            "page_state_assertion": {
                                "schema_version": 1,
                                "kind": "page_state_assertion",
                                "session_id": "portal-session",
                                "verified": True,
                                "contract": {
                                    "required": ["OLRP payment portal"],
                                    "forbidden": [],
                                    "selector": "#portal",
                                },
                                "result": {
                                    "ok": True,
                                    "url": "https://kmrm.appfolio.com/connect/users/sign_in",
                                    "selector": "#portal",
                                    "required": [
                                        {
                                            "expected": "OLRP payment portal",
                                            "match": "OLRP payment portal",
                                        }
                                    ],
                                    "forbidden": [],
                                    "missing": [],
                                    "forbidden_found": [],
                                },
                            },
                        },
                    ),
                    ToolResult("wait", "browser_wait_for", "failed", {}, "Timed out waiting for navigation."),
                ],
            )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: chat_operator.ArtifactWorkflowResult("none", "not_matched"),
    )
    monkeypatch.setattr(chat_operator, "_should_use_plain_no_tool_direct_model_reply", lambda **kwargs: False)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)
    streamed: list[str] = []

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Get me payment portal for olrp",
        chat_id="123",
        settings=settings,
        agent_orchestrator=BrowserCronReceiptOrchestrator(),
        text_delta_callback=streamed.append,
        append_activity_trace=False,
    )

    assert "OLRP payment portal" in reply
    assert "Tenant Login: https://kmrm.appfolio.com/connect/users/sign_in" in reply
    assert "Live-search blocker" not in reply
    assert streamed and "".join(streamed) == str(reply)
    events = list(
        runtime.store.list_recent_conversation_events(
            "telegram:123",
            event_type="conversation.chat_turn",
            limit=1,
        )
    )
    assert events
    assert events[0]["assistant_reply"] == str(reply)


def test_chat_operator_numeric_reply_runs_structured_cron_selection_without_artifact_reuse(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    runtime = bootstrap_persistent_runtime(tmp_path / "cron-selection-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "pdf_inventory.txt"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("stale inventory", encoding="utf-8")
    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="run_cron",
            description="Run a scheduled cron job",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {"id": {"type": "string"}}},
            capability_tags=("scheduler", "cron"),
        ),
        lambda invocation: (
            invoked.append(dict(invocation.arguments or {}))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"message": "Ran selected cron.", "delivery_status": "sent"},
            )
        ),
    )
    runtime.active_tool_registry = registry
    chat_operator._remember_chat_turn(
        runtime,
        chat_id="123",
        user_message="Trigger my pre-market cron",
        assistant_reply=(
            "I found multiple matching cron jobs. Which one should I use?\n\n"
            "1. Daily Pre-Market Stock Research\n"
            "2. Mid-Market Stock Alert\n\n"
            "Reply with the number."
        ),
        tool_results=[
            ToolResult(
                "ambiguous-run-cron",
                "run_cron",
                "failed",
                {
                    "matches": [
                        {"selection_index": 1, "id": "cron-daily", "name": "Daily Pre-Market Stock Research"},
                        {"selection_index": 2, "id": "cron-mid", "name": "Mid-Market Stock Alert"},
                    ],
                },
                error="Multiple crons matched; ask the user to choose by number.",
            )
        ],
    )

    class NoModelOrchestrator:
        model_client = None

        def run_turn(self, **kwargs):
            raise AssertionError("numeric cron selection should run the stored structured tool choice directly")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("numeric cron selection must not use artifact reuse")),
    )
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    events: list[dict] = []

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat 1",
        chat_id="123",
        settings=settings,
        agent_orchestrator=NoModelOrchestrator(),
        activity_callback=events.append,
        append_activity_trace=False,
    )

    assert reply == "Ran selected cron."
    assert invoked == [{"id": "cron-daily"}]
    assert "pdf_inventory.txt" not in reply
    assert "Reusing existing artifact" not in {event["label"] for event in events}


def test_chat_operator_reply_to_scheduled_task_card_delete_requires_approval(tmp_path, monkeypatch) -> None:
    from nullion import crons
    from nullion.tools import register_cron_tools

    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "crons.db")
    runtime = bootstrap_persistent_runtime(tmp_path / "reply-cron-delete-runtime.db")
    registry = ToolRegistry()
    register_cron_tools(registry)
    runtime.active_tool_registry = registry
    target = crons.add_cron(
        "Peak Design shipment tracker",
        "0 21 * * *",
        "Check shipment status.",
        workspace_id="workspace_admin",
    )
    crons.add_cron(
        "Email deadline triage",
        "0 8 * * *",
        "Check email deadlines.",
        workspace_id="workspace_admin",
    )
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})

    class ScopeModel:
        def create(self, **kwargs):  # noqa: ANN001
            system = str(kwargs.get("system") or "")
            if "relationship" in system and "target_index" in system:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "relationship": "follow_up",
                                    "effect": "continue",
                                    "confidence": 0.97,
                                    "target_index": 0,
                                }
                            ),
                        }
                    ]
                }
            if "web_action" in system and "scheduler_action" in system:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "web_action": "none",
                                    "scheduler_action": "mutate",
                                    "skill_pack_action": "none",
                                    "requested_tool_names": ["delete_cron"],
                                    "required_tool_names": ["delete_cron"],
                                    "confidence": 0.95,
                                }
                            ),
                        }
                    ]
                }
            return {"content": [{"type": "text", "text": "{}"}]}

    class ShouldNotAskOrchestrator:
        model_client = ScopeModel()

        def run_turn(self, **kwargs):
            raise AssertionError("quoted cron delete should request approval directly")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_needs_required_attachment_repair", lambda *args, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Delete this cron",
        chat_id="123",
        settings=settings,
        agent_orchestrator=ShouldNotAskOrchestrator(),
        append_activity_trace=False,
        reply_context={
            "platform": "telegram",
            "reply_to_message_id": "telegram-message-1",
            "reply_to_from_bot": True,
            "reply_to_text": (
                "⏰ **SCHEDULED TASK**\n"
                "For: Peak Design shipment tracker\n"
                "◑ Running: Manual scheduled task run started.\n"
                "  Result will be delivered to this chat when ready."
            ),
        },
    )
    assert "approval before deleting" in reply
    assert "Peak Design shipment tracker" in reply
    assert crons.get_cron(target.id) is not None
    approvals = runtime.store.list_approval_requests()
    assert len(approvals) == 1
    assert approvals[0].action == "use_tool"
    assert approvals[0].resource == "delete_cron"
    assert [job.name for job in crons.list_crons(workspace_id="workspace_admin")] == [
        "Peak Design shipment tracker",
        "Email deadline triage",
    ]


def test_chat_operator_reply_to_scheduled_task_card_disables_quoted_cron(tmp_path, monkeypatch) -> None:
    from nullion import crons
    from nullion.tools import register_cron_tools

    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "crons.db")
    runtime = bootstrap_persistent_runtime(tmp_path / "reply-cron-disable-runtime.db")
    registry = ToolRegistry()
    register_cron_tools(registry)
    runtime.active_tool_registry = registry
    target = crons.add_cron(
        "Peak Design shipment tracker",
        "0 21 * * *",
        "Check shipment status.",
        workspace_id="workspace_admin",
        enabled=True,
    )
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})

    class ScopeModel:
        def create(self, **kwargs):  # noqa: ANN001
            system = str(kwargs.get("system") or "")
            if "relationship" in system and "target_index" in system:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "relationship": "follow_up",
                                    "effect": "continue",
                                    "confidence": 0.97,
                                    "target_index": 0,
                                }
                            ),
                        }
                    ]
                }
            if "web_action" in system and "scheduler_action" in system:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "web_action": "none",
                                    "scheduler_action": "mutate",
                                    "scheduler_toggle_enabled": False,
                                    "skill_pack_action": "none",
                                    "requested_tool_names": ["toggle_cron"],
                                    "required_tool_names": ["toggle_cron"],
                                    "confidence": 0.95,
                                }
                            ),
                        }
                    ]
                }
            return {"content": [{"type": "text", "text": "{}"}]}

    class ShouldNotAskOrchestrator:
        model_client = ScopeModel()

        def run_turn(self, **kwargs):
            raise AssertionError("quoted cron disable should run toggle_cron directly")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_needs_required_attachment_repair", lambda *args, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)

    events: list[dict[str, object]] = []
    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Disable this cron",
        chat_id="123",
        settings=settings,
        agent_orchestrator=ShouldNotAskOrchestrator(),
        activity_callback=events.append,
        append_activity_trace=False,
        reply_context={
            "platform": "telegram",
            "reply_to_message_id": "telegram-message-1",
            "reply_to_from_bot": True,
            "reply_to_text": (
                "⏰ **SCHEDULED TASK**\n"
                "For: Peak Design shipment tracker\n"
                "◑ Running: Manual scheduled task run started.\n"
                "  Result will be delivered to this chat when ready."
            ),
        },
    )

    assert "disabled" in reply
    assert "deleted" not in reply
    assert crons.get_cron(target.id).enabled is False
    assert [approval.resource for approval in runtime.store.list_approval_requests()] == []
    assert any(event.get("id") == "toggle-cron-reply-context" for event in events)


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
    running_tool_result = ToolResult(
        invocation_id="tool-1",
        tool_name="web_fetch",
        status="running",
        output={},
    )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
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

    captured_histories: list[list[dict]] = []

    class Orchestrator:
        model_client = object()

        def run_turn(self, *, tool_result_callback=None, **kwargs):
            captured_histories.append(list(kwargs.get("conversation_history") or ()))
            if tool_result_callback is not None:
                tool_result_callback(running_tool_result)
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
    web_fetch_events = [event for event in events if event.get("label") == "web_fetch"]
    assert [event.get("status") for event in web_fetch_events[:2]] == ["running", "done"]
    assert web_fetch_events[0]["id"] == web_fetch_events[1]["id"]
    assert chat_operator._recent_tool_context_prompt(runtime, "telegram:123")

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat separate request",
        chat_id="123",
        settings=settings,
        agent_orchestrator=Orchestrator(),
        activity_callback=events.append,
    )
    assert "Model answer" in reply
    latest_history = captured_histories[-1]
    assert not any(
        "Historical, timestamped tool outcomes" in str(block)
        for message in latest_history
        for block in message.get("content", [])
    )

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


def test_chat_operator_exposes_current_conversation_history_tool_beyond_visible_window(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "history-runtime.db")
    conversation_id = "telegram:123"
    for index in range(30):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 25, 12, index, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-{index}",
                "user_message": (
                    "NJ Transit train 372 departs at 1:14 PM"
                    if index == 1
                    else f"ordinary chat turn {index}"
                ),
                "assistant_reply": (
                    "It reaches New York Penn Station at 2:08 PM."
                    if index == 1
                    else f"ordinary assistant reply {index}"
                ),
            }
        )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class HistoryLookupOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.initial_tool_names: set[str] = set()
            self.widened_tool_names: set[str] = set()

        def run_turn(self, **kwargs):
            tool_registry = kwargs["tool_registry"]
            self.initial_tool_names = {definition["name"] for definition in tool_registry.list_tool_definitions()}
            scope_result, widened_registry = tool_registry.apply_scope_request(
                ToolInvocation(
                    invocation_id="scope-history",
                    tool_name="request_tool_scope",
                    principal_id="telegram_chat",
                    arguments={"capabilities": ["conversation_history"]},
                )
            )
            self.widened_tool_names = {definition["name"] for definition in widened_registry.list_tool_definitions()}
            lookup_result = widened_registry.invoke(
                ToolInvocation(
                    invocation_id="search-history",
                    tool_name="chat_history_search",
                    principal_id="telegram_chat",
                    arguments={"query": "NJ Transit train 372", "limit": 3},
                )
            )
            match = lookup_result.output["matches"][0]
            return SimpleNamespace(
                tool_results=[scope_result, lookup_result],
                suspended_for_approval=False,
                approval_id=None,
                final_text=match["assistant_reply"],
                artifacts=[],
            )

    orchestrator = HistoryLookupOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="Search chat",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert "chat_history_search" in orchestrator.widened_tool_names
    assert "2:08 PM" in reply


def test_chat_operator_exposes_saved_history_tool_when_context_is_outside_visible_window(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-runtime.db")
    conversation_id = "telegram:123"
    for index in range(30):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 25, 12, index, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-{index}",
                "user_message": (
                    "tren 372 salida confirmada"
                    if index == 1
                    else "ese tren 372 Penn?"
                    if index == 28
                    else "tren Penn salida anterior"
                    if index == 0
                    else f"turno ordinario {index}"
                ),
                "assistant_reply": (
                    "El tren 372 llega a Penn a las 2:08 PM."
                    if index == 1
                    else "No tengo datos suficientes del tren 372 Penn."
                    if index == 28
                    else "Otro tren a Penn sin numero."
                    if index == 0
                    else f"respuesta ordinaria {index}"
                ),
            }
        )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class AutoHistoryOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.history_text = ""
            self.initial_tool_names: list[str] = []
            self.widened_tool_names: list[str] = []

        def run_turn(self, **kwargs):
            blocks: list[str] = []
            for message in kwargs["conversation_history"]:
                for block in message.get("content", []):
                    if isinstance(block, dict):
                        blocks.append(str(block.get("text") or ""))
            self.history_text = "\n".join(blocks)
            registry = kwargs["tool_registry"]
            self.initial_tool_names = [
                str(spec.get("name") if isinstance(spec, dict) else getattr(spec, "name", ""))
                for spec in registry.list_tool_definitions()
            ]
            assert "Automatic saved-chat lookup" not in self.history_text
            assert "chat_history_search" in self.initial_tool_names
            scope_result, widened_registry = registry.apply_scope_request(
                ToolInvocation(
                    invocation_id="scope-history",
                    tool_name="request_tool_scope",
                    principal_id="telegram_chat",
                    arguments={"capabilities": ["conversation_history"]},
                )
            )
            self.widened_tool_names = [
                str(spec.get("name") if isinstance(spec, dict) else getattr(spec, "name", ""))
                for spec in widened_registry.list_tool_definitions()
            ]
            assert "chat_history_search" in self.widened_tool_names
            return SimpleNamespace(
                tool_results=[scope_result],
                suspended_for_approval=False,
                approval_id=None,
                final_text="I can search saved chat history for that train.",
                artifacts=[],
            )

    orchestrator = AutoHistoryOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="ese tren 372 Penn?",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert "saved chat history" in reply
    assert "chat_history_search" in orchestrator.widened_tool_names


def test_chat_operator_uses_saved_history_when_user_answers_previous_clarification(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "clarification-history-runtime.db")
    conversation_id = "telegram:123"
    registry = ToolRegistry()
    for tool_name in ("browser_navigate", "browser_extract_text", "browser_extract_items", "browser_open"):
        registry.register(
            ToolSpec(
                tool_name,
                tool_name.replace("_", " "),
                ToolRiskLevel.LOW,
                ToolSideEffectClass.READ,
                False,
                5,
                input_schema={"type": "object", "properties": {}},
                capability_tags=("web", "browser"),
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    runtime.active_tool_registry = registry
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 12, 0, tzinfo=UTC).isoformat(),
            "turn_id": "turn-pr-evidence",
            "user_message": "earlier repo status",
            "assistant_reply": "PR target: https://github.com/sample-team/sample-app/pull/28 is merged.",
            "structured_refs": [
                {
                    "type": "url",
                    "url": "https://github.com/sample-team/sample-app/pull/28",
                    "domain": "github.com",
                }
            ],
        }
    )
    chat_operator._remember_chat_turn(
        runtime,
        chat_id="123",
        user_message="Can you check on my PR?",
        assistant_reply=(
            "Which PR would you like me to check?\n\n"
            "1. Provide the link, repository, or PR number.\n"
            "2. Use saved conversation history to find the PR target."
        ),
    )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_compact_clarification_followup_history_context",
        lambda **_kwargs: "Clarification follow-up context: use saved history for the missing PR target.",
    )
    monkeypatch.setattr(
        chat_operator,
        "_automatic_saved_chat_history_prompt",
        lambda *args, **kwargs: "\n".join(
            [
                "Automatic saved-chat lookup for this turn found candidate prior turns.",
                "Structured saved reference candidates (ranked by relevance and recency):",
                'Target 1: {"domain": "github.com", "type": "url", "url": "https://github.com/sample-team/sample-app/pull/28"}',
                "Target source assistant: PR target: https://github.com/sample-team/sample-app/pull/28 is merged.",
                "Candidate prior turns:",
            ]
        ),
    )

    class ClarificationHistoryOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.history_text = ""
            self.tool_names: list[str] = []

        def run_turn(self, **kwargs):  # noqa: ANN001
            blocks: list[str] = []
            for message in kwargs["conversation_history"]:
                for block in message.get("content", []):
                    if isinstance(block, dict):
                        blocks.append(str(block.get("text") or ""))
            self.history_text = "\n".join(blocks)
            self.tool_names = [
                str(spec.get("name") if isinstance(spec, dict) else getattr(spec, "name", ""))
                for spec in kwargs["tool_registry"].list_tool_definitions()
            ]
            assert "Clarification follow-up context" in self.history_text
            assert "github.com/sample-team/sample-app/pull/28" in self.history_text
            assert "chat_history_search" in self.tool_names
            assert "browser_navigate" in self.tool_names
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text="I found the PR target in saved history: PR #28 is merged.",
                artifacts=[],
            )

    orchestrator = ClarificationHistoryOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="Check in the history",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert "PR #28 is merged" in reply
    assert "What history" not in reply


def test_chat_operator_does_not_auto_scan_saved_history_for_independent_plain_turn(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-fast-path-runtime.db")
    runtime.store.add_conversation_event(
        {
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 12, 0, tzinfo=UTC).isoformat(),
            "turn_id": "turn-old",
            "user_message": "old project detail",
            "assistant_reply": "Saved old project detail.",
        }
    )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_automatic_saved_chat_history_prompt",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("plain turns should not pre-scan saved history")),
    )
    monkeypatch.setattr(
        chat_operator,
        "_compact_clarification_followup_history_context",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("plain turns should not run clarification-history classifier")),
    )
    monkeypatch.setattr(
        chat_operator,
        "_compact_unstructured_target_clarification_reply",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("plain turns should not run target-clarification classifier")),
    )

    class PlainOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text="Plain answer",
                artifacts=[],
            )

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat tell me something simple",
        chat_id="123",
        settings=settings,
        agent_orchestrator=PlainOrchestrator(),
        append_activity_trace=False,
    )

    assert "Plain answer" in reply


def test_chat_operator_recovers_saved_tool_context_for_linked_turn_after_restart(
    tmp_path,
    monkeypatch,
) -> None:
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "restart-context-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 6, 9, 14, 38, tzinfo=UTC).isoformat(),
            "turn_id": "turn-email-thread",
            "user_message": "revisar conversacion Rosemarie contrato firmado",
            "assistant_reply": "Revise el hilo de Rosemarie y encontre detalles del contrato.",
            "tool_results": [
                {
                    "tool_name": "request_tool_scope",
                    "status": "completed",
                    "output": {"available_tools": ["email_search", "email_read"]},
                },
                {
                    "tool_name": "email_search",
                    "status": "completed",
                    "output": {"messages": [{"id": "msg-lease", "thread_id": "thread-lease"}]},
                },
                {
                    "tool_name": "email_read",
                    "status": "completed",
                    "output": {
                        "message": {
                            "id": "msg-lease",
                            "thread_id": "thread-lease",
                            "from": "rosemarie@example.test",
                            "body": (
                                "Rosemarie agrego a John y envio el contrato, "
                                "pero todavia no envio el contrato firmado."
                            ),
                        }
                    },
                },
            ],
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 6, 9, 14, 39, tzinfo=UTC).isoformat(),
            "turn_id": "turn-plain-followup",
            "user_message": "Ella agrego a John y mando el contrato, pero no firmado",
            "assistant_reply": "Eso coincide con el hilo guardado.",
            "tool_results": [],
        }
    )

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_compact_clarification_followup_history_context",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("restart recovery should not call classifier")),
    )

    class RestartRecoveryOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.history_text = ""

        def run_turn(self, **kwargs):
            self.history_text = "\n".join(
                str(block.get("text") or "")
                for message in kwargs["conversation_history"]
                for block in message.get("content", [])
                if isinstance(block, dict)
            )
            assert "Recent runtime-evidence-backed saved turns" in self.history_text
            assert "email_read" in self.history_text
            assert "contrato firmado" in self.history_text
            assert "Ambiguous compact follow-up guard" not in self.history_text
            return SimpleNamespace(
                tool_results=[],
                suspended_for_approval=False,
                approval_id=None,
                final_text="Si, tengo el hilo guardado: aun falta el contrato firmado.",
                artifacts=[],
            )

    orchestrator = RestartRecoveryOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="ese contrato firmado?",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        turn_dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL,
            disposition=chat_operator.ConversationTurnDisposition.CONTINUE,
            dependency_turn_ids=("turn-email-thread",),
            reason="explicit_reply_context",
        ),
    )

    assert "falta el contrato firmado" in reply
    assert "thread-lease" in orchestrator.history_text


def test_compact_followup_guard_skips_fresh_session_without_prior_context() -> None:
    assert (
        chat_operator._compact_unstructured_followup_guard_prompt(
            prompt="neutral compact request",
            reply_context_prompt=None,
            numbered_option_context=None,
            has_prior_context=False,
        )
        is None
    )


def test_chat_operator_does_not_treat_numbered_reply_as_saved_history_retry_signal(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-clarification-retry-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 28, 12, 0, tzinfo=UTC).isoformat(),
            "turn_id": "turn-pr-cli",
            "user_message": "repo sample-team/sample-app",
            "assistant_reply": "Checked the saved pull request target with local CLI evidence.",
            "tool_results": [
                {
                    "tool_name": "terminal_exec",
                    "status": "completed",
                    "output": {
                        "stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n",
                    },
                }
            ],
        }
    )
    for index in range(8):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 28, 12, index + 1, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-filler-{index}",
                "user_message": f"mensaje intermedio {index}",
                "assistant_reply": f"respuesta intermedia {index}",
            }
        )
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="terminal_exec",
            description="Run a local read-only command.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            capability_tags=("local_shell",),
        ),
        lambda invocation: ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n"},
        ),
    )
    runtime.active_tool_registry = registry

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class ClarificationThenHistoryOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.calls: list[dict] = []

        def run_turn(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return SimpleNamespace(
                    tool_results=[],
                    suspended_for_approval=False,
                    approval_id=None,
                    final_text="Necesito elegir una fuente:\n\n1. Sistema A\n2. Sistema B",
                    artifacts=[],
                )
            history_text = "\n".join(
                str(block.get("text") or "")
                for message in kwargs["conversation_history"]
                for block in message.get("content", [])
                if isinstance(block, dict)
            )
            tool_names = {definition["name"] for definition in kwargs["tool_registry"].list_tool_definitions()}
            assert "Saved prior runtime-evidence context" in history_text
            assert "terminal_exec" in tool_names
            return SimpleNamespace(
                tool_results=[
                    ToolResult(
                        invocation_id="cli-prs",
                        tool_name="terminal_exec",
                        status="completed",
                        output={"stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n"},
                    )
                ],
                suspended_for_approval=False,
                approval_id=None,
                final_text="Usé el historial guardado y revisé sample-team/sample-app #28.",
                artifacts=[],
            )

    orchestrator = ClarificationThenHistoryOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat sample-team/sample-app?",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert len(orchestrator.calls) == 1
    assert "Sistema A" in reply
    assert "sample-team/sample-app #28" not in reply


def test_chat_operator_retries_history_only_answer_with_saved_evidence_tool(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-only-retry-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 28, 12, 0, tzinfo=UTC).isoformat(),
            "turn_id": "turn-pr-cli",
            "user_message": "repo sample-team/sample-app",
            "assistant_reply": "Checked the saved pull request target with local CLI evidence.",
            "tool_results": [
                {
                    "tool_name": "terminal_exec",
                    "status": "completed",
                    "output": {"stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n"},
                }
            ],
        }
    )
    for index in range(8):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 28, 12, index + 1, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-history-only-filler-{index}",
                "user_message": f"mensaje intermedio {index}",
                "assistant_reply": f"respuesta intermedia {index}",
            }
        )
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="terminal_exec",
            description="Run a local read-only command.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            capability_tags=("local_shell",),
        ),
        lambda invocation: ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n"},
        ),
    )
    runtime.active_tool_registry = registry

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_user_context_prompt_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "update_active_task_frame_from_outcomes", lambda *args, **kwargs: None)

    class HistoryOnlyThenLiveOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.calls: list[dict] = []

        def run_turn(self, **kwargs):
            self.calls.append(kwargs)
            tool_names = {definition["name"] for definition in kwargs["tool_registry"].list_tool_definitions()}
            if len(self.calls) == 1:
                return SimpleNamespace(
                    tool_results=[
                        ToolResult(
                            invocation_id="history-only",
                            tool_name="chat_history_search",
                            status="completed",
                            output={"matches": []},
                        )
                    ],
                    suspended_for_approval=False,
                    approval_id=None,
                    final_text="Saved history says sample-team/sample-app #28 existed.",
                    artifacts=[],
                )
            assert "terminal_exec" in tool_names
            history_text = "\n".join(
                str(block.get("text") or "")
                for message in kwargs["conversation_history"]
                for block in message.get("content", [])
                if isinstance(block, dict)
            )
            assert "previous attempt used saved chat history" in history_text
            return SimpleNamespace(
                tool_results=[
                    ToolResult(
                        invocation_id="cli-prs",
                        tool_name="terminal_exec",
                        status="completed",
                        output={"stdout": "sample-team/sample-app #28 Prepare release reliability fixes\n"},
                    )
                ],
                suspended_for_approval=False,
                approval_id=None,
                final_text="I did a fresh CLI check for sample-team/sample-app #28.",
                artifacts=[],
            )

    orchestrator = HistoryOnlyThenLiveOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat sample-team/sample-app?",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert len(orchestrator.calls) == 2
    assert reply == "I did a fresh CLI check for sample-team/sample-app #28."


def test_chat_operator_saved_history_prompt_does_not_fall_back_to_unrelated_recent_turns(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-recent-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 12, 1, tzinfo=UTC).isoformat(),
            "turn_id": "turn-pr-context",
            "user_message": "PR 28",
            "assistant_reply": "PR 28 is waiting on the Windows installer check.",
        }
    )

    prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="zz-no-match",
        visible_turns=[],
    )

    assert prompt is None


def test_chat_operator_saved_history_prompt_adds_recent_context_to_direct_matches(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-direct-plus-recent-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 12, 1, tzinfo=UTC).isoformat(),
            "turn_id": "turn-direct-match",
            "user_message": "alpha",
            "assistant_reply": "Need more structured target data.",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 12, 2, tzinfo=UTC).isoformat(),
            "turn_id": "turn-recent-target",
            "user_message": "target-42",
            "assistant_reply": "Use local_shell for target-42 checks.",
        }
    )

    prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="alpha",
        visible_turns=[],
    )

    assert prompt is not None
    assert "Need more structured target data" in prompt
    assert "target-42" in prompt
    assert "Use local_shell" in prompt


def test_chat_operator_saved_history_prompt_orders_recent_candidates_first(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-ranking-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 12, 1, tzinfo=UTC).isoformat(),
            "turn_id": "turn-stale",
            "user_message": "beta item update status beta item update",
            "assistant_reply": "Need more structured target data.",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 25, 12, 2, tzinfo=UTC).isoformat(),
            "turn_id": "turn-latest",
            "user_message": "find beta",
            "assistant_reply": "beta item resolved as catalog target 4821 and source link ready.",
        }
    )

    prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="beta item update?",
        visible_turns=[],
    )

    assert prompt is not None
    assert prompt.index("beta item resolved as catalog target 4821") < prompt.index("Need more structured target data")


def test_chat_operator_saved_history_prompt_prefers_adjacent_reply_over_later_side_turn(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-adjacent-runtime.db")
    conversation_id = "telegram:123"
    matches = [
        {
            "created_at": datetime(2026, 5, 27, 12, 2, tzinfo=UTC).isoformat(),
            "context": "following_turn",
            "match_score": 17,
            "user_message": "target item side note",
            "assistant_reply": "Need separate runtime details for the side path.",
        },
        {
            "created_at": datetime(2026, 5, 25, 12, 1, tzinfo=UTC).isoformat(),
            "context": "following_turn",
            "match_score": 18,
            "assistant_reply": "target item resolved as catalog target 4821 and source link ready.",
        },
    ]

    class HistoryRegistry:
        def invoke(self, invocation):
            if invocation.arguments.get("query"):
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="completed",
                    output={"matches": matches, "searched_turn_count": len(matches)},
                )
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={"matches": [], "searched_turn_count": len(matches)},
            )

    monkeypatch.setattr(chat_operator, "with_conversation_history_tool", lambda *args, **kwargs: HistoryRegistry())

    prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="target item update?",
        visible_turns=[],
    )

    assert prompt is not None
    assert prompt.index("target item resolved as catalog target 4821") < prompt.index("Need separate runtime details")
    assert "Highest-priority candidate" in prompt
    assert "ground the answer in these saved facts first" in prompt
    reference_message = chat_operator._saved_history_reference_context_message(prompt)
    assert reference_message is not None
    assert reference_message["role"] == "assistant"
    assert "target item resolved as catalog target 4821" in reference_message["content"][0]["text"]


def test_chat_operator_saved_history_prompt_surfaces_recent_structured_targets_over_generic_match(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-structured-target-runtime.db")
    conversation_id = "telegram:123"
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

    prompt = chat_operator._automatic_saved_chat_history_prompt(
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
    reference_message = chat_operator._saved_history_reference_context_message(prompt)
    assert reference_message is not None
    reference_text = reference_message["content"][0]["text"]
    assert "https://records.example.test/workspaces/release-7/items/28" in reference_text
    assert "Please send the target details" not in reference_text
    assert "verify fresh/current status with live tools" in reference_text


def test_chat_operator_saved_history_prompt_uses_matching_scheduled_delivery_descriptor(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-scheduled-delivery-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "workspace_id": "workspace_admin",
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 29, 13, 8, tzinfo=UTC).isoformat(),
            "turn_id": "turn-shoes-delivery",
            "user_message": (
                "[Scheduled task delivery context] task=New Balance shoes delivery tracker; "
                "channel=web; artifacts=none"
            ),
            "assistant_reply": (
                "New Balance shipment tracker update for order U16461913 / item U906027Z-D-10: "
                "UPS delivery estimate found: Friday 05/29/2026 between 4:30 PM and 8:30 PM."
            ),
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "workspace_id": "workspace_admin",
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 29, 14, 35, tzinfo=UTC).isoformat(),
            "turn_id": "turn-pr-status",
            "user_message": (
                "[Scheduled task delivery context] task=Check PR #29 merge readiness every 2 minutes; "
                "channel=web; artifacts=none"
            ),
            "assistant_reply": "PR #29 checks are green.",
        }
    )
    for index in range(20):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "workspace_id": "workspace_admin",
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 29, 14, 36 + index, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-generic-check-{index}",
                "user_message": f"Can u check unrelated saved item {index}?",
                "assistant_reply": f"Checked unrelated saved item {index}.",
            }
        )

    prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="Can u check where those shoes are?",
        visible_turns=[
            {
                "user": "Can u check where those shoes are?",
                "assistant": "On it! Feel free to send other tasks.",
            }
        ],
    )

    assert prompt is not None
    assert "New Balance shoes delivery tracker" in prompt
    assert "U16461913" in prompt
    reference_message = chat_operator._saved_history_reference_context_message(prompt)
    assert reference_message is not None
    reference_text = reference_message["content"][0]["text"]
    assert "New Balance shoes delivery tracker" in reference_text
    assert "U16461913" in reference_text
    assert "Check PR #29" not in reference_text


def test_saved_history_prompt_resolves_non_english_short_reference_from_structured_task_descriptor(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-non-english-structured-task-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "workspace_id": "workspace_admin",
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 29, 9, 0, tzinfo=UTC).isoformat(),
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
            "workspace_id": "workspace_admin",
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 29, 9, 10, tzinfo=UTC).isoformat(),
            "turn_id": "turn-lab-sample",
            "user_message": (
                "[Scheduled task delivery context] task=lab sample cold-chain tracker; "
                "channel=discord; artifacts=none"
            ),
            "assistant_reply": "Cold-chain sample CS-17 is delayed at the intake dock.",
        }
    )
    for index in range(12):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "workspace_id": "workspace_admin",
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 29, 9, 11 + index, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-generic-status-{index}",
                "user_message": f"status check {index}",
                "assistant_reply": f"generic status result {index}",
            }
        )

    prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="donde esta el conference badge packet?",
        visible_turns=[],
    )

    assert prompt is not None
    assert "Structured saved reference candidates" in prompt
    assert '"type": "scheduled_task"' in prompt
    assert "conference badge packet courier tracker" in prompt
    assert "BPK-441" in prompt
    assert "lab sample cold-chain tracker" not in chat_operator._saved_history_reference_context_message(prompt)["content"][0]["text"]


def test_chat_operator_saved_history_structured_target_fallback_is_single_target_only() -> None:
    single_target_context = "\n".join(
        [
            "Automatic saved-chat lookup for this turn found candidate prior turns.",
            "Structured saved reference candidates (ranked by relevance and recency):",
            'Target 1: {"artifacts": "none", "channel": "web", "task": "New Balance shoes delivery tracker", "type": "scheduled_task"}',
            "Target source user: [Scheduled task delivery context] task=New Balance shoes delivery tracker; channel=web; artifacts=none",
            "Target source assistant: New Balance shipment tracker update for order U16461913 / item U906027Z-D-10.",
            "These targets come from structured runtime evidence such as URLs or task descriptors.",
            "Candidate prior turns:",
        ]
    )
    multiple_target_context = "\n".join(
        [
            "Automatic saved-chat lookup for this turn found candidate prior turns.",
            "Structured saved reference candidates (ranked by relevance and recency):",
            'Target 1: {"task": "New Balance shoes delivery tracker", "type": "scheduled_task"}',
            "Target source assistant: New Balance shipment tracker update.",
            'Target 2: {"task": "Check PR #29 merge readiness", "type": "scheduled_task"}',
            "Target source assistant: PR #29 checks are green.",
            "Candidate prior turns:",
        ]
    )

    fallback = chat_operator._saved_history_structured_target_fallback_reply(single_target_context)

    assert fallback is not None
    assert "New Balance shoes delivery tracker" in fallback
    assert "U16461913" in fallback
    assert chat_operator._saved_history_structured_target_fallback_reply(multiple_target_context) is None
    assert chat_operator._saved_history_structured_target_fallback_reply("Candidate prior turns:") is None


def test_shared_saved_history_retry_runs_platform_callback_and_applies_fallback() -> None:
    class Registry:
        def __init__(self, name: str) -> None:
            self.name = name

        def list_specs(self):
            return []

        def list_tool_definitions(self):
            return []

    first_result = SimpleNamespace(
        final_text="Which packet?\n1. Photo\n2. Link",
        tool_results=[ToolResult("history-1", chat_operator.CHAT_HISTORY_SEARCH_TOOL_NAME, "completed", {"matches": []})],
        artifacts=[],
        suspended_for_approval=False,
    )
    retry_result = SimpleNamespace(
        final_text="Which packet?\n1. Photo\n2. Link",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    history_context = "\n".join(
        [
            "Automatic saved-chat lookup for this turn found candidate prior turns.",
            "Structured saved reference candidates (ranked by relevance and recency):",
            'Target 1: {"task": "conference badge packet courier tracker", "type": "scheduled_task"}',
            "Target source assistant: Courier packet BPK-441 is out for delivery to the west lobby desk.",
            "Candidate prior turns:",
        ]
    )
    calls: list[tuple[list[dict[str, object]], object]] = []
    marks: list[str] = []

    def run_turn(history: list[dict[str, object]], registry: object):
        calls.append((history, registry))
        return retry_result

    result, registry, retried = chat_operator._run_chat_turn_saved_history_retry(
        initial_result=first_result,
        automatic_history_context=history_context,
        allow_saved_history_retry=True,
        build_history_context=lambda: None,
        base_history=[{"role": "system", "content": [{"type": "text", "text": "base"}]}],
        active_tool_registry=Registry("active"),
        base_tool_registry=Registry("base"),
        turn_tool_evidence=SimpleNamespace(),
        run_turn=run_turn,
        mark_retry=lambda: marks.append("retry"),
    )

    assert retried is True
    assert result is retry_result
    assert registry.name == "active"
    assert marks == ["retry"]
    assert len(calls) == 1
    retry_history, retry_registry = calls[0]
    assert retry_registry is registry
    assert any(
        "conference badge packet courier tracker" in block.get("text", "")
        for message in retry_history
        for block in message.get("content", [])
        if isinstance(block, dict)
    )
    assert "BPK-441" in retry_result.final_text


def test_shared_saved_history_retry_preserves_clarification_for_unanchored_vague_reference() -> None:
    class Registry:
        def list_specs(self):
            return []

        def list_tool_definitions(self):
            return []

    first_result = SimpleNamespace(
        final_text="What should I pick from?\n1. Crons\n2. Recent files",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    history_context = "\n".join(
        [
            "Automatic saved-chat lookup for this turn found candidate prior turns.",
            "Structured saved reference candidates (ranked by relevance and recency):",
            'Target 1: {"task": "Random website artifact generator", "type": "scheduled_task"}',
            "Target source assistant: [Scheduled task delivery context] task=Random website artifact generator; channel=telegram; artifacts=random_website_stress_rows.csv, random_website_stress_workbook.xlsx",
            "Candidate prior turns:",
        ]
    )

    result, _registry, retried = chat_operator._run_chat_turn_saved_history_retry(
        initial_result=first_result,
        automatic_history_context=history_context,
        allow_saved_history_retry=True,
        build_history_context=lambda: None,
        base_history=[],
        active_tool_registry=Registry(),
        base_tool_registry=Registry(),
        turn_tool_evidence=SimpleNamespace(),
        run_turn=lambda _history, _registry: pytest.fail("plain numbered clarification must not retry"),
    )

    assert retried is False
    assert result is first_result
    assert "I found the saved chat target" not in result.final_text
    assert "Random website artifact generator" not in result.final_text


def test_shared_saved_history_retry_does_not_retry_complete_no_tool_numbered_reply() -> None:
    class Registry:
        def list_specs(self):
            return []

        def list_tool_definitions(self):
            return [{"name": "calendar_list", "description": "read calendar", "input_schema": {}}]

    first_result = SimpleNamespace(
        final_text="Choose next action:\n1. Keep current status\n2. Open review",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    history_context = "\n".join(
        [
            "Automatic saved-chat lookup for this turn found candidate prior turns.",
            "Recent runtime-evidence-backed saved turns (ranked by relevance and recency):",
            "Evidence 1.",
            "Tools: calendar_list",
            "User: opaque prior request",
            "Assistant: prior answer",
        ]
    )
    calls: list[str] = []

    result, registry, retried = chat_operator._run_chat_turn_saved_history_retry(
        initial_result=first_result,
        automatic_history_context=history_context,
        allow_saved_history_retry=True,
        build_history_context=lambda: calls.append("context") or history_context,
        base_history=[],
        active_tool_registry=Registry(),
        base_tool_registry=Registry(),
        turn_tool_evidence=SimpleNamespace(context_linked=False),
        run_turn=lambda _history, _registry: calls.append("run") or first_result,
    )

    assert retried is False
    assert result is first_result
    assert registry is not None
    assert calls == []


def test_common_retries_do_not_reset_an_exhausted_agent_iteration_budget() -> None:
    class Registry:
        def list_specs(self):
            return []

        def list_tool_definitions(self):
            return []

    initial = SimpleNamespace(
        final_text="The task is still open.",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        reached_iteration_limit=True,
    )

    outcome = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=Registry(),
        base_tool_registry=Registry(),
        user_message="opaque request",
        evidence=SimpleNamespace(),
        run_turn=lambda _history, _registry: pytest.fail("iteration-limited turn must not restart"),
    )

    assert outcome.result is initial
    assert outcome.retried_model_tool_request is False
    assert outcome.retried_schema_followup is False


def test_chat_turn_limit_persistence_failure_is_visible_partial_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion.agent_orchestrator import TurnResult

    settings = load_settings(
        env={
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "limit-persistence-failure.db")
    runtime.active_tool_registry = ToolRegistry()
    monkeypatch.setattr(
        chat_operator,
        "_telegram_screenshot_reply_if_requested",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(
        chat_operator,
        "_chat_capability_inventory_prompt",
        lambda runtime, **kwargs: None,
    )
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_user_context_prompt_for_chat",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        chat_operator,
        "_broadcast_new_workspace_approvals",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_enforce_chat_response_fulfillment",
        lambda runtime, **kwargs: kwargs["reply"],
    )
    monkeypatch.setattr(
        chat_operator,
        "update_active_task_frame_from_outcomes",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        chat_operator,
        "_store_agent_turn_limit_extension_suspended_turn",
        lambda *args, **kwargs: None,
    )
    observed_outcomes = []
    monkeypatch.setattr(
        chat_operator,
        "_schedule_builder_reflection",
        lambda *args, **kwargs: observed_outcomes.append(kwargs["outcome"]),
    )

    class Orchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            return TurnResult(
                turn_id="turn-limit-persistence-failure",
                final_text="Product B — $25.00",
                tool_results=[
                    ToolResult(
                        "failed-file-write",
                        "file_write",
                        "failed",
                        {"reason": "disk_unavailable"},
                        error="disk unavailable",
                    )
                ],
                reached_iteration_limit=True,
                response_fulfilled=None,
            )

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat compare the two current products",
        chat_id="123",
        settings=settings,
        agent_orchestrator=Orchestrator(),
        append_activity_trace=False,
    )

    assert "couldn't save a continuation request" in reply
    assert "Product B" not in reply
    assert observed_outcomes == [chat_operator.TurnOutcome.PARTIAL]


def test_chat_turn_hides_confident_draft_when_orchestrator_marks_response_unfulfilled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion.agent_orchestrator import TurnResult

    settings = load_settings(
        env={
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "unfulfilled-turn.db")
    runtime.active_tool_registry = ToolRegistry()
    monkeypatch.setattr(
        chat_operator,
        "_telegram_screenshot_reply_if_requested",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: False)
    monkeypatch.setattr(
        chat_operator,
        "_chat_capability_inventory_prompt",
        lambda runtime, **kwargs: None,
    )
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_memory_context_for_chat", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_user_context_prompt_for_chat",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        chat_operator,
        "_broadcast_new_workspace_approvals",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(chat_operator, "_remember_explicit_memory", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "_enforce_chat_response_fulfillment",
        lambda runtime, **kwargs: kwargs["reply"],
    )
    monkeypatch.setattr(
        chat_operator,
        "update_active_task_frame_from_outcomes",
        lambda *args, **kwargs: None,
    )
    observed_outcomes = []
    monkeypatch.setattr(
        chat_operator,
        "_schedule_builder_reflection",
        lambda *args, **kwargs: observed_outcomes.append(kwargs["outcome"]),
    )

    class Orchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            return TurnResult(
                turn_id="turn-unfulfilled",
                final_text="The product is definitely valid and costs $1.",
                tool_results=[
                    ToolResult(
                        "unbound-detail",
                        "browser_extract_detail",
                        "completed",
                        {
                            "kind": "browser_dom_detail",
                            "origin": "runtime_owned_dom_extractor",
                            "record": {"title": "Wrong product", "price": "$1"},
                        },
                    )
                ],
                response_fulfilled=False,
            )

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat opaque product request",
        chat_id="123",
        settings=settings,
        agent_orchestrator=Orchestrator(),
        append_activity_trace=False,
    )

    assert "couldn’t finish this request" in reply
    assert "No usable result was produced" in reply
    assert "definitely valid" not in reply
    assert "Wrong product" not in reply
    assert observed_outcomes == [chat_operator.TurnOutcome.PARTIAL]


def test_shared_saved_history_retry_can_skip_history_search_for_unanchored_vague_reference() -> None:
    result = SimpleNamespace(
        final_text="What should I pick five from?\n1. Crons\n2. Files",
        tool_results=[ToolResult("history-1", chat_operator.CHAT_HISTORY_SEARCH_TOOL_NAME, "completed", {"matches": []})],
        artifacts=[],
        suspended_for_approval=False,
    )
    calls: list[str] = []

    final_result, _registry, retried = chat_operator._run_chat_turn_saved_history_retry(
        initial_result=result,
        automatic_history_context="Structured saved reference candidates (ranked by relevance and recency):",
        allow_saved_history_retry=False,
        allow_history_search_retry=False,
        build_history_context=lambda: calls.append("context") or "stale target",
        base_history=[],
        active_tool_registry=SimpleNamespace(),
        base_tool_registry=SimpleNamespace(),
        turn_tool_evidence=SimpleNamespace(),
        run_turn=lambda _history, _registry: calls.append("run") or result,
    )

    assert retried is False
    assert final_result is result
    assert calls == []


def test_shared_saved_history_retry_requires_structured_anchor_even_after_history_search() -> None:
    result = SimpleNamespace(
        final_text="Which thing should I use?\n1. Old account page\n2. Recent task",
        tool_results=[ToolResult("history-1", chat_operator.CHAT_HISTORY_SEARCH_TOOL_NAME, "completed", {"matches": []})],
        artifacts=[],
        suspended_for_approval=False,
    )
    calls: list[str] = []

    final_result, final_registry, retried = chat_operator._run_chat_turn_saved_history_retry(
        initial_result=result,
        automatic_history_context="\n".join(
            [
                "Automatic saved-chat lookup for this turn found candidate prior turns.",
                "Structured saved reference candidates (ranked by relevance and recency):",
                'Target 1: {"url": "https://myaccount.google.com/notifications", "type": "url", "domain": "myaccount.google.com"}',
                "Target source assistant: Open your Google security notifications and review connected access.",
                "Candidate prior turns:",
            ]
        ),
        allow_saved_history_retry=False,
        build_history_context=lambda: calls.append("context") or "stale security page",
        base_history=[],
        active_tool_registry=SimpleNamespace(),
        base_tool_registry=SimpleNamespace(),
        turn_tool_evidence=SimpleNamespace(context_linked=False),
        run_turn=lambda _history, _registry: calls.append("run") or result,
    )

    assert retried is False
    assert final_result is result
    assert final_registry is not None
    assert calls == []


def test_compact_unstructured_followup_guard_is_prompt_agnostic() -> None:
    guard = chat_operator._compact_unstructured_followup_guard_prompt(
        prompt="Can you pick five?",
        reply_context_prompt=None,
        numbered_option_context=None,
    )

    assert guard is not None
    assert "structured target evidence" in guard
    assert "Ask one brief clarification" in guard
    assert "Can you pick five" not in guard


def test_compact_unstructured_followup_guard_skips_structured_anchors() -> None:
    assert chat_operator._compact_unstructured_followup_guard_prompt(
        prompt="2",
        reply_context_prompt=None,
        numbered_option_context="Numbered option selected: 2",
    ) is None
    assert chat_operator._compact_unstructured_followup_guard_prompt(
        prompt="Can you check https://example.test/status?",
        reply_context_prompt=None,
        numbered_option_context=None,
    ) is None


def test_compact_unstructured_target_clarification_uses_structured_model_output() -> None:
    class Model:
        def create(self, **kwargs):  # noqa: ANN001
            assert kwargs["tools"] == []
            assert "choosing from history would require guessing" in kwargs["system"]
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "needs_clarification": True,
                                "question": "What should I pick five from?",
                                "confidence": 0.91,
                            }
                        ),
                    }
                ]
            }

    reply = chat_operator._compact_unstructured_target_clarification_reply(
        prompt="Can you pick five?",
        model_client=Model(),
    )

    assert reply == "What should I pick five from?"


def test_compact_unstructured_target_clarification_skips_complete_request() -> None:
    class Model:
        def create(self, **_kwargs):  # noqa: ANN001
            return {"content": [{"type": "text", "text": '{"needs_clarification": false, "confidence": 0.9}'}]}

    assert chat_operator._compact_unstructured_target_clarification_reply(
        prompt="Pick five crons.",
        model_client=Model(),
    ) is None


def test_compact_clarification_followup_history_context_uses_structured_model_output() -> None:
    class Model:
        def create(self, **kwargs):  # noqa: ANN001
            assert kwargs["tools"] == []
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "answers_previous_clarification": True,
                                "use_saved_history": True,
                                "target_summary": "the missing target from the prior assistant clarification",
                                "confidence": 0.88,
                            }
                        ),
                    }
                ]
            }

    context = chat_operator._compact_clarification_followup_history_context(
        prompt="Use the saved conversation",
        previous_assistant_message="Which target should I check?",
        model_client=Model(),
    )

    assert context is not None
    assert "Clarification follow-up context" in context
    assert "saved conversation history" in context


def test_plain_complete_reply_bypasses_recovery_with_safe_direct_read_tools_visible() -> None:
    registry = ToolRegistry()
    for tool_name in ("market_quote", "weather_forecast", "request_tool_scope"):
        registry.register(
            ToolSpec(
                name=tool_name,
                description=tool_name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    assert chat_operator._plain_no_tool_common_retries_may_bypass(
        SimpleNamespace(final_text="Hello. How can I help?", tool_results=[], artifacts=[]),
        registry,
    )


def test_shared_saved_history_retry_skips_independent_no_tool_ambiguity() -> None:
    result = SimpleNamespace(
        final_text="Which saved item?\n1. One\n2. Two",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    calls: list[str] = []

    final_result, final_registry, retried = chat_operator._run_chat_turn_saved_history_retry(
        initial_result=result,
        automatic_history_context=None,
        allow_saved_history_retry=False,
        build_history_context=lambda: calls.append("context") or "stale saved target",
        base_history=[],
        active_tool_registry=SimpleNamespace(),
        base_tool_registry=SimpleNamespace(),
        turn_tool_evidence=SimpleNamespace(context_linked=False),
        run_turn=lambda _history, _registry: calls.append("run") or result,
    )

    assert retried is False
    assert final_result is result
    assert final_registry is not None
    assert calls == []


def test_shared_account_read_retry_uses_visible_read_tool_after_option_prompt() -> None:
    class Registry:
        def list_specs(self):
            return [
                SimpleNamespace(name="request_tool_scope"),
                SimpleNamespace(name="calendar_list"),
                SimpleNamespace(name="connector_request"),
            ]

        def list_tool_definitions(self):
            return [{"name": "request_tool_scope"}, {"name": "calendar_list"}, {"name": "connector_request"}]

    result = SimpleNamespace(
        final_text="Pick a range.\n1. Today\n2. Tomorrow",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    retry_result = SimpleNamespace(
        final_text="Checked next 7 days.",
        tool_results=[ToolResult("tool-1", "calendar_list", "completed", {"results": []})],
        artifacts=[],
        suspended_for_approval=False,
    )
    calls: list[list[dict[str, object]]] = []
    marks: list[str] = []

    final_result, final_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=result,
        base_history=[{"role": "system", "content": [{"type": "text", "text": "base"}]}],
        active_tool_registry=Registry(),
        user_message="calendar source",
        run_turn=lambda history, _registry: calls.append(history) or retry_result,
        mark_retry=lambda: marks.append("retry"),
    )

    assert retried is True
    assert final_result is retry_result
    assert final_registry is not None
    assert marks == ["retry"]
    assert any(
        "calendar_list" in block.get("text", "")
        for message in calls[0]
        for block in message.get("content", [])
        if isinstance(block, dict)
    )


def test_shared_account_read_retry_skips_without_visible_read_tool() -> None:
    class Registry:
        def list_specs(self):
            return [SimpleNamespace(name="request_tool_scope"), SimpleNamespace(name="connector_request")]

        def list_tool_definitions(self):
            return [{"name": "request_tool_scope"}, {"name": "connector_request"}]

    result = SimpleNamespace(
        final_text="Pick one.\n1. A\n2. B",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    calls: list[str] = []

    final_result, _registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=result,
        base_history=[],
        active_tool_registry=Registry(),
        user_message="choose source",
        run_turn=lambda _history, _registry: calls.append("run") or result,
    )

    assert retried is False
    assert final_result is result
    assert calls == []


def test_shared_account_read_retry_forces_scope_after_context_discovery_misroute(monkeypatch) -> None:
    from nullion import turn_context_policy

    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("chat_history_search", ("conversation_history",)),
        ("file_search", ("local",)),
        ("email_search", ("email", "connector", "account_read")),
        ("email_read", ("email", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_google_mail",
                "active_app_ids": ["google-mail"],
                "structured_tools": ["email_search", "email_read"],
            }
        ],
    )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="opaque account source request 77",
        conversation_result=None,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="opaque account source request 77",
    )
    assert {definition["name"] for definition in active_registry.list_tool_definitions()} == {"request_tool_scope", "chat_history_search"}
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("history-1", "chat_history_search", "completed", {"match_count": 3}),
            ToolResult("file-1", "file_search", "failed", {"error": "Missing required arguments: pattern"}),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I do not have the live account tool in this turn.",
    )
    decision_messages: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decision_messages.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_app_ids=("google-mail",),
            connector_source_user_requested=True,
            requested_tool_names=("email_search",),
            required_tool_names=(),
            confidence=0.93,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible = {definition["name"] for definition in retry_registry.list_tool_definitions()}
        visible_tool_calls.append(visible)
        assert "Account read retry" in retry_history[-1]["content"][0]["text"]
        assert "request_tool_scope" not in visible
        assert "email_search" in visible
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "email-1",
                    "email_search",
                    "completed",
                    {"results": [{"id": "msg-1", "subject": "Current account record"}]},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="I checked the current account source.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[
            {"role": "user", "content": [{"type": "text", "text": "opaque account source request 77"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "I tried saved context first."}]},
        ],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque account source request 77",
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is True
    assert "Recent visible conversation context" in decision_messages[0]
    assert visible_tool_calls and "email_search" in visible_tool_calls[0]
    assert result.final_text == "I checked the current account source."
    assert [tool.tool_name for tool in result.tool_results] == [
        "chat_history_search",
        "file_search",
        "email_search",
    ]
    assert "email_search" in {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_shared_account_read_retry_forces_scope_after_no_tool_account_miss(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("connector_request", ("connector",)),
        ("calendar_list", ("calendar", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"results": [{"summary": "Structured calendar event"}]},
            ),
        )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="opaque connected-source read request 913",
        conversation_result=None,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="opaque connected-source read request 913",
    )
    assert {definition["name"] for definition in active_registry.list_tool_definitions()} == {"request_tool_scope"}
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        reached_iteration_limit=False,
        final_text="I cannot access that connected source.",
    )
    decision_messages: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decision_messages.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_app_ids=("google-calendar",),
            connector_source_user_requested=True,
            requested_tool_names=("calendar_list",),
            required_tool_names=(),
            confidence=0.94,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    retry_visible_tools: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible = {definition["name"] for definition in retry_registry.list_tool_definitions()}
        retry_visible_tools.append(visible)
        assert "calendar_list" in visible
        assert "request_tool_scope" not in visible
        assert "Account read retry" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "calendar-1",
                    "calendar_list",
                    "completed",
                    {"results": [{"summary": "Structured calendar event"}]},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="I checked the live connected calendar.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[
            {"role": "user", "content": [{"type": "text", "text": "opaque connected-source read request 913"}]},
        ],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque connected-source read request 913",
        evidence=evidence,
        principal_id="telegram:100001",
        run_turn=run_turn,
    )

    assert retried is True
    assert len(decision_messages) == 1
    assert "opaque connected-source read request 913" in decision_messages[0]
    assert retry_visible_tools and "calendar_list" in retry_visible_tools[0]
    assert result.final_text == "I checked the live connected calendar."
    assert [tool.tool_name for tool in result.tool_results] == ["calendar_list"]
    assert "calendar_list" in {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_shared_account_read_retry_does_not_override_structured_web_scope(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("browser_navigate", ("web", "browser")),
        ("email_search", ("email", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    active_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="opaque follow-up 42", conversation_result=None),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="live_research",
            requested_tool_names=("browser_navigate",),
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need another public source.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("account classifier must not override web scope")),
    )
    calls: list[str] = []

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque follow-up 42",
        evidence=chat_operator.build_turn_tool_evidence(user_message="opaque follow-up 42", conversation_result=None),
        run_turn=lambda _history, _registry: calls.append("run") or initial_result,
    )

    assert result is initial_result
    assert retry_registry is active_registry
    assert retried is False
    assert calls == []


def test_shared_account_read_retry_does_not_promote_numbered_draft_when_scope_classifier_is_empty(
    monkeypatch,
) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("connector_request", ("connector",)),
        ("calendar_list", ("calendar", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="opaque connected-source read request 472",
        conversation_result=None,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="opaque connected-source read request 472",
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        reached_iteration_limit=False,
        final_text="Choose a connected source:\n1. Use the active source\n2. Stop",
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(valid=True, confidence=0.5)

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible = {definition["name"] for definition in retry_registry.list_tool_definitions()}
        visible_tool_calls.append(visible)
        assert "calendar_list" in visible
        assert "request_tool_scope" not in visible
        assert "Account read retry" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("calendar-1", "calendar_list", "completed", {"results": []})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Checked the active connected source.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[
            {"role": "user", "content": [{"type": "text", "text": "opaque connected-source read request 472"}]},
        ],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque connected-source read request 472",
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is active_registry
    assert visible_tool_calls == []


def test_shared_account_read_retry_does_not_read_single_option_source_picker_without_selection(monkeypatch) -> None:
    registry = ToolRegistry()
    invoked: list[str] = []
    for name, tags in (
        ("connector_request", ("connector",)),
        ("email_search", ("email", "connector", "account_read")),
        ("email_read", ("email", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: (
                invoked.append(invocation.tool_name)
                or ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {"results": [{"id": "msg-1", "subject": "current record"}]},
                )
            ),
        )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="opaque connected-source read request 811",
        conversation_result=None,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="opaque connected-source read request 811",
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        reached_iteration_limit=False,
        final_text="Choose the authorized source:\n1. Active source",
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: chat_operator.TurnToolScopeDecision(valid=True, confidence=0.5),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[
            {"role": "user", "content": [{"type": "text", "text": "opaque connected-source read request 811"}]},
        ],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque connected-source read request 811",
        evidence=evidence,
        principal_id="telegram:100001",
        run_turn=lambda _history, _registry: SimpleNamespace(
            tool_results=[],
            artifacts=[],
            suspended_for_approval=False,
            reached_iteration_limit=False,
            final_text="",
        ),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is active_registry
    assert invoked == []


def test_shared_account_read_retry_recovers_after_history_only_account_miss(monkeypatch) -> None:
    registry = ToolRegistry()
    invoked: list[str] = []
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("chat_history_search", ("conversation_history",)),
        ("calendar_list", ("calendar", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: (
                invoked.append(invocation.tool_name)
                or ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {"results": [{"summary": "Live structured result"}]},
                )
            ),
        )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="structured connected-source read 482",
        conversation_result=None,
        saved_history_available=True,
    )
    active_registry = chat_operator.turn_tool_registry_for_evidence(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="structured connected-source read 482",
        skip_tool_scope_decision=True,
    )
    assert "calendar_list" not in {definition["name"] for definition in active_registry.list_tool_definitions()}
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "history-1",
                "chat_history_search",
                "completed",
                {"match_count": 4, "matches": [{"summary": "stale saved context"}]},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        reached_iteration_limit=False,
        final_text="The live connected source is not visible in this turn.",
    )
    decision_messages: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decision_messages.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_app_ids=("google-calendar",),
            connector_source_user_requested=True,
            requested_tool_names=("calendar_list",),
            required_tool_names=("calendar_list",),
            confidence=0.95,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    retry_visible_tools: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible = {definition["name"] for definition in retry_registry.list_tool_definitions()}
        retry_visible_tools.append(visible)
        assert visible == {"calendar_list"}
        assert "Account read retry" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[],
            artifacts=[],
            suspended_for_approval=False,
            reached_iteration_limit=False,
            final_text="",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[
            {"role": "user", "content": [{"type": "text", "text": "structured connected-source read 482"}]},
        ],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="structured connected-source read 482",
        evidence=evidence,
        principal_id="telegram:100001",
        run_turn=run_turn,
    )

    assert retried is True
    assert decision_messages
    assert retry_visible_tools == [{"calendar_list"}]
    assert invoked == ["calendar_list"]
    assert [tool.tool_name for tool in result.tool_results] == ["chat_history_search", "calendar_list"]
    assert "calendar_list" in {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_shared_account_read_retry_requires_structured_connector_scope_after_discovery(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("file_search", ("local",)),
        ("email_search", ("email", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("file-1", "file_search", "failed", {"error": "Missing required arguments: pattern"})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found no local files.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: chat_operator.TurnToolScopeDecision(valid=True, confidence=0.88),
    )
    calls: list[str] = []

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque local discovery request 42",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="opaque local discovery request 42",
            conversation_result=None,
        ),
        run_turn=lambda _history, _registry: calls.append("run") or initial_result,
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry
    assert calls == []


def test_numbered_option_turn_uses_interactive_fast_profile() -> None:
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="Structured numbered-option selection:\nSelected option 1:\nProceed",
        conversation_result=None,
        numbered_option_selected=True,
    )

    assert chat_operator._messaging_turn_fast_profile_candidate(
        evidence=evidence,
        user_message="Structured numbered-option selection:\nSelected option 1:\nProceed",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=None,
    )


def test_numbered_option_turn_skips_pre_scope_classifier_without_structured_tool_context() -> None:
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="Structured numbered-option selection:\nSelected option 1:\nProceed",
        conversation_result=None,
        numbered_option_selected=True,
    )

    assert chat_operator._messaging_turn_skip_tool_scope_decision(
        evidence=evidence,
        user_message="Structured numbered-option selection:\nSelected option 1:\nProceed",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=SimpleNamespace(
            disposition=chat_operator.ConversationTurnDisposition.CONTINUE,
            dependency_turn_ids=("turn-1",),
        ),
    )


def test_numbered_option_turn_keeps_pre_scope_available_with_prior_tool_context() -> None:
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="Structured numbered-option selection:\nSelected option 1:\nProceed",
        conversation_result=None,
        numbered_option_selected=True,
        prior_tool_scopes=("connector",),
    )

    assert not chat_operator._messaging_turn_skip_tool_scope_decision(
        evidence=evidence,
        user_message="Structured numbered-option selection:\nSelected option 1:\nProceed",
        config_action=None,
        allow_mini_agents=False,
        force_mini_agent_dispatch=False,
        turn_dispatch_decision=SimpleNamespace(
            disposition=chat_operator.ConversationTurnDisposition.CONTINUE,
            dependency_turn_ids=("turn-1",),
        ),
    )


def test_numbered_option_context_does_not_auto_inject_saved_history() -> None:
    conversation_result = SimpleNamespace(
        turn=SimpleNamespace(
            parent_turn_id="turn-parent",
            disposition=chat_operator.ConversationTurnDisposition.CONTINUE,
        )
    )

    assert not chat_operator._should_auto_include_saved_chat_history(
        conversation_result,
        reply_context_prompt=None,
        numbered_option_context="Structured numbered-option selection:\nSelected option 1:\nProceed",
        requested_extensions=(),
    )
    assert chat_operator._should_auto_include_saved_chat_history(
        conversation_result,
        reply_context_prompt="Explicit reply anchor context",
        numbered_option_context="Structured numbered-option selection:\nSelected option 1:\nProceed",
        requested_extensions=(),
    )


def test_shared_saved_history_retry_skips_after_substantive_tool_work() -> None:
    result = SimpleNamespace(
        final_text="Which packet?\n1. Photo\n2. Link",
        tool_results=[ToolResult("tool-1", "email_search", "completed", {"results": []})],
        artifacts=[],
        suspended_for_approval=False,
    )
    calls: list[str] = []

    final_result, final_registry, retried = chat_operator._run_chat_turn_saved_history_retry(
        initial_result=result,
        automatic_history_context="Structured saved reference candidates (ranked by relevance and recency):",
        allow_saved_history_retry=True,
        build_history_context=lambda: calls.append("context") or "unused",
        base_history=[],
        active_tool_registry=SimpleNamespace(),
        base_tool_registry=SimpleNamespace(),
        turn_tool_evidence=SimpleNamespace(),
        run_turn=lambda _history, _registry: calls.append("run") or result,
    )

    assert retried is False
    assert final_result is result
    assert final_registry is not None
    assert calls == []


def test_chat_operator_saved_history_prompt_surfaces_recent_tool_backed_context(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "auto-history-tool-backed-runtime.db")
    conversation_id = "telegram:123"
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 27, 14, 1, tzinfo=UTC).isoformat(),
            "turn_id": "turn-generic-clarification",
            "user_message": "estado?",
            "assistant_reply": "Please send the target details.",
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 27, 14, 2, tzinfo=UTC).isoformat(),
            "turn_id": "turn-runtime-evidence",
            "user_message": "sí, ese objetivo",
            "assistant_reply": "The saved target was resolved as catalog item 4821 with a pending external state.",
            "tool_results": [
                {
                    "tool_name": "external_status_read",
                    "status": "completed",
                    "output": {"status": "pending"},
                }
            ],
        }
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 27, 14, 30, tzinfo=UTC).isoformat(),
            "turn_id": "turn-scope-only-clarification",
            "user_message": "estado?",
            "assistant_reply": "Please choose a source:\n\n1. A\n2. B",
            "tool_results": [
                {
                    "tool_name": "request_tool_scope",
                    "status": "completed",
                    "output": {
                        "scope_requested": True,
                        "available_tools": [],
                        "message": "No registered tools matched the requested capability scope.",
                    },
                }
            ],
        }
    )
    for index in range(12):
        runtime.store.add_conversation_event(
            {
                "conversation_id": conversation_id,
                "event_type": "conversation.chat_turn",
                "created_at": datetime(2026, 5, 27, 14, 31 + index, tzinfo=UTC).isoformat(),
                "turn_id": f"turn-filler-{index}",
                "user_message": f"relleno {index}",
                "assistant_reply": f"contexto ordinario {index}",
            }
        )

    prompt = chat_operator._automatic_saved_chat_history_prompt(
        runtime,
        conversation_id=conversation_id,
        prompt="estado?",
        visible_turns=[],
    )

    assert prompt is not None
    runtime_evidence_block = prompt.split("Candidate prior turns:")[0]
    assert "Please choose a source" not in runtime_evidence_block
    assert "catalog item 4821" in prompt
    reference_message = chat_operator._saved_history_reference_context_message(prompt)
    assert reference_message is not None
    reference_text = reference_message["content"][0]["text"]
    assert "Please choose a source" not in reference_text


def test_chat_operator_saved_history_context_widens_tools_from_ranked_runtime_evidence() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="external_status_read",
            description="Read status for an external target",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("external_status",),
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"state": "fresh"},
        ),
    )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="estado?",
        conversation_result=None,
        saved_history_available=True,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="estado?",
    )
    assert "external_status_read" not in {
        definition["name"] for definition in active_registry.list_tool_definitions()
    }
    history_context = "\n".join(
        [
            "Recent runtime-evidence-backed saved turns (ranked by relevance and recency):",
            "Evidence 1. 2026-05-27T14:02:00+00:00",
            "Tools: request_tool_scope, chat_history_search",
            "User: sí, ese objetivo",
            "Assistant: The saved target was catalog item 4821.",
            "Evidence 2. 2026-05-27T14:01:00+00:00",
            "Tools: request_tool_scope, external_status_read",
            "User: objetivo 4821",
            "Assistant: External state was pending.",
        ]
    )

    widened_registry = chat_operator._augment_tool_registry_from_saved_history_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        history_context=history_context,
        allow_unlinked_context_scope=True,
    )

    assert "external_status_read" in {
        definition["name"] for definition in widened_registry.list_tool_definitions()
    }
    scope_prompt = chat_operator._saved_history_tool_scope_prompt(history_context)
    assert scope_prompt is not None
    assert "external_status_read" in scope_prompt
    assert "catalog item 4821" not in scope_prompt


def test_chat_operator_structured_tool_context_widens_schema_advertised_followup_tool(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_search", "email_read"],
            }
        ],
    )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="continue from that result",
        conversation_result=SimpleNamespace(
            turn=SimpleNamespace(parent_turn_id="turn-1", disposition=chat_operator.ConversationTurnDisposition.CONTINUE)
        ),
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="continue from that result",
    )
    assert "email_read" not in {
        definition["name"] for definition in active_registry.list_tool_definitions()
    }
    recent_context = (
        "Historical, timestamped tool outcomes from this conversation:\n"
        + json.dumps(
            [
                {
                    "tool_results": [
                        {
                            "tool_name": "email_search",
                            "status": "completed",
                            "output": {
                                "body_included": False,
                                "body_requires_tool": "email_read",
                                "next_tool_for_body": "email_read",
                                "results": [{"id": "message-1", "subject": "Receipt"}],
                            },
                        }
                    ]
                }
            ]
        )
    )

    widened_registry = chat_operator._augment_tool_registry_from_structured_tool_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        context=recent_context,
    )

    names = {definition["name"] for definition in widened_registry.list_tool_definitions()}
    assert names == {"email_read", "request_tool_scope"}
    scope_prompt = chat_operator._structured_tool_context_scope_prompt(recent_context)
    assert scope_prompt is not None
    assert "email_read" in scope_prompt
    assert "Receipt" not in scope_prompt


def test_chat_operator_structured_tool_context_does_not_widen_account_tool_for_independent_turn(
    monkeypatch,
) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_read"],
            }
        ],
    )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="fresh independent check",
        conversation_result=None,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="fresh independent check",
    )
    recent_context = 'Tool evidence: [{"output": {"body_requires_tool": "email_read"}}]'

    widened_registry = chat_operator._augment_tool_registry_from_structured_tool_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        context=recent_context,
    )

    assert widened_registry is active_registry
    assert "email_read" not in {definition["name"] for definition in widened_registry.list_tool_definitions()}


def test_chat_operator_structured_scheduler_context_exposes_live_read_tool(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags, side_effect in (
        ("request_tool_scope", ("tool_scope",), ToolSideEffectClass.READ),
        ("list_crons", ("scheduler", "cron"), ToolSideEffectClass.READ),
        ("toggle_cron", ("scheduler", "cron"), ToolSideEffectClass.WRITE),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    evidence = TurnToolEvidence(
        context_linked=True,
        prior_tool_scopes=("scheduler",),
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="neutral continuation",
    )
    assert "list_crons" in {definition["name"] for definition in active_registry.list_tool_definitions()}
    recent_context = (
        "Historical, timestamped tool outcomes from this conversation:\n"
        + json.dumps(
            [
                {
                    "tool_results": [
                        {
                            "tool_name": "toggle_cron",
                            "status": "completed",
                            "output": {"updated": 21},
                        }
                    ]
                }
            ]
        )
    )

    widened_registry = chat_operator._augment_tool_registry_from_structured_tool_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        context=recent_context,
    )

    names = {definition["name"] for definition in widened_registry.list_tool_definitions()}
    assert names == {"list_crons", "request_tool_scope"}
    decision = getattr(widened_registry, "turn_tool_scope_decision", None)
    assert getattr(decision, "scheduler_action", None) == "inspect"
    scope_prompt = chat_operator._structured_tool_context_scope_prompt(recent_context)
    assert scope_prompt is not None
    assert "list_crons" in scope_prompt
    assert "toggle_cron" not in scope_prompt


def test_chat_operator_structured_scheduler_context_exposes_run_tool_without_mutation_tools() -> None:
    registry = ToolRegistry()
    for name, tags, side_effect in (
        ("request_tool_scope", ("tool_scope",), ToolSideEffectClass.READ),
        ("list_crons", ("scheduler", "cron"), ToolSideEffectClass.READ),
        ("run_cron", ("scheduler", "cron"), ToolSideEffectClass.WRITE),
        ("toggle_cron", ("scheduler", "cron"), ToolSideEffectClass.WRITE),
        ("update_cron", ("scheduler", "cron"), ToolSideEffectClass.WRITE),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    evidence = TurnToolEvidence(
        context_linked=True,
        prior_tool_scopes=("scheduler",),
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="neutral continuation",
    )
    recent_context = (
        "Historical, timestamped tool outcomes from this conversation:\n"
        + json.dumps(
            [
                {
                    "tool_results": [
                        {
                            "tool_name": "list_crons",
                            "status": "completed",
                            "output": {
                                "cron_count": 30,
                                "crons": [
                                    {
                                        "id": "cron-target",
                                        "name": "Stage live scheduler short followup target",
                                        "enabled": True,
                                        "has_task": True,
                                    }
                                ],
                                "matched_crons": [
                                    {
                                        "id": "cron-target",
                                        "name": "Stage live scheduler short followup target",
                                        "enabled": True,
                                        "has_task": True,
                                    }
                                ],
                                "matched_cron_count": 1,
                                "resolved_target": {"id": "cron-target"},
                                "continuation_tools": ["run_cron"],
                            },
                        }
                    ]
                }
            ]
        )
    )

    widened_registry = chat_operator._augment_tool_registry_from_structured_tool_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        context=recent_context,
    )

    names = {definition["name"] for definition in widened_registry.list_tool_definitions()}
    assert names == {"list_crons", "request_tool_scope"}
    decision = getattr(widened_registry, "turn_tool_scope_decision", None)
    assert getattr(decision, "scheduler_action", None) == "inspect"
    assert "toggle_cron" not in names
    assert "update_cron" not in names


def test_chat_operator_recent_tool_scopes_include_toggle_cron() -> None:
    class Store:
        def list_recent_conversation_events(self, conversation_id, *, event_type=None, limit=25):
            assert conversation_id == "telegram:scheduler"
            assert event_type == "conversation.chat_turn"
            return [
                {
                    "tool_results": [
                        {
                            "tool_name": "toggle_cron",
                            "status": "completed",
                            "output": {"updated": 21},
                        }
                    ]
                }
            ]

    runtime = SimpleNamespace(store=Store())

    assert chat_operator._recent_tool_scopes_for_context(runtime, "telegram:scheduler") == ("scheduler",)


def test_chat_operator_tool_context_stops_at_session_reset() -> None:
    from nullion.runtime_store import RuntimeStore

    store = RuntimeStore()
    store.add_conversation_event(
        {
            "conversation_id": "telegram:fresh",
            "event_type": "conversation.chat_turn",
            "assistant_reply": "Done.\nMEDIA:/tmp/old-report.pdf",
            "tool_results": [{"tool_name": "browser_navigate", "status": "completed", "output": {}}],
        }
    )
    runtime = SimpleNamespace(store=store)

    assert chat_operator._recent_tool_scopes_for_context(runtime, "telegram:fresh") == ("web",)
    assert chat_operator._saved_conversation_history_available(runtime, "telegram:fresh") is True
    assert chat_operator._recent_conversation_has_structured_tool_evidence(
        runtime,
        conversation_id="telegram:fresh",
    ) is True
    assert chat_operator._recent_assistant_turn_has_structured_artifact_evidence(
        runtime,
        conversation_id="telegram:fresh",
    ) is True

    store.add_conversation_event(
        {"conversation_id": "telegram:fresh", "event_type": "conversation.session_reset"}
    )

    assert chat_operator._recent_tool_scopes_for_context(runtime, "telegram:fresh") == ()
    assert chat_operator._saved_conversation_history_available(runtime, "telegram:fresh") is False
    assert chat_operator._recent_conversation_has_structured_tool_evidence(
        runtime,
        conversation_id="telegram:fresh",
    ) is False
    assert chat_operator._recent_assistant_turn_has_structured_artifact_evidence(
        runtime,
        conversation_id="telegram:fresh",
    ) is False


def test_chat_operator_scheduler_read_verification_uses_base_registry_after_action() -> None:
    invoked: list[str] = []
    registry = ToolRegistry()
    for name, side_effect in (
        ("list_crons", ToolSideEffectClass.READ),
        ("toggle_cron", ToolSideEffectClass.WRITE),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
            ),
            lambda invocation: (
                invoked.append(invocation.tool_name)
                or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"jobs": []})
            ),
        )
    active_registry = chat_operator._ToolRegistryAllowedNames(registry, allowed_tool_names=("toggle_cron",))
    result = SimpleNamespace(
        tool_results=[
            ToolResult("toggle-1", "toggle_cron", "completed", {"updated": 1}),
        ],
    )

    appended = chat_operator._append_scheduler_read_verification_if_missing(
        result,
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        principal_id="telegram:100001",
    )

    assert appended is True
    assert invoked == ["list_crons"]
    assert [tool.tool_name for tool in result.tool_results] == ["toggle_cron", "list_crons"]


def test_chat_operator_scheduler_read_verification_covers_manual_run_once() -> None:
    invoked: list[str] = []
    registry = ToolRegistry()
    for name, side_effect in (
        ("list_crons", ToolSideEffectClass.READ),
        ("run_cron", ToolSideEffectClass.WRITE),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
            ),
            lambda invocation: (
                invoked.append(invocation.tool_name)
                or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"jobs": []})
            ),
        )
    result = SimpleNamespace(
        tool_results=[
            ToolResult("run-1", "run_cron", "completed", {"delivery_status": "deferred"}),
        ],
    )

    first_appended = chat_operator._append_scheduler_read_verification_if_missing(
        result,
        active_tool_registry=registry,
        base_tool_registry=None,
        principal_id="telegram:100001",
    )
    second_appended = chat_operator._append_scheduler_read_verification_if_missing(
        result,
        active_tool_registry=registry,
        base_tool_registry=None,
        principal_id="telegram:100001",
    )

    assert first_appended is True
    assert second_appended is False
    assert invoked == ["list_crons"]
    assert [tool.tool_name for tool in result.tool_results] == ["run_cron", "list_crons"]


def test_chat_operator_sanitizes_media_reply_for_discovery_only_terminal_listing(tmp_path: Path) -> None:
    discovered_pdf = tmp_path / "stage_file_search_delivery_guard.pdf"
    discovered_pdf.write_bytes(b"%PDF-1.4\n%discovered\n")

    reply = chat_operator._sanitize_chat_reply(
        prompt="neutral file lookup",
        reply=f"Done — attached the requested PDF.\n\nMEDIA:{discovered_pdf}",
        tool_results=[
            ToolResult(
                "terminal-1",
                "terminal_exec",
                "completed",
                {"stdout": f"{discovered_pdf.name}\n", "stderr": ""},
            )
        ],
    )

    assert reply == f"I found 1 file:\n- {discovered_pdf.name}"


def test_chat_operator_bare_format_names_require_a_structured_output_decision() -> None:
    assert chat_operator._literal_requested_attachment_extensions("Create a CSV artifact") == ()
    assert chat_operator._literal_requested_attachment_extensions("Create a PDF summary") == ()
    assert chat_operator._literal_requested_attachment_extensions("Discuss a pdf issue") == ()
    assert chat_operator._requested_attachment_extensions(
        "Find the pdf file named stage_file_search_delivery_guard_db56f6871b.pdf in my workspace and reply with a concise filename-only listing."
    ) == ()
    assert chat_operator._requested_attachment_extensions(
        "Create a PDF named stage_followup_reuse_summary.pdf and attach only the PDF."
    ) == ()
    from test_attachment_format_graph import FakeFormatModel
    assert chat_operator._requested_attachment_extensions(
        "Create a PDF named stage_followup_reuse_summary.pdf and attach only the PDF.",
        model_client=FakeFormatModel('.pdf'),
    ) == (".pdf",)
    assert chat_operator._requested_attachment_extensions("artifact=stage_file_search_delivery_guard_db56f6871b.pdf") == (".pdf",)


def test_chat_operator_attachment_extensions_skip_planner_when_disabled(monkeypatch) -> None:
    def fail_plan_attachment_format(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain chat must not run attachment planning")

    monkeypatch.setattr(chat_operator, "plan_attachment_format", fail_plan_attachment_format)

    assert chat_operator._requested_attachment_extensions(
        "How are you?",
        model_client=object(),
        allow_model_planning=False,
    ) == ()
    assert chat_operator._requested_attachment_extensions(
        "Create qa_report.pdf and qa_catalog.xlsx.",
        model_client=object(),
        allow_model_planning=False,
    ) == ()
    assert chat_operator._requested_attachment_extensions(
        "artifact=qa_report.pdf output=qa_catalog.xlsx",
        model_client=object(),
        allow_model_planning=False,
    ) == (".pdf", ".xlsx")
    assert chat_operator._requested_attachment_extensions(
        "Save as .pdf and .xlsx.",
        model_client=object(),
        allow_model_planning=False,
    ) == (".pdf", ".xlsx")
    assert chat_operator._requested_attachment_extensions(
        "Find the pdf file named stage_file_search_delivery_guard_1234.pdf in my workspace.",
        model_client=object(),
        allow_model_planning=False,
    ) == ()
    assert chat_operator._requested_attachment_extensions(
        "procesa https://example.test/Archive.zip y genera XLSX",
        model_client=object(),
        allow_model_planning=False,
    ) == ()
    assert chat_operator._requested_attachment_extensions(
        "Using the CSV you just created, create an XLSX workbook named stage_followup_reuse_values.xlsx.",
        model_client=object(),
        allow_model_planning=False,
    ) == ()
    assert chat_operator._requested_attachment_extensions(
        (
            "Using the CSV you just created, create an XLSX workbook named "
            "stage_followup_reuse_values.xlsx. Attach only the XLSX."
        ),
        model_client=object(),
        allow_model_planning=False,
    ) == ()


def test_chat_operator_detects_existing_named_workspace_artifact_without_language_trigger(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    existing = artifact_root / "stage_requested_file_delivery.xlsx"
    existing.write_bytes(b"fake workbook bytes")

    monkeypatch.setattr(
        chat_operator,
        "_artifact_roots_for_delivery",
        lambda _runtime, _principal_id: (artifact_root,),
    )

    assert chat_operator._existing_named_workspace_artifact_extensions(
        object(),
        prompt=(
            f"Adjunta el archivo exacto llamado {existing.name} como archivo descargable. "
            "No respondas con una lista."
        ),
        principal_id="web:operator",
    ) == (".xlsx",)
    assert chat_operator._existing_named_workspace_artifact_extensions(
        object(),
        prompt=f"Adjunta {existing.name} y crea nuevo_resultado.xlsx",
        principal_id="web:operator",
    ) == ()

    class DeliveryModel:
        def create(self, **kwargs):  # noqa: ANN003
            assert kwargs["tools"] == []
            return {"content": [{"type": "text", "text": '{"action": "deliver"}'}]}

    class ListingModel:
        def create(self, **kwargs):  # noqa: ANN003
            assert kwargs["tools"] == []
            return {"content": [{"type": "text", "text": '{"action": "list"}'}]}

    class GeneratedContentModel:
        def create(self, **kwargs):  # noqa: ANN003
            assert kwargs["tools"] == []
            return {
                "content": [
                    {
                        "type": "text",
                        "text": '{"action": "deliver", "requires_new_or_modified_content": true}',
                    }
                ]
            }

    assert chat_operator._should_direct_deliver_existing_named_workspace_artifacts(
        existing_named_artifact_paths=(str(existing),),
        effective_prompt=f"Adjunta el archivo exacto llamado {existing.name} como archivo descargable.",
        attachment_context_present=False,
        conversation_result=None,
        numbered_option_context=None,
        reply_context_prompt=None,
        planner_requested=False,
        model_client=DeliveryModel(),
    )
    assert chat_operator._direct_existing_named_workspace_artifact_action(
        existing_named_artifact_paths=(str(existing),),
        effective_prompt=f"Enumera el archivo exacto llamado {existing.name}. No lo adjuntes.",
        attachment_context_present=False,
        conversation_result=None,
        numbered_option_context=None,
        reply_context_prompt=None,
        planner_requested=False,
        model_client=ListingModel(),
    ) == "list"
    assert chat_operator._existing_named_workspace_artifact_listing_reply((str(existing),)) == (
        f"I found 1 file:\n- {existing.name}"
    )
    evidence = chat_operator._direct_existing_named_workspace_artifact_tool_result((str(existing),), action="list")
    assert evidence is not None
    assert evidence.tool_name == "file_search"
    assert evidence.status == "completed"
    assert evidence.output["count"] == 1
    assert evidence.output["matches"][0]["name"] == existing.name
    assert evidence.output["matches"][0]["path"] == str(existing)
    assert not chat_operator._should_direct_deliver_existing_named_workspace_artifacts(
        existing_named_artifact_paths=(str(existing),),
        effective_prompt=f"Adjunta el archivo exacto llamado {existing.name} como archivo descargable.",
        attachment_context_present=False,
        conversation_result=None,
        numbered_option_context=None,
        reply_context_prompt=None,
        planner_requested=False,
        model_client=ListingModel(),
    )
    assert not chat_operator._should_direct_deliver_existing_named_workspace_artifacts(
        existing_named_artifact_paths=(str(existing),),
        effective_prompt=f"https://example.invalid/{existing.name}",
        attachment_context_present=False,
        conversation_result=None,
        numbered_option_context=None,
        reply_context_prompt=None,
        planner_requested=False,
    )
    assert chat_operator._direct_existing_named_workspace_artifact_action(
        existing_named_artifact_paths=(str(existing),),
        effective_prompt=f"Adjunta el archivo exacto llamado {existing.name} como archivo descargable.",
        attachment_context_present=False,
        saved_history_available=True,
        conversation_result=None,
        numbered_option_context=None,
        reply_context_prompt=None,
        planner_requested=False,
        model_client=DeliveryModel(),
    ) == "deliver"
    assert chat_operator._direct_existing_named_workspace_artifact_action(
        existing_named_artifact_paths=(str(existing),),
        effective_prompt=f"Crea nuevas filas dentro de {existing.name} y adjunta el resultado.",
        attachment_context_present=False,
        conversation_result=None,
        numbered_option_context=None,
        reply_context_prompt=None,
        planner_requested=False,
        model_client=GeneratedContentModel(),
    ) == "use_tools"


def test_chat_operator_attachment_format_words_require_structured_planner() -> None:
    class Planner:
        calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            self.calls += 1
            assert kwargs["tools"][0]["name"] == "select_attachment_format"
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {
                            "extension": ".xlsx",
                            "embedded_media_extensions": [".xlsx"],
                        },
                    }
                ]
            }

    prompt = (
        "I need an excel for first 20 products on this link. Excel should have each "
        "product's picture, details, lizzie's note and price. "
        "https://lizziefortunato.com/collections/necklaces"
    )

    assert chat_operator._requested_attachment_extensions(
        prompt,
        model_client=object(),
        allow_model_planning=False,
    ) == ()

    planner = Planner()
    assert chat_operator._requested_attachment_extensions(
        prompt,
        model_client=planner,
        allow_model_planning=True,
    ) == (".xlsx",)
    assert chat_operator._requested_embedded_media_extensions(
        prompt,
        model_client=planner,
        allow_model_planning=True,
    ) == (".xlsx",)
    assert planner.calls == 2


def test_chat_operator_attachment_extensions_skip_planner_for_plain_chat_even_when_allowed(monkeypatch) -> None:
    def fail_plan_attachment_format(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("plain chat must not run attachment planning")

    monkeypatch.setattr(chat_operator, "plan_attachment_format", fail_plan_attachment_format)

    assert chat_operator._requested_attachment_extensions(
        "For a test-only account alert, give concise recommendations and numbered next actions.",
        model_client=object(),
        allow_model_planning=True,
    ) == ()


def test_chat_operator_multi_artifact_pack_keeps_structured_embedded_media_requirement() -> None:
    class Planner:
        def create(self, **kwargs):  # noqa: ANN003
            assert kwargs["tools"][0]["name"] == "select_attachment_format"
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {
                            "extension": ".xlsx",
                            "embedded_media_extensions": [".xlsx"],
                        },
                    }
                ]
            }

    assert chat_operator._requested_embedded_media_extensions(
        "Create an XLSX with embedded media, a CSV export, and a PDF summary.",
        model_client=Planner(),
        allow_model_planning=True,
    ) == (".xlsx",)
    assert chat_operator._requested_embedded_media_extensions(
        "For a test-only account alert, give concise recommendations and numbered next actions.",
        model_client=object(),
        allow_model_planning=True,
    ) == ()


def test_chat_operator_ambiguous_filename_extension_uses_structured_planner(monkeypatch) -> None:
    class Planner:
        def __init__(self, extension: str | None) -> None:
            self.extension = extension
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            self.calls += 1
            assert kwargs["tools"][0]["name"] == "select_attachment_format"
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {
                            "extension": self.extension,
                            "embedded_media_extensions": [],
                        },
                    }
                ]
            }

    finder = Planner(None)
    assert chat_operator._requested_attachment_extensions(
        "Find the pdf file named stage_file_search_delivery_guard_1234.pdf in my workspace.",
        model_client=finder,
        allow_model_planning=False,
    ) == ()
    assert finder.calls == 1

    svg_generator = Planner(".svg")
    assert chat_operator._requested_attachment_extensions(
        "Create a downloadable SVG file named stage_svg_live_contract.svg.",
        model_client=svg_generator,
        allow_model_planning=False,
    ) == (".svg",)
    assert svg_generator.calls == 1
    assert chat_operator._requested_attachment_extensions(
        "Create a downloadable SVG file named stage_svg_live_contract.svg. Attach only the SVG artifact.",
        model_client=svg_generator,
        allow_model_planning=False,
    ) == (".svg",)


def test_local_file_reference_retry_exposes_file_write_for_text_artifact_suffixes() -> None:
    registry = ToolRegistry()
    for name, side_effect in (
        ("file_search", ToolSideEffectClass.READ),
        ("file_read", ToolSideEffectClass.READ),
        ("workspace_summary", ToolSideEffectClass.READ),
        ("file_write", ToolSideEffectClass.WRITE),
        ("pdf_create", ToolSideEffectClass.WRITE),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    svg_decision = chat_operator._local_file_reference_scope_decision_from_message(
        "Opaque file request for stage_svg_live_contract.svg.",
        registry=registry,
        allow_file_write_artifacts=True,
    )
    assert svg_decision is not None
    assert "file_write" in svg_decision.requested_tool_names
    assert svg_decision.required_tool_names == ("file_write",)
    assert svg_decision.requested_artifact_extensions == (".svg",)

    listing_decision = chat_operator._local_file_reference_scope_decision_from_message(
        "Opaque file reference stage_file_search_delivery_guard_1234.pdf.",
        registry=registry,
        allow_file_write_artifacts=True,
    )
    assert listing_decision is not None
    assert "pdf_create" not in listing_decision.requested_tool_names
    assert "file_write" not in listing_decision.requested_tool_names
    assert listing_decision.required_tool_names == ("file_search", "file_read", "workspace_summary")

    regeneration_decision = chat_operator._local_file_reference_scope_decision_from_message(
        "Regenerate regional-values.xlsx and create regional-summary.pdf from it.",
        registry=registry,
        allow_file_write_artifacts=True,
        allow_dedicated_artifact_tools=True,
    )
    assert regeneration_decision is not None
    assert "pdf_create" in regeneration_decision.requested_tool_names
    assert regeneration_decision.requested_artifact_extensions == (".xlsx", ".pdf")


def test_chat_operator_embedded_media_extensions_use_structured_planner_for_format_candidate() -> None:
    class Planner:
        calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            self.calls += 1
            assert kwargs["tools"][0]["name"] == "select_attachment_format"
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {
                            "extension": ".pdf",
                            "embedded_media_extensions": [".pdf"],
                        },
                    }
                ]
            }

    planner = Planner()
    assert chat_operator._requested_embedded_media_extensions(
        "Create a PDF pamphlet with visual media.",
        model_client=planner,
        allow_model_planning=False,
    ) == (".pdf",)
    assert planner.calls == 1


def test_chat_operator_artifact_extensions_do_not_force_saved_history_lookup() -> None:
    independent_result = SimpleNamespace(
        turn=SimpleNamespace(parent_turn_id=None, disposition=chat_operator.ConversationTurnDisposition.INDEPENDENT)
    )
    linked_result = SimpleNamespace(
        turn=SimpleNamespace(parent_turn_id="turn-1", disposition=chat_operator.ConversationTurnDisposition.CONTINUE)
    )

    assert chat_operator._should_auto_include_saved_chat_history(
        independent_result,
        reply_context_prompt=None,
        numbered_option_context=None,
        requested_extensions=(".pdf", ".csv"),
    ) is False
    assert chat_operator._should_auto_include_saved_chat_history(
        linked_result,
        reply_context_prompt=None,
        numbered_option_context=None,
        requested_extensions=(),
    ) is True
    assert chat_operator._should_auto_include_saved_chat_history(
        independent_result,
        reply_context_prompt="prior reply",
        numbered_option_context=None,
        requested_extensions=(),
    ) is True


def test_chat_operator_saved_history_prefers_schema_followup_tool_over_preview_tool(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_search", "email_read"],
            }
        ],
    )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="continue",
        conversation_result=SimpleNamespace(
            turn=SimpleNamespace(parent_turn_id="turn-1", disposition=chat_operator.ConversationTurnDisposition.CONTINUE)
        ),
        saved_history_available=True,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="continue",
    )
    history_context = "\n".join(
        [
            "Recent runtime-evidence-backed saved turns (ranked by relevance and recency):",
            "Evidence 1. 2026-05-31T17:00:00+00:00",
            "Tools: email_search",
            'Tool evidence: [{"output": {"body_requires_tool": "email_read", "next_tool_for_body": "email_read"}}]',
            "User: prior account read",
            "Assistant: Preview only.",
        ]
    )

    widened_registry = chat_operator._augment_tool_registry_from_saved_history_context(
        active_registry,
        base_registry=registry,
        evidence=evidence,
        history_context=history_context,
    )

    names = {definition["name"] for definition in widened_registry.list_tool_definitions()}
    assert names == {"email_read", "request_tool_scope"}
    scope_prompt = chat_operator._saved_history_tool_scope_prompt(history_context)
    assert scope_prompt is not None
    assert "email_read" in scope_prompt
    assert "email_search" not in scope_prompt


def test_chat_operator_schema_followup_retry_narrows_to_named_tool() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(user_message="continue", conversation_result=None)
    initial_result = SimpleNamespace(
        tool_results=[
            {
                "tool_name": "email_search",
                "status": "completed",
                "output": {"body_requires_tool": "email_read", "next_tool_for_body": "email_read"},
            }
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Preview only.",
    )
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("read-1", "email_read", "completed", {"body": "full"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Full body summary.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text == "Full body summary."
    assert calls == [{"email_read"}]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"email_read"}


def test_chat_operator_schema_followup_retry_preserves_initial_result_on_backend_failure() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    evidence = chat_operator.build_turn_tool_evidence(user_message="continue", conversation_result=None)
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "doc-1",
                "document_create",
                "completed",
                {
                    "path": "/tmp/artifact.docx",
                    "body_requires_tool": "email_read",
                    "next_tool_for_body": "email_read",
                },
            )
        ],
        artifacts=["/tmp/artifact.docx"],
        suspended_for_approval=False,
        final_text="Done.",
    )

    def run_turn(_history, _retry_registry):
        raise chat_operator.ChatBackendUnavailableError("usage limit")

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=evidence,
        run_turn=run_turn,
    )

    assert result is initial_result
    assert retry_registry is registry
    assert retried is False


def test_chat_operator_schema_followup_skips_model_retry_after_required_sources_are_complete() -> None:
    registry = ToolRegistry()
    evidence = TurnToolEvidence(requested_extensions=(".xlsx", ".pdf", ".html"))
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "required_tool_names": [
                        "weather_forecast",
                        "market_quote",
                        "browser_navigate",
                        "spreadsheet_create",
                        "pdf_create",
                        "file_write",
                    ]
                },
            ),
            ToolResult("weather-1", "weather_forecast", "completed", {"temperature": 72}),
            ToolResult("market-1", "market_quote", "completed", {"symbol": "SPY"}),
            ToolResult(
                "navigate-1",
                "browser_navigate",
                "completed",
                {"url": "https://example.com/markets", "session_id": "browser-1"},
            ),
            ToolResult(
                "extract-1",
                "browser_extract_text",
                "completed",
                {"text": "Verified public market source.", "session_id": "browser-1"},
            ),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Sources collected.",
    )

    def run_turn(_history, _retry_registry):
        raise AssertionError("artifact production must remain in the orchestrator loop")

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=evidence,
        run_turn=run_turn,
    )

    assert result is initial_result
    assert retry_registry is registry
    assert retried is False


def test_chat_operator_schema_followup_retry_uses_source_search_after_invalid_email_read() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "read-1",
                "email_read",
                "failed",
                {
                    "reason": "invalid_message_id",
                    "required_source_tool": "email_search",
                    "next_step": "Search email and pass one returned result id to email_read.",
                },
                error="Invalid email message id.",
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need the exact email message ID.",
    )
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[
                ToolResult("search-1", "email_search", "completed", {"results": [{"id": "message-1"}]}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="I searched email for fresh matches.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="check more", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text == "I searched email for fresh matches."
    assert calls == [{"email_search"}]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"email_search"}


def test_chat_operator_schema_followup_retry_skips_after_requested_artifact(tmp_path) -> None:
    workbook = tmp_path / "deliverable.xlsx"
    workbook.write_bytes(b"PK\x03\x04xlsx")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="browser_click_element",
            description="Click element",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {"artifact_extensions": [".xlsx"], "available_tools": ["spreadsheet_create", "browser_click_element"]},
            ),
            ToolResult(
                "sheet-1",
                "spreadsheet_create",
                "completed",
                {
                    "artifact_path": str(workbook),
                    "artifact_paths": [str(workbook)],
                    "artifact_descriptors": [
                        {
                            "path": str(workbook),
                            "extension": ".xlsx",
                            "role": "deliverable",
                            "kind": "spreadsheet",
                        }
                    ],
                    "next_tool": "browser_click_element",
                },
            ),
        ],
        artifacts=[str(workbook)],
        suspended_for_approval=False,
        final_text=f"Done — attached the requested spreadsheet.\n\nMEDIA:{workbook}",
    )
    calls: list[object] = []

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="create a workbook", conversation_result=None),
        run_turn=lambda history, retry_registry: calls.append((history, retry_registry)) or initial_result,
    )

    assert result is initial_result
    assert retry_registry is registry
    assert retried is False
    assert calls == []


def test_chat_operator_schema_followup_retry_uses_tool_continuation_metadata() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="list_crons",
            description="List scheduled work",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            continuation_tools=("run_cron", "delete_cron", "update_cron"),
        ),
        ToolSpec(
            name="run_cron",
            description="Run scheduled work",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
        ToolSpec(
            name="delete_cron",
            description="Delete scheduled work",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
        ToolSpec(
            name="update_cron",
            description="Update scheduled work",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(user_message="continue", conversation_result=None)
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("list-1", "list_crons", "completed", {"crons": [{"id": "cron-1", "name": "Morning"}]})
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I can see the list, but the action tool is not available.",
    )
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"id": "cron-1"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Started Morning.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text == "Started Morning."
    assert {"run_cron", "delete_cron", "update_cron"}.issubset(calls[0])
    assert "request_tool_scope" not in calls[0]
    retry_names = {definition["name"] for definition in retry_registry.list_tool_definitions()}
    assert {"run_cron", "delete_cron", "update_cron"}.issubset(retry_names)
    assert "request_tool_scope" not in retry_names


def test_chat_operator_schema_followup_retry_does_not_add_browser_screenshot_without_artifact_evidence() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="browser_extract_items",
            description="Extract browser rows",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            continuation_tools=("browser_extract_text", "browser_screenshot"),
        ),
        ToolSpec(
            name="browser_extract_text",
            description="Extract browser text",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="browser_screenshot",
            description="Capture browser screenshot",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="Check open pull requests for https://github.com/sample-team/sample-app",
        conversation_result=None,
    )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "items-1",
                "browser_extract_items",
                "completed",
                {"items": [{"title": "0 Open"}]},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need more page text.",
    )
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("text-1", "browser_extract_text", "completed", {"text": "0 Open"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="There are 0 open pull requests.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is False
    assert result is initial_result
    assert calls == []
    assert retry_registry is registry


def test_chat_operator_schema_followup_retry_keeps_open_url_continuations_read_only() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="browser_extract_items",
            description="Extract browser rows",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            continuation_tools=(
                "browser_extract_text",
                "browser_snapshot",
                "browser_run_js",
                "browser_wait_for",
            ),
        ),
        ToolSpec(
            name="browser_extract_text",
            description="Extract browser text",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="browser_snapshot",
            description="Snapshot browser state",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="browser_run_js",
            description="Run browser JavaScript",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="browser_wait_for",
            description="Wait for browser state",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="Check open pull requests for https://github.com/sample-team/sample-app",
        conversation_result=None,
    )
    active_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=evidence,
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="open_url",
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "items-1",
                "browser_extract_items",
                "completed",
                {"items": [{"title": "0 Open"}]},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need more page text.",
    )
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("text-1", "browser_extract_text", "completed", {"text": "0 Open"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="There are 0 open pull requests.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        evidence=evidence,
        run_turn=run_turn,
        fallback_tool_names=("browser_extract_text",),
    )

    assert retried is True
    assert result.final_text == "There are 0 open pull requests."
    assert "browser_extract_text" in calls[0]
    assert "browser_snapshot" not in calls[0]
    assert "browser_run_js" not in calls[0]
    assert "browser_wait_for" not in calls[0]
    retry_names = {definition["name"] for definition in retry_registry.list_tool_definitions()}
    assert "browser_extract_text" in retry_names
    assert "browser_snapshot" not in retry_names
    assert "browser_run_js" not in retry_names
    assert "browser_wait_for" not in retry_names


def test_chat_operator_schema_followup_retry_preserves_substantive_result_when_retry_has_no_tools() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "search-1",
                "email_search",
                "completed",
                {
                    "results": [{"id": "msg-1", "subject": "Stage QA"}],
                    "next_tool_for_body": "email_read",
                    "body_requires_tool": "email_read",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found one email preview.",
    )
    retry_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I do not have an email search tool available.",
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="open it", conversation_result=None),
        run_turn=lambda _history, _registry: retry_result,
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_schema_followup_retry_skips_browser_continuation_after_verified_answer() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="browser_extract_text",
            description="Extract browser text",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            continuation_tools=("browser_extract_items",),
        ),
        ToolSpec(
            name="browser_assert_page_state",
            description="Assert browser state",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            continuation_tools=("browser_extract_items",),
        ),
        ToolSpec(
            name="browser_extract_items",
            description="Extract browser rows",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "assert-1",
                "browser_assert_page_state",
                "completed",
                {"verified": True, "result": {"required": [{"expected": "$120", "match": "$120"}]}},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Option A is available for $120 total.",
    )
    calls: list[object] = []

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="compare available options", conversation_result=None),
        run_turn=lambda history, retry_registry: calls.append((history, retry_registry)) or initial_result,
    )

    assert result is initial_result
    assert retry_registry is registry
    assert retried is False
    assert calls == []


def test_chat_operator_schema_followup_retry_skips_browser_metadata_after_read_answer() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="browser_navigate",
            description="Open browser page",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            continuation_tools=("browser_snapshot", "browser_extract_text", "browser_extract_items"),
        ),
        ToolSpec(
            name="browser_extract_text",
            description="Extract browser text",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            continuation_tools=("browser_extract_items",),
        ),
        ToolSpec(
            name="browser_snapshot",
            description="Snapshot browser page",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="browser_extract_items",
            description="Extract browser rows",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("nav-1", "browser_navigate", "completed", {"result": {"url": "https://example.test"}}),
            ToolResult("text-1", "browser_extract_text", "completed", {"text": "Adults are not required to wear helmets."}),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Adults do not need a helmet for this rental.",
    )
    calls: list[object] = []

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="verify the source", conversation_result=None),
        run_turn=lambda history, retry_registry: calls.append((history, retry_registry)) or initial_result,
    )

    assert result is initial_result
    assert retry_registry is registry
    assert retried is False
    assert calls == []


def test_chat_operator_schema_followup_retry_uses_continuation_metadata_without_reply_phrase() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="file_search",
            description="Search files",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            continuation_tools=("file_read",),
        ),
        ToolSpec(
            name="file_read",
            description="Read files",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("search-1", "file_search", "completed", {"matches": ["a.txt"]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found one matching file.",
    )

    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("read-1", "file_read", "completed", {"path": "a.txt"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Read a.txt.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="continue", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text == "Read a.txt."
    assert calls == [{"file_read"}]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"file_read"}


def test_chat_operator_schema_followup_retry_refreshes_saved_history_account_read() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="chat_history_search",
            description="Search saved chat history",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("conversation_history",),
        ),
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="email_read",
            description="Read email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(user_message="continue", conversation_result=None)
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("history-1", "chat_history_search", "completed", {"matches": []}),
            ToolResult("read-1", "email_read", "completed", {"id": "message-1", "message": "body"}),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I read one saved-context email but could not refresh the inbox.",
    )
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[
                ToolResult("search-1", "email_search", "completed", {"results": [{"id": "message-2"}]}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Fresh inbox search complete.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text == "Fresh inbox search complete."
    assert calls == [{"email_search"}]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"email_search"}


def test_chat_operator_missing_tool_scope_retry_forces_structured_scope_after_scope_tool(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="calendar_list",
            description="List calendar events",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(user_message="calendario?", conversation_result=None)
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="calendario?",
    )
    assert {definition["name"] for definition in active_registry.list_tool_definitions()} == {"request_tool_scope"}
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {"requested_tool_names": ["calendar_list"], "required_tool_names": ["calendar_list"]},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need a calendar read.",
    )
    decisions: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decisions.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            requested_tool_names=("calendar_list",),
            required_tool_names=("calendar_list",),
            connector_app_ids=("workspace_admin",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Scoped tool recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("calendar-1", "calendar_list", "completed", {"events": []})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Calendar checked.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="calendario?",
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is True
    assert decisions == ["calendario?"]
    assert calls == [{"calendar_list"}]
    assert result.final_text == "Calendar checked."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"calendar_list"}


def test_chat_operator_missing_tool_scope_retry_ignores_reply_phrase_without_scope_gateway(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="calendar_list",
            description="List calendar events",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I don’t currently have a calendar connector/tool available in this session.",
    )

    def forbidden_build_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("reply prose must not force a scope model call")

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", forbidden_build_decision)
    result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="calendario?",
        evidence=chat_operator.build_turn_tool_evidence(user_message="calendario?", conversation_result=None),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("must not retry")),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_missing_tool_scope_retry_recovers_capability_denial_scheduler(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            "I can help, but I need the scheduler/crons access for your workspace first. "
            "Please grant or expose the scheduler tools so I can list the disabled cron jobs and enable them safely."
        ),
    )
    classifier_calls: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        classifier_calls.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="mutate",
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            scheduler_toggle_enabled=True,
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Scoped tool recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"updated": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Cron jobs enabled.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="habilita las tareas programadas",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="habilita las tareas programadas",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert classifier_calls == ["habilita las tareas programadas"]
    assert calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Cron jobs enabled."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {
        "list_crons",
        "toggle_cron",
    }


def test_chat_operator_missing_tool_scope_retry_uses_scheduler_action_defaults(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="The scheduler capability is not exposed here.",
    )

    def fake_build_decision(**_kwargs):  # noqa: ANN003
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="mutate",
            requested_tool_names=(),
            required_tool_names=(),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"updated": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Updated.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="non English scheduler mutation",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="non English scheduler mutation",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Updated."


def test_chat_operator_missing_tool_scope_retry_recovers_scheduler_clarification(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            "I can help, but I need to know which cron system you mean before enabling anything.\n\n"
            "1. Local machine crons\n"
            "2. Nullion scheduled tasks"
        ),
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="mutate",
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            scheduler_toggle_enabled=True,
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"updated": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled scheduled tasks.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="enable everything scheduled here",
        evidence=chat_operator.build_turn_tool_evidence(user_message="enable everything scheduled here", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Enabled scheduled tasks."


def test_chat_operator_tool_alias_markup_retries_scheduler_mutation_tools() -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="<tool:scheduler_mutation />I need scheduler tools.",
    )
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"updated": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled scheduled tasks.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Enabled scheduled tasks."


def test_chat_operator_media_directive_without_tools_recovers_file_scope(monkeypatch, tmp_path: Path) -> None:
    registry = ToolRegistry()
    for name in ("request_tool_scope", "file_search", "file_read", "workspace_summary"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",) if name == "request_tool_scope" else ("local_files",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    guessed_artifact = tmp_path / "stage_requested_file_delivery_contract.xlsx"
    guessed_artifact.write_bytes(b"PK\x03\x04structured workbook")
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=f"No tengo acceso de archivos.\n\nMEDIA:{guessed_artifact}",
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(confidence=1.0, valid=True)

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("file-1", "file_search", "completed", {"matches": [str(guessed_artifact)]})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="File checked.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message=f"adjunta {guessed_artifact.name}",
        evidence=chat_operator.build_turn_tool_evidence(user_message=f"adjunta {guessed_artifact.name}", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"file_search", "file_read", "workspace_summary"}]
    assert result.final_text == "File checked."


def test_chat_operator_missing_tool_scope_retry_recovers_local_shell_denial(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("terminal_exec", ToolSideEffectClass.DANGEROUS_EXEC, ("local_shell",)),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.HIGH if name == "terminal_exec" else ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=name == "terminal_exec",
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I can do that, but local shell access isn’t currently available in this turn.",
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            requested_tool_names=("terminal_exec",),
            required_tool_names=("terminal_exec",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("terminal-1", "terminal_exec", "completed", {"stdout": "ok"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Local check complete.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="revise el entorno local",
        evidence=chat_operator.build_turn_tool_evidence(user_message="revise el entorno local", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"terminal_exec"}]
    assert result.final_text == "Local check complete."


def test_chat_operator_local_system_fallback_recovers_no_tool_shell_denial(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="terminal_exec",
            description="Run terminal command",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
            capability_tags=("local_shell",),
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="The local shell tool is not available to me in this turn.",
    )

    initial_result.final_text = '<tool_request tool="request_tool_scope" capabilities="local_shell"></tool_request>'

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            requested_tool_names=("terminal_exec",),
            required_tool_names=("terminal_exec",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("terminal-1", "terminal_exec", "completed", {"stdout": "Python 3.11.15"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Python 3.11.15",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="revise el entorno local",
        evidence=chat_operator.build_turn_tool_evidence(user_message="revise el entorno local", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"terminal_exec"}]
    assert result.final_text == "Python 3.11.15"


def test_chat_operator_local_system_fallback_recovers_approval_command_draft(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="terminal_exec",
            description="Run terminal command",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
            capability_tags=("local_shell",),
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="If you approve, I will run python3 --version and which python3.",
    )

    initial_result.final_text = '<tool_request tool="request_tool_scope" capabilities="local_shell"></tool_request>'

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            requested_tool_names=("terminal_exec",),
            required_tool_names=("terminal_exec",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("terminal-1", "terminal_exec", "completed", {"stdout": "Python 3.11.15"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Python 3.11.15",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="revise el entorno local",
        evidence=chat_operator.build_turn_tool_evidence(user_message="revise el entorno local", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"terminal_exec"}]
    assert result.final_text == "Python 3.11.15"


def test_chat_operator_missing_tool_scope_retry_does_not_slow_plain_reply(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Hi, how can I help?",
    )

    def forbidden_build_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("plain replies must not run a forced scope model call")

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", forbidden_build_decision)

    result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="hi",
        evidence=chat_operator.build_turn_tool_evidence(user_message="hi", conversation_result=None),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("must not retry")),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_model_request_scope_markup_retries_scheduler_tools() -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("run_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<request_tool_scope>{"scheduler_run":true,'
            '"tool_names":["list_crons","run_cron"]}</request_tool_scope>'
            "I cannot start it yet."
        ),
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Structured tool-request recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"started": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Manual run started.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"list_crons", "run_cron"}]
    assert result.final_text == "Manual run started."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "run_cron"}


def test_chat_operator_request_scope_markup_retries_web_scope_without_exact_tools() -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("web_search", ("web",)),
        ("web_fetch", ("web",)),
        ("browser_extract_text", ("browser", "web")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text='<request_tool_scope>{"tool_scope":"web"}</request_tool_scope>I need a live check.',
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible = {definition["name"] for definition in retry_registry.list_tool_definitions()}
        calls.append(visible)
        assert "web_fetch" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("fetch-1", "web_fetch", "completed", {"text": "live result"})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Live result checked.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(has_url_target=True),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"web_search", "web_fetch", "browser_extract_text"}]
    assert result.final_text == "Live result checked."


def test_chat_operator_request_scope_markup_retries_singular_file_family() -> None:
    registry = ToolRegistry()
    for name in ("request_tool_scope", "file_search", "file_read", "workspace_summary"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",) if name == "request_tool_scope" else ("local_files",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text='<request_tool_scope tool_family="files" />Need file access.',
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Structured tool-request recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("file-1", "file_search", "completed", {"matches": []})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="File checked.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"file_search", "file_read", "workspace_summary"}]
    assert result.final_text == "File checked."


def test_chat_operator_request_scope_markup_retries_even_with_unverified_media_artifact(tmp_path: Path) -> None:
    registry = ToolRegistry()
    for name in ("request_tool_scope", "file_search", "file_read", "workspace_summary"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",) if name == "request_tool_scope" else ("local_files",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    guessed_artifact = tmp_path / "guessed.xlsx"
    guessed_artifact.write_bytes(b"not verified by tool")
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[str(guessed_artifact)],
        suspended_for_approval=False,
        final_text=f'<request_tool_scope tool_family="files" />\nMEDIA:{guessed_artifact}',
    )
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("file-1", "file_search", "completed", {"matches": []})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Verified through file tools.",
        )

    result, _retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"file_search", "file_read", "workspace_summary"}]
    assert result.final_text == "Verified through file tools."
    assert str(guessed_artifact) not in result.artifacts


def test_chat_operator_no_tool_retry_verifies_model_media_path_before_delivery(tmp_path: Path) -> None:
    registry = ToolRegistry()
    for name in ("request_tool_scope", "file_search", "file_read", "workspace_summary"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",) if name == "request_tool_scope" else ("local_files",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    guessed_artifact = tmp_path / "guessed.xlsx"
    guessed_artifact.write_bytes(b"not verified by tool")
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[str(guessed_artifact)],
        suspended_for_approval=False,
        final_text=f"Done.\n\nMEDIA:{guessed_artifact}",
    )
    calls: list[set[str]] = []

    def forbidden_build_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("unverified media retry should use file-tool structure without a model call")

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        visible = {definition["name"] for definition in retry_registry.list_tool_definitions()}
        calls.append(visible)
        return SimpleNamespace(
            tool_results=[ToolResult("file-1", "file_search", "completed", {"matches": []})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Verified through file tools.",
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", forbidden_build_decision)
    try:
        result, _retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
            initial_result=initial_result,
            base_history=[],
            active_tool_registry=registry,
            base_tool_registry=registry,
            model_client=object(),
            user_message=f"adjunta {guessed_artifact.name}",
            evidence=TurnToolEvidence(),
            run_turn=run_turn,
        )
    finally:
        monkeypatch.undo()

    assert retried is True
    assert calls == [{"file_search", "file_read", "workspace_summary"}]
    assert result.final_text == "Verified through file tools."
    assert str(guessed_artifact) not in result.artifacts


def test_chat_operator_no_tool_retry_uses_filename_extension_signal_without_media(monkeypatch) -> None:
    registry = ToolRegistry()
    for name in ("request_tool_scope", "file_search", "file_read", "workspace_summary"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("tool_scope",) if name == "request_tool_scope" else ("local_files",),
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="The file tool is not active.",
    )
    calls: list[set[str]] = []

    def forbidden_build_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("filename extension signal should not require a model scope call")

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("file-1", "file_search", "completed", {"matches": []})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Verified through file tools.",
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", forbidden_build_decision)

    result, _retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Adjunta el archivo exacto llamado stage_requested_file_delivery.xlsx como descargable.",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"file_search", "file_read", "workspace_summary"}]
    assert result.final_text == "Verified through file tools."


def test_chat_operator_no_tool_retry_skips_completed_scheduler_read_only(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="list_crons",
            description="List scheduled tasks",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"crons": [{"id": "cron-1"}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Here are your scheduled tasks.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("read-only scheduler result must not rescope")),
    )

    def forbidden_retry(_retry_history, _retry_registry):  # noqa: ANN001
        raise AssertionError("completed scheduler inventory must not rerun")

    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque scheduler inventory request 491",
        evidence=TurnToolEvidence(),
        run_turn=forbidden_retry,
    )

    assert result is initial_result
    assert retry_registry is registry
    assert retried is False


def test_chat_operator_no_tool_retry_keeps_complete_plain_reply_on_fast_path(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("weather_forecast", ("weather",)),
        ("market_quote", ("market_data", "quote")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Hello — how can I help?",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("plain reply must not run a scope classifier")),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="salut",
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("no retry expected")),
    )

    assert result is initial_result
    assert retry_registry is registry
    assert retried is False


def test_chat_operator_no_tool_retry_clarifies_unanchored_local_discovery_request(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        (chat_operator.CHAT_HISTORY_SEARCH_TOOL_NAME, ("conversation_history",)),
        ("file_search", ("local_files",)),
        ("file_read", ("local_files",)),
        ("workspace_summary", ("local_files",)),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    class ClarificationModel:
        def create(self, **kwargs):  # noqa: ANN001
            assert kwargs["tools"] == []
            assert kwargs["timeout"] == 8
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "needs_clarification": False,
                                "confidence": 0.97,
                            }
                        ),
                    }
                ]
            }

    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need local file access to identify the report.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(
            requested_tool_names=(
                chat_operator.CHAT_HISTORY_SEARCH_TOOL_NAME,
                "file_search",
                "file_read",
                "workspace_summary",
            ),
            confidence=1.0,
            valid=True,
        ),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=ClarificationModel(),
        user_message="opaque compact reference 491",
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(
            AssertionError("ambiguous local discovery must not execute tools")
        ),
    )

    assert retried is True
    assert retry_registry is registry
    assert result.final_text == (
        "Which prior item do you mean? Please send its title, file, link, or reply to the relevant message."
    )
    assert result.tool_results == []
    assert result.artifacts == []


def test_chat_operator_no_tool_retry_continues_after_partial_tool_unfulfilled_draft(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"items": [{"id": "cron-1"}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I checked scheduler state, but the toggle tool is not exposed yet.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        ),
    )
    visible_tool_calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled the disabled scheduler item.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque scheduler mutation request 712",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert visible_tool_calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Enabled the disabled scheduler item."
    assert [tool.tool_name for tool in result.tool_results] == ["list_crons", "toggle_cron"]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "toggle_cron"}


def test_chat_operator_missing_tool_scope_retry_exposes_scope_request_from_base_registry(monkeypatch) -> None:
    active_registry = ToolRegistry()
    base_registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        base_registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need the scheduler tool, but it is not exposed in this turn.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(confidence=0.0, valid=False),
    )
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "request_tool_scope is registered" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "scope-1",
                    "request_tool_scope",
                    "completed",
                    {"scope_requested": True, "available_tools": ["list_crons", "toggle_cron"]},
                ),
                ToolResult("list-1", "list_crons", "completed", {"items": [{"id": "cron-1"}]}),
                ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled the scheduler item.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="opaque scheduler mutation request 884",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert visible_tool_calls == [{"request_tool_scope"}]
    assert result.final_text == "Enabled the scheduler item."
    assert [tool.tool_name for tool in result.tool_results] == ["request_tool_scope", "list_crons", "toggle_cron"]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"request_tool_scope"}


def test_chat_operator_missing_tool_scope_retry_preserves_reply_when_scope_tool_not_used(monkeypatch) -> None:
    active_registry = ToolRegistry()
    base_registry = ToolRegistry()
    base_registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I opened the conversation entry, but I still need to read the live thread.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(confidence=0.0, valid=False),
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "request_tool_scope is registered" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[],
            artifacts=[],
            suspended_for_approval=False,
            final_text="I checked your inbox for unread updates.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="Open this app thread and summarize the last message.",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is active_registry
    assert calls == []


def test_chat_operator_no_tool_scope_retry_uses_explicit_connector_identifier(monkeypatch) -> None:
    from nullion import turn_context_policy

    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("calendar_list", ("calendar", "connector", "account_read")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        lambda *args, **kwargs: (),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_agenda",
                "active_app_ids": ["proveedor-agenda"],
                "structured_tools": ["calendar_list"],
            }
        ],
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="No live source was used.",
    )
    decisions: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decisions.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_app_ids=("proveedor-agenda",),
            connector_source_user_requested=True,
            requested_tool_names=("calendar_list",),
            required_tool_names=("calendar_list",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Structured no-tool recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("calendar-1", "calendar_list", "completed", {"events": []})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Agenda checked.",
        )

    active = chat_operator.ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(valid=True, skill_pack_action="connector",
            connector_app_ids=("proveedor-agenda",), connector_source_user_requested=True))
    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active,
        base_tool_registry=registry,
        model_client=object(),
        user_message="proveedor-agenda mañana",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert result.retried_no_tool_scope_decision
    assert decisions == ["proveedor-agenda mañana"]
    assert calls == [{"calendar_list"}]
    assert result.result.final_text == "Agenda checked."


def test_chat_operator_ambiguous_scope_markup_forces_typed_scope_decision(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("request_tool_scope", ToolSideEffectClass.READ, ("tool_scope",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("run_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text='<request_tool_scope>{"tool_families":["scheduler"]}</request_tool_scope>I need tools.',
    )
    classifier_calls: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        classifier_calls.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="run",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"started": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Manual run started.",
        )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="ejecuta la tarea recurrente seleccionada ahora",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert result.retried_no_tool_scope_decision
    assert classifier_calls == ["ejecuta la tarea recurrente seleccionada ahora"]
    assert calls == [{"list_crons", "run_cron"}]
    assert result.result.final_text == "Manual run started."


def test_chat_operator_connector_skill_scope_retry_reuses_accepted_skill(monkeypatch, tmp_path: Path) -> None:
    from nullion import turn_context_policy
    from nullion.runtime import PersistentRuntime
    from nullion.runtime_store import RuntimeStore

    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="connector_request",
            description="Use connector gateway",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("connector",),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(
        chat_operator,
        "_active_connector_app_id_providers_for_message",
        lambda: ({"active_app_ids": ("google-drive",)},),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_google_drive",
                "active_app_ids": ["google-drive"],
                "structured_tools": ["connector_request"],
            }
        ],
    )
    monkeypatch.setattr(
        chat_operator,
        "_active_connector_structured_tools_for_app",
        lambda app_id: ("connector_request",) if app_id == "google-drive" else (),
    )
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
    )
    runtime.create_skill(
        title="Google Drive connector workflow",
        summary="Reuse the Google Drive connector route.",
        trigger="Use for Google Drive requests.",
        steps=["Request the connector route and verify the answer."],
        tags=["builder", "connector", "workflow", "connector-app:google-drive"],
    )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="Can u check recent folder created on google-drive?",
        conversation_result=None,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="Can u check recent folder created on google-drive?",
    )
    assert {definition["name"] for definition in active_registry.list_tool_definitions()} == {"request_tool_scope"}
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need Google Drive approved again.",
    )
    calls: list[set[str]] = []
    skill_reuse: list[str] = []
    activity_events: list[dict[str, object]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        prompt = retry_history[-1]["content"][0]["text"]
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Connector skill reuse" in prompt
        assert "Do not ask the user to approve or build the same skill again" in prompt
        return SimpleNamespace(
            tool_results=[ToolResult("drive-1", "connector_request", "completed", {"items": ["Phone Bill"]})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="The newest folder is Phone Bill.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_connector_skill_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        runtime=runtime,
        evidence=evidence,
        user_message="Can u check recent folder created on google-drive?",
        run_turn=run_turn,
        skill_reuse_callback=lambda titles: (
            skill_reuse.extend(titles),
            chat_operator._emit_skill_usage_activity(activity_events.append, titles),
        ),
    )

    assert retried is True
    assert calls == [{"connector_request"}]
    assert skill_reuse == ["Google Drive connector workflow"]
    assert activity_events == [
        {
            "id": "skill",
            "label": "Using learned skill",
            "status": "done",
            "detail": "  \u29c1 Google Drive connector workflow",
        }
    ]
    assert result.final_text == "The newest folder is Phone Bill."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"connector_request"}


def test_chat_operator_connector_skill_retry_does_not_require_scope_tool(monkeypatch, tmp_path: Path) -> None:
    from nullion import turn_context_policy
    from nullion.runtime import PersistentRuntime
    from nullion.runtime_store import RuntimeStore

    base_registry = ToolRegistry()
    base_registry.register(
        ToolSpec(
            name="connector_request",
            description="Use connector gateway",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("connector",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    active_registry = ToolRegistry()
    active_registry.register(
        ToolSpec(
            name="workspace_summary",
            description="Summarize workspace",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("local",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    monkeypatch.setattr(
        chat_operator,
        "_active_connector_app_id_providers_for_message",
        lambda: ({"active_app_ids": ("google-drive",)},),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_google_drive",
                "active_app_ids": ["google-drive"],
                "structured_tools": ["connector_request"],
            }
        ],
    )
    monkeypatch.setattr(
        chat_operator,
        "_active_connector_structured_tools_for_app",
        lambda app_id: ("connector_request",) if app_id == "google-drive" else (),
    )
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
    )
    runtime.create_skill(
        title="Google Drive connector workflow",
        summary="Reuse the Google Drive connector route.",
        trigger="Use for Google Drive requests.",
        steps=["Use the connector route and verify the answer."],
        tags=["builder", "connector", "workflow", "connector-app:google-drive"],
    )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("history-1", chat_operator.CHAT_HISTORY_SEARCH_TOOL_NAME, "completed", {"match_count": 1}),
            ToolResult("workspace-1", "workspace_summary", "completed", {"file_count": 10}),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I can't do a fresh live Google Drive check from this turn.",
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        prompt = retry_history[-1]["content"][0]["text"]
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Connector skill reuse" in prompt
        return SimpleNamespace(
            tool_results=[ToolResult("drive-1", "connector_request", "completed", {"items": ["Travel"]})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="The newest folder is Travel.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_connector_skill_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        runtime=runtime,
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="connector app google-drive latest folder",
            conversation_result=None,
        ),
        user_message="connector app google-drive latest folder",
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"connector_request"}]
    assert result.final_text == "The newest folder is Travel."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"connector_request"}


def test_chat_operator_scheduler_run_fallback_recovers_after_browser_misroute(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("browser_open", ToolSideEffectClass.READ, ("browser",)),
        ("browser_extract_text", ToolSideEffectClass.READ, ("browser",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("run_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.MEDIUM if name == "run_cron" else ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    evidence = chat_operator.build_turn_tool_evidence(user_message="I meant the cron", conversation_result=None)
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("open-1", "browser_open", "completed", {"url": "https://example.invalid/stale"}),
            ToolResult("extract-1", "browser_extract_text", "completed", {"text": "unrelated page"}),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I ended up on an unrelated browser page.",
    )
    decisions: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decisions.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="run",
            requested_tool_names=("run_cron",),
            required_tool_names=("run_cron",),
            confidence=0.91,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Scheduler run recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"message": "Started tracker."})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Started tracker.",
        )

    registry.turn_tool_scope_decision = chat_operator.TurnToolScopeDecision(
        scheduler_action="run", requested_tool_names=("list_crons", "run_cron"),
        required_tool_names=("run_cron",), valid=True)

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_run_fallback_retry(
        initial_result=initial_result,
        base_history=[
            {"role": "user", "content": [{"type": "text", "text": "Trigger my nullion tracker"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "I need scheduler scope to run that."}]},
        ],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="I meant the cron",
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is True
    assert decisions == []
    assert calls == [{"list_crons", "run_cron"}]
    assert result.final_text == "Started tracker."
    assert [tool.tool_name for tool in result.tool_results] == ["browser_open", "browser_extract_text", "run_cron"]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "run_cron"}


def test_chat_operator_scheduler_run_fallback_recovers_from_structured_no_tool_scheduler_scope(monkeypatch) -> None:
    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.MEDIUM if name == "run_cron" else ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    registry.turn_tool_scope_decision = chat_operator.TurnToolScopeDecision(
        scheduler_action="run",
        requested_tool_names=("run_cron",),
        required_tool_names=("run_cron",),
        confidence=1.0,
        valid=True,
    )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="Can u run nullion social cron?",
        conversation_result=None,
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Opaque incomplete scheduler response.",
    )

    def forbidden_build_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("existing structured scheduler scope should avoid another model call")

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", forbidden_build_decision)
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "structured scope decision" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"message": "Started tracker."})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Started tracker.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_run_fallback_retry(
        initial_result=initial_result,
        base_history=[
            {
                "role": "assistant",
                "content": [{"type": "text", "text": "Scheduled task: Nullion social + GitHub traffic monitor"}],
            }
        ],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Can u run nullion social cron?",
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"list_crons", "run_cron"}]
    assert result.final_text == "Started tracker."
    assert [tool.tool_name for tool in result.tool_results] == ["run_cron"]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "run_cron"}


def test_chat_operator_scheduler_run_fallback_does_not_retry_plain_no_tool_reply(monkeypatch) -> None:
    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.MEDIUM if name == "run_cron" else ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )

    def forbidden_build_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("plain no-tool replies must not force a scope model call")

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", forbidden_build_decision)
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            "I can trigger it, but the cron run tool isn't exposed in this turn, "
            "so I can't actually start it from here yet."
        ),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_run_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="hello",
        evidence=chat_operator.build_turn_tool_evidence(user_message="hello", conversation_result=None),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("must not retry")),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_scheduler_run_fallback_requires_structured_scheduler_decision(monkeypatch) -> None:
    registry = ToolRegistry()
    for name in ("file_search", "list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.MEDIUM if name == "run_cron" else ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron") if name in {"list_crons", "run_cron"} else ("local",),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: chat_operator.TurnToolScopeDecision(valid=True, confidence=0.9),
    )
    calls: list[object] = []
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("search-1", "file_search", "completed", {"matches": ["agent_orchestrator.py"]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found 1 file.",
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_run_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="inspect local files",
        evidence=chat_operator.build_turn_tool_evidence(user_message="inspect local files", conversation_result=None),
        run_turn=lambda history, retry_registry: calls.append((history, retry_registry)) or initial_result,
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry
    assert calls == []


def test_chat_operator_scheduler_mutation_fallback_recovers_after_history_misroute(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("chat_history_search", ToolSideEffectClass.READ, ("conversation_history",)),
        ("file_search", ToolSideEffectClass.READ, ("local",)),
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
        ("delete_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.MEDIUM if name == "delete_cron" else ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=name == "delete_cron",
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("history-1", "chat_history_search", "completed", {"matches": [{"summary": "stale"}]}),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found a saved turn, but not the scheduler state.",
    )
    registry.turn_tool_scope_decision = chat_operator.TurnToolScopeDecision(
        scheduler_action="mutate",
        scheduler_toggle_enabled=False,
        requested_tool_names=("toggle_cron",),
        required_tool_names=("toggle_cron",),
        confidence=0.92,
        valid=True,
    )
    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", lambda **kwargs: pytest.fail("Existing typed scope must be reused"))
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Scheduler mutation recovery" in retry_history[-1]["content"][0]["text"]
        assert "local file search" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"message": "Updated scheduler state."})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Updated scheduler state.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial_result,
        base_history=[
            {"role": "user", "content": [{"type": "text", "text": "Opaque current request 8842"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "I found a stale saved turn."}]},
        ],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Opaque current request 8842",
        evidence=chat_operator.build_turn_tool_evidence(user_message="Opaque current request 8842", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert visible_tool_calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Updated scheduler state."
    assert [tool.tool_name for tool in result.tool_results] == ["chat_history_search", "toggle_cron", "list_crons"]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "toggle_cron"}


def test_chat_operator_scheduler_mutation_fallback_recovers_after_read_only_listing(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    registry.turn_tool_scope_decision = chat_operator.TurnToolScopeDecision(
        scheduler_action="mutate",
        scheduler_toggle_enabled=True,
        requested_tool_names=("list_crons", "toggle_cron"),
        required_tool_names=("toggle_cron",),
        confidence=1.0,
        valid=True,
    )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"items": [{"id": "cron-1"}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found one disabled scheduler item but did not change it.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("existing typed scope should be reused")),
    )
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Scheduler mutation recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult("list-2", "list_crons", "completed", {"items": [{"id": "cron-1"}]}),
                ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled the scheduler item.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque scheduler mutation request 931",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="opaque scheduler mutation request 931",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert visible_tool_calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Enabled the scheduler item."
    assert "toggle_cron" in [tool.tool_name for tool in result.tool_results]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "toggle_cron"}


def test_chat_operator_scheduler_mutation_fallback_recovers_after_unfulfilled_read_only_listing(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )

    initial_result = SimpleNamespace(
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"items": [{"id": "cron-1", "enabled": False}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I can see the disabled cron, but I don't currently have the scheduler mutation tool available.",
    )

    decision_messages: list[str] = []

    def build_decision(**kwargs):  # noqa: ANN001
        decision_messages.append(str(kwargs["assistant_no_tool_draft"]))
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=0.92,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", build_decision)
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Scheduler mutation recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult("list-2", "list_crons", "completed", {"items": [{"id": "cron-1", "enabled": False}]}),
                ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled the disabled cron.",
        )

    registry.turn_tool_scope_decision = build_decision(assistant_no_tool_draft=initial_result.final_text)
    decision_messages.clear()

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque scheduler mutation request 482",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="opaque scheduler mutation request 482",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert decision_messages == []
    assert visible_tool_calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Enabled the disabled cron."
    assert [tool.tool_name for tool in result.tool_results] == ["list_crons", "list_crons", "toggle_cron"]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "toggle_cron"}


def test_chat_operator_scheduler_object_schema_cannot_authorize_mutation(
    monkeypatch,
) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )

    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {"crons": [{"id": "cron-1", "name": "Fixture cron", "enabled": False}]},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="The scheduler mutation tool is not available in this turn.",
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            required_tool_names=(),
            confidence=0.4,
            valid=True,
        ),
    )
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Scheduler mutation recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Updated scheduler state.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque scheduler mutation request 619",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="opaque scheduler mutation request 619",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is False
    assert visible_tool_calls == []
    assert result is initial_result
    assert [tool.tool_name for tool in result.tool_results] == ["list_crons"]
    assert retry_registry is registry


def test_chat_operator_scheduler_mutation_fallback_uses_active_retry_scope_after_model_directive(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, side_effect, tags in (
        ("list_crons", ToolSideEffectClass.READ, ("scheduler", "cron")),
        ("toggle_cron", ToolSideEffectClass.WRITE, ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    retry_scope_decision = chat_operator.TurnToolScopeDecision(
        scheduler_action="mutate",
        scheduler_toggle_enabled=True,
        requested_tool_names=("toggle_cron",),
        required_tool_names=("toggle_cron",),
        confidence=1.0,
        valid=True,
    )
    active_retry_registry = chat_operator._ToolRegistryAllowedNames(
        registry,
        allowed_tool_names=("toggle_cron",),
        turn_tool_scope_decision=retry_scope_decision,
    )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"items": [{"id": "cron-1"}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_request>{"tool":"request_tool_scope","data":{"tool_scope":"scheduler",'
            '"source_user_requested":true,"tool_names":["toggle_cron"]}}</tool_request>'
        ),
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("active retry scope should be reused")),
    )
    visible_tool_calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[
                ToolResult("list-2", "list_crons", "completed", {"items": [{"id": "cron-1"}]}),
                ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled the scheduler item.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_retry_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque scheduler mutation request 510",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="opaque scheduler mutation request 510",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert visible_tool_calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Enabled the scheduler item."
    assert "toggle_cron" in [tool.tool_name for tool in result.tool_results]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "toggle_cron"}


def test_chat_operator_scheduler_mutation_fallback_allows_history_when_scope_is_not_scheduler(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("chat_history_search", ("conversation_history",)),
        ("list_crons", ("scheduler", "cron")),
        ("toggle_cron", ("scheduler", "cron")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ if name != "toggle_cron" else ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: chat_operator.TurnToolScopeDecision(
            requested_tool_names=("chat_history_search",),
            required_tool_names=(),
            confidence=0.88,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("history-1", "chat_history_search", "completed", {"matches": [{"summary": "useful"}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="The saved history has the answer.",
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Opaque history request 9921",
        evidence=chat_operator.build_turn_tool_evidence(user_message="Opaque history request 9921", conversation_result=None),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("must not retry")),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_schema_followup_retry_resolves_structured_email_links() -> None:
    registry = ToolRegistry()
    for name in ("email_read", "browser_navigate", "browser_extract_text", "web_fetch"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    url = "https://www.google.com/maps/search/300+West+55?entry=gmail&source=g"
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "read-1",
                "email_read",
                "completed",
                {
                    "message": {
                        "subject": "Lease",
                        "links": [{"url": url, "text": "300 West 55"}],
                        "link_followup_required": True,
                        "next_tools_for_links": ["browser_navigate", "browser_extract_text", "web_fetch"],
                    }
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I cannot confirm from email alone.",
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        prompt = retry_history[-1]["content"][0]["text"]
        visible_names = {definition["name"] for definition in retry_registry.list_tool_definitions()}
        calls.append(visible_names)
        assert {"browser_navigate", "browser_extract_text", "web_fetch"}.issubset(visible_names)
        assert url in prompt
        assert "resolve the relevant link" in prompt
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "browser-1",
                    "browser_navigate",
                    "completed",
                    {"url": url, "title": "Hoffman Management", "text": "300 W 55th St, New York, NY"},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="The office is 300 W 55th St, New York, NY.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=None,
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"browser_navigate", "browser_extract_text", "web_fetch"}]
    assert result.final_text == "The office is 300 W 55th St, New York, NY."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {
        "browser_navigate",
        "browser_extract_text",
        "web_fetch",
    }


def test_chat_operator_connector_skill_fallback_does_not_rebuild_accepted_skill(monkeypatch, tmp_path: Path) -> None:
    from nullion.runtime import PersistentRuntime
    from nullion.runtime_store import RuntimeStore

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    monkeypatch.setattr(
        chat_operator,
        "_active_connector_app_id_providers_for_message",
        lambda: ({"active_app_ids": ("google-drive",)},),
    )
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
    )
    runtime.create_skill(
        title="Google Drive connector workflow",
        summary="Reuse the Google Drive connector route.",
        trigger="Use for Google Drive requests.",
        steps=["Request the connector route and verify the answer."],
        tags=["builder", "connector", "workflow", "connector-app:google-drive"],
    )

    reply = chat_operator._connector_scope_builder_fallback(
        [ToolResult("scope", "request_tool_scope", "completed", {
            "capabilities": ["connector"], "connector_app_ids": ["google-drive"],
        })],
        runtime=runtime,
    )

    assert reply is not None
    assert "saved Google Drive connector skill" in reply
    assert "without rebuilding it" in reply
    assert runtime.list_pending_builder_proposals() == []


def test_chat_operator_account_read_direct_fallback_does_not_pick_tool_from_reply_text() -> None:
    registry = ToolRegistry()
    invocations: list[str] = []
    for spec in (
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="calendar_list",
            description="List calendar events",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: (
                invocations.append(invocation.tool_name)
                or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {})
            ),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Which source should I check?\n\n1. Email\n2. Calendar",
    )
    retry_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="",
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
        run_turn=lambda _history, _registry: retry_result,
    )

    assert retried is True
    assert result is retry_result
    assert retry_registry is not registry
    assert invocations == []


def test_chat_operator_account_read_direct_fallback_uses_structured_scope_tool() -> None:
    registry = ToolRegistry()
    invocations: list[str] = []
    for spec in (
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
        ),
        ToolSpec(
            name="calendar_list",
            description="List calendar events",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: (
                invocations.append(invocation.tool_name)
                or ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {"results": [{"id": "msg-1", "subject": "Structured scope"}]},
                )
            ),
        )
    registry.turn_tool_scope_decision = chat_operator.TurnToolScopeDecision(
        requested_tool_names=("email_search",),
        required_tool_names=("email_search",),
        confidence=1.0,
        valid=True,
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Which source should I check?\n\n1. Email\n2. Calendar",
    )
    retry_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="",
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
        run_turn=lambda _history, _registry: retry_result,
    )

    assert retried is True
    assert invocations == ["email_search"]
    assert result.tool_results[0].tool_name == "email_search"
    assert "email_search" in {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_chat_operator_account_read_retry_rejects_structured_calendar_tool_without_connector_flag(
    monkeypatch,
) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="calendar_list",
            description="List calendar events",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(
        user_message="calendario?",
        conversation_result=None,
    )
    active_registry = chat_operator.scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="calendario?",
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I cannot access that account source from here.",
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            requested_tool_names=("calendar_list",),
            required_tool_names=("calendar_list",),
            confidence=0.94,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "calendar-1",
                    "calendar_list",
                    "completed",
                    {"results": [{"summary": "Lunch", "id": "evt-1"}]},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Calendar checked.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_account_read_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="calendario?",
        evidence=evidence,
        run_turn=run_turn,
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is active_registry
    assert calls == []


def test_chat_operator_model_tool_request_tag_retries_with_calendar_tool() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="calendar_list",
            description="List calendar events",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_request scope="connector" source_user_requested="true" '
            'connector_app_ids="google-calendar" />Which calendar source should I check?'
        ),
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Structured tool-request recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "calendar-1",
                    "calendar_list",
                    "completed",
                    {"results": [{"summary": "Dentist", "id": "evt-1"}]},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Calendar checked.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="calendario?", conversation_result=None),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"calendar_list"}]
    assert result.final_text == "Calendar checked."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"calendar_list"}


def test_chat_operator_model_tool_request_json_body_retries_with_calendar_tool() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="calendar_list",
            description="List calendar events",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_request>{"tool":"request_tool_scope","data":{"tool_scope":"connector",'
            '"source_user_requested":true,"connector_app_ids":["google-calendar"]}}</tool_request>\n'
            "I need scoped account access."
        ),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=chat_operator.build_turn_tool_evidence(user_message="calendario?", conversation_result=None),
        run_turn=lambda _history, _registry: SimpleNamespace(
            tool_results=[
                ToolResult(
                    "calendar-1",
                    "calendar_list",
                    "completed",
                    {"results": [{"summary": "Dentist", "id": "evt-1"}]},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Calendar checked.",
        ),
    )

    assert retried is True
    assert result.final_text == "Calendar checked."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"calendar_list"}


def test_chat_operator_model_tool_request_direct_json_body_retries_with_named_tool() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_request>{"tool":"weather_forecast","data":{"location":"10024, New York","days":1}}</tool_request>'
            "I need live weather."
        ),
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "weather_forecast" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "weather-1",
                    "weather_forecast",
                    "completed",
                    {"temperature_f": 72, "summary": "Clear"},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Weather checked.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"weather_forecast"}]
    assert result.final_text == "Weather checked."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"weather_forecast"}


def test_chat_operator_model_tool_retry_preserves_active_required_source_contract() -> None:
    registry = ToolRegistry()
    for name in (
        "weather_forecast",
        "market_quote",
        "browser_navigate",
        "browser_extract_text",
        "file_write",
    ):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    active_decision = TurnToolScopeDecision(
        web_action="live_research",
        requested_tool_names=(
            "weather_forecast",
            "market_quote",
            "browser_navigate",
            "browser_extract_text",
            "file_write",
        ),
        required_tool_names=("weather_forecast", "browser_navigate", "file_write"),
        requested_artifact_extensions=(".html",),
        confidence=1.0,
        valid=True,
    )
    active_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(requested_extensions=(".html",)),
        tool_scope_decision=active_decision,
    )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("weather-1", "weather_forecast", "failed", {}, error="missing location")
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_request capabilities="market_data" tools="market_quote">'
            '{"tool":"market_quote","data":{"symbols":["SPY"]}}'
            "</tool_request>"
        ),
    )
    seen: dict[str, object] = {}

    def run_turn(_history, retry_registry):  # noqa: ANN001
        seen["names"] = {
            definition["name"] for definition in retry_registry.list_tool_definitions()
        }
        seen["decision"] = retry_registry.turn_tool_scope_decision
        return SimpleNamespace(
            tool_results=[],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Still working.",
        )

    _result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(requested_extensions=(".html",)),
        run_turn=run_turn,
    )

    assert retried is True
    assert seen["names"] == {
        "market_quote",
        "weather_forecast",
        "browser_navigate",
        "file_write",
    }
    retry_decision = seen["decision"]
    assert set(retry_decision.required_tool_names) == {
        "market_quote",
        "weather_forecast",
        "browser_navigate",
        "file_write",
    }
    assert retry_decision.web_action == "live_research"
    assert retry_decision.requested_artifact_extensions == (".html",)
    assert {
        definition["name"] for definition in retry_registry.list_tool_definitions()
    } == seen["names"]


def test_completed_artifact_lock_waits_for_all_scoped_required_tools() -> None:
    results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {
                "required_tool_names": ["weather_forecast", "browser_navigate", "file_write"]
            },
        ),
        ToolResult("weather-1", "weather_forecast", "failed", {}, error="missing location"),
        ToolResult(
            "file-1",
            "file_write",
            "completed",
            {"artifact_paths": ["/workspace/briefing.html"]},
        ),
    ]

    assert chat_operator._turn_still_needs_scoped_required_tools(results) is True

    results.extend(
        [
            ToolResult("weather-2", "weather_forecast", "completed", {"current": {}}),
            ToolResult("browser-1", "browser_navigate", "completed", {"url": "https://example.test"}),
        ]
    )
    assert chat_operator._turn_still_needs_scoped_required_tools(results) is False


def test_artifact_browser_followup_extracts_page_directly_from_session() -> None:
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            "browser_extract_text",
            "Read current page",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            5,
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"text": "Official museum hours", "url": "https://example.test/museum"},
            )
        ),
    )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "browser-1",
                "browser_navigate",
                "completed",
                {"session_id": "session-1", "url": "https://example.test/museum"},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )

    result = chat_operator._direct_browser_page_read_after_open(
        initial_result=initial_result,
        registry=registry,
        principal_id="workspace_admin",
    )

    assert result is not None
    assert result.status == "completed"
    assert calls == [{"session_id": "session-1"}]


def test_required_attachment_repair_does_not_run_before_required_sources_complete() -> None:
    results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {"required_tool_names": ["weather_forecast", "file_write"]},
        ),
        ToolResult("weather-1", "weather_forecast", "failed", {}, error="missing location"),
    ]

    assert chat_operator._needs_required_attachment_repair(
        SimpleNamespace(),
        conversation_id="web:1",
        prompt="structured artifact request",
        reply="Location is required.",
        tool_results=results,
        artifact_paths=[],
        principal_id="workspace_admin",
        required_attachment_extensions=(".html",),
        required_embedded_media_extensions=(),
        source_attachment_paths=(),
    ) is False


def test_chat_operator_loose_tool_use_json_body_runs_safe_read_directly(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "location": {"name": invocation.arguments["location_text"]},
                    "current": {"temperature_2m": 76, "apparent_temperature": 74},
                    "daily": [
                        {
                            "temperature_max_f": 82,
                            "temperature_min_f": 68,
                            "summary": "clouds",
                            "precipitation_probability_max": 20,
                        }
                    ],
                },
            )
        ),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_use tool_name="weather_forecast" tool_call_id="toolu_1"وا۾json\n'
            '{"location":"116 west 87th st, NYC","days":1}\n'
            "</tool_use>I’m checking the live weather."
        ),
    )

    def forbidden_retry_model(_history, _registry):  # noqa: ANN001
        raise AssertionError("loose tool_use JSON should run directly")

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        runtime=runtime,
        model_client=None,
        principal_id="workspace_admin",
        user_message="opaque read request",
        evidence=TurnToolEvidence(),
        run_turn=forbidden_retry_model,
    )

    assert retried is True
    assert calls == [
        {
            "location_text": "116 west 87th st, NYC",
            "forecast_days": 1,
            "timezone": chat_operator._current_user_timezone_name(),
            "include_hourly": False,
        }
    ]
    assert [tool.tool_name for tool in result.tool_results] == ["weather_forecast"]
    assert "76°F" in result.final_text
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"weather_forecast"}


def test_scoped_safe_read_direct_weather_uses_structured_arguments(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "location": {"name": "10024, New York"},
                    "current": {"temperature_2m": 72, "apparent_temperature": 70},
                    "daily": [
                        {
                            "temperature_max_f": 81,
                            "temperature_min_f": 64,
                            "summary": "clear",
                            "precipitation_probability_max": 5,
                        }
                    ],
                },
            )
        ),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            requested_tool_names=("weather_forecast",),
            required_tool_names=("weather_forecast",),
            confidence=1.0,
            valid=True,
        ),
    )

    class Planner:
        def create(self, **kwargs):  # noqa: ANN003
            assert kwargs["tools"] == []
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "use_direct": True,
                                "arguments": {
                                    "location_text": "10024, New York",
                                    "forecast_days": 1,
                                    "timezone": "America/New_York",
                                },
                            }
                        ),
                    }
                ]
            }

    result = chat_operator._run_scoped_safe_read_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        model_client=Planner(),
        principal_id="workspace_admin",
        turn_id="turn-1",
        user_message="weather request in a non-English phrasing",
    )

    assert result is not None
    assert calls == [{"location_text": "10024, New York", "forecast_days": 1, "timezone": "America/New_York", "include_hourly": False}]
    assert [tool.tool_name for tool in result.tool_results] == ["weather_forecast"]
    assert "72°F" in result.final_text


@pytest.mark.parametrize('prompt_text', ['Give me ten current headlines.', 'Dame diez noticias actuales.', '最新のニュースを十件教えて。'])
def test_scoped_safe_read_timeout_does_not_substitute_profile_weather(monkeypatch, tmp_path: Path, prompt_text: str) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "location": {"name": invocation.arguments["location_text"]},
                    "current": {"temperature_2m": 75, "apparent_temperature": 74},
                    "daily": [
                        {
                            "temperature_max_f": 83,
                            "temperature_min_f": 67,
                            "summary": "sunny",
                            "precipitation_probability_max": 0,
                        }
                    ],
                },
            )
        ),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            requested_tool_names=("weather_forecast",),
            required_tool_names=("weather_forecast",),
            confidence=1.0,
            valid=True,
        ),
    )
    monkeypatch.setattr(chat_operator, "_profile_default_location_text", lambda: "10024, New York")
    monkeypatch.setattr(chat_operator, "_current_user_timezone_name", lambda: "America/New_York")

    class StalledPlanner:
        def create(self, **kwargs):  # noqa: ANN003
            raise TimeoutError("planner unavailable")

    result = chat_operator._run_scoped_safe_read_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        model_client=StalledPlanner(),
        principal_id="workspace_admin",
        turn_id="turn-1",
        user_message=prompt_text,
    )

    assert result is None
    assert calls == []


def test_direct_open_url_uses_browser_navigate_without_agent_loop(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="Open a URL",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"url": invocation.arguments["url"], "session_id": "agent-browser-default"},
            )
        ),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="open_url",
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_open_url_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        principal_id="workspace_admin",
        turn_id="turn-1",
        user_message="https://example.com/path keep it open",
    )

    assert result is not None
    assert calls == [{"url": "https://example.com/path"}]
    assert [tool.tool_name for tool in result.tool_results] == ["browser_navigate"]
    assert result.final_text == "Opened https://example.com/path."


def test_direct_open_url_finishes_without_automatic_read_verification(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[str] = []
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="Open a URL",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
            continuation_tools=("browser_extract_text",),
        ),
        lambda invocation: (
            calls.append(invocation.tool_name)
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"url": invocation.arguments["url"], "session_id": "agent-browser-default"},
            )
        ),
    )
    registry.register(
        ToolSpec(
            name="browser_extract_text",
            description="Extract visible page text",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: (
            calls.append(invocation.tool_name)
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "text": (
                        "sample-team/sample-app Pull requests 2 Open "
                        "Fix browser evidence retry #123 opened by sample-user "
                        "Update docs #124 open"
                    ),
                    "session_id": "agent-browser-default",
                },
            )
        ),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="open_url",
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_open_url_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        base_registry=registry,
        principal_id="workspace_admin",
        turn_id="turn-1",
        user_message="Open https://github.com/sample-team/sample-app",
    )

    assert result is not None
    assert calls == ["browser_navigate"]
    assert [tool.tool_name for tool in result.tool_results] == ["browser_navigate"]
    assert "sample-team/sample-app" in result.final_text
    assert result.final_text == "Opened https://github.com/sample-team/sample-app."
    assert "Live-search blocker" not in result.final_text


def test_direct_open_url_allows_typed_validation_tools_without_agent_loop(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="Open a URL",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"url": invocation.arguments["url"], "session_id": "agent-browser-default"},
            )
        ),
    )
    registry.register(
        ToolSpec(
            name="browser_wait_for",
            description="Wait for a loaded page state",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="open_url",
            requested_tool_names=("browser_navigate", "browser_wait_for"),
            required_tool_names=("browser_navigate",),
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_open_url_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        principal_id="workspace_admin",
        turn_id="turn-1",
        user_message="https://example.com/path",
    )

    assert result is not None
    assert calls == [{"url": "https://example.com/path"}]
    assert [tool.tool_name for tool in result.tool_results] == ["browser_navigate"]
    assert result.final_text == "Opened https://example.com/path."


def test_direct_open_url_allows_narrow_browser_interaction_navigation(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="Open a URL",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"url": invocation.arguments["url"], "session_id": "agent-browser-default"},
            )
        ),
    )
    registry.register(
        ToolSpec(
            name="browser_wait_for",
            description="Wait for a loaded page state",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="browser_interaction",
            requested_tool_names=("browser_navigate", "browser_wait_for"),
            required_tool_names=("browser_navigate",),
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_open_url_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        principal_id="workspace_admin",
        turn_id="turn-1",
        user_message="https://example.com/path",
    )

    assert result is not None
    assert calls == [{"url": "https://example.com/path"}]
    assert [tool.tool_name for tool in result.tool_results] == ["browser_navigate"]
    assert result.final_text == "Opened https://example.com/path."


def test_direct_open_url_rejects_broad_browser_interaction_without_typed_navigation(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="Open a URL",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="browser_interaction",
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_open_url_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        principal_id="workspace_admin",
        turn_id="turn-1",
        user_message="https://example.com/path",
    )

    assert result is None


def test_direct_browser_screenshot_defers_without_a_verified_url_target(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    artifact = tmp_path / "cart.png"
    calls: list[dict[str, object]] = []
    registry = ToolRegistry()

    def screenshot_handler(invocation):  # noqa: ANN001
        calls.append(dict(invocation.arguments))
        artifact.write_bytes(b"png bytes")
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "path": str(artifact),
                "artifact_path": str(artifact),
                "artifact_paths": [str(artifact)],
                "page_url": "https://example.com/cart",
            },
        )

    registry.register(
        ToolSpec(
            name="browser_screenshot",
            description="Capture a screenshot",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        screenshot_handler,
    )
    registry.register(
        ToolSpec(
            name="browser_snapshot",
            description="Inspect the browser",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(requested_extensions=(".png",)),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="browser_interaction",
            requested_tool_names=("browser_screenshot",),
            required_tool_names=("browser_screenshot",),
            requested_artifact_extensions=(".png",),
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_browser_screenshot_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        principal_id="workspace_admin",
        turn_id="turn-1",
    )

    assert result is None
    assert calls == []
    assert not artifact.exists()


def test_direct_browser_screenshot_does_not_run_for_requested_only_continuation(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            name="browser_screenshot",
            description="Capture a screenshot",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {})
        ),
    )
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(context_linked=True, prior_tool_scopes=("web",)),
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="browser_interaction",
            requested_tool_names=("browser_screenshot",),
            required_tool_names=(),
            requested_artifact_extensions=(),
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_browser_screenshot_tool_directly(
        runtime=runtime,
        registry=scoped_registry,
        principal_id="workspace_admin",
        turn_id="turn-1",
    )

    assert result is None
    assert calls == []


def test_schema_followup_does_not_expand_completed_requested_url_open() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="Open a URL",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
            continuation_tools=("browser_extract_text",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        ToolSpec(
            name="browser_extract_text",
            description="Extract page text",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("browser",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    evidence = TurnToolEvidence(has_url_target=True)
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=evidence,
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            web_action="browser_interaction",
            requested_tool_names=("browser_navigate",),
            required_tool_names=("browser_navigate",),
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        turn_id="turn-1",
        tool_results=[
            ToolResult(
                "nav-1",
                "browser_navigate",
                "completed",
                {"result": "Navigated to https://example.com/?cart=1 (status 200)"},
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Opened https://example.com/?cart=1.",
    )

    def forbidden_retry(_history, _registry):  # noqa: ANN001
        raise AssertionError("completed URL open must not request browser continuation tools")

    result, retry_registry, retried = chat_operator._run_chat_turn_schema_followup_tool_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        evidence=evidence,
        run_turn=forbidden_retry,
        user_message="Open this page: https://example.com/?cart=1",
    )

    assert result is initial_result
    assert retry_registry is scoped_registry
    assert retried is False


def test_model_tool_request_weather_recovery_invokes_direct_read_without_retry_model(tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "location": {"name": invocation.arguments["location_text"]},
                "current": {"temperature_2m": 73, "apparent_temperature": 71},
                "daily": [
                    {
                        "temperature_max_f": 80,
                        "temperature_min_f": 65,
                        "summary": "partly cloudy",
                        "precipitation_probability_max": 10,
                    }
                ],
            },
        ),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_request>{"tool":"weather_forecast","data":{"location":"10024, New York","days":1}}</tool_request>'
        ),
    )

    def forbidden_retry_model(_history, _registry):  # noqa: ANN001
        raise AssertionError("structured tool-request recovery should invoke the safe read tool directly")

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "weather"}]}],
        active_tool_registry=registry,
        base_tool_registry=registry,
        runtime=runtime,
        model_client=None,
        principal_id="workspace_admin",
        user_message="weather",
        evidence=TurnToolEvidence(),
        run_turn=forbidden_retry_model,
    )

    assert retried is True
    assert [tool.tool_name for tool in result.tool_results] == ["weather_forecast"]
    assert result.tool_results[0].output["location"]["name"] == "10024, New York"
    assert "73°F" in result.final_text
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"weather_forecast"}


def test_no_tool_scope_retry_invokes_safe_read_directly_without_second_model(monkeypatch, tmp_path: Path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "location": {"name": invocation.arguments["location_text"]},
                "current": {"temperature_2m": 74, "apparent_temperature": 72},
                "daily": [
                    {
                        "temperature_max_f": 82,
                        "temperature_min_f": 66,
                        "summary": "sunny",
                        "precipitation_probability_max": 0,
                    }
                ],
            },
        ),
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            requested_tool_names=("weather_forecast",),
            required_tool_names=("weather_forecast",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)

    class Planner:
        def create(self, **kwargs):  # noqa: ANN003
            assert kwargs["tools"] == []
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "use_direct": True,
                                "arguments": {
                                    "location_text": "10024, New York",
                                    "forecast_days": 1,
                                    "timezone": "America/New_York",
                                },
                            }
                        ),
                    }
                ]
            }

    def forbidden_second_model(_history, _registry):  # noqa: ANN001
        raise AssertionError("no-tool recovery should invoke the safe read tool directly")

    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I cannot access live weather from here.",
    )
    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "weather"}]}],
        active_tool_registry=registry,
        base_tool_registry=registry,
        runtime=runtime,
        model_client=Planner(),
        principal_id="workspace_admin",
        user_message="weather",
        evidence=TurnToolEvidence(),
        run_turn=forbidden_second_model,
    )

    assert retried is True
    assert [tool.tool_name for tool in result.tool_results] == ["weather_forecast"]
    assert result.tool_results[0].output["location"]["name"] == "10024, New York"
    assert "74°F" in result.final_text
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"weather_forecast"}


def test_no_tool_scope_retry_skips_when_requested_artifact_already_exists(monkeypatch, tmp_path: Path) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="spreadsheet_create",
            description="Create spreadsheet",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("artifact",),
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    evidence = TurnToolEvidence()
    scoped_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=evidence,
        tool_scope_decision=chat_operator.TurnToolScopeDecision(
            requested_tool_names=("spreadsheet_create",),
            requested_artifact_extensions=(".xlsx",),
            confidence=1.0,
            valid=True,
        ),
    )
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    workbook = artifact_root / "stage_archive_transform_field_notes.xlsx"
    workbook.write_bytes(b"PK\x03\x04xlsx bytes")

    monkeypatch.setattr(chat_operator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(chat_operator, "artifact_root_for_runtime", lambda runtime: artifact_root)

    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "sheet-1",
                "spreadsheet_create",
                "completed",
                {
                    "artifact_path": str(workbook),
                    "artifact_paths": [str(workbook)],
                },
            )
        ],
        artifacts=[str(workbook)],
        suspended_for_approval=False,
        final_text="Done.",
    )

    def forbidden_retry(_history, _registry):  # noqa: ANN001
        raise AssertionError("completed requested artifact must not enter no-tool recovery")

    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        runtime=Runtime(),
        model_client=object(),
        principal_id="telegram:1",
        user_message="opaque artifact request",
        evidence=evidence,
        run_turn=forbidden_retry,
    )

    assert result is initial_result
    assert retry_registry is scoped_registry
    assert retried is False


def test_safe_direct_read_preflight_does_not_choose_between_calendar_and_weather(monkeypatch) -> None:
    registry = ToolRegistry()
    for name, tags in (
        ("calendar_list", ("calendar", "connector", "account_read")),
        ("weather_forecast", ("weather",)),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}, "additionalProperties": True},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    def fail_scope_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("preflight must not classify between multiple safe read tools")

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fail_scope_decision)

    assert (
        chat_operator._safe_direct_read_scope_preflight_registry(
            base_tool_registry=registry,
            evidence=TurnToolEvidence(),
            model_client=object(),
            user_message="opaque read request",
        )
        is None
    )


def test_safe_direct_read_preflight_uses_memory_only_connector_snapshot(monkeypatch) -> None:
    from nullion import connector_prompt_context

    registry = ToolRegistry()
    allow_runtime_load_values: list[bool] = []

    def fake_snapshot(*, allow_runtime_load: bool = True):  # noqa: ANN202
        allow_runtime_load_values.append(allow_runtime_load)
        return (
            {
                "provider_id": "calendar",
                "structured_tools": ("calendar_list",),
                "active_app_ids": ("google-calendar",),
            },
        )

    def fail_scope_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("memory-only connector snapshot should not force a direct-read model decision")

    monkeypatch.setattr(connector_prompt_context, "active_connector_provider_context_snapshot", fake_snapshot)
    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fail_scope_decision)

    assert (
        chat_operator._safe_direct_read_scope_preflight_registry(
            base_tool_registry=registry,
            evidence=TurnToolEvidence(),
            model_client=object(),
            user_message="opaque read request",
        )
        is None
    )
    assert allow_runtime_load_values == [False]


def test_chat_operator_model_tool_call_attribute_body_retries_with_named_tool() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_call name="weather_forecast">'
            '<arg name="location">10024, New York</arg><arg name="days">1</arg>'
            "</tool_call>I need live weather."
        ),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, _registry: SimpleNamespace(
            tool_results=[ToolResult("weather-1", "weather_forecast", "completed", {"temperature_f": 72})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Weather checked.",
        ),
    )

    assert retried is True
    assert result.final_text == "Weather checked."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"weather_forecast"}


def test_chat_operator_model_tool_use_self_closing_retries_with_named_tool() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_use tool_name="weather_forecast" '
            "arguments='{\"location\":\"10024, New York\",\"days\":1}' />"
            "I need live weather."
        ),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, _registry: SimpleNamespace(
            tool_results=[ToolResult("weather-1", "weather_forecast", "completed", {"temperature_f": 72})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Weather checked.",
        ),
    )

    assert retried is True
    assert result.final_text == "Weather checked."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"weather_forecast"}


def test_chat_operator_raw_payload_block_recovers_from_typed_tool_scope_decision() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="image_generate",
            description="Generate image",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("image_generation",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    decision = chat_operator.TurnToolScopeDecision(
        requested_tool_names=("image_generate",),
        required_tool_names=("image_generate",),
        confidence=1.0,
        valid=True,
    )
    active_registry = chat_operator.ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=decision,
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        raw_tool_payload_blocked=True,
        final_text="I blocked a malformed structured response.",
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, _registry: SimpleNamespace(
            tool_results=[ToolResult("image-1", "image_generate", "completed", {"artifact_path": "/tmp/image.png"})],
            artifacts=["/tmp/image.png"],
            suspended_for_approval=False,
            final_text="Done - attached the generated image.",
        ),
    )

    assert retried is True
    assert result.final_text == "Done - attached the generated image."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"image_generate"}


def test_chat_operator_model_tool_use_input_body_retries_with_named_tool() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="weather_forecast",
            description="Fetch weather",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("weather",),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_use name="weather_forecast">'
            '<input>{ "location": "116 west 87th st, NYC", "days": 1 }</input>'
            "</tool_use>I need live weather."
        ),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, _registry: SimpleNamespace(
            tool_results=[ToolResult("weather-1", "weather_forecast", "completed", {"temperature_f": 72})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Weather checked.",
        ),
    )

    assert retried is True
    assert result.final_text == "Weather checked."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"weather_forecast"}


def test_chat_operator_model_tool_request_retries_even_after_context_tool_result() -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="list_crons",
            description="List crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "scheduler_read"),
        ),
        ToolSpec(
            name="toggle_cron",
            description="Toggle cron",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "scheduler_mutate"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"items": [{"id": "cron-1"}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text=(
            '<tool_request>{"tool":"request_tool_scope","data":{"tool_scope":"scheduler",'
            '"source_user_requested":true,"tool_names":["toggle_cron"]}}</tool_request>'
        ),
    )
    calls: list[set[str]] = []

    def run_turn(_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True})],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled the disabled cron.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert calls == [{"list_crons", "toggle_cron"}]
    assert result.final_text == "Enabled the disabled cron."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "toggle_cron"}


def test_chat_operator_no_tool_retry_reports_image_generation_setup_when_tool_absent(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I can't generate the image directly in this chat right now. Here is a prompt you can use.",
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(confidence=0.0, valid=False),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="synthetic visual artifact request",
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("no retry turn expected")),
    )

    assert retried is True
    assert retry_registry is registry
    assert "Image generation is not configured." in result.final_text
    assert "Here is a prompt" not in result.final_text


def test_chat_operator_numbered_no_tool_clarification_retries_structured_scheduler_scope(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="list_crons",
            description="List scheduled tasks",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
        ToolSpec(
            name="toggle_cron",
            description="Enable or disable a scheduled task",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Which scheduler target should I use?\n\n1. Managed scheduled tasks\n2. Local crontab",
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: chat_operator.TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=0.97,
            valid=True,
        ),
    )
    calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Structured no-tool recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult("list-1", "list_crons", "completed", {"jobs": []}),
                ToolResult("toggle-1", "toggle_cron", "completed", {"enabled": True}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled scheduled tasks.",
        )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "solicitud estructurada"}]}],
        active_tool_registry=chat_operator.ScopedTurnToolRegistry(registry,
            evidence=chat_operator.build_turn_tool_evidence(user_message="solicitud estructurada", conversation_result=None),
            tool_scope_decision=chat_operator.TurnToolScopeDecision(valid=True, scheduler_action="mutate")),
        base_tool_registry=registry,
        model_client=object(),
        user_message="solicitud estructurada",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="solicitud estructurada",
            conversation_result=None,
        ),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_no_tool_scope_decision is True
    assert result.result.final_text == "Enabled scheduled tasks."
    assert calls == [{"list_crons", "toggle_cron"}]


def test_chat_operator_no_tool_second_chance_uses_structured_scheduler_scope(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="request_tool_scope",
            description="Request more tools",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
        ),
        ToolSpec(
            name="list_crons",
            description="List scheduled tasks",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
        ToolSpec(
            name="toggle_cron",
            description="Toggle a scheduled task",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need to inspect the current scheduler state first.",
    )

    decisions: list[dict[str, object]] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decisions.append(kwargs)
        return chat_operator.TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=0.96,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "Structured no-tool recovery" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[
                ToolResult("list-1", "list_crons", "completed", {"crons": [{"id": "cron-1", "enabled": False}]}),
                ToolResult("toggle-1", "toggle_cron", "completed", {"id": "cron-1", "enabled": True}),
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="Enabled scheduled tasks.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_no_tool_scope_decision_retry(
        initial_result=initial_result,
        base_history=[{"role": "user", "content": [{"type": "text", "text": "solicitud estructurada 643"}]}],
        active_tool_registry=chat_operator.turn_tool_registry_for_evidence(
            registry,
            evidence=chat_operator.build_turn_tool_evidence(
                user_message="solicitud estructurada 643",
                conversation_result=None,
            ),
            model_client=None,
            user_message="solicitud estructurada 643",
            skip_tool_scope_decision=True,
        ),
        base_tool_registry=registry,
        model_client=object(),
        user_message="solicitud estructurada 643",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="solicitud estructurada 643",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text == "Enabled scheduled tasks."
    assert visible_tool_calls == [{"list_crons", "toggle_cron"}]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons", "toggle_cron"}
    assert decisions
    assert decisions[0]["assistant_no_tool_draft"] == "I need to inspect the current scheduler state first."


def test_chat_operator_missing_scope_retry_wraps_raw_base_registry(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="list_crons",
            description="List scheduled tasks",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
        ToolSpec(
            name="toggle_cron",
            description="Toggle a scheduled task",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need to inspect the current cron list first.",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: chat_operator.TurnToolScopeDecision(valid=False),
    )
    visible_tool_calls: list[set[str]] = []

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        assert "request_tool_scope is registered" in retry_history[-1]["content"][0]["text"]
        return SimpleNamespace(
            tool_results=[ToolResult("scope-1", "request_tool_scope", "completed", {"available_tools": ["list_crons", "toggle_cron"]})],
            artifacts=[],
            suspended_for_approval=False,
            final_text='<tool_request>{"tool":"request_tool_scope","data":{"tool_scope":"scheduler","tool_names":["list_crons","toggle_cron"]}}</tool_request>',
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=chat_operator.PlainNoToolFastPathRegistry(registry),
        base_tool_registry=registry,
        model_client=object(),
        user_message="neutral structured scheduler turn",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="neutral structured scheduler turn",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text.startswith("<tool_request>")
    assert visible_tool_calls == [{"request_tool_scope"}]
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"request_tool_scope"}


def test_chat_operator_local_system_fallback_does_not_retry_after_discovery_matches(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="file_search",
            description="Search local workspace files.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="terminal_exec",
            description="Run a local shell command with approval.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    evidence = chat_operator.build_turn_tool_evidence(user_message="inspect local runtime", conversation_result=None)
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("search-1", "file_search", "completed", {"matches": ["python3", "python3.11"]})
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found 2 files:\n- python3\n- python3.11",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not ask for terminal fallback")),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="inspect local runtime",
        evidence=evidence,
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("must not retry")),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_local_system_fallback_retries_after_local_pdf_read_failure(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="email_attachment_read",
            description="Read an email attachment.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="file_read",
            description="Read a local file.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="terminal_exec",
            description="Run a local shell command with approval.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "attachment-1",
                "email_attachment_read",
                "completed",
                {
                    "filename": "i-751-checklist.pdf",
                    "artifact_path": "/workspace/artifacts/i-751-checklist.pdf",
                    "artifact_paths": ["/workspace/artifacts/i-751-checklist.pdf"],
                },
            ),
            ToolResult(
                "read-1",
                "file_read",
                "failed",
                {
                    "path": "/workspace/artifacts/i-751-checklist.pdf",
                    "type": "pdf",
                    "media_type": "application/pdf",
                },
                error="Could not extract PDF text.",
            ),
        ],
        artifacts=["/workspace/artifacts/i-751-checklist.pdf"],
        suspended_for_approval=False,
        final_text="I could not extract the checklist text.",
    )

    def forbidden_scope_decision(**_kwargs):  # noqa: ANN003
        raise AssertionError("local read failures should expose terminal_exec without another scope guess")

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", forbidden_scope_decision)

    def run_turn(retry_history, retry_registry):  # noqa: ANN001
        prompt = retry_history[-1]["content"][0]["text"]
        assert "local file/document extraction" in prompt
        assert "connected-account uploads, downloads, or external writes" in prompt
        assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"terminal_exec"}
        return SimpleNamespace(
            tool_results=[
                ToolResult("exec-1", "terminal_exec", "completed", {"stdout": "Document Checklist for Joint I-751"})
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="The checklist text was extracted.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="compare the attachment checklist against my docs",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="compare the attachment checklist against my docs",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is True
    assert result.final_text == "The checklist text was extracted."
    assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"terminal_exec"}


def test_chat_operator_local_system_fallback_preserves_discovery_after_empty_terminal_retry(
    monkeypatch,
) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="file_search",
            description="Search local workspace files.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="file_read",
            description="Read a local file.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="terminal_exec",
            description="Run a local shell command with approval.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult(
                "search-1",
                "file_search",
                "completed",
                {"matches": ["/workspace/artifacts/stage_file_search_delivery_guard.pdf"]},
            ),
            ToolResult(
                "read-1",
                "file_read",
                "failed",
                {
                    "path": "/workspace/artifacts/stage_file_search_delivery_guard.pdf",
                    "type": "pdf",
                    "media_type": "application/pdf",
                },
                error="Could not extract PDF text.",
            ),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found 1 file:\n- stage_file_search_delivery_guard.pdf",
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("read failures use direct terminal fallback")),
    )

    def run_turn(_retry_history, _retry_registry):  # noqa: ANN001
        return SimpleNamespace(
            tool_results=[
                ToolResult(
                    "exec-1",
                    "terminal_exec",
                    "completed",
                    {"stdout": "", "stderr": "", "exit_code": 0},
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
            final_text="I ran the local command.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="find the pdf in my workspace",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="find the pdf in my workspace",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_local_system_fallback_does_not_retry_read_failure_after_write_tool(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="file_write",
            description="Write a local file.",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=True,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="file_read",
            description="Read a local file.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="terminal_exec",
            description="Run a local shell command with approval.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            ToolResult("write-1", "file_write", "completed", {"path": "/workspace/generated.pdf"}),
            ToolResult(
                "read-1",
                "file_read",
                "failed",
                {"path": "/workspace/generated.pdf", "type": "pdf", "media_type": "application/pdf"},
            ),
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Could not read the generated file.",
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not ask for terminal fallback")),
    )

    result, retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="check generated PDF",
        evidence=chat_operator.build_turn_tool_evidence(user_message="check generated PDF", conversation_result=None),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("must not retry")),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_local_system_fallback_does_not_retry_when_scope_declines_terminal(monkeypatch) -> None:
    registry = ToolRegistry()
    for spec in (
        ToolSpec(
            name="file_search",
            description="Search local workspace files.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        ToolSpec(
            name="terminal_exec",
            description="Run a local shell command with approval.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
        ),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    initial_result = SimpleNamespace(
        tool_results=[
            {
                "tool_name": "file_search",
                "status": "completed",
                "output": {"matches": ["notes.txt", "todo.txt"]},
            }
        ],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I found 2 files:\n- notes.txt\n- todo.txt",
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(
            requested_tool_names=("file_read",),
            required_tool_names=(),
            confidence=0.9,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)

    result, retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="show matching files",
        evidence=chat_operator.build_turn_tool_evidence(user_message="show matching files", conversation_result=None),
        run_turn=lambda _history, _registry: (_ for _ in ()).throw(AssertionError("must not retry")),
    )

    assert retried is False
    assert result is initial_result
    assert retry_registry is registry


def test_chat_operator_local_system_fallback_does_not_override_declined_scope(monkeypatch) -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="terminal_exec",
            description="Run a local shell command with approval.",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial_result = SimpleNamespace(
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
        final_text="I need approval to run the local terminal diagnostic before I can verify it.",
    )

    def fake_build_decision(**kwargs):  # noqa: ANN003
        assert kwargs["force_model_decision"] is True
        return chat_operator.TurnToolScopeDecision(valid=True, confidence=0.4)

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    visible_tool_calls: list[set[str]] = []

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            tool_results=[],
            artifacts=[],
            suspended_for_approval=True,
            approval_id="approval-1",
            final_text="Approval requested.",
        )

    result, retry_registry, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque local system request 771",
        evidence=chat_operator.build_turn_tool_evidence(
            user_message="opaque local system request 771",
            conversation_result=None,
        ),
        run_turn=run_turn,
    )

    assert retried is False
    assert result is initial_result
    assert visible_tool_calls == []
    assert retry_registry is registry


def test_chat_operator_saved_history_reference_context_does_not_select_multiple_targets() -> None:
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

    assert chat_operator._saved_history_reference_context_message(prompt) is None


def test_orchestrator_transport_timeout_does_not_plain_chat_fallback(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "timeout-runtime.db")
    events: list[dict] = []

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
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

    def forbidden_fallback(*args, **kwargs):
        raise AssertionError("transport timeouts must not run the plain no-tools fallback")

    monkeypatch.setattr(chat_operator, "generate_chat_reply", forbidden_fallback)

    class TimeoutOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):
            raise TimeoutError("provider read timed out")

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat structured local file task",
        chat_id="123",
        settings=settings,
        agent_orchestrator=TimeoutOrchestrator(),
        append_activity_trace=True,
        activity_callback=events.append,
    )

    assert "timed out" in reply
    assert "No tool result was produced" in reply
    assert "currently have access" not in reply
    assert any(event.get("status") == "blocked" for event in events)


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
        prompt="artifact=report.txt",
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


def test_resumed_turn_limit_persistence_failure_stays_visible_and_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nullion.agent_orchestrator import TurnResult

    runtime = bootstrap_persistent_runtime(tmp_path / "resume-limit-persistence-failure.db")
    runtime.active_tool_registry = ToolRegistry()
    conversation_id = "telegram:123"
    approval_id = "ap-resume-limit"
    runtime.store.add_approval_request(
        ApprovalRequest(
            approval_id=approval_id,
            requested_by=conversation_id,
            action=chat_operator.AGENT_TURN_LIMIT_EXTENSION_ACTION,
            resource=conversation_id,
            status=ApprovalStatus.APPROVED,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            request_kind=chat_operator.AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
            context={},
        )
    )
    suspended_turn = SuspendedTurn(
        approval_id=approval_id,
        conversation_id=conversation_id,
        chat_id="123",
        message="/chat compare the two current products",
        request_id="req-resume-limit",
        message_id="msg-resume-limit",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        messages_snapshot=[
            {
                "role": "user",
                "content": [{"type": "text", "text": "compare the two current products"}],
            }
        ],
        pending_tool_calls=[],
    )
    runtime.store.add_suspended_turn(suspended_turn)
    frame = TaskFrame(
        frame_id="frame-resume-limit",
        conversation_id=conversation_id,
        branch_id="branch-resume-limit",
        source_turn_id="turn-source",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.FETCH_RESOURCE,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="compare two current products",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id(conversation_id, frame.frame_id)

    monkeypatch.setattr(
        chat_operator,
        "_telegram_screenshot_reply_if_requested",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        chat_operator,
        "_store_agent_turn_limit_extension_suspended_turn",
        lambda *args, **kwargs: None,
    )

    class ResumeOrchestrator:
        model_client = object()

        def resume_turn(self, **kwargs):  # noqa: ANN003
            return TurnResult(
                turn_id="turn-resume-limit",
                final_text="Product B — $25.00",
                tool_results=[
                    ToolResult(
                        "failed-file-write",
                        "file_write",
                        "failed",
                        {"reason": "disk_unavailable"},
                        error="disk unavailable",
                    )
                ],
                reached_iteration_limit=True,
                response_fulfilled=False,
            )

    reply = chat_operator.resume_approved_telegram_request(
        runtime,
        approval_id=approval_id,
        chat_id="123",
        agent_orchestrator=ResumeOrchestrator(),
    )

    assert reply is not None
    assert "couldn't save a continuation request" in reply
    assert "Product B" not in reply
    assert runtime.store.get_suspended_turn(approval_id) is None
    active_frame_id = runtime.store.get_active_task_frame_id(conversation_id)
    assert active_frame_id == frame.frame_id
    assert runtime.store.get_task_frame(frame.frame_id).status is TaskFrameStatus.ACTIVE


def test_chat_operator_does_not_gate_chat_on_pending_model_api_action(monkeypatch) -> None:
    runtime = Runtime()
    settings = load_settings(
        env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"}
    )
    runtime.store.actions.append({
        "action_id": "act-model-api",
        "action_type": "inspect",
        "recommendation_code": "model_api_unreachable",
        "status": "pending",
        "summary": "Model API is unreachable",
        "severity": "high",
        "owner": "runtime",
        "reason": None,
        "error": None,
        "source_reason": "probe_failed",
    })

    rendered: list[str] = []

    def render_chat_turn(*args, **kwargs):  # noqa: ANN002, ANN003
        rendered.append(str(kwargs.get("message") or args[1]))
        return "chat rendered"

    monkeypatch.setattr(chat_operator, "_render_chat_turn", render_chat_turn)

    assert chat_operator.handle_chat_operator_message(
        runtime,
        "hello",
        chat_id="123",
        settings=settings,
    ) == "chat rendered"
    assert chat_operator.handle_chat_operator_message(
        runtime,
        "/chat hello",
        chat_id="123",
        settings=settings,
    ) == "chat rendered"

    runtime.store.actions[0]["recommendation_code"] = "model_quota_exhausted"
    assert chat_operator.handle_chat_operator_message(
        runtime,
        "hello",
        chat_id="123",
        settings=settings,
    ) == "chat rendered"
    assert rendered == ["/chat hello", "/chat hello", "/chat hello"]


def test_chat_operator_clears_stale_model_api_action_when_probe_recovers(monkeypatch, tmp_path) -> None:
    from nullion import health_probes

    runtime = bootstrap_persistent_runtime(tmp_path / "model-issue-recovered.db")
    model_client = object()
    runtime.store.add_doctor_action({
        "action_id": "act-model-api",
        "action_type": "inspect",
        "recommendation_code": "model_api_unreachable",
        "status": "pending",
        "summary": "Model API is unreachable",
        "severity": "high",
        "owner": "runtime",
        "reason": None,
        "error": None,
        "source_reason": "probe_failed",
    })

    def fake_make_model_api_probe(client):
        assert client is model_client
        return lambda: SimpleNamespace(ok=True)

    monkeypatch.setattr(health_probes, "make_model_api_probe", fake_make_model_api_probe)

    assert chat_operator._chat_model_issue_reply(runtime, message="hello", model_client=model_client) is None
    [action] = runtime.store.list_doctor_actions()
    assert action["status"] == "cancelled"
    assert "recovered" in str(action["reason"]).lower()


def test_chat_operator_clears_legacy_codex_error_action_when_probe_recovers(monkeypatch, tmp_path) -> None:
    from nullion import health_probes

    runtime = bootstrap_persistent_runtime(tmp_path / "legacy-model-issue-recovered.db")
    model_client = object()
    runtime.store.add_doctor_action({
        "action_id": "act-legacy-codex-error",
        "action_type": "inspect",
        "recommendation_code": "review_doctor_route",
        "status": "pending",
        "summary": "Review the routed health issue and choose the next repair step.",
        "severity": "critical",
        "owner": "runtime",
        "reason": (
            "health_alert routed to doctor;issue_type=error;source=telegram_chat;"
            "detail=nullion.model_clients.ModelClientConfigurationError: "
            "Codex Responses request failed while waiting for model response."
        ),
        "error": None,
    })

    def fake_make_model_api_probe(client):
        assert client is model_client
        return lambda: SimpleNamespace(ok=True)

    monkeypatch.setattr(health_probes, "make_model_api_probe", fake_make_model_api_probe)

    assert chat_operator._chat_model_issue_reply(runtime, message="hello", model_client=model_client) is None
    [action] = runtime.store.list_doctor_actions()
    assert action["status"] == "cancelled"
    assert "recovered" in str(action["reason"]).lower()


def test_recent_model_issue_records_respect_recovery_watermark(tmp_path) -> None:
    import sqlite3

    runtime = bootstrap_persistent_runtime(tmp_path / "model-issue-signal.db")
    runtime.checkpoint()
    checkpoint_path = Path(runtime.checkpoint_path)
    updated = datetime.now(UTC)
    payload = {
        "recommendation_code": "model_api_unreachable",
        "reason": "health_alert routed to doctor;service_id=model_api",
        "error": "Codex Responses request failed",
        "summary": "The AI model API is not reachable.",
    }
    with sqlite3.connect(checkpoint_path) as conn:
        conn.execute(
            """
            INSERT INTO doctor_actions (collection, item_key, payload, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            ("doctor_signals", "signal-model-api", json.dumps(payload), updated.isoformat()),
        )

    key = chat_operator._model_issue_runtime_key(runtime)
    chat_operator._MODEL_ISSUE_RECOVERED_AT_BY_RUNTIME.pop(key, None)
    assert chat_operator._recent_model_issue_doctor_records(runtime)

    chat_operator._MODEL_ISSUE_RECOVERED_AT_BY_RUNTIME[key] = updated + timedelta(seconds=1)
    assert chat_operator._recent_model_issue_doctor_records(runtime) == []
    chat_operator._MODEL_ISSUE_RECOVERED_AT_BY_RUNTIME.pop(key, None)


def test_chat_operator_slash_prefixed_paths_are_normal_chat(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "chat-runtime.db")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )

    assert (
        chat_operator._chat_prompt_for_message("/Users/example/Downloads/synthetic-chat-path.txt")
        == "/Users/example/Downloads/synthetic-chat-path.txt"
    )
    assert chat_operator._chat_prompt_for_message("/tmp/資料.txt") == "/tmp/資料.txt"
    assert chat_operator._chat_prompt_for_message("/status") is None
    assert chat_operator._requested_attachment_extension("/Users/example/Downloads/synthetic-chat-path.txt") is None

    monkeypatch.setattr(chat_operator, "_render_chat_turn", lambda *args, **kwargs: "chat rendered")
    assert chat_operator.handle_chat_operator_message(
        runtime,
        "/Users/example/Downloads/synthetic-chat-path.txt",
        chat_id="123",
        settings=settings,
    ) == "chat rendered"


def test_structured_followup_evidence_ignores_malformed_url_like_tokens() -> None:
    from nullion.turn_relationship_evidence import has_structured_turn_relationship_evidence

    malformed_prior_reply = (
        "I checked the thread.\n"
        "One copied row contained a malformed token prefix[example.com that is not a URL.\n"
        "A new plain follow-up must not crash while scanning this prior text."
    )

    assert extract_url_target(malformed_prior_reply) is None
    assert has_structured_turn_relationship_evidence(malformed_prior_reply) is False


def test_structured_followup_evidence_accepts_filename_extension_signal() -> None:
    from nullion.turn_relationship_evidence import has_structured_turn_relationship_evidence

    assert has_structured_turn_relationship_evidence("create stage_active_related_followup.html")


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
    assert "couldn't attach the requested html attachment" in chat_operator._incomplete_artifact_delivery_reply(
        runtime,
        conversation_id="telegram:123",
        reply="not done",
    )
    assert chat_operator._should_include_conversation_context(
        SimpleNamespace(turn=SimpleNamespace(parent_turn_id="turn-0"), task_frame_continuation=None),
        [{"user": "u", "assistant": "a"}],
    ) is True
    assert chat_operator._should_include_conversation_context(
        SimpleNamespace(turn=SimpleNamespace(parent_turn_id=None, disposition="independent"), task_frame_continuation=None),
        [{"user": "u", "assistant": "a"}],
    ) is True
    connector_registry = SimpleNamespace(
        list_specs=lambda: [
            SimpleNamespace(name="email_search", capability_tags=("email", "connector", "account_read")),
        ]
    )
    assert chat_operator._should_include_conversation_context(
        SimpleNamespace(turn=SimpleNamespace(parent_turn_id=None, disposition="independent"), task_frame_continuation=None),
        [{"user": "run the Provincetown cron", "assistant": "Started."}],
        connector_registry,
    ) is False
    assert chat_operator._conversation_context_turns(
        SimpleNamespace(turn=SimpleNamespace(parent_turn_id=None, disposition="independent"), task_frame_continuation=None),
        [
            {"user": "run the saved recurring job", "assistant": "Started."},
            {"user": "it was delivered", "assistant": "Delivered update."},
        ],
        connector_registry,
    ) == [
        {"user": "run the saved recurring job", "assistant": "Started."},
        {"user": "it was delivered", "assistant": "Delivered update."},
    ]
    assert chat_operator._mission_step_titles(SimpleNamespace(steps=[SimpleNamespace(title=" One "), SimpleNamespace(title="")])) == ["One"]
    assert chat_operator._mini_agent_activity_detail(SimpleNamespace(task_status_detail=" detail\n", task_titles=("A",)), 1) == " detail"
    summary = "\u2192 \u2610 Gather data\n\u2611 Summarize\nplain"
    assert chat_operator._task_titles_from_status_summary(summary) == ["Gather data", "Summarize"]
    assert chat_operator._should_show_reply_label(local_reply=None, prior_user_message="hello") is True

    waiting_frame = chat_operator._planner_clarification_frame(
        conversation_id="telegram:123",
        branch_id="branch-1",
        turn_id="turn-2",
        original_message="/planner Build the requested planner PDF",
        question="Which destination should I use?",
        requested_attachment_extensions=(".pdf",),
    )
    runtime.store.add_task_frame(waiting_frame)
    runtime.store.set_active_task_frame_id("telegram:123", waiting_frame.frame_id)
    waiting_result = SimpleNamespace(
        active_task_frame_id=waiting_frame.frame_id,
        task_frame_continuation=SimpleNamespace(mode=SimpleNamespace(value="continue")),
    )
    assert chat_operator._planner_clarification_reply_from_waiting_frame(
        runtime,
        conversation_result=waiting_result,
        prompt="/planner   Build the requested planner PDF",
    ) == "Question: Which destination should I use?"
    assert chat_operator._planner_clarification_reply_from_waiting_frame(
        runtime,
        conversation_result=waiting_result,
        prompt="Use Beacon, NY",
    ) is None
    assert telegram_app._telegram_active_turn_accepts_unstructured_followup(runtime, "telegram:123") is True
    runtime.store.set_active_task_frame_id("telegram:123", None)
    assert telegram_app._telegram_active_turn_accepts_unstructured_followup(runtime, "telegram:123") is False


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


def test_persistent_runtime_can_defer_conversation_checkpoint(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "deferred-checkpoint.db")
    checkpoint_calls = {"count": 0}
    original_checkpoint_store = runtime_module.checkpoint_runtime_store

    def count_checkpoint_store(store, path, **kwargs):
        checkpoint_calls["count"] += 1
        return original_checkpoint_store(store, path, **kwargs)

    monkeypatch.setattr(runtime_module, "checkpoint_runtime_store", count_checkpoint_store)

    result = runtime.process_conversation_message(
        conversation_id="telegram:123",
        chat_id="123",
        user_message="hi",
        checkpoint=False,
    )

    assert result.turn.user_message == "hi"
    assert checkpoint_calls["count"] == 0
    assert runtime.store.list_conversation_events("telegram:123")

    runtime.process_conversation_message(
        conversation_id="telegram:123",
        chat_id="123",
        user_message="persist me",
    )

    assert checkpoint_calls["count"] == 1


def test_persistent_runtime_force_checkpoint_skips_fingerprint_render(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "force-checkpoint.db")
    calls = {"fingerprint": 0}

    def fail_fingerprint(store):  # noqa: ANN001
        calls["fingerprint"] += 1
        raise AssertionError("force checkpoint should not render the full runtime fingerprint")

    monkeypatch.setattr(runtime_module, "render_runtime_store_payload_json", fail_fingerprint)

    runtime.store.add_conversation_ingress_id("telegram:123", "telegram-update:1")
    runtime.checkpoint(force=True)

    assert calls["fingerprint"] == 0


def test_chat_turn_force_checkpoint_skips_plain_no_tool_turn(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_CHAT_FORCE_CHECKPOINT_ON_EVERY_TURN", raising=False)

    assert (
        chat_operator._should_force_checkpoint_after_chat_turn(
            tool_results=[],
            artifact_count=0,
            suspended_for_approval=False,
        )
        is False
    )
    assert (
        chat_operator._should_force_checkpoint_after_chat_turn(
            tool_results=[ToolResult("tool-1", "weather_forecast", "completed", {})],
            artifact_count=0,
            suspended_for_approval=False,
        )
        is False
    )
    assert (
        chat_operator._should_force_checkpoint_after_chat_turn(
            tool_results=[],
            artifact_count=1,
            suspended_for_approval=False,
        )
        is True
    )
    assert (
        chat_operator._should_force_checkpoint_after_chat_turn(
            tool_results=[],
            artifact_count=0,
            suspended_for_approval=True,
        )
        is True
    )


def test_runtime_checkpoint_conversation_events_persists_append_only_rows(tmp_path) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "event-checkpoint.db")
    event = {
        "conversation_id": "web:test",
        "event_type": "conversation.latency_timing",
        "created_at": "2026-06-11T00:00:00+00:00",
        "marks": [{"name": "model_start", "ms": 12.3}],
    }
    runtime.store.add_conversation_event(event)

    runtime.checkpoint_conversation_events()

    with sqlite3.connect(runtime.checkpoint_path) as conn:
        rows = conn.execute(
            "SELECT payload FROM conversation_events WHERE collection='conversation_events'"
        ).fetchall()

    assert rows
    payloads = [json.loads(row[0]) for row in rows]
    assert any(payload.get("conversation_id") == "web:test" for payload in payloads)


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
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
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
        allow_mini_agents=True,
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
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: True)
    monkeypatch.setattr(
        chat_operator,
        "_try_builder_reflection",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("builder should not run for delegated ack")),
    )
    class FakeDagPlanner:
        def __init__(self, *, model_client):
            self.model_client = model_client

        def plan_dag(self, message, *, available_tools):
            return SimpleNamespace(
                can_dispatch=True,
                tasks=[
                    SimpleNamespace(title="Find OpenClaw config files"),
                    SimpleNamespace(title="Audit OpenClaw configuration"),
                ],
            )

    monkeypatch.setattr(chat_operator, "TaskDecomposer", FakeDagPlanner)

    class DelegatingOrchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            assert kwargs["dag_plan"].can_dispatch is True
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
        allow_mini_agents=True,
    )

    assert "Planner: Sequential Mission • 2 tasks" in reply
    assert "☐ Find OpenClaw config files" in reply
    assert "Learned a new skill" not in reply


def test_chat_operator_running_planner_artifact_request_does_not_emit_premature_attachment_failure(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "delegated-artifact-runtime.db")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: True)
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)

    class FakeDagPlanner:
        def __init__(self, *, model_client):
            self.model_client = model_client

        def plan_dag(self, message, *, available_tools):
            return SimpleNamespace(
                can_dispatch=True,
                can_dispatch_when_requested=True,
                needs_clarification=False,
                tasks=[
                    SimpleNamespace(title="Collect records"),
                    SimpleNamespace(title="Create workbook"),
                ],
            )

    monkeypatch.setattr(chat_operator, "TaskDecomposer", FakeDagPlanner)

    class DelegatingOrchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            return SimpleNamespace(
                dispatched=True,
                task_count=2,
                acknowledgment=(
                    "Planner: Sequential Mission • 2 tasks\n"
                    "→ Working on 2 tasks:\n"
                    "  ☐ Collect records\n"
                    "  ☐ Create workbook"
                ),
                planner_summary="Sequential Mission • 2 tasks",
                task_titles=["Collect records", "Create workbook"],
                task_status_detail="  ☐ Collect records\n  ☐ Create workbook",
                group_id="grp-running-artifact",
                status_delivered=True,
            )

        def run_turn(self, **kwargs):
            raise AssertionError("delegated path should not run a direct turn")

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/planner create a workbook named records.xlsx from the available data",
        chat_id="123",
        settings=settings,
        agent_orchestrator=DelegatingOrchestrator(),
        append_activity_trace=False,
        allow_mini_agents=True,
    )

    assert "Planner: Sequential Mission • 2 tasks" in reply
    assert "☐ Create workbook" in reply
    assert "couldn't attach" not in reply.lower()


def test_chat_operator_explicit_planner_dispatches_requested_single_task(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "planner-single-runtime.db")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: True)
    monkeypatch.setattr(chat_operator, "_requested_attachment_extension", lambda prompt, **kwargs: ".pdf")
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)

    class FakeDagPlanner:
        def __init__(self, *, model_client):
            self.model_client = model_client

        def plan_dag(self, message, *, available_tools):
            return SimpleNamespace(
                can_dispatch=False,
                can_dispatch_when_requested=True,
                tasks=[SimpleNamespace(title="Build requested PDF")],
            )

    monkeypatch.setattr(chat_operator, "TaskDecomposer", FakeDagPlanner)

    captured: dict[str, object] = {}

    class DelegatingOrchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                dispatched=True,
                task_count=1,
                acknowledgment=(
                    "Planner: Single Turn • 1 task\n"
                    "For: Build the requested planner PDF\n"
                    "→ Working on 1 task:\n"
                    "  ☐ Build requested PDF"
                ),
                planner_summary="Single Turn • 1 task",
                task_titles=["Build requested PDF"],
                task_status_detail="  ☐ Build requested PDF",
            )

        def run_turn(self, **kwargs):
            raise AssertionError("explicit planner should not fall through to a direct turn")

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/planner Build the requested planner PDF",
        chat_id="123",
        settings=settings,
        agent_orchestrator=DelegatingOrchestrator(),
        append_activity_trace=False,
        allow_mini_agents=True,
    )

    assert "Planner: Single Turn • 1 task" in reply
    assert "☐ Build requested PDF" in reply
    assert captured["user_message"] == "Build the requested planner PDF"
    assert captured["single_task_fast_path"] is False
    assert captured["dag_plan"].can_dispatch_when_requested is True


def test_chat_operator_structured_attachment_request_skips_mini_agent_dispatch(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    runtime = bootstrap_persistent_runtime(tmp_path / "attachment-runtime.db")

    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name, **kwargs: True)
    monkeypatch.setattr(
        chat_operator,
        "TaskPlanner",
        lambda: SimpleNamespace(
            build_execution_plan=lambda **kwargs: SimpleNamespace(
                mission=SimpleNamespace(steps=[SimpleNamespace(title="Find"), SimpleNamespace(title="Build")]),
                can_dispatch_mini_agents=True,
                can_run_mission=False,
            )
        ),
    )
    monkeypatch.setattr(
        chat_operator,
        "_requested_attachment_extension",
        lambda prompt, **kwargs: ".xlsx",
    )
    monkeypatch.setattr(chat_operator, "_needs_required_attachment_repair", lambda *args, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)

    class AttachmentOrchestrator:
        model_client = object()

        def __init__(self) -> None:
            self.run_calls = 0

        def dispatch_request_sync(self, **kwargs):
            raise AssertionError("attachment deliverable should stay on the direct artifact path")

        def run_turn(self, **kwargs):
            self.run_calls += 1
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[],
                final_text="Direct artifact turn",
                artifacts=[],
                thinking_text="",
                reached_iteration_limit=False,
            )

    orchestrator = AttachmentOrchestrator()
    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat find similar products and return the planned workbook",
        chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    assert reply == "Direct artifact turn"
    assert orchestrator.run_calls == 1


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
    assert "Installed dependency cards" in (chat_operator._chat_capability_inventory_prompt(runtime) or "")

    chat_operator._NEWLY_LEARNED_SKILLS[runtime] = ["One", "Two"]
    assert "Learned 2 new skills" in (chat_operator._pop_learned_skills_notification(runtime) or "")
    chat_operator._NEWLY_LEARNED_SKILLS[runtime] = {"telegram:123": ["Later skill"]}
    assert chat_operator._append_runtime_nudges(
        runtime,
        prompt="next unrelated prompt",
        reply="normal reply",
        conversation_id="telegram:123",
    ) == "normal reply"
    assert runtime not in chat_operator._NEWLY_LEARNED_SKILLS
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

        def accept_stored_builder_skill_proposal_result(self, proposal_id, actor):
            return SimpleNamespace(
                skill=SimpleNamespace(title="Auto skill", skill_id="skill-auto"),
                created=True,
                merged_duplicate=False,
            )

    auto_runtime = AutoRuntime()
    chat_operator._auto_accept_proposal(auto_runtime, proposal, source="test")
    assert chat_operator._NEWLY_LEARNED_SKILLS[auto_runtime] == ["Auto skill"]

    class DuplicateRuntime:
        def store_builder_proposal(self, proposal, actor):
            return SimpleNamespace(status="pending", proposal_id="prop-duplicate")

        def accept_stored_builder_skill_proposal_result(self, proposal_id, actor):
            return SimpleNamespace(
                skill=SimpleNamespace(title="Auto skill", skill_id="skill-auto"),
                created=False,
                merged_duplicate=True,
            )

    duplicate_runtime = DuplicateRuntime()
    chat_operator._NEWLY_LEARNED_SKILLS.pop(duplicate_runtime, None)
    chat_operator._auto_accept_proposal(duplicate_runtime, proposal, source="test")
    assert duplicate_runtime not in chat_operator._NEWLY_LEARNED_SKILLS

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
    assert chat_operator.handle_chat_operator_message(
        runtime,
        "approve",
        chat_id="123",
        settings=settings,
    ) == "chat rendered"


def test_chat_capability_inventory_directs_scope_request_before_unavailable_reply(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "scope-contract-runtime.db")
    registry = ToolRegistry()
    for name, tags in (
        ("request_tool_scope", ("tool_scope",)),
        ("weather_forecast", ("weather",)),
        ("market_quote", ("market_data", "quote")),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    monkeypatch.setattr(type(runtime), "active_tool_registry", property(lambda self: registry))
    monkeypatch.setattr(
        "nullion.system_context.build_system_context_snapshot",
        lambda tool_registry=None: SimpleNamespace(snapshot=True),
    )
    monkeypatch.setattr(
        "nullion.system_context.format_compact_system_context_for_prompt",
        lambda snapshot: "Visible tool: request_tool_scope",
    )
    monkeypatch.setattr("nullion.web_research_policy.format_web_research_guidance", lambda **kwargs: "")
    monkeypatch.setattr("nullion.builder_capabilities.format_installed_dependency_context", lambda runtime: "")
    snapshot_load_flags = []
    monkeypatch.setattr(
        chat_operator,
        "active_connector_provider_context_snapshot",
        lambda *, allow_runtime_load=True: snapshot_load_flags.append(allow_runtime_load) or (),
    )

    prompt = chat_operator._chat_capability_inventory_prompt(runtime)

    assert prompt is not None
    assert "request_tool_scope" in prompt
    assert '"capabilities":["weather"],"tool_names":["weather_forecast"]' in prompt
    assert '"capabilities":["market_data"],"tool_names":["market_quote"]' in prompt
    assert "copy its scope_request exactly" in prompt
    assert "Do not tell the user you cannot use a browser" in prompt
    assert "calendar" in prompt
    assert "email" in prompt
    assert "shell" in prompt
    assert "request the matching scope first" in prompt
    assert "discover active authorized sources" in prompt
    assert "source_selection_required" in prompt
    assert "choose from numbered source options" in prompt
    assert "Do not pick email" in prompt
    assert "Answer the user's actual question first" in prompt
    assert "do not substitute a list of matching messages" in prompt
    assert "search results, or connector rows for the answer" in prompt
    assert "attempt the browser/web tool path before substituting a rough estimate" in prompt
    assert snapshot_load_flags == [False]


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
    assert telegram_app._extract_safe_alternatives("I can't do that\n- Summarize the policy\n1. Draft a safe note") == []
    assert telegram_app._extract_safe_alternatives("Sure, I can help") == []


def test_telegram_local_bot_api_transport_configuration(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_TELEGRAM_LOCAL_BOT_API_URL", "http://127.0.0.1:8081/")
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_API_BASE_URL", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_API_FILE_BASE_URL", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_API_LOCAL_MODE", raising=False)

    assert telegram_transport.telegram_bot_api_endpoint_kwargs() == {
        "base_url": "http://127.0.0.1:8081/bot",
        "base_file_url": "http://127.0.0.1:8081/file/bot",
        "local_mode": True,
    }
    assert telegram_transport.telegram_uses_local_bot_api() is True

    class FakeBot:
        def __init__(self, token, **kwargs):
            self.token = token
            self.kwargs = kwargs

    bot = telegram_transport.build_telegram_bot(FakeBot, "123:token")
    assert bot.token == "123:token"
    assert bot.kwargs["base_url"] == "http://127.0.0.1:8081/bot"
    assert bot.kwargs["base_file_url"] == "http://127.0.0.1:8081/file/bot"
    assert bot.kwargs["local_mode"] is True

    class FakeBuilder:
        def __init__(self):
            self.calls = []

        def base_url(self, value):
            self.calls.append(("base_url", value))
            return self

        def base_file_url(self, value):
            self.calls.append(("base_file_url", value))
            return self

        def local_mode(self, value):
            self.calls.append(("local_mode", value))
            return self

        def connect_timeout(self, value):
            self.calls.append(("connect_timeout", value))
            return self

    builder = FakeBuilder()
    assert telegram_transport.configure_telegram_application_builder(builder) is builder
    assert ("base_url", "http://127.0.0.1:8081/bot") in builder.calls
    assert ("base_file_url", "http://127.0.0.1:8081/file/bot") in builder.calls
    assert ("local_mode", True) in builder.calls


def test_telegram_cards_and_decision_actions(tmp_path, monkeypatch) -> None:
    runtime = Runtime()
    service = SimpleNamespace(runtime=runtime, model_client=None)

    assert isinstance(telegram_app.build_telegram_bot_commands(), list)
    assert telegram_app._get_telegram_channel_label(None, "123") == "Telegram \u00b7 123"
    assert telegram_app._telegram_attachment_caption_kwargs(None) == (None, {}, False)
    long_caption, kwargs, too_long = telegram_app._telegram_attachment_caption_kwargs("x" * 1200)
    assert long_caption is None and kwargs == {} and too_long is True
    assert telegram_app._should_disable_web_preview("Opened https://example.com") is True

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
        "🚫 🛡️ Denied web access: `web_fetch`. I'll stop here.\n\nRequest:\n`web_fetch`",
    )
    assert runtime.denied == ["operator:ap-deny"]

    runtime.store.actions = [{"action_id": "doc-1", "status": "pending", "summary": "Ready"}]
    runtime.start_doctor_action = lambda action_id: {"action_id": action_id, "status": "running", "summary": "Started"}
    runtime.complete_doctor_action = lambda action_id: {"action_id": action_id, "status": "completed"}
    runtime.cancel_doctor_action = lambda action_id, reason: {"action_id": action_id, "status": "cancelled", "reason": reason}
    assert telegram_app._execute_decision_action(service, kind="doctor", action="start", record_id="doc-1")[0] == "Started"
    assert telegram_app._execute_decision_action(service, kind="doctor", action="complete", record_id="doc-1") == ("Completed", "Action completed.")
    assert telegram_app._execute_decision_action(service, kind="doctor", action="cancel", record_id="doc-1") == ("Dismissed", "Action dismissed.")

    assert telegram_app._execute_decision_action(service, kind="setting", action="set", record_id="verbose_on", chat_id="123")[0] == "Verbose on"
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
    captured_settings = []
    monkeypatch.setattr(
        "nullion.model_clients.build_model_client_from_settings",
        lambda settings: captured_settings.append(settings) or SimpleNamespace(name=settings.model.openai_model),
    )
    monkeypatch.setattr("nullion.agent_orchestrator.AgentOrchestrator", lambda model_client: SimpleNamespace(model_client=model_client))
    monkeypatch.setattr(
        "nullion.auth.load_stored_credentials",
        lambda: {"keys": {"openrouter": "sk-or-test"}},
    )
    svc.swap_model_client("gpt-new")
    assert svc.model_client.name == "gpt-new"
    svc.swap_provider_model_client("openrouter", "qwen-test")
    assert svc.model_client.name == "qwen-test"
    assert captured_settings[-1].model.openai_api_key == "sk-or-test"
    assert captured_settings[-1].model.openai_base_url == "https://openrouter.ai/api/v1"
    monkeypatch.setattr(
        "nullion.runtime_config.current_runtime_config",
        lambda **kwargs: SimpleNamespace(
            provider="openrouter",
            model="qwen-fresh",
            admin_forced_provider=None,
            admin_forced_model=None,
        ),
    )
    svc.model_client = SimpleNamespace(provider="codex", model="gpt-stale")
    svc.refresh_model_client_from_saved_settings()
    assert svc.model_client.name == "qwen-fresh"
    assert captured_settings[-1].model.openai_api_key == "sk-or-test"

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
    assert telegram_app._new_decision_card(runtime, before, reply="Checked amazon.com") is None
    approval_card = telegram_app._new_decision_card(runtime, before, reply="Tool approval requested: ap-new")
    assert approval_card is not None and "access" in approval_card.text.lower()
    assert telegram_app._existing_pending_approval_card(runtime, "Tool approval requested: ap-new") is not None
    assert telegram_app._existing_pending_approval_card(runtime, "Approval required before Nullion can continue.") is None

    runtime.store.approvals = []
    telegram_app._TELEGRAM_NOTIFIED_BUILDER_PROPOSAL_IDS.clear()
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
    assert proposal_card is not None
    assert proposal_card.supplemental is True
    assert "Coverage helper" in proposal_card.text

    shared_fallback = messaging_adapters._new_builder_proposal_text_fallbacks(runtime, frozenset())
    assert shared_fallback and "Coverage helper" in shared_fallback[0]

    runtime.list_pending_builder_proposals = lambda: []
    runtime.store.actions = [{"action_id": "doc-new", "status": telegram_app.DOCTOR_ACTION_PENDING, "severity": "medium", "summary": "Check health"}]
    doctor_card = telegram_app._new_decision_card(runtime, before)
    assert doctor_card is not None and doctor_card.supplemental is True

    runtime.store.reminders = {
        "rem-new": SimpleNamespace(task_id="rem-new", text="stand up", due_at=datetime(2026, 1, 1, tzinfo=UTC))
    }
    runtime.store.get_reminder = lambda task_id: runtime.store.reminders.get(task_id)
    reminder_card = telegram_app._check_new_reminder_card(runtime, frozenset())
    assert reminder_card is not None and "stand up" in reminder_card.text
    primary_card, supplemental_card = telegram_app._promote_reminder_card_to_primary(None, reminder_card)
    assert primary_card is reminder_card
    assert supplemental_card is None
    primary_card, supplemental_card = telegram_app._promote_reminder_card_to_primary(approval_card, reminder_card)
    assert primary_card is approval_card
    assert supplemental_card is reminder_card

    alternatives = telegram_app._extract_safe_alternatives("I won't do that\n- Write a summary")
    markup = telegram_app._build_suggestion_markup(alternatives)
    if telegram_app.InlineKeyboardButton is not None:
        assert markup is None

    class EditOnly:
        def __init__(self) -> None:
            self.edits = []

        async def edit_text(self, text, **kwargs):
            self.edits.append((text, kwargs))

    message = EditOnly()
    monkeypatch.setattr(
        telegram_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, principal_id=None, allow_attachments=True, **kwargs: SimpleNamespace(
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
        sent_operator_messages.append((bot_token, chat_id, text, kwargs))
        return None

    async def fake_send_or_edit_status(bot, status_messages, **kwargs):
        status_calls.append(kwargs)

    monkeypatch.setattr(telegram_app, "_send_operator_telegram_message", fake_send_operator_message, raising=False)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_status_message", fake_send_or_edit_status, raising=False)
    monkeypatch.setattr(type(svc), "register_handlers", lambda self, app: setattr(app, "registered", True))
    async def monitor_noop():
        return None

    async def register_commands_noop(bot):
        return False

    async def send_message_noop(*args, **kwargs):
        return None

    async def reminder_loop_noop(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(
        type(svc),
        "_build_health_monitor",
        lambda self, app: SimpleNamespace(start=monitor_noop, stop=monitor_noop),
    )
    monkeypatch.setattr(telegram_app, "register_telegram_bot_commands", register_commands_noop)
    monkeypatch.setattr("nullion.reminder_delivery.run_reminder_delivery_loop", reminder_loop_noop)
    monkeypatch.delenv("NULLION_ENV_FILE", raising=False)
    monkeypatch.setenv("NULLION_TELEGRAM_CONFIG_POLL_SECONDS", "0.25")

    class App:
        def __init__(self) -> None:
            self.bot = SimpleNamespace(token="123456:abcdefghijklmnopqrstuvwxyz", send_message=send_message_noop)
            self.ran = False
            self.stopped = False

        def add_handler(self, handler):
            pass

        def post_init(self, fn):
            self.post_init_fn = fn

        def post_shutdown(self, fn):
            self.post_shutdown_fn = fn

        def run_polling(self, **kwargs):
            self.ran = True
            self.kwargs = kwargs

        def stop_running(self):
            self.stopped = True

    app = App()
    returned = svc.run_polling(application=app)
    assert returned is app and app.ran is True
    deliver_holder["fn"]("telegram:123", "progress", is_status=True, group_id="g", status_kind="progress_note")
    deliver_holder["fn"]("telegram:123", "status", is_status=True, group_id="g", status_kind="task_summary")
    deliver_holder["fn"]("telegram:123", "hello")

    def refresh_live_config_for_test(self, *, force=False):
        self.bot_token = "654321:abcdefghijklmnopqrstuvwxyz"
        return True

    monkeypatch.setattr(type(svc), "refresh_live_configuration", refresh_live_config_for_test)

    async def run_lifecycle() -> None:
        await app.post_init(app)
        await asyncio.sleep(0.35)
        assert app.stopped is True
        deliver_holder["fn"]("telegram:123", "/tmp/report.pdf", is_artifact=True)
        await app.post_shutdown(app)

    asyncio.run(run_lifecycle())
    assert not any(call.get("text") == "progress" for call in status_calls)
    assert (
        "123456:abcdefghijklmnopqrstuvwxyz",
        "123",
        "hello",
        {"principal_id": "telegram_chat"},
    ) in sent_operator_messages
    assert (
        "654321:abcdefghijklmnopqrstuvwxyz",
        "123",
        "MEDIA:/tmp/report.pdf",
        {"principal_id": "telegram_chat"},
    ) in sent_operator_messages


def test_telegram_refresh_busy_ack_and_registration_helpers(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "refresh-runtime.db")
    assert telegram_app._refresh_runtime_from_checkpoint(SimpleNamespace(checkpoint_path=None)) is False
    assert telegram_app._refresh_runtime_from_checkpoint(SimpleNamespace(checkpoint_path=tmp_path / "missing.json")) is False
    checkpoint = tmp_path / "runtime.json"
    checkpoint.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(telegram_app, "load_runtime_store", lambda path: SimpleNamespace(loaded=True))
    holder = SimpleNamespace(
        checkpoint_path=checkpoint,
        store=None,
        last_checkpoint_fingerprint="stale",
        last_checkpoint_file_signature=None,
    )
    assert telegram_app._refresh_runtime_from_checkpoint(holder) is True
    assert holder.last_checkpoint_fingerprint is None
    monkeypatch.setattr(telegram_app, "load_runtime_store", lambda path: (_ for _ in ()).throw(RuntimeError("bad")))
    assert telegram_app._refresh_runtime_from_checkpoint(holder) is False

    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    assert telegram_app._busy_chat_ack_text(None, chat_id="123", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("/status", chat_id="123", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("thanks", chat_id="123", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("do useful work", chat_id="999", settings=settings) is None
    assert telegram_app._busy_chat_ack_text("do useful work", chat_id="123", settings=settings) is None
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
    assert waiting_ack in {"Got it — I’ll continue from the current request."}
    assert "parallel" not in waiting_ack.lower()
    foreground_ack = telegram_app._foreground_working_ack_text(
        "add images too",
        chat_id="123",
        settings=settings,
        dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
            dependency_turn_ids=("turn:first",),
        ),
    )
    assert foreground_ack in {"Got it — I’ll continue from the current request."}
    assert foreground_ack != telegram_app.WORKING_ACK_TEXT

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


def test_refresh_runtime_from_checkpoint_signature_cache(tmp_path, monkeypatch) -> None:
    checkpoint = tmp_path / "runtime.json"
    checkpoint.write_text("{}", encoding="utf-8")

    calls: list[int] = [0]

    monkeypatch.setattr(
        telegram_app,
        "load_runtime_store",
        lambda path: calls.__setitem__(0, calls[0] + 1) or SimpleNamespace(loaded=True),
    )
    holder = SimpleNamespace(
        checkpoint_path=checkpoint,
        store=None,
        last_checkpoint_fingerprint="stale",
        last_checkpoint_file_signature=None,
    )
    assert telegram_app._refresh_runtime_from_checkpoint(holder) is True
    assert telegram_app._refresh_runtime_from_checkpoint(holder) is False
    assert calls == [1]
    assert holder.last_checkpoint_fingerprint is None
    assert holder.last_checkpoint_file_signature == telegram_app._checkpoint_file_signature(checkpoint)

    checkpoint.write_text("{\"v\": 1}", encoding="utf-8")
    assert telegram_app._refresh_runtime_from_checkpoint(holder) is True
    assert calls == [2]


def test_cache_runtime_checkpoint_signature_updates_runtime_signature(tmp_path) -> None:
    checkpoint = tmp_path / "runtime.json"
    checkpoint.write_text("{\"v\": 0}", encoding="utf-8")
    runtime = SimpleNamespace(checkpoint_path=checkpoint)
    telegram_app._CHECKPOINT_FILE_SIGNATURES.clear()

    telegram_app._cache_runtime_checkpoint_signature(runtime)
    path_key = str(checkpoint)
    first_signature = telegram_app._CHECKPOINT_FILE_SIGNATURES.get(path_key)
    assert first_signature is not None

    checkpoint.write_text("{\"v\": 1}", encoding="utf-8")
    # Shared container filesystems may coalesce consecutive write timestamps.
    import os
    changed_time = checkpoint.stat().st_mtime_ns + 1_000_000_000
    os.utime(checkpoint, ns=(changed_time, changed_time))
    telegram_app._cache_runtime_checkpoint_signature(runtime)
    second_signature = telegram_app._CHECKPOINT_FILE_SIGNATURES.get(path_key)
    assert second_signature is not None
    assert second_signature != first_signature


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


def test_chat_model_options_do_not_resurface_stale_live_client_model(monkeypatch) -> None:
    monkeypatch.setattr(
        operator_commands,
        "_read_credentials",
        lambda: {
            "provider": "openrouter",
            "keys": {"codex": "oauth-token", "openrouter": "sk-or-test"},
            "models": {"codex": "gpt-5.4", "openrouter": "qwen/qwen3.6-flash"},
            "providers_enabled": {"codex": True, "openrouter": True},
        },
    )

    options = operator_commands.chat_model_options(current_provider="codex", current_model="gpt-5.3")

    assert {"provider": "codex", "model": "gpt-5.4"} in options
    assert {"provider": "codex", "model": "gpt-5.3"} not in options


def test_chat_model_options_credentials_cache_invalidates_on_runtime_db_change(monkeypatch, tmp_path) -> None:
    creds_file = tmp_path / "credentials.json"
    creds_file.write_text('{"provider":"openai","models":{"openai":"gpt"},"keys":{"openai":"ok-key"}}', encoding="utf-8")
    runtime_db = tmp_path / "runtime.db"
    runtime_db.write_text("{}", encoding="utf-8")

    calls: list[dict[str, object]] = []

    import nullion.credential_store

    def _fake_migrate(path, db_path=None):
        calls.append({"path": str(path), "db": str(db_path)})
        return {
            "provider": "openai",
            "models": {"openai": f"gpt-{len(calls)}"},
            "keys": {"openai": "ok-key"},
        }

    monkeypatch.setattr(operator_commands, "_credentials_path", lambda: creds_file)
    monkeypatch.setattr(nullion.credential_store, "migrate_credentials_json_to_db", _fake_migrate)

    operator_commands._CREDENTIALS_CACHE_KEY = None
    operator_commands._CREDENTIALS_CACHE_VALUE = None

    assert {"provider": "openai", "model": "gpt-1"} in operator_commands.chat_model_options(
        current_provider="openai",
        current_model="gpt-1",
    )
    operator_commands.chat_model_options(current_provider="openai", current_model="gpt-1")
    assert len(calls) == 1

    runtime_db.write_text("updated-runtime-db", encoding="utf-8")
    assert {"provider": "openai", "model": "gpt-2"} in operator_commands.chat_model_options(
        current_provider="openai",
        current_model="gpt-2",
    )
    assert len(calls) == 2


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
    assert "💡 Setup tip:" in operator_commands._render_plugin_detail("browser_plugin")
    assert "💡 Setup tip:" in operator_commands._render_skill_pack_detail("nullion/web-research")

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


def test_status_summary_surfaces_pending_mini_agent_runs(tmp_path) -> None:
    from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_mini_agent_run(
        MiniAgentRun(
            "task-pr-readiness",
            "grp-pr-readiness",
            "Check Nullion PR readiness",
            MiniAgentRunStatus.PENDING,
            datetime(2026, 1, 1, tzinfo=UTC),
        )
    )

    rendered = operator_commands.handle_operator_command(runtime, "/status")

    assert "0 active mission(s), 1 active mini-agent(s)" in rendered


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
    streamed = await telegram_app._reply_text_with_streaming_edits(message, "x" * 250, do_quote=False)
    assert streamed.message_id == 44

    attachment = tmp_path / "artifact.txt"
    attachment.write_text("artifact", encoding="utf-8")
    monkeypatch.setattr(
        telegram_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, principal_id=None, allow_attachments=True, **kwargs: SimpleNamespace(
            text="caption",
            attachments=(attachment,),
        ),
    )
    await telegram_app._deliver_reply(message, "MEDIA:" + str(attachment), principal_id="telegram_chat")
    assert message.documents[-1]["data"] == b"artifact"

    monkeypatch.setattr(
        telegram_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, principal_id=None, allow_attachments=True, **kwargs: SimpleNamespace(
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
    assert {attachment["source_kind"] for attachment in attachments} == {"photo", "audio", "document"}

    async def no_sleep(*args, **kwargs):
        return None

    async def fail_get_file():
        raise TimeoutError("telegram file expired")

    monkeypatch.setattr(telegram_app.asyncio, "sleep", no_sleep)
    failed_message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        photo=[],
        audio=None,
        voice=None,
        video=None,
        document=SimpleNamespace(
            file_name="New folder (2).zip",
            mime_type="application/zip",
            get_file=fail_get_file,
        ),
    )
    failed_attachments = await telegram_app._download_telegram_attachments(
        failed_message,
        SimpleNamespace(bot=None),
        settings=None,
    )
    assert failed_attachments == [
        {
            "name": "New folder (2).zip",
            "path": "",
            "media_type": "application/zip",
            "source_kind": "document",
            "download_status": "download_failed",
            "error": "download_failed",
            "detail": "telegram file expired",
            "attempts": "3",
        }
    ]
    assert telegram_app._telegram_unavailable_attachment_reply(failed_attachments) is None

    class BadRequest(Exception):
        pass

    monkeypatch.delenv("NULLION_TELEGRAM_LOCAL_BOT_API_URL", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_API_URL", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_API_BASE_URL", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_API_FILE_BASE_URL", raising=False)
    monkeypatch.delenv("NULLION_TELEGRAM_BOT_API_LOCAL_MODE", raising=False)

    too_big_calls = 0

    async def too_big_get_file():
        nonlocal too_big_calls
        too_big_calls += 1
        raise AssertionError("hosted Bot API oversized metadata should skip get_file")

    too_big_message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        photo=[],
        audio=None,
        voice=None,
        video=None,
        document=SimpleNamespace(
            file_name="New folder (2).zip",
            file_size=62000000,
            mime_type="application/zip",
            get_file=too_big_get_file,
        ),
    )
    too_big_attachments = await telegram_app._download_telegram_attachments(
        too_big_message,
        SimpleNamespace(bot=None),
        settings=None,
    )
    assert too_big_calls == 0
    assert too_big_attachments == [
        {
            "name": "New folder (2).zip",
            "path": "",
            "media_type": "application/zip",
            "source_kind": "document",
            "download_status": "file_too_large",
            "error": "file_too_large",
            "detail": "File is too big for Telegram hosted Bot API download",
            "attempts": "0",
            "bytes": "62000000",
        }
    ]
    too_big_reply = telegram_app._telegram_unavailable_attachment_reply(too_big_attachments)
    assert too_big_reply is not None
    assert "New folder (2).zip" in too_big_reply
    assert "Telegram hosted Bot API" in too_big_reply
    assert "62000000 bytes" in too_big_reply
    assert "20 MB" in too_big_reply
    assert "local Telegram Bot API server" in too_big_reply

    local_calls = 0

    class LocalFile:
        async def download_as_bytearray(self):
            return bytearray(b"zip-bytes")

    async def local_get_file():
        nonlocal local_calls
        local_calls += 1
        return LocalFile()

    monkeypatch.setenv("NULLION_TELEGRAM_LOCAL_BOT_API_URL", "http://127.0.0.1:8081")
    local_message = SimpleNamespace(
        chat=SimpleNamespace(id=123),
        photo=[],
        audio=None,
        voice=None,
        video=None,
        document=SimpleNamespace(
            file_name="New folder (2).zip",
            file_size=62000000,
            mime_type="application/zip",
            get_file=local_get_file,
        ),
    )
    local_attachments = await telegram_app._download_telegram_attachments(
        local_message,
        SimpleNamespace(bot=None),
        settings=None,
    )
    assert local_calls == 1
    assert len(local_attachments) == 1
    assert local_attachments[0]["name"] == "New folder (2).zip"
    assert local_attachments[0]["source_kind"] == "document"
    assert local_attachments[0]["path"]
    assert saved[-1]["filename"] == "New folder (2).zip"
    assert saved[-1]["data"] == b"zip-bytes"

    class VoiceRegistry:
        def __init__(self) -> None:
            self.invocations = []

        def get_spec(self, name):
            assert name == "audio_transcribe"
            return object()

        def invoke(self, invocation):
            self.invocations.append(invocation)
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"text": "check the latest PR status"},
            )

    voice_path = tmp_path / "telegram-voice.ogg"
    voice_path.write_bytes(b"voice")
    voice_audio = {
        "name": "telegram-voice.ogg",
        "path": str(voice_path),
        "media_type": "audio/ogg",
        "source_kind": "voice",
    }
    voice_registry = VoiceRegistry()
    assert (
        telegram_app._voice_instruction_transcript(
            [voice_audio],
            registry=voice_registry,
            principal_id="telegram:123",
        )
        == "check the latest PR status"
    )
    assert voice_registry.invocations[0].arguments == {"path": str(voice_path)}
    assert voice_registry.invocations[0].principal_id == "telegram:123"
    uploaded_audio = {**voice_audio, "source_kind": "audio"}
    assert telegram_app._voice_instruction_transcript([uploaded_audio], registry=voice_registry) is None
    assert telegram_app._voice_instruction_transcript([voice_audio, uploaded_audio], registry=voice_registry) is None

    health_runtime = SimpleNamespace(issues=[], report_health_issue=lambda **kwargs: health_runtime.issues.append(kwargs))
    async def ok_action(action):
        return None

    await telegram_app._send_typing_indicator(SimpleNamespace(chat=SimpleNamespace(id=1, send_action=ok_action)), runtime=health_runtime, text="hi")

    async def failing_action(action):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await telegram_app._send_typing_indicator(SimpleNamespace(chat=SimpleNamespace(id=1, send_action=failing_action)), runtime=health_runtime, text="hi")
    assert health_runtime.issues
    assert health_runtime.issues[-1]["issue_type"] is telegram_app.HealthIssueType.DEGRADED
    assert "RuntimeError: boom" in health_runtime.issues[-1]["details"]["detail"]
    assert telegram_app._should_quote_reply("/chat hi") is False
    assert telegram_app._should_quote_reply("plain") is True
    assert telegram_app._telegram_tool_event_should_emit_working_ack({"id": "tool-1", "tool_name": "weather_forecast"}) is False
    assert telegram_app._telegram_tool_event_should_emit_working_ack({"id": "tool-1", "tool_name": "run_cron"}) is False
    assert telegram_app._telegram_tool_event_should_emit_working_ack({"id": "mini-agents"}) is False
    assert telegram_app._telegram_tool_event_should_emit_working_ack({"id": "foreground-working-ack"}) is True
    assert telegram_app._telegram_tool_event_should_emit_working_ack({"id": "phase-check-attachments", "label": "Checking attachments"}) is False
    activity_runtime = SimpleNamespace(
        store=SimpleNamespace(
            list_conversation_events=lambda conversation_id: [
                {"event_type": "conversation.session_settings", "activity_trace_enabled": True}
            ]
        )
    )
    assert telegram_app._should_live_stream_telegram_activity(
        activity_runtime,
        chat_id="123",
        message=message,
        text="/planner Build a PDF itinerary for this weekend.",
        planner_status_requested=True,
    ) is True

    streamer = telegram_app._TelegramActivityStreamer(message)
    await streamer._update({"id": "orchestrate", "label": "Run", "status": "running", "detail": "Tools:\n  web"})
    await streamer._update({"id": "tool-1", "label": "web_fetch", "status": "done", "detail": "example.com"})
    assert "Activity" in streamer._render()
    assert streamer._detail_is_activity_sublist("\u2192 task") is True
    assert await streamer.stop() is True
    streamer.emit({"id": "tool-2", "label": "browser_navigate", "status": "done"})
    await asyncio.sleep(0)
    assert "browser_navigate" not in streamer._render()
    assert "Stopped by /stop" in streamer._render()

    live_tool_streamer = telegram_app._TelegramActivityStreamer(message)
    await live_tool_streamer._update({"id": "tool-email", "label": "email_search", "status": "running"})
    assert "→ email_search" in live_tool_streamer._render()
    await live_tool_streamer._update({"id": "tool-email", "label": "email_search", "status": "done", "detail": "2 messages"})
    rendered_live_tool = live_tool_streamer._render()
    assert rendered_live_tool.count("email_search") == 1
    assert "✓ email_search — 2 messages" in rendered_live_tool

    select_tools_streamer = telegram_app._TelegramActivityStreamer(message)
    await select_tools_streamer._update({"id": "phase-select-tools", "label": "Selecting tools", "status": "running"})
    assert "→ Selecting tools" in select_tools_streamer._render()

    registry = telegram_app._TelegramActivityRegistry()
    registered_streamer = telegram_app._TelegramActivityStreamer(message)
    await registry.register("telegram:123", registered_streamer)
    assert await registry.stop_conversation("telegram:123") == 1
    registered_streamer.emit({"id": "late", "label": "late activity", "status": "done"})
    await asyncio.sleep(0)
    assert "late activity" not in registered_streamer._render()

    def handle_update_with_activity(
        update,
        *,
        attachments=None,
        text_override=None,
        activity_callback=None,
        append_activity_trace=True,
    ):
        return {
            "attachments": attachments,
            "text_override": text_override,
            "activity_callback": activity_callback,
            "append_activity_trace": append_activity_trace,
        }

    service = SimpleNamespace(handle_update=handle_update_with_activity)
    call_result = telegram_app._call_handle_update_with_activity(
        service,
        object(),
        attachments=[{"x": "y"}],
        text_override="from voice",
    )
    assert call_result["attachments"] == [{"x": "y"}]
    assert call_result["text_override"] == "from voice"

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
    from nullion import platform_chat

    monkeypatch.setattr(
        platform_chat,
        "run_platform_chat_request",
        lambda *args, **kwargs: platform_chat.PlatformChatResponse(text="handled"),
    )
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
async def test_telegram_delivery_uploads_bare_media_filename_from_workspace_artifacts(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "nullion_public_awareness_github_traffic_monitor.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<html><body>" + ("traffic " * 20) + "</body></html>", encoding="utf-8")

    class Message:
        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=100001)
            self.message_id = 99
            self.documents: list[dict] = []
            self.replies: list[tuple[str, dict]] = []

        async def reply_document(self, document, **kwargs):
            self.documents.append({"data": document.read(), "kwargs": kwargs})

        async def reply_text(self, text, **kwargs):
            self.replies.append((text, kwargs))

    message = Message()

    await telegram_app._deliver_reply(
        message,
        "Run complete.\n\nMEDIA nullion_public_awareness_github_traffic_monitor.html",
        principal_id="telegram:100001",
    )

    assert message.documents
    assert message.documents[-1]["data"] == artifact.read_bytes()
    assert "Run complete." in message.documents[-1]["kwargs"]["caption"]
    assert message.replies == []


def test_telegram_document_timeout_kwargs_uses_media_timeouts(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_TELEGRAM_READ_TIMEOUT_SECONDS", "3")
    monkeypatch.setenv("NULLION_TELEGRAM_WRITE_TIMEOUT_SECONDS", "4")
    monkeypatch.setenv("NULLION_TELEGRAM_MEDIA_READ_TIMEOUT_SECONDS", "77")
    monkeypatch.setenv("NULLION_TELEGRAM_MEDIA_WRITE_TIMEOUT_SECONDS", "88")

    kwargs = telegram_app._telegram_document_timeout_kwargs()

    assert kwargs["read_timeout"] == 77.0
    assert kwargs["write_timeout"] == 88.0


@pytest.mark.asyncio
async def test_telegram_document_attachment_upload_retries_transient_timeout(tmp_path, monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(telegram_app.asyncio, "sleep", fake_sleep)

    class Message:
        def __init__(self) -> None:
            self.calls = 0
            self.payloads: list[bytes] = []

        async def reply_document(self, document, **kwargs):
            self.calls += 1
            self.payloads.append(document.read())
            if self.calls == 1:
                raise TimeoutError("telegram upload response timed out")
            return object()

    message = Message()
    attachment = tmp_path / "artifact.png"
    attachment.write_bytes(b"image")

    await telegram_app._reply_document_attachment(message, attachment, caption="caption")

    assert message.calls == 2
    assert message.payloads == [b"image", b"image"]
    assert sleeps == [0.5]


@pytest.mark.asyncio
async def test_telegram_document_attachment_upload_retries_connect_timeout(tmp_path, monkeypatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(telegram_app.asyncio, "sleep", fake_sleep)

    class TelegramTimedOut(Exception):
        pass

    class ConnectTimeout(Exception):
        pass

    class Message:
        def __init__(self) -> None:
            self.calls = 0
            self.payloads: list[bytes] = []

        async def reply_document(self, document, **kwargs):
            self.calls += 1
            self.payloads.append(document.read())
            if self.calls == 1:
                exc = TelegramTimedOut("timed out before upload")
                exc.__cause__ = ConnectTimeout("connect timed out")
                raise exc
            return object()

    message = Message()
    attachment = tmp_path / "artifact.xlsx"
    attachment.write_bytes(b"workbook")

    await telegram_app._reply_document_attachment(message, attachment, caption="caption")

    assert message.calls == 2
    assert message.payloads == [b"workbook", b"workbook"]
    assert sleeps == [0.5]


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
    download_calls: list[str] = []
    typing_calls: list[str] = []

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
        download_calls.append(str(getattr(message, "text", "") or ""))
        return []

    async def no_typing(message, runtime, text):
        typing_calls.append(str(text or ""))
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
    monkeypatch.setattr(
        telegram_app,
        "_refresh_runtime_from_checkpoint",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fast Telegram commands must not refresh checkpoint")),
    )
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_build_help_menu_card", lambda: telegram_app.DecisionCard("help card", object()))
    monkeypatch.setattr(telegram_app, "_build_models_card", lambda runtime: telegram_app.DecisionCard("models card", object()))
    monkeypatch.setattr(telegram_app, "_build_verbose_settings_card", lambda runtime, chat_id=None: telegram_app.DecisionCard("verbose card", object()))
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", lambda service, update, **kwargs: "I can't do that\n- Safer summary")

    def forbid_fast_command_checkpoint(*args, **kwargs):
        raise AssertionError("read-only fast Telegram commands must not checkpoint before replying")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(type(runtime), "checkpoint", forbid_fast_command_checkpoint)
        await svc.on_message(SimpleNamespace(update_id=1, message=Message("/help")), SimpleNamespace(bot=None))
        await svc.on_message(SimpleNamespace(update_id=2, message=Message("/models")), SimpleNamespace(bot=None))
        await svc.on_message(SimpleNamespace(update_id=3, message=Message("/verbose status")), SimpleNamespace(bot=None))
    assert download_calls == []
    assert typing_calls == []
    await svc.on_message(SimpleNamespace(update_id=4, message=Message("hello")), SimpleNamespace(bot=None))
    assert [item["reply"] for item in deliveries[:3]] == ["help card", "models card", "verbose card"]
    assert deliveries[-1]["additional_markup"] is None
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

    from nullion import platform_chat

    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_callback_follow_up)
    monkeypatch.setattr(
        telegram_app.ChatOperatorService,
        "handle_text_request",
        lambda self, **kwargs: platform_chat.PlatformChatResponse(text="suggestion handled"),
    )
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
    assert any(item.get("decision_card") is not None and item.get("quote_reply") is False for item in deliveries)

    monkeypatch.setattr(telegram_app, "resume_approved_telegram_request", lambda *args, **kwargs: "final resumed answer")
    approval_callback_resume_output = Callback(
        telegram_app._build_callback_data(kind="approval", action="approve", record_id="ap-resume"),
        callback_message,
    )
    await svc.on_callback_query(SimpleNamespace(callback_query=approval_callback_resume_output), SimpleNamespace())
    assert any(item.get("reply") == "final resumed answer" and item.get("quote_reply") is False for item in deliveries)

    monkeypatch.setattr(telegram_app, "resume_approved_telegram_request", lambda *args, **kwargs: None)
    approval_callback_no_output = Callback(
        telegram_app._build_callback_data(kind="approval", action="approve", record_id="ap-resume"),
        callback_message,
    )
    await svc.on_callback_query(SimpleNamespace(callback_query=approval_callback_no_output), SimpleNamespace())
    assert any("resend your message" in item.get("reply", "") and item.get("quote_reply") is False for item in deliveries)

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


@pytest.mark.asyncio
async def test_telegram_new_command_uses_fast_path_without_checkpoint_refresh(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-new-fast-path.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[str] = []

    class Message:
        text = "/new"
        caption = None
        message_id = 43
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")
        chat = SimpleNamespace(id=123, send_action=lambda action: None)

    async def capture_reply(message, reply, **kwargs) -> None:  # noqa: ANN001, ANN003
        deliveries.append(str(reply))

    def fail_checkpoint_refresh(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("/new must not reload the runtime checkpoint before replying")

    def fail_checkpoint(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("/new must not synchronously checkpoint before replying")

    monkeypatch.setattr(telegram_app, "_refresh_runtime_from_checkpoint", fail_checkpoint_refresh)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_reply)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(type(runtime), "checkpoint", fail_checkpoint)
        await svc.on_message(SimpleNamespace(update_id=43, message=Message()), SimpleNamespace(bot=None))

    assert deliveries == ["Starting fresh."]
    assert any(
        event.get("event_type") == "conversation.session_reset"
        for event in runtime.store.list_conversation_events("telegram:123")
    )


@pytest.mark.asyncio
async def test_telegram_streamed_plain_reply_edits_preview_without_duplicate_terminal_message(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_TELEGRAM_CHAT_STREAMING_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-streamed-final.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    primary_deliveries: list[str] = []
    receipts: list[dict[str, object]] = []

    class Message:
        text = "hello"
        caption = None
        message_id = 44
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[str] = []
            self.edits: list[str] = []
            self.edit_kwargs: list[dict[str, object]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append(str(text))
            return SimpleNamespace(message_id=91, edit_text=self.edit_text)

        async def edit_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.edits.append(str(text))
            self.edit_kwargs.append(dict(kwargs))

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    async def capture_primary_delivery(message, reply, **kwargs):  # noqa: ANN001, ANN003
        primary_deliveries.append(str(reply))

    def capture_receipt(message, delivery, **kwargs):  # noqa: ANN001, ANN003
        receipts.append({"message": message, "delivery": delivery, **kwargs})

    def streamed_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        kwargs["text_delta_callback"]("Hi Himan — ")
        return "Hi Himan — how can I help?"

    message = Message()
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_primary_delivery)
    monkeypatch.setattr(telegram_app, "_record_telegram_delivery_receipt", capture_receipt)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", streamed_handler)

    await svc.on_message(SimpleNamespace(update_id=44, message=message), SimpleNamespace(bot=None))

    assert message.replies == ["Hi Himan — "]
    assert message.edits == ["Hi Himan — how can I help?"]
    assert message.edit_kwargs == [{}]
    assert primary_deliveries == []
    assert len(receipts) == 1
    assert receipts[0]["transport_ok"] is True
    assert receipts[0]["delivered_message"].message_id == 91


@pytest.mark.asyncio
async def test_telegram_failed_stream_edit_falls_back_to_primary_delivery(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_TELEGRAM_CHAT_STREAMING_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-streamed-fallback.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    primary_deliveries: list[str] = []

    class Message:
        text = "hello"
        caption = None
        message_id = 45
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            return SimpleNamespace(message_id=92, edit_text=self.edit_text)

        async def edit_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            raise RuntimeError("simulated Telegram edit failure")

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    async def capture_primary_delivery(message, reply, **kwargs):  # noqa: ANN001, ANN003
        primary_deliveries.append(str(reply))

    def streamed_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        kwargs["text_delta_callback"]("Working")
        return "Human-readable final answer."

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_primary_delivery)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", streamed_handler)

    await svc.on_message(SimpleNamespace(update_id=45, message=Message()), SimpleNamespace(bot=None))

    assert primary_deliveries == ["Human-readable final answer."]


@pytest.mark.asyncio
async def test_telegram_streamed_final_formats_markdown_in_existing_preview(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_TELEGRAM_CHAT_STREAMING_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-streamed-formatting.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )

    class Message:
        text = "opaque formatting request"
        caption = None
        message_id = 45
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []
            self.edits: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((str(text), dict(kwargs)))
            return SimpleNamespace(message_id=92, edit_text=self.edit_text)

        async def edit_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.edits.append((str(text), dict(kwargs)))

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    def streamed_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        kwargs["text_delta_callback"]("**Alpha**")
        return "**Alpha** and **Beta**"

    message = Message()
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", streamed_handler)

    await svc.on_message(SimpleNamespace(update_id=45, message=message), SimpleNamespace(bot=None))

    assert message.replies == [("**Alpha**", {"do_quote": True})]
    assert message.edits == [
        ("<b>Alpha</b> and <b>Beta</b>", {"parse_mode": "HTML"}),
    ]


@pytest.mark.asyncio
async def test_telegram_streamed_final_records_receipt_before_slow_activity_finish(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_TELEGRAM_CHAT_STREAMING_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-streamed-receipt-order.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    receipt_recorded = asyncio.Event()
    allow_activity_finish = asyncio.Event()

    class Message:
        text = "opaque request"
        caption = None
        message_id = 46
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            return SimpleNamespace(message_id=93, edit_text=self.edit_text)

        async def edit_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            return None

    class SlowActivityStreamer:
        def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
            pass

        def emit(self, event):  # noqa: ANN001
            return None

        def disable_typing_refresh(self) -> None:
            return None

        async def finish(self) -> None:
            await allow_activity_finish.wait()

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    def streamed_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        kwargs["text_delta_callback"]("Final answer")
        return "Final answer"

    def capture_receipt(message, delivery, **kwargs):  # noqa: ANN001, ANN003
        receipt_recorded.set()

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_should_live_stream_telegram_activity", lambda *args, **kwargs: True)
    monkeypatch.setattr(telegram_app, "_TelegramActivityStreamer", SlowActivityStreamer)
    monkeypatch.setattr(telegram_app, "_record_telegram_delivery_receipt", capture_receipt)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", streamed_handler)

    message = Message()
    task = asyncio.create_task(
        svc.on_message(SimpleNamespace(update_id=46, message=message), SimpleNamespace(bot=None))
    )
    await asyncio.wait_for(receipt_recorded.wait(), timeout=1)
    assert not task.done()
    allow_activity_finish.set()
    await task


@pytest.mark.asyncio
async def test_telegram_on_message_blocks_unauthorized_chat_before_handler_or_tools(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-unauthorized.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )

    class Message:
        text = "write a script and run it"
        caption = None
        message_id = 10
        chat = SimpleNamespace(id=999, send_action=lambda action: None)
        from_user = SimpleNamespace(id=999, first_name="Mallory", last_name="", username=None)

        def __init__(self) -> None:
            self.replies: list[tuple[str, dict]] = []

        async def reply_text(self, text, **kwargs):
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    message = Message()

    async def fail_download(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unauthorized Telegram ingress must not download attachments")

    def fail_health(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unauthorized Telegram ingress must not run model health checks")

    def fail_handler(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unauthorized Telegram ingress must not reach chat handler")

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", fail_download)
    monkeypatch.setattr(telegram_app, "_chat_model_issue_reply", fail_health, raising=False)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", fail_handler)

    await svc.on_message(SimpleNamespace(update_id=1, message=message), SimpleNamespace(bot=None))

    assert message.replies == [("Unauthorized messaging identity.", {"do_quote": False})]
    assert not runtime.store.list_conversation_events("telegram:999")


@pytest.mark.asyncio
async def test_telegram_member_cannot_approve_dangerous_tool_callback(tmp_path, monkeypatch) -> None:
    from nullion import users
    from nullion.users import NullionUser, UserRegistry, save_user_registry

    users_path = tmp_path / "users.json"
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    save_user_registry(
        UserRegistry(
            multi_user_enabled=True,
            users=[
                NullionUser("admin", "Admin", role="admin", telegram_chat_id="123"),
                NullionUser("member", "Member", role="member", telegram_chat_id="456"),
            ],
        ),
        path=users_path,
        settings=settings,
    )
    monkeypatch.setattr(users, "_USERS_PATH", users_path)

    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-member-approval.db")
    runtime.store.add_approval_request(
        ApprovalRequest(
            approval_id="ap-terminal",
            requested_by="user:member",
            action="use_tool",
            resource="terminal_exec",
            status=ApprovalStatus.PENDING,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            context={},
        )
    )
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )

    class Callback:
        data = telegram_app._build_callback_data(kind="approval", action="allow_once", record_id="ap-terminal")
        from_user = SimpleNamespace(id=456)

        def __init__(self) -> None:
            self.message = SimpleNamespace(chat=SimpleNamespace(id=456))
            self.answers: list[tuple[str, dict]] = []

        async def answer(self, text, **kwargs):
            self.answers.append((text, kwargs))

    callback = Callback()
    monkeypatch.setattr(
        telegram_app,
        "_execute_decision_action",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("member must not approve")),
    )

    await svc.on_callback_query(SimpleNamespace(callback_query=callback), SimpleNamespace())

    assert callback.answers == [("Admin approval required", {"show_alert": True})]
    assert runtime.store.get_approval_request("ap-terminal").status is ApprovalStatus.PENDING


@pytest.mark.asyncio
async def test_telegram_on_message_checkpoints_when_chat_saved_ingress(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-dedupe.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    checkpoint_calls = {"count": 0}
    original_checkpoint_store = runtime_module.checkpoint_runtime_store

    class Message:
        text = "hello"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")
        chat = SimpleNamespace(id=123, send_action=lambda action: None)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    async def capture_reply(message, reply, **kwargs) -> None:  # noqa: ANN001, ANN003
        return None

    def count_checkpoint_store(store, path, **kwargs):  # noqa: ANN001
        checkpoint_calls["count"] += 1
        return original_checkpoint_store(store, path, **kwargs)

    def chat_handler_saves_ingress(service, update, **kwargs):  # noqa: ANN001
        assert kwargs["cancellation_checker"] is not None
        request_id = "telegram-update:99"
        conversation_id = "telegram:123"
        service.runtime.store.add_conversation_ingress_id(conversation_id, request_id)
        return "hi"

    monkeypatch.setattr(runtime_module, "checkpoint_runtime_store", count_checkpoint_store)
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_reply)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", chat_handler_saves_ingress)

    await svc.on_message(SimpleNamespace(update_id=99, message=Message()), SimpleNamespace(bot=None))

    assert runtime.store.has_conversation_ingress_id("telegram:123", "telegram-update:99")
    assert checkpoint_calls["count"] >= 1
    loaded = runtime_persistence.load_runtime_store(runtime.checkpoint_path)
    assert loaded.has_conversation_ingress_id("telegram:123", "telegram-update:99")


@pytest.mark.asyncio
async def test_telegram_on_message_caches_handler_checkpoint_signature(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-signature.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    telegram_app._CHECKPOINT_FILE_SIGNATURES.pop(str(runtime.checkpoint_path), None)

    class Message:
        text = "hello"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")
        chat = SimpleNamespace(id=123, send_action=lambda action: None)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    async def capture_reply(message, reply, **kwargs) -> None:  # noqa: ANN001, ANN003
        return None

    def chat_handler_saves_runtime(service, update, **kwargs):  # noqa: ANN001
        request_id = "telegram-update:99"
        conversation_id = "telegram:123"
        service.runtime.store.add_conversation_ingress_id(conversation_id, request_id)
        service.runtime.checkpoint(force=True)
        return "hi"

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_reply)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", chat_handler_saves_runtime)

    await svc.on_message(SimpleNamespace(update_id=99, message=Message()), SimpleNamespace(bot=None))

    assert telegram_app._CHECKPOINT_FILE_SIGNATURES[str(runtime.checkpoint_path)] == telegram_app._checkpoint_file_signature(runtime.checkpoint_path)
    load_calls = {"count": 0}

    def count_load(path):  # noqa: ANN001
        load_calls["count"] += 1
        raise AssertionError("cached own checkpoint should not be reloaded")

    monkeypatch.setattr(telegram_app, "load_runtime_store", count_load)
    assert telegram_app._refresh_runtime_from_checkpoint(runtime) is False
    assert load_calls["count"] == 0


@pytest.mark.asyncio
async def test_telegram_on_message_does_not_gate_chat_on_model_api_action(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-model-gate.db")
    runtime.store.add_doctor_action(
        {
            "action_id": "act-1",
            "owner": "system",
            "status": "pending",
            "action_type": "runtime",
            "recommendation_code": "model_api_unreachable",
            "summary": "Model API is unreachable",
            "severity": "high",
            "reason": None,
            "error": None,
        }
    )
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    delivered: list[dict[str, object]] = []

    class Message:
        def __init__(self, text: str) -> None:
            self.text = text
            self.caption = None
            self.message_id = 21
            self.from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")
            self.chat = SimpleNamespace(id=123, send_action=lambda action: None)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        return None

    async def capture_reply(message, reply, **kwargs) -> None:  # noqa: ANN001, ANN003
        delivered.append({"message": message, "reply": reply, **kwargs})

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_reply)
    monkeypatch.setattr(
        telegram_app,
        "_call_handle_update_with_activity",
        lambda *args, **kwargs: "chat path reached",
    )

    await svc.on_message(SimpleNamespace(update_id=77, message=Message("is it slow right now")), SimpleNamespace(bot=None))

    assert delivered and delivered[0]["reply"] == "chat path reached"


@pytest.mark.asyncio
async def test_telegram_handle_update_only_refreshes_config_once(tmp_path, monkeypatch) -> None:
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-config-refresh.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    refresh_calls: list[tuple[bool]] = []

    def track_refresh(_self, *, force: bool = False) -> bool:
        refresh_calls.append((force,))
        return False

    class Message:
        text = "hello"
        caption = None
        message_id = 101
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            return SimpleNamespace(message_id=77)

        async def edit_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            return None

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(*_args, **_kwargs):
        return None

    monkeypatch.setattr(type(svc), "refresh_live_configuration", track_refresh)
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(
        telegram_app,
        "_call_handle_update_with_activity",
        lambda service, update, **kwargs: "done",
    )

    await svc.on_message(SimpleNamespace(update_id=88, message=Message()), SimpleNamespace(bot=None))

    assert refresh_calls == [(False,)]


@pytest.mark.asyncio
async def test_telegram_refreshes_configuration_without_model_health_gate(tmp_path, monkeypatch) -> None:
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-config-before-health.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    calls: list[str] = []

    def track_refresh(_self, *, force: bool = False) -> bool:
        calls.append("refresh")
        return True

    def track_health(runtime, *, message, model_client):  # noqa: ANN001
        calls.append("health")
        raise AssertionError("Telegram chat ingress must not run a model health precheck")

    class Message:
        text = "hello"
        caption = None
        message_id = 102
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(*_args, **_kwargs):
        return None

    monkeypatch.setattr(type(svc), "refresh_live_configuration", track_refresh)
    monkeypatch.setattr(telegram_app, "_chat_model_issue_reply", track_health, raising=False)
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(
        telegram_app,
        "_call_handle_update_with_activity",
        lambda service, update, **kwargs: "done",
    )

    await svc.on_message(SimpleNamespace(update_id=89, message=Message()), SimpleNamespace(bot=None))

    assert calls == ["refresh"]


@pytest.mark.asyncio
async def test_telegram_media_group_batches_album_into_one_turn(tmp_path, monkeypatch) -> None:
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-media-group.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    monkeypatch.setattr(telegram_app, "_TELEGRAM_MEDIA_GROUP_DEBOUNCE_SECONDS", 0.01)

    class Message:
        media_group_id = "album-1"
        text = None
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username="ada")

        def __init__(self, message_id: int, caption: str | None = None) -> None:
            self.message_id = message_id
            self.caption = caption
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            return SimpleNamespace(message_id=770)

        async def edit_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            return None

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    downloaded_batches: list[list[int]] = []
    captured: list[dict[str, object]] = []
    deliveries: list[str] = []

    async def grouped_download(messages, context, *, settings=None):  # noqa: ANN001, ANN003
        downloaded_batches.append([message.message_id for message in messages])
        return [
            {"path": f"/tmp/media-{message.message_id}.png", "name": f"media-{message.message_id}.png", "media_type": "image/png"}
            for message in messages
        ]

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        captured.append(kwargs)
        return "album handled"

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append(reply)

    monkeypatch.setattr(telegram_app, "_download_telegram_attachment_messages", grouped_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    await asyncio.gather(
        svc.on_message(SimpleNamespace(update_id=21, message=Message(501)), SimpleNamespace(bot=None)),
        svc.on_message(SimpleNamespace(update_id=22, message=Message(502, "create the catalog")), SimpleNamespace(bot=None)),
    )

    assert downloaded_batches == [[501, 502]]
    assert len(captured) == 1
    assert captured[0]["attachments"] == [
        {"path": "/tmp/media-501.png", "name": "media-501.png", "media_type": "image/png"},
        {"path": "/tmp/media-502.png", "name": "media-502.png", "media_type": "image/png"},
    ]
    assert deliveries == ["album handled"]


@pytest.mark.asyncio
async def test_telegram_inflight_plain_turn_does_not_send_working_ack_without_tool_activity(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-no-tool-working-ack.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    svc._inflight_chat_by_id["123"] = 1
    deliveries: list[dict[str, object]] = []

    class Message:
        text = "do useful work"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    def no_tool_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        assert kwargs["activity_callback"] is not None
        kwargs["activity_callback"]({"id": "phase-check-attachments", "label": "Checking attachments", "status": "running"})
        return "Plain final reply"

    message = Message()
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", no_tool_handler)

    await svc.on_message(SimpleNamespace(update_id=99, message=message), SimpleNamespace(bot=None))

    reply_texts = [text for text, _ in message.replies]
    assert "⌛ On it! Feel free to send other tasks — I can handle multiple at once." not in reply_texts
    assert [delivery["reply"] for delivery in deliveries] == ["Plain final reply"]


@pytest.mark.asyncio
async def test_telegram_sends_quoted_working_ack_for_foreground_tool_use_signal(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "false")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
            "NULLION_ACTIVITY_TRACE_ENABLED": "false",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-foreground-tool-use-ack.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []

    class Message:
        text = "neutral tool turn"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    def tool_decision_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        kwargs["activity_callback"]({"id": "foreground-working-ack", "label": "Preparing work", "status": "running"})
        kwargs["activity_callback"]({"id": "tool-1", "tool_name": "calendar_search", "status": "running"})
        return "Calendar checked"

    message = Message()
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", tool_decision_handler)

    await svc.on_message(SimpleNamespace(update_id=99, message=message), SimpleNamespace(bot=None))
    await asyncio.sleep(0)

    assert message.replies == [
        (
            telegram_app.WORKING_ACK_TEXT,
            {"do_quote": True},
        )
    ]
    assert [delivery["reply"] for delivery in deliveries] == ["Calendar checked"]


@pytest.mark.asyncio
async def test_telegram_verbose_off_does_not_send_tool_working_ack_without_activity_trace(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NULLION_ACTIVITY_TRACE_ENABLED", "false")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
            "NULLION_ACTIVITY_TRACE_ENABLED": "false",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-verbose-off-working-ack.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    captured: dict[str, object] = {}

    class Message:
        text = "check my email"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    def tool_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        captured.update(kwargs)
        kwargs["activity_callback"]({"id": "tool-1", "tool_name": "connector_request", "status": "running"})
        return "Email summary"

    message = Message()
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", tool_handler)

    await svc.on_message(SimpleNamespace(update_id=99, message=message), SimpleNamespace(bot=None))
    await asyncio.sleep(0)

    assert captured["activity_callback"] is not None
    assert captured["append_activity_trace"] is False
    assert [text for text, _ in message.replies] == []
    assert [delivery["reply"] for delivery in deliveries] == ["Email summary"]


@pytest.mark.asyncio
async def test_telegram_visible_activity_stream_does_not_send_separate_working_ack(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        }
    )
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-visible-activity-no-working-ack.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []

    class Message:
        text = "remind me at noon to stretch"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001, ANN003
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    def tool_handler(service, update, **kwargs):  # noqa: ANN001, ANN003
        kwargs["activity_callback"]({"id": "tool-reminder", "tool_name": "set_reminder", "status": "done"})
        return "Reminder set"

    message = Message()
    monkeypatch.setattr(telegram_app, "_should_live_stream_telegram_activity", lambda *args, **kwargs: True)
    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", tool_handler)

    await svc.on_message(SimpleNamespace(update_id=99, message=message), SimpleNamespace(bot=None))
    await asyncio.sleep(0)

    reply_texts = [text for text, _ in message.replies]
    assert any("Activity" in text for text in reply_texts)
    assert all("Feel free to send other tasks" not in text for text in reply_texts)
    assert [delivery["reply"] for delivery in deliveries] == ["Reminder set"]


@pytest.mark.asyncio
async def test_telegram_planner_command_delivers_planner_reply_without_old_activity_stream(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_ACTIVITY_TRACE_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-planner.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    status_deliveries: list[dict[str, object]] = []
    captured: dict[str, object] = {}

    class Message:
        text = "/planner Build the requested planner PDF"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    async def capture_status(bot, status_messages, **kwargs):  # noqa: ANN001, ANN003
        status_deliveries.append({"bot": bot, "status_messages": status_messages, **kwargs})
        return True

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        captured.update(kwargs)
        return (
            "Planner: Single Turn • 1 task\n"
            "For: Build the requested planner PDF\n"
            "→ Working on 1 task:\n"
            "  ☐ Build requested PDF"
        )

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", capture_status)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    message = Message()
    await svc.on_message(SimpleNamespace(update_id=99, message=message), SimpleNamespace(bot=None))

    assert captured["activity_callback"] is not None
    assert captured["append_activity_trace"] is False
    assert deliveries == []
    assert len(status_deliveries) == 1
    assert status_deliveries[0]["chat_id"] == "123"
    assert status_deliveries[0]["group_id"]
    assert "🎯 **PLANNER" in status_deliveries[0]["text"]
    assert "SINGLE TURN" in status_deliveries[0]["text"]
    assert "Build the requested planner PDF" in status_deliveries[0]["text"]
    assert [text for text, _ in message.replies] == [
        "Activity\n→ Planner — Building and running the plan",
    ]


@pytest.mark.asyncio
async def test_telegram_planner_command_does_not_trust_unconfirmed_existing_status_card(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_ACTIVITY_TRACE_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-planner-existing-status.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    status_deliveries: list[dict[str, object]] = []

    class Message:
        text = "/planner Build the requested planner PDF"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    async def capture_status(bot, status_messages, **kwargs):  # noqa: ANN001, ANN003
        status_deliveries.append({"bot": bot, "status_messages": status_messages, **kwargs})
        return True

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        reply = (
            "Planner: Single Turn • 1 task\n"
            "For: Build the requested planner PDF\n"
            "→ Working on 1 task:\n"
            "  ☐ Build requested PDF"
        )
        service.runtime.store.add_conversation_event({
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "assistant_reply": reply,
            "tool_results": [
                {"tool_name": "Planner", "status": "completed", "output": {"summary": "Single Turn • 1 task"}},
                {
                    "tool_name": "Mini-Agents",
                    "status": "running",
                    "output": {"status_delivered": True, "tasks": ["Build requested PDF"]},
                },
            ],
        })
        return reply

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", capture_status)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    message = Message()
    await svc.on_message(SimpleNamespace(update_id=100, message=message), SimpleNamespace(bot=None))

    assert len(status_deliveries) == 1
    assert status_deliveries[0]["chat_id"] == "123"
    assert status_deliveries[0]["group_id"]
    assert "🎯 **PLANNER" in status_deliveries[0]["text"]
    assert "SINGLE TURN" in status_deliveries[0]["text"]
    assert "Build the requested planner PDF" in status_deliveries[0]["text"]
    assert deliveries == []
    assert [text for text, _ in message.replies] == [
        "Activity\n→ Planner — Building and running the plan",
    ]


@pytest.mark.asyncio
async def test_telegram_planner_command_does_not_duplicate_confirmed_status_card(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_ACTIVITY_TRACE_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-planner-confirmed-status.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    status_deliveries: list[dict[str, object]] = []

    class Message:
        text = "/planner Build the requested planner PDF"
        caption = None
        message_id = 43
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=89)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    async def capture_status(bot, status_messages, **kwargs):  # noqa: ANN001, ANN003
        status_deliveries.append({"bot": bot, "status_messages": status_messages, **kwargs})
        return True

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        reply = (
            "Planner: Single Turn • 1 task\n"
            "For: Build the requested planner PDF\n"
            "→ Working on 1 task:\n"
            "  ☐ Build requested PDF"
        )
        service.runtime.store.add_conversation_event({
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "assistant_reply": reply,
            "tool_results": [
                {"tool_name": "Planner", "status": "completed", "output": {"summary": "Single Turn • 1 task"}},
                {
                    "tool_name": "Mini-Agents",
                    "status": "running",
                    "output": {
                        "status_delivered": True,
                        "status_delivery_confirmed": True,
                        "tasks": ["Build requested PDF"],
                    },
                },
            ],
        })
        return reply

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", capture_status)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    message = Message()
    await svc.on_message(SimpleNamespace(update_id=101, message=message), SimpleNamespace(bot=None))

    assert status_deliveries == []
    assert deliveries == []
    assert [text for text, _ in message.replies] == [
        "Activity\n→ Planner — Building and running the plan",
    ]


@pytest.mark.asyncio
async def test_telegram_orchestrator_deliver_fn_confirms_task_summary_status(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-deliver-fn-status.db")
    status_calls: list[dict[str, object]] = []

    class Orchestrator:
        deliver_fn = None

        def set_deliver_fn(self, deliver_fn):  # noqa: ANN001
            self.deliver_fn = deliver_fn

    class App:
        def run_polling(self, **kwargs):  # noqa: ANN003
            self.kwargs = kwargs

    async def failing_status_send(bot, status_messages, **kwargs):  # noqa: ANN001, ANN003
        status_calls.append({"bot": bot, "status_messages": status_messages, **kwargs})
        return False

    orchestrator = Orchestrator()
    service = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
        agent_orchestrator=orchestrator,
    )

    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", failing_status_send)

    service.run_polling(application=App())

    assert orchestrator.deliver_fn is not None
    delivered = orchestrator.deliver_fn(
        "telegram:123",
        "Planner: Sequential Mission • 2 tasks\nFor: Build a file\n→ Working on 2 tasks:\n  ☐ Gather\n  ☐ Create",
        is_status=True,
        group_id="grp-confirmed-card",
        status_kind="task_summary",
    )

    assert delivered is False
    assert status_calls
    assert status_calls[0]["chat_id"] == "123"
    assert status_calls[0]["group_id"] == "grp-confirmed-card"
    assert "🎯 **PLANNER" in str(status_calls[0]["text"])


@pytest.mark.asyncio
async def test_telegram_planner_command_suppresses_text_when_matching_planner_card_already_visible(
    tmp_path,
    monkeypatch,
) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_ACTIVITY_TRACE_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-planner-existing-card.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    planner_group_id = "grp-planner-existing-visible"
    svc._status_messages[("123", planner_group_id)] = 77
    svc._status_texts[("123", planner_group_id)] = "🎯 **PLANNER  SINGLE TURN**\n\nFor: Build the requested planner PDF"

    class Message:
        text = "/planner Build the requested planner PDF"
        caption = None
        message_id = 45
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=91)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        reply = (
            "Planner: Single Turn • 1 task\n"
            "For: Build the requested planner PDF\n"
            "→ Working on 1 task - you can send the next request anytime:\n"
            "  ☐ Build requested PDF"
        )
        service.runtime.store.add_conversation_event({
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "assistant_reply": reply,
            "tool_results": [
                {"tool_name": "Planner", "status": "completed", "output": {"summary": "Single Turn • 1 task"}},
                {
                    "tool_name": "Mini-Agents",
                    "status": "running",
                    "output": {"task_group_id": planner_group_id, "status_delivered": True},
                },
            ],
        })
        return reply

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    message = Message()
    await svc.on_message(SimpleNamespace(update_id=103, message=message), SimpleNamespace(bot=None))

    assert deliveries == []
    # The foreground planner-summary text should be suppressed when the planner
    # card for this same task group is already present.
    assert all("Planner: Single Turn" not in text for text, _ in message.replies)


def test_planner_summary_reply_should_be_suppressed_when_card_already_exists() -> None:
    runtime = Runtime()
    reply = (
        "Planner: Single Turn • 1 task\n"
        "For: Build the requested planner PDF\n"
        "→ Working on 1 task - you can send the next request anytime:\n"
        "  ☐ Build requested PDF"
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "assistant_reply": reply,
            "tool_results": [
                {"tool_name": "Planner", "status": "completed", "output": {"summary": "Single Turn • 1 task"}},
                {
                    "tool_name": "Mini-Agents",
                    "status": "running",
                    "output": {"task_group_id": "grp-123"},
                },
            ],
        }
    )
    status_messages = {("123", "grp-123"): 42}
    status_texts = {("123", "grp-123"): "🎯 **PLANNER  SINGLE TURN**\n\nFor: Build the requested planner PDF"}

    assert telegram_app._planner_summary_reply_should_be_suppressed(
        runtime,
        chat_id="123",
        reply=reply,
        status_messages=status_messages,
        status_texts=status_texts,
    ) is True


def test_planner_summary_reply_not_suppressed_without_existing_card() -> None:
    runtime = Runtime()
    reply = (
        "Planner: Parallel Mission • 3 tasks\n"
        "For: compare cnn.com and bbc.com home page news side by side\n"
        "→ Running 3 tasks in parallel - you can send the next request anytime:\n"
        "  ☐ Extract CNN homepage news"
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "assistant_reply": reply,
            "tool_results": [
                {"tool_name": "Planner", "status": "completed", "output": {"summary": "Parallel Mission • 3 tasks"}},
                {
                    "tool_name": "Mini-Agents",
                    "status": "running",
                    "output": {"task_group_id": "grp-abc"},
                },
            ],
        }
    )

    assert telegram_app._planner_summary_reply_should_be_suppressed(
        runtime,
        chat_id="123",
        reply=reply,
        status_messages={},
        status_texts={},
    ) is False


@pytest.mark.asyncio
async def test_telegram_planner_clarification_delivers_planner_card(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_ACTIVITY_TRACE_ENABLED": "true",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-planner-clarification.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    status_deliveries: list[dict[str, object]] = []

    class Message:
        text = "/planner Build the requested planner PDF"
        caption = None
        message_id = 44
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=90)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    async def capture_status(bot, status_messages, **kwargs):  # noqa: ANN001, ANN003
        status_deliveries.append({"bot": bot, "status_messages": status_messages, **kwargs})
        return True

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        return "Question: Which destination should I use?"

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", capture_status)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    message = Message()
    await svc.on_message(SimpleNamespace(update_id=102, message=message), SimpleNamespace(bot=None))

    assert len(status_deliveries) == 1
    assert status_deliveries[0]["chat_id"] == "123"
    assert "🎯 **PLANNER" in status_deliveries[0]["text"]
    assert "WAITING FOR DETAILS" in status_deliveries[0]["text"]
    assert "Build the requested planner PDF" in status_deliveries[0]["text"]
    assert "Which destination should I use?" in status_deliveries[0]["text"]
    assert deliveries == []


@pytest.mark.asyncio
async def test_telegram_deferred_manual_cron_delivers_receipt_without_synthesized_planner_card(tmp_path, monkeypatch) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_ACTIVITY_TRACE_ENABLED": "true",
        "NULLION_TASK_PLANNER_FEED_MODE": "task",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-deferred-cron-planner.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    status_deliveries: list[dict[str, object]] = []

    class Message:
        text = "Trigger my Provincetown tracker"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)
            self.replies: list[tuple[str, dict[str, object]]] = []

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=88)

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    async def capture_status(bot, status_messages, **kwargs):  # noqa: ANN001, ANN003
        status_deliveries.append({"bot": bot, "status_messages": status_messages, **kwargs})
        return True

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        _ = (update, kwargs)
        reply = (
            "Manual scheduled task run started: Provincetown Airbnb Monitor. "
            "The result will be delivered to this chat when ready."
        )
        service.runtime.store.add_conversation_event({
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "assistant_reply": reply,
            "tool_results": [
                {
                    "tool_name": "run_cron",
                    "status": "completed",
                    "output": {
                        "delivery_status": "deferred",
                        "name": "Provincetown Airbnb Monitor",
                        "result_text": reply,
                    },
                }
            ],
        })
        return reply

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", capture_status)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    await svc.on_message(SimpleNamespace(update_id=101, message=Message()), SimpleNamespace(bot=None))

    expected_reply = (
        "Manual scheduled task run started: Provincetown Airbnb Monitor. "
        "The result will be delivered to this chat when ready."
    )
    assert status_deliveries == []
    assert any(item.get("reply") == expected_reply for item in deliveries)


def test_deferred_cron_planner_status_uses_structured_task_group() -> None:
    status_text = (
        "Scheduled task\n"
        "For: Provincetown Airbnb Monitor\n"
        "◐ Running: Manual scheduled task run started.\n"
        "  Result will be delivered to this chat when ready."
    )
    event = {
        "event_type": "conversation.chat_turn",
        "assistant_reply": "Manual scheduled task run started: Provincetown Airbnb Monitor.",
        "tool_results": [
            {
                "tool_name": "run_cron",
                "status": "completed",
                "output": {
                    "delivery_status": "deferred",
                    "mini_agent_dispatch": True,
                    "task_group_id": "cron-d58ac467-inv-run-cron",
                    "progress_status_text": status_text,
                    "name": "Provincetown Airbnb Monitor",
                    "result_text": (
                        "Manual scheduled task run started: Provincetown Airbnb Monitor. "
                        "The result will be delivered to this chat when ready."
                    ),
                },
            }
        ],
    }

    assert telegram_app._chat_turn_deferred_cron_planner_status(event) == (
        "cron-d58ac467-inv-run-cron",
        status_text,
    )


@pytest.mark.asyncio
async def test_telegram_deferred_manual_cron_shows_running_status_and_suppresses_receipt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = load_settings(env={
        "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
        "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
        "NULLION_TELEGRAM_CHAT_ENABLED": "true",
        "NULLION_ACTIVITY_TRACE_ENABLED": "true",
        "NULLION_TASK_PLANNER_FEED_MODE": "task",
    })
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-background-owned-cron.db")
    svc = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token="123456:abcdefghijklmnopqrstuvwxyz",
        operator_chat_id="123",
        settings=settings,
    )
    deliveries: list[dict[str, object]] = []
    status_deliveries: list[dict[str, object]] = []
    reply = (
        "Manual scheduled task run started: Peak Design shipment tracker. "
        "The result will be delivered to this chat when ready."
    )
    status_text = (
        "Scheduled task\n"
        "For: Peak Design shipment tracker\n"
        "◐ Running: Manual scheduled task run started.\n"
        "  Result will be delivered to this chat when ready."
    )

    class Message:
        text = "Check on the peak design package"
        caption = None
        message_id = 42
        from_user = SimpleNamespace(id=123, first_name="Ada", last_name="", username=None)

        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=123, send_action=self.send_action)

        async def send_action(self, action):  # noqa: ANN001
            return None

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            deliveries.append({"reply": text, **kwargs})
            return SimpleNamespace(message_id=88)

    def capture_handle_update(service, update, **kwargs):  # noqa: ANN001, ANN003
        _ = (update, kwargs)
        service.runtime.store.add_conversation_event({
            "conversation_id": "telegram:123",
            "event_type": "conversation.chat_turn",
            "assistant_reply": reply,
            "tool_results": [
                {
                    "tool_name": "run_cron",
                    "status": "completed",
                    "output": {
                        "delivery_status": "deferred",
                        "mini_agent_dispatch": True,
                        "task_group_id": "cron-peak-design-inv-run-cron",
                        "progress_status_text": status_text,
                        "name": "Peak Design shipment tracker",
                        "result_text": reply,
                        "status_delivered": True,
                    },
                }
            ],
        })
        return reply

    async def no_download(message, context, settings=None):  # noqa: ANN001
        return []

    async def no_typing(message, runtime, text):  # noqa: ANN001
        return None

    async def stop_typing(task):  # noqa: ANN001
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def capture_deliver(message, reply, **kwargs):  # noqa: ANN001, ANN003
        deliveries.append({"reply": reply, **kwargs})

    async def capture_status(bot, status_messages, **kwargs):  # noqa: ANN001, ANN003
        status_deliveries.append({"bot": bot, "status_messages": status_messages, **kwargs})
        return True

    monkeypatch.setattr(telegram_app, "_download_telegram_attachments", no_download)
    monkeypatch.setattr(telegram_app, "_run_typing_keepalive", no_typing)
    monkeypatch.setattr(telegram_app, "_stop_typing_keepalive", stop_typing)
    monkeypatch.setattr(telegram_app, "_deliver_reply", capture_deliver)
    monkeypatch.setattr(telegram_app, "_send_or_edit_telegram_task_status_message", capture_status)
    monkeypatch.setattr(telegram_app, "_call_handle_update_with_activity", capture_handle_update)

    assert telegram_app._chat_turn_deferred_cron_planner_status({
        "tool_results": [
            {
                "tool_name": "run_cron",
                "output": {
                    "delivery_status": "deferred",
                    "mini_agent_dispatch": True,
                    "task_group_id": "cron-peak-design-inv-run-cron",
                    "progress_status_text": status_text,
                    "status_delivered": True,
                },
            }
        ]
    }) == ("cron-peak-design-inv-run-cron", status_text)

    await svc.on_message(SimpleNamespace(update_id=101, message=Message()), SimpleNamespace(bot=None))

    assert status_deliveries == []
    assert not any(item.get("reply") == reply for item in deliveries)


def test_telegram_registered_run_cron_returns_deferred_receipt_with_running_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons, telegram_entrypoint

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "123456:telegram-token")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "999")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")

    job = crons.CronJob(
        id="cron-telegram-deferred-card",
        name="Telegram deferred status card cron",
        schedule="0 * * * *",
        task="Return a tiny status message.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="999",
    )
    crons.save_crons([job])

    registry = ToolRegistry()
    completed = threading.Event()
    delivered = threading.Event()
    captured: dict[str, object] = {}
    deliveries: list[dict[str, object]] = []

    class FakeOrchestrator:
        model_client = object()

        def run_turn(self, *args, **kwargs):  # noqa: ANN002, ANN003
            captured["run_turn"] = kwargs
            completed.set()
            return SimpleNamespace(
                final_text="Cron finished.",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    def service_factory(runtime, *, settings):  # noqa: ANN001
        return SimpleNamespace(
            settings=settings,
            model_client=object(),
            agent_orchestrator=FakeOrchestrator(),
            refresh_live_configuration=lambda: None,
        )

    async def fake_send_operator_telegram_delivery(*args, **kwargs):  # noqa: ANN002, ANN003
        deliveries.append({"args": args, "kwargs": kwargs})
        delivered.set()
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

    telegram_entrypoint._build_runtime_service_from_settings(
        checkpoint_path=tmp_path / "runtime.db",
        service_factory=service_factory,
    )

    result = registry.invoke(
        ToolInvocation(
            "inv-telegram-deferred-card",
            "run_cron",
            "telegram:100001",
            {"id": job.id},
        )
    )

    assert result.status == "completed"
    assert result.output["delivery_status"] == "deferred"
    assert completed.wait(2)
    assert "mini_agent_dispatch" not in result.output
    assert result.output["task_group_id"].startswith("manual-cron-")
    assert "◐ Running" in result.output["progress_status_text"]
    assert "planner_status_owned_by_background" not in result.output
    assert result.output.get("foreground_reply_suppressed") is not True
    assert result.output.get("status_delivered") is not True
    assert captured["run_turn"]["conversation_id"] == "telegram:100001"
    assert delivered.wait(2)
    assert deliveries


def test_telegram_registered_run_cron_registers_cancellable_conversation_id(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion import crons, telegram_entrypoint

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home" / ".nullion"))
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "123456:telegram-token")
    monkeypatch.setenv("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "999")
    monkeypatch.setattr("nullion.connections.workspace_id_for_principal", lambda principal: "workspace_admin")
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")

    job = crons.CronJob(
        id="cron-telegram-cancellable",
        name="Telegram cancellable cron",
        schedule="0 * * * *",
        task="Return a tiny status message.",
        workspace_id="workspace_admin",
        delivery_channel="telegram",
        delivery_target="999",
    )
    crons.save_crons([job])

    registry = ToolRegistry()
    captured_start: dict[str, object] = {}

    def service_factory(runtime, *, settings):  # noqa: ANN001
        return SimpleNamespace(
            settings=settings,
            model_client=object(),
            agent_orchestrator=SimpleNamespace(model_client=object()),
            refresh_live_configuration=lambda: None,
        )

    def fake_start_manual_cron_background_delivery(*args, **kwargs):  # noqa: ANN002, ANN003
        captured_start.update(kwargs)
        return {
            "cron_delivery_status": "deferred",
            "task_group_id": "manual-cron-test",
            "progress_status_text": "◐ Running: Manual scheduled task run started.",
        }

    monkeypatch.setattr(telegram_entrypoint, "_terminal_executor_backend_from_settings", lambda settings: object())
    monkeypatch.setattr(telegram_entrypoint, "create_plugin_tool_registry", lambda **kwargs: registry)
    monkeypatch.setattr(telegram_entrypoint, "_validate_plugin_bootstrap_settings", lambda settings: {})
    monkeypatch.setattr(telegram_entrypoint, "_register_enabled_plugins", lambda **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "register_media_plugin", lambda *args, **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "register_reminder_tools", lambda *args, **kwargs: None)
    monkeypatch.setattr(telegram_entrypoint, "_resolve_browser_backend", lambda: None)
    monkeypatch.setattr(telegram_entrypoint, "default_browser_backend_for_web_research", lambda settings: None)
    monkeypatch.setattr(
        "nullion.cron_delivery.start_manual_cron_background_delivery",
        fake_start_manual_cron_background_delivery,
    )

    telegram_entrypoint._build_runtime_service_from_settings(
        checkpoint_path=tmp_path / "runtime.db",
        service_factory=service_factory,
    )

    result = registry.invoke(
        ToolInvocation(
            "inv-telegram-cancellable",
            "run_cron",
            "telegram:100001",
            {"id": job.id},
        )
    )

    assert result.status == "completed"
    assert captured_start["origin_conversation_id"] == "telegram:100001"
    assert result.output["delivery_status"] == "deferred"


def test_telegram_service_falls_back_from_codex_oauth_to_platform_openai_key(tmp_path, monkeypatch) -> None:
    from nullion.model_clients import ModelClientConfigurationError

    runtime = bootstrap_persistent_runtime(tmp_path / "fallback-model.db")
    settings = load_settings(
        env={
            "NULLION_TELEGRAM_BOT_TOKEN": "123456:abcdefghijklmnopqrstuvwxyz",
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID": "123",
            "NULLION_TELEGRAM_CHAT_ENABLED": "true",
            "NULLION_SLACK_ENABLED": "true",
            "NULLION_MODEL_PROVIDER": "codex",
            "NULLION_MODEL": "gpt-5.5",
            "OPENAI_API_KEY": "sk-platform",
        }
    )
    seen_providers: list[str] = []

    def fake_build_model_client(candidate_settings):
        provider = candidate_settings.model.provider
        seen_providers.append(provider)
        if provider == "codex":
            raise ModelClientConfigurationError("Codex OAuth access token has no chatgpt_account_id claim")
        return SimpleNamespace(provider=provider, model=candidate_settings.model.openai_model)

    monkeypatch.setattr(telegram_app, "build_model_client_from_settings", fake_build_model_client)
    monkeypatch.setattr(telegram_app, "AgentOrchestrator", lambda model_client: SimpleNamespace(model_client=model_client))

    service = telegram_app.build_telegram_operator_service(runtime, settings=settings)
    messaging = telegram_app.build_messaging_operator_service(runtime, settings=settings)

    assert seen_providers == ["codex", "openai", "codex", "openai"]
    assert service.model_client.provider == "openai"
    assert service.agent_orchestrator.model_client is service.model_client
    assert messaging.model_client.provider == "openai"


def test_telegram_service_refreshes_model_client_from_live_config(tmp_path, monkeypatch) -> None:
    runtime = bootstrap_persistent_runtime(tmp_path / "live-config.db")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "NULLION_TELEGRAM_BOT_TOKEN=123456:abcdefghijklmnopqrstuvwxyz",
                "NULLION_TELEGRAM_OPERATOR_CHAT_ID=123",
                "NULLION_TELEGRAM_CHAT_ENABLED=true",
                "NULLION_MODEL_PROVIDER=openai",
                "NULLION_MODEL=gpt-initial",
                "NULLION_OPENAI_API_KEY=sk-initial",
            ]
        ),
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_file))
    monkeypatch.setenv("NULLION_CHECKPOINT_PATH", str(runtime.checkpoint_path))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path))
    settings = load_settings(env_path=env_file)

    monkeypatch.setattr(
        telegram_app,
        "_build_chat_model_client_with_fallback",
        lambda candidate_settings, *, surface: SimpleNamespace(
            provider=candidate_settings.model.provider,
            model=candidate_settings.model.openai_model,
        ),
    )
    monkeypatch.setattr(telegram_app, "AgentOrchestrator", lambda model_client: SimpleNamespace(model_client=model_client))

    service = telegram_app.ChatOperatorService(
        runtime=runtime,
        bot_token=settings.telegram.bot_token or "",
        operator_chat_id=settings.telegram.operator_chat_id,
        settings=settings,
        model_client=None,
    )

    assert service.refresh_model_client_if_configuration_changed(force=True) is True
    assert service.model_client.model == "gpt-initial"

    env_file.write_text(
        "\n".join(
            [
                "NULLION_TELEGRAM_BOT_TOKEN=654321:abcdefghijklmnopqrstuvwxyz",
                "NULLION_TELEGRAM_OPERATOR_CHAT_ID=456",
                "NULLION_TELEGRAM_CHAT_ENABLED=true",
                "NULLION_MODEL_PROVIDER=openai",
                "NULLION_MODEL=gpt-next",
                "NULLION_OPENAI_API_KEY=sk-next",
                "NULLION_ENABLED_SKILL_PACKS=nullion/example-skill",
                "NULLION_WEB_ACCESS_ENABLED=false",
                "NULLION_BROWSER_ENABLED=false",
            ]
        ),
        encoding="utf-8",
    )
    os.utime(env_file, None)

    assert service.refresh_model_client_if_configuration_changed() is True
    assert service.model_client.model == "gpt-next"
    assert service.agent_orchestrator.model_client is service.model_client
    assert service.bot_token == "654321:abcdefghijklmnopqrstuvwxyz"
    assert service.operator_chat_id == "456"
    assert service.settings.enabled_skill_packs == ("nullion/example-skill",)
    assert os.environ["NULLION_WEB_ACCESS_ENABLED"] == "false"
    assert os.environ["NULLION_BROWSER_ENABLED"] == "false"

    env_file.write_text(
        "\n".join(
            [
                "NULLION_TELEGRAM_BOT_TOKEN=654321:abcdefghijklmnopqrstuvwxyz",
                "NULLION_TELEGRAM_OPERATOR_CHAT_ID=456",
                "NULLION_TELEGRAM_CHAT_ENABLED=false",
                "NULLION_MODEL_PROVIDER=openai",
                "NULLION_MODEL=gpt-disabled",
                "NULLION_OPENAI_API_KEY=sk-disabled",
            ]
        ),
        encoding="utf-8",
    )
    os.utime(env_file, None)

    assert service.refresh_live_configuration() is True
    assert service.settings.telegram.chat_enabled is False
    assert service.model_client is None
    assert service.agent_orchestrator is None


@pytest.mark.asyncio
async def test_telegram_deliver_reply_logs_slow_timing_as_single_payload(monkeypatch) -> None:
    class Message:
        def __init__(self) -> None:
            self.chat = SimpleNamespace(id=100001)
            self.message_id = 123
            self.replies: list[tuple[str, dict]] = []

        async def reply_text(self, text, **kwargs):  # noqa: ANN001, ANN003
            self.replies.append((text, kwargs))
            return SimpleNamespace(message_id=901)

    message = Message()
    warnings: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def capture_warning(*args: object, **kwargs: object) -> None:  # noqa: ANN001
        warnings.append((tuple(args), kwargs))

    monkeypatch.setenv("NULLION_TELEGRAM_DELIVERY_SLOW_LOG_MS", "0")
    monkeypatch.setattr(telegram_app.logger, "warning", capture_warning)
    monkeypatch.setattr(
        telegram_app,
        "prepare_reply_for_platform_delivery",
        lambda reply, principal_id=None, **kwargs: SimpleNamespace(text=reply, attachments=()),
    )
    monkeypatch.setattr(telegram_app, "_record_telegram_delivery_receipt", lambda *args, **kwargs: None)

    await telegram_app._deliver_reply(
        message,
        "Quick response with markdown **works**",
        principal_id="telegram:100001",
    )

    assert message.replies
    assert warnings
    logged_args, _ = warnings[0]
    assert logged_args[0].startswith("telegram delivery slow timing")
    assert len(logged_args) == 2
    payload = logged_args[1]
    assert isinstance(payload, dict)
    assert payload["chat_id"] == "100001"
    assert payload["phase"] == "primary"
    assert payload["outcome"] == "ok"
    assert payload["attachment_count"] == 0
    assert payload["reply_len"] == len("Quick response with markdown **works**")


@pytest.mark.parametrize("command", ["/chat", "/chat@NullionBot"])
@pytest.mark.parametrize("body", [
    "形式を確認してください\n\n```python\n    value = 1\n```",
    "Проверьте формат\n\n| A | B |\n| --- | --- |\n| | value |",
])
def test_chat_wrapper_preserves_multiline_literal_request(command, body):
    import nullion.chat_operator as chat_operator
    assert chat_operator._chat_prompt_for_message(command + " " + body) == body

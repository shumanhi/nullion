from __future__ import annotations

import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run language-neutral routing e2e contracts",
)


class _StructuredDispatchModel:
    def __init__(
        self,
        *,
        relationship: str = "follow_up",
        effect: str = "continue",
        target_index: int | None = 0,
        confidence: float = 0.97,
        scheduler_action: str = "none",
        scheduler_toggle_enabled: bool | None = None,
        requested_tool_names: tuple[str, ...] = (),
        required_tool_names: tuple[str, ...] = (),
    ) -> None:
        self.relationship = relationship
        self.effect = effect
        self.target_index = target_index
        self.confidence = confidence
        self.scheduler_action = scheduler_action
        self.scheduler_toggle_enabled = scheduler_toggle_enabled
        self.requested_tool_names = requested_tool_names
        self.required_tool_names = required_tool_names
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        system = str(kwargs.get("system") or "")
        if "relationship" in system and "target_index" in system:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "relationship": self.relationship,
                                "effect": self.effect,
                                "confidence": self.confidence,
                                "target_index": self.target_index,
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
                                "scheduler_action": self.scheduler_action,
                                "scheduler_toggle_enabled": self.scheduler_toggle_enabled,
                                "skill_pack_action": "none",
                                "requested_tool_names": list(self.requested_tool_names),
                                "required_tool_names": list(self.required_tool_names),
                                "confidence": 0.9,
                            }
                        ),
                    }
                ]
            }
        return {"content": [{"type": "text", "text": "{}"}]}


class _SequentialOrchestrator:
    def __init__(self, model_client, replies: tuple[str, ...]) -> None:  # noqa: ANN001
        self.model_client = model_client
        self.replies = replies
        self.calls: list[dict[str, object]] = []

    def run_turn(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        index = min(len(self.calls) - 1, len(self.replies) - 1)
        return SimpleNamespace(
            suspended_for_approval=False,
            approval_id=None,
            tool_results=[],
            final_text=self.replies[index],
            artifacts=[],
            thinking_text="",
            reached_iteration_limit=False,
        )


def _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator) -> None:  # noqa: ANN001
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


def _history_has_text(history: list[dict[str, object]], *, role: str, text: str) -> bool:
    return any(
        item.get("role") == role
        and item.get("content", [{}])[0].get("text") == text
        for item in history
    )


def _history_text_sequence(history: list[dict[str, object]]) -> list[tuple[str, str]]:
    return [
        (str(item.get("role") or ""), str(item.get("content", [{}])[0].get("text") or ""))
        for item in history
        if item.get("role") in {"user", "assistant"}
    ]


def test_account_source_discovery_asks_before_reading_vague_account_source_e2e(monkeypatch) -> None:
    from nullion import turn_context_policy
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_backend import generate_chat_reply
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "display_name": "Example connector",
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    registry = ToolRegistry()
    calls: list[str] = []
    for name, tags, side_effect in (
        ("connector_request", ("connector",), ToolSideEffectClass.READ),
        ("skill_pack_read", ("skill_pack",), ToolSideEffectClass.READ),
        ("email_send", ("email", "connector", "account_write"), ToolSideEffectClass.ACCOUNT_WRITE),
        ("email_search", ("email", "connector", "account_read"), ToolSideEffectClass.READ),
        ("email_read", ("email", "connector", "account_read"), ToolSideEffectClass.READ),
        ("calendar_list", ("calendar", "connector", "account_read"), ToolSideEffectClass.READ),
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
            lambda invocation: (
                calls.append(invocation.tool_name)
                or ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {"results": [{"id": "msg-1", "subject": "External status"}]}
                    if invocation.tool_name == "email_search"
                    else {},
                )
            ),
        )

    class SourceDiscoveryModel:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN003
            visible = {str(tool.get("name") or "") for tool in (kwargs.get("tools") or ())}
            self.tool_sets.append(visible)
            if len(self.tool_sets) == 1:
                assert visible == {"request_tool_scope"}
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-1",
                            "name": "request_tool_scope",
                            "input": {"capabilities": ["connector"]},
                        }
                    ],
                }
            if len(self.tool_sets) == 2:
                assert "email_search" not in visible
                assert "email_read" not in visible
                assert "calendar_list" not in visible
                assert "email_send" not in visible
                return {
                    "stop_reason": "end_turn",
                    "content": [{
                        "type": "text",
                        "text": (
                            "I found connected read sources, but I need you to choose where to check:\n"
                            "1. Email\n"
                            "2. Calendar"
                        ),
                    }],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "Unexpected extra model call."}],
            }

    model = SourceDiscoveryModel()
    reply = generate_chat_reply(
        message="verificar estado externo",
        live_tool_registry=registry,
        agent_orchestrator=AgentOrchestrator(model_client=model),
        conversation_id="web:e2e-account-source-discovery",
        principal_id="web:e2e-account-source-discovery",
    )

    assert "please choose which authorized source" in reply
    assert "1. Example connector: Email records" in reply
    assert "2. Example connector: Calendar records" in reply
    assert calls == []
    assert "request_tool_scope" in model.tool_sets[0]


def test_account_source_selection_overrides_unhelpful_scope_reply_e2e(monkeypatch) -> None:
    from nullion import turn_context_policy
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_backend import generate_chat_reply
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "display_name": "Example connector",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    registry = ToolRegistry()
    calls: list[str] = []
    for name, tags in (
        ("connector_request", ("connector",)),
        ("skill_pack_read", ("skill_pack",)),
        ("email_search", ("email", "connector", "account_read")),
        ("email_read", ("email", "connector", "account_read")),
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
                calls.append(invocation.tool_name)
                or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {})
            ),
        )

    class BadScopeReplyModel:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN003
            visible = {str(tool.get("name") or "") for tool in (kwargs.get("tools") or ())}
            self.tool_sets.append(visible)
            if len(self.tool_sets) == 1:
                assert visible == {"request_tool_scope"}
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-1",
                            "name": "request_tool_scope",
                            "input": {"capabilities": ["connector"]},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "I ran the local command.\n\n```text\nnoop\n```"}],
            }

    model = BadScopeReplyModel()
    reply = generate_chat_reply(
        message="verificar estado externo",
        live_tool_registry=registry,
        agent_orchestrator=AgentOrchestrator(model_client=model),
        conversation_id="web:e2e-account-source-selection-bad-reply",
        principal_id="web:e2e-account-source-selection-bad-reply",
    )

    assert "please choose which authorized source" in reply
    assert "1. Example connector: Email records" in reply
    assert "2. Example connector: Calendar records" in reply
    assert "noop" not in reply
    assert calls == []
    assert "request_tool_scope" in model.tool_sets[0]


def test_generic_gateway_skill_pack_context_exposes_connector_path_e2e(tmp_path, monkeypatch) -> None:
    from nullion import connections, skill_pack_installer
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_backend import generate_chat_reply
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    pack_dir = tmp_path / "skill-packs" / "example" / "api-gateway-skill"
    (pack_dir / "references" / "example-drive").mkdir(parents=True)
    (pack_dir / "references" / "example-drive" / "README.md").write_text(
        "# Example Drive\n\nUse `GET /example-drive/files` for read-only file listing.\n",
        encoding="utf-8",
    )
    (pack_dir / "SKILL.md").write_text(
        "---\n"
        "metadata:\n"
        "  clawdbot:\n"
        "    requires:\n"
        "      env:\n"
        "        - EXAMPLE_GATEWAY_KEY\n"
        "---\n"
        "# Example API Gateway\n\n"
        "## Base URL\n\n"
        "https://gateway.example.test/{app}/{native-api-path}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        connections,
        "load_connection_registry",
        lambda *args, **kwargs: SimpleNamespace(
            connections=[
                SimpleNamespace(
                    provider_id="custom_connector_provider",
                    display_name="Example API Gateway",
                    provider_profile="https://gateway.example.test",
                    credential_ref="EXAMPLE_GATEWAY_KEY",
                    active=True,
                    permission_mode="write",
                    credential_scope="workspace",
                )
            ]
        ),
    )
    monkeypatch.setattr(
        skill_pack_installer,
        "list_installed_skill_packs",
        lambda *args, **kwargs: [
            SimpleNamespace(pack_id="example/api-gateway-skill", path=str(pack_dir), source="fixture")
        ],
    )

    registry = ToolRegistry()
    calls: list[tuple[str, dict[str, object]]] = []

    def _record(invocation):  # noqa: ANN001
        calls.append((invocation.tool_name, dict(invocation.arguments)))
        if invocation.tool_name == "skill_pack_read":
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"path": invocation.arguments.get("path"), "text": "GET /example-drive/files"},
            )
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"json": {"files": [{"id": "file-1", "name": "Lease.pdf"}]}},
        )

    for name, tags in (
        ("connector_request", ("connector",)),
        ("skill_pack_read", ("skill_pack",)),
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
            _record,
        )

    class GatewayModel:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN003
            visible = {str(tool.get("name") or "") for tool in (kwargs.get("tools") or ())}
            self.tool_sets.append(visible)
            if len(self.tool_sets) == 1:
                context = json.dumps(kwargs, default=str)
                assert "Associated connector skill context:" in context
                assert "active_app_ids=[example-drive]" in context
                assert "Builder can create or save a reusable skill or connector workflow" in context
                assert visible == {"request_tool_scope"}
                return {
                    "stop_reason": "tool_use",
                    "content": [{
                        "type": "tool_use",
                        "id": "scope-1",
                        "name": "request_tool_scope",
                        "input": {
                            "capabilities": ["connector"],
                            "source_user_requested": True,
                            "source_evidence": "source d'espace de travail",
                        },
                    }],
                }
            if len(self.tool_sets) == 2:
                assert "connector_request" in visible
                assert "skill_pack_read" in visible
                return {
                    "stop_reason": "tool_use",
                    "content": [{
                        "type": "tool_use",
                        "id": "skill-1",
                        "name": "skill_pack_read",
                        "input": {
                            "pack_id": "example/api-gateway-skill",
                            "path": "example-drive",
                        },
                    }],
                }
            if len(self.tool_sets) == 3:
                assert "connector_request" in visible
                return {
                    "stop_reason": "tool_use",
                    "content": [{
                        "type": "tool_use",
                        "id": "connector-1",
                        "name": "connector_request",
                        "input": {
                            "provider_id": "custom_connector_provider",
                            "url": "https://gateway.example.test/example-drive/files",
                            "method": "GET",
                        },
                    }],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "I checked the gateway and found Lease.pdf."}],
            }

    model = GatewayModel()
    reply = generate_chat_reply(
        message="source d'espace de travail",
        live_tool_registry=registry,
        agent_orchestrator=AgentOrchestrator(model_client=model),
        conversation_id="web:e2e-generic-gateway-skill-pack",
        principal_id="web:e2e-generic-gateway-skill-pack",
    )

    assert "Lease.pdf" in reply
    assert [name for name, _args in calls] == ["skill_pack_read", "connector_request"]
    assert calls[0][1]["path"] == "example-drive"
    assert calls[1][1]["provider_id"] == "custom_connector_provider"
    assert "connector_request" in model.tool_sets[1]
    assert "skill_pack_read" in model.tool_sets[1]


def test_account_tool_answer_is_not_replaced_by_generic_message_list_e2e(monkeypatch) -> None:
    from nullion import turn_context_policy
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_backend import generate_chat_reply
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Example connector",
                "structured_tools": ["email_search"],
                "active_app_ids": ["google-mail"],
            }
        ],
    )
    registry = ToolRegistry()
    calls: list[str] = []
    registry.register(
        ToolSpec(
            name="email_search",
            description="Search email",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
            input_schema={"type": "object", "properties": {}, "additionalProperties": True},
        ),
        lambda invocation: calls.append(invocation.tool_name)
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "query": str(invocation.arguments.get("query") or ""),
                "results": [
                    {
                        "id": "msg-1",
                        "subject": "Reminder: Initial consultation",
                        "from": "Office <office@example.com>",
                        "snippet": "You have an upcoming booking.",
                    },
                    {
                        "id": "msg-2",
                        "subject": "Confirmed: Initial consultation",
                        "from": "Office <office@example.com>",
                        "snippet": "Your appointment is confirmed.",
                    },
                ],
            },
        ),
    )

    class EvidenceAnswerModel:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN003
            visible = {str(tool.get("name") or "") for tool in (kwargs.get("tools") or ())}
            self.tool_sets.append(visible)
            if len(self.tool_sets) == 1:
                assert visible == {"request_tool_scope"}
                return {
                    "stop_reason": "tool_use",
                    "content": [{
                        "type": "tool_use",
                        "id": "scope-1",
                        "name": "request_tool_scope",
                        "input": {
                            "capabilities": ["connector"],
                            "source_user_requested": True,
                            "source_evidence": "mi correo",
                            "connector_app_ids": ["google-mail"],
                            "tool_names": ["email_search"],
                        },
                    }],
                }
            if len(self.tool_sets) == 2:
                assert "email_search" in visible
                return {
                    "stop_reason": "tool_use",
                    "content": [{
                        "type": "tool_use",
                        "id": "search-1",
                        "name": "email_search",
                        "input": {"query": "recent account evidence", "limit": 5},
                    }],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{
                    "type": "text",
                    "text": (
                        "I checked your email, but those records do not prove the requested answer. "
                        "They only show appointment reminders and confirmations."
                    ),
                }],
            }

    reply = generate_chat_reply(
        message="verificar este estado en mi correo",
        live_tool_registry=registry,
        agent_orchestrator=AgentOrchestrator(model_client=EvidenceAnswerModel()),
        conversation_id="web:e2e-account-answer-preserved",
        principal_id="web:e2e-account-answer-preserved",
    )

    assert "do not prove the requested answer" in reply
    assert "appointment reminders and confirmations" in reply
    assert "Found 2 matching messages" not in reply
    assert calls == ["email_search"]


def test_language_neutral_routing_uses_structural_signals_across_runtime_contracts(tmp_path, monkeypatch) -> None:
    from nullion.attachment_format_graph import plan_attachment_format
    from nullion.live_information import LiveInformationRoute, classify_live_information_route
    from nullion.task_planner import TaskPlanner
    from nullion import crons, users
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (
        TerminalBackendDescriptor,
        TerminalExecutionResult,
        ToolInvocation,
        _build_delete_cron_handler,
        _build_terminal_exec_handler,
        _build_update_cron_handler,
    )

    planner = TaskPlanner()
    structural_pdf = planner.build_execution_plan(
        user_message="résumé la page et rends-la en .pdf",
        principal_id="e2e:user",
        active_task_frame=None,
    ).mission.steps[0].metadata
    prose_pdf = planner.build_execution_plan(
        user_message="résumé la page en pdf",
        principal_id="e2e:user",
        active_task_frame=None,
    ).mission.steps[0].metadata
    structural_url = planner.build_execution_plan(
        user_message="analiza ejemplo.test/path",
        principal_id="e2e:user",
        active_task_frame=None,
    ).mission.steps[0].metadata
    hidden_app_path = "No there's a hermes app in ~/.hermes"
    hidden_app_plan = plan_attachment_format(hidden_app_path)
    hidden_app_metadata = planner.build_execution_plan(
        user_message=hidden_app_path,
        principal_id="e2e:user",
        active_task_frame=None,
    ).mission.steps[0].metadata
    users_path = tmp_path / "users.json"
    users.save_user_registry(
        users.UserRegistry(
            multi_user_enabled=True,
            users=[
                users.NullionUser("admin", "Admin", role="admin", workspace_id="workspace_admin"),
                users.NullionUser("member_nate", "Nate", workspace_id="workspace_nate"),
            ],
        ),
        path=users_path,
    )
    monkeypatch.setattr(users, "_USERS_PATH", users_path)
    workspace_storage = tmp_path / "workspaces"
    (workspace_storage / "workspace_nate").mkdir(parents=True)
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(workspace_storage))

    class Backend:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def describe(self):
            return TerminalBackendDescriptor("subprocess", (), {})

        def run(self, command, *, cwd, timeout, policy):  # noqa: ANN001
            self.calls.append(command)
            return TerminalExecutionResult(0, "ok", "")

    backend = Backend()
    terminal_handler = _build_terminal_exec_handler(
        workspace_root=tmp_path,
        terminal_executor_backend=backend,
    )
    unknown_workspace_result = terminal_handler(
        ToolInvocation(
            "inv-unknown-workspace",
            "terminal_exec",
            "operator",
            {"command": f"mkdir -p {workspace_storage / 'workspace_nathan' / 'files'}"},
        )
    )
    registry_prompt = users.format_workspace_registry_for_prompt()
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    misplaced_cron = crons.add_cron(
        "Misplaced workspace repair contract",
        "0 8 * * *",
        "Repair cron ownership",
        workspace_id="workspace_nathan",
        enabled=False,
    )
    admin_cron_update = _build_update_cron_handler()(
        ToolInvocation(
            "inv-admin-cron-update",
            "update_cron",
            "workspace:workspace_admin",
            {"id": misplaced_cron.id, "workspace_id": "workspace_nate", "enabled": True},
        )
    )
    member_cron_delete = _build_delete_cron_handler()(
        ToolInvocation(
            "inv-member-cron-delete",
            "delete_cron",
            "workspace:workspace_nate",
            {"id": misplaced_cron.id},
        )
    )

    assert structural_pdf["tool_scope"] == ["pdf_create"]
    assert structural_pdf["tool_scope_evidence"] == ["literal_extension"]
    assert prose_pdf["tool_scope"] == []
    assert structural_url["tool_scope"] == ["web_fetch"]
    assert structural_url["tool_scope_evidence"] == ["url_target"]
    assert hidden_app_plan.extension is None
    assert hidden_app_metadata["tool_scope"] == []
    assert "tool_scope_evidence" not in hidden_app_metadata
    assert registry_prompt is not None
    assert "workspace_id=workspace_nate; display_name=Nate;" in registry_prompt
    assert "workspace_nathan" not in registry_prompt
    assert unknown_workspace_result.status == "failed"
    assert unknown_workspace_result.output["reason"] == "unknown_workspace_denied"
    assert unknown_workspace_result.output["workspace_id"] == "workspace_nathan"
    assert not (workspace_storage / "workspace_nathan").exists()
    assert backend.calls == []
    assert admin_cron_update.status == "completed"
    assert admin_cron_update.output["workspace_id"] == "workspace_nate"
    assert admin_cron_update.output["enabled"] is True
    assert admin_cron_update.output["admin_cross_workspace"] is True
    assert member_cron_delete.status == "completed"
    assert member_cron_delete.output["id"] == misplaced_cron.id
    assert classify_live_information_route("analiza ejemplo.test/path") is LiveInformationRoute.LIVE_LOOKUP
    assert classify_live_information_route("10001") is LiveInformationRoute.NONE

    class AttachmentFormatModel:
        def create(self, **kwargs):  # noqa: ANN003
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {"extension": ".xlsx", "confidence": 0.99},
                    }
                ]
            }

    class ArtifactOrchestrator:
        model_client = AttachmentFormatModel()

        def __init__(self) -> None:
            self.run_calls = 0

        def dispatch_request_sync(self, **kwargs):  # noqa: ANN003
            raise AssertionError("structured attachment deliverables must not return a mini-agent dispatch ack")

        def run_turn(self, **kwargs):  # noqa: ANN003
            self.run_calls += 1
            return type(
                "TurnResult",
                (),
                {
                    "suspended_for_approval": False,
                    "approval_id": None,
                    "tool_results": [],
                    "final_text": "Direct artifact workflow stayed active.",
                    "artifacts": [],
                    "thinking_text": "",
                    "reached_iteration_limit": False,
                },
            )()

    runtime = bootstrap_persistent_runtime(tmp_path / "chat-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    orchestrator = ArtifactOrchestrator()
    monkeypatch.setattr(chat_operator, "_telegram_screenshot_reply_if_requested", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        chat_operator,
        "run_pre_chat_artifact_workflow",
        lambda *args, **kwargs: type("ArtifactResult", (), {"kind": "none", "image_result": None})(),
    )
    monkeypatch.setattr(chat_operator, "_chat_capability_inventory_prompt", lambda runtime, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_enabled_skill_pack_prompt", lambda settings: None)
    monkeypatch.setattr(chat_operator, "_build_skill_hint", lambda runtime, user_message: None)
    monkeypatch.setattr(chat_operator, "_broadcast_new_workspace_approvals", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_operator, "_feature_enabled", lambda name: True)
    monkeypatch.setattr(
        chat_operator,
        "TaskPlanner",
        lambda: type(
            "Planner",
            (),
            {
                "build_execution_plan": lambda self, **kwargs: type(
                    "Plan",
                    (),
                    {
                        "mission": type(
                            "Mission",
                            (),
                            {"steps": [type("Step", (), {"title": "Find"})(), type("Step", (), {"title": "Build"})()]},
                        )(),
                        "can_dispatch_mini_agents": True,
                        "can_run_mission": False,
                    },
                )()
            },
        )(),
    )
    monkeypatch.setattr(chat_operator, "_needs_required_attachment_repair", lambda *args, **kwargs: False)
    monkeypatch.setattr(chat_operator, "_enforce_chat_response_fulfillment", lambda runtime, **kwargs: kwargs["reply"])
    monkeypatch.setattr(chat_operator, "_try_builder_reflection", lambda *args, **kwargs: None)

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat 添付ファイルとして表形式で返して",
        chat_id="100001",
        settings=settings,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    assert reply == "Direct artifact workflow stayed active."
    assert orchestrator.run_calls == 1


def test_structured_parallel_follow_up_survives_persistent_runtime_boundary(tmp_path) -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime

    db_path = tmp_path / "parallel-runtime.db"
    conversation_id = "telegram:parallel-e2e"
    runtime = bootstrap_persistent_runtime(db_path)

    email_turn = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="メールの新着を確認する",
        request_id="telegram-update:email",
        message_id="telegram-message:email",
        turn_id="turn-email",
    )
    cron_turn = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="informe bursátil posterior al cierre",
        request_id="telegram-update:cron",
        message_id="telegram-message:cron",
        turn_id="turn-cron",
    )
    follow_up = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="¿Eso requiere atención ahora?",
        request_id="telegram-update:follow-up",
        message_id="telegram-message:follow-up",
        turn_id="turn-follow-up",
        dispatch_disposition=ConversationTurnDisposition.CONTINUE,
        dispatch_dependency_turn_ids=("telegram-update:email",),
        dispatch_reason="model_structured_follow_up",
    )
    runtime.checkpoint()

    reloaded = bootstrap_persistent_runtime(db_path)
    persisted_follow_up = reloaded.store.get_conversation_turn(follow_up.turn.turn_id)
    persisted_events = reloaded.store.list_conversation_events(conversation_id)
    persisted_follow_event = [
        event
        for event in persisted_events
        if event.get("turn_id") == follow_up.turn.turn_id and event.get("event_type") == "conversation.turn_recorded"
    ][-1]

    assert cron_turn.turn.parent_turn_id is None
    assert persisted_follow_up is not None
    assert persisted_follow_up.parent_turn_id == email_turn.turn.turn_id
    assert persisted_follow_up.branch_id == email_turn.turn.branch_id
    assert persisted_follow_up.branch_id != cron_turn.turn.branch_id
    assert persisted_follow_up.disposition is ConversationTurnDisposition.CONTINUE
    assert persisted_follow_event["dispatch_dependency_turn_ids"] == ["telegram-update:email"]
    assert persisted_follow_event["dispatch_parent_turn_id"] == email_turn.turn.turn_id
    assert persisted_follow_event["dispatch_linked"] is True


def test_structured_follow_up_targets_middle_parallel_turn_not_first_or_latest(tmp_path) -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime

    db_path = tmp_path / "parallel-middle-runtime.db"
    conversation_id = "telegram:middle-target-e2e"
    runtime = bootstrap_persistent_runtime(db_path)

    email_turn = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="Revisar correo de facturación",
        request_id="telegram-update:email",
        message_id="telegram-message:email",
        turn_id="turn-email",
    )
    cron_turn = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="Bereite den Börsenbericht nach Marktschluss vor",
        request_id="telegram-update:cron",
        message_id="telegram-message:cron",
        turn_id="turn-cron",
    )
    calendar_turn = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="予定表の明日の予定を確認して",
        request_id="telegram-update:calendar",
        message_id="telegram-message:calendar",
        turn_id="turn-calendar",
    )
    follow_up = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="Incluye una tabla comparativa en ese informe.",
        request_id="telegram-update:follow-up",
        message_id="telegram-message:follow-up",
        turn_id="turn-follow-up",
        dispatch_disposition=ConversationTurnDisposition.CONTINUE,
        dispatch_dependency_turn_ids=("telegram-update:cron",),
        dispatch_reason="model_structured_follow_up",
    )
    runtime.checkpoint()

    reloaded = bootstrap_persistent_runtime(db_path)
    persisted_follow_up = reloaded.store.get_conversation_turn(follow_up.turn.turn_id)
    persisted_events = reloaded.store.list_conversation_events(conversation_id)
    persisted_follow_event = [
        event
        for event in persisted_events
        if event.get("turn_id") == follow_up.turn.turn_id and event.get("event_type") == "conversation.turn_recorded"
    ][-1]

    assert persisted_follow_up is not None
    assert persisted_follow_up.parent_turn_id == cron_turn.turn.turn_id
    assert persisted_follow_up.branch_id == cron_turn.turn.branch_id
    assert persisted_follow_up.branch_id != email_turn.turn.branch_id
    assert persisted_follow_up.branch_id != calendar_turn.turn.branch_id
    assert persisted_follow_event["dispatch_dependency_turn_ids"] == ["telegram-update:cron"]
    assert persisted_follow_event["dispatch_parent_turn_id"] == cron_turn.turn.turn_id
    assert persisted_follow_event["dispatch_linked"] is True


def test_runtime_rejects_unverified_structured_dispatch_dependency(tmp_path) -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime

    conversation_id = "telegram:missing-dependency-e2e"
    runtime = bootstrap_persistent_runtime(tmp_path / "missing-dependency-runtime.db")

    first = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="列車ルートを調べて",
        request_id="telegram-update:first",
        message_id="telegram-message:first",
        turn_id="turn-first",
    )
    follow_up = runtime.process_conversation_message(
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="Maak daar een mooi bestand van.",
        request_id="telegram-update:follow-up",
        message_id="telegram-message:follow-up",
        turn_id="turn-follow-up",
        dispatch_disposition=ConversationTurnDisposition.CONTINUE,
        dispatch_dependency_turn_ids=("telegram-update:not-in-store",),
        dispatch_reason="model_structured_follow_up",
    )
    events = runtime.store.list_conversation_events(conversation_id)
    follow_event = [
        event
        for event in events
        if event.get("turn_id") == follow_up.turn.turn_id and event.get("event_type") == "conversation.turn_recorded"
    ][-1]

    assert follow_up.turn.parent_turn_id is None
    assert follow_up.turn.branch_id != first.turn.branch_id
    assert follow_up.turn.disposition is ConversationTurnDisposition.INDEPENDENT
    assert follow_up.disposition_reason == "default_independent"
    assert follow_event["dispatch_dependency_turn_ids"] == ["telegram-update:not-in-store"]
    assert follow_event.get("dispatch_parent_turn_id") is None
    assert follow_event["dispatch_linked"] is False


def test_active_dispatch_tracker_selects_structured_target_without_latest_assumption() -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker

    async def scenario() -> None:
        tracker = AsyncTurnDispatchTracker()
        release = asyncio.Event()

        async def parked() -> None:
            await release.wait()

        first_task = asyncio.create_task(parked())
        second_task = asyncio.create_task(parked())
        current_task = asyncio.create_task(parked())
        model = _StructuredDispatchModel(target_index=1)
        try:
            first = await tracker.register(
                "telegram:active-middle-e2e",
                "Comprueba el correo",
                turn_id="turn-email",
                task=first_task,
                model_client=model,
            )
            second = await tracker.register(
                "telegram:active-middle-e2e",
                "Prépare le rapport de marché",
                turn_id="turn-cron",
                task=second_task,
                model_client=model,
            )
            follow = await tracker.register(
                "telegram:active-middle-e2e",
                "Ajoute aussi les gagnants et perdants.",
                turn_id="turn-follow-up",
                task=current_task,
                model_client=model,
                active_turn_followup_evidence=True,
            )

            assert first.decision.dependency_turn_ids == ()
            assert second.decision.dependency_turn_ids == ()
            assert follow.decision.disposition is ConversationTurnDisposition.CONTINUE
            assert follow.decision.dependency_turn_ids == ("turn-cron",)
            assert follow.dependency_tasks == (second_task,)
        finally:
            await tracker.finish("telegram:active-middle-e2e", "turn-follow-up")
            await tracker.finish("telegram:active-middle-e2e", "turn-cron")
            await tracker.finish("telegram:active-middle-e2e", "turn-email")
            release.set()
            await asyncio.gather(first_task, second_task, current_task)

    asyncio.run(scenario())


def test_active_revision_supersedes_only_selected_parallel_target() -> None:
    from nullion.turn_dispatch_graph import AsyncTurnDispatchTracker

    async def scenario() -> None:
        tracker = AsyncTurnDispatchTracker()
        release = asyncio.Event()

        async def parked() -> None:
            await release.wait()

        first_task = asyncio.create_task(parked())
        second_task = asyncio.create_task(parked())
        revision_task = asyncio.create_task(parked())
        model = _StructuredDispatchModel(effect="revise", target_index=0)
        try:
            await tracker.register(
                "telegram:active-revise-e2e",
                "メールを確認",
                turn_id="turn-email",
                task=first_task,
                model_client=model,
            )
            await tracker.register(
                "telegram:active-revise-e2e",
                "run scheduled stock report",
                turn_id="turn-cron",
                task=second_task,
                model_client=model,
            )
            revision = await tracker.register(
                "telegram:active-revise-e2e",
                "Stattdessen nur dringende Elemente zusammenfassen.",
                turn_id="turn-revision",
                task=revision_task,
                model_client=model,
                active_turn_followup_evidence=True,
            )

            assert revision.decision.dependency_turn_ids == ("turn-email",)
            assert await tracker.is_superseded("telegram:active-revise-e2e", "turn-email") is True
            assert await tracker.is_superseded("telegram:active-revise-e2e", "turn-cron") is False
        finally:
            await tracker.finish("telegram:active-revise-e2e", "turn-revision")
            await tracker.finish("telegram:active-revise-e2e", "turn-cron")
            await tracker.finish("telegram:active-revise-e2e", "turn-email")
            release.set()
            await asyncio.gather(first_task, second_task, revision_task)

    asyncio.run(scenario())


def test_messaging_platform_dispatch_decision_reaches_runtime_persistence(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    runtime = bootstrap_persistent_runtime(tmp_path / "platform-dispatch-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(relationship="separate", target_index=None)
    orchestrator = _SequentialOrchestrator(model, ("Follow-up attached to selected active turn.",))
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    email_turn = runtime.process_conversation_message(
        conversation_id="telegram:100001",
        chat_id="100001",
        user_message="メールを確認して",
        request_id="telegram-update:email",
        message_id="telegram-message:email",
        turn_id="turn-email",
    )
    cron_turn = runtime.process_conversation_message(
        conversation_id="telegram:100001",
        chat_id="100001",
        user_message="ejecuta el informe de mercado",
        request_id="telegram-update:cron",
        message_id="telegram-message:cron",
        turn_id="turn-cron",
    )

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Rien de critique alors ?",
        chat_id="100001",
        settings=settings,
        request_id="telegram-update:follow-up",
        message_id="telegram-message:follow-up",
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        turn_dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
            dependency_turn_ids=("telegram-update:email",),
            disposition=ConversationTurnDisposition.CONTINUE,
            reason="model_structured_follow_up",
            target_active_turn_index=0,
        ),
    )
    follow_up_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])

    assert reply == "Follow-up attached to selected active turn."
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id == email_turn.turn.turn_id
    assert follow_up_turn.branch_id == email_turn.turn.branch_id
    assert follow_up_turn.branch_id != cron_turn.turn.branch_id
    assert orchestrator.calls[-1]["user_message"] == "Rien de critique alors ?"


def test_completed_messaging_turn_follow_up_uses_structured_recent_context(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    class RecentContextModel:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
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
                                    "confidence": 0.96,
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
                                    "scheduler_action": "none",
                                    "skill_pack_action": "none",
                                    "confidence": 0.9,
                                }
                            ),
                        }
                    ]
                }
            return {"content": [{"type": "text", "text": "{}"}]}

    class RecentContextOrchestrator:
        def __init__(self, model_client) -> None:  # noqa: ANN001
            self.model_client = model_client
            self.calls: list[dict[str, object]] = []

        def run_turn(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            text = "Sí, hay una ruta vía Chicago y Emeryville." if len(self.calls) == 1 else "Crearé el PDF de esa ruta."
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[],
                final_text=text,
                artifacts=[],
                thinking_text="",
                reached_iteration_limit=False,
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "recent-context-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = RecentContextModel()
    orchestrator = RecentContextOrchestrator(model)

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

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat ¿Hay tren de Nueva York a San Francisco?",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat Génère un .pdf détaillé avec durée, prix et vues panoramiques.",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        reply_context={"reply_to_text": first},
    )

    second_history = orchestrator.calls[-1]["conversation_history"]
    conversation_turns = runtime.store.list_conversation_chat_turns("telegram:100001")
    follow_up_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])

    assert first == "Sí, hay una ruta vía Chicago y Emeryville."
    assert second == "Crearé el PDF de esa ruta."
    assert any(
        block.get("role") == "user"
        and block.get("content", [{}])[0].get("text") == "¿Hay tren de Nueva York a San Francisco?"
        for block in second_history
    )
    assert any(
        block.get("role") == "assistant"
        and block.get("content", [{}])[0].get("text") == "Sí, hay una ruta vía Chicago y Emeryville."
        for block in second_history
    )
    assert conversation_turns[-2]["user"] == "¿Hay tren de Nueva York a San Francisco?"
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id is not None


def test_completed_messaging_turn_follow_up_keeps_full_context_across_a_third_message(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    class ThreeTurnContextModel:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
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
                                    "confidence": 0.98,
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
                                    "scheduler_action": "none",
                                    "skill_pack_action": "none",
                                    "confidence": 0.9,
                                }
                            ),
                        }
                    ]
                }
            return {"content": [{"type": "text", "text": "{}"}]}

    class ThreeTurnContextOrchestrator:
        def __init__(self, model_client) -> None:  # noqa: ANN001
            self.model_client = model_client
            self.calls: list[dict[str, object]] = []

        def run_turn(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            replies = (
                "Sí, hay una ruta vía Chicago y Emeryville.",
                "Crearé el PDF de esa ruta.",
                "Y ahora lo mantengo conectado al contexto anterior.",
            )
            index = min(len(self.calls) - 1, len(replies) - 1)
            return SimpleNamespace(
                suspended_for_approval=False,
                approval_id=None,
                tool_results=[],
                final_text=replies[index],
                artifacts=[],
                thinking_text="",
                reached_iteration_limit=False,
            )

    runtime = bootstrap_persistent_runtime(tmp_path / "three-turn-context-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = ThreeTurnContextModel()
    orchestrator = ThreeTurnContextOrchestrator(model)

    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat ¿Hay tren de Nueva York a San Francisco?",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat Génère un .pdf détaillé avec durée, prix et vues panoramiques.",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    third = chat_operator._render_chat_turn(
        runtime,
        message="/chat Añade una nota breve de seguimiento para ese mismo viaje.",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )

    third_history = orchestrator.calls[-1]["conversation_history"]
    conversation_turns = runtime.store.list_conversation_chat_turns("telegram:100001")
    follow_up_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])

    assert first == "Sí, hay una ruta vía Chicago y Emeryville."
    assert second == "Crearé el PDF de esa ruta."
    assert third == "Y ahora lo mantengo conectado al contexto anterior."
    assert _history_text_sequence(third_history) == [
        ("user", "¿Hay tren de Nueva York a San Francisco?"),
        ("assistant", "Sí, hay una ruta vía Chicago y Emeryville."),
        ("user", "Génère un .pdf détaillé avec durée, prix et vues panoramiques."),
        ("assistant", "Crearé el PDF de esa ruta."),
    ]
    assert conversation_turns[-3]["user"] == "¿Hay tren de Nueva York a San Francisco?"
    assert conversation_turns[-2]["user"] == "Génère un .pdf détaillé avec durée, prix et vues panoramiques."
    assert conversation_turns[-1]["user"] == "Añade una nota breve de seguimiento para ese mismo viaje."
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id is not None


def test_completed_messaging_turn_structured_separate_stays_independent(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "recent-separate-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(relationship="separate", target_index=None)
    orchestrator = _SequentialOrchestrator(model, ("Route answer stored.", "Independent answer stored."))
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat ¿Hay tren de Nueva York a San Francisco?",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    first_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat 日本語で今日の献立を考えて",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    second_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])
    second_history = orchestrator.calls[-1]["conversation_history"]

    assert first == "Route answer stored."
    assert second == "Independent answer stored."
    assert first_turn is not None
    assert second_turn is not None
    assert second_turn.parent_turn_id is None
    assert second_turn.branch_id != first_turn.branch_id
    assert _history_has_text(
        second_history,
        role="user",
        text="¿Hay tren de Nueva York a San Francisco?",
    )
    assert _history_has_text(second_history, role="assistant", text="Route answer stored.")


def test_telegram_reply_confirmation_uses_prior_assistant_context_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-confirmation-follow-up-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(relationship="follow_up", target_index=0)
    orchestrator = _SequentialOrchestrator(
        model,
        (
            "I can copy the Codex token from stage to prod, but please confirm before I touch prod credentials.",
            "Confirmed path preserved.",
        ),
    )
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Can u copy over my auth token from stage to prod for codex,",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    first_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat ✓",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        reply_context={
            "platform": "telegram",
            "reply_to_message_id": "telegram-message-1",
            "reply_to_from_bot": True,
            "reply_to_text": first,
        },
    )
    follow_up_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])
    second_history = orchestrator.calls[-1]["conversation_history"]

    assert first == "I can copy the Codex token from stage to prod, but please confirm before I touch prod credentials."
    assert second == "Confirmed path preserved."
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id == first_turn_id
    assert _history_has_text(second_history, role="user", text="Can u copy over my auth token from stage to prod for codex,")
    assert _history_has_text(
        second_history,
        role="assistant",
        text="I can copy the Codex token from stage to prod, but please confirm before I touch prod credentials.",
    )


def test_telegram_reply_confirmation_uses_prior_option_context_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-option-follow-up-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(relationship="follow_up", target_index=0)
    orchestrator = _SequentialOrchestrator(
        model,
        (
            "I can look up nearby showtimes for Upper West Side / 10024.",
            "Using Upper West Side / 10024 for showtimes.",
        ),
    )
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Movies playing tonight?",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    first_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat Yes",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        reply_context={
            "platform": "telegram",
            "reply_to_message_id": "telegram-message-1",
            "reply_to_from_bot": True,
            "reply_to_text": first,
        },
    )
    follow_up_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])
    second_history = orchestrator.calls[-1]["conversation_history"]

    assert first == "I can look up nearby showtimes for Upper West Side / 10024."
    assert second == "Using Upper West Side / 10024 for showtimes."
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id == first_turn_id
    assert _history_has_text(second_history, role="user", text="Movies playing tonight?")
    assert _history_has_text(second_history, role="assistant", text="I can look up nearby showtimes for Upper West Side / 10024.")


def test_telegram_reply_to_older_bot_message_does_not_use_active_weather_context_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-older-reply-anchor-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(relationship="follow_up", target_index=0)
    email_reply = (
        "I didn't find email evidence of a Zelle payment. "
        "If you want, I can run one deeper account search."
    )
    weather_reply = "Tomorrow looks sunny. If you want, I can give an hour-by-hour breakdown."
    orchestrator = _SequentialOrchestrator(
        model,
        (
            email_reply,
            weather_reply,
            "I will run the deeper account search from the quoted email result.",
        ),
    )
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Did I make a payment?",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    quoted_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat What's the weather tomorrow?",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    weather_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    third = chat_operator._render_chat_turn(
        runtime,
        message="/chat Sí",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        reply_context={
            "platform": "telegram",
            "reply_to_message_id": 17022,
            "reply_to_from_bot": True,
            "reply_to_text": first,
        },
        turn_dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL,
            disposition=ConversationTurnDisposition.INDEPENDENT,
            reason="explicit_reply_context",
        ),
    )
    reply_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])
    third_history = orchestrator.calls[-1]["conversation_history"]
    history_sequence = _history_text_sequence(third_history)

    assert first == email_reply
    assert second == weather_reply
    assert third == "I will run the deeper account search from the quoted email result."
    assert reply_turn is not None
    assert reply_turn.parent_turn_id == quoted_turn_id
    assert reply_turn.turn_id != weather_turn_id
    assert reply_turn.disposition is ConversationTurnDisposition.CONTINUE
    assert reply_turn.disposition_reason == "dispatch:explicit_reply_context"
    assert ("assistant", weather_reply) not in history_sequence
    assert history_sequence[-1] == ("assistant", email_reply)


def test_telegram_reply_anchor_blocks_saved_history_retry_to_unrelated_tool_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-reply-anchor-saved-history-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(relationship="follow_up", target_index=0)
    quoted_browser_reply = (
        "I opened the conversation entry, but I still need to read the live thread. "
        "If you want, I can keep trying to open that thread."
    )
    unrelated_email_reply = "I checked your inbox for unread updates."
    ambiguous_first_model_reply = "Which saved item should I use?\n1. Email receipt\n2. Prior file"
    stale_retry_reply = "WRONG: I used the stale email search target."
    orchestrator = _SequentialOrchestrator(
        model,
        (
            quoted_browser_reply,
            unrelated_email_reply,
            ambiguous_first_model_reply,
            stale_retry_reply,
        ),
    )
    stale_history_context = "\n".join(
        [
            "Automatic saved-chat lookup for this turn found candidate prior turns.",
            "Recent runtime-evidence-backed saved turns (ranked by relevance and recency):",
            "Evidence 1. tool=email_search status=completed",
            "Structured saved reference candidates (ranked by relevance and recency):",
            'Target 1: {"tool": "email_search", "type": "connector"}',
            "Target source assistant: I checked your inbox for unread updates.",
            "Candidate prior turns:",
        ]
    )
    monkeypatch.setattr(
        chat_operator,
        "_automatic_saved_chat_history_prompt",
        lambda *args, **kwargs: stale_history_context,
    )
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Open this app thread and summarize the last message.",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    quoted_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat Check unread inbox updates.",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    third = chat_operator._render_chat_turn(
        runtime,
        message="/chat Check again",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        reply_context={
            "platform": "telegram",
            "reply_to_message_id": 17285,
            "reply_to_from_bot": True,
            "reply_to_text": first,
        },
        turn_dispatch_decision=TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL,
            disposition=ConversationTurnDisposition.INDEPENDENT,
            reason="explicit_reply_context",
        ),
    )
    reply_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])
    third_history = orchestrator.calls[-1]["conversation_history"]
    history_sequence = _history_text_sequence(third_history)

    assert first == quoted_browser_reply
    assert second == unrelated_email_reply
    assert third == ambiguous_first_model_reply
    assert len(orchestrator.calls) == 3
    assert reply_turn is not None
    assert reply_turn.parent_turn_id == quoted_turn_id
    assert reply_turn.disposition is ConversationTurnDisposition.CONTINUE
    assert reply_turn.disposition_reason == "dispatch:explicit_reply_context"
    assert ("assistant", unrelated_email_reply) not in history_sequence
    assert history_sequence[-1] == ("assistant", quoted_browser_reply)


def test_telegram_compact_reply_continues_numbered_option_prompt_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-numbered-option-follow-up-runtime.db")
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(relationship="follow_up", target_index=0)
    previous_assistant = (
        "I can keep going by:\n"
        "1. Searching the merchant name\n"
        "2. Opening the item page\n"
        "Reply 1 and I will continue."
    )
    orchestrator = _SequentialOrchestrator(
        model,
        (
            previous_assistant,
            "Continuing with the merchant and item context.",
        ),
    )
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Add the identified items to my cart.",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    first_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat sí",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
    )
    follow_up_turn = runtime.store.get_conversation_turn(runtime.store.get_conversation_head("telegram:100001")["active_turn_id"])
    second_history = orchestrator.calls[-1]["conversation_history"]

    assert first == previous_assistant
    assert second == "Continuing with the merchant and item context."
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id == first_turn_id
    assert _history_has_text(second_history, role="user", text="Add the identified items to my cart.")
    assert _history_has_text(second_history, role="assistant", text=previous_assistant)


def test_telegram_no_active_dispatch_preserves_completed_turn_correction_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator, turn_context_policy
    from nullion.config import load_settings
    from nullion.conversation_runtime import ConversationTurnDisposition
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy

    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-completed-turn-correction-runtime.db")
    registry = ToolRegistry()
    invoked_tools: list[str] = []
    registry.register(
        ToolSpec(
            name="email_search",
            description="Search connected mail.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("connector", "account_read"),
        ),
        lambda invocation: (
            invoked_tools.append(invocation.tool_name)
            or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"results": []})
        ),
    )
    runtime.active_tool_registry = registry
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["email_search"],
                "active_app_ids": ["google-mail"],
            }
        ],
    )
    settings = load_settings(
        env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"}
    )
    model = _StructuredDispatchModel(relationship="follow_up", effect="revise", target_index=0)
    previous_assistant = (
        "If you want, I can find you:\n"
        "1. a lawn mower\n"
        "2. a lawn roller\n"
        "3. both, with Amazon links"
    )
    corrected_reply = "Understood — you mean a tobacco rolling machine, not lawn equipment."
    orchestrator = _SequentialOrchestrator(model, (previous_assistant, corrected_reply))
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)
    no_active = TurnDispatchDecision(
        policy=TurnDispatchPolicy.PARALLEL,
        disposition=ConversationTurnDisposition.INDEPENDENT,
        reason="no_active_turn",
    )

    first = chat_operator._render_chat_turn(
        runtime,
        message="/chat Find me a machine that rolls grass",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        turn_dispatch_decision=no_active,
    )
    first_turn_id = runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    second = chat_operator._render_chat_turn(
        runtime,
        message="/chat I meant for rolling tobacco",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
        append_activity_trace=False,
        turn_dispatch_decision=no_active,
    )
    follow_up_turn = runtime.store.get_conversation_turn(
        runtime.store.get_conversation_head("telegram:100001")["active_turn_id"]
    )
    second_history = orchestrator.calls[-1]["conversation_history"]
    visible_tool_names = {
        str(definition.get("name") or "")
        for definition in orchestrator.calls[-1]["tool_registry"].list_tool_definitions()
    }

    assert first == previous_assistant
    assert second == corrected_reply
    assert follow_up_turn is not None
    assert follow_up_turn.parent_turn_id == first_turn_id
    assert follow_up_turn.disposition is ConversationTurnDisposition.REVISE
    assert _history_has_text(second_history, role="user", text="Find me a machine that rolls grass")
    assert _history_has_text(second_history, role="assistant", text=previous_assistant)
    assert "email_search" not in visible_tool_names
    assert invoked_tools == []


def test_telegram_reply_to_scheduled_task_card_delete_requires_approval_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator, crons
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, register_cron_tools

    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "crons.db")
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-cron-reply-runtime.db")
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
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(
        relationship="follow_up",
        target_index=0,
        scheduler_action="mutate",
        requested_tool_names=("delete_cron",),
        required_tool_names=("delete_cron",),
    )
    orchestrator = _SequentialOrchestrator(model, ("The model should not ask which cron.",))
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Delete this cron",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
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
    assert orchestrator.calls == []

def test_telegram_reply_to_scheduled_task_card_disables_quoted_cron_e2e(tmp_path, monkeypatch) -> None:
    from nullion import chat_operator, crons
    from nullion.config import load_settings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry, register_cron_tools

    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "crons.db")
    runtime = bootstrap_persistent_runtime(tmp_path / "telegram-cron-disable-reply-runtime.db")
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
    crons.add_cron(
        "Email deadline triage",
        "0 8 * * *",
        "Check email deadlines.",
        workspace_id="workspace_admin",
    )
    settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
    model = _StructuredDispatchModel(
        relationship="follow_up",
        target_index=0,
        scheduler_action="mutate",
        scheduler_toggle_enabled=False,
        requested_tool_names=("toggle_cron",),
        required_tool_names=("toggle_cron",),
    )
    orchestrator = _SequentialOrchestrator(model, ("The model should not ask which cron.",))
    _patch_chat_operator_for_direct_turn(monkeypatch, chat_operator)

    reply = chat_operator._render_chat_turn(
        runtime,
        message="/chat Disable this cron",
        chat_id="100001",
        settings=settings,
        model_client=model,
        agent_orchestrator=orchestrator,
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
    assert [job.name for job in crons.list_crons(workspace_id="workspace_admin")] == [
        "Peak Design shipment tracker",
        "Email deadline triage",
    ]
    assert orchestrator.calls == []

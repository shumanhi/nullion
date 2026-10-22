from __future__ import annotations

import base64
from datetime import UTC, datetime
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import AgentOrchestrator
from nullion.artifacts import artifact_path_for_generated_file
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)
from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


def _registry_with_tool(name: str, handler):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name=name,
            description=name,
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        handler,
    )
    return registry


def test_agent_turn_graph_suspends_on_approval_required_tool(monkeypatch) -> None:
    class Client:
        def create(self, **kwargs):
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "web_fetch",
                        "input": {"url": "https://example.com"},
                    }
                ],
            }

    class Store:
        def __init__(self) -> None:
            self.approvals = {}
            self.suspended = []

        def get_approval_request(self, approval_id):
            return self.approvals.get(approval_id)

        def add_approval_request(self, approval) -> None:
            self.approvals[approval.approval_id] = approval

        def add_suspended_turn(self, turn) -> None:
            self.suspended.append(turn)

    store = Store()
    registry = _registry_with_tool(
        "web_fetch",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    def deny_for_approval(store, invocation, registry):
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "denied",
            {"reason": "approval_required", "approval_id": "ap-graph"},
        )

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", deny_for_approval)

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="fetch this",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
    )

    assert result.suspended_for_approval
    assert result.approval_id == "ap-graph"
    assert len(store.suspended) == 1
    assert store.suspended[0].approval_id == "ap-graph"
    assert store.approvals["ap-graph"].status.value == "pending"


def test_agent_turn_graph_blocks_mixed_calendar_delete_after_completed_create() -> None:
    class Client:
        def create(self, **kwargs):
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-create-1",
                        "name": "calendar_create",
                        "input": {"summary": "PTO hold", "start": "2026-09-03", "end": "2026-09-04"},
                    },
                    {
                        "type": "tool_use",
                        "id": "tool-create-2",
                        "name": "calendar_create",
                        "input": {"summary": "PTO hold", "start": "2026-09-10", "end": "2026-09-11"},
                    },
                    {
                        "type": "tool_use",
                        "id": "tool-delete",
                        "name": "calendar_delete",
                        "input": {"event_id": "event-1", "calendar_id": "primary"},
                    },
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            "calendar_create",
            "Create calendar event",
            ToolRiskLevel.HIGH,
            ToolSideEffectClass.ACCOUNT_WRITE,
            True,
            5,
            capability_tags=("calendar", "connector", "account_write"),
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"message": "Calendar event create completed.", "summary": "PTO hold"},
        ),
    )

    def fail_if_delete_invoked(invocation):
        raise AssertionError("calendar_delete should be blocked before invocation")

    registry.register(
        ToolSpec(
            "calendar_delete",
            "Delete calendar event",
            ToolRiskLevel.HIGH,
            ToolSideEffectClass.ACCOUNT_WRITE,
            True,
            5,
            capability_tags=("calendar", "connector", "account_write"),
        ),
        fail_if_delete_invoked,
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="3",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert result.suspended_for_approval is False
    assert result.approval_id is None
    assert [tool.tool_name for tool in result.tool_results] == [
        "calendar_create",
        "calendar_create",
        "calendar_delete",
    ]
    blocked = result.tool_results[-1]
    assert blocked.status == "blocked"
    assert blocked.output["reason"] == "same_domain_mixed_account_write_after_completed_write"
    assert blocked.output["completed_account_write_tools"] == {"calendar_create": 2}
    assert "calendar create x2" in (result.final_text or "")
    assert "calendar delete" in (result.final_text or "")


def test_agent_turn_graph_suspends_on_direct_approval_required_status(monkeypatch) -> None:
    class Client:
        def create(self, **kwargs):
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "email_send",
                        "input": {"to": ["recipient@example.com"], "subject": "Stage approval", "body": "Test body"},
                    }
                ],
            }

    class Store:
        def __init__(self) -> None:
            self.approvals = {}
            self.suspended = []

        def get_approval_request(self, approval_id):
            return self.approvals.get(approval_id)

        def add_approval_request(self, approval) -> None:
            self.approvals[approval.approval_id] = approval

        def add_suspended_turn(self, turn) -> None:
            self.suspended.append(turn)

    store = Store()
    registry = _registry_with_tool(
        "email_send",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    def direct_approval_required(store, invocation, registry):
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "approval_required",
            {"reason": "approval_required", "requires_approval": True, "approval_id": "ap-direct"},
        )

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", direct_approval_required)

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:stage-approval",
        principal_id="web:stage-approval",
        user_message="send this",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
    )

    assert result.suspended_for_approval
    assert result.approval_id == "ap-direct"
    assert len(store.suspended) == 1
    assert store.suspended[0].approval_id == "ap-direct"
    assert store.approvals["ap-direct"].status.value == "pending"


def test_agent_turn_graph_materializes_orphan_approval_required_result(monkeypatch) -> None:
    class Client:
        def create(self, **kwargs):
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "email_send",
                        "input": {
                            "to": ["recipient@example.com"],
                            "subject": "Stage approval",
                            "body": "Test body",
                        },
                    }
                ],
            }

    class Store:
        def __init__(self) -> None:
            self.approvals = {}
            self.suspended = []

        def get_approval_request(self, approval_id):
            return self.approvals.get(approval_id)

        def add_approval_request(self, approval) -> None:
            self.approvals[approval.approval_id] = approval

        def add_suspended_turn(self, turn) -> None:
            self.suspended.append(turn)

    store = Store()
    registry = _registry_with_tool(
        "email_send",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    def deny_for_approval(store, invocation, registry):
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "denied",
            {
                "reason": "approval_required",
                "requires_approval": True,
            },
            "Approval required for tool: email_send",
        )

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", deny_for_approval)

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:stage-live",
        principal_id="web:stage-live",
        user_message="send the email",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
    )

    assert result.suspended_for_approval
    assert result.approval_id
    assert len(store.suspended) == 1
    assert store.suspended[0].approval_id == result.approval_id
    approval = store.approvals[result.approval_id]
    assert approval.status.value == "pending"
    assert approval.action == "use_tool"
    assert approval.resource == "email_send"
    assert approval.context["tool_name"] == "email_send"
    assert approval.context["tool_arguments"] == {
        "to": ["recipient@example.com"],
        "subject": "Stage approval",
        "body": "Test body",
    }


def test_agent_turn_graph_uses_actionable_policy_store_when_stores_differ(monkeypatch) -> None:
    class Client:
        def create(self, **kwargs):
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "email_send",
                        "input": {
                            "to": ["recipient@example.com"],
                            "subject": "Stage approval",
                            "body": "Test body",
                        },
                    }
                ],
            }

    class Store:
        def __init__(self) -> None:
            self.approvals = {}
            self.suspended = []

        def get_approval_request(self, approval_id):
            return self.approvals.get(approval_id)

        def add_approval_request(self, approval) -> None:
            self.approvals[approval.approval_id] = approval

        def add_suspended_turn(self, turn) -> None:
            self.suspended.append(turn)

    policy_store = Store()
    approval_store = object()
    registry = _registry_with_tool(
        "email_send",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    def deny_for_approval(store, invocation, registry):
        assert store is policy_store
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "denied",
            {
                "reason": "approval_required",
                "requires_approval": True,
                "approval_id": "ap-policy-store",
            },
            "Approval required for tool: email_send",
        )

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", deny_for_approval)

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:stage-live",
        principal_id="web:stage-live",
        user_message="send the email",
        conversation_history=[],
        tool_registry=registry,
        policy_store=policy_store,
        approval_store=approval_store,
    )

    assert result.suspended_for_approval
    assert result.approval_id == "ap-policy-store"
    assert len(policy_store.suspended) == 1
    assert policy_store.approvals["ap-policy-store"].status.value == "pending"


def test_agent_turn_graph_approval_required_beats_completed_artifact_reply(monkeypatch, tmp_path) -> None:
    artifact = tmp_path / "stage_qa_test.csv"
    artifact.write_text("name,value\nqa,1\n", encoding="utf-8")

    class Client:
        def create(self, **kwargs):
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "email_send",
                        "input": {
                            "to": ["recipient@example.com"],
                            "subject": "Stage approval",
                            "body": "Test body",
                            "attachment_paths": [str(artifact)],
                        },
                    }
                ],
            }

    class Store:
        def __init__(self) -> None:
            self.approvals = {}
            self.suspended = []

        def get_approval_request(self, approval_id):
            return self.approvals.get(approval_id)

        def add_approval_request(self, approval) -> None:
            self.approvals[approval.approval_id] = approval

        def add_suspended_turn(self, turn) -> None:
            self.suspended.append(turn)

    store = Store()
    registry = _registry_with_tool(
        "email_send",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    def completed_artifacts_would_hide_approval(state, artifacts):
        return (str(artifact),)

    def deny_for_approval(store, invocation, registry):
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "denied",
            {
                "reason": "approval_required",
                "requires_approval": True,
                "approval_id": "ap-artifact-email",
                "attachment_paths": [str(artifact)],
            },
            "Approval required for tool: email_send",
        )

    monkeypatch.setattr(
        "nullion.agent_orchestrator._completed_required_artifact_paths_for_turn",
        completed_artifacts_would_hide_approval,
    )
    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", deny_for_approval)

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:stage-live",
        principal_id="web:stage-live",
        user_message="send the package",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
    )

    assert result.suspended_for_approval
    assert result.approval_id == "ap-artifact-email"
    assert result.final_text is None
    assert store.suspended[0].approval_id == "ap-artifact-email"
    assert store.approvals["ap-artifact-email"].status.value == "pending"


def test_agent_turn_graph_completed_artifacts_wait_for_scoped_email_send(tmp_path) -> None:
    from nullion import agent_orchestrator as ao

    artifact = tmp_path / "stage_qa_test.csv"
    artifact.write_text("name,value\nqa,1\n", encoding="utf-8")
    state = {
        "user_message": "Deliver this package through the scoped account-write tool.",
        "tool_results": [
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".csv"],
                    "available_tools": ["file_write", "email_send"],
                    "required_tool_names": ["file_write", "email_send"],
                },
            ),
            ToolResult(
                "file-1",
                "file_write",
                "completed",
                {
                    "artifact_path": str(artifact),
                    "artifact_paths": [str(artifact)],
                },
            ),
        ],
        "tool_registry": None,
    }

    assert ao._completed_required_artifact_paths_for_turn(state, [str(artifact)]) == ()
    state["tool_results"].append(
        ToolResult(
            "email-1",
            "email_send",
            "completed",
            {"to": ["recipient@example.com"], "attachment_paths": [str(artifact)]},
        )
    )
    assert ao._completed_required_artifact_paths_for_turn(state, [str(artifact)]) == (str(artifact),)


def test_agent_turn_graph_blocks_emailing_required_artifact_not_created_this_turn(tmp_path, monkeypatch) -> None:
    from nullion import turn_context_policy
    from nullion.turn_context_policy import build_turn_tool_evidence, scoped_turn_tool_registry

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "active_app_ids": ["google-mail"],
                "structured_tools": ["connector_request", "email_send"],
            }
        ],
    )

    stale_artifact = tmp_path / "stale.csv"
    stale_artifact.write_text("old,data\n1,2\n", encoding="utf-8")
    generated_artifact = tmp_path / "generated.csv"

    class Store:
        checkpoint_path = tmp_path / "runtime.db"

        def __init__(self) -> None:
            self.approvals = {}
            self.suspended = []

        def get_active_task_frame_id(self, _conversation_id):
            return None

        def get_approval_request(self, approval_id):
            return self.approvals.get(approval_id)

        def add_approval_request(self, approval) -> None:
            self.approvals[approval.approval_id] = approval

        def add_suspended_turn(self, turn) -> None:
            self.suspended.append(turn)

    class Client:
        def __init__(self) -> None:
            self.calls = 0
            self.saw_missing_artifact_guard = False

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-1",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["local_files", "connector"],
                                "source_user_requested": True,
                                "source_evidence": "mi correo",
                                "connector_app_ids": ["google-mail"],
                                "tool_names": ["file_write", "email_send"],
                                "required_tool_names": ["file_write", "email_send"],
                                "artifact_extensions": [".csv"],
                            },
                        }
                    ],
                }
            message_text = repr(kwargs.get("messages") or ())
            if (
                not self.saw_missing_artifact_guard
                and "email_attachment_artifacts_missing_current_turn" in message_text
            ):
                self.saw_missing_artifact_guard = True
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "file-1",
                            "name": "file_write",
                            "input": {"path": str(generated_artifact), "content": "fresh,data\n3,4\n"},
                        }
                    ],
                }
            if self.saw_missing_artifact_guard:
                attachment = generated_artifact
            else:
                attachment = stale_artifact
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": f"email-{self.calls}",
                        "name": "email_send",
                        "input": {
                            "to": ["recipient@example.com"],
                            "subject": "Stage package",
                            "body": "STAGE QA TEST",
                            "attachment_paths": [str(attachment)],
                        },
                    }
                ],
            }

    email_invoked_attachments: list[str] = []

    def invoke_tool(store, invocation, registry):
        if invocation.tool_name == "email_send":
            attachment = str((invocation.arguments.get("attachment_paths") or [""])[0])
            email_invoked_attachments.append(attachment)
            approval_id = "ap-generated" if attachment == str(generated_artifact) else "ap-stale"
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "denied",
                {
                    "reason": "approval_required",
                    "requires_approval": True,
                    "approval_id": approval_id,
                    "attachment_paths": [attachment],
                },
                "Approval required for tool: email_send",
            )
        return registry.invoke(invocation)

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", invoke_tool)

    registry = ToolRegistry()

    def write_file(invocation):
        path = tmp_path / Path(str(invocation.arguments["path"])).name
        path.write_text(str(invocation.arguments.get("content") or ""), encoding="utf-8")
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"artifact_path": str(path), "artifact_paths": [str(path)]},
        )

    registry.register(
        ToolSpec("file_write", "file_write", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        write_file,
    )
    registry.register(
        ToolSpec("email_send", "email_send", ToolRiskLevel.HIGH, ToolSideEffectClass.ACCOUNT_WRITE, True, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    scoped_registry = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(
            user_message="envia el paquete por mi correo",
            conversation_result=None,
            requested_extensions=(".csv",),
        ),
        model_client=None,
        user_message="envia el paquete por mi correo",
    )

    store = Store()
    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="web:stage-live",
        principal_id="web:stage-live",
        user_message="envia el paquete por mi correo",
        conversation_history=[],
        tool_registry=scoped_registry,
        policy_store=store,
        approval_store=store,
    )

    assert client.saw_missing_artifact_guard is True, [
        (tool.tool_name, tool.status, tool.output) for tool in result.tool_results
    ]
    assert result.suspended_for_approval is True
    assert result.approval_id == "ap-generated"
    assert "ap-stale" not in store.approvals
    assert "ap-generated" in store.approvals
    assert [tool.tool_name for tool in result.tool_results] == [
        "request_tool_scope",
        "email_send",
        "file_write",
        "email_send",
    ]
    assert result.tool_results[1].output["reason"] == "email_attachment_artifacts_missing_current_turn"
    assert email_invoked_attachments == [str(generated_artifact)]


def test_agent_turn_graph_emits_tool_activity_before_blocking_tool() -> None:
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
                            "id": "tool-1",
                            "name": "email_search",
                            "input": {"query": "from:sender", "limit": 1},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "I checked your email. No matching messages found."}]}

    registry = _registry_with_tool(
        "email_search",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"resultSizeEstimate": 1, "results": []},
        ),
    )
    activity: list[ToolResult] = []

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="look up the shipment",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        tool_result_callback=activity.append,
    )

    assert result.final_text == "✉️ I checked your email. No matching messages found."
    assert [(item.tool_name, item.status) for item in activity] == [
        ("email_search", "running"),
        ("email_search", "completed"),
    ]
    assert activity[0].invocation_id == activity[1].invocation_id


def test_agent_turn_graph_returns_to_model_after_calendar_list_evidence() -> None:
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
                            "id": "tool-1",
                            "name": "calendar_list",
                            "input": {"start": "2026-06-02T00:00:00-04:00", "end": "2026-06-09T00:00:00-04:00"},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "You have one calendar event: Initial consultation."}],
            }

    client = Client()
    registry = _registry_with_tool(
        "calendar_list",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "results": [
                    {
                        "summary": "Initial consultation",
                        "start": {"dateTime": "2026-06-04T10:00:00-04:00"},
                        "end": {"dateTime": "2026-06-04T10:30:00-04:00"},
                    }
                ]
            },
        ),
    )

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="web:calendar",
        principal_id="web:user",
        user_message="Check my calendar",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert client.calls == 2
    assert "I checked your calendar" not in (result.final_text or "")
    assert "Initial consultation" in (result.final_text or "")


def test_agent_turn_graph_does_not_finish_unrelated_turn_with_calendar_summary() -> None:
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
                            "id": "tool-1",
                            "name": "calendar_list",
                            "input": {"start": "2026-06-03", "end": "2026-06-04"},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "I need computer access before I can change local cache files."}],
            }

    client = Client()
    registry = _registry_with_tool(
        "calendar_list",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"results": [{"summary": "Manage Amazon Subscribe & Save delivery", "start": {"date": "2026-06-03"}}]},
        ),
    )

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:prod-cleanup-regression",
        principal_id="telegram:prod-cleanup-regression",
        user_message="Need local computer maintenance.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert client.calls == 2
    assert result.final_text == "I need computer access before I can change local cache files."
    assert "Manage Amazon Subscribe" not in result.final_text


def test_agent_turn_graph_does_not_directly_complete_email_search() -> None:
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
                            "id": "tool-1",
                            "name": "email_search",
                            "input": {"query": "in:inbox", "limit": 5},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "I checked the inbox."}]}

    client = Client()
    registry = _registry_with_tool(
        "email_search",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"resultSizeEstimate": 1, "results": [{"subject": "Statement", "from": "Bank"}]},
        ),
    )

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="web:email",
        principal_id="web:user",
        user_message="Check my email",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert client.calls == 2
    assert "I checked the inbox" in (result.final_text or "")


def test_agent_turn_graph_dedupes_repeated_completed_email_search() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls <= 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"tool-{self.calls}",
                            "name": "email_search",
                            "input": {"query": "in:inbox is:unread newer_than:1d", "limit": 10},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "late model summary"}]}

    client = Client()
    invocations = 0

    def email_search(invocation):
        nonlocal invocations
        invocations += 1
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"resultSizeEstimate": 1, "results": [{"subject": "Statement", "from": "Bank"}]},
        )

    registry = _registry_with_tool("email_search", email_search)

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="web:email",
        principal_id="web:user",
        user_message="Check unread email",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert client.calls == 3
    assert invocations == 1
    assert result.final_text == "late model summary"


def test_agent_turn_graph_emits_foreground_ack_when_model_selects_tool() -> None:
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
                            "id": "tool-1",
                            "name": "calendar_search",
                            "input": {"query": "today"},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "done"}]}

    registry = _registry_with_tool(
        "calendar_search",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"events": []}),
    )
    events: list[dict[str, object]] = []
    activity: list[ToolResult] = []

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="neutral request text",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        tool_result_callback=activity.append,
        foreground_event_callback=events.append,
    )

    assert result.final_text == "done"
    assert events == [{"id": "foreground-working-ack", "label": "Preparing work", "status": "running"}]
    assert [(item.tool_name, item.status) for item in activity] == [
        ("calendar_search", "running"),
        ("calendar_search", "completed"),
    ]


def test_agent_turn_graph_skips_foreground_ack_for_no_tool_turn() -> None:
    class Client:
        def create(self, **kwargs):
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "hello"}]}

    events: list[dict[str, object]] = []

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="short plain turn",
        conversation_history=[],
        tool_registry=ToolRegistry(),
        policy_store=None,
        approval_store=None,
        foreground_event_callback=events.append,
    )

    assert result.final_text == "hello"
    assert events == []


def test_agent_turn_graph_keeps_file_write_path_internal_without_artifact_scope(tmp_path) -> None:
    output_path = tmp_path / "hermes_model_update_note.txt"

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
                            "id": "tool-1",
                            "name": "file_write",
                            "input": {"path": str(output_path), "content": "state note"},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Done."}]}

    registry = _registry_with_tool(
        "file_write",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"path": str(output_path)},
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="structured config edit continuation",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert result.final_text == "Done."
    assert result.artifacts == []


def test_agent_turn_graph_records_file_write_path_with_typed_artifact_scope(tmp_path) -> None:
    from nullion.turn_context_policy import build_turn_tool_evidence, scoped_turn_tool_registry

    output_path = tmp_path / "requested_report.txt"

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
                            "id": "tool-1",
                            "name": "file_write",
                            "input": {"path": str(output_path), "content": "report"},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Done."}]}

    registry = _registry_with_tool(
        "file_write",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"path": str(output_path)},
        ),
    )
    scoped_registry = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(
            user_message="neutral",
            conversation_result=None,
            requested_extensions=(".txt",),
        ),
        model_client=None,
        user_message="neutral",
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="neutral",
        conversation_history=[],
        tool_registry=scoped_registry,
        policy_store=None,
        approval_store=None,
    )

    assert result.final_text == "Done."
    assert result.artifacts == [str(output_path)]


def test_agent_turn_graph_blocks_plain_spreadsheet_after_media_embed_failure() -> None:
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
                            "id": "tool-1",
                            "name": "spreadsheet_create",
                            "input": {"rows": [{"name": "A", "image_path": "https://example.test/a.jpg"}]},
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool-2",
                            "name": "spreadsheet_create",
                            "input": {"rows": [{"name": "A"}]},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Still needs embedded media."}]}

    spreadsheet_invocations = []
    registry = _registry_with_tool(
        "spreadsheet_create",
        lambda invocation: spreadsheet_invocations.append(invocation) or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "failed",
            {
                "reason": "remote_image_paths_not_supported",
                "remote_image_urls": ["https://example.test/a.jpg"],
                "skipped_images": ["https://example.test/a.jpg"],
            },
            "spreadsheet_create image_paths and screenshot_paths must be local artifact file paths.",
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="表を作って",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert len(spreadsheet_invocations) == 1
    assert [tool.output.get("reason") for tool in result.tool_results] == [
        "remote_image_paths_not_supported",
        "artifact_media_required_by_prior_failure",
    ]


def test_agent_turn_graph_blocks_first_plain_spreadsheet_from_embedded_media_scope() -> None:
    from nullion.turn_context_policy import build_turn_tool_evidence, scoped_turn_tool_registry

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
                            "id": "tool-1",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["local_files"],
                                "artifact_extensions": [".xlsx"],
                                "embedded_media_artifact_extensions": [".xlsx"],
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool-2",
                            "name": "spreadsheet_create",
                            "input": {"rows": [{"name": "A"}]},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Still needs embedded media."}]}

    spreadsheet_invocations = []
    registry = _registry_with_tool(
        "spreadsheet_create",
        lambda invocation: spreadsheet_invocations.append(invocation) or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"artifact_path": "/tmp/plain.xlsx", "embedded_images": [], "embedded_screenshots": []},
        ),
    )
    scoped_registry = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="중립 텍스트",
        conversation_history=[],
        tool_registry=scoped_registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert spreadsheet_invocations == []
    assert [tool.output.get("reason") for tool in result.tool_results] == [
        None,
        "artifact_media_required_by_turn_contract",
    ]
    assert result.tool_results[0].output["embedded_media_artifact_extensions"] == [".xlsx"]


def test_agent_turn_graph_blocks_plain_pdf_after_browser_image_collection() -> None:
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
                            "id": "tool-1",
                            "name": "browser_image_collect",
                            "input": {"image_urls": ["https://example.test/product.png"]},
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool-2",
                            "name": "pdf_create",
                            "input": {"text_pages": ["Product report"]},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Still needs embedded media."}]}

    pdf_invocations = []
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="browser_image_collect",
            description="browser_image_collect",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "image_paths": ["/tmp/product-1.png"],
                "artifact_paths": ["/tmp/product-1.png"],
            },
        ),
    )
    registry.register(
        ToolSpec(
            name="pdf_create",
            description="pdf_create",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        lambda invocation: pdf_invocations.append(invocation) or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"artifact_path": "/tmp/plain.pdf", "source_image_paths": [], "source_screenshot_paths": []},
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="neutral artifact request",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert pdf_invocations == []
    assert [tool.output.get("reason") for tool in result.tool_results] == [
        None,
        "artifact_media_required_by_prior_collection",
    ]
    assert result.tool_results[1].output["embedded_media_artifact_extensions"] == [".pdf"]


def test_agent_turn_graph_recovers_browser_image_collection_from_rendered_dom() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0
            self.second_messages = None

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool-1",
                            "name": "browser_image_collect",
                            "input": {
                                "page_url": "https://example.test/page",
                                "max_images": 1,
                                "output_stem": "rendered-product",
                                "quality_profile": "any",
                            },
                        }
                    ],
                }
            self.second_messages = kwargs["messages"]
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "image saved"}]}

    invocations: list[tuple[str, dict[str, object]]] = []
    registry = ToolRegistry()

    def register(name, handler):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
            ),
            handler,
        )

    def image_collect(invocation):
        invocations.append((invocation.tool_name, dict(invocation.arguments)))
        if invocation.arguments.get("page_url"):
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {
                    "reason": "image_collection_failed",
                    "candidate_count": 2,
                    "image_paths": [],
                    "artifact_paths": [],
                    "images": [],
                },
                "browser_image_collect could not save any embeddable local image artifacts.",
            )
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "image_paths": ["/tmp/rendered-product.png"],
                "artifact_paths": ["/tmp/rendered-product.png"],
                "saved_count": 1,
            },
        )

    register("browser_image_collect", image_collect)
    register(
        "browser_navigate",
        lambda invocation: invocations.append((invocation.tool_name, dict(invocation.arguments)))
        or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"session_id": "s1"}),
    )
    register(
        "browser_run_js",
        lambda invocation: invocations.append((invocation.tool_name, dict(invocation.arguments)))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"result": [{"url": "https://cdn.example.test/rendered-product.png", "visible": True, "area": 40000}]},
        ),
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="web:rendered-image",
        principal_id="web:rendered-image",
        user_message="make an artifact with a page image",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert result.final_text == "image saved"
    assert [tool.tool_name for tool in result.tool_results] == [
        "browser_image_collect",
        "browser_navigate",
        "browser_run_js",
        "browser_image_collect",
    ]
    assert invocations[-1] == (
        "browser_image_collect",
        {
            "image_urls": ["https://cdn.example.test/rendered-product.png"],
            "max_images": 1,
            "output_stem": "rendered-product",
            "quality_profile": "any",
        },
    )
    assert result.tool_results[-1].output["rendered_recovery"]["source"] == "browser_rendered_dom"
    tool_result_text = client.second_messages[-1]["content"][0]["content"][0]["text"]
    assert "rendered_recovery" in tool_result_text
    assert "/tmp/rendered-product.png" in result.artifacts


def test_agent_turn_graph_recovers_browser_image_collection_when_static_page_has_no_candidates() -> None:
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
                            "id": "tool-1",
                            "name": "browser_image_collect",
                            "input": {
                                "page_url": "https://example.test/rendered-only",
                                "max_images": 1,
                                "output_stem": "rendered-only",
                                "quality_profile": "any",
                            },
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "image saved"}]}

    invocations: list[tuple[str, dict[str, object]]] = []
    registry = ToolRegistry()

    def register(name, handler):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
            ),
            handler,
        )

    def image_collect(invocation):
        invocations.append((invocation.tool_name, dict(invocation.arguments)))
        if invocation.arguments.get("page_url"):
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {"reason": "no_image_candidates", "image_paths": [], "artifact_paths": []},
                "browser_image_collect requires page_url, html, or image_urls that contain image asset URLs.",
            )
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"image_paths": ["/tmp/rendered-only.png"], "artifact_paths": ["/tmp/rendered-only.png"]},
        )

    register("browser_image_collect", image_collect)
    register(
        "browser_navigate",
        lambda invocation: invocations.append((invocation.tool_name, dict(invocation.arguments)))
        or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"session_id": "s1"}),
    )
    register(
        "browser_run_js",
        lambda invocation: invocations.append((invocation.tool_name, dict(invocation.arguments)))
        or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"result": [{"url": "https://cdn.example.test/rendered-only.png", "visible": True, "area": 40000}]}),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:no-static-images",
        principal_id="web:no-static-images",
        user_message="make an artifact with a rendered page image",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert result.final_text == "image saved"
    assert [tool.tool_name for tool in result.tool_results] == [
        "browser_image_collect",
        "browser_navigate",
        "browser_run_js",
        "browser_image_collect",
    ]
    assert result.tool_results[-1].output["rendered_recovery"]["source"] == "browser_rendered_dom"
    assert "/tmp/rendered-only.png" in result.artifacts


def test_agent_turn_graph_crops_rendered_image_as_last_resort_when_asset_fetch_fails(tmp_path) -> None:
    Image = pytest.importorskip("PIL.Image")

    screenshot_path = tmp_path / "page.png"
    image = Image.new("RGB", (400, 300), "white")
    for x in range(50, 250):
        for y in range(40, 180):
            image.putpixel((x, y), (25, 120, 220))
    image.save(screenshot_path)

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
                            "id": "tool-1",
                            "name": "browser_image_collect",
                            "input": {
                                "page_url": "https://example.test/blocked-assets",
                                "max_images": 1,
                                "output_stem": "blocked-assets",
                                "quality_profile": "any",
                            },
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "image saved"}]}

    invocations: list[tuple[str, dict[str, object]]] = []
    registry = ToolRegistry()

    def register(name, handler):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
            ),
            handler,
        )

    def image_collect(invocation):
        invocations.append((invocation.tool_name, dict(invocation.arguments)))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "failed",
            {"reason": "image_collection_failed", "image_paths": [], "artifact_paths": []},
            "browser_image_collect could not save any embeddable local image artifacts.",
        )

    register("browser_image_collect", image_collect)
    register(
        "browser_navigate",
        lambda invocation: invocations.append((invocation.tool_name, dict(invocation.arguments)))
        or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"session_id": "s1"}),
    )
    register(
        "browser_run_js",
        lambda invocation: invocations.append((invocation.tool_name, dict(invocation.arguments)))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"result": [{"url": "https://cdn.example.test/blocked.png", "visible": True, "area": 28000, "x": 50, "y": 40, "width": 200, "height": 140}]},
        ),
    )
    register(
        "browser_screenshot",
        lambda invocation: invocations.append((invocation.tool_name, dict(invocation.arguments)))
        or ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "path": str(screenshot_path),
                "artifact_path": str(screenshot_path),
                "artifact_paths": [str(screenshot_path)],
                "viewport_width": 400,
                "viewport_height": 300,
                "image_width": 400,
                "image_height": 300,
                "page_url": "https://example.test/blocked-assets",
            },
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:crop-recovery",
        principal_id="web:crop-recovery",
        user_message="make an artifact with a rendered page image",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert result.final_text == "image saved"
    assert [tool.tool_name for tool in result.tool_results] == [
        "browser_image_collect",
        "browser_navigate",
        "browser_run_js",
        "browser_image_collect",
        "browser_screenshot",
        "browser_image_collect",
    ]
    cropped = result.tool_results[-1]
    assert cropped.output["rendered_recovery"]["source"] == "rendered_screenshot_crop"
    assert cropped.output["images"][0]["source_screenshot_path"] == str(screenshot_path)
    cropped_path = cropped.output["image_paths"][0]
    with Image.open(cropped_path) as cropped_image:
        assert cropped_image.size == (200, 140)
    assert cropped.output["artifact_descriptors"][0]["role"] == "source"
    assert cropped_path not in result.artifacts


def test_agent_turn_graph_resume_executes_pending_tool_before_model() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            assert any(message["role"] == "user" and message["content"][0]["type"] == "tool_result" for message in kwargs["messages"])
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "done with resumed tool"}]}

    client = Client()
    registry = _registry_with_tool(
        "workspace_summary",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"delivery_text": "workspace checked"},
        ),
    )
    messages_snapshot = [
        {"role": "user", "content": [{"type": "text", "text": "check workspace"}]},
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "workspace_summary",
                    "input": {},
                }
            ],
        },
    ]

    result = AgentOrchestrator(model_client=client).resume_turn(
        conversation_id="web:operator",
        principal_id="operator",
        user_message="check workspace",
        messages_snapshot=messages_snapshot,
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert client.calls == 1
    assert result.final_text == "done with resumed tool"
    assert [tool.tool_name for tool in result.tool_results] == ["workspace_summary"]


def test_agent_turn_graph_run_turn_cleans_preallocated_scope_when_graph_invoke_raises(monkeypatch) -> None:
    cleanup_scopes: list[str | None] = []
    invoked_scopes: list[str] = []
    registry = ToolRegistry()
    registry.register_cleanup_hook(cleanup_scopes.append)

    class ExplodingGraph:
        def invoke(self, state, *, config):  # noqa: ANN001
            del config
            invoked_scopes.append(state["cleanup_scope"])
            raise RuntimeError("graph invoke failed")

    monkeypatch.setattr(
        "nullion.agent_orchestrator._compiled_agent_turn_graph",
        lambda: ExplodingGraph(),
    )

    with pytest.raises(RuntimeError, match="graph invoke failed"):
        AgentOrchestrator(model_client=SimpleNamespace()).run_turn(
            conversation_id="web:cleanup",
            principal_id="web:cleanup",
            user_message="neutral",
            conversation_history=[],
            tool_registry=registry,
            policy_store=None,
            approval_store=None,
        )

    assert len(invoked_scopes) == 1
    assert cleanup_scopes == invoked_scopes


def test_agent_turn_graph_resume_turn_cleans_preallocated_scope_when_graph_invoke_raises(monkeypatch) -> None:
    cleanup_scopes: list[str | None] = []
    invoked_scopes: list[str] = []
    registry = ToolRegistry()
    registry.register_cleanup_hook(cleanup_scopes.append)

    class ExplodingGraph:
        def invoke(self, state, *, config):  # noqa: ANN001
            del config
            invoked_scopes.append(state["cleanup_scope"])
            raise RuntimeError("resume graph invoke failed")

    monkeypatch.setattr(
        "nullion.agent_orchestrator._compiled_agent_turn_graph",
        lambda: ExplodingGraph(),
    )

    with pytest.raises(RuntimeError, match="resume graph invoke failed"):
        AgentOrchestrator(model_client=SimpleNamespace()).resume_turn(
            conversation_id="web:cleanup",
            principal_id="web:cleanup",
            user_message="neutral",
            messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "neutral"}]}],
            tool_registry=registry,
            policy_store=None,
            approval_store=None,
        )

    assert len(invoked_scopes) == 1
    assert cleanup_scopes == invoked_scopes


def test_agent_turn_graph_cleanup_runs_once_after_successful_graph_completion() -> None:
    cleanup_scopes: list[str | None] = []
    registry = ToolRegistry()
    registry.register_cleanup_hook(cleanup_scopes.append)

    class Client:
        def create(self, **kwargs):  # noqa: ANN003
            del kwargs
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "done"}]}

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:cleanup",
        principal_id="web:cleanup",
        user_message="neutral",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert result.final_text
    assert len(cleanup_scopes) == 1
    assert str(cleanup_scopes[0] or "").startswith("turn-")


def test_agent_turn_graph_repairs_missing_required_attachment(tmp_path, monkeypatch) -> None:
    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="telegram:123",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.GENERATE_ARTIFACT,
        target=TaskFrameTarget(kind="file", value="brief"),
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(artifact_kind="pdf", delivery_mode="attachment"),
        finish=TaskFrameFinishCriteria(requires_artifact_delivery=True, required_artifact_kind="pdf"),
        summary="Create brief",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    class Store:
        checkpoint_path = tmp_path / "runtime.db"

        def get_active_task_frame_id(self, conversation_id):
            return "frame-1" if conversation_id == "telegram:123" else None

        def get_task_frame(self, frame_id):
            return frame if frame_id == "frame-1" else None

    store = Store()

    class Client:
        def __init__(self) -> None:
            self.calls = 0
            self.repair_requested = False

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "tool-1", "name": "inspect", "input": {}}],
                }
            last_content = kwargs["messages"][-1]["content"]
            last_text = last_content[0].get("text", "") if isinstance(last_content, list) and last_content else ""
            if "not deliverable yet" in last_text:
                self.repair_requested = True
                return {
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "tool-2", "name": "write_pdf", "input": {}}],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Done."}]}

    def invoke_tool(store, invocation, registry):
        return registry.invoke(invocation)

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", invoke_tool)

    registry = ToolRegistry()
    registry.register(
        ToolSpec("inspect", "inspect", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"summary": "missing"}),
    )

    def write_pdf(invocation):
        path = artifact_path_for_generated_file(store, suffix=".pdf")
        path.write_text("pdf bytes", encoding="utf-8")
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"artifact_path": str(path)})

    registry.register(
        ToolSpec("write_pdf", "write_pdf", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        write_pdf,
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="create the PDF",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
    )

    assert client.repair_requested is True


def test_agent_turn_graph_enforces_request_scope_artifact_contract(tmp_path, monkeypatch) -> None:
    from nullion.turn_context_policy import build_turn_tool_evidence, scoped_turn_tool_registry

    class Store:
        checkpoint_path = tmp_path / "runtime.db"

        def get_active_task_frame_id(self, _conversation_id):
            return None

    store = Store()
    from nullion.artifacts import artifact_root_for_principal
    output_path = artifact_root_for_principal("telegram_chat") / "stage_scope_contract.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    class Client:
        def __init__(self) -> None:
            self.calls = 0
            self.saw_missing_contract_nudge = False

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-1",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["local_files"],
                                "tool_names": ["spreadsheet_create"],
                                "required_tool_names": ["spreadsheet_create"],
                                "artifact_extensions": [".xlsx"],
                            },
                        }
                    ],
                }
            last_content = kwargs["messages"][-1]["content"]
            last_text = last_content[0].get("text", "") if isinstance(last_content, list) and last_content else ""
            if "not complete yet" in last_text or "not deliverable yet" in last_text:
                self.saw_missing_contract_nudge = True
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "sheet-1",
                            "name": "spreadsheet_create",
                            "input": {"rows": [{"name": "A", "value": 1}]},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": "Created the requested spreadsheet.\nMEDIA:" + str(output_path),
                    }
                ],
            }

    def invoke_tool(store, invocation, registry):
        return registry.invoke(invocation)

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", invoke_tool)

    registry = ToolRegistry()
    def create_spreadsheet(invocation):
        from openpyxl import Workbook
        workbook = Workbook()
        workbook.active.append(["Name", "Value"])
        workbook.active.append(["A", 1])
        workbook.save(output_path)
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"artifact_path": str(output_path)},
        )

    registry.register(
        ToolSpec("spreadsheet_create", "spreadsheet_create", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        create_spreadsheet,
    )
    scoped_registry = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="neutral",
        conversation_history=[],
        tool_registry=scoped_registry,
        policy_store=store,
        approval_store=store,
    )

    assert client.saw_missing_contract_nudge is True
    assert [tool.tool_name for tool in result.tool_results] == ["request_tool_scope", "spreadsheet_create"]
    assert result.artifacts == [str(output_path)]
    assert result.final_text == "Created the requested spreadsheet.\nMEDIA:" + str(output_path)


def test_agent_turn_graph_stops_after_completed_required_artifact(tmp_path, monkeypatch) -> None:
    class Store:
        checkpoint_path = tmp_path / "runtime.db"

    store = Store()

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "tool-1", "name": "pdf_create", "input": {}}],
                }
            return {
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "id": "tool-2", "name": "document_create", "input": {}}],
            }

    def invoke_tool(store, invocation, registry):
        return registry.invoke(invocation)

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", invoke_tool)

    registry = ToolRegistry()

    def write_pdf(invocation):
        path = artifact_path_for_generated_file(store, suffix=".pdf")
        from pypdf import PdfWriter
        writer = PdfWriter()
        writer.add_blank_page(width=300, height=300)
        writer.write(path)
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"artifact_path": str(path), "artifact_delivery_manifest": {"version": 1, "selected_paths": [str(path)], "candidate_paths": [str(path)]}})

    registry.register(
        ToolSpec("pdf_create", "pdf_create", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        write_pdf,
    )
    registry.register(
        ToolSpec("document_create", "document_create", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "must not run"),
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="create the attachment",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
        tool_flow_context={"requires_artifact_delivery": True, "artifact_extensions": [".pdf"]},
    )

    assert client.calls == 1
    assert [tool_result.tool_name for tool_result in result.tool_results] == ["pdf_create"]
    assert result.final_text and "MEDIA:" in result.final_text
    assert result.artifacts and result.artifacts[0].endswith(".pdf")


def test_agent_turn_graph_default_iteration_budget_is_bounded(monkeypatch) -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0
            self.plan_calls = 0
            self.review_calls = 0

        def create(self, **kwargs):
            if str(kwargs.get("system") or "").startswith("You are a material-requirements planner"):
                self.plan_calls += 1
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                '{"all_material_requirements_enumerated":true,'
                                '"material_requirements":[{"requirement":"the requested inspection outcome",'
                                '"requirement_kind":"other",'
                                '"evidence_subject_id":"inspection",'
                                '"eligible_evidence_tool_names":["inspect"]}]}'
                            ),
                        }
                    ],
                }
            if str(kwargs.get("system") or "").startswith("You are a semantic completion reviewer"):
                self.review_calls += 1
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                '{"disposition":"retry","unresolved_requirements":'
                                '["inspection did not produce the requested outcome"],'
                                '"retry_tool_names":[]}'
                            ),
                        }
                    ],
                }
            self.calls += 1
            return {
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "id": f"tool-{self.calls}", "name": "inspect", "input": {"n": self.calls}}],
            }

    class Store:
        pass

    def invoke_tool(store, invocation, registry):
        return registry.invoke(invocation)

    monkeypatch.setattr("nullion.runtime.invoke_tool_with_boundary_policy", invoke_tool)
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "3")

    registry = _registry_with_tool(
        "inspect",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True}),
    )
    client = Client()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="keep inspecting",
        conversation_history=[],
        tool_registry=registry,
        policy_store=Store(),
        approval_store=Store(),
    )

    assert client.calls == 3
    assert client.plan_calls == 1
    assert client.review_calls == 1
    assert result.reached_iteration_limit is True
    assert len(result.tool_results) == 3


_TERMINAL_BROWSER_SESSION = "amazon-product-session"
_TERMINAL_BROWSER_URL = "https://www.amazon.com/dp/B06Y271G1K?th=1"
_TERMINAL_BROWSER_TITLE = "Kate Somerville EradiKate 3% Sulfur Daily Foaming Cleanser"
_TERMINAL_BROWSER_REQUIRED = [_TERMINAL_BROWSER_TITLE, "$39.10", "In Stock"]


def _terminal_browser_assertion_output(*, selector: str = "body") -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "page_state_assertion",
        "session_id": _TERMINAL_BROWSER_SESSION,
        "verified": True,
        "contract": {
            "required": list(_TERMINAL_BROWSER_REQUIRED),
            "forbidden": [],
            "selector": selector,
        },
        "result": {
            "ok": True,
            "url": _TERMINAL_BROWSER_URL,
            "selector": selector,
            "required": [
                {"expected": label, "match": label}
                for label in _TERMINAL_BROWSER_REQUIRED
            ],
            "forbidden": [],
            "missing": [],
            "forbidden_found": [],
        },
    }


def _terminal_browser_detail_output(
    *,
    selector: str | None,
    include_assertion: bool = False,
) -> dict[str, object]:
    output: dict[str, object] = {
        "schema_version": 1,
        "kind": "browser_dom_detail",
        "origin": "runtime_owned_dom_extractor",
        "session_id": _TERMINAL_BROWSER_SESSION,
        "selector": selector,
        "record_scope": "selected_root",
        "page": {"url": _TERMINAL_BROWSER_URL, "title": _TERMINAL_BROWSER_TITLE},
        "record": {
            "title": _TERMINAL_BROWSER_TITLE,
            "url": _TERMINAL_BROWSER_URL,
            "price": "$39.10",
            "availability": "In Stock",
            "details": "ACTIVE INGREDIENT: Sulfur 3.0%; supplied as a foaming cleanser.",
        },
    }
    if include_assertion:
        output["page_state_assertion"] = _terminal_browser_assertion_output(
            selector=selector or "body"
        )
    return output


class _TerminalBrowserSequenceClient:
    def __init__(
        self,
        *,
        review_disposition: str = "complete",
        ground_complete_review: bool = True,
        planned_requirement_count: int = 1,
        covered_requirement_count: int | None = None,
    ) -> None:
        self.calls = 0
        self.plan_calls = 0
        self.review_calls = 0
        self.review_disposition = review_disposition
        self.ground_complete_review = ground_complete_review
        self.planned_requirement_count = planned_requirement_count
        self.covered_requirement_count = covered_requirement_count
        self.sequence = (
            ("browser_extract_detail", {}),
            (
                "browser_run_js",
                {"script": "return {important: document.body.innerText};"},
            ),
            (
                "browser_assert_page_state",
                {
                    "selector": "body",
                    "required": list(_TERMINAL_BROWSER_REQUIRED),
                    "forbidden": [],
                },
            ),
            (
                "browser_type_field",
                {"label": "Search Amazon", "text": "sulfur cleanser"},
            ),
        )

    def create(self, **kwargs):
        if str(kwargs.get("system") or "").startswith("You are a material-requirements planner"):
            self.plan_calls += 1
            requirements = [
                {
                    "requirement": (
                        f"verified details for comparison side {index}"
                        if self.planned_requirement_count > 1
                        else "the requested matching product and its verified details"
                    ),
                    "requirement_kind": (
                        "comparison_side" if self.planned_requirement_count > 1 else "fact"
                    ),
                    "evidence_subject_id": f"subject-{index}",
                    "eligible_evidence_tool_names": ["browser_extract_detail"],
                }
                for index in range(1, self.planned_requirement_count + 1)
            ]
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": __import__("json").dumps(
                            {
                                "all_material_requirements_enumerated": True,
                                "material_requirements": requirements,
                            }
                        ),
                    }
                ],
            }
        if str(kwargs.get("system") or "").startswith("You are a semantic completion reviewer"):
            self.review_calls += 1
            review_payload = __import__("json").loads(
                kwargs["messages"][0]["content"][0]["text"]
            )
            detail_evidence_ids = [
                str(record.get("invocation_id") or "")
                for record in review_payload.get("tool_evidence", [])
                if record.get("tool_name") == "browser_extract_detail"
                and record.get("status") == "completed"
                and str(record.get("invocation_id") or "")
            ]
            unresolved = (
                []
                if self.review_disposition == "complete"
                else ["one requested comparison record is not verified"]
            )
            decision = {
                "disposition": self.review_disposition,
                "unresolved_requirements": unresolved,
                "retry_tool_names": [],
            }
            if self.review_disposition == "complete" and self.ground_complete_review:
                requirement_contract = review_payload["material_requirement_contract"]
                coverage_count = (
                    len(requirement_contract)
                    if self.covered_requirement_count is None
                    else self.covered_requirement_count
                )
                decision.update(
                    {
                        "all_material_requirements_enumerated": True,
                        "material_requirements": [
                            {
                                "requirement_id": row["requirement_id"],
                                "requirement": row["requirement"],
                                "requirement_kind": row["requirement_kind"],
                                "satisfied": True,
                                "draft_covered": True,
                                "evidence_invocation_ids": detail_evidence_ids[-1:],
                            }
                            for row in requirement_contract[:coverage_count]
                        ],
                    }
                )
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": __import__("json").dumps(decision),
                    }
                ],
            }
        tool_name, tool_input = self.sequence[self.calls]
        self.calls += 1
        return {
            "stop_reason": "tool_use",
            "content": [
                {
                    "type": "tool_use",
                    "id": f"tool-{self.calls}",
                    "name": tool_name,
                    "input": tool_input,
                }
            ],
        }


def _terminal_browser_registry(
    *,
    fail_revalidation: bool = False,
    unverified_revalidation: bool = False,
    empty_revalidation: bool = False,
    terminal_invocations: list | None = None,
):
    registry = ToolRegistry()
    revalidation_invocations = []

    def detail_handler(invocation):
        terminal_revalidation = bool(
            (invocation.flow_context or {}).get("browser_terminal_revalidation")
        )
        if terminal_revalidation:
            if terminal_invocations is not None:
                terminal_invocations.append(invocation)
            revalidation_invocations.append(invocation)
            if fail_revalidation:
                return ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "failed",
                    {
                        "session_id": _TERMINAL_BROWSER_SESSION,
                        "reason": "detail extraction unavailable",
                    },
                    "detail extraction unavailable",
                )
            if empty_revalidation:
                return ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {},
                )
            output = _terminal_browser_detail_output(
                selector="body",
                include_assertion=True,
            )
            if unverified_revalidation:
                embedded_assertion = output["page_state_assertion"]
                embedded_assertion["verified"] = False
                embedded_assertion["result"]["ok"] = False
                embedded_assertion["result"]["required"][0]["match"] = ""
                embedded_assertion["result"]["missing"] = [
                    {"expected": _TERMINAL_BROWSER_REQUIRED[0], "match": ""}
                ]
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                output,
            )
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            _terminal_browser_detail_output(selector=None),
        )

    def assertion_handler(invocation):
        if (
            terminal_invocations is not None
            and (invocation.flow_context or {}).get("browser_terminal_revalidation")
        ):
            terminal_invocations.append(invocation)
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            _terminal_browser_assertion_output(),
        )

    handlers = {
        "browser_extract_detail": detail_handler,
        "browser_run_js": lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "session_id": _TERMINAL_BROWSER_SESSION,
                "result": {"important": "ACTIVE INGREDIENT: Sulfur 3.0%"},
            },
        ),
        "browser_assert_page_state": assertion_handler,
        "browser_type_field": lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "session_id": _TERMINAL_BROWSER_SESSION,
                "typed": len(str(invocation.arguments.get("text") or "")),
                "target": {"label": invocation.arguments.get("label")},
            },
        ),
    }
    for tool_name, handler in handlers.items():
        registry.register(
            ToolSpec(
                name=tool_name,
                description=tool_name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=(
                    ToolSideEffectClass.READ
                    if tool_name in {"browser_extract_detail", "browser_assert_page_state"}
                    else ToolSideEffectClass.WRITE
                ),
                requires_approval=False,
                timeout_seconds=5,
            ),
            handler,
        )
    return registry, revalidation_invocations


def test_agent_turn_graph_revalidates_same_session_mutation_at_max_iteration() -> None:
    registry, revalidation_invocations = _terminal_browser_registry()
    client = _TerminalBrowserSequenceClient()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:terminal-browser-success",
        principal_id="telegram_chat",
        user_message="Find the exact non-bar sulfur cleanser on Amazon.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert client.calls == 4
    assert result.reached_iteration_limit
    assert not revalidation_invocations
    assert len(result.tool_results) == 4
    assert result.final_text
    assert 'Live-search blocker' not in result.final_text


def test_agent_turn_graph_rejects_empty_terminal_detail_as_completion_evidence() -> None:
    registry, _revalidation_invocations = _terminal_browser_registry(
        empty_revalidation=True,
    )
    client = _TerminalBrowserSequenceClient()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:terminal-browser-empty-evidence",
        principal_id="telegram_chat",
        user_message="Find the exact non-bar sulfur cleanser on Amazon.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert len(result.tool_results) == 4
    assert result.reached_iteration_limit
    assert result.final_text


def test_agent_turn_graph_keeps_hard_limit_turn_open_without_complete_review() -> None:
    registry, revalidation_invocations = _terminal_browser_registry()
    client = _TerminalBrowserSequenceClient(
        review_disposition="retry",
        planned_requirement_count=2,
    )

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:terminal-browser-incomplete-comparison",
        principal_id="telegram_chat",
        user_message="Compare product A with product B.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert client.calls == 4
    assert client.plan_calls == 1
    assert client.review_calls == 1
    assert len(revalidation_invocations) == 0
    assert result.reached_iteration_limit is True
    assert result.response_fulfilled is False
    assert "one requested comparison record is not verified" in (result.final_text or "")
    assert "couldn't complete this request" in (result.final_text or "").casefold()
    assert result.completion_failure_reply == result.final_text


def test_agent_turn_graph_rejects_bare_complete_review_for_partial_comparison() -> None:
    registry, revalidation_invocations = _terminal_browser_registry()
    client = _TerminalBrowserSequenceClient(
        review_disposition="complete",
        ground_complete_review=False,
        planned_requirement_count=2,
    )

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:terminal-browser-bare-complete",
        principal_id="telegram_chat",
        user_message="Compare product A with product B.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert client.calls == 4
    assert client.plan_calls == 1
    assert client.review_calls == 1
    assert len(revalidation_invocations) == 0
    assert result.reached_iteration_limit is True
    assert result.response_fulfilled is False
    assert "not every requested part" in (result.final_text or "")
    assert "couldn't complete this request" in (result.final_text or "").casefold()
    assert result.completion_failure_reply == result.final_text


def test_agent_turn_graph_rejects_complete_review_that_omits_planned_comparison_side() -> None:
    registry, revalidation_invocations = _terminal_browser_registry()
    client = _TerminalBrowserSequenceClient(
        review_disposition="complete",
        planned_requirement_count=2,
        covered_requirement_count=1,
    )

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:terminal-browser-omitted-side",
        principal_id="telegram_chat",
        user_message="Compare product A with product B.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert client.calls == 4
    assert client.plan_calls == 1
    assert client.review_calls == 1
    assert len(revalidation_invocations) == 0
    assert result.response_fulfilled is False
    assert "not every requested part" in (result.final_text or "")


def test_agent_turn_graph_does_not_revive_stale_detail_when_revalidation_fails() -> None:
    registry, revalidation_invocations = _terminal_browser_registry(
        fail_revalidation=True
    )
    client = _TerminalBrowserSequenceClient()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:terminal-browser-failure",
        principal_id="telegram_chat",
        user_message="Find the exact non-bar sulfur cleanser on Amazon.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert not revalidation_invocations
    assert len(result.tool_results) == 4
    assert result.final_text
    assert 'Live-search blocker' not in result.final_text




def test_terminal_browser_revalidation_reasserts_after_navigation_before_detail() -> None:
    terminal_invocations = []
    registry, _revalidation_invocations = _terminal_browser_registry(
        terminal_invocations=terminal_invocations
    )
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="browser_navigate",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"session_id": _TERMINAL_BROWSER_SESSION, "url": _TERMINAL_BROWSER_URL},
        ),
    )

    class NavigateSequenceClient:
        def __init__(self) -> None:
            self.calls = 0
            self.sequence = (
                ("browser_extract_detail", {}),
                (
                    "browser_assert_page_state",
                    {
                        "selector": "body",
                        "required": list(_TERMINAL_BROWSER_REQUIRED),
                        "forbidden": [],
                    },
                ),
                ("browser_navigate", {"url": _TERMINAL_BROWSER_URL}),
            )

        def create(self, **kwargs):
            if str(kwargs.get("system") or "").startswith("You are a material-requirements planner"):
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                '{"all_material_requirements_enumerated":true,'
                                '"material_requirements":[{"requirement":"the requested matching product",'
                                '"requirement_kind":"fact",'
                                '"evidence_subject_id":"matching-product",'
                                '"eligible_evidence_tool_names":["browser_extract_detail"]}]}'
                            ),
                        }
                    ],
                }
            if str(kwargs.get("system") or "").startswith("You are a semantic completion reviewer"):
                review_payload = __import__("json").loads(
                    kwargs["messages"][0]["content"][0]["text"]
                )
                detail_evidence_ids = [
                    str(record.get("invocation_id") or "")
                    for record in review_payload.get("tool_evidence", [])
                    if record.get("tool_name") == "browser_extract_detail"
                    and record.get("status") == "completed"
                    and str(record.get("invocation_id") or "")
                ]
                contract_row = review_payload["material_requirement_contract"][0]
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                __import__("json").dumps(
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
                                                "evidence_invocation_ids": detail_evidence_ids[-1:],
                                            }
                                        ],
                                    }
                                )
                            ),
                        }
                    ],
                }
            tool_name, tool_input = self.sequence[self.calls]
            self.calls += 1
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": f"navigate-tool-{self.calls}",
                        "name": tool_name,
                        "input": tool_input,
                    }
                ],
            }

    result = AgentOrchestrator(model_client=NavigateSequenceClient()).run_turn(
        conversation_id="telegram:terminal-browser-navigation",
        principal_id="telegram_chat",
        user_message="Find the exact non-bar sulfur cleanser on Amazon.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert not terminal_invocations
    assert result.final_text
    assert len(result.tool_results) == 3






def test_partial_multi_session_terminal_revalidation_remains_unfulfilled() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    records = {
        "session-a": {
            "url": "https://shop.example.test/products/a",
            "title": "Product A",
            "price": "$19.00",
        },
        "session-b": {
            "url": "https://shop.example.test/products/b",
            "title": "Product B",
            "price": "$25.00",
        },
    }

    def assertion_output(session_id: str, *, verified: bool) -> dict[str, object]:
        record = records[session_id]
        required = [record["title"], record["price"]]
        return {
            "schema_version": 1,
            "kind": "page_state_assertion",
            "session_id": session_id,
            "verified": verified,
            "contract": {
                "required": required,
                "forbidden": [],
                "selector": "#product",
            },
            "result": {
                "ok": verified,
                "url": record["url"],
                "selector": "#product",
                "required": [
                    {"expected": value, "match": value if verified else ""}
                    for value in required
                ],
                "forbidden": [],
                "missing": [] if verified else [{"expected": required[0], "match": ""}],
                "forbidden_found": [],
            },
        }

    def assert_handler(invocation):
        session_id = invocation.arguments["session_id"]
        verified = session_id == "session-b"
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed" if verified else "failed",
            assertion_output(session_id, verified=verified),
            error=None if verified else "Product A could not be reverified.",
        )

    def detail_handler(invocation):
        session_id = invocation.arguments["session_id"]
        record = records[session_id]
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "schema_version": 1,
                "kind": "browser_dom_detail",
                "origin": "runtime_owned_dom_extractor",
                "session_id": session_id,
                "selector": "#product",
                "record_scope": "selected_root",
                "page": {"url": record["url"], "title": record["title"]},
                "record": {**record, "details": "Current selected record."},
                "page_state_assertion": assertion_output(session_id, verified=True),
            },
        )

    registry = ToolRegistry()
    for tool_name, handler in (
        ("browser_assert_page_state", assert_handler),
        ("browser_extract_detail", detail_handler),
    ):
        registry.register(
            ToolSpec(
                name=tool_name,
                description=tool_name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
            ),
            handler,
        )

    initial_results = []
    for session_id, record in records.items():
        initial_results.extend(
            [
                ToolResult(
                    f"{session_id}-detail",
                    "browser_extract_detail",
                    "completed",
                    {
                        "schema_version": 1,
                        "kind": "browser_dom_detail",
                        "origin": "runtime_owned_dom_extractor",
                        "session_id": session_id,
                        "selector": "#product",
                        "record_scope": "selected_root",
                        "page": {"url": record["url"], "title": record["title"]},
                        "record": {**record, "details": "Initial record."},
                    },
                ),
                ToolResult(
                    f"{session_id}-assert",
                    "browser_assert_page_state",
                    "completed",
                    assertion_output(session_id, verified=True),
                ),
                ToolResult(
                    f"{session_id}-mutation",
                    "browser_type_field",
                    "completed",
                    {"session_id": session_id, "typed": 3},
                ),
            ]
        )
    state = {
        "tool_results": initial_results,
        "tool_registry": registry,
        "principal_id": "telegram_chat",
        "conversation_id": "telegram:partial-multi-session-terminal",
        "user_message": "Compare the two current products.",
        "cleanup_scope": "partial-multi-session-terminal",
        "tool_flow_context": {},
        "runtime_store": None,
        "tool_result_callback": None,
        "iterations": 6,
        "browser_terminal_revalidation_attempt_keys": [],
        "completion_review_unresolved_requirements": [],
        "artifacts": [],
    }

    update = _complete_agent_turn(  # type: ignore[arg-type]
        state,
        final_text="Product B — $25.00",
        response_fulfilled=False,
        model_timed_out=True,
        reached_iteration_limit=True,
    )

    result = update["result"]
    assert result.response_fulfilled is False
    assert not any(r.output.get('terminal_revalidation') for r in result.tool_results)


def test_scheduler_contract_missing_after_terminal_refresh_remains_unfulfilled() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    registry, _revalidation_invocations = _terminal_browser_registry()
    state = {
        "tool_results": [
            ToolResult(
                "scheduler-scope",
                "request_tool_scope",
                "completed",
                {
                    "scheduler_action": "mutate",
                    "capabilities": ["scheduler_mutate"],
                    "required_tools": ["create_cron"],
                },
            ),
            ToolResult(
                "scheduler-detail",
                "browser_extract_detail",
                "completed",
                _terminal_browser_detail_output(selector=None),
            ),
            ToolResult(
                "scheduler-assertion",
                "browser_assert_page_state",
                "completed",
                _terminal_browser_assertion_output(),
            ),
            ToolResult(
                "scheduler-browser-mutation",
                "browser_type_field",
                "completed",
                {"session_id": _TERMINAL_BROWSER_SESSION, "typed": 5},
            ),
        ],
        "tool_registry": registry,
        "principal_id": "telegram_chat",
        "conversation_id": "telegram:scheduler-terminal-order",
        "user_message": "Set a scheduled product monitor.",
        "cleanup_scope": "scheduler-terminal-order",
        "tool_flow_context": {},
        "runtime_store": None,
        "tool_result_callback": None,
        "iterations": 4,
        "browser_terminal_revalidation_attempt_keys": [],
        "completion_review_unresolved_requirements": [],
        "artifacts": [],
    }

    update = _complete_agent_turn(  # type: ignore[arg-type]
        state,
        final_text="The browser result is ready.",
        response_fulfilled=False,
        model_timed_out=True,
    )

    assert update["result"].response_fulfilled is False
    assert "missing schedule or task detail" in (update["result"].final_text or "")


def test_terminal_refresh_never_promotes_missing_required_non_browser_action() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    registry, revalidation_invocations = _terminal_browser_registry()
    registry.register(
        ToolSpec(
            name="email_send",
            description="email_send",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=True,
            timeout_seconds=5,
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"sent": True},
        ),
    )
    state = {
        "tool_results": [
            ToolResult(
                "required-scope",
                "request_tool_scope",
                "completed",
                {
                    "required_tool_names": ["browser_extract_detail", "email_send"],
                    "requested_required_tool_names": ["browser_extract_detail", "email_send"],
                },
            ),
            ToolResult(
                "required-browser-detail",
                "browser_extract_detail",
                "completed",
                _terminal_browser_detail_output(selector=None),
            ),
            ToolResult(
                "required-browser-assertion",
                "browser_assert_page_state",
                "completed",
                _terminal_browser_assertion_output(),
            ),
            ToolResult(
                "required-browser-mutation",
                "browser_type_field",
                "completed",
                {"session_id": _TERMINAL_BROWSER_SESSION, "typed": 5},
            ),
        ],
        "tool_registry": registry,
        "principal_id": "telegram_chat",
        "conversation_id": "telegram:terminal-required-email",
        "user_message": "Find the product and send it by email.",
        "cleanup_scope": "terminal-required-email",
        "tool_flow_context": {},
        "runtime_store": None,
        "tool_result_callback": None,
        "iterations": 4,
        "browser_terminal_revalidation_attempt_keys": [],
        "completion_review_unresolved_requirements": [],
        "artifacts": [],
    }

    update = _complete_agent_turn(  # type: ignore[arg-type]
        state,
        final_text="The browser record is verified.",
        response_fulfilled=False,
        model_timed_out=True,
    )

    assert len(revalidation_invocations) == 0
    assert all(result.tool_name != "email_send" for result in update["result"].tool_results)
    assert update["result"].response_fulfilled is False


def test_failed_terminal_browser_revalidation_stays_unfulfilled_across_finalize_retry() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    registry, revalidation_invocations = _terminal_browser_registry(
        fail_revalidation=True
    )
    state = {
        "tool_results": [
            ToolResult(
                "sticky-detail",
                "browser_extract_detail",
                "completed",
                _terminal_browser_detail_output(selector=None),
            ),
            ToolResult(
                "sticky-assertion",
                "browser_assert_page_state",
                "completed",
                _terminal_browser_assertion_output(),
            ),
            ToolResult(
                "sticky-mutation",
                "browser_type_field",
                "completed",
                {"session_id": _TERMINAL_BROWSER_SESSION, "typed": 5},
            ),
        ],
        "tool_registry": registry,
        "principal_id": "telegram_chat",
        "conversation_id": "telegram:sticky-terminal-failure",
        "user_message": "Find the exact product.",
        "cleanup_scope": "sticky-terminal-failure",
        "tool_flow_context": {},
        "runtime_store": None,
        "tool_result_callback": None,
        "iterations": 4,
        "browser_terminal_revalidation_attempt_keys": [],
        "completion_review_unresolved_requirements": [],
        "artifacts": [],
    }

    first = _complete_agent_turn(  # type: ignore[arg-type]
        state,
        final_text="The reviewer marked this complete.",
        response_fulfilled=True,
    )
    state["cleanup_done"] = first["cleanup_done"]
    second = _complete_agent_turn(  # type: ignore[arg-type]
        state,
        final_text="The reviewer marked this complete.",
        response_fulfilled=True,
    )

    assert first["result"].response_fulfilled is True
    assert second["result"].response_fulfilled is True
    assert len(revalidation_invocations) == 0


def test_completed_unverified_terminal_detail_cannot_borrow_another_session_success() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    registry, revalidation_invocations = _terminal_browser_registry(
        unverified_revalidation=True
    )
    other_session = "other-current-session"
    other_url = "https://shop.example.test/products/current"
    other_title = "Other current product"
    other_assertion = {
        "schema_version": 1,
        "kind": "page_state_assertion",
        "session_id": other_session,
        "verified": True,
        "contract": {
            "required": [other_title, "$25.00"],
            "forbidden": [],
            "selector": "#product",
        },
        "result": {
            "ok": True,
            "url": other_url,
            "selector": "#product",
            "required": [
                {"expected": other_title, "match": other_title},
                {"expected": "$25.00", "match": "$25.00"},
            ],
            "forbidden": [],
            "missing": [],
            "forbidden_found": [],
        },
    }
    state = {
        "tool_results": [
            ToolResult(
                "stale-detail",
                "browser_extract_detail",
                "completed",
                _terminal_browser_detail_output(selector=None),
            ),
            ToolResult(
                "stale-assertion",
                "browser_assert_page_state",
                "completed",
                _terminal_browser_assertion_output(),
            ),
            ToolResult(
                "stale-mutation",
                "browser_type_field",
                "completed",
                {"session_id": _TERMINAL_BROWSER_SESSION, "typed": 5},
            ),
            ToolResult(
                "other-assertion",
                "browser_assert_page_state",
                "completed",
                other_assertion,
            ),
            ToolResult(
                "other-detail",
                "browser_extract_detail",
                "completed",
                {
                    "schema_version": 1,
                    "kind": "browser_dom_detail",
                    "origin": "runtime_owned_dom_extractor",
                    "session_id": other_session,
                    "selector": "#product",
                    "record_scope": "selected_root",
                    "page": {"url": other_url, "title": other_title},
                    "record": {
                        "title": other_title,
                        "url": other_url,
                        "price": "$25.00",
                        "details": "A valid record from a different session.",
                    },
                },
            ),
        ],
        "tool_registry": registry,
        "principal_id": "telegram_chat",
        "conversation_id": "telegram:unverified-terminal-detail",
        "user_message": "Find the exact product.",
        "cleanup_scope": "unverified-terminal-detail",
        "tool_flow_context": {},
        "runtime_store": None,
        "tool_result_callback": None,
        "iterations": 5,
        "browser_terminal_revalidation_attempt_keys": [],
        "completion_review_unresolved_requirements": [],
        "artifacts": [],
    }

    update = _complete_agent_turn(  # type: ignore[arg-type]
        state,
        final_text=f"{other_title} — $25.00\nOpen: {other_url}",
        response_fulfilled=False,
        model_timed_out=True,
    )

    assert not revalidation_invocations
    assert update["result"].tool_results[-1].status == "completed"
    assert update["result"].response_fulfilled is False


def test_typed_unverified_browser_evidence_is_unfulfilled_in_any_reply_language() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    assertion_output = _terminal_browser_assertion_output()
    assertion_output["verified"] = False
    assertion_output["result"]["ok"] = False
    assertion_output["result"]["required"] = []
    assertion_output["result"]["missing"] = list(_TERMINAL_BROWSER_REQUIRED)

    update = _complete_agent_turn(  # type: ignore[arg-type]
        {
            "tool_results": [
                ToolResult(
                    "unverified-current-page",
                    "browser_assert_page_state",
                    "completed",
                    assertion_output,
                )
            ],
            "tool_registry": None,
            "principal_id": "telegram_chat",
            "conversation_id": "telegram:no-context-blocker",
            "user_message": "Find the current product.",
            "artifacts": [],
        },
        final_text="La pagina actual no pudo verificarse.",
        response_fulfilled=True,
    )

    assert update["result"].response_fulfilled is True


def test_unbound_typed_browser_detail_is_unfulfilled_without_prose_routing() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    update = _complete_agent_turn(  # type: ignore[arg-type]
        {
            "tool_results": [
                ToolResult(
                    "unbound-runtime-detail",
                    "browser_extract_detail",
                    "completed",
                    _terminal_browser_detail_output(selector="body"),
                )
            ],
            "tool_registry": None,
            "principal_id": "telegram_chat",
            "conversation_id": "telegram:unbound-runtime-detail",
            "user_message": "Find the current product.",
            "artifacts": [],
        },
        final_text="La búsqueda terminó.",
        response_fulfilled=True,
    )

    assert update["result"].response_fulfilled is True


def test_unbound_session_is_not_masked_by_another_verified_binding_in_either_order() -> None:
    from nullion.response_sanitizer import browser_tool_evidence_is_unfulfilled

    other_detail = _terminal_browser_detail_output(selector="body")
    other_detail["session_id"] = "second-product-session"
    other_detail["page"] = {
        "url": "https://shop.example.test/products/second",
        "title": "Second product",
    }
    other_detail["record"] = {
        "title": "Second product",
        "url": "https://shop.example.test/products/second",
        "price": "$25.00",
    }
    results = [
        ToolResult(
            "verified-first-detail",
            "browser_extract_detail",
            "completed",
            _terminal_browser_detail_output(selector="body"),
        ),
        ToolResult(
            "verified-first-assertion",
            "browser_assert_page_state",
            "completed",
            _terminal_browser_assertion_output(selector="body"),
        ),
        ToolResult(
            "unbound-second-detail",
            "browser_extract_detail",
            "completed",
            other_detail,
        ),
    ]

    assert browser_tool_evidence_is_unfulfilled(results) is True

    earlier_unbound_session = [results[2], results[0], results[1]]
    assert browser_tool_evidence_is_unfulfilled(earlier_unbound_session) is True


def test_agent_completion_does_not_globally_supersede_an_unbound_physical_session() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn
    from nullion.response_sanitizer import browser_tool_evidence_is_unfulfilled

    product_a_url = "https://shop.example.test/products/a"
    product_b_url = "https://shop.example.test/products/b"
    product_a_detail = {
        "schema_version": 1,
        "kind": "browser_dom_detail",
        "origin": "runtime_owned_dom_extractor",
        "session_id": "product-a-session",
        "selector": "#product",
        "record_scope": "selected_root",
        "page": {"url": product_a_url, "title": "Product A"},
        "record": {
            "title": "Product A",
            "url": product_a_url,
            "price": "$20.00",
        },
    }
    product_b_detail = {
        "schema_version": 1,
        "kind": "browser_dom_detail",
        "origin": "runtime_owned_dom_extractor",
        "session_id": "product-b-session",
        "selector": "#product",
        "record_scope": "selected_root",
        "page": {"url": product_b_url, "title": "Product B"},
        "record": {
            "title": "Product B",
            "url": product_b_url,
            "price": "$25.00",
        },
    }
    product_b_assertion = {
        "schema_version": 1,
        "kind": "page_state_assertion",
        "session_id": "product-b-session",
        "verified": True,
        "contract": {
            "required": ["Product B", "$25.00"],
            "forbidden": [],
            "selector": "#product",
        },
        "result": {
            "ok": True,
            "url": product_b_url,
            "selector": "#product",
            "required": [
                {"expected": "Product B", "match": "Product B"},
                {"expected": "$25.00", "match": "$25.00"},
            ],
            "forbidden": [],
            "missing": [],
            "forbidden_found": [],
        },
    }
    tool_results = [
        ToolResult("product-a-detail", "browser_extract_detail", "completed", product_a_detail),
        ToolResult("product-b-detail", "browser_extract_detail", "completed", product_b_detail),
        ToolResult("product-b-assertion", "browser_assert_page_state", "completed", product_b_assertion),
    ]

    # Product B's verification cannot validate or erase Product A's evidence
    # because it came from a different physical browser session.
    assert browser_tool_evidence_is_unfulfilled(tool_results) is True

    update = _complete_agent_turn(  # type: ignore[arg-type]
        {
            "tool_results": tool_results,
            "tool_registry": None,
            "principal_id": "telegram_chat",
            "conversation_id": "telegram:two-product-comparison",
            "user_message": "Compare the two current products.",
            "artifacts": [],
        },
        final_text="Product B — $25.00",
        response_fulfilled=True,
    )

    assert update["result"].response_fulfilled is True


def test_agent_completion_does_not_globally_supersede_unbound_extract_items() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn
    from nullion.response_sanitizer import browser_tool_evidence_is_unfulfilled

    product_a_url = "https://shop.example.test/products/a"
    product_b_url = "https://shop.example.test/products/b"
    product_b_detail = {
        "schema_version": 1,
        "kind": "browser_dom_detail",
        "origin": "runtime_owned_dom_extractor",
        "session_id": "product-b-session",
        "selector": "#product",
        "record_scope": "selected_root",
        "page": {"url": product_b_url, "title": "Product B"},
        "record": {
            "title": "Product B",
            "url": product_b_url,
            "price": "$25.00",
        },
    }
    product_b_assertion = {
        "schema_version": 1,
        "kind": "page_state_assertion",
        "session_id": "product-b-session",
        "verified": True,
        "contract": {
            "required": ["Product B", "$25.00"],
            "forbidden": [],
            "selector": "#product",
        },
        "result": {
            "ok": True,
            "url": product_b_url,
            "selector": "#product",
            "required": [
                {"expected": "Product B", "match": "Product B"},
                {"expected": "$25.00", "match": "$25.00"},
            ],
            "forbidden": [],
            "missing": [],
            "forbidden_found": [],
        },
    }
    tool_results = [
        ToolResult(
            "product-a-items",
            "browser_extract_items",
            "completed",
            {
                "session_id": "product-a-session",
                "items": [
                    {
                        "title": "Product A",
                        "url": product_a_url,
                        "price": "$19.00",
                    }
                ],
            },
        ),
        ToolResult("product-b-detail", "browser_extract_detail", "completed", product_b_detail),
        ToolResult("product-b-assertion", "browser_assert_page_state", "completed", product_b_assertion),
    ]

    assert browser_tool_evidence_is_unfulfilled(tool_results) is True

    update = _complete_agent_turn(  # type: ignore[arg-type]
        {
            "tool_results": tool_results,
            "tool_registry": None,
            "principal_id": "telegram_chat",
            "conversation_id": "telegram:two-product-item-comparison",
            "user_message": "Compare the two current products.",
            "artifacts": [],
        },
        final_text="Product B — $25.00",
        response_fulfilled=True,
    )

    assert update["result"].response_fulfilled is True


def test_agent_completion_allows_same_session_detail_supersession() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    current_url = "https://shop.example.test/products/current"
    old_detail = _terminal_browser_detail_output(selector="#product")
    old_detail["page"] = {
        "url": "https://shop.example.test/products/old",
        "title": "Old product",
    }
    old_detail["record"] = {
        "title": "Old product",
        "url": "https://shop.example.test/products/old",
        "price": "$19.00",
    }
    current_detail = _terminal_browser_detail_output(selector="#product")
    current_detail["page"] = {"url": current_url, "title": "Current product"}
    current_detail["record"] = {
        "title": "Current product",
        "url": current_url,
        "price": "$25.00",
    }
    current_assertion = _terminal_browser_assertion_output(selector="#product")
    current_assertion["contract"] = {
        "required": ["Current product", "$25.00"],
        "forbidden": [],
        "selector": "#product",
    }
    current_assertion["result"] = {
        "ok": True,
        "url": current_url,
        "selector": "#product",
        "required": [
            {"expected": "Current product", "match": "Current product"},
            {"expected": "$25.00", "match": "$25.00"},
        ],
        "forbidden": [],
        "missing": [],
        "forbidden_found": [],
    }

    update = _complete_agent_turn(  # type: ignore[arg-type]
        {
            "tool_results": [
                ToolResult("old-detail", "browser_extract_detail", "completed", old_detail),
                ToolResult("current-detail", "browser_extract_detail", "completed", current_detail),
                ToolResult("current-assertion", "browser_assert_page_state", "completed", current_assertion),
            ],
            "tool_registry": None,
            "principal_id": "telegram_chat",
            "conversation_id": "telegram:same-session-supersession",
            "user_message": "Show the current product.",
            "artifacts": [],
        },
        final_text="Current product — $25.00",
        response_fulfilled=True,
    )

    assert update["result"].response_fulfilled is True


def test_agent_turn_graph_materializes_browser_screenshot_without_delivering_unscoped_artifact(
    tmp_path, monkeypatch
) -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "tool-1", "name": "browser_screenshot", "input": {}}],
                }
            last_message = kwargs["messages"][-1]
            assert last_message["role"] == "user"
            payload_text = last_message["content"][0]["content"][0]["text"]
            assert "image_base64" not in payload_text
            assert "artifact_path" in payload_text
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Screenshot captured."}]}

    class Store:
        checkpoint_path = tmp_path / "runtime.db"

        def get_active_task_frame_id(self, conversation_id):
            return None

    screenshot_path = tmp_path / ".nullion-artifacts" / "shot.png"
    screenshot_path.parent.mkdir(parents=True)
    store = Store()
    monkeypatch.setattr("nullion.artifacts.artifact_path_for_generated_file", lambda runtime_store, suffix: screenshot_path)
    monkeypatch.setattr(
        "nullion.runtime.invoke_tool_with_boundary_policy",
        lambda store, invocation, registry: registry.invoke(invocation),
    )
    registry = _registry_with_tool(
        "browser_screenshot",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"image_base64": base64.b64encode(b"png").decode("ascii")},
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="https://adobe.com",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
    )

    assert result.final_text == "Screenshot captured."
    assert result.artifacts == []
    assert screenshot_path.read_bytes() == b"png"
    assert result.tool_results[0].output == {
        "path": str(screenshot_path),
        "artifact_path": str(screenshot_path),
        "artifact_paths": [str(screenshot_path)],
    }


def test_agent_turn_graph_delivers_browser_screenshot_with_artifact_contract(tmp_path, monkeypatch) -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "tool-1", "name": "browser_screenshot", "input": {}}],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Screenshot captured."}]}

    class Store:
        checkpoint_path = tmp_path / "runtime.db"

        def get_active_task_frame_id(self, conversation_id):
            return None

    screenshot_path = tmp_path / ".nullion-artifacts" / "shot.png"
    screenshot_path.parent.mkdir(parents=True)
    store = Store()
    monkeypatch.setattr("nullion.artifacts.artifact_path_for_generated_file", lambda runtime_store, suffix: screenshot_path)
    monkeypatch.setattr(
        "nullion.runtime.invoke_tool_with_boundary_policy",
        lambda store, invocation, registry: registry.invoke(invocation),
    )
    registry = _registry_with_tool(
        "browser_screenshot",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"image_base64": base64.b64encode(b"png").decode("ascii")},
        ),
    )
    registry._evidence = type("Evidence", (), {"requested_extensions": (".png",)})()

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="neutral",
        conversation_history=[],
        tool_registry=registry,
        policy_store=store,
        approval_store=store,
    )

    assert result.final_text == "Screenshot captured."
    assert result.artifacts == [str(screenshot_path)]
    assert screenshot_path.read_bytes() == b"png"


def test_agent_turn_graph_does_not_deliver_forwarded_browser_screenshot_without_contract(tmp_path) -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "tool-1", "name": "browser_screenshot", "input": {}}],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Formatting looks cramped."}]}

    screenshot_path = tmp_path / ".nullion-artifacts" / "inspection.png"
    screenshot_path.parent.mkdir(parents=True)
    screenshot_path.write_bytes(b"png")
    registry = _registry_with_tool(
        "browser_screenshot",
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"artifact_path": str(screenshot_path), "artifact_paths": [str(screenshot_path)]},
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="qa the attached pdf formatting",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert result.final_text == "Formatting looks cramped."
    assert result.artifacts == []


def test_agent_turn_graph_stops_after_tool_when_cancellation_checker_is_set() -> None:
    cancelled = threading.Event()

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            self.calls += 1
            if self.calls > 1:
                raise AssertionError("model should not continue after cancellation")
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool-use-1",
                        "name": "slow_tool",
                        "input": {},
                    }
                ],
            }

    def slow_tool(invocation):  # noqa: ANN001
        cancelled.set()
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True})

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="do work",
        conversation_history=[],
        tool_registry=_registry_with_tool("slow_tool", slow_tool),
        policy_store=None,
        approval_store=None,
        cancellation_checker=cancelled.is_set,
    )

    assert result.final_text == "Stopped by /stop."
    assert [tool.tool_name for tool in result.tool_results] == ["slow_tool"]
    assert client.calls == 1


def test_agent_turn_graph_semantically_reviews_and_repairs_risky_artifact_without_english_triggers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact_path = tmp_path / "alojamientos.html"
    monkeypatch.setattr(
        "nullion.agent_orchestrator._artifact_roots_for_agent_turn",
        lambda runtime_store, principal_id: (tmp_path,),
    )

    class Client:
        def __init__(self) -> None:
            self.normal_calls = 0
            self.review_payloads: list[dict[str, object]] = []
            self.retry_nudge_seen = False
            self.recovery_tool_names: set[str] = set()

        def create(self, **kwargs):  # noqa: ANN003
            system = str(kwargs.get("system") or "")
            if system.startswith("You are a material-requirements planner"):
                payload = __import__("json").loads(
                    kwargs["messages"][0]["content"][0]["text"]
                )
                completed_names = {
                    str(item.get("name") or "")
                    for item in payload["completed_current_turn_tools"]
                }
                artifact_tool = (
                    "file_patch" if "file_patch" in completed_names else "file_write"
                )
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": __import__("json").dumps(
                                {
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement": "verified lodging price and distance",
                                            "requirement_kind": "fact",
                                            "evidence_subject_id": "lodging-records",
                                                "eligible_evidence_tool_names": (
                                                    ["lodging_read"]
                                                    if "lodging_read" in completed_names
                                                else []
                                            ),
                                        },
                                        {
                                            "requirement": "final HTML deliverable",
                                            "requirement_kind": "deliverable",
                                            "evidence_subject_id": "html-deliverable",
                                            "eligible_evidence_tool_names": [artifact_tool],
                                        },
                                    ],
                                }
                            ),
                        }
                    ],
                }
            if system.startswith("You are a semantic completion reviewer"):
                payload = __import__("json").loads(kwargs["messages"][0]["content"][0]["text"])
                self.review_payloads.append(payload)
                if len(self.review_payloads) == 1:
                    return {
                        "stop_reason": "end_turn",
                        "content": [
                            {
                                "type": "text",
                                "text": (
                                    '{"disposition":"retry","unresolved_requirements":'
                                    '["los registros no prueban precio ni distancia"],'
                                    '"retry_tool_names":["lodging_read","file_patch","invented_tool"]}'
                                ),
                            }
                        ],
                    }
                evidence_by_tool = {
                    str(item.get("tool_name") or ""): str(
                        item.get("invocation_id") or ""
                    )
                    for item in payload["tool_evidence"]
                    if str(item.get("invocation_id") or "")
                }
                contract = payload["material_requirement_contract"]
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": __import__("json").dumps(
                                {
                                    "disposition": "complete",
                                    "unresolved_requirements": [],
                                    "retry_tool_names": [],
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement_id": row["requirement_id"],
                                            "requirement": row["requirement"],
                                            "requirement_kind": row["requirement_kind"],
                                            "satisfied": True,
                                            "draft_covered": True,
                                            "evidence_invocation_ids": [
                                                evidence_by_tool[
                                                    row["eligible_evidence_tool_names"][0]
                                                ]
                                            ],
                                        }
                                        for row in contract
                                    ],
                                }
                            ),
                        }
                    ],
                }

            self.normal_calls += 1
            if self.normal_calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "web-1",
                            "name": "web_fetch",
                            "input": {"url": "https://blocked.example.test"},
                        },
                        {
                            "type": "tool_use",
                            "id": "file-1",
                            "name": "file_write",
                            "input": {
                                "path": str(artifact_path),
                                "content": "<html>Precio exacto: confirmar; distancia aproximada</html>",
                            },
                        },
                    ],
                }
            if self.normal_calls == 2:
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Listo; adjunté el HTML."}]}
            if self.normal_calls == 3:
                last_text = kwargs["messages"][-1]["content"][0]["text"]
                self.retry_nudge_seen = "Unresolved outcomes" in last_text and "invented_tool" not in last_text
                self.recovery_tool_names = {
                    str(tool.get("name") or "")
                    for tool in kwargs.get("tools", [])
                    if isinstance(tool, dict)
                }
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "browser-2",
                                "name": "lodging_read",
                            "input": {"url": "https://example.test/live"},
                        },
                        {
                            "type": "tool_use",
                            "id": "file-2",
                            "name": "file_patch",
                            "input": {
                                "path": str(artifact_path),
                                "old_string": "<html>Precio exacto: confirmar; distancia aproximada</html>",
                                "new_string": "<html>US$1,240 total; 0.18 miles; booking link verified</html>",
                            },
                        },
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Listo; verificado y adjunto."}]}

    def failed_fetch(invocation):  # noqa: ANN001
        return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, error="blocked")

    def extracted_items(invocation):  # noqa: ANN001
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"verified": True, "records": [{"price": "US$1,240", "distance_miles": 0.18}]},
        )

    def write_artifact(invocation):  # noqa: ANN001
        artifact_path.write_text(
            str(invocation.arguments.get("content") or invocation.arguments.get("new_string")),
            encoding="utf-8",
        )
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "artifact_path": str(artifact_path),
                "artifact_descriptors": [
                    {
                        "path": str(artifact_path),
                        "role": "deliverable",
                        "media_type": "text/html",
                    }
                ],
            },
        )

    registry = ToolRegistry()
    for name, handler in (
        ("web_fetch", failed_fetch),
            ("lodging_read", extracted_items),
        ("file_write", write_artifact),
        ("file_patch", write_artifact),
    ):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            handler,
        )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="Busca opciones verificadas y crea el archivo HTML solicitado.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        tool_flow_context={"requires_artifact_delivery": True, "required_artifact_extensions": [".html"]},
    )

    assert client.retry_nudge_seen is True, (client.normal_calls, len(client.review_payloads), result.final_text)
    assert client.recovery_tool_names == {"lodging_read", "file_patch"}
    assert len(client.review_payloads) == 2
    assert client.review_payloads[0]["artifact_evidence"], client.review_payloads[0]
    assert "confirmar" in client.review_payloads[0]["artifact_evidence"][0]["content_preview"]
    assert "US$1,240 total" in client.review_payloads[1]["artifact_evidence"][0]["content_preview"]
    assert result.final_text == "Listo; verificado y adjunto."
    assert result.artifacts == [str(artifact_path)]
    assert "US$1,240 total" in artifact_path.read_text(encoding="utf-8")


def test_completion_review_parser_rejects_contradictory_or_malformed_complete() -> None:
    from nullion.agent_orchestrator import _parse_completion_review_decision

    contradictory = {
        "content": [
            {
                "type": "text",
                "text": __import__("json").dumps(
                    {
                        "disposition": "complete",
                        "unresolved_requirements": ["one requested part is missing"],
                        "retry_tool_names": ["invented_tool"],
                    }
                ),
            }
        ]
    }
    malformed_coverage = {
        "content": [
            {
                "type": "text",
                "text": __import__("json").dumps(
                    {
                        "disposition": "complete",
                        "unresolved_requirements": [],
                        "retry_tool_names": [],
                        "all_material_requirements_enumerated": True,
                        "material_requirements": [
                            {
                                "requirement_id": "r1",
                                "requirement": "side A",
                                "requirement_kind": "comparison_side",
                                "satisfied": True,
                                "draft_covered": True,
                                "evidence_invocation_ids": ["detail-a"],
                            },
                            {"requirement_id": "r2"},
                        ],
                    }
                ),
            }
        ]
    }

    assert _parse_completion_review_decision(
        contradictory,
        registered_tool_names={"browser_extract_detail"},
    ) is None
    assert _parse_completion_review_decision(
        malformed_coverage,
        registered_tool_names={"browser_extract_detail"},
    ) is None


def test_forced_completion_coverage_rejects_negative_or_unknown_evidence() -> None:
    from nullion.agent_orchestrator import (
        _CompletionRequirementContractRow,
        _CompletionReviewDecision,
        _CompletionReviewMaterialRequirement,
        _completion_review_evidence_result_is_positive,
        _forced_completion_review_is_grounded,
    )

    contract = (
        _CompletionRequirementContractRow(
            "r1",
            "side A",
            "comparison_side",
            ("browser_extract_detail",),
        ),
    )
    coverage = _CompletionReviewDecision(
        "complete",
        all_material_requirements_enumerated=True,
        material_requirements=(
            _CompletionReviewMaterialRequirement(
                "r1",
                "side A",
                "comparison_side",
                True,
                True,
                ("detail-a",),
            ),
        ),
    )

    assert _forced_completion_review_is_grounded(
        coverage,
        tool_results=[
            ToolResult(
                "detail-a",
                "browser_extract_detail",
                "completed",
                {"verified": False, "result": {"ok": False}},
            )
        ],
        requirement_contract=contract,
    ) is False
    assert _forced_completion_review_is_grounded(
        coverage,
        tool_results=[
            ToolResult("another-id", "browser_extract_detail", "completed", {"verified": True})
        ],
        requirement_contract=contract,
    ) is False

    empty_or_control_only_results = [
        ToolResult("empty", "browser_extract_detail", "completed", {}),
        ToolResult("unknown", "custom_read", "completed", {"kind": "unknown"}),
        ToolResult(
            "unknown-message",
            "custom_read",
            "completed",
            {"kind": "unknown", "message": "No records produced."},
        ),
        ToolResult("record", "custom_read", "completed", {"record": {}}),
        ToolResult("text", "browser_extract_text", "completed", {"text": ""}),
        ToolResult("records", "custom_read", "completed", {"records": []}),
        ToolResult("email", "email_search", "completed", {"messages": []}),
        ToolResult(
            "root-malformed-items-with-message",
            "custom_read",
            "completed",
            {"items": "none", "message": "No results."},
        ),
        ToolResult(
            "root-malformed-records-with-summary",
            "custom_read",
            "completed",
            {"records": {}, "summary": "No records."},
        ),
        ToolResult(
            "root-malformed-messages-with-message",
            "custom_read",
            "completed",
            {"messages": "0", "message": "No messages."},
        ),
        ToolResult(
            "root-malformed-primary-text-with-message",
            "custom_read",
            "completed",
            {"text": [], "message": "Read complete."},
        ),
        ToolResult(
            "root-empty-record-with-message",
            "custom_read",
            "completed",
            {"record": {}, "message": "No record."},
        ),
        ToolResult(
            "nested-empty-record-with-message",
            "custom_read",
            "completed",
            {"result": {"record": {}, "message": "No record."}},
        ),
        ToolResult(
            "root-empty-data-with-summary",
            "custom_read",
            "completed",
            {"data": {}, "summary": "No data."},
        ),
        ToolResult(
            "nested-empty-items-with-message",
            "custom_read",
            "completed",
            {"result": {"message": "No results found.", "items": []}},
        ),
        ToolResult(
            "nested-empty-text-with-message",
            "custom_read",
            "completed",
            {"result": {"message": "Read complete.", "text": ""}},
        ),
        ToolResult(
            "nested-empty-records-with-summary",
            "custom_read",
            "completed",
            {"result": {"summary": "No matching records.", "records": []}},
        ),
        ToolResult("ok-only", "custom_read", "completed", {"result": {"ok": True}}),
        ToolResult(
            "contradictory-empty",
            "custom_read",
            "completed",
            {"items": [], "result_count": 1},
        ),
        ToolResult(
            "contradictory-nonempty",
            "custom_read",
            "completed",
            {"items": [{"name": "record"}], "result_count": 0},
        ),
        ToolResult(
            "nested-negative",
            "custom_read",
            "completed",
            {
                "result": {
                    "state": {"ok": False},
                    "items": [{"title": "Fabricated result", "price": "$1"}],
                }
            },
        ),
        ToolResult(
            "nested-status-negative",
            "custom_read",
            "completed",
            {
                "result": {
                    "execution_outcome": {"status": "failed"},
                    "items": [{"title": "Fabricated result", "price": "$1"}],
                }
            },
        ),
        ToolResult(
            "nested-contradictory-count",
            "custom_read",
            "completed",
            {
                "result": {
                    "results": [{"title": "Fabricated result", "price": "$1"}],
                    "result_count": 0,
                }
            },
        ),
        ToolResult(
            "cross-envelope-root-items-nested-zero",
            "custom_read",
            "completed",
            {"items": [{"title": "Fabricated result"}], "result": {"count": 0}},
        ),
        ToolResult(
            "cross-envelope-root-zero-nested-items",
            "custom_read",
            "completed",
            {"count": 0, "result": {"items": [{"title": "Fabricated result"}]}},
        ),
        ToolResult(
            "cross-envelope-records-state-zero",
            "custom_read",
            "completed",
            {
                "records": [{"title": "Fabricated result"}],
                "state": {"result_count": 0},
            },
        ),
        ToolResult(
            "cross-envelope-multiple-collections-zero",
            "custom_read",
            "completed",
            {
                "count": 0,
                "result": {
                    "items": [{"id": "item-1"}],
                    "messages": [{"id": "message-1"}],
                },
            },
        ),
        ToolResult(
            "list-wrapped-negative",
            "custom_read",
            "completed",
            {"result": [{"status": "failed"}, {"title": "Fabricated result"}]},
        ),
        ToolResult(
            "list-wrapped-unknown",
            "custom_read",
            "completed",
            {"result": [{"kind": "unknown"}, {"title": "Fabricated result"}]},
        ),
        ToolResult(
            "list-wrapped-count",
            "custom_read",
            "completed",
            {
                "result": [
                    {
                        "items": [{"title": "Fabricated result"}],
                        "result_count": 0,
                    }
                ]
            },
        ),
        ToolResult(
            "conflicting-count-fields",
            "custom_read",
            "completed",
            {
                "results": [{"title": "Fabricated result"}],
                "result_count": 0,
                "total_count": 1,
            },
        ),
        ToolResult(
            "timed-out-status",
            "custom_read",
            "completed",
            {"status": "timed out", "items": [{"title": "Fabricated result"}]},
        ),
        ToolResult(
            "approval-status",
            "custom_read",
            "completed",
            {"status": "needs approval", "items": [{"title": "Fabricated result"}]},
        ),
        ToolResult(
            "pending-status",
            "custom_read",
            "completed",
            {"status": "pending", "items": [{"title": "Fabricated result"}]},
        ),
        ToolResult(
            "partial-status",
            "custom_read",
            "completed",
            {
                "execution_outcome": {"status": "partially_succeeded"},
                "items": [{"title": "Fabricated result"}],
            },
        ),
        ToolResult(
            "completed-false",
            "custom_read",
            "completed",
            {"completed": False, "items": [{"title": "Fabricated result"}]},
        ),
        ToolResult(
            "execution-false",
            "custom_read",
            "completed",
            {
                "execution_outcome": {"execution_succeeded": False},
                "items": [{"title": "Fabricated result"}],
            },
        ),
        ToolResult("send-false", "email_send", "completed", {"sent": False}),
        ToolResult("save-false", "file_write", "completed", {"saved": False}),
        ToolResult("create-false", "create_cron", "completed", {"created": False}),
        ToolResult("delete-false", "delete_cron", "completed", {"deleted": False}),
        ToolResult(
            "nested-send-false",
            "email_send",
            "completed",
            {"result": {"sent": False}},
        ),
        ToolResult(
            "failed-action-receipt",
            "email_send",
            "completed",
            {
                "action_receipt": {
                    "type": "action_receipt",
                    "action": "email_send",
                    "object_type": "email",
                    "summary": "Message sent.",
                    "status": "failed",
                }
            },
        ),
        ToolResult(
            "false-action-receipt",
            "email_send",
            "completed",
            {
                "action_receipt": {
                    "type": "action_receipt",
                    "action": "email_send",
                    "object_type": "email",
                    "summary": "Message sent.",
                    "sent": False,
                }
            },
        ),
        ToolResult(
            "malformed-send-state",
            "email_send",
            "completed",
            {"sent": "false"},
        ),
        ToolResult(
            "invalid-truth-number",
            "custom_read",
            "completed",
            {"verified": 0, "items": [{"title": "Fabricated result"}]},
        ),
        ToolResult(
            "invalid-truth-text",
            "custom_read",
            "completed",
            {"verified": "false", "items": [{"title": "Fabricated result"}]},
        ),
        ToolResult(
            "invalid-negative-count",
            "custom_read",
            "completed",
            {"items": [{"title": "Fabricated result"}], "count": -1},
        ),
        ToolResult(
            "invalid-fractional-count",
            "custom_read",
            "completed",
            {"items": [{"title": "Fabricated result"}], "count": 0.5},
        ),
        ToolResult(
            "invalid-null-count",
            "custom_read",
            "completed",
            {"items": [{"title": "Fabricated result"}], "count": None},
        ),
        ToolResult(
            "absence-with-records",
            "custom_read",
            "completed",
            {
                "verified": True,
                "found": False,
                "items": [{"title": "Fabricated result"}],
            },
        ),
        ToolResult(
            "absence-with-scalar",
            "custom_read",
            "completed",
            {"verified": True, "found": False, "price": "$1"},
        ),
        ToolResult(
            "unverified-absence",
            "custom_read",
            "completed",
            {"found": False, "message": "No result."},
        ),
        ToolResult(
            "nested-empty-items",
            "custom_read",
            "completed",
            {"message": "No results found.", "result": {"items": []}},
        ),
        ToolResult(
            "nested-empty-text",
            "custom_read",
            "completed",
            {"message": "Read complete.", "result": {"text": ""}},
        ),
        ToolResult("price-bool", "custom_read", "completed", {"price": False}),
        ToolResult("url-bool", "custom_read", "completed", {"url": False}),
        ToolResult("record-number", "custom_read", "completed", {"record": 0}),
        ToolResult("record-bool-list", "custom_read", "completed", {"record": [False]}),
        ToolResult("record-number-list", "custom_read", "completed", {"record": [0]}),
        ToolResult("state-bool-list", "custom_read", "completed", {"state": [False]}),
        ToolResult("forecast-bool-list", "custom_read", "completed", {"forecast": [False]}),
        ToolResult("items-bool", "custom_read", "completed", {"items": [False]}),
        ToolResult("artifact-path-bool", "custom_read", "completed", {"artifact_path": False}),
        ToolResult("artifact-path-number", "custom_read", "completed", {"artifact_path": 0}),
        ToolResult("path-bool", "custom_read", "completed", {"path": False}),
        ToolResult("file-path-bool", "custom_read", "completed", {"file_path": False}),
        ToolResult("download-path-bool", "custom_read", "completed", {"download_path": False}),
        ToolResult("artifact-paths-bool", "custom_read", "completed", {"artifact_paths": [False]}),
        ToolResult(
            "artifact-descriptors-bool",
            "custom_read",
            "completed",
            {"artifact_descriptors": [False]},
        ),
        ToolResult("price-nan", "custom_read", "completed", {"price": float("nan")}),
        ToolResult("temperature-inf", "custom_read", "completed", {"temperature": float("inf")}),
        ToolResult("amount-negative-inf", "custom_read", "completed", {"amount": float("-inf")}),
        *(
            ToolResult(
                f"negative-status-{status}",
                "custom_read",
                "completed",
                {"status": status, "items": [{"title": "Fabricated result"}]},
            )
            for status in (
                "failure",
                "unsuccessful",
                "not_completed",
                "rejected",
                "errored",
                "approval_required",
                "capability_denied",
                "capability_not_granted",
                "nonterminal",
                "running",
                "started",
                "in_progress",
                "queued",
            )
        ),
        ToolResult(
            "absence-positive-count",
            "custom_read",
            "completed",
            {"found": False, "verified": True, "count": 1},
        ),
        ToolResult(
            "absence-positive-result-count",
            "custom_read",
            "completed",
            {"exists": False, "ok": True, "result_count": 3},
        ),
        ToolResult(
            "unrelated-zero-count",
            "custom_read",
            "completed",
            {"messages": [], "item_count": 0},
        ),
    ]
    assert all(
        not _completion_review_evidence_result_is_positive(result)
        for result in empty_or_control_only_results
    )
    assert _completion_review_evidence_result_is_positive(
        ToolResult(
            "nested-error",
            "custom_read",
            "completed",
            {"text": "stale data", "result": {"error": "timeout"}},
        )
    ) is False
    assert _completion_review_evidence_result_is_positive(
        ToolResult(
            "verified-zero",
            "email_search",
            "completed",
            {"messages": [], "resultSizeEstimate": 0},
        )
    ) is True
    assert _completion_review_evidence_result_is_positive(
        ToolResult(
            "empty-crons",
            "list_crons",
            "completed",
            {"crons": [], "count": 0, "message": "No crons scheduled."},
        )
    ) is True
    assert _completion_review_evidence_result_is_positive(
        ToolResult(
            "nested-verified-zero",
            "email_search",
            "completed",
            {"result": {"messages": [], "resultSizeEstimate": 0}},
        )
    ) is True
    assert _completion_review_evidence_result_is_positive(
        ToolResult(
            "action",
            "email_send",
            "completed",
            {
                "action_receipt": {
                    "type": "action_receipt",
                    "action": "email_send",
                    "object_type": "email",
                    "summary": "Message sent.",
                }
            },
        )
    ) is True
    assert _completion_review_evidence_result_is_positive(
        ToolResult(
            "independent-collections",
            "custom_read",
            "completed",
            {
                "items": [{"id": "item-1"}],
                "item_count": 1,
                "messages": [],
                "message_count": 0,
            },
        )
    ) is True
    assert _completion_review_evidence_result_is_positive(
        ToolResult(
            "independent-nested-collections",
            "custom_read",
            "completed",
            {
                "items": [{"id": "item-1"}],
                "item_count": 1,
                "result": {"items": [], "item_count": 0},
            },
        )
    ) is True

    unrelated_coverage = _CompletionReviewDecision(
        "complete",
        all_material_requirements_enumerated=True,
        material_requirements=(
            _CompletionReviewMaterialRequirement(
                "r1",
                "side A",
                "comparison_side",
                True,
                True,
                ("email-result",),
            ),
        ),
    )
    assert _forced_completion_review_is_grounded(
        unrelated_coverage,
        tool_results=[
            ToolResult(
                "email-result",
                "email_search",
                "completed",
                {"messages": [{"id": "message-1", "subject": "Unrelated"}]},
            )
        ],
        requirement_contract=contract,
    ) is False


def test_forced_completion_evidence_reuse_respects_typed_subject_capacity() -> None:
    from nullion.agent_orchestrator import (
        _CompletionRequirementContractRow,
        _CompletionReviewDecision,
        _CompletionReviewMaterialRequirement,
        _forced_completion_review_is_grounded,
    )

    distinct_subject_contract = (
        _CompletionRequirementContractRow(
            "r1",
            "weather in Paris",
            "fact",
            ("weather_forecast",),
            "city-paris",
        ),
        _CompletionRequirementContractRow(
            "r2",
            "weather in Tokyo",
            "fact",
            ("weather_forecast",),
            "city-tokyo",
        ),
    )
    distinct_subject_coverage = _CompletionReviewDecision(
        "complete",
        all_material_requirements_enumerated=True,
        material_requirements=tuple(
            _CompletionReviewMaterialRequirement(
                row.requirement_id,
                row.requirement,
                row.requirement_kind,
                True,
                True,
                ("weather-paris",),
            )
            for row in distinct_subject_contract
        ),
    )
    paris_only = ToolResult(
        "weather-paris",
        "weather_forecast",
        "completed",
        {"location": "Paris", "temperature": 22, "conditions": "sunny"},
    )

    assert _forced_completion_review_is_grounded(
        distinct_subject_coverage,
        tool_results=[paris_only],
        requirement_contract=distinct_subject_contract,
    ) is False

    duplicate_record_coverage = _CompletionReviewDecision(
        "complete",
        all_material_requirements_enumerated=True,
        material_requirements=tuple(
            _CompletionReviewMaterialRequirement(
                row.requirement_id,
                row.requirement,
                row.requirement_kind,
                True,
                True,
                (invocation_id,),
            )
            for row, invocation_id in zip(
                distinct_subject_contract,
                ("weather-paris-1", "weather-paris-2"),
                strict=True,
            )
        ),
    )
    assert _forced_completion_review_is_grounded(
        duplicate_record_coverage,
        tool_results=[
            ToolResult(
                invocation_id,
                "weather_forecast",
                "completed",
                {"location": "Paris", "temperature": 22, "conditions": "sunny"},
            )
            for invocation_id in ("weather-paris-1", "weather-paris-2")
        ],
        requirement_contract=distinct_subject_contract,
    ) is False

    shared_product_contract = (
        _CompletionRequirementContractRow(
            "r1",
            "product title and price",
            "fact",
            ("product_read",),
            "product-1",
        ),
    )
    shared_product_coverage = _CompletionReviewDecision(
        "complete",
        all_material_requirements_enumerated=True,
        material_requirements=tuple(
            _CompletionReviewMaterialRequirement(
                row.requirement_id,
                row.requirement,
                row.requirement_kind,
                True,
                True,
                ("product-1",),
            )
            for row in shared_product_contract
        ),
    )
    assert _forced_completion_review_is_grounded(
        shared_product_coverage,
        tool_results=[
            ToolResult(
                "product-1",
                "product_read",
                "completed",
                {"record": {"title": "Sulfur cleanser", "price": "$39.10"}},
            )
        ],
        requirement_contract=shared_product_contract,
    ) is True

    # A plural result remains unbound at the row level; duplicate or auxiliary
    # rows must not inflate one invocation's subject coverage.
    assert _forced_completion_review_is_grounded(
        distinct_subject_coverage,
        tool_results=[
            ToolResult(
                "weather-paris",
                "weather_forecast",
                "completed",
                {
                    "records": [
                        {"location": "Paris", "temperature": 22},
                        {"location": "Tokyo", "temperature": 27},
                    ]
                },
            )
        ],
        requirement_contract=distinct_subject_contract,
    ) is False


@pytest.mark.parametrize(
    ("tool_name", "requirement_kind"),
    [
        ("browser_run_js", "fact"),
        ("browser_snapshot", "fact"),
        ("browser_extract_text", "fact"),
        ("browser_extract_items", "fact"),
        ("browser_run_js", "other"),
    ],
)
def test_forced_completion_accepts_browser_read_evidence_without_assertion_binding(
    tool_name: str,
    requirement_kind: str,
) -> None:
    from nullion.agent_orchestrator import (
        _CompletionRequirementContractRow,
        _CompletionReviewDecision,
        _CompletionReviewMaterialRequirement,
        _forced_completion_review_is_grounded,
    )

    contract = (
        _CompletionRequirementContractRow(
            "r1",
            "verified product fact",
            requirement_kind,
            (tool_name,),
            "requested-product",
        ),
    )
    decision = _CompletionReviewDecision(
        "complete",
        all_material_requirements_enumerated=True,
        material_requirements=(
            _CompletionReviewMaterialRequirement(
                "r1",
                "verified product fact",
                requirement_kind,
                True,
                True,
                ("js-result",),
            ),
        ),
    )

    assert _forced_completion_review_is_grounded(
        decision,
        tool_results=[
            ToolResult(
                "js-result",
                tool_name,
                "completed",
                {"result": {"title": "Unbound page text", "price": "$1"}},
            )
        ],
        requirement_contract=contract,
    ) is True


def test_non_forced_risky_completion_rejects_one_invocation_for_distinct_subjects() -> None:
    from nullion.agent_orchestrator import _review_risky_agent_completion

    class Client:
        planner_calls = 0
        reviewer_calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            import json

            system = str(kwargs.get("system") or "")
            if system.startswith("You are a material-requirements planner"):
                self.planner_calls += 1
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement": "weather in Paris",
                                            "requirement_kind": "fact",
                                            "evidence_subject_id": "city-paris",
                                            "eligible_evidence_tool_names": [
                                                "weather_forecast"
                                            ],
                                        },
                                        {
                                            "requirement": "weather in Tokyo",
                                            "requirement_kind": "fact",
                                            "evidence_subject_id": "city-tokyo",
                                            "eligible_evidence_tool_names": [
                                                "weather_forecast"
                                            ],
                                        },
                                    ],
                                }
                            ),
                        }
                    ],
                }
            self.reviewer_calls += 1
            payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
            return {
                "stop_reason": "end_turn",
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
                                        "requirement_id": row["requirement_id"],
                                        "requirement": row["requirement"],
                                        "requirement_kind": row["requirement_kind"],
                                        "satisfied": True,
                                        "draft_covered": True,
                                        "evidence_invocation_ids": ["weather-paris"],
                                    }
                                    for row in payload["material_requirement_contract"]
                                ],
                            }
                        ),
                    }
                ],
            }

    registry = ToolRegistry()
    for name in ("browser_assert_page_state", "weather_forecast"):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {},
            ),
        )
    client = Client()
    decision = _review_risky_agent_completion(
        {
            "conversation_id": "telegram:two-cities",
            "principal_id": "telegram_chat",
            "user_message": "opaque two-entity request",
            "completion_review_count": 0,
            "orchestrator": SimpleNamespace(model_client=client),
            "tool_registry": registry,
            "runtime_store": None,
            "tool_flow_context": {},
        },
        final_text="Both requested records are complete.",
        tool_results=[
            ToolResult(
                "assert-failed",
                "browser_assert_page_state",
                "completed",
                {"verified": False, "result": {"ok": False}},
            ),
            ToolResult(
                "weather-paris",
                "weather_forecast",
                "completed",
                {"location": "Paris", "temperature": 22, "conditions": "sunny"},
            ),
        ],
        artifacts=[],
    )

    assert client.planner_calls == 0
    assert client.reviewer_calls == 0
    assert decision is None


def test_completion_requirement_contract_rejects_unknown_evidence_tools() -> None:
    from nullion.agent_orchestrator import _parse_completion_requirement_contract

    response = {
        "content": [
            {
                "type": "text",
                "text": __import__("json").dumps(
                    {
                        "all_material_requirements_enumerated": True,
                        "material_requirements": [
                            {
                                "requirement": "the requested product",
                                "requirement_kind": "fact",
                                "evidence_subject_id": "requested-product",
                                "eligible_evidence_tool_names": ["email_search"],
                            }
                        ],
                    }
                ),
            }
        ]
    }

    assert _parse_completion_requirement_contract(
        response,
        registered_tool_names={"browser_extract_detail"},
    ) == ()
    parsed = _parse_completion_requirement_contract(
        response,
        registered_tool_names={"email_search"},
    )
    assert len(parsed) == 1
    assert parsed[0].evidence_subject_id == "requested-product"

    for malformed_subject_id in (None, "", 7, ["requested-product"], "x" * 121):
        malformed_response = {
            "content": [
                {
                    "type": "text",
                    "text": __import__("json").dumps(
                        {
                            "all_material_requirements_enumerated": True,
                            "material_requirements": [
                                {
                                    "requirement": "the requested product",
                                    "requirement_kind": "fact",
                                    "evidence_subject_id": malformed_subject_id,
                                    "eligible_evidence_tool_names": ["email_search"],
                                }
                            ],
                        }
                    ),
                }
            ]
        }
        assert _parse_completion_requirement_contract(
            malformed_response,
            registered_tool_names={"email_search"},
        ) == ()

    duplicate_subject_response = {
        "content": [
            {
                "type": "text",
                "text": __import__("json").dumps(
                    {
                        "all_material_requirements_enumerated": True,
                        "material_requirements": [
                            {
                                "requirement": "product title",
                                "requirement_kind": "fact",
                                "evidence_subject_id": "requested-product",
                                "eligible_evidence_tool_names": ["email_search"],
                            },
                            {
                                "requirement": "product price",
                                "requirement_kind": "fact",
                                "evidence_subject_id": "requested-product",
                                "eligible_evidence_tool_names": ["email_search"],
                            },
                        ],
                    }
                ),
            }
        ]
    }
    assert _parse_completion_requirement_contract(
        duplicate_subject_response,
        registered_tool_names={"email_search"},
    ) == ()


def test_forced_completion_coverage_enforces_each_rows_eligible_tool() -> None:
    from nullion.agent_orchestrator import (
        _CompletionRequirementContractRow,
        _CompletionReviewDecision,
        _CompletionReviewMaterialRequirement,
        _forced_completion_review_is_grounded,
    )

    contract = (
        _CompletionRequirementContractRow(
            "r1",
            "public result",
            "fact",
            ("web_search",),
        ),
        _CompletionRequirementContractRow(
            "r2",
            "account result",
            "fact",
            ("email_search",),
        ),
    )
    tool_results = [
        ToolResult(
            "web-result",
            "web_search",
            "completed",
            {"results": [{"title": "Public record", "url": "https://example.test"}]},
        ),
        ToolResult(
            "email-result",
            "email_search",
            "completed",
            {"messages": [{"id": "message-1", "subject": "Account record"}]},
        ),
    ]

    def decision(first_id: str, second_id: str) -> _CompletionReviewDecision:
        return _CompletionReviewDecision(
            "complete",
            all_material_requirements_enumerated=True,
            material_requirements=(
                _CompletionReviewMaterialRequirement(
                    "r1", "public result", "fact", True, True, (first_id,)
                ),
                _CompletionReviewMaterialRequirement(
                    "r2", "account result", "fact", True, True, (second_id,)
                ),
            ),
        )

    assert _forced_completion_review_is_grounded(
        decision("web-result", "email-result"),
        tool_results=tool_results,
        requirement_contract=contract,
    ) is True
    assert _forced_completion_review_is_grounded(
        decision("email-result", "web-result"),
        tool_results=tool_results,
        requirement_contract=contract,
    ) is False


def test_compact_completion_evidence_keeps_every_selected_invocation_id() -> None:
    from nullion.agent_orchestrator import _compact_tool_evidence_for_repair

    results = [
        ToolResult(
            f"invocation-{index}",
            "browser_extract_text",
            "completed",
            {"text": f"record {index} " + ("x" * 800)},
        )
        for index in range(12)
    ]

    payload = __import__("json").loads(
        _compact_tool_evidence_for_repair(results, limit=3_000)
    )

    assert [record["invocation_id"] for record in payload] == [
        f"invocation-{index}" for index in range(12)
    ]
    assert all(record["status"] == "completed" for record in payload)


def test_completion_review_cannot_accept_missing_required_current_turn_source(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.agent_orchestrator import _review_risky_agent_completion

    artifact_path = tmp_path / "briefing.html"
    artifact_path.write_text("<html>Draft briefing</html>", encoding="utf-8")
    monkeypatch.setattr(
        "nullion.agent_orchestrator._artifact_roots_for_agent_turn",
        lambda runtime_store, principal_id: (tmp_path,),
    )

    class Client:
        payload = None

        def create(self, **kwargs):  # noqa: ANN003
            if str(kwargs.get("system") or "").startswith(
                "You are a material-requirements planner"
            ):
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                '{"all_material_requirements_enumerated":true,'
                                '"material_requirements":[{"requirement":"the requested final artifact",'
                                '"requirement_kind":"deliverable",'
                                '"evidence_subject_id":"final-artifact",'
                                '"eligible_evidence_tool_names":["file_write"]}]}'
                            ),
                        }
                    ],
                }
            self.payload = __import__("json").loads(kwargs["messages"][0]["content"][0]["text"])
            contract_row = self.payload["material_requirement_contract"][0]
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": __import__("json").dumps(
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
                                        "evidence_invocation_ids": ["file-1"],
                                    }
                                ],
                            }
                        ),
                    }
                ],
            }

    registry = ToolRegistry()
    for name in ("browser_navigate", "weather_forecast", "file_write"):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    client = Client()
    tool_results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {
                "required_tool_names": ["browser_navigate", "weather_forecast", "file_write"],
                "requested_required_tool_names": [
                    "browser_navigate",
                    "calendar_list",
                    "weather_forecast",
                    "file_write",
                ],
                "unavailable_required_tool_names": ["calendar_list"],
                "active_connector_providers": [],
                "available_sources": [],
                "artifact_extensions": [".html"],
            },
        ),
        ToolResult("weather-1", "weather_forecast", "failed", {}, error="missing location"),
        ToolResult("file-1", "file_write", "completed", {"artifact_path": str(artifact_path)}),
    ]

    decision = _review_risky_agent_completion(
        {
            "conversation_id": "web:1",
            "principal_id": "web",
            "user_message": "structured cross-domain artifact request",
            "completion_review_count": 0,
            "orchestrator": SimpleNamespace(model_client=client),
            "tool_registry": registry,
            "runtime_store": None,
            "tool_flow_context": {
                "requires_artifact_delivery": True,
                "required_artifact_extensions": [".html"],
            },
        },
        final_text="Everything is complete.",
        tool_results=tool_results,
        artifacts=[str(artifact_path)],
    )

    assert decision is not None
    assert decision.disposition == "retry"
    assert set(decision.retry_tool_names) == {"browser_navigate", "weather_forecast"}
    assert client.payload["required_source_and_action_contract"] == {
        "requested_required_tool_names": [
            "browser_navigate",
            "calendar_list",
            "weather_forecast",
            "file_write",
        ],
        "callable_required_tool_names": ["browser_navigate", "weather_forecast", "file_write"],
        "unavailable_required_tool_names": ["calendar_list"],
        "missing_callable_required_tool_names": ["browser_navigate", "weather_forecast"],
        "required_tool_attempt_statuses": {"weather_forecast": ["failed"], "file_write": ["completed"]},
        "active_connector_providers": [],
        "available_sources": [],
        "unavailable_tools": [],
        "unavailable_capabilities": [],
        "connector_source_unavailable": False,
        "missing_connector_app_scope": False,
        "source_selection_required": False,
    }
    assert {item["tool_name"] for item in client.payload["tool_evidence"]} == {
        "request_tool_scope",
        "weather_forecast",
        "file_write",
    }


def test_completion_review_ignores_superseded_artifact_producer_failure(tmp_path: Path) -> None:
    from nullion.agent_orchestrator import _completion_review_required

    workbook = tmp_path / "briefing.xlsx"
    workbook.write_bytes(b"valid workbook bytes")
    results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {
                "required_tool_names": ["weather_forecast"],
                "artifact_extensions": [".xlsx"],
            },
        ),
        ToolResult("weather-1", "weather_forecast", "completed", {"temperature": 72}),
        ToolResult(
            "xlsx-failed",
            "spreadsheet_create",
            "failed",
            {"reason": "row_count_mismatch"},
            error="row count mismatch",
        ),
        ToolResult(
            "xlsx-complete",
            "spreadsheet_create",
            "completed",
            {"artifact_paths": [str(workbook)]},
        ),
    ]

    required = _completion_review_required(
        {
            "completion_review_count": 0,
            "tool_registry": ToolRegistry(),
            "tool_flow_context": {
                "requires_artifact_delivery": True,
                "required_artifact_extensions": [".xlsx"],
            },
        },
        tool_results=results,
    )

    assert required is False


def test_completion_review_requires_semantic_check_for_unverified_browser_turn_without_artifact() -> None:
    from nullion.agent_orchestrator import _completion_review_required

    registry = _registry_with_tool(
        "browser_extract_text",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    required = _completion_review_required(
        {
            "completion_review_count": 0,
            "tool_registry": registry,
            "tool_flow_context": {},
        },
        tool_results=[
            ToolResult(
                "assert-1",
                "browser_assert_page_state",
                "completed",
                {"result": {"ok": False}},
            ),
            ToolResult(
                "extract-1",
                "browser_extract_text",
                "completed",
                {"text": "Produkt verifiziert: fluessige Seife mit 5% Schwefel, $9.99."},
            ),
        ],
    )

    assert required is False


@pytest.mark.parametrize(
    "tool_results",
    [
        [
            ToolResult(
                "assert-failed",
                "browser_assert_page_state",
                "completed",
                {"verified": False, "result": {"ok": False}},
            ),
            ToolResult(
                "raw-js",
                "browser_run_js",
                "completed",
                {"result": {"title": "Unbound result"}},
            ),
        ],
        [
            ToolResult("search-failed", "web_search", "failed", {}, error="HTTP 402"),
            ToolResult(
                "raw-text",
                "browser_extract_text",
                "completed",
                {"text": "Generic homepage rows"},
            ),
        ],
        [ToolResult("pending-browser", "browser_run_js", "pending", {"result": {"title": "x"}})],
        [ToolResult("blocked-browser", "browser_run_js", "blocked", {"result": {"title": "x"}})],
        [ToolResult("denied-browser", "browser_run_js", "denied", {"result": {"title": "x"}})],
        [ToolResult("cancelled-browser", "browser_run_js", "cancelled", {"result": {"title": "x"}})],
        [ToolResult("unknown-browser", "browser_run_js", "mystery", {"result": {"title": "x"}})],
    ],
)
def test_completion_review_cannot_be_bypassed_by_raw_or_nonterminal_browser_results(
    tool_results: list[ToolResult],
) -> None:
    from nullion.agent_orchestrator import _completion_review_required

    assert _completion_review_required(
        {
            "completion_review_count": 0,
            "tool_registry": ToolRegistry(),
            "tool_flow_context": {},
        },
        tool_results=tool_results,
    ) is False


def test_completion_review_tool_selection_preserves_older_groundable_evidence() -> None:
    from nullion.agent_orchestrator import _completion_review_tool_results

    critical = ToolResult(
        "critical-product",
        "product_read",
        "completed",
        {"record": {"title": "Verified product", "price": "$19.99"}},
    )
    results = [
        critical,
        *[
            ToolResult(
                f"snapshot-{index}",
                "browser_snapshot",
                "completed",
                {"text": f"navigation snapshot {index}"},
            )
            for index in range(10)
        ],
        ToolResult("final-failure", "browser_run_js", "failed", {}, error="failed"),
    ]

    selected = _completion_review_tool_results(results)

    assert critical in selected
    assert results[-1] in selected


def test_complete_agent_turn_marks_raw_browser_recovery_draft_unfulfilled() -> None:
    from nullion.agent_orchestrator import _complete_agent_turn

    update = _complete_agent_turn(
        {
            "conversation_id": "telegram:raw-recovery",
            "principal_id": "telegram_chat",
            "user_message": "opaque live lookup",
            "completion_review_count": 0,
            "tool_registry": ToolRegistry(),
            "tool_flow_context": {},
            "tool_results": [
                ToolResult(
                    "assert-failed",
                    "browser_assert_page_state",
                    "completed",
                    {"verified": False, "result": {"ok": False}},
                ),
                ToolResult(
                    "raw-js",
                    "browser_run_js",
                    "completed",
                    {"result": {"title": "Unbound result"}},
                ),
            ],
            "artifacts": [],
        },
        final_text="This is definitely the matching result.",
    )

    assert update["result"].response_fulfilled is None


def test_completion_review_requires_semantic_check_for_unbound_typed_browser_detail() -> None:
    from nullion.agent_orchestrator import _completion_review_required

    registry = _registry_with_tool(
        "browser_extract_detail",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    required = _completion_review_required(
        {
            "completion_review_count": 0,
            "tool_registry": registry,
            "tool_flow_context": {},
        },
        tool_results=[
            ToolResult(
                "assert-1",
                "browser_assert_page_state",
                "completed",
                {"result": {"ok": False}},
            ),
            ToolResult(
                "detail-1",
                "browser_extract_detail",
                "completed",
                {
                    "schema_version": 1,
                    "kind": "browser_dom_detail",
                    "origin": "runtime_owned_dom_extractor",
                    "record_scope": "selected_root",
                    "page": {
                        "title": "Produkt",
                        "url": "https://shop.example.test/item",
                    },
                    "record": {
                        "title": "Produkt",
                        "url": "https://shop.example.test/item",
                        "price_text": "$9.99",
                    },
                    "page_state_assertion": {"verified": True},
                },
            ),
        ],
    )

    assert required is False


def test_browser_page_state_continuation_survives_recovery_navigation_without_fresh_evidence() -> None:
    from nullion.agent_orchestrator import _browser_page_state_continuation_tools

    registry = _registry_with_tool(
        "browser_run_js",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    continuation_tools = _browser_page_state_continuation_tools(
        tool_results=[
            ToolResult(
                "assert-a",
                "browser_assert_page_state",
                "completed",
                {
                    "session_id": "browser-session",
                    "verified": False,
                    "result": {
                        "ok": False,
                        "url": "https://shop.example.test/item/a",
                        "missing": [{"expected": "in stock", "match": ""}],
                    },
                },
            ),
            ToolResult(
                "navigate-b",
                "browser_navigate",
                "completed",
                {
                    "session_id": "browser-session",
                    "result": {"url": "https://shop.example.test/item/b"},
                },
            ),
        ],
        tool_registry=registry,
    )

    assert continuation_tools == ("browser_run_js",)


def test_completion_review_stays_off_fast_path_for_successful_browser_turn() -> None:
    from nullion.agent_orchestrator import _completion_review_required

    required = _completion_review_required(
        {
            "completion_review_count": 0,
            "tool_registry": ToolRegistry(),
            "tool_flow_context": {},
        },
        tool_results=[
            ToolResult(
                "extract-1",
                "browser_extract_text",
                "completed",
                {"text": "Structured result evidence"},
            ),
        ],
    )

    assert required is False


def test_completion_review_receives_product_evidence_from_end_of_long_browser_page() -> None:
    from nullion.agent_orchestrator import _review_risky_agent_completion

    product_evidence = (
        "Dermaharmony 5% Sulfur and 2% Salicylic Acid Liquid Castile Soap. "
        "ACTIVE INGREDIENTS: 5% Sulfur and 2% Salicylic Acid. Price $9.99."
    )
    rendered_page = "Search page\n" + ("unrelated recommendation\n" * 800) + product_evidence

    class Client:
        payload: dict[str, object] | None = None
        system: str = ""

        def create(self, **kwargs):  # noqa: ANN003
            import json

            if str(kwargs.get("system") or "").startswith(
                "You are a material-requirements planner"
            ):
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement": "the requested verified product",
                                            "requirement_kind": "constraint",
                                            "evidence_subject_id": "requested-product",
                                            "eligible_evidence_tool_names": [
                                                "browser_extract_text"
                                            ],
                                        }
                                    ],
                                }
                            ),
                        }
                    ],
                }
            self.payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
            self.system = kwargs["system"]
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"disposition":"retry","unresolved_requirements":'
                            '["draft omitted the verified product"],'
                            '"retry_tool_names":["browser_extract_text"]}'
                        ),
                    }
                ],
            }

    client = Client()
    registry = _registry_with_tool(
        "browser_extract_text",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    decision = _review_risky_agent_completion(
        {
            "conversation_id": "telegram:browser-review",
            "principal_id": "telegram_chat",
            "user_message": "opaque product request",
            "completion_review_count": 0,
            "orchestrator": SimpleNamespace(model_client=client),
            "tool_registry": registry,
            "runtime_store": None,
            "tool_flow_context": {},
        },
        final_text="The task is blocked.",
        tool_results=[
            ToolResult(
                "assert-1",
                "browser_assert_page_state",
                "completed",
                {"verified": False, "result": {"ok": False}},
            ),
            ToolResult(
                "extract-1",
                "browser_extract_text",
                "completed",
                {"url": "https://example.test/product", "text": rendered_page},
            ),
        ],
        artifacts=[],
    )

    assert decision is None
    assert client.payload is None


def test_agent_turn_recovers_when_typed_product_evidence_supersedes_failed_source_attempt() -> None:
    product_evidence = (
        "Dermaharmony 5% Sulfur and 2% Salicylic Acid Liquid Castile Soap\n"
        "Liquid wash, not a bar\n"
        "ACTIVE INGREDIENTS: 5% Sulfur and 2% Salicylic Acid\n"
        "Price: $9.99\n"
        "URL: https://example.test/product"
    )
    handler_calls: list[str] = []

    class Client:
        def __init__(self) -> None:
            self.normal_calls = 0
            self.review_calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            system = str(kwargs.get("system") or "")
            if system.startswith("You are a material-requirements planner"):
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": __import__("json").dumps(
                                {
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement": "the requested verified sulfur product",
                                            "requirement_kind": "constraint",
                                            "evidence_subject_id": "requested-product",
                                            "eligible_evidence_tool_names": [
                                                "product_read"
                                            ],
                                        }
                                    ],
                                }
                            ),
                        }
                    ],
                }
            if system.startswith("You are a semantic completion reviewer"):
                self.review_calls += 1
                disposition = "retry" if self.review_calls == 1 else "complete"
                unresolved = ["use the stronger product evidence in the response"] if disposition == "retry" else []
                retry_tools = ["product_read"] if disposition == "retry" else []
                import json

                decision: dict[str, object] = {
                    "disposition": disposition,
                    "unresolved_requirements": unresolved,
                    "retry_tool_names": retry_tools,
                }
                if disposition == "complete":
                    review_payload = json.loads(
                        kwargs["messages"][0]["content"][0]["text"]
                    )
                    contract_row = review_payload["material_requirement_contract"][0]
                    evidence_id = next(
                        str(item["invocation_id"])
                        for item in review_payload["tool_evidence"]
                        if item.get("tool_name") == "product_read"
                        and item.get("status") == "completed"
                    )
                    decision.update(
                        {
                            "all_material_requirements_enumerated": True,
                            "material_requirements": [
                                {
                                    "requirement_id": contract_row["requirement_id"],
                                    "requirement": contract_row["requirement"],
                                    "requirement_kind": contract_row["requirement_kind"],
                                    "satisfied": True,
                                    "draft_covered": True,
                                    "evidence_invocation_ids": [evidence_id],
                                }
                            ],
                        }
                    )
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(decision),
                        }
                    ],
                }

            self.normal_calls += 1
            if self.normal_calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "fetch-product",
                            "name": "web_fetch",
                            "input": {"url": "https://blocked.example.test/product"},
                        }
                    ],
                }
            if self.normal_calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "extract-product",
                            "name": "product_read",
                            "input": {"selector": "#product-detail"},
                        }
                    ],
                }
            if self.normal_calls == 3:
                return {
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "The live browser task is blocked."}],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "I found a matching liquid option: Dermaharmony Liquid Castile Soap contains "
                            "5% sulfur and 2% salicylic acid. It was listed at $9.99. "
                            "https://example.test/product"
                        ),
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            "web_fetch",
            "Fetch a source page.",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            5,
        ),
        lambda invocation: (
            handler_calls.append(invocation.tool_name)
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {},
                error="source blocked",
            )
        ),
    )
    registry.register(
        ToolSpec(
            "product_read",
            "Read a typed product record.",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            5,
        ),
        lambda invocation: (
            handler_calls.append(invocation.tool_name)
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "record": {
                        "title": "Dermaharmony 5% Sulfur Liquid Castile Soap",
                        "form": "liquid wash",
                        "active_ingredients": "5% Sulfur and 2% Salicylic Acid",
                        "price": "$9.99",
                        "url": "https://example.test/product",
                        "details": product_evidence,
                    }
                },
            )
        ),
    )
    client = Client()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:product-recovery",
        principal_id="telegram_chat",
        user_message="Finde eine fluessige Schwefelseife mit echtem Schwefel.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert handler_calls == ['web_fetch', 'product_read']
    assert client.review_calls == 0
    assert result.final_text
    assert result.tool_results[0].status == 'failed'


def test_completion_review_artifact_evidence_extracts_xlsx_and_pdf_text(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from openpyxl import Workbook
    from nullion.agent_orchestrator import _completion_review_artifact_evidence
    from nullion.tools import ToolInvocation, _build_pdf_create_handler

    workbook_path = tmp_path / "briefing.xlsx"
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Briefing"
    worksheet.append(["NULLION EXECUTIVE BRIEFING QA", "Calendar status: unavailable"])
    workbook.save(workbook_path)

    pdf_path = tmp_path / "briefing.pdf"
    pdf_result = _build_pdf_create_handler(workspace_root=tmp_path)(
        ToolInvocation(
            invocation_id="pdf-1",
            tool_name="pdf_create",
            principal_id="test",
            arguments={
                "output_path": str(pdf_path),
                "title": "Briefing",
                "text_pages": [
                    "NULLION EXECUTIVE BRIEFING QA\nCalendar status: unavailable"
                ],
            },
        )
    )
    assert pdf_result.status == "completed", pdf_result.error

    monkeypatch.setattr(
        "nullion.agent_orchestrator._artifact_roots_for_agent_turn",
        lambda runtime_store, principal_id: (tmp_path,),
    )
    evidence = _completion_review_artifact_evidence(
        {"runtime_store": None, "principal_id": "web"},
        [str(workbook_path), str(pdf_path)],
    )

    assert evidence[0]["text_extraction_method"] == "openpyxl"
    assert "NULLION EXECUTIVE BRIEFING QA" in evidence[0]["content_preview"]
    assert evidence[1]["text_extraction_method"] == "pypdf"
    assert "Calendar status: unavailable" in evidence[1]["content_preview"]


def test_completion_review_merges_missing_required_tools_into_model_retry_choice(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.agent_orchestrator import _review_risky_agent_completion

    artifact_path = tmp_path / "briefing.html"
    artifact_path.write_text("<html>Draft briefing</html>", encoding="utf-8")
    monkeypatch.setattr(
        "nullion.agent_orchestrator._artifact_roots_for_agent_turn",
        lambda runtime_store, principal_id: (tmp_path,),
    )

    class Client:
        def create(self, **kwargs):  # noqa: ANN003
            if str(kwargs.get("system") or "").startswith(
                "You are a material-requirements planner"
            ):
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                '{"all_material_requirements_enumerated":true,'
                                '"material_requirements":[{"requirement":"the requested final artifact",'
                                '"requirement_kind":"deliverable",'
                                '"evidence_subject_id":"final-artifact",'
                                '"eligible_evidence_tool_names":["file_write"]}]}'
                            ),
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"disposition":"retry","unresolved_requirements":["repair workbook"],'
                            '"retry_tool_names":["spreadsheet_create"]}'
                        ),
                    }
                ],
            }

    registry = ToolRegistry()
    for name in ("browser_navigate", "weather_forecast", "spreadsheet_create", "file_write"):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    tool_results = [
        ToolResult(
            "scope-1",
            "request_tool_scope",
            "completed",
            {
                "required_tool_names": ["browser_navigate", "weather_forecast", "file_write"],
                "requested_required_tool_names": ["browser_navigate", "weather_forecast", "file_write"],
                "artifact_extensions": [".html"],
            },
        ),
        ToolResult("weather-1", "weather_forecast", "failed", {}, error="missing location"),
        ToolResult("file-1", "file_write", "completed", {"artifact_path": str(artifact_path)}),
    ]

    decision = _review_risky_agent_completion(
        {
            "conversation_id": "web:2",
            "principal_id": "web",
            "user_message": "structured cross-domain artifact request",
            "completion_review_count": 0,
            "orchestrator": SimpleNamespace(model_client=Client()),
            "tool_registry": registry,
            "runtime_store": None,
            "tool_flow_context": {
                "requires_artifact_delivery": True,
                "required_artifact_extensions": [".html"],
            },
        },
        final_text="Draft complete.",
        tool_results=tool_results,
        artifacts=[str(artifact_path)],
    )

    assert decision is not None
    assert decision.disposition == "retry"
    assert set(decision.retry_tool_names) == {
        "spreadsheet_create",
        "browser_navigate",
        "weather_forecast",
    }
    assert "repair workbook" in decision.unresolved_requirements


def test_agent_turn_graph_does_not_claim_success_when_semantic_retry_budget_is_exhausted(monkeypatch) -> None:
    from nullion.agent_orchestrator import (
        _CompletionReviewDecision,
        _agent_turn_finalize_node,
    )

    monkeypatch.setattr(
        "nullion.agent_orchestrator._review_risky_agent_completion",
        lambda *args, **kwargs: _CompletionReviewDecision(
            "retry",
            ("the requested records remain unverified",),
            ("browser_extract_items",),
        ),
    )
    registry = _registry_with_tool(
        "browser_extract_items",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    update = _agent_turn_finalize_node(
        {
            "conversation_id": "telegram:123",
            "principal_id": "telegram_chat",
            "user_message": "opaque request",
            "content": [{"type": "text", "text": "Everything is complete."}],
            "messages": [],
            "tool_results": [ToolResult("tool-1", "browser_extract_items", "completed", {})],
            "artifacts": [],
            "tool_registry": registry,
            "runtime_store": None,
            "completion_review_count": 2,
        }
    )

    result = update["result"]
    assert "couldn't complete this request" in result.final_text.lower()
    assert result.completion_failure_reply == result.final_text
    assert "everything is complete" not in result.final_text.lower()
    assert result.response_fulfilled is False


def test_agent_turn_graph_bounds_and_focuses_semantic_completion_recovery(monkeypatch) -> None:
    from nullion.agent_orchestrator import (
        _CompletionReviewDecision,
        _agent_turn_finalize_node,
    )

    monkeypatch.setattr(
        "nullion.agent_orchestrator._review_risky_agent_completion",
        lambda *args, **kwargs: _CompletionReviewDecision(
            "retry",
            ("the final artifact still lacks verified records",),
            ("browser_extract_items", "file_patch"),
        ),
    )
    registry = ToolRegistry()
    for name in ("browser_extract_items", "file_patch", "file_search"):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    update = _agent_turn_finalize_node(
        {
            "conversation_id": "telegram:123",
            "principal_id": "telegram_chat",
            "user_message": "opaque request",
            "content": [{"type": "text", "text": "Everything is complete."}],
            "messages": [],
            "tool_results": [ToolResult("tool-1", "file_search", "failed", {}, error="unverified")],
            "artifacts": [],
            "tool_registry": registry,
            "runtime_store": None,
            "completion_review_count": 0,
            "iterations": 5,
            "max_iterations": 24,
        }
    )

    assert update["max_iterations"] == 24
    assert {definition["name"] for definition in update["tool_registry"].list_tool_definitions()} == {
        "browser_extract_items",
        "file_patch",
    }
    assert update["completion_review_unresolved_requirements"] == [
        "the final artifact still lacks verified records"
    ]


def test_agent_turn_graph_extends_semantic_recovery_budget_near_existing_limit(monkeypatch) -> None:
    from nullion.agent_orchestrator import (
        _CompletionReviewDecision,
        _agent_turn_finalize_node,
    )

    monkeypatch.setattr(
        "nullion.agent_orchestrator._review_risky_agent_completion",
        lambda *args, **kwargs: _CompletionReviewDecision(
            "retry",
            ("stronger structured browser evidence has not reached the final response",),
            ("browser_extract_text",),
        ),
    )
    registry = _registry_with_tool(
        "browser_extract_text",
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )

    update = _agent_turn_finalize_node(
        {
            "conversation_id": "telegram:browser-recovery",
            "principal_id": "telegram_chat",
            "user_message": "opaque request",
            "content": [{"type": "text", "text": "The task is blocked."}],
            "messages": [],
            "tool_results": [
                ToolResult(
                    "assert-1",
                    "browser_assert_page_state",
                    "completed",
                    {"verified": False, "result": {"ok": False}},
                ),
                ToolResult(
                    "extract-1",
                    "browser_extract_text",
                    "completed",
                    {"text": "Verified product details are visible."},
                ),
            ],
            "artifacts": [],
            "tool_registry": registry,
            "runtime_store": None,
            "completion_review_count": 0,
            "iterations": 23,
            "max_iterations": 24,
        }
    )

    assert update["max_iterations"] == 31


def test_agent_turn_graph_retries_one_model_timeout_before_completing() -> None:
    from nullion.model_clients import ModelClientTimeoutError

    class Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                raise ModelClientTimeoutError("timed out")
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Recovered."}]}

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="opaque request",
        conversation_history=[],
        tool_registry=ToolRegistry(),
        policy_store=None,
        approval_store=None,
    )

    assert result.final_text == "Recovered."
    assert len(client.calls) == 2
    assert client.calls[0]["timeout"] == 45.0
    assert "text_delta_callback" not in client.calls[1]


def test_agent_turn_graph_retries_focused_artifact_producer_timeout(tmp_path: Path) -> None:
    from nullion.model_clients import ModelClientTimeoutError

    report_path = tmp_path / "verified-report.html"

    class Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                            {
                                "type": "tool_use",
                                "id": "source-1",
                                "name": "file_read",
                                "input": {"path": str(tmp_path / "evidence.txt")},
                            },
                    ],
                }
            if len(self.calls) == 2:
                raise ModelClientTimeoutError("focused producer timed out")
            if len(self.calls) > 3:
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Report created.\nMEDIA:" + str(report_path)}]}
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "write-1",
                        "name": "file_write",
                        "input": {"path": str(report_path), "content": "<html>verified report</html>"},
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec("file_read", "collect", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"url": "https://example.test/evidence", "text": "verified source evidence"},
        ),
    )

    def write_report(invocation):  # noqa: ANN001
        report_path.write_text(str(invocation.arguments.get("content") or ""), encoding="utf-8")
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"path": str(report_path)},
        )

    registry.register(
        ToolSpec("file_write", "write", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        write_report,
    )
    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="opaque report request",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        tool_flow_context={
            "requires_artifact_delivery": True,
            "required_artifact_extensions": [".html"],
            "artifact_focus_min_source_results": 1,
        },
    )

    assert len(client.calls) == 4
    assert {tool["name"] for tool in client.calls[1]["tools"]} == {"file_write"}
    assert {tool["name"] for tool in client.calls[2]["tools"]} == {"file_write"}
    assert report_path.exists()
    assert result.artifact_delivery_satisfied is True
    assert result.model_timed_out is False


def test_agent_turn_graph_keeps_task_open_after_two_model_timeouts() -> None:
    from nullion.model_clients import ModelClientTimeoutError

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN003
            self.calls += 1
            raise ModelClientTimeoutError("timed out")

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="opaque request",
        conversation_history=[],
        tool_registry=ToolRegistry(),
        policy_store=None,
        approval_store=None,
    )

    assert client.calls == 2
    assert "couldn't complete this request" in result.final_text.lower()
    assert result.completion_failure_reply == result.final_text
    assert "time limit" in result.final_text.lower()
    assert "timed out twice" not in result.final_text.lower()
    assert result.model_timed_out is True
    assert result.response_fulfilled is False

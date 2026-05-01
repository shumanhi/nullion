from __future__ import annotations

from datetime import UTC, datetime

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
            self.suspended = []

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

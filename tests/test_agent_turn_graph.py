from __future__ import annotations

from nullion.agent_orchestrator import AgentOrchestrator
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

from __future__ import annotations

from nullion.agent_orchestrator import AgentOrchestrator
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
from nullion.plugins.browser_plugin.browser_tools import BrowserTools
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


class FakeBrowserBackend:
    def __init__(self) -> None:
        self.closed_sessions: list[str] = []

    async def navigate(self, session_id: str, url: str) -> str:
        return f"navigated:{session_id}:{url}"

    async def close_session(self, session_id: str) -> None:
        self.closed_sessions.append(session_id)


def test_browser_tools_cleanup_closes_tracked_scope_only() -> None:
    backend = FakeBrowserBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    result = tools.browser_navigate(
        ToolInvocation(
            invocation_id="inv-browser",
            tool_name="browser_navigate",
            principal_id="operator",
            arguments={"url": "https://example.com", "session_id": "tab-1"},
            capsule_id="scope-1",
        )
    )
    assert result.status == "completed"

    tools.close_tracked_sessions("other-scope")
    assert backend.closed_sessions == []

    tools.close_tracked_sessions("scope-1")
    assert backend.closed_sessions == ["tab-1"]


def test_agent_turn_runs_scoped_browser_cleanup_hook() -> None:
    cleanup_scopes: list[str | None] = []
    invocation_scopes: list[str | None] = []

    class ModelClient:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, *, messages, tools):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool-1",
                            "name": "browser_navigate",
                            "input": {"url": "https://example.com"},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "done"}]}

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="browser_navigate",
            description="Navigate",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        lambda invocation: (
            invocation_scopes.append(invocation.capsule_id)
            or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True})
        ),
    )
    registry.register_cleanup_hook(lambda scope_id: cleanup_scopes.append(scope_id))

    result = AgentOrchestrator(model_client=ModelClient()).run_turn(
        conversation_id="web:operator",
        principal_id="operator",
        user_message="open this",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert result.final_text
    assert len(invocation_scopes) == 1
    assert invocation_scopes[0] is not None
    assert cleanup_scopes == invocation_scopes

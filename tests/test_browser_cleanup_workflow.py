from __future__ import annotations

import asyncio
import pytest

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
            flow_context={"browser_session_id": "tab-1"},
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

        def create(self, *, messages, tools, **kwargs):  # noqa: ANN001
            from tests.workflow_provider_protocol import workflow_provider_reply
            reply = workflow_provider_reply({**kwargs, "messages": messages, "tools": tools})
            if reply is not None: return reply
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


def test_runtime_owned_open_keeps_session_until_explicit_owner_cleanup():
    class Backend(FakeBrowserBackend):
        async def open(self, session_id):
            return 'opened'

    backend = Backend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    result = tools.browser_open(ToolInvocation('open', 'browser_open', 'conversation-owner', {},
        capsule_id='opening-turn', flow_context={'browser_session_id': 'owned-page',
            'browser_session_owner': 'conversation-owner'}))
    assert result.status == 'completed'
    tools.close_tracked_sessions('opening-turn')
    assert backend.closed_sessions == []
    tools.close_tracked_sessions('unrelated-owner')
    assert backend.closed_sessions == []
    tools.close_tracked_sessions('conversation-owner')
    assert backend.closed_sessions == ['owned-page']


def test_failed_open_does_not_retain_session_after_cleanup():
    class Backend(FakeBrowserBackend):
        async def open(self, session_id):
            raise RuntimeError('unavailable')

    backend = Backend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    result = tools.browser_open(ToolInvocation('open', 'browser_open', 'owner', {},
        capsule_id='failed-turn', flow_context={'browser_session_id': 'failed-page'}))
    assert result.status == 'failed'
    tools.close_tracked_sessions('failed-turn')
    assert backend.closed_sessions == ['failed-page']


def test_conversation_navigation_survives_retry_capsules_and_owner_cleanup():
    backend = FakeBrowserBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    flow = {"browser_session_scope": "request-1", "browser_session_owner": "conversation:one"}
    first = tools.browser_navigate(ToolInvocation("nav-1", "browser_navigate", "operator",
        {"url": "https://example.com"}, capsule_id="initial-pass", flow_context=flow))
    tools.close_tracked_sessions("initial-pass")
    assert backend.closed_sessions == []
    retry = tools.browser_navigate(ToolInvocation("nav-2", "browser_navigate", "operator",
        {"url": "https://example.com"}, capsule_id="retry-pass", flow_context=flow))
    assert retry.output["session_id"] == first.output["session_id"]
    tools.close_tracked_sessions("retry-pass")
    assert backend.closed_sessions == []
    other = tools.browser_navigate(ToolInvocation("nav-3", "browser_navigate", "operator",
        {"url": "https://example.com"}, capsule_id="other-pass", flow_context={
            "browser_session_scope": "request-2", "browser_session_owner": "conversation:two"}))
    assert other.output["session_id"] != first.output["session_id"]
    tools.close_tracked_sessions("conversation:one")
    assert backend.closed_sessions == [first.output["session_id"]]
    tools.close_tracked_sessions("other-pass")
    assert backend.closed_sessions == [first.output["session_id"]]
    tools.close_tracked_sessions("conversation:two")
    assert backend.closed_sessions == [first.output["session_id"], other.output["session_id"]]


def test_shared_stop_releases_only_conversation_resource_scope():
    import asyncio
    from types import SimpleNamespace
    from nullion.session_stop import stop_session_async
    calls = []
    registry = ToolRegistry()
    registry.register_cleanup_hook(lambda scope: calls.append(scope))
    runtime = SimpleNamespace(active_tool_registry=registry, store=None)
    asyncio.run(stop_session_async(conversation_id='telegram:17', runtime=runtime))
    assert calls == ['conversation:telegram:17']
    asyncio.run(stop_session_async(conversation_id='', runtime=runtime))
    assert len(calls) == 1


def test_browser_open_without_runtime_owner_is_released_at_turn_end():
    class Backend(FakeBrowserBackend):
        async def open(self, session_id): return 'opened'
    backend = Backend()
    tools = BrowserTools(backend, BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_open(ToolInvocation('open', 'browser_open', 'member', {}, capsule_id='turn'))
    tools.close_tracked_sessions('turn')
    assert backend.closed_sessions == [result.output['session_id']]


def test_failed_close_retains_exact_ownership_and_retries_without_forgetting():
    class Backend(FakeBrowserBackend):
        attempts = 0
        async def close_session(self, session_id):
            self.attempts += 1
            if self.attempts <= 2: raise ConnectionError('transport unavailable')
            await super().close_session(session_id)
    backend = Backend()
    tools = BrowserTools(backend, BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_navigate(ToolInvocation('nav', 'browser_navigate', 'member',
        {'url':'https://example.test'}, capsule_id='turn'))
    session = result.output['session_id']
    tools.close_tracked_sessions('turn')
    assert tools._sessions_by_scope['turn'] == {session}
    assert not backend.closed_sessions
    tools.close_tracked_sessions('turn')
    assert backend.closed_sessions == [session]
    assert not tools._sessions_by_scope


def test_absent_browser_session_does_not_get_a_successful_close_receipt():
    class Backend(FakeBrowserBackend):
        async def close_session(self, session_id): return False
    tools = BrowserTools(Backend(), BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_close(ToolInvocation('close', 'browser_close', 'member', {}))
    assert result.output['closed'] is None
    assert result.output['closed_count'] == 0
    assert result.output['scope'] == 'session'


@pytest.mark.asyncio
@pytest.mark.parametrize('engine', ['cdp', 'playwright'])
async def test_browser_transport_keeps_failed_close_available_for_retry(engine):
    from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
    from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
    class Page:
        attempts = 0
        async def close(self):
            self.attempts += 1
            if self.attempts == 1: raise ConnectionError('disconnected')
    page = Page()
    backend = (CDPBackend if engine == 'cdp' else PlaywrightBackend).__new__(
        CDPBackend if engine == 'cdp' else PlaywrightBackend)
    backend._lock = asyncio.Lock()
    backend._pages = {'owned': page}
    backend._browser = None
    backend._contexts = {}
    backend._context_roots = {}
    with pytest.raises(ConnectionError): await backend.close_session('owned')
    assert backend._pages['owned'] is page
    assert await backend.close_session('owned') is True
    assert not backend._pages
    assert await backend.close_session('owned') is False


@pytest.mark.asyncio
async def test_raw_cdp_close_does_not_mark_failed_target_closed(monkeypatch):
    from nullion.plugins.browser_plugin.backends import cdp_backend
    async def failed_close(*args): return False
    monkeypatch.setattr(cdp_backend, '_async_close_cdp_target', failed_close)
    page = cdp_backend._RawCDPPage({'id': 'owned-target'}, cdp_url='http://example.test', close_target_on_close=True)
    class Client:
        async def send(self, *args, **kwargs): raise ConnectionError('disconnected')
        async def close(self): pass
    page._client = Client()
    with pytest.raises(ConnectionError): await page.close()
    assert not page.is_closed()
    async def closed(*args): return True
    monkeypatch.setattr(cdp_backend, '_async_close_cdp_target', closed)
    await page.close()
    assert page.is_closed()

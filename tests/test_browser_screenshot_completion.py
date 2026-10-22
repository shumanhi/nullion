from types import SimpleNamespace

import pytest

from nullion import chat_operator
from nullion.plugins.browser_plugin.backends.auto_backend import AutoBackend
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult


def _registry(requested=("browser_navigate", "browser_screenshot"), required=()):
    registry = ToolRegistry()
    for name in {"browser_navigate", "browser_screenshot", *required, *requested}:
        registry.register(ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ, requires_approval=False, timeout_seconds=5),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}))
    return ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(has_url_target=True),
        tool_scope_decision=TurnToolScopeDecision(web_action="browser_interaction",
            requested_tool_names=requested, required_tool_names=required, valid=True))


def test_structured_screenshot_request_does_not_require_redundant_extension():
    assert chat_operator._direct_browser_screenshot_allowed(_registry())


@pytest.mark.parametrize("other_tool", ["email_send", "browser_extract_detail", "browser_click_id", "browser_wait_for"])
def test_capture_fast_path_preserves_other_requested_work(other_tool):
    assert not chat_operator._direct_browser_screenshot_allowed(
        _registry(required=("browser_screenshot", other_tool)))


def test_browser_fallback_does_not_change_process_session_configuration(monkeypatch):
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    backend = AutoBackend()._make_playwright()
    import os
    assert backend._headless is True
    assert os.environ["NULLION_BROWSER_HEADLESS"] == "false"


def test_direct_capture_binds_runtime_navigation_session(monkeypatch, tmp_path):
    calls = []
    path = tmp_path / "capture.png"
    from PIL import Image
    Image.new("RGB", (32, 32), "blue").save(path)

    def invoke(store, invocation, *, registry):
        calls.append(invocation)
        output = {"session_id": "physical-navigation-session"}
        if invocation.tool_name == "browser_screenshot":
            assert invocation.flow_context["browser_session_id"] == "physical-navigation-session"
            output.update(path=str(path), page_url="https://example.test/", artifact_paths=[str(path)])
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", output)

    monkeypatch.setattr(chat_operator, "invoke_tool_with_boundary_policy", invoke)
    result = chat_operator._run_browser_screenshot_tool_directly(runtime=SimpleNamespace(store=object()),
        registry=_registry(), principal_id="operator", turn_id="capture-request",
        user_message="https://example.test/")
    assert result.artifacts == [str(path)]
    assert [call.tool_name for call in calls] == ["browser_navigate", "browser_screenshot"]
    assert {call.flow_context["browser_session_scope"] for call in calls} == {"capture-request"}


def _capture_receipts(tmp_path):
    from PIL import Image
    path = tmp_path / "verified.png"
    Image.new("RGB", (32, 32), "blue").save(path)
    return [
        ToolResult("nav", "browser_navigate", "completed", {"session_id": "physical-1"}),
        ToolResult("capture", "browser_screenshot", "completed", {"session_id": "physical-1", "page_url": "https://example.test/", "path": str(path)}),
    ]


def test_capture_contract_accepts_runtime_bound_png(tmp_path):
    from nullion.browser_capture_contract import completed_capture_paths
    results = _capture_receipts(tmp_path)
    assert completed_capture_paths(_registry().turn_tool_scope_decision, results) == (results[-1].output["path"],)


@pytest.mark.parametrize("case", ["blank_page", "other_session", "missing_file", "invalid_png", "later_navigation", "failed_wait", "missing_required_wait", "other_task"])
def test_capture_contract_rejects_incomplete_or_unrelated_evidence(tmp_path, case):
    from nullion.browser_capture_contract import completed_capture_paths
    results = _capture_receipts(tmp_path)
    decision = _registry().turn_tool_scope_decision
    if case == "blank_page":
        results[-1].output["page_url"] = "about:blank"
    elif case == "other_session":
        results[-1].output["session_id"] = "physical-2"
    elif case == "missing_file":
        results[-1].output["path"] = str(tmp_path / "missing.png")
    elif case == "invalid_png":
        from pathlib import Path
        Path(results[-1].output["path"]).write_text("not a PNG")
    elif case == "later_navigation":
        results.append(results[0])
    elif case == "failed_wait":
        results.insert(1, ToolResult("wait", "browser_wait_for", "failed", {}))
    elif case == "missing_required_wait":
        decision = _registry(required=("browser_wait_for", "browser_screenshot")).turn_tool_scope_decision
    elif case == "other_task":
        results.append(ToolResult("email", "email_send", "completed", {}))
    assert completed_capture_paths(decision, results) == ()


def test_scope_expansion_preserves_exact_requested_work(tmp_path):
    from nullion.browser_capture_contract import completed_capture_paths
    results = _capture_receipts(tmp_path)
    decision = _registry(requested=("browser_navigate", "browser_screenshot", "browser_extract_detail"), required=("browser_navigate", "browser_screenshot")).turn_tool_scope_decision
    receipt = ToolResult("scope", "request_tool_scope", "completed", {"explicit_tool_names": ["browser_navigate", "browser_screenshot"]})
    assert completed_capture_paths(decision, [receipt, *results])
    receipt.output["explicit_tool_names"].append("browser_extract_detail")
    assert not completed_capture_paths(decision, [receipt, *results])


@pytest.mark.parametrize("backend_name", ["auto", "playwright", "cdp"])
def test_runtime_session_receipts_are_reusable_only_in_owning_scope(backend_name, monkeypatch):
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.tools import ToolInvocation
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    tools = BrowserTools(SimpleNamespace(BACKEND_NAME=backend_name), BrowserSessionPool(), BrowserPolicy())
    def invocation(scope, raw=""):
        return ToolInvocation("inv", "browser_navigate", "operator", {"session_id": raw},
            capsule_id=scope, flow_context={"browser_session_scope": scope})
    first = invocation("one")
    session = tools._session_id(first)
    tools._remember_session(first, session)
    second = invocation("one", "second-tab")
    second_session = tools._session_id(second)
    tools._remember_session(second, second_session)
    assert tools._session_id(invocation("one", session)) == session
    assert tools._session_id(invocation("one", second_session)) == second_session
    assert tools._session_id(invocation("two", session)) != session
    tools._forget_session(session)
    assert tools._session_id(invocation("one", session)) != session


def test_uninitialized_capture_fails_before_creating_deliverable(tmp_path, monkeypatch):
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool, BrowserScreenshotResult
    from nullion.tools import ToolInvocation
    class Backend:
        async def screenshot(self, session_id, mode):
            return BrowserScreenshotResult(data=b"png", mode=mode, requested_mode=mode, page_url="about:blank")
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path))
    tools = BrowserTools(Backend(), BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_screenshot(ToolInvocation("capture", "browser_screenshot", "operator", {}))
    assert result.status == "failed"
    assert not list(tmp_path.rglob("*.png"))


@pytest.mark.parametrize("extra", ["browser_screenshot", "browser_wait_for", "email_send"])
def test_url_fast_path_preserves_required_work(extra):
    from dataclasses import replace
    registry = _registry(requested=("browser_navigate",), required=(extra,))
    decision = replace(registry.turn_tool_scope_decision, web_action="open_url")
    assert chat_operator._direct_open_url_tool_name(registry, decision) is None


@pytest.mark.parametrize("case", ["valid", "other_conversation", "changed_session", "failed_action", "preserved_read", "preserved_other_session"])
def test_context_capture_uses_only_continuous_conversation_navigation(case):
    from nullion.browser_capture_contract import conversation_navigation
    nav = {"tool_name": "browser_navigate", "status": "completed", "output": {"session_id": "one"}}
    later = {"tool_name": "browser_click", "status": "completed", "output": {"session_id": "one"}}
    if case == "changed_session":
        later["output"]["session_id"] = "two"
    if case == "failed_action":
        later["status"] = "failed"
    if case.startswith("preserved_"):
        later = {"tool_name": "browser_extract_detail", "status": "failed",
                 "output": {"session_id": "two" if case == "preserved_other_session" else "one",
                            "page_state_preserved": True}}
        # A later successful read confirms which physical page remains active.
        events_tail = {"tool_name": "browser_extract_text", "status": "completed", "output": {"session_id": "one"}}
    else:
        events_tail = None
    events = [{"event_type": "conversation.chat_turn", "tool_results": [nav, later]}]
    if events_tail:
        events[0]["tool_results"].append(events_tail)
    store = SimpleNamespace(list_conversation_events=lambda conversation: events if conversation == "owned" else [])
    receipt = conversation_navigation(store, "other" if case == "other_conversation" else "owned")
    assert bool(receipt) == (case in {"valid", "preserved_read"})


def test_context_capture_requires_matching_runtime_navigation_receipt(tmp_path):
    from nullion.browser_capture_contract import completed_capture_paths
    results = _capture_receipts(tmp_path)
    capture = results[-1]
    decision = _registry(requested=("browser_screenshot",)).turn_tool_scope_decision
    assert not completed_capture_paths(decision, [capture])
    capture.output["navigation_receipt"] = results[0].output
    assert completed_capture_paths(decision, [capture])
    capture.output["navigation_receipt"]["session_id"] = "another-session"
    assert not completed_capture_paths(decision, [capture])


@pytest.mark.parametrize("case", ["delivered", "not_declared", "different_file", "required_action", "other_work"])
def test_declared_capture_completion_preserves_remaining_work(tmp_path, case):
    from nullion.browser_capture_contract import delivered_capture_paths
    results = _capture_receipts(tmp_path)
    path = results[-1].output["path"]
    decision = SimpleNamespace(valid=False, required_tool_names=())
    final_text = f"MEDIA:{path}"
    if case == "not_declared":
        final_text = "Screenshot attached."
    elif case == "different_file":
        final_text = f"MEDIA:{tmp_path / 'other.png'}"
    elif case == "required_action":
        decision.required_tool_names = ("browser_wait_for",)
    elif case == "other_work":
        decision = _registry(required=("email_send",)).turn_tool_scope_decision
    assert bool(delivered_capture_paths(decision, results, final_text)) == (case == "delivered")


def test_context_capture_scope_does_not_require_fresh_navigation():
    from nullion.tools import ToolInvocation
    registry = ScopedTurnToolRegistry(_registry()._delegate,
        evidence=TurnToolEvidence(prior_tool_scopes=("web",)))
    result, widened = registry.apply_scope_request(ToolInvocation("scope", "request_tool_scope", "operator", {
        "capabilities": ["web"], "tool_names": ["browser_screenshot"], "required_tool_names": ["browser_screenshot"],
    }))
    assert result.status == "completed"
    assert widened.can_invoke_tool("browser_screenshot")
    assert set(widened.turn_tool_scope_decision.required_tool_names) == {"browser_screenshot"}


def test_capture_completion_accepts_serialized_runtime_receipts(tmp_path):
    from dataclasses import asdict
    from nullion.browser_capture_contract import delivered_capture_paths

    receipts = _capture_receipts(tmp_path)
    serialized = [asdict(result) for result in receipts]
    path = receipts[-1].output["path"]
    assert delivered_capture_paths(None, serialized, f"MEDIA:{path}") == (path,)
    assert not delivered_capture_paths(None, [{"tool_name": "email_search", "status": "completed", "output": {}}], "")


@pytest.mark.parametrize('requested', [False, True])
def test_low_level_capture_role_requires_typed_delivery(tmp_path, monkeypatch, requested):
    import io
    from pathlib import Path
    from PIL import Image
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.tools import ToolInvocation
    data = io.BytesIO(); Image.new('RGB', (20, 20)).save(data, format='PNG')
    class Backend:
        async def screenshot(self, session_id, mode):
            return data.getvalue()
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT', str(tmp_path))
    tools = BrowserTools(Backend(), BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_screenshot(ToolInvocation('capture', 'browser_screenshot', 'operator', {},
        flow_context={'artifact_extensions': ['.png']} if requested else {}))
    assert result.status == 'completed'
    assert result.output['artifact_descriptors'][0]['role'] == ('deliverable' if requested else 'source')
    assert Path(result.output['artifact_path']).read_bytes() == data.getvalue()

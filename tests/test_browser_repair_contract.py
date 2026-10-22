from types import SimpleNamespace

import pytest

from nullion.agent_orchestrator import _should_block_failed_tool_during_recovery, _tool_failure_fingerprint
from nullion.browser_capture_contract import completed_capture_paths
from nullion.tool_repair_contract import tool_repair_kind
from nullion.tools import ToolResult


def repair_result(tool="browser_screenshot", session="physical-one", kind="repair_precondition", error="Page missing"):
    recovery = {"kind": kind, "retry_tool_name": tool}
    reason = "tool_precondition_unmet"
    if kind == "repair_precondition":
        recovery["required_tool_names"] = ["browser_navigate"]
    else:
        reason = "invalid_tool_arguments"
        recovery["argument_constraints"] = {"at_least_one": ["selector", "text"]}
    return ToolResult("failure", tool, "failed", {"reason": reason, "session_id": session, "recovery": recovery}, error)


@pytest.mark.parametrize("kind", ["repair_precondition", "correct_arguments"])
def test_typed_repair_keeps_tool_available_without_parsing_error_language(kind):
    for error in ["页面尚未加载", "Página vacía", "صفحة فارغة"]:
        result = repair_result(kind=kind, error=error)
        assert tool_repair_kind(result) == kind
        assert not _should_block_failed_tool_during_recovery(result)
    first = repair_result(kind=kind)
    translated = repair_result(kind=kind, error="ページがありません")
    assert _tool_failure_fingerprint(result=first, invocation_signature="args-one") == _tool_failure_fingerprint(
        result=translated, invocation_signature="args-two")


@pytest.mark.parametrize("invalid", ["wrong_tool", "denied", "wrong_reason", "missing_contract", "empty_constraints"])
def test_unvalidated_recovery_does_not_keep_a_failed_tool(invalid):
    result = repair_result()
    if invalid == "wrong_tool":
        result.output["recovery"]["retry_tool_name"] = "other_tool"
    elif invalid == "denied":
        result.status = "denied"
    elif invalid == "wrong_reason":
        result.output["reason"] = "blocked_url"
    elif invalid == "missing_contract":
        result.output.pop("recovery")
    else:
        result.output["recovery"]["required_tool_names"] = []
    assert tool_repair_kind(result) is None
    assert _should_block_failed_tool_during_recovery(result)


@pytest.mark.parametrize("case", ["repaired", "other_session", "missing_navigation", "unresolved_wait", "corrected_wait"])
def test_capture_completion_requires_actual_same_session_repair(tmp_path, case):
    from PIL import Image
    path = tmp_path / "capture.png"
    Image.new("RGB", (32, 32), "blue").save(path)
    failed = repair_result()
    nav = ToolResult("nav", "browser_navigate", "completed", {"session_id": "physical-one"})
    capture = ToolResult("cap", "browser_screenshot", "completed", {
        "session_id": "physical-one", "page_url": "https://example.test/", "path": str(path)})
    results = [failed, nav, capture]
    decision = SimpleNamespace(valid=True, requested_tool_names=("browser_navigate", "browser_screenshot"),
                               required_tool_names=("browser_screenshot",))
    if case == "other_session":
        failed.output["session_id"] = "physical-two"
    elif case == "missing_navigation":
        results.remove(nav)
        capture.output["navigation_receipt"] = nav.output
    elif case in {"unresolved_wait", "corrected_wait"}:
        results.insert(2, repair_result("browser_wait_for", kind="correct_arguments"))
        if case == "corrected_wait":
            results.insert(3, ToolResult("wait", "browser_wait_for", "completed", {"session_id": "physical-one"}))
    assert bool(completed_capture_paths(decision, results)) == (case in {"repaired", "corrected_wait"})


def test_implicit_session_reuses_last_physical_session_only_in_owning_scope():
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.tools import ToolInvocation
    tools = BrowserTools(SimpleNamespace(BACKEND_NAME="playwright"), BrowserSessionPool(), BrowserPolicy())
    first = ToolInvocation("open", "browser_open", "operator", {"session_id": "new-tab"}, capsule_id="turn-one")
    session = tools._session_id(first)
    tools._remember_session(first, session)
    capture = ToolInvocation("capture", "browser_screenshot", "operator", {}, capsule_id="turn-one")
    assert tools._session_id(capture) == session
    assert tools._session_id(ToolInvocation("other", "browser_screenshot", "operator", {}, capsule_id="turn-two")) != session

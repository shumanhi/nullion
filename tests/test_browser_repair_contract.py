import json
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


def test_readiness_failures_are_bound_to_the_observed_source_not_just_session():
    from nullion.agent_orchestrator import _tool_failure_source_context
    first = repair_result(tool='browser_extract_text', kind='correct_arguments')
    first.output['reason'] = 'source_not_ready'
    def opened(url, session='physical-one'):
        return ToolResult('nav', 'browser_navigate', 'completed',
            {'result_kind': 'browser_navigation', 'session_id': session, 'page_url': url})
    one = _tool_failure_source_context(first, [opened('https://one.example/read')])
    two = _tool_failure_source_context(first, [opened('https://two.example/read')])
    assert one != two
    assert _tool_failure_fingerprint(result=first, invocation_signature='a', source_context=one) != _tool_failure_fingerprint(
        result=first, invocation_signature='a', source_context=two)
    # Changing a selector or translating an error cannot reset the same-source budget.
    assert _tool_failure_fingerprint(result=first, invocation_signature='a', source_context=one) == _tool_failure_fingerprint(
        result=first, invocation_signature='b', source_context=one)
    assert _tool_failure_source_context(first, [opened('https://other.example/read', session='other')]) is None


def test_successful_same_session_read_resets_corrected_argument_failure():
    from nullion.agent_orchestrator import _reset_repaired_precondition_streaks
    key = json.dumps({'tool_name': 'browser_extract_text', 'session_id': 'physical-one', 'repair_kind': 'correct_arguments'})
    result = ToolResult('ok', 'browser_extract_text', 'completed', {'session_id': 'physical-one'})
    assert _reset_repaired_precondition_streaks({key: 1}, result) == {}


def test_native_source_timeout_keeps_other_browser_operations_available():
    from nullion.plugins.browser_plugin.browser_tools import _browser_failure_output
    output = _browser_failure_output(TimeoutError('応答がありません'))
    assert output['failure_scope'] == 'invocation'
    assert not _should_block_failed_tool_during_recovery(ToolResult('nav', 'browser_navigate', 'failed', output))
    # A lost browser connection is still a capability failure, not source proof.
    unavailable = _browser_failure_output(ConnectionError('Connexion indisponible'))
    assert unavailable.get('failure_scope') != 'invocation'
    assert _should_block_failed_tool_during_recovery(ToolResult('nav', 'browser_navigate', 'failed', unavailable))


@pytest.mark.parametrize('wrapped', [False, True])
def test_native_http_read_timeout_is_also_invocation_scoped(wrapped):
    from urllib.error import URLError
    from nullion.tools import _source_request_failure_metadata
    timeout = TimeoutError('Sin respuesta')
    output = _source_request_failure_metadata(URLError(timeout) if wrapped else timeout)
    assert output['reason'] == 'handler_timeout' and output['failure_scope'] == 'invocation'
    assert not _should_block_failed_tool_during_recovery(ToolResult('fetch', 'web_fetch', 'failed', output))


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


@pytest.mark.parametrize('fields',[None,['title','canonical_url'],['url']])
@pytest.mark.parametrize('truncated',[False,True])
def test_item_projection_preserves_coverage_membership_and_stable_identity(fields,truncated):
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.tools import ToolInvocation
    class Backend:
        BACKEND_NAME='cdp'
        async def run_js(self,session,script):
            if script=='() => location.href':return 'https://example.test/news'
            return {'url':'https://example.test/news','title':'News','coverage':{'scope_found':True,'truncated':truncated,'candidate_count':2 if truncated else 1,'returned_count':1,'sections':[]},
                'items':[{'title':'Story','url':'https://example.test/news/story','canonical_url':'https://example.test/news/story','image_url':'https://example.test/photo.png','section_id':'s1','source_index':1}]}
    tools=BrowserTools(Backend(),BrowserSessionPool(),BrowserPolicy())
    args={} if fields is None else {'fields':fields}
    result=tools.browser_extract_items(ToolInvocation('read','browser_extract_items','operator',args,capsule_id='projection'))
    assert result.status=='completed',result
    assert result.output['coverage']['truncated'] is truncated and result.output['item_count']==1
    item=result.output['items'][0]
    assert item['section_id']=='s1' and item['source_index']==1
    if fields is not None:assert set(item)==set(fields)|{'source_index','section_id'}
    meta=result.output['source_record_collections'][0]
    assert meta['change_identity_fields']==(['url'] if fields==['url'] else ['canonical_url'])


@pytest.mark.parametrize('fields',[[],['unknown'],'title'])
def test_invalid_item_projection_is_a_correctable_argument_error(fields):
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.tools import ToolInvocation
    tools=BrowserTools(SimpleNamespace(BACKEND_NAME='cdp'),BrowserSessionPool(),BrowserPolicy())
    result=tools.browser_extract_items(ToolInvocation('read','browser_extract_items','operator',{'fields':fields}))
    assert result.status=='failed' and tool_repair_kind(result)=='correct_arguments'

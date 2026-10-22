import json
from types import SimpleNamespace

import pytest

from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, register_cron_tools


def failed_wait(session="one"):
    return ToolResult("bad", "browser_wait_for", "failed", {
        "session_id": session, "reason": "invalid_tool_arguments",
        "recovery": {"kind": "correct_arguments", "retry_tool_name": "browser_wait_for",
                     "argument_constraints": {"at_least_one": ["selector", "url_pattern", "text"]}}})


@pytest.mark.parametrize("same_session", [True, False])
def test_final_failure_cause_excludes_only_verified_repaired_wait(same_session):
    from nullion.response_fulfillment_contract import operation_failure_reply
    results = [failed_wait(), ToolResult("fixed", "browser_wait_for", "completed", {
        "session_id": "one" if same_session else "two"})]
    reply = operation_failure_reply(results)
    assert ("browser operation failed" in reply) is not same_session


def test_limit_cause_outranks_earlier_browser_error():
    from nullion.cron_delivery import _cron_report_failure_notice
    text = _cron_report_failure_notice({"reached_iteration_limit": True, "tool_results": [failed_wait()]})
    assert "execution limit" in text and "browser operation failed" not in text


@pytest.mark.parametrize("flag", ["reached_iteration_limit", "response_fulfilled"])
@pytest.mark.parametrize("silent", [True, False])
def test_nonempty_failed_cron_recovers_current_source_evidence(flag, silent):
    from nullion.cron_delivery import _repair_cron_agent_final_text_if_needed
    state = {"text": "未確認", flag: flag == "reached_iteration_limit", "tool_results": [
        failed_wait(), ToolResult("read", "browser_extract_text", "completed", {"text": "確認済み: 62 USD"})]}
    calls = []
    class Model:
        def create(self, **kwargs):
            calls.append(kwargs)
            return {"content": [{"type": "text", "text": json.dumps({
                "requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"],
                "report": "" if silent else "確認済み: 62 USD",
                "delivery_decision": "silent" if silent else "send",
                "delivery_reason": "no_verified_match" if silent else "verified_match"})}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name="確認", task="条件に合う結果だけ通知"), state, model_client=Model())
    assert len(calls) == 1
    assert state["response_fulfilled"] is True
    assert state["reached_iteration_limit"] is False
    assert state["text"] == ("" if silent else "確認済み: 62 USD")


def test_cron_tool_exposes_and_preserves_notification_policy(tmp_path, monkeypatch):
    from nullion import crons
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "runtime.db")
    registry = ToolRegistry(); register_cron_tools(registry)
    schema = registry.get_spec("create_cron").input_schema
    if not schema:
        from nullion.tools import _default_input_schema_for_tool
        schema = _default_input_schema_for_tool("create_cron")
    assert schema["properties"]["artifact_delivery_options"]["properties"]["alert_delivery_mode"]["enum"] == [
        "verified_matches_only", "report_each_run"]
    created = registry.invoke(ToolInvocation("new", "create_cron", "web:owner", {
        "name": "確認", "schedule": "0 0 1 1 *", "task": "条件に合う場合だけ通知", "enabled": False,
        "artifact_delivery_options": {"alert_delivery_mode": "verified_matches_only",
                                      "required_artifact_extensions": [".html"]}}))
    assert created.status == "completed", created
    job = crons.get_cron(created.output["id"])
    updated = registry.invoke(ToolInvocation("update", "update_cron", "web:owner", {
        "id": job.id, "artifact_delivery_options": {"alert_delivery_mode": "report_each_run"}}))
    assert updated.status == "completed", updated
    assert crons.get_cron(job.id).artifact_delivery_options == {
        "alert_delivery_mode": "report_each_run", "required_artifact_extensions": [".html"]}


def test_failed_wait_preserves_page_and_has_bounded_inspection_recovery():
    from nullion.plugins.browser_plugin.browser_tools import BrowserTools
    from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
    from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
    from nullion.tool_repair_contract import tool_repair_kind
    from nullion.agent_orchestrator import _should_block_failed_tool_during_recovery
    class Backend:
        BACKEND_NAME = "playwright"
        async def wait_for(self, *args): raise TimeoutError("時間切れ")
    tools = BrowserTools(Backend(), BrowserSessionPool(), BrowserPolicy())
    result = tools.browser_wait_for(ToolInvocation("wait", "browser_wait_for", "member", {"text": "準備完了"}))
    assert result.status == "failed" and result.output["page_state_preserved"] is True
    assert result.output["conditions"]["text"] == "準備完了"
    assert tool_repair_kind(result) == "inspect_page"
    assert not _should_block_failed_tool_during_recovery(result)

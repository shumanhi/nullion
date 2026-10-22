import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Container E2E only")


@pytest.mark.parametrize("task", ["条件に合う情報だけ通知。", "Notifica solo resultados que cumplen la condición."])
@pytest.mark.parametrize("match", [True, False])
@pytest.mark.parametrize("stop", ["limit", "provider_failure"])
def test_real_cron_salvages_browser_evidence_and_obeys_alert_policy(tmp_path, monkeypatch, task, match, stop):
    from nullion import crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.cron_delivery import (run_single_agent_cron_turn, run_cron_delivery_workflow,
        CronRunDeliveryCallbacks, cron_delivery_block_reason, record_cron_delivery_chat_turn)
    from nullion.plugins.browser_plugin import register_browser_tools
    from nullion.plugins.browser_plugin.backends import auto_backend
    from nullion.model_clients import ModelClientResponseError
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry
    from nullion.policy import BoundaryPolicyRule, BoundaryKind, PolicyMode, GLOBAL_PERMISSION_PRINCIPAL
    from nullion.events import make_event
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("NULLION_CRON_AGENT_MAX_ITERATIONS", "3" if stop == "limit" else "6")
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "auto")
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    db = tmp_path / "runtime.db"; monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", db)
    runtime = bootstrap_persistent_runtime(db)
    class Website:
        BACKEND_NAME = "playwright"
        async def wait_for(self, session, selector, url, text, timeout): assert text == "準備完了"
        async def extract_text(self, *args): return "確認済み: 62 USD。条件一致=" + str(match)
        async def close_session(self, *args): pass
    monkeypatch.setattr(auto_backend, "AutoBackend", Website)
    registry = ToolRegistry(); register_browser_tools(registry)
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id="public", principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector="*", created_by="owner", created_at=datetime.now(UTC)))
    class Provider:
        steps = 0; repairs = 0
        def create(self, **kwargs):
            if not kwargs.get("tools"):
                if kwargs.get("system", "").startswith("You are a material-requirements planner"):
                    return {"content": [{"type": "text", "text": json.dumps({
                        "all_material_requirements_enumerated": True, "material_requirements": [
                            {"requirement": "Verify the monitored condition", "requirement_kind": "fact",
                             "evidence_subject_id": "condition", "eligible_evidence_tool_names": ["browser_extract_text"]}]})}]}
                if kwargs.get("system", "").startswith("You are a semantic completion reviewer"):
                    payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
                    row = payload["material_requirement_contract"][0]
                    refs = [r["invocation_id"] for r in payload["tool_evidence"] if r["tool_name"] == "browser_extract_text"]
                    assert "62 USD" in json.dumps(payload["tool_evidence"])
                    return {"content": [{"type": "text", "text": json.dumps({
                        "disposition": "complete", "all_material_requirements_enumerated": True,
                        "unresolved_requirements": [], "material_requirements": [{
                            "requirement_id": row["requirement_id"], "requirement": row["requirement"],
                            "requirement_kind": row["requirement_kind"], "satisfied": True,
                            "draft_covered": True, "evidence_invocation_ids": refs}]})}]}
                if kwargs.get("system", "").startswith("You repair scheduled-task final answers"):
                    self.repairs += 1
                    prompt = kwargs["messages"][0]["content"][0]["text"]
                    assert task in prompt and "62 USD" in prompt
                    return {"content": [{"type": "text", "text": json.dumps({
                        "requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"],
                        "report": "確認済み: 62 USD" if match else "",
                        "delivery_decision": "send" if match else "silent",
                        "delivery_reason": "verified_match" if match else "no_verified_match"})}]}
                if kwargs.get("system", "").startswith("Classify delivery"):
                    return {"content": [{"type": "text", "text": json.dumps({
                        "decision": "send" if match else "silent", "reason": "verified_match" if match else "no_verified_match"})}]}
                return {"content": [{"type": "text", "text": "{}"}]}
            self.steps += 1
            if self.steps > 3:
                raise ModelClientResponseError("server_error", retryable=True)
            tool, args = [("browser_wait_for", {}), ("browser_wait_for", {"text": "準備完了"}),
                          ("browser_extract_text", {})][self.steps - 1]
            return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": str(self.steps), "name": tool, "input": args}]}
    provider = Provider()
    job = crons.add_cron("確認", "0 0 1 1 *", task, enabled=False, delivery_channel="telegram", delivery_target="73",
        artifact_delivery_options={"alert_delivery_mode": "verified_matches_only"})
    def agent(job, conversation):
        return run_single_agent_cron_turn(job, conversation, label="Scheduled task", orchestrator=AgentOrchestrator(model_client=provider),
            runtime=runtime, tool_registry=registry, settings=SimpleNamespace(enabled_skill_packs=()), model_client=provider)
    sent = []
    def record(kind, job, channel, target, conversation, **details):
        runtime.store.add_event(make_event(kind, "cron_scheduler", {"cron_id": job.id, **details}))
    callbacks = CronRunDeliveryCallbacks(effective_channel=lambda j: j.delivery_channel, delivery_target=lambda j, c: j.delivery_target,
        run_agent_turn=agent, record_event=record, block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda *a: False, send_platform_delivery=lambda j, c, target, text: sent.append(text) or True,
        record_chat_turn=lambda j, conv, c, target, text: record_cron_delivery_chat_turn(
            runtime.store, j, conversation_id=conv, delivery_channel=c, delivery_target=target, delivered_text=text))
    result = run_cron_delivery_workflow(job, label="Scheduled task", callbacks=callbacks)
    assert provider.repairs == 1, result
    assert result["response_fulfilled"] is True and result["reached_iteration_limit"] is False
    assert not result.get("cron_report_failed"), result
    assert [r.status for r in result["tool_results"]] == ["failed", "completed", "completed"]
    assert result["cron_delivery_status"] == ("sent" if match else "silent"), result
    assert len(sent) == int(match)
    if match:
        assert "62 USD" in sent[0] and "browser operation failed" not in sent[0]
    runtime.checkpoint(force=True)
    saved = load_runtime_store(db)
    turns = [r for r in saved.list_conversation_events() if r.get("event_type") == "conversation.chat_turn"]
    assert len(turns) == int(match)
    assert any(r.event_type == ("cron.delivery.sent" if match else "cron.delivery.silent") for r in saved.list_events())

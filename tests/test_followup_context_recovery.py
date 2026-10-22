from types import SimpleNamespace

import pytest

from nullion import chat_operator
from nullion.agent_orchestrator import TurnResult, _complete_agent_turn, _CompletionReviewDecision, _completion_review_open_task_reply
from nullion.tools import ToolRegistry, ToolResult, ToolSpec, ToolRiskLevel, ToolSideEffectClass


@pytest.mark.parametrize("prompt", ["But there are more weekends in this month", "Pero hay más fines de semana este mes", "今月にはまだ他の週末があります"])
def test_history_alone_never_selects_scheduler_recovery(monkeypatch, prompt):
    registry = ToolRegistry()
    for name in ("list_crons", "update_cron", "toggle_cron", "delete_cron"):
        registry.register(ToolSpec(name=name, description=name, risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ if name == "list_crons" else ToolSideEffectClass.WRITE,
            requires_approval=False, timeout_seconds=5, capability_tags=("scheduler",)),
            lambda invocation: pytest.fail("Flight correction must not execute scheduler tools"))
    result = TurnResult("failed-history", "Could not find the trip", tool_results=[
        ToolResult("history", "chat_history_search", "completed", {"matches": [{"summary": "Deleted cron f37b4270"}]})],
        response_fulfilled=False)
    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", lambda **kwargs: pytest.fail("No family evidence: skip scheduler classifier"))
    actual, active, retried = chat_operator._run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=result, base_history=[], active_tool_registry=registry, base_tool_registry=registry,
        model_client=object(), user_message=prompt, evidence=SimpleNamespace(),
        run_turn=lambda *args: pytest.fail("No scheduler recovery"))
    assert actual is result and active is registry and not retried


def test_unfulfilled_result_preserves_only_structured_failure():
    reason = "I couldn't complete this request because the flight search service returned a connection error."
    result = TurnResult("failed", "All flights are confirmed at $1", response_fulfilled=False,
        completion_failure_reply=reason)
    assert chat_operator._unfulfilled_turn_reply(result) == (reason, chat_operator.TurnOutcome.PARTIAL)
    result.completion_failure_reply = None
    reply, outcome = chat_operator._unfulfilled_turn_reply(result)
    assert "confirmed" not in reply and "still open" not in reply and "verified evidence" not in reply
    assert outcome is chat_operator.TurnOutcome.PARTIAL


def test_terminal_review_reason_survives_orchestrator_completion():
    review = _CompletionReviewDecision(disposition="blocked", unresolved_requirements=("the search service refused the connection",))
    reply = _completion_review_open_task_reply(review)
    result = _complete_agent_turn({"tool_results": [], "artifacts": []}, final_text=reply,
        response_fulfilled=False, completion_failure_reply=reply)["result"]
    assert result.completion_failure_reply == reply
    assert "refused the connection" in chat_operator._unfulfilled_turn_reply(result)[0]
    assert "still open" not in reply


@pytest.mark.parametrize("new_evidence", [False, True])
def test_common_retries_do_not_reclassify_unchanged_scheduler_state(monkeypatch, new_evidence):
    registry = ToolRegistry()
    initial = TurnResult("unresolved", "Incomplete", tool_results=[ToolResult("list-1", "list_crons", "completed", {"crons": []})])
    calls = []
    def unchanged(**kwargs):
        return kwargs["initial_result"], kwargs["active_tool_registry"], False
    def scheduler(**kwargs):
        calls.append(kwargs["initial_result"])
        return unchanged(**kwargs)
    for name in ("model_tool_request", "no_tool_scope_decision", "missing_tool_scope", "connector_skill_scope", "scheduler_run_scope", "scheduler_run_fallback", "account_read", "schema_followup_tool"):
        monkeypatch.setattr(chat_operator, f"_run_chat_turn_{name}_retry", unchanged)
    monkeypatch.setattr(chat_operator, "_run_chat_turn_scheduler_mutation_fallback_retry", scheduler)
    for name in ("_scheduler_common_retries_may_bypass", "_scheduler_common_retries_may_bypass_before_mutation_recovery", "_turn_result_completed_scheduler_inspect_scope", "_append_scheduler_read_verification_if_missing"):
        monkeypatch.setattr(chat_operator, name, lambda *args, **kwargs: False)
    if new_evidence:
        def add_evidence(**kwargs):
            result = kwargs["initial_result"]
            result.tool_results.append(ToolResult("list-2", "list_crons", "completed", {"crons": []}))
            return result, kwargs["active_tool_registry"], True
        monkeypatch.setattr(chat_operator, "_run_chat_turn_no_tool_scope_decision_retry", add_evidence)
    result = chat_operator._run_chat_turn_common_retries(initial_result=initial, base_history=[],
        active_tool_registry=registry, base_tool_registry=registry, runtime=None, model_client=object(),
        user_message="修正", evidence=SimpleNamespace(), run_turn=lambda *args: pytest.fail("Unexpected execution"))
    assert len(calls) == (2 if new_evidence else 1)
    assert not result.retried_scheduler_mutation_fallback


def test_partial_turn_does_not_reenter_last_mile_attachment_validation(monkeypatch):
    reason = "The flight search service refused the connection. No updated PDF was produced."
    monkeypatch.setattr(chat_operator, "_last_mile_artifact_fulfillment_reply", lambda *args, **kwargs: pytest.fail("Terminal failure must keep its reason"))
    monkeypatch.setattr(chat_operator, "_pop_learned_skills_notification", lambda *args, **kwargs: None)
    assert chat_operator._append_runtime_nudges(SimpleNamespace(), prompt="修正", reply=reason,
        conversation_id="telegram:123", outcome=chat_operator.TurnOutcome.PARTIAL) == reason

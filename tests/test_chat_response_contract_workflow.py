from __future__ import annotations

from datetime import UTC, datetime

from nullion.approvals import ApprovalRequest, ApprovalStatus
from nullion.chat_response_contract import (
    ChatResponseContract,
    ChatTurnStateSnapshot,
    ContextLinkMode,
    ModelDraftResponse,
    OperationalFact,
    OperationalFactKind,
    build_live_information_resolution_facts,
    build_pending_approval_facts_from_tool_results,
    build_tool_execution_facts_from_tool_results,
    is_canonical_deferred_runtime_offer_reply,
    render_chat_response_for_telegram,
    text_mentions_approval_claim,
)
from nullion.live_information import LiveInformationResolution
from nullion.tools import ToolResult


def _state(*facts: OperationalFact) -> ChatTurnStateSnapshot:
    return ChatTurnStateSnapshot(
        conversation_id="conv",
        turn_id="turn",
        user_message="message",
        context_link=ContextLinkMode.STANDALONE,
        facts=facts,
        pending_approval_ids=tuple(
            str(fact.payload["approval_id"])
            for fact in facts
            if fact.kind is OperationalFactKind.APPROVAL_REQUEST_PENDING and fact.payload.get("approval_id")
        ),
    )


def _render(text: str, *facts: OperationalFact) -> str:
    return render_chat_response_for_telegram(ChatResponseContract(_state(*facts), ModelDraftResponse(text)))


def test_pending_approval_facts_render_real_approval_prompt(monkeypatch) -> None:
    approval = ApprovalRequest(
        "ap-1",
        "operator",
        "allow_boundary",
        "web_fetch",
        ApprovalStatus.PENDING,
        datetime(2026, 1, 1, tzinfo=UTC),
        context={"boundary_kind": "outbound_network", "selector": "example.com"},
    )
    result = ToolResult("inv-1", "web_fetch", "DENIED", {"reason": "approval_required", "approval_id": "ap-1"})
    facts, pending = build_pending_approval_facts_from_tool_results([result, result], approval_lookup=lambda approval_id: approval)

    monkeypatch.setattr("nullion.chat_response_contract.load_settings", lambda: type("S", (), {"web_session_allow_duration": "1h"})())

    assert pending == ("ap-1",)
    reply = _render("approval required", *facts)
    assert "Approval required before Nullion can continue." in reply
    assert "Approval ID: ap-1" in reply
    assert "Reply /approve ap-1" in reply

    ignored, ignored_pending = build_pending_approval_facts_from_tool_results(
        [ToolResult("inv-2", "web_fetch", "denied", {"reason": "other", "approval_id": "ap-2"})],
        approval_lookup=lambda approval_id: None,
    )
    assert ignored == ()
    assert ignored_pending == ()


def test_tool_execution_facts_drive_failure_completed_and_negative_claim_replies() -> None:
    failed_facts = build_tool_execution_facts_from_tool_results(
        [ToolResult("inv-fail", "web_fetch", "failed", {}, error="network down")]
    )
    reply = _render("web_fetch failed", *failed_facts)
    assert "I attempted web_fetch" in reply
    assert "network down" in reply

    facts = build_tool_execution_facts_from_tool_results([ToolResult("inv-ok", "file_write", "completed", {"path": "/tmp/a.txt"})])
    assert _render("permission mode says approval required", *facts) == "I completed file_write in this turn."
    assert _render("Tool: `web_fetch` — approval required", *facts) == "I haven't attempted web_fetch in this turn yet."
    assert _render("Tool: `web_fetch` — approval required\nMEDIA: /tmp/a.png", *facts).endswith("MEDIA: /tmp/a.png")

    attempted_only = build_tool_execution_facts_from_tool_results([ToolResult("inv-run", "web_fetch", "running", {})])
    assert "haven't attempted shell_exec" in _render("I haven't attempted shell_exec in this turn yet", *attempted_only)


def test_execution_state_narration_reports_latest_tool_and_live_information_paths() -> None:
    completed = build_tool_execution_facts_from_tool_results([ToolResult("inv-ok", "web_search", "completed", {})])
    assert _render("what is the execution state?", *completed) == "Execution state: preferred plugin path."

    denied = build_tool_execution_facts_from_tool_results(
        [ToolResult("inv-deny", "web_fetch", "denied", {"reason": "approval_required"})]
    )
    assert _render("runtime state please", *denied).startswith("Execution state: approval required.")

    nonterminal = build_tool_execution_facts_from_tool_results([ToolResult("inv-run", "web_fetch", "queued", {})])
    assert _render("execution state", *nonterminal) == "Execution state: in progress."

    for resolution, expected in [
        (LiveInformationResolution.PREFERRED_PLUGIN_PATH.value, "Execution state: preferred plugin path."),
        (LiveInformationResolution.CORE_FALLBACK.value, "Execution state: core fallback path."),
        (LiveInformationResolution.APPROVAL_REQUIRED.value, "Execution state: approval required."),
        (LiveInformationResolution.NO_USEFUL_RESULT.value, "Execution state: no useful result."),
        (LiveInformationResolution.BLOCKED.value, "Execution state: blocked."),
    ]:
        assert _render("execution state", *build_live_information_resolution_facts([resolution])) == expected


def test_ungrounded_drafts_are_rewritten_and_canonical_replies_detected() -> None:
    assert text_mentions_approval_claim("This requires approval before I can run it")
    reply = _render("I need permission to fetch web content first.")
    assert "haven't actually run that tool path" in reply
    assert is_canonical_deferred_runtime_offer_reply(reply)

    success_reply = _render("I executed it successfully.")
    assert "haven't actually run a tool" in success_reply
    assert is_canonical_deferred_runtime_offer_reply(success_reply)
    assert is_canonical_deferred_runtime_offer_reply(None) is False


def test_live_information_fact_builder_ignores_unknown_resolution_values() -> None:
    facts = build_live_information_resolution_facts([
        "unknown",
        LiveInformationResolution.NO_USEFUL_RESULT.value,
    ])
    assert len(facts) == 1
    assert facts[0].payload["resolution"] == LiveInformationResolution.NO_USEFUL_RESULT.value

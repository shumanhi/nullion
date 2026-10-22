import os
from pathlib import Path

from nullion import web_app


TEST_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = Path(os.environ.get("NULLION_APP_REPO", TEST_ROOT)).resolve()


def _web_app_source() -> str:
    return (APP_ROOT / "src" / "nullion" / "web_app.py").read_text(encoding="utf-8")


def test_web_screenshot_shortcut_does_not_run_for_image_generation(monkeypatch):
    def fail_if_called(*args, **kwargs):
        raise AssertionError("image prompts must not run the screenshot artifact shortcut")

    monkeypatch.setattr(web_app, "run_pre_chat_artifact_workflow", fail_if_called)

    payload = web_app._web_screenshot_payload_if_requested(
        object(),
        user_text="generate an image of a peacock",
        conversation_id="web:test",
        registry=None,
    )

    assert payload is None


def test_web_screenshot_shortcut_does_not_parse_free_text(monkeypatch):
    def fail_if_called(*args, **kwargs):
        raise AssertionError("free-text screenshot prompts must route through model/tool schemas")

    monkeypatch.setattr(web_app, "run_pre_chat_artifact_workflow", fail_if_called)

    payload = web_app._web_screenshot_payload_if_requested(
        object(),
        user_text="take a screenshot of https://example.com",
        conversation_id="web:test",
        registry=None,
    )

    assert payload is None


def test_web_screenshot_shortcut_does_not_call_prompt_parser():
    source = _web_app_source()

    assert "parse_screenshot_request" not in source


def test_recent_web_tool_context_is_historical_not_fresh():
    class Store:
        def list_conversation_events(self, conversation_id):
            return [
                {
                    "event_type": "conversation.chat_turn",
                    "created_at": "2026-05-02T04:33:46Z",
                    "user_message": "prior request",
                    "assistant_reply": "prior reply",
                    "tool_results": [{"tool_name": "web_fetch", "status": "ok"}],
                }
            ]

    runtime = type("Runtime", (), {"store": Store()})()

    prompt = web_app._recent_web_tool_context_prompt(runtime, "web:test")

    assert prompt is not None
    assert "Historical, timestamped tool outcomes" in prompt
    assert "prior evidence" in prompt
    assert "user instructions" in prompt
    assert "stale for the user's current-time need" in prompt
    assert "2026-05-02T04:33:46Z" in prompt


def test_web_approval_bubbles_reconcile_external_decisions():
    source = _web_app_source()

    assert "function reconcileApprovalBubbles(list)" in source
    assert "reconcileApprovalBubbles(data.approvals || [])" in source
    assert "setApprovalStateEverywhere(approvalId, approval.reason || 'Approved.', 'ok')" in source
    assert "removeApprovalBubblesEverywhere(approvalId)" in source
    assert "refreshConversationAfterExternalApproval(approvalId)" in source
    assert "function historyHasNewBotReply(msgs, minCount)" not in source
    assert "refreshConversationAfterExternalApproval(attempt + 1, baselineCount)" not in source


def test_web_approval_resolved_title_is_idempotent():
    source = _web_app_source()

    assert "data-approval-title-original" in source
    assert "resolvedApprovalTitle(title, kind)" in source
    assert "Approved|Denied|Request denied" not in source


def test_builder_reflection_does_not_route_from_build_mode_prompt_prefix():
    source = _web_app_source()

    assert ".startswith(\"mode: build" not in source.lower()
    assert ".startswith('mode: build" not in source.lower()
    assert "_queue_web_build_mode_proposal" not in source


def test_web_parallel_dispatch_uses_stored_turn_text_and_background_model_call():
    source = _web_app_source()

    assert "active_turn_text_by_id[turn_id] = user_text" in source
    assert "active_turn_text_by_id.get(active_turn_id, \"\")" in source
    assert "dispatch_decision = await asyncio.to_thread(" in source
    assert ".get_name()" not in source

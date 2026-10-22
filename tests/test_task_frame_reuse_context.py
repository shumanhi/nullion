from datetime import UTC, datetime
from dataclasses import replace
from types import SimpleNamespace

from nullion.chat_operator import (
    _chat_current_turn_has_structured_followup_evidence,
    _recent_tool_context_prompt,
    _recent_tool_scopes_for_context,
    _should_include_recent_tool_context,
)
from nullion.conversation_runtime import ConversationBranchStatus, ConversationTurnDisposition
from nullion.runtime import process_conversation_message, update_active_task_frame_from_outcomes
from nullion.runtime_store import ConversationBranch, ConversationTurn, RuntimeStore
from nullion.task_frames import (
    TaskFrame,
    TaskFrameContinuationDecision,
    TaskFrameContinuationMode,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
)
from nullion.tools import ToolResult
from nullion.web_app import _recent_web_tool_context_prompt, _should_include_web_recent_tool_context


def _store_with_active_frame() -> RuntimeStore:
    store = RuntimeStore()
    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="telegram:1",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.FETCH_RESOURCE,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(artifact_kind="pdf", delivery_mode="attachment"),
        finish=TaskFrameFinishCriteria(requires_artifact_delivery=True),
        summary="Fetch current headlines and deliver an artifact",
        created_at=datetime(2026, 5, 2, tzinfo=UTC),
        updated_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("telegram:1", "frame-1")
    return store


def test_task_frame_outcome_metadata_feeds_follow_up_context_without_chat_turn():
    store = _store_with_active_frame()

    update_active_task_frame_from_outcomes(
        store,
        conversation_id="telegram:1",
        tool_results=[
            ToolResult(
                invocation_id="extract-1",
                tool_name="browser_extract_text",
                status="completed",
                output={"summary": "CNN headline evidence extracted from the active page."},
            ),
            ToolResult(
                invocation_id="pdf-1",
                tool_name="pdf_create",
                status="completed",
                output={"artifact_path": "/tmp/cnn_latest_news_2026-05-02.pdf"},
            ),
        ],
        rendered_reply="Done — attached the requested PDF.\n\nMEDIA:/tmp/cnn_latest_news_2026-05-02.pdf",
        completion_turn_id="turn-1",
    )

    runtime = type("Runtime", (), {"store": store})()

    prompt = _recent_tool_context_prompt(runtime, "telegram:1")

    assert prompt is not None
    assert "task_frame_outcome" in prompt
    assert "do not silently reattach stale files from prior turns" in prompt
    assert "CNN headline evidence extracted" in prompt
    assert "cnn_latest_news_2026-05-02.pdf" in prompt


def test_web_recent_context_also_includes_task_frame_outcome():
    store = _store_with_active_frame()
    update_active_task_frame_from_outcomes(
        store,
        conversation_id="telegram:1",
        tool_results=[
            ToolResult(
                invocation_id="write-1",
                tool_name="file_write",
                status="completed",
                output={"path": "/tmp/cnn_latest_news_2026-05-02.txt"},
            )
        ],
        rendered_reply="Done — attached the requested text file.\n\nMEDIA:/tmp/cnn_latest_news_2026-05-02.txt",
        completion_turn_id="turn-2",
    )
    runtime = type("Runtime", (), {"store": store})()

    prompt = _recent_web_tool_context_prompt(runtime, "telegram:1")

    assert prompt is not None
    assert "task_frame_outcome" in prompt
    assert "cnn_latest_news_2026-05-02.txt" in prompt


def test_account_read_tool_result_anchors_compact_followup_context():
    store = RuntimeStore()
    store.add_conversation_event(
        {
            "conversation_id": "telegram:mail",
            "event_type": "conversation.chat_turn",
            "created_at": datetime.now(UTC).isoformat(),
            "user_message": "Busca correos no leídos.",
            "assistant_reply": "Encontré dos correos.",
            "tool_results": [
                {
                    "tool_name": "email_search",
                    "status": "completed",
                    "output": {
                        "body_requires_tool": "email_read",
                        "next_tool_for_body": "email_read",
                        "results": [{"id": "email-2", "subject": "Prueba"}],
                    },
                }
            ],
        }
    )
    runtime = type("Runtime", (), {"store": store})()

    assert _chat_current_turn_has_structured_followup_evidence(
        runtime,
        prompt="ouvre le deuxième",
        conversation_id="telegram:mail",
        attachments=None,
        previous_assistant_message=None,
        ambiguity_fallback_reason=None,
    )
    assert _recent_tool_scopes_for_context(runtime, "telegram:mail") == ("connector",)


def test_task_frame_completes_when_media_reply_has_no_tool_results(tmp_path):
    store = _store_with_active_frame()
    artifact = tmp_path / "lakehouse_weekend_itinerary.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%test\n")
    frame = store.get_task_frame("frame-1")
    store.add_task_frame(replace(frame, status=TaskFrameStatus.WAITING_INPUT))

    updated = update_active_task_frame_from_outcomes(
        store,
        conversation_id="telegram:1",
        tool_results=[],
        rendered_reply=f"Done — attached the requested PDF.\n\nMEDIA:{artifact}",
        completion_turn_id="turn-artifact",
    )

    assert updated is not None
    assert updated.status is TaskFrameStatus.COMPLETED
    assert updated.completion_turn_id == "turn-artifact"
    assert store.get_active_task_frame_id("telegram:1") is None


def test_delivered_artifact_frame_is_cleared_before_unrelated_turn(tmp_path):
    store = _store_with_active_frame()
    artifact = tmp_path / "weekend_plan.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%test\n")
    now = datetime(2026, 5, 2, tzinfo=UTC)
    store.add_conversation_turn(
        ConversationTurn(
            "turn-1",
            "telegram:1",
            "branch-1",
            None,
            ConversationTurnDisposition.INDEPENDENT,
            "/planner create a weekend PDF",
            "complete",
            now,
        )
    )
    store.add_conversation_branch(
        ConversationBranch("branch-1", "telegram:1", ConversationBranchStatus.ACTIVE, "turn-1")
    )
    store.set_conversation_head("telegram:1", active_branch_id="branch-1", active_turn_id="turn-1")
    frame = store.get_task_frame("frame-1")
    store.add_task_frame(
        replace(
            frame,
            status=TaskFrameStatus.WAITING_INPUT,
            operation=TaskFrameOperation.GENERATE_ARTIFACT,
            metadata={
                "last_outcome": {
                    "completion_turn_id": "turn-1",
                    "rendered_reply": f"Done — attached the requested PDF.\n\nMEDIA:{artifact}",
                    "tool_results": [],
                }
            },
        )
    )

    result = process_conversation_message(
        store,
        conversation_id="telegram:1",
        user_message="資料",
    )

    completed = store.get_task_frame("frame-1")
    assert completed.status is TaskFrameStatus.COMPLETED
    assert store.get_active_task_frame_id("telegram:1") is None
    assert result.active_task_frame_id is None
    assert result.turn.disposition is ConversationTurnDisposition.INDEPENDENT


def test_new_structured_turn_clears_stale_active_task_frame():
    store = _store_with_active_frame()

    result = process_conversation_message(
        store,
        conversation_id="telegram:1",
        user_message="Hello?",
    )

    assert result.turn.disposition is ConversationTurnDisposition.INDEPENDENT
    assert store.get_active_task_frame_id("telegram:1") is None
    assert result.active_task_frame_id is None


def test_recent_tool_context_only_included_for_structured_continuations():
    independent = SimpleNamespace(
        turn=SimpleNamespace(disposition=ConversationTurnDisposition.INDEPENDENT),
        task_frame_continuation=TaskFrameContinuationDecision(
            mode=TaskFrameContinuationMode.START_NEW,
            operation=None,
            target=None,
            output=None,
            execution=None,
            finish=None,
        ),
    )
    continuation = SimpleNamespace(
        turn=SimpleNamespace(disposition=ConversationTurnDisposition.CONTINUE),
        task_frame_continuation=None,
    )

    assert not _should_include_recent_tool_context(independent)
    assert _should_include_recent_tool_context(
        independent,
        structured_followup_evidence=True,
    )
    assert _should_include_recent_tool_context(continuation)
    assert not _should_include_web_recent_tool_context(independent)
    assert _should_include_web_recent_tool_context(
        independent,
        structured_followup_evidence=True,
    )
    assert _should_include_web_recent_tool_context(continuation)

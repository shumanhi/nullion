from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from nullion.telegram_turn_graph import plan_telegram_post_run_delivery
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)


def test_telegram_post_run_graph_splits_supplemental_card_and_contracts_media(monkeypatch, tmp_path) -> None:
    artifact = tmp_path / "result.custom"
    artifact.write_text("artifact", encoding="utf-8")
    supplemental_card = SimpleNamespace(text="Doctor card", supplemental=True)
    suggestion_markup = object()

    monkeypatch.setattr("nullion.messaging_adapters.media_candidate_paths_from_text", lambda text: [artifact])

    plan = plan_telegram_post_run_delivery(
        text_for_ack="whatever the user says",
        reply=f"Done.\nMEDIA:{artifact}",
        decision_card=supplemental_card,
        suggestion_markup=suggestion_markup,
        stream_final_reply=True,
        streaming_mode="stream",
        final_only_streaming_mode="final",
    )

    assert plan.primary_card is None
    assert plan.supplemental_card is supplemental_card
    assert plan.additional_markup is suggestion_markup
    assert plan.streaming_mode == "stream"
    assert plan.delivery_contract.requires_attachment_delivery
    assert plan.delivery_contract.source == "media_directive"


def test_telegram_post_run_graph_prefers_primary_card_and_final_streaming() -> None:
    primary_card = SimpleNamespace(text="Approval", supplemental=False)
    suggestion_markup = object()

    plan = plan_telegram_post_run_delivery(
        text_for_ack="/chat hi",
        reply="Plain reply",
        inbound_attachments=({"path": "/tmp/upload.bin", "name": "upload.bin"},),
        decision_card=primary_card,
        suggestion_markup=suggestion_markup,
        stream_final_reply=False,
        streaming_mode="stream",
        final_only_streaming_mode="final",
    )

    assert plan.primary_card is primary_card
    assert plan.supplemental_card is None
    assert plan.additional_markup is None
    assert plan.streaming_mode == "final"
    assert not plan.delivery_contract.requires_attachment_delivery


def test_telegram_post_run_graph_ignores_text_only_file_verbs() -> None:
    plan = plan_telegram_post_run_delivery(
        text_for_ack="send me this as a text file",
        reply="Here is the summary in chat.",
        decision_card=None,
        suggestion_markup=None,
        stream_final_reply=True,
        streaming_mode="stream",
        final_only_streaming_mode="final",
    )

    assert not plan.delivery_contract.requires_attachment_delivery
    assert plan.delivery_contract.source == "message"


def test_telegram_post_run_graph_uses_runtime_task_frame_contract() -> None:
    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="telegram:123",
        branch_id="branch-1",
        source_turn_id="turn-1",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.GENERATE_ARTIFACT,
        target=TaskFrameTarget(kind="file", value="news"),
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(artifact_kind="pdf", delivery_mode="attachment"),
        finish=TaskFrameFinishCriteria(requires_artifact_delivery=True, required_artifact_kind="pdf"),
        summary="Create PDF",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    class Store:
        def get_active_task_frame_id(self, conversation_id):
            return "frame-1" if conversation_id == "telegram:123" else None

        def get_task_frame(self, frame_id):
            return frame if frame_id == "frame-1" else None

    plan = plan_telegram_post_run_delivery(
        text_for_ack="and those columns sounds good",
        reply="I’m not done yet — this task still needs a file attachment.",
        runtime=SimpleNamespace(store=Store()),
        conversation_id="telegram:123",
        decision_card=None,
        suggestion_markup=None,
        stream_final_reply=True,
        streaming_mode="stream",
        final_only_streaming_mode="final",
    )

    assert plan.delivery_contract.requires_attachment_delivery
    assert plan.delivery_contract.source == "task_frame"
    assert plan.delivery_contract.required_attachment_extensions == (".pdf",)

from __future__ import annotations

from nullion.execution_outcome import ExecutionStatus, build_execution_outcome
from nullion.response_fulfillment_contract import evaluate_response_execution_outcome
from nullion.tools import ToolResult


class EmptyStore:
    def get_active_task_frame_id(self, conversation_id: str):  # noqa: ARG002
        return None


def test_execution_outcome_marks_connector_reauth_as_user_action() -> None:
    result = ToolResult(
        "tool-1",
        "connector_request",
        "failed",
        {"connection_state": "pending_or_failed", "connector_app_id": "google-mail"},
        "google-mail connection is pending or failed",
    )

    outcome = build_execution_outcome(
        requested_outcome="check email",
        tool_results=[result],
        fulfillment_satisfied=False,
    )

    assert outcome.status is ExecutionStatus.NEEDS_USER_ACTION
    assert outcome.failure_reason == "google-mail connection is pending or failed"
    assert outcome.next_action == "Reconnect the account, then retry the task."
    assert outcome.execution_succeeded is False


def test_execution_outcome_marks_terminal_user_action_result_as_user_action() -> None:
    result = ToolResult(
        "tool-1",
        "email_search",
        "completed",
        {
            "terminal_user_action_required": True,
            "reason": "account_connection_reconnect_required",
            "connector_app_id": "google-mail",
            "result_text": "Reconnect Google Mail.",
        },
    )

    outcome = build_execution_outcome(
        requested_outcome="check email",
        tool_results=[result],
        fulfillment_satisfied=True,
    )

    assert outcome.status is ExecutionStatus.NEEDS_USER_ACTION
    assert outcome.failure_reason == "google-mail connection is pending or failed"
    assert outcome.next_action == "Reconnect the account, then retry the task."


def test_execution_outcome_marks_attachment_contract_failure_as_failed() -> None:
    evaluation = evaluate_response_execution_outcome(
        store=EmptyStore(),
        conversation_id="telegram:1",
        user_message="make a pdf",
        reply="Done.",
        required_attachment_extensions=[".pdf"],
    )

    assert evaluation.satisfied is False
    assert evaluation.outcome.status is ExecutionStatus.FAILED
    assert evaluation.outcome.missing_requirements == ("pdf attachment",)


def test_response_execution_outcome_rewrites_connector_reauth_reply() -> None:
    result = ToolResult(
        "tool-1",
        "connector_request",
        "failed",
        {"connection_state": "pending_or_failed", "connector_app_id": "google-mail"},
        "google-mail connection is pending or failed",
    )

    evaluation = evaluate_response_execution_outcome(
        store=EmptyStore(),
        conversation_id="telegram:1",
        user_message="check my email",
        reply="Done.",
        tool_results=[result],
    )

    assert evaluation.outcome.status is ExecutionStatus.NEEDS_USER_ACTION
    assert evaluation.satisfied is False
    assert evaluation.reply == (
        "I need one more step before I can complete this: google-mail connection is pending or failed. "
        "Reconnect the account, then retry the task."
    )


def test_execution_outcome_marks_partial_delivery_success() -> None:
    outcome = build_execution_outcome(
        requested_outcome="send report",
        user_visible_message="The report ran, but the attachment failed.",
        artifacts_created=["/tmp/report.pdf"],
        delivery_status="partial_success",
        reason="attachment delivery failed",
    )

    assert outcome.status is ExecutionStatus.PARTIALLY_SUCCEEDED
    assert outcome.execution_succeeded is True
    assert outcome.delivery_succeeded is True
    assert outcome.failure_reason == "attachment delivery failed"


def test_execution_outcome_marks_deferred_as_visible_background_start() -> None:
    outcome = build_execution_outcome(
        requested_outcome="run scheduled job",
        user_visible_message="Manual scheduled task run started.",
        delivery_status="deferred",
    )

    assert outcome.status is ExecutionStatus.PARTIALLY_SUCCEEDED
    assert outcome.delivery_succeeded is False
    assert outcome.next_action == "Wait for the background run to deliver its terminal result."

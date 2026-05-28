"""Platform-neutral execution outcome classification.

This module is intentionally deterministic and cheap. It turns already-known
runtime facts into a shared outcome contract without calling a model, scanning
large stores, or loading connector inventories on the hot path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable


class ExecutionStatus(str, Enum):
    SUCCEEDED = "succeeded"
    PARTIALLY_SUCCEEDED = "partially_succeeded"
    NEEDS_USER_ACTION = "needs_user_action"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_EXECUTION_STATUSES = frozenset(status.value for status in ExecutionStatus)


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    status: ExecutionStatus
    requested_outcome: str = ""
    user_visible_message: str = ""
    tools_used: tuple[str, ...] = ()
    failed_tools: tuple[str, ...] = ()
    artifacts_created: tuple[str, ...] = ()
    artifacts_delivered: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()
    missing_requirements: tuple[str, ...] = ()
    failure_reason: str | None = None
    next_action: str | None = None
    execution_succeeded: bool = False
    delivery_succeeded: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "requested_outcome": self.requested_outcome,
            "user_visible_message": self.user_visible_message,
            "tools_used": list(self.tools_used),
            "failed_tools": list(self.failed_tools),
            "artifacts_created": list(self.artifacts_created),
            "artifacts_delivered": list(self.artifacts_delivered),
            "evidence": list(self.evidence),
            "missing_requirements": list(self.missing_requirements),
            "failure_reason": self.failure_reason,
            "next_action": self.next_action,
            "execution_succeeded": self.execution_succeeded,
            "delivery_succeeded": self.delivery_succeeded,
        }


@dataclass(frozen=True, slots=True)
class ExecutionOutcomeEvaluation:
    reply: str
    satisfied: bool
    missing_requirements: tuple[str, ...] = ()
    outcome: ExecutionOutcome = field(default_factory=lambda: ExecutionOutcome(status=ExecutionStatus.FAILED))


def build_execution_outcome(
    *,
    requested_outcome: str = "",
    user_visible_message: str = "",
    tool_results: Iterable[object] | None = None,
    artifacts_created: Iterable[str] | None = None,
    artifacts_delivered: Iterable[str] | None = None,
    delivery_status: str | None = None,
    fulfillment_satisfied: bool = True,
    missing_requirements: Iterable[str] | None = None,
    cancelled: bool = False,
    suspended_for_approval: bool = False,
    reason: str | None = None,
) -> ExecutionOutcome:
    """Build a normalized terminal outcome from structured execution facts."""
    tools_used: list[str] = []
    failed_tools: list[str] = []
    evidence: list[str] = []
    needs_user_action_reason = ""
    first_failure_reason = str(reason or "").strip()
    completed_tools = 0
    failed_or_denied_tools = 0
    for result in tool_results or ():
        tool_name = _tool_name(result)
        if tool_name:
            tools_used.append(tool_name)
        status = _tool_status(result)
        output = _tool_output(result)
        if isinstance(output, dict):
            user_action_reason = _user_action_reason_from_output(output)
            if user_action_reason:
                needs_user_action_reason = needs_user_action_reason or user_action_reason
                first_failure_reason = first_failure_reason or user_action_reason
        if status == "completed":
            completed_tools += 1
            if tool_name:
                evidence.append(f"tool_completed:{tool_name}")
        elif status in {"failed", "denied"}:
            failed_or_denied_tools += 1
            if tool_name:
                failed_tools.append(tool_name)
                evidence.append(f"tool_{status}:{tool_name}")
            if isinstance(output, dict) and output.get("connection_state") == "pending_or_failed":
                app_id = str(output.get("connector_app_id") or "connector").strip()
                needs_user_action_reason = f"{app_id} connection is pending or failed"
                first_failure_reason = first_failure_reason or needs_user_action_reason
            if not first_failure_reason:
                first_failure_reason = _tool_error(result) or f"{tool_name or 'tool'} {status}"
            if isinstance(output, dict) and output.get("reason") == "missing_dependency":
                package = str(output.get("package") or output.get("dependency") or "dependency").strip()
                needs_user_action_reason = f"missing dependency: {package}"
    created = _unique_paths(artifacts_created or ())
    delivered = _unique_paths(artifacts_delivered or ())
    if created:
        evidence.append(f"artifacts_created:{len(created)}")
    if delivered:
        evidence.append(f"artifacts_delivered:{len(delivered)}")

    normalized_delivery = str(delivery_status or "").strip().lower()
    missing = tuple(str(item).strip() for item in (missing_requirements or ()) if str(item).strip())
    has_visible_output = bool(str(user_visible_message or "").strip()) or bool(delivered)

    if cancelled:
        return _outcome(
            ExecutionStatus.CANCELLED,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=first_failure_reason or "cancelled",
            next_action=None,
            delivery_status=normalized_delivery,
        )

    if suspended_for_approval or normalized_delivery == "paused_for_approval":
        return _outcome(
            ExecutionStatus.NEEDS_USER_ACTION,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=first_failure_reason or "waiting for approval",
            next_action="Approve or deny the pending action.",
            delivery_status=normalized_delivery,
        )

    if needs_user_action_reason:
        return _outcome(
            ExecutionStatus.NEEDS_USER_ACTION,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=needs_user_action_reason,
            next_action=_next_action_for_user_action_reason(needs_user_action_reason),
            delivery_status=normalized_delivery,
        )

    if normalized_delivery == "partial_success":
        return _outcome(
            ExecutionStatus.PARTIALLY_SUCCEEDED,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=first_failure_reason or "delivery partially failed",
            next_action="Review the delivered text and retry attachment delivery if needed.",
            delivery_status=normalized_delivery,
        )

    if normalized_delivery == "deferred":
        return _outcome(
            ExecutionStatus.PARTIALLY_SUCCEEDED,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=first_failure_reason,
            next_action="Wait for the background run to deliver its terminal result.",
            delivery_status=normalized_delivery,
        )

    if normalized_delivery == "failed" or not fulfillment_satisfied:
        return _outcome(
            ExecutionStatus.FAILED,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=first_failure_reason or _missing_reason(missing) or "execution was not fulfilled",
            next_action="Retry after fixing the reported blocker." if first_failure_reason or missing else None,
            delivery_status=normalized_delivery,
        )

    if failed_or_denied_tools and (completed_tools or has_visible_output or created):
        return _outcome(
            ExecutionStatus.PARTIALLY_SUCCEEDED,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=first_failure_reason,
            next_action="Review the partial result and retry the failed step if needed.",
            delivery_status=normalized_delivery,
        )

    if failed_or_denied_tools and not has_visible_output:
        return _outcome(
            ExecutionStatus.FAILED,
            requested_outcome=requested_outcome,
            user_visible_message=user_visible_message,
            tools_used=tools_used,
            failed_tools=failed_tools,
            artifacts_created=created,
            artifacts_delivered=delivered,
            evidence=evidence,
            missing_requirements=missing,
            failure_reason=first_failure_reason or "all attempted tools failed",
            next_action="Retry after fixing the reported blocker.",
            delivery_status=normalized_delivery,
        )

    return _outcome(
        ExecutionStatus.SUCCEEDED,
        requested_outcome=requested_outcome,
        user_visible_message=user_visible_message,
        tools_used=tools_used,
        failed_tools=failed_tools,
        artifacts_created=created,
        artifacts_delivered=delivered,
        evidence=evidence,
        missing_requirements=missing,
        failure_reason=None,
        next_action=None,
        delivery_status=normalized_delivery,
    )


def _outcome(
    status: ExecutionStatus,
    *,
    requested_outcome: str,
    user_visible_message: str,
    tools_used: list[str],
    failed_tools: list[str],
    artifacts_created: tuple[str, ...],
    artifacts_delivered: tuple[str, ...],
    evidence: list[str],
    missing_requirements: tuple[str, ...],
    failure_reason: str | None,
    next_action: str | None,
    delivery_status: str,
) -> ExecutionOutcome:
    delivery_succeeded = delivery_status in {"sent", "saved", "silent", "partial_success"} or bool(artifacts_delivered)
    execution_succeeded = status in {ExecutionStatus.SUCCEEDED, ExecutionStatus.PARTIALLY_SUCCEEDED}
    return ExecutionOutcome(
        status=status,
        requested_outcome=str(requested_outcome or "").strip(),
        user_visible_message=str(user_visible_message or "").strip(),
        tools_used=tuple(dict.fromkeys(tool for tool in tools_used if tool)),
        failed_tools=tuple(dict.fromkeys(tool for tool in failed_tools if tool)),
        artifacts_created=artifacts_created,
        artifacts_delivered=artifacts_delivered,
        evidence=tuple(dict.fromkeys(item for item in evidence if item)),
        missing_requirements=missing_requirements,
        failure_reason=failure_reason,
        next_action=next_action,
        execution_succeeded=execution_succeeded,
        delivery_succeeded=delivery_succeeded,
    )


def _tool_name(result: object) -> str:
    if isinstance(result, dict):
        return str(result.get("tool_name") or result.get("name") or "").strip()
    return str(getattr(result, "tool_name", "") or getattr(result, "name", "") or "").strip()


def _tool_status(result: object) -> str:
    raw = result.get("status") if isinstance(result, dict) else getattr(result, "status", None)
    text = str(raw or "").strip().lower()
    if text in {"complete", "completed", "success", "succeeded", "ok", "done"}:
        return "completed"
    if text in {"denied", "blocked", "approval_required"}:
        return "denied"
    if text in {"failed", "failure", "error", "errored", "timeout", "timed_out", "partial"}:
        return "failed"
    return text or "unknown"


def _tool_output(result: object) -> object:
    if isinstance(result, dict):
        return result.get("output")
    return getattr(result, "output", None)


def _tool_error(result: object) -> str:
    if isinstance(result, dict):
        return str(result.get("error") or "").strip()
    return str(getattr(result, "error", "") or "").strip()


def _user_action_reason_from_output(output: dict[object, object]) -> str:
    if output.get("connection_state") == "pending_or_failed":
        app_id = str(output.get("connector_app_id") or output.get("app") or "connector").strip()
        return f"{app_id} connection is pending or failed"
    if output.get("terminal_user_action_required"):
        reason = str(output.get("reason") or "").strip()
        if reason == "account_connection_reconnect_required":
            app_id = str(output.get("connector_app_id") or output.get("app") or "connector").strip()
            return f"{app_id} connection is pending or failed"
        result_text = str(output.get("result_text") or output.get("next_step") or "").strip()
        if result_text:
            return result_text
        return "user action required"
    return ""


def _unique_paths(paths: Iterable[str]) -> tuple[str, ...]:
    values: list[str] = []
    for path in paths:
        text = str(path or "").strip()
        if not text:
            continue
        try:
            text = str(Path(text).expanduser())
        except (OSError, RuntimeError, ValueError):
            pass
        if text not in values:
            values.append(text)
    return tuple(values)


def _missing_reason(missing_requirements: tuple[str, ...]) -> str | None:
    if not missing_requirements:
        return None
    return "missing requirement: " + "; ".join(missing_requirements)


def _next_action_for_user_action_reason(reason: str) -> str:
    if "connection is pending or failed" in reason:
        return "Reconnect the account, then retry the task."
    if reason.startswith("missing dependency:"):
        return "Install or approve the missing dependency, then retry the task."
    return "Complete the required user action, then retry the task."


__all__ = [
    "ExecutionOutcome",
    "ExecutionOutcomeEvaluation",
    "ExecutionStatus",
    "TERMINAL_EXECUTION_STATUSES",
    "build_execution_outcome",
]

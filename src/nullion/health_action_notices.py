"""Public receipts for explicit health-alert status changes, never repair proof."""
from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class HealthActionNotice:
    acknowledgement: str
    notice_title: str
    message: str
    effect: str = "alert_status"
    repair_verified: bool = False

    def as_payload(self) -> dict[str, object]:
        return asdict(self)


def health_action_notice(
    family: str, command: str, status: str, *, already_handled: bool = False,
) -> HealthActionNotice:
    """Use command IDs and persisted status, without inspecting prompt/detail text."""
    noun = {"doctor": "Doctor alert", "sentinel": "Sentinel alert"}[family]
    command = {"cancel": "dismiss"}.get(command, command)
    labels = {
        "completed": "resolved", "resolved": "resolved", "cancelled": "dismissed",
        "dismissed": "dismissed", "failed": "failed", "in_progress": "in progress",
        "running": "in progress", "pending": "pending", "acknowledged": "reviewed",
        "escalated": "open",
    }
    expected = {
        "start": {"in_progress", "running"}, "complete": {"completed", "resolved"},
        "dismiss": {"cancelled", "dismissed"}, "acknowledge": {"acknowledged"},
        "resolve": {"resolved"},
    }
    if command not in expected:
        raise ValueError("Unsupported health-alert status command")
    if already_handled or status not in expected[command]:
        label = labels.get(status, "closed")
        return HealthActionNotice(
            f"Already {label}", f"{noun} status",
            f"{noun} is already {label}.\n\nNo new action was run.",
        )
    label = labels[status]
    message = f"{noun} marked as {label}."
    if command == "dismiss":
        message = f"{noun} dismissed."
    if command in {"complete", "resolve"}:
        message += "\n\nThis records your choice; it does not verify a repair."
    elif command == "dismiss":
        message += "\n\nThe underlying issue has not been marked as fixed."
    elif command == "start":
        message += "\n\nThis updates the alert status only."
    else:
        message += "\n\nThe alert remains open for follow-up."
    return HealthActionNotice(label.capitalize(), f"{noun} updated", message)

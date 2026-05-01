"""Progress and nudge primitives for intent execution."""

from dataclasses import dataclass
from enum import Enum

from nullion.intent import IntentCapsule, IntentState


class ProgressState(str, Enum):
    ACKNOWLEDGED = "acknowledged"
    WORKING = "working"
    WAITING_APPROVAL = "waiting_approval"
    BLOCKED = "blocked"
    COMPLETED = "completed"


@dataclass(slots=True, frozen=True)
class ProgressUpdate:
    state: ProgressState
    message: str
    capsule_id: str


_MESSAGE_BY_INTENT_STATE: dict[IntentState, tuple[ProgressState, str]] = {
    IntentState.PENDING: (ProgressState.ACKNOWLEDGED, "Got it."),
    IntentState.RUNNING: (ProgressState.WORKING, "Working on it."),
    IntentState.WAITING_APPROVAL: (
        ProgressState.WAITING_APPROVAL,
        "Waiting for approval.",
    ),
    IntentState.BLOCKED: (ProgressState.BLOCKED, "Blocked for now."),
    IntentState.COMPLETED: (ProgressState.COMPLETED, "Done."),
    IntentState.FAILED: (ProgressState.BLOCKED, "Hit a failure."),
}


def _humanize_label(value: object) -> str:
    return str(value).replace("_", " ")


def build_progress_update_snapshot(progress_update: ProgressUpdate) -> dict[str, object]:
    return {
        "capsule_id": progress_update.capsule_id,
        "state": _humanize_label(progress_update.state.value),
        "message": progress_update.message,
    }


def format_progress_update_for_telegram(snapshot: dict[str, object]) -> str:
    lines = [
        "📣 Nullion progress",
        str(snapshot["capsule_id"]),
        f"State: {snapshot['state']}",
        str(snapshot["message"]),
    ]
    return "\n".join(lines)


def render_progress_update_for_telegram(progress_update: ProgressUpdate) -> str:
    return format_progress_update_for_telegram(build_progress_update_snapshot(progress_update))


def progress_update_for_intent(capsule: IntentCapsule) -> ProgressUpdate:
    state, message = _MESSAGE_BY_INTENT_STATE[capsule.state]
    return ProgressUpdate(state=state, message=message, capsule_id=capsule.capsule_id)


def should_emit_nudge(previous_state: ProgressState, current_state: ProgressState) -> bool:
    return previous_state is not current_state


__all__ = [
    "ProgressState",
    "ProgressUpdate",
    "build_progress_update_snapshot",
    "format_progress_update_for_telegram",
    "progress_update_for_intent",
    "render_progress_update_for_telegram",
    "should_emit_nudge",
]

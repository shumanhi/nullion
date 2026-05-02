"""Typed active task-frame datamodel for Nullion finish-line runtime work."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, TypedDict
from urllib.parse import urlparse

from langgraph.graph import END, START, StateGraph

from nullion.attachment_format_graph import plan_attachment_format


_URL_DISALLOWED_EDGE_CHARS = "()[]{}<>`\"'\".,;:!?"
DELIVERY_MODE_ATTACHMENT = "attachment"
DELIVERY_MODE_LEGACY_TELEGRAM_ATTACHMENT = "telegram_attachment"
DELIVERY_MODE_INLINE_TEXT = "inline_text"
ATTACHMENT_DELIVERY_MODES = frozenset({DELIVERY_MODE_ATTACHMENT, DELIVERY_MODE_LEGACY_TELEGRAM_ATTACHMENT})


def normalize_delivery_mode(delivery_mode: str | None) -> str | None:
    if delivery_mode == DELIVERY_MODE_LEGACY_TELEGRAM_ATTACHMENT:
        return DELIVERY_MODE_ATTACHMENT
    return delivery_mode


def is_attachment_delivery_mode(delivery_mode: str | None) -> bool:
    return delivery_mode in ATTACHMENT_DELIVERY_MODES


class TaskFrameStatus(str, Enum):
    ACTIVE = "active"
    WAITING_APPROVAL = "waiting_approval"
    WAITING_INPUT = "waiting_input"
    RUNNING = "running"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    SUPERSEDED = "superseded"
    CANCELLED = "cancelled"


class TaskFrameOperation(str, Enum):
    FETCH_RESOURCE = "fetch_resource"
    GENERATE_ARTIFACT = "generate_artifact"
    EXECUTE_COMMAND = "execute_command"
    MODIFY_CODE = "modify_code"
    ANSWER_WITH_CONTEXT = "answer_with_context"
    UNKNOWN = "unknown"


class TaskFrameContinuationMode(str, Enum):
    CONTINUE = "continue"
    REVISE = "revise"
    SUBSTITUTE_TARGET = "substitute_target"
    START_NEW = "start_new"


@dataclass(slots=True)
class TaskFrameTarget:
    kind: str
    value: str
    normalized_value: str | None = None


@dataclass(slots=True)
class TaskFrameOutputContract:
    artifact_kind: str | None = None
    delivery_mode: str | None = None
    response_shape: str | None = None

    def __post_init__(self) -> None:
        self.delivery_mode = normalize_delivery_mode(self.delivery_mode)


@dataclass(slots=True)
class TaskFrameExecutionContract:
    preferred_tool_family: str | None = None
    fallback_tool_family: str | None = None
    approval_sensitive: bool = False
    boundary_kind: str | None = None


@dataclass(slots=True)
class TaskFrameFinishCriteria:
    requires_attempt: bool = True
    requires_artifact_delivery: bool = False
    required_artifact_kind: str | None = None
    required_tool_completion: tuple[str, ...] = ()


@dataclass(slots=True)
class TaskFrame:
    frame_id: str
    conversation_id: str
    branch_id: str
    source_turn_id: str
    parent_frame_id: str | None
    status: TaskFrameStatus
    operation: TaskFrameOperation
    target: TaskFrameTarget | None
    execution: TaskFrameExecutionContract
    output: TaskFrameOutputContract
    finish: TaskFrameFinishCriteria
    summary: str
    created_at: datetime
    updated_at: datetime
    last_activity_turn_id: str | None = None
    completion_turn_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TaskFrameContinuationDecision:
    mode: TaskFrameContinuationMode
    operation: TaskFrameOperation | None
    target: TaskFrameTarget | None
    output: TaskFrameOutputContract | None
    execution: TaskFrameExecutionContract | None
    finish: TaskFrameFinishCriteria | None


class _TaskFrameContinuationState(TypedDict, total=False):
    text: str
    active_frame: TaskFrame | None
    branch_continuous: bool
    decision: TaskFrameContinuationDecision | None


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    normalized = parsed._replace(fragment="")
    normalized_text = normalized.geturl()
    if parsed.path == "" and not normalized_text.endswith("/"):
        normalized_text = f"{normalized_text}/"
    return normalized_text


def _strip_url_enclosing_chars(raw_token: str) -> str:
    return raw_token.strip(_URL_DISALLOWED_EDGE_CHARS)


def _looks_like_domain(hostname: str) -> bool:
    if not hostname or " " in hostname:
        return False
    if hostname.startswith(".") or hostname.endswith("."):
        return False
    labels = hostname.split(".")
    if len(labels) < 2:
        return False
    if not all(labels):
        return False
    if not all(part[0].isalnum() and part[-1].isalnum() for part in labels):
        return False
    for part in labels:
        if not all(ch.isalnum() or ch == "-" for ch in part):
            return False
    tld = labels[-1]
    return len(tld) >= 2 and all(ch.isalpha() for ch in tld)


def _to_url_if_hosted(raw_candidate: str, *, has_explicit_scheme: bool) -> str | None:
    parsed = urlparse(raw_candidate)
    if not parsed.netloc:
        return None
    hostname = parsed.hostname or ""
    if not _looks_like_domain(hostname):
        return None
    normalized = parsed._replace(fragment="")
    if has_explicit_scheme and not parsed.scheme:
        # Defensive: if someone gave //example.com
        normalized = normalized._replace(scheme="https")
    return normalized.geturl()


def _extract_url_target(text: str) -> TaskFrameTarget | None:
    for raw_token in text.split():
        candidate = _strip_url_enclosing_chars(raw_token)
        if not candidate:
            continue

        explicit_with_scheme = candidate.startswith("http://") or candidate.startswith("https://")
        if explicit_with_scheme:
            raw_url = _to_url_if_hosted(candidate, has_explicit_scheme=True)
            if raw_url is not None:
                return TaskFrameTarget(
                    kind="url",
                    value=raw_url,
                    normalized_value=_normalize_url(raw_url),
                )

        if "//" not in candidate and "/" in candidate:
            prefixed = f"https://{candidate}"
            raw_url = _to_url_if_hosted(prefixed, has_explicit_scheme=False)
            if raw_url is not None:
                return TaskFrameTarget(
                    kind="url",
                    value=raw_url,
                    normalized_value=_normalize_url(raw_url),
                )

        candidate_without_trailing = candidate.rstrip("/")
        if "." in candidate_without_trailing:
            prefixed = f"https://{candidate_without_trailing}"
            raw_url = _to_url_if_hosted(prefixed, has_explicit_scheme=False)
            if raw_url is not None:
                return TaskFrameTarget(
                    kind="url",
                    value=raw_url,
                    normalized_value=_normalize_url(raw_url),
                )

    return None




def _detect_output_override(text: str, current: TaskFrameOutputContract) -> TaskFrameOutputContract | None:
    requested_format = plan_attachment_format(text)
    if requested_format.extension is not None:
        return TaskFrameOutputContract(
            artifact_kind=requested_format.extension.removeprefix("."),
            delivery_mode=DELIVERY_MODE_ATTACHMENT,
            response_shape=current.response_shape,
        )
    return None



def _is_referential_task_follow_up(text: str) -> bool:
    """Free-form referential language is handled by the model, not local keywords."""

    return False



def _apply_output_to_finish(
    finish: TaskFrameFinishCriteria,
    output: TaskFrameOutputContract,
) -> TaskFrameFinishCriteria:
    requires_delivery = is_attachment_delivery_mode(output.delivery_mode)
    return TaskFrameFinishCriteria(
        requires_attempt=finish.requires_attempt,
        requires_artifact_delivery=requires_delivery,
        required_artifact_kind=output.artifact_kind,
        required_tool_completion=finish.required_tool_completion,
    )



def _task_frame_start_or_continue_node(state: _TaskFrameContinuationState) -> dict[str, object]:
    active_frame = state.get("active_frame")
    if active_frame is None or not bool(state.get("branch_continuous")):
        return {"decision": TaskFrameContinuationDecision(
            mode=TaskFrameContinuationMode.START_NEW,
            operation=None,
            target=None,
            output=None,
            execution=None,
            finish=None,
        )}
    return {}


def _task_frame_output_override_node(state: _TaskFrameContinuationState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    active_frame = state["active_frame"]
    assert active_frame is not None
    output_override = _detect_output_override(state["text"], active_frame.output)
    if output_override is not None:
        return {"decision": TaskFrameContinuationDecision(
            mode=TaskFrameContinuationMode.REVISE,
            operation=active_frame.operation,
            target=active_frame.target,
            output=output_override,
            execution=active_frame.execution,
            finish=_apply_output_to_finish(active_frame.finish, output_override),
        )}
    return {}


def _task_frame_substitute_target_node(state: _TaskFrameContinuationState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    active_frame = state["active_frame"]
    assert active_frame is not None
    new_target = _extract_url_target(state["text"])
    if (
        new_target is not None
        and active_frame.target is not None
        and new_target.normalized_value != active_frame.target.normalized_value
    ):
        return {"decision": TaskFrameContinuationDecision(
            mode=TaskFrameContinuationMode.SUBSTITUTE_TARGET,
            operation=active_frame.operation,
            target=new_target,
            output=active_frame.output,
            execution=active_frame.execution,
            finish=active_frame.finish,
        )}
    return {}


def _task_frame_actionable_new_task_node(state: _TaskFrameContinuationState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    return {}


def _task_frame_fallback_node(state: _TaskFrameContinuationState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    return {"decision": TaskFrameContinuationDecision(
        mode=TaskFrameContinuationMode.START_NEW,
        operation=None,
        target=None,
        output=None,
        execution=None,
        finish=None,
    )}


@lru_cache(maxsize=1)
def _compiled_task_frame_continuation_graph():
    graph = StateGraph(_TaskFrameContinuationState)
    graph.add_node("start_or_continue", _task_frame_start_or_continue_node)
    graph.add_node("output_override", _task_frame_output_override_node)
    graph.add_node("substitute_target", _task_frame_substitute_target_node)
    graph.add_node("actionable_new_task", _task_frame_actionable_new_task_node)
    graph.add_node("fallback", _task_frame_fallback_node)
    graph.add_edge(START, "start_or_continue")
    graph.add_edge("start_or_continue", "output_override")
    graph.add_edge("output_override", "substitute_target")
    graph.add_edge("substitute_target", "actionable_new_task")
    graph.add_edge("actionable_new_task", "fallback")
    graph.add_edge("fallback", END)
    return graph.compile()


def resolve_task_frame_continuation(
    *,
    text: str,
    active_frame: TaskFrame | None,
    branch_continuous: bool,
) -> TaskFrameContinuationDecision:
    final_state = _compiled_task_frame_continuation_graph().invoke(
        {
            "text": text,
            "active_frame": active_frame,
            "branch_continuous": branch_continuous,
            "decision": None,
        },
        config={"configurable": {"thread_id": "task-frame-continuation"}},
    )
    decision = final_state.get("decision")
    if isinstance(decision, TaskFrameContinuationDecision):
        return decision
    return TaskFrameContinuationDecision(
        mode=TaskFrameContinuationMode.START_NEW,
        operation=None,
        target=None,
        output=None,
        execution=None,
        finish=None,
    )


__all__ = [
    "TaskFrameStatus",
    "TaskFrameOperation",
    "TaskFrameContinuationMode",
    "TaskFrameTarget",
    "TaskFrameOutputContract",
    "TaskFrameExecutionContract",
    "TaskFrameFinishCriteria",
    "TaskFrame",
    "TaskFrameContinuationDecision",
    "DELIVERY_MODE_ATTACHMENT",
    "DELIVERY_MODE_LEGACY_TELEGRAM_ATTACHMENT",
    "DELIVERY_MODE_INLINE_TEXT",
    "ATTACHMENT_DELIVERY_MODES",
    "normalize_delivery_mode",
    "is_attachment_delivery_mode",
    "resolve_task_frame_continuation",
]

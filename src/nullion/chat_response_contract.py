"""Typed state-backed chat response contract for Nullion."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, Callable, Mapping

import nullion.system_context as system_context
from nullion.config import load_settings, web_session_allow_duration_label
from nullion.approval_display import approval_display_from_request
from nullion.live_information import (
    LiveInformationResolution,
    actionable_live_information_resolutions,
    format_live_information_resolution_label,
    format_live_information_states_for_prompt,
)
from nullion.tools import ToolResult, normalize_tool_status


class OperationalFactKind(str, Enum):
    APPROVAL_REQUEST_PENDING = "approval_request_pending"
    TOOL_ATTEMPTED = "tool_attempted"
    TOOL_COMPLETED = "tool_completed"
    TOOL_FAILED = "tool_failed"
    LIVE_INFORMATION_RESOLUTION = "live_information_resolution"


class ContextLinkMode(str, Enum):
    STANDALONE = "standalone"
    CONTINUE = "continue"


@dataclass(frozen=True, slots=True)
class OperationalFact:
    fact_id: str
    kind: OperationalFactKind
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ChatTurnStateSnapshot:
    conversation_id: str
    turn_id: str
    user_message: str
    context_link: ContextLinkMode
    facts: tuple[OperationalFact, ...] = ()
    pending_approval_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ModelDraftResponse:
    text: str


@dataclass(frozen=True, slots=True)
class ChatResponseContract:
    state: ChatTurnStateSnapshot
    draft: ModelDraftResponse


_UNGROUNDED_APPROVAL_CLAIM_PHRASES = (
    "permission mode",
    "needs your approval to run",
    "got blocked by the permission mode",
    "approval to run",
    "allow it",
    "requires approval",
    "can't run",
    "approval required before nullion can continue",
    "please approve to proceed",
)
_UNGROUNDED_OPERATIONAL_NARRATION_PHRASES = (
    "i need permission",
    "resend the message",
    "try again",
    "fetch web content first",
    "runtime state",
    "permission to fetch",
    "webfetch",
)
_EXECUTION_STATE_NARRATION_PHRASES = (
    "runtime state",
    "execution state",
    "execution path",
)
_NEGATIVE_TOOL_STATUS_MARKERS = (
    "denied",
    "blocked",
    "failed",
    "approval required",
    "requires approval",
)


def _tokenize_text(text: str) -> list[str]:
    normalized_chars = [char if (char.isalnum() or char in {"_", "'"}) else " " for char in text.casefold().replace("’", "'")]
    return "".join(normalized_chars).split()


def _normalized_lower_text(text: str) -> str:
    return " ".join(_tokenize_text(text))


def _text_has_any_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    normalized = _normalized_lower_text(text)
    padded = f" {normalized} "
    return any(f" {phrase} " in padded for phrase in phrases)


def _is_ungrounded_operational_narration(text: str) -> bool:
    if _text_has_any_phrase(text, _UNGROUNDED_OPERATIONAL_NARRATION_PHRASES):
        return True
    tokens = _tokenize_text(text)
    for index, token in enumerate(tokens):
        if token != "approve":
            continue
        next_tokens = tokens[index + 1 : index + 5]
        if "tool" in next_tokens:
            return True
    return False


def _is_tool_success_claim(text: str) -> bool:
    normalized = _normalized_lower_text(text)
    if "completed successfully" in normalized:
        return True
    success_index = normalized.find("successfully")
    if success_index < 0:
        return False
    for verb in ("executed", "executing", "ran"):
        verb_index = normalized.find(verb)
        if verb_index >= 0 and verb_index < success_index:
            return True
    return False


def _is_execution_state_narration(text: str) -> bool:
    return _text_has_any_phrase(text, _EXECUTION_STATE_NARRATION_PHRASES)


def _extract_tool_name_from_segment(segment: str, known_tool_names: set[str]) -> str | None:
    tokens = _tokenize_text(segment)
    for token in tokens:
        if token in known_tool_names:
            return token
    for token in tokens:
        if "_" in token and token and token[0].isalpha():
            return token
    return None


def _negative_tool_status_claims(text: str, known_tool_names: set[str]) -> list[str]:
    claims: list[str] = []
    tokens = _tokenize_text(text)
    for index in range(len(tokens) - 7):
        if tokens[index : index + 3] != ["i", "haven't", "attempted"]:
            continue
        tool_name = tokens[index + 3]
        if tokens[index + 4 : index + 8] != ["in", "this", "turn", "yet"]:
            continue
        if tool_name and tool_name[0].isalpha():
            claims.append(tool_name)

    for raw_line in text.splitlines():
        line = raw_line.strip().lstrip("- ").strip()
        if not line:
            continue
        if "—" in line:
            left, right = line.split("—", 1)
        elif " - " in line:
            left, right = line.split(" - ", 1)
        else:
            continue
        status_text = " ".join(_tokenize_text(right))
        if not any(status_text.startswith(marker) for marker in _NEGATIVE_TOOL_STATUS_MARKERS):
            continue
        tool_name = _extract_tool_name_from_segment(left, known_tool_names)
        if tool_name is not None:
            claims.append(tool_name)
    return claims


_UNGROUNDED_APPROVAL_CLAIM_REPLY = (
    "I haven't triggered a real approval prompt yet. "
    "I can draft the exact curl command and the text-file step, or I can attempt the actual command path so Nullion surfaces a real approval request."
)


def text_mentions_approval_claim(text: str | None) -> bool:
    return isinstance(text, str) and _text_has_any_phrase(text, _UNGROUNDED_APPROVAL_CLAIM_PHRASES)
_UNGROUNDED_OPERATIONAL_NARRATION_REPLY = (
    "I haven't actually run that tool path in this turn yet. "
    "If you want, I can try it now or draft the exact next step first."
)
_UNGROUNDED_TOOL_SUCCESS_REPLY = (
    "I haven't actually run a tool for that in this turn yet. "
    "I can attempt it, or I can draft the exact command first."
)
_COMPLETED_TOOL_WITH_STALE_APPROVAL_REPLY = "Done — I attached the requested file."
_MISMATCHED_COMPLETED_TOOL_REPLY = (
    "I haven't fetched or attached the requested page in this turn yet. "
    "If you want, I can try the actual fetch now or draft the exact next step first."
)
_LIVE_INFORMATION_RESOLUTION_VALUES = frozenset(
    resolution.value for resolution in actionable_live_information_resolutions()
)
_EXECUTION_STATE_PLUGIN_PATH_REPLY = "Execution state: preferred plugin path."
_EXECUTION_STATE_CORE_FALLBACK_REPLY = "Execution state: core fallback path."
_EXECUTION_STATE_APPROVAL_REQUIRED_REPLY = "Execution state: approval required."
_EXECUTION_STATE_NO_USEFUL_RESULT_REPLY = "Execution state: no useful result."
_EXECUTION_STATE_BLOCKED_REPLY = "Execution state: blocked."
_EXECUTION_STATE_IN_PROGRESS_REPLY = "Execution state: in progress."
_EXECUTION_STATE_UNKNOWN_REPLY = "Execution state: unknown."

ApprovalLookup = Callable[[str], Any | None]


def _has_pending_approval_fact(state: ChatTurnStateSnapshot) -> bool:
    return any(fact.kind is OperationalFactKind.APPROVAL_REQUEST_PENDING for fact in state.facts)


def _pending_approval_reply(state: ChatTurnStateSnapshot) -> str | None:
    for fact in state.facts:
        if fact.kind is not OperationalFactKind.APPROVAL_REQUEST_PENDING:
            continue
        approval_id = fact.payload.get("approval_id")
        if not isinstance(approval_id, str) or not approval_id:
            continue
        tool_name = fact.payload.get("tool_name") or fact.payload.get("resource")
        display_label = fact.payload.get("display_label")
        display_detail = fact.payload.get("display_detail")
        lines = ["Approval required before Nullion can continue."]
        if isinstance(display_label, str) and display_label:
            lines.append(f"Action: {display_label}")
        elif isinstance(tool_name, str) and tool_name:
            lines.append(f"Tool: {tool_name}")
        if isinstance(display_detail, str) and display_detail:
            lines.append(f"Detail: {display_detail}")
        lines.append(f"Approval ID: {approval_id}")
        action = fact.payload.get("action")
        if action == "allow_boundary":
            label = web_session_allow_duration_label(load_settings().web_session_allow_duration)
            lines.append(f"Reply /approve {approval_id} to allow all web domains ({label}).")
            lines.append(f"Reply /deny {approval_id} to stop.")
        return "\n".join(lines)
    return None


def build_pending_approval_facts_from_tool_results(
    tool_results: list[ToolResult] | tuple[ToolResult, ...],
    *,
    approval_lookup: ApprovalLookup,
) -> tuple[tuple[OperationalFact, ...], tuple[str, ...]]:
    facts: list[OperationalFact] = []
    pending_approval_ids: list[str] = []
    seen_approval_ids: set[str] = set()
    for result in tool_results:
        normalized_status = normalize_tool_status(result.status)
        if normalized_status != "denied":
            continue
        if result.output.get("reason") != "approval_required":
            continue
        approval_id = result.output.get("approval_id")
        if not isinstance(approval_id, str) or not approval_id or approval_id in seen_approval_ids:
            continue
        approval = approval_lookup(approval_id)
        if approval is None or approval.status.value != "pending":
            continue
        display = approval_display_from_request(approval)
        seen_approval_ids.add(approval_id)
        pending_approval_ids.append(approval_id)
        facts.append(
            OperationalFact(
                fact_id=f"approval:{approval.approval_id}",
                kind=OperationalFactKind.APPROVAL_REQUEST_PENDING,
                payload={
                    "approval_id": approval.approval_id,
                    "requested_by": approval.requested_by,
                    "action": approval.action,
                    "resource": approval.resource,
                    "tool_name": result.tool_name,
                    "display_label": display.label,
                    "display_detail": display.detail,
                    "display_title": display.title,
                    "status": normalized_status,
                    "raw_status": result.status,
                },
            )
        )
    return tuple(facts), tuple(pending_approval_ids)


def build_tool_execution_facts_from_tool_results(
    tool_results: list[ToolResult] | tuple[ToolResult, ...],
) -> tuple[OperationalFact, ...]:
    facts: list[OperationalFact] = []
    for result in tool_results:
        normalized_status = normalize_tool_status(result.status)
        base_payload = {
            "tool_name": result.tool_name,
            "invocation_id": result.invocation_id,
            "status": normalized_status,
            "raw_status": result.status,
        }
        facts.append(
            OperationalFact(
                fact_id=f"tool-attempted:{result.invocation_id}",
                kind=OperationalFactKind.TOOL_ATTEMPTED,
                payload=base_payload,
            )
        )
        if normalized_status == "completed":
            facts.append(
                OperationalFact(
                    fact_id=f"tool-completed:{result.invocation_id}",
                    kind=OperationalFactKind.TOOL_COMPLETED,
                    payload=base_payload | {"output": result.output},
                )
            )
        elif normalized_status in {"failed", "denied"}:
            facts.append(
                OperationalFact(
                    fact_id=f"tool-failed:{result.invocation_id}",
                    kind=OperationalFactKind.TOOL_FAILED,
                    payload=base_payload | {"error": result.error, "output": result.output},
                )
            )
    return tuple(facts)


def build_live_information_resolution_facts(
    resolutions: list[str] | tuple[str, ...],
) -> tuple[OperationalFact, ...]:
    facts: list[OperationalFact] = []
    for index, resolution in enumerate(resolutions):
        if resolution not in _LIVE_INFORMATION_RESOLUTION_VALUES:
            continue
        facts.append(
            OperationalFact(
                fact_id=f"live-information-resolution:{index}",
                kind=OperationalFactKind.LIVE_INFORMATION_RESOLUTION,
                payload={"resolution": resolution},
            )
        )
    return tuple(facts)


def _tool_failure_reply(state: ChatTurnStateSnapshot, *, text: str | None = None) -> str | None:
    for fact in state.facts:
        if fact.kind is not OperationalFactKind.TOOL_FAILED:
            continue
        tool_name = fact.payload.get("tool_name")
        if isinstance(text, str) and isinstance(tool_name, str) and tool_name and tool_name.lower() not in text.lower():
            continue
        if not isinstance(tool_name, str) or not tool_name:
            tool_name = "that tool"
        error = fact.payload.get("error")
        lines = [f"I attempted {tool_name} in this turn, but it did not complete successfully."]
        if isinstance(error, str) and error:
            lines.append(f"Error: {error}")
        return "\n".join(lines)
    return None


def _has_tool_attempt_fact(state: ChatTurnStateSnapshot) -> bool:
    return any(fact.kind is OperationalFactKind.TOOL_ATTEMPTED for fact in state.facts)


def _has_tool_completed_fact(state: ChatTurnStateSnapshot) -> bool:
    return any(fact.kind is OperationalFactKind.TOOL_COMPLETED for fact in state.facts)


def _has_live_information_resolution_fact(state: ChatTurnStateSnapshot) -> bool:
    return any(fact.kind is OperationalFactKind.LIVE_INFORMATION_RESOLUTION for fact in state.facts)


def _has_tool_completed_fact_for_name(state: ChatTurnStateSnapshot, tool_name: str) -> bool:
    tool_name_lower = tool_name.lower()
    return any(
        fact.kind is OperationalFactKind.TOOL_COMPLETED
        and isinstance(fact.payload.get("tool_name"), str)
        and fact.payload["tool_name"].lower() == tool_name_lower
        for fact in state.facts
    )


def _latest_tool_attempt(state: ChatTurnStateSnapshot) -> Mapping[str, Any] | None:
    for fact in reversed(state.facts):
        if fact.kind is OperationalFactKind.TOOL_ATTEMPTED:
            return fact.payload
    return None


def _latest_live_information_resolution_fact(state: ChatTurnStateSnapshot) -> Mapping[str, Any] | None:
    for fact in reversed(state.facts):
        if fact.kind is OperationalFactKind.LIVE_INFORMATION_RESOLUTION:
            return fact.payload
    return None


def _tool_failed_fact_for_invocation(state: ChatTurnStateSnapshot, invocation_id: str) -> Mapping[str, Any] | None:
    for fact in state.facts:
        if fact.kind is not OperationalFactKind.TOOL_FAILED:
            continue
        if fact.payload.get("invocation_id") == invocation_id:
            return fact.payload
    return None


def _tool_completed_fact_for_invocation(state: ChatTurnStateSnapshot, invocation_id: str) -> Mapping[str, Any] | None:
    for fact in state.facts:
        if fact.kind is not OperationalFactKind.TOOL_COMPLETED:
            continue
        if fact.payload.get("invocation_id") == invocation_id:
            return fact.payload
    return None


def _primary_tool_outcome_failure_reply(state: ChatTurnStateSnapshot) -> str | None:
    latest_attempt = _latest_tool_attempt(state)
    if latest_attempt is None:
        return None
    invocation_id = latest_attempt.get("invocation_id")
    if not isinstance(invocation_id, str) or not invocation_id:
        return None
    failed_payload = _tool_failed_fact_for_invocation(state, invocation_id)
    if failed_payload is None:
        return None
    tool_name = failed_payload.get("tool_name")
    if not isinstance(tool_name, str) or not tool_name:
        tool_name = "that tool"
    error = failed_payload.get("error")
    lines = [f"I attempted {tool_name} in this turn, but it did not complete successfully."]
    if isinstance(error, str) and error:
        lines.append(f"Error: {error}")
    return "\n".join(lines)


def _primary_tool_completed(state: ChatTurnStateSnapshot) -> bool:
    latest_attempt = _latest_tool_attempt(state)
    if latest_attempt is None:
        return False
    invocation_id = latest_attempt.get("invocation_id")
    if not isinstance(invocation_id, str) or not invocation_id:
        return False
    return _tool_completed_fact_for_invocation(state, invocation_id) is not None


def _draft_media_suffix(text: str) -> str:
    media_lines = [line for line in text.splitlines() if line.strip().startswith("MEDIA:")]
    if not media_lines:
        return ""
    return "\n\n" + "\n".join(media_lines)


def _completed_tool_reply_with_preserved_media(text: str) -> str:
    return _COMPLETED_TOOL_WITH_STALE_APPROVAL_REPLY + _draft_media_suffix(text)



def _latest_completed_tool_fact(state: ChatTurnStateSnapshot) -> Mapping[str, Any] | None:
    for fact in reversed(state.facts):
        if fact.kind is OperationalFactKind.TOOL_COMPLETED:
            return fact.payload
    return None



def _claimed_tool_name_from_text(text: str) -> str | None:
    normalized = text.replace("*", "")
    match = re.search(r"tool:\s*`?([a-z][a-z0-9_]+)`?", normalized, re.IGNORECASE)
    if match is None:
        return None
    return match.group(1).lower()



def _completed_tool_reply(state: ChatTurnStateSnapshot, *, text: str) -> str | None:
    completed_payload = _latest_completed_tool_fact(state)
    if completed_payload is None:
        return None
    tool_name = completed_payload.get("tool_name")
    if not isinstance(tool_name, str) or not tool_name:
        tool_name = "that tool"
    claimed_tool_name = _claimed_tool_name_from_text(text)
    if claimed_tool_name and tool_name.lower() != claimed_tool_name and not _draft_media_suffix(text):
        return _MISMATCHED_COMPLETED_TOOL_REPLY
    if _draft_media_suffix(text):
        return _completed_tool_reply_with_preserved_media(text)
    return f"I completed {tool_name} in this turn."



def _draft_conflicts_with_completed_tool(text: str) -> bool:
    return text_mentions_approval_claim(text) or _is_ungrounded_operational_narration(text)


def _tool_names_for_fact_kind(state: ChatTurnStateSnapshot, kind: OperationalFactKind) -> set[str]:
    names: set[str] = set()
    for fact in state.facts:
        if fact.kind is not kind:
            continue
        tool_name = fact.payload.get("tool_name")
        if isinstance(tool_name, str) and tool_name:
            names.add(tool_name.lower())
    return names


def _pending_approval_tool_names(state: ChatTurnStateSnapshot) -> set[str]:
    names: set[str] = set()
    for fact in state.facts:
        if fact.kind is not OperationalFactKind.APPROVAL_REQUEST_PENDING:
            continue
        for key in ("tool_name", "resource"):
            tool_name = fact.payload.get(key)
            if isinstance(tool_name, str) and tool_name:
                names.add(tool_name.lower())
    return names


def _unsupported_negative_tool_claim_reply(state: ChatTurnStateSnapshot, text: str) -> str | None:
    completed_tools = _tool_names_for_fact_kind(state, OperationalFactKind.TOOL_COMPLETED)
    failed_tools = _tool_names_for_fact_kind(state, OperationalFactKind.TOOL_FAILED)
    attempted_tools = _tool_names_for_fact_kind(state, OperationalFactKind.TOOL_ATTEMPTED)
    pending_tools = _pending_approval_tool_names(state)
    known_tools = completed_tools | failed_tools | attempted_tools | pending_tools
    completed_claims: list[str] = []
    unattempted_claims: list[str] = []
    seen: set[tuple[str, str]] = set()
    for tool_name in _negative_tool_status_claims(text, known_tools):
        if tool_name in failed_tools or tool_name in pending_tools:
            continue
        if (tool_name, "completed") in seen or (tool_name, "unattempted") in seen:
            continue
        if tool_name in completed_tools:
            completed_claims.append(tool_name)
            seen.add((tool_name, "completed"))
            continue
        if tool_name not in attempted_tools:
            unattempted_claims.append(tool_name)
            seen.add((tool_name, "unattempted"))
    if not completed_claims and not unattempted_claims:
        return None
    if unattempted_claims and failed_tools and not completed_claims:
        return _tool_failure_reply(state)
    lines = [f"I completed {tool_name} in this turn." for tool_name in completed_claims]
    lines.extend(f"I haven't attempted {tool_name} in this turn yet." for tool_name in unattempted_claims)
    return "\n".join(lines) + _draft_media_suffix(text)


def _execution_state_reply(state: ChatTurnStateSnapshot) -> str | None:
    latest_attempt = _latest_tool_attempt(state)
    if latest_attempt is not None:
        invocation_id = latest_attempt.get("invocation_id")
        if not isinstance(invocation_id, str) or not invocation_id:
            return None

        completed_payload = _tool_completed_fact_for_invocation(state, invocation_id)
        if completed_payload is not None:
            tool_name = completed_payload.get("tool_name")
            if isinstance(tool_name, str) and tool_name in system_context.CORE_FALLBACK_TOOL_NAMES:
                return _EXECUTION_STATE_CORE_FALLBACK_REPLY
            return _EXECUTION_STATE_PLUGIN_PATH_REPLY

        failed_payload = _tool_failed_fact_for_invocation(state, invocation_id)
        if failed_payload is not None:
            if failed_payload.get("status") != "denied":
                return None
            output = failed_payload.get("output")
            reason: str | None = None
            if isinstance(output, Mapping):
                reason_value = output.get("reason")
                if isinstance(reason_value, str) and reason_value:
                    reason = reason_value
            lines = [
                _EXECUTION_STATE_APPROVAL_REQUIRED_REPLY if reason == "approval_required" else _EXECUTION_STATE_BLOCKED_REPLY
            ]
            if reason:
                lines.append(f"Reason: {reason}")
            return "\n".join(lines)

        status = latest_attempt.get("status")
        if status == "nonterminal":
            return _EXECUTION_STATE_IN_PROGRESS_REPLY
        if status == "unknown":
            return _EXECUTION_STATE_UNKNOWN_REPLY

    resolution_payload = _latest_live_information_resolution_fact(state)
    if resolution_payload is None:
        return None
    resolution = resolution_payload.get("resolution")
    label = format_live_information_resolution_label(resolution)
    if label is None:
        return None
    if resolution == LiveInformationResolution.PREFERRED_PLUGIN_PATH.value:
        return _EXECUTION_STATE_PLUGIN_PATH_REPLY
    if resolution == LiveInformationResolution.CORE_FALLBACK.value:
        return _EXECUTION_STATE_CORE_FALLBACK_REPLY
    if resolution == LiveInformationResolution.APPROVAL_REQUIRED.value:
        return _EXECUTION_STATE_APPROVAL_REQUIRED_REPLY
    if resolution == LiveInformationResolution.NO_USEFUL_RESULT.value:
        return _EXECUTION_STATE_NO_USEFUL_RESULT_REPLY
    if resolution == LiveInformationResolution.BLOCKED.value:
        return _EXECUTION_STATE_BLOCKED_REPLY
    return f"Execution state: {label}."


def is_canonical_deferred_runtime_offer_reply(reply: str | None) -> bool:
    if not isinstance(reply, str):
        return False
    return reply in {
        _UNGROUNDED_APPROVAL_CLAIM_REPLY,
        _UNGROUNDED_OPERATIONAL_NARRATION_REPLY,
        _UNGROUNDED_TOOL_SUCCESS_REPLY,
    }



def render_chat_response_for_telegram(contract: ChatResponseContract) -> str:
    approval_reply = _pending_approval_reply(contract.state)
    if approval_reply is not None:
        return approval_reply
    text = contract.draft.text
    if text_mentions_approval_claim(text) and not _has_pending_approval_fact(contract.state):
        if _has_tool_completed_fact(contract.state):
            completed_reply = _completed_tool_reply(contract.state, text=text)
            if completed_reply is not None:
                return completed_reply
        return _UNGROUNDED_APPROVAL_CLAIM_REPLY
    if (
        not _has_pending_approval_fact(contract.state)
        and not _has_tool_attempt_fact(contract.state)
        and not _has_live_information_resolution_fact(contract.state)
    ):
        if _is_ungrounded_operational_narration(text):
            return _UNGROUNDED_OPERATIONAL_NARRATION_REPLY
    if _is_execution_state_narration(text):
        execution_state_reply = _execution_state_reply(contract.state)
        if execution_state_reply is not None:
            return execution_state_reply
    if _has_tool_attempt_fact(contract.state):
        unsupported_negative_tool_claim_reply = _unsupported_negative_tool_claim_reply(contract.state, text)
        if unsupported_negative_tool_claim_reply is not None:
            return unsupported_negative_tool_claim_reply
        named_failure_reply = _tool_failure_reply(contract.state, text=text)
        if named_failure_reply is not None:
            for fact in contract.state.facts:
                if fact.kind is not OperationalFactKind.TOOL_FAILED:
                    continue
                tool_name = fact.payload.get("tool_name")
                if isinstance(tool_name, str) and tool_name and tool_name.lower() in text.lower():
                    if not _has_tool_completed_fact_for_name(contract.state, tool_name):
                        return named_failure_reply
                    break
        primary_failure_reply = _primary_tool_outcome_failure_reply(contract.state)
        if primary_failure_reply is not None:
            return primary_failure_reply
        if _primary_tool_completed(contract.state):
            if not _has_pending_approval_fact(contract.state) and _draft_conflicts_with_completed_tool(text):
                return _completed_tool_reply_with_preserved_media(text)
            return text
        failure_reply = _tool_failure_reply(contract.state)
        if failure_reply is not None and not _has_tool_completed_fact(contract.state):
            return failure_reply
    if _is_tool_success_claim(text):
        return _UNGROUNDED_TOOL_SUCCESS_REPLY
    return text


__all__ = [
    "ChatResponseContract",
    "ChatTurnStateSnapshot",
    "ContextLinkMode",
    "ModelDraftResponse",
    "OperationalFact",
    "OperationalFactKind",
    "build_pending_approval_facts_from_tool_results",
    "build_live_information_resolution_facts",
    "build_tool_execution_facts_from_tool_results",
    "is_canonical_deferred_runtime_offer_reply",
    "render_chat_response_for_telegram",
]

"""Per-turn tool overlay for current workspace history lookup."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import math
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import urlparse

from nullion.tools import ToolInvocation, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


CHAT_HISTORY_SEARCH_TOOL_NAME = "chat_history_search"
_MAX_HISTORY_SCAN_LIMIT = 200
_MAX_HISTORY_RETURN_LIMIT = 50
_MAX_SNIPPET_CHARS = 900
_MAX_TOOL_EVIDENCE_ITEMS = 4
_MAX_TOOL_EVIDENCE_OUTPUT_CHARS = 1600
_MIN_SEARCH_TOKEN_CHARS = 2
_MATCH_CONTEXT_PREVIOUS_TURNS = 4
_MATCH_CONTEXT_FOLLOWING_TURNS = 1
_MAX_STRUCTURED_REFERENCES = 8
_URL_RE = re.compile(r"https?://[^\s<>()\[\]{}\"']+")


CHAT_HISTORY_SEARCH_TOOL_SPEC = ToolSpec(
    name=CHAT_HISTORY_SEARCH_TOOL_NAME,
    description=(
        "Search or inspect the current workspace's saved chat history, including turns "
        "older than the visible prompt context. Use this before telling the user that recent "
        "chat details are unavailable when the answer may be in this workspace."
    ),
    risk_level=ToolRiskLevel.LOW,
    side_effect_class=ToolSideEffectClass.READ,
    requires_approval=False,
    timeout_seconds=2,
    input_schema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Optional search text. Leave empty to return the most recent saved turns.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum turns to return.",
                "minimum": 1,
                "maximum": _MAX_HISTORY_RETURN_LIMIT,
            },
        },
        "additionalProperties": False,
    },
    capability_tags=("conversation_history", "account_read"),
)


def _coerce_limit(value: object, *, default: int = 20) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        limit = default
    return max(1, min(_MAX_HISTORY_RETURN_LIMIT, limit))


def _parse_created_at(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return datetime.min.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)


def _snippet(value: object, *, max_chars: int = _MAX_SNIPPET_CHARS) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "..."


def _normalized_url(raw_url: str) -> str | None:
    text = str(raw_url or "").strip().rstrip(".,;:!?)]}")
    if not text:
        return None
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return text


def _structured_references_from_text(text: object) -> list[dict[str, object]]:
    refs: list[dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    for raw_url in _URL_RE.findall(str(text or "")):
        url = _normalized_url(raw_url)
        if url is None:
            continue
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        ref: dict[str, object] = {"type": "url", "domain": domain, "url": url}
        identity = tuple(sorted(ref.items()))
        if identity in seen:
            continue
        refs.append(ref)
        seen.add(identity)
        if len(refs) >= _MAX_STRUCTURED_REFERENCES:
            break
    return refs


def _structured_references_from_event(event: dict[str, object]) -> list[dict[str, object]]:
    refs: list[dict[str, object]] = []
    seen: set[tuple[object, ...]] = set()
    for source in (
        event.get("user_message"),
        event.get("assistant_reply"),
    ):
        for ref in _structured_references_from_text(source):
            identity = tuple(sorted(ref.items()))
            if identity in seen:
                continue
            refs.append(ref)
            seen.add(identity)
            if len(refs) >= _MAX_STRUCTURED_REFERENCES:
                return refs
    return refs


def _tokenize(value: object) -> tuple[str, ...]:
    tokens: list[str] = []
    for token in re.findall(r"\w+", str(value or "")):
        normalized = token.lower()
        if len(normalized) < _MIN_SEARCH_TOKEN_CHARS:
            continue
        tokens.append(normalized)
    return tuple(dict.fromkeys(tokens))


def _event_text(event: dict[str, object]) -> str:
    return "\n".join(
        part
        for part in (
            str(event.get("user_message") or ""),
            str(event.get("assistant_reply") or ""),
        )
        if part.strip()
    )


def _event_has_history_search_tool_result(event: dict[str, object]) -> bool:
    tool_results = event.get("tool_results")
    if not isinstance(tool_results, list):
        return False
    for result in tool_results:
        if not isinstance(result, dict):
            continue
        if str(result.get("tool_name") or "") == CHAT_HISTORY_SEARCH_TOOL_NAME:
            return True
    return False


def _event_has_richer_runtime_metadata(event: dict[str, object]) -> bool:
    return bool(
        _event_has_history_search_tool_result(event)
        or event.get("tool_results")
        or event.get("branch_id")
        or event.get("parent_turn_id")
    )


def _tool_names_from_event(event: dict[str, object]) -> list[str]:
    tool_results = event.get("tool_results")
    if not isinstance(tool_results, list):
        return []
    names: list[str] = []
    seen: set[str] = set()
    for result in tool_results:
        if not isinstance(result, dict):
            continue
        name = str(result.get("tool_name") or "").strip()
        if not name or name in seen:
            continue
        names.append(name)
        seen.add(name)
    return names


def _compact_tool_evidence_output(value: object) -> str:
    try:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except TypeError:
        encoded = str(value)
    return _snippet(encoded, max_chars=_MAX_TOOL_EVIDENCE_OUTPUT_CHARS)


def _tool_evidence_from_event(event: dict[str, object]) -> list[dict[str, object]]:
    tool_results = event.get("tool_results")
    if not isinstance(tool_results, list):
        return []
    evidence: list[dict[str, object]] = []
    for result in tool_results:
        if not isinstance(result, dict):
            continue
        name = str(result.get("tool_name") or "").strip()
        if not name or name in {CHAT_HISTORY_SEARCH_TOOL_NAME, "request_tool_scope"}:
            continue
        item: dict[str, object] = {
            "tool_name": name,
            "status": str(result.get("status") or "").strip(),
        }
        output = result.get("output")
        if output not in (None, "", {}, []):
            item["output_preview"] = _compact_tool_evidence_output(output)
        error = str(result.get("error") or "").strip()
        if error:
            item["error"] = _snippet(error, max_chars=500)
        evidence.append(item)
        if len(evidence) >= _MAX_TOOL_EVIDENCE_ITEMS:
            break
    return evidence


def _event_record(event: dict[str, object], *, index: int) -> dict[str, object]:
    record = {
        "index": index,
        "conversation_id": str(event.get("conversation_id") or ""),
        "created_at": str(event.get("created_at") or ""),
        "turn_id": str(event.get("turn_id") or ""),
        "user_message": _snippet(event.get("user_message")),
        "assistant_reply": _snippet(event.get("assistant_reply")),
    }
    workspace_id = str(event.get("workspace_id") or "").strip()
    if workspace_id:
        record["workspace_id"] = workspace_id
    score = event.get("_history_match_score")
    if isinstance(score, int):
        record["match_score"] = score
    context = event.get("_history_context")
    if isinstance(context, str) and context.strip():
        record["context"] = context.strip()
    structured_refs = _structured_references_from_event(event)
    if structured_refs:
        record["structured_refs"] = structured_refs
    tool_names = _tool_names_from_event(event)
    if tool_names:
        record["tool_names"] = tool_names
    tool_evidence = _tool_evidence_from_event(event)
    if tool_evidence:
        record["tool_evidence"] = tool_evidence
    return record


def _workspace_id_for_conversation(conversation_id: str | None) -> str:
    try:
        from nullion.connections import workspace_id_for_principal

        return str(workspace_id_for_principal(conversation_id)).strip()
    except Exception:
        return "workspace_admin"


def _event_workspace_id(event: dict[str, object]) -> str:
    workspace_id = str(event.get("workspace_id") or "").strip()
    if workspace_id:
        return workspace_id
    context = event.get("context")
    if isinstance(context, dict):
        workspace_id = str(context.get("workspace_id") or "").strip()
        if workspace_id:
            return workspace_id
    return _workspace_id_for_conversation(str(event.get("conversation_id") or ""))


def _event_matches_workspace(event: dict[str, object], *, workspace_id: str) -> bool:
    expected = str(workspace_id or "").strip()
    if not expected:
        return True
    observed = _event_workspace_id(event)
    return not observed or observed == expected


def _chat_store_history_turns(
    runtime: object,
    conversation_id: str,
    *,
    scan_limit: int | None,
    workspace_id: str,
) -> list[dict[str, object]]:
    if not conversation_id:
        return []
    try:
        checkpoint_path = Path(getattr(runtime, "checkpoint_path")).expanduser()
    except (TypeError, ValueError):
        return []
    home = checkpoint_path.parent
    db_path = home / "chat_history.db"
    key_path = home / "chat_history.key"
    if not db_path.is_file():
        return []
    try:
        from nullion.chat_store import ChatStore

        chat_store = ChatStore(db_path=db_path, key_path=key_path)
        conversation_ids = _workspace_chat_store_conversation_ids(
            chat_store,
            conversation_id=conversation_id,
            workspace_id=workspace_id,
        )
        messages_by_conversation: list[tuple[str, list[dict[str, object]]]] = []
        for candidate_conversation_id in conversation_ids:
            message_limit = (
                max(1, scan_limit * 2)
                if scan_limit is not None
                else max(1, chat_store.message_count(candidate_conversation_id))
            )
            messages = chat_store.load_messages(
                candidate_conversation_id,
                limit=message_limit,
            )
            if isinstance(messages, list):
                messages_by_conversation.append((candidate_conversation_id, messages))
    except Exception:
        return []
    turns: list[dict[str, object]] = []
    for candidate_conversation_id, messages in messages_by_conversation:
        pending_user: dict[str, object] | None = None
        for message in messages:
            if not isinstance(message, dict):
                continue
            role = str(message.get("role") or "").strip().lower()
            text = str(message.get("text") or "").strip()
            if not text:
                continue
            created_at = str(message.get("created_at") or "").strip()
            message_id = str(message.get("id") or "").strip()
            if role == "user":
                if pending_user is not None:
                    turns.append(
                        {
                            "conversation_id": candidate_conversation_id,
                            "event_type": "conversation.chat_turn",
                            "created_at": str(pending_user.get("created_at") or created_at),
                            "turn_id": f"chat-store:{pending_user.get('id') or len(turns)}",
                            "user_message": str(pending_user.get("text") or ""),
                            "assistant_reply": "",
                            "workspace_id": workspace_id,
                            "source": "chat_history",
                        }
                    )
                pending_user = {"id": message_id, "text": text, "created_at": created_at}
                continue
            if role not in {"assistant", "bot"}:
                continue
            if pending_user is not None:
                turns.append(
                    {
                        "conversation_id": candidate_conversation_id,
                        "event_type": "conversation.chat_turn",
                        "created_at": created_at or str(pending_user.get("created_at") or ""),
                        "turn_id": f"chat-store:{pending_user.get('id') or len(turns)}:{message_id or len(turns)}",
                        "user_message": str(pending_user.get("text") or ""),
                        "assistant_reply": text,
                        "workspace_id": workspace_id,
                        "source": "chat_history",
                    }
                )
                pending_user = None
            else:
                turns.append(
                    {
                        "conversation_id": candidate_conversation_id,
                        "event_type": "conversation.chat_turn",
                        "created_at": created_at,
                        "turn_id": f"chat-store:{message_id or len(turns)}",
                        "user_message": "",
                        "assistant_reply": text,
                        "workspace_id": workspace_id,
                        "source": "chat_history",
                    }
                )
        if pending_user is not None:
            turns.append(
                {
                    "conversation_id": candidate_conversation_id,
                    "event_type": "conversation.chat_turn",
                    "created_at": str(pending_user.get("created_at") or ""),
                    "turn_id": f"chat-store:{pending_user.get('id') or len(turns)}",
                    "user_message": str(pending_user.get("text") or ""),
                    "assistant_reply": "",
                    "workspace_id": workspace_id,
                    "source": "chat_history",
                }
            )
    turns.sort(key=lambda event: _parse_created_at(event.get("created_at")))
    if scan_limit is not None:
        return turns[-scan_limit:]
    return turns


def _workspace_chat_store_conversation_ids(
    chat_store: object,
    *,
    conversation_id: str,
    workspace_id: str,
) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()

    def add_candidate(candidate: object) -> None:
        candidate_id = str(candidate or "").strip()
        if not candidate_id or candidate_id in seen:
            return
        if workspace_id and _workspace_id_for_conversation(candidate_id) != workspace_id:
            return
        selected.append(candidate_id)
        seen.add(candidate_id)

    list_conversations = getattr(chat_store, "list_conversations", None)
    if callable(list_conversations):
        for status in ("active", "archived"):
            try:
                conversations = list_conversations(status=status, limit=1000)
            except Exception:
                conversations = []
            for conversation in conversations if isinstance(conversations, list) else []:
                if isinstance(conversation, dict):
                    add_candidate(conversation.get("id"))

    add_candidate(conversation_id)
    return selected


def _dedupe_history_events(
    events: Iterable[dict[str, object]],
    *,
    scan_limit: int | None,
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    selected_indexes: dict[tuple[str, str], int] = {}
    for event in sorted(
        (dict(item) for item in events if isinstance(item, dict)),
        key=lambda item: _parse_created_at(item.get("created_at")),
    ):
        identity = (
            str(event.get("conversation_id") or ""),
            " ".join(str(event.get("user_message") or "").split()),
            " ".join(str(event.get("assistant_reply") or "").split()),
        )
        selected_index = selected_indexes.get(identity)
        if selected_index is not None:
            existing = selected[selected_index]
            if not _event_has_richer_runtime_metadata(existing) and _event_has_richer_runtime_metadata(event):
                selected[selected_index] = event
            continue
        selected_indexes[identity] = len(selected)
        selected.append(event)
    if scan_limit is not None:
        return selected[-scan_limit:]
    return selected


def _conversation_events_after_reset(
    runtime: object,
    store: object,
    conversation_id: str,
    *,
    full_history: bool,
    workspace_id: str,
) -> list[dict[str, object]]:
    if not conversation_id:
        return []
    event_history: list[dict[str, object]] = []
    scan_limit = None if full_history else _MAX_HISTORY_SCAN_LIMIT
    if callable(getattr(store, "list_conversation_events", None)):
        list_events = getattr(store, "list_conversation_events")
        try:
            events = list_events()
        except Exception:
            try:
                events = list_events(conversation_id)
            except Exception:
                events = []
        event_history = _workspace_chat_turns_after_resets(
            events if isinstance(events, list) else [],
            workspace_id=workspace_id,
            scan_limit=scan_limit,
        )
    if not event_history:
        list_after_reset = getattr(store, "list_recent_conversation_events_after_reset", None)
        if callable(list_after_reset):
            try:
                events = list_after_reset(
                    conversation_id,
                    event_type="conversation.chat_turn",
                    limit=_MAX_HISTORY_SCAN_LIMIT,
                )
                if isinstance(events, list):
                    event_history = [
                        dict(event)
                        for event in events
                        if isinstance(event, dict) and _event_matches_workspace(event, workspace_id=workspace_id)
                    ]
            except Exception:
                pass
        if not event_history and callable(getattr(store, "list_recent_conversation_events", None)):
            list_recent = getattr(store, "list_recent_conversation_events")
            try:
                events = list_recent(
                    conversation_id,
                    event_type="conversation.chat_turn",
                    limit=_MAX_HISTORY_SCAN_LIMIT,
                )
                if isinstance(events, list):
                    event_history = [
                        dict(event)
                        for event in events
                        if isinstance(event, dict) and _event_matches_workspace(event, workspace_id=workspace_id)
                    ]
            except Exception:
                pass
    return _dedupe_history_events(
        [
            *event_history,
            *_chat_store_history_turns(
                runtime,
                conversation_id,
                scan_limit=scan_limit,
                workspace_id=workspace_id,
            ),
        ],
        scan_limit=scan_limit,
    )


def _workspace_chat_turns_after_resets(
    events: Iterable[dict[str, object]],
    *,
    workspace_id: str,
    scan_limit: int | None,
) -> list[dict[str, object]]:
    selected_by_conversation: dict[str, list[dict[str, object]]] = {}
    for event in sorted(
        (dict(item) for item in events if isinstance(item, dict)),
        key=lambda item: _parse_created_at(item.get("created_at")),
    ):
        if not _event_matches_workspace(event, workspace_id=workspace_id):
            continue
        conversation_id = str(event.get("conversation_id") or "").strip()
        if not conversation_id:
            continue
        event_type = str(event.get("event_type") or "")
        if event_type == "conversation.session_reset":
            selected_by_conversation[conversation_id] = []
            continue
        if event_type != "conversation.chat_turn":
            continue
        selected_by_conversation.setdefault(conversation_id, []).append(event)
    selected = [
        event
        for conversation_events in selected_by_conversation.values()
        for event in conversation_events
    ]
    selected.sort(key=lambda event: _parse_created_at(event.get("created_at")))
    if scan_limit is not None:
        return selected[-scan_limit:]
    return selected


def _ranked_history_events(
    events: Iterable[dict[str, object]],
    *,
    query: str,
    limit: int,
    fallback_to_recent_on_no_match: bool,
) -> tuple[list[dict[str, object]], bool]:
    ordered = sorted(
        (dict(event) for event in events),
        key=lambda event: _parse_created_at(event.get("created_at")),
    )
    if not query.strip():
        return ordered[-limit:], False

    query_text = query.strip().lower()
    query_tokens = set(_tokenize(query_text))
    token_document_counts: dict[str, int] = {token: 0 for token in query_tokens}
    for event in ordered:
        event_tokens = set(_tokenize(_event_text(event).lower()))
        for token in query_tokens.intersection(event_tokens):
            token_document_counts[token] = token_document_counts.get(token, 0) + 1

    total_documents = max(1, len(ordered))

    def _token_weight(token: str) -> int:
        document_count = token_document_counts.get(token, 0)
        if document_count <= 0:
            return 0
        return max(1, min(16, int(round(math.log2((total_documents + 1) / (document_count + 1)) * 4))))

    scored: list[tuple[int, int, dict[str, object]]] = []
    direct_scores_by_index: dict[int, int] = {}
    for index, event in enumerate(ordered):
        haystack = _event_text(event).lower()
        if not haystack:
            continue
        user_tokens = set(_tokenize(str(event.get("user_message") or "").lower()))
        assistant_tokens = set(_tokenize(str(event.get("assistant_reply") or "").lower()))
        haystack_tokens = user_tokens.union(assistant_tokens)
        matched_tokens = query_tokens.intersection(haystack_tokens)
        score = sum(_token_weight(token) for token in matched_tokens)
        score += sum(
            max(1, _token_weight(token) * 2)
            for token in matched_tokens
            if token in user_tokens and token in assistant_tokens
        )
        if query_text and query_text in haystack:
            score += max(64, len(query_tokens) * 16)
        if score > 0 and _event_has_history_search_tool_result(event):
            score = max(1, score // 4)
        if score > 0:
            direct_scores_by_index[index] = score
            scored.append((score, index, event))
    if not scored and fallback_to_recent_on_no_match:
        return ordered[-limit:], True
    if not scored:
        return [], False
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    selected_by_index: dict[int, dict[str, object]] = {}

    def _select_context_event(index: int, *, score: int, context: str) -> None:
        if index < 0 or index >= len(ordered):
            return
        context_score = direct_scores_by_index.get(index, 0) or min(score, 2)
        existing = selected_by_index.get(index)
        existing_score = existing.get("_history_match_score") if existing else None
        if isinstance(existing_score, int) and existing_score >= context_score:
            return
        event = dict(ordered[index])
        event["_history_match_score"] = context_score
        event["_history_context"] = context
        selected_by_index[index] = event

    for score, index, event in scored:
        selected_event = dict(event)
        selected_event["_history_match_score"] = score
        existing_current = selected_by_index.get(index)
        existing_current_score = existing_current.get("_history_match_score") if existing_current else None
        if not isinstance(existing_current_score, int) or existing_current_score <= score:
            selected_by_index[index] = selected_event
        for offset in range(1, _MATCH_CONTEXT_PREVIOUS_TURNS + 1):
            context = "previous_turn" if offset == 1 else f"previous_turn_{offset}"
            _select_context_event(index - offset, score=score, context=context)
        for offset in range(1, _MATCH_CONTEXT_FOLLOWING_TURNS + 1):
            context = "following_turn" if offset == 1 else f"following_turn_{offset}"
            _select_context_event(index + offset, score=score, context=context)
        if len(selected_by_index) >= limit:
            break
    selected = list(selected_by_index.values())
    selected.sort(
        key=lambda event: (
            -int(event.get("_history_match_score") or 0),
            _parse_created_at(event.get("created_at")),
        )
    )
    return selected[:limit], False


def _ranked_structured_reference_events(
    events: Iterable[dict[str, object]],
    *,
    query: str,
    limit: int,
) -> list[dict[str, object]]:
    ordered = sorted(
        (dict(event) for event in events),
        key=lambda event: _parse_created_at(event.get("created_at")),
    )
    query_tokens = set(_tokenize(query.lower()))
    token_document_counts: dict[str, int] = {token: 0 for token in query_tokens}
    for event in ordered:
        event_tokens = set(_tokenize(_event_text(event).lower()))
        for token in query_tokens.intersection(event_tokens):
            token_document_counts[token] = token_document_counts.get(token, 0) + 1
    total_documents = max(1, len(ordered))

    def _token_weight(token: str) -> int:
        document_count = token_document_counts.get(token, 0)
        if document_count <= 0:
            return 0
        return max(1, min(16, int(round(math.log2((total_documents + 1) / (document_count + 1)) * 4))))

    scored: list[tuple[int, datetime, int, dict[str, object]]] = []
    for index, event in enumerate(ordered):
        if not _structured_references_from_event(event):
            continue
        event_tokens = set(_tokenize(_event_text(event).lower()))
        matched_tokens = query_tokens.intersection(event_tokens)
        if query_tokens and not matched_tokens:
            continue
        score = sum(_token_weight(token) for token in matched_tokens)
        if not query_tokens:
            score = 1
        if score <= 0:
            continue
        selected_event = dict(event)
        selected_event["_history_match_score"] = score
        selected_event["_history_context"] = "structured_reference"
        scored.append((score, _parse_created_at(event.get("created_at")), index, selected_event))
    scored.sort(key=lambda item: (item[0] // 16, item[1], item[0], item[2]), reverse=True)
    return [event for _score, _created, _index, event in scored[:limit]]


def _chat_history_search_result(
    *,
    runtime: object,
    conversation_id: str,
    invocation: ToolInvocation,
    workspace_id: str | None = None,
) -> ToolResult:
    store = getattr(runtime, "store", None)
    if store is None:
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="failed",
            output={"reason": "runtime_store_unavailable", "suppress_activity": True},
            error="Conversation history store is unavailable.",
        )
    arguments = invocation.arguments or {}
    query = str(arguments.get("query") or "").strip()
    limit = _coerce_limit(arguments.get("limit"))
    full_history = bool(query)
    workspace_id = str(workspace_id or _workspace_id_for_conversation(conversation_id)).strip()
    events = _conversation_events_after_reset(
        runtime,
        store,
        conversation_id,
        full_history=full_history,
        workspace_id=workspace_id,
    )
    selected, fallback_to_recent = _ranked_history_events(
        events,
        query=query,
        limit=limit,
        fallback_to_recent_on_no_match=not full_history,
    )
    structured_selected = _ranked_structured_reference_events(
        events,
        query=query,
        limit=min(_MAX_HISTORY_RETURN_LIMIT, max(limit, 12)),
    )
    records = [_event_record(event, index=index) for index, event in enumerate(selected, start=1)]
    structured_records = [
        _event_record(event, index=index)
        for index, event in enumerate(structured_selected, start=1)
    ]
    return ToolResult(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        status="completed",
        output={
            "conversation_id": conversation_id,
            "workspace_id": workspace_id,
            "query": query,
            "match_count": len(records),
            "searched_turn_count": len(events),
            "searched_scope": "full_workspace_after_reset" if full_history else "recent_workspace_after_reset",
            "fallback_to_recent": fallback_to_recent,
            "matches": records,
            "structured_matches": structured_records,
            "message": (
                f"Found {len(records)} saved conversation turn{'s' if len(records) != 1 else ''}."
                if records
                else "No saved conversation turns were found."
            ),
        },
    )


class ConversationHistoryToolRegistry:
    """Read-through registry that adds a current-conversation history search tool."""

    def __init__(
        self,
        delegate: object,
        *,
        runtime: object,
        conversation_id: str,
        workspace_id: str | None = None,
    ) -> None:
        self._delegate = delegate
        self._runtime = runtime
        self._conversation_id = conversation_id
        self._workspace_id = str(workspace_id or _workspace_id_for_conversation(conversation_id)).strip()

    def get_spec(self, name: str):
        if name == CHAT_HISTORY_SEARCH_TOOL_NAME:
            return CHAT_HISTORY_SEARCH_TOOL_SPEC
        return self._delegate.get_spec(name)

    def list_specs(self) -> list[object]:
        specs = list(self._delegate.list_specs())
        if not any(str(getattr(spec, "name", "") or "") == CHAT_HISTORY_SEARCH_TOOL_NAME for spec in specs):
            specs.append(CHAT_HISTORY_SEARCH_TOOL_SPEC)
        return sorted(specs, key=lambda spec: str(getattr(spec, "name", "") or ""))

    def list_tool_definitions(self, *args, **kwargs) -> list[dict[str, object]]:
        definitions = list(self._delegate.list_tool_definitions(*args, **kwargs))
        if not any(str(definition.get("name") or "") == CHAT_HISTORY_SEARCH_TOOL_NAME for definition in definitions):
            spec = CHAT_HISTORY_SEARCH_TOOL_SPEC
            definitions.append(
                {
                    "name": spec.name,
                    "description": spec.description,
                    "input_schema": spec.input_schema,
                    "capability_tags": list(spec.capability_tags),
                    "side_effect_class": spec.side_effect_class.value,
                    "risk_level": spec.risk_level.value,
                    "requires_approval": False,
                }
            )
        return sorted(definitions, key=lambda definition: str(definition.get("name") or ""))

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
        if invocation.tool_name == CHAT_HISTORY_SEARCH_TOOL_NAME:
            return _chat_history_search_result(
                runtime=self._runtime,
                conversation_id=self._conversation_id,
                invocation=invocation,
                workspace_id=self._workspace_id,
            )
        return self._delegate.invoke(invocation)

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


def with_conversation_history_tool(
    registry: object,
    *,
    runtime: object,
    conversation_id: str | None,
    workspace_id: str | None = None,
) -> object:
    normalized_conversation_id = str(conversation_id or "").strip()
    if registry is None or not normalized_conversation_id or getattr(runtime, "store", None) is None:
        return registry
    try:
        existing_names = {str(getattr(spec, "name", "") or "") for spec in registry.list_specs()}
    except Exception:
        try:
            existing_names = {str(definition.get("name") or "") for definition in registry.list_tool_definitions()}
        except Exception:
            existing_names = set()
    if CHAT_HISTORY_SEARCH_TOOL_NAME in existing_names:
        return registry
    return ConversationHistoryToolRegistry(
        registry,
        runtime=runtime,
        conversation_id=normalized_conversation_id,
        workspace_id=workspace_id,
    )


__all__ = [
    "CHAT_HISTORY_SEARCH_TOOL_NAME",
    "CHAT_HISTORY_SEARCH_TOOL_SPEC",
    "ConversationHistoryToolRegistry",
    "with_conversation_history_tool",
]

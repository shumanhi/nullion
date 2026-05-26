"""LangGraph routing for concurrent chat turns across platforms."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
import json
import logging
import os
import threading
import time
from typing import TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from .conversation_runtime import ConversationTurnDisposition
from .turn_relationship_evidence import has_structured_turn_relationship_evidence


logger = logging.getLogger(__name__)


def _dispatch_slow_log_threshold_ms() -> float:
    try:
        return float(os.environ.get("NULLION_TURN_DISPATCH_SLOW_LOG_MS", "500"))
    except ValueError:
        return 500.0


class TurnDispatchPolicy(str, Enum):
    PARALLEL = "parallel"
    WAIT_FOR_ACTIVE = "wait_for_active"


@dataclass(frozen=True, slots=True)
class TurnDispatchDecision:
    policy: TurnDispatchPolicy
    dependency_turn_ids: tuple[str, ...] = ()
    disposition: ConversationTurnDisposition = ConversationTurnDisposition.INDEPENDENT
    reason: str = "default_independent"
    target_active_turn_index: int | None = None

    @property
    def should_wait(self) -> bool:
        return self.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE and bool(self.dependency_turn_ids)


@dataclass(frozen=True, slots=True)
class _StructuredTurnDisposition:
    disposition: ConversationTurnDisposition
    reason: str
    target_active_turn_index: int | None = None


class _TurnDispatchState(TypedDict, total=False):
    text: str
    active_turn_ids: tuple[str, ...]
    active_turn_texts: tuple[str, ...]
    structured_followup_evidence: bool
    model_client: object | None
    disposition: ConversationTurnDisposition
    disposition_reason: str
    target_active_turn_index: int | None
    decision: TurnDispatchDecision


def _normalize_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(str(turn_id) for turn_id in state.get("active_turn_ids", ()) if str(turn_id).strip())
    raw_active_turn_texts = tuple(str(text) for text in state.get("active_turn_texts", ()))
    if len(raw_active_turn_texts) < len(active_turn_ids):
        raw_active_turn_texts = raw_active_turn_texts + ("",) * (len(active_turn_ids) - len(raw_active_turn_texts))
    active_turn_texts = raw_active_turn_texts[: len(active_turn_ids)]
    return {
        "active_turn_ids": active_turn_ids,
        "active_turn_texts": active_turn_texts,
    }


def _text_from_model_response(response: object) -> str:
    if not isinstance(response, dict):
        return ""
    parts: list[str] = []
    for block in response.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(str(block.get("text") or ""))
    return "".join(parts).strip()


def _parse_json_object(text: str) -> dict[str, object] | None:
    stripped = str(text or "").strip()
    if not stripped:
        return None
    try:
        payload = json.loads(stripped)
    except Exception:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            payload = json.loads(stripped[start : end + 1])
        except Exception:
            return None
    return payload if isinstance(payload, dict) else None


def _trim_active_request_text(text: str, *, limit: int = 700) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "..."


def _model_turn_disposition(
    *,
    model_client: object | None,
    current_text: str,
    active_turn_texts: tuple[str, ...],
) -> _StructuredTurnDisposition | None:
    if model_client is None or not active_turn_texts:
        return None
    create = getattr(model_client, "create", None)
    if not callable(create):
        return None
    active_payload = [
        {"index": index, "user_request": _trim_active_request_text(text)}
        for index, text in enumerate(active_turn_texts[-3:], start=max(0, len(active_turn_texts) - 3))
    ]
    system = (
        "Classify whether the current user message should link to one active or recent request "
        "or run as a separate request. Return only JSON with keys relationship, effect, confidence, "
        "and target_index. "
        "relationship must be one of: follow_up, separate. effect must be one of: continue, revise, interrupt. "
        "For follow_up, target_index must be the index of exactly one active_requests item. "
        "For separate, target_index must be null. "
        "Use continue when the active request should still produce its own result, revise when the current "
        "message changes the active request's requested output or parameters, and interrupt when the active "
        "request should be replaced. "
        "Use semantic understanding across languages. Do not split the current user message into multiple tasks."
    )
    user = json.dumps(
        {
            "active_requests": active_payload,
            "current_message": current_text,
            "allowed_relationships": ["follow_up", "separate"],
            "allowed_effects": ["continue", "revise", "interrupt"],
            "allowed_target_indexes": [item["index"] for item in active_payload],
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    response = create(
        messages=[{"role": "user", "content": [{"type": "text", "text": user}]}],
        tools=[],
        max_tokens=120,
        system=system,
    )
    payload = _parse_json_object(_text_from_model_response(response))
    if payload is None:
        return None
    relationship = str(payload.get("relationship") or "").strip().lower()
    try:
        confidence = float(payload.get("confidence", 0))
    except Exception:
        confidence = 0.0
    if confidence < 0.55:
        return None
    if relationship == "follow_up":
        target_index = payload.get("target_index")
        if target_index is None and len(active_turn_texts) == 1:
            target_index = 0
        try:
            target_index = int(target_index)
        except Exception:
            return None
        allowed_target_indexes = {int(item["index"]) for item in active_payload}
        if target_index not in allowed_target_indexes:
            return None
        effect = str(payload.get("effect") or "continue").strip().lower()
        if effect == "revise":
            return _StructuredTurnDisposition(
                ConversationTurnDisposition.REVISE,
                "model_structured_revision",
                target_index,
            )
        if effect == "interrupt":
            return _StructuredTurnDisposition(
                ConversationTurnDisposition.INTERRUPT,
                "model_structured_interrupt",
                target_index,
            )
        return _StructuredTurnDisposition(
            ConversationTurnDisposition.CONTINUE,
            "model_structured_follow_up",
            target_index,
        )
    if relationship == "separate":
        return _StructuredTurnDisposition(ConversationTurnDisposition.INDEPENDENT, "model_structured_separate")
    return None


def _classify_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(state.get("active_turn_ids", ()))
    if not active_turn_ids:
        disposition = ConversationTurnDisposition.INDEPENDENT
        reason = "no_active_turn"
        target_active_turn_index = None
    else:
        has_structured_evidence = (
            bool(state.get("structured_followup_evidence"))
            or
            has_structured_turn_relationship_evidence(str(state.get("text") or ""))
            or any(has_structured_turn_relationship_evidence(text) for text in tuple(state.get("active_turn_texts", ())))
        )
        model_create = getattr(state.get("model_client"), "create", None)
        has_structured_model_route = callable(model_create) and len(active_turn_ids) > 1
        if not has_structured_evidence and not has_structured_model_route:
            # Concurrent-turn dispatch must fail open unless either the new turn
            # or active runtime task has typed evidence, or a structured model
            # route is available to classify against the active task state. This
            # preserves the no-active-turn fast path while avoiding prose-only
            # local heuristics for follow-ups.
            disposition = ConversationTurnDisposition.INDEPENDENT
            reason = "no_structured_dispatch_evidence"
            target_active_turn_index = None
        else:
            model_decision = _model_turn_disposition(
                model_client=state.get("model_client"),
                current_text=str(state.get("text") or ""),
                active_turn_texts=tuple(state.get("active_turn_texts", ())),
            )
            if model_decision is None:
                disposition = ConversationTurnDisposition.INDEPENDENT
                reason = "no_structured_dispatch_decision"
                target_active_turn_index = None
            else:
                disposition = model_decision.disposition
                reason = model_decision.reason
                target_active_turn_index = model_decision.target_active_turn_index
    return {
        "disposition": disposition,
        "disposition_reason": reason,
        "target_active_turn_index": target_active_turn_index,
    }


def _dispatch_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(state.get("active_turn_ids", ()))
    disposition = state.get("disposition") or ConversationTurnDisposition.INDEPENDENT
    reason = str(state.get("disposition_reason") or "default_independent")
    target_active_turn_index = state.get("target_active_turn_index")
    if active_turn_ids and disposition in {
        ConversationTurnDisposition.CONTINUE,
        ConversationTurnDisposition.REVISE,
        ConversationTurnDisposition.INTERRUPT,
        ConversationTurnDisposition.BACKGROUND_FOLLOW_UP,
    }:
        try:
            dependency_turn_id = active_turn_ids[int(target_active_turn_index)]
        except Exception:
            return {
                "decision": TurnDispatchDecision(
                    policy=TurnDispatchPolicy.PARALLEL,
                    dependency_turn_ids=(),
                    disposition=ConversationTurnDisposition.INDEPENDENT,
                    reason="invalid_structured_dispatch_target",
                )
            }
        return {
            "decision": TurnDispatchDecision(
                policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
                dependency_turn_ids=(dependency_turn_id,),
                disposition=disposition,
                reason=reason,
                target_active_turn_index=int(target_active_turn_index),
            )
        }
    return {
        "decision": TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL,
            dependency_turn_ids=(),
            disposition=disposition,
            reason=reason,
        )
    }


@lru_cache(maxsize=1)
def _compiled_turn_dispatch_graph():
    graph = StateGraph(_TurnDispatchState)
    graph.add_node("normalize", _normalize_node)
    graph.add_node("classify", _classify_node)
    graph.add_node("dispatch", _dispatch_node)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "classify")
    graph.add_edge("classify", "dispatch")
    graph.add_edge("dispatch", END)
    return graph.compile()


def route_turn_dispatch(text: str, *, active_turn_ids: tuple[str, ...] = ()) -> TurnDispatchDecision:
    return route_turn_dispatch_with_context(text, active_turn_ids=active_turn_ids)


def route_turn_dispatch_with_context(
    text: str,
    *,
    active_turn_ids: tuple[str, ...] = (),
    active_turn_texts: tuple[str, ...] = (),
    structured_followup_evidence: bool = False,
    model_client: object | None = None,
) -> TurnDispatchDecision:
    final_state = _compiled_turn_dispatch_graph().invoke(
        {
            "text": text,
            "active_turn_ids": tuple(active_turn_ids),
            "active_turn_texts": tuple(active_turn_texts),
            "structured_followup_evidence": bool(structured_followup_evidence),
            "model_client": model_client,
        },
        config={"configurable": {"thread_id": "turn-dispatch"}},
    )
    decision = final_state.get("decision")
    if isinstance(decision, TurnDispatchDecision):
        return decision
    return TurnDispatchDecision(policy=TurnDispatchPolicy.PARALLEL)


@dataclass(slots=True)
class ActiveTurnRegistration:
    conversation_id: str
    turn_id: str
    decision: TurnDispatchDecision
    dependency_tasks: tuple[asyncio.Task, ...]
    cancellation_event: threading.Event
    _tracker: "AsyncTurnDispatchTracker"
    _finished: bool = False

    async def __aenter__(self) -> "ActiveTurnRegistration":
        await self.wait_for_dependencies()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        await self.finish()

    async def wait_for_dependencies(self) -> None:
        for dependency_task in self.dependency_tasks:
            try:
                await dependency_task
            except asyncio.CancelledError:
                raise
            except Exception:
                # The follow-up should still get a chance to respond with the
                # current state even if the referenced turn failed.
                pass

    async def finish(self) -> None:
        if self._finished:
            return
        self._finished = True
        await self._tracker.finish(self.conversation_id, self.turn_id)

    async def is_superseded(self) -> bool:
        return await self._tracker.is_superseded(self.conversation_id, self.turn_id)

    def is_cancelled(self) -> bool:
        return self.cancellation_event.is_set()


class AsyncTurnDispatchTracker:
    """Tracks active turns per conversation and applies dispatch dependencies."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._tasks_by_conversation: dict[str, dict[str, asyncio.Task]] = {}
        self._order_by_conversation: dict[str, list[str]] = {}
        self._text_by_conversation: dict[str, dict[str, str]] = {}
        self._superseded_by_conversation: dict[str, set[str]] = {}
        self._cancel_events_by_conversation: dict[str, dict[str, threading.Event]] = {}

    async def register(
        self,
        conversation_id: object,
        text: object,
        *,
        turn_id: object | None = None,
        task: asyncio.Task | None = None,
        model_client: object | None = None,
        reply_context: dict[str, object] | None = None,
    ) -> ActiveTurnRegistration:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        turn_key = _normalize_key(turn_id, fallback=f"turn-{uuid4().hex[:12]}")
        current_task = task or asyncio.current_task()
        if current_task is None:
            raise RuntimeError("register must be called from within an asyncio task")
        async with self._lock:
            active_order = list(self._order_by_conversation.get(conversation_key, []))
            active_texts_by_id = dict(self._text_by_conversation.get(conversation_key, {}))
        active_turn_ids = tuple(active_order)
        active_turn_texts = tuple(active_texts_by_id.get(active_turn_id, "") for active_turn_id in active_order)
        dispatch_started_at = time.perf_counter()
        if reply_context:
            decision = TurnDispatchDecision(
                policy=TurnDispatchPolicy.PARALLEL,
                dependency_turn_ids=(),
                disposition=ConversationTurnDisposition.INDEPENDENT,
                reason="explicit_reply_context",
            )
        elif active_turn_ids:
            decision = await asyncio.to_thread(
                route_turn_dispatch_with_context,
                str(text or ""),
                active_turn_ids=active_turn_ids,
                active_turn_texts=active_turn_texts,
                model_client=model_client,
            )
        else:
            # Keep the no-active-turn path synchronous. It is cheap and avoids
            # yielding before the first turn is registered, which would make a
            # concurrent follow-up miss the active turn it should observe.
            decision = route_turn_dispatch_with_context(
                str(text or ""),
                active_turn_ids=active_turn_ids,
                active_turn_texts=active_turn_texts,
                model_client=model_client,
            )
        dispatch_ms = (time.perf_counter() - dispatch_started_at) * 1000
        # This is intentionally keyed by ids/counts rather than message text so
        # production latency triage can find wasted relationship preflights
        # without leaking prompts into infrastructure logs.
        if active_order or dispatch_ms >= _dispatch_slow_log_threshold_ms():
            logger.info(
                "turn dispatch timing conversation_id=%s turn_id=%s active_turns=%s dependencies=%s policy=%s disposition=%s reason=%s total_ms=%.1f",
                conversation_key,
                turn_key,
                len(active_order),
                len(decision.dependency_turn_ids),
                decision.policy.value,
                decision.disposition.value,
                decision.reason,
                dispatch_ms,
            )
        async with self._lock:
            active_order = list(self._order_by_conversation.get(conversation_key, []))
            active_tasks = self._tasks_by_conversation.setdefault(conversation_key, {})
            dependency_tasks = tuple(
                active_tasks[dependency_turn_id]
                for dependency_turn_id in decision.dependency_turn_ids
                if dependency_turn_id in active_tasks and active_tasks[dependency_turn_id] is not current_task
            )
            if decision.disposition in {
                ConversationTurnDisposition.REVISE,
                ConversationTurnDisposition.INTERRUPT,
            }:
                superseded_turns = self._superseded_by_conversation.setdefault(conversation_key, set())
                superseded_turns.update(
                    dependency_turn_id
                    for dependency_turn_id in decision.dependency_turn_ids
                    if dependency_turn_id in active_tasks and active_tasks[dependency_turn_id] is not current_task
                )
            cancellation_event = threading.Event()
            active_tasks[turn_key] = current_task
            self._order_by_conversation.setdefault(conversation_key, []).append(turn_key)
            self._text_by_conversation.setdefault(conversation_key, {})[turn_key] = str(text or "")
            self._cancel_events_by_conversation.setdefault(conversation_key, {})[turn_key] = cancellation_event
        return ActiveTurnRegistration(
            conversation_id=conversation_key,
            turn_id=turn_key,
            decision=decision,
            dependency_tasks=dependency_tasks,
            cancellation_event=cancellation_event,
            _tracker=self,
        )

    async def finish(self, conversation_id: object, turn_id: object) -> None:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        turn_key = _normalize_key(turn_id, fallback="")
        async with self._lock:
            active_tasks = self._tasks_by_conversation.get(conversation_key)
            if active_tasks is not None:
                active_tasks.pop(turn_key, None)
                if not active_tasks:
                    self._tasks_by_conversation.pop(conversation_key, None)
                    self._text_by_conversation.pop(conversation_key, None)
            active_texts = self._text_by_conversation.get(conversation_key)
            if active_texts is not None:
                active_texts.pop(turn_key, None)
                if not active_texts:
                    self._text_by_conversation.pop(conversation_key, None)
            active_order = self._order_by_conversation.get(conversation_key)
            if active_order is not None:
                try:
                    active_order.remove(turn_key)
                except ValueError:
                    pass
                if not active_order:
                    self._order_by_conversation.pop(conversation_key, None)
            cancel_events = self._cancel_events_by_conversation.get(conversation_key)
            if cancel_events is not None:
                cancel_events.pop(turn_key, None)
                if not cancel_events:
                    self._cancel_events_by_conversation.pop(conversation_key, None)
            superseded_turns = self._superseded_by_conversation.get(conversation_key)
            if superseded_turns is not None:
                superseded_turns.discard(turn_key)
                if not superseded_turns:
                    self._superseded_by_conversation.pop(conversation_key, None)

    async def is_superseded(self, conversation_id: object, turn_id: object) -> bool:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        turn_key = _normalize_key(turn_id, fallback="")
        async with self._lock:
            return turn_key in self._superseded_by_conversation.get(conversation_key, set())

    async def active_turn_ids(self, conversation_id: object) -> tuple[str, ...]:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        async with self._lock:
            return tuple(self._order_by_conversation.get(conversation_key, ()))

    async def cancel_conversation(self, conversation_id: object) -> tuple[str, ...]:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        current_task = asyncio.current_task()
        tasks_to_cancel: list[tuple[str, asyncio.Task]] = []
        async with self._lock:
            active_tasks = self._tasks_by_conversation.get(conversation_key)
            if not active_tasks:
                return ()
            for turn_key, task in list(active_tasks.items()):
                if task is current_task or task.done():
                    continue
                tasks_to_cancel.append((turn_key, task))
            if not tasks_to_cancel:
                return ()
            cancelled_turn_ids = tuple(turn_key for turn_key, _task in tasks_to_cancel)
            superseded_turns = self._superseded_by_conversation.setdefault(conversation_key, set())
            superseded_turns.update(cancelled_turn_ids)
            active_texts = self._text_by_conversation.get(conversation_key)
            active_order = self._order_by_conversation.get(conversation_key)
            cancel_events = self._cancel_events_by_conversation.get(conversation_key)
            for turn_key, _task in tasks_to_cancel:
                if cancel_events is not None:
                    cancel_event = cancel_events.pop(turn_key, None)
                    if cancel_event is not None:
                        cancel_event.set()
                active_tasks.pop(turn_key, None)
                if active_texts is not None:
                    active_texts.pop(turn_key, None)
                if active_order is not None:
                    try:
                        active_order.remove(turn_key)
                    except ValueError:
                        pass
            if not active_tasks:
                self._tasks_by_conversation.pop(conversation_key, None)
            if active_texts is not None and not active_texts:
                self._text_by_conversation.pop(conversation_key, None)
            if active_order is not None and not active_order:
                self._order_by_conversation.pop(conversation_key, None)
            if cancel_events is not None and not cancel_events:
                self._cancel_events_by_conversation.pop(conversation_key, None)
        for _turn_key, task in tasks_to_cancel:
            task.cancel()
        return tuple(turn_key for turn_key, _task in tasks_to_cancel)


def _normalize_key(value: object, *, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


GLOBAL_TURN_DISPATCH_TRACKER = AsyncTurnDispatchTracker()


__all__ = [
    "ActiveTurnRegistration",
    "AsyncTurnDispatchTracker",
    "GLOBAL_TURN_DISPATCH_TRACKER",
    "TurnDispatchDecision",
    "TurnDispatchPolicy",
    "route_turn_dispatch",
    "route_turn_dispatch_with_context",
]

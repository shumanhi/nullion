"""LangGraph routing for concurrent chat turns across platforms."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
import json
import logging
import math
import os
import threading
import time
from typing import TypedDict
from uuid import uuid4
from weakref import WeakValueDictionary

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
    work_ack_recommended: bool = False
    clarification_targets: tuple[tuple[str, str], ...] = ()
    clarification_effect: ConversationTurnDisposition = ConversationTurnDisposition.CONTINUE
    resolved_message: str | None = None

    @property
    def should_wait(self) -> bool:
        return self.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE and bool(self.dependency_turn_ids)


@dataclass(frozen=True, slots=True)
class _StructuredTurnDisposition:
    disposition: ConversationTurnDisposition
    reason: str
    target_active_turn_index: int | None = None
    target_is_explicit: bool = False
    needs_clarification: bool = False
    clarification_effect: ConversationTurnDisposition = ConversationTurnDisposition.CONTINUE
    plausible_target_indexes: tuple[int, ...] | None = None


class _TurnDispatchState(TypedDict, total=False):
    text: str
    active_turn_ids: tuple[str, ...]
    active_turn_texts: tuple[str, ...]
    active_turn_followup_evidence: bool
    model_client: object | None
    disposition: ConversationTurnDisposition
    disposition_reason: str
    target_active_turn_index: int | None
    needs_clarification: bool
    clarification_effect: ConversationTurnDisposition
    target_context_selected: bool
    decision: TurnDispatchDecision
    candidate_contexts: tuple[dict, ...]
    plausible_target_indexes: tuple[int, ...] | None


def _normalize_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(str(turn_id) for turn_id in state.get("active_turn_ids", ()) if str(turn_id).strip())
    raw_active_turn_texts = tuple(str(text) for text in state.get("active_turn_texts", ()))
    if len(raw_active_turn_texts) < len(active_turn_ids):
        raw_active_turn_texts = raw_active_turn_texts + ("",) * (len(active_turn_ids) - len(raw_active_turn_texts))
    active_turn_texts = raw_active_turn_texts[: len(active_turn_ids)]
    active_turn_structured_evidence = any(
        has_structured_turn_relationship_evidence(text) for text in active_turn_texts
    )
    return {
        "active_turn_ids": active_turn_ids,
        "active_turn_texts": active_turn_texts,
        "active_turn_followup_evidence": bool(state.get("active_turn_followup_evidence", True))
        or active_turn_structured_evidence,
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
    active_turn_followup_evidence: bool,
    target_context_selected: bool = False,
    candidate_contexts: tuple[dict, ...] = (),
) -> _StructuredTurnDisposition | None:
    if model_client is None or not active_turn_texts:
        return None
    create = getattr(model_client, "create", None)
    if not callable(create):
        return None
    active_payload = [
        {**(candidate_contexts[index] if index < len(candidate_contexts) else {}),
         "index": index, "user_request": _trim_active_request_text(text)}
        for index, text in enumerate(active_turn_texts)
    ]
    system = (
        "Classify whether the current user message should link to one active or recent request "
        "or run as a separate request. Return only JSON with keys relationship, effect, confidence, "
        "target_index, target_is_explicit, and plausible_target_indexes. "
        "relationship must be one of: follow_up, separate, ambiguous. effect must be one of: continue, revise, interrupt. "
        "For follow_up, target_index must be the index of exactly one active_requests item. "
        "For separate, target_index must be null. "
        "target_is_explicit must be true only when the current_message itself semantically identifies, "
        "modifies, or requests work on a specific active_requests item. Set target_is_explicit false for "
        "generic agreement, acknowledgement, permission, or continuation messages that could refer to "
        "an earlier completed turn instead of the active request. "
        "Use ambiguous when the message refers to prior work but does not identify which request it means. "
        "For ambiguous, plausible_target_indexes must list only candidates the requested work can meaningfully "
        "apply to, based on their last reply and tool evidence. Do not list every recent conversation. "
        "A request can identify its target semantically through a result or page from that conversation, "
        "without repeating the original request title. A greeting with no page is not a page-work candidate. "
        "The presence of a URL, file, or running task in the candidates does not identify the user's target. "
        "Even one running candidate is not enough to resolve an unanchored, vague reference: it may refer "
        "to a recently completed request. Never guess a target for a revision or cancellation. "
        "Use continue when the active request should still produce its own result, revise when the current "
        "message changes the active request's requested output or parameters, and interrupt when the active "
        "request should be replaced. "
        "Use semantic understanding across languages. Do not split the current user message into multiple tasks."
    )
    if target_context_selected:
        system += (" This is sequential recovery within an already selected request context, not concurrent task "
            "admission. A semantic reference to that selected context can be follow_up without an explicit name.")
    user = json.dumps(
        {
            "active_requests": active_payload,
            "current_message": current_text,
            "allowed_relationships": ["follow_up", "separate", "ambiguous"],
            "allowed_effects": ["continue", "revise", "interrupt"],
            "allowed_target_indexes": [item["index"] for item in active_payload],
            "target_context_selected": target_context_selected,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    response = create(
        messages=[{"role": "user", "content": [{"type": "text", "text": user}]}],
        tools=[],
        # The budget includes reasoning as well as the typed JSON decision.
        # 120 tokens can truncate an otherwise valid semantic relationship.
        max_tokens=512,
        system=system,
    )
    payload = _parse_json_object(_text_from_model_response(response))
    if payload is None:
        return None
    relationship = str(payload.get("relationship") or "").strip().lower()
    raw_effect = payload.get("effect")
    clarification_effect = (ConversationTurnDisposition(raw_effect)
        if isinstance(raw_effect, str) and raw_effect in {"continue", "revise", "interrupt"}
        else ConversationTurnDisposition.CONTINUE)
    try:
        confidence = float(payload.get("confidence", 0))
    except Exception:
        confidence = 0.0
    if not math.isfinite(confidence):
        confidence = 0.0
    plausible = payload.get("plausible_target_indexes")
    if (isinstance(plausible, list) and all(type(i) is int and 0 <= i < len(active_payload) for i in plausible)):
        plausible = tuple(dict.fromkeys(plausible))
    else:
        plausible = None
    if relationship == "ambiguous" or (relationship == "follow_up" and confidence < 0.55):
        return _StructuredTurnDisposition(ConversationTurnDisposition.INDEPENDENT,
            "ambiguous_dispatch_target", needs_clarification=True, clarification_effect=clarification_effect,
            plausible_target_indexes=plausible)
    if confidence < 0.55:
        return None
    explicit_field = payload.get("target_is_explicit")
    target_is_explicit = explicit_field is True
    if relationship == "follow_up":
        # An active request is itself typed runtime evidence. A validated
        # semantic target can link ordinary follow-ups without requiring a
        # URL, filename, or attachment in either message. Ambiguous assent
        # still needs a stronger runtime anchor.
        if not target_is_explicit and not (target_context_selected and active_turn_followup_evidence):
            return _StructuredTurnDisposition(ConversationTurnDisposition.INDEPENDENT,
                "ambiguous_dispatch_target", needs_clarification=True, clarification_effect=clarification_effect)
        target_index = payload.get("target_index")
        if target_index is None and len(active_turn_texts) == 1 and active_turn_followup_evidence:
            target_index = 0
        if not isinstance(target_index, int) or isinstance(target_index, bool):
            return None
        allowed_target_indexes = {int(item["index"]) for item in active_payload}
        if target_index not in allowed_target_indexes:
            return None
        effect = str(payload.get("effect") or "continue").strip().lower()
        if effect not in {"continue", "revise", "interrupt"}:
            return None
        if effect == "revise":
            return _StructuredTurnDisposition(
                ConversationTurnDisposition.REVISE,
                "model_structured_revision",
                target_index,
                target_is_explicit,
            )
        if effect == "interrupt":
            return _StructuredTurnDisposition(
                ConversationTurnDisposition.INTERRUPT,
                "model_structured_interrupt",
                target_index,
                target_is_explicit,
            )
        return _StructuredTurnDisposition(
            ConversationTurnDisposition.CONTINUE,
            "model_structured_follow_up",
            target_index,
            target_is_explicit,
        )
    if relationship == "separate":
        return _StructuredTurnDisposition(
            ConversationTurnDisposition.INDEPENDENT,
            "model_structured_separate",
        )
    return None


def _classify_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(state.get("active_turn_ids", ()))
    model_decision = None
    if not active_turn_ids:
        disposition = ConversationTurnDisposition.INDEPENDENT
        reason = "no_active_turn"
        target_active_turn_index = None
    else:
        try:
            model_decision = _model_turn_disposition(
                model_client=state.get("model_client"),
                current_text=str(state.get("text") or ""),
                active_turn_texts=tuple(state.get("active_turn_texts", ())),
                active_turn_followup_evidence=bool(state.get("active_turn_followup_evidence", True)),
                target_context_selected=bool(state.get("target_context_selected", False)),
                candidate_contexts=tuple(state.get("candidate_contexts", ())),
            )
        except Exception:
            # The user can choose a typed target even when the optional
            # relationship provider is unavailable. Existing work stays live.
            logger.warning("Concurrent request target classification unavailable")
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
        "needs_clarification": bool(active_turn_ids and state.get("model_client") is not None
            and (model_decision is None or model_decision.needs_clarification)),
        "clarification_effect": model_decision.clarification_effect if model_decision else ConversationTurnDisposition.CONTINUE,
        "plausible_target_indexes": model_decision.plausible_target_indexes if model_decision else None,
    }


def _dispatch_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(state.get("active_turn_ids", ()))
    disposition = state.get("disposition") or ConversationTurnDisposition.INDEPENDENT
    reason = str(state.get("disposition_reason") or "default_independent")
    target_active_turn_index = state.get("target_active_turn_index")
    if state.get("needs_clarification"):
        plausible = state.get("plausible_target_indexes")
        targets = tuple(zip(active_turn_ids, state.get("active_turn_texts", ())))
        if plausible is not None:
            targets = tuple(targets[index] for index in plausible)
            if not targets:
                return {"decision": TurnDispatchDecision(policy=TurnDispatchPolicy.PARALLEL,
                    reason="no_plausible_dispatch_target")}
        return {"decision": TurnDispatchDecision(
            policy=TurnDispatchPolicy.PARALLEL, reason="ambiguous_dispatch_target",
            clarification_targets=targets,
            clarification_effect=state.get("clarification_effect", ConversationTurnDisposition.CONTINUE))}
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
    active_turn_followup_evidence: bool = True,
    model_client: object | None = None,
    target_context_selected: bool = False,
    candidate_contexts: tuple[dict, ...] = (),
) -> TurnDispatchDecision:
    final_state = _compiled_turn_dispatch_graph().invoke(
        {
            "text": text,
            "active_turn_ids": tuple(active_turn_ids),
            "active_turn_texts": tuple(active_turn_texts),
            "active_turn_followup_evidence": bool(active_turn_followup_evidence),
            "model_client": model_client,
            "target_context_selected": target_context_selected,
            "candidate_contexts": candidate_contexts,
        },
        config={"configurable": {"thread_id": "turn-dispatch"}},
    )
    decision = final_state.get("decision")
    if isinstance(decision, TurnDispatchDecision):
        return decision
    return TurnDispatchDecision(policy=TurnDispatchPolicy.PARALLEL)


class TurnPublicationRevoked(BaseException):
    """A superseded publication is control flow, never a transport retry."""


class TurnPublicationGate:
    """Revoke obsolete generations and pause publication during overlap decisions."""

    def __init__(self, *, on_revoke=None) -> None:
        self._on_revoke = on_revoke
        self._ready = asyncio.Event()
        self._ready.set()
        self._revoked = threading.Event()
        self._pending = 0

    def hold(self) -> None:
        self._pending += 1
        self._ready.clear()

    def release(self) -> None:
        self._pending = max(0, self._pending - 1)
        if not self._pending:
            self._ready.set()

    def revoke(self) -> None:
        if self.revoked:
            return
        self._revoked.set()
        self._ready.set()
        if self._on_revoke is not None:
            try:
                self._on_revoke()
            except Exception:
                logger.warning("Could not persist turn publication revocation", exc_info=True)

    @property
    def revoked(self) -> bool:
        return self._revoked.is_set()

    def can_publish(self) -> bool:
        return not self.revoked and self._ready.is_set()

    def guard(self, operation, *, withdraw=None):
        async def guarded(*args, **kwargs):
            if not await self.wait():
                raise TurnPublicationRevoked()
            result = await operation(*args, **kwargs)
            if not await self.wait():
                if withdraw is not None:
                    try:
                        await withdraw(result)
                    except Exception:
                        logger.warning("Could not withdraw superseded delivery", exc_info=True)
                raise TurnPublicationRevoked()
            return result
        return guarded

    async def wait(self) -> bool:
        if self.revoked:
            return False
        await self._ready.wait()
        return not self.revoked


@dataclass(slots=True)
class ActiveTurnRegistration:
    conversation_id: str
    turn_id: str
    decision: TurnDispatchDecision
    dependency_tasks: tuple[asyncio.Future, ...]
    cancellation_event: threading.Event
    publication_gate: TurnPublicationGate
    _tracker: "AsyncTurnDispatchTracker"
    _finished: bool = False
    finish_after_processing: bool = True

    async def __aenter__(self) -> "ActiveTurnRegistration":
        await self.wait_for_dependencies()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        if self.finish_after_processing:
            await self.finish()

    async def wait_for_dependencies(self) -> None:
        for dependency_task in self.dependency_tasks:
            try:
                # Cancelling one successor must not cancel the shared signal
                # or a predecessor which is still serving other successors.
                await asyncio.shield(dependency_task)
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
        await self.publication_gate.wait()
        return self.publication_gate.revoked

    def is_cancelled(self) -> bool:
        return self.cancellation_event.is_set()


class AsyncTurnDispatchTracker:
    """Tracks active turns per conversation and applies dispatch dependencies."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._admission_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self._publication_by_conversation: dict[str, dict[str, TurnPublicationGate]] = {}
        self._tasks_by_conversation: dict[str, dict[str, asyncio.Task]] = {}
        self._completion_by_conversation: dict[str, dict[str, asyncio.Future]] = {}
        self._done_callbacks: dict[tuple[str, str], object] = {}
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
        active_turn_followup_evidence: bool = False,
        finish_after_processing: bool = True,
        runtime_store=None,
    ) -> ActiveTurnRegistration:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        # Serialise only overlapping admission decisions for this conversation.
        # Waiting successors then observe the revision that preceded them.
        admission = self._admission_locks.setdefault(conversation_key, asyncio.Lock())
        async with admission:
            async with self._lock:
                held = tuple(self._publication_by_conversation.get(conversation_key, {}).values())
                for gate in held:
                    gate.hold()
            try:
                return await self._register(conversation_id, text, turn_id=turn_id, task=task,
                    model_client=model_client, reply_context=reply_context,
                    active_turn_followup_evidence=active_turn_followup_evidence,
                    finish_after_processing=finish_after_processing, runtime_store=runtime_store)
            finally:
                for gate in held:
                    gate.release()

    async def _register(
        self,
        conversation_id: object,
        text: object,
        *,
        turn_id: object | None = None,
        task: asyncio.Task | None = None,
        model_client: object | None = None,
        reply_context: dict[str, object] | None = None,
        active_turn_followup_evidence: bool = False,
        finish_after_processing: bool = True,
        runtime_store=None,
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
        from .turn_dispatch_choices import dispatch_candidates, dispatch_candidate_contexts, record_dispatch_choice, selected_dispatch_choice, selected_conversation_option
        choice = selected_dispatch_choice(runtime_store, conversation_key, text, reply_context)
        if choice is not None:
            decision = choice
        elif reply_context:
            decision = TurnDispatchDecision(
                policy=TurnDispatchPolicy.PARALLEL,
                dependency_turn_ids=(),
                disposition=ConversationTurnDisposition.INDEPENDENT,
                reason="explicit_reply_context",
            )
        elif (option := selected_conversation_option(runtime_store, conversation_key, text)) is not None:
            decision = option
        elif active_turn_ids:
            candidate_ids, candidate_texts = dispatch_candidates(
                runtime_store, conversation_key, active_turn_ids, active_turn_texts)
            decision = await asyncio.to_thread(
                route_turn_dispatch_with_context,
                str(text or ""),
                active_turn_ids=candidate_ids,
                active_turn_texts=candidate_texts,
                active_turn_followup_evidence=active_turn_followup_evidence,
                model_client=model_client,
                candidate_contexts=dispatch_candidate_contexts(runtime_store, conversation_key, candidate_ids, active_turn_ids),
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
        if decision.reason in {"ambiguous_dispatch_target", "dispatch_target_unavailable"}:
            record_dispatch_choice(runtime_store, conversation_key, turn_key,
                decision.resolved_message or str(text or ""), decision)
        if decision.dependency_turn_ids and not set(decision.dependency_turn_ids).intersection(active_turn_ids):
            decision = replace(decision, policy=TurnDispatchPolicy.PARALLEL)
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
                self._completion_by_conversation[conversation_key][dependency_turn_id]
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
                for dependency_turn_id in decision.dependency_turn_ids:
                    if dependency_turn_id not in active_tasks or active_tasks[dependency_turn_id] is current_task:
                        continue
                    gate = self._publication_by_conversation.get(conversation_key, {}).get(dependency_turn_id)
                    if gate is not None:
                        gate.revoke()
                    cancel = self._cancel_events_by_conversation.get(conversation_key, {}).get(dependency_turn_id)
                    if cancel is not None:
                        cancel.set()
            cancellation_event = threading.Event()
            def record_revocation():
                if runtime_store is not None:
                    runtime_store.add_conversation_event({
                        "event_type": "conversation.reply_superseded", "conversation_id": conversation_key,
                        "turn_id": turn_key, "created_at": datetime.now(timezone.utc).isoformat(),
                    })
            publication_gate = TurnPublicationGate(on_revoke=record_revocation)
            self._publication_by_conversation.setdefault(conversation_key, {})[turn_key] = publication_gate
            active_tasks[turn_key] = current_task
            self._completion_by_conversation.setdefault(conversation_key, {})[turn_key] = asyncio.get_running_loop().create_future()
            # Retire orphan registrations even if an adapter exits before its
            # normal finally block. Identity is unique for this registration.
            def completed(_task):
                if not _task.get_loop().is_closed():
                    _task.get_loop().create_task(self.finish(conversation_key, turn_key))
            current_task.add_done_callback(completed)
            self._done_callbacks[(conversation_key, turn_key)] = completed
            self._order_by_conversation.setdefault(conversation_key, []).append(turn_key)
            self._text_by_conversation.setdefault(conversation_key, {})[turn_key] = str(text or "")
            self._cancel_events_by_conversation.setdefault(conversation_key, {})[turn_key] = cancellation_event
        return ActiveTurnRegistration(
            conversation_id=conversation_key,
            turn_id=turn_key,
            decision=decision,
            dependency_tasks=dependency_tasks,
            cancellation_event=cancellation_event,
            publication_gate=publication_gate,
            _tracker=self,
            finish_after_processing=finish_after_processing,
        )

    async def finish(self, conversation_id: object, turn_id: object) -> None:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        turn_key = _normalize_key(turn_id, fallback="")
        async with self._lock:
            completions = self._completion_by_conversation.get(conversation_key)
            if completions is not None:
                completed = completions.pop(turn_key, None)
                if completed is not None and not completed.done():
                    completed.set_result(None)
                if not completions:
                    self._completion_by_conversation.pop(conversation_key, None)
            publications = self._publication_by_conversation.get(conversation_key)
            if publications is not None:
                publications.pop(turn_key, None)
                if not publications:
                    self._publication_by_conversation.pop(conversation_key, None)
            active_tasks = self._tasks_by_conversation.get(conversation_key)
            if active_tasks is not None:
                task = active_tasks.pop(turn_key, None)
                callback = self._done_callbacks.pop((conversation_key, turn_key), None)
                if task is not None and callback is not None:
                    task.remove_done_callback(callback)
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
            publications = self._publication_by_conversation.get(conversation_key, {})
            for turn_key, _task in tasks_to_cancel:
                completed = self._completion_by_conversation.get(conversation_key, {}).get(turn_key)
                if completed is not None and not completed.done():
                    completed.set_result(None)
                gate = publications.pop(turn_key, None)
                if gate is not None:
                    gate.revoke()
                if cancel_events is not None:
                    cancel_event = cancel_events.pop(turn_key, None)
                    if cancel_event is not None:
                        cancel_event.set()
                active_tasks.pop(turn_key, None)
                callback = self._done_callbacks.pop((conversation_key, turn_key), None)
                if callback is not None:
                    _task.remove_done_callback(callback)
                completions = self._completion_by_conversation.get(conversation_key)
                if completions is not None:
                    completions.pop(turn_key, None)
                    if not completions:
                        self._completion_by_conversation.pop(conversation_key, None)
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
    "TurnPublicationGate",
    "route_turn_dispatch",
    "route_turn_dispatch_with_context",
]

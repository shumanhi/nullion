"""LangGraph routing for concurrent chat turns across platforms."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from .conversation_runtime import ConversationTurnDisposition
from .intent_router import classify_turn_disposition_with_reason


class TurnDispatchPolicy(str, Enum):
    PARALLEL = "parallel"
    WAIT_FOR_ACTIVE = "wait_for_active"


@dataclass(frozen=True, slots=True)
class TurnDispatchDecision:
    policy: TurnDispatchPolicy
    dependency_turn_ids: tuple[str, ...] = ()
    disposition: ConversationTurnDisposition = ConversationTurnDisposition.INDEPENDENT
    reason: str = "default_independent"

    @property
    def should_wait(self) -> bool:
        return self.policy is TurnDispatchPolicy.WAIT_FOR_ACTIVE and bool(self.dependency_turn_ids)


class _TurnDispatchState(TypedDict, total=False):
    text: str
    normalized: str
    active_turn_ids: tuple[str, ...]
    disposition: ConversationTurnDisposition
    disposition_reason: str
    decision: TurnDispatchDecision


_FOLLOW_UP_DISPOSITIONS = {
    ConversationTurnDisposition.CONTINUE,
    ConversationTurnDisposition.REVISE,
    ConversationTurnDisposition.INTERRUPT,
    ConversationTurnDisposition.BACKGROUND_FOLLOW_UP,
}


def _normalize_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(str(turn_id) for turn_id in state.get("active_turn_ids", ()) if str(turn_id).strip())
    return {
        "normalized": str(state.get("text") or "").strip(),
        "active_turn_ids": active_turn_ids,
    }


def _classify_node(state: _TurnDispatchState) -> dict[str, object]:
    text = str(state.get("normalized") or "")
    active_turn_ids = tuple(state.get("active_turn_ids", ()))
    decision = classify_turn_disposition_with_reason(
        text,
        active_branch_exists=bool(active_turn_ids),
    )
    return {
        "disposition": decision.disposition,
        "disposition_reason": decision.reason,
    }


def _dispatch_node(state: _TurnDispatchState) -> dict[str, object]:
    active_turn_ids = tuple(state.get("active_turn_ids", ()))
    disposition = state.get("disposition") or ConversationTurnDisposition.INDEPENDENT
    reason = str(state.get("disposition_reason") or "default_independent")
    if active_turn_ids and disposition in _FOLLOW_UP_DISPOSITIONS:
        return {
            "decision": TurnDispatchDecision(
                policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
                dependency_turn_ids=(active_turn_ids[-1],),
                disposition=disposition,
                reason=reason,
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
    final_state = _compiled_turn_dispatch_graph().invoke(
        {
            "text": text,
            "active_turn_ids": tuple(active_turn_ids),
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


class AsyncTurnDispatchTracker:
    """Tracks active turns per conversation and applies dispatch dependencies."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._tasks_by_conversation: dict[str, dict[str, asyncio.Task]] = {}
        self._order_by_conversation: dict[str, list[str]] = {}

    async def register(
        self,
        conversation_id: object,
        text: object,
        *,
        turn_id: object | None = None,
        task: asyncio.Task | None = None,
    ) -> ActiveTurnRegistration:
        conversation_key = _normalize_key(conversation_id, fallback="conversation:default")
        turn_key = _normalize_key(turn_id, fallback=f"turn-{uuid4().hex[:12]}")
        current_task = task or asyncio.current_task()
        if current_task is None:
            raise RuntimeError("register must be called from within an asyncio task")
        async with self._lock:
            active_order = list(self._order_by_conversation.get(conversation_key, []))
            active_tasks = self._tasks_by_conversation.setdefault(conversation_key, {})
            decision = route_turn_dispatch(str(text or ""), active_turn_ids=tuple(active_order))
            dependency_tasks = tuple(
                active_tasks[dependency_turn_id]
                for dependency_turn_id in decision.dependency_turn_ids
                if dependency_turn_id in active_tasks and active_tasks[dependency_turn_id] is not current_task
            )
            active_tasks[turn_key] = current_task
            self._order_by_conversation.setdefault(conversation_key, []).append(turn_key)
        return ActiveTurnRegistration(
            conversation_id=conversation_key,
            turn_id=turn_key,
            decision=decision,
            dependency_tasks=dependency_tasks,
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
            active_order = self._order_by_conversation.get(conversation_key)
            if active_order is not None:
                try:
                    active_order.remove(turn_key)
                except ValueError:
                    pass
                if not active_order:
                    self._order_by_conversation.pop(conversation_key, None)


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
]

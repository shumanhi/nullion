"""LangGraph-backed orchestration for messaging turns."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.approval_markers import split_tool_approval_marker, strip_tool_approval_marker
from nullion import messaging_adapters as adapters


class MessagingTurnState(TypedDict, total=False):
    service: Any
    ingress: adapters.MessagingIngress
    before_decision_snapshot: Any
    raw_reply: str | None
    visible_reply: str | None
    reply: str | None
    delivery_contract: adapters.DeliveryContract
    status: Literal["running", "reply_ready", "no_reply"]


@dataclass(frozen=True, slots=True)
class MessagingTurnResult:
    reply: str | None
    delivery_contract: adapters.DeliveryContract
    status: Literal["reply_ready", "no_reply"]


def _capture_decision_snapshot_node(state: MessagingTurnState) -> dict[str, object]:
    return {
        "before_decision_snapshot": adapters._capture_messaging_decision_snapshot(state["service"]),
        "status": "running",
    }


def _run_service_node(state: MessagingTurnState) -> dict[str, object]:
    ingress = state["ingress"]
    reply = state["service"].handle_text_message(
        text=ingress.text,
        chat_id=ingress.operator_chat_id,
        reminder_chat_id=ingress.reminder_chat_id,
        attachments=list(ingress.attachments),
        request_id=ingress.request_id,
        message_id=ingress.message_id,
    )
    return {"raw_reply": reply}


def _finalize_reply_node(state: MessagingTurnState) -> dict[str, object]:
    ingress = state["ingress"]
    raw_reply = state.get("raw_reply")
    marker = split_tool_approval_marker(raw_reply)
    visible_reply = strip_tool_approval_marker(raw_reply)
    before_decision_snapshot = state["before_decision_snapshot"]
    fallbacks = adapters._new_decision_text_fallbacks(state["service"], before_decision_snapshot)
    if marker is not None and (
        not marker.approval_id or marker.approval_id in before_decision_snapshot.pending_approval_ids
    ):
        fallbacks = (
            *fallbacks,
            *adapters._approval_text_fallback_for_marker(getattr(state["service"], "runtime", None), marker.approval_id),
        )
    reply = adapters._append_decision_fallbacks(visible_reply, fallbacks)
    adapters.save_messaging_chat_history(ingress, reply)
    return {
        "visible_reply": visible_reply,
        "reply": reply,
        "delivery_contract": adapters.delivery_contract_for_runtime_turn(
            getattr(state["service"], "runtime", None),
            ingress.operator_chat_id,
            ingress.text,
            reply=reply,
            inbound_attachments=ingress.attachments,
        ),
        "status": "reply_ready" if reply is not None else "no_reply",
    }


@lru_cache(maxsize=1)
def _compiled_messaging_turn_graph():
    graph = StateGraph(MessagingTurnState)
    graph.add_node("capture_decision_snapshot", _capture_decision_snapshot_node)
    graph.add_node("run_service", _run_service_node)
    graph.add_node("finalize_reply", _finalize_reply_node)
    graph.add_edge(START, "capture_decision_snapshot")
    graph.add_edge("capture_decision_snapshot", "run_service")
    graph.add_edge("run_service", "finalize_reply")
    graph.add_edge("finalize_reply", END)
    return graph.compile()


def run_messaging_turn_graph(service: object, ingress: adapters.MessagingIngress) -> MessagingTurnResult:
    final_state = _compiled_messaging_turn_graph().invoke(
        {"service": service, "ingress": ingress},
        config={"configurable": {"thread_id": ingress.request_id or ingress.operator_chat_id}},
    )
    delivery_contract = final_state.get("delivery_contract")
    if not isinstance(delivery_contract, adapters.DeliveryContract):
        delivery_contract = adapters.DeliveryContract.message_only()
    status = final_state.get("status")
    if status not in {"reply_ready", "no_reply"}:
        status = "no_reply"
    return MessagingTurnResult(
        reply=final_state.get("reply"),
        delivery_contract=delivery_contract,
        status=status,
    )


__all__ = [
    "MessagingTurnResult",
    "MessagingTurnState",
    "run_messaging_turn_graph",
]

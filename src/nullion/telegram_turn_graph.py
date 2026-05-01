"""LangGraph planning for Telegram post-run delivery."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.messaging_adapters import DeliveryContract, delivery_contract_for_turn


class TelegramPostRunState(TypedDict, total=False):
    text_for_ack: str | None
    reply: str
    inbound_attachments: tuple[dict[str, str], ...]
    decision_card: Any
    suggestion_markup: Any
    stream_final_reply: bool
    streaming_mode: Any
    final_only_streaming_mode: Any
    primary_card: Any
    supplemental_card: Any
    additional_markup: Any
    delivery_contract: DeliveryContract
    selected_streaming_mode: Any


@dataclass(frozen=True, slots=True)
class TelegramPostRunDeliveryPlan:
    primary_card: Any
    supplemental_card: Any
    additional_markup: Any
    delivery_contract: DeliveryContract
    streaming_mode: Any


def _split_decision_card_node(state: TelegramPostRunState) -> dict[str, object]:
    decision_card = state.get("decision_card")
    supplemental_card = decision_card if bool(getattr(decision_card, "supplemental", False)) else None
    primary_card = None if supplemental_card is not None else decision_card
    additional_markup = state.get("suggestion_markup") if primary_card is None else None
    return {
        "primary_card": primary_card,
        "supplemental_card": supplemental_card,
        "additional_markup": additional_markup,
    }


def _build_delivery_contract_node(state: TelegramPostRunState) -> dict[str, object]:
    return {
        "delivery_contract": delivery_contract_for_turn(
            state.get("text_for_ack"),
            reply=state["reply"],
            inbound_attachments=state.get("inbound_attachments") or (),
        )
    }


def _select_streaming_mode_node(state: TelegramPostRunState) -> dict[str, object]:
    return {
        "selected_streaming_mode": (
            state.get("streaming_mode")
            if bool(state.get("stream_final_reply"))
            else state.get("final_only_streaming_mode")
        )
    }


@lru_cache(maxsize=1)
def _compiled_telegram_post_run_graph():
    graph = StateGraph(TelegramPostRunState)
    graph.add_node("split_decision_card", _split_decision_card_node)
    graph.add_node("build_delivery_contract", _build_delivery_contract_node)
    graph.add_node("select_streaming_mode", _select_streaming_mode_node)
    graph.add_edge(START, "split_decision_card")
    graph.add_edge("split_decision_card", "build_delivery_contract")
    graph.add_edge("build_delivery_contract", "select_streaming_mode")
    graph.add_edge("select_streaming_mode", END)
    return graph.compile()


def plan_telegram_post_run_delivery(
    *,
    text_for_ack: str | None,
    reply: str,
    inbound_attachments: tuple[dict[str, str], ...] = (),
    decision_card: Any = None,
    suggestion_markup: Any = None,
    stream_final_reply: bool,
    streaming_mode: Any,
    final_only_streaming_mode: Any,
) -> TelegramPostRunDeliveryPlan:
    final_state = _compiled_telegram_post_run_graph().invoke(
        {
            "text_for_ack": text_for_ack,
            "reply": reply,
            "inbound_attachments": inbound_attachments,
            "decision_card": decision_card,
            "suggestion_markup": suggestion_markup,
            "stream_final_reply": stream_final_reply,
            "streaming_mode": streaming_mode,
            "final_only_streaming_mode": final_only_streaming_mode,
        },
        config={"configurable": {"thread_id": "telegram-post-run-delivery"}},
    )
    delivery_contract = final_state.get("delivery_contract")
    if not isinstance(delivery_contract, DeliveryContract):
        delivery_contract = DeliveryContract.message_only()
    return TelegramPostRunDeliveryPlan(
        primary_card=final_state.get("primary_card"),
        supplemental_card=final_state.get("supplemental_card"),
        additional_markup=final_state.get("additional_markup"),
        delivery_contract=delivery_contract,
        streaming_mode=final_state.get("selected_streaming_mode"),
    )


__all__ = [
    "TelegramPostRunDeliveryPlan",
    "TelegramPostRunState",
    "plan_telegram_post_run_delivery",
]

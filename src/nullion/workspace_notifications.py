"""Workspace-scoped notification fanout for approvals and scheduled work."""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Iterable, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.approval_context import approval_trigger_flow_label
from nullion.approval_display import (
    approval_display_from_request,
    approval_inline_code,
    format_approval_detail_markdown,
)
from nullion.approvals import ApprovalRequest, ApprovalStatus
from nullion.config import NullionSettings
from nullion.connections import workspace_id_for_principal
from nullion.events import make_event
from nullion.users import messaging_delivery_targets_for_workspace

log = logging.getLogger(__name__)


def approval_workspace_id(approval: ApprovalRequest) -> str:
    context = approval.context if isinstance(approval.context, dict) else {}
    raw_workspace = context.get("workspace_id")
    if isinstance(raw_workspace, str) and raw_workspace.strip():
        return raw_workspace.strip()
    return workspace_id_for_principal(approval.requested_by)


def approval_notification_text(approval: ApprovalRequest) -> str:
    workspace_id = approval_workspace_id(approval)
    display = approval_display_from_request(approval)
    lines = [
        f"{display.title}",
        f"Workspace: {workspace_id}",
        f"Request: {display.label}",
    ]
    if display.detail:
        lines.append(format_approval_detail_markdown(display.detail))
    trigger_label = approval_trigger_flow_label(approval)
    if trigger_label:
        lines.append(f"Triggered by: {approval_inline_code(trigger_label)}")
    lines.append(f"Approval ID: {approval.approval_id}")
    lines.append("Open the web dashboard or use the originating chat approval controls to decide.")
    return "\n".join(lines)


def _approval_broadcast_event_exists(store, approval_id: str) -> bool:
    try:
        return any(
            event.event_type == "approval.notification.broadcasted"
            and isinstance(event.payload, dict)
            and event.payload.get("approval_id") == approval_id
            for event in store.list_events()
        )
    except Exception:
        return False


def _record_approval_broadcast_event(store, approval: ApprovalRequest, *, workspace_id: str, targets: Iterable[str]) -> None:
    try:
        store.add_event(
            make_event(
                "approval.notification.broadcasted",
                "workspace_notifications",
                {
                    "approval_id": approval.approval_id,
                    "workspace_id": workspace_id,
                    "targets": list(targets),
                    "created_at": datetime.now(UTC).isoformat(),
                },
            )
        )
    except Exception:
        log.debug("Could not record approval notification event", exc_info=True)


def _run_delivery(coro) -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return bool(asyncio.run(coro))

    result: list[bool] = []
    errors: list[BaseException] = []

    def _runner() -> None:
        try:
            result.append(bool(asyncio.run(coro)))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=_runner, name="nullion-approval-notification-delivery", daemon=True)
    thread.start()
    thread.join()
    if errors:
        raise errors[0]
    return bool(result[0]) if result else False


def _approval_origin_delivery_target(store, approval: ApprovalRequest) -> tuple[str, str] | None:
    try:
        suspended_turn = store.get_suspended_turn(approval.approval_id)
    except Exception:
        suspended_turn = None
    if suspended_turn is None:
        return None
    conversation_id = str(getattr(suspended_turn, "conversation_id", "") or "")
    channel, separator, identity = conversation_id.partition(":")
    channel = channel.strip().lower()
    target_id = str(getattr(suspended_turn, "chat_id", "") or identity or "").strip()
    if target_id.startswith(f"{channel}:"):
        target_id = target_id.split(":", 1)[1].strip()
    if not separator or not channel or not target_id:
        return None
    return channel, target_id


def _is_origin_delivery_target(target, origin: tuple[str, str] | None) -> bool:
    if origin is None:
        return False
    origin_channel, origin_target_id = origin
    target_channel = str(getattr(target, "channel", "") or "").strip().lower()
    target_id = str(getattr(target, "target_id", "") or "").strip()
    if target_id.startswith(f"{target_channel}:"):
        target_id = target_id.split(":", 1)[1].strip()
    return target_channel == origin_channel and target_id == origin_target_id


class _ApprovalBroadcastState(TypedDict, total=False):
    runtime: Any
    approval: ApprovalRequest
    settings: NullionSettings | None
    store: Any
    workspace_id: str
    text: str
    targets: tuple[Any, ...]
    origin: tuple[str, str] | None
    delivered: tuple[str, ...]
    skipped_origin: bool
    skip_reason: str


def _deliver_approval_notification_target(
    target,
    *,
    approval: ApprovalRequest,
    settings: NullionSettings | None,
    text: str,
) -> bool:
    if target.channel == "telegram":
        bot_token = getattr(getattr(settings, "telegram", None), "bot_token", None)
        if not bot_token:
            return False
        from nullion.telegram_entrypoint import _send_operator_telegram_delivery
        from nullion.telegram_app import _approval_card_text, _build_approval_markup

        return _run_delivery(_send_operator_telegram_delivery(
            bot_token,
            target.target_id,
            _approval_card_text(approval),
            principal_id=target.principal_id,
            reply_markup=_build_approval_markup(approval=approval),
            suppress_link_preview=True,
        ))
    if target.channel == "slack":
        bot_token = getattr(getattr(settings, "slack", None), "bot_token", None)
        if not bot_token:
            return False
        from nullion.slack_app import send_slack_platform_delivery

        return _run_delivery(send_slack_platform_delivery(
            bot_token=bot_token,
            channel=target.target_id,
            text=text,
            principal_id=target.principal_id,
        ))
    if target.channel == "discord":
        bot_token = getattr(getattr(settings, "discord", None), "bot_token", None)
        if not bot_token:
            return False
        from nullion.discord_app import send_discord_platform_delivery

        return _run_delivery(send_discord_platform_delivery(
            bot_token=bot_token,
            channel_id=target.target_id,
            text=text,
            principal_id=target.principal_id,
        ))
    return False


def _approval_broadcast_prepare_node(state: _ApprovalBroadcastState) -> dict[str, object]:
    approval = state["approval"]
    if approval.status is not ApprovalStatus.PENDING:
        return {"delivered": (), "skip_reason": "approval_not_pending"}
    runtime = state.get("runtime")
    store = getattr(runtime, "store", None)
    if store is None:
        return {"delivered": (), "skip_reason": "store_unavailable"}
    if _approval_broadcast_event_exists(store, approval.approval_id):
        return {"delivered": (), "skip_reason": "already_broadcasted"}
    workspace_id = approval_workspace_id(approval)
    settings = state.get("settings")
    return {
        "store": store,
        "workspace_id": workspace_id,
        "text": approval_notification_text(approval),
        "targets": tuple(messaging_delivery_targets_for_workspace(workspace_id, settings=settings)),
        "origin": _approval_origin_delivery_target(store, approval),
        "delivered": (),
        "skipped_origin": False,
        "skip_reason": "",
    }


def _approval_broadcast_route_prepared(state: _ApprovalBroadcastState) -> str:
    return END if state.get("skip_reason") else "deliver"


def _approval_broadcast_deliver_node(state: _ApprovalBroadcastState) -> dict[str, object]:
    approval = state["approval"]
    settings = state.get("settings")
    text = str(state.get("text") or "")
    origin = state.get("origin")
    delivered: list[str] = []
    skipped_origin = False
    for target in state.get("targets") or ():
        if _is_origin_delivery_target(target, origin):
            skipped_origin = True
            continue
        try:
            if _deliver_approval_notification_target(target, approval=approval, settings=settings, text=text):
                delivered.append(f"{target.channel}:{target.target_id}")
        except Exception:
            log.warning("Workspace approval notification failed for %s", target.channel, exc_info=True)
    return {"delivered": tuple(delivered), "skipped_origin": skipped_origin}


def _approval_broadcast_finalize_node(state: _ApprovalBroadcastState) -> dict[str, object]:
    approval = state["approval"]
    delivered = tuple(state.get("delivered") or ())
    workspace_id = str(state.get("workspace_id") or approval_workspace_id(approval))
    store = state.get("store")
    if delivered and store is not None:
        _record_approval_broadcast_event(store, approval, workspace_id=workspace_id, targets=delivered)
    elif state.get("skipped_origin"):
        log.debug("Skipped approval notification back to originating chat for approval_id=%s", approval.approval_id)
    else:
        log.warning("Approval notification was not delivered for approval_id=%s workspace_id=%s", approval.approval_id, workspace_id)
    return {}


@lru_cache(maxsize=1)
def _compiled_approval_broadcast_graph():
    graph = StateGraph(_ApprovalBroadcastState)
    graph.add_node("prepare", _approval_broadcast_prepare_node)
    graph.add_node("deliver", _approval_broadcast_deliver_node)
    graph.add_node("finalize", _approval_broadcast_finalize_node)
    graph.add_edge(START, "prepare")
    graph.add_conditional_edges("prepare", _approval_broadcast_route_prepared, {"deliver": "deliver", END: END})
    graph.add_edge("deliver", "finalize")
    graph.add_edge("finalize", END)
    return graph.compile()


def broadcast_approval_request(
    runtime,
    approval: ApprovalRequest,
    *,
    settings: NullionSettings | None,
) -> tuple[str, ...]:
    final_state = _compiled_approval_broadcast_graph().invoke(
        {"runtime": runtime, "approval": approval, "settings": settings, "delivered": ()},
        config={"configurable": {"thread_id": f"approval-broadcast:{approval.approval_id}"}},
    )
    return tuple(final_state.get("delivered") or ())


def broadcast_new_pending_approvals(
    runtime,
    *,
    before_ids: Iterable[str],
    settings: NullionSettings | None,
) -> tuple[str, ...]:
    store = getattr(runtime, "store", None)
    if store is None:
        return ()
    before = {str(item) for item in before_ids}
    delivered: list[str] = []
    for approval in store.list_approval_requests():
        if approval.status is not ApprovalStatus.PENDING:
            continue
        if approval.approval_id in before:
            continue
        delivered.extend(broadcast_approval_request(runtime, approval, settings=settings))
    return tuple(delivered)


def broadcast_pending_approval(
    runtime,
    approval_id: str | None,
    *,
    settings: NullionSettings | None,
) -> tuple[str, ...]:
    if not approval_id:
        return ()
    store = getattr(runtime, "store", None)
    if store is None:
        return ()
    approval = store.get_approval_request(str(approval_id))
    if approval is None:
        return ()
    return broadcast_approval_request(runtime, approval, settings=settings)


__all__ = [
    "approval_notification_text",
    "approval_workspace_id",
    "broadcast_approval_request",
    "broadcast_pending_approval",
    "broadcast_new_pending_approvals",
]

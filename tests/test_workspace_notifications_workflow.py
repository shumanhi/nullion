from __future__ import annotations

import asyncio

from nullion.approvals import create_approval_request
from nullion.config import NullionSettings, TelegramSettings
from nullion.runtime_store import RuntimeStore
from nullion.suspended_turns import SuspendedTurn
from nullion.users import MessagingDeliveryTarget, messaging_delivery_targets_for_workspace
from nullion.workspace_notifications import (
    approval_workspace_id,
    broadcast_pending_approval,
    broadcast_approval_request,
    broadcast_new_pending_approvals,
)


class RuntimeDouble:
    def __init__(self) -> None:
        self.store = RuntimeStore()


def _settings() -> NullionSettings:
    return NullionSettings(
        telegram=TelegramSettings(
            bot_token="test-token",
            operator_chat_id="514132807",
            chat_enabled=True,
        )
    )


def _telegram_target() -> MessagingDeliveryTarget:
    return MessagingDeliveryTarget(
        channel="telegram",
        target_id="514132807",
        principal_id="telegram_chat",
        workspace_id="workspace_admin",
        user_id="admin",
        display_name="Admin",
    )


def _member_telegram_target() -> MessagingDeliveryTarget:
    return MessagingDeliveryTarget(
        channel="telegram",
        target_id="8674359402",
        principal_id="user:member_nathan",
        workspace_id="workspace_nathan",
        user_id="member_nathan",
        display_name="Nathan",
    )


def _web_boundary_approval():
    return create_approval_request(
        requested_by="web:session-123",
        action="allow_boundary",
        resource="https://www.bing.com/*",
        request_kind="boundary_policy",
        context={
            "workspace_id": "workspace_admin",
            "tool_name": "web_search",
            "boundary_kind": "outbound_network",
            "target": "https://www.bing.com/*",
        },
    )


def _member_web_boundary_approval():
    approval = _web_boundary_approval()
    return create_approval_request(
        requested_by=approval.requested_by,
        action=approval.action,
        resource=approval.resource,
        request_kind=approval.request_kind,
        context={**approval.context, "workspace_id": "workspace_nathan"},
    )


def _broadcast_events(runtime: RuntimeDouble):
    return [
        event
        for event in runtime.store.list_events()
        if event.event_type == "approval.notification.broadcasted"
    ]


def test_admin_workspace_has_configured_telegram_delivery_target() -> None:
    targets = messaging_delivery_targets_for_workspace("workspace_admin", settings=_settings())

    assert ("telegram", "514132807") in {(target.channel, target.target_id) for target in targets}


def test_web_origin_approval_maps_to_admin_workspace() -> None:
    approval = _web_boundary_approval()

    assert approval_workspace_id(approval) == "workspace_admin"


def test_web_created_approval_broadcasts_telegram_card_with_buttons(monkeypatch) -> None:
    runtime = RuntimeDouble()
    approval = _web_boundary_approval()
    runtime.store.add_approval_request(approval)
    sent: list[dict[str, object]] = []

    monkeypatch.setattr(
        "nullion.workspace_notifications.messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: (_telegram_target(),),
    )

    async def fake_send(bot_token, chat_id, text, **kwargs):  # noqa: ANN001
        sent.append({"bot_token": bot_token, "chat_id": chat_id, "text": text, **kwargs})
        return True

    monkeypatch.setattr("nullion.telegram_entrypoint._send_operator_telegram_delivery", fake_send)

    delivered = broadcast_new_pending_approvals(runtime, before_ids=(), settings=_settings())

    assert delivered == ("telegram:514132807",)
    assert sent and sent[0]["chat_id"] == "514132807"
    assert "Allow web access?" in str(sent[0]["text"])
    assert "Requested URL: `bing.com`" in str(sent[0]["text"])
    assert "`https://www.bing.com/*`" in str(sent[0]["text"])
    assert sent[0]["reply_markup"] is not None
    assert _broadcast_events(runtime)


def test_originating_telegram_chat_does_not_receive_duplicate_workspace_approval(monkeypatch) -> None:
    from datetime import UTC, datetime

    runtime = RuntimeDouble()
    approval = _web_boundary_approval()
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="telegram:514132807",
            chat_id="514132807",
            message="/chat weather",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
        )
    )
    sent: list[dict[str, object]] = []

    monkeypatch.setattr(
        "nullion.workspace_notifications.messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: (_telegram_target(),),
    )

    async def fake_send(bot_token, chat_id, text, **kwargs):  # noqa: ANN001
        sent.append({"bot_token": bot_token, "chat_id": chat_id, "text": text, **kwargs})
        return True

    monkeypatch.setattr("nullion.telegram_entrypoint._send_operator_telegram_delivery", fake_send)

    assert broadcast_new_pending_approvals(runtime, before_ids=(), settings=_settings()) == ()
    assert sent == []
    assert _broadcast_events(runtime) == []


def test_current_blocking_approval_broadcasts_even_when_id_existed_before(monkeypatch) -> None:
    runtime = RuntimeDouble()
    approval = _member_web_boundary_approval()
    runtime.store.add_approval_request(approval)
    sent: list[dict[str, object]] = []

    def targets_for_workspace(workspace_id, *, settings):  # noqa: ANN001
        assert workspace_id == "workspace_nathan"
        return (_member_telegram_target(),)

    monkeypatch.setattr(
        "nullion.workspace_notifications.messaging_delivery_targets_for_workspace",
        targets_for_workspace,
    )

    async def fake_send(bot_token, chat_id, text, **kwargs):  # noqa: ANN001
        sent.append({"bot_token": bot_token, "chat_id": chat_id, "text": text, **kwargs})
        return True

    monkeypatch.setattr("nullion.telegram_entrypoint._send_operator_telegram_delivery", fake_send)

    assert broadcast_new_pending_approvals(
        runtime,
        before_ids=(approval.approval_id,),
        settings=_settings(),
    ) == ()
    assert broadcast_pending_approval(runtime, approval.approval_id, settings=_settings()) == ("telegram:8674359402",)
    assert sent and sent[0]["chat_id"] == "8674359402"
    assert "Allow web access?" in str(sent[0]["text"])
    assert len(_broadcast_events(runtime)) == 1
    assert broadcast_pending_approval(runtime, approval.approval_id, settings=_settings()) == ()


def test_failed_telegram_delivery_does_not_mark_approval_broadcasted(monkeypatch) -> None:
    runtime = RuntimeDouble()
    approval = _web_boundary_approval()
    runtime.store.add_approval_request(approval)
    attempts = 0

    monkeypatch.setattr(
        "nullion.workspace_notifications.messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: (_telegram_target(),),
    )

    async def fake_send(bot_token, chat_id, text, **kwargs):  # noqa: ANN001, ARG001
        nonlocal attempts
        attempts += 1
        return attempts > 1

    monkeypatch.setattr("nullion.telegram_entrypoint._send_operator_telegram_delivery", fake_send)

    assert broadcast_approval_request(runtime, approval, settings=_settings()) == ()
    assert _broadcast_events(runtime) == []
    assert broadcast_approval_request(runtime, approval, settings=_settings()) == ("telegram:514132807",)
    assert attempts == 2
    assert len(_broadcast_events(runtime)) == 1


def test_approval_broadcast_can_run_inside_existing_event_loop(monkeypatch) -> None:
    runtime = RuntimeDouble()
    approval = _web_boundary_approval()
    runtime.store.add_approval_request(approval)

    monkeypatch.setattr(
        "nullion.workspace_notifications.messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings: (_telegram_target(),),
    )

    async def fake_send(bot_token, chat_id, text, **kwargs):  # noqa: ANN001, ARG001
        return True

    monkeypatch.setattr("nullion.telegram_entrypoint._send_operator_telegram_delivery", fake_send)

    async def _inside_loop() -> tuple[str, ...]:
        return broadcast_approval_request(runtime, approval, settings=_settings())

    assert asyncio.run(_inside_loop()) == ("telegram:514132807",)
    assert len(_broadcast_events(runtime)) == 1

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from nullion.approvals import ApprovalRequest, ApprovalStatus
from nullion.config import NullionSettings, TelegramSettings
from nullion.reminders import ReminderRecord


def test_messaging_roots_and_allowed_roots_use_data_dir(tmp_path, monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))

    upload_root, scratch_root = adapters.ensure_messaging_storage_roots()
    assert upload_root == tmp_path / "uploads"
    assert scratch_root == tmp_path / "tmp" / "media"
    assert upload_root.exists()
    assert scratch_root.exists()

    roots = adapters.messaging_file_allowed_roots(tmp_path / "extra", [tmp_path / "nested", ""])
    assert upload_root.resolve() in roots
    assert scratch_root.resolve() in roots
    assert (tmp_path / "extra").resolve() in roots
    assert (tmp_path / "nested").resolve() in roots


def test_messaging_ingress_ids_and_authorization(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    ingress = adapters.MessagingIngress(channel="slack", user_id="U1", text="hi", delivery_target_id="C1")
    assert ingress.operator_chat_id == "slack:U1"
    assert ingress.reminder_chat_id == "slack:C1"

    monkeypatch.setattr(adapters, "is_authorized_messaging_identity", lambda channel, user_id, settings: channel == "slack")
    assert adapters.require_authorized_ingress(ingress, NullionSettings()) is True

    monkeypatch.setattr(adapters, "resolve_messaging_user", lambda channel, user_id, settings=None: SimpleNamespace(role="member", user_id="member-1"))
    assert adapters.principal_id_for_messaging_identity("slack", "U1") == "user:member-1"
    monkeypatch.setattr(adapters, "resolve_messaging_user", lambda channel, user_id, settings=None: SimpleNamespace(role="admin", user_id="admin"))
    assert adapters.principal_id_for_messaging_identity("telegram", "1") == "telegram_chat"


def test_save_messaging_chat_history_is_best_effort(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    saved: list[tuple[str, str, str, str, str]] = []

    class Store:
        def save_message(self, conversation_id, role, text, *, channel, channel_label):
            saved.append((conversation_id, role, text, channel, channel_label))

    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: Store())

    adapters.save_messaging_chat_history(adapters.MessagingIngress("discord", "D1", "hello"), "reply")

    assert saved == [
        ("discord:D1", "user", "hello", "discord:D1", "Discord · D1"),
        ("discord:D1", "bot", "reply", "discord:D1", "Discord · D1"),
    ]

    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: (_ for _ in ()).throw(RuntimeError("disk")))
    adapters.save_messaging_chat_history(adapters.MessagingIngress("discord", "D1", "hello"), "reply")


def test_decision_snapshot_and_fallbacks_are_appended(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    approval = ApprovalRequest(
        "ap-new",
        "operator",
        "execute",
        "deploy",
        ApprovalStatus.PENDING,
        datetime(2026, 1, 1, tzinfo=UTC),
    )
    reminder = ReminderRecord("rem-new", "telegram:1", "check", datetime(2026, 1, 1, 12, tzinfo=UTC))
    action = {
        "action_id": "doc-new",
        "status": "pending",
        "severity": "high",
        "summary": "Fix runtime",
        "recommendation_code": "doctor:retry_model_api",
    }

    class Store:
        def __init__(self):
            self.approvals = []
            self.actions = []
            self.reminders = []

        def list_approval_requests(self):
            return list(self.approvals)

        def list_doctor_actions(self):
            return list(self.actions)

        def list_reminders(self):
            return list(self.reminders)

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            self.runtime.store.approvals = [approval]
            self.runtime.store.actions = [action]
            self.runtime.store.reminders = [reminder]
            self.runtime.list_pending_builder_proposals = lambda: [
                SimpleNamespace(proposal_id="prop-new", proposal=SimpleNamespace(title="Proposal", summary="Change code"))
            ]
            return "Base reply"

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    reply = adapters.handle_messaging_ingress(Service(), adapters.MessagingIngress("telegram", "1", "do it"))

    assert reply.startswith("Base reply")
    assert "Approval pending" in reply
    assert "Builder proposal pending" in reply
    assert "Doctor action pending" in reply
    assert "Reminder set" in reply


def test_messaging_turn_graph_returns_delivery_contract(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    artifact = tmp_path / "result.anything"

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            return f"Finished.\nMEDIA:{artifact}"

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)
    monkeypatch.setattr(adapters, "media_candidate_paths_from_text", lambda text: [artifact])

    result = adapters.handle_messaging_ingress_result(
        Service(),
        adapters.MessagingIngress("slack", "U1", "arbitrary wording"),
    )

    assert result.reply == f"Finished.\nMEDIA:{artifact}"
    assert result.status == "reply_ready"
    assert result.delivery_contract.requires_attachment_delivery
    assert result.delivery_contract.source == "media_directive"


def test_messaging_turn_graph_ignores_text_only_file_verbs(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    class Store:
        def list_approval_requests(self):
            return []

        def list_doctor_actions(self):
            return []

        def list_reminders(self):
            return []

    class Service:
        def __init__(self):
            self.runtime = SimpleNamespace(store=Store(), list_pending_builder_proposals=lambda: [])

        def handle_text_message(self, **kwargs):
            return "Here is the summary in chat."

    monkeypatch.setattr(adapters, "save_messaging_chat_history", lambda ingress, reply: None)

    result = adapters.handle_messaging_ingress_result(
        Service(),
        adapters.MessagingIngress("slack", "U1", "send that as a text file"),
    )

    assert result.status == "reply_ready"
    assert not result.delivery_contract.requires_attachment_delivery
    assert result.delivery_contract.source == "message"


def test_prepare_reply_for_platform_delivery_sanitizes_and_reports_missing_attachments(monkeypatch, tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "report.txt"
    attachment.write_text("hello", encoding="utf-8")
    monkeypatch.setattr(adapters, "media_candidate_paths_from_text", lambda text: [attachment])
    monkeypatch.setattr(adapters, "split_reply_for_platform_delivery", lambda text, principal_id=None: ("caption", (attachment,)))

    delivery = adapters.prepare_reply_for_platform_delivery("<b>caption</b> [file]")
    assert delivery.text == "caption"
    assert delivery.attachments == (attachment,)
    assert delivery.requires_attachment_delivery
    assert delivery.has_deliverable_attachments

    monkeypatch.setattr(adapters, "split_reply_for_platform_delivery", lambda text, principal_id=None: ("caption", ()))
    missing = adapters.prepare_reply_for_platform_delivery("<b>caption</b> [file]")
    assert "artifact is unavailable" in missing.text
    assert missing.unavailable_attachment_count == 1

    assert adapters.prepare_reply_for_platform_delivery(None).text is None
    assert adapters.platform_delivery_failure_reply() == "I couldn't upload the requested attachment to this platform. I won't mark it delivered."


def test_platform_delivery_receipts_are_durable_and_contract_aware(tmp_path) -> None:
    from nullion import messaging_adapters as adapters

    attachment = tmp_path / "report.txt"
    attachment.write_text("hello", encoding="utf-8")
    delivery = adapters.PlatformDelivery(
        text="Attached.",
        attachments=(attachment,),
        media_directive_count=1,
    )

    receipt = adapters.build_platform_delivery_receipt(
        channel="telegram",
        target_id="123",
        delivery=delivery,
        transport_ok=True,
        request_id="req-1",
        message_id="msg-1",
    )
    assert receipt.status == "succeeded"
    assert receipt.attachment_required is True
    assert receipt.attachment_count == 1

    missing = adapters.PlatformDelivery(
        text="I couldn't attach it.",
        attachments=(),
        media_directive_count=1,
        unavailable_attachment_count=1,
    )
    missing_receipt = adapters.build_platform_delivery_receipt(
        channel="slack",
        target_id="C1",
        delivery=missing,
        transport_ok=True,
        error="artifact_unavailable",
    )
    assert missing_receipt.status == "failed"
    assert missing_receipt.text_delivered is True
    assert missing_receipt.unavailable_attachment_count == 1

    receipt_path = tmp_path / "receipts.jsonl"
    assert adapters.record_platform_delivery_receipt(receipt, path=receipt_path) is True
    assert adapters.record_platform_delivery_receipt(missing_receipt, path=receipt_path) is True
    line = receipt_path.read_text(encoding="utf-8").strip()
    assert '"channel": "telegram"' in line
    assert '"status": "succeeded"' in line
    failed = adapters.list_platform_delivery_receipts(path=receipt_path, status="failed")
    assert [record["channel"] for record in failed] == ["slack"]
    all_receipts = adapters.list_platform_delivery_receipts(path=receipt_path, status=None)
    assert [record["channel"] for record in all_receipts] == ["slack", "telegram"]


def test_retry_messaging_delivery_operation_retries_async_and_stops_on_non_retryable(monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(adapters.asyncio, "sleep", fake_sleep)

    attempts = {"count": 0}

    class TimeoutErrorForTest(Exception):
        pass

    async def run_success():
        def operation():
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise TimeoutErrorForTest("timeout")
            return "ok"

        return await adapters.retry_messaging_delivery_operation(operation, attempts=3, retry_delay_seconds=0.1)

    assert asyncio.run(run_success()) == "ok"
    assert sleeps == [0.1]

    with pytest.raises(ValueError):
        asyncio.run(adapters.retry_messaging_delivery_operation(lambda: (_ for _ in ()).throw(ValueError("bad")), attempts=3))

    class ResponseError(Exception):
        status_code = 503

    assert adapters.is_retryable_messaging_delivery_error(ResponseError()) is True
    assert adapters.is_retryable_messaging_delivery_error(ValueError("bad")) is False


def test_split_reply_for_platform_and_save_attachment(tmp_path, monkeypatch) -> None:
    from nullion import messaging_adapters as adapters

    chunks = adapters.split_reply_for_platform("alpha beta gamma", limit=8)
    assert chunks == ["alpha", "beta", "gamma"]
    assert adapters.split_reply_for_platform(None, limit=8) == []
    assert adapters.split_reply_for_platform("   ", limit=8) == []

    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    monkeypatch.setattr(adapters, "ensure_messaging_storage_roots", lambda: (upload_dir, tmp_path / "scratch"))

    saved = adapters.save_messaging_attachment(filename="../note.txt", data=b"hello")
    assert saved["name"] == "note.txt"
    assert (upload_dir / "note.txt").read_bytes() == b"hello"

    saved_again = adapters.save_messaging_attachment(filename="../note.txt", data=b"again")
    assert saved_again["name"] != "note.txt"
    assert adapters.save_messaging_attachment(filename="x", data=b"", max_bytes=1) is None
    assert adapters.save_messaging_attachment(filename="x", data=b"too large", max_bytes=1) is None


@pytest.mark.asyncio
async def test_slack_delivery_retries_plain_text_after_formatted_send_failure() -> None:
    from nullion import slack_app

    class FormattingFailureSlackClient:
        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def chat_postMessage(self, **kwargs):
            self.messages.append(kwargs)
            if len(self.messages) == 1:
                raise RuntimeError("formatted send failed")

    client = FormattingFailureSlackClient()

    await slack_app._post_slack_message_with_plain_fallback(
        client,
        channel="C1",
        formatted_text="Hello *Ada*",
        plain_text="Hello **Ada**",
    )

    assert client.messages[0]["text"] == "Hello *Ada*"
    assert client.messages[1]["mrkdwn"] is False
    assert "```text\nHello **Ada**\n```" in client.messages[1]["text"]


@pytest.mark.asyncio
async def test_discord_delivery_retries_plain_text_after_send_failure() -> None:
    from nullion import discord_app

    class FormattingFailureChannel:
        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def send(self, *args, **kwargs):
            if args:
                kwargs["content"] = args[0]
            self.messages.append(kwargs)
            if len(self.messages) == 1:
                raise RuntimeError("formatted send failed")

    channel = FormattingFailureChannel()

    await discord_app._send_discord_chunks_with_plain_fallback(channel, "Hello **Ada**")

    assert channel.messages[0]["content"] == "Hello **Ada**"
    assert "```text\nHello **Ada**\n```" in channel.messages[1]["content"]

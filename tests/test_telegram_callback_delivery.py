import pytest
from datetime import UTC, datetime
from types import SimpleNamespace

from nullion.approvals import create_approval_request
from nullion.builder import BuilderDecisionType, BuilderProposal
from nullion.chat_operator import resume_approved_telegram_request
from nullion.runtime import PersistentRuntime
from nullion.runtime_store import RuntimeStore
from nullion.suspended_turns import SuspendedTurn
from nullion import telegram_app


class ReplyMessage:
    def __init__(self) -> None:
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class CallbackMessage(ReplyMessage):
    def __init__(self) -> None:
        super().__init__()
        self.edits = []

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class CaptionCallbackMessage(ReplyMessage):
    def __init__(self) -> None:
        super().__init__()
        self.caption_edits = []

    async def edit_text(self, text, **kwargs):
        class BadRequest(Exception):
            pass

        raise BadRequest("There is no text in the message to edit")

    async def edit_caption(self, caption, **kwargs):
        self.caption_edits.append((caption, kwargs))


@pytest.mark.asyncio
async def test_callback_follow_up_does_not_require_attachment_for_plain_text():
    message = ReplyMessage()

    await telegram_app._send_callback_follow_up(
        message,
        "Here are the latest CNN headlines.",
        principal_id="telegram_chat",
    )

    assert message.replies
    assert message.replies[-1][0] == "Here are the latest CNN headlines."
    assert "couldn't attach" not in message.replies[-1][0].lower()


@pytest.mark.asyncio
async def test_telegram_callback_action_result_chunks_oversized_doctor_inspection():
    message = CallbackMessage()
    long_reply = "Doctor run inspection\n\n" + "\n".join(f"- Evidence {index}: {'x' * 120}" for index in range(80))

    await telegram_app._deliver_callback_action_result(message, "Inspect run", long_reply)

    assert message.edits
    assert "too long for one Telegram message" in message.edits[0][0]
    assert len(message.edits[0][0]) < telegram_app._TELEGRAM_MESSAGE_CHUNK_SIZE
    assert len(message.replies) > 1
    assert "Doctor run inspection" in message.replies[0][0]
    assert all(len(text) <= telegram_app._TELEGRAM_MESSAGE_CHUNK_SIZE for text, _ in message.replies)


@pytest.mark.asyncio
async def test_telegram_callback_action_result_edits_document_caption_card():
    message = CaptionCallbackMessage()

    await telegram_app._deliver_callback_action_result(message, "Email sent", "Email sent.")

    assert message.caption_edits == [("Email sent.", {"reply_markup": None})]


def test_telegram_approval_resumes_web_originated_suspended_turn(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    approval = create_approval_request(
        requested_by="web:abc123",
        action="allow_boundary",
        resource="https://www.bbc.com/news",
        request_kind="boundary_policy",
        context={},
    )
    runtime.store.add_approval_request(approval)
    runtime.approve_approval_request(
        approval.approval_id,
        principal_id="web:abc123",
        permissions=(),
        reason="Allowed all web domains from Telegram",
    )
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="web:abc123",
            chat_id="abc123",
            message="/chat can you fetch latest news from bbc.com",
            request_id=None,
            message_id=None,
            created_at=datetime(2026, 5, 2, tzinfo=UTC),
            messages_snapshot=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "can you fetch latest news from bbc.com"}],
                }
            ],
        )
    )

    class Orchestrator:
        principal_id = None

        def resume_turn(self, **kwargs):
            self.principal_id = kwargs["principal_id"]
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Fetched BBC headlines.",
                artifacts=[],
                tool_results=[],
                reached_iteration_limit=False,
            )

    orchestrator = Orchestrator()

    reply = resume_approved_telegram_request(
        runtime,
        approval_id=approval.approval_id,
        chat_id="100001",
        agent_orchestrator=orchestrator,
    )

    assert orchestrator.principal_id == "web:abc123"
    assert reply is not None
    assert "Fetched BBC headlines." in reply
    assert runtime.store.get_suspended_turn(approval.approval_id) is None
    web_turns = runtime.list_conversation_chat_turns("web:abc123")
    assert web_turns[-1] == {
        "user": "can you fetch latest news from bbc.com",
        "assistant": reply,
    }
    assert runtime.list_conversation_chat_turns("telegram:abc123") == []


def test_telegram_callback_routes_resumed_reply_to_origin_channel(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    approval = create_approval_request(
        requested_by="web:abc123",
        action="allow_boundary",
        resource="https://www.cnn.com",
        request_kind="boundary_policy",
        context={},
    )
    runtime.store.add_approval_request(approval)
    runtime.store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="web:abc123",
            chat_id="abc123",
            message="/chat can fetch fresh news from cnn.com",
            request_id=None,
            message_id=None,
            created_at=datetime(2026, 5, 2, tzinfo=UTC),
            messages_snapshot=[],
        )
    )

    assert telegram_app._resume_delivery_channel_for_approval(runtime, approval) == "web"


def _add_doctor_action(runtime, *, action_id="act-test", status="pending", severity="low"):
    runtime.store.add_doctor_action(
        {
            "action_id": action_id,
            "owner": "doctor",
            "status": status,
            "action_type": "diagnosis",
            "recommendation_code": "inspect",
            "summary": "Telegram typing indicator failed",
            "severity": severity,
            "reason": "detail=Failed to send Telegram typing indicator",
            "error": None,
        }
    )


def test_telegram_doctor_callback_is_idempotent_for_closed_action(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    _add_doctor_action(runtime, action_id="act-closed", status="cancelled")
    service = SimpleNamespace(runtime=runtime)

    acknowledgement, reply = telegram_app._execute_decision_action(
        service,
        kind="doctor",
        action="complete",
        record_id="act-closed",
        chat_id="100001",
    )

    assert acknowledgement == "Already closed"
    assert "already cancelled" in reply
    assert "Doctor: Telegram typing indicator failed" in reply


def test_telegram_doctor_notification_dedupes_same_action_id(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    telegram_app._TELEGRAM_NOTIFIED_DOCTOR_ACTION_IDS.clear()
    before = telegram_app.DecisionSnapshot(
        pending_approval_ids=frozenset(),
        pending_builder_proposal_ids=frozenset(),
        pending_doctor_action_ids=frozenset(),
    )
    _add_doctor_action(runtime, action_id="act-duplicate", status="pending", severity="medium")

    first = telegram_app._new_decision_card(runtime, before)
    second = telegram_app._new_decision_card(runtime, before)

    assert first is not None
    assert second is None


def test_telegram_builder_notification_dedupes_same_proposal_id(tmp_path, monkeypatch):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    class Button:
        def __init__(self, label, callback_data):
            self.label = label
            self.callback_data = callback_data

    class Markup:
        def __init__(self, rows):
            self.rows = rows

    monkeypatch.setattr(telegram_app, "InlineKeyboardButton", Button)
    monkeypatch.setattr(telegram_app, "InlineKeyboardMarkup", Markup)
    telegram_app._TELEGRAM_NOTIFIED_BUILDER_PROPOSAL_IDS.clear()
    before = telegram_app.DecisionSnapshot(
        pending_approval_ids=frozenset(),
        pending_builder_proposal_ids=frozenset(),
        pending_doctor_action_ids=frozenset(),
    )
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Check calendar for next weekend",
        summary="Captured from: calendar request",
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Check calendar for next weekend",
        suggested_trigger="Use for next-weekend calendar checks.",
        suggested_steps=("List calendar events.",),
    )
    runtime.store_builder_proposal(proposal, proposal_id="prop-builder", actor="builder_reflector")

    first = telegram_app._new_decision_card(runtime, before)
    second = telegram_app._new_decision_card(runtime, before)

    assert first is not None
    assert first.supplemental is True
    assert first.text.startswith("🛠️ **Builder suggestion**\n\n")
    assert "Builder suggestion" in first.text
    assert "Check calendar for next weekend" in first.text
    callbacks = [button.callback_data for row in first.reply_markup.rows for button in row]
    assert callbacks == [
        "d:p:rv:prop-builder",
        "d:p:ac:prop-builder",
        "d:p:rj:prop-builder",
    ]
    assert second is None


def test_telegram_connector_builder_card_discloses_read_write_permissions(tmp_path, monkeypatch):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )

    class Button:
        def __init__(self, label, callback_data):
            self.label = label
            self.callback_data = callback_data

    class Markup:
        def __init__(self, rows):
            self.rows = rows

    monkeypatch.setattr(telegram_app, "InlineKeyboardButton", Button)
    monkeypatch.setattr(telegram_app, "InlineKeyboardMarkup", Markup)
    telegram_app._TELEGRAM_NOTIFIED_BUILDER_PROPOSAL_IDS.clear()
    before = telegram_app.DecisionSnapshot(
        pending_approval_ids=frozenset(),
        pending_builder_proposal_ids=frozenset(),
        pending_doctor_action_ids=frozenset(),
    )
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Google Drive connector workflow",
        summary=(
            "Create a reusable connector workflow. Permission note: this skill may request read access "
            "through the connector; it does not grant write access by itself."
        ),
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Google Drive connector workflow",
        suggested_trigger="Use for Google Drive requests.",
        suggested_steps=("Request the structured connector route for Google Drive.",),
        suggested_tags=("builder", "connector", "workflow", "connector-app:google-drive"),
    )
    runtime.store_builder_proposal(proposal, proposal_id="prop-drive", actor="builder_reflector")

    card = telegram_app._new_decision_card(runtime, before)

    assert card is not None
    assert card.text.startswith("🛠️ **Builder suggestion**\n\n")
    assert "Permission note" in card.text
    assert "may request read access" in card.text
    assert "does not grant write access by itself" in card.text
    assert "write-capable connector, explicit chat confirmation, and normal approval" in card.text


def test_telegram_proposal_callback_explains_auto_created_skill(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Check calendar for next weekend",
        summary="Captured from: calendar request",
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Check calendar for next weekend",
        suggested_trigger="Use for next-weekend calendar checks.",
        suggested_steps=("List calendar events.",),
    )
    record = runtime.store_builder_proposal(proposal, proposal_id="prop-builder", actor="builder_reflector")
    runtime.accept_stored_builder_skill_proposal_result(record.proposal_id, actor="builder_auto")

    acknowledgement, reply = telegram_app._execute_decision_action(
        SimpleNamespace(runtime=runtime),
        kind="proposal",
        action="accept",
        record_id=record.proposal_id,
        chat_id="100001",
    )

    assert acknowledgement == "Skill ready"
    assert "Builder already created the skill: **Check calendar for next weekend**." in reply
    assert "No second approval is needed" in reply
    assert "That Builder suggestion is already accepted" not in reply


def test_telegram_proposal_callback_explains_reused_skill(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Check calendar for next weekend",
        summary="Captured from: calendar request",
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Check calendar for next weekend",
        suggested_trigger="Use for next-weekend calendar checks.",
        suggested_steps=("List calendar events.",),
    )
    record = runtime.store_builder_proposal(proposal, proposal_id="prop-builder", actor="builder_reflector")
    runtime.create_skill(
        title="Check calendar for next weekend",
        summary="Captured from: calendar request",
        trigger="Use for next-weekend calendar checks.",
        steps=["List calendar events."],
        actor="operator",
    )
    runtime.accept_stored_builder_skill_proposal_result(record.proposal_id, actor="builder_auto")

    acknowledgement, reply = telegram_app._execute_decision_action(
        SimpleNamespace(runtime=runtime),
        kind="proposal",
        action="accept",
        record_id=record.proposal_id,
        chat_id="100001",
    )

    assert acknowledgement == "Skill ready"
    assert "Builder already reused and updated the existing skill: **Check calendar for next weekend**." in reply
    assert "Future matching requests can use that skill now." in reply
    assert "That Builder suggestion is already accepted" not in reply


def test_telegram_connector_builder_accept_explains_saved_skill_next_step(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Google Drive connector workflow",
        summary="Remember how to request a Google Drive connector route.",
        confidence=0.9,
        approval_mode="skill",
        suggested_skill_title="Google Drive connector workflow",
        suggested_trigger="Use for Google Drive requests when a native connector is not loaded.",
        suggested_steps=("Request the structured connector route for Google Drive.",),
        suggested_tags=("builder", "connector", "workflow", "connector-app:google-drive"),
    )
    record = runtime.store_builder_proposal(proposal, proposal_id="prop-drive", actor="builder_reflector")

    acknowledgement, reply = telegram_app._execute_decision_action(
        SimpleNamespace(runtime=runtime),
        kind="proposal",
        action="accept",
        record_id=record.proposal_id,
        chat_id="100001",
    )

    assert acknowledgement == "Accepted"
    assert "Saved skill: Google Drive connector workflow" in reply
    assert "This does not connect the account or rerun the previous request by itself." in reply
    assert "Send the Google Drive task you want next" in reply
    assert runtime.get_builder_proposal(record.proposal_id).status == "accepted"


def test_telegram_doctor_notification_suppresses_low_severity_actions_by_default(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    telegram_app._TELEGRAM_NOTIFIED_DOCTOR_ACTION_IDS.clear()
    before = telegram_app.DecisionSnapshot(
        pending_approval_ids=frozenset(),
        pending_builder_proposal_ids=frozenset(),
        pending_doctor_action_ids=frozenset(),
    )
    _add_doctor_action(runtime, action_id="act-low", status="pending", severity="low")

    assert telegram_app._new_decision_card(runtime, before) is None
    assert runtime.store.get_doctor_action("act-low") is not None

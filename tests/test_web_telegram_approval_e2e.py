from datetime import UTC, datetime
from types import SimpleNamespace

from nullion.approvals import create_approval_request
from nullion.chat_operator import resume_approved_telegram_request
from nullion.chat_store import ChatStore
from nullion.runtime import PersistentRuntime
from nullion.runtime_store import RuntimeStore
from nullion.suspended_turns import SuspendedTurn
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
)
from nullion.web_app import (
    _remember_web_chat_turn,
    _resume_telegram_turn_from_web_approval,
    _sync_runtime_chat_history_to_store,
)


def test_web_runtime_turns_import_into_chat_history(tmp_path):
    store = ChatStore(tmp_path / "chat.db", key_path=tmp_path / "chat.key")

    imported = store.import_runtime_chat_turns(
        [
            {
                "conversation_id": "web:abc123",
                "event_type": "conversation.chat_turn",
                "created_at": "2026-05-02T05:00:00+00:00",
                "user_message": "fetch cnn",
                "assistant_reply": "CNN reply",
            }
        ]
    )

    assert imported == 2
    assert [
        (message["role"], message["text"])
        for message in store.load_messages("web:abc123")
    ] == [("user", "fetch cnn"), ("bot", "CNN reply")]


def test_web_turn_backend_persists_bot_reply_to_chat_history(tmp_path, monkeypatch):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    chat_store = ChatStore(tmp_path / "chat.db", key_path=tmp_path / "chat.key")

    import nullion.chat_store as chat_store_module

    monkeypatch.setattr(chat_store_module, "get_chat_store", lambda: chat_store)

    _remember_web_chat_turn(
        runtime,
        conversation_id="web:abc123",
        user_message="fetch can.com",
        assistant_reply="I fetched can.com, but found no usable news results.",
    )

    assert runtime.store.list_conversation_chat_turns("web:abc123") == [
        {
            "user": "fetch can.com",
            "assistant": "I fetched can.com, but found no usable news results.",
        }
    ]
    assert [
        (message["role"], message["text"])
        for message in chat_store.load_messages("web:abc123")
    ] == [
        ("user", "fetch can.com"),
        ("bot", "I fetched can.com, but found no usable news results."),
    ]


def test_web_request_approved_from_telegram_reappears_in_web_history(tmp_path):
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
            message="/chat fetch cnn",
            request_id=None,
            message_id=None,
            created_at=datetime(2026, 5, 2, tzinfo=UTC),
            messages_snapshot=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "fetch cnn"}],
                }
            ],
        )
    )
    frame = TaskFrame(
        frame_id="frame-web-test",
        conversation_id="web:abc123",
        branch_id="branch-test",
        source_turn_id="turn-test",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.FETCH_RESOURCE,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="fetch cnn",
        created_at=datetime(2026, 5, 2, tzinfo=UTC),
        updated_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("web:abc123", frame.frame_id)

    class Orchestrator:
        def resume_turn(self, **kwargs):
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="CNN reply from resumed web request.",
                artifacts=[],
                tool_results=[],
                reached_iteration_limit=False,
            )

    reply = resume_approved_telegram_request(
        runtime,
        approval_id=approval.approval_id,
        chat_id="telegram-operator",
        agent_orchestrator=Orchestrator(),
    )

    assert "CNN reply from resumed web request." in reply
    updated_frame = runtime.store.get_task_frame(frame.frame_id)
    assert updated_frame.status is TaskFrameStatus.COMPLETED
    assert runtime.store.get_active_task_frame_id("web:abc123") is None

    store = ChatStore(tmp_path / "chat.db", key_path=tmp_path / "chat.key")
    _sync_runtime_chat_history_to_store(runtime, store)

    messages = store.load_messages("web:abc123")
    assert [(message["role"], message["text"]) for message in messages[-2:]] == [
        ("user", "fetch cnn"),
        ("bot", reply),
    ]
    assert store.load_messages("telegram:abc123") == []


async def _noop_send_telegram_delivery(*args, **kwargs):
    _noop_send_telegram_delivery.calls.append((args, kwargs))
    return True


def _reset_noop_send_telegram_delivery():
    _noop_send_telegram_delivery.calls = []


def test_telegram_request_approved_from_web_delivers_back_to_telegram(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    approval = create_approval_request(
        requested_by="telegram_chat",
        action="allow_boundary",
        resource="https://www.caa.com",
        request_kind="boundary_policy",
        context={},
    )
    runtime.store.add_approval_request(approval)
    runtime.approve_approval_request(
        approval.approval_id,
        principal_id="telegram_chat",
        permissions=(),
        reason="Allowed once from web UI",
    )
    suspended_turn = SuspendedTurn(
        approval_id=approval.approval_id,
        conversation_id="telegram:100001",
        chat_id="100001",
        message="/chat fetch caa.com and send me as a text file",
        request_id=None,
        message_id=None,
        created_at=datetime(2026, 5, 2, tzinfo=UTC),
        messages_snapshot=[
            {
                "role": "user",
                "content": [{"type": "text", "text": "fetch caa.com and send me as a text file"}],
            }
        ],
    )
    runtime.store.add_suspended_turn(suspended_turn)

    class Orchestrator:
        def resume_turn(self, **kwargs):
            return SimpleNamespace(
                suspended_for_approval=False,
                final_text="Attached caa.com text file.",
                artifacts=[],
                tool_results=[],
                reached_iteration_limit=False,
            )

    _reset_noop_send_telegram_delivery()

    import asyncio

    payload = asyncio.run(
        _resume_telegram_turn_from_web_approval(
            runtime,
            approval_id=approval.approval_id,
            suspended_turn=suspended_turn,
            orchestrator=Orchestrator(),
            bot_token="token",
            send_telegram_delivery=_noop_send_telegram_delivery,
        )
    )

    assert payload["channel"] == "telegram"
    assert payload["chat_id"] == "100001"
    assert "Attached caa.com text file." in payload["text"]
    assert runtime.store.get_suspended_turn(approval.approval_id) is None
    assert _noop_send_telegram_delivery.calls
    args, kwargs = _noop_send_telegram_delivery.calls[-1]
    assert args[:2] == ("token", "100001")
    assert "Attached caa.com text file." in args[2]
    assert kwargs["principal_id"] == "telegram:100001"
    assert runtime.list_conversation_chat_turns("telegram:100001")[-1] == {
        "user": "fetch caa.com and send me as a text file",
        "assistant": "Attached caa.com text file.",
    }

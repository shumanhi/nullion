"""Exact approvals through the real Web action endpoint and SQLite runtime."""
import os
from dataclasses import replace
from pathlib import Path
import threading

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Container E2E")


def email_fixture(tmp_path, monkeypatch, sender, channel="web"):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import (ToolExecutor, ToolInvocation, ToolRegistry, ToolRiskLevel,
        ToolSideEffectClass, ToolSpec, _build_email_send_handler)
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate

    runtime = isolate(tmp_path, monkeypatch)
    principal = "web:exact-email" if channel == "web" else "telegram:123"
    owned = artifact_root_for_principal(principal)
    owned.mkdir(parents=True, exist_ok=True)
    html = owned / "report.html"
    html.write_text('<p>承認された本文</p><a href="https://example.test/">資料</a>', encoding="utf-8")
    registry = ToolRegistry(filesystem_allowed_roots=[owned])
    registry.register(ToolSpec("email_send", "Send reviewed email", ToolRiskLevel.HIGH,
        ToolSideEffectClass.ACCOUNT_WRITE, True, 10, permission_scope="exact_invocation"),
        _build_email_send_handler(sender))
    runtime.active_tool_registry = registry
    invocation = ToolInvocation("native-email-action", "email_send", principal,
        {"to": "reader@example.test", "subject": "レポート", "html_path": str(html)})
    return runtime, registry, ToolExecutor(store=runtime.store, registry=registry), invocation, html


@pytest.mark.parametrize("changed", [None, "html", "attachment", "legacy"])
def test_approve_email_sends_exact_bytes_or_refuses_mutation(tmp_path, monkeypatch, changed):
    from nullion.web_app import create_app
    sent = []

    def sender(message, attachments):
        sent.append(message.get_body(preferencelist=("html",)).get_content())
        return {"message": "Sent approved email", "message_id": "native-fixture-message"}

    runtime, registry, executor, invocation, html = email_fixture(tmp_path, monkeypatch, sender)
    attachment = html.with_name("figures.txt")
    attachment.write_bytes(b"reviewed attachment")
    invocation = replace(invocation, arguments={**invocation.arguments, "attachment_paths": [str(attachment)]})
    report = executor.invoke(invocation)
    assert report.output["reason"] == "approval_required"
    preview = Path(report.output["html_preview_path"]).read_text(encoding="utf-8")
    assert "承認された本文" in preview
    approval_id = report.output["approval_id"]
    if changed == "html":
        html.write_text("<p>Changed after approval preview.</p>", encoding="utf-8")
    elif changed == "attachment":
        attachment.write_bytes(b"changed attachment")
    elif changed == "legacy":
        approval = runtime.store.get_approval_request(approval_id)
        context = {key: value for key, value in approval.context.items() if key != "reviewed_email_files"}
        runtime.store.add_approval_request(replace(approval, context=context))

    with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client:
        approved = client.post(f"/api/approve/{approval_id}", json={"mode": "once"})
        assert approved.status_code == 200, approved.text
        assert approved.json()["ok"]
        duplicate = client.post(f"/api/approve/{approval_id}", json={"mode": "once"})
        assert duplicate.json()["already_handled"] is True
    if changed:
        assert sent == []
        expected_error = "Request a fresh draft" if changed == "legacy" else "changed after review"
        assert expected_error in approved.json()["resumed_text"]
    else:
        assert len(sent) == 1 and "承認された本文" in sent[0]
        assert "email sent" in approved.json()["resumed_text"].lower()
    expected_reason = "email_source_not_reviewed" if changed == "legacy" else "approval_already_used"
    assert executor.invoke(invocation).output["reason"] == expected_reason


def test_fresh_draft_reviews_changed_file_while_previous_card_is_pending(tmp_path, monkeypatch):
    from nullion.web_app import create_app

    sent = []
    def sender(message, attachments):
        sent.append(message.get_body(preferencelist=("html",)).get_content())
        return {"message_id": "fixture-new-draft"}

    runtime, registry, executor, invocation, html = email_fixture(tmp_path, monkeypatch, sender)
    original = executor.invoke(invocation).output
    html.write_text("<p>改訂版</p>", encoding="utf-8")
    revised = executor.invoke(replace(invocation, invocation_id="revised-email-action")).output
    assert original["approval_id"] != revised["approval_id"]
    assert "改訂版" in Path(revised["html_preview_path"]).read_text(encoding="utf-8")
    assert runtime.store.get_approval_request(original["approval_id"]).status.value == "pending"
    with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client:
        response = client.post(f"/api/approve/{revised['approval_id']}", json={"mode": "once"})
        assert response.json()["ok"], response.text
    assert len(sent) == 1 and "改訂版" in sent[0]


def test_web_to_telegram_approval_keeps_health_responsive_and_resumes_without_model(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from datetime import UTC, datetime
    from nullion.runtime_store import SuspendedTurn
    from nullion.web_app import create_app
    import nullion.telegram_entrypoint as entrypoint

    entered, release = threading.Event(), threading.Event()
    sent, delivered = [], []

    def sender(message, attachments):
        entered.set()
        assert release.wait(8)
        sent.append(message.get_body(preferencelist=("html",)).get_content())
        return {"message": "Sent approved email", "message_id": "native-fixture-message"}

    async def deliver(token, chat_id, text, **kwargs):
        delivered.append(text)
        return True

    runtime, registry, executor, invocation, _ = email_fixture(tmp_path, monkeypatch, sender, "telegram")
    requested = executor.invoke(invocation)
    approval_id = requested.output["approval_id"]
    # Existing approvals can have a missing model snapshot or incomplete
    # pending receipts. The persisted native approval still owns the exact send.
    runtime.store.add_suspended_turn(SuspendedTurn(approval_id=approval_id,
        conversation_id=invocation.principal_id, chat_id="123", message="資料を送信してください。",
        request_id=None, message_id=None, created_at=datetime.now(UTC)))
    runtime.checkpoint()
    monkeypatch.setenv("NULLION_TELEGRAM_BOT_TOKEN", "fixture-only")
    monkeypatch.setattr(entrypoint, "_send_operator_telegram_delivery", deliver)
    assert runtime.store.get_approval_request(approval_id).status.value == "pending"
    conversation_id = "web:approval-desk"
    with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client, ThreadPoolExecutor() as pool:
        call = pool.submit(client.post, f"/api/approve/{approval_id}", json={"mode": "once", "progress_conversation_id": conversation_id})
        try:
            assert entered.wait(5)
            assert pool.submit(client.get, "/api/health").result(timeout=2).status_code == 200
            progress = client.get("/api/chat/progress", params={
                "conversation_id": conversation_id, "turn_id": f"approval:{approval_id}",
            })
            assert progress.json()["active"] and "approved action" in progress.json()["text"]
        finally:
            release.set()
        response = call.result(timeout=8)
    assert response.json()["ok"] and len(delivered) == 1
    assert len(sent) == 1 and "承認された本文" in sent[0]


@pytest.mark.parametrize("suspended", [False, True])
def test_web_approval_publishes_owned_progress_during_delivery(tmp_path, monkeypatch, suspended):
    from concurrent.futures import ThreadPoolExecutor
    from datetime import UTC, datetime
    from nullion.runtime_store import SuspendedTurn
    from nullion.web_app import create_app

    entered, release = threading.Event(), threading.Event()

    def sender(message, attachments):
        entered.set()
        assert release.wait(8)
        return {"message": "Sent approved email", "message_id": "native-fixture-message"}

    runtime, registry, executor, invocation, _ = email_fixture(tmp_path, monkeypatch, sender)
    approval_id = executor.invoke(invocation).output["approval_id"]
    if suspended:
        runtime.store.add_suspended_turn(SuspendedTurn(
            approval_id=approval_id, conversation_id=invocation.principal_id, chat_id=None,
            message="資料を送信してください。", request_id=None, message_id=None, created_at=datetime.now(UTC),
        ))
    params = {"conversation_id": invocation.principal_id, "turn_id": f"approval:{approval_id}"}
    with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client, ThreadPoolExecutor() as pool:
        call = pool.submit(client.post, f"/api/approve/{approval_id}", json={"mode": "once"})
        try:
            assert entered.wait(5)
            progress = pool.submit(client.get, "/api/chat/progress", params=params).result(timeout=2)
            assert progress.status_code == 200
            assert progress.json()["active"] and "approved action" in progress.json()["text"]
            other = client.get("/api/chat/progress", params={**params, "conversation_id": "web:other"})
            assert other.json()["active"] is False
        finally:
            release.set()
        response = call.result(timeout=8)
        assert "email sent" in response.json()["resumed_text"].lower()
        assert client.get("/api/chat/progress", params=params).json()["active"] is False


def test_web_rejection_keeps_health_responsive_and_denies_review(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from nullion.web_app import create_app

    def unexpected_send(*args):
        pytest.fail("Declining a review must not send the draft")

    runtime, registry, executor, invocation, _ = email_fixture(tmp_path, monkeypatch, unexpected_send)
    approval_id = executor.invoke(invocation).output["approval_id"]
    entered, release = threading.Event(), threading.Event()
    checkpoint = type(runtime).checkpoint

    def slow_checkpoint(self, **kwargs):
        if self is runtime and runtime.store.get_approval_request(approval_id).status.value == "denied":
            entered.set()
            assert release.wait(8)
        return checkpoint(self, **kwargs)

    monkeypatch.setattr(type(runtime), "checkpoint", slow_checkpoint)
    with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client, ThreadPoolExecutor() as pool:
        call = pool.submit(client.post, f"/api/reject/{approval_id}")
        try:
            assert entered.wait(5)
            assert pool.submit(client.get, "/api/health").result(timeout=2).status_code == 200
        finally:
            release.set()
        assert call.result(timeout=8).json()["ok"]
    assert runtime.store.get_approval_request(approval_id).status.value == "denied"


@pytest.mark.parametrize("first_action", ["approve", "reject"])
def test_web_approval_and_rejection_have_one_winner(tmp_path, monkeypatch, first_action):
    from concurrent.futures import ThreadPoolExecutor
    import nullion.runtime as decision_runtime
    from nullion.web_app import create_app

    sent = []
    def sender(message, attachments):
        sent.append(message["Subject"])
        return {"message_id": "fixture-decision-race"}

    runtime, registry, executor, invocation, _ = email_fixture(tmp_path, monkeypatch, sender)
    approval_id = executor.invoke(invocation).output["approval_id"]
    entered, release, competing = threading.Event(), threading.Event(), threading.Event()
    other_action = "reject" if first_action == "approve" else "approve"
    first_transition = "approve" if first_action == "approve" else "deny"
    other_transition = "approve_approval_request" if other_action == "approve" else "deny_approval_request"
    transition = getattr(decision_runtime, first_transition)
    other_transition_func = getattr(decision_runtime, other_transition)

    def pause_transition(*args, **kwargs):
        entered.set()
        assert release.wait(8)
        return transition(*args, **kwargs)

    def observe_competing_request(*args, **kwargs):
        competing.set()
        return other_transition_func(*args, **kwargs)

    monkeypatch.setattr(decision_runtime, first_transition, pause_transition)
    monkeypatch.setattr(decision_runtime, other_transition, observe_competing_request)
    with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client, ThreadPoolExecutor() as pool:
        first = pool.submit(client.post, f"/api/{first_action}/{approval_id}", json={"mode": "once"})
        try:
            assert entered.wait(5)
            second = pool.submit(client.post, f"/api/{other_action}/{approval_id}", json={"mode": "once"})
            assert competing.wait(5)
            assert not second.done()
        finally:
            release.set()
        assert first.result(timeout=8).status_code == 200
        assert second.result(timeout=8).status_code == 409

    expected = "approved" if first_action == "approve" else "denied"
    assert runtime.store.get_approval_request(approval_id).status.value == expected
    assert len(sent) == (1 if expected == "approved" else 0)


@pytest.mark.parametrize("checkpoint_suffix", [".db", ".json"])
def test_allow_once_does_not_cover_second_action_and_two_drafts_do_not_reconcile(tmp_path, monkeypatch, checkpoint_suffix):
    from nullion.approval_decisions import approve_request_with_mode
    from nullion.tools import ToolExecutor, ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
    from tests.e2e.test_quoted_reply_review_and_cleanup import isolate

    runtime = isolate(tmp_path, monkeypatch)
    from nullion.runtime import bootstrap_persistent_runtime
    if checkpoint_suffix == ".json":
        runtime = bootstrap_persistent_runtime(tmp_path / "runtime.json")
    registry = ToolRegistry()
    writes = []

    def provider(invocation):
        writes.append(invocation.arguments["summary"])
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"ok": True})

    registry.register(ToolSpec("write_reviewed_event", "Create calendar event", ToolRiskLevel.HIGH,
        ToolSideEffectClass.ACCOUNT_WRITE, True, 5), provider)
    executor = ToolExecutor(store=runtime.store, registry=registry)
    a = ToolInvocation("event-a", "write_reviewed_event", "web:exact-calendar", {"summary": "会議 A"})
    b = replace(a, invocation_id="event-b", arguments={"summary": "会議 B"})
    a_review, b_review = executor.invoke(a), executor.invoke(b)
    assert a_review.output["approval_id"] != b_review.output["approval_id"]
    approve_request_with_mode(runtime, a_review.output["approval_id"], source="web", mode="once")
    assert runtime.reconcile_effectively_approved_pending_approvals() == []
    assert runtime.store.get_approval_request(b_review.output["approval_id"]).status.value == "pending"
    assert executor.invoke(b).status == "denied"
    completed = executor.invoke(a)
    assert completed.status == "completed", completed.error
    assert executor.invoke(a).output["reason"] == "approval_already_used"
    restarted = bootstrap_persistent_runtime(runtime.checkpoint_path)
    assert ToolExecutor(store=restarted.store, registry=registry).invoke(a).output["reason"] == "approval_already_used"
    assert writes == ["会議 A"]


def test_telegram_approval_shows_progress_before_exact_email_delivery(tmp_path, monkeypatch):
    import asyncio
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from nullion.runtime_store import SuspendedTurn
    from nullion.config import NullionSettings, TelegramSettings
    from nullion import telegram_app

    progress_published = threading.Event()
    views, card_edits, sent = [], [], []

    def sender(message, attachments):
        assert progress_published.wait(8), "Approval did not publish progress while the provider was working"
        sent.append(message.get_body(preferencelist=("html",)).get_content())
        return {"message": "Sent approved email", "message_id": "native-fixture-message"}

    runtime, _, executor, invocation, _ = email_fixture(tmp_path, monkeypatch, sender, "telegram")
    approval_id = executor.invoke(invocation).output["approval_id"]
    runtime.store.add_suspended_turn(SuspendedTurn(approval_id=approval_id,
        conversation_id=invocation.principal_id, chat_id="123", message="資料を送ってください。",
        request_id=None, message_id=None, created_at=datetime.now(UTC)))
    runtime.checkpoint()

    class Notice:
        async def edit_text(self, text, **kwargs):
            views.append(text)

        async def delete(self):
            pass

    class Message:
        chat = SimpleNamespace(id=123, send_action=lambda **_: asyncio.sleep(0))
        message_id = 7

        async def edit_text(self, text, **kwargs):
            card_edits.append(text)

        async def reply_text(self, text, **kwargs):
            views.append(text)
            if "Continuing the approved action" in text:
                progress_published.set()
            return Notice()

    class Query:
        message = Message()
        from_user = SimpleNamespace(id=123)
        data = telegram_app._build_callback_data(kind="approval", action="allow_once", record_id=approval_id)

        async def answer(self, *args, **kwargs):
            pass

    settings = NullionSettings(telegram=TelegramSettings(
        bot_token="123456:synthetic", operator_chat_id="123", chat_enabled=True))
    service = telegram_app.ChatOperatorService(runtime, "123456:synthetic", "123", settings)
    asyncio.run(service.on_callback_query(SimpleNamespace(callback_query=Query()), None))
    assert len(sent) == 1 and "承認された本文" in sent[0]
    assert any("Continuing the approved action" in text for text in views)
    assert any("email sent" in text.lower() for text in views)
    assert any("email" in text.lower() for text in card_edits)
    assert not any("Approved web access" in text for text in card_edits)

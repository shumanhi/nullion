"""Exact approvals through the real Web action endpoint and SQLite runtime."""
import json
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


@pytest.mark.parametrize("mode", ["native", "compacted", "missing_and_timeout", "web_missing",
    "missing_journal", "web_missing_journal", "foreign_journal", "used_journal",
    "telegram_report", "web_report", "telegram_mixed", "web_mixed"])
def test_scheduled_email_approval_sends_the_original_draft_once(tmp_path, monkeypatch, mode):
    import json

    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.chat_operator import resume_approved_telegram_request
    from nullion import cron_execution_tools
    from nullion.crons import CronJob
    from nullion.runtime import bootstrap_persistent_runtime
    from test_scheduled_report_completion import run_scheduled, telegram_boundary, text_response, tool_response
    from tests.workflow_provider_protocol import workflow_provider_reply

    sent = []

    def sender(message, attachments):
        sent.append((message["Subject"], message.get_body(preferencelist=("html",)).get_content()))
        return {"message_id": "scheduled-email-receipt"}

    channel = "web" if mode.startswith("web_") else "telegram"
    use_code_mode = mode != "native"
    receipt_only = mode in {"telegram_report", "web_report"}
    mixed_delivery = mode in {"telegram_mixed", "web_mixed"}
    report_reply = receipt_only or mixed_delivery
    telegram_callback = mode in {"telegram_report", "telegram_mixed"}
    runtime, registry, _, invocation, _ = email_fixture(tmp_path, monkeypatch, sender, channel)
    alter_journal = mode in {"missing_journal", "web_missing_journal", "foreign_journal", "used_journal"}
    if alter_journal or report_reply:
        registry.get_spec("email_send").capability_tags = ("connector", "email", "account_write")
    monkeypatch.setenv("NULLION_CODE_MODE", "on" if use_code_mode else "off")
    monkeypatch.setenv("NULLION_MODEL_TOOL_INPUT_HISTORY_MAX_CHARS", "2000")
    monkeypatch.setattr(cron_execution_tools, "_connected_connector_provider_summaries", lambda _: ({
        "provider_id": "fixture-mail",
        "display_name": "Fixture Mail",
        "permission_mode": "write",
    },))
    body = ('<!doctype html><html><head><meta charset="utf-8"><title>日次レポート</title></head>'
        '<body><p>承認された日次レポート</p><p>確認済みの記録 ' + 'x' * 3000 + '</p></body></html>')
    draft = {"to": "reader@example.test", "subject": "日次レポート", "html_body": body}
    job = CronJob(id="daily-email", name="日次レポート", schedule="0 * * * *",
        task="確認済みの日次レポートをメールで送信してください。"
        + ("このチャットにも要点を表示してください。" if mixed_delivery else ""),
        delivery_channel=channel, delivery_target=invocation.principal_id if channel == "web" else "123")

    class StartModel:
        def create(self, **kwargs):
            system = str(kwargs.get("system") or "")
            if "available_connector_providers" in system and "response_schema" in system:
                return text_response(json.dumps({"requires_account_connector": True,
                    "provider_ids": ["fixture-mail"], "confidence": 1,
                    "write_tool_names": ["email_send"], "delivery_mode": "report_each_run",
                    **({"chat_delivery_mode": "confirmation_only" if receipt_only else "report"}
                       if report_reply else {})}))
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            if use_code_mode:
                return tool_response("code_mode", {"tool_names": ["email_send"],
                    "code": f"call_tool('email_send', {draft!r})"})
            return tool_response("email_send", draft)

    deliveries, send = telegram_boundary(monkeypatch)
    pending = run_scheduled(job, StartModel(), runtime, registry, send,
        agent_conversation_id=invocation.principal_id)
    assert pending.get("suspended_for_approval"), pending
    approval_id = pending["approval_id"]
    approval = runtime.store.get_approval_request(approval_id)
    assert approval.requested_by == invocation.principal_id
    assert approval.context["tool_arguments"] == draft
    if report_reply:
        assert approval.context["trigger_flow"]["scheduled_chat_delivery_mode"] == (
            "confirmation_only" if receipt_only else "report"
        )
    preview = Path(approval.context["html_preview_path"]).read_text(encoding="utf-8")
    assert body in preview
    assert not sent and not deliveries
    if use_code_mode:
        suspended = runtime.store.get_suspended_turn(approval_id)
        block = suspended.messages_snapshot[-1]["content"][-1]
        assert block["input"]["history_compaction"]["original_json_chars"] > 2000
        if mode in {"missing_and_timeout", "web_missing"}:
            runtime.store.add_suspended_turn(replace(suspended, messages_snapshot=None))
    runtime.checkpoint(force=True)
    if alter_journal:
        import sqlite3
        from nullion.code_mode_sessions import checkpoint_reference

        flow = approval.context["trigger_flow"]
        assert flow["scheduled_task_run"] is True
        assert flow["conversation_id"] == suspended.conversation_id
        assert flow["planned_action_tool_names"] == ["email_send"], flow["planned_action_tool_names"]
        assert flow["requires_artifact_delivery"] is False
        assert any(row.get("invocation_id") == approval.context["approval_invocation_id"]
                   and row.get("status") == "denied" for row in suspended.pending_tool_calls), [
            (row.get("tool_name"), row.get("status"), row.get("invocation_id")) for row in suspended.pending_tool_calls]
        identity = checkpoint_reference(suspended.pending_tool_calls)
        assert identity
        with sqlite3.connect(runtime.checkpoint_path) as db:
            if mode == "foreign_journal":
                cursor = db.execute("UPDATE code_mode_sessions SET payload=json_set(payload,'$.owner[0]',?) WHERE id=?",
                    ("another-owner", identity))
            else:
                cursor = db.execute("DELETE FROM code_mode_sessions WHERE id=?", (identity,))
            assert cursor.rowcount == 1
    restarted = bootstrap_persistent_runtime(runtime.checkpoint_path)
    restarted.active_tool_registry = registry
    if channel == "telegram" and not telegram_callback:
        restarted.approve_approval_request(approval_id, principal_id=invocation.principal_id,
            permissions=["tool:email_send"])
    if mode == "used_journal":
        from nullion.approval_resume import claim_approved_tool_use
        assert claim_approved_tool_use(restarted.store, approval_id)

    emitted_reports = []

    class ResumeModel:
        def create(self, **kwargs):
            assert not alter_journal
            assert sent, "The exact reviewed send must run before any model call"
            protocol = workflow_provider_reply(kwargs, completion=True)
            if protocol is not None:
                return protocol
            if mode == "missing_and_timeout":
                raise TimeoutError("Synthetic timeout after acknowledged send")
            if report_reply:
                emitted_reports.append(True)
                return text_response(json.dumps({"format": "nullion.report.v1",
                    "text": "確認済みの航空券レポート",
                    "message": {"summary": "確認済みの航空券レポート", "detail_level": "compact",
                        "sections": [], "sources": [], "shared_details": [],
                        "footer": "比較結果と運賃の内訳。 Message ID: scheduled-email-receipt"}}, ensure_ascii=False))
            return text_response("Email sent.")

    runner = AgentOrchestrator(model_client=ResumeModel())
    if channel == "web":
        from nullion.web_app import create_app

        with TestClient(create_app(restarted, runner, registry)) as client:
            result = client.post(f"/api/approve/{approval_id}", json={"mode": "once"})
            assert result.json()["ok"], result.text
            reply = result.json()["resumed_text"]
            if receipt_only:
                assert result.json()["resume"]["artifacts"] == []
            duplicate = client.post(f"/api/approve/{approval_id}", json={"mode": "once"})
            assert duplicate.json()["already_handled"] is True
    elif telegram_callback:
        import asyncio
        from types import SimpleNamespace
        from nullion import telegram_app
        from nullion.config import NullionSettings, TelegramSettings

        class Message:
            chat = SimpleNamespace(id=123, send_action=lambda **_: asyncio.sleep(0))
            message_id = 7

            async def edit_text(self, text, **kwargs):
                pass

            async def reply_text(self, text, **kwargs):
                deliveries.append(text)
                return self

            async def delete(self):
                pass

        async def answer(*args, **kwargs):
            pass

        query = SimpleNamespace(message=Message(), from_user=SimpleNamespace(id=123), answer=answer,
            data=telegram_app._build_callback_data(kind="approval", action="allow_once", record_id=approval_id))
        settings = NullionSettings(telegram=TelegramSettings(
            bot_token="123456:synthetic", operator_chat_id="123", chat_enabled=True))
        service = telegram_app.ChatOperatorService(restarted, "123456:synthetic", "123", settings,
            agent_orchestrator=runner)
        asyncio.run(service.on_callback_query(SimpleNamespace(callback_query=query), None))
        asyncio.run(service.on_callback_query(SimpleNamespace(callback_query=query), None))
        reply = deliveries[-1]
        if receipt_only:
            assert sum("Email sent to" in text for text in deliveries) == 1, deliveries
    else:
        reply = resume_approved_telegram_request(restarted, approval_id=approval_id, chat_id="123",
            agent_orchestrator=runner)
    if mode == "foreign_journal":
        assert "couldn’t resume the saved program" in reply
        assert not sent
        return
    if mode == "used_journal":
        assert "already used" in reply
        assert not sent
        return
    if mode == "missing_and_timeout":
        assert "The follow-up response timed out." in reply
    if report_reply:
        assert emitted_reports
    if receipt_only:
        from html import unescape

        assert unescape(reply).strip().removeprefix("✉️ ") == (
            'Email sent to reader@example.test with subject "日次レポート".'
        )
    if mixed_delivery:
        assert "比較結果と運賃の内訳" in reply
    else:
        assert "email sent" in reply.lower()
    assert [(subject, html.strip()) for subject, html in sent] == [(draft["subject"], preview.strip())]
    assert any(event.payload.get("tool_name") == "email_send" and event.payload.get("status") == "completed"
        and event.payload.get("invocation_id") == approval.context["approval_invocation_id"]
        for event in restarted.store.list_events())
    assert len(restarted.store.list_approval_requests()) == 1
    if channel == "telegram":
        assert resume_approved_telegram_request(restarted, approval_id=approval_id, chat_id="123",
            agent_orchestrator=runner) is None
    assert len(sent) == 1


def test_email_approval_is_claimed_before_provider_call_across_runtime_instances(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from nullion.approval_decisions import approve_request_with_mode
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolExecutor

    entered, release = threading.Event(), threading.Event()
    sent = []

    def sender(message, attachments):
        sent.append(message["Subject"])
        entered.set()
        assert release.wait(8)
        return {"message_id": "single-delivery"}

    runtime, registry, executor, invocation, _ = email_fixture(tmp_path, monkeypatch, sender)
    approval_id = executor.invoke(invocation).output["approval_id"]
    approve_request_with_mode(runtime, approval_id, source="web", mode="once")
    runtime.checkpoint(force=True)
    second = bootstrap_persistent_runtime(runtime.checkpoint_path)
    second_executor = ToolExecutor(store=second.store, registry=registry)

    with ThreadPoolExecutor() as pool:
        winner = pool.submit(executor.invoke, invocation)
        try:
            assert entered.wait(5)
            duplicate = second_executor.invoke(invocation)
            assert duplicate.output.get("reason") == "approval_already_used"
            assert sent == ["レポート"]
        finally:
            release.set()
        assert winner.result(timeout=8).status == "completed"

    unrelated = second_executor.invoke(replace(invocation, invocation_id="unrelated-email-action"))
    assert unrelated.output["reason"] == "approval_required"
    assert unrelated.output["approval_id"] != approval_id
    assert sent == ["レポート"]


@pytest.mark.asyncio
async def test_deep_agent_exact_email_resumes_once_after_restart(tmp_path, monkeypatch):
    from nullion.deep_agent_runner import _pending_approval_from_tool_results
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    from nullion.runtime import bootstrap_persistent_runtime

    sent = []

    def sender(message, _attachments):
        sent.append(message.get_body(preferencelist=("html",)).get_content())
        return {"message_id": "fixture-deep-agent-email"}

    runtime, registry, _, invocation, _ = email_fixture(tmp_path, monkeypatch, sender)
    monkeypatch.setenv("NULLION_CODE_MODE", "on")
    flow = {"conversation_id": invocation.principal_id, "original_user_message": "確認してメールを送信"}
    observed = []

    def harness(store, session=None):
        return nullion_tools_as_langchain_tools(registry, allowed_tools=["email_send"],
            principal_id=invocation.principal_id, cleanup_scope="exact-email-fixture",
            policy_store=store, tool_result_callback=observed.append,
            tool_flow_context={**flow, "code_mode_session_id": session})[0]

    program = harness(runtime.store)
    with pytest.raises(ValueError, match="Unexpected Code Mode arguments"):
        await program.ainvoke({"tool_names": ["email_send"], "code": "1",
            "_approved_call": {"tool_name": "email_send", "invocation_id": invocation.invocation_id}})
    assert not runtime.store.list_approval_requests() and not sent
    arguments = {**invocation.arguments, "to": ("reader@example.test",)}
    response = await program.ainvoke({"tool_names": ["email_send"],
        "code": f"call_tool('email_send', {arguments!r})"})
    assert json.loads(response)["output"]["reason"] == "code_mode_awaiting_approval"
    pending = _pending_approval_from_tool_results(observed)
    assert pending and not sent
    approval_id = pending["approval_id"]
    approval = runtime.store.get_approval_request(approval_id)
    session = json.loads(response)["output"]["session_id"]
    runtime.checkpoint(force=True)
    restarted = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    restarted.approve_approval_request(approval_id, principal_id=invocation.principal_id,
        decision_mode="once", permissions=["tool:email_send"])
    observed.clear()
    resumed = harness(restarted.store, session)
    await resumed.coroutine._nullion_code_mode_resume()
    assert len(sent) == 1 and "承認された本文" in sent[0]
    assert [a.approval_id for a in restarted.store.list_approval_requests()] == [approval_id]
    assert _pending_approval_from_tool_results(observed) is None
    assert any(r.status == "completed" and r.invocation_id == approval.context["approval_invocation_id"] for r in observed)
    with pytest.raises(RuntimeError, match="not resumable"):
        await resumed.coroutine._nullion_code_mode_resume()
    assert len(sent) == 1

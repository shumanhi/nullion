from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from nullion import chat_operator, telegram_app, web_app
from nullion.agent_turn_limits import AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND, max_iterations_from_resume_token
from nullion.approval_decisions import approve_request_with_mode
from nullion.runtime import PersistentRuntime
from nullion.runtime_store import RuntimeStore
from nullion.suspended_turns import SuspendedTurn
from nullion.tools import ToolRegistry
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


def _runtime(tmp_path):
    return PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 6, 5, tzinfo=UTC),
    )


def test_chat_operator_stores_doctor_limit_extension_approval(tmp_path) -> None:
    runtime = _runtime(tmp_path)

    approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
        runtime,
        conversation_id="telegram:123",
        chat_id="123",
        message="/chat build the workbook",
        request_id="req-1",
        message_id="msg-1",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "build the workbook"}]}],
        current_max_iterations=12,
        tool_results=[],
        required_attachment_extensions=("xlsx",),
    )

    approval = runtime.store.get_approval_request(approval_id)
    suspended = runtime.store.get_suspended_turn(approval_id)

    assert approval is not None
    assert approval.request_kind == AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND
    assert approval.context["presented_by"] == "doctor"
    assert suspended is not None
    assert suspended.resume_token["current_max_iterations"] == 12


def test_telegram_limit_extension_callback_sets_resume_budget(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
        runtime,
        conversation_id="telegram:123",
        chat_id="123",
        message="/chat build the workbook",
        request_id="req-1",
        message_id="msg-1",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "build the workbook"}]}],
        current_max_iterations=12,
        tool_results=[],
    )
    service = SimpleNamespace(runtime=runtime)

    acknowledgement, reply = telegram_app._execute_decision_action(
        service,
        kind="approval",
        action="extend_limit_5x",
        record_id=approval_id,
    )
    suspended = runtime.store.get_suspended_turn(approval_id)

    assert acknowledgement == "Doctor approved 5x tool budget"
    assert "Continuing" in reply
    assert "Approved continuation budget: 60 model rounds" in reply
    assert "Previous budget: 12 model rounds" in reply
    assert "Current turn budget: 12" not in reply
    assert runtime.store.get_approval_request(approval_id).context["approved_max_iterations"] == 60
    assert max_iterations_from_resume_token(suspended.resume_token) == 60
    assert runtime.store.get_approval_request(approval_id).status.value == "approved"


def test_browser_scoped_limit_extension_uses_larger_resume_baseline(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("NULLION_AGENT_TURN_MAX_ITERATIONS", raising=False)
    monkeypatch.delenv("NULLION_BROWSER_AGENT_TURN_MAX_ITERATIONS", raising=False)
    runtime = _runtime(tmp_path)
    browser_registry = ScopedTurnToolRegistry(
        ToolRegistry(),
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            web_action="browser_interaction",
            requested_tool_names=("browser_navigate", "browser_extract_text"),
            confidence=1.0,
            valid=True,
        ),
    )
    approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
        runtime,
        conversation_id="telegram:browser",
        chat_id="123",
        message="/chat check https://example.test/orders",
        request_id="req-browser",
        message_id="msg-browser",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "check https://example.test/orders"}]}],
        current_max_iterations=chat_operator._chat_agent_turn_max_iterations(browser_registry),
        tool_results=[],
    )
    service = SimpleNamespace(runtime=runtime)

    acknowledgement, reply = telegram_app._execute_decision_action(
        service,
        kind="approval",
        action="extend_limit_2x",
        record_id=approval_id,
    )
    suspended = runtime.store.get_suspended_turn(approval_id)

    assert chat_operator._chat_agent_turn_max_iterations(ToolRegistry()) == 48
    assert acknowledgement == "Doctor approved 2x tool budget"
    assert "Continuing" in reply
    assert suspended.resume_token["current_max_iterations"] == 48
    assert max_iterations_from_resume_token(suspended.resume_token) == 96


def test_multi_artifact_scope_gets_complex_turn_budget_without_slowing_plain_turns(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "2")
    monkeypatch.delenv("NULLION_COMPLEX_AGENT_TURN_MAX_ITERATIONS", raising=False)
    artifact_registry = ScopedTurnToolRegistry(
        ToolRegistry(),
        evidence=TurnToolEvidence(requested_extensions=(".xlsx", ".pdf", ".html")),
        tool_scope_decision=TurnToolScopeDecision(
            requested_artifact_extensions=(".xlsx", ".pdf", ".html"),
            confidence=1.0,
            valid=True,
        ),
    )

    assert chat_operator._chat_agent_turn_max_iterations(ToolRegistry()) == 2
    assert chat_operator._chat_agent_turn_max_iterations(artifact_registry) == 48


def test_consumed_telegram_limit_extension_callback_is_idempotent(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
        runtime,
        conversation_id="telegram:123",
        chat_id="123",
        message="/chat build the workbook",
        request_id="req-1",
        message_id="msg-1",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "build the workbook"}]}],
        current_max_iterations=12,
        tool_results=[],
    )
    approve_request_with_mode(runtime, approval_id, mode="limit_10x", source="Telegram")
    runtime.store.remove_suspended_turn(approval_id)
    service = SimpleNamespace(runtime=runtime)

    acknowledgement, reply = telegram_app._execute_decision_action(
        service,
        kind="approval",
        action="extend_limit_10x",
        record_id=approval_id,
    )

    assert acknowledgement == "Already handled"
    assert "already approved" in reply
    assert runtime.store.get_suspended_turn(approval_id) is None


def test_consumed_limit_extension_does_not_replay_active_prompt(tmp_path, monkeypatch) -> None:
    runtime = _runtime(tmp_path)
    approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
        runtime,
        conversation_id="telegram:123",
        chat_id="123",
        message="/chat build the workbook",
        request_id="req-1",
        message_id="msg-1",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "build the workbook"}]}],
        current_max_iterations=12,
        tool_results=[],
    )
    approve_request_with_mode(runtime, approval_id, mode="limit_10x", source="Telegram")
    runtime.store.remove_suspended_turn(approval_id)
    monkeypatch.setattr(chat_operator, "_active_task_frame_prompt", lambda *args, **kwargs: "build the workbook")

    def forbidden_replay(*args, **kwargs):
        raise AssertionError("consumed tool-budget approval must not replay active prompt")

    monkeypatch.setattr(chat_operator, "_render_chat_turn", forbidden_replay)

    assert chat_operator.resume_approved_telegram_request(
        runtime,
        approval_id=approval_id,
        chat_id="123",
    ) is None


def test_web_consumed_limit_extension_returns_already_handled(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.tools import ToolRegistry

    runtime = _runtime(tmp_path)
    approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
        runtime,
        conversation_id="web:operator",
        chat_id=None,
        message="/chat build the workbook",
        request_id="req-1",
        message_id="msg-1",
        messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "build the workbook"}]}],
        current_max_iterations=12,
        tool_results=[],
    )
    approve_request_with_mode(runtime, approval_id, mode="limit_10x", source="web UI")
    runtime.store.remove_suspended_turn(approval_id)
    app = web_app.create_app(runtime, orchestrator=object(), registry=ToolRegistry())

    response = TestClient(app).post(f"/api/approve/{approval_id}", json={"mode": "limit_10x"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["already_handled"] is True
    assert payload["resume"] is None
    assert payload["resumed_text"] is None


def test_web_limit_extension_payload_preserves_request_kind() -> None:
    payload = web_app._web_approval_required_payload(
        {
            "approval_id": "approval-limit",
            "tool_name": "doctor_extend_agent_turn_limit",
            "tool_detail": "Current turn budget: 12",
            "request_kind": AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
        }
    )

    assert payload["type"] == "approval_required"
    assert payload["request_kind"] == AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND
    assert payload["text"]
    assert "tool budget" in payload["text"]


def test_resume_token_without_multiplier_does_not_extend() -> None:
    suspended = SuspendedTurn(
        approval_id="approval-limit",
        conversation_id="telegram:123",
        chat_id="123",
        message="/chat build the workbook",
        request_id="req-1",
        message_id="msg-1",
        created_at=datetime(2026, 6, 5, tzinfo=UTC),
        resume_token={"reason": AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND, "current_max_iterations": 12},
    )

    assert max_iterations_from_resume_token(suspended.resume_token) is None


def test_repeated_budget_decision_preserves_approved_budget_and_confirmation(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
        runtime, conversation_id="telegram:budget", chat_id="budget", message="次へ", request_id="budget",
        message_id="budget", messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "次へ"}]}],
        current_max_iterations=24, tool_results=[])
    service = SimpleNamespace(runtime=runtime)
    telegram_app._execute_decision_action(service, kind="approval", action="extend_limit_5x", record_id=approval_id)
    _, repeated_reply = telegram_app._execute_decision_action(service, kind="approval", action="extend_limit_2x", record_id=approval_id)
    assert max_iterations_from_resume_token(runtime.store.get_suspended_turn(approval_id).resume_token) == 120
    assert "Doctor approved 5x" in repeated_reply
    assert "Approved continuation budget: 120 model rounds" in repeated_reply
    approval = runtime.store.get_approval_request(approval_id)
    web_reply = web_app._approval_confirmation_text_for_web(approval, action="approve", mode="limit_2x")
    assert "Doctor approved 5X" in web_reply
    assert "Approved continuation budget: 120 model rounds" in web_reply

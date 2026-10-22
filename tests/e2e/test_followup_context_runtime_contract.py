"""Real Telegram handler, routing, LangGraph tools, SQLite and PDF delivery.

Only the external model and flight search service are deterministic fixtures.
"""
import json
import os
import time
from pathlib import Path
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E only")

PARENT = "Can you search couple of direct flights to see which one might be cheaper for a weekend this month (except the next one) ..leave wed..come back Sunday. Nyc to SF create a pdf for easy delivery"
REPORT = "NYC to San Francisco nonstop, Wednesday to Sunday.\nOctober 14-18: USD 643.\nOctober 21-25: USD 521.\nOctober 28-November 1: USD 487.\nCheapest: October 28-November 1; the Sunday return is in November."


@pytest.mark.parametrize("parent,correction", [
    ("Create a comparison.pdf report with Wednesday departure and Sunday return; mark it RESUME ORIGINAL.", "Add the other weekends too"),
    ("Crea comparison.pdf con salida miércoles y regreso domingo; incluye RESUME ORIGINAL.", "Añade los otros fines de semana"),
    ("水曜出発と日曜帰着の comparison.pdf を作成し、RESUME ORIGINAL を含めてください。", "他の週末も追加してください"),
])
def test_real_web_budget_resume_keeps_original_request_before_followup(tmp_path, monkeypatch, parent, correction):
    from fastapi.testclient import TestClient
    from pypdf import PdfReader
    from nullion import web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_plugin_tool_registry

    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    runtime = bootstrap_persistent_runtime(tmp_path / "resume.db")
    registry = create_plugin_tool_registry(workspace_root=tmp_path, allowed_roots=[tmp_path])
    conversation_id = "web:resume-original"
    from nullion.chat_operator import _remember_chat_turn
    _remember_chat_turn(runtime, chat_id=None, conversation_id=conversation_id,
        user_message=parent, assistant_reply="The request paused for a larger budget.",
        conversation_turn_id="accepted-original")
    approval_id = web_app._store_web_agent_turn_limit_extension_approval(runtime,
        conversation_id=conversation_id, user_text=parent, request_id="original-request",
        messages_snapshot=[{"role": "user", "content": parent},
            {"role": "user", "content": "Internal recovery instruction: continue the same request."}],
        current_max_iterations=4, tool_results=[], required_attachment_extensions=(".pdf",))

    class Model:
        created = 0
        def create(self, **kwargs):
            system = str(kwargs.get("system") or "")
            names = {tool["name"] for tool in kwargs.get("tools", [])}
            if "select_attachment_format" in names:
                return tool_response("select_attachment_format", {"extension": ".pdf", "confidence": 1, "embedded_media_extensions": []})
            if "relationship" in system and "target_index" in system:
                return text_response(json.dumps({"relationship": "follow_up", "effect": "revise", "confidence": 1, "target_index": 0}))
            if "material-requirements planner" in system:
                return text_response(json.dumps({"all_material_requirements_enumerated": True, "material_requirements": [
                    {"requirement": "Create the requested PDF", "requirement_kind": "deliverable", "evidence_subject_id": "comparison", "eligible_evidence_tool_names": ["pdf_create"]}]}))
            if "semantic completion reviewer" in system:
                return text_response(json.dumps({"disposition": "complete", "unresolved_requirements": [], "retry_tool_names": []}))
            if "web_action" in system and "scheduler_action" in system:
                return text_response(json.dumps({"web_action": "none", "scheduler_action": "none", "skill_pack_action": "none",
                    "requested_tool_names": ["pdf_create"], "required_tool_names": ["pdf_create"], "requested_artifact_extensions": [".pdf"], "confidence": 1}))
            if not names and system:
                return text_response("{}")
            if self.created:
                assert parent in json.dumps(kwargs["messages"], ensure_ascii=False), "Resume persisted the runtime nudge instead of the user's request"
            last_content = kwargs["messages"][-1].get("content")
            if isinstance(last_content, list) and any(
                block.get("type") == "tool_result" and block.get("tool_use_id") == "pdf_create"
                for block in last_content if isinstance(block, dict)
            ):
                return text_response("The comparison PDF is ready.")
            if "pdf_create" not in names:
                return tool_response("request_tool_scope", {"capabilities": ["local_files"], "artifact_extensions": [".pdf"], "tool_names": ["pdf_create"], "required_tool_names": ["pdf_create"]})
            self.created += 1
            return tool_response("pdf_create", {"title": f"comparison revision {self.created}",
                "text_pages": ["RESUME ORIGINAL: Wednesday departure, Sunday return. " + ("UPDATED WEEKENDS" if self.created > 1 else "Original comparison")]})

    model = Model()
    client = TestClient(web_app.create_app(runtime, AgentOrchestrator(model_client=model), registry))
    approved = client.post(f"/api/approve/{approval_id}", json={"mode": "limit_2x"})
    assert approved.status_code == 200, approved.text
    assert approved.json()["ok"] and approved.json()["resume"]["artifacts"], approved.json()
    events = [event for event in runtime.store.list_conversation_events(conversation_id)
        if event.get("event_type") == "conversation.chat_turn"]
    assert events[-1]["user_message"] == parent, events[-1]
    parent_id = events[-1]["turn_id"]
    corrected = client.post("/api/chat", json={"text": correction, "conversation_id": conversation_id})
    assert corrected.status_code == 200, corrected.text
    payload = corrected.json()
    assert payload["artifacts"], payload
    events = [event for event in runtime.store.list_conversation_events(conversation_id)
        if event.get("event_type") == "conversation.chat_turn"]
    assert events[-1]["parent_turn_id"] == parent_id
    pdf_path = Path(payload["artifacts"][0]["path"])
    assert pdf_path.read_bytes().startswith(b"%PDF-")
    assert "UPDATED WEEKENDS" in "\n".join(page.extract_text() for page in PdfReader(pdf_path).pages)
    assert runtime.store.get_approval_request(approval_id).status.value == "approved"
    assert runtime.store.get_suspended_turn(approval_id) is None


def text_response(text):
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}


def tool_response(name, args):
    return {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": name, "name": name, "input": args}]}


@pytest.mark.parametrize("history_detour", [False, True])
def test_weekend_corrections_keep_parent_and_deliver_updated_pdf(tmp_path, monkeypatch, record_property, history_detour, sourced_report_model_reply):
    from nullion import chat_operator, crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.tools import create_plugin_tool_registry, register_search_plugin, register_cron_tools
    from pypdf import PdfReader

    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    elapsed = []
    for index, prompt in enumerate(("But there are more weekends in this month", "Pero hay más fines de semana este mes", "今月にはまだ他の週末があります")):
        db = tmp_path / f"turn-{index}.db"
        monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", db)
        runtime = bootstrap_persistent_runtime(db)
        runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(
            rule_id="fixture-network", principal_id=GLOBAL_PERMISSION_PRINCIPAL,
            kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector="*",
            created_by="fixture-user", created_at=datetime.now(UTC)))
        registry = create_plugin_tool_registry(workspace_root=tmp_path, allowed_roots=[tmp_path])
        searches = []
        def flight_service(query, limit):
            searches.append(query)
            return [{"title": "Nonstop weekend fares", "url": "https://fares.fixture.local/weekends", "snippet": REPORT}]
        register_search_plugin(registry, web_searcher=flight_service)
        register_cron_tools(registry)
        job = crons.add_cron("Unrelated report", "0 12 * * 1-5", "Keep unchanged")
        runtime.active_tool_registry = registry
        chat_id = str(90300 + index)
        conversation_id = f"telegram:{chat_id}"
        settings = NullionSettings(telegram=TelegramSettings(bot_token="123456:abcdefghijklmnopqrstuvwxyz", operator_chat_id=chat_id, chat_enabled=True))
        chat_operator._remember_chat_turn(runtime, chat_id=chat_id, user_message=PARENT,
            assistant_reply="I compared October 14-18 only: Delta USD 643. Previous report: old-comparison.pdf")
        parent_id = runtime.store.get_conversation_head(conversation_id)["active_turn_id"]
        runtime.store.add_conversation_event({"conversation_id": "telegram:other", "event_type": "conversation.chat_turn",
            "created_at": datetime.now(UTC).isoformat(), "user_message": "Cron f37b4270", "assistant_reply": "Deleted cron f37b4270",
            "workspace_id": "workspace_admin"})

        class Model:
            def __init__(self):
                self.steps = 0
                self.slow_calls = 0
                self.main_histories = []
                self.history_attempts = 0
                self.recovery_scope_calls = 0
            def create(self, **kwargs):
                review = sourced_report_model_reply(kwargs, source_tools=['web_search'],
                    producer_tool='pdf_create', expected_tokens=['USD 643', 'USD 521', 'USD 487', 'Cheapest'])
                if review is not None:
                    return review
                system = str(kwargs.get("system") or "")
                names = {tool["name"] for tool in kwargs.get("tools", [])}
                if "select_attachment_format" in names:
                    return tool_response("select_attachment_format", {"extension": None, "confidence": 1, "embedded_media_extensions": []})
                if "relationship" in system and "target_index" in system:
                    return text_response(json.dumps({"relationship": "follow_up", "effect": "revise", "confidence": 1, "target_index": 0}))
                if "answers_previous_clarification" in system or "Scheduler mutation recovery classifier" in json.dumps(kwargs):
                    self.slow_calls += 1
                    time.sleep(1)
                    return text_response("{}")
                if "web_action" in system and "scheduler_action" in system:
                    if history_detour and self.history_attempts == 2:
                        self.recovery_scope_calls += 1
                        assert PARENT in json.dumps(kwargs["messages"]), "Recovery classifier lost the parent context"
                    return text_response(json.dumps({"web_action": "live_research", "scheduler_action": "none", "skill_pack_action": "none",
                        "requested_tool_names": ["web_search", "pdf_create"], "required_tool_names": ["web_search", "pdf_create"], "requested_artifact_extensions": [".pdf"], "confidence": 1}))
                if not names and system:
                    return text_response("{}")
                self.main_histories.append(kwargs["messages"])
                assert PARENT in json.dumps(kwargs["messages"], ensure_ascii=False), "Follow-up lost its persisted parent request"
                if history_detour and self.history_attempts == 0:
                    assert "chat_history_search" in names
                    self.history_attempts = 1
                    return tool_response("chat_history_search", {"query": "f37b4270", "search_scope": "full_workspace_after_reset", "limit": 3})
                if history_detour and self.history_attempts == 1:
                    self.history_attempts = 2
                    return text_response("I can't access live web search in this attempt.")
                needed = "web_search" if self.steps == 0 else "pdf_create" if self.steps == 1 else None
                if needed is not None and needed not in names:
                    assert "request_tool_scope" in names, names
                    return tool_response("request_tool_scope", {"capabilities": ["web", "local_files"], "artifact_extensions": [".pdf"], "tool_names": ["web_search", "pdf_create"], "required_tool_names": ["web_search", "pdf_create"]})
                self.steps += 1
                if self.steps == 1:
                    return tool_response("web_search", {"query": "NYC San Francisco nonstop Wednesday Sunday October 2026 remaining weekends", "limit": 3})
                if self.steps == 2:
                    assert "October 21-25" in json.dumps(kwargs["messages"]), json.dumps(kwargs["messages"])
                    return tool_response("pdf_create", {"title": "All eligible weekends", "text_pages": [REPORT]})
                assert self.steps == 3, "Unexpected recovery or verification model call"
                return text_response(REPORT)

        model = Model()
        started = time.perf_counter()
        reply = chat_operator.handle_chat_operator_message(runtime, prompt, chat_id=chat_id, settings=settings,
            request_id=f"weekend-{index}", model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
            append_activity_trace=False, allow_mini_agents=False)
        elapsed.append(time.perf_counter() - started)
        delivered = prepare_reply_for_platform_delivery(reply, principal_id="telegram_chat")
        assert delivered.text and len(delivered.attachments) == 1 and delivered.attachments[0].suffix.lower() == ".pdf", reply
        assert model.slow_calls == 0 and model.steps in {2, 3}
        if history_detour:
            assert model.recovery_scope_calls == 1
        assert len(searches) == 1, runtime.store.list_conversation_events(conversation_id)[-1]
        events = [event for event in runtime.store.list_conversation_events(conversation_id) if event.get("event_type") == "conversation.chat_turn"]
        assert events[-1]["parent_turn_id"] == parent_id
        tools = events[-1]["tool_results"]
        assert [result["tool_name"] for result in tools if result["tool_name"] != "request_tool_scope"] == (["chat_history_search"] if history_detour else []) + ["web_search", "pdf_create"]
        pdf_path = Path(next(result["output"]["artifact_path"] for result in tools if result["tool_name"] == "pdf_create"))
        assert pdf_path.read_bytes().startswith(b"%PDF-")
        text = "\n".join(page.extract_text() for page in PdfReader(pdf_path).pages)
        assert "October 21-25" in text and "October 28-November 1" in text
        assert [str(attachment) for attachment in delivered.attachments] == [str(pdf_path)]
        assert crons.get_cron(job.id).task == "Keep unchanged"
        persisted = load_runtime_store(db)
        assert persisted.list_approval_requests() == []
    record_property("elapsed_seconds", json.dumps(elapsed))
    assert sum(elapsed) / len(elapsed) < 8, elapsed
    assert max(elapsed) < 12, elapsed


def test_failed_report_keeps_concrete_failure_at_telegram_boundary(tmp_path, monkeypatch):
    import urllib.error
    from nullion import chat_operator
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.policy import BoundaryKind, GLOBAL_PERMISSION_PRINCIPAL, BoundaryPolicyRule, PolicyMode
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_plugin_tool_registry, register_web_fetch_tool

    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    runtime = bootstrap_persistent_runtime(tmp_path / "failed.db")
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule(rule_id="fixture-network", principal_id=GLOBAL_PERMISSION_PRINCIPAL,
        kind=BoundaryKind.OUTBOUND_NETWORK, mode=PolicyMode.ALLOW, selector="*", created_by="fixture-user", created_at=datetime.now(UTC)))
    registry = create_plugin_tool_registry(workspace_root=tmp_path, allowed_roots=[tmp_path])
    def unavailable(url, timeout):
        raise urllib.error.URLError("Flight search service refused the connection")
    register_web_fetch_tool(registry, web_fetcher=unavailable)
    runtime.active_tool_registry = registry
    chat_operator._remember_chat_turn(runtime, chat_id="90400", user_message=PARENT,
        assistant_reply="I compared October 14-18 only: USD 643. Previous report: old-comparison.pdf")

    class Model:
        fetched = False
        def create(self, **kwargs):
            system = str(kwargs.get("system") or "")
            names = {tool["name"] for tool in kwargs.get("tools", [])}
            if "select_attachment_format" in names:
                return tool_response("select_attachment_format", {"extension": None, "confidence": 1, "embedded_media_extensions": []})
            if "relationship" in system and "target_index" in system:
                return text_response(json.dumps({"relationship": "follow_up", "effect": "revise", "confidence": 1, "target_index": 0}))
            if "material-requirements planner" in system:
                return text_response(json.dumps({"all_material_requirements_enumerated": True, "material_requirements": [
                    {"requirement": "Produce a sourced comparison PDF", "requirement_kind": "deliverable", "evidence_subject_id": "comparison-report", "eligible_evidence_tool_names": []}]}))
            if "semantic completion reviewer" in system:
                return text_response(json.dumps({"disposition": "blocked", "unresolved_requirements": ["the flight search service refused the connection"], "retry_tool_names": []}))
            if "web_action" in system and "scheduler_action" in system:
                return text_response(json.dumps({"web_action": "live_research", "scheduler_action": "none", "skill_pack_action": "none",
                    "requested_tool_names": ["web_fetch", "pdf_create"], "requested_artifact_extensions": [".pdf"], "confidence": 1}))
            if system:
                return text_response("{}")
            if not self.fetched:
                if "web_fetch" not in names:
                    return tool_response("request_tool_scope", {"capabilities": ["web", "local_files"], "artifact_extensions": [".pdf"], "tool_names": ["web_fetch", "pdf_create"]})
                self.fetched = True
                return tool_response("web_fetch", {"url": "https://fares.fixture.local/weekends"})
            return text_response("The flight search service refused the connection. No comparison or updated PDF was produced.")

    model = Model()
    settings = NullionSettings(telegram=TelegramSettings(operator_chat_id="90400", chat_enabled=True))
    reply = chat_operator.handle_chat_operator_message(runtime, "Compare the other weekends too", chat_id="90400", settings=settings,
        model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model), append_activity_trace=False, allow_mini_agents=False)
    assert "flight search service refused the connection" in reply, reply
    assert "enough verified evidence" not in reply and "task is still open" not in reply
    events = [event for event in runtime.store.list_conversation_events("telegram:90400") if event.get("event_type") == "conversation.chat_turn"]
    assert events[-1]["assistant_reply"] == reply
    assert any(result["tool_name"] == "web_fetch" and result["status"] == "failed" for result in events[-1]["tool_results"])
    assert runtime.store.list_approval_requests() == []


def test_plain_telegram_latency_with_optional_tools_avoids_recovery(tmp_path, monkeypatch, record_property):
    from nullion import chat_operator, crons
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_plugin_tool_registry, register_search_plugin, register_cron_tools
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    runtime = bootstrap_persistent_runtime(tmp_path / "plain.db")
    monkeypatch.setattr(crons, "_RUNTIME_DB_PATH", tmp_path / "plain.db")
    registry = create_plugin_tool_registry(workspace_root=tmp_path, allowed_roots=[tmp_path])
    register_search_plugin(registry, web_searcher=lambda *args: pytest.fail("Plain chat must not search"))
    register_cron_tools(registry)
    crons.add_cron("Installed report", "0 12 * * 1-5", "Keep unchanged")
    runtime.active_tool_registry = registry
    elapsed = []
    for index, prompt in enumerate(("Hello", "Hola", "こんにちは", "Bonjour", "नमस्ते")):
        class Model:
            main_calls = 0
            classifier_calls = 0
            def create(self, **kwargs):
                if kwargs.get("system"):
                    self.classifier_calls += 1
                    time.sleep(1)  # Previously slow dependency must stay out of plain chat.
                    return text_response("{}")
                self.main_calls += 1
                return text_response("こんにちは。")
        model = Model()
        chat_id = str(90500 + index)
        settings = NullionSettings(telegram=TelegramSettings(operator_chat_id=chat_id, chat_enabled=True))
        started = time.perf_counter()
        reply = chat_operator.handle_chat_operator_message(runtime, prompt, chat_id=chat_id, settings=settings,
            model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model), append_activity_trace=False, allow_mini_agents=False)
        elapsed.append(time.perf_counter() - started)
        assert reply == "こんにちは。"
        assert model.main_calls == 1 and model.classifier_calls == 0
        event = [e for e in runtime.store.list_conversation_events(f"telegram:{chat_id}") if e.get("event_type") == "conversation.chat_turn"][-1]
        assert event["tool_results"] == []
    record_property("elapsed_seconds", json.dumps(elapsed))
    assert sum(elapsed) / len(elapsed) < 0.4, elapsed
    assert max(elapsed) < 0.7, elapsed

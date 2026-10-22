import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path

from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.attachment_format_graph import plan_attachment_format
from nullion.approvals import create_approval_request
from nullion.chat_operator import (
    _auto_approval_command_for_message,
    _chat_ambiguity_classifier,
    _chat_ambiguity_fallback,
    _chat_current_turn_has_structured_followup_evidence,
    _chat_delivery_contract_prompt,
    _chat_thread_has_structured_relationship_evidence,
    _deferred_runtime_follow_up_source_prompt,
    _turn_tool_scope_uses_compact_delivery_contract,
)
from nullion.intent_router import selected_numbered_option_context
from nullion.builder import BuilderDecisionType, BuilderProposal
from nullion.runtime import (
    PersistentRuntime,
    accept_stored_builder_skill_proposal_result,
    create_skill,
    create_skill_result,
    recommend_skills,
    store_builder_proposal,
)
from nullion.runtime_store import RuntimeStore
from nullion.skills import SKILL_WRITE_DELETE_CONSENT_STEP
from nullion.skill_usage import build_learned_skill_usage_hint
from nullion.task_planner import TaskPlanner
from nullion.telegram_app import _is_telegram_message_not_modified_error, _stop_telegram_status_typing_for_group
from nullion.turn_context_policy import TurnToolScopeDecision


class StructuredDispatchModel:
    def __init__(self, *, relationship: str, effect: str = "continue", confidence: float = 0.9) -> None:
        self.relationship = relationship
        self.effect = effect
        self.confidence = confidence
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "relationship": self.relationship,
                            "effect": self.effect,
                            "confidence": self.confidence,
                        }
                    ),
                }
            ]
        }


class AttachmentFormatModel:
    def __init__(self, extension: str | None) -> None:
        self.extension = extension

    def create(self, **kwargs):
        assert kwargs["tools"][0]["name"] == "select_attachment_format"
        return {
            "content": [
                {
                    "type": "tool_use",
                    "name": "select_attachment_format",
                    "input": {"extension": self.extension, "confidence": 0.94},
                }
            ]
        }


def test_plain_text_approval_words_do_not_route_to_approval_command(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    approval = create_approval_request(
        requested_by="telegram_chat",
        action="use_tool",
        resource="tool:web_fetch",
        context={},
    )
    runtime.store.add_approval_request(approval)

    assert _auto_approval_command_for_message(runtime, "approved", chat_id="123") is None


def test_prompt_words_do_not_route_to_learned_skills():
    store = RuntimeStore()
    skill = create_skill(
        store,
        title="Check tonight weather",
        summary="Fetches weather details.",
        trigger="Use when the user asks for weather tonight.",
        steps=["Check forecast data", "Summarize conditions"],
        tags=["weather", "forecast"],
    )

    assert recommend_skills(store, "what is the weather tonight?") == []
    assert build_learned_skill_usage_hint(store, "what is the weather tonight?") is None

    explicit_hint = build_learned_skill_usage_hint(
        store,
        "what is the weather tonight?",
        skill_ids=[skill.skill_id],
    )
    assert explicit_hint is not None
    assert explicit_hint.titles == ("Check tonight weather",)


def test_auto_learned_skill_duplicate_identity_ignores_captured_prompt_text():
    store = RuntimeStore()
    first = create_skill_result(
        store,
        title="Trigger named cron",
        summary="Captured from: Trigger my email cron",
        trigger="Use when the user wants to manually run a specific cron job by name or purpose.",
        steps=["List crons", "Run the selected cron"],
        tags=["cron"],
        actor="builder_auto",
    )
    second = create_skill_result(
        store,
        title="Trigger named cron",
        summary="Captured from: Trigger the mid market cron too",
        trigger="Use when the user wants to manually trigger a scheduled cron job by name or description.",
        steps=["List available cron jobs", "Run the identified cron"],
        tags=["scheduled-jobs"],
        actor="builder_auto",
    )

    assert first.created is True
    assert second.created is False
    assert second.merged_duplicate is True
    assert len(store.skills) == 1
    assert second.skill.skill_id == first.skill.skill_id
    assert SKILL_WRITE_DELETE_CONSENT_STEP in first.skill.steps


def test_builder_duplicate_skill_is_merged_for_runtime_actor_too():
    store = RuntimeStore()
    first = create_skill_result(
        store,
        title="Trigger named cron",
        summary="Captured from: Trigger my email cron",
        trigger="Use when the user wants to manually run a specific cron job by name or purpose.",
        steps=["List crons", "Run the selected cron"],
        tags=["cron"],
        actor="runtime",
    )
    second = create_skill_result(
        store,
        title="Trigger named cron",
        summary="Captured from: Trigger the mid market cron too",
        trigger="Use when the user wants to manually trigger a scheduled cron job by name or description.",
        steps=["List available cron jobs", "Run the identified cron"],
        tags=["scheduled-jobs"],
        actor="runtime",
    )

    assert first.created is True
    assert second.created is False
    assert second.merged_duplicate is True
    assert len(store.skills) == 1
    assert second.skill.skill_id == first.skill.skill_id
    assert "scheduled-jobs" in second.skill.tags


def test_auto_duplicate_builder_proposal_is_not_stored_for_reacceptance():
    store = RuntimeStore()
    first = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Trigger named cron",
        summary="Captured from: Trigger my email cron",
        confidence=0.86,
        approval_mode="skill",
        suggested_skill_title="Trigger named cron",
        suggested_trigger="Use when the user wants to manually run a specific cron job by name or purpose.",
        suggested_steps=("List crons", "Run the selected cron"),
        suggested_tags=("cron",),
    )
    record = store_builder_proposal(store, first, actor="builder_reflector")
    accepted = accept_stored_builder_skill_proposal_result(store, record.proposal_id, actor="builder_auto")
    duplicate = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title="Trigger named cron",
        summary="Captured from: Trigger the mid market cron too",
        confidence=0.86,
        approval_mode="skill",
        suggested_skill_title="Trigger named cron",
        suggested_trigger="Use when the user wants to manually trigger a scheduled cron job by name or description.",
        suggested_steps=("List available cron jobs", "Run the identified cron"),
        suggested_tags=("scheduled-jobs",),
    )
    duplicate_record = store_builder_proposal(store, duplicate, actor="builder_reflector")

    assert accepted.created is True
    assert duplicate_record.status == "accepted"
    assert duplicate_record.accepted_skill_id == accepted.skill.skill_id
    assert duplicate_record.proposal_id not in store.builder_proposals
    assert len(store.builder_proposals) == 1


def test_follow_up_words_do_not_rewrite_prompt_from_prior_offer():
    thread = [
        {
            "user": "fetch current news from cnn.com",
            "assistant": "If you want, I can try the actual fetch now or draft the exact next step first.",
        }
    ]

    assert _deferred_runtime_follow_up_source_prompt(thread, "yes") is None


def test_numeric_reply_uses_structured_numbered_options_only():
    prior_reply = "Stage alert context.\nOptions:\n1. Use the saved account\n2. Configure another account"

    context = selected_numbered_option_context("2", prior_reply)

    assert context is not None
    assert "option 2" in context
    assert "Configure another account" in context
    assert "Previous assistant context" in context
    assert "Stage alert context." in context
    assert selected_numbered_option_context("2", "No numbered choices here") is None


def test_numeric_reply_flags_ambiguous_multiple_numbered_option_blocks():
    prior_reply = (
        "Next step I recommend\n"
        "1. Open your Google account security page\n"
        "2. Review the Maton access\n"
        "3. Remove it if you do not recognize it\n\n"
        "If you want, I can also:\n"
        "1. Check for any other important emails\n"
        "2. Help you summarize the latest 10 emails\n"
        "3. Help you review the security alert in more detail"
    )

    context = selected_numbered_option_context("1", prior_reply)

    assert context is not None
    assert "ambiguity" in context
    assert "multiple numbered option groups" in context
    assert "Open your Google account security page" not in context
    assert "Check for any other important emails" not in context


def test_compact_reply_after_numbered_prompt_has_structured_followup_evidence(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "numbered-followup-evidence.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )

    assert _chat_current_turn_has_structured_followup_evidence(
        runtime,
        prompt="✓",
        conversation_id="telegram:choice",
        attachments=(),
        previous_assistant_message=(
            "I can keep looking by:\n"
            "1. Searching the restaurant name\n"
            "2. Opening the restaurant page\n"
            "Reply 1 and I will keep looking."
        ),
        ambiguity_fallback_reason=None,
    )


def test_compact_reply_after_recent_numbered_prompt_has_structured_followup_evidence(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "recent-numbered-followup-evidence.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    runtime.store.add_conversation_event(
        {
            "conversation_id": "web:choice",
            "event_type": "conversation.chat_turn",
            "created_at": datetime(2026, 5, 2, tzinfo=UTC).isoformat(),
            "user_message": "Add the identified items to my cart.",
            "assistant_reply": (
                "I can keep looking by:\n"
                "1. Searching the restaurant name\n"
                "2. Opening the restaurant page\n"
                "Reply 1 and I will keep looking."
            ),
        }
    )

    assert _chat_current_turn_has_structured_followup_evidence(
        runtime,
        prompt="sí",
        conversation_id="web:choice",
        attachments=(),
        previous_assistant_message=None,
        ambiguity_fallback_reason=None,
    )


def test_numbered_prompt_evidence_lets_structured_model_link_compact_reply(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "numbered-followup-classifier.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    previous_assistant = (
        "I can keep looking by:\n"
        "1. Searching the restaurant name\n"
        "2. Opening the restaurant page\n"
        "Reply 1 and I will keep looking."
    )
    structured_evidence = _chat_current_turn_has_structured_followup_evidence(
        runtime,
        prompt="sí",
        conversation_id="telegram:choice",
        attachments=(),
        previous_assistant_message=previous_assistant,
        ambiguity_fallback_reason=None,
    )
    model = StructuredDispatchModel(relationship="follow_up")
    classifier, reason = _chat_ambiguity_classifier(
        [
            {
                "user": "Add the previously identified items to my cart.",
                "assistant": previous_assistant,
            }
        ],
        model_client=model,
        structured_followup_evidence=structured_evidence,
        allow_plain_compact_classification=True,
    )

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    ctx = type(
        "Ctx",
        (),
        {
            "active_branch_exists": True,
            "previous_assistant_message": previous_assistant,
        },
    )()
    assert classifier("sí", ctx) is ConversationTurnDisposition.CONTINUE
    assert model.calls


def test_chat_delivery_contract_keeps_cart_page_discovery_before_asking_for_link(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "cart-delivery-contract.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )

    prompt = _chat_delivery_contract_prompt(runtime, principal_id="telegram:100001")

    assert "prior screenshot, attachment, or conversation-derived merchant and item names" in prompt
    assert "reasonable page-discovery attempts before asking the user for a direct link" in prompt
    assert "do not submit checkout, payment, purchase, booking, or order placement" in prompt
    assert "exactly one numbered choice list" in prompt
    assert "numeric replies always map to one clear selectable list" in prompt
    assert "choose the layout from the content shape" in prompt
    assert "Use compact prose or tight short-line summaries for a few facts" in prompt
    assert "Do not force every reply into bullets" in prompt
    assert "multiple separate final deliverables" in prompt
    assert "create every separate deliverable before finalizing" in prompt


def test_compact_message_only_delivery_contract_omits_artifact_handbook(tmp_path):
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "compact-delivery-contract.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )

    prompt = _chat_delivery_contract_prompt(
        runtime,
        principal_id="web:operator",
        compact_message_only=True,
    )

    assert len(prompt) < 800
    assert "message-only turn" in prompt
    assert "answer the user's actual question first" in prompt.lower()
    assert "exactly one numbered choice list" in prompt
    assert "Never answer only" in prompt
    assert "artifact directory" not in prompt
    assert "spreadsheet_create" not in prompt
    assert "terminal_exec" not in prompt


def test_browser_text_scope_uses_compact_delivery_contract_but_artifacts_do_not():
    browser_registry = type(
        "Registry",
        (),
        {
            "turn_tool_scope_decision": TurnToolScopeDecision(
                web_action="live_research",
                requested_tool_names=("request_tool_scope", "browser_navigate", "browser_extract_text"),
                confidence=1.0,
                valid=True,
            )
        },
    )()
    artifact_registry = type(
        "Registry",
        (),
        {
            "turn_tool_scope_decision": TurnToolScopeDecision(
                requested_tool_names=("spreadsheet_create",),
                requested_artifact_extensions=(".xlsx",),
                confidence=1.0,
                valid=True,
            )
        },
    )()
    url_registry = type(
        "Registry",
        (),
        {
            "turn_tool_scope_decision": TurnToolScopeDecision(
                web_action="open_url",
                confidence=1.0,
                valid=True,
            )
        },
    )()

    assert _turn_tool_scope_uses_compact_delivery_contract(browser_registry) is True
    assert _turn_tool_scope_uses_compact_delivery_contract(url_registry) is True
    assert _turn_tool_scope_uses_compact_delivery_contract(artifact_registry) is False


def test_chat_delivery_contract_exposes_local_web_app_target_only_to_admins(tmp_path, monkeypatch):
    monkeypatch.setenv("NULLION_WEB_PORT", "8753")
    admin_runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "admin-local-target.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    member_runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "member-local-target.db",
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )

    admin_prompt = _chat_delivery_contract_prompt(admin_runtime, principal_id="telegram_chat")
    member_prompt = _chat_delivery_contract_prompt(member_runtime, principal_id="user:member-1")

    assert "Admin local app targets:" in admin_prompt
    assert "Nullion web app: http://127.0.0.1:8753/" in admin_prompt
    assert "Admin local app targets:" not in member_prompt
    assert "http://127.0.0.1:8753/" not in member_prompt


def test_chat_ambiguity_fallback_does_not_continue_from_prior_url_reply():
    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=Path("/tmp/nullion-language-neutral-routing.db"),
        started_at=datetime(2026, 5, 2, tzinfo=UTC),
    )

    fallback, reason = _chat_ambiguity_fallback(runtime, chat_id="123", prompt="Whats weather like tomorrow?")

    assert reason is None
    assert fallback("Whats weather like tomorrow?", True) is None


def test_chat_ambiguity_classifier_uses_structured_model_output_for_follow_up():
    model = StructuredDispatchModel(relationship="follow_up")
    thread = [
        {
            "user": "https://example.com を .pdf で保存",
            "assistant": "Which format should I use?",
        }
    ]

    classifier, reason = _chat_ambiguity_classifier(
        thread,
        model_client=model,
        structured_followup_evidence=True,
        allow_plain_compact_classification=True,
    )

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    assert classifier("別の形式にしてください", type("Ctx", (), {"active_branch_exists": True})()) is ConversationTurnDisposition.CONTINUE
    request_payload = json.loads(model.calls[0]["messages"][0]["content"][0]["text"])
    assert request_payload["active_requests"][0]["user_request"] == "https://example.com を .pdf で保存"


def test_chat_ambiguity_classifier_uses_recent_completed_turn_without_keyword_gate():
    model = StructuredDispatchModel(relationship="follow_up")
    thread = [
        {
            "user": "¿Hay tren de Nueva York a San Francisco?",
            "assistant": "Sí, normalmente via Chicago y Emeryville.",
        }
    ]

    classifier, reason = _chat_ambiguity_classifier(thread, model_client=model)

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    assert (
        classifier(
            "Génère un .pdf détaillé avec durée, prix et vues panoramiques.",
            type("Ctx", (), {"active_branch_exists": True})(),
        )
        is ConversationTurnDisposition.CONTINUE
    )
    request_payload = json.loads(model.calls[0]["messages"][0]["content"][0]["text"])
    assert request_payload["active_requests"][0]["user_request"] == "¿Hay tren de Nueva York a San Francisco?"


def test_chat_ambiguity_classifier_uses_previous_assistant_context_for_bare_reply():
    model = StructuredDispatchModel(relationship="follow_up")
    thread = [
        {
            "user": "Copy my Codex auth token from stage to prod.",
            "assistant": "I can do that, but please confirm before I touch prod credentials.",
        }
    ]

    classifier, reason = _chat_ambiguity_classifier(
        thread,
        model_client=model,
        structured_followup_evidence=True,
        allow_plain_compact_classification=True,
    )

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    ctx = type(
        "Ctx",
        (),
        {
            "active_branch_exists": True,
            "previous_assistant_message": thread[-1]["assistant"],
        },
    )()
    assert classifier("✓", ctx) is ConversationTurnDisposition.CONTINUE
    request_payload = json.loads(model.calls[0]["messages"][0]["content"][0]["text"])
    active_request = request_payload["active_requests"][0]["user_request"]
    assert "Copy my Codex auth token from stage to prod." in active_request


def test_chat_ambiguity_classifier_skips_plain_turn_when_only_stale_history_is_structured():
    model = StructuredDispatchModel(relationship="follow_up")
    thread = [
        {
            "user": "https://example.com を .pdf で保存",
            "assistant": "Saved the requested artifact.",
        }
    ]

    classifier, reason = _chat_ambiguity_classifier(
        thread,
        model_client=model,
        structured_followup_evidence=True,
        allow_plain_compact_classification=False,
    )

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    assert classifier("hola", type("Ctx", (), {"active_branch_exists": True})()) is None
    assert model.calls == []


def test_chat_ambiguity_classifier_skips_completed_turn_without_structured_evidence():
    model = StructuredDispatchModel(relationship="follow_up")
    thread = [
        {
            "user": "¿Hay tren de Nueva York a San Francisco?",
            "assistant": "Sí, normalmente via Chicago y Emeryville.",
        }
    ]

    classifier, reason = _chat_ambiguity_classifier(thread, model_client=model)

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    ctx = type(
        "Ctx",
        (),
        {
            "active_branch_exists": True,
            "previous_assistant_message": "Sí, normalmente via Chicago y Emeryville.",
        },
    )()
    assert classifier("que esta pasando", ctx) is None
    assert model.calls == []


def test_chat_ambiguity_classifier_keeps_structured_separate_request_independent():
    model = StructuredDispatchModel(relationship="separate")
    thread = [
        {
            "user": "https://example.com を .pdf で保存",
            "assistant": "Which format should I use?",
        }
    ]

    classifier, reason = _chat_ambiguity_classifier(
        thread,
        model_client=model,
        structured_followup_evidence=True,
        allow_plain_compact_classification=True,
    )

    assert reason == "model_structured_turn_relationship"
    assert classifier is not None
    assert classifier("mañana tiempo 10024", type("Ctx", (), {"active_branch_exists": True})()) is ConversationTurnDisposition.INDEPENDENT


def test_chat_thread_structured_evidence_only_uses_recent_turn_window():
    thread = [
        {
            "user": "https://example.com を .pdf で保存",
            "assistant": "Saved.",
        },
        {
            "user": "Hi",
            "assistant": "Hi there.",
        },
    ]

    assert _chat_thread_has_structured_relationship_evidence(thread) is True
    assert _chat_thread_has_structured_relationship_evidence(thread, recent_turn_limit=1) is False


def test_attachment_format_uses_literal_extension_evidence():
    assert plan_attachment_format("save as .pdf").extension == ".pdf"
    assert plan_attachment_format("output=/tmp/report.xlsx").extension == ".xlsx"
    assert plan_attachment_format("find report.pdf").extension is None
    bare_word = plan_attachment_format("save as pdf")
    source_file_request = plan_attachment_format("search all the pdf files on my computer and send me in a text file")
    structured = plan_attachment_format("save as pdf", model_client=AttachmentFormatModel(".pdf"))

    assert bare_word.extension is None
    assert source_file_request.extension is None
    assert structured.extension == ".pdf"
    assert structured.evidence == "model_structured_output"


def test_task_planner_scopes_tools_from_structural_evidence_only():
    planner = TaskPlanner()
    pdf_plan = planner.build_execution_plan(
        user_message="résumé la page et rends-la en .pdf",
        principal_id="test:user",
        active_task_frame=None,
    )
    pdf_metadata = pdf_plan.mission.steps[0].metadata
    assert pdf_metadata["tool_scope"] == ["pdf_create"]
    assert pdf_metadata["tool_scope_evidence"] == ["literal_extension"]

    bare_word_plan = planner.build_execution_plan(
        user_message="résumé la page en pdf",
        principal_id="test:user",
        active_task_frame=None,
    )
    bare_word_metadata = bare_word_plan.mission.steps[0].metadata
    assert bare_word_metadata["tool_scope"] == []
    assert bare_word_metadata.get("tool_scope_evidence", []) == []

    url_plan = planner.build_execution_plan(
        user_message="analiza ejemplo.test/path",
        principal_id="test:user",
        active_task_frame=None,
    )
    url_metadata = url_plan.mission.steps[0].metadata
    assert url_metadata["tool_scope"] == ["web_fetch"]
    assert url_metadata["tool_scope_evidence"] == ["url_target"]


def test_telegram_message_not_modified_bad_request_is_idempotent():
    BadRequest = type("BadRequest", (Exception,), {})

    assert _is_telegram_message_not_modified_error(BadRequest("Message is not modified"))
    assert not _is_telegram_message_not_modified_error(BadRequest("can't parse entities"))


def test_telegram_final_group_delivery_stops_status_typing_keepalive():
    async def run_check() -> bool:
        async def keepalive() -> None:
            while True:
                await asyncio.sleep(10)

        task = asyncio.create_task(keepalive())
        typing_tasks = {("123", "group-a"): task}

        await _stop_telegram_status_typing_for_group(typing_tasks, chat_id="123", group_id="group-a")

        return task.cancelled() and typing_tasks == {}

    assert asyncio.run(run_check())

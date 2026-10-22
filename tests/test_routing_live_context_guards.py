from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
import warnings

import pytest

from nullion.conversation_runtime import (
    ConversationBranch,
    ConversationBranchStatus,
    ConversationTurn,
    ConversationTurnDisposition,
)
from nullion.entrypoint_guard import (
    SingleInstanceLock,
    _lock_key,
    _single_instance_wait_seconds,
    _truthy_env,
    run_single_instance_entrypoint,
    run_user_facing_entrypoint,
    single_instance,
)
from nullion.intent_router import (
    IntentLabel,
    classify_intent,
    classify_turn_disposition,
    classify_turn_disposition_with_reason,
    split_compound_intent,
)
from nullion.live_information import (
    LiveInformationResolution,
    LiveInformationRoute,
    actionable_live_information_resolutions,
    classify_live_information_route,
    format_live_information_resolution_label,
    format_live_information_states_for_prompt,
    resolve_live_information_resolution,
    route_required_plugins,
)
from nullion.runtime import process_conversation_message
from nullion.runtime_store import RuntimeStore
from nullion.system_context import build_system_context_snapshot, format_system_context_for_prompt
from nullion.task_frames import (
    TaskFrame,
    TaskFrameContinuationMode,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
    resolve_task_frame_continuation,
)
from nullion.task_planner import PlanDispatchMode, PlanDisposition, PlannerConfig, TaskPlanner
from nullion.tools import ToolRiskLevel, ToolSideEffectClass, ToolSpec


def test_intent_classification_turn_disposition_and_compound_split() -> None:
    assert classify_intent("").label is IntentLabel.AMBIGUOUS
    assert classify_intent("thanks").label is IntentLabel.AMBIGUOUS
    assert classify_intent("fetch the report").label is IntentLabel.AMBIGUOUS
    assert classify_intent("https://example.com").label is IntentLabel.ACTIONABLE

    assert classify_turn_disposition("", False) is ConversationTurnDisposition.CHATTER
    assert classify_turn_disposition("wait stop", True) is ConversationTurnDisposition.INDEPENDENT
    assert classify_turn_disposition("also do this", True) is ConversationTurnDisposition.INDEPENDENT
    assert classify_turn_disposition("blue", True, previous_assistant_message="Which color?") is ConversationTurnDisposition.INDEPENDENT
    assert classify_turn_disposition(
        "bleu",
        True,
        previous_assistant_message="Quelle couleur?",
        ambiguity_classifier=lambda text, ctx: ConversationTurnDisposition.CONTINUE,
    ) is ConversationTurnDisposition.CONTINUE
    assert classify_turn_disposition("why is that", True, previous_assistant_message="It failed.") is ConversationTurnDisposition.INDEPENDENT
    assert classify_turn_disposition("hello", True) is ConversationTurnDisposition.INDEPENDENT
    assert classify_turn_disposition_with_reason(
        "ambiguous",
        True,
        ambiguity_fallback=lambda text, active: ConversationTurnDisposition.REVISE,
    ).reason == "ambiguity_fallback"
    assert classify_turn_disposition_with_reason(
        "ambiguous",
        True,
        ambiguity_fallback=lambda text, active: "bad",
    ).reason == "ambiguity_fallback_invalid_return"
    assert classify_turn_disposition_with_reason(
        "ambiguous",
        True,
        ambiguity_classifier=lambda text, ctx: ConversationTurnDisposition.BACKGROUND_FOLLOW_UP,
    ).reason == "ambiguity_classifier"
    assert classify_turn_disposition_with_reason(
        "ambiguous",
        True,
        ambiguity_classifier=lambda text, ctx: (_ for _ in ()).throw(RuntimeError()),
    ).reason == "ambiguity_classifier_error"

    assert split_compound_intent("fetch the page and then summarize the page") == ["fetch the page and then summarize the page"]
    assert split_compound_intent("hello and then summarize") == ["hello and then summarize"]


def test_task_planner_builds_single_parallel_and_sequential_missions() -> None:
    planner = TaskPlanner()
    single = planner.build_execution_plan(user_message="Fetch https://example.com", principal_id="p1", active_task_frame=None)
    assert single.disposition is PlanDisposition.SINGLE_TURN
    assert single.dispatch_mode is PlanDispatchMode.NONE
    assert single.mission.steps[0].metadata["tool_scope"] == ["web_fetch"]
    assert "deep_agent_profiles" not in single.mission.steps[0].metadata
    assert "deep_agent_skills" not in single.mission.steps[0].metadata
    assert "deep_agent_subagents" not in single.mission.steps[0].metadata

    sequential_by_default = planner.build_execution_plan(
        user_message="search docs today; check weather at the same time",
        principal_id="p1",
        active_task_frame=None,
    )
    assert sequential_by_default.disposition is PlanDisposition.SINGLE_TURN
    assert sequential_by_default.can_dispatch_mini_agents is False
    assert sequential_by_default.can_run_mission is False
    assert [step.title for step in sequential_by_default.mission.steps] == ["Search docs today; check weather at the same time"]

    generic_send = planner.build_execution_plan(
        user_message="send summary now",
        principal_id="p1",
        active_task_frame=None,
    )
    assert generic_send.mission.steps[0].metadata["tool_scope"] == []

    non_tool_words = planner.build_execution_plan(
        user_message="review the female customer segment findings",
        principal_id="p1",
        active_task_frame=None,
    )
    assert non_tool_words.mission.steps[0].metadata["tool_scope"] == []

    sequential = planner.build_execution_plan(
        user_message="fetch page and then confirm before email results",
        principal_id="p1",
        active_task_frame=SimpleNamespace(frame_id="frame", status=SimpleNamespace(value="active"), operation=SimpleNamespace(value="fetch_resource")),
        config=PlannerConfig(max_steps=10),
    )
    assert sequential.disposition is PlanDisposition.SINGLE_TURN
    assert sequential.dispatch_mode is PlanDispatchMode.NONE
    assert sequential.mission.steps[0].metadata["checkpoint_before"] is False
    assert sequential.mission.steps[0].metadata["active_task_frame_id"] == "frame"

    scoped_single = planner.build_execution_plan(
        user_message="Can you check Heremes app on my computer and list all the crons?",
        principal_id="p1",
        active_task_frame=None,
    )
    assert scoped_single.disposition is PlanDisposition.SINGLE_TURN
    assert scoped_single.dispatch_mode is PlanDispatchMode.NONE
    scoped_sequence = planner.build_execution_plan(
        user_message="Can you check Heremes app on my computer and then list all the crons?",
        principal_id="p1",
        active_task_frame=None,
    )
    assert scoped_sequence.disposition is PlanDisposition.SINGLE_TURN
    assert scoped_sequence.dispatch_mode is PlanDispatchMode.NONE
    with pytest.raises(ValueError):
        planner.plan(user_message=" ", principal_id="p1", active_task_frame=None)


def test_task_frame_plain_text_does_not_auto_continue_active_frame() -> None:
    active_frame = SimpleNamespace(
        operation=SimpleNamespace(value="generate_artifact"),
        target=SimpleNamespace(normalized_value="report"),
        output=SimpleNamespace(artifact_kind="txt", delivery_mode="attachment", response_shape="text"),
        execution=SimpleNamespace(),
        finish=SimpleNamespace(requires_attempt=True, requires_artifact_delivery=True),
    )

    language_neutral = resolve_task_frame_continuation(
        text="明日 10024",
        active_frame=active_frame,
        branch_continuous=True,
    )
    arbitrary_text = resolve_task_frame_continuation(
        text="verde 42",
        active_frame=active_frame,
        branch_continuous=True,
    )

    assert language_neutral.mode is TaskFrameContinuationMode.START_NEW
    assert arbitrary_text.mode is TaskFrameContinuationMode.START_NEW


def test_task_frame_output_override_uses_shared_attachment_format_graph() -> None:
    active_frame = SimpleNamespace(
        operation=SimpleNamespace(value="fetch_resource"),
        target=SimpleNamespace(kind="url", value="https://example.com/", normalized_value="https://example.com/"),
        output=SimpleNamespace(artifact_kind="txt", delivery_mode="attachment", response_shape="text"),
        execution=SimpleNamespace(),
        finish=SimpleNamespace(requires_attempt=True, requires_artifact_delivery=True, required_tool_completion=None),
    )

    decision = resolve_task_frame_continuation(
        text="output=report.docx",
        active_frame=active_frame,
        branch_continuous=True,
    )

    assert decision.mode is TaskFrameContinuationMode.REVISE
    assert decision.output.artifact_kind == "docx"
    assert decision.finish.requires_artifact_delivery is True
    assert decision.finish.required_artifact_kind == "docx"


def test_task_frame_target_substitution_uses_url_signal() -> None:
    active_frame = SimpleNamespace(
        operation=SimpleNamespace(value="fetch_resource"),
        target=TaskFrameTarget(kind="url", value="https://bbc.com/", normalized_value="bbc.com"),
        output=SimpleNamespace(artifact_kind="txt", delivery_mode="attachment", response_shape="text"),
        execution=SimpleNamespace(),
        finish=SimpleNamespace(requires_attempt=True, requires_artifact_delivery=True, required_tool_completion=None),
    )

    decision = resolve_task_frame_continuation(
        text="https://cnn.com",
        active_frame=active_frame,
        branch_continuous=True,
    )

    assert decision.mode is TaskFrameContinuationMode.SUBSTITUTE_TARGET
    assert decision.target.normalized_value == "https://cnn.com/"


def test_runtime_active_task_frame_does_not_force_unstructured_follow_up() -> None:
    store = RuntimeStore()
    first = process_conversation_message(
        store,
        conversation_id="web:test",
        user_message="https://bbc.com",
        turn_id="turn-1",
        received_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    frame = TaskFrame(
        frame_id="frame-1",
        conversation_id="web:test",
        branch_id=first.branch.branch_id,
        source_turn_id=first.turn.turn_id,
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.FETCH_RESOURCE,
        target=TaskFrameTarget(kind="url", value="https://bbc.com", normalized_value="bbc.com"),
        execution=TaskFrameExecutionContract(approval_sensitive=True, boundary_kind="network"),
        output=TaskFrameOutputContract(artifact_kind="text", delivery_mode="inline_text"),
        finish=TaskFrameFinishCriteria(requires_attempt=True),
        summary="Fetch a URL",
        created_at=datetime(2026, 5, 2, tzinfo=UTC),
        updated_at=datetime(2026, 5, 2, tzinfo=UTC),
    )
    store.add_task_frame(frame)
    store.set_active_task_frame_id("web:test", "frame-1")

    second = process_conversation_message(
        store,
        conversation_id="web:test",
        user_message="明日 10024",
        turn_id="turn-2",
        received_at=datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC),
    )

    assert second.turn.disposition is not ConversationTurnDisposition.CONTINUE
    assert second.disposition_reason != "task_frame:continue"
    assert second.task_frame_continuation is not None
    assert second.task_frame_continuation.mode is TaskFrameContinuationMode.START_NEW
    assert store.get_active_task_frame_id("web:test") is None

    stale_frame_url = process_conversation_message(
        store,
        conversation_id="web:test",
        user_message="https://cnn.com",
        turn_id="turn-3",
        received_at=datetime(2026, 5, 2, 0, 0, 2, tzinfo=UTC),
    )

    assert stale_frame_url.disposition_reason != "task_frame:substitute_target"
    assert stale_frame_url.task_frame_continuation is not None
    assert stale_frame_url.task_frame_continuation.mode is TaskFrameContinuationMode.START_NEW


def test_runtime_structured_dispatch_links_follow_up_to_selected_parallel_turn() -> None:
    store = RuntimeStore()
    conversation_id = "telegram:parallel"

    email_turn = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="メールの新着を確認する",
        request_id="telegram-update:email",
        message_id="telegram-message:email",
        turn_id="turn-email",
        received_at=datetime(2026, 5, 7, 19, 43, 12, tzinfo=UTC),
    )
    cron_turn = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="informe bursátil posterior al cierre",
        request_id="telegram-update:cron",
        message_id="telegram-message:cron",
        turn_id="turn-cron",
        received_at=datetime(2026, 5, 7, 19, 43, 23, tzinfo=UTC),
    )

    follow_up = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="¿Eso requiere atención ahora?",
        request_id="telegram-update:follow-up",
        message_id="telegram-message:follow-up",
        turn_id="turn-follow-up",
        received_at=datetime(2026, 5, 7, 19, 45, 2, tzinfo=UTC),
        dispatch_disposition=ConversationTurnDisposition.CONTINUE,
        dispatch_dependency_turn_ids=("telegram-update:email",),
        dispatch_reason="model_structured_follow_up",
    )

    assert cron_turn.turn.parent_turn_id is None
    assert follow_up.turn.parent_turn_id == email_turn.turn.turn_id
    assert follow_up.turn.branch_id == email_turn.turn.branch_id
    assert follow_up.turn.branch_id != cron_turn.turn.branch_id
    assert follow_up.turn.disposition is ConversationTurnDisposition.CONTINUE
    assert follow_up.disposition_reason == "dispatch:model_structured_follow_up"
    assert store.get_conversation_head(conversation_id) == {
        "active_branch_id": email_turn.turn.branch_id,
        "active_turn_id": follow_up.turn.turn_id,
    }
    follow_up_events = [
        event
        for event in store.list_conversation_events(conversation_id)
        if event.get("turn_id") == follow_up.turn.turn_id and event.get("event_type") == "conversation.turn_recorded"
    ]
    assert follow_up_events
    assert follow_up_events[-1]["dispatch_dependency_turn_ids"] == ["telegram-update:email"]
    assert follow_up_events[-1]["dispatch_parent_turn_id"] == email_turn.turn.turn_id
    assert follow_up_events[-1]["dispatch_linked"] is True


def test_runtime_structured_revision_supersedes_selected_parallel_branch_only() -> None:
    store = RuntimeStore()
    conversation_id = "telegram:parallel-revise"

    email_turn = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="メールの新着を確認する",
        request_id="telegram-update:email",
        message_id="telegram-message:email",
        turn_id="turn-email",
        received_at=datetime(2026, 5, 7, 19, 43, 12, tzinfo=UTC),
    )
    cron_turn = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="informe bursátil posterior al cierre",
        request_id="telegram-update:cron",
        message_id="telegram-message:cron",
        turn_id="turn-cron",
        received_at=datetime(2026, 5, 7, 19, 43, 23, tzinfo=UTC),
    )

    revised = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="revísalo con una ventana más pequeña",
        request_id="telegram-update:revise",
        message_id="telegram-message:revise",
        turn_id="turn-revise",
        received_at=datetime(2026, 5, 7, 19, 45, 2, tzinfo=UTC),
        dispatch_disposition=ConversationTurnDisposition.REVISE,
        dispatch_dependency_turn_ids=("telegram-update:email",),
        dispatch_reason="model_structured_revision",
    )

    email_branch = store.get_conversation_branch(email_turn.turn.branch_id)
    cron_branch = store.get_conversation_branch(cron_turn.turn.branch_id)

    assert email_branch is not None
    assert email_branch.status is ConversationBranchStatus.SUPERSEDED
    assert email_branch.superseded_by_branch_id == revised.turn.branch_id
    assert cron_branch is not None
    assert cron_branch.status is ConversationBranchStatus.ACTIVE
    assert revised.turn.parent_turn_id == email_turn.turn.turn_id
    assert revised.turn.branch_id not in {email_turn.turn.branch_id, cron_turn.turn.branch_id}
    assert revised.superseded_branch_id == email_turn.turn.branch_id
    assert revised.disposition_reason == "dispatch:model_structured_revision"


def test_runtime_structured_revision_links_from_same_conversation_event_when_branch_state_is_missing() -> None:
    store = RuntimeStore()
    conversation_id = "web:event-backed-related-follow-up"
    now = datetime(2026, 6, 17, 9, 33, 29, tzinfo=UTC)
    store.add_conversation_event(
        {
            "event_type": "conversation.turn_recorded",
            "conversation_id": conversation_id,
            "turn_id": "turn-artifact",
            "branch_id": "branch-artifact",
            "user_message": "artifact request",
            "created_at": now.isoformat(),
            "disposition": ConversationTurnDisposition.INDEPENDENT.value,
            "disposition_reason": "dispatch:no_active_turn",
        }
    )

    revised = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="web",
        user_message="revise that artifact",
        request_id="web-follow-up",
        message_id="web-follow-up",
        turn_id="turn-follow-up",
        received_at=datetime(2026, 6, 17, 9, 33, 52, tzinfo=UTC),
        dispatch_disposition=ConversationTurnDisposition.REVISE,
        dispatch_dependency_turn_ids=("turn-artifact",),
        dispatch_reason="model_structured_revision",
    )

    assert revised.turn.parent_turn_id == "turn-artifact"
    assert revised.turn.disposition is ConversationTurnDisposition.REVISE
    assert revised.disposition_reason == "dispatch:model_structured_revision"
    assert revised.superseded_branch_id == "branch-artifact"
    restored_branch = store.get_conversation_branch("branch-artifact")
    assert restored_branch is not None
    assert restored_branch.status is ConversationBranchStatus.SUPERSEDED
    follow_up_events = [
        event
        for event in store.list_conversation_events(conversation_id)
        if event.get("turn_id") == "turn-follow-up" and event.get("event_type") == "conversation.turn_recorded"
    ]
    assert follow_up_events
    assert follow_up_events[-1]["dispatch_parent_turn_id"] == "turn-artifact"
    assert follow_up_events[-1]["dispatch_parent_branch_id"] == "branch-artifact"
    assert follow_up_events[-1]["dispatch_linked"] is True


def test_runtime_ignores_unverified_dispatch_dependency() -> None:
    store = RuntimeStore()
    conversation_id = "telegram:parallel-missing"
    first = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="メールの新着を確認する",
        request_id="telegram-update:email",
        message_id="telegram-message:email",
        turn_id="turn-email",
        received_at=datetime(2026, 5, 7, 19, 43, 12, tzinfo=UTC),
    )

    follow_up = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="100001",
        user_message="¿Eso requiere atención ahora?",
        request_id="telegram-update:follow-up",
        message_id="telegram-message:follow-up",
        turn_id="turn-follow-up",
        received_at=datetime(2026, 5, 7, 19, 45, 2, tzinfo=UTC),
        dispatch_disposition=ConversationTurnDisposition.CONTINUE,
        dispatch_dependency_turn_ids=("telegram-update:not-found",),
        dispatch_reason="model_structured_follow_up",
    )

    assert follow_up.turn.parent_turn_id is None
    assert follow_up.turn.branch_id != first.turn.branch_id
    assert follow_up.turn.disposition is ConversationTurnDisposition.INDEPENDENT
    assert follow_up.disposition_reason == "default_independent"


def test_runtime_honors_structured_independent_dispatch_over_ambiguity_classifier() -> None:
    store = RuntimeStore()
    conversation_id = "web:parallel-independent"
    artifact_turn = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="web",
        user_message="Crée un paquet de fichiers de test.",
        request_id="web-artifact",
        message_id="web-artifact",
        turn_id="turn-artifact",
        received_at=datetime(2026, 5, 7, 19, 43, 12, tzinfo=UTC),
    )

    follow_up = process_conversation_message(
        store,
        conversation_id=conversation_id,
        chat_id="web",
        user_message="Sí",
        request_id="web-sure",
        message_id="web-sure",
        turn_id="turn-sure",
        received_at=datetime(2026, 5, 7, 19, 45, 2, tzinfo=UTC),
        dispatch_disposition=ConversationTurnDisposition.INDEPENDENT,
        dispatch_reason="no_active_turn_followup_evidence",
        ambiguity_classifier=lambda text, ctx: ConversationTurnDisposition.CONTINUE,
    )

    assert artifact_turn.turn.parent_turn_id is None
    assert follow_up.turn.parent_turn_id is None
    assert follow_up.turn.branch_id != artifact_turn.turn.branch_id
    assert follow_up.turn.disposition is ConversationTurnDisposition.INDEPENDENT
    assert follow_up.disposition_reason == "dispatch:no_active_turn_followup_evidence"


def test_web_task_frame_records_structured_dispatch_parent_turn(tmp_path) -> None:
    from nullion.runtime import PersistentRuntime
    from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy
    from nullion.web_app import _start_web_task_frame

    runtime = PersistentRuntime(
        store=RuntimeStore(),
        checkpoint_path=tmp_path / "runtime.db",
        started_at=datetime(2026, 5, 7, tzinfo=UTC),
    )
    conversation_id = "web:parallel"
    _, _, email_result = _start_web_task_frame(
        runtime,
        conversation_id=conversation_id,
        user_text="メールの新着を確認する",
        turn_id="web-email",
    )
    runtime.store.set_active_task_frame_id(conversation_id, None)
    _, _, cron_result = _start_web_task_frame(
        runtime,
        conversation_id=conversation_id,
        user_text="informe bursátil posterior al cierre",
        turn_id="web-cron",
    )
    runtime.store.set_active_task_frame_id(conversation_id, None)
    decision = TurnDispatchDecision(
        policy=TurnDispatchPolicy.WAIT_FOR_ACTIVE,
        dependency_turn_ids=("web-email",),
        disposition=ConversationTurnDisposition.CONTINUE,
        reason="model_structured_follow_up",
        target_active_turn_index=0,
    )

    _, _, follow_result = _start_web_task_frame(
        runtime,
        conversation_id=conversation_id,
        user_text="¿Eso requiere atención ahora?",
        turn_id="web-follow-up",
        turn_dispatch_decision=decision,
    )

    assert email_result is not None
    assert cron_result is not None
    assert follow_result is not None
    assert cron_result.turn.parent_turn_id is None
    assert follow_result.turn.parent_turn_id == email_result.turn.turn_id
    assert follow_result.turn.branch_id == email_result.turn.branch_id
    assert follow_result.turn.branch_id != cron_result.turn.branch_id
    assert follow_result.disposition_reason == "dispatch:model_structured_follow_up"


def test_runtime_dispatch_parent_resolution_uses_same_conversation_event_on_turn_id_collision() -> None:
    store = RuntimeStore()
    now = datetime(2026, 5, 7, 19, 43, 12, tzinfo=UTC)
    store.add_conversation_branch(
        ConversationBranch(
            branch_id="stale-branch",
            conversation_id="stale-conversation",
            status=ConversationBranchStatus.ACTIVE,
            created_from_turn_id="turn-colliding",
        )
    )
    store.add_conversation_turn(
        ConversationTurn(
            turn_id="turn-colliding",
            conversation_id="stale-conversation",
            branch_id="stale-branch",
            parent_turn_id=None,
            disposition=ConversationTurnDisposition.INDEPENDENT,
            user_message="old request",
            status="accepted",
            created_at=now,
        )
    )
    store.add_conversation_branch(
        ConversationBranch(
            branch_id="current-branch",
            conversation_id="current-conversation",
            status=ConversationBranchStatus.ACTIVE,
            created_from_turn_id="turn-colliding",
        )
    )
    store.add_conversation_event(
        {
            "event_type": "conversation.message_received",
            "conversation_id": "current-conversation",
            "turn_id": "turn-colliding",
            "branch_id": "current-branch",
            "user_message": "current request",
            "created_at": now.isoformat(),
        }
    )

    follow_up = process_conversation_message(
        store,
        conversation_id="current-conversation",
        chat_id="web",
        user_message="revise that active artifact",
        turn_id="turn-follow-up",
        received_at=now,
        dispatch_disposition=ConversationTurnDisposition.REVISE,
        dispatch_dependency_turn_ids=("turn-colliding",),
        dispatch_reason="model_structured_revision",
    )

    assert follow_up.turn.parent_turn_id == "turn-colliding"
    assert follow_up.turn.disposition is ConversationTurnDisposition.REVISE
    assert follow_up.disposition_reason == "dispatch:model_structured_revision"
    assert follow_up.turn.branch_id != "current-branch"
    assert store.get_conversation_branch("current-branch").status is ConversationBranchStatus.SUPERSEDED


def test_task_planner_ignores_web_composer_mode_prefix_for_dispatch_shape() -> None:
    planner = TaskPlanner()
    plan = planner.build_execution_plan(
        user_message="Mode: Diagnose. Investigate the system, explain evidence, and recommend fixes. 10026",
        principal_id="p1",
        active_task_frame=None,
    )

    assert plan.disposition is PlanDisposition.SINGLE_TURN
    assert plan.dispatch_mode is PlanDispatchMode.NONE
    assert plan.mission.goal == "10026"
    assert [step.title for step in plan.mission.steps] == ["10026"]


@pytest.mark.parametrize(
    ("message", "expected_goal"),
    [
        (
            "Mode: Build. Treat this as an implementation mission. add login and update docs",
            "add login and update docs",
        ),
        (
            "mode: diagnose. investigate the system, explain evidence, and recommend fixes.  10026",
            "10026",
        ),
        (
            "Mode: Remember. Extract durable preferences or project context if appropriate. I prefer concise replies",
            "I prefer concise replies",
        ),
    ],
)
def test_task_planner_strips_all_web_composer_mode_prefixes(message: str, expected_goal: str) -> None:
    plan = TaskPlanner().build_execution_plan(
        user_message=message,
        principal_id="p1",
        active_task_frame=None,
    )

    assert plan.mission.goal == expected_goal


def test_task_planner_preserves_actual_user_mode_text_when_prefix_is_unknown() -> None:
    message = "Mode: Expert. Investigate the system, explain evidence, and recommend fixes. 10026"
    plan = TaskPlanner().build_execution_plan(
        user_message=message,
        principal_id="p1",
        active_task_frame=None,
    )

    assert plan.mission.goal == message


def test_task_planner_does_not_infer_parallel_request_after_mode_prefix() -> None:
    plan = TaskPlanner().build_execution_plan(
        user_message=(
            "Mode: Build. Treat this as an implementation mission. "
            "search docs today; check weather at the same time"
        ),
        principal_id="p1",
        active_task_frame=None,
    )

    assert plan.disposition is PlanDisposition.SINGLE_TURN
    assert plan.dispatch_mode is PlanDispatchMode.NONE
    assert [step.title for step in plan.mission.steps] == ["Search docs today; check weather at the same time"]


def test_live_information_route_resolution_and_prompt_labels() -> None:
    assert classify_live_information_route("what tools are available?") is LiveInformationRoute.NONE
    assert classify_live_information_route("example.com") is LiveInformationRoute.LIVE_LOOKUP
    assert classify_live_information_route("10001") is LiveInformationRoute.NONE
    assert classify_live_information_route("latest news near") is LiveInformationRoute.NONE
    assert classify_live_information_route("run this python script") is LiveInformationRoute.NONE
    assert route_required_plugins(LiveInformationRoute.LIVE_LOOKUP) == ("search_plugin",)
    assert actionable_live_information_resolutions()[0] is LiveInformationResolution.PREFERRED_PLUGIN_PATH
    assert format_live_information_resolution_label("core_fallback") == "core fallback path"
    assert format_live_information_resolution_label("bad") is None
    assert "approval required" in format_live_information_states_for_prompt()

    installed = SimpleNamespace(is_plugin_installed=lambda name: True)
    missing = SimpleNamespace(is_plugin_installed=lambda name: False)
    assert resolve_live_information_resolution(LiveInformationRoute.NONE, tool_registry=missing, fallback_available=False).resolution is LiveInformationResolution.NOT_REQUIRED
    assert resolve_live_information_resolution(LiveInformationRoute.LIVE_LOOKUP, tool_registry=installed, fallback_available=False).resolution is LiveInformationResolution.PREFERRED_PLUGIN_PATH
    assert resolve_live_information_resolution(LiveInformationRoute.LIVE_LOOKUP, tool_registry=missing, fallback_available=True).resolution is LiveInformationResolution.CORE_FALLBACK
    blocked = resolve_live_information_resolution(LiveInformationRoute.LIVE_LOOKUP, tool_registry=None, fallback_available=False)
    assert blocked.resolution is LiveInformationResolution.BLOCKED
    assert blocked.missing_plugins == ("search_plugin",)


def test_system_context_snapshot_formats_tools_sections_and_unavailable_capabilities() -> None:
    registry = SimpleNamespace(
        list_specs=lambda: [
            ToolSpec("file_read", "Read files", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            ToolSpec(
                "web_search",
                "Search web",
                ToolRiskLevel.MEDIUM,
                ToolSideEffectClass.READ,
                True,
                5,
                capability_tags=("network", "search"),
            ),
        ],
        list_installed_plugins=lambda: ["search_plugin", "workspace_plugin"],
    )
    snapshot = build_system_context_snapshot(
        project_summary="  Build Nullion  ",
        goals=[" ship ", ""],
        initial_focus=[" coverage "],
        tool_registry=registry,
        sections={"Notes": [" one ", ""], "": ["drop"], "Empty": []},
    )

    assert snapshot.installed_plugins == ("search_plugin", "workspace_plugin")
    assert snapshot.core_fallback_tool_names == ("file_read",)
    names = [tool.name for tool in snapshot.available_tools]
    assert "file_read" in names
    assert "file_write" in names
    assert "file_patch" in names
    prompt = format_system_context_for_prompt(snapshot)
    assert "Project summary:\nBuild Nullion" in prompt
    assert "- ship" in prompt
    assert "web_search [plugin path:search_plugin • approval required]" in prompt
    assert "tags=network,search" in prompt
    assert "file_write [core fallback • unavailable]" in prompt
    assert "Notes:\n- one" in prompt


def test_entrypoint_guard_locking_and_keyboard_interrupt(tmp_path, monkeypatch, capsys) -> None:
    assert _truthy_env("yes") is True
    assert _truthy_env("no") is False
    assert _lock_key(" My App!* ") == "my-app"
    monkeypatch.setenv("NULLION_SINGLE_INSTANCE_WAIT_SECONDS", "bad")
    assert _single_instance_wait_seconds(0.2) == 0.2
    monkeypatch.setenv("NULLION_SINGLE_INSTANCE_WAIT_SECONDS", "-5")
    assert _single_instance_wait_seconds(0.2) == 0.0

    lock = SingleInstanceLock("test", lock_dir=tmp_path)
    assert lock.acquire() is True
    competing = SingleInstanceLock("test", lock_dir=tmp_path)
    assert competing.acquire() is False
    lock.release()
    assert competing.acquire() is True
    competing.release()

    with single_instance("disabled", lock_dir=tmp_path) as acquired:
        assert acquired is True
    assert run_single_instance_entrypoint("run", lambda: "ok", lock_dir=tmp_path) == "ok"
    first = SingleInstanceLock("busy", lock_dir=tmp_path)
    assert first.acquire() is True
    assert run_single_instance_entrypoint("busy", lambda: "no", lock_dir=tmp_path, description="Busy app") is None
    assert "Busy app is already running" in capsys.readouterr().err
    first.release()

    with pytest.raises(SystemExit) as exc:
        run_user_facing_entrypoint(lambda: (_ for _ in ()).throw(KeyboardInterrupt()))
    assert exc.value.code == 130


def test_run_user_facing_entrypoint_suppresses_known_langgraph_pending_warning() -> None:
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")

        def _emit_warning() -> str:
            warnings.warn_explicit(
                "The default value of allowed_objects will change in a future version.",
                category=Warning,
                filename="langgraph/cache/base/__init__.py",
                lineno=8,
                module="langgraph.cache.base",
            )
            return "ok"

        assert run_user_facing_entrypoint(_emit_warning) == "ok"

    assert captured == []

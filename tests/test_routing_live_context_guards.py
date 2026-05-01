from __future__ import annotations

from types import SimpleNamespace

import pytest

from nullion.conversation_runtime import ConversationTurnDisposition
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
from nullion.system_context import build_system_context_snapshot, format_system_context_for_prompt
from nullion.task_frames import TaskFrameContinuationMode, resolve_task_frame_continuation
from nullion.task_planner import PlanDispatchMode, PlanDisposition, PlannerConfig, TaskPlanner
from nullion.tools import ToolRiskLevel, ToolSideEffectClass, ToolSpec


def test_intent_classification_turn_disposition_and_compound_split() -> None:
    assert classify_intent("").label is IntentLabel.AMBIGUOUS
    assert classify_intent("thanks").intent_key == "gratitude"
    assert classify_intent("good morning").intent_key == "morning"
    assert classify_intent("bye").intent_key == "farewell"
    assert classify_intent("hey 123").intent_key == "greeting"
    assert classify_intent("fetch the report").label is IntentLabel.ACTIONABLE
    assert classify_intent("hey fetch the report").intent_key == "mixed"
    assert classify_intent("send").label is IntentLabel.AMBIGUOUS
    assert classify_intent("send it").label is IntentLabel.AMBIGUOUS
    assert classify_intent("send the report file").label is IntentLabel.ACTIONABLE
    assert classify_intent("file").label is IntentLabel.AMBIGUOUS

    assert classify_turn_disposition("", False) is ConversationTurnDisposition.CHATTER
    assert classify_turn_disposition("wait stop", True) is ConversationTurnDisposition.INTERRUPT
    assert classify_turn_disposition("also do this", True) is ConversationTurnDisposition.CONTINUE
    assert classify_turn_disposition("blue", True, previous_assistant_message="Which color?") is ConversationTurnDisposition.CONTINUE
    assert classify_turn_disposition("why is that", True, previous_assistant_message="It failed.") is ConversationTurnDisposition.CONTINUE
    assert classify_turn_disposition("hello", True) is ConversationTurnDisposition.CHATTER
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

    assert split_compound_intent("fetch the page and then summarize the page") == ["fetch the page", "summarize the page"]
    assert split_compound_intent("hello and then summarize") == ["hello and then summarize"]


def test_task_planner_builds_single_parallel_and_sequential_missions() -> None:
    planner = TaskPlanner()
    single = planner.build_execution_plan(user_message="Fetch https://example.com", principal_id="p1", active_task_frame=None)
    assert single.disposition is PlanDisposition.SINGLE_TURN
    assert single.dispatch_mode is PlanDispatchMode.NONE
    assert single.mission.steps[0].metadata["tool_scope"] == ["web_fetch"]
    assert single.mission.steps[0].metadata["deep_agent_profiles"] == ["research"]
    assert single.mission.steps[0].metadata["deep_agent_skills"] == ["/skills/nullion/"]
    assert single.mission.steps[0].metadata["deep_agent_subagents"] == ["research_agent"]

    parallel = planner.build_execution_plan(
        user_message="search docs today and email summary now",
        principal_id="p1",
        active_task_frame=None,
    )
    assert parallel.disposition is PlanDisposition.PARALLEL_MISSION
    assert parallel.can_dispatch_mini_agents is True
    assert parallel.can_run_mission is True
    assert [step.title for step in parallel.mission.steps] == ["Search docs today", "Email summary now"]
    assert parallel.mission.steps[0].metadata["deep_agent_profiles"] == ["research"]
    assert parallel.mission.steps[1].metadata["tool_scope"] == ["email_send"]
    assert "deep_agent_profiles" not in parallel.mission.steps[1].metadata

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
    assert sequential.disposition is PlanDisposition.SEQUENTIAL_MISSION
    assert sequential.dispatch_mode is PlanDispatchMode.SEQUENTIAL
    assert sequential.mission.steps[1].metadata["checkpoint_before"] is True
    assert sequential.mission.steps[0].metadata["active_task_frame_id"] == "frame"
    with pytest.raises(ValueError):
        planner.plan(user_message=" ", principal_id="p1", active_task_frame=None)


def test_task_frame_referential_send_continues_active_frame() -> None:
    active_frame = SimpleNamespace(
        operation=SimpleNamespace(value="generate_artifact"),
        target=SimpleNamespace(normalized_value="report"),
        output=SimpleNamespace(artifact_kind="txt", delivery_mode="attachment", response_shape="text"),
        execution=SimpleNamespace(),
        finish=SimpleNamespace(requires_attempt=True, requires_artifact_delivery=True),
    )

    referential = resolve_task_frame_continuation(
        text="send it",
        active_frame=active_frame,
        branch_continuous=True,
    )
    new_task = resolve_task_frame_continuation(
        text="send the new email",
        active_frame=active_frame,
        branch_continuous=True,
    )

    assert referential.mode is TaskFrameContinuationMode.CONTINUE
    assert new_task.mode is TaskFrameContinuationMode.START_NEW


def test_task_frame_output_override_uses_shared_attachment_format_graph() -> None:
    active_frame = SimpleNamespace(
        operation=SimpleNamespace(value="fetch_resource"),
        target=SimpleNamespace(kind="url", value="https://example.com/", normalized_value="https://example.com/"),
        output=SimpleNamespace(artifact_kind="txt", delivery_mode="attachment", response_shape="text"),
        execution=SimpleNamespace(),
        finish=SimpleNamespace(requires_attempt=True, requires_artifact_delivery=True, required_tool_completion=None),
    )

    decision = resolve_task_frame_continuation(
        text="actually make that a word document",
        active_frame=active_frame,
        branch_continuous=True,
    )

    assert decision.mode is TaskFrameContinuationMode.REVISE
    assert decision.output.artifact_kind == "docx"
    assert decision.finish.requires_artifact_delivery is True
    assert decision.finish.required_artifact_kind == "docx"


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


def test_task_planner_still_dispatches_real_parallel_request_after_mode_prefix() -> None:
    plan = TaskPlanner().build_execution_plan(
        user_message="Mode: Build. Treat this as an implementation mission. search docs today and email summary now",
        principal_id="p1",
        active_task_frame=None,
    )

    assert plan.disposition is PlanDisposition.PARALLEL_MISSION
    assert plan.dispatch_mode is PlanDispatchMode.PARALLEL
    assert [step.title for step in plan.mission.steps] == ["Search docs today", "Email summary now"]


def test_live_information_route_resolution_and_prompt_labels() -> None:
    assert classify_live_information_route("what tools are available?") is LiveInformationRoute.NONE
    assert classify_live_information_route("example.com") is LiveInformationRoute.LIVE_LOOKUP
    assert classify_live_information_route("latest news near 10001") is LiveInformationRoute.LIVE_LOOKUP
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
            ToolSpec("web_search", "Search web", ToolRiskLevel.MEDIUM, ToolSideEffectClass.READ, True, 5),
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

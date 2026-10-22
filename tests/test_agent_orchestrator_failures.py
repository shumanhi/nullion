import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from nullion.agent_orchestrator import (
    AgentOrchestrator,
    _BlockedToolRegistry,
    _artifact_file_search_budget_guard_result,
    _artifact_tool_delivery_nudge_for_missing_requirements,
    _completed_required_artifact_paths_for_turn,
    _conversation_visible_content,
    _compact_completed_artifact_producer_history,
    _compact_focused_artifact_source_history,
    _execute_agent_turn_tool_uses,
    _expand_planner_group_tool_scopes,
    _expand_planner_local_artifact_tool_scope,
    _focus_tools_for_ready_artifact_production,
    _agent_turn_finalize_node,
    _agent_turn_model_node,
    _missing_scope_action_nudge,
    _scheduler_action_contract_missing,
    _scheduler_read_completion_text,
    _scope_request_stalled_after_completed_tool,
    _message_payload_shape,
    _tool_result_message_payload,
    _weather_forecast_completion_text,
    _browser_extract_evidence_completion_text,
    _maybe_widen_scope_after_missing_connector_app_scope,
    _maybe_widen_scope_after_required_source_tool_failure,
    _maybe_widen_scope_after_scope_denial,
    _maybe_widen_scope_after_repeated_tool_failure,
    _mini_agent_run_metadata_for_task,
    _scope_recovery_capabilities_for_failed_result,
    _mini_agent_run_with_current_task_metadata,
    _should_block_failed_tool_during_recovery,
    _effective_task_timeout_seconds,
    _failed_dependency_ids,
    _group_uses_planner_budget,
    _context_input_for_task,
    _deferred_background_tool_completion_text,
    _incomplete_dependency_ids,
    _task_depends_on_any,
    _task_result_artifact_contract_failure,
    _repeated_tool_failure_message,
    _recoverable_tool_contract_failure,
    _scheduler_scope_guard_result,
    _terminal_tool_failure_text,
    _with_structured_output_path_from_turn,
)
from nullion.chat_operator import (
    _deferred_run_cron_receipt_reply,
    _run_chat_turn_common_retries,
    _run_chat_turn_model_tool_request_retry,
    _run_chat_turn_scheduler_mutation_fallback_retry,
    _run_chat_turn_scheduler_run_scope_retry,
    _scheduler_common_retries_may_bypass_before_mutation_recovery,
)
from nullion.chat_operator import _turn_still_needs_scoped_email_send as _chat_turn_still_needs_scoped_email_send
from nullion.task_decomposer import DagPlan, DecomposedTask, TaskDecomposer
from nullion.task_queue import TaskGroup, TaskPriority, TaskRecord, TaskResult, TaskStatus
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


def test_planner_mini_agent_budget_defaults_support_long_running_tasks(monkeypatch) -> None:
    from nullion.mini_agent_config import (
        mini_agent_max_continuations,
        mini_agent_max_iterations,
        mini_agent_timeout_seconds,
        planner_mini_agent_max_continuations,
        planner_mini_agent_max_iterations,
        planner_mini_agent_timeout_seconds,
    )

    for name in (
        "NULLION_MINI_AGENT_TIMEOUT_SECONDS",
        "NULLION_PLANNER_MINI_AGENT_TIMEOUT_SECONDS",
        "NULLION_MINI_AGENT_MAX_ITERATIONS",
        "NULLION_PLANNER_MINI_AGENT_MAX_ITERATIONS",
        "NULLION_MINI_AGENT_MAX_CONTINUATIONS",
        "NULLION_PLANNER_MINI_AGENT_MAX_CONTINUATIONS",
    ):
        monkeypatch.delenv(name, raising=False)

    assert mini_agent_timeout_seconds() == 180
    assert planner_mini_agent_timeout_seconds() >= 1800
    assert planner_mini_agent_timeout_seconds() > mini_agent_timeout_seconds()
    assert mini_agent_max_iterations() == 24
    assert planner_mini_agent_max_iterations() > mini_agent_max_iterations()
    assert planner_mini_agent_max_continuations() >= mini_agent_max_continuations()


def test_deferred_run_cron_receipt_reply_uses_structured_delivery_status() -> None:
    reply = _deferred_run_cron_receipt_reply(
        [
            ToolResult(
                "run-1",
                "run_cron",
                "completed",
                {
                    "delivery_status": "deferred",
                    "result_text": "Manual scheduled task run started: Daily report. The result will be delivered to this chat when ready.",
                },
            )
        ]
    )

    assert reply == "Manual scheduled task run started: Daily report. The result will be delivered to this chat when ready."


def test_structured_output_path_from_turn_uses_single_matching_artifact_filename() -> None:
    arguments = _with_structured_output_path_from_turn(
        "spreadsheet_create",
        {"rows": [{"File": "a.txt"}]},
        user_message=(
            "Download https://example.com/Archive.zip and create a workbook named "
            "stage_archive_fallback_chain.xlsx listing the real files."
        ),
    )

    assert arguments["output_path"] == "stage_archive_fallback_chain.xlsx"


def test_structured_output_path_from_turn_does_not_guess_between_same_format_names() -> None:
    arguments = _with_structured_output_path_from_turn(
        "spreadsheet_create",
        {"rows": [{"File": "a.txt"}]},
        user_message="Create first.xlsx and second.xlsx from the extracted files.",
    )

    assert "output_path" not in arguments


def test_direct_run_cron_deferred_result_is_terminal_completion_text() -> None:
    text = _deferred_background_tool_completion_text(
        ToolResult(
            "run-1",
            "run_cron",
            "completed",
            {
                "delivery_status": "deferred",
                "result_text": "Manual scheduled task run started: Daily report. The result will be delivered to this chat when ready.",
            },
        )
    )

    assert text == "Manual scheduled task run started: Daily report. The result will be delivered to this chat when ready."


def test_scheduler_run_contract_does_not_bypass_after_only_inventory_match() -> None:
    decision = TurnToolScopeDecision(
        scheduler_action="run",
        scheduler_selection_policy="user_selected",
        requested_tool_names=("list_crons", "run_cron"),
        required_tool_names=("run_cron",),
        confidence=1.0,
        valid=True,
    )
    registry = SimpleNamespace(
        turn_tool_scope_decision=decision,
        list_tool_definitions=lambda: [{"name": "list_crons"}, {"name": "run_cron"}],
        list_specs=lambda: [],
        can_invoke_tool=lambda name: name in {"list_crons", "run_cron"},
    )
    result = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Stage live scheduler manual run 6df73781e4",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "1fe692c9",
                            "name": "Stage live scheduler manual run 6df73781e4",
                            "display_name": "Stage live scheduler manual run 6df73781e4",
                            "enabled": True,
                            "has_task": True,
                        }
                    ]
                },
            )
        ],
    )

    assert not _scheduler_common_retries_may_bypass_before_mutation_recovery(
        result,
        registry,
        retry_already_scoped=False,
        user_message="Use the scheduled task Stage live scheduler manual run 6df73781e4.",
    )


def test_scheduler_inventory_bypasses_mutation_retry_for_read_only_scope() -> None:
    decision = TurnToolScopeDecision(
        scheduler_action="inspect",
        scheduler_selection_policy="none",
        requested_tool_names=("list_crons",),
        required_tool_names=(),
        confidence=1.0,
        valid=True,
    )
    registry = SimpleNamespace(
        turn_tool_scope_decision=decision,
        list_tool_definitions=lambda: [{"name": "list_crons"}, {"name": "toggle_cron"}],
        list_specs=lambda: [],
        can_invoke_tool=lambda name: name in {"list_crons", "toggle_cron"},
    )
    result = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Disabled fixture",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-disabled",
                            "name": "Disabled fixture",
                            "enabled": False,
                            "has_task": True,
                        }
                    ]
                },
            )
        ],
    )

    assert _scheduler_common_retries_may_bypass_before_mutation_recovery(
        result,
        registry,
        retry_already_scoped=False,
        user_message="opaque scheduler inventory request",
    )


def test_scheduler_inspect_list_reclassifies_to_delegate_run(monkeypatch) -> None:
    base_registry = ToolRegistry()
    run_calls: list[dict[str, object]] = []
    base_registry.register(
        ToolSpec(
            name="list_crons",
            description="List crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    base_registry.register(
        ToolSpec(
            name="run_cron",
            description="Run cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: (
            run_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "delivery_status": "deferred",
                    "result_text": "Manual scheduled task run started: Stage live scheduler manual run. The result will be delivered to this chat when ready.",
                },
            )
        ),
    )
    active_registry = ScopedTurnToolRegistry(
        base_registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="delegate_one",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Stage live scheduler manual run",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-1",
                            "name": "Stage live scheduler manual run",
                            "enabled": True,
                            "has_task": True,
                        }
                    ]
                },
            )
        ],
    )

    def fake_decision(**_kwargs):  # noqa: ANN003
        return TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="delegate_one",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr("nullion.chat_operator.build_turn_tool_scope_decision", fake_decision)

    def forbidden_retry_model(_history, _registry):  # noqa: ANN001
        raise AssertionError("structured delegate target should run directly")

    result, _retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="Choose exactly one enabled scheduled task and start it.",
        evidence=TurnToolEvidence(),
        run_turn=forbidden_retry_model,
        principal_id="workspace_admin",
    )

    assert retried is True
    assert run_calls == [{"id": "cron-1"}]
    assert [tool.tool_name for tool in result.tool_results][-1] == "run_cron"
    assert "Manual scheduled task run started" in result.final_text


def test_scheduler_inspect_list_single_candidate_direct_runs_when_classifier_selects_run(monkeypatch) -> None:
    base_registry = ToolRegistry()
    run_calls: list[dict[str, object]] = []
    base_registry.register(
        ToolSpec(
            name="list_crons",
            description="List crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    base_registry.register(
        ToolSpec(
            name="run_cron",
            description="Run cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: (
            run_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"result_text": "Manual scheduled task run started."},
            )
        ),
    )
    active_registry = ScopedTurnToolRegistry(
        base_registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Unique runnable cron",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {"crons": [{"id": "cron-42", "name": "Unique runnable cron", "enabled": True, "has_task": True}]},
            )
        ],
    )

    monkeypatch.setattr(
        "nullion.chat_operator.build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    result, _retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=base_registry,
        model_client=object(),
        user_message="Run the scheduled task named Unique runnable cron.",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("single typed target should run directly")),
        principal_id="workspace_admin",
    )

    assert retried is True
    assert run_calls == [{"name": "Unique runnable cron"}]
    assert [tool.tool_name for tool in result.tool_results][-1] == "run_cron"


def test_scheduler_run_recovery_unwraps_read_only_scope_without_base_registry(monkeypatch) -> None:
    base_registry = ToolRegistry()
    run_calls: list[dict[str, object]] = []
    base_registry.register(
        ToolSpec(
            name="list_crons",
            description="List crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    base_registry.register(
        ToolSpec(
            name="run_cron",
            description="Run cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: (
            run_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"result_text": "Manual scheduled task run started."},
            )
        ),
    )
    read_only_registry = ScopedTurnToolRegistry(
        base_registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Stage live scheduler manual run fixture",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-42",
                            "name": "Stage live scheduler manual run fixture",
                            "enabled": True,
                            "has_task": True,
                        }
                    ]
                },
            )
        ],
    )

    monkeypatch.setattr(
        "nullion.chat_operator.build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    result, _retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=read_only_registry,
        base_tool_registry=None,
        model_client=object(),
        user_message="Run the scheduled task named Stage live scheduler manual run fixture.",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should run directly")),
        principal_id="workspace_admin",
    )

    assert retried is True
    assert run_calls == [{"name": "Stage live scheduler manual run fixture"}]
    assert [tool.tool_name for tool in result.tool_results][-1] == "run_cron"


def test_common_retries_run_scheduler_action_before_generic_retries(monkeypatch) -> None:
    base_registry = ToolRegistry()
    run_calls: list[dict[str, object]] = []
    base_registry.register(
        ToolSpec(
            name="list_crons",
            description="List crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    base_registry.register(
        ToolSpec(
            name="run_cron",
            description="Run cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: (
            run_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"result_text": "Manual scheduled task run started."},
            )
        ),
    )
    read_only_registry = ScopedTurnToolRegistry(
        base_registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        turn_id="turn-scheduler-read-only",
        final_text="Scheduled-task inventory:\n1. Stage live scheduler manual run fixture",
        artifacts=[],
        suspended_for_approval=False,
        approval_id=None,
        thinking_text=None,
        reached_iteration_limit=False,
        raw_tool_payload_blocked=False,
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-42",
                            "name": "Stage live scheduler manual run fixture",
                            "enabled": True,
                            "has_task": True,
                        }
                    ]
                },
            )
        ],
    )

    classifier_calls: list[str] = []

    def classify_scheduler_action(**kwargs):  # noqa: ANN003
        classifier_calls.append(str(kwargs.get("user_message") or ""))
        return TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr("nullion.chat_operator.build_turn_tool_scope_decision", classify_scheduler_action)

    retry_result = _run_chat_turn_common_retries(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=read_only_registry,
        base_tool_registry=None,
        runtime=None,
        model_client=object(),
        user_message="Find the enabled scheduled task named 'Stage live scheduler manual run fixture' and start one manual run now.",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("scheduler direct action should not call a generic retry turn")
        ),
        principal_id="workspace_admin",
    )

    assert retry_result.retried_scheduler_run_scope is True
    assert classifier_calls == []
    assert run_calls == [{"name": "Stage live scheduler manual run fixture"}]
    assert [tool.tool_name for tool in retry_result.result.tool_results][-1] == "run_cron"


def test_model_tool_request_scheduler_mutation_directly_toggles_all_current_workspace() -> None:
    base_registry = ToolRegistry()
    toggle_calls: list[dict[str, object]] = []
    base_registry.register(
        ToolSpec(
            name="toggle_cron",
            description="Toggle cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: (
            toggle_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "action_receipt": {"summary": "Enabled 1 cron job."},
                    "message": "Enabled 1 cron job.",
                    "updated_count": 1,
                },
            )
        ),
    )
    initial_result = SimpleNamespace(
        turn_id="turn-model-tool-scheduler-mutation",
        final_text=(
            '<tool_request scheduler_action="mutate" scheduler_target_scope="all_current_workspace" '
            'scheduler_toggle_enabled="true" tool_names="toggle_cron"></tool_request>'
        ),
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[],
    )

    result, retry_registry, retried = _run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=base_registry,
        base_tool_registry=base_registry,
        runtime=None,
        model_client=object(),
        principal_id="workspace_admin",
        user_message="opaque scheduler mutation request",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("typed scheduler mutation should execute directly")
        ),
    )

    assert retried is True
    assert toggle_calls == [{"all_current_workspace": True, "enabled": True}]
    assert result.final_text == "Enabled 1 cron job."
    assert [tool.tool_name for tool in result.tool_results] == ["toggle_cron"]
    assert retry_registry.can_invoke_tool("toggle_cron") is True


def test_model_tool_request_scheduler_run_directly_invokes_run_cron() -> None:
    base_registry = ToolRegistry()
    run_calls: list[dict[str, object]] = []
    base_registry.register(
        ToolSpec(
            name="run_cron",
            description="Run cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: (
            run_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"result_text": "Manual scheduled task run started."},
            )
        ),
    )
    initial_result = SimpleNamespace(
        turn_id="turn-model-tool-scheduler-run",
        final_text='<tool_request scheduler_action="run" tool_names="run_cron">{"tool":"run_cron","cron_name":"Daily digest"}</tool_request>',
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[],
    )

    result, retry_registry, retried = _run_chat_turn_model_tool_request_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=base_registry,
        base_tool_registry=base_registry,
        runtime=None,
        model_client=object(),
        principal_id="workspace_admin",
        user_message="opaque scheduler run request",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("typed scheduler run should execute directly")
        ),
    )

    assert retried is True
    assert run_calls == [{"name": "Daily digest"}]
    assert result.final_text == "Manual scheduled task run started."
    assert [tool.tool_name for tool in result.tool_results] == ["run_cron"]
    assert retry_registry.can_invoke_tool("run_cron") is True


def test_scheduler_mutation_recovery_unwraps_read_only_scope_without_base_registry(monkeypatch) -> None:
    base_registry = ToolRegistry()
    toggle_calls: list[dict[str, object]] = []
    base_registry.register(
        ToolSpec(
            name="list_crons",
            description="List crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    base_registry.register(
        ToolSpec(
            name="toggle_cron",
            description="Toggle cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler",),
        ),
        lambda invocation: (
            toggle_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"message": "Enabled 1 cron job.", "updated_count": 1},
            )
        ),
    )
    read_only_registry = ScopedTurnToolRegistry(
        base_registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            scheduler_target_scope="all_current_workspace",
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        ),
    )
    initial_result = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Disabled fixture",
        artifacts=[],
        suspended_for_approval=False,
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {"crons": [{"id": "cron-42", "name": "Disabled fixture", "enabled": False, "has_task": True}]},
            )
        ],
    )

    monkeypatch.setattr(
        "nullion.chat_operator.build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_selection_policy="none",
            scheduler_target_scope="all_current_workspace",
            scheduler_toggle_enabled=True,
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    result, _retry_registry, retried = _run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial_result,
        base_history=[],
        active_tool_registry=read_only_registry,
        base_tool_registry=None,
        model_client=object(),
        user_message="opaque scheduler mutation request",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should mutate directly")),
        principal_id="workspace_admin",
    )

    assert retried is True
    assert toggle_calls == [{"all_current_workspace": True, "enabled": True}]
    assert [tool.tool_name for tool in result.tool_results][-1] == "toggle_cron"


def test_weather_forecast_failure_is_terminal_after_bounded_retries() -> None:
    text = _terminal_tool_failure_text(
        ToolResult(
            "weather-1",
            "weather_forecast",
            "failed",
            {"reason": "weather_forecast_failed"},
            "<urlopen error temporary TLS reset>",
        )
    )

    assert text is not None
    assert "weather service failed after bounded retries" in text
    assert "try again" in text.lower()


def test_weather_forecast_completed_result_has_direct_summary() -> None:
    text = _weather_forecast_completion_text(
        ToolResult(
            "weather-1",
            "weather_forecast",
            "completed",
            {
                "location": {"name": "New York, New York, United States"},
                "current": {"temperature_2m": 71.3, "apparent_temperature": 77.0},
                "daily": [
                    {
                        "summary": "light drizzle",
                        "temperature_max_f": 96.6,
                        "temperature_min_f": 71.1,
                        "precipitation_probability_max": 26,
                    }
                ],
            },
        )
    )

    assert text is not None
    assert "New York, New York, United States" in text
    assert "71°F" in text
    assert "feeling like **77°F**" in text
    assert "light drizzle" in text
    assert "precipitation chance **26%**" in text


def test_browser_extract_evidence_completion_does_not_promote_raw_page_after_search_failure() -> None:
    text = _browser_extract_evidence_completion_text(
        state={"user_message": "Can you research NYC Citi Bike e-bike helmet requirements for adults?"},
        tool_results=[
            ToolResult("search", "web_search", "failed", {"error": "HTTP 402"}, "HTTP Error 402"),
            ToolResult("nav", "browser_navigate", "completed", {"url": "https://example.test/citi-bike"}),
            ToolResult(
                "extract",
                "browser_extract_text",
                "completed",
                {
                    "url": "https://www.nyc.gov/html/dot/html/bicyclists/ebikes.shtml",
                    "text": (
                        "Electric Bicycles & More\n"
                        "Do I need to wear a helmet? | Recommended (Yes, if working) | Recommended | Yes\n"
                        "The speed limit for e-bike and pedal assist bicycle riders is 15MPH.\n"
                    ),
                },
            ),
        ],
        artifacts=[],
    )

    assert text is None


def test_browser_extract_evidence_completion_does_not_finish_form_workflow() -> None:
    text = _browser_extract_evidence_completion_text(
        state={"user_message": "Find me a rental car with live prices."},
        tool_results=[
            ToolResult("nav", "browser_navigate", "completed", {"url": "https://example.test/rentals"}),
            ToolResult("click", "browser_click_id", "completed", {"clicked": True}),
            ToolResult(
                "extract",
                "browser_extract_text",
                "completed",
                {"text": "Cars\nSUVs\nVans\nLuxury\n"},
            ),
        ],
        artifacts=[],
    )

    assert text is None


def test_browser_extract_evidence_completion_does_not_finish_multi_symbol_quote_from_generic_page() -> None:
    text = _browser_extract_evidence_completion_text(
        state={"user_message": "Give me current market prices for CRML, MSFT, OKLO, and TMC."},
        tool_results=[
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {"required_tool_names": ["market_quote"]},
            ),
            ToolResult("search", "web_search", "failed", {"error": "HTTP 402"}, "HTTP Error 402"),
            ToolResult("nav", "browser_navigate", "completed", {"url": "https://finance.yahoo.com/quote/CRML"}),
            ToolResult(
                "extract",
                "browser_extract_text",
                "completed",
                {
                    "url": "https://finance.yahoo.com/quote/CRML",
                    "text": (
                        "Yahoo Finance | Research | US Markets | Robinhood Markets, Inc. | "
                        "Critical Metals Corp. (CRML) | 10.07 | Pre-Market: 5:56:17 AM EDT"
                    ),
                },
            ),
        ],
        artifacts=[],
    )

    assert text is None


def test_browser_extract_evidence_completion_respects_explicit_artifact_contract() -> None:
    text = _browser_extract_evidence_completion_text(
        state={
            "user_message": "Research this and create an HTML report.",
            "tool_flow_context": {"requires_artifact_delivery": True, "artifact_extensions": [".html"]},
        },
        tool_results=[
            ToolResult("search", "web_search", "failed", {"error": "HTTP 402"}, "HTTP Error 402"),
            ToolResult(
                "extract",
                "browser_extract_text",
                "completed",
                {"text": "Trail details and train directions are visible on this page."},
            ),
        ],
        artifacts=[],
    )

    assert text is None


def test_repeated_scope_requests_after_completed_tool_are_stalled() -> None:
    assert _scope_request_stalled_after_completed_tool(
        [
            ToolResult("scope-1", "request_tool_scope", "completed", {"available_tools": ["archive_extract"]}),
            ToolResult("extract-1", "archive_extract", "completed", {"entries": []}),
            ToolResult("scope-2", "request_tool_scope", "completed", {"available_tools": ["spreadsheet_create"]}),
            ToolResult("scope-3", "request_tool_scope", "completed", {"available_tools": ["spreadsheet_create"]}),
        ]
    )
    assert not _scope_request_stalled_after_completed_tool(
        [
            ToolResult("extract-1", "archive_extract", "completed", {"entries": []}),
            ToolResult("scope-1", "request_tool_scope", "completed", {"available_tools": ["spreadsheet_create"]}),
        ]
    )
    assert not _scope_request_stalled_after_completed_tool(
        [
            ToolResult("extract-1", "archive_extract", "failed", {"reason": "bad_archive_file"}),
            ToolResult("scope-1", "request_tool_scope", "completed", {"available_tools": ["file_read"]}),
            ToolResult("scope-2", "request_tool_scope", "completed", {"available_tools": ["file_read"]}),
        ]
    )


def test_planner_budget_uses_typed_group_state_not_prompt_text() -> None:
    typed_planner_group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="single delegated request",
        tasks=[],
        planner_metadata={"disposition": "sequential_mission"},
    )
    multi_task_group = TaskGroup(
        group_id="grp-multi",
        conversation_id="telegram:123",
        original_message="short request",
        tasks=[
            TaskRecord(
                task_id="task-a",
                group_id="grp-multi",
                conversation_id="telegram:123",
                principal_id="telegram:123",
                title="A",
                description="A",
                status=TaskStatus.QUEUED,
                priority=TaskPriority.NORMAL,
                allowed_tools=[],
                dependencies=[],
            ),
            TaskRecord(
                task_id="task-b",
                group_id="grp-multi",
                conversation_id="telegram:123",
                principal_id="telegram:123",
                title="B",
                description="B",
                status=TaskStatus.PENDING,
                priority=TaskPriority.NORMAL,
                allowed_tools=[],
                dependencies=["task-a"],
            ),
        ],
    )
    ordinary_group = TaskGroup(
        group_id="grp-plain",
        conversation_id="telegram:123",
        original_message="/planner appears here but without typed planner state",
        tasks=[
            TaskRecord(
                task_id="task-only",
                group_id="grp-plain",
                conversation_id="telegram:123",
                principal_id="telegram:123",
                title="Only",
                description="Only",
                status=TaskStatus.QUEUED,
                priority=TaskPriority.NORMAL,
                allowed_tools=[],
                dependencies=[],
            ),
        ],
    )

    assert _group_uses_planner_budget(typed_planner_group) is True
    assert _group_uses_planner_budget(multi_task_group) is True
    assert _group_uses_planner_budget(ordinary_group) is False


def test_effective_task_timeout_honors_planner_group_state(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_PLANNER_MINI_AGENT_TIMEOUT_SECONDS", "1800")
    task = TaskRecord(
        task_id="task-short",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create workbook from structured data.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
        timeout_s=420,
    )
    planner_group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="Build a large artifact.",
        tasks=[task],
        planner_metadata={"disposition": "sequential_mission"},
    )
    ordinary_group = TaskGroup(
        group_id="grp-plain",
        conversation_id="telegram:123",
        original_message="Plain delegated task.",
        tasks=[task],
    )

    assert _effective_task_timeout_seconds(task, planner_group) == 1800
    assert _effective_task_timeout_seconds(task, ordinary_group) == 420


def test_dependency_context_uses_completed_task_result_without_model_context_key(tmp_path) -> None:
    source_file = tmp_path / "source_records.json"
    source_file.write_text('[{"name":"alpha","value":1}]', encoding="utf-8")
    collect = TaskRecord(
        task_id="task-collect",
        group_id="grp-handoff",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect source records",
        description="Collect source records.",
        status=TaskStatus.COMPLETE,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_extract_items", "file_write"],
        dependencies=[],
        result=TaskResult(
            "task-collect",
            "success",
            output="Saved source records.",
            context_out={
                "output": "Saved source records.",
                "workspace_files": [{"path": str(source_file), "source_tool": "file_write"}],
                "workspace_file_paths": [str(source_file)],
            },
        ),
    )
    create = TaskRecord(
        task_id="task-create",
        group_id="grp-handoff",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create workbook from collected source records.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create", "file_read"],
        dependencies=["task-collect"],
        context_key_in=None,
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    group = TaskGroup(
        group_id="grp-handoff",
        conversation_id="telegram:123",
        original_message="Create a workbook from collected source records.",
        tasks=[collect, create],
        planner_metadata={"disposition": "sequential_mission"},
    )

    context = _context_input_for_task(create, group, context_bus=None)

    assert isinstance(context, dict)
    assert context["schema"] == "nullion.dependency_context.v1"
    assert context["workspace_file_paths"] == [str(source_file.resolve())]
    assert context["dependencies"][0]["task_id"] == "task-collect"
    assert context["dependencies"][0]["workspace_file_paths"] == [str(source_file.resolve())]


def test_mini_agent_run_metadata_includes_structured_task_scope() -> None:
    task = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create the structured workbook.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create", "file_read"],
        dependencies=["task-inspect"],
        metadata={
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
            "artifact_role": "producer",
        },
    )

    metadata = _mini_agent_run_metadata_for_task(task)

    assert metadata["task_id"] == "task-create"
    assert metadata["group_id"] == "grp-planner"
    assert metadata["conversation_id"] == "telegram:123"
    assert metadata["dependencies"] == ("task-inspect",)
    assert metadata["allowed_tools"] == ("spreadsheet_create", "file_read")
    assert metadata["requires_artifact_delivery"] is True
    assert metadata["required_artifact_kind"] == "xlsx"


def test_mini_agent_run_metadata_refreshes_recovered_scope() -> None:
    from nullion.mini_agent_runs import create_mini_agent_run

    task = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create the structured workbook.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create", "file_read", "workspace_summary"],
        dependencies=[],
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    run = create_mini_agent_run(
        run_id="task-create",
        capsule_id="grp-planner",
        mini_agent_type="Create workbook",
        created_at=task.created_at,
        metadata={"allowed_tools": ("spreadsheet_create",)},
    )

    refreshed = _mini_agent_run_with_current_task_metadata(run, task)

    assert refreshed.metadata["allowed_tools"] == ("spreadsheet_create", "file_read", "workspace_summary")
    assert refreshed.metadata["requires_artifact_delivery"] is True
    assert refreshed.metadata["required_artifact_kind"] == "xlsx"


def test_failed_dependency_scan_does_not_pierce_active_dependency() -> None:
    source = TaskRecord(
        task_id="task-source",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Source",
        description="Collect source data.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )
    producer = TaskRecord(
        task_id="task-producer",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Producer",
        description="Create the artifact.",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-source"],
    )
    verifier = TaskRecord(
        task_id="task-verifier",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Verifier",
        description="Verify the artifact.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read"],
        dependencies=["task-producer"],
    )

    assert _failed_dependency_ids(
        verifier,
        {"task-source"},
        {task.task_id: task for task in (source, producer, verifier)},
    ) == []


def test_failed_dependency_scan_still_reports_blocked_intermediate_root() -> None:
    source = TaskRecord(
        task_id="task-source",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Source",
        description="Collect source data.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )
    producer = TaskRecord(
        task_id="task-producer",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Producer",
        description="Create the artifact.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-source"],
    )
    verifier = TaskRecord(
        task_id="task-verifier",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Verifier",
        description="Verify the artifact.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read"],
        dependencies=["task-producer"],
    )

    assert _failed_dependency_ids(
        verifier,
        {"task-source"},
        {task.task_id: task for task in (source, producer, verifier)},
    ) == ["task-source"]


def test_recoverable_artifact_producer_defers_downstream_failure_cascade() -> None:
    source = TaskRecord(
        task_id="task-source",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Source",
        description="Collect source data.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_run_js"],
        dependencies=[],
    )
    producer = TaskRecord(
        task_id="task-producer",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Producer",
        description="Create the artifact.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-source"],
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    verifier = TaskRecord(
        task_id="task-verifier",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Verifier",
        description="Verify the artifact.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read"],
        dependencies=["task-producer"],
        metadata={"artifact_role": "verify", "requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    receipt = TaskRecord(
        task_id="task-receipt",
        group_id="grp",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Receipt",
        description="Deliver the artifact receipt.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=["task-verifier"],
        metadata={"artifact_role": "deliver_receipt", "requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    tasks_by_id = {task.task_id: task for task in (source, producer, verifier, receipt)}

    assert _failed_dependency_ids(verifier, {"task-source"}, tasks_by_id) == ["task-source"]
    assert _task_depends_on_any(tasks_by_id, verifier, {"task-producer"}) is True
    assert _task_depends_on_any(tasks_by_id, receipt, {"task-producer"}) is True
    assert _task_depends_on_any(tasks_by_id, source, {"task-producer"}) is False


def test_runner_reblocks_queued_task_with_incomplete_dependencies() -> None:
    from nullion.task_queue import TaskRegistry

    registry = TaskRegistry()
    source = TaskRecord(
        task_id="task-source",
        group_id="grp-guard",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect source records",
        description="Collect source records.",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_run_js"],
        dependencies=[],
    )
    producer = TaskRecord(
        task_id="task-create",
        group_id="grp-guard",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create the requested workbook.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-source"],
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    group = TaskGroup(
        group_id="grp-guard",
        conversation_id="telegram:123",
        original_message="Create a workbook from source records.",
        tasks=[source, producer],
        planner_metadata={"disposition": "sequential_mission"},
    )
    acquired: list[str] = []

    class GuardedPool:
        async def acquire(self, *, preferred_tools=None, task_id=None):
            acquired.append(str(task_id))
            raise AssertionError("producer should not acquire an agent before dependencies complete")

        def release(self, _agent) -> None:
            raise AssertionError("no agent should have been acquired")

    async def run_guard() -> None:
        await registry.add_group(group)
        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator._task_registry = registry
        orchestrator._pool = GuardedPool()
        assert _incomplete_dependency_ids(producer, group) == ["task-source"]
        await orchestrator._run_task_inner(
            producer,
            runner=SimpleNamespace(),
            group=group,
            tool_registry=ToolRegistry(),
            policy_store=None,
            approval_store=None,
        )

    asyncio.run(run_guard())

    updated = registry.get_task("task-create")
    assert acquired == []
    assert updated is not None
    assert updated.status is TaskStatus.BLOCKED
    assert updated.started_at is None
    assert updated.agent_id is None


def test_repeated_tool_failure_reply_hides_raw_tool_trace():
    reply = _repeated_tool_failure_message(
        result=ToolResult(
            invocation_id="tool-1",
            tool_name="browser_click",
            status="failed",
            output={},
            error=(
                "Click failed: Page.click: Timeout 10000ms exceeded.\n"
                "Call log:\n"
                "  - waiting for locator(\"a[href=\\\"javascript:dateFilterChanged('2026-5-3')\\\"]\")"
            ),
        ),
        repeated_count=2,
    )

    assert "same browser click step failed 2 times" in reply
    assert "Page.click" not in reply
    assert "Call log" not in reply
    assert "locator" not in reply
    assert "javascript:" not in reply


def test_repeated_email_failure_respects_expressive_emoji_preference(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_HOME", str(tmp_path))
    from nullion.preferences import Preferences, save_preferences

    save_preferences(Preferences(emoji_level="expressive"))

    reply = _repeated_tool_failure_message(
        result=ToolResult(
            invocation_id="tool-1",
            tool_name="email_read",
            status="failed",
            output={},
            error="temporary provider failure",
        ),
        repeated_count=3,
    )

    assert reply.startswith("✉️ ")
    assert "fresh email search" in reply
    assert "same email read step failed 3 times" not in reply


def test_run_cron_no_match_failure_is_terminal_user_reply() -> None:
    reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-1",
            tool_name="run_cron",
            status="failed",
            output={"id": "", "name": "stage telegram access smoke 1208"},
            error="No cron found for 'stage telegram access smoke 1208'",
        )
    )

    assert reply == "I couldn't find a scheduled cron job for `stage telegram access smoke 1208`."


def test_run_cron_no_enabled_failure_is_terminal_user_reply() -> None:
    reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-1",
            tool_name="run_cron",
            status="failed",
            output={"all_enabled": True, "started_count": 0},
            error="No enabled cron jobs found in this workspace.",
        )
    )

    assert reply == "I couldn't find any enabled scheduled cron jobs to run."


def test_email_read_invalid_message_id_failure_is_terminal_user_reply() -> None:
    reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-1",
            tool_name="email_read",
            status="failed",
            output={"reason": "invalid_message_id", "id": "?unknown", "required_source_tool": "email_search"},
            error="Invalid email message id: use an id returned by email_search.",
        )
    )

    assert reply is not None
    assert "search email" in reply
    assert "message id" not in reply.lower()
    assert "failed 3 times" not in reply


def test_email_read_missing_id_failure_is_terminal_user_reply_without_internal_id() -> None:
    reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-1",
            tool_name="email_read",
            status="failed",
            output={"reason": "invalid_tool_arguments", "error": "missing required argument"},
            error="Missing required argument: id",
        )
    )

    assert reply is not None
    assert "search email" in reply
    assert "message id" not in reply.lower()
    assert "required argument" not in reply.lower()


def test_invalid_structured_ids_are_terminal_user_replies() -> None:
    cron_reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-1",
            tool_name="delete_cron",
            status="failed",
            output={"reason": "invalid_cron_id", "id": "?unknown", "required_source_tool": "list_crons"},
            error="Invalid id: use an exact id returned by list_crons.",
        )
    )
    cron_name_reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-1b",
            tool_name="run_cron",
            status="failed",
            output={"reason": "invalid_cron_name", "name": "__list__", "required_source_tool": "list_crons"},
            error="Invalid name: use an exact id returned by list_crons.",
        )
    )
    reminder_reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-2",
            tool_name="delete_reminder",
            status="failed",
            output={
                "reason": "invalid_reminder_task_id",
                "task_id": "?unknown",
                "required_source_tool": "list_reminders",
            },
            error="Invalid task_id: use an exact id returned by list_reminders.",
        )
    )

    assert cron_reply == "I couldn't use that scheduled-task id because it was not a concrete id from `list_crons`."
    assert cron_name_reply == "I couldn't use that scheduled-task name because it was not a concrete name from `list_crons`."
    assert reminder_reply == "I couldn't use that reminder id because it was not a concrete id from `list_reminders`."


def test_list_crons_model_context_keeps_compact_task_excerpt_without_private_delivery_target() -> None:
    long_task = (
        "START: Investigate this recurring job and compare email attachments. "
        + ("middle instructions. " * 500)
        + "TAIL: mention missing checklist documents before answering."
    )
    result = ToolResult(
        invocation_id="list-1",
        tool_name="list_crons",
        status="completed",
        output={
            "message": "Here are your crons:\n\n1. Research tracker\n   Status: enabled",
            "crons": [
                {
                    "selection_index": 1,
                    "id": "cron-1",
                    "name": "Research tracker",
                    "display_name": "Research tracker",
                    "enabled": True,
                    "schedule_description": "Every hour",
                    "next_run_description": "Today at 5:00 PM",
                    "run_by_name": 'run_cron name="Research tracker"',
                    "task": long_task,
                    "last_run": "2026-05-31T12:00:00+00:00",
                    "last_result": "Large prior result " * 500,
                    "delivery_target": "private-chat-id",
                }
            ],
        },
    )

    payload = _tool_result_message_payload(result)
    parsed = json.loads(payload)
    compact_cron = parsed["output"]["crons"][0]

    assert "Research tracker" in payload
    assert "run_cron name" in payload
    assert "START: Investigate this recurring job" in compact_cron["task_excerpt"]
    assert "TAIL: mention missing checklist documents" in compact_cron["task_excerpt"]
    assert "chars omitted" in compact_cron["task_excerpt"]
    assert compact_cron["task_original_chars"] == len(" ".join(long_task.split()))
    assert "Large prior result" not in payload
    assert "private-chat-id" not in payload
    assert len(parsed["output"]["message"]) <= 700


def test_scheduler_read_completion_summarizes_inventory_without_model_dump() -> None:
    crons = [
        {
            "selection_index": index,
            "display_name": f"Stage QA cron {index}",
            "enabled": index % 2 == 0,
            "schedule_description": "Every day at 8:00 AM",
            "next_run_description": "Tomorrow",
        }
        for index in range(1, 13)
    ]
    result = ToolResult(
        invocation_id="list-1",
        tool_name="list_crons",
        status="completed",
        output={"crons": crons, "message": "internal raw inventory"},
    )

    text = _scheduler_read_completion_text(result, tool_registry=None, tool_results=[result])

    assert text is not None
    assert text.startswith("Scheduled-task inventory:")
    assert "1. Stage QA cron 1 - disabled - Every day at 8:00 AM - next: Tomorrow" in text
    assert "10. Stage QA cron 10" in text
    assert "Stage QA cron 11" not in text
    assert "...and 2 more." in text
    assert "internal raw inventory" not in text


def test_scheduler_read_completion_returns_single_structured_match_without_inventory_dump() -> None:
    crons = [
        {
            "selection_index": index,
            "display_name": f"Unrelated scheduled task {index}",
            "enabled": True,
            "schedule_description": "Every day",
            "next_run_description": "Tomorrow",
        }
        for index in range(1, 15)
    ]
    crons.append(
        {
            "selection_index": 15,
            "id": "target-cron",
            "name": "Stage live scheduler short followup target",
            "display_name": "Stage live scheduler short followup target",
            "enabled": True,
            "schedule_description": "Every 30 minutes",
            "next_run_description": "Today",
            "has_task": True,
        }
    )
    result = ToolResult(
        invocation_id="list-1",
        tool_name="list_crons",
        status="completed",
        output={"crons": crons, "message": "internal raw inventory"},
    )

    text = _scheduler_read_completion_text(
        result,
        tool_registry=None,
        tool_results=[result],
        user_message="Find the cron named 'Stage live scheduler short followup target'.",
    )

    assert text is None  # The model must answer the original request from evidence.


def test_scheduler_read_completion_defers_when_run_scope_is_required() -> None:
    result = ToolResult(
        invocation_id="list-1",
        tool_name="list_crons",
        status="completed",
        output={"crons": [{"selection_index": 1, "display_name": "Run me", "enabled": True}]},
    )
    scope = ToolResult(
        invocation_id="scope-1",
        tool_name="request_tool_scope",
        status="completed",
        output={"required_tool_names": ["run_cron"]},
    )

    assert _scheduler_read_completion_text(result, tool_registry=None, tool_results=[scope, result]) is None


def test_scheduler_read_completion_defers_when_run_tool_is_visible() -> None:
    registry = ToolRegistry()
    for name, side_effect in (
        ("list_crons", ToolSideEffectClass.READ),
        ("run_cron", ToolSideEffectClass.WRITE),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    result = ToolResult(
        invocation_id="list-1",
        tool_name="list_crons",
        status="completed",
        output={"crons": [{"selection_index": 1, "display_name": "Run me", "enabled": True}]},
    )

    assert _scheduler_read_completion_text(result, tool_registry=registry, tool_results=[result]) is None


def test_scheduler_read_completion_uses_scoped_available_tools_over_base_registry() -> None:
    registry = ToolRegistry()
    for name, side_effect in (
        ("list_crons", ToolSideEffectClass.READ),
        ("run_cron", ToolSideEffectClass.WRITE),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    scope = ToolResult(
        invocation_id="scope-1",
        tool_name="request_tool_scope",
        status="completed",
        output={"available_tools": ["list_crons"], "capabilities": ["scheduler_read"], "scheduler_action": "inspect"},
    )
    result = ToolResult(
        invocation_id="list-1",
        tool_name="list_crons",
        status="completed",
        output={"crons": [{"selection_index": 1, "display_name": "Read only", "enabled": True}]},
    )

    text = _scheduler_read_completion_text(result, tool_registry=registry, tool_results=[scope, result])

    assert text is not None
    assert "Read only - enabled" in text


def test_scheduler_read_completion_uses_latest_scope_over_stale_run_contract() -> None:
    stale_run_decision = TurnToolScopeDecision(
        scheduler_action="run",
        scheduler_selection_policy="user_selected",
        requested_tool_names=("list_crons", "run_cron"),
        required_tool_names=("run_cron",),
        confidence=1.0,
        valid=True,
    )
    registry = SimpleNamespace(
        turn_tool_scope_decision=stale_run_decision,
        list_tool_definitions=lambda: [{"name": "list_crons"}, {"name": "run_cron"}],
        list_specs=lambda: [],
        can_invoke_tool=lambda name: name in {"list_crons", "run_cron"},
    )
    scope = ToolResult(
        invocation_id="scope-1",
        tool_name="request_tool_scope",
        status="completed",
        output={
            "available_tools": ["list_crons"],
            "capabilities": ["scheduler_read"],
            "required_tool_names": [],
            "scheduler_action": "inspect",
            "scheduler_selection_policy": "none",
        },
    )
    result = ToolResult(
        invocation_id="list-1",
        tool_name="list_crons",
        status="completed",
        output={
            "crons": [
                {
                    "selection_index": 33,
                    "id": "target-cron",
                    "display_name": "Stage live scheduler short followup target",
                    "enabled": True,
                    "has_task": True,
                }
            ]
        },
    )

    text = _scheduler_read_completion_text(
        result,
        tool_registry=registry,
        tool_results=[scope, result],
        user_message="Find the cron named Stage live scheduler short followup target.",
    )

    assert text is None


def test_scheduler_read_turn_finishes_after_unique_match_despite_stale_run_contract() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-read",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_read"],
                                "scheduler_action": "inspect",
                                "scheduler_selection_policy": "none",
                                "tool_names": ["list_crons"],
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "tool_use", "id": "list", "name": "list_crons", "input": {}}
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text":
                "The selected tracker is enabled."}]}

    base_registry = ToolRegistry()
    for name, side_effect in (
        ("list_crons", ToolSideEffectClass.READ),
        ("run_cron", ToolSideEffectClass.WRITE),
    ):
        base_registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=side_effect,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "crons": [
                        {
                            "selection_index": 33,
                            "id": "target-cron",
                            "display_name": "Stage live scheduler short followup target",
                            "enabled": True,
                            "has_task": True,
                        }
                    ]
                }
                if invocation.tool_name == "list_crons"
                else {"message": "unexpected run"},
            ),
        )
    stale_run_decision = TurnToolScopeDecision(
        scheduler_action="run",
        scheduler_selection_policy="user_selected",
        requested_tool_names=("list_crons", "run_cron"),
        required_tool_names=("run_cron",),
        confidence=1.0,
        valid=True,
    )
    scoped_registry = ScopedTurnToolRegistry(
        base_registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=stale_run_decision,
    )
    client = Client()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="web:scheduler-read-stale-run",
        principal_id="web:scheduler-read-stale-run",
        user_message="Find the cron named Stage live scheduler short followup target.",
        conversation_history=[],
        tool_registry=scoped_registry,
        policy_store=None,
        approval_store=None,
        max_iterations=6,
    )

    assert client.calls == 3
    assert [tool.tool_name for tool in result.tool_results] == ["request_tool_scope", "list_crons"]
    assert result.final_text == "The selected tracker is enabled."


def test_archive_extract_model_context_keeps_deliverable_and_compacts_sidecars() -> None:
    entries = [
        {
            "name": f"item-{index}.txt",
            "path": f"/workspace/extracted/item-{index}.txt",
            "bytes": index,
            "media_type": "text/plain",
        }
        for index in range(1200)
    ]
    result = ToolResult(
        invocation_id="archive-1",
        tool_name="archive_extract",
        status="completed",
        output={
            "archive_path": "/workspace/Archive.zip",
            "archive_format": "zip",
            "output_dir": "/workspace/Archive_extracted",
            "entries": entries,
            "entry_count": 1200,
            "file_count": 1200,
            "metadata_entries_ignored": 12,
            "unsafe_entries_ignored": 0,
            "truncated": False,
            "manifest_csv_path": "/workspace/Archive_extracted_manifest.csv",
            "manifest_json_path": "/workspace/Archive_extracted_manifest.json",
            "manifest_xlsx_path": "/workspace/Archive_extracted_manifest.xlsx",
            "manifest_row_count": 1200,
            "manifest_artifact_paths": [
                "/workspace/Archive_extracted_manifest.xlsx",
                "/workspace/Archive_extracted_manifest.csv",
                "/workspace/Archive_extracted_manifest.json",
            ],
            "artifact_path": "/workspace/Archive_extracted_manifest.xlsx",
            "artifact_paths": ["/workspace/Archive_extracted_manifest.xlsx"],
            "artifact_descriptors": [
                {"path": "/workspace/Archive_extracted_manifest.xlsx", "role": "deliverable", "kind": "archive_manifest"},
                {"path": "/workspace/Archive_extracted_manifest.csv", "role": "intermediate", "kind": "archive_manifest_sidecar"},
                {"path": "/workspace/Archive_extracted_manifest.json", "role": "intermediate", "kind": "archive_manifest_sidecar"},
            ],
        },
    )

    payload = json.loads(_tool_result_message_payload(result))

    assert payload["output"]["entry_count"] == 1200
    assert payload["output"]["manifest_row_count"] == 1200
    assert payload["output"]["manifest_xlsx_path"] == "/workspace/Archive_extracted_manifest.xlsx"
    assert "manifest_json_path" not in payload["output"]
    assert "manifest_csv_path" not in payload["output"]
    assert "manifest_artifact_paths" not in payload["output"]
    assert payload["output"]["deliverable_ready"] is True
    assert len(payload["output"]["entries_sample"]) == 20
    assert payload["output"]["entries_sample_truncated"] == {
        "shown": 20,
        "head": 12,
        "tail": 8,
        "total": 1200,
    }
    payload_text = json.dumps(payload)
    assert "item-0.txt" in payload_text
    assert "/workspace/extracted/item-0.txt" not in payload_text
    assert "item-1199.txt" in payload_text
    assert "item-600.txt" not in payload_text


def test_terminal_model_context_truncation_preserves_start_and_end(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MODEL_TOOL_RESULT_MAX_CHARS", "10000")
    stdout = "STDOUT-START " + ("x" * 25_000) + " STDOUT-END"
    stderr = "STDERR-START " + ("y" * 25_000) + " STDERR-END"
    result = ToolResult(
        invocation_id="terminal-1",
        tool_name="terminal_exec",
        status="completed",
        output={"stdout": stdout, "stderr": stderr, "exit_code": 0},
    )

    payload_text = _tool_result_message_payload(result)
    payload = json.loads(payload_text)

    assert len(payload_text) < 10000
    assert "STDOUT-START" in payload["output"]["stdout"]
    assert "STDOUT-END" in payload["output"]["stdout"]
    assert "STDERR-START" in payload["output"]["stderr"]
    assert "STDERR-END" in payload["output"]["stderr"]
    assert "chars omitted" in payload["output"]["stdout"]
    assert "chars omitted" in payload["output"]["stderr"]


def test_weather_model_context_keeps_current_daily_and_next_24_hours_only() -> None:
    hourly = [
        {"time": f"2026-07-{13 + (index // 24):02d}T{index % 24:02d}:00", "temperature_f": 70 + index}
        for index in range(48)
    ]
    result = ToolResult(
        invocation_id="weather-1",
        tool_name="weather_forecast",
        status="completed",
        output={
            "provider": "open-meteo",
            "location": {"name": "New York"},
            "current": {"time": "2026-07-13T14:00", "temperature_2m": 84},
            "daily": [{"date": "2026-07-13"}, {"date": "2026-07-14"}],
            "hourly": hourly,
            "source_url": "https://weather.example.test/live",
        },
    )

    payload_text = _tool_result_message_payload(result)
    payload = json.loads(payload_text)

    assert payload["output"]["current"]["temperature_2m"] == 84
    assert payload["output"]["daily"] == [{"date": "2026-07-13"}, {"date": "2026-07-14"}]
    assert len(payload["output"]["hourly_next_24"]) == 24
    assert payload["output"]["hourly_next_24"][0]["time"] == "2026-07-13T14:00"
    assert payload["output"]["hourly_compaction"]["total"] == 48
    assert "\"hourly\"" not in payload_text


def test_web_search_model_context_keeps_three_compact_source_records() -> None:
    results = [
        {
            "title": f"Result {index}",
            "url": f"https://example.test/{index}",
            "snippet": (f"Evidence {index}. " * 200),
            "irrelevant_payload": "x" * 10_000,
        }
        for index in range(12)
    ]
    payload_text = _tool_result_message_payload(
        ToolResult(
            invocation_id="search-compact",
            tool_name="web_search",
            status="completed",
            output={"query": "structured search query", "result_count": 12, "results": results},
        )
    )
    payload = json.loads(payload_text)

    assert payload["output"]["query"] == "structured search query"
    assert len(payload["output"]["results"]) == 2
    assert payload["output"]["results_compaction"] == {"shown": 2, "total": 12}
    assert len(payload["output"]["results"][0]["snippet"]) <= 350
    assert "irrelevant_payload" not in payload_text
    assert len(payload_text) < 3_000


def test_completed_tool_use_history_compacts_large_artifact_inputs_and_counts_them() -> None:
    large_content = "HTML-START " + ("detail " * 20_000) + " HTML-END"
    content = [
        {
            "type": "tool_use",
            "id": "write-1",
            "name": "file_write",
            "input": {"path": "/workspace/report.html", "content": large_content},
        }
    ]

    compacted = _conversation_visible_content(content)
    compacted_input = compacted[0]["input"]
    compacted_json = json.dumps(compacted_input)

    assert compacted[0]["id"] == "write-1"
    assert compacted_input["path"] == "/workspace/report.html"
    assert "HTML-START" in compacted_json
    assert "HTML-END" in compacted_json
    assert compacted_input["history_compaction"]["original_json_chars"] > 100_000
    assert len(compacted_json) < 12_000
    assert _message_payload_shape([{"role": "assistant", "content": content}])["text_chars"] > 100_000
    assert _message_payload_shape([{"role": "assistant", "content": compacted}])["text_chars"] < 12_000


def test_ready_multi_artifact_turn_focuses_on_remaining_producers_after_source_records() -> None:
    registry = ToolRegistry()
    for name in (
        "file_search",
        "file_read",
        "weather_forecast",
        "market_quote",
        "spreadsheet_create",
        "pdf_create",
        "file_write",
    ):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    scope_result = ToolResult(
        "scope-1",
        "request_tool_scope",
        "completed",
        {
            "capabilities": ["local_files", "weather", "market_data"],
            "required_tool_names": ["file_search", "file_read", "weather_forecast", "market_quote"],
            "artifact_extensions": [".xlsx", ".pdf", ".html"],
        },
    )
    incomplete_source_results = [
        scope_result,
        ToolResult("browser-1", "file_search", "completed", {}),
        ToolResult("weather-1", "weather_forecast", "completed", {}),
        ToolResult("market-1", "market_quote", "completed", {}),
    ]
    state = {
        "tool_flow_context": {
            "requires_artifact_delivery": True,
            "required_artifact_extensions": [".xlsx", ".pdf", ".html"],
        },
        # Pre-existing workspace files are not current-turn producer receipts
        # and therefore must not satisfy requested output extensions.
        "artifacts": [
            "/workspace/executive_personal_briefing.xlsx",
            "/workspace/executive_personal_briefing.pdf",
        ],
    }

    not_ready = _focus_tools_for_ready_artifact_production(
        state,
        tool_registry=registry,
        tool_results=incomplete_source_results,
    )
    ready = _focus_tools_for_ready_artifact_production(
        state,
        tool_registry=registry,
        tool_results=[
            *incomplete_source_results,
            ToolResult("extract-1", "file_read", "completed", {"text": "verified records"}),
        ],
    )

    assert {definition["name"] for definition in not_ready.list_tool_definitions()} == {
        "file_search",
        "file_read",
        "weather_forecast",
        "market_quote",
        "spreadsheet_create",
        "pdf_create",
        "file_write",
    }
    assert {definition["name"] for definition in ready.list_tool_definitions()} == {
        "spreadsheet_create",
    }

    deferred_state = {
        **state,
        "tool_flow_context": {
            **state["tool_flow_context"],
            "defer_artifact_focus": True,
        },
    }
    deferred = _focus_tools_for_ready_artifact_production(
        deferred_state,
        tool_registry=registry,
        tool_results=[
            *incomplete_source_results,
            ToolResult("extract-1", "file_read", "completed", {"text": "verified records"}),
        ],
    )
    assert {definition["name"] for definition in deferred.list_tool_definitions()} == {
        "file_search",
        "file_read",
        "weather_forecast",
        "market_quote",
        "spreadsheet_create",
        "pdf_create",
        "file_write",
    }

    budgeted_state = {
        **state,
        "tool_flow_context": {
            **state["tool_flow_context"],
            "artifact_focus_min_source_results": 5,
        },
    }
    four_source_results = [
        *incomplete_source_results,
        ToolResult("extract-1", "file_read", "completed", {"text": "verified records"}),
    ]
    before_budget = _focus_tools_for_ready_artifact_production(
        budgeted_state,
        tool_registry=registry,
        tool_results=four_source_results,
    )
    failed_source_does_not_fill_budget = _focus_tools_for_ready_artifact_production(
        budgeted_state,
        tool_registry=registry,
        tool_results=[
            *four_source_results,
            ToolResult("fetch-1", "web_fetch", "failed", {}, error="source unavailable"),
        ],
    )
    one_structured_call_fills_budget = _focus_tools_for_ready_artifact_production(
        budgeted_state,
        tool_registry=registry,
        tool_results=[
            *incomplete_source_results,
            ToolResult(
                "extract-records",
                "file_read",
                "completed",
                {
                    "item_count": 5,
                    "items": [{"title": f"Verified result {index}"} for index in range(5)],
                },
            ),
        ],
    )
    assert len(before_budget.list_tool_definitions()) == 7
    assert len(failed_source_does_not_fill_budget.list_tool_definitions()) == 7
    assert {definition["name"] for definition in one_structured_call_fills_budget.list_tool_definitions()} == {
        "spreadsheet_create",
    }

    after_html = _focus_tools_for_ready_artifact_production(
        state,
        tool_registry=registry,
        tool_results=[
            *incomplete_source_results,
            ToolResult("extract-1", "file_read", "completed", {"text": "verified records"}),
            ToolResult(
                "html-1",
                "file_write",
                "completed",
                {"artifact_paths": ["/workspace/executive_personal_briefing.html"]},
            ),
        ],
    )
    assert {definition["name"] for definition in after_html.list_tool_definitions()} == {
        "spreadsheet_create",
    }

    after_workbook = _focus_tools_for_ready_artifact_production(
        state,
        tool_registry=registry,
        tool_results=[
            *incomplete_source_results,
            ToolResult("extract-1", "file_read", "completed", {"text": "verified records"}),
            ToolResult(
                "xlsx-1",
                "spreadsheet_create",
                "completed",
                {"artifact_paths": ["/workspace/executive_personal_briefing.xlsx"]},
            ),
        ],
    )
    assert {definition["name"] for definition in after_workbook.list_tool_definitions()} == {
        "pdf_create",
    }

    after_workbook_and_pdf = _focus_tools_for_ready_artifact_production(
        state,
        tool_registry=registry,
        tool_results=[
            *incomplete_source_results,
            ToolResult("extract-1", "file_read", "completed", {"text": "verified records"}),
            ToolResult(
                "xlsx-1",
                "spreadsheet_create",
                "completed",
                {"artifact_paths": ["/workspace/executive_personal_briefing.xlsx"]},
            ),
            ToolResult(
                "pdf-1",
                "pdf_create",
                "completed",
                {"artifact_paths": ["/workspace/executive_personal_briefing.pdf"]},
            ),
        ],
    )
    assert {definition["name"] for definition in after_workbook_and_pdf.list_tool_definitions()} == {
        "file_write",
    }


def test_focused_artifact_model_context_is_compacted_to_verified_evidence_packet() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec("file_write", "write", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    messages = [
        {"role": "system", "content": [{"type": "text", "text": "Use registered tools."}]},
        {"role": "user", "content": [{"type": "text", "text": "Create the required HTML report."}]},
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "terminal-call",
                    "name": "terminal_exec",
                    "input": {"command": "collect evidence", "large": "input" * 5000},
                }
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "terminal-call",
                    "content": [{"type": "text", "text": "raw terminal output " * 5000}],
                }
            ],
        },
    ]
    tool_results = [
        ToolResult(
            "terminal-call",
            "terminal_exec",
            "completed",
            {
                "exit_code": 0,
                "stdout": (
                    "REPOSITORY\n"
                    + json.dumps({"stars": 16, "metadata": "x" * 8000})
                    + "\nTRAFFIC_VIEWS\n"
                    + json.dumps({"count": 34, "uniques": 19})
                    + "\nTRAFFIC_CLONES\n"
                    + json.dumps({"count": 148, "uniques": 27})
                    + "\nTRAFFIC_REFERRERS\n"
                    + json.dumps([{"referrer": "reddit.com", "count": 14, "uniques": 12}])
                ),
            },
        ),
        *[
            ToolResult(
                f"browser-{index}",
                "browser_extract_text",
                "completed",
                {
                    "url": f"https://example.test/source/{index}",
                    "text": "Reddit mention with a verified source link. " + ("source " * 500),
                },
            )
            for index in range(20)
        ],
    ]
    state = {
        "tool_flow_context": {
            "requires_artifact_delivery": True,
            "required_artifact_extensions": [".html"],
            "required_artifact_content_tokens": ["GitHub Traffic Trend", "Reddit"],
        }
    }

    compacted = _compact_focused_artifact_source_history(
        messages,
        state=state,
        tool_results=tool_results,
        model_tool_registry=registry,
    )
    serialized = json.dumps(compacted)
    packet = compacted[-1]["content"][0]["text"]

    assert "terminal-call" not in serialized
    assert "raw terminal output" not in serialized
    assert "Create the required HTML report" in serialized
    assert "Focused artifact production packet" in serialized
    assert "GitHub Traffic Trend" in serialized
    assert "terminal_exec" in serialized
    assert "browser_extract_text" in serialized
    assert "TRAFFIC_VIEWS" in serialized
    assert "TRAFFIC_CLONES" in serialized
    assert "TRAFFIC_REFERRERS" in serialized
    assert '\\"count\\": 34' in packet
    assert '\\"count\\": 148' in packet
    assert "reddit.com" in serialized
    assert len(serialized) < 30_000


def test_artifact_is_rewritten_after_new_source_evidence(tmp_path) -> None:  # noqa: ANN001
    report_path = tmp_path / "current-report.html"
    evidence_path = tmp_path / "views.json"
    report_path.write_text("<html>old report</html>", encoding="utf-8")
    evidence_path.write_text('{"count": 34, "uniques": 19}', encoding="utf-8")
    registry = ToolRegistry()
    registry.register(
        ToolSpec("file_write", "write", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        ToolSpec("file_read", "read", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    tool_results = [
        ToolResult("source-1", "file_read", "completed", {"text": "initial evidence"}),
        ToolResult("write-1", "file_write", "completed", {"path": str(report_path)}),
        ToolResult(
            "read-1",
            "file_read",
            "completed",
            {"path": str(evidence_path), "content": evidence_path.read_text(encoding="utf-8")},
        ),
    ]
    state = {
        "tool_registry": registry,
        "tool_results": tool_results,
        "tool_flow_context": {
            "requires_artifact_delivery": True,
            "required_artifact_extensions": [".html"],
        },
    }

    assert _completed_required_artifact_paths_for_turn(state, [str(report_path)]) == ()

    finalize_update = _agent_turn_finalize_node(
        {
            **state,
            "content": [{"type": "text", "text": "Report complete."}],
            "messages": [],
            "artifacts": [str(report_path)],
            "runtime_store": None,
            "principal_id": "telegram_chat",
            "conversation_id": "telegram:123",
            "user_message": "opaque report request",
        }
    )
    assert finalize_update["artifact_refresh_nudged"] is True
    assert {tool["name"] for tool in finalize_update["tool_registry"].list_tool_definitions()} == {
        "file_write"
    }
    assert "New verified source evidence" in finalize_update["messages"][-1]["content"][0]["text"]

    second_evidence_path = tmp_path / "clones.json"
    second_evidence_path.write_text('{"count": 148, "uniques": 27}', encoding="utf-8")
    two_source_results = [
        *tool_results,
        ToolResult(
            "read-2",
            "file_read",
            "completed",
            {"path": str(second_evidence_path), "content": second_evidence_path.read_text(encoding="utf-8")},
        ),
    ]
    focused = _focus_tools_for_ready_artifact_production(
        {**state, "tool_results": two_source_results},
        tool_registry=registry,
        tool_results=two_source_results,
    )
    assert {tool["name"] for tool in focused.list_tool_definitions()} == {"file_write"}

    rewritten_results = [
        *tool_results,
        ToolResult("write-2", "file_write", "completed", {"path": str(report_path)}),
    ]
    rewritten_state = {**state, "tool_results": rewritten_results}
    assert _completed_required_artifact_paths_for_turn(
        rewritten_state,
        [str(report_path)],
    ) == (str(report_path),)


def test_artifact_producer_focus_is_ephemeral_across_model_iterations() -> None:
    registry = ToolRegistry()
    for name in (
        "file_search",
        "file_read",
        "weather_forecast",
        "market_quote",
        "spreadsheet_create",
        "pdf_create",
        "file_write",
    ):
        registry.register(
            ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )

    class RecordingModel:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []
            self.timeouts: list[float] = []

        def create(self, **kwargs):
            self.calls.append([tool["name"] for tool in kwargs["tools"]])
            self.timeouts.append(float(kwargs["timeout"]))
            return {"content": [], "stop_reason": "end_turn"}

    model = RecordingModel()
    scope_result = ToolResult(
        "scope-1",
        "request_tool_scope",
        "completed",
        {
            "capabilities": ["local_files", "weather", "market_data"],
            "required_tool_names": ["file_search", "weather_forecast", "market_quote"],
            "artifact_extensions": [".xlsx", ".pdf", ".html"],
        },
    )
    source_results = [
        scope_result,
        ToolResult("browser-1", "file_search", "completed", {}),
        ToolResult("extract-1", "file_read", "completed", {"text": "verified records"}),
        ToolResult("weather-1", "weather_forecast", "completed", {}),
        ToolResult("market-1", "market_quote", "completed", {}),
    ]
    state = {
        "iterations": 0,
        "max_iterations": 20,
        "messages": [],
        "tool_registry": registry,
        "tool_results": source_results,
        "tool_recovery_scopes_attempted": [],
        "orchestrator": SimpleNamespace(model_client=model),
        "conversation_id": "artifact-focus-test",
        "principal_id": "test-user",
        "user_message": "Create the requested files",
        "tool_flow_context": {
            "requires_artifact_delivery": True,
            "required_artifact_extensions": [".xlsx", ".pdf", ".html"],
        },
    }

    first = _agent_turn_model_node(state)
    assert model.calls[-1] == ["spreadsheet_create"]
    assert model.timeouts[-1] >= 90
    assert first["tool_registry"] is registry

    second = _agent_turn_model_node(
        {
            **state,
            **first,
            "tool_results": [
                *source_results,
                ToolResult(
                    "xlsx-1",
                    "spreadsheet_create",
                    "completed",
                    {"artifact_paths": ["/workspace/executive_personal_briefing.xlsx"]},
                ),
            ],
        }
    )
    assert model.calls[-1] == ["pdf_create"]
    assert model.timeouts[-1] >= 90
    assert second["tool_registry"] is registry


def test_next_artifact_call_compacts_completed_producer_payload_pair() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec("pdf_create", "pdf_create", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    messages = [
        {"role": "user", "content": [{"type": "text", "text": "Create the requested files"}]},
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "xlsx-call",
                    "name": "spreadsheet_create",
                    "input": {"sheets": [{"rows": [{"large": "payload" * 1000}]}]},
                }
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "xlsx-call",
                    "content": [{"type": "text", "text": "large workbook result" * 1000}],
                }
            ],
        },
        {
            "role": "user",
            "content": [{"type": "text", "text": "Verified weather and market source evidence"}],
        },
    ]
    results = [
        ToolResult(
            "xlsx-result",
            "spreadsheet_create",
            "completed",
            {"artifact_paths": ["/workspace/executive_personal_briefing.xlsx"]},
        )
    ]

    compacted = _compact_completed_artifact_producer_history(
        messages,
        state={
            "tool_registry": registry,
            "tool_flow_context": {
                "requires_artifact_delivery": True,
                "artifact_extensions": [".xlsx", ".pdf"],
            },
        },
        tool_results=results,
        model_tool_registry=registry,
    )
    serialized = json.dumps(compacted)

    assert "xlsx-call" not in serialized
    assert "large workbook result" not in serialized
    assert "Verified weather and market source evidence" in serialized
    assert "executive_personal_briefing.xlsx" in serialized
    assert "Do not regenerate" in serialized


def test_generic_large_tool_context_keeps_source_record_head_and_tail(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MODEL_TOOL_RESULT_MAX_CHARS", "10000")
    result = ToolResult(
        invocation_id="search-1",
        tool_name="file_search",
        status="completed",
        output={
            "message": "Found matching files.",
            "matches": [
                {
                    "path": f"/workspace/result-{index}.txt",
                    "preview": f"preview-{index} " + ("detail " * 80),
                }
                for index in range(40)
            ],
        },
    )

    payload = json.loads(_tool_result_message_payload(result))
    matches = payload["output"]["matches"]

    assert matches["total_items"] == 40
    assert matches["omitted_items"] == 32
    assert matches["items_head"][0]["path"] == "/workspace/result-0.txt"
    assert matches["items_tail"][-1]["path"] == "/workspace/result-39.txt"
    payload_text = json.dumps(payload)
    assert "/workspace/result-20.txt" not in payload_text


def test_planner_download_scope_adds_local_artifact_inspection_tools() -> None:
    expanded = _expand_planner_local_artifact_tool_scope(
        ["browser_navigate", "file_download"],
        available_tools=[
            "browser_navigate",
            "file_download",
            "file_read",
            "workspace_summary",
            "file_search",
            "archive_extract",
        ],
    )

    assert expanded == [
        "browser_navigate",
        "file_download",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
    ]


def test_planner_download_scope_filters_unregistered_companion_tools() -> None:
    expanded = _expand_planner_local_artifact_tool_scope(
        ["file_download"],
        available_tools=["file_download", "file_read"],
    )

    assert expanded == ["file_download", "file_read"]


def test_planner_web_source_artifact_workflow_adds_generic_data_fallback_tools() -> None:
    collect = TaskRecord(
        task_id="task-collect",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect source data",
        description="Collect structured data from the public source.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_navigate", "browser_extract_text"],
        dependencies=[],
        context_key_out="source-data",
    )
    create = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create the requested workbook.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-collect"],
        context_key_in="source-data",
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="structured artifact request",
        tasks=[collect, create],
    )

    expanded = _expand_planner_group_tool_scopes(
        group,
        available_tools=[
            "browser_navigate",
            "browser_extract_text",
            "browser_extract_items",
            "browser_run_js",
            "browser_image_collect",
            "web_fetch",
            "file_download",
            "file_read",
            "file_write",
            "workspace_summary",
            "file_search",
            "archive_extract",
            "spreadsheet_create",
        ],
        tool_profile_metadata=None,
    )

    collect_tools = expanded.tasks[0].allowed_tools
    assert collect_tools == [
        "browser_navigate",
        "browser_extract_text",
        "browser_extract_items",
        "browser_run_js",
        "web_fetch",
        "file_download",
        "browser_image_collect",
        "file_write",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
    ]
    assert expanded.tasks[1].allowed_tools == [
        "spreadsheet_create",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
        "browser_image_collect",
        "file_download",
    ]


def test_planner_web_source_artifact_workflow_detects_structured_producer_tool_without_metadata() -> None:
    collect = TaskRecord(
        task_id="task-collect",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect source data",
        description="Collect structured data from the public source.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_extract_text"],
        dependencies=[],
        context_key_out="source-data",
    )
    create = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create the requested workbook.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-collect"],
        context_key_in="source-data",
    )
    group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="structured artifact request",
        tasks=[collect, create],
    )

    expanded = _expand_planner_group_tool_scopes(
        group,
        available_tools=[
            "browser_extract_text",
            "browser_extract_items",
            "browser_run_js",
            "file_download",
            "browser_image_collect",
            "file_write",
            "file_read",
            "workspace_summary",
            "file_search",
            "archive_extract",
            "spreadsheet_create",
        ],
        tool_profile_metadata=None,
    )

    assert expanded.tasks[0].allowed_tools == [
        "browser_extract_text",
        "browser_extract_items",
        "browser_run_js",
        "file_download",
        "browser_image_collect",
        "file_write",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
    ]
    assert expanded.tasks[1].allowed_tools == [
        "spreadsheet_create",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
        "browser_image_collect",
        "file_download",
    ]


def test_decomposer_uses_visible_artifact_producer_over_handoff_artifact_kind() -> None:
    plan = DagPlan(
        disposition="sequential_mission",
        tasks=[
            DecomposedTask(
                title="Collect records",
                description="Collect structured records from a public source.",
                tool_scope=["browser_run_js"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="source_records",
                required_inputs=[],
                can_start=True,
            ),
            DecomposedTask(
                title="Persist source dataset",
                description="Persist structured source data for downstream use.",
                tool_scope=["file_write"],
                priority=TaskPriority.NORMAL,
                dep_indices=[0],
                context_key_in="source_records",
                context_key_out="source_dataset",
                required_inputs=[],
                can_start=True,
                metadata={"requires_artifact_delivery": True, "required_artifact_kind": "json"},
            ),
            DecomposedTask(
                title="Create workbook",
                description="Create the requested workbook from the source dataset.",
                tool_scope=["spreadsheet_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[1],
                context_key_in="source_dataset",
                context_key_out="workbook_artifact",
                required_inputs=[],
                can_start=True,
            ),
        ],
        routing_evidence=["model_structured_plan"],
    )

    group = TaskDecomposer(model_client=object()).decompose(
        "structured artifact request",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        available_tools=["browser_run_js", "file_write", "spreadsheet_create", "file_read", "file_search"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="json",
    )

    persist_metadata = group.tasks[1].metadata
    workbook_metadata = group.tasks[2].metadata
    verifier_tasks = [task for task in group.tasks if (task.metadata or {}).get("artifact_role") == "verify"]

    assert persist_metadata.get("requires_artifact_delivery") is not True
    assert persist_metadata.get("required_artifact_kind") is None
    assert workbook_metadata["requires_artifact_delivery"] is True
    assert workbook_metadata["required_artifact_kind"] == "xlsx"
    assert len(verifier_tasks) == 1
    assert verifier_tasks[0].dependencies == [group.tasks[2].task_id]


def test_planner_web_source_without_downstream_artifact_keeps_scope_tight() -> None:
    collect = TaskRecord(
        task_id="task-collect",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Read source",
        description="Read structured source data.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_navigate", "browser_extract_text"],
        dependencies=[],
    )
    group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="source lookup",
        tasks=[collect],
    )

    expanded = _expand_planner_group_tool_scopes(
        group,
        available_tools=[
            "browser_navigate",
            "browser_extract_text",
            "browser_run_js",
            "file_download",
            "file_read",
            "workspace_summary",
        ],
        tool_profile_metadata=None,
    )

    assert expanded.tasks[0].allowed_tools == ["browser_navigate", "browser_extract_text"]


def test_missing_artifact_nudge_targets_registered_structured_producer_after_source_collection() -> None:
    registry = SimpleNamespace(
        list_tool_definitions=lambda: [
            {"name": "browser_extract_items"},
            {"name": "file_read"},
            {"name": "spreadsheet_create"},
        ]
    )
    tool_results = [
        ToolResult("tool-1", "browser_extract_items", "completed", {"items": [{"name": "Item 1"}]}),
        ToolResult("tool-2", "file_read", "completed", {"text": "{\"items\":[]}"}),
    ]

    nudge = _artifact_tool_delivery_nudge_for_missing_requirements(
        missing_requirements=("attachment:.xlsx",),
        tool_registry=registry,
        tool_results=tool_results,
    )

    assert nudge is not None
    assert "spreadsheet_create" in nudge
    assert "browser_extract_items" in nudge
    assert "file_read" in nudge


def test_missing_artifact_nudge_does_not_repeat_after_candidate_producer_attempt() -> None:
    registry = SimpleNamespace(
        list_tool_definitions=lambda: [
            {"name": "browser_extract_items"},
            {"name": "spreadsheet_create"},
        ]
    )
    tool_results = [
        ToolResult("tool-1", "browser_extract_items", "completed", {"items": [{"name": "Item 1"}]}),
        ToolResult("tool-2", "spreadsheet_create", "failed", {}, error="invalid rows"),
    ]

    assert (
        _artifact_tool_delivery_nudge_for_missing_requirements(
            missing_requirements=("attachment:.xlsx",),
            tool_registry=registry,
            tool_results=tool_results,
        )
        is None
    )


def test_completed_required_artifacts_do_not_bypass_missing_required_tool(tmp_path) -> None:
    pdf_path = tmp_path / "report.pdf"
    xlsx_path = tmp_path / "workbook.xlsx"
    pdf_path.write_bytes(b"%PDF-1.4\n% test\n")
    xlsx_path.write_bytes(b"PK\x03\x04 test")
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            required_tool_names=("email_send",),
            requested_artifact_extensions=(".pdf", ".xlsx"),
        )
    )
    base_state = {
        "tool_registry": registry,
        "tool_results": [
            ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(pdf_path)}),
            ToolResult("sheet", "spreadsheet_create", "completed", {"artifact_path": str(xlsx_path)}),
        ],
    }

    assert _completed_required_artifact_paths_for_turn(base_state, [str(pdf_path), str(xlsx_path)]) == ()

    sent_state = {
        **base_state,
        "tool_results": [
            *base_state["tool_results"],
            ToolResult("email", "email_send", "completed", {"to": "recipient@example.com", "sent": True}),
        ],
    }

    assert _completed_required_artifact_paths_for_turn(sent_state, [str(pdf_path), str(xlsx_path)]) == (
        str(pdf_path),
        str(xlsx_path),
    )


def test_failed_email_send_keeps_turn_from_finishing_with_artifacts_only(tmp_path) -> None:
    pdf_path = tmp_path / "report.pdf"
    xlsx_path = tmp_path / "workbook.xlsx"
    pdf_path.write_bytes(b"%PDF-1.4\n% test\n")
    xlsx_path.write_bytes(b"PK\x03\x04 test")
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            requested_artifact_extensions=(".pdf", ".xlsx"),
        )
    )
    tool_results = [
        ToolResult("pdf", "pdf_create", "completed", {"artifact_path": str(pdf_path)}),
        ToolResult("sheet", "spreadsheet_create", "completed", {"artifact_path": str(xlsx_path)}),
        ToolResult(
            "email",
            "email_send",
            "failed",
            {"reason": "email_attachment_requires_embedded_media", "to": "recipient@example.com"},
        ),
    ]
    state = {
        "tool_registry": registry,
        "tool_results": tool_results,
    }

    assert _completed_required_artifact_paths_for_turn(state, [str(pdf_path), str(xlsx_path)]) == ()
    assert _chat_turn_still_needs_scoped_email_send("send this by email", tool_results)


def test_completed_archive_manifest_does_not_bypass_required_helper_tools(tmp_path) -> None:
    xlsx_path = tmp_path / "stage_archive_manifest_rebuilt.xlsx"
    xlsx_path.write_bytes(b"PK\x03\x04 archive manifest")
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            required_tool_names=("file_download", "archive_extract", "spreadsheet_create", "file_search", "file_read"),
            requested_artifact_extensions=(".xlsx",),
        )
    )
    state = {
        "tool_registry": registry,
        "tool_results": [
            ToolResult("download", "file_download", "completed", {"artifact_path": str(tmp_path / "Archive.zip")}),
            ToolResult("extract", "archive_extract", "completed", {"artifact_path": str(xlsx_path)}),
        ],
    }

    assert _completed_required_artifact_paths_for_turn(state, [str(xlsx_path)]) == ()


def test_completed_embedded_artifact_requires_source_acquisition_evidence(tmp_path) -> None:
    html_path = tmp_path / "train_accessible_outdoor_day_trips.html"
    source_image = tmp_path / "trail.png"
    html_path.write_text(
        "<html><body><img src='data:image/png;base64,AAAA'></body></html>",
        encoding="utf-8",
    )
    source_image.write_bytes(b"\x89PNG\r\n\x1a\n")
    state = {
        "tool_results": [
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".html"],
                    "embedded_media_artifact_extensions": [".html"],
                    "required_tool_names": ["file_download", "file_write"],
                },
            ),
            ToolResult(
                "html",
                "file_write",
                "completed",
                {
                    "path": str(html_path),
                    "artifact_path": str(html_path),
                    "artifact_paths": [str(html_path)],
                    "embedded_html_images": [{"path": str(source_image)}],
                },
            ),
        ],
    }

    assert _completed_required_artifact_paths_for_turn(state, [str(html_path)]) == ()

    state["tool_results"].insert(
        1,
        ToolResult(
            "download",
            "file_download",
            "completed",
            {"path": str(source_image), "artifact_path": str(source_image)},
        ),
    )
    assert _completed_required_artifact_paths_for_turn(state, [str(html_path)]) == (str(html_path),)


def test_completed_artifact_does_not_bypass_required_current_web_source(tmp_path) -> None:
    html_path = tmp_path / "briefing.html"
    html_path.write_text("<html><body>Current briefing</body></html>", encoding="utf-8")
    registry = SimpleNamespace(
        turn_tool_scope_decision=SimpleNamespace(
            required_tool_names=("browser_navigate", "file_write"),
            requested_artifact_extensions=(".html",),
        )
    )
    state = {
        "tool_registry": registry,
        "tool_results": [
            ToolResult(
                "html",
                "file_write",
                "completed",
                {"path": str(html_path), "artifact_path": str(html_path)},
            )
        ],
    }

    assert _completed_required_artifact_paths_for_turn(state, [str(html_path)]) == ()

    state["tool_results"].insert(
        0,
        ToolResult(
            "source",
            "browser_navigate",
            "completed",
            {"url": "https://example.org/current-source", "status": 200},
        ),
    )
    assert _completed_required_artifact_paths_for_turn(state, [str(html_path)]) == (str(html_path),)


def test_completed_artifact_candidates_include_file_write_tool_result_paths(tmp_path) -> None:
    from nullion.response_fulfillment_contract import artifact_paths_from_tool_results

    html_path = tmp_path / "train_accessible_outdoor_day_trips.html"
    source_image = tmp_path / "trail.png"
    html_path.write_text(
        "<html><body><img src='data:image/png;base64,AAAA'>Verified itinerary content.</body></html>",
        encoding="utf-8",
    )
    source_image.write_bytes(b"\x89PNG\r\n\x1a\n")
    state = {
        "tool_results": [
            ToolResult(
                "scope",
                "request_tool_scope",
                "completed",
                {
                    "artifact_extensions": [".html"],
                    "embedded_media_artifact_extensions": [".html"],
                    "required_tool_names": ["file_write"],
                },
            ),
            ToolResult(
                "html",
                "file_write",
                "completed",
                {
                    "path": str(html_path),
                    "artifact_path": str(html_path),
                    "artifact_paths": [str(html_path)],
                    "embedded_html_images": [{"path": str(source_image)}],
                },
            ),
        ],
        "artifacts": [],
    }
    candidates = list(dict.fromkeys([*state["artifacts"], *artifact_paths_from_tool_results(state["tool_results"])]))

    assert _completed_required_artifact_paths_for_turn(state, candidates) == (str(html_path),)


def test_completed_required_artifact_stops_remaining_queued_helper_writes(tmp_path) -> None:
    html_path = tmp_path / "train_accessible_outdoor_day_trips.html"
    svg_path = tmp_path / "train_day_trips_summary.svg"
    source_image = tmp_path / "trail.png"
    source_image.write_bytes(b"\x89PNG\r\n\x1a\n")
    calls: list[str] = []

    class Registry:
        turn_tool_scope_decision = SimpleNamespace(required_tool_names=("file_write",))

        def can_invoke_tool(self, name: str) -> bool:
            return name == "file_write"

        def list_tool_definitions(self) -> list[dict[str, object]]:
            return [{"name": "file_write"}]

        def invoke(self, invocation: ToolInvocation) -> ToolResult:
            calls.append(str(invocation.arguments.get("path") or ""))
            path = str(invocation.arguments["path"])
            if path.endswith(".html"):
                html_path.write_text(
                    "<html><body><img src='data:image/png;base64,AAAA'>Verified itinerary.</body></html>",
                    encoding="utf-8",
                )
                output = {
                    "path": path,
                    "artifact_path": path,
                    "artifact_paths": [path],
                    "embedded_html_images": [{"path": str(source_image)}],
                    "artifact_delivery_manifest": {"version": 1, "candidate_paths": [path], "selected_paths": [path]},
                }
            else:
                svg_path.write_text("<svg><text>helper</text></svg>", encoding="utf-8")
                output = {"path": path, "artifact_path": path, "artifact_paths": [path]}
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", output)

    update = _execute_agent_turn_tool_uses(
        {
            "principal_id": "web:helper",
            "conversation_id": "web:helper",
            "user_message": "structured artifact request",
            "tool_registry": Registry(),
            "cleanup_scope": "turn-test",
            "messages": [],
            "tool_results": [
                ToolResult(
                    "scope",
                    "request_tool_scope",
                    "completed",
                    {
                        "artifact_extensions": [".html"],
                        "embedded_media_artifact_extensions": [".html"],
                        "required_tool_names": ["file_write"],
                    },
                )
            ],
            "artifacts": [],
        },
        [
            {"type": "tool_use", "id": "toolu-html", "name": "file_write", "input": {"path": str(html_path)}},
            {"type": "tool_use", "id": "toolu-svg", "name": "file_write", "input": {"path": str(svg_path)}},
        ],
    )

    assert calls == [str(html_path)]
    assert update["artifacts"] == [str(html_path)]
    assert not svg_path.exists()


def test_structured_artifact_source_failures_recover_without_hiding_required_tools() -> None:
    placeholder_failure = ToolResult(
        "write",
        "file_write",
        "failed",
        {"reason": "placeholder_source_urls", "path": "/tmp/report.html"},
        error="placeholder source URLs",
    )
    data_image_download_failure = ToolResult(
        "download",
        "file_download",
        "failed",
        {"reason": "blocked_url", "url": "data:image/png;base64,iVBORw0KGgo="},
        error="blocked URL",
    )

    assert _scope_recovery_capabilities_for_failed_result(placeholder_failure) == (
        "web",
        "image_generation",
        "local_shell",
    )
    assert _should_block_failed_tool_during_recovery(placeholder_failure) is False
    assert _scope_recovery_capabilities_for_failed_result(data_image_download_failure) == (
        "image_generation",
        "web",
        "local_shell",
    )
    assert _should_block_failed_tool_during_recovery(data_image_download_failure) is False


def test_scoped_tool_guard_records_blocked_invocation_result() -> None:
    class Registry:
        turn_tool_scope_decision = SimpleNamespace(required_tool_names=("spreadsheet_create",))

        def can_invoke_tool(self, name: str) -> bool:
            return name == "request_tool_scope"

        def list_tool_definitions(self) -> list[dict[str, object]]:
            return [{"name": "request_tool_scope"}]

    update = _execute_agent_turn_tool_uses(
        {
            "principal_id": "telegram:123",
            "conversation_id": "telegram:123",
            "user_message": "structured spreadsheet artifact request",
            "tool_registry": Registry(),
            "cleanup_scope": "turn-test",
            "messages": [],
            "tool_results": [],
            "artifacts": [],
        },
        [
            {
                "type": "tool_use",
                "id": "toolu-spreadsheet",
                "name": "spreadsheet_create",
                "input": {"rows": []},
            }
        ],
    )

    result = update["tool_results"][0]
    assert result.tool_name == "spreadsheet_create"
    assert result.status == "failed"
    assert result.output["reason"] == "tool_not_allowed_for_current_scope"
    assert update["messages"][0]["content"][0]["tool_use_id"] == "toolu-spreadsheet"


def test_required_artifact_filter_excludes_intermediate_zip(tmp_path) -> None:
    archive_path = tmp_path / "Archive.zip"
    workbook_path = tmp_path / "Archive_extracted_manifest.xlsx"
    archive_path.write_bytes(b"PK\x03\x04zip")
    workbook_path.write_bytes(b"PK\x03\x04xlsx")
    state = {
        "tool_registry": SimpleNamespace(
            turn_tool_scope_decision=TurnToolScopeDecision(
                requested_artifact_extensions=(".xlsx",),
                valid=True,
            )
        )
    }

    assert _completed_required_artifact_paths_for_turn(
        state,
        [str(archive_path), str(workbook_path)],
    ) == (str(workbook_path),)


def test_scheduler_run_scope_retry_exposes_run_cron_after_listing_failure() -> None:
    registry = ToolRegistry()
    for name in ("request_tool_scope", "list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    initial = SimpleNamespace(
        final_text="I listed the scheduled tasks, but `run_cron` is not exposed in this turn.",
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"crons": [], "message": "Here are your crons"})],
        artifacts=[],
        suspended_for_approval=False,
    )
    calls: list[tuple[list[dict[str, object]], object]] = []

    def run_turn(history, scoped_registry):  # noqa: ANN001
        calls.append((history, scoped_registry))
        return SimpleNamespace(
            final_text="Manual scheduled task run started.",
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"message": "started"})],
            artifacts=[],
            suspended_for_approval=False,
        )

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(
            registry,
            evidence=TurnToolEvidence(),
            tool_scope_decision=TurnToolScopeDecision(
                scheduler_action="run",
                requested_tool_names=("list_crons", "run_cron"),
                required_tool_names=("run_cron",),
                confidence=1.0,
                valid=True,
            ),
        ),
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert retry_result.final_text == "Manual scheduled task run started."
    names = {definition["name"] for definition in retry_registry.list_tool_definitions()}
    assert {"list_crons", "run_cron"} <= names
    assert "run_cron was not available" in str(calls[0][0][-1]["content"][0]["text"])


def test_scheduler_run_scope_retry_promotes_unique_target_after_structured_run_decision(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[tuple[str, dict[str, object]]] = []
    for name in ("list_crons", "run_cron"):
        def _handler(invocation, *, _name=name):  # noqa: ANN001
            invoked.append((_name, dict(invocation.arguments or {})))
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"})

        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            _handler,
        )
    initial = SimpleNamespace(
        final_text="Here are the scheduled tasks.",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-read-only",
                            "name": "Stage live scheduler manual run abc123",
                            "enabled": True,
                            "has_task": True,
                        }
                    ],
                    "message": "Here are your crons",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )
    classifier_calls: list[str] = []

    def classify_scheduler_action(**kwargs):  # noqa: ANN003
        classifier_calls.append(str(kwargs.get("user_message") or ""))
        return TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", classify_scheduler_action)

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Find the enabled scheduled task named 'Stage live scheduler manual run abc123' and start one manual run now.",
        evidence=TurnToolEvidence(),
        run_turn=lambda history, scoped_registry: SimpleNamespace(
            final_text="unexpected",
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"message": "started"})],
            artifacts=[],
            suspended_for_approval=False,
        ),
    )

    assert retried is True
    assert classifier_calls == []
    assert invoked == [("run_cron", {"name": "Stage live scheduler manual run abc123"})]
    assert [result.tool_name for result in retry_result.tool_results] == ["list_crons", "run_cron"]
    names = {definition["name"] for definition in retry_registry.list_tool_definitions()}
    assert {"list_crons", "run_cron"} <= names


def test_scheduler_run_scope_retry_promotes_exact_listed_target_after_structured_run_decision(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[tuple[str, dict[str, object]]] = []
    for name in ("list_crons", "run_cron"):
        def _handler(invocation, *, _name=name):  # noqa: ANN001
            invoked.append((_name, dict(invocation.arguments or {})))
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "message": (
                        "Manual scheduled task run started: Stage live scheduler manual run abc123. "
                        "The result will be delivered to this chat when ready."
                    )
                    if invocation.tool_name == "run_cron"
                    else "ok",
                    "name": "Stage live scheduler manual run abc123",
                },
            )

        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            _handler,
        )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Daily digest\n2. Stage live scheduler manual run abc123",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {"id": "cron-a", "name": "Daily digest", "enabled": True, "has_task": True},
                        {
                            "id": "cron-b",
                            "name": "Stage live scheduler manual run abc123",
                            "display_name": "Stage live scheduler manual run abc123",
                            "enabled": True,
                            "has_task": True,
                        },
                    ],
                    "message": "Here are your crons",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            confidence=1.0,
            valid=True,
        ),
    )
    classifier_calls: list[str] = []

    def classify_scheduler_action(**kwargs):  # noqa: ANN003
        classifier_calls.append(str(kwargs.get("user_message") or ""))
        return TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", classify_scheduler_action)
    def run_turn(history, retry_registry):  # noqa: ANN001
        raise AssertionError("exact structured target should be run directly, not rediscovered by the model")

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Find the enabled scheduled task named 'Stage live scheduler manual run abc123' and start one manual run now.",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        principal_id="web:stage",
    )

    assert retried is True
    assert classifier_calls == []
    assert "Manual scheduled task run started: Stage live scheduler manual run abc123" in retry_result.final_text
    assert invoked == [("run_cron", {"name": "Stage live scheduler manual run abc123"})]
    assert [result.tool_name for result in retry_result.tool_results] == ["list_crons", "run_cron"]
    assert {"list_crons", "run_cron"} <= {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_scheduler_run_scope_retry_runs_delegated_enabled_cron_without_listing_loop(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[tuple[str, dict[str, object]]] = []
    for name in ("list_crons", "run_cron"):
        def _handler(invocation, *, _name=name):  # noqa: ANN001
            invoked.append((_name, dict(invocation.arguments or {})))
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "message": (
                        "Manual scheduled task run started: Delegated cron. "
                        "The result will be delivered to this chat when ready."
                    )
                    if invocation.tool_name == "run_cron"
                    else "ok",
                    "id": "cron-delegated",
                },
            )

        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            _handler,
        )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Delegated cron",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {"id": "cron-disabled", "name": "Disabled cron", "enabled": False, "has_task": True},
                        {"id": "cron-delegated", "name": "Delegated cron", "enabled": True, "has_task": True},
                    ]
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="delegate_one",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("delegate-one run should not need reclassification")),
    )

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        model_client=None,
        user_message="Use the current scheduler action.",
        evidence=TurnToolEvidence(),
        run_turn=lambda history, retry_registry: (_ for _ in ()).throw(
            AssertionError("delegate-one target should run directly")
        ),
        principal_id="web:stage",
    )

    assert retried is True
    assert "Manual scheduled task run started: Delegated cron" in retry_result.final_text
    assert invoked == [("run_cron", {"id": "cron-delegated"})]
    assert [result.tool_name for result in retry_result.tool_results] == ["list_crons", "run_cron"]
    assert {"list_crons", "run_cron"} <= {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_scheduler_run_scope_retry_uses_structured_recovery_after_read_only_listing(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Daily digest - enabled",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [{"id": "cron-1", "name": "Daily digest", "enabled": True}],
                    "message": "Here are your crons",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            confidence=1.0,
            valid=True,
        ),
    )
    decisions: list[str] = []

    def fake_build_decision(**kwargs):  # noqa: ANN003
        decisions.append(kwargs["user_message"])
        assert kwargs["force_model_decision"] is True
        assert "Scheduled-task inventory" in kwargs["assistant_no_tool_draft"]
        return TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("run_cron",),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_build_decision)
    visible_tool_calls: list[set[str]] = []

    def run_turn(_history, retry_registry):  # noqa: ANN001
        visible_tool_calls.append({definition["name"] for definition in retry_registry.list_tool_definitions()})
        return SimpleNamespace(
            final_text="Manual scheduled task run started.",
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"message": "started"})],
            artifacts=[],
            suspended_for_approval=False,
        )

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="neutral scheduler request",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
    )

    assert retried is True
    assert decisions == []
    assert retry_result.final_text == "Manual scheduled task run started."
    assert {"list_crons", "run_cron"} <= visible_tool_calls[0]
    assert {"list_crons", "run_cron"} <= {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_scheduler_mutation_retry_directly_toggles_exact_listed_cron(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )

    def toggle(invocation: ToolInvocation) -> ToolResult:
        invoked.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "id": invocation.arguments.get("id"),
                "name": "Stage live disable fixture abc123",
                "enabled": invocation.arguments.get("enabled"),
                "action_receipt": {
                    "summary": "Cron 'Stage live disable fixture abc123' (cron-123) is now disabled.",
                    "details": ["Status: disabled.", "Workspace: workspace_admin."],
                },
            },
        )

    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        toggle,
    )
    registry.register(
        ToolSpec(
            name="file_search",
            description="file_search",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("local_files",),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "noop"}),
    )

    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:\n1. Stage live disable fixture abc123 - enabled",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-123",
                            "name": "Stage live disable fixture abc123",
                            "display_name": "Stage live disable fixture abc123",
                            "enabled": True,
                            "has_task": True,
                        }
                    ],
                    "message": "Here are your crons",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=False,
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        ),
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("structured mutation target must not need model reclassification")),
    )

    def run_turn(_history, _retry_registry):  # noqa: ANN001
        raise AssertionError("exact structured scheduler mutation should be completed directly")

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        model_client=None,
        user_message="Disable the scheduled task named 'Stage live disable fixture abc123'. Do not delete it.",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        principal_id="web:stage",
    )

    assert retried is True
    assert retry_result is initial
    assert invoked == [{"id": "cron-123", "enabled": False}]
    assert [result.tool_name for result in retry_result.tool_results] == ["list_crons", "toggle_cron"]
    assert retry_registry.can_invoke_tool("toggle_cron") is True


def test_scheduler_mutation_retry_scopes_after_read_only_list_then_toggles(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )

    def toggle(invocation: ToolInvocation) -> ToolResult:
        invoked.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "id": invocation.arguments.get("id"),
                "enabled": invocation.arguments.get("enabled"),
                "action_receipt": {"summary": "Cron is now disabled."},
            },
        )

    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        toggle,
    )

    initial = SimpleNamespace(
        final_text="Here are your scheduled tasks.",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-stage-fixture",
                            "name": "Stage live disable fixture 123abc",
                            "display_name": "Stage live disable fixture 123abc",
                            "enabled": True,
                            "has_task": True,
                        }
                    ],
                    "message": "Here are your crons",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )

    def build_decision(**kwargs):  # noqa: ANN001
        assert kwargs["force_model_decision"] is True
        return TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=False,
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", build_decision)

    def run_turn(_history, _retry_registry):  # noqa: ANN001
        raise AssertionError("structured scheduler mutation should complete directly after read-only listing")

    registry.turn_tool_scope_decision = build_decision(force_model_decision=True)

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Disable the scheduled task named 'Stage live disable fixture 123abc'. Do not delete it.",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        principal_id="web:stage",
    )

    assert retried is True
    assert retry_result is initial
    assert invoked == [{"id": "cron-stage-fixture", "enabled": False}]
    assert [result.tool_name for result in retry_result.tool_results] == ["list_crons", "toggle_cron"]
    assert retry_registry.can_invoke_tool("toggle_cron") is True


def test_missing_tool_scope_one_shot_scheduler_mutation_scope_directly_toggles(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    toggle_calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="request_tool_scope",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: (
            toggle_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"action_receipt": {"summary": "Enabled 1 cron job."}, "updated_count": 1},
            )
        ),
    )
    initial = SimpleNamespace(
        final_text="I can’t do a fresh live cron check because the scheduler tool is not exposed.",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(confidence=0.0, valid=False),
    )

    def run_turn(_history, retry_registry):  # noqa: ANN001
        assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"request_tool_scope"}
        return SimpleNamespace(
            final_text="Tool scope updated.",
            tool_results=[
                ToolResult(
                    "scope-1",
                    "request_tool_scope",
                    "completed",
                    {
                        "scope_requested": True,
                        "available_tools": ["list_crons", "toggle_cron"],
                        "required_tool_names": ["toggle_cron"],
                        "capabilities": ["scheduler_mutate"],
                        "scheduler_action": "mutate",
                        "scheduler_target_scope": "all_current_workspace",
                        "scheduler_toggle_enabled": True,
                    },
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
        )

    retry_result, retry_registry, retried = chat_operator._run_chat_turn_missing_tool_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        model_client=object(),
        user_message="opaque scheduler mutation request",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        principal_id="workspace_admin",
    )

    assert retried is True
    assert toggle_calls == [{"all_current_workspace": True, "enabled": True}]
    assert [tool.tool_name for tool in retry_result.tool_results] == ["request_tool_scope", "toggle_cron"]
    assert retry_registry.can_invoke_tool("toggle_cron") is True


def test_common_retries_scheduler_unfulfilled_mutation_runs_before_missing_scope(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    toggle_calls: list[dict[str, object]] = []
    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: (
            toggle_calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"action_receipt": {"summary": "Enabled 1 cron job."}, "updated_count": 1},
            )
        ),
    )
    registry.register(
        ToolSpec(
            name="request_tool_scope",
            description="request_tool_scope",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("tool_scope",),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    initial = SimpleNamespace(
        final_text="I can’t do a fresh live cron check because the scheduler tool is not exposed.",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    classifier_prompts: list[str] = []

    def classify_scheduler_mutation(**kwargs):  # noqa: ANN003
        classifier_prompts.append(str(kwargs.get("user_message") or ""))
        return TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_target_scope="all_current_workspace",
            scheduler_toggle_enabled=True,
            requested_tool_names=("toggle_cron",),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        )

    registry.turn_tool_scope_decision = classify_scheduler_mutation(user_message="typed scheduler request")
    classifier_prompts.clear()
    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", classify_scheduler_mutation)

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="opaque scheduler mutation request",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("scheduler mutation recovery should run before missing-tool retry")
        ),
        principal_id="workspace_admin",
    )

    assert result.retried_scheduler_mutation_fallback is True
    assert result.retried_missing_tool_scope is False
    assert toggle_calls == [{"all_current_workspace": True, "enabled": True}]
    assert [tool.tool_name for tool in result.result.tool_results] == ["toggle_cron"]
    assert classifier_prompts == []


def test_scheduler_common_retries_stop_after_completed_inspect_result() -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:\n\n1. Morning - enabled\n2. Evening - enabled",
        tool_results=[ToolResult("list-1", "list_crons", "completed", {"crons": []})],
        artifacts=[],
        suspended_for_approval=False,
    )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler inventory",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("completed scheduler inspection must not enter recovery retries")
        ),
        include_local_system_fallback=True,
    )

    assert result.result is initial
    assert result.retried_model_tool_request is False
    assert result.retried_account_read is False
    assert result.retried_local_system_fallback is False


def test_common_retries_bypass_completed_request_scope_scheduler_inspect(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron", "toggle_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name != "list_crons" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:\n\n1. Morning - enabled",
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "scope_requested": True,
                    "available_tools": ["list_crons"],
                    "scheduler_action": "inspect",
                },
            ),
            ToolResult("list-1", "list_crons", "completed", {"crons": []}),
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("completed structured scheduler inspection must not reclassify")
        ),
    )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=registry,
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="opaque scheduler inspection request",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("completed scheduler inspection must not enter retry turn")
        ),
        include_local_system_fallback=True,
    )

    assert result.result is initial
    assert result.retried_scheduler_run_scope is False
    assert result.retried_scheduler_mutation_fallback is False
    assert result.retried_missing_tool_scope is False


def test_completed_inspect_scope_with_resolved_target_cannot_gain_run_authority(monkeypatch) -> None:
    from nullion import chat_operator

    calls: list[dict[str, object]] = []
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )
    registry.register(
        ToolSpec(
            name="run_cron",
            description="run_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: (
            calls.append(dict(invocation.arguments))
            or ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "delivery_status": "deferred",
                    "result_text": "Manual scheduled task run started. The result will be delivered to this chat when ready.",
                },
            )
        ),
    )
    fixture_name = "Stage live scheduler manual run 3145c5710e"
    initial = SimpleNamespace(
        final_text=f"Scheduled-task inventory:\n\n1. {fixture_name} - enabled",
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "scope_requested": True,
                    "available_tools": ["list_crons"],
                    "scheduler_action": "inspect",
                },
            ),
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-1",
                            "name": fixture_name,
                            "enabled": True,
                            "has_task": True,
                        }
                    ]
                },
            ),
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    classifier_calls: list[dict[str, object]] = []

    def fake_decision(**kwargs):  # noqa: ANN003
        classifier_calls.append(kwargs)
        return TurnToolScopeDecision(
            scheduler_action="run",
            scheduler_selection_policy="user_selected",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", fake_decision)
    active_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message=f"Find the enabled scheduled task named '{fixture_name}' and start one manual run now.",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("exact structured cron target should run directly")
        ),
        include_local_system_fallback=True,
    )

    assert result.retried_scheduler_run_scope is False
    assert [tool.tool_name for tool in result.result.tool_results] == ["request_tool_scope", "list_crons"]
    assert calls == []
    assert classifier_calls == []


def test_completed_inspect_scope_with_resolved_target_preserves_read_only_when_classifier_declines(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    fixture_name = "Stage live scheduler short followup 3145c5710e"
    initial = SimpleNamespace(
        final_text=f"Scheduled-task inventory:\n\n1. {fixture_name} - enabled",
        tool_results=[
            ToolResult(
                "scope-1",
                "request_tool_scope",
                "completed",
                {
                    "scope_requested": True,
                    "available_tools": ["list_crons"],
                    "scheduler_action": "inspect",
                },
            ),
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {
                            "id": "cron-1",
                            "name": fixture_name,
                            "enabled": True,
                            "has_task": True,
                        }
                    ]
                },
            ),
        ],
        artifacts=[],
        suspended_for_approval=False,
    )

    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )
    active_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=active_registry,
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message=f"Find the enabled scheduled task named '{fixture_name}' in my current cron list.",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("read-only resolved target must not enter retry turn")
        ),
        include_local_system_fallback=True,
    )

    assert result.result is initial
    assert result.retried_scheduler_run_scope is False
    assert result.retried_scheduler_mutation_fallback is False


def test_common_retries_recover_scheduler_inventory_failure_draft(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )
    initial = SimpleNamespace(
        final_text=(
            "I’m not able to show the scheduled-task inventory from the available context because "
            "the scheduler listing tool did not return any visible task data in this turn."
        ),
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            required_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons"}
        return SimpleNamespace(
            final_text="Scheduled-task inventory:\n\n1. Morning - enabled",
            tool_results=[ToolResult("list-1", "list_crons", "completed", {"crons": []})],
            artifacts=[],
            suspended_for_approval=False,
        )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
            tool_scope_decision=TurnToolScopeDecision(valid=True, scheduler_action="inspect")),
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler inventory",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_no_tool_scope_decision is True
    assert [tool.tool_name for tool in result.result.tool_results] == ["list_crons"]


def test_common_retries_recover_unverified_empty_scheduler_inventory(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )
    initial = SimpleNamespace(
        final_text="1. **No scheduled tasks found** — your scheduled-task inventory is currently empty.",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            required_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
            tool_scope_decision=TurnToolScopeDecision(valid=True, scheduler_action="inspect")),
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler inventory",
        evidence=TurnToolEvidence(),
        run_turn=lambda _history, retry_registry: SimpleNamespace(
            final_text="Scheduled-task inventory:\n\n1. Morning - enabled",
            tool_results=[ToolResult("list-1", "list_crons", "completed", {"crons": []})],
            artifacts=[],
            suspended_for_approval=False,
        ),
        include_local_system_fallback=True,
    )

    assert result.retried_no_tool_scope_decision is True
    assert [tool.tool_name for tool in result.result.tool_results] == ["list_crons"]


def test_scheduler_mutation_retry_toggles_all_current_workspace_from_typed_target() -> None:
    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )

    def toggle(invocation: ToolInvocation) -> ToolResult:
        invoked.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "all_current_workspace": True,
                "enabled": invocation.arguments.get("enabled"),
                "updated_count": 2,
                "message": "Enabled 2 cron jobs.",
            },
        )

    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        toggle,
    )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory.",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {"id": "cron-1", "name": "Morning", "enabled": False, "has_task": True},
                        {"id": "cron-2", "name": "Evening", "enabled": False, "has_task": True},
                    ],
                    "message": "Here are your crons",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            scheduler_target_scope="all_current_workspace",
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_mutation_fallback_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        model_client=None,
        user_message="structured scheduler workspace mutation",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("typed all-current-workspace scheduler mutation should complete directly")
        ),
        principal_id="web:stage",
    )

    assert retried is True
    assert retry_result is initial
    assert invoked == [{"all_current_workspace": True, "enabled": True}]
    assert [result.tool_name for result in retry_result.tool_results] == ["list_crons", "toggle_cron"]
    assert retry_registry.can_invoke_tool("toggle_cron") is True


def test_common_retries_execute_scheduler_all_workspace_after_scope_only_result(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )

    def toggle(invocation: ToolInvocation) -> ToolResult:
        invoked.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "all_current_workspace": True,
                "enabled": invocation.arguments.get("enabled"),
                "updated_count": 21,
                "message": "Enabled 21 cron jobs.",
            },
        )

    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        toggle,
    )

    initial = SimpleNamespace(
        final_text="I can’t do a fresh live cron check from the tools currently exposed in this turn.",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(valid=False),
    )

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"request_tool_scope"}
        return SimpleNamespace(
            final_text="Tool scope updated. Continue the same user request using the newly available tools.",
            tool_results=[
                ToolResult(
                    "scope-1",
                    "request_tool_scope",
                    "completed",
                    {
                        "scope_requested": True,
                        "capabilities": ["scheduler_mutate"],
                        "scheduler_action": "mutate",
                        "scheduler_toggle_enabled": True,
                        "scheduler_target_scope": "all_current_workspace",
                        "available_tools": ["list_crons", "toggle_cron"],
                        "required_tool_names": ["toggle_cron"],
                    },
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
        )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
            tool_scope_decision=TurnToolScopeDecision(valid=True, scheduler_action="mutate")),
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler workspace mutation",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_missing_tool_scope is True
    assert result.retried_scheduler_mutation_fallback is True
    assert invoked == [{"all_current_workspace": True, "enabled": True}]
    assert [tool.tool_name for tool in result.result.tool_results] == ["request_tool_scope", "toggle_cron"]


def test_common_retries_execute_scheduler_read_after_scope_only_result(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "crons": [
                    {"id": "cron-1", "name": "Morning", "enabled": True, "has_task": True},
                ],
                "message": "Here are your crons",
            },
        ),
    )

    initial = SimpleNamespace(
        final_text="I requested read-only scheduler access, but the actual scheduled-task listing tool was not exposed.",
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(valid=False),
    )

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        if 'list_crons' in {definition['name'] for definition in retry_registry.list_tool_definitions()}:
            assert 'already completed' in str(_retry_history)
            return SimpleNamespace(final_text='Morning is enabled.', tool_results=[], artifacts=[], suspended_for_approval=False)
        assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"request_tool_scope"}
        return SimpleNamespace(
            final_text="Tool scope updated. Continue the same user request using the newly available tools.",
            tool_results=[
                ToolResult(
                    "scope-1",
                    "request_tool_scope",
                    "completed",
                    {
                        "scope_requested": True,
                        "capabilities": ["scheduler_read"],
                        "scheduler_action": "inspect",
                        "available_tools": ["list_crons"],
                        "required_tool_names": [],
                    },
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
        )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
            tool_scope_decision=TurnToolScopeDecision(valid=True, scheduler_action="inspect")),
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler inventory",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert result.retried_missing_tool_scope is True
    assert [tool.tool_name for tool in result.result.tool_results] == ["request_tool_scope", "list_crons"]
    assert result.result.final_text == 'Morning is enabled.'


def test_common_retries_keep_model_scheduler_read_within_declared_effects(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )

    def toggle(invocation: ToolInvocation) -> ToolResult:
        invoked.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "all_current_workspace": True,
                "enabled": invocation.arguments.get("enabled"),
                "updated_count": 21,
                "message": "Enabled 21 cron jobs.",
            },
        )

    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        toggle,
    )
    initial = SimpleNamespace(
        final_text='<tool_request>{"tool":"list_crons"}</tool_request>',
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            scheduler_target_scope="all_current_workspace",
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    run_turn_calls = 0

    def run_turn(_retry_history, retry_registry):  # noqa: ANN001
        nonlocal run_turn_calls
        run_turn_calls += 1
        assert {definition["name"] for definition in retry_registry.list_tool_definitions()} == {"list_crons"}
        return SimpleNamespace(
            final_text="Scheduled-task inventory:\n\n1. Morning - disabled",
            tool_results=[
                ToolResult(
                    "list-1",
                    "list_crons",
                    "completed",
                    {
                        "crons": [{"id": "cron-1", "name": "Morning", "enabled": False, "has_task": True}],
                        "message": "Here are your crons",
                    },
                )
            ],
            artifacts=[],
            suspended_for_approval=False,
        )

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler workspace mutation",
        evidence=TurnToolEvidence(),
        run_turn=run_turn,
        include_local_system_fallback=True,
    )

    assert run_turn_calls == 1
    assert result.retried_model_tool_request is True
    assert result.retried_scheduler_mutation_fallback is False
    assert invoked == []
    assert [tool.tool_name for tool in result.result.tool_results] == ["list_crons"]


def test_common_retries_never_promote_unplanned_read_only_to_mutation(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )

    def toggle(invocation: ToolInvocation) -> ToolResult:
        invoked.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "all_current_workspace": True,
                "enabled": invocation.arguments.get("enabled"),
                "updated_count": 21,
                "message": "Enabled 21 cron jobs.",
            },
        )

    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        toggle,
    )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:\n\n1. Morning - disabled\n2. Evening - disabled",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {"id": "cron-1", "name": "Morning", "enabled": False, "has_task": True},
                        {"id": "cron-2", "name": "Evening", "enabled": False, "has_task": True},
                    ],
                    "message": "Here are your crons",
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    classifier_registry_tool_names: list[set[str]] = []

    def classify_recovery(**kwargs):
        classifier_registry_tool_names.append(
            {
                str(definition.get("name") or "")
                for definition in kwargs["registry"].list_tool_definitions()
            }
        )
        return TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_toggle_enabled=True,
            scheduler_target_scope="all_current_workspace",
            requested_tool_names=("list_crons", "toggle_cron"),
            required_tool_names=("toggle_cron",),
            confidence=1.0,
            valid=True,
        )

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", classify_recovery)

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler workspace mutation",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("typed scheduler mutation should complete directly after scheduler read")
        ),
        include_local_system_fallback=True,
    )

    assert result.retried_scheduler_mutation_fallback is False
    assert classifier_registry_tool_names == []
    assert invoked == []
    assert [tool.tool_name for tool in result.result.tool_results] == ["list_crons"]


def test_common_retries_recover_scheduler_setup_clarification_to_mutation(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    invoked: list[dict[str, object]] = []

    registry.register(
        ToolSpec(
            name="list_crons",
            description="list_crons",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
    )

    def toggle(invocation: ToolInvocation) -> ToolResult:
        invoked.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "all_current_workspace": True,
                "enabled": invocation.arguments.get("enabled"),
                "updated_count": 2,
                "message": "Enabled 2 cron jobs.",
            },
        )

    registry.register(
        ToolSpec(
            name="toggle_cron",
            description="toggle_cron",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("scheduler", "cron"),
            input_schema={"type": "object", "properties": {}},
        ),
        toggle,
    )
    initial = SimpleNamespace(
        final_text=(
            "I can set that up, but I need the missing schedule or task detail first.\n\n"
            "Reply with one option:\n"
            "1. The exact schedule and what I should check.\n"
            "2. The frequency, such as hourly, daily, weekdays, or weekly."
        ),
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )

    def build_decision(**kwargs):  # noqa: ANN001
        user_message = str(kwargs.get("user_message") or "")
        if user_message.startswith("Scheduler mutation recovery classifier."):
            return TurnToolScopeDecision(
                scheduler_action="mutate",
                scheduler_toggle_enabled=True,
                scheduler_target_scope="all_current_workspace",
                requested_tool_names=("list_crons", "toggle_cron"),
                required_tool_names=("toggle_cron",),
                confidence=1.0,
                valid=True,
            )
        return TurnToolScopeDecision(valid=False)

    monkeypatch.setattr(chat_operator, "build_turn_tool_scope_decision", build_decision)

    result = chat_operator._run_chat_turn_common_retries(
        initial_result=initial,
        base_history=[],
        active_tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(),
            tool_scope_decision=build_decision(user_message="Scheduler mutation recovery classifier.")),
        base_tool_registry=registry,
        runtime=None,
        model_client=object(),
        user_message="structured scheduler workspace mutation",
        evidence=TurnToolEvidence(),
        run_turn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("scheduler setup clarification should recover through typed fallback directly")
        ),
        include_local_system_fallback=True,
    )

    assert result.retried_scheduler_mutation_fallback is True
    assert invoked == [{"all_current_workspace": True, "enabled": True}]
    assert [tool.tool_name for tool in result.result.tool_results] == ["toggle_cron"]


def test_scheduler_read_scope_request_drops_prior_run_tools() -> None:
    registry = ToolRegistry()
    for name, continuations in (
        ("list_crons", ("run_cron", "delete_cron", "update_cron", "toggle_cron")),
        ("list_reminders", ()),
        ("run_cron", ()),
        ("delete_cron", ()),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE
                if name in {"run_cron", "delete_cron"}
                else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
                continuation_tools=continuations,
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    scope_result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-read",
            "request_tool_scope",
            "web:stage",
            {"capabilities": ["scheduler_read"], "tool_names": ["list_crons", "list_reminders"]},
        )
    )

    assert scope_result.status == "completed"
    assert scope_result.output["available_tools"] == ["list_crons", "list_reminders"]
    assert widened.can_invoke_tool("list_crons") is True
    assert widened.can_invoke_tool("run_cron") is False
    assert widened.can_invoke_tool("delete_cron") is False
    definitions = {definition["name"]: definition for definition in widened.list_tool_definitions()}
    assert set(definitions) == {"request_tool_scope", "list_crons", "list_reminders"}
    assert "continuation_tools" not in definitions["list_crons"]


def test_scheduler_run_retry_respects_active_read_only_scope(monkeypatch) -> None:
    from nullion import chat_operator

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    active = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )
    initial = SimpleNamespace(
        final_text="Scheduled-task inventory:",
        tool_results=[
            ToolResult(
                "list-1",
                "list_crons",
                "completed",
                {
                    "crons": [
                        {"id": "cron-1", "name": "Stage live fresh cron check", "enabled": True, "has_task": True}
                    ]
                },
            )
        ],
        artifacts=[],
        suspended_for_approval=False,
    )
    monkeypatch.setattr(
        chat_operator,
        "build_turn_tool_scope_decision",
        lambda **_kwargs: TurnToolScopeDecision(
            scheduler_action="run",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    def fail_run_turn(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("read-only scheduler follow-up must not be retried as run_cron")

    result, returned_registry, retried = chat_operator._run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=active,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Do a fresh check",
        evidence=TurnToolEvidence(),
        run_turn=fail_run_turn,
    )

    assert result is initial
    assert returned_registry is active
    assert retried is False

    fallback_result, fallback_registry, fallback_retried = chat_operator._run_chat_turn_scheduler_run_fallback_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=active,
        base_tool_registry=registry,
        model_client=object(),
        user_message="Do a fresh check",
        evidence=TurnToolEvidence(),
        run_turn=fail_run_turn,
    )

    assert fallback_result is initial
    assert fallback_registry is active
    assert fallback_retried is False


def test_scheduler_inspect_scope_guard_blocks_run_before_handler() -> None:
    scoped_registry = SimpleNamespace(
        turn_tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        )
    )
    result = _scheduler_scope_guard_result(
        ToolInvocation("run-1", "run_cron", "web:stage", {"name": "Example"}),
        scoped_registry,
    )

    assert result is not None
    assert result.status == "failed"
    assert result.output["reason"] == "scheduler_tool_not_allowed_for_scope"


def test_completed_scheduler_run_stops_before_followup_mutation_tools() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-run",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_run"],
                                "tool_names": ["list_crons", "run_cron"],
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "tool_use", "id": "list", "name": "list_crons", "input": {}}
                    ],
                }
            if self.calls == 3:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "run",
                            "name": "run_cron",
                            "input": {"id": "cron-1"},
                        }
                    ],
                }
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "delete",
                        "name": "delete_cron",
                        "input": {"id": "cron-1"},
                    }
                ],
            }

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron", "delete_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE
                if name in {"run_cron", "delete_cron"}
                else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"message": "Manual scheduled task run started."}
                if invocation.tool_name == "run_cron"
                else {"crons": [{"id": "cron-1", "name": "Example", "enabled": True}], "message": "1. Example"},
            ),
        )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:scheduler-run-terminal",
        principal_id="web:scheduler-run-terminal",
        user_message="start one scheduled task now",
        conversation_history=[],
        tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        policy_store=None,
        approval_store=None,
        max_iterations=8,
    )

    assert [tool.tool_name for tool in result.tool_results] == [
        "request_tool_scope",
        "list_crons",
        "run_cron",
    ]
    assert result.final_text == "Manual scheduled task run started."


def test_delegated_scheduler_run_starts_only_enabled_cron_after_listing() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-run",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_run"],
                                "tool_names": ["list_crons", "run_cron"],
                                "scheduler_selection_policy": "delegate_one",
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "tool_use", "id": "list", "name": "list_crons", "input": {}}
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": "Reply with the exact scheduled task number.",
                    }
                ],
            }

    run_arguments: list[dict[str, object]] = []

    def handler(invocation: ToolInvocation) -> ToolResult:
        if invocation.tool_name == "list_crons":
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "crons": [
                        {"id": "cron-disabled", "name": "Disabled", "enabled": False},
                        {"id": "cron-enabled", "name": "Enabled", "enabled": True},
                    ],
                    "message": "1. Disabled\n2. Enabled",
                },
            )
        run_arguments.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"message": "Manual scheduled task run started."},
        )

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            handler,
        )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:scheduler-delegate-one",
        principal_id="web:scheduler-delegate-one",
        user_message="run one eligible scheduled task",
        conversation_history=[],
        tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        policy_store=None,
        approval_store=None,
        max_iterations=8,
    )

    assert [tool.tool_name for tool in result.tool_results] == [
        "request_tool_scope",
        "list_crons",
        "run_cron",
    ]
    assert result.tool_results[0].output["scheduler_selection_policy"] == "delegate_one"
    assert result.tool_results[-1].output["selection_policy"] == "delegate_one"
    assert result.tool_results[-1].output["selected_from_tool"] == "list_crons"
    assert run_arguments == [{"id": "cron-enabled"}]
    assert result.final_text == "Manual scheduled task run started."


def test_user_selected_scheduler_run_uses_run_cron_resolver_after_listing() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-run",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_run"],
                                "tool_names": ["list_crons", "run_cron"],
                                "required_tool_names": ["run_cron"],
                                "scheduler_selection_policy": "user_selected",
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "tool_use", "id": "list", "name": "list_crons", "input": {}}
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "This should not need another model turn."}],
            }

    run_arguments: list[dict[str, object]] = []

    def handler(invocation: ToolInvocation) -> ToolResult:
        if invocation.tool_name == "list_crons":
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "crons": [
                        {"id": "cron-a", "name": "Daily digest", "enabled": True},
                        {"id": "cron-b", "name": "Stage live scheduler manual run abc123", "enabled": True},
                    ],
                    "message": "1. Daily digest\n2. Stage live scheduler manual run abc123",
                },
            )
        run_arguments.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"message": "Manual scheduled task run started."},
        )

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            handler,
        )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:scheduler-user-selected",
        principal_id="web:scheduler-user-selected",
        user_message="please run Stage live scheduler manual run abc123 now",
        conversation_history=[],
        tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        policy_store=None,
        approval_store=None,
        max_iterations=8,
    )

    assert [tool.tool_name for tool in result.tool_results] == [
        "request_tool_scope",
        "list_crons",
        "run_cron",
    ]
    assert result.tool_results[-1].output["selection_policy"] == "user_selected"
    assert result.tool_results[-1].output["selected_from_tool"] == "list_crons"
    assert run_arguments == [{"name": "please run Stage live scheduler manual run abc123 now"}]
    assert result.final_text == "Manual scheduled task run started."


def test_delegated_scheduler_run_does_not_guess_between_multiple_enabled_crons() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-run",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_run"],
                                "tool_names": ["list_crons", "run_cron"],
                                "required_tool_names": ["run_cron"],
                                "scheduler_selection_policy": "delegate_one",
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "tool_use", "id": "list", "name": "list_crons", "input": {}}
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "Which scheduled task should I run?"}],
            }

    run_arguments: list[dict[str, object]] = []

    def handler(invocation: ToolInvocation) -> ToolResult:
        if invocation.tool_name == "list_crons":
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {
                    "crons": [
                        {"id": "cron-a", "name": "First enabled", "enabled": True},
                        {"id": "cron-b", "name": "Second enabled", "enabled": True},
                    ],
                    "message": "1. First enabled\n2. Second enabled",
                },
            )
        run_arguments.append(dict(invocation.arguments))
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"message": "Manual scheduled task run started."},
        )

    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            handler,
        )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:scheduler-delegate-ambiguous",
        principal_id="web:scheduler-delegate-ambiguous",
        user_message="run one eligible scheduled task",
        conversation_history=[],
        tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        policy_store=None,
        approval_store=None,
        max_iterations=8,
    )

    assert [tool.tool_name for tool in result.tool_results] == [
        "request_tool_scope",
        "list_crons",
    ]
    assert run_arguments == []
    assert "specific scheduled task" in result.final_text.lower()
    assert "1." in result.final_text
    assert "2." in result.final_text


def test_scheduler_inspect_scope_hides_blocked_run_cron_from_tool_results() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-read",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_read"],
                                "tool_names": ["list_crons", "list_reminders"],
                            },
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "tool_use", "id": "list", "name": "list_crons", "input": {}}
                    ],
                }
            if self.calls == 3:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "run",
                            "name": "run_cron",
                            "input": {"name": "__list_only__"},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Here are your crons."}]}

    registry = ToolRegistry()
    for name in ("list_crons", "list_reminders", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"crons": [{"id": "cron-1", "name": "Example", "enabled": True}], "message": "1. Example"},
            ),
        )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:scheduler-inspect-no-run",
        principal_id="web:scheduler-inspect-no-run",
        user_message="show scheduled task inventory",
        conversation_history=[],
        tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        policy_store=None,
        approval_store=None,
        max_iterations=8,
    )

    assert [tool.tool_name for tool in result.tool_results] == ["request_tool_scope", "list_crons"]
    assert result.final_text == "Here are your crons."


def test_current_scope_hides_blocked_local_file_write_from_tool_results() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-local",
                            "name": "request_tool_scope",
                            "input": {"capabilities": ["local_files"], "tool_names": ["file_search"]},
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "search",
                            "name": "file_search",
                            "input": {"query": "report.pdf"},
                        }
                    ],
                }
            if self.calls == 3:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "write",
                            "name": "file_write",
                            "input": {"path": "/tmp/should-not-write.txt", "content": "no"},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "report.pdf"}]}

    registry = ToolRegistry()
    for name in ("file_search", "file_read", "workspace_summary", "file_write"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "file_write" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("local_files",),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"matches": ["/tmp/report.pdf"]} if invocation.tool_name == "file_search" else {"message": "unexpected"},
            ),
        )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:local-read-no-write",
        principal_id="web:local-read-no-write",
        user_message="find a local file and only list it",
        conversation_history=[],
        tool_registry=ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence()),
        policy_store=None,
        approval_store=None,
        max_iterations=8,
    )

    assert [tool.tool_name for tool in result.tool_results] == ["request_tool_scope", "file_search"]
    assert result.final_text == "report.pdf"


def test_file_search_tool_result_payload_marks_filename_matches_as_listing_evidence() -> None:
    result = ToolResult(
        "search",
        "file_search",
        "completed",
        {
            "matches": ["/tmp/report.pdf"],
            "match_details": [{"path": "/tmp/report.pdf", "match_type": "filename"}],
            "search_contents": False,
            "searched_file_count": 42,
            "searched_roots": ["/tmp"],
            "preview": "x" * 10_000,
        },
    )

    payload = json.loads(_tool_result_message_payload(result))

    assert payload["output"]["matches"] == ["/tmp/report.pdf"]
    assert payload["output"]["match_types"] == ["filename"]
    assert payload["output"]["result_kind"] == "filename_match_listing"
    assert "completion_guidance" in payload["output"]
    assert "preview" not in payload["output"]


def test_artifact_file_search_budget_is_gated_by_structured_delivery_contract(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_ARTIFACT_FILE_SEARCH_LIMIT", "3")
    invocation = ToolInvocation(
        invocation_id="search-4",
        tool_name="file_search",
        principal_id="web:qa",
        arguments={"query": "another workspace asset"},
    )
    completed_searches = [
        ToolResult(f"search-{index}", "file_search", "completed", {"matches": []})
        for index in range(1, 4)
    ]

    assert _artifact_file_search_budget_guard_result(
        invocation,
        state={"user_message": "locate a workspace file", "tool_results": completed_searches},
        tool_results=completed_searches,
    ) is None

    guarded = _artifact_file_search_budget_guard_result(
        invocation,
        state={
            "user_message": "build executive_personal_briefing.xlsx",
            "tool_flow_context": {
                "requires_artifact_delivery": True,
                "required_artifact_extensions": [".xlsx"],
            },
            "tool_results": completed_searches,
        },
        tool_results=completed_searches,
    )

    assert guarded is not None
    assert guarded.status == "failed"
    assert guarded.output["reason"] == "artifact_discovery_budget_exhausted"
    assert guarded.output["requested_output_filenames"] == ["executive_personal_briefing.xlsx"]
    assert guarded.output["next_action"] == "create_or_update_requested_artifacts"


def test_completed_artifact_resets_file_search_discovery_budget(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_ARTIFACT_FILE_SEARCH_LIMIT", "2")
    invocation = ToolInvocation(
        invocation_id="search-next",
        tool_name="file_search",
        principal_id="web:qa",
        arguments={"query": "source for second deliverable"},
    )
    results = [
        ToolResult("search-1", "file_search", "completed", {"matches": []}),
        ToolResult("search-2", "file_search", "completed", {"matches": []}),
        ToolResult(
            "sheet-1",
            "spreadsheet_create",
            "completed",
            {"path": "/workspace/briefing.xlsx"},
        ),
        ToolResult("search-3", "file_search", "completed", {"matches": []}),
    ]

    assert _artifact_file_search_budget_guard_result(
        invocation,
        state={
            "user_message": "build briefing.xlsx and briefing.pdf",
            "tool_flow_context": {
                "requires_artifact_delivery": True,
                "required_artifact_extensions": [".xlsx", ".pdf"],
            },
            "tool_results": results,
        },
        tool_results=results,
    ) is None


def test_artifact_turn_blocks_additional_file_search_and_moves_to_creation(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_ARTIFACT_FILE_SEARCH_LIMIT", "3")
    output_path = tmp_path / "executive_personal_briefing.html"
    search_queries: list[str] = []

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls >= 3:
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": f"Created the file.\nMEDIA:{output_path}"}]}
            visible_tools = {item["name"] for item in kwargs["tools"]}
            if self.calls == 1:
                assert "file_search" in visible_tools
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"search-{index}",
                            "name": "file_search",
                            "input": {"query": f"asset {index}"},
                        }
                        for index in range(1, 5)
                    ],
                }
            assert "file_search" not in visible_tools
            assert "file_write" in visible_tools
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "write-final",
                        "name": "file_write",
                        "input": {"path": str(output_path), "content": "<html>complete</html>"},
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="file_search",
            description="Search workspace files",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("local_files",),
            input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
        ),
        lambda invocation: (
            search_queries.append(str(invocation.arguments.get("query") or ""))
            or ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"matches": []})
        ),
    )

    def write_file(invocation: ToolInvocation) -> ToolResult:
        output_path.write_text(str(invocation.arguments.get("content") or ""), encoding="utf-8")
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"path": str(output_path)},
        )

    registry.register(
        ToolSpec(
            name="file_write",
            description="Write a workspace file",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("local_files", "artifact"),
            input_schema={"type": "object", "properties": {}},
        ),
        write_file,
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:artifact-search-budget",
        principal_id="web:artifact-search-budget",
        user_message="Create executive_personal_briefing.html",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=6,
        tool_flow_context={
            "requires_artifact_delivery": True,
            "required_artifact_extensions": [".html"],
        },
    )

    assert search_queries == ["asset 1", "asset 2", "asset 3"]
    assert any(
        tool.output.get("reason") == "artifact_discovery_budget_exhausted"
        for tool in result.tool_results
        if isinstance(tool.output, dict)
    )
    assert output_path.is_file()
    assert result.artifacts == [str(output_path)]


def test_browser_snapshot_tool_result_payload_keeps_actionable_elements() -> None:
    noisy_links = [
        {
            "element_id": f"n-{index}",
            "role": "link",
            "tag": "a",
            "text": f"Footer link {index}",
            "visible": True,
            "editable": False,
        }
        for index in range(1, 45)
    ]
    useful_controls = [
        {
            "element_id": "n-50",
            "role": "textbox",
            "tag": "input",
            "label": "Pick-Up Location",
            "placeholder": "City or Airport",
            "value": "Providence, RI",
            "visible": True,
            "editable": True,
        },
        {
            "element_id": "n-51",
            "role": "textbox",
            "tag": "input",
            "label": "Pick-Up Date",
            "placeholder": "MM/DD/YYYY",
            "visible": True,
            "editable": True,
        },
        {
            "element_id": "n-52",
            "role": "button",
            "tag": "button",
            "text": "Continue",
            "visible": True,
            "editable": False,
        },
    ]
    payload = {
        "session_id": "agent-browser-default",
        "snapshot": {
            "url": "https://example.test/reservation",
            "title": "Reservation",
            "active_element_id": "n-50",
            "element_count": len(noisy_links) + len(useful_controls),
            "elements": noisy_links + useful_controls,
        },
    }
    result = ToolResult("snap", "browser_snapshot", "completed", payload)

    message = json.loads(_tool_result_message_payload(result))
    output = message["output"]
    ids = {item["element_id"] for item in output["elements"]}

    assert "n-50" in ids
    assert "n-51" in ids
    assert "n-52" in ids
    assert output["session_id"] == "agent-browser-default"
    assert output["url"] == "https://example.test/reservation"
    assert "preview" not in output
    assert "original_chars" not in output


def test_scheduler_run_scope_retry_recovers_contextual_telegram_claim_without_same_turn_list() -> None:
    registry = ToolRegistry()
    for name in ("list_crons", "run_cron"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name == "run_cron" else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"message": "ok"}),
        )
    initial = SimpleNamespace(
        final_text=(
            "I can identify the scheduled task from your cron list, but I only have access to the scheduler "
            "listing tool, not the manual run tool. Target cron: Daily digest. `run_cron` is not exposed."
        ),
        tool_results=[],
        artifacts=[],
        suspended_for_approval=False,
    )

    scoped_registry = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="run",
            requested_tool_names=("run_cron",),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    retry_result, retry_registry, retried = _run_chat_turn_scheduler_run_scope_retry(
        initial_result=initial,
        base_history=[],
        active_tool_registry=scoped_registry,
        base_tool_registry=registry,
        evidence=TurnToolEvidence(),
        run_turn=lambda history, scoped_registry: SimpleNamespace(
            final_text="Manual scheduled task run started.",
            tool_results=[ToolResult("run-1", "run_cron", "completed", {"message": "started"})],
            artifacts=[],
            suspended_for_approval=False,
        ),
    )

    assert retried is True
    assert retry_result.final_text == "Manual scheduled task run started."
    assert {"list_crons", "run_cron"} <= {definition["name"] for definition in retry_registry.list_tool_definitions()}


def test_live_dispatch_group_ids_uses_actual_runner_tasks_only() -> None:
    orchestrator = AgentOrchestrator(model_client=object())

    class Task:
        def __init__(self, done: bool) -> None:
            self._done = done

        def done(self) -> bool:
            return self._done

    orchestrator._runner_tasks_by_group = {
        "live-group": {Task(False), Task(True)},
        "finished-group": {Task(True)},
    }

    assert orchestrator.live_dispatch_group_ids() == {"live-group"}


def test_run_cron_ambiguous_match_failure_prompts_numbered_selection() -> None:
    reply = _terminal_tool_failure_text(
        ToolResult(
            invocation_id="tool-1",
            tool_name="run_cron",
            status="failed",
            output={
                "name": "market cron",
                "matches": [
                    {"selection_index": 1, "name": "Daily Pre-Market Stock Research"},
                    {"selection_index": 2, "name": "Mid-Market Stock Alert"},
                ],
            },
            error="Multiple crons matched; ask the user to choose by number.",
        )
    )

    assert reply is not None
    assert "1. Daily Pre-Market Stock Research" in reply
    assert "2. Mid-Market Stock Alert" in reply
    assert "Reply with the number." in reply


def test_scheduler_mutation_scope_cannot_finish_with_read_only_listing() -> None:
    cron_list_message = '1. Existing task · Status: enabled · Run by name: run_cron name="Existing task"'

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-1",
                            "name": "request_tool_scope",
                            "input": {"capabilities": ["scheduler_mutate"]},
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "list-1",
                            "name": "list_crons",
                            "input": {},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": cron_list_message}]}

    registry = ToolRegistry()
    for name in (
        "list_crons",
        "list_reminders",
        "create_cron",
        "update_cron",
        "delete_cron",
        "delete_reminder",
        "toggle_cron",
        "set_reminder",
        "update_reminder",
        "run_cron",
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE if name not in {"list_crons", "list_reminders"} else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"message": cron_list_message, "crons": [{"name": "Existing task"}]}
                if invocation.tool_name == "list_crons"
                else {"message": "ok"},
            ),
        )
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence())

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:scheduler-mutate-contract",
        principal_id="telegram:scheduler-mutate-contract",
        user_message="structured scheduler change",
        conversation_history=[],
        tool_registry=scoped,
        policy_store=None,
        approval_store=None,
        max_iterations=6,
    )

    assert [tool.tool_name for tool in result.tool_results] == ["request_tool_scope", "list_crons"]
    assert "I can set that up" in result.final_text
    assert "Reply with one option" in result.final_text
    assert "1. The exact schedule and what I should check." in result.final_text
    assert cron_list_message not in result.final_text


def test_plain_calendar_read_with_events_does_not_finish_with_done() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "calendar-1",
                            "name": "calendar_list",
                            "input": {"start": "2026-06-02", "end": "2026-06-03", "max": 50},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": "You have one calendar event today: Payment due reminder.",
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="calendar_list",
            description="calendar_list",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "results": [
                    {
                        "summary": "Payment due reminder",
                        "start": {"dateTime": "2026-06-02T09:00:00-04:00"},
                        "end": {"dateTime": "2026-06-02T09:30:00-04:00"},
                    }
                ]
            },
        ),
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:calendar-plain-read",
        principal_id="telegram:calendar-plain-read",
        user_message="Check my calendar",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert client.calls == 2
    assert [tool.tool_name for tool in result.tool_results] == ["calendar_list"]
    assert result.final_text != "Done."
    assert "Payment due reminder" in result.final_text


def test_scheduled_market_quote_continues_to_artifact_delivery_step() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "quote-1",
                            "name": "market_quote",
                            "input": {"symbols": ["MSFT"]},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Pre-market report is attached.\n\n"
                            "MEDIA:artifacts/pre_market_report_2026-07-09.html"
                        ),
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="market_quote",
            description="market_quote",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("finance",),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "quotes": [
                    {
                        "symbol": "MSFT",
                        "name": "Microsoft Corporation",
                        "price": 383.34,
                        "currency": "USD",
                        "exchange": "NasdaqGS",
                    }
                ]
            },
        ),
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:scheduled-market-report",
        principal_id="telegram:scheduled-market-report",
        user_message="[Scheduled task: Market report] create the report",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
        tool_flow_context={"scheduled_task_run": True, "cron_id": "cron-market"},
    )

    assert client.calls == 2
    assert [tool.tool_name for tool in result.tool_results] == ["market_quote"]
    assert "Market prices from live quote data" not in result.final_text
    assert "MEDIA:artifacts/pre_market_report_2026-07-09.html" in result.final_text


def test_plain_market_quote_still_finishes_from_compact_tool_summary() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "quote-1",
                        "name": "market_quote",
                        "input": {"symbols": ["MSFT"]},
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="market_quote",
            description="market_quote",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("finance",),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "quotes": [
                    {
                        "symbol": "MSFT",
                        "name": "Microsoft Corporation",
                        "price": 383.34,
                        "currency": "USD",
                        "exchange": "NasdaqGS",
                    }
                ]
            },
        ),
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:plain-market-quote",
        principal_id="telegram:plain-market-quote",
        user_message="show MSFT price",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert client.calls == 1
    assert [tool.tool_name for tool in result.tool_results] == ["market_quote"]
    assert "Market prices from live quote data" in result.final_text
    assert "MSFT (Microsoft Corporation): $383.34" in result.final_text


def test_market_quote_continues_when_turn_requires_artifact_delivery(tmp_path) -> None:
    workbook = tmp_path / "executive_briefing.xlsx"

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls >= 3:
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": f"Created the workbook.\nMEDIA:{workbook}"}]}
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "quote-1",
                            "name": "market_quote",
                            "input": {"symbols": ["MSFT"]},
                        }
                    ],
                }
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "sheet-1",
                        "name": "spreadsheet_create",
                        "input": {"output_path": str(workbook)},
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="market_quote",
            description="market_quote",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("finance",),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"quotes": [{"symbol": "MSFT", "name": "Microsoft", "price": 383.34, "currency": "USD"}]},
        ),
    )

    def create_workbook(invocation):  # noqa: ANN001
        workbook.write_bytes(b"PK\x03\x04 workbook")
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"artifact_path": str(workbook), "artifact_paths": [str(workbook)]},
        )

    registry.register(
        ToolSpec(
            name="spreadsheet_create",
            description="spreadsheet_create",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("artifact",),
            input_schema={"type": "object", "properties": {}},
        ),
        create_workbook,
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:market-artifact-pack",
        principal_id="telegram:market-artifact-pack",
        user_message="Create the workbook",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
        tool_flow_context={
            "requires_artifact_delivery": True,
            "required_artifact_extensions": [".xlsx"],
        },
    )

    assert client.calls == 3
    assert [tool.tool_name for tool in result.tool_results] == ["market_quote", "spreadsheet_create"]
    assert result.artifacts == [str(workbook)]
    assert str(workbook) in result.final_text


def test_scheduled_task_duplicate_calendar_read_does_not_finish_with_raw_listing() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            if self.calls in {1, 2}:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": f"calendar-{self.calls}",
                            "name": "calendar_list",
                            "input": {"start": "2026-06-02", "end": "2026-06-03", "max": 50},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Payment check complete.\n\n"
                            "- Payment due item found: Online Payment Site / balance due.\n"
                            "- Next action: verify payment status before sending another reminder."
                        ),
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="calendar_list",
            description="calendar_list",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("calendar", "connector", "account_read"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {
                "results": [
                    {
                        "summary": "Payment due: Online Payment Site / balance due",
                        "start": {"date": "2026-06-02"},
                        "end": {"date": "2026-06-03"},
                    }
                ]
            },
        ),
    )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:scheduled-calendar-dedupe",
        principal_id="telegram:scheduled-calendar-dedupe",
        user_message="[Scheduled task: Payment due reminder check] Check calendar and email for payment reminders.",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        tool_flow_context={"scheduled_task_run": True},
        max_iterations=4,
    )

    assert client.calls == 3
    assert [tool.tool_name for tool in result.tool_results] == ["calendar_list"]
    assert "Payment check complete." in result.final_text
    assert "I checked your calendar. Found" not in result.final_text


def test_bare_scheduler_run_scope_gets_neutral_recovery_nudge() -> None:
    registry = ToolRegistry()
    for name in ("list_crons", "list_reminders", "run_cron", "set_reminder"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE
                if name in {"run_cron", "set_reminder"}
                else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"message": "ok"},
            ),
        )

    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence())
    scope_result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:scheduler",
            {"capabilities": ["scheduler_run"]},
        )
    )

    missing = _scheduler_action_contract_missing(
        tool_registry=widened,
        tool_results=[scope_result],
    )

    assert missing == "scheduler action"
    nudge = _missing_scope_action_nudge(missing)
    assert "scheduler_mutate" in nudge
    assert "set_reminder" in nudge
    assert "delete_reminder" in nudge
    assert "update_reminder" in nudge
    assert "exactly one numbered selection list" in nudge
    assert "specific scheduled task selection" not in nudge


def test_exact_scheduler_run_scope_keeps_specific_run_selection_contract() -> None:
    registry = ToolRegistry()
    for name in ("list_crons", "run_cron", "set_reminder"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE
                if name in {"run_cron", "set_reminder"}
                else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"message": "ok"},
            ),
        )

    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence())
    scope_result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:scheduler",
            {"capabilities": ["scheduler_run"], "tool_names": ["run_cron"]},
        )
    )

    assert _scheduler_action_contract_missing(
        tool_registry=widened,
        tool_results=[scope_result],
    ) == "scheduler run"


def test_bare_scheduler_run_scope_cannot_gain_reminder_mutation_effects() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0
            self.nudges: list[str] = []

        def create(self, **kwargs):  # noqa: ANN001
            self.calls += 1
            messages = kwargs.get("messages") or []
            if messages and messages[-1].get("role") == "user":
                content = messages[-1].get("content") or []
                if content and isinstance(content[0], dict):
                    self.nudges.append(str(content[0].get("text") or ""))
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-run",
                            "name": "request_tool_scope",
                            "input": {"capabilities": ["scheduler_run"]},
                        }
                    ],
                }
            if self.calls == 2:
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Working."}]}
            if not any("scheduler_mutate" in nudge for nudge in self.nudges):
                return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Missing structured recovery."}]}
            if self.calls == 3:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-mutate",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["scheduler_mutate"],
                                "tool_names": ["set_reminder"],
                            },
                        }
                    ],
                }
            if self.calls == 4:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "set-reminder",
                            "name": "set_reminder",
                            "input": {
                                "text": "structured reminder",
                                "due_at": "2026-05-28T14:00:00-04:00",
                            },
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Reminder saved."}]}

    registry = ToolRegistry()
    for name in ("list_crons", "list_reminders", "run_cron", "set_reminder"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE
                if name in {"run_cron", "set_reminder"}
                else ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=("scheduler", "cron"),
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"message": "saved"} if invocation.tool_name == "set_reminder" else {"message": "ok"},
            ),
        )
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence())
    client = Client()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:scheduler-recovery",
        principal_id="telegram:scheduler-recovery",
        user_message="structured scheduler request",
        conversation_history=[],
        tool_registry=scoped,
        policy_store=None,
        approval_store=None,
        max_iterations=12,
    )

    assert [tool.tool_name for tool in result.tool_results] == [
        "request_tool_scope",
        "request_tool_scope",
    ]
    assert result.final_text != "Reminder saved."


def test_mini_agent_runner_concurrency_limit_is_bounded(monkeypatch) -> None:
    from nullion.agent_orchestrator import _mini_agent_runner_concurrency_limit

    monkeypatch.delenv("NULLION_MINI_AGENT_RUNNER_CONCURRENCY", raising=False)
    assert _mini_agent_runner_concurrency_limit() == 3

    monkeypatch.setenv("NULLION_MINI_AGENT_RUNNER_CONCURRENCY", "0")
    assert _mini_agent_runner_concurrency_limit() == 1

    monkeypatch.setenv("NULLION_MINI_AGENT_RUNNER_CONCURRENCY", "50")
    assert _mini_agent_runner_concurrency_limit() == 8


def test_blocked_tool_registry_preserves_filesystem_boundary_roots(tmp_path) -> None:
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    blocked = _BlockedToolRegistry(registry, {"unstable_tool"})

    assert blocked.filesystem_allowed_roots() == (tmp_path.resolve(),)

    replacement = tmp_path / "replacement"
    replacement.mkdir()
    blocked.set_filesystem_allowed_roots([replacement])

    assert registry.filesystem_allowed_roots() == (replacement.resolve(),)


def test_repeated_connector_failure_can_widen_to_web_scope_after_public_url_block() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []
            self.turn_tool_scope_decision = None

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["browser_navigate", "browser_extract_text"],
                        "capabilities": ["web"],
                    },
                ),
                self,
            )

    registry = Registry()
    blocked_public_url = ToolResult(
        invocation_id="tool-1",
        tool_name="connector_request",
        status="failed",
        output={"provider_id": "connector"},
        error=(
            "Blocked URL for connector_request: https://api.example.com/resource "
            "is not under configured connector base URL(s): https://gateway.example/"
        ),
    )
    repeated_failure = ToolResult(
        invocation_id="tool-2",
        tool_name="connector_request",
        status="failed",
        output={"provider_id": "connector"},
        error="HTTP Error 400: Bad Request",
    )

    recovery = _maybe_widen_scope_after_repeated_tool_failure(
        {
            "principal_id": "telegram:123",
            "cleanup_scope": "turn-test",
        },
        result=repeated_failure,
        tool_registry=registry,
        tool_results=[blocked_public_url, repeated_failure],
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    widened_registry, scope_result, scope = recovery
    assert widened_registry is registry
    assert scope == "web"
    assert scope_result.status == "completed"
    assert registry.calls[0].tool_name == "request_tool_scope"
    assert registry.calls[0].arguments == {"capabilities": ["web"]}


def test_connector_capability_failures_recover_to_browser_scope_before_giving_up() -> None:
    class Client:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            self.tool_sets.append({tool["name"] for tool in (kwargs.get("tools") or [])})
            if self.calls == 1:
                assert "connector_request" in self.tool_sets[-1]
                assert "browser_navigate" not in self.tool_sets[-1]
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "connector-1",
                            "name": "connector_request",
                            "input": {"url": "https://api.example.test/first"},
                        }
                    ],
                }
            if self.calls == 2:
                assert "browser_navigate" not in self.tool_sets[-1]
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "connector-2",
                            "name": "connector_request",
                            "input": {"url": "https://gateway.example.test/second"},
                        }
                    ],
                }
            assert "browser_navigate" in self.tool_sets[-1]
            assert "connector_request" not in self.tool_sets[-1]
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "browser-1",
                        "name": "browser_navigate",
                        "input": {"url": "https://example.test"},
                    }
                ],
            } if self.calls == 3 else {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Recovered with browser."}]}

    registry = ToolRegistry()
    for name in ("connector_request", "browser_navigate", "browser_extract_text"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                input_schema={"type": "object", "properties": {}},
            ),
            (
                lambda invocation: ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "failed",
                    {"provider_id": "connector"},
                    error="HTTP Error 400: Bad Request",
                )
            )
            if name == "connector_request"
            else (lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {})),
        )
    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_source_user_requested=True,
            requested_tool_names=("connector_request",),
            valid=True,
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="web:connector-recovery",
        principal_id="web:connector-recovery",
        user_message="check a connected account",
        conversation_history=[],
        tool_registry=scoped,
        policy_store=None,
        approval_store=None,
        max_iterations=5,
    )

    assert result.final_text == "Recovered with browser."
    assert [tool.tool_name for tool in result.tool_results] == [
        "connector_request",
        "connector_request",
        "request_tool_scope",
        "browser_navigate",
    ]


def test_connector_failure_recovery_still_prompts_web_when_web_scope_already_allowed() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []
            self.turn_tool_scope_decision = TurnToolScopeDecision(web_action="live_research", valid=True)

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {"available_tools": ["browser_navigate"], "capabilities": ["web"]},
                ),
                self,
            )

    registry = Registry()
    result = ToolResult(
        invocation_id="tool-1",
        tool_name="connector_request",
        status="failed",
        output={"provider_id": "connector"},
        error=(
            "Blocked URL for connector_request: https://api.example.com/resource "
            "is not under configured connector base URL(s): https://gateway.example/"
        ),
    )

    recovery = _maybe_widen_scope_after_repeated_tool_failure(
        {"principal_id": "web:1", "cleanup_scope": "turn-test", "repeated_failure_limit": 2},
        result=result,
        tool_registry=registry,
        tool_results=[result],
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    assert registry.calls[0].arguments == {"capabilities": ["web"]}


def test_connector_failure_recovery_uses_local_shell_when_web_tools_are_unavailable() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []
            self.turn_tool_scope_decision = None

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            capability = invocation.arguments["capabilities"][0]
            tools = [] if capability == "web" else ["terminal_exec"]
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {"available_tools": tools, "capabilities": [capability]},
                ),
                self,
            )

    registry = Registry()
    first_failure = ToolResult(
        invocation_id="tool-1",
        tool_name="connector_request",
        status="failed",
        output={"provider_id": "connector"},
        error="HTTP Error 400: Bad Request",
    )
    second_failure = ToolResult(
        invocation_id="tool-2",
        tool_name="connector_request",
        status="failed",
        output={"provider_id": "connector"},
        error="HTTP Error 404: Not Found",
    )

    recovery = _maybe_widen_scope_after_repeated_tool_failure(
        {"principal_id": "web:1", "cleanup_scope": "turn-test", "repeated_failure_limit": 2},
        result=second_failure,
        tool_registry=registry,
        tool_results=[first_failure, second_failure],
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    _, scope_result, scope = recovery
    assert scope == "local_shell"
    assert scope_result.output["available_tools"] == ["terminal_exec"]
    assert [call.arguments for call in registry.calls] == [
        {"capabilities": ["web"]},
        {"capabilities": ["local_shell"]},
    ]


def test_connector_failure_recovery_uses_already_visible_terminal_without_scope_adapter() -> None:
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="terminal_exec",
            description="Run shell",
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
            requires_approval=True,
            timeout_seconds=5,
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    first_failure = ToolResult(
        invocation_id="tool-1",
        tool_name="connector_request",
        status="failed",
        output={"provider_id": "connector"},
        error="HTTP Error 400: Bad Request",
    )
    second_failure = ToolResult(
        invocation_id="tool-2",
        tool_name="connector_request",
        status="failed",
        output={"provider_id": "connector"},
        error="HTTP Error 400: Bad Request",
    )

    recovery = _maybe_widen_scope_after_repeated_tool_failure(
        {"principal_id": "web:1", "cleanup_scope": "turn-test", "repeated_failure_limit": 2},
        result=second_failure,
        tool_registry=registry,
        tool_results=[first_failure, second_failure],
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    _, scope_result, scope = recovery
    assert scope == "local_shell"
    assert scope_result.tool_name == "request_tool_scope"
    assert scope_result.output["available_tools"] == ["terminal_exec"]


def test_scope_denial_recovery_requests_matching_web_capability() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["browser_navigate", "browser_extract_text"],
                        "capabilities": ["web"],
                    },
                ),
                self,
            )

    registry = Registry()
    denied = ToolResult(
        invocation_id="tool-denied",
        tool_name="browser_navigate",
        status="denied",
        output={"reason": "tool_requires_structured_turn_scope"},
        error="Tool requires structured turn evidence: browser_navigate",
    )

    recovery = _maybe_widen_scope_after_scope_denial(
        {
            "principal_id": "telegram:123",
            "cleanup_scope": "turn-test",
        },
        result=denied,
        tool_registry=registry,
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    widened_registry, scope_result, scope = recovery
    assert widened_registry is registry
    assert scope == "web"
    assert scope_result.status == "completed"
    assert registry.calls[0].tool_name == "request_tool_scope"
    assert registry.calls[0].arguments == {"capabilities": ["web"]}


def test_missing_connector_app_scope_recovers_to_web_tools() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["browser_navigate", "browser_extract_text"],
                        "capabilities": ["web"],
                    },
                ),
                self,
            )

    registry = Registry()
    missing_scope = ToolResult(
        invocation_id="scope-missing",
        tool_name="request_tool_scope",
        status="completed",
        output={
            "available_tools": [],
            "capabilities": ["connector"],
            "missing_connector_app_scope": True,
        },
    )

    recovery = _maybe_widen_scope_after_missing_connector_app_scope(
        {
            "principal_id": "telegram:123",
            "cleanup_scope": "turn-test",
        },
        result=missing_scope,
        tool_registry=registry,
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    widened_registry, scope_result, scope = recovery
    assert widened_registry is registry
    assert scope == "web+local_shell"
    assert scope_result.status == "completed"
    assert scope_result.output["available_tools"] == ["browser_navigate", "browser_extract_text"]
    assert registry.calls[0].arguments == {"capabilities": ["web", "local_shell"]}


def test_required_source_tool_failure_requests_exact_source_tool() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["email_search"],
                        "capabilities": ["connector"],
                        "required_tool_names": ["email_search"],
                    },
                ),
                self,
            )

    registry = Registry()
    read_failure = ToolResult(
        invocation_id="read-1",
        tool_name="email_read",
        status="failed",
        output={"reason": "invalid_message_id", "required_source_tool": "email_search"},
        error="HTTP Error 400: Bad Request",
    )

    recovery = _maybe_widen_scope_after_required_source_tool_failure(
        {
            "principal_id": "telegram:123",
            "cleanup_scope": "turn-test",
        },
        result=read_failure,
        tool_registry=registry,
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    widened_registry, scope_result, scope = recovery
    assert widened_registry is registry
    assert scope == "required_source:email_read:email_search"
    assert scope_result.status == "completed"
    assert scope_result.output["available_tools"] == ["email_search"]


def test_required_source_tool_failure_infers_email_search_for_missing_read_id() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["email_search"],
                        "capabilities": ["connector"],
                        "required_tool_names": ["email_search"],
                    },
                ),
                self,
            )

    registry = Registry()
    read_failure = ToolResult(
        invocation_id="read-1",
        tool_name="email_read",
        status="failed",
        output={"reason": "invalid_tool_arguments", "error": "missing required argument"},
        error="Missing required argument: id",
    )

    recovery = _maybe_widen_scope_after_required_source_tool_failure(
        {
            "principal_id": "telegram:123",
            "cleanup_scope": "turn-test",
        },
        result=read_failure,
        tool_registry=registry,
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    _widened_registry, scope_result, scope = recovery
    assert scope == "required_source:email_read:email_search"
    assert scope_result.status == "completed"
    assert registry.calls[0].arguments["tool_names"] == ["email_search"]
    assert registry.calls[0].arguments == {
        "capabilities": ["connector"],
        "tool_names": ["email_search"],
        "source_user_requested": False,
    }


def test_agent_recovers_email_read_source_failure_to_email_search(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["email_search", "email_read"],
                "active_app_ids": ["google-mail"],
            }
        ],
    )

    class Client:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN001
            visible = {tool["name"] for tool in (kwargs.get("tools") or [])}
            self.tool_sets.append(visible)
            if len(self.tool_sets) == 1:
                assert "email_read" in visible
                assert "email_search" not in visible
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "read-1",
                            "name": "email_read",
                            "input": {"id": "stale-message-id"},
                        }
                    ],
                }
            assert "email_search" in visible
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "search-1",
                        "name": "email_search",
                        "input": {"query": "newer_than:1d", "limit": 5},
                    }
                ],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_read",
            description="email_read",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "failed",
            {"reason": "invalid_message_id", "required_source_tool": "email_search"},
            error="HTTP Error 400: Bad Request",
        ),
    )
    registry.register(
        ToolSpec(
            name="email_search",
            description="email_search",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"results": [{"id": "msg-1", "subject": "Urgent account note"}]},
        ),
    )
    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(context_linked=True, prior_tool_scopes=("connector",)),
        tool_scope_decision=TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_source_user_requested=True,
            requested_tool_names=("email_read",),
            required_tool_names=("email_read",),
            confidence=1.0,
            valid=True,
        ),
    )
    client = Client()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:email-read-recovery",
        principal_id="telegram:email-read-recovery",
        user_message="Anything urgent in my email overnight",
        conversation_history=[],
        tool_registry=scoped,
        policy_store=None,
        approval_store=None,
        max_iterations=3,
    )

    assert "email_read" in client.tool_sets[0]
    assert "email_search" not in client.tool_sets[0]
    assert "email_search" in client.tool_sets[1]
    assert [tool.tool_name for tool in result.tool_results] == ["email_read", "request_tool_scope", "email_search"]
    assert result.final_text is not None
    assert "same email read step failed" not in result.final_text.lower()


def test_agent_returns_structured_source_summary_after_invalid_read_when_source_records_exist() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def create(self, **kwargs):  # noqa: ANN001
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "search-1",
                            "name": "email_search",
                            "input": {"query": "source", "limit": 5},
                        }
                    ],
                }
            if len(self.calls) == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "read-1",
                            "name": "email_read",
                            "input": {"id": "dummy"},
                        }
                    ],
                }
            rendered_messages = json.dumps(kwargs.get("messages") or [], sort_keys=True)
            assert "Structured recovery" in rendered_messages
            assert "msg-1" in rendered_messages
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "I found a matching email result with payment evidence."}],
            }

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="email_search",
            description="email_search",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "completed",
            {"results": [{"id": "msg-1", "subject": "Payment sent"}]},
        ),
    )
    registry.register(
        ToolSpec(
            name="email_read",
            description="email_read",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
            capability_tags=("email", "connector", "account_read"),
            input_schema={"type": "object", "properties": {}},
        ),
        lambda invocation: ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "failed",
            {"reason": "invalid_message_id", "id": invocation.arguments.get("id"), "required_source_tool": "email_search"},
            error="HTTP Error 400: Bad Request",
        ),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:invalid-read-after-search",
        principal_id="telegram:invalid-read-after-search",
        user_message="Check the selected account source",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        max_iterations=5,
    )

    assert [tool.tool_name for tool in result.tool_results] == ["email_search", "email_read"]
    assert result.final_text.startswith("✉️ I found structured source evidence")
    assert "Payment sent" in result.final_text
    assert "message id" not in result.final_text.lower()
    assert "record id" not in result.final_text.lower()


def test_agent_rejects_malformed_connector_scope_without_fallback_tools(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-calendar"],
            }
        ],
    )

    class Client:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN001
            visible = {tool["name"] for tool in (kwargs.get("tools") or [])}
            self.tool_sets.append(visible)
            if len(self.tool_sets) == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "scope-1",
                            "name": "request_tool_scope",
                            "input": {
                                "capabilities": ["connector"],
                                "tool_names": ["email_search", "email_read"],
                            },
                        }
                    ],
            }
            assert "browser_navigate" not in visible
            assert "browser_extract_text" not in visible
            assert "email_search" not in visible
            assert "connector_request" not in visible
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "Please name the connected source you want me to use."}],
            }

    registry = ToolRegistry()
    for name, tags in (
        ("connector_request", ("connector",)),
        ("email_search", ("email", "connector", "account_read")),
        ("email_read", ("email", "connector", "account_read")),
        ("calendar_list", ("calendar", "connector", "account_read")),
        ("browser_navigate", ()),
        ("browser_extract_text", ()),
        ("terminal_exec", ()),
    ):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                capability_tags=tags,
                input_schema={"type": "object", "properties": {}},
            ),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence())
    client = Client()

    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:missing-connector-scope",
        principal_id="telegram:missing-connector-scope",
        user_message="structured connected account request",
        conversation_history=[],
        tool_registry=scoped,
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert client.tool_sets[0] == {"request_tool_scope", "terminal_exec"}
    assert client.tool_sets[1] == {"request_tool_scope", "terminal_exec"}
    assert "email_search" not in client.tool_sets[1]
    assert "connector_request" not in client.tool_sets[1]
    assert [tool.tool_name for tool in result.tool_results] == ["request_tool_scope"]
    assert result.tool_results[0].output["connector_source_evidence_required"] is True
    assert result.tool_results[0].output["source_evidence_verified"] is False
    assert result.tool_results[0].output["missing_connector_app_scope"] is False
    assert "choose which authorized source" in result.final_text
    assert "1. Connected account: Email records" in result.final_text
    assert "2. Provide another source or details instead" in result.final_text


def test_unknown_tool_recovery_prefers_local_files_scope_for_file_tools() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["file_read", "file_search", "file_write", "file_patch", "workspace_summary"],
                        "capabilities": ["local_files"],
                    },
                ),
                self,
            )

    registry = Registry()
    failed = ToolResult(
        invocation_id="tool-failed",
        tool_name="file_read",
        status="failed",
        output={"reason": "unknown_tool", "requested_tool_name": "file_read"},
        error="Unknown tool: file_read",
    )

    recovery = _maybe_widen_scope_after_scope_denial(
        {
            "principal_id": "web:0",
            "cleanup_scope": "turn-test",
        },
        result=failed,
        tool_registry=registry,
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    _, scope_result, scope = recovery
    assert scope == "local_files"
    assert scope_result.status == "completed"
    assert registry.calls[0].arguments == {"capabilities": ["local_files"]}


def test_unknown_tool_recovery_includes_workspace_file_tools_in_local_scope() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["file_patch", "workspace_summary"],
                        "capabilities": ["local_files"],
                    },
                ),
                self,
            )

    registry = Registry()
    failed = ToolResult(
        invocation_id="tool-failed",
        tool_name="workspace_summary",
        status="failed",
        output={"reason": "unknown_tool", "requested_tool_name": "workspace_summary"},
        error="Unknown tool: workspace_summary",
    )

    recovery = _maybe_widen_scope_after_scope_denial(
        {
            "principal_id": "web:0",
            "cleanup_scope": "turn-test",
        },
        result=failed,
        tool_registry=registry,
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    _, scope_result, scope = recovery
    assert scope == "local_files"
    assert scope_result.status == "completed"
    assert registry.calls[0].arguments == {"capabilities": ["local_files"]}


def test_unknown_browser_tool_recovery_blocks_missing_tool_and_continues_with_browser_alternates() -> None:
    class Client:
        def __init__(self) -> None:
            self.tool_sets: list[set[str]] = []

        def create(self, **kwargs):  # noqa: ANN001
            system = str(kwargs.get("system") or "")
            if system.startswith("You are a material-requirements planner"):
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement": "current page inspection",
                                            "requirement_kind": "other",
                                            "evidence_subject_id": "current-page",
                                            "eligible_evidence_tool_names": [
                                                "browser_extract_text"
                                            ],
                                        }
                                    ],
                                }
                            ),
                        }
                    ],
                }
            if system.startswith("You are a semantic completion reviewer"):
                payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
                row = payload["material_requirement_contract"][0]
                evidence_id = next(
                    item["invocation_id"]
                    for item in payload["tool_evidence"]
                    if item["tool_name"] == "browser_extract_text"
                )
                return {
                    "stop_reason": "end_turn",
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {
                                    "disposition": "complete",
                                    "unresolved_requirements": [],
                                    "retry_tool_names": [],
                                    "all_material_requirements_enumerated": True,
                                    "material_requirements": [
                                        {
                                            "requirement_id": row["requirement_id"],
                                            "requirement": row["requirement"],
                                            "requirement_kind": row["requirement_kind"],
                                            "satisfied": True,
                                            "draft_covered": True,
                                            "evidence_invocation_ids": [evidence_id],
                                        }
                                    ],
                                }
                            ),
                        }
                    ],
                }
            visible = {tool["name"] for tool in (kwargs.get("tools") or [])}
            self.tool_sets.append(visible)
            if len(self.tool_sets) == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "snapshot-1",
                            "name": "browser_snapshot",
                            "input": {},
                        }
                    ],
                }
            assert "browser_snapshot" not in visible
            assert "browser_extract_text" in visible
            return (
                {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "extract-1",
                            "name": "browser_extract_text",
                            "input": {},
                        }
                    ],
                }
                if len(self.tool_sets) == 2
                else {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Recovered with page text."}]}
            )

    class Registry:
        def __init__(self) -> None:
            self.turn_tool_scope_decision = None
            self.specs = {
                name: ToolSpec(
                    name=name,
                    description=name,
                    risk_level=ToolRiskLevel.LOW,
                    side_effect_class=ToolSideEffectClass.READ,
                    requires_approval=False,
                    timeout_seconds=5,
                    input_schema={"type": "object", "properties": {}},
                )
                for name in ("browser_snapshot", "browser_extract_text", "browser_navigate")
            }

        def list_specs(self):
            return list(self.specs.values())

        def list_tool_definitions(self):
            return [
                {
                    "name": name,
                    "description": name,
                    "input_schema": {"type": "object", "properties": {}},
                    "capability_tags": [],
                    "side_effect_class": "read",
                    "risk_level": "low",
                    "requires_approval": False,
                }
                for name in sorted(self.specs)
            ]

        def get_spec(self, name: str):
            return self.specs[name]

        def can_invoke_tool(self, name: str) -> bool:
            return name in self.specs

        def invoke(self, invocation):
            if invocation.tool_name == "browser_snapshot":
                raise KeyError("Unknown tool: browser_snapshot")
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "completed",
                {"text": "Visible page text", "session_id": "browser-neutral"},
            )

        def apply_scope_request(self, invocation):
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "capabilities": ["web"],
                        "available_tools": ["browser_snapshot", "browser_extract_text", "browser_navigate"],
                    },
                ),
                self,
            )

    client = Client()
    result = AgentOrchestrator(model_client=client).run_turn(
        conversation_id="telegram:browser-recovery",
        principal_id="telegram:browser-recovery",
        user_message="Inspect the current web page",
        conversation_history=[],
        tool_registry=Registry(),
        policy_store=None,
        approval_store=None,
        max_iterations=4,
    )

    assert result.final_text == "Recovered with page text."
    assert result.response_fulfilled is not False
    assert [tool.tool_name for tool in result.tool_results] == [
        "browser_snapshot",
        "request_tool_scope",
        "browser_extract_text",
    ]
    assert "browser_snapshot" not in result.tool_results[1].output["available_tools"]
    assert "browser_snapshot" not in client.tool_sets[1]


def test_repeated_browser_tool_failure_recovers_to_callable_same_family_alternates() -> None:
    class Registry:
        def __init__(self) -> None:
            self.calls = []

        def apply_scope_request(self, invocation):
            self.calls.append(invocation)
            return (
                ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "available_tools": ["browser_snapshot", "browser_extract_text", "browser_run_js"],
                        "capabilities": ["web"],
                    },
                ),
                self,
            )

    failed = ToolResult(
        invocation_id="snapshot-failed",
        tool_name="browser_snapshot",
        status="failed",
        output={"reason": "backend_observation_failed"},
        error="Snapshot backend could not inspect this page",
    )

    recovery = _maybe_widen_scope_after_repeated_tool_failure(
        {"principal_id": "telegram:browser", "cleanup_scope": "turn-browser", "repeated_failure_limit": 2},
        result=failed,
        tool_registry=Registry(),
        tool_results=[failed],
        tool_recovery_scopes_attempted=[],
    )

    assert recovery is not None
    _, scope_result, scope = recovery
    assert scope == "web:without:browser_snapshot"
    assert scope_result.output["available_tools"] == ["browser_extract_text", "browser_run_js"]


def test_incomplete_artifact_contract_failures_keep_the_producer_available_for_retry() -> None:
    for reason in (
        "artifact_required_content_missing",
        "html_embedded_raster_image_required",
        "incomplete_html_document",
    ):
        assert _recoverable_tool_contract_failure(
            ToolResult(
                invocation_id=f"failed-{reason}",
                tool_name="file_write",
                status="failed",
                output={"reason": reason},
                error="Artifact is not complete yet.",
            )
        ) is True


def test_cron_tool_failure_recovers_to_visible_same_family_tool_without_scope_adapter() -> None:
    from nullion.cron_execution_tools import CronExecutionToolRegistry

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            names = {tool["name"] for tool in kwargs.get("tools") or []}
            if self.calls == 1:
                assert {"web_search", "browser_navigate"}.issubset(names)
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "search-unavailable",
                            "name": "web_search",
                            "input": {"query": "first source"},
                        }
                    ],
                }
            if self.calls == 2:
                assert "web_search" not in names
                assert {"browser_navigate", "browser_extract_text"}.issubset(names)
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "browser-fallback",
                            "name": "browser_navigate",
                            "input": {"url": "https://example.test"},
                        }
                    ],
                }
            if self.calls == 3:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "browser-evidence",
                            "name": "browser_extract_text",
                            "input": {"session_id": "browser-fallback"},
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "Recovered with the available browser tool."}],
            }

    registry = ToolRegistry()
    for name in ("web_search", "browser_navigate", "browser_extract_text"):
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=5,
                input_schema={"type": "object", "properties": {}},
            ),
            (
                lambda invocation: ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "failed",
                    {
                        "reason": "web_search_failed",
                        "failure_scope": "tool",
                        "status_code": 402,
                    },
                    error="HTTP Error 402: Payment Required",
                )
            )
            if name == "web_search"
            else (
                lambda invocation: ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    (
                        {"url": "https://example.test", "session_id": "browser-fallback"}
                        if invocation.tool_name == "browser_navigate"
                        else {
                            "text": "Verified source record with current evidence.",
                            "length": 45,
                            "session_id": "browser-fallback",
                        }
                    ),
                )
            ),
        )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:cron-recovery",
        principal_id="telegram:cron-recovery",
        user_message="Research the current source and finish the scheduled report.",
        conversation_history=[],
        tool_registry=CronExecutionToolRegistry(registry),
        policy_store=None,
        approval_store=None,
        max_iterations=5,
        tool_flow_context={"scheduled_task_run": True},
    )

    assert "Live-search blocker" not in str(result.final_text)
    assert [tool.tool_name for tool in result.tool_results] == [
        "web_search",
        "request_tool_scope",
        "browser_navigate",
        "browser_extract_text",
    ]


def test_scheduled_artifact_task_can_recover_after_dependency_failure(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_PLANNER_DEPENDENCY_RECOVERY_ATTEMPTS", "1")
    orchestrator = AgentOrchestrator(model_client=object())
    task = SimpleNamespace(
        allowed_tools=["file_write"],
        retry_count=0,
        metadata={"scheduled_task_run": True},
    )

    assert orchestrator._can_recover_blocked_artifact_task(task, ["task-upstream"]) is True


def test_dependency_recovery_still_requires_artifact_scope(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_PLANNER_DEPENDENCY_RECOVERY_ATTEMPTS", "1")
    orchestrator = AgentOrchestrator(model_client=object())
    task = SimpleNamespace(
        allowed_tools=["browser_extract_text"],
        retry_count=0,
        metadata={"scheduled_task_run": True},
    )

    assert orchestrator._can_recover_blocked_artifact_task(task, ["task-upstream"]) is False


def test_dependency_recovery_does_not_run_verifier_or_receipt_without_artifact(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_PLANNER_DEPENDENCY_RECOVERY_ATTEMPTS", "1")
    orchestrator = AgentOrchestrator(model_client=object())

    verifier = SimpleNamespace(
        allowed_tools=["file_read", "file_search"],
        retry_count=0,
        metadata={
            "artifact_role": "verify",
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    receipt = SimpleNamespace(
        allowed_tools=[],
        retry_count=0,
        metadata={
            "artifact_role": "deliver_receipt",
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )

    assert orchestrator._can_recover_blocked_artifact_task(verifier, ["task-create"]) is False
    assert orchestrator._can_recover_blocked_artifact_task(receipt, ["task-verify"]) is False


def test_orchestrator_rejects_successful_artifact_task_with_wrong_required_kind() -> None:
    task = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create the requested workbook.",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create", "file_write"],
        dependencies=[],
        metadata={
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    result = TaskResult(
        task_id="task-create",
        status="success",
        output="Created a fallback table.",
        artifacts=["/tmp/records.csv"],
    )

    failure = _task_result_artifact_contract_failure(task, result)

    assert failure is not None
    assert "required .xlsx artifact" in failure
    assert "records.csv" in failure


def test_dependency_recovery_expands_downloaded_artifact_scope() -> None:
    from nullion.task_queue import TaskRegistry

    registry = TaskRegistry()
    upstream = TaskRecord(
        task_id="task-collect",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect data",
        description="Collect structured source data.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_download"],
        dependencies=[],
        result=TaskResult("task-collect", "failure", error="not enough verified source data"),
    )
    producer = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create artifact",
        description="Create the requested artifact.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-collect"],
        metadata={
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="structured artifact request",
        tasks=[upstream, producer],
        planner_metadata={"disposition": "sequential_mission"},
    )

    async def run_recovery() -> TaskRecord | None:
        await registry.add_group(group)
        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator._task_registry = registry
        return await orchestrator._recover_blocked_artifact_task(
            producer,
            group=group,
            failed_dependency_ids=["task-collect"],
            tasks_by_id={"task-collect": upstream, "task-create": producer},
            available_tools=[
                "spreadsheet_create",
                "file_download",
                "file_read",
                "workspace_summary",
                "file_search",
                "archive_extract",
            ],
        )

    recovered = asyncio.run(run_recovery())

    assert recovered is not None
    assert recovered.status is TaskStatus.QUEUED
    assert recovered.dependencies == []
    assert recovered.retry_count == 1
    assert recovered.allowed_tools == [
        "spreadsheet_create",
        "file_download",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
    ]


def test_recovered_artifact_producer_completes_stale_delivery_failures(tmp_path) -> None:
    from nullion.task_queue import TaskRegistry

    artifact = tmp_path / "recovered_workbook.xlsx"
    artifact.write_bytes(b"PK\x03\x04 workbook")
    earlier = datetime.now(UTC) - timedelta(minutes=2)
    later = datetime.now(UTC)
    registry = TaskRegistry()
    collect = TaskRecord(
        task_id="task-collect",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect source records",
        description="Collect source records.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_run_js"],
        dependencies=[],
        completed_at=earlier,
        result=TaskResult("task-collect", "failure", error="Source record extraction was partial."),
    )
    producer = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create workbook",
        description="Create the requested workbook.",
        status=TaskStatus.COMPLETE,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=[],
        completed_at=later,
        result=TaskResult(
            "task-create",
            "success",
            output="Created workbook.",
            artifacts=[str(artifact)],
        ),
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    verify = TaskRecord(
        task_id="task-verify",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Verify artifact",
        description="Verify the requested workbook.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read", "file_search"],
        dependencies=["task-create"],
        completed_at=earlier,
        result=TaskResult("task-verify", "failure", error="Upstream artifact was not ready."),
        metadata={
            "artifact_role": "verify",
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    receipt = TaskRecord(
        task_id="task-receipt",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Deliver receipt",
        description="Confirm delivery of the requested workbook.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=["task-verify"],
        completed_at=earlier,
        result=TaskResult("task-receipt", "failure", error="Verifier was not complete."),
        metadata={
            "artifact_role": "deliver_receipt",
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    unrelated_verify = TaskRecord(
        task_id="task-unrelated-verify",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Verify separate artifact",
        description="Verify a separate requested workbook.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read"],
        dependencies=["task-collect"],
        completed_at=earlier,
        result=TaskResult("task-unrelated-verify", "failure", error="Separate source record failed."),
        metadata={
            "artifact_role": "verify",
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="Create a workbook from source records.",
        tasks=[collect, producer, verify, receipt, unrelated_verify],
        planner_metadata={"disposition": "sequential_mission"},
    )

    async def reconcile() -> None:
        await registry.add_group(group)
        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator._task_registry = registry
        await orchestrator._complete_artifact_delivery_tasks_after_producer_success(
            producer_task=producer,
            group=group,
            policy_store=None,
        )

    asyncio.run(reconcile())

    updated_verify = registry.get_task("task-verify")
    updated_receipt = registry.get_task("task-receipt")
    assert updated_verify is not None
    assert updated_receipt is not None
    assert updated_verify.status is TaskStatus.COMPLETE
    assert updated_receipt.status is TaskStatus.COMPLETE
    assert updated_verify.result is not None
    assert updated_verify.result.artifacts == [str(artifact)]
    assert updated_receipt.result is not None
    assert updated_receipt.result.artifacts == [str(artifact)]
    assert registry.get_task("task-collect").status is TaskStatus.FAILED
    assert registry.get_task("task-unrelated-verify").status is TaskStatus.FAILED


def test_supervisor_status_does_not_flap_failed_source_during_artifact_path(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.01")
    from nullion.task_queue import TaskRegistry

    registry = TaskRegistry()
    source = TaskRecord(
        task_id="task-source",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect product data",
        description="Collect source records.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_run_js"],
        dependencies=[],
        result=TaskResult("task-source", "failure", error="Source extraction was partial."),
    )
    producer = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create Excel file",
        description="Create the requested workbook.",
        status=TaskStatus.COMPLETE,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=[],
        result=TaskResult("task-create", "success", output="Created workbook.", artifacts=["/tmp/workbook.xlsx"]),
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    verifier = TaskRecord(
        task_id="task-verify",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Verify artifact",
        description="Verify the workbook.",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read", "file_search"],
        dependencies=["task-create"],
        started_at=datetime.now(UTC),
        timeout_s=999,
        metadata={"artifact_role": "verify", "requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    receipt = TaskRecord(
        task_id="task-receipt",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Deliver receipt",
        description="Deliver the verified workbook receipt.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=["task-verify"],
        metadata={"artifact_role": "deliver_receipt", "requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="Create an Excel workbook from product records.",
        tasks=[source, producer, verifier, receipt],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["source", "create", "verify", "receipt"]},
    )
    emitted: list[str] = []

    async def run_supervisor() -> None:
        await registry.add_group(group)
        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator._task_registry = registry

        async def capture_status(_conversation_id: str, text: str, **_kwargs) -> bool:
            emitted.append(text)
            raise RuntimeError("stop after first status")

        orchestrator._emit_supervised_status = capture_status
        await orchestrator._supervise_dispatch_group("grp-planner", policy_store=None)

    asyncio.run(run_supervisor())

    assert emitted
    assert "☑ Collect product data" in emitted[-1]
    assert "☑ Create Excel file" in emitted[-1]
    assert "◐ Verify artifact" in emitted[-1]
    assert "✕ Collect product data" not in emitted[-1]


def test_supervisor_fails_artifact_verifier_after_failed_dependency_and_records_doctor_action(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.01")
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_queue import TaskRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = TaskRegistry()
    producer = TaskRecord(
        task_id="task-create",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create spreadsheet",
        description="Create the spreadsheet artifact.",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=[],
        result=TaskResult("task-create", "failure", error="artifact creation failed"),
        metadata={
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    verifier = TaskRecord(
        task_id="task-verify",
        group_id="grp-planner",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Verify artifact",
        description="Verify current-run artifact descriptors.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read", "file_search"],
        dependencies=["task-create"],
        metadata={
            "artifact_role": "verify",
            "requires_artifact_delivery": True,
            "required_artifact_kind": "xlsx",
        },
    )
    group = TaskGroup(
        group_id="grp-planner",
        conversation_id="telegram:123",
        original_message="Create a spreadsheet with images.",
        tasks=[producer, verifier],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["create", "verify"]},
    )

    async def run_supervisor() -> None:
        await registry.add_group(group)
        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator._task_registry = registry
        orchestrator._progress_queue = asyncio.Queue()
        await orchestrator._supervise_dispatch_group(
            "grp-planner",
            policy_store=runtime.store,
            runner=SimpleNamespace(),
            tool_registry=ToolRegistry(),
            approval_store=runtime.store,
        )

    asyncio.run(run_supervisor())

    updated = registry.get_task("task-verify")
    assert updated is not None
    assert updated.status is TaskStatus.FAILED
    assert updated.result is not None
    assert updated.result.error == "Dependency failed: task-create"
    assert runtime.store.get_doctor_action("act-planner-dependency-grp-planner-task-verify") is not None


def test_supervisor_does_not_timeout_task_while_runner_is_active(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.01")
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_TIMEOUT_GRACE_SECONDS", "0.01")
    from nullion.task_queue import TaskRegistry

    registry = TaskRegistry()
    task = TaskRecord(
        task_id="task-create",
        group_id="grp-active",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create spreadsheet",
        description="Create the spreadsheet artifact.",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=[],
        timeout_s=0.01,
        started_at=datetime.now(UTC) - timedelta(seconds=5),
    )
    group = TaskGroup(
        group_id="grp-active",
        conversation_id="telegram:123",
        original_message="Create a spreadsheet with images.",
        tasks=[task],
        planner_metadata={"disposition": "sequential_mission"},
    )

    async def run_supervisor() -> None:
        await registry.add_group(group)
        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator._task_registry = registry
        orchestrator._progress_queue = asyncio.Queue()

        async def finish_active_task() -> None:
            await asyncio.sleep(0.03)
            await registry.update_task(
                "task-create",
                status=TaskStatus.COMPLETE,
                completed_at=datetime.now(UTC),
                result=TaskResult("task-create", "success", output="created"),
            )

        active_task = asyncio.create_task(finish_active_task(), name="task-task-create")
        orchestrator._runner_tasks_by_group = {"grp-active": {active_task}}
        await orchestrator._supervise_dispatch_group("grp-active", policy_store=None)

    asyncio.run(run_supervisor())

    updated = registry.get_task("task-create")
    assert updated is not None
    assert updated.status is TaskStatus.COMPLETE
    assert updated.result is not None
    assert updated.result.status == "success"


def test_supervisor_does_not_timeout_blocked_dependency_waiter(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.01")
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_TIMEOUT_GRACE_SECONDS", "0.01")
    from nullion.task_queue import TaskRegistry

    registry = TaskRegistry()
    source = TaskRecord(
        task_id="task-collect",
        group_id="grp-waiting",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Collect product data",
        description="Collect source records.",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_run_js"],
        dependencies=[],
        timeout_s=999,
        started_at=datetime.now(UTC),
    )
    blocked = TaskRecord(
        task_id="task-create",
        group_id="grp-waiting",
        conversation_id="telegram:123",
        principal_id="telegram:123",
        title="Create Excel workbook",
        description="Create workbook from collected records.",
        status=TaskStatus.BLOCKED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["spreadsheet_create"],
        dependencies=["task-collect"],
        created_at=datetime.now(UTC) - timedelta(seconds=30),
        started_at=None,
        timeout_s=0.01,
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )
    group = TaskGroup(
        group_id="grp-waiting",
        conversation_id="telegram:123",
        original_message="Create an Excel workbook from collected product records.",
        tasks=[source, blocked],
        planner_metadata={"disposition": "sequential_mission"},
    )

    async def run_supervisor_briefly() -> None:
        await registry.add_group(group)
        orchestrator = AgentOrchestrator(model_client=object())
        orchestrator._task_registry = registry
        orchestrator._progress_queue = asyncio.Queue()
        try:
            await asyncio.wait_for(orchestrator._supervise_dispatch_group("grp-waiting", policy_store=None), timeout=0.05)
        except asyncio.TimeoutError:
            pass

    asyncio.run(run_supervisor_briefly())

    updated = registry.get_task("task-create")
    assert updated is not None
    assert updated.status is TaskStatus.BLOCKED
    assert updated.started_at is None
    assert updated.result is None


def test_browser_item_context_preserves_records_without_duplicate_fields():
    from nullion.agent_orchestrator import _compact_tool_output_for_model_context
    items = [{"title": f"項目 {i}", "link_text": f"項目 {i}", "url": f"https://example.org/{i}",
              "canonical_url": f"https://example.org/{i}", "price": 0, "available": False,
              "compact_text": "確認された情報", "price_candidates": [], "image_alt": ""} for i in range(40)]
    result = _compact_tool_output_for_model_context("browser_extract_items", {"items": items, "item_count": 40})
    assert len(result["items"]) == 40
    for original, compact in zip(items, result["items"]):
        assert compact["title"] == original["title"]
        assert compact["url"] == original["url"]
        assert compact["compact_text"] == original["compact_text"]
        assert compact["price"] == 0 and compact["available"] is False
        assert "canonical_url" not in compact and "link_text" not in compact
    assert items[0]["canonical_url"] == "https://example.org/0"

from __future__ import annotations

from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from nullion.chat_streaming import (
    ChatPlatformCapabilities,
    ChatStreamingMode,
    chunk_chat_text,
    iter_chat_text_chunks,
    platform_supports_text_streaming,
    select_chat_streaming_mode,
    streaming_enabled_by_default,
)
from nullion.chat_text import make_markdown_tables_chat_readable
from nullion.mini_agent_runner import ProgressUpdate
from nullion.mini_agents import MiniAgentLaunchDecision, decide_mini_agent_launch
from nullion.mission_status import (
    build_mission_snapshot,
    classify_mission_execution_phase,
    classify_mission_execution_role,
    format_mission_for_telegram,
    render_mission_for_telegram,
)
from nullion.missions import (
    MissionChecklistItem,
    MissionContinuationPolicy,
    MissionRecord,
    MissionStatus,
    MissionStep,
    MissionTerminalReason,
)
from nullion.result_aggregator import ResultAggregator, _planner_summary_from_group
from nullion.task_queue import TaskGroup, TaskPriority, TaskRecord, TaskResult, TaskStatus
from nullion.telegram_formatting import format_telegram_text
from nullion.thinking_display import (
    append_thinking_to_reply,
    extract_thinking_text,
    format_thinking_display,
    set_thinking_display_enabled,
    thinking_display_enabled,
    thinking_display_status_text,
)
from nullion.agent_orchestrator import AgentOrchestrator


def test_markdown_table_rendering_preserves_non_tables_and_fenced_tables() -> None:
    text = "\n".join(
        [
            "Before",
            "| Name | Value |",
            "| --- | :---: |",
            "| Alpha | 1 |",
            "| Beta | 2 |",
            "",
            "```",
            "| Raw | Table |",
            "| --- | --- |",
            "```",
        ]
    )

    assert make_markdown_tables_chat_readable(text) == "\n".join(
        [
            "Before",
            "Row 1:",
            "- Name: Alpha",
            "- Value: 1",
            "",
            "Row 2:",
            "- Name: Beta",
            "- Value: 2",
            "",
            "```",
            "| Raw | Table |",
            "| --- | --- |",
            "```",
        ]
    )
    assert make_markdown_tables_chat_readable("| A | B |\nnot separator") == "| A | B |\nnot separator"


def test_chat_streaming_modes_and_chunking() -> None:
    chunks = ChatPlatformCapabilities("chunks", supports_chunks=True)
    edits = ChatPlatformCapabilities("edits", supports_message_edits=True)
    typing = ChatPlatformCapabilities("typing", supports_typing_indicator=True)
    quiet = ChatPlatformCapabilities("quiet")

    assert platform_supports_text_streaming(chunks) is True
    assert streaming_enabled_by_default(edits) is True
    assert select_chat_streaming_mode(chunks) is ChatStreamingMode.CHUNKS
    assert select_chat_streaming_mode(edits) is ChatStreamingMode.MESSAGE_EDITS
    assert select_chat_streaming_mode(typing) is ChatStreamingMode.FINAL_ONLY
    assert select_chat_streaming_mode(typing, streaming_enabled=True) is ChatStreamingMode.TYPING_ONLY
    assert select_chat_streaming_mode(quiet) is ChatStreamingMode.FINAL_ONLY
    assert select_chat_streaming_mode(chunks, streaming_enabled=False) is ChatStreamingMode.FINAL_ONLY

    assert chunk_chat_text("") == []
    with pytest.raises(ValueError):
        chunk_chat_text("hello", max_chars=7)
    split = chunk_chat_text("alpha beta gamma " + ("x" * 130), max_chars=20)
    assert "alpha beta gamma " in split
    assert all(len(part) <= 40 for part in split)
    assert list(iter_chat_text_chunks("alpha beta", max_chars=20)) == ["alpha beta"]


def test_thinking_display_helpers_obey_env_and_structured_blocks(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_SHOW_THINKING_ENABLED", raising=False)
    assert thinking_display_enabled(default=True) is True
    assert thinking_display_enabled(default=False) is False

    set_thinking_display_enabled(True)
    assert thinking_display_status_text() == "on"
    set_thinking_display_enabled(False)
    assert thinking_display_status_text() == "off"
    monkeypatch.setenv("NULLION_SHOW_THINKING_ENABLED", "yes")
    assert thinking_display_enabled() is True

    text = extract_thinking_text(
        [
            {"type": "text", "text": "ignored"},
            {"type": "thinking", "text": "first"},
            "ignored",
            {"type": "reasoning_summary", "summary": "second"},
        ]
    )
    assert text == "first\n\nsecond"
    assert extract_thinking_text([{"type": "thinking", "text": " "}]) is None
    assert format_thinking_display("  careful thought  ", label="Notes") == "Notes\ncareful thought"
    assert format_thinking_display("") is None
    assert append_thinking_to_reply("Reply\n", "why", enabled=True) == "Reply\n\nThinking\nwhy"
    assert append_thinking_to_reply("", "why", enabled=True) == "Thinking\nwhy"
    assert append_thinking_to_reply("Reply", "why", enabled=False) == "Reply"


def test_telegram_formatting_escapes_html_and_sets_parse_mode_when_needed() -> None:
    formatted, options = format_telegram_text("Use **bold** and `x < y`")
    assert formatted == "Use <b>bold</b> and <code>x &lt; y</code>"
    assert options == {"parse_mode": "HTML"}
    assert format_telegram_text("plain <unsafe>") == ("plain <unsafe>", {})


def _mission(status: MissionStatus, *, active_kind: str = "tool") -> MissionRecord:
    return MissionRecord(
        mission_id="m1",
        owner="workspace:one",
        title="Ship tests",
        goal="Increase coverage",
        status=status,
        continuation_policy=MissionContinuationPolicy.MANUAL,
        active_step_id="s2",
        steps=(
            MissionStep(step_id="s1", title="Plan", status="completed", kind="plan"),
            MissionStep(
                step_id="s2",
                title="Execute",
                status="running",
                kind=active_kind,
                mini_agent_run_id="run-1",
                mini_agent_run_ids=("run-1", "run-2"),
                required_mini_agent_run_ids=("run-2",),
            ),
        ),
        completion_checklist=(
            MissionChecklistItem(item_id="c1", label="Tests", required=True, satisfied=True),
            MissionChecklistItem(item_id="c2", label="Docs", required=False, satisfied=False),
        ),
        last_progress_message="Keep going",
    )


@pytest.mark.parametrize(
    ("status", "phase", "role"),
    [
        (MissionStatus.COMPLETED, "terminal", "terminal"),
        (MissionStatus.WAITING_APPROVAL, "waiting", "executor"),
        (MissionStatus.VERIFYING, "verifying", "executor"),
        (MissionStatus.RUNNING, "executing", "executor"),
        (MissionStatus.PENDING, "planning", "executor"),
    ],
)
def test_mission_phase_role_and_snapshot(status: MissionStatus, phase: str, role: str) -> None:
    mission = _mission(status)
    snapshot = build_mission_snapshot(mission)

    assert classify_mission_execution_phase(mission) == phase
    assert classify_mission_execution_role(mission) == role
    assert snapshot["active_step"] == "Execute"
    assert snapshot["active_step_run_ids"] == ["run-1", "run-2"]
    assert snapshot["required_step_run_ids"] == ["run-1", "run-2"]
    assert snapshot["checklist"] == {"total": 2, "required": 1, "satisfied": 1}


def test_mission_rendering_includes_waiting_result_and_run_details() -> None:
    waiting = _mission(MissionStatus.WAITING_USER, active_kind="user_input")
    waiting = replace(waiting, waiting_on="user answer")
    rendered = render_mission_for_telegram(waiting)
    assert "Role: waiting" in rendered
    assert "Mini-Agents: 2 running" in rendered
    assert "Runs: run-1, run-2" in rendered
    assert "Waiting on: user answer" in rendered

    completed = MissionRecord(
        mission_id="m2",
        owner="workspace:one",
        title="Done",
        goal="goal",
        status=MissionStatus.COMPLETED,
        continuation_policy=MissionContinuationPolicy.AUTO_FINISH,
        result_summary="all set",
        terminal_reason=MissionTerminalReason.COMPLETED,
    )
    assert "Result: all set" in format_mission_for_telegram(build_mission_snapshot(completed))


@dataclass
class State:
    value: str


def test_mini_agent_launch_policy_covers_hold_deny_launch_and_safe_routes() -> None:
    def capsule(state: str, risk: str = ""):
        return SimpleNamespace(state=State(state), risk_level=risk)

    assert decide_mini_agent_launch(capsule("waiting_approval")).decision is MiniAgentLaunchDecision.HOLD
    assert decide_mini_agent_launch(capsule("completed")).decision is MiniAgentLaunchDecision.DENY
    assert decide_mini_agent_launch(capsule("pending", "high")).decision is MiniAgentLaunchDecision.HOLD
    safe_high = decide_mini_agent_launch(capsule("pending", "high"), {"safe": True, "mini_agent_type": "researcher"})
    assert safe_high.decision is MiniAgentLaunchDecision.LAUNCH
    assert safe_high.mini_agent_type == "researcher"
    assert decide_mini_agent_launch(capsule("pending"), {"safe": True}).decision is MiniAgentLaunchDecision.LAUNCH
    unsafe = decide_mini_agent_launch(capsule("pending"), {"safe": False, "reason": "policy denied"})
    assert unsafe.decision is MiniAgentLaunchDecision.DENY
    assert unsafe.reason == "policy_route=safe_false;policy_reason=policy denied;decision=deny"
    assert decide_mini_agent_launch(capsule("running"), {"target": "sentinel"}).decision is MiniAgentLaunchDecision.HOLD
    assert decide_mini_agent_launch(capsule("unknown")).reason == "intent_state=unknown;decision=deny"


def task(task_id: str, status: TaskStatus = TaskStatus.QUEUED, *, result: TaskResult | None = None) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        group_id="g1",
        conversation_id="c1",
        principal_id="p1",
        title=f"Task {task_id}",
        description="desc",
        status=status,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
        result=result,
    )


@pytest.mark.asyncio
async def test_result_aggregator_delivers_status_summary_final_and_artifacts() -> None:
    deliveries: list[tuple[str, str, dict]] = []

    async def deliver(conversation_id: str, text: str, **kwargs) -> None:
        deliveries.append((conversation_id, text, kwargs))

    registry = SimpleNamespace()
    group = TaskGroup(
        group_id="g1",
        conversation_id="c1",
        original_message="Do work",
        tasks=[
            task("t1", TaskStatus.COMPLETE, result=TaskResult("t1", "success", output="alpha", artifacts=["/tmp/a.txt"])),
            task("t2", TaskStatus.COMPLETE, result=TaskResult("t2", "success", output="beta")),
        ],
        planner_metadata={"disposition": "parallel", "tasks": ["a", "b"]},
    )
    registry.get_group = lambda group_id: group if group_id == "g1" else None
    registry.get_task = lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None)

    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)
    await aggregator._handle(ProgressUpdate(agent_id="a1", task_id="t1", group_id="g1", kind="task_started"))
    await aggregator._handle(ProgressUpdate(agent_id="a1", task_id="t1", group_id="g1", kind="task_complete", message="ok"))

    assert any("Parallel • 2 tasks" in text for _, text, kwargs in deliveries if kwargs.get("is_status"))
    assert ("c1", "Completed 2/2 task(s). • Task t1: alpha • Task t2: beta", {}) in deliveries
    assert ("c1", "/tmp/a.txt", {"is_artifact": True}) in deliveries

    deliveries.clear()
    await aggregator._handle(ProgressUpdate(agent_id="a1", task_id="t1", group_id="g1", kind="task_complete"))
    assert not any(kwargs == {} for _, _, kwargs in deliveries)


@pytest.mark.asyncio
async def test_result_aggregator_updates_checklist_top_to_bottom_without_marking_future_work_running() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    first = task("find", TaskStatus.RUNNING)
    second = task("audit", TaskStatus.BLOCKED)
    group = TaskGroup(
        group_id="g-openclaw",
        conversation_id="telegram:123",
        original_message="audit openclaw config",
        tasks=[first, second],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["find", "audit"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-openclaw" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="find", group_id="g-openclaw", kind="task_started"))

    assert deliveries[-1][1] == (
        "Planner: Sequential Mission • 2 tasks\n"
        "→ Working on 2 tasks:\n"
        "  ◐ Task find\n"
        "  ☐ Task audit"
    )
    assert deliveries[-1][2]["status_kind"] == "task_summary"


@pytest.mark.asyncio
async def test_result_aggregator_progress_input_failure_cancel_and_model_summary() -> None:
    deliveries: list[tuple[str, str, dict]] = []

    def deliver(conversation_id: str, text: str, **kwargs) -> None:
        deliveries.append((conversation_id, text, kwargs))

    failing_task = task("t1", TaskStatus.FAILED, result=TaskResult("t1", "failure", error="bad"))
    group = TaskGroup(group_id="g1", conversation_id="c1", original_message="Do work", tasks=[failing_task])
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g1" else None,
        get_task=lambda task_id: failing_task if task_id == "t1" else None,
    )
    model_client = SimpleNamespace(
        create=lambda **kwargs: {"content": [{"type": "text", "text": "model summary"}]}
    )
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, model_client=model_client, min_progress_interval_s=0)

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="t1", group_id="g1", kind="progress_note", message="halfway"))
    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="t1", group_id="g1", kind="input_needed", message="Pick one"))
    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="t1", group_id="g1", kind="task_failed", message="bad"))

    assert any(text == "→ [Task t1] halfway" for _, text, _ in deliveries)
    assert any(text == "? Pick one" and kwargs["is_question"] for _, text, kwargs in deliveries)
    assert any(text == "model summary" for _, text, _ in deliveries)

    deliveries.clear()
    group2 = TaskGroup(group_id="g2", conversation_id="c2", original_message="Cancel", tasks=[task("t2", TaskStatus.CANCELLED)])
    registry.get_group = lambda group_id: group2 if group_id == "g2" else None
    registry.get_task = lambda task_id: group2.tasks[0]
    no_model = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)
    await no_model._handle(ProgressUpdate(agent_id="a", task_id="t2", group_id="g2", kind="task_cancelled"))
    assert any("Completed 0/1 task(s)." in text for _, text, _ in deliveries)


@pytest.mark.asyncio
async def test_result_aggregator_ignores_terminal_event_until_registry_is_terminal() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    running_task = task("t1", TaskStatus.RUNNING)
    group = TaskGroup(group_id="g1", conversation_id="c1", original_message="Do work", tasks=[running_task])
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g1" else None,
        get_task=lambda task_id: running_task if task_id == "t1" else None,
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="t1", group_id="g1", kind="task_complete"))

    assert deliveries == []


@pytest.mark.asyncio
async def test_dispatch_supervisor_refreshes_checklist_instead_of_leaking_prose(monkeypatch) -> None:
    deliveries: list[tuple[str, str, dict]] = []
    running = task("find", TaskStatus.RUNNING)
    group = TaskGroup(
        group_id="g-openclaw",
        conversation_id="telegram:123",
        original_message="audit openclaw config",
        tasks=[running],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["find"]},
    )

    class Registry:
        def __init__(self) -> None:
            self.calls = 0

        def get_group(self, group_id: str):
            self.calls += 1
            if self.calls > 2:
                running.status = TaskStatus.COMPLETE
            return group

    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator._task_registry = Registry()
    orchestrator._deliver_fn = lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs))
    orchestrator._supervisor_tasks = set()
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.01")

    await orchestrator._supervise_dispatch_group("g-openclaw", policy_store=None)

    assert deliveries
    assert all("Mini-Agents are still working" not in text for _, text, _ in deliveries)
    assert deliveries[0][2]["status_kind"] == "task_summary"
    assert "→ Working on 1 tasks:" in deliveries[0][1]


def test_planner_summary_metadata_edge_cases() -> None:
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata=None)) == ""
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": ""})) == ""
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": "fallback", "valid": False})) == "Fallback to normal turn"
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": "clarify", "needs_clarification": True})) == "Needs clarification"
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": "single_task", "tasks": []})) == "Single Task"

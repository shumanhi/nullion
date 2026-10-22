from __future__ import annotations

import asyncio
import os
import zipfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
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
from nullion.mini_agent_runner import MiniAgentConfig, ProgressUpdate
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
from nullion.result_aggregator import (
    ResultAggregator,
    _merge_missing_requested_artifact_summary,
    _planner_summary_from_group,
    _requested_attachment_extension_for_group,
)
from nullion.task_queue import TaskGroup, TaskPriority, TaskRecord, TaskRegistry, TaskResult, TaskStatus
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
            "- Alpha: 1",
            "- Beta: 2",
            "",
            "```",
            "| Raw | Table |",
            "| --- | --- |",
            "```",
        ]
    )
    assert make_markdown_tables_chat_readable("| A | B |\nnot separator") == "| A | B |\nnot separator"


def test_markdown_table_rendering_avoids_row_column_dump_for_chat() -> None:
    weather_table = "\n".join(
        [
            "| Column 1 | Details |",
            "| --- | --- |",
            "| Conditions | Partly cloudy early, becoming overcast overnight |",
            "| Current | 61°F, sunny |",
            "| Sunset | 7:52 PM |",
            "",
            "Hourly outlook:",
            "| Time | Temp |",
            "| --- | --- |",
            "| 6 PM | 60° |",
            "| 7 PM | 59° |",
        ]
    )

    rendered = make_markdown_tables_chat_readable(weather_table)

    assert "Row 1" not in rendered
    assert "Column 1" not in rendered
    assert "- Conditions: Partly cloudy early, becoming overcast overnight" in rendered
    assert "- 6 PM: 60°" in rendered


def test_markdown_table_rendering_expands_compacted_model_table_rows() -> None:
    text = (
        "| Artist / Person | Date | Venue / Location | "
        "|---|---|---| "
        "| Carly Rae Jepsen | Sun, Sep 27, 2026 | Forest Hills Stadium | "
        "| Cole Escola | Ongoing dates | Lyceum Theatre |"
    )

    rendered = make_markdown_tables_chat_readable(text)

    assert "|---|" not in rendered
    assert "1. Artist / Person: Carly Rae Jepsen" in rendered
    assert "\nDate: Sun, Sep 27, 2026" in rendered
    assert "\nVenue / Location: Forest Hills Stadium" in rendered
    assert "Venue / Location: Forest Hills Stadium\n\n2. Artist / Person: Cole Escola" in rendered
    assert "2. Artist / Person: Cole Escola" in rendered
    assert " · " not in rendered


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


def test_telegram_formatting_renders_long_raw_urls_as_clickable_links() -> None:
    url = (
        "https://dddcaa.r.tsp1-brevo.net/tr/cl/"
        "kAEx2mZXu4DuX7VOOs8gQ-gUM4GVcGTElobJJBsfoPigRNwMNVE4mm"
    )

    formatted, options = format_telegram_text(f"**Online Payment Site**\n{url}")

    assert options == {"parse_mode": "HTML"}
    assert '<b>Online Payment Site</b>' in formatted
    assert (
        f'<a href="{url}">Open link (dddcaa.r.tsp1-brevo.net)</a>'
        in formatted
    )
    assert url not in formatted.replace(f'href="{url}"', "")


def test_telegram_formatting_does_not_swallow_bold_markers_after_raw_url() -> None:
    url = "https://events.example.test/pages/dates"

    formatted, options = format_telegram_text(f"📌 **Source: {url}**")

    assert options == {"parse_mode": "HTML"}
    assert formatted == f'📌 <b>Source: <a href="{url}">{url}</a></b>'
    assert "**" not in formatted


def test_telegram_formatting_renders_markdown_links_as_html_anchors() -> None:
    formatted, options = format_telegram_text(
        "Portal: [Online Payment Site](https://pay.example.test/checkout?a=1&b=2)"
    )

    assert options == {"parse_mode": "HTML"}
    assert formatted == (
        'Portal: <a href="https://pay.example.test/checkout?a=1&amp;b=2">'
        "Online Payment Site</a>"
    )


def test_telegram_formatting_makes_markdown_tables_readable_on_mobile() -> None:
    text = "\n".join(
        [
            "SCHEDULED TASK: Tour Tracker",
            "",
            "Upcoming / currently relevant NYC-area events found:",
            "",
            "| Artist / Person | Date | Venue / Location | Source / ticket link | Note |",
            "|---|---|---|---|---|",
            (
                "| Carly Rae Jepsen | Sun, Sep 27, 2026 | Forest Hills Stadium, Forest Hills, Queens, NY | "
                "Official artist listing / AXS listing surfaced in search | Listed as All Things Go NYC 2026 |"
            ),
            (
                "| Cole Escola | Ongoing Broadway engagement / ticketed dates | Lyceum Theatre, New York, NY | "
                "Official listing | Tickets available |"
            ),
        ]
    )

    formatted, options = format_telegram_text(text)

    assert options == {}
    assert "|---|" not in formatted
    assert "| Artist / Person |" not in formatted
    assert "1. Artist / Person: Carly Rae Jepsen" in formatted
    assert "\nDate: Sun, Sep 27, 2026" in formatted
    assert "\nVenue / Location: Forest Hills Stadium" in formatted
    assert "Note: Listed as All Things Go NYC 2026\n\n2. Artist / Person: Cole Escola" in formatted
    assert "2. Artist / Person: Cole Escola" in formatted
    assert " · " not in formatted


def test_telegram_formatting_normalizes_quote_fenced_code_blocks() -> None:
    formatted, options = format_telegram_text("'''svg\n<svg width=\"1\"></svg>\n'''")

    assert options == {"parse_mode": "HTML"}
    assert "<pre><code class=\"language-svg\">" in formatted
    assert "&lt;svg width=&quot;1&quot;&gt;&lt;/svg&gt;" in formatted
    assert "'''svg" not in formatted


def test_telegram_formatting_renders_markdown_blockquotes_without_visible_markers() -> None:
    formatted, options = format_telegram_text("Body:\n> A puppy wakes\n> With bright eyes")

    assert options == {"parse_mode": "HTML"}
    assert "<blockquote>A puppy wakes\nWith bright eyes</blockquote>" in formatted
    assert "&gt;" not in formatted


def test_telegram_formatting_renders_single_star_emphasis_and_bullets() -> None:
    formatted, options = format_telegram_text(
        "3. Search results\n"
        "- PBS surfaced examples:\n"
        "  - Coffee and Anthropology from *Crossing South*\n"
        "  - Science at The Museum from *Colorado Classroom*"
    )

    assert options == {"parse_mode": "HTML"}
    assert "*Crossing South*" not in formatted
    assert "<i>Crossing South</i>" in formatted
    assert "<i>Colorado Classroom</i>" in formatted
    assert "- PBS surfaced" not in formatted
    assert "• PBS surfaced examples:" in formatted
    assert "  • Coffee and Anthropology" in formatted


def test_telegram_formatting_does_not_repeat_blockquotes_after_flush() -> None:
    formatted, options = format_telegram_text(
        "Say:\n\n"
        '> "Hi, I would like to place a delivery order."\n'
        "> Delivery address is **116 West 87th St, Apt 2**.\n"
        "> Can you tell me the total and estimated delivery time?\n\n"
        "Then give payment info if they ask.\n\n"
        "Choose:\n"
        '- **Large 18"**'
    )

    assert options == {"parse_mode": "HTML"}
    assert formatted.count("<blockquote>") == 1
    assert formatted.count("Delivery address is") == 1
    assert "Then give payment info if they ask." in formatted
    assert "Choose:" in formatted
    assert "Large 18&quot;" in formatted


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


def test_orchestrator_partial_task_results_map_to_waiting_input() -> None:
    from nullion.agent_orchestrator import (
        _mini_agent_run_status_for_task_result,
        _progress_kind_for_task_result,
        _task_status_for_task_result,
    )
    from nullion.mini_agent_runs import MiniAgentRunStatus

    user_input = TaskResult("t1", "partial", output="Waiting for user input: Pick one")
    approval = TaskResult("t2", "partial", output="Approval required before this delegated task can continue.")

    assert _task_status_for_task_result(user_input) is TaskStatus.WAITING_INPUT
    assert _mini_agent_run_status_for_task_result(user_input) is MiniAgentRunStatus.WAITING_INPUT
    assert _progress_kind_for_task_result(user_input) == "input_needed"
    assert _task_status_for_task_result(approval) is TaskStatus.WAITING_INPUT
    assert _progress_kind_for_task_result(approval) == "approval_needed"


def test_delegated_pause_persists_resume_token_for_approval() -> None:
    from nullion.agent_orchestrator import _store_delegated_pause_suspended_turn
    from nullion.runtime_store import RuntimeStore

    store = RuntimeStore()
    record = SimpleNamespace(
        task_id="task-1",
        group_id="group-1",
        conversation_id="web:abc",
        description="Fetch a page",
        agent_id=None,
    )
    result = TaskResult(
        "task-1",
        "partial",
        output="Approval required before this delegated task can continue. Approval ID: ap-1",
        resume_token={
            "backend": "deepagents",
            "reason": "approval_required",
            "approval_id": "ap-1",
            "thread_id": "thread-1",
        },
    )

    _store_delegated_pause_suspended_turn(store, store, task=record, result=result, agent_id="agent-1")

    suspended = store.get_suspended_turn("ap-1")
    assert suspended is not None
    assert suspended.task_id == "task-1"
    assert suspended.group_id == "group-1"
    assert suspended.agent_id == "agent-1"
    assert suspended.resume_token["thread_id"] == "thread-1"


def test_delegated_resume_uses_waiting_task_state_without_text_phrases() -> None:
    from nullion.chat_operator import _latest_waiting_delegated_suspended_turn, _resume_delegated_suspended_turn
    from nullion.mini_agent_runs import MiniAgentRunStatus, create_mini_agent_run, transition_mini_agent_run_status
    from nullion.runtime_store import RuntimeStore
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry

    store = RuntimeStore()
    created_at = datetime.now(UTC)
    run = create_mini_agent_run(
        run_id="task-1",
        capsule_id="group-1",
        mini_agent_type="Create workbook",
        created_at=created_at,
    )
    store.add_mini_agent_run(transition_mini_agent_run_status(run, MiniAgentRunStatus.RUNNING))
    store.add_mini_agent_run(
        transition_mini_agent_run_status(
            store.get_mini_agent_run("task-1"),
            MiniAgentRunStatus.WAITING_INPUT,
            result_summary="Waiting for user input: choose",
        )
    )
    store.add_suspended_turn(
        SuspendedTurn(
            approval_id="task:task-1",
            conversation_id="telegram:123",
            chat_id="123",
            message="Create workbook",
            request_id=None,
            message_id=None,
            created_at=created_at,
            task_id="task-1",
            group_id="group-1",
            agent_id="agent-1",
            resume_token={"reason": "user_input", "thread_id": "thread-1"},
        )
    )
    calls: list[dict[str, object]] = []

    class Orchestrator:
        def resume_paused_task_background_sync(self, **kwargs):
            calls.append(kwargs)
            return True

    runtime = SimpleNamespace(
        store=store,
        active_tool_registry=ToolRegistry(),
        checkpoint=lambda *args, **kwargs: None,
    )

    suspended = _latest_waiting_delegated_suspended_turn(
        runtime,
        conversation_id="telegram:123",
        chat_id="123",
    )

    assert suspended is not None
    reply = _resume_delegated_suspended_turn(
        runtime,
        suspended_turn=suspended,
        agent_orchestrator=Orchestrator(),
        tool_registry=ToolRegistry(),
        user_response="arbitrary non-command response",
        background=True,
    )

    assert reply == "Continuing the paused planner task."
    assert calls[0]["task_id"] == "task-1"
    assert calls[0]["user_response"] == "arbitrary non-command response"


def test_telegram_approval_resume_uses_delegated_task_branch() -> None:
    from dataclasses import replace as dataclass_replace

    from nullion.approvals import ApprovalStatus, create_approval_request
    from nullion.chat_operator import resume_approved_telegram_request
    from nullion.runtime_store import RuntimeStore
    from nullion.suspended_turns import SuspendedTurn
    from nullion.tools import ToolRegistry

    store = RuntimeStore()
    approval = create_approval_request(
        "telegram:123",
        "use_tool",
        "spreadsheet_create",
        context={"tool_name": "spreadsheet_create"},
    )
    store.add_approval_request(
        dataclass_replace(
            approval,
            status=ApprovalStatus.APPROVED,
            decided_by="doctor",
            decided_at=datetime.now(UTC),
        )
    )
    store.add_suspended_turn(
        SuspendedTurn(
            approval_id=approval.approval_id,
            conversation_id="telegram:123",
            chat_id="123",
            message="Create workbook",
            request_id=None,
            message_id=None,
            created_at=datetime.now(UTC),
            task_id="task-1",
            group_id="group-1",
            agent_id="agent-1",
            resume_token={"reason": "approval_required", "thread_id": "thread-1"},
        )
    )
    calls: list[dict[str, object]] = []

    class Orchestrator:
        def resume_paused_task_background_sync(self, **kwargs):
            calls.append(kwargs)
            return True

    runtime = SimpleNamespace(
        store=store,
        active_tool_registry=ToolRegistry(),
        checkpoint=lambda *args, **kwargs: None,
    )

    reply = resume_approved_telegram_request(
        runtime,
        approval_id=approval.approval_id,
        chat_id="123",
        agent_orchestrator=Orchestrator(),
    )

    assert reply == "Continuing the paused planner task."
    assert calls[0]["task_id"] == "task-1"
    assert calls[0]["user_response"] is None


def test_deep_agent_thread_id_prefers_persisted_resume_thread() -> None:
    from nullion.deep_agent_runner import _deep_agent_thread_id

    config = SimpleNamespace(
        agent_id="new-agent",
        task=SimpleNamespace(
            group_id="group-1",
            task_id="task-1",
            metadata={"deep_agent_thread_id": "nullion:group-1:task-1:old-agent"},
        ),
    )

    assert _deep_agent_thread_id(config) == "nullion:group-1:task-1:old-agent"


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
    group.tasks[0].allowed_tools = ["file_write"]
    registry.get_group = lambda group_id: group if group_id == "g1" else None
    registry.get_task = lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None)

    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)
    await aggregator._handle(ProgressUpdate(agent_id="a1", task_id="t1", group_id="g1", kind="task_started"))
    await aggregator._handle(ProgressUpdate(agent_id="a1", task_id="t1", group_id="g1", kind="task_complete", message="ok"))

    assert any("Parallel • 2 tasks" in text for _, text, kwargs in deliveries if kwargs.get("is_status"))
    assert (
        "c1",
        "📊 **PLANNER RESULT**\n\nFor: Do work\n\nCompleted 2/2 task(s). • Task t1: alpha • Task t2: beta",
        {"group_id": "g1"},
    ) in deliveries
    assert ("c1", "/tmp/a.txt", {"group_id": "g1", "is_artifact": True}) in deliveries

    deliveries.clear()
    await aggregator._handle(ProgressUpdate(agent_id="a1", task_id="t1", group_id="g1", kind="task_complete"))
    assert not any(kwargs == {} for _, _, kwargs in deliveries)


@pytest.mark.asyncio
async def test_result_aggregator_delivers_terminal_failed_task_card_without_artifact() -> None:
    deliveries: list[tuple[str, str, dict]] = []

    def deliver(conversation_id: str, text: str, **kwargs) -> None:
        deliveries.append((conversation_id, text, kwargs))

    collect = task("collect", TaskStatus.COMPLETE, result=TaskResult("collect", "success", output="collected data"))
    create = task(
        "create",
        TaskStatus.FAILED,
        result=TaskResult("create", "failure", error="No deliverable file was produced."),
    )
    verify = task(
        "verify",
        TaskStatus.FAILED,
        result=TaskResult("verify", "failure", error="Dependency failed: create"),
    )
    verify.dependencies = ["create"]
    deliver_receipt = task(
        "deliver",
        TaskStatus.FAILED,
        result=TaskResult("deliver", "failure", error="Dependency failed: verify"),
    )
    deliver_receipt.dependencies = ["verify"]
    group = TaskGroup(
        group_id="g1",
        conversation_id="telegram:123",
        original_message="Create a spreadsheet from collected source data.",
        tasks=[collect, create, verify, deliver_receipt],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["collect", "create", "verify", "deliver"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g1" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)

    await aggregator._handle(
        ProgressUpdate(agent_id="agent-create", task_id="create", group_id="g1", kind="task_failed", message="failed")
    )

    terminal_cards = [
        text for _, text, kwargs in deliveries if kwargs.get("is_status") and kwargs.get("terminal") is True
    ]
    assert terminal_cards
    terminal = terminal_cards[-1]
    assert "✕ Task create" in terminal
    assert "✕ Task verify" in terminal
    assert "✕ Task deliver" in terminal
    assert "◐ Task create" not in terminal
    assert "☐ Task verify" not in terminal
    assert any(kwargs == {"group_id": "g1"} for _, _, kwargs in deliveries)


@pytest.mark.asyncio
async def test_result_aggregator_fallback_hides_raw_tool_payloads() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    raw_payload = (
        '{"stdout":"AAPL 188.42\\nMSFT 426.11","stderr":"","exit_code":0,'
        '"shell":"bash","network_mode":"enabled"}'
    )
    group = TaskGroup(
        group_id="g-raw",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Market scan] Pull live quotes and summarize.",
        tasks=[
            task(
                "quotes",
                TaskStatus.COMPLETE,
                result=TaskResult("quotes", "success", output=raw_payload),
            ),
            task(
                "rank",
                TaskStatus.FAILED,
                result=TaskResult("rank", "failure", error="Deep Agent finished without a final answer."),
            ),
        ],
        planner_metadata={"disposition": "parallel_mission", "tasks": ["quotes", "rank"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-raw" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(agent_id="agent-rank", task_id="rank", group_id="g-raw", kind="task_failed")
    )

    final_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-raw"}]
    assert final_messages
    final = final_messages[-1]
    assert "No report was published" in final
    assert "stdout" not in final
    assert "stderr" not in final
    assert "exit_code" not in final
    assert "shell" not in final
    assert "network_mode" not in final


@pytest.mark.asyncio
async def test_scheduled_result_aggregator_does_not_attach_text_mentioned_pdf(tmp_path) -> None:
    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    pdf = artifact_root / "email_deadline_triage_2026-05-04.pdf"
    pdf.write_bytes(b"%PDF-1.4\n% test\n")

    async def deliver(conversation_id: str, text: str, **kwargs) -> None:
        deliveries.append((conversation_id, text, kwargs))

    report_task = task(
        "deliver",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "deliver",
            "success",
            output=(
                "Email deadline triage completed. A PDF copy of the triage report is attached.\n\n"
                f"MEDIA:{pdf}"
            ),
        ),
    )
    report_task.principal_id = "telegram:123"
    group = TaskGroup(
        group_id="g-scheduled",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Email deadline triage] Check email deadlines.",
        tasks=[report_task],
        planner_metadata={"disposition": "single_task", "tasks": ["deliver"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-scheduled" else None,
        get_task=lambda task_id: report_task if task_id == "deliver" else None,
    )
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)

    await aggregator._handle(
        ProgressUpdate(agent_id="a1", task_id="deliver", group_id="g-scheduled", kind="task_complete")
    )

    terminal_messages = [text for _, text, kwargs in deliveries if not kwargs.get("is_status")]
    attached_artifacts = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert attached_artifacts == []
    assert terminal_messages
    assert "PDF copy" not in terminal_messages[-1]
    assert "attached" not in terminal_messages[-1].lower()


@pytest.mark.asyncio
async def test_scheduled_result_aggregator_does_not_attach_unrequested_result_pdf(tmp_path, monkeypatch) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    pdf = artifact_root / "peak_design_shipment_update.pdf"
    pdf.write_bytes(b"%PDF-1.4\n% unrequested scheduled artifact\n")
    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    report_task = task(
        "check",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "check",
            "success",
            output=f"I created the PDF attachment successfully: PDF: {pdf.name}",
            artifacts=[str(pdf)],
        ),
    )
    report_task.principal_id = "telegram:123"
    group = TaskGroup(
        group_id="g-scheduled-unrequested-pdf",
        conversation_id="telegram:123",
        original_message=(
            "[Manual scheduled task run: Peak Design shipment tracker] "
            "Track the shipment status and report only when the status changes."
        ),
        tasks=[report_task],
        planner_metadata={"disposition": "single_task", "tasks": ["check"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-scheduled-unrequested-pdf" else None,
        get_task=lambda task_id: report_task if task_id == "check" else None,
    )
    aggregator = ResultAggregator(deliver_fn=lambda *args, **kwargs: deliveries.append((*args, kwargs)), task_registry=registry, min_progress_interval_s=0)

    await aggregator._handle(
        ProgressUpdate(agent_id="a1", task_id="check", group_id="g-scheduled-unrequested-pdf", kind="task_complete")
    )

    attached_artifacts = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    terminal_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-scheduled-unrequested-pdf"}]
    assert attached_artifacts == []
    assert terminal_messages
    assert "PDF" not in terminal_messages[-1]
    assert "attachment" not in terminal_messages[-1].lower()


@pytest.mark.asyncio
async def test_scheduled_result_aggregator_keeps_structured_artifact_delivery(tmp_path) -> None:
    deliveries: list[tuple[str, str, dict]] = []
    html = tmp_path / "report.html"
    html.write_text("<h1>Report</h1>", encoding="utf-8")
    report_task = task(
        "deliver",
        TaskStatus.COMPLETE,
        result=TaskResult("deliver", "success", output="Report complete.", artifacts=[str(html)]),
    )
    report_task.metadata["requires_artifact_delivery"] = True
    group = TaskGroup(
        group_id="g-scheduled",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Public report] Create the HTML report.",
        tasks=[report_task],
        planner_metadata={"disposition": "single_task", "tasks": ["deliver"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-scheduled" else None,
        get_task=lambda task_id: report_task if task_id == "deliver" else None,
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(agent_id="a1", task_id="deliver", group_id="g-scheduled", kind="task_complete")
    )

    assert ("telegram:123", str(html), {"group_id": "g-scheduled", "is_artifact": True}) in deliveries


@pytest.mark.asyncio
async def test_scheduled_result_aggregator_prefers_final_summary_artifact_over_intermediate_media(
    tmp_path, monkeypatch
) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    intermediate = artifact_root / "nullion_social_github_traffic_analysis_20260517T112120Z.html"
    final = artifact_root / "nullion-social-github-traffic-report-20260517T112200Z.html"
    history = artifact_root / "nullion_github_metrics_history.json"
    intermediate.write_text(
        "<html><body><h1>Intermediate analysis</h1><p>This file is long enough to pass verification.</p></body></html>",
        encoding="utf-8",
    )
    final.write_text(
        "<html><body><h1>Final report</h1><p>This verified report is long enough to pass verification.</p></body></html>",
        encoding="utf-8",
    )
    history.write_text('{"snapshots": []}', encoding="utf-8")

    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    class SummaryModel:
        def create(self, **_kwargs):  # noqa: ANN003
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Nullion social + GitHub traffic monitor completed. "
                            f"The verified report is attached.\n\nMEDIA artifacts/{final.name}"
                        ),
                    }
                ]
            }

    analysis_task = task(
        "analysis",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "analysis",
            "success",
            output=f"Analysis complete.\n\nReport created:\nMEDIA artifacts/{intermediate.name}",
        ),
    )
    final_task = task(
        "final",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "final",
            "success",
            output=f"Created self-contained HTML report:\n\nMEDIA artifacts/{final.name}",
            artifacts=[str(history)],
        ),
    )
    final_task.dependencies = ["analysis"]
    group = TaskGroup(
        group_id="g-scheduled-report",
        conversation_id="telegram:123",
        original_message=(
            "[Manual scheduled task run: Nullion social + GitHub traffic monitor] "
            "Create the .html report with no empty graphs."
        ),
        tasks=[analysis_task, final_task],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["analysis", "final"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-scheduled-report" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        model_client=SummaryModel(),
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(agent_id="agent-final", task_id="final", group_id="g-scheduled-report", kind="task_complete")
    )

    artifact_deliveries = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert artifact_deliveries == [str(final)], deliveries
    final_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-scheduled-report"}]
    assert final_messages
    assert "MEDIA" not in final_messages[-1]
    assert intermediate.name not in final_messages[-1]
    assert history.name not in final_messages[-1]


@pytest.mark.asyncio
async def test_scheduled_result_aggregator_prefers_created_artifact_over_stale_receipt_media(
    tmp_path, monkeypatch
) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    fresh = artifact_root / "provincetown_harbor_walk_report.html"
    stale = artifact_root / "provincetown_harbor_walkable_lodging_2026-07-01_to_2026-07-05.html"
    fresh.write_text(
        "<html><body><h1>Fresh report</h1><p>This verified report is long enough to pass validation.</p></body></html>",
        encoding="utf-8",
    )
    stale.write_text(
        "<html><body><h1>Stale report</h1><p>This older report is long enough to pass validation.</p></body></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    create_task = task(
        "create",
        TaskStatus.COMPLETE,
        result=TaskResult("create", "success", output=f"Created report.\n\nMEDIA:{fresh}"),
    )
    create_task.principal_id = "telegram:123"
    create_task.allowed_tools = ["file_write"]
    receipt_task = task(
        "receipt",
        TaskStatus.COMPLETE,
        result=TaskResult("receipt", "success", output=f"MEDIA artifacts/{stale.name}\nCAPTION report"),
    )
    receipt_task.principal_id = "telegram:123"
    receipt_task.dependencies = ["create"]
    group = TaskGroup(
        group_id="g-scheduled-stale-receipt",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Provincetown Airbnb Monitor] Build .html report",
        tasks=[create_task, receipt_task],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["create", "receipt"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-scheduled-stale-receipt" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="agent-receipt",
            task_id="receipt",
            group_id="g-scheduled-stale-receipt",
            kind="task_complete",
        )
    )

    artifact_deliveries = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert artifact_deliveries == [str(fresh)]


@pytest.mark.asyncio
async def test_scheduled_result_aggregator_blocks_artifact_when_verifier_failed_with_stale_path(
    tmp_path, monkeypatch
) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    fresh = artifact_root / "current_report.html"
    stale = artifact_root / "stale_report.html"
    fresh.write_text(
        "<html><body><h1>Fresh report</h1><p>This current artifact has enough content.</p></body></html>",
        encoding="utf-8",
    )
    stale.write_text(
        "<html><body><h1>Stale report</h1><p>This older artifact has enough content.</p></body></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    create_task = task(
        "create",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "create",
            "success",
            output=f"Created current report.\n\nMEDIA:{fresh}",
            artifacts=[str(fresh)],
        ),
    )
    create_task.principal_id = "telegram:123"
    create_task.allowed_tools = ["file_write"]
    create_task.metadata = {"requires_artifact_delivery": True, "required_artifact_kind": "html"}
    verify_task = task(
        "verify",
        TaskStatus.FAILED,
        result=TaskResult(
            "verify",
            "failure",
            error=f"Expected {fresh.name}; observed {stale.name}.",
        ),
    )
    verify_task.principal_id = "telegram:123"
    verify_task.dependencies = ["create"]
    verify_task.metadata = {"artifact_role": "verify", "requires_artifact_delivery": True}
    group = TaskGroup(
        group_id="g-scheduled-stale-verifier",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Provincetown Airbnb Monitor] Build .html report",
        tasks=[create_task, verify_task],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["create", "verify"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-scheduled-stale-verifier" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="agent-verify",
            task_id="verify",
            group_id="g-scheduled-stale-verifier",
            kind="task_failed",
        )
    )

    assert [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")] == []
    final_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-scheduled-stale-verifier"}]
    assert final_messages
    assert "Expected current_report.html" in final_messages[-1]
    assert "could not attach the requested .html file" in final_messages[-1]


@pytest.mark.asyncio
async def test_result_aggregator_withholds_artifact_until_failed_verification_is_resolved(
    tmp_path, monkeypatch
) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    workbook = artifact_root / "media_workbook.xlsx"
    workbook.write_bytes(b"PK\x03\x04xlsx content with embedded media")
    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    monkeypatch.setattr(
        result_aggregator,
        "_requested_attachment_extension_for_group",
        lambda group, model_client=None: ".png",
    )

    create_task = task(
        "create",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "create",
            "success",
            output="Created workbook with embedded media.",
            artifacts=[str(workbook)],
        ),
    )
    create_task.principal_id = "telegram:123"
    create_task.allowed_tools = ["spreadsheet_create"]
    create_task.metadata = {"requires_artifact_delivery": True, "required_artifact_kind": "png"}
    verify_task = task(
        "verify",
        TaskStatus.FAILED,
        result=TaskResult("verify", "failure", error="Verifier looked for PNG instead of the workbook."),
    )
    verify_task.principal_id = "telegram:123"
    verify_task.dependencies = ["create"]
    verify_task.metadata = {
        "artifact_role": "verify",
        "requires_artifact_delivery": True,
        "required_artifact_kind": "png",
    }
    group = TaskGroup(
        group_id="g-planner-kind-mismatch",
        conversation_id="telegram:123",
        original_message="Create a workbook with embedded media.",
        tasks=[create_task, verify_task],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["create", "verify"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-planner-kind-mismatch" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="agent-verify",
            task_id="verify",
            group_id="g-planner-kind-mismatch",
            kind="task_failed",
        )
    )

    artifact_deliveries = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert artifact_deliveries == []
    final_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-planner-kind-mismatch"}]
    assert final_messages
    assert "No report was published" in final_messages[-1]


@pytest.mark.asyncio
async def test_scheduled_result_aggregator_collapses_unmarked_same_format_artifacts(
    tmp_path, monkeypatch
) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    first = artifact_root / "nullion-social-github-traffic-report-20260517T222209Z.html"
    second = artifact_root / "nullion-social-github-traffic-report-20260517T222401Z.html"
    first.write_text("<html><body>first report with enough content</body></html>", encoding="utf-8")
    second.write_text("<html><body>second report with enough content</body></html>", encoding="utf-8")
    os.utime(first, (100, 100))
    os.utime(second, (200, 200))
    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    create_task = task(
        "create",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "create",
            "success",
            output=f"Created report.\n\nMEDIA artifacts/{first.name}",
            artifacts=[str(first)],
        ),
    )
    create_task.principal_id = "telegram:123"
    summary_task = task(
        "summary",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "summary",
            "success",
            output=f"Prepared summary.\n\nMEDIA artifacts/{second.name}",
            artifacts=[str(second)],
        ),
    )
    summary_task.principal_id = "telegram:123"
    group = TaskGroup(
        group_id="g-scheduled-same-format",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Nullion monitor] Build .html report",
        tasks=[create_task, summary_task],
        planner_metadata={"disposition": "parallel_mission", "tasks": ["create", "summary"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-scheduled-same-format" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="agent-summary",
            task_id="summary",
            group_id="g-scheduled-same-format",
            kind="task_complete",
        )
    )

    artifact_deliveries = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert artifact_deliveries == [str(second)]


def test_result_aggregator_tool_activity_keeps_task_title_only_in_label() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    research = task("research", TaskStatus.RUNNING)
    research.title = "Research Boston day trip"
    group = TaskGroup(
        group_id="g-tools",
        conversation_id="web:operator",
        original_message="Boston trip",
        tasks=[research],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["research"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-tools" else None,
        get_task=lambda task_id: research if task_id == "research" else None,
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    async def run() -> None:
        await aggregator._handle(
            ProgressUpdate(
                agent_id="agent-1",
                task_id="research",
                group_id="g-tools",
                kind="tool_activity",
                message="✓ browser_extract_text",
            )
        )

    asyncio.run(run())

    assert deliveries == [
        (
            "web:operator",
            "✓ browser_extract_text",
            {
                "is_status": True,
                "group_id": "g-tools",
                "status_kind": "tool_activity",
                "task_id": "research",
                "activity_id": "mini-agent-tools:research",
                "activity_label": "Research Boston day trip tools",
            },
        )
    ]


@pytest.mark.asyncio
async def test_result_aggregator_delivers_terminal_failure_after_visible_task_card() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    failed_group = TaskGroup(
        group_id="g-timeout",
        conversation_id="telegram:123",
        original_message="Plan the trip and report back",
        tasks=[
            task(
                "research",
                TaskStatus.FAILED,
                result=TaskResult("research", "failure", error="Timed out without reaching a terminal state."),
            ),
            task(
                "write",
                TaskStatus.FAILED,
                result=TaskResult("write", "failure", error="Dependency failed: research"),
            ),
        ],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["research", "write"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: failed_group if group_id == "g-timeout" else None,
        get_task=lambda task_id: next((item for item in failed_group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="supervisor",
            task_id="research",
            group_id="g-timeout",
            kind="task_failed",
            message="Timed out without reaching a terminal state.",
        )
    )

    assert any(kwargs.get("is_status") and kwargs.get("status_kind") == "task_summary" for _, _, kwargs in deliveries)
    final_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-timeout"}]
    assert final_messages == [
        "📊 **PLANNER RESULT**\n\nFor: Plan the trip and report back\n\n"
        "Completed 0/2 task(s). 2 failed. Failure reasons: "
        "Task research: Timed out without reaching a terminal state.; "
        "Task write: Dependency failed: research. No report was published."
    ]


@pytest.mark.asyncio
async def test_result_aggregator_materializes_missing_text_artifact(monkeypatch, tmp_path) -> None:
    import nullion.delegated_artifact_workflow as delegated_artifacts
    from nullion.delegated_artifact_workflow import finalize_delegated_artifacts

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    output_paths = [artifact_root / "cnn-headlines.txt", artifact_root / "duplicate.txt"]

    monkeypatch.setattr(delegated_artifacts, "artifact_root_for_principal", lambda principal_id: artifact_root)
    path_calls = {"count": 0}

    def next_artifact_path(*, principal_id, suffix, stem="nullion-artifact"):
        path_calls["count"] += 1
        if path_calls["count"] == 1:
            return output_paths[0]
        return output_paths[1]

    monkeypatch.setattr(
        delegated_artifacts,
        "artifact_path_for_generated_workspace_file",
        next_artifact_path,
    )

    group = TaskGroup(
        group_id="g-file",
        conversation_id="telegram:123",
        original_message="Can you fetch cnn.com headlines and attach report.txt?",
        tasks=[
            task(
                "fetch",
                TaskStatus.COMPLETE,
                result=TaskResult(
                    "fetch",
                    "success",
                    output="Judge privately admonishes prosecutors\nNew video shows moments before shooting",
                ),
            ),
            task(
                "write",
                TaskStatus.FAILED,
                result=TaskResult("write", "failure", output="No file was generated due to the path error."),
            ),
        ],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["fetch", "write"]},
    )
    group.tasks[1].allowed_tools=["file_write"]
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-file" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="write", group_id="g-file", kind="task_failed"))

    output_path = output_paths[0]
    assert output_path.read_text(encoding="utf-8").startswith(
        "Original request: Can you fetch cnn.com headlines and attach report.txt?"
    )
    assert "Judge privately admonishes prosecutors" in output_path.read_text(encoding="utf-8")
    assert "path error" not in output_path.read_text(encoding="utf-8")
    assert (
        "telegram:123",
        str(output_path),
        {"group_id": "g-file", "is_artifact": True},
    ) in deliveries
    assert any("requested file is attached" in text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-file"})
    assert finalize_delegated_artifacts(group) == [str(output_path)]
    assert not output_paths[1].exists()


@pytest.mark.asyncio
async def test_result_aggregator_delivers_existing_artifact_named_in_task_output(monkeypatch, tmp_path) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    pdf_path = artifact_root / "nyc_after_dark_dining_nightlife.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%aggregator delivery\n")

    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)
    group = TaskGroup(
        group_id="g-pdf",
        conversation_id="telegram:123",
        original_message="creer une brochure",
        tasks=[
            task(
                "write",
                TaskStatus.COMPLETE,
                result=TaskResult(
                    "write",
                    "success",
                    output=(
                        "Created and verified the stylish promotional PDF.\n\n"
                        "Saved file:\n"
                        "`nyc_after_dark_dining_nightlife.pdf`"
                    ),
                ),
            ),
        ],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["write"]},
    )
    group.tasks[0].allowed_tools = ["file_write"]
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-pdf" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="write", group_id="g-pdf", kind="task_complete"))

    assert (
        "telegram:123",
        str(pdf_path),
        {"group_id": "g-pdf", "is_artifact": True},
    ) in deliveries


@pytest.mark.asyncio
async def test_result_aggregator_prefers_summary_named_artifact_over_intermediate_context(monkeypatch, tmp_path) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    final_pdf = artifact_root / "new_york_after_dark_promotional_brochure.pdf"
    intermediate_pdf = artifact_root / "ny_after_dark_brochure.pdf"
    draft_pdf = artifact_root / "new-york-after-dark-brochure.pdf"
    for path in (final_pdf, intermediate_pdf, draft_pdf):
        path.write_bytes(b"%PDF-1.4\n%candidate\n")

    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    class SummaryModel:
        def create(self, **_kwargs):  # noqa: ANN003
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Done — generated a stylish brochure PDF. The finished file is saved at "
                            "new_york_after_dark_promotional_brochure.pdf."
                        ),
                    }
                ]
            }

    group = TaskGroup(
        group_id="g-summary-pdf",
        conversation_id="telegram:123",
        original_message="creer une brochure",
        tasks=[
            task(
                "research",
                TaskStatus.COMPLETE,
                result=TaskResult("research", "success", output="Research complete."),
            ),
            task(
                "draft",
                TaskStatus.COMPLETE,
                result=TaskResult(
                    "draft",
                    "success",
                    output="Drafted copy.",
                    context_out="Draft explored `new-york-after-dark-brochure.pdf` and `ny_after_dark_brochure.pdf`.",
                ),
            ),
            task(
                "pdf",
                TaskStatus.COMPLETE,
                result=TaskResult(
                    "pdf",
                    "success",
                    output="Created final PDF: `new_york_after_dark_promotional_brochure.pdf`",
                ),
            ),
        ],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["research", "draft", "pdf"]},
    )
    for item in group.tasks:
        if item.task_id == "pdf":
            item.allowed_tools = ["pdf_create"]
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-summary-pdf" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        model_client=SummaryModel(),
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="pdf", group_id="g-summary-pdf", kind="task_complete"))

    artifact_deliveries = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert artifact_deliveries == [str(final_pdf)]


@pytest.mark.asyncio
async def test_result_aggregator_falls_back_to_artifact_scoped_task_output(monkeypatch, tmp_path) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    final_pdf = artifact_root / "final-deliverable.pdf"
    scratch_pdf = artifact_root / "scratch-candidate.pdf"
    final_pdf.write_bytes(b"%PDF-1.4\n%final\n")
    scratch_pdf.write_bytes(b"%PDF-1.4\n%scratch\n")

    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    class SummaryModel:
        def create(self, **_kwargs):  # noqa: ANN003
            return {"content": [{"type": "text", "text": "Done — generated the requested file."}]}

    group = TaskGroup(
        group_id="g-artifact-scope",
        conversation_id="telegram:123",
        original_message="creer une brochure",
        tasks=[
            task(
                "research",
                TaskStatus.COMPLETE,
                result=TaskResult("research", "success", output="Research notes referenced scratch-candidate.pdf."),
            ),
            task(
                "pdf",
                TaskStatus.COMPLETE,
                result=TaskResult("pdf", "success", output="Created final-deliverable.pdf"),
            ),
        ],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["research", "pdf"]},
    )
    for item in group.tasks:
        if item.task_id == "pdf":
            item.allowed_tools = ["pdf_create"]
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-artifact-scope" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        model_client=SummaryModel(),
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="pdf", group_id="g-artifact-scope", kind="task_complete"))

    artifact_deliveries = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert artifact_deliveries == [str(final_pdf)]


@pytest.mark.asyncio
async def test_result_aggregator_does_not_deliver_unrequested_research_html(monkeypatch, tmp_path) -> None:
    import nullion.result_aggregator as result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    raw_html = artifact_root / "bbc_homepage_current.html"
    raw_html.write_text("<!doctype html><html><body>BBC</body></html>", encoding="utf-8")

    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    class SummaryModel:
        def create(self, **_kwargs):  # noqa: ANN003
            return {
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "BBC and CNN were compared. The BBC raw HTML artifact "
                            "was saved at artifacts/bbc_homepage_current.html."
                        ),
                    }
                ]
            }

    group = TaskGroup(
        group_id="g-research-html",
        conversation_id="telegram:123",
        original_message="compare results from cnn.com and bbc.com home page",
        tasks=[
            task(
                "bbc",
                TaskStatus.COMPLETE,
                result=TaskResult(
                    "bbc",
                    "success",
                    output="Raw HTML was saved at artifacts/bbc_homepage_current.html.",
                ),
            ),
            task("compare", TaskStatus.COMPLETE, result=TaskResult("compare", "success", output="Compared pages.")),
        ],
        planner_metadata={"disposition": "parallel_mission", "tasks": ["bbc", "compare"]},
    )
    group.tasks[0].allowed_tools = ["terminal_exec"]
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-research-html" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        model_client=SummaryModel(),
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="compare", group_id="g-research-html", kind="task_complete"))

    artifact_deliveries = [text for _, text, kwargs in deliveries if kwargs.get("is_artifact")]
    assert artifact_deliveries == []
    final_texts = [text for _, text, kwargs in deliveries if not kwargs.get("is_status") and not kwargs.get("is_artifact")]
    assert final_texts == [
        "📊 **PLANNER RESULT**\n\nFor: compare results from cnn.com and bbc.com home page\n\n"
        "BBC and CNN were compared. The BBC raw HTML artifact was saved at artifacts/bbc_homepage_current.html."
    ]


def test_result_aggregator_final_summary_names_original_request() -> None:
    from nullion.result_aggregator import _summary_with_original_request_context

    group = TaskGroup(
        group_id="g1",
        conversation_id="telegram:1",
        original_message="can you fetch news from cbs and attach report.txt?",
        tasks=[],
    )

    assert _summary_with_original_request_context(group, "The text file failed.") == (
        'Result for "can you fetch news from cbs and attach report.txt?":\n'
        "The text file failed."
    )


def test_result_aggregator_final_summary_does_not_echo_scheduled_task_prompt() -> None:
    from nullion.result_aggregator import _summary_with_original_request_context

    group = TaskGroup(
        group_id="g-cron",
        conversation_id="telegram:1",
        original_message=(
            "[Scheduled task: Nullion social + GitHub traffic monitor] "
            "Run the Nullion public awareness and GitHub audience metrics monitor."
        ),
        tasks=[],
    )

    assert _summary_with_original_request_context(group, "No report artifact was produced.") == (
        "No report artifact was produced."
    )


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
        "For: audit openclaw config\n"
        "→ Working on 2 tasks:\n"
        "  ◐ Task find\n"
        "  ☐ Task audit"
    )
    assert deliveries[-1][2]["status_kind"] == "task_summary"


@pytest.mark.asyncio
async def test_result_aggregator_delivers_failure_summary_when_task_feed_visible() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    tasks = [
        task("fetch", TaskStatus.FAILED, result=TaskResult("fetch", "failure", error="fetch failed")),
        task("pdf", TaskStatus.FAILED, result=TaskResult("pdf", "failure", error="pdf failed")),
    ]
    group = TaskGroup(
        group_id="g-failed",
        conversation_id="telegram:123",
        original_message="make a report",
        tasks=tasks,
        planner_metadata={"disposition": "sequential_mission", "tasks": ["fetch", "pdf"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-failed" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="fetch", group_id="g-failed", kind="task_failed", message="fetch failed"))

    assert any("✕ Task fetch" in text and "✕ Task pdf" in text for _, text, kwargs in deliveries if kwargs.get("is_status"))
    assert (
        "telegram:123",
        "📊 **PLANNER RESULT**\n\nFor: make a report\n\n"
        "Completed 0/2 task(s). 2 failed. Failure reasons: "
        "Task fetch: fetch failed; Task pdf: pdf failed. No report was published.",
        {"group_id": "g-failed"},
    ) in deliveries


@pytest.mark.asyncio
async def test_result_aggregator_keeps_failure_summary_when_task_feed_hidden() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    failed_task = task("fetch", TaskStatus.FAILED, result=TaskResult("fetch", "failure", error="fetch failed"))
    group = TaskGroup(
        group_id="g-hidden",
        conversation_id="telegram:123",
        original_message="make a report",
        tasks=[failed_task],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["fetch"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-hidden" else None,
        get_task=lambda task_id: failed_task if task_id == "fetch" else None,
    )

    def deliver(conversation_id: str, text: str, **kwargs) -> bool | None:
        deliveries.append((conversation_id, text, kwargs))
        if kwargs.get("is_status"):
            return False
        return None

    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="fetch", group_id="g-hidden", kind="task_failed", message="fetch failed"))

    assert any(kwargs.get("is_status") for _, _, kwargs in deliveries)
    assert (
        "telegram:123",
        "📊 **PLANNER RESULT**\n\nFor: make a report\n\n"
        "Completed 0/1 task(s). 1 failed. Failure reasons: Task fetch: fetch failed. No report was published.",
        {"group_id": "g-hidden"},
    ) in deliveries


@pytest.mark.asyncio
async def test_result_aggregator_preserves_failure_context_when_requested_artifact_is_missing() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    failed_task = task("fetch", TaskStatus.FAILED, result=TaskResult("fetch", "failure", error="source lookup failed"))
    group = TaskGroup(
        group_id="g-missing-artifact",
        conversation_id="telegram:123",
        original_message="create report.html",
        tasks=[failed_task],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["fetch"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-missing-artifact" else None,
        get_task=lambda task_id: failed_task if task_id == "fetch" else None,
    )

    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="a",
            task_id="fetch",
            group_id="g-missing-artifact",
            kind="task_failed",
            message="source lookup failed",
        )
    )

    final_messages = [
        text
        for conversation_id, text, kwargs in deliveries
        if conversation_id == "telegram:123" and kwargs == {"group_id": "g-missing-artifact"}
    ]
    assert final_messages
    final_text = final_messages[-1]
    assert "Failure reasons: Task fetch: source lookup failed." in final_text
    assert "I could not attach the requested .html file because no verified .html artifact was produced for this run." in final_text


@pytest.mark.asyncio
async def test_result_aggregator_keeps_failed_source_and_verifier_rows_when_a_file_exists(tmp_path) -> None:
    deliveries: list[tuple[str, str, dict]] = []
    workbook = tmp_path / "delivered_workbook.xlsx"
    workbook.write_bytes(b"PK\x03\x04 workbook")
    collect = task(
        "collect",
        TaskStatus.FAILED,
        result=TaskResult("collect", "failure", error="Source data was partial."),
    )
    collect.title = "Collect product data"
    collect.allowed_tools = ["browser_extract_items"]
    create = task(
        "create",
        TaskStatus.COMPLETE,
        result=TaskResult("create", "success", output="Created workbook.", artifacts=[str(workbook)]),
    )
    create.title = "Create Excel spreadsheet"
    create.allowed_tools = ["spreadsheet_create"]
    create.metadata["requires_artifact_delivery"] = True
    create.metadata["required_artifact_kind"] = "xlsx"
    verify = task(
        "verify",
        TaskStatus.FAILED,
        result=TaskResult("verify", "failure", error="Dependency failed: collect"),
    )
    verify.title = "Verify artifact"
    verify.dependencies = ["create"]
    verify.metadata["artifact_role"] = "verify"
    verify.metadata["requires_artifact_delivery"] = True
    verify.metadata["required_artifact_kind"] = "xlsx"
    receipt = task(
        "receipt",
        TaskStatus.FAILED,
        result=TaskResult("receipt", "failure", error="Dependency failed: verify"),
    )
    receipt.title = "Deliver receipt"
    receipt.dependencies = ["verify"]
    receipt.metadata["artifact_role"] = "deliver_receipt"
    receipt.metadata["requires_artifact_delivery"] = True
    receipt.metadata["required_artifact_kind"] = "xlsx"
    group = TaskGroup(
        group_id="g-delivered-card",
        conversation_id="telegram:123",
        original_message="Create an xlsx workbook from the source data.",
        tasks=[collect, create, verify, receipt],
        planner_metadata={
            "disposition": "sequential_mission",
            "tasks": ["collect", "create", "verify", "receipt"],
        },
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-delivered-card" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="a",
            task_id="receipt",
            group_id="g-delivered-card",
            kind="task_failed",
            message="Dependency failed: verify",
        )
    )

    assert not any(kw.get("is_artifact") for _,_,kw in deliveries)
    status_messages = [text for _, text, kwargs in deliveries if kwargs.get("status_kind") == "task_summary"]
    assert status_messages
    final_status = status_messages[-1]
    assert "✕ Collect product data" in final_status
    assert "☑ Create Excel spreadsheet" in final_status
    assert "✕ Verify artifact" in final_status
    assert "✕ Deliver receipt" in final_status
    assert any("No report was published" in text for _,text,kw in deliveries if not kw.get("is_status"))


@pytest.mark.asyncio
async def test_result_aggregator_hides_recoverable_artifact_x_while_producer_runs() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    collect = task(
        "collect",
        TaskStatus.FAILED,
        result=TaskResult("collect", "failure", error="Only partial source data was verified."),
    )
    collect.title = "Collect product links"
    collect.allowed_tools = ["browser_extract_items"]
    extract = task(
        "extract",
        TaskStatus.FAILED,
        result=TaskResult("extract", "failure", error="Dependency failed: collect"),
    )
    extract.title = "Extract product data"
    extract.allowed_tools = ["browser_run_js"]
    create = task("create", TaskStatus.RUNNING)
    create.title = "Create Excel file"
    create.allowed_tools = ["spreadsheet_create", "browser_image_collect"]
    create.metadata["requires_artifact_delivery"] = True
    create.metadata["required_artifact_kind"] = "xlsx"
    verify = task("verify", TaskStatus.FAILED, result=TaskResult("verify", "failure", error="Dependency failed"))
    verify.title = "Verify artifact"
    verify.dependencies = ["create"]
    verify.metadata["artifact_role"] = "verify"
    verify.metadata["requires_artifact_delivery"] = True
    receipt = task("receipt", TaskStatus.FAILED, result=TaskResult("receipt", "failure", error="Dependency failed"))
    receipt.title = "Deliver receipt"
    receipt.dependencies = ["verify"]
    receipt.metadata["artifact_role"] = "deliver_receipt"
    receipt.metadata["requires_artifact_delivery"] = True
    group = TaskGroup(
        group_id="g-active-artifact",
        conversation_id="telegram:123",
        original_message="Create an xlsx workbook from collected product data.",
        tasks=[collect, extract, create, verify, receipt],
        planner_metadata={
            "disposition": "sequential_mission",
            "tasks": ["collect", "extract", "create", "verify", "receipt"],
        },
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-active-artifact" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="a",
            task_id="collect",
            group_id="g-active-artifact",
            kind="task_failed",
            message="Only partial source data was verified.",
        )
    )

    status_messages = [text for _, text, kwargs in deliveries if kwargs.get("status_kind") == "task_summary"]
    assert status_messages
    active_status = status_messages[-1]
    assert "☑ Collect product links" in active_status
    assert "☑ Extract product data" in active_status
    assert "◐ Create Excel file" in active_status
    assert "☐ Verify artifact" in active_status
    assert "☐ Deliver receipt" in active_status
    assert "✕" not in active_status


@pytest.mark.asyncio
async def test_deep_agent_fallback_creates_spreadsheet_from_browser_items(tmp_path, monkeypatch) -> None:
    from nullion import deep_agent_runner
    from nullion.tools import ToolResult

    output_path = tmp_path / "collected_items.xlsx"
    local_image = tmp_path / "product.png"
    local_image.write_bytes(b"\x89PNG\r\n\x1a\nfake")
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeToolRegistry:
        def invoke(self, invocation):  # noqa: ANN001
            calls.append((invocation.tool_name, dict(invocation.arguments)))
            if invocation.tool_name == "browser_image_collect":
                return ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "image_paths": [str(local_image)],
                        "artifact_paths": [str(local_image)],
                        "images": [
                            {
                                "source_url": "https://example.test/product.png",
                                "final_url": "https://example.test/product.png",
                                "local_path": str(local_image),
                            }
                        ],
                    },
                )
            if invocation.tool_name == "spreadsheet_create":
                with zipfile.ZipFile(output_path, "w") as workbook:
                    workbook.writestr(
                        "xl/worksheets/sheet1.xml",
                        "<worksheet><sheetData><row r=\"2\"><c r=\"A2\"><v>1</v></c></row></sheetData></worksheet>",
                    )
                    workbook.writestr("xl/media/image1.png", b"\x89PNG\r\n\x1a\n" + b"0" * 1024)
                return ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "path": str(output_path),
                        "artifact_path": str(output_path),
                        "artifact_paths": [str(output_path)],
                        "rows": len(invocation.arguments["rows"]),
                        "embedded_images": [str(local_image)],
                    },
                )
            raise AssertionError(invocation.tool_name)

    monkeypatch.setattr(
        deep_agent_runner,
        "artifact_path_for_generated_workspace_file",
        lambda **_kwargs: output_path,
    )
    monkeypatch.setattr(deep_agent_runner, "artifact_root_for_principal", lambda _principal_id: tmp_path)

    task_config = SimpleNamespace(
        task=SimpleNamespace(
            task_id="task-sheet",
            group_id="group-sheet",
            principal_id="telegram:123",
            title="Create Excel file",
            description="Create a spreadsheet from collected product data.",
            allowed_tools=["browser_image_collect", "spreadsheet_create"],
            context_key_in=None,
            context_key_out=None,
            metadata={"requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
        ),
        agent_id="agent-sheet",
        context_in=None,
    )
    tool_results = [
        ToolResult(
            "items",
            "browser_extract_items",
            "completed",
            {
                "items": [
                    {
                        "title": "Example Pendant",
                        "url": "https://example.test/products/pendant",
                        "image_url": "https://example.test/product.png",
                        "price_text": "$120",
                        "compact_text": "Example Pendant $120",
                    }
                ]
            },
        )
    ]

    result = await deep_agent_runner._artifact_task_fallback_from_tool_evidence(
        task_config,
        tool_registry=FakeToolRegistry(),
        tool_results=tool_results,
        progress_queue=asyncio.Queue(),
    )

    assert result is not None
    assert result.status == "success"
    assert result.artifacts == [str(output_path)]
    assert calls[0][0] == "browser_image_collect"
    assert calls[-1][0] == "spreadsheet_create"
    spreadsheet_rows = calls[-1][1]["rows"]
    assert isinstance(spreadsheet_rows, list)
    assert spreadsheet_rows[0]["image_path"] == str(local_image)
    assert "https://example.test/product.png" not in str(spreadsheet_rows[0])


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
    assert any("Failure reasons: Task t1: bad" in text and "No report was published" in text for _,text,_ in deliveries)

    await aggregator._handle(
        ProgressUpdate(
            agent_id="a",
            task_id="t1",
            group_id="g1",
            kind="approval_needed",
            message="Approval required before continuing.",
        )
    )
    assert any(
        text == "Approval required before continuing."
        and kwargs == {"is_status": True, "group_id": "g1", "status_kind": "approval_needed"}
        for _, text, kwargs in deliveries
    )

    deliveries.clear()
    group2 = TaskGroup(group_id="g2", conversation_id="c2", original_message="Cancel", tasks=[task("t2", TaskStatus.CANCELLED)])
    registry.get_group = lambda group_id: group2 if group_id == "g2" else None
    registry.get_task = lambda task_id: group2.tasks[0]
    no_model = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)
    await no_model._handle(ProgressUpdate(agent_id="a", task_id="t2", group_id="g2", kind="task_cancelled"))
    assert any("Completed 0/1 task(s)." in text for _, text, _ in deliveries)


@pytest.mark.asyncio
async def test_result_aggregator_dedupes_replayed_input_needed_question() -> None:
    deliveries: list[tuple[str, str, dict]] = []

    def deliver(conversation_id: str, text: str, **kwargs) -> None:
        deliveries.append((conversation_id, text, kwargs))

    waiting_task = task("t1", TaskStatus.WAITING_INPUT, result=TaskResult("t1", "partial", output="Waiting"))
    group = TaskGroup(group_id="g1", conversation_id="c1", original_message="Do work", tasks=[waiting_task])
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g1" else None,
        get_task=lambda task_id: waiting_task if task_id == "t1" else None,
    )
    aggregator = ResultAggregator(deliver_fn=deliver, task_registry=registry, min_progress_interval_s=0)

    update = ProgressUpdate(
        agent_id="a",
        task_id="t1",
        group_id="g1",
        kind="input_needed",
        message="Need one decision",
    )
    await aggregator._handle(update)
    await aggregator._handle(update)
    await aggregator._handle(update)

    question_deliveries = [
        (text, kwargs)
        for _, text, kwargs in deliveries
        if kwargs.get("is_question")
    ]
    assert question_deliveries == [("? Need one decision", {"is_question": True})]


def test_result_aggregator_requested_attachment_detection_avoids_model_call() -> None:
    class FailingModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            raise AssertionError(f"unexpected model call: {kwargs}")

    group = TaskGroup(
        group_id="g-format",
        conversation_id="c-format",
        original_message="Create the requested artifact.pdf",
        tasks=[],
    )

    assert _requested_attachment_extension_for_group(group, model_client=FailingModelClient()) == ".pdf"


def test_result_aggregator_requested_attachment_detection_uses_structured_model_output() -> None:
    class FormatModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            return {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "select_attachment_format",
                        "input": {"extension": ".pdf", "confidence": 0.92},
                    }
                ]
            }

    group = TaskGroup(
        group_id="g-format-model",
        conversation_id="c-format",
        original_message="Create a professional report as an attachment.",
        tasks=[],
    )

    assert _requested_attachment_extension_for_group(group, model_client=FormatModelClient()) == ".pdf"


def test_result_aggregator_missing_artifact_merge_uses_task_status_not_prose() -> None:
    completed_group = TaskGroup(
        group_id="g-complete",
        conversation_id="c1",
        original_message="Create artifact.pdf",
        tasks=[
            task(
                "inspect",
                TaskStatus.COMPLETE,
                result=TaskResult(
                    "inspect",
                    "success",
                    output="An optional source said it could not expose one field.",
                ),
            )
        ],
    )
    failed_group = TaskGroup(
        group_id="g-failed",
        conversation_id="c1",
        original_message="Create artifact.pdf",
        tasks=[task("create", TaskStatus.FAILED, result=TaskResult("create", "error", output="failed"))],
    )

    completed = _merge_missing_requested_artifact_summary(
        completed_group,
        "An optional source said it could not expose one field.",
        requested_extension=".pdf",
    )
    failed = _merge_missing_requested_artifact_summary(
        failed_group,
        "The artifact producer stopped",
        requested_extension=".pdf",
    )

    assert completed == (
        "I could not attach the requested .pdf file because no verified .pdf artifact was produced for this run."
    )
    assert failed == (
        "The artifact producer stopped. Also, I could not attach the requested .pdf file because no verified "
        ".pdf artifact was produced for this run."
    )


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
async def test_result_aggregator_status_summaries_do_not_regress_terminal_rows() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    first = task("first", TaskStatus.COMPLETE)
    second = task("second", TaskStatus.RUNNING)
    group = TaskGroup(group_id="g1", conversation_id="c1", original_message="Do work", tasks=[first, second])
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g1" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="second", group_id="g1", kind="task_started"))
    second.status = TaskStatus.PENDING
    await aggregator._handle(ProgressUpdate(agent_id="a", task_id="second", group_id="g1", kind="task_started"))

    assert "☑ Task first" in deliveries[-1][1]
    assert "◐ Task second" in deliveries[-1][1]
    assert "☐ Task first" not in deliveries[-1][1]


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
    assert "→ Working on 1 task:" in deliveries[0][1]
    assert "For: audit openclaw config" in deliveries[0][1]


@pytest.mark.asyncio
async def test_dispatch_request_delivers_initial_task_card_from_structured_plan(monkeypatch) -> None:
    import nullion.task_decomposer as task_decomposer
    import nullion.warm_pool as warm_pool

    deliveries: list[tuple[str, str, dict]] = []
    planned_group = TaskGroup(
        group_id="g-card",
        conversation_id="telegram:123",
        original_message="Build a structured trip plan",
        tasks=[
            task("research", TaskStatus.WAITING_INPUT),
            task("write", TaskStatus.WAITING_INPUT),
        ],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["research", "write"]},
    )

    class FakeTaskDecomposer:
        def __init__(self, *, model_client) -> None:
            self.model_client = model_client

        def decompose(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return planned_group

    class FakeWarmPool:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

        async def start(self) -> None:
            return None

    monkeypatch.setattr(task_decomposer, "TaskDecomposer", FakeTaskDecomposer)
    monkeypatch.setattr(warm_pool, "WarmAgentPool", FakeWarmPool)

    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator.set_deliver_fn(
        lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs))
    )
    result = await orchestrator.dispatch_request(
        conversation_id="telegram:123",
        principal_id="operator",
        user_message="Build a structured trip plan",
        tool_registry=SimpleNamespace(list_tool_definitions=lambda: []),
        policy_store=SimpleNamespace(),
        approval_store=SimpleNamespace(),
        single_task_fast_path=False,
        dag_plan=SimpleNamespace(can_dispatch=True),
    )

    assert result.status_delivered is True
    assert deliveries == [
        (
            "telegram:123",
            result.acknowledgment,
            {"is_status": True, "group_id": "g-card", "status_kind": "task_summary"},
        )
    ]
    assert "Planner: Sequential Mission" in result.acknowledgment
    assert "For: Build a structured trip plan" in result.acknowledgment
    for pending in list(orchestrator._supervisor_tasks or ()):
        pending.cancel()
    if orchestrator._supervisor_tasks:
        await asyncio.gather(*list(orchestrator._supervisor_tasks), return_exceptions=True)


@pytest.mark.asyncio
async def test_dispatch_request_delivers_requested_single_task_card(monkeypatch) -> None:
    import nullion.warm_pool as warm_pool
    from nullion.task_decomposer import DagPlan, DecomposedTask

    deliveries: list[tuple[str, str, dict]] = []

    class FakeWarmPool:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

        async def start(self) -> None:
            return None

    async def no_op_run_task(*args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(warm_pool, "WarmAgentPool", FakeWarmPool)

    orchestrator = AgentOrchestrator(model_client=None)
    monkeypatch.setattr(orchestrator, "_run_task", no_op_run_task)
    orchestrator.set_deliver_fn(
        lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs))
    )

    result = await orchestrator.dispatch_request(
        conversation_id="telegram:123",
        principal_id="operator",
        user_message="Build the requested planner PDF",
        tool_registry=SimpleNamespace(list_tool_definitions=lambda: []),
        policy_store=SimpleNamespace(),
        approval_store=SimpleNamespace(),
        single_task_fast_path=False,
        dag_plan=DagPlan(
            disposition="single_turn",
            tasks=[
                DecomposedTask(
                    title="Build requested PDF",
                    description="Build the requested planner PDF.",
                    tool_scope=[],
                    priority=TaskPriority.NORMAL,
                    dep_indices=[],
                    context_key_in=None,
                    context_key_out=None,
                    required_inputs=[],
                    can_start=True,
                )
            ],
        ),
    )

    assert result.dispatched is True
    assert result.is_single_task is True
    assert result.task_count == 1
    assert "Planner: Single Turn" in result.acknowledgment
    assert deliveries == [
        (
            "telegram:123",
            result.acknowledgment,
            {"is_status": True, "group_id": result.group_id, "status_kind": "task_summary"},
        )
    ]
    for pending in list(orchestrator._supervisor_tasks or ()):
        pending.cancel()
    if orchestrator._supervisor_tasks:
        await asyncio.gather(*list(orchestrator._supervisor_tasks), return_exceptions=True)


@pytest.mark.asyncio
async def test_mini_agent_task_runner_times_out_and_emits_terminal_failure() -> None:
    registry = TaskRegistry()
    queued = task("slow", TaskStatus.QUEUED)
    queued.timeout_s = 0.01
    group = TaskGroup(
        group_id="g1",
        conversation_id="telegram:123",
        original_message="Research and report",
        tasks=[queued],
        planner_metadata={"disposition": "single_task", "tasks": ["slow"]},
    )
    await registry.add_group(group)

    class FakePool:
        async def acquire(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(agent_id="agent-1")

        def release(self, agent) -> None:  # noqa: ANN001
            return None

    class SlowRunner:
        async def run(self, *args, **kwargs):  # noqa: ANN002, ANN003
            await asyncio.sleep(1)
            return TaskResult("slow", "success", output="too late")

    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator._pool = FakePool()
    orchestrator._task_registry = registry
    orchestrator._context_bus = SimpleNamespace(get=lambda *args, **kwargs: None, clear_group=lambda group_id: None)
    orchestrator._progress_queue = asyncio.Queue()

    await orchestrator._run_task(
        queued,
        runner=SlowRunner(),
        group=group,
        tool_registry=SimpleNamespace(),
        policy_store=SimpleNamespace(),
        approval_store=SimpleNamespace(),
    )

    updated = registry.get_task("slow")
    assert updated is not None
    assert updated.status is TaskStatus.FAILED
    assert updated.result is not None
    assert updated.result.error == "Timed out after 0.15s without reaching a terminal state."
    progress = [await orchestrator._progress_queue.get(), await orchestrator._progress_queue.get()]
    assert [item.kind for item in progress] == ["task_started", "task_failed"]
    assert progress[-1].message == updated.result.error


@pytest.mark.asyncio
async def test_scheduled_background_task_runner_is_not_cut_off_by_interactive_timeout() -> None:
    registry = TaskRegistry()
    queued = task("scheduled", TaskStatus.QUEUED)
    queued.timeout_s = 0.01
    queued.metadata["scheduled_task_run"] = True
    group = TaskGroup(
        group_id="g-scheduled",
        conversation_id="telegram:123",
        original_message="Run scheduled research",
        tasks=[queued],
        planner_metadata={"disposition": "single_task", "tasks": ["scheduled"]},
    )
    await registry.add_group(group)

    class FakePool:
        async def acquire(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(agent_id="agent-1")

        def release(self, agent) -> None:  # noqa: ANN001
            return None

    class SlowSuccessfulRunner:
        async def run(self, *args, **kwargs):  # noqa: ANN002, ANN003
            await asyncio.sleep(0.03)
            return TaskResult("scheduled", "success", output="finished after interactive timeout")

    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator._pool = FakePool()
    orchestrator._task_registry = registry
    orchestrator._context_bus = SimpleNamespace(get=lambda *args, **kwargs: None, clear_group=lambda group_id: None)
    orchestrator._progress_queue = asyncio.Queue()

    await orchestrator._run_task(
        queued,
        runner=SlowSuccessfulRunner(),
        group=group,
        tool_registry=SimpleNamespace(),
        policy_store=SimpleNamespace(),
        approval_store=SimpleNamespace(),
    )

    updated = registry.get_task("scheduled")
    assert updated is not None
    assert updated.status is TaskStatus.COMPLETE
    assert updated.result is not None
    assert updated.result.output == "finished after interactive timeout"
    progress = [await orchestrator._progress_queue.get(), await orchestrator._progress_queue.get()]
    assert [item.kind for item in progress] == ["task_started", "task_complete"]


@pytest.mark.asyncio
async def test_supervisor_does_not_fail_scheduled_background_task_at_interactive_timeout(monkeypatch) -> None:
    deliveries: list[tuple[str, str, dict]] = []
    running = task("scheduled", TaskStatus.RUNNING)
    running.timeout_s = 0.01
    running.started_at = datetime.now(UTC) - timedelta(seconds=5)
    running.metadata["scheduled_task_run"] = True
    group = TaskGroup(
        group_id="g-scheduled",
        conversation_id="telegram:123",
        original_message="Run scheduled research",
        tasks=[running],
        planner_metadata={"disposition": "single_task", "tasks": ["scheduled"]},
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
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_GRACE_SECONDS", "0")

    await orchestrator._supervise_dispatch_group("g-scheduled", policy_store=None)

    assert running.status is TaskStatus.COMPLETE
    assert running.result is None
    assert deliveries
    assert all("Timed out" not in text for _, text, _ in deliveries)


@pytest.mark.asyncio
async def test_supervisor_finalizes_terminal_group_when_progress_aggregator_missed_update() -> None:
    deliveries: list[tuple[str, str, dict]] = []
    completed = task(
        "scheduled",
        TaskStatus.COMPLETE,
        result=TaskResult("scheduled", "success", output="Scheduled report finished."),
    )
    group = TaskGroup(
        group_id="g-terminal-supervisor",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Monitor] Build .html report",
        tasks=[completed],
        planner_metadata={"disposition": "single_task", "tasks": ["scheduled"]},
    )

    class Registry:
        def get_group(self, group_id: str):
            return group if group_id == "g-terminal-supervisor" else None

        def get_task(self, task_id: str):
            return completed if task_id == "scheduled" else None

    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator._task_registry = Registry()
    orchestrator._deliver_fn = lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs))
    from nullion.result_aggregator import ResultAggregator

    orchestrator._result_aggregator = ResultAggregator(
        deliver_fn=orchestrator._deliver_fn,
        task_registry=orchestrator._task_registry,
        min_progress_interval_s=0,
    )

    await orchestrator._supervise_dispatch_group("g-terminal-supervisor", policy_store=None)

    assert any(kwargs == {"group_id": "g-terminal-supervisor"} for _, _, kwargs in deliveries)


@pytest.mark.asyncio
async def test_supervisor_finalizes_after_dependency_failure_marks_group_terminal(monkeypatch) -> None:
    deliveries: list[tuple[str, str, dict]] = []
    registry = TaskRegistry()
    failed = task(
        "research",
        TaskStatus.FAILED,
        result=TaskResult("research", "failure", error="Search could not be verified."),
    )
    failed.group_id = "g-terminal-deps"
    blocked = task("deliver", TaskStatus.BLOCKED)
    blocked.group_id = "g-terminal-deps"
    blocked.dependencies = ["research"]
    group = TaskGroup(
        group_id="g-terminal-deps",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Lodging] Build verified report",
        tasks=[failed, blocked],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["research", "deliver"]},
    )
    await registry.add_group(group)

    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator._task_registry = registry
    orchestrator._deliver_fn = lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs))
    orchestrator._supervisor_tasks = set()
    from nullion.result_aggregator import ResultAggregator

    orchestrator._result_aggregator = ResultAggregator(
        deliver_fn=orchestrator._deliver_fn,
        task_registry=registry,
        min_progress_interval_s=0,
    )
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.01")

    await orchestrator._supervise_dispatch_group("g-terminal-deps", policy_store=None)

    updated = registry.get_task("deliver")
    assert updated is not None
    assert updated.status is TaskStatus.FAILED
    assert updated.result is not None
    assert "Dependency failed: research" == updated.result.error
    assert any(kwargs == {"group_id": "g-terminal-deps"} for _, _, kwargs in deliveries)


@pytest.mark.asyncio
async def test_supervisor_transitively_fails_blocked_descendants(monkeypatch) -> None:
    deliveries: list[tuple[str, str, dict]] = []
    registry = TaskRegistry()
    failed = task(
        "search",
        TaskStatus.FAILED,
        result=TaskResult("search", "failure", error="Evidence did not validate search results."),
    )
    failed.group_id = "g-transitive-deps"
    filter_task = task("filter", TaskStatus.BLOCKED)
    filter_task.group_id = "g-transitive-deps"
    filter_task.dependencies = ["search"]
    report = task("report", TaskStatus.BLOCKED)
    report.group_id = "g-transitive-deps"
    report.dependencies = ["filter"]
    delivery = task("delivery", TaskStatus.PENDING)
    delivery.group_id = "g-transitive-deps"
    delivery.dependencies = ["report"]
    group = TaskGroup(
        group_id="g-transitive-deps",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Lodging] Build verified report",
        tasks=[failed, filter_task, report, delivery],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["search", "filter", "report", "delivery"]},
    )
    await registry.add_group(group)

    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator._task_registry = registry
    orchestrator._deliver_fn = lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs))
    orchestrator._supervisor_tasks = set()
    from nullion.result_aggregator import ResultAggregator

    orchestrator._result_aggregator = ResultAggregator(
        deliver_fn=orchestrator._deliver_fn,
        task_registry=registry,
        min_progress_interval_s=0,
    )
    monkeypatch.setenv("NULLION_MINI_AGENT_SUPERVISION_INTERVAL_SECONDS", "0.01")

    await orchestrator._supervise_dispatch_group("g-transitive-deps", policy_store=None)

    updated = registry.get_group("g-transitive-deps")
    assert updated is not None
    assert updated.all_terminal()
    assert registry.get_task("filter").result.error == "Dependency failed: search"
    assert registry.get_task("report").result.error == "Dependency failed: search"
    assert registry.get_task("delivery").result.error == "Dependency failed: search"
    final_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-transitive-deps"}]
    assert final_messages
    assert "Failure reasons:" in final_messages[-1]


@pytest.mark.asyncio
async def test_deep_agent_runner_does_not_cut_off_scheduled_background_task(monkeypatch) -> None:
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner

    async def slow_success(self, config, **kwargs):
        await asyncio.sleep(0.03)
        return TaskResult(config.task.task_id, "success", output="finished after scheduler timeout window")

    scheduled = task("scheduled-deepagent", TaskStatus.RUNNING)
    scheduled.metadata["scheduled_task_run"] = True
    config = MiniAgentConfig(
        agent_id="agent-scheduled",
        task=scheduled,
        timeout_s=0.01,
    )
    monkeypatch.setattr(DeepAgentMiniAgentRunner, "_run_inner", slow_success)

    result = await DeepAgentMiniAgentRunner().run(
        config,
        anthropic_client=None,
        tool_registry=SimpleNamespace(cleanup_scope=lambda scope_id: None),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(publish=lambda *args, **kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "success"
    assert result.output == "finished after scheduler timeout window"


@pytest.mark.asyncio
async def test_result_aggregator_delivers_verified_media_from_completed_non_leaf_task(tmp_path, monkeypatch) -> None:
    from nullion import result_aggregator

    deliveries: list[tuple[str, str, dict]] = []
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    report = artifact_root / "cron-report.html"
    report.write_text(
        "<html><body><h1>Verified cron report</h1><p>Enough content to pass artifact verification.</p></body></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(result_aggregator, "artifact_root_for_principal", lambda principal_id: artifact_root)

    report_task = task(
        "report",
        TaskStatus.COMPLETE,
        result=TaskResult(
            "report",
            "success",
            output=f"Created report.\n\nMEDIA artifacts/{report.name}",
        ),
    )
    summary_task = task(
        "summary",
        TaskStatus.FAILED,
        result=TaskResult("summary", "failure", error="Timed out before summary."),
    )
    summary_task.dependencies = ["report"]
    summary_task.metadata["artifact_role"] = "deliver_receipt"
    group = TaskGroup(
        group_id="g-cron-report",
        conversation_id="telegram:123",
        original_message="[Manual scheduled task run: Monitor] Build .html report",
        tasks=[report_task, summary_task],
        planner_metadata={"disposition": "sequential_mission", "tasks": ["report", "summary"]},
    )
    registry = SimpleNamespace(
        get_group=lambda group_id: group if group_id == "g-cron-report" else None,
        get_task=lambda task_id: next((item for item in group.tasks if item.task_id == task_id), None),
    )
    aggregator = ResultAggregator(
        deliver_fn=lambda conversation_id, text, **kwargs: deliveries.append((conversation_id, text, kwargs)),
        task_registry=registry,
        min_progress_interval_s=0,
    )

    await aggregator._handle(
        ProgressUpdate(
            agent_id="agent-summary",
            task_id="summary",
            group_id="g-cron-report",
            kind="task_failed",
            message="Timed out before summary.",
        )
    )

    assert ("telegram:123", str(report), {"group_id": "g-cron-report", "is_artifact": True}) in deliveries
    final_messages = [text for _, text, kwargs in deliveries if kwargs == {"group_id": "g-cron-report"}]
    assert final_messages
    assert all("MEDIA" not in message and str(report) not in message for message in final_messages)


def test_planner_summary_metadata_edge_cases() -> None:
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata=None)) == ""
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": ""})) == ""
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": "fallback", "valid": False})) == "Fallback to normal turn"
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": "clarify", "needs_clarification": True})) == "Needs clarification"
    assert _planner_summary_from_group(SimpleNamespace(planner_metadata={"disposition": "single_task", "tasks": []})) == "Single Task"

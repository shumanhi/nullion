from __future__ import annotations

from datetime import UTC, datetime

import pytest

from nullion import runtime
from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus
from nullion.missions import MissionChecklistItem, MissionContinuationPolicy, MissionStatus, MissionStep
from nullion.runtime_store import RuntimeStore


def test_runtime_mission_plan_step_checklist_and_run_mutations() -> None:
    store = RuntimeStore()
    mission = runtime.create_mission(
        store,
        owner=" workspace:one ",
        title=" Ship ",
        goal=" Increase coverage ",
        continuation_policy=MissionContinuationPolicy.AUTO_FINISH,
        mission_id="mission-1",
    )
    assert mission.status is MissionStatus.PENDING
    assert runtime.get_mission(store, "mission-1") is not None
    assert runtime.list_missions(store)

    planned = runtime.set_mission_plan(
        store,
        "mission-1",
        steps=[
            MissionStep(" step-1 ", " Plan ", " pending ", " plan ", mini_agent_run_ids=("run-1", " ", "run-1"), required_mini_agent_run_ids=("run-1",), notes=" notes "),
            MissionStep("step-2", "Approval", "pending", "approval"),
            MissionStep("step-3", "Ask user", "pending", "user_input"),
            MissionStep("step-4", "External", "pending", "external_wait"),
        ],
        completion_checklist=[MissionChecklistItem(" item-1 ", " Verify ", details=" done ")],
        active_step_id="step-1",
    )
    assert planned.steps[0].step_id == "step-1"
    assert planned.completion_checklist[0].details == "done"

    running = runtime.mark_mission_step_running(store, "mission-1", step_id="step-1", notes="Working")
    assert running.status is MissionStatus.RUNNING
    assert running.active_step_id == "step-1"

    with pytest.raises(PermissionError):
        runtime.mark_mission_step_running(store, "mission-1", step_id="step-1", actor="web")
    with pytest.raises(KeyError):
        runtime.mark_mission_step_completed(store, "mission-1", step_id="missing")

    updated_notes = runtime.update_mission_step_notes(store, "mission-1", step_id="step-1", notes=" refined ")
    assert updated_notes.steps[0].notes == "refined"

    linked = runtime.link_mini_agent_run_to_mission_step(
        store,
        "mission-1",
        step_id="step-1",
        mini_agent_run_id="run-2",
        required=True,
    )
    assert linked.steps[0].mini_agent_run_id == "run-2"
    linked = runtime.link_mini_agent_run_to_mission_step(
        store,
        "mission-1",
        step_id="step-1",
        mini_agent_run_id="run-3",
        required=True,
    )
    assert "run-3" in linked.steps[0].required_mini_agent_run_ids
    linked_again = runtime.link_mini_agent_run_to_mission_step(
        store,
        "mission-1",
        step_id="step-1",
        mini_agent_run_id="run-2",
        required=True,
    )
    assert linked_again.steps[0].mini_agent_run_id == "run-2"

    now = datetime(2026, 1, 1, tzinfo=UTC)
    store.add_mini_agent_run(MiniAgentRun("run-1", "capsule-1", "researcher", MiniAgentRunStatus.COMPLETED, now, "run one done"))
    store.add_mini_agent_run(MiniAgentRun("run-2", "capsule-1", "researcher", MiniAgentRunStatus.RUNNING, now, "run two working"))
    store.add_mini_agent_run(MiniAgentRun("run-3", "capsule-1", "researcher", MiniAgentRunStatus.COMPLETED, now, "run three done"))
    synced = runtime._update_mission_step_for_mini_agent_run(store, linked_again, run=store.get_mini_agent_run("run-2"))
    assert synced.steps[0].status == "running"
    store.add_mini_agent_run(MiniAgentRun("run-2", "capsule-1", "researcher", MiniAgentRunStatus.COMPLETED, now, "run two done"))
    synced = runtime._update_mission_step_for_mini_agent_run(store, synced, run=store.get_mini_agent_run("run-2"))
    assert synced.steps[0].status == "completed"

    completed = runtime.mark_mission_step_completed(store, "mission-1", step_id="step-1", notes="done")
    assert completed.status is MissionStatus.WAITING_APPROVAL
    assert completed.active_step_id == "step-2"

    blocked = runtime.mark_mission_step_blocked(store, "mission-1", step_id="step-4", notes="waiting on vendor")
    assert blocked.status is MissionStatus.BLOCKED
    assert blocked.blocked_reason == "waiting on vendor"

    failed = runtime.mark_mission_step_failed(store, "mission-1", step_id="step-4", notes="vendor failed")
    assert failed.status is MissionStatus.FAILED
    assert failed.result_summary == "vendor failed"

    mission2 = runtime.create_mission(store, owner="owner", title="Checklist", goal="done", mission_id="mission-2")
    runtime.set_mission_plan(
        store,
        mission2.mission_id,
        steps=[MissionStep("s1", "Step", "completed", "tool")],
        completion_checklist=[MissionChecklistItem("c1", "Check", required=True, satisfied=False)],
    )
    verifying = runtime.advance_mission(store, mission2.mission_id)
    assert verifying.status is MissionStatus.VERIFYING
    done = runtime.set_mission_checklist_item_satisfied(store, mission2.mission_id, item_id="c1", satisfied=True, details="ok")
    assert done.status is MissionStatus.COMPLETED
    assert done.terminal_reason.value == "completed"

    mission3 = runtime.create_mission(store, owner="owner", title="Waiting", goal="done", mission_id="mission-3")
    runtime.set_mission_plan(store, mission3.mission_id, steps=[MissionStep("s1", "Ask", "pending", "user_input")], active_step_id="s1")
    assert runtime.advance_mission(store, mission3.mission_id).status is MissionStatus.WAITING_USER


def test_runtime_mission_validation_and_terminal_guards() -> None:
    store = RuntimeStore()
    with pytest.raises(ValueError):
        runtime.create_mission(store, owner=" ", title="Title", goal="Goal")
    mission = runtime.create_mission(store, owner="owner", title="Title", goal="Goal", mission_id="mission-x")
    with pytest.raises(ValueError):
        runtime.set_mission_plan(store, mission.mission_id, steps=[MissionStep("", "Title", "pending", "tool")])
    with pytest.raises(ValueError):
        runtime.set_mission_plan(
            store,
            mission.mission_id,
            steps=[MissionStep("s1", "Title", "pending", "tool"), MissionStep("s1", "Other", "pending", "tool")],
        )
    with pytest.raises(ValueError):
        runtime.set_mission_plan(store, mission.mission_id, steps=[MissionStep("s1", "Title", "bad", "tool")])
    with pytest.raises(ValueError):
        runtime.set_mission_plan(store, mission.mission_id, steps=[MissionStep("s1", "Title", "pending", "bad")])
    with pytest.raises(ValueError):
        runtime.set_mission_plan(
            store,
            mission.mission_id,
            steps=[MissionStep("s1", "Title", "pending", "tool")],
            active_step_id="missing",
        )
    with pytest.raises(ValueError):
        runtime.set_mission_plan(
            store,
            mission.mission_id,
            steps=[MissionStep("s1", "Title", "pending", "tool")],
            completion_checklist=[MissionChecklistItem("", "Check")],
        )

    runtime.set_mission_plan(store, mission.mission_id, steps=[MissionStep("s1", "Title", "pending", "tool")])
    terminal = runtime.mark_mission_completed(store, mission.mission_id, result_summary="done")
    assert terminal.steps[0].status == "completed"
    assert runtime.advance_mission(store, mission.mission_id) is terminal
    with pytest.raises(ValueError):
        runtime.mark_mission_step_running(store, mission.mission_id, step_id="s1")
    with pytest.raises(ValueError):
        runtime.set_mission_checklist_item_satisfied(store, mission.mission_id, item_id="c1", satisfied=True)

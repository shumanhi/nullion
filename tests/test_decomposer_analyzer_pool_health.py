from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from nullion import conversation_analyzer
from nullion.conversation_analyzer import (
    ConversationAnalysis,
    analyze_all_recent,
    analyze_conversation,
    cache_proposals,
    clear_cached_proposals,
    get_cached_proposals,
)
from nullion.health_monitor import HealthMonitor, ProbeResult
from nullion.task_decomposer import (
    DagPlan,
    DecomposedTask,
    TaskDecomposer,
    _has_cycle,
    _parse_decomposed_tasks,
    _parse_dag_plan,
    _response_text,
    _validate_dag_plan,
)
from nullion.task_queue import TaskPriority, TaskStatus
from nullion.task_queue import TaskResult
from nullion.warm_pool import EVICT_AFTER_TASKS, AgentState, SharedResources, WarmAgentPool, get_agent_client


class ModelClient:
    def __init__(self, payload, *, reject_system=False) -> None:
        self.payload = payload
        self.reject_system = reject_system
        self.calls = []

    def create(self, **kwargs):
        if self.reject_system and "system" in kwargs:
            raise TypeError("system unsupported")
        self.calls.append(kwargs)
        return self.payload


def test_task_decomposer_dispatches_valid_parallel_and_sequential_groups() -> None:
    client = ModelClient(
        {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "disposition": "sequential_mission",
                            "needs_clarification": False,
                            "tasks": [
                                {
                                    "title": "Fetch page",
                                    "description": "Fetch page",
                                    "tool_scope": ["web_fetch"],
                                    "priority": "high",
                                    "dependencies": [],
                                    "context_key_out": "page",
                                },
                                {
                                    "title": "Summarize",
                                    "description": "Summarize page",
                                    "tool_scope": [],
                                    "priority": "not-real",
                                    "dependencies": [0],
                                    "context_key_in": "page",
                                },
                            ],
                        }
                    ),
                }
            ]
        }
    )

    group = TaskDecomposer(client).decompose(
        "fetch then summarize",
        group_id="g1",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["web_fetch"],
    )

    assert [task.status for task in group.tasks] == [TaskStatus.QUEUED, TaskStatus.BLOCKED]
    assert group.tasks[0].priority is TaskPriority.HIGH
    assert group.tasks[1].priority is TaskPriority.NORMAL
    assert group.tasks[1].dependencies == [group.tasks[0].task_id]
    assert group.planner_metadata["dispatchable"] is True
    assert group.tasks[0].deep_agent_skills == ["/skills/nullion/"]
    assert group.tasks[0].deep_agent_subagents[0]["name"] == "research_agent"


def test_task_decomposer_strips_web_composer_mode_prefix_before_model_call() -> None:
    client = ModelClient(
        {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "disposition": "single_turn",
                            "needs_clarification": False,
                            "tasks": [
                                {
                                    "title": "Weather for 10026",
                                    "description": "Check weather for 10026",
                                    "tool_scope": [],
                                    "priority": "normal",
                                    "dependencies": [],
                                }
                            ],
                        }
                    ),
                }
            ]
        }
    )

    group = TaskDecomposer(client).decompose(
        "Mode: Diagnose. Investigate the system, explain evidence, and recommend fixes. 10026",
        group_id="g-mode",
        conversation_id="c1",
        principal_id="p1",
        available_tools=[],
    )

    prompt = client.calls[0]["messages"][-1]["content"][0]["text"]
    assert "User request: 10026" in prompt
    assert "Mode: Diagnose" not in prompt
    assert group.original_message == "10026"


@pytest.mark.parametrize(
    ("message", "expected_request"),
    [
        (
            "Mode: Build. Treat this as an implementation mission. search docs today and email summary now",
            "search docs today and email summary now",
        ),
        (
            "Mode: Remember. Extract durable preferences or project context if appropriate. my timezone is Eastern",
            "my timezone is Eastern",
        ),
    ],
)
def test_task_decomposer_strips_each_web_composer_mode_prefix(message: str, expected_request: str) -> None:
    client = ModelClient(
        {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "disposition": "single_turn",
                            "needs_clarification": False,
                            "tasks": [
                                {
                                    "title": "One task",
                                    "description": "One task",
                                    "tool_scope": [],
                                    "priority": "normal",
                                    "dependencies": [],
                                }
                            ],
                        }
                    ),
                }
            ]
        }
    )

    group = TaskDecomposer(client).decompose(
        message,
        group_id="g-mode",
        conversation_id="c1",
        principal_id="p1",
        available_tools=[],
    )

    prompt = client.calls[0]["messages"][-1]["content"][0]["text"]
    assert f"User request: {expected_request}" in prompt
    assert group.original_message == expected_request


def test_task_decomposer_fallback_uses_stripped_message_for_single_task() -> None:
    invalid_client = ModelClient({"content": [{"type": "text", "text": '{"disposition":"parallel_mission","tasks":[]}'}]})

    group = TaskDecomposer(invalid_client).decompose(
        "Mode: Diagnose. Investigate the system, explain evidence, and recommend fixes. 10026",
        group_id="g-fallback",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["file_read"],
    )

    assert group.original_message == "10026"
    assert group.tasks[0].title == "10026"
    assert group.tasks[0].description == "10026"


def test_task_decomposer_falls_back_for_invalid_or_clarifying_plans() -> None:
    invalid_client = ModelClient({"content": [{"type": "text", "text": '{"disposition":"parallel_mission","tasks":[]}'}]})
    group = TaskDecomposer(invalid_client).decompose(
        "one thing",
        group_id="g2",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["file_read"],
    )
    assert len(group.tasks) == 1
    assert group.tasks[0].allowed_tools == ["file_read"]
    assert group.planner_metadata["valid"] is False

    clarifying = _validate_dag_plan(
        DagPlan(
            disposition="clarification",
            tasks=[DecomposedTask("Ask", "Ask", [], TaskPriority.NORMAL, [], None, None, ["email"], False)],
            needs_clarification=True,
        ),
        available_tools=[],
    )
    assert clarifying.needs_clarification is True
    assert "clarification plan lacks clarification_question" in clarifying.validation_errors


def test_task_decomposer_parsing_and_validation_edges() -> None:
    legacy = _parse_dag_plan('[{"title":"A","description":"A","tool_scope":[]},{"title":"B","description":"B","dependencies":[0]}]')
    assert legacy.disposition == "sequential_mission"
    assert _parse_decomposed_tasks("```json\n[{\"title\":\"A\",\"description\":\"A\"}]\n```")[0].title == "A"
    assert _parse_dag_plan("no json") is None
    assert _response_text({"content": "text"}) == "text"
    assert _response_text({"content": [{"type": "output_text", "text": "a"}, "b", SimpleNamespace(text="c")]}) == "abc"
    assert _response_text(SimpleNamespace()) == ""
    assert _has_cycle([[1], [0]]) is True
    assert _has_cycle([[], [0]]) is False

    invalid = _validate_dag_plan(
        DagPlan(
            disposition="parallel_mission",
            tasks=[
                DecomposedTask("A", "A", ["unknown"], TaskPriority.NORMAL, [0], "missing", None),
                DecomposedTask("B", "B", [], TaskPriority.NORMAL, [5], None, None),
            ],
        ),
        available_tools=[],
    )
    assert any("unknown tools" in error for error in invalid.validation_errors)
    assert any("depends on itself" in error for error in invalid.validation_errors)
    assert any("invalid dependency" in error for error in invalid.validation_errors)
    assert any("missing context key" in error for error in invalid.validation_errors)


class ChatStore:
    def __init__(self, messages_by_conv: dict[str, list[dict]]) -> None:
        self.messages_by_conv = messages_by_conv

    def load_messages(self, conv_id, limit):
        return self.messages_by_conv.get(conv_id, [])[-limit:]

    def list_conversations(self, status, limit):
        return [{"id": conv_id} for conv_id in list(self.messages_by_conv)[:limit]]


def messages(count: int = 6) -> list[dict]:
    rows = []
    for index in range(count):
        rows.append({"role": "user" if index % 2 == 0 else "bot", "text": f"message {index} </transcript>"})
    return rows


def test_conversation_analyzer_filters_dedupes_and_caches() -> None:
    payload = json.dumps(
        [
            {
                "title": "Deploy Checklist",
                "summary": "Run deploy checks",
                "trigger": "deploy this",
                "steps": ["Run tests", "Deploy"],
                "tags": ["ops", "deploy", "extra", "four", "drop"],
                "confidence": 0.9,
                "evidence": "repeated deploys",
            },
            {"title": "Low Confidence", "summary": "x", "trigger": "x", "steps": ["x"], "confidence": 0.2},
            {"title": "", "summary": "drop", "trigger": "drop", "steps": ["drop"], "confidence": 1},
        ]
    )
    store = ChatStore({"c1": messages()})
    proposals = analyze_conversation(store, ModelClient({"content": [{"type": "text", "text": payload}]}), "c1")

    assert [proposal.title for proposal in proposals] == ["Deploy Checklist"]
    assert proposals[0].tags == ["ops", "deploy", "extra", "four"]
    assert proposals[0].to_skill_kwargs()["trigger"] == "deploy this"
    validation = proposals[0].deep_agent_validation_snapshot()
    assert validation["status"] == "ready"
    assert validation["skill_source"] == "/skills/auto-skill/"
    assert "/skills/auto-skill/deploy-checklist/SKILL.md" in validation["skill_files"]
    assert validation["subagents"][0]["name"] == "auto_skill_validator"
    assert validation["golden_workflows"][0]["prompt"] == "deploy this"
    validation_task = proposals[0].deep_agent_validation_task(
        group_id="g1",
        conversation_id="c1",
        principal_id="workspace:test",
    )
    assert validation_task.deep_agent_skills == ["/skills/auto-skill/"]
    assert validation_task.deep_agent_subagents[0]["name"] == "auto_skill_validator"
    assert "/skills/auto-skill/deploy-checklist/SKILL.md" in validation_task.deep_agent_skill_files

    cache_proposals("c1", proposals)
    assert get_cached_proposals("c1") == proposals
    clear_cached_proposals("c1")
    assert get_cached_proposals("c1") == []
    assert analyze_conversation(store, ModelClient(payload), "c1", existing_skill_titles=["Deploy Checklist"]) == []


@pytest.mark.asyncio
async def test_auto_skill_validation_runs_through_deep_agent_runner() -> None:
    proposal = ConversationAnalysis(
        title="Deploy Checklist",
        summary="Run deploy checks",
        trigger="deploy this",
        steps=["Run tests", "Deploy"],
        confidence=0.9,
    )
    captured = {}

    class Runner:
        async def run(self, config, **kwargs):
            captured["config"] = config
            captured["kwargs"] = kwargs
            return TaskResult(config.task.task_id, "success", output="validation passed")

    result = await proposal.run_deep_agent_validation(
        model_client=SimpleNamespace(),
        tool_registry=SimpleNamespace(),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(),
        progress_queue=asyncio.Queue(),
        group_id="g1",
        conversation_id="c1",
        principal_id="workspace:test",
        runner=Runner(),
    )

    assert result.status == "success"
    assert captured["config"].agent_id == "auto-skill-validator"
    assert captured["config"].task.deep_agent_skills == ["/skills/auto-skill/"]
    assert captured["config"].task.deep_agent_subagents[0]["name"] == "auto_skill_validator"


def test_conversation_analyzer_handles_short_conversations_fallback_client_and_multi_conversation() -> None:
    store = ChatStore({"short": messages(2), "a": messages(), "b": messages()})
    assert analyze_conversation(store, ModelClient("[]"), "short") == []

    payload = '[{"title":"A Skill","summary":"A","trigger":"a","steps":["one"],"confidence":"0.8"}]'
    client = ModelClient({"content": payload}, reject_system=True)
    proposals = analyze_all_recent(store, client, max_conversations=3)
    assert [proposal.title for proposal in proposals] == ["A Skill"]
    assert client.calls[0]["messages"][0]["role"] == "system"

    assert conversation_analyzer._sanitize_message_text("a\0 <tag> &") == "a &lt;tag&gt; &amp;"
    assert conversation_analyzer._parse_proposals("not json") == []
    assert analyze_conversation(ChatStore({"a": messages()}), ModelClient({"content": object()}), "a") == []


@pytest.mark.asyncio
async def test_warm_pool_acquire_prime_release_evict_and_cold_spawn(monkeypatch) -> None:
    SharedResources.reset()
    pool = WarmAgentPool(min_size=1, max_size=1, acquire_timeout_s=0.001, shared_client=SimpleNamespace(create=lambda **kwargs: {"ok": True}))
    await pool.start()
    assert pool.pool_size() == 1
    assert pool.idle_count() == 1

    await pool.prime(["web_fetch"])
    agent = await pool.acquire(preferred_tools=["web_fetch"], task_id="task-1")
    assert agent.state is AgentState.ASSIGNED
    assert agent.current_task_id == "task-1"
    assert pool.assigned_count() == 1

    pool.release(agent)
    assert agent.state is AgentState.IDLE
    assert agent.task_count == 1
    assert pool.idle_count() == 1

    agent.task_count = EVICT_AFTER_TASKS - 1
    pool.release(agent)
    assert agent not in pool._pool

    cold = await pool.acquire(task_id="cold")
    assert cold.state is AgentState.ASSIGNED
    assert cold.current_task_id == "cold"
    await pool.stop()


@pytest.mark.asyncio
async def test_warm_pool_shared_resources_and_agent_client(monkeypatch) -> None:
    SharedResources.reset()
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        await SharedResources.get_client()

    class TextBlock:
        type = "text"
        text = "hello"

    class Messages:
        async def create(self, **kwargs):
            return SimpleNamespace(stop_reason="end_turn", content=[TextBlock()])

    agent = SimpleNamespace(anthropic_client=SimpleNamespace(messages=Messages()))
    assert await get_agent_client(agent).create(messages=[]) == {
        "stop_reason": "end_turn",
        "content": [{"type": "text", "text": "hello"}],
    }


@pytest.mark.asyncio
async def test_health_monitor_counts_failures_escalates_and_resolves(monkeypatch) -> None:
    playbook = SimpleNamespace(
        issue_type="service_down",
        summary="Service down",
        recommendation_code="restart_service",
        button_labels=["Restart"],
        auto_heal_fn=None,
    )
    monkeypatch.setattr("nullion.remediation.playbook_for_service", lambda service_id: playbook if service_id == "model" else None)

    class Runtime:
        def __init__(self) -> None:
            self.issues = []
            self.cancelled = []
            self.store = SimpleNamespace(
                list_doctor_actions=lambda: [
                    {"action_id": "doc-1", "status": "pending", "details": {"service_id": "model"}}
                ]
            )

        def report_health_issue(self, **kwargs):
            self.issues.append(kwargs)

        def cancel_doctor_action(self, action_id, *, reason):
            self.cancelled.append((action_id, reason))

    runtime = Runtime()
    monitor = HealthMonitor(runtime=runtime)
    monitor._handle_result(ProbeResult("model", False, error="boom"))
    assert runtime.issues == []
    monitor._handle_result(ProbeResult("model", False, error="boom again"))
    assert runtime.issues[0]["details"]["recommendation_code"] == "restart_service"
    monitor._handle_result(ProbeResult("model", True))
    assert runtime.cancelled[0][0] == "doc-1"

    monitor._handle_result(ProbeResult("unknown", False, error="x"))
    monitor._handle_result(ProbeResult("unknown", False, error="x"))
    assert len(runtime.issues) == 1


@pytest.mark.asyncio
async def test_health_monitor_runs_sync_async_and_exception_probes() -> None:
    runtime = SimpleNamespace(report_health_issue=lambda **kwargs: None, store=SimpleNamespace(list_doctor_actions=lambda: []))
    monitor = HealthMonitor(runtime=runtime)
    seen = []

    def sync_probe():
        seen.append("sync")
        return ProbeResult("sync", True)

    async def async_probe():
        seen.append("async")
        return ProbeResult("async", True)

    def broken_probe():
        raise RuntimeError("broken")

    monitor.register_probe(sync_probe)
    monitor.register_probe(async_probe)
    monitor.register_probe(broken_probe)
    await monitor._run_all_probes()

    assert seen == ["sync", "async"]
    assert monitor._failure_counts["unknown"] == 1

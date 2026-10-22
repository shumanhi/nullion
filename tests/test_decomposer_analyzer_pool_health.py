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
from nullion.health_monitor import HealthMonitor, ProbeResult, clear_recovered_service_doctor_actions
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
                            "routing_evidence": ["model_structured_plan"],
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
    assert group.planner_metadata["routing_evidence"] == ["model_structured_plan"]
    assert group.tasks[0].deep_agent_skills == []
    assert group.tasks[0].deep_agent_subagents == []


def test_task_decomposer_runtime_artifact_contract_overrides_model_artifact_metadata() -> None:
    plan = DagPlan(
        disposition="sequential_mission",
        tasks=[
            DecomposedTask(
                title="Create artifact",
                description="Create artifact.",
                tool_scope=["spreadsheet_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="artifact",
                required_inputs=[],
                can_start=True,
                metadata={"requires_artifact_delivery": True, "required_artifact_kind": "png"},
            ),
            DecomposedTask(
                title="Verify artifact",
                description="Verify artifact.",
                tool_scope=[],
                priority=TaskPriority.NORMAL,
                dep_indices=[0],
                context_key_in="artifact",
                context_key_out="artifact_verified",
                required_inputs=[],
                can_start=True,
                metadata={
                    "artifact_role": "verify",
                    "requires_artifact_delivery": True,
                    "required_artifact_kind": "png",
                },
            ),
            DecomposedTask(
                title="Deliver receipt",
                description="Deliver receipt.",
                tool_scope=[],
                priority=TaskPriority.NORMAL,
                dep_indices=[1],
                context_key_in="artifact_verified",
                context_key_out=None,
                required_inputs=[],
                can_start=True,
                metadata={
                    "artifact_role": "deliver_receipt",
                    "requires_artifact_delivery": True,
                    "required_artifact_kind": "png",
                },
            ),
        ],
        routing_evidence=["structured_plan"],
    )

    group = TaskDecomposer(ModelClient({})).decompose(
        "structured artifact mission",
        group_id="g-artifact-contract",
        conversation_id="telegram:stage",
        principal_id="telegram:stage",
        available_tools=["spreadsheet_create"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="xlsx",
    )

    for task in group.tasks:
        if task.metadata.get("requires_artifact_delivery") or task.metadata.get("artifact_role"):
            assert task.metadata["required_artifact_kind"] == "xlsx"
            assert task.metadata["delivery_contract"]["artifact_kind"] == "xlsx"


def test_task_decomposer_infers_artifact_contract_from_single_producer_tool_when_model_added_roles() -> None:
    plan = DagPlan(
        disposition="sequential_mission",
        tasks=[
            DecomposedTask(
                title="Create artifact",
                description="Create artifact.",
                tool_scope=["spreadsheet_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="artifact",
                required_inputs=[],
                can_start=True,
                metadata={"requires_artifact_delivery": True, "required_artifact_kind": "png"},
            ),
            DecomposedTask(
                title="Verify artifact",
                description="Verify artifact.",
                tool_scope=[],
                priority=TaskPriority.NORMAL,
                dep_indices=[0],
                context_key_in="artifact",
                context_key_out="artifact_verified",
                required_inputs=[],
                can_start=True,
                metadata={
                    "artifact_role": "verify",
                    "requires_artifact_delivery": True,
                    "required_artifact_kind": "png",
                },
            ),
        ],
        routing_evidence=["structured_plan"],
    )

    group = TaskDecomposer(ModelClient({})).decompose(
        "structured artifact mission",
        group_id="g-artifact-infer",
        conversation_id="telegram:stage",
        principal_id="telegram:stage",
        available_tools=["spreadsheet_create"],
        dag_plan=plan,
    )

    for task in group.tasks:
        if task.metadata.get("requires_artifact_delivery") or task.metadata.get("artifact_role"):
            assert task.metadata["required_artifact_kind"] == "xlsx"
            assert task.metadata["delivery_contract"]["artifact_kind"] == "xlsx"


def test_task_decomposer_chains_sequential_plan_when_model_omits_dependencies() -> None:
    client = ModelClient(
        {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "disposition": "sequential_mission",
                            "needs_clarification": False,
                            "routing_evidence": ["model_structured_plan"],
                            "tasks": [
                                {"title": "Collect evidence", "description": "Collect evidence.", "tool_scope": [], "dependencies": []},
                                {"title": "Update history", "description": "Update history.", "tool_scope": [], "dependencies": []},
                                {"title": "Write report", "description": "Write report.", "tool_scope": [], "dependencies": []},
                            ],
                        }
                    ),
                }
            ]
        }
    )

    group = TaskDecomposer(client).decompose(
        "run sequential cron",
        group_id="g-sequential-chain",
        conversation_id="c-sequential-chain",
        principal_id="p-sequential-chain",
        available_tools=[],
    )

    assert [task.status for task in group.tasks] == [TaskStatus.QUEUED, TaskStatus.BLOCKED, TaskStatus.BLOCKED]
    assert group.tasks[1].dependencies == [group.tasks[0].task_id]
    assert group.tasks[2].dependencies == [group.tasks[1].task_id]
    assert group.planner_metadata["disposition"] == "sequential_mission"


def test_task_decomposer_links_source_tasks_to_requested_artifact_producer_even_when_model_marks_parallel() -> None:
    plan = DagPlan(
        disposition="parallel_mission",
        tasks=[
            DecomposedTask(
                title="Collect source links",
                description="Collect structured source links.",
                tool_scope=["browser_run_js"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="links",
                required_inputs=[],
                can_start=True,
            ),
            DecomposedTask(
                title="Collect source rows",
                description="Collect structured row records.",
                tool_scope=["browser_extract_items"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="rows",
                required_inputs=[],
                can_start=True,
            ),
            DecomposedTask(
                title="Create workbook",
                description="Create the requested workbook from source rows.",
                tool_scope=["spreadsheet_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out=None,
                required_inputs=[],
                can_start=True,
            ),
        ],
        routing_evidence=["model_structured_plan"],
    )

    group = TaskDecomposer(ModelClient({})).decompose(
        "create the requested workbook",
        group_id="g-artifact-source-deps",
        conversation_id="telegram:artifact-source-deps",
        principal_id="p-artifact-source-deps",
        available_tools=["browser_run_js", "browser_extract_items", "spreadsheet_create", "file_read"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="xlsx",
    )

    assert [task.status for task in group.tasks[:3]] == [TaskStatus.QUEUED, TaskStatus.QUEUED, TaskStatus.BLOCKED]
    assert set(group.tasks[2].dependencies) == {group.tasks[0].task_id, group.tasks[1].task_id}
    assert group.tasks[3].title == "Verify artifact"
    assert group.tasks[3].dependencies == [group.tasks[2].task_id]


def test_task_decomposer_orders_requested_artifact_producer_after_later_source_tasks() -> None:
    plan = DagPlan(
        disposition="sequential_mission",
        tasks=[
            DecomposedTask(
                title="Collect source links",
                description="Collect structured source links.",
                tool_scope=["browser_run_js"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="links",
                required_inputs=[],
                can_start=True,
            ),
            DecomposedTask(
                title="Create workbook",
                description="Create the requested workbook from all source records.",
                tool_scope=["spreadsheet_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out=None,
                required_inputs=[],
                can_start=True,
            ),
            DecomposedTask(
                title="Extract source records",
                description="Extract row-level source records.",
                tool_scope=["browser_extract_items"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="rows",
                required_inputs=[],
                can_start=True,
            ),
        ],
        routing_evidence=["model_structured_plan"],
    )

    group = TaskDecomposer(ModelClient({})).decompose(
        "create the requested workbook",
        group_id="g-artifact-source-reorder",
        conversation_id="telegram:artifact-source-reorder",
        principal_id="p-artifact-source-reorder",
        available_tools=["browser_run_js", "browser_extract_items", "spreadsheet_create", "file_read"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="xlsx",
    )

    assert [task.title for task in group.tasks[:3]] == [
        "Collect source links",
        "Extract source records",
        "Create workbook",
    ]
    assert [task.status for task in group.tasks[:5]] == [
        TaskStatus.QUEUED,
        TaskStatus.BLOCKED,
        TaskStatus.BLOCKED,
        TaskStatus.BLOCKED,
        TaskStatus.BLOCKED,
    ]
    assert set(group.tasks[2].dependencies) == {group.tasks[0].task_id, group.tasks[1].task_id}
    assert group.tasks[3].title == "Verify artifact"
    assert group.tasks[3].dependencies == [group.tasks[2].task_id]


def test_task_decomposer_repairs_public_domain_scope_to_browser_tools() -> None:
    client = ModelClient(
        {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "disposition": "parallel_mission",
                            "needs_clarification": False,
                            "routing_evidence": ["model_structured_plan"],
                            "tasks": [
                                {
                                    "title": "Source A",
                                    "description": "cnn.com の現在の内容を取得",
                                    "tool_scope": ["connector_request"],
                                    "dependencies": [],
                                },
                                {
                                    "title": "Source B",
                                    "description": "bbc.com の現在の内容を取得",
                                    "tool_scope": [],
                                    "dependencies": [],
                                },
                            ],
                        }
                    ),
                }
            ]
        }
    )

    group = TaskDecomposer(client).decompose(
        "cnn.com と bbc.com を比較",
        group_id="g-public-web",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["connector_request", "browser_navigate", "browser_extract_text"],
    )

    assert [task.allowed_tools for task in group.tasks] == [
        ["browser_navigate", "browser_extract_text"],
        ["browser_navigate", "browser_extract_text"],
    ]


def test_task_decomposer_adds_dependent_artifact_verification_and_receipt_tasks() -> None:
    plan = DagPlan(
        disposition="sequential_mission",
        routing_evidence=["model_structured_plan"],
        tasks=[
            DecomposedTask(
                title="Collect source",
                description="Collect source data.",
                tool_scope=["web_fetch"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out="source",
            ),
            DecomposedTask(
                title="Create report",
                description="Create the requested report artifact.",
                tool_scope=["pdf_create"],
                priority=TaskPriority.NORMAL,
                dep_indices=[0],
                context_key_in="source",
                context_key_out=None,
            ),
        ],
    )

    group = TaskDecomposer(ModelClient({})).decompose(
        "create the requested attachment",
        group_id="g-artifact",
        conversation_id="telegram:artifact",
        principal_id="p-artifact",
        available_tools=["web_fetch", "pdf_create", "file_read"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="pdf",
    )

    assert [task.title for task in group.tasks] == [
        "Collect source",
        "Create report",
        "Verify artifact",
        "Deliver receipt",
    ]
    assert group.tasks[1].metadata["requires_artifact_delivery"] is True
    assert group.tasks[1].metadata["required_artifact_kind"] == "pdf"
    assert group.tasks[1].metadata["delivery_contract"] == {
        "platform": "telegram",
        "delivery_mode": "attachment",
        "artifact_kind": "pdf",
        "portable_file": True,
        "supports_javascript": False,
        "requires_static_primary_content": False,
    }
    assert group.tasks[1].context_key_out == "artifact_1"
    assert group.tasks[2].dependencies == [group.tasks[1].task_id]
    assert group.tasks[2].metadata["artifact_role"] == "verify"
    assert group.tasks[2].deep_agent_subagents[0]["name"] == "artifact_verifier_agent"
    assert group.tasks[3].dependencies == [group.tasks[2].task_id]
    assert group.tasks[3].metadata["artifact_role"] == "deliver_receipt"


def test_task_decomposer_adds_platform_aware_html_delivery_contract() -> None:
    plan = DagPlan(
        disposition="single_turn",
        tasks=[
            DecomposedTask(
                title="Create report",
                description="Create the requested HTML report.",
                tool_scope=["file_write"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out=None,
            ),
        ],
    )

    telegram_group = TaskDecomposer(ModelClient({})).decompose(
        "create report",
        group_id="g-telegram-html",
        conversation_id="telegram:123",
        principal_id="p-telegram",
        available_tools=["file_write", "file_read"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="html",
    )
    web_group = TaskDecomposer(ModelClient({})).decompose(
        "create report",
        group_id="g-web-html",
        conversation_id="web:123",
        principal_id="p-web",
        available_tools=["file_write", "file_read"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="html",
    )
    unknown_group = TaskDecomposer(ModelClient({})).decompose(
        "create report",
        group_id="g-legacy-html",
        conversation_id="legacy-conversation-id",
        principal_id="p-legacy",
        available_tools=["file_write", "file_read"],
        dag_plan=plan,
        requires_artifact_delivery=True,
        required_artifact_kind="html",
    )

    telegram_contract = telegram_group.tasks[0].metadata["delivery_contract"]
    assert telegram_contract["platform"] == "telegram"
    assert telegram_contract["supports_javascript"] is False
    assert telegram_contract["requires_static_primary_content"] is True

    web_contract = web_group.tasks[0].metadata["delivery_contract"]
    assert web_contract["platform"] == "web"
    assert web_contract["supports_javascript"] is True
    assert web_contract["requires_static_primary_content"] is False

    unknown_contract = unknown_group.tasks[0].metadata["delivery_contract"]
    assert unknown_contract["platform"] == "unknown"
    assert unknown_contract["supports_javascript"] is False
    assert unknown_contract["requires_static_primary_content"] is True


def test_task_decomposer_keeps_connector_scope_without_url_domain_evidence() -> None:
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
                                    "title": "Connector status",
                                    "description": "Read the configured account status from the connector.",
                                    "tool_scope": ["connector_request"],
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
        "接続済みアカウントの状態",
        group_id="g-connector",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["connector_request", "browser_navigate", "browser_extract_text"],
    )

    assert group.tasks[0].allowed_tools == ["connector_request"]


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


def test_requested_planner_dispatch_allows_valid_single_turn_plan() -> None:
    plan = DagPlan(
        disposition="single_turn",
        tasks=[
            DecomposedTask(
                title="Build requested PDF",
                description="Complete the explicitly requested planner task.",
                tool_scope=[],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out=None,
                required_inputs=[],
                can_start=True,
            )
        ],
        routing_evidence=[],
    )

    assert plan.can_dispatch is False
    assert plan.can_dispatch_when_requested is True


def test_task_decomposer_repairs_public_domain_tasks_to_available_browser_scope() -> None:
    plan = DagPlan(
        disposition="single_turn",
        tasks=[
            DecomposedTask(
                title="Check example.com",
                description="Review example.com and summarize the page.",
                tool_scope=["connector_request"],
                priority=TaskPriority.NORMAL,
                dep_indices=[],
                context_key_in=None,
                context_key_out=None,
                required_inputs=[],
                can_start=True,
            )
        ],
        routing_evidence=[],
    )

    group = TaskDecomposer(ModelClient({})).decompose(
        "Review example.com",
        group_id="g-browser-repair",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["connector_request", "browser_navigate", "browser_extract_text"],
        dag_plan=plan,
    )

    assert group.tasks[0].allowed_tools == ["browser_navigate", "browser_extract_text"]


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


def test_task_decomposer_caps_prompt_tool_list_for_speed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NULLION_TASK_DECOMPOSER_MAX_TOOLS", "2")
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

    TaskDecomposer(client).decompose(
        "run monitor",
        group_id="g-cap",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["z_tool", "a_tool", "m_tool"],
    )

    prompt = client.calls[0]["messages"][-1]["content"][0]["text"]
    assert "Available tools (2 shown of 3): a_tool, m_tool" in prompt


def test_task_decomposer_passes_timeout_and_caches_create_capabilities() -> None:
    class TimeoutClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, *, messages, tools, max_tokens=4096, timeout=None, system=None):
            self.calls.append(
                {
                    "messages": messages,
                    "tools": tools,
                    "max_tokens": max_tokens,
                    "timeout": timeout,
                    "system": system,
                }
            )
            return {
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

    client = TimeoutClient()
    decomposer = TaskDecomposer(client, model_timeout_seconds=3.5)
    decomposer.decompose(
        "run monitor",
        group_id="g-timeout-1",
        conversation_id="c1",
        principal_id="p1",
        available_tools=[],
    )
    decomposer.decompose(
        "run monitor again",
        group_id="g-timeout-2",
        conversation_id="c1",
        principal_id="p1",
        available_tools=[],
    )

    assert len(client.calls) == 2
    assert client.calls[0]["timeout"] == 3.5
    assert client.calls[0]["max_tokens"] == 512
    assert getattr(client, "_nullion_decomposer_create_caps", None) is not None


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

    missing_evidence_client = ModelClient(
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
                                    "title": "Fetch",
                                    "description": "Fetch",
                                    "tool_scope": ["web_fetch"],
                                    "dependencies": [],
                                },
                                {
                                    "title": "Summarize",
                                    "description": "Summarize",
                                    "tool_scope": [],
                                    "dependencies": [0],
                                },
                            ],
                        }
                    ),
                }
            ]
        }
    )
    missing_evidence = TaskDecomposer(missing_evidence_client).decompose(
        "fetch then summarize",
        group_id="g-missing-evidence",
        conversation_id="c1",
        principal_id="p1",
        available_tools=["web_fetch"],
    )
    assert len(missing_evidence.tasks) == 1
    assert any(
        "model_structured_plan routing evidence" in error
        for error in missing_evidence.planner_metadata["validation_errors"]
    )

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


def test_task_decomposer_uses_scheduled_preview_system_override() -> None:
    from nullion.task_decomposer import TaskDecomposer

    class CaptureClient:
        def __init__(self) -> None:
            self.system: list[str] = []

        def create(self, *, messages, tools, max_tokens=None, timeout=None, system=None):  # noqa: ANN001, ANN003
            _ = (messages, tools, max_tokens, timeout)
            self.system.append(str(system or ""))
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "disposition": "single_turn",
                                "needs_clarification": False,
                                "clarification_question": None,
                                "routing_evidence": [],
                                "tasks": [
                                    {
                                        "title": "Run scheduled task",
                                        "description": "Run scheduled task",
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

    preview_request = "\n".join(
        [
            "scheduled_task_preview=true",
            "required_inputs_resolved=true",
            "clarification_allowed=false",
            "decompose_best_effort=true",
            "scheduled_task_request=Daily Pre-Market Stock Research",
        ]
    )
    client = CaptureClient()
    TaskDecomposer(client, scheduled_preview_mode=True).decompose(
        preview_request,
        group_id="g-preview",
        conversation_id="c-preview",
        principal_id="p-preview",
        available_tools=[],
    )
    assert client.system
    assert "Scheduled-task preview override" in client.system[0]

    plain_client = CaptureClient()
    TaskDecomposer(plain_client).decompose(
        preview_request,
        group_id="g-preview-plain",
        conversation_id="c-preview",
        principal_id="p-preview",
        available_tools=[],
    )
    assert plain_client.system
    assert "Scheduled-task preview override" not in plain_client.system[0]


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
                "activation_condition": "Use when the operator requests the standard deploy verification workflow.",
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
    assert proposals[0].to_skill_kwargs()["trigger"] == "Use when the operator requests the standard deploy verification workflow."
    validation = proposals[0].deep_agent_validation_snapshot()
    assert validation["status"] == "ready"
    assert validation["skill_source"] == "/skills/auto-skill/"
    assert "/skills/auto-skill/deploy-checklist/SKILL.md" in validation["skill_files"]
    assert validation["subagents"][0]["name"] == "auto_skill_validator"
    assert validation["golden_workflows"][0]["prompt"] == "Use when the operator requests the standard deploy verification workflow."
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


def test_conversation_analyzer_accepts_language_neutral_activation_condition() -> None:
    payload = json.dumps(
        [
            {
                "title": "Revisión de despliegue",
                "summary": "Ejecuta la verificación estándar antes del despliegue",
                "activation_condition": "Usar cuando la persona pida la verificación estándar previa al despliegue.",
                "steps": ["Ejecutar pruebas", "Revisar artefactos"],
                "confidence": 0.86,
                "evidence": "Flujo repetido con el mismo objetivo y resultado.",
            },
            {
                "title": "Legacy Trigger",
                "summary": "Still accepts old payloads",
                "trigger": "Use for older analyzer payloads.",
                "steps": ["Read payload"],
                "confidence": 0.8,
            },
            {
                "title": "Missing Condition",
                "summary": "No activation condition",
                "steps": ["Drop"],
                "confidence": 0.95,
            },
        ]
    )

    proposals = conversation_analyzer._parse_proposals(payload)

    assert [proposal.title for proposal in proposals] == ["Revisión de despliegue", "Legacy Trigger"]
    assert proposals[0].trigger == "Usar cuando la persona pida la verificación estándar previa al despliegue."
    assert proposals[0].to_skill_kwargs()["trigger"] == proposals[0].trigger
    assert proposals[1].trigger == "Use for older analyzer payloads."


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

    class NoMaxTokensClient:
        def __init__(self) -> None:
            self.calls = []

        def create(self, *, messages, tools=None, system=None):
            self.calls.append({"messages": messages, "tools": tools, "system": system})
            return {"ok": True}

    no_max = NoMaxTokensClient()
    wrapped = await get_agent_client(SimpleNamespace(anthropic_client=no_max)).create(
        messages=[{"role": "user", "content": "hi"}],
        tools=[],
        max_tokens=128,
        system="sys",
    )
    assert wrapped == {"ok": True}
    assert no_max.calls == [{"messages": [{"role": "user", "content": "hi"}], "tools": [], "system": "sys"}]


def test_agent_orchestrator_checkpoint_callback_records_dispatch_state() -> None:
    from nullion.agent_orchestrator import AgentOrchestrator

    checkpoints = {"count": 0}
    orchestrator = AgentOrchestrator(model_client=None)
    orchestrator.set_checkpoint_fn(lambda: checkpoints.__setitem__("count", checkpoints["count"] + 1))

    orchestrator._checkpoint_dispatch_state()

    assert checkpoints["count"] == 1


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


def test_health_monitor_pauses_terminal_model_auth_failures(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_HEALTH_MONITOR_AUTH_BACKOFF_SECONDS", "60")
    playbook = SimpleNamespace(
        issue_type="service_down",
        summary="Model auth failed",
        recommendation_code="reauth",
        button_labels=["Sign in"],
        auto_heal_fn=None,
    )
    monkeypatch.setattr("nullion.remediation.playbook_for_service", lambda service_id: playbook if service_id == "model_api" else None)

    class Runtime:
        def __init__(self) -> None:
            self.issues = []
            self.store = SimpleNamespace(list_doctor_actions=lambda: [])

        def report_health_issue(self, **kwargs):
            self.issues.append(kwargs)

    runtime = Runtime()
    monitor = HealthMonitor(runtime=runtime)
    calls = {"count": 0}

    def probe():
        calls["count"] += 1
        return ProbeResult(
            "model_api",
            False,
            error="Codex Responses request failed with HTTP 401: token_invalidated",
            details={"http_status": 401, "provider_code": "token_invalidated", "category": "auth", "terminal": True},
        )

    setattr(probe, "service_id", "model_api")
    monitor.register_probe(probe)

    asyncio.run(monitor._run_all_probes())
    asyncio.run(monitor._run_all_probes())

    assert calls["count"] == 1
    assert len(runtime.issues) == 1
    assert monitor._failure_counts["model_api"] == 2
    assert monitor._probe_is_paused("model_api") is True


def test_health_monitor_pauses_terminal_model_quota_failures(monkeypatch) -> None:
    playbooks = {
        "model_quota": SimpleNamespace(
            issue_type="service_down",
            summary="Model quota exhausted",
            recommendation_code="model_quota_exhausted",
            button_labels=["Switch model"],
            auto_heal_fn=None,
        ),
    }
    monkeypatch.setattr("nullion.remediation.playbook_for_service", lambda service_id: playbooks.get(service_id))

    class Runtime:
        def __init__(self) -> None:
            self.issues = []
            self.store = SimpleNamespace(list_doctor_actions=lambda: [])

        def report_health_issue(self, **kwargs):
            self.issues.append(kwargs)

    runtime = Runtime()
    monitor = HealthMonitor(runtime=runtime)
    calls = {"count": 0}

    def probe():
        calls["count"] += 1
        return ProbeResult(
            "model_api",
            False,
            error="Codex Responses request failed with HTTP 429: usage_limit_reached",
            details={
                "http_status": 429,
                "error_type": "usage_limit_reached",
                "category": "quota",
                "terminal": True,
                "retry_after_seconds": 30,
            },
        )

    setattr(probe, "service_id", "model_api")
    monitor.register_probe(probe)

    asyncio.run(monitor._run_all_probes())
    asyncio.run(monitor._run_all_probes())

    assert calls["count"] == 1
    assert runtime.issues[0]["details"]["recommendation_code"] == "model_quota_exhausted"
    assert runtime.issues[0]["details"]["service_id"] == "model_api"
    assert runtime.issues[0]["details"]["playbook_service_id"] == "model_quota"
    assert monitor._failure_counts["model_api"] == 2
    assert monitor._probe_is_paused("model_api") is True


def test_recovered_model_api_clears_stale_doctor_actions_without_details() -> None:
    class Runtime:
        def __init__(self) -> None:
            self.cancelled: list[tuple[str, str]] = []
            self.store = SimpleNamespace(
                list_doctor_actions=lambda: [
                    {
                        "action_id": "stale-model",
                        "status": "pending",
                        "recommendation_code": "model_api_unreachable",
                        "reason": "health_alert routed to doctor;source=model_api;service_id=model_api",
                        "error": "Codex Responses request failed with HTTP 429: usage_limit_reached",
                    },
                    {
                        "action_id": "unrelated",
                        "status": "pending",
                        "recommendation_code": "telegram_bot_unreachable",
                        "reason": "service_id=telegram_bot",
                    },
                    {
                        "action_id": "closed-model",
                        "status": "cancelled",
                        "recommendation_code": "model_api_unreachable",
                    },
                ]
            )

        def cancel_doctor_action(self, action_id: str, *, reason: str) -> None:
            self.cancelled.append((action_id, reason))

    runtime = Runtime()

    cleared = clear_recovered_service_doctor_actions(
        runtime,
        "model_api",
        reason="model check succeeded",
    )

    assert cleared == 1
    assert runtime.cancelled == [("stale-model", "model check succeeded")]


def test_model_probe_extracts_terminal_auth_details() -> None:
    from nullion.health_probes import _model_probe_error_details

    details = _model_probe_error_details(
        RuntimeError(
            'Codex Responses request failed with HTTP 401: { "error": { "code": "token_revoked" }, "status": 401 }'
        )
    )

    assert details["http_status"] == 401
    assert details["provider_code"] == "token_revoked"
    assert details["category"] == "auth"
    assert details["terminal"] is True


def test_model_probe_uses_a_valid_bounded_completion_budget() -> None:
    from nullion.health_probes import make_model_api_probe

    class Client:
        def create(self, *, messages, tools, max_tokens):
            if max_tokens < 16:
                raise ValueError("Completion budget is below the provider minimum")
            assert max_tokens <= 32
            return {"content": [], "completion": {"status": "incomplete"}}

    assert make_model_api_probe(Client())().ok is True


def test_model_probe_preserves_clients_without_completion_budget_parameter() -> None:
    from nullion.health_probes import make_model_api_probe

    class Client:
        def create(self, *, messages, tools):
            return {"content": []}

    assert make_model_api_probe(Client())().ok is True


def test_model_probe_extracts_terminal_quota_details() -> None:
    from nullion.health_probes import _model_probe_error_details

    details = _model_probe_error_details(
        RuntimeError(
            'Codex Responses request failed with HTTP 429: {"error":{"type":"usage_limit_reached",'
            '"resets_in_seconds":123}}'
        )
    )

    assert details["http_status"] == 429
    assert details["error_type"] == "usage_limit_reached"
    assert details["category"] == "quota"
    assert details["terminal"] is True
    assert details["retry_after_seconds"] == 123


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

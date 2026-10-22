from __future__ import annotations

from types import SimpleNamespace

from nullion import chat_backend
from nullion.mini_agent_routing import should_keep_dag_plan_in_direct_turn, should_route_without_mini_agents
from nullion.tools import ToolRegistry


def test_structural_attachment_turns_route_without_mini_agents() -> None:
    assert should_route_without_mini_agents("anything", has_attachments=True)
    assert not should_route_without_mini_agents("Can you fetch cnn.com headlines and send me in a txt?")
    assert not should_route_without_mini_agents("Compare cnn.com and bbc.com and make a report")
    assert not should_route_without_mini_agents("Audit openclaw config")


def test_small_structured_artifact_plan_stays_in_direct_turn() -> None:
    plan = SimpleNamespace(
        disposition="sequential_mission",
        tasks=[
            SimpleNamespace(tool_scope=["web_search"]),
            SimpleNamespace(tool_scope=["pdf_create"]),
        ],
    )

    assert should_keep_dag_plan_in_direct_turn(plan, available_tools=["web_search", "pdf_create"])


def test_stateful_tool_plan_stays_in_direct_turn_from_structured_scope() -> None:
    plan = SimpleNamespace(
        disposition="sequential_mission",
        tasks=[
            SimpleNamespace(tool_scope=["list_crons"]),
            SimpleNamespace(tool_scope=["toggle_cron"]),
        ],
    )

    assert should_keep_dag_plan_in_direct_turn(
        plan,
        available_tools=["web_search", "list_crons", "toggle_cron"],
    )


def test_requested_artifact_plan_requires_installed_artifact_tool() -> None:
    plan = SimpleNamespace(
        disposition="sequential_mission",
        tasks=[
            SimpleNamespace(tool_scope=["web_search"]),
            SimpleNamespace(tool_scope=["web_fetch"]),
        ],
    )

    assert should_keep_dag_plan_in_direct_turn(
        plan,
        available_tools=["web_search", "pdf_create"],
        requested_extensions=(".pdf",),
    )
    assert not should_keep_dag_plan_in_direct_turn(
        plan,
        available_tools=["web_search"],
        requested_extensions=(".pdf",),
    )


def test_non_artifact_structured_plan_can_dispatch() -> None:
    plan = SimpleNamespace(
        disposition="parallel_mission",
        tasks=[
            SimpleNamespace(tool_scope=["web_search"]),
            SimpleNamespace(tool_scope=["web_fetch"]),
        ],
    )

    assert not should_keep_dag_plan_in_direct_turn(plan)


def test_chat_backend_simple_file_request_skips_mini_agent_dispatch(monkeypatch) -> None:
    class Orchestrator:
        def dispatch_request_sync(self, **kwargs):
            raise AssertionError("simple file request should stay in the direct turn")

        def run_turn(self, **kwargs):
            return SimpleNamespace(final_text="direct artifact turn")

    monkeypatch.setattr(chat_backend, "_feature_enabled", lambda name: True)

    assert (
        chat_backend.generate_chat_reply(
            message="Can you fetch cnn.com headlines and send me in a text file?",
            active_tool_registry=ToolRegistry(),
            agent_orchestrator=Orchestrator(),
        )
        == "direct artifact turn"
    )


def test_chat_backend_dispatches_from_model_structured_dag(monkeypatch) -> None:
    class FakeDagPlanner:
        def __init__(self, *, model_client):
            self.model_client = model_client

        def plan_dag(self, message, *, available_tools):
            return SimpleNamespace(
                can_dispatch=True,
                tasks=[
                    SimpleNamespace(title="Fetch first source"),
                    SimpleNamespace(title="Fetch second source"),
                ],
            )

    class Orchestrator:
        model_client = object()

        def dispatch_request_sync(self, **kwargs):
            assert kwargs["dag_plan"].can_dispatch is True
            assert isinstance(kwargs["available_tools"], list)
            return SimpleNamespace(dispatched=True, task_count=2, acknowledgment="delegated")

        def run_turn(self, **kwargs):
            raise AssertionError("structured DAG should dispatch before direct turn")

    monkeypatch.setattr(chat_backend, "_feature_enabled", lambda name: True)
    monkeypatch.setattr(chat_backend, "TaskDecomposer", FakeDagPlanner)

    assert (
        chat_backend.generate_chat_reply(
            message="調査対象 A と B を比較してください",
            active_tool_registry=ToolRegistry(),
            agent_orchestrator=Orchestrator(),
            allow_mini_agents=True,
        )
        == "delegated"
    )

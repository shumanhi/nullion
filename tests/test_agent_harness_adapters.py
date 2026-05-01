from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nullion.langchain_adapters import nullion_tools_as_langchain_tools, optional_agent_harness_status
from nullion.mini_agent_runner import MiniAgentConfig, MiniAgentRunner
from nullion.context_bus import ContextBus
from nullion.task_queue import TaskPriority, TaskRecord, TaskResult, TaskStatus
from nullion.tools import ToolResult


def test_optional_agent_harness_status_is_stable() -> None:
    status = optional_agent_harness_status()

    assert set(status) == {"langchain", "deepagents"}
    assert isinstance(status["langchain"], bool)
    assert isinstance(status["deepagents"], bool)


@pytest.mark.asyncio
async def test_langchain_tool_adapter_invokes_nullion_registry() -> None:
    calls = []

    class Registry:
        def list_tool_definitions(self, *, allowed):
            return [
                {
                    "name": name,
                    "description": f"Run {name}",
                    "input_schema": {
                        "type": "object",
                        "properties": {"path": {"type": "string"}},
                        "required": ["path"],
                    },
                }
                for name in allowed
            ]

        def invoke(self, invocation):
            calls.append(invocation)
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"path": invocation.arguments["path"]})

    tools = nullion_tools_as_langchain_tools(
        Registry(),
        allowed_tools=["file_read"],
        principal_id="workspace:demo",
        cleanup_scope="task-1",
    )

    assert [tool.name for tool in tools] == ["file_read"]
    assert await tools[0].ainvoke({"path": "/tmp/a.txt"}) == '{"path": "/tmp/a.txt"}'
    assert calls[0].principal_id == "workspace:demo"
    assert calls[0].capsule_id == "task-1"


@pytest.mark.asyncio
async def test_mini_agent_runner_delegates_to_deepagents(monkeypatch) -> None:
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner

    async def fake_run(self, config, **kwargs):
        return TaskResult(config.task.task_id, "success", output="deep done")

    monkeypatch.setattr(DeepAgentMiniAgentRunner, "run", fake_run)

    task = TaskRecord(
        task_id="task-1",
        group_id="group-1",
        conversation_id="conv-1",
        principal_id="workspace:demo",
        title="Test",
        description="Run the deep backend",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )

    result = await MiniAgentRunner().run(
        MiniAgentConfig(agent_id="agent-1", task=task),
        anthropic_client=SimpleNamespace(),
        tool_registry=SimpleNamespace(),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "success"
    assert result.output == "deep done"


@pytest.mark.asyncio
async def test_deepagents_default_path_tracks_artifacts_and_progress(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("NULLION_DEEP_AGENTS_MODEL", raising=False)
    artifact = tmp_path / "out.txt"

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "progress-1",
                            "name": "report_progress",
                            "input": {"message": "Writing file"},
                        }
                    ],
                }
            if self.calls == 2:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "write-1",
                            "name": "file_write",
                            "input": {"path": str(artifact), "content": "hello"},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "saved"}]}

    class Registry:
        def list_tool_definitions(self, *, allowed):
            return [
                {
                    "name": name,
                    "description": f"Run {name}",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"},
                            "content": {"type": "string"},
                        },
                    },
                }
                for name in allowed
            ]

        def invoke(self, invocation):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"path": invocation.arguments["path"]})

        def run_cleanup_hooks(self, *, scope_id):
            return None

    task = TaskRecord(
        task_id="task-artifact",
        group_id="group-1",
        conversation_id="conv-1",
        principal_id="workspace:demo",
        title="Write",
        description="Write the file",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_write"],
        dependencies=[],
    )
    progress_queue: asyncio.Queue = asyncio.Queue()

    result = await MiniAgentRunner().run(
        MiniAgentConfig(agent_id="agent-1", task=task),
        anthropic_client=Client(),
        tool_registry=Registry(),
        policy_store=None,
        approval_store=None,
        context_bus=ContextBus(),
        progress_queue=progress_queue,
    )

    assert result.status == "success"
    assert result.output == "saved"
    assert result.artifacts == [str(artifact)]
    progress_messages = [progress_queue.get_nowait().message for _ in range(progress_queue.qsize())]
    assert "Writing file" in progress_messages


@pytest.mark.asyncio
async def test_deepagents_default_path_can_pause_for_user_input(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_DEEP_AGENTS_MODEL", raising=False)

    class Client:
        def create(self, **kwargs):
            return {
                "stop_reason": "tool_use",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "input-1",
                        "name": "request_user_input",
                        "input": {"question": "Which file?", "options": ["A", "B"]},
                    }
                ],
            }

    task = TaskRecord(
        task_id="task-input",
        group_id="group-1",
        conversation_id="conv-1",
        principal_id="workspace:demo",
        title="Ask",
        description="Ask for input",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )
    progress_queue: asyncio.Queue = asyncio.Queue()

    result = await MiniAgentRunner().run(
        MiniAgentConfig(agent_id="agent-1", task=task),
        anthropic_client=Client(),
        tool_registry=SimpleNamespace(list_tool_definitions=lambda **kwargs: [], run_cleanup_hooks=lambda **kwargs: None),
        policy_store=None,
        approval_store=None,
        context_bus=ContextBus(),
        progress_queue=progress_queue,
    )

    assert result.status == "partial"
    assert result.output == "Waiting for user input: Which file?"
    updates = [progress_queue.get_nowait() for _ in range(progress_queue.qsize())]
    input_updates = [update for update in updates if update.kind == "input_needed"]
    assert input_updates
    assert input_updates[0].data == {"options": ["A", "B"]}

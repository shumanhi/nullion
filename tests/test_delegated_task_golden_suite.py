from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nullion.context_bus import ContextBus
from nullion.mini_agent_runner import MiniAgentConfig, MiniAgentRunner
from nullion.task_queue import TaskPriority, TaskRecord, TaskStatus
from nullion.tools import ToolResult


class ScriptedClient:
    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if not self.responses:
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "done"}]}
        return self.responses.pop(0)


class GoldenRegistry:
    def __init__(self, outputs: dict[str, object]) -> None:
        self.outputs = outputs
        self.calls = []
        self.cleanup_scopes = []

    def list_tool_definitions(self, *, allowed):
        return [
            {
                "name": name,
                "description": f"Run {name}",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "path": {"type": "string"},
                        "content": {"type": "string"},
                    },
                },
            }
            for name in allowed
        ]

    def invoke(self, invocation):
        self.calls.append(invocation)
        output = self.outputs.get(invocation.tool_name, {"ok": True})
        if isinstance(output, Exception):
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {"error": str(output)},
                error=str(output),
            )
        if isinstance(output, ToolResult):
            return output
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", output)

    def run_cleanup_hooks(self, *, scope_id):
        self.cleanup_scopes.append(scope_id)


def task(*, task_id: str = "task-1", title: str = "Task", description: str = "Do the task", tools: list[str] | None = None):
    return TaskRecord(
        task_id=task_id,
        group_id="group-1",
        conversation_id="conv-1",
        principal_id="workspace:demo",
        title=title,
        description=description,
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=list(tools or []),
        dependencies=[],
    )


async def run_task(client, registry, record):
    progress_queue: asyncio.Queue = asyncio.Queue()
    result = await MiniAgentRunner().run(
        MiniAgentConfig(agent_id="agent-1", task=record),
        anthropic_client=client,
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
        context_bus=ContextBus(),
        progress_queue=progress_queue,
    )
    updates = [progress_queue.get_nowait() for _ in range(progress_queue.qsize())]
    return result, updates


def tool_use(tool_id: str, name: str, args: dict) -> dict:
    return {"type": "tool_use", "id": tool_id, "name": name, "input": args}


def final(text: str) -> dict:
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}


def use_tool(*blocks: dict) -> dict:
    return {"stop_reason": "tool_use", "content": list(blocks)}


@pytest.mark.asyncio
async def test_golden_research_workflow_uses_scoped_search_tool() -> None:
    client = ScriptedClient([
        use_tool(tool_use("search-1", "web_search", {"query": "Nullion architecture"})),
        final("Research summary: local-first agent console."),
    ])
    registry = GoldenRegistry({"web_search": {"summary": "local-first agent console"}})

    result, updates = await run_task(client, registry, task(description="Research Nullion", tools=["web_search"]))

    assert result.status == "success"
    assert result.output == "Research summary: local-first agent console."
    assert [call.tool_name for call in registry.calls] == ["web_search"]
    assert any(update.message == "Calling web_search." for update in updates)


@pytest.mark.asyncio
async def test_golden_repo_analysis_reads_files_before_answering() -> None:
    client = ScriptedClient([
        use_tool(tool_use("read-1", "file_read", {"path": "/repo/README.md"})),
        final("README says Nullion is local-first."),
    ])
    registry = GoldenRegistry({"file_read": {"content": "# Nullion\nlocal-first"}})

    result, _updates = await run_task(client, registry, task(description="Analyze README", tools=["file_read"]))

    assert result.status == "success"
    assert result.output == "README says Nullion is local-first."
    assert registry.calls[0].arguments["path"] == "/repo/README.md"


@pytest.mark.asyncio
async def test_golden_artifact_workflow_tracks_created_file() -> None:
    client = ScriptedClient([
        use_tool(tool_use("write-1", "file_write", {"path": "/tmp/report.md", "content": "# Report"})),
        final("Saved the markdown report."),
    ])
    registry = GoldenRegistry({"file_write": {"path": "/tmp/report.md"}})

    result, updates = await run_task(client, registry, task(description="Write report", tools=["file_write"]))

    assert result.status == "success"
    assert result.artifacts == ["/tmp/report.md"]
    assert result.output == "Saved the markdown report."
    assert any(update.message == "file_write completed." for update in updates)


@pytest.mark.asyncio
async def test_golden_user_input_workflow_pauses_cleanly() -> None:
    client = ScriptedClient([
        use_tool(tool_use("input-1", "request_user_input", {"question": "Which repo?", "options": ["A", "B"]})),
    ])
    registry = GoldenRegistry({})

    result, updates = await run_task(client, registry, task(description="Ask a clarifying question"))

    assert result.status == "partial"
    assert result.output == "Waiting for user input: Which repo?"
    assert any(update.kind == "input_needed" and update.data == {"options": ["A", "B"]} for update in updates)


@pytest.mark.asyncio
async def test_golden_approval_workflow_returns_partial_result() -> None:
    client = ScriptedClient([
        use_tool(tool_use("fetch-1", "web_fetch", {"url": "https://example.com"})),
        final("Approval is needed before fetch can continue."),
    ])
    registry = GoldenRegistry(
        {
            "web_fetch": ToolResult(
                "placeholder",
                "web_fetch",
                "denied",
                {"reason": "approval_required", "requires_approval": True, "approval_id": "ap-1"},
                error="Approval required",
            )
        }
    )

    result, updates = await run_task(client, registry, task(description="Fetch a page", tools=["web_fetch"]))

    assert result.status == "partial"
    assert result.output == "Approval required before this delegated task can continue. Approval ID: ap-1"
    assert result.resume_token["reason"] == "approval_required"
    assert result.resume_token["approval_id"] == "ap-1"
    assert any(update.kind == "approval_needed" and update.data["approval_id"] == "ap-1" for update in updates)


@pytest.mark.asyncio
async def test_golden_failure_recovery_workflow_can_use_fallback_tool() -> None:
    client = ScriptedClient([
        use_tool(tool_use("primary-1", "primary_lookup", {"query": "x"})),
        use_tool(tool_use("fallback-1", "fallback_lookup", {"query": "x"})),
        final("Recovered with fallback data."),
    ])
    registry = GoldenRegistry({"primary_lookup": RuntimeError("primary failed"), "fallback_lookup": {"answer": "ok"}})

    result, _updates = await run_task(
        client,
        registry,
        task(description="Recover from a failed lookup", tools=["primary_lookup", "fallback_lookup"]),
    )

    assert result.status == "success"
    assert result.output == "Recovered with fallback data."
    assert [call.tool_name for call in registry.calls] == ["primary_lookup", "fallback_lookup"]


@pytest.mark.asyncio
async def test_golden_skills_and_subagents_are_forwarded(monkeypatch) -> None:
    import deepagents

    captured = {}

    class FakeAgent:
        async def ainvoke(self, payload, config=None):
            captured["payload"] = payload
            return {"messages": [SimpleNamespace(type="ai", content="delegated")]}

    def fake_create_deep_agent(**kwargs):
        captured.update(kwargs)
        return FakeAgent()

    monkeypatch.setattr(deepagents, "create_deep_agent", fake_create_deep_agent)
    record = task(description="Delegate", tools=[])
    record.deep_agent_skills = ["/skills/custom/"]
    record.deep_agent_skill_files = {
        "/skills/custom/repo-review/SKILL.md": "---\nname: repo-review\ndescription: Inspect repository context\n---\n"
    }
    record.deep_agent_subagents = [
        {
            "name": "repo-researcher",
            "description": "Inspect repository context",
            "system_prompt": "Focus on repository evidence.",
        }
    ]

    result, _updates = await run_task(ScriptedClient([]), GoldenRegistry({}), record)

    assert result.status == "success"
    assert result.output == "delegated"
    assert captured["skills"] == ["/skills/custom/"]
    assert captured["payload"]["files"] == {
        "/skills/custom/repo-review/SKILL.md": {
            "content": "---\nname: repo-review\ndescription: Inspect repository context\n---\n",
            "encoding": "utf-8",
        }
    }
    assert captured["subagents"] == [
        {
            "name": "repo-researcher",
            "description": "Inspect repository context",
            "system_prompt": "Focus on repository evidence.",
        }
    ]


@pytest.mark.asyncio
async def test_golden_inferred_subagents_are_forwarded(monkeypatch) -> None:
    import deepagents

    captured = {}
    payloads = []

    class FakeAgent:
        async def ainvoke(self, payload, config=None):
            payloads.append(payload)
            return {"messages": [SimpleNamespace(type="ai", content="profiled")]}

    def fake_create_deep_agent(**kwargs):
        captured.update(kwargs)
        return FakeAgent()

    monkeypatch.setattr(deepagents, "create_deep_agent", fake_create_deep_agent)
    record = task(description="Research the docs and write an artifact", tools=["web_search", "file_write"])

    result, _updates = await run_task(ScriptedClient([]), GoldenRegistry({}), record)

    assert result.status == "success"
    assert result.output == "profiled"
    assert captured["skills"] == ["/skills/nullion/"]
    assert [agent["name"] for agent in captured["subagents"]] == [
        "research_agent",
        "artifact_agent",
        "artifact_verifier_agent",
    ]
    assert "/skills/nullion/research/SKILL.md" in payloads[0]["files"]
    assert "/skills/nullion/artifact/SKILL.md" in payloads[0]["files"]
    assert "/skills/nullion/artifact-verifier/SKILL.md" in payloads[0]["files"]

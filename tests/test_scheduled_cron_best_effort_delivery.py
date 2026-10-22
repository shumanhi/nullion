from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import pytest

from nullion.mini_agent_runner import MiniAgentConfig
from nullion.task_queue import TaskPriority, TaskRecord, TaskStatus
from nullion.tools import ToolResult


@pytest.mark.asyncio
async def test_scheduled_completion_failure_keeps_findings_internal(monkeypatch) -> None:
    import nullion.deep_agent_runner as deep_agent_runner
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner

    monkeypatch.setenv("NULLION_DEEP_AGENTS_MODEL", "gpt-5-mini")

    class FakeDeepAgent:
        async def ainvoke(self, _payload, config=None):  # noqa: ANN001
            return {"content": [{"type": "text", "text": "draft output"}]}

    monkeypatch.setitem(
        sys.modules,
        "deepagents",
        SimpleNamespace(create_deep_agent=lambda **_kwargs: FakeDeepAgent()),
    )

    def fake_langchain_tools(_registry, **kwargs):  # noqa: ANN001, ANN003
        callback = kwargs["tool_result_callback"]
        callback(
            ToolResult(
                invocation_id="tool-1",
                tool_name="browser_extract_text",
                status="completed",
                output={"text": "verified listing mention"},
            )
        )
        return []

    monkeypatch.setattr(deep_agent_runner, "nullion_tools_as_langchain_tools", fake_langchain_tools)
    monkeypatch.setattr(deep_agent_runner, "_deep_agent_meta_tools", lambda *_args, **_kwargs: [])

    async def fail_completion(*_args, **_kwargs):  # noqa: ANN001
        return {"status": "failure", "answer": "Insufficient verified evidence."}

    async def best_effort_fallback(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("Rejected work must not be promoted by another summary")

    monkeypatch.setattr(deep_agent_runner, "_scheduled_task_completion_decision", fail_completion)
    monkeypatch.setattr(deep_agent_runner, "_fallback_answer_from_tool_evidence", best_effort_fallback)

    task = TaskRecord(
        task_id="t-best-effort",
        group_id="g1",
        conversation_id="telegram:1",
        principal_id="telegram:1",
        title="Search lodging options",
        description="Search listings and summarize verified findings.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_extract_text"],
        dependencies=[],
        metadata={"scheduled_task_run": True},
    )
    result = await DeepAgentMiniAgentRunner()._run_inner(
        MiniAgentConfig(agent_id="a1", task=task, can_request_user_input=False),
        anthropic_client=SimpleNamespace(create=lambda **_kwargs: {"content": []}),
        tool_registry=object(),
        policy_store=object(),
        context_bus=SimpleNamespace(get=lambda *_args, **_kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "failure"
    assert result.output is None and result.context_out is None and not result.artifacts
    assert result.error == "Insufficient verified evidence."


@pytest.mark.asyncio
async def test_scheduled_completion_failure_without_tool_evidence_stays_failure(monkeypatch) -> None:
    import nullion.deep_agent_runner as deep_agent_runner
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner

    monkeypatch.setenv("NULLION_DEEP_AGENTS_MODEL", "gpt-5-mini")

    class FakeDeepAgent:
        async def ainvoke(self, _payload, config=None):  # noqa: ANN001
            return {"content": [{"type": "text", "text": "draft output"}]}

    monkeypatch.setitem(
        sys.modules,
        "deepagents",
        SimpleNamespace(create_deep_agent=lambda **_kwargs: FakeDeepAgent()),
    )
    monkeypatch.setattr(deep_agent_runner, "nullion_tools_as_langchain_tools", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(deep_agent_runner, "_deep_agent_meta_tools", lambda *_args, **_kwargs: [])

    async def fail_completion(*_args, **_kwargs):  # noqa: ANN001
        return {"status": "failure", "answer": "Insufficient verified evidence."}

    async def unexpected_fallback(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("best-effort fallback should not run without completed tool evidence")

    monkeypatch.setattr(deep_agent_runner, "_scheduled_task_completion_decision", fail_completion)
    monkeypatch.setattr(deep_agent_runner, "_fallback_answer_from_tool_evidence", unexpected_fallback)

    task = TaskRecord(
        task_id="t-no-evidence",
        group_id="g1",
        conversation_id="telegram:1",
        principal_id="telegram:1",
        title="Search lodging options",
        description="Search listings and summarize verified findings.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_extract_text"],
        dependencies=[],
        metadata={"scheduled_task_run": True},
    )
    result = await DeepAgentMiniAgentRunner()._run_inner(
        MiniAgentConfig(agent_id="a1", task=task, can_request_user_input=False),
        anthropic_client=SimpleNamespace(create=lambda **_kwargs: {"content": []}),
        tool_registry=object(),
        policy_store=object(),
        context_bus=SimpleNamespace(get=lambda *_args, **_kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "failure"
    assert "Insufficient verified evidence." in str(result.error)

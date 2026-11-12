from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import pytest

from nullion.mini_agent_runner import MiniAgentConfig
from nullion.task_queue import TaskPriority, TaskRecord, TaskStatus
from nullion.tools import ToolResult


@pytest.mark.asyncio
async def test_scheduled_critique_preserves_producer_findings(monkeypatch) -> None:
    import nullion.deep_agent_runner as deep_agent_runner
    from nullion.deep_agent_runner import DeepAgentMiniAgentRunner

    monkeypatch.setenv("NULLION_DEEP_AGENTS_MODEL", "gpt-5-mini")

    class FakeDeepAgent:
        async def ainvoke(self, _payload, config=None):  # noqa: ANN001
            return {"content": [{"type": "text", "text": "Verified listing mention."}]}

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
        title="Summarize supplied findings",
        description="Summarize the supplied notes without new source research.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_extract_text"],
        dependencies=[],
        metadata={"scheduled_task_run": True},
    )
    def review_reply(**kwargs):
        import json
        if "material-requirements planner" in kwargs.get("system", ""):
            value = {"all_material_requirements_enumerated": True, "requested_artifact_extensions": [],
                "material_requirements": [{"requirement": "Verified listings", "requirement_kind": "fact",
                    "evidence_subject_id": "source", "eligible_evidence_tool_names": ["browser_extract_text"]}]}
        else:
            value = {"disposition": "blocked", "unresolved_requirements": ["Insufficient verified evidence."],
                "user_facing_failure": "Insufficient verified evidence.", "retry_tool_names": []}
        return {"content": [{"type": "text", "text": json.dumps(value)}]}
    result = await DeepAgentMiniAgentRunner().run(
        MiniAgentConfig(agent_id="a1", task=task, can_request_user_input=False),
        anthropic_client=SimpleNamespace(create=review_reply),
        tool_registry=object(),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(get=lambda *_args, **_kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "success"
    assert result.output == "Verified listing mention."
    assert result.error is None
    assert result.report_presentation["outcome_review_role"] == "helper"


@pytest.mark.asyncio
async def test_scheduled_synthesis_does_not_need_editor_permission(monkeypatch) -> None:
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
        title="Summarize supplied findings",
        description="Summarize the supplied notes without new source research.",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=["browser_extract_text"],
        dependencies=[],
        metadata={"scheduled_task_run": True},
    )
    result = await DeepAgentMiniAgentRunner().run(
        MiniAgentConfig(agent_id="a1", task=task, can_request_user_input=False),
        anthropic_client=SimpleNamespace(create=lambda **_kwargs: {"content": []}),
        tool_registry=object(),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(get=lambda *_args, **_kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "success"
    assert result.output == "draft output" and result.error is None
    assert result.report_presentation["outcome_review_role"] == "helper"

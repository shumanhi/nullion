from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from nullion.mini_agent_runner import (
    MiniAgentConfig,
    MiniAgentRunner,
    ProgressUpdate,
    _build_tool_list,
    _emit,
    _model_create,
    _tool_result,
)
from nullion.context_bus import ContextBus
from nullion.response_sanitizer import sanitize_user_visible_reply, user_requested_raw_output
from nullion.response_fulfillment_contract import (
    artifact_paths_from_tool_results,
    evaluate_response_fulfillment,
    guaranteed_user_visible_text,
    user_visible_text_from_output,
)
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)
from nullion.task_queue import TaskPriority, TaskRecord, TaskStatus
from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


def test_user_visible_text_extracts_from_common_shapes_and_fallbacks() -> None:
    assert user_visible_text_from_output(" hello ") == "hello"
    assert user_visible_text_from_output({"result": {"summary": " nested "}}) == "nested"
    assert user_visible_text_from_output(SimpleNamespace(final_text="final")) == "final"
    assert user_visible_text_from_output({"message": "(no reply)"}) == ""
    assert guaranteed_user_visible_text(subject=SimpleNamespace(name="Build"), output={}, kind="task").startswith(
        "Task 'Build' completed"
    )
    assert guaranteed_user_visible_text(subject="", output={"text": "done"}, kind="task") == "done"


def test_artifact_paths_from_completed_tool_results_are_deduped() -> None:
    results = [
        ToolResult("1", "file_write", "success", {"path": "/tmp/a.txt"}),
        ToolResult("2", "render", "completed", {"artifact_path": "/tmp/b.txt", "artifact_paths": ["/tmp/a.txt", "/tmp/c.txt"]}),
        ToolResult("3", "render", "failed", {"artifact_path": "/tmp/ignored.txt"}),
        ToolResult("4", "render", "completed", {"artifacts": ["/tmp/c.txt", "", 42]}),
    ]

    assert artifact_paths_from_tool_results(results) == ["/tmp/a.txt", "/tmp/b.txt", "/tmp/c.txt"]


class Store:
    def __init__(self, frame: TaskFrame | None = None) -> None:
        self.frame = frame

    def get_active_task_frame_id(self, conversation_id: str) -> str | None:
        return self.frame.frame_id if self.frame and conversation_id == self.frame.conversation_id else None

    def get_task_frame(self, frame_id: str) -> TaskFrame | None:
        return self.frame if self.frame and self.frame.frame_id == frame_id else None


def frame(**kwargs) -> TaskFrame:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    defaults = dict(
        frame_id="f1",
        conversation_id="c1",
        branch_id="b1",
        source_turn_id="t1",
        parent_frame_id=None,
        status=TaskFrameStatus.ACTIVE,
        operation=TaskFrameOperation.GENERATE_ARTIFACT,
        target=TaskFrameTarget(kind="file", value="report"),
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(artifact_kind="txt", delivery_mode="attachment"),
        finish=TaskFrameFinishCriteria(
            requires_artifact_delivery=True,
            required_artifact_kind="txt",
            required_tool_completion=("file_write",),
        ),
        summary="write report",
        created_at=now,
        updated_at=now,
    )
    defaults.update(kwargs)
    return TaskFrame(**defaults)


def test_response_fulfillment_blocks_completion_claims_without_required_evidence(tmp_path) -> None:
    decision = evaluate_response_fulfillment(
        store=Store(frame()),
        conversation_id="c1",
        user_message="save this as a txt file",
        reply="Done, I saved it.",
        tool_results=[],
        artifact_roots=[tmp_path],
    )

    assert decision.satisfied is False
    assert "required tool completion: file_write" in decision.missing_requirements
    assert "txt attachment" in decision.missing_requirements
    assert "mark this complete yet" in decision.reply


def test_response_fulfillment_accepts_valid_artifact_or_platform_attachment(tmp_path) -> None:
    artifact = tmp_path / "nullion-artifact-report.txt"
    artifact.write_text("hello", encoding="utf-8")
    tool_result = ToolResult("1", "file_write", "completed", {"path": str(artifact)})

    decision = evaluate_response_fulfillment(
        store=Store(frame()),
        conversation_id="c1",
        user_message="save this as a txt file",
        reply="Done, attached.",
        tool_results=[tool_result],
        artifact_roots=[tmp_path],
    )
    assert decision.satisfied is True

    platform_decision = evaluate_response_fulfillment(
        store=Store(frame(finish=TaskFrameFinishCriteria(requires_artifact_delivery=True))),
        conversation_id="c1",
        user_message="send a file",
        reply="Done.",
        platform_artifact_count=1,
        artifact_roots=[tmp_path],
    )
    assert platform_decision.satisfied is True


def test_response_fulfillment_rejects_wrong_required_artifact_extension(tmp_path) -> None:
    html_artifact = tmp_path / "nullion-artifact-report.html"
    html_artifact.write_text("<html><body>report</body></html>", encoding="utf-8")
    tool_result = ToolResult("1", "file_write", "completed", {"path": str(html_artifact)})

    decision = evaluate_response_fulfillment(
        store=Store(frame(finish=TaskFrameFinishCriteria(requires_artifact_delivery=True, required_artifact_kind="pdf"))),
        conversation_id="c1",
        user_message="send this as a pdf",
        reply=f"Done, attached the requested PDF.\nMEDIA:{html_artifact}",
        tool_results=[tool_result],
        artifact_roots=[tmp_path],
    )

    assert decision.satisfied is False
    assert "pdf attachment" in decision.missing_requirements


def test_response_fulfillment_handles_requested_deliverable_without_active_frame(tmp_path) -> None:
    no_claim = evaluate_response_fulfillment(
        store=Store(None),
        conversation_id="c1",
        user_message="please send the image file",
        reply="I can do that next.",
        tool_results=[ToolResult("1", "file_write", "completed", {"path": str(tmp_path / "missing.png")})],
        artifact_roots=[tmp_path],
    )
    assert no_claim.satisfied is False
    assert no_claim.reply == "I can do that next."

    blocked = evaluate_response_fulfillment(
        store=Store(None),
        conversation_id="c1",
        user_message="please send the image file",
        reply="Done, generated.",
        tool_results=[],
        artifact_paths=[str(tmp_path / "missing.png")],
        artifact_roots=[tmp_path],
    )
    assert blocked.satisfied is False
    assert "mark this complete yet" in blocked.reply


def test_build_tool_list_supports_modern_legacy_and_broken_registries() -> None:
    class Modern:
        def list_tool_definitions(self, *, allowed):
            return [{"name": name} for name in allowed]

    class Legacy:
        def list_tool_definitions(self):
            return [{"name": "a"}, {"name": "b"}]

    class Broken:
        def list_tool_definitions(self, *args, **kwargs):
            raise RuntimeError("boom")

    assert [item["name"] for item in _build_tool_list(Modern(), allowed=["a"])][:1] == ["a"]
    assert [item["name"] for item in _build_tool_list(Legacy(), allowed=["b"])][:1] == ["b"]
    assert [item["name"] for item in _build_tool_list(Broken(), allowed=["a"])] == [
        "report_progress",
        "request_user_input",
    ]


def test_raw_structured_tool_payload_is_blocked_unless_requested() -> None:
    payload = {
        "directory_count": 1622,
        "file_count": 15533,
        "roots": ["/Users/example/.nullion"],
        "sample_files": ["/Users/example/.nullion::.env"],
    }
    result = ToolResult("inv", "workspace_summary", "completed", payload)
    raw_reply = '{"directory_count": 1622, "file_count": 15533, "roots": ["/Users/example/.nullion"], "sample_files": ["/Users/example/.nullion::.env"]}'

    sanitized = sanitize_user_visible_reply(
        user_message="Do u have the git repo ready?",
        reply=raw_reply,
        tool_results=[result],
        source="agent",
    )

    assert sanitized is not None
    assert "blocked a raw structured payload" in sanitized
    assert "/Users/example" not in sanitized
    assert sanitize_user_visible_reply(
        user_message="show me the raw JSON output",
        reply=raw_reply,
        tool_results=[result],
    ) == raw_reply
    assert user_requested_raw_output("please dump the raw tool result as JSON") is True


def test_sanitizer_humanizes_file_search_payload_without_paths() -> None:
    result = ToolResult(
        "search-1",
        "file_search",
        "completed",
        {
            "matches": [
                "/Users/example/.nullion/workspaces/workspace_admin/artifacts/provincetown_airbnb_monitor_2026-04-30_images-fixed.html"
            ]
        },
    )
    raw_reply = (
        '{"matches": ["/Users/example/.nullion/workspaces/workspace_admin/artifacts/'
        'provincetown_airbnb_monitor_2026-04-30_images-fixed.html"]}'
    )

    sanitized = sanitize_user_visible_reply(
        user_message="Images are missing",
        reply=raw_reply,
        tool_results=[result],
        source="agent",
    )

    assert sanitized == (
        "I found 1 matching file: `provincetown_airbnb_monitor_2026-04-30_images-fixed.html`."
    )
    assert "/Users/example" not in sanitized
    assert "blocked a raw structured payload" not in sanitized


def test_sanitizer_covers_expected_and_unexpected_payload_shapes() -> None:
    sensitive_result = ToolResult(
        "inv-sensitive",
        "workspace_summary",
        "completed",
        {"directory_count": 1, "roots": ["/Users/example/.nullion"], "sample_files": ["/Users/example/.nullion::.env"]},
    )
    benign_result = ToolResult("inv-benign", "custom_tool", "completed", {"answer": 42})

    expected_passthroughs = [
        "Repo is ready: branch main is clean and origin is configured.",
        'Here is the small JSON example you asked about: {"ok": true}',
        '{"ok": true}',
    ]
    for reply in expected_passthroughs:
        assert sanitize_user_visible_reply(
            user_message="is it ready?",
            reply=reply,
            tool_results=[],
        ) == reply

    blocked_replies = [
        '{"answer": 42}',
        "```json\n{\"directory_count\": 1, \"roots\": [\"/Users/example/.nullion\"]}\n```",
        "{'directory_count': 1, 'sample_files': ['/Users/example/.nullion::.env']}",
        '[{"path": "/Users/example/.nullion/.env"}]',
        '{"status": "completed", "output": {"sample_files": ["/Users/example/.nullion::.env"]}}',
    ]
    for reply in blocked_replies:
        sanitized = sanitize_user_visible_reply(
            user_message="is it ready?",
            reply=reply,
            tool_results=[sensitive_result, benign_result],
            source="agent",
        )
        assert sanitized is not None
        assert "blocked a raw structured payload" in sanitized
        assert "/Users/example" not in sanitized

    raw_allowed = sanitize_user_visible_reply(
        user_message="return the exact raw payload as JSON",
        reply='{"status": "completed", "output": {"sample_files": ["/Users/example/.nullion::.env"]}}',
        tool_results=[sensitive_result],
    )
    assert raw_allowed == '{"status": "completed", "output": {"sample_files": ["/Users/example/.nullion::.env"]}}'
    assert user_requested_raw_output("please summarize the JSON instead") is False


def test_agent_structured_tool_fallback_does_not_dump_json() -> None:
    from nullion.agent_orchestrator import _tool_result_structured_text

    result = ToolResult(
        "inv",
        "workspace_summary",
        "completed",
        {"directory_count": 2, "sample_files": ["/tmp/private.env"]},
    )

    text = _tool_result_structured_text([result])

    assert text is not None
    assert "blocked a raw structured payload" in text
    assert "private.env" not in text


def test_agent_final_raw_tool_payload_is_sanitized() -> None:
    from nullion.agent_orchestrator import AgentOrchestrator

    payload = {"directory_count": 1, "sample_files": ["/tmp/private.env"]}

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
                            "id": "tu-1",
                            "name": "workspace_summary",
                            "input": {},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": str(payload)}]}

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="workspace_summary",
            description="Summarize workspace",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=5,
        ),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", payload),
    )

    result = AgentOrchestrator(model_client=Client()).run_turn(
        conversation_id="telegram:123",
        principal_id="telegram_chat",
        user_message="Do u have the git repo ready?",
        conversation_history=[],
        tool_registry=registry,
        policy_store=None,
        approval_store=None,
    )

    assert result.final_text is not None
    assert "blocked a raw structured payload" in result.final_text
    assert "private.env" not in result.final_text


@pytest.mark.asyncio
async def test_mini_agent_sanitizes_raw_final_tool_output() -> None:
    payload = {"directory_count": 1, "sample_files": ["/tmp/secret.env"]}

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
                            "id": "tu-1",
                            "name": "workspace_summary",
                            "input": {},
                        }
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": str(payload)}]}

    class Registry:
        def list_tool_definitions(self, *, allowed):
            return [{"name": name, "input_schema": {"type": "object", "properties": {}}} for name in allowed]

        def invoke(self, invocation):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", payload)

        def run_cleanup_hooks(self, *, scope_id):
            return None

    task = TaskRecord(
        task_id="task-1",
        group_id="group-1",
        conversation_id="conv",
        principal_id="telegram_chat",
        title="Check repo",
        description="Do u have the git repo ready?",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["workspace_summary"],
        dependencies=[],
    )

    result = await MiniAgentRunner().run(
        MiniAgentConfig(agent_id="agent-1", task=task),
        anthropic_client=Client(),
        tool_registry=Registry(),
        policy_store=None,
        approval_store=None,
        context_bus=ContextBus(),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "success"
    assert result.output is not None
    assert "blocked a raw structured payload" in result.output
    assert "secret.env" not in result.output


@pytest.mark.asyncio
async def test_mini_agent_empty_final_answer_is_failure() -> None:
    class EmptyFinalClient:
        def create(self, **kwargs):  # noqa: ANN002, ANN003
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "   "}]}

    task = TaskRecord(
        task_id="t1",
        group_id="g1",
        conversation_id="c1",
        principal_id="p1",
        title="Empty final",
        description="Return nothing",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )

    result = await MiniAgentRunner()._inner_loop(
        MiniAgentConfig(agent_id="a1", task=task),
        anthropic_client=EmptyFinalClient(),
        tool_registry=SimpleNamespace(list_tool_definitions=lambda **kwargs: []),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(get=lambda *args, **kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "failure"
    assert result.error == "Agent finished without a final answer."
    assert result.output is None


@pytest.mark.asyncio
async def test_mini_agent_can_continue_once_after_iteration_tranche() -> None:
    class ContinuingClient:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs):  # noqa: ANN002, ANN003
            self.calls += 1
            if self.calls == 1:
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {"type": "tool_use", "id": "tool-1", "name": "file_read", "input": {"path": "/tmp/a"}}
                    ],
                }
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": "done after continuation"}]}

    class Registry:
        def list_tool_definitions(self, *, allowed):  # noqa: ANN002, ANN003
            return [{"name": name} for name in allowed]

        def invoke(self, invocation):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", "tool output")

    task = TaskRecord(
        task_id="t1",
        group_id="g1",
        conversation_id="c1",
        principal_id="p1",
        title="Continue",
        description="Use a tool and finish",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read"],
        dependencies=[],
    )
    client = ContinuingClient()
    progress_queue: asyncio.Queue = asyncio.Queue()

    result = await MiniAgentRunner()._inner_loop(
        MiniAgentConfig(agent_id="a1", task=task, max_iterations=1, max_continuations=1),
        anthropic_client=client,
        tool_registry=Registry(),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(get=lambda *args, **kwargs: None),
        progress_queue=progress_queue,
    )

    assert result.status == "success"
    assert result.output == "done after continuation"
    assert client.calls == 2
    progress_messages = [progress_queue.get_nowait().message for _ in range(progress_queue.qsize())]
    assert "Continuing after 1 tool steps." in progress_messages


@pytest.mark.asyncio
async def test_mini_agent_max_iterations_still_stops_without_continuation_budget() -> None:
    class LoopingClient:
        def create(self, **kwargs):  # noqa: ANN002, ANN003
            return {
                "stop_reason": "tool_use",
                "content": [{"type": "tool_use", "id": "tool-1", "name": "file_read", "input": {"path": "/tmp/a"}}],
            }

    class Registry:
        def list_tool_definitions(self, *, allowed):  # noqa: ANN002, ANN003
            return [{"name": name} for name in allowed]

        def invoke(self, invocation):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", "tool output")

    task = TaskRecord(
        task_id="t1",
        group_id="g1",
        conversation_id="c1",
        principal_id="p1",
        title="Stop",
        description="Never finish",
        status=TaskStatus.RUNNING,
        priority=TaskPriority.NORMAL,
        allowed_tools=["file_read"],
        dependencies=[],
    )

    result = await MiniAgentRunner()._inner_loop(
        MiniAgentConfig(agent_id="a1", task=task, max_iterations=1, max_continuations=0),
        anthropic_client=LoopingClient(),
        tool_registry=Registry(),
        policy_store=None,
        approval_store=None,
        context_bus=SimpleNamespace(get=lambda *args, **kwargs: None),
        progress_queue=asyncio.Queue(),
    )

    assert result.status == "partial"
    assert result.output == "Reached max iterations (1) without completing."


@pytest.mark.asyncio
async def test_model_create_supports_sync_async_and_anthropic_style_clients() -> None:
    class SyncClient:
        def create(self, **kwargs):
            return {"content": [{"type": "text", "text": kwargs["prompt"]}]}

    class AsyncClient:
        async def create(self, **kwargs):
            return {"content": [{"type": "text", "text": kwargs["prompt"]}]}

    class Block:
        type = "text"
        text = "anthropic text"

    class ToolBlock:
        type = "tool_use"
        id = "tool-1"
        name = "file_read"
        input = {"path": "x"}

    class Messages:
        async def create(self, **kwargs):
            return SimpleNamespace(stop_reason="end_turn", content=[Block(), ToolBlock()])

    assert await _model_create(SyncClient(), prompt="sync") == {"content": [{"type": "text", "text": "sync"}]}
    assert await _model_create(AsyncClient(), prompt="async") == {"content": [{"type": "text", "text": "async"}]}
    assert await _model_create(SimpleNamespace(messages=Messages()), tools=[]) == {
        "stop_reason": "end_turn",
        "content": [
            {"type": "text", "text": "anthropic text"},
            {"type": "tool_use", "id": "tool-1", "name": "file_read", "input": {"path": "x"}},
        ],
    }


@pytest.mark.asyncio
async def test_emit_drops_when_queue_is_full_and_tool_result_marks_errors() -> None:
    queue: asyncio.Queue = asyncio.Queue(maxsize=1)
    update = ProgressUpdate(agent_id="a", task_id="t", group_id="g", kind="progress_note")

    await _emit(queue, update)
    await _emit(queue, update)

    assert queue.qsize() == 1
    assert queue.get_nowait() is update
    assert _tool_result("tool-1", "ok") == {
        "type": "tool_result",
        "tool_use_id": "tool-1",
        "content": [{"type": "text", "text": "ok"}],
    }
    assert _tool_result("tool-1", "bad", is_error=True)["is_error"] is True

"""Mini-agent runner for a single scoped task."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from nullion.context_bus import ContextBus
from nullion.mini_agent_config import (
    mini_agent_max_continuations,
    mini_agent_max_iterations,
    mini_agent_timeout_seconds,
)
from nullion.task_queue import TaskRecord, TaskResult


@dataclass
class MiniAgentConfig:
    agent_id: str
    task: TaskRecord
    context_in: Any | None = None
    max_iterations: int = field(default_factory=mini_agent_max_iterations)
    max_continuations: int = field(default_factory=mini_agent_max_continuations)
    timeout_s: float = field(default_factory=mini_agent_timeout_seconds)
    can_request_user_input: bool = True
    depth: int = 0


class SubAgentDepthError(RuntimeError):
    """Raised when a mini-agent would exceed the maximum nesting depth."""


class MiniAgentRunner:
    """Runs one task asynchronously through the Deep Agents harness."""

    async def run(
        self,
        config: MiniAgentConfig,
        *,
        anthropic_client: Any,
        tool_registry: Any,
        policy_store: Any,
        approval_store: Any,
        context_bus: ContextBus,
        progress_queue: asyncio.Queue,
    ) -> TaskResult:
        if config.depth >= 3:
            raise SubAgentDepthError(
                f"Agent {config.agent_id} reached max nesting depth {config.depth}"
            )

        from nullion.deep_agent_runner import DeepAgentMiniAgentRunner

        task_metadata = getattr(config.task, "metadata", None)
        task_metadata = task_metadata if isinstance(task_metadata, dict) else {}
        if task_metadata.get("scheduled_task_run"):
            return await self._run_scheduled_task_off_loop(
                config,
                anthropic_client=anthropic_client,
                tool_registry=tool_registry,
                policy_store=policy_store,
                approval_store=approval_store,
                context_bus=context_bus,
                progress_queue=progress_queue,
            )

        return await DeepAgentMiniAgentRunner().run(
            config,
            anthropic_client=anthropic_client,
            tool_registry=tool_registry,
            policy_store=policy_store,
            approval_store=approval_store,
            context_bus=context_bus,
            progress_queue=progress_queue,
        )

    async def _run_scheduled_task_off_loop(
        self,
        config: MiniAgentConfig,
        *,
        anthropic_client: Any,
        tool_registry: Any,
        policy_store: Any,
        approval_store: Any,
        context_bus: ContextBus,
        progress_queue: asyncio.Queue,
    ) -> TaskResult:
        """Run scheduled DeepAgent work away from the dispatcher event loop.

        Some LangGraph/DeepAgents providers and tool adapters can block while
        streaming graph events. Cron runs need the dispatcher loop to remain
        responsive so supervision, timeouts, and planner-card updates keep
        working even when one scheduled subtask stalls.
        """

        from nullion.deep_agent_runner import DeepAgentMiniAgentRunner

        dispatcher_loop = asyncio.get_running_loop()
        bridged_progress = _ThreadsafeProgressQueue(progress_queue, dispatcher_loop)

        def _run_in_thread() -> TaskResult:
            return asyncio.run(
                DeepAgentMiniAgentRunner().run(
                    config,
                    anthropic_client=anthropic_client,
                    tool_registry=tool_registry,
                    policy_store=policy_store,
                    approval_store=approval_store,
                    context_bus=context_bus,
                    progress_queue=bridged_progress,
                )
            )

        try:
            return await asyncio.to_thread(_run_in_thread)
        finally:
            bridged_progress.close()


class _ThreadsafeProgressQueue:
    def __init__(self, target: asyncio.Queue, loop: asyncio.AbstractEventLoop) -> None:
        self._target = target
        self._loop = loop
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def put_nowait(self, item: Any) -> None:
        if self._closed or self._loop.is_closed():
            return

        def _put() -> None:
            if self._closed:
                return
            self._target.put_nowait(item)

        self._loop.call_soon_threadsafe(_put)


@dataclass
class ProgressUpdate:
    agent_id: str
    task_id: str
    group_id: str
    kind: str
    message: str | None = None
    data: dict | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


__all__ = [
    "MiniAgentConfig",
    "MiniAgentRunner",
    "ProgressUpdate",
    "SubAgentDepthError",
]

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

        return await DeepAgentMiniAgentRunner().run(
            config,
            anthropic_client=anthropic_client,
            tool_registry=tool_registry,
            policy_store=policy_store,
            approval_store=approval_store,
            context_bus=context_bus,
            progress_queue=progress_queue,
        )


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

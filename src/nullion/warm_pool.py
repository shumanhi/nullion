"""Warm agent pool — pre-initialized agents ready to accept tasks immediately.

The pool maintains a floor of min_size idle agents so task assignment is
instantaneous (< 50 ms). Heavy resources (model client, HTTP session) are
shared via SharedResources and initialized once at startup.

Usage::

    pool = WarmAgentPool(min_size=3, max_size=20)
    await pool.start(settings)          # starts _maintain_loop background task
    agent = await pool.acquire()
    # ... assign task, run mini_agent_runner ...
    pool.release(agent)
    await pool.stop()
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

EVICT_AFTER_TASKS: int = 100


class AgentState(str, Enum):
    IDLE      = "idle"        # in pool, ready to accept a task
    PRIMED    = "primed"      # pre-warmed with a likely tool scope
    ASSIGNED  = "assigned"    # executing a task
    RECYCLING = "recycling"   # post-task reset, not yet back in pool


@dataclass
class PooledAgent:
    agent_id: str
    state: AgentState
    # Shared (not owned per-agent):
    anthropic_client: Any                  # AsyncAnthropic or sync adapter
    # Per-agent, zeroed on recycle:
    messages: list[dict] = field(default_factory=list)
    current_task_id: str | None = None
    task_count: int = 0
    primed_for: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_used_at: datetime | None = None


# ── Shared resources ───────────────────────────────────────────────────────────

class SharedResources:
    """Singleton: one model client reused across all pool agents."""

    _client: Any = None
    _init_lock: asyncio.Lock | None = None

    @classmethod
    async def get_client(cls) -> Any:
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        async with cls._init_lock:
            if cls._client is None:
                cls._client = cls._build_client()
        return cls._client

    @classmethod
    def _build_client(cls) -> Any:
        api_key_oai = os.environ.get("OPENAI_API_KEY")
        if api_key_oai:
            return _build_sync_openai_adapter(api_key_oai)
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if api_key:
            try:
                import anthropic
                return anthropic.AsyncAnthropic(api_key=api_key)
            except ImportError:
                pass
        raise RuntimeError(
            "No model provider configured. Set OPENAI_API_KEY or ANTHROPIC_API_KEY."
        )

    @classmethod
    def reset(cls) -> None:
        """Reset shared state (used in tests)."""
        cls._client = None
        cls._init_lock = None


# ── Pool ───────────────────────────────────────────────────────────────────────

class WarmAgentPool:
    """Pre-warmed pool of PooledAgents ready for task assignment."""

    def __init__(
        self,
        *,
        min_size: int = 3,
        max_size: int = 20,
        acquire_timeout_s: float = 0.5,
        shared_client: Any | None = None,
    ) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self.acquire_timeout_s = acquire_timeout_s
        self._shared_client = shared_client

        self._pool: list[PooledAgent] = []
        self._lock = asyncio.Lock()
        self._available = asyncio.Event()
        self._running = False
        self._maintain_task: asyncio.Task | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Initialise the pool to min_size and start the maintenance loop."""
        self._running = True
        client = self._shared_client or await SharedResources.get_client()
        async with self._lock:
            while len(self._pool) < self.min_size:
                agent = _spawn_agent(client)
                self._pool.append(agent)
        self._available.set()
        self._maintain_task = asyncio.create_task(self._maintain_loop(), name="warm-pool-maintain")
        logger.debug("WarmAgentPool: started with %d agents", len(self._pool))

    async def stop(self) -> None:
        self._running = False
        if self._maintain_task is not None:
            self._maintain_task.cancel()
            try:
                await self._maintain_task
            except asyncio.CancelledError:
                pass

    # ── Acquire / Release ──────────────────────────────────────────────────

    async def acquire(
        self,
        *,
        preferred_tools: list[str] | None = None,
        task_id: str | None = None,
    ) -> PooledAgent:
        """Return a warm agent. Prefers agents primed for *preferred_tools*.

        Waits up to *acquire_timeout_s*. Falls back to spawning a cold agent
        if the pool is exhausted.
        """
        deadline = asyncio.get_event_loop().time() + self.acquire_timeout_s
        while True:
            async with self._lock:
                agent = self._pick(preferred_tools)
                if agent is not None:
                    agent.state = AgentState.ASSIGNED
                    agent.current_task_id = task_id
                    agent.last_used_at = datetime.now(timezone.utc)
                    return agent

            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(asyncio.shield(self._available.wait()), timeout=remaining)
                self._available.clear()
            except asyncio.TimeoutError:
                break

        # Pool exhausted — spawn cold agent.
        logger.warning("WarmAgentPool: pool exhausted, spawning cold agent")
        client = self._shared_client or await SharedResources.get_client()
        agent = _spawn_agent(client)
        agent.state = AgentState.ASSIGNED
        agent.current_task_id = task_id
        agent.last_used_at = datetime.now(timezone.utc)
        return agent

    def release(self, agent: PooledAgent) -> None:
        """Reset agent state and return to pool (or discard if past eviction threshold)."""
        agent.state = AgentState.RECYCLING
        agent.messages.clear()
        agent.current_task_id = None
        agent.primed_for = []
        agent.task_count += 1

        if agent.task_count >= EVICT_AFTER_TASKS:
            logger.debug("WarmAgentPool: evicting agent %s (task_count=%d)", agent.agent_id, agent.task_count)
            # Remove from pool if present; _maintain_loop will respawn.
            try:
                self._pool.remove(agent)
            except ValueError:
                pass
        else:
            agent.state = AgentState.IDLE
            if agent not in self._pool:
                self._pool.append(agent)
            self._available.set()

    async def prime(self, likely_tools: list[str]) -> None:
        """Mark an idle agent as PRIMED for the given tool scope."""
        async with self._lock:
            for agent in self._pool:
                if agent.state == AgentState.IDLE:
                    agent.state = AgentState.PRIMED
                    agent.primed_for = list(likely_tools)
                    logger.debug("WarmAgentPool: primed agent %s for %s", agent.agent_id, likely_tools)
                    return

    def _pick(self, preferred_tools: list[str] | None = None) -> PooledAgent | None:
        """Choose an idle agent, preferring one primed for the requested tools."""
        preferred = set(preferred_tools or [])
        candidates = [
            agent
            for agent in self._pool
            if agent.state in {AgentState.IDLE, AgentState.PRIMED}
        ]
        if not candidates:
            return None
        if preferred:
            for agent in candidates:
                if agent.state == AgentState.PRIMED and preferred.issubset(set(agent.primed_for)):
                    return agent
            for agent in candidates:
                if agent.state == AgentState.PRIMED and preferred.intersection(agent.primed_for):
                    return agent
        for agent in candidates:
            if agent.state == AgentState.IDLE:
                return agent
        return candidates[0]

    # ── Status ─────────────────────────────────────────────────────────────

    def pool_size(self) -> int:
        return len(self._pool)

    def idle_count(self) -> int:
        return sum(1 for a in self._pool if a.state in {AgentState.IDLE, AgentState.PRIMED})

    def assigned_count(self) -> int:
        return sum(1 for a in self._pool if a.state == AgentState.ASSIGNED)

    # ── Maintenance loop ───────────────────────────────────────────────────

    async def _maintain_loop(self) -> None:
        while self._running:
            try:
                async with self._lock:
                    idle = sum(1 for a in self._pool if a.state in {AgentState.IDLE, AgentState.PRIMED})
                    total = len(self._pool)
                    needed = self.min_size - idle
                    if needed > 0 and total < self.max_size:
                        client = self._shared_client or await SharedResources.get_client()
                        for _ in range(min(needed, self.max_size - total)):
                            agent = _spawn_agent(client)
                            self._pool.append(agent)
                            logger.debug("WarmAgentPool: spawned replacement agent %s", agent.agent_id)
                        self._available.set()
            except Exception as exc:
                logger.debug("WarmAgentPool maintain error: %s", exc)
            await asyncio.sleep(1.0)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _spawn_agent(client: Any) -> PooledAgent:
    return PooledAgent(
        agent_id=f"agent-{uuid4().hex[:10]}",
        state=AgentState.IDLE,
        anthropic_client=client,
    )


def _build_sync_openai_adapter(api_key: str) -> Any:
    """Build a minimal synchronous OpenAI adapter compatible with the runner."""
    try:
        import openai
    except ImportError:
        raise RuntimeError("openai package not installed")

    import json as _json
    client = openai.OpenAI(api_key=api_key)
    model = os.environ.get("NULLION_MODEL", "gpt-4o")

    class _Adapter:
        async def messages_create(self, **kwargs) -> Any:
            msgs = list(kwargs.get("messages", []))
            system = kwargs.get("system")
            if system:
                msgs = [{"role": "system", "content": system}] + msgs
            tools_raw = kwargs.get("tools") or []
            kw: dict = dict(model=model, messages=msgs, max_tokens=kwargs.get("max_tokens", 2048))
            if tools_raw:
                kw["tools"] = [
                    {"type": "function", "function": {
                        "name": t["name"],
                        "description": t.get("description", ""),
                        "parameters": t.get("input_schema", {}),
                    }}
                    for t in tools_raw
                ]
                kw["tool_choice"] = "auto"

            import asyncio as _asyncio
            loop = _asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, lambda: client.chat.completions.create(**kw))
            choice = resp.choices[0]
            msg = choice.message
            content = []
            if msg.content:
                content.append({"type": "text", "text": msg.content})
            if msg.tool_calls:
                from nullion.model_clients import parse_tool_arguments

                for tc in msg.tool_calls:
                    content.append({
                        "type": "tool_use", "id": tc.id,
                        "name": tc.function.name,
                        "input": parse_tool_arguments(tc.function.arguments),
                    })

            class _Resp:
                stop_reason = "tool_use" if msg.tool_calls else "end_turn"
                pass
            _Resp.content = content
            return _Resp()

        @property
        def messages(self):
            return self

        async def create(self, **kwargs):
            return await self.messages_create(**kwargs)

    return _Adapter()


class _PooledAgent:
    """WarmAgentPool picks agents; this gives Deep Agents a compatible client."""
    def __init__(self, agent: PooledAgent) -> None:
        self._agent = agent

    async def create(self, **kwargs) -> Any:
        client = self._agent.anthropic_client
        if hasattr(client, "messages") and hasattr(client.messages, "create"):
            resp = await client.messages.create(**kwargs)
            return {
                "stop_reason": resp.stop_reason,
                "content": [
                    {"type": b.type, "text": b.text} if b.type == "text"
                    else {"type": b.type, "id": b.id, "name": b.name, "input": b.input}
                    for b in resp.content
                ],
            }
        # Sync adapter
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: client.create(**kwargs))


def get_agent_client(agent: PooledAgent) -> Any:
    """Return a client wrapper suitable for the Deep Agents model adapter."""
    return _PooledAgent(agent)


__all__ = [
    "AgentState",
    "PooledAgent",
    "WarmAgentPool",
    "SharedResources",
    "get_agent_client",
    "EVICT_AFTER_TASKS",
]

"""Configuration helpers for Mini-Agent execution and reconciliation."""

from __future__ import annotations

from datetime import timedelta
import os


DEFAULT_MINI_AGENT_TIMEOUT_SECONDS = 180.0
DEFAULT_MINI_AGENT_MAX_ITERATIONS = 12
DEFAULT_MINI_AGENT_MAX_CONTINUATIONS = 1
DEFAULT_MINI_AGENT_STALE_AFTER_SECONDS = 600.0


def _float_env(name: str, default: float, *, minimum: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _int_env(name: str, default: int, *, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def mini_agent_timeout_seconds() -> float:
    return _float_env(
        "NULLION_MINI_AGENT_TIMEOUT_SECONDS",
        DEFAULT_MINI_AGENT_TIMEOUT_SECONDS,
        minimum=1.0,
    )


def mini_agent_max_iterations() -> int:
    return _int_env(
        "NULLION_MINI_AGENT_MAX_ITERATIONS",
        DEFAULT_MINI_AGENT_MAX_ITERATIONS,
        minimum=1,
    )


def mini_agent_max_continuations() -> int:
    return _int_env(
        "NULLION_MINI_AGENT_MAX_CONTINUATIONS",
        DEFAULT_MINI_AGENT_MAX_CONTINUATIONS,
        minimum=0,
    )


def mini_agent_stale_after_seconds() -> float:
    return _float_env(
        "NULLION_MINI_AGENT_STALE_AFTER_SECONDS",
        DEFAULT_MINI_AGENT_STALE_AFTER_SECONDS,
        minimum=1.0,
    )


def mini_agent_stale_after() -> timedelta:
    return timedelta(seconds=mini_agent_stale_after_seconds())


__all__ = [
    "DEFAULT_MINI_AGENT_MAX_CONTINUATIONS",
    "DEFAULT_MINI_AGENT_MAX_ITERATIONS",
    "DEFAULT_MINI_AGENT_STALE_AFTER_SECONDS",
    "DEFAULT_MINI_AGENT_TIMEOUT_SECONDS",
    "mini_agent_max_continuations",
    "mini_agent_max_iterations",
    "mini_agent_stale_after",
    "mini_agent_stale_after_seconds",
    "mini_agent_timeout_seconds",
]

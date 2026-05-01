"""Routing policy for when not to spend a mini-agent run."""

from __future__ import annotations


def should_route_without_mini_agents(message: str, *, has_attachments: bool = False) -> bool:
    """Return True only for structural cases that should never fork mini-agents."""

    return bool(has_attachments)


__all__ = ["should_route_without_mini_agents"]

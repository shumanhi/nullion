"""Compatibility facade for the shared platform chat ingress.

The implementation lives in :mod:`nullion.messaging_turn_graph` so Web,
Telegram, Slack, and Discord share one typed chat ingress contract. Keep this
module for existing imports and monkeypatch points.
"""

from __future__ import annotations

from nullion import messaging_turn_graph as _impl

PlatformChatRequest = _impl.PlatformChatRequest
PlatformChatResponse = _impl.PlatformChatResponse
platform_chat_id = _impl.platform_chat_id
run_platform_chat_request = _impl.run_platform_chat_request


def __getattr__(name: str):
    return getattr(_impl, name)


__all__ = [
    "PlatformChatRequest",
    "PlatformChatResponse",
    "platform_chat_id",
    "run_platform_chat_request",
]

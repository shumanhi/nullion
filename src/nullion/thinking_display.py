"""Provider-neutral thinking display helpers.

This module only handles thinking text that the runtime has as structured data
(for example provider-exposed reasoning summaries or Nullion activity notes).
It intentionally does not parse assistant prose or try to reconstruct hidden
chain-of-thought.
"""

from __future__ import annotations

import os
from typing import Any, Iterable


def thinking_display_enabled(*, default: bool = False) -> bool:
    raw = os.environ.get("NULLION_SHOW_THINKING_ENABLED")
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def set_thinking_display_enabled(enabled: bool) -> None:
    os.environ["NULLION_SHOW_THINKING_ENABLED"] = "true" if enabled else "false"


def thinking_display_status_text() -> str:
    return "on" if thinking_display_enabled() else "off"


def extract_thinking_text(content: Iterable[Any]) -> str | None:
    parts: list[str] = []
    for block in content or ():
        if not isinstance(block, dict):
            continue
        if block.get("type") not in {"thinking", "reasoning", "reasoning_summary"}:
            continue
        text = block.get("text") or block.get("summary")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
    return "\n\n".join(parts) or None


def format_thinking_display(thinking_text: str | None, *, label: str = "Thinking") -> str | None:
    text = str(thinking_text or "").strip()
    if not text:
        return None
    return f"{label}\n{text}"


def append_thinking_to_reply(
    reply: str,
    thinking_text: str | None,
    *,
    enabled: bool | None = None,
    label: str = "Thinking",
) -> str:
    should_append = thinking_display_enabled() if enabled is None else enabled
    if not should_append:
        return reply
    thinking = format_thinking_display(thinking_text, label=label)
    if thinking is None:
        return reply
    return f"{reply.rstrip()}\n\n{thinking}" if reply else thinking


__all__ = [
    "append_thinking_to_reply",
    "extract_thinking_text",
    "format_thinking_display",
    "set_thinking_display_enabled",
    "thinking_display_enabled",
    "thinking_display_status_text",
]

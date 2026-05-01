"""Basic Telegram-friendly formatting helpers."""

from __future__ import annotations

import html
import re

from nullion.messaging_adapters import sanitize_external_inline_markup


_BOLD_PATTERN = re.compile(r"\*\*(.+?)\*\*")
_CODE_PATTERN = re.compile(r"`([^`]+)`")


def format_telegram_text(text: str) -> tuple[str, dict[str, str]]:
    text = sanitize_external_inline_markup(text)
    escaped = html.escape(text)
    formatted = _CODE_PATTERN.sub(lambda match: f"<code>{match.group(1)}</code>", escaped)
    formatted = _BOLD_PATTERN.sub(lambda match: f"<b>{match.group(1)}</b>", formatted)
    if formatted != escaped:
        return formatted, {"parse_mode": "HTML"}
    return text, {}


__all__ = ["format_telegram_text"]

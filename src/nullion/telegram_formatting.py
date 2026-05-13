"""Basic Telegram-friendly formatting helpers."""

from __future__ import annotations

import html
import re

from nullion.messaging_adapters import sanitize_external_inline_markup


_BOLD_PATTERN = re.compile(r"\*\*(.+?)\*\*")
_INLINE_CODE_PATTERN = re.compile(r"`([^`\n]+)`")
_FENCED_CODE_BLOCK_PATTERN = re.compile(r"```([^\n`]*)\n(.*?)```", re.DOTALL)
_LANGUAGE_TOKEN_PATTERN = re.compile(r"^[a-z0-9_+-]{1,32}$", re.IGNORECASE)
_FENCE_PLACEHOLDER_PREFIX = "__NULLION_TELEGRAM_FENCE_"


def _replace_fenced_code_blocks(text: str) -> tuple[str, list[str]]:
    formatted_blocks: list[str] = []

    def replace(match: re.Match[str]) -> str:
        language = str(match.group(1) or "").strip()
        code = str(match.group(2) or "")
        class_attr = ""
        if language and _LANGUAGE_TOKEN_PATTERN.fullmatch(language):
            class_attr = f' class="language-{language.lower()}"'
        escaped_code = html.escape(code)
        formatted_blocks.append(f"<pre><code{class_attr}>{escaped_code}</code></pre>")
        return f"{_FENCE_PLACEHOLDER_PREFIX}{len(formatted_blocks) - 1}__"

    rewritten = _FENCED_CODE_BLOCK_PATTERN.sub(replace, text)
    return rewritten, formatted_blocks


def format_telegram_text(text: str) -> tuple[str, dict[str, str]]:
    text = sanitize_external_inline_markup(text)
    text_with_placeholders, formatted_blocks = _replace_fenced_code_blocks(text)
    escaped = html.escape(text_with_placeholders)
    formatted = _INLINE_CODE_PATTERN.sub(lambda match: f"<code>{match.group(1)}</code>", escaped)
    formatted = _BOLD_PATTERN.sub(lambda match: f"<b>{match.group(1)}</b>", formatted)
    for index, block in enumerate(formatted_blocks):
        placeholder = f"{_FENCE_PLACEHOLDER_PREFIX}{index}__"
        formatted = formatted.replace(placeholder, block)
    if formatted != escaped:
        return formatted, {"parse_mode": "HTML"}
    return text, {}


__all__ = ["format_telegram_text"]

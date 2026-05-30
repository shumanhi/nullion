"""Basic Telegram-friendly formatting helpers."""

from __future__ import annotations

import html
import re

from nullion.messaging_adapters import sanitize_external_inline_markup


_BOLD_PATTERN = re.compile(r"\*\*(.+?)\*\*")
_ITALIC_PATTERN = re.compile(r"(?<!\*)\*([^*\n]+?)\*(?!\*)")
_INLINE_CODE_PATTERN = re.compile(r"`([^`\n]+)`")
_FENCED_CODE_BLOCK_PATTERN = re.compile(r"```([^\n`]*)\n(.*?)```", re.DOTALL)
_LANGUAGE_TOKEN_PATTERN = re.compile(r"^[a-z0-9_+-]{1,32}$", re.IGNORECASE)
_FENCE_PLACEHOLDER_PREFIX = "__NULLION_TELEGRAM_FENCE_"
_QUOTE_PLACEHOLDER_PREFIX = "__NULLION_TELEGRAM_QUOTE_"


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


def _replace_blockquotes(text: str) -> tuple[str, list[str]]:
    formatted_quotes: list[str] = []
    output: list[str] = []
    quote_lines: list[str] = []

    def flush_quote() -> None:
        if not quote_lines:
            return
        while quote_lines and not quote_lines[0].strip():
            quote_lines.pop(0)
        while quote_lines and not quote_lines[-1].strip():
            quote_lines.pop()
        if not quote_lines:
            return
        escaped = html.escape("\n".join(quote_lines))
        formatted_quotes.append(f"<blockquote>{escaped}</blockquote>")
        output.append(f"{_QUOTE_PLACEHOLDER_PREFIX}{len(formatted_quotes) - 1}__")
        quote_lines.clear()

    for line in str(text).splitlines():
        stripped = line.lstrip()
        if stripped.startswith(">"):
            quote_lines.append(stripped[1:].removeprefix(" "))
            continue
        flush_quote()
        output.append(line)
    flush_quote()
    if str(text).endswith("\n"):
        return "\n".join(output) + "\n", formatted_quotes
    return "\n".join(output), formatted_quotes


def _normalize_markdown_bullets(text: str) -> str:
    lines: list[str] = []
    for line in str(text).splitlines():
        match = re.match(r"^(?P<indent>\s*)-\s+(?P<body>\S.*)$", line)
        if match:
            lines.append(f"{match.group('indent')}• {match.group('body')}")
            continue
        lines.append(line)
    if str(text).endswith("\n"):
        return "\n".join(lines) + "\n"
    return "\n".join(lines)


def format_telegram_text(text: str) -> tuple[str, dict[str, str]]:
    text = sanitize_external_inline_markup(text)
    text = _normalize_markdown_bullets(text)
    text_with_placeholders, formatted_blocks = _replace_fenced_code_blocks(text)
    text_with_placeholders, formatted_quotes = _replace_blockquotes(text_with_placeholders)
    escaped = html.escape(text_with_placeholders)
    formatted = _INLINE_CODE_PATTERN.sub(lambda match: f"<code>{match.group(1)}</code>", escaped)
    formatted = _BOLD_PATTERN.sub(lambda match: f"<b>{match.group(1)}</b>", formatted)
    formatted = _ITALIC_PATTERN.sub(lambda match: f"<i>{match.group(1)}</i>", formatted)
    for index, block in enumerate(formatted_blocks):
        placeholder = f"{_FENCE_PLACEHOLDER_PREFIX}{index}__"
        formatted = formatted.replace(placeholder, block)
    for index, quote in enumerate(formatted_quotes):
        placeholder = f"{_QUOTE_PLACEHOLDER_PREFIX}{index}__"
        formatted = formatted.replace(placeholder, quote)
    if formatted != escaped:
        return formatted, {"parse_mode": "HTML"}
    return text, {}


__all__ = ["format_telegram_text"]

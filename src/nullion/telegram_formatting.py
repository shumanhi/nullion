"""Basic Telegram-friendly formatting helpers."""

from __future__ import annotations

import html
import re
from urllib.parse import urlparse

from nullion.chat_text import make_markdown_tables_chat_readable
from nullion.messaging_adapters import sanitize_external_inline_markup


_BOLD_PATTERN = re.compile(r"\*\*(.+?)\*\*")
_ITALIC_PATTERN = re.compile(r"(?<!\*)\*([^*\n]+?)\*(?!\*)")
_INLINE_CODE_PATTERN = re.compile(r"`([^`\n]+)`")
_FENCED_CODE_BLOCK_PATTERN = re.compile(r"```([^\n`]*)\n(.*?)```", re.DOTALL)
_MARKDOWN_LINK_PATTERN = re.compile(r"(?<!!)\[([^\]\n]{1,240})\]\((https?://[^\s<>()]+)\)", re.IGNORECASE)
_RAW_URL_PATTERN = re.compile(r"(?i)\bhttps?://[^\s<>()\"'`]+")
_LANGUAGE_TOKEN_PATTERN = re.compile(r"^[a-z0-9_+-]{1,32}$", re.IGNORECASE)
_FENCE_PLACEHOLDER_PREFIX = "__NULLION_TELEGRAM_FENCE_"
_INLINE_CODE_PLACEHOLDER_PREFIX = "__NULLION_TELEGRAM_CODE_"
_QUOTE_PLACEHOLDER_PREFIX = "__NULLION_TELEGRAM_QUOTE_"
_LINK_PLACEHOLDER_PREFIX = "__NULLION_TELEGRAM_LINK_"
_LONG_VISIBLE_URL_CHARS = 72
_URL_TRAILING_PUNCTUATION = ".,;:!?]}'\""
_URL_TRAILING_MARKUP_DELIMITERS = ("***", "___", "**", "__")


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


def _replace_inline_code_spans(text: str) -> tuple[str, list[str]]:
    formatted_codes: list[str] = []

    def replace(match: re.Match[str]) -> str:
        code = str(match.group(1) or "")
        formatted_codes.append(f"<code>{html.escape(code)}</code>")
        return f"{_INLINE_CODE_PLACEHOLDER_PREFIX}{len(formatted_codes) - 1}__"

    rewritten = _INLINE_CODE_PATTERN.sub(replace, text)
    return rewritten, formatted_codes


def _telegram_link_label(url: str) -> str:
    if len(url) <= _LONG_VISIBLE_URL_CHARS:
        return url
    parsed = urlparse(url)
    host = (parsed.netloc or "").strip()
    if host:
        return f"Open link ({host})"
    return "Open link"


def _telegram_link_anchor(url: str, label: str) -> str:
    safe_url = html.escape(url, quote=True)
    safe_label = html.escape(label.strip() or _telegram_link_label(url))
    return f'<a href="{safe_url}">{safe_label}</a>'


def _replace_links(text: str) -> tuple[str, list[str]]:
    formatted_links: list[str] = []

    def add_link(url: str, label: str) -> str:
        formatted_links.append(_telegram_link_anchor(url, label))
        return f"{_LINK_PLACEHOLDER_PREFIX}{len(formatted_links) - 1}__"

    def replace_markdown(match: re.Match[str]) -> str:
        label = str(match.group(1) or "").strip()
        url = str(match.group(2) or "").strip()
        return add_link(url, label or _telegram_link_label(url))

    rewritten = _MARKDOWN_LINK_PATTERN.sub(replace_markdown, text)

    def replace_raw(match: re.Match[str]) -> str:
        token = str(match.group(0) or "")
        url = token
        suffix = ""
        while url:
            stripped = url.rstrip(_URL_TRAILING_PUNCTUATION)
            if stripped != url:
                suffix = f"{url[len(stripped):]}{suffix}"
                url = stripped
                continue
            delimiter = next(
                (candidate for candidate in _URL_TRAILING_MARKUP_DELIMITERS if url.endswith(candidate)),
                None,
            )
            if delimiter is None:
                break
            url = url[: -len(delimiter)]
            suffix = f"{delimiter}{suffix}"
        if not url:
            return token
        return f"{add_link(url, _telegram_link_label(url))}{suffix}"

    rewritten = _RAW_URL_PATTERN.sub(replace_raw, rewritten)
    return rewritten, formatted_links


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
    text = make_markdown_tables_chat_readable(text)
    text = _normalize_markdown_bullets(text)
    text_with_placeholders, formatted_blocks = _replace_fenced_code_blocks(text)
    text_with_placeholders, formatted_codes = _replace_inline_code_spans(text_with_placeholders)
    text_with_placeholders, formatted_links = _replace_links(text_with_placeholders)
    text_with_placeholders, formatted_quotes = _replace_blockquotes(text_with_placeholders)
    escaped = html.escape(text_with_placeholders)
    formatted = _BOLD_PATTERN.sub(lambda match: f"<b>{match.group(1)}</b>", escaped)
    formatted = _ITALIC_PATTERN.sub(lambda match: f"<i>{match.group(1)}</i>", formatted)
    for index, block in enumerate(formatted_blocks):
        placeholder = f"{_FENCE_PLACEHOLDER_PREFIX}{index}__"
        formatted = formatted.replace(placeholder, block)
    for index, code in enumerate(formatted_codes):
        placeholder = f"{_INLINE_CODE_PLACEHOLDER_PREFIX}{index}__"
        formatted = formatted.replace(placeholder, code)
    for index, link in enumerate(formatted_links):
        placeholder = f"{_LINK_PLACEHOLDER_PREFIX}{index}__"
        formatted = formatted.replace(placeholder, link)
    for index, quote in enumerate(formatted_quotes):
        placeholder = f"{_QUOTE_PLACEHOLDER_PREFIX}{index}__"
        formatted = formatted.replace(placeholder, quote)
    if formatted != escaped:
        return formatted, {"parse_mode": "HTML"}
    return text, {}


__all__ = ["format_telegram_text"]

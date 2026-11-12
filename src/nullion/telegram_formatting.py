"""Basic Telegram-friendly formatting helpers."""

from __future__ import annotations

import html
import re
from functools import lru_cache
from urllib.parse import urlparse

import mistune

from nullion.chat_text import make_markdown_tables_chat_readable
from nullion.messaging_adapters import sanitize_external_inline_markup


_FENCED_CODE_BLOCK_PATTERN = re.compile(r"```([^\n`]*)\n(.*?)```", re.DOTALL)
_LANGUAGE_TOKEN_PATTERN = re.compile(r"^[a-z0-9_+-]{1,32}$", re.IGNORECASE)
_FENCE_PLACEHOLDER_PREFIX = "NULLIONTELEGRAMFENCE_"
_QUOTE_PLACEHOLDER_PREFIX = "NULLIONTELEGRAMQUOTE_"
_LONG_VISIBLE_URL_CHARS = 72
_URL_TRAILING_PUNCTUATION = ".,;:!?]}'\""


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


class _TelegramInlineParser(mistune.InlineParser):
    # Unlike HTML, Telegram displays indentation in its plain text stream.
    # Keep indentation after breaks so nested lists retain their hierarchy.
    SPECIFICATION = {
        **mistune.InlineParser.SPECIFICATION,
        "softbreak": r" *\n",
        "linebreak": r"(?:\\| {2,})\n",
    }


@lru_cache(maxsize=1)
def _inline_parser():
    # Already a shipped dependency. Parse Markdown syntax, never user intent.
    from mistune.plugins.formatting import strikethrough

    return mistune.Markdown(renderer=None, inline=_TelegramInlineParser(), plugins=[strikethrough]).inline


def _render_text(text: str) -> str:
    output = []
    cursor = 0
    for match in re.finditer(r"https?://[^\s<>()\"'`]+", text, re.IGNORECASE):
        url = match.group().rstrip(_URL_TRAILING_PUNCTUATION)
        output.append(html.escape(text[cursor:match.start()]))
        output.append(_telegram_link_anchor(url, _telegram_link_label(url)))
        output.append(html.escape(match.group()[len(url):]))
        cursor = match.end()
    output.append(html.escape(text[cursor:]))
    return "".join(output)


def _render_inline(text: str, *, allow_code: bool = True) -> str:
    def render(tokens, autolink=True, code_allowed=allow_code):
        output = []
        for token in tokens:
            kind = token["type"]
            children = token.get("children", [])
            if kind in {"strong", "emphasis", "strikethrough"}:
                tag = {"strong": "b", "emphasis": "i", "strikethrough": "s"}[kind]
                content = render(children, autolink, code_allowed)
                # Telegram forbids emphasis inside/around code entities.
                output.extend(part if part.startswith("<code>") else f"<{tag}>{part}</{tag}>"
                              for part in re.split(r"(<code>[\s\S]*?</code>)", content) if part)
            elif kind == "codespan":
                literal = html.escape(token.get('raw', ''))
                output.append(f"<code>{literal}</code>" if code_allowed else literal)
            elif kind in {"softbreak", "linebreak"}:
                output.append("\n")
            elif kind == "link":
                url = token.get("attrs", {}).get("url", "")
                label = render(children, False, False)
                if urlparse(url).scheme.lower() in {"http", "https"}:
                    if len(children) == 1 and children[0].get("raw") == url:
                        label = html.escape(_telegram_link_label(url))
                    output.append(f'<a href="{html.escape(url, quote=True)}">{label}</a>')
                else:
                    output.append(label)
            elif children:
                output.append(render(children, autolink, code_allowed))
            elif kind == "text":
                raw = token.get("raw", "")
                output.append(_render_text(raw) if autolink else html.escape(raw))
            else:
                output.append(html.escape(token.get("raw", "")))
        return "".join(output)

    # Emphasis cannot cross a paragraph boundary. Single newlines, including
    # a source's soft wrap inside emphasis, remain valid inline Markdown.
    return "".join(part if re.fullmatch(r"\n[ \t]*\n", part) else render(_inline_parser()(part, {}))
                   for part in re.split(r"(\n[ \t]*\n)", text))


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
        escaped = _render_inline("\n".join(quote_lines), allow_code=False)
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
        match = re.match(r"^(?P<indent>[ \t]*)[-+*]\s+(?P<body>\S.*)$", line)
        if match:
            lines.append(f"{match.group('indent')}• {match.group('body')}")
            continue
        lines.append(line)
    if str(text).endswith("\n"):
        return "\n".join(lines) + "\n"
    return "\n".join(lines)


def format_telegram_text(text: str) -> tuple[str, dict[str, str]]:
    text = sanitize_external_inline_markup(text)
    prose, formatted_blocks = _replace_fenced_code_blocks(text)
    prose = make_markdown_tables_chat_readable(prose)
    prose = _normalize_markdown_bullets(prose)
    prose, formatted_quotes = _replace_blockquotes(prose)
    prose = re.sub(r"(?m)^#{1,6} +(.+)$", r"**\1**", prose)
    formatted = _render_inline(prose)
    for index, block in enumerate(formatted_blocks):
        formatted = formatted.replace(f"{_FENCE_PLACEHOLDER_PREFIX}{index}__", block)
    for index, quote in enumerate(formatted_quotes):
        formatted = formatted.replace(f"{_QUOTE_PLACEHOLDER_PREFIX}{index}__", quote)
    if formatted != html.escape(prose):
        return formatted, {"parse_mode": "HTML"}
    return prose, {}


def telegram_html_to_plain(text: str, *, include_link_targets: bool = False) -> str:
    from html.parser import HTMLParser

    class VisibleText(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.parts = []
            self.links = []

        def handle_starttag(self, tag, attrs):
            if tag == "a" and include_link_targets:
                self.links.append((len(self.parts), dict(attrs).get("href", "")))

        def handle_endtag(self, tag):
            if tag == "a" and self.links:
                start, target = self.links.pop()
                if target and target not in "".join(self.parts[start:]):
                    self.parts.append(f" ({target})")

        def handle_data(self, data):
            self.parts.append(data)

    parser = VisibleText()
    parser.feed(text)
    parser.close()
    return "".join(parser.parts)


def telegram_plain_text(text: str) -> str:
    rendered, options = format_telegram_text(text)
    return telegram_html_to_plain(rendered, include_link_targets=True) if options else rendered


def telegram_text_units(text: str) -> int:
    """Telegram entity offsets and text budgets use UTF-16 code units."""
    return sum(2 if ord(char) > 0xFFFF else 1 for char in text)


def _plain_chunks(text: str, limit: int) -> list[tuple[str, dict[str, str]]]:
    chunks = []
    while text:
        count = 0
        used = 0
        for char in text:
            size = 2 if ord(char) > 0xFFFF else 1
            if used + size > limit:
                break
            used += size
            count += 1
        chunks.append((text[:count], {}))
        text = text[count:]
    return chunks


def format_telegram_chunks(text: str, *, limit: int = 3900) -> list[tuple[str, dict[str, str]]]:
    """Render once, then split HTML with balanced, reopened formatting spans.

    Splitting Markdown before parsing can orphan emphasis, links or code. Count
    serialized characters conservatively, including reopened tags and entities.
    """
    if limit < 80:
        raise ValueError("Telegram formatted chunk limit must be at least 80")
    formatted, options = format_telegram_text(text)
    if telegram_text_units(formatted) <= limit:
        return [(formatted, options)] if formatted else []
    if not options:
        # Escape plain text before the same entity-aware splitter so astral
        # Unicode and literal angle brackets receive the same safe budget.
        formatted = html.escape(formatted)
    tokens = re.findall(r"<[^>]+>|&(?:\#\d+|\#x[0-9a-fA-F]+|[a-zA-Z]+);|[^<&\n]+|\n|[<&]", formatted)
    line_costs = [0] * len(tokens)
    cost = 0
    for index in range(len(tokens) - 1, -1, -1):
        cost = telegram_text_units(tokens[index]) + (0 if tokens[index] == "\n" else cost)
        line_costs[index] = cost
    stack: list[tuple[str, str]] = []
    chunks: list[tuple[str, dict[str, str]]] = []
    current = ""
    has_content = False
    line_start = True

    def suffix():
        return "".join(f"</{tag}>" for tag, _ in reversed(stack))

    def flush():
        nonlocal current, has_content
        if has_content:
            chunks.append((current + suffix(), {"parse_mode": "HTML"}))
        current = "".join(opener for _, opener in stack)
        has_content = False

    for index, token in enumerate(tokens):
        if line_start and has_content and any(tag == "pre" for tag, _ in stack):
            # Keep ordinary code lines together. Escaped tags/entities are
            # separate tokens, but they still belong to the same literal line.
            empty_size = telegram_text_units("".join(opener for _, opener in stack) + suffix())
            if (line_costs[index] <= limit - empty_size
                    and line_costs[index] > limit - telegram_text_units(current + suffix())):
                flush()
        if token.startswith("<"):
            closing = token.startswith("</")
            tag = token.lstrip("</").split()[0].rstrip(">")
            if closing:
                stack.pop()
                current += token
            else:
                if telegram_text_units(current + token + suffix()) + len(tag) + 3 >= limit:
                    flush()
                # An unusually large href cannot fit as an HTML tag. Use a
                # safe plain representation rather than generating invalid HTML.
                if telegram_text_units(current + token + suffix()) + len(tag) + 3 >= limit:
                    plain = telegram_html_to_plain(formatted)
                    return _plain_chunks(plain, limit)
                current += token
                stack.append((tag, token))
            continue
        while token:
            room = limit - telegram_text_units(current + suffix())
            first_size = len(token) if token.startswith("&") else telegram_text_units(token[0])
            if room < first_size:
                flush()
                room = limit - telegram_text_units(current + suffix())
                if room < first_size:
                    # Reopened tags can consume the whole budget. Do not split
                    # an entity/surrogate or loop forever with an empty chunk.
                    return _plain_chunks(telegram_html_to_plain(formatted), limit)
            count = 0
            used = 0
            for char in token:
                size = 2 if ord(char) > 0xFFFF else 1
                if used + size > room:
                    break
                count += 1
                used += size
            if count < len(token) and not token.startswith("&"):
                boundary = max(token.rfind("\n", 0, count), token.rfind(" ", 0, count))
                if boundary >= count // 2:
                    count = boundary + 1
            part = token[:count]
            current += part
            line_start = part.endswith("\n")
            token = token[count:]
            has_content = True
            if token:
                flush()
    flush()
    return chunks


__all__ = ["format_telegram_text", "format_telegram_chunks", "telegram_plain_text"]

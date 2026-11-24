"""Verified browser records used by the same request's recovery and renderer."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import quote, urlsplit

_RECORD_KIND = "verified_browser_page.v1"
MAX_VERIFIED_PAGES = 4
MAX_PAGE_RECORDS = 3


def _page_address(value):
    if not isinstance(value, str) or len(value) > 1400:
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme in {"http", "https"} and parsed.hostname and parsed.username is None:
        return parsed
    return None


def _literal(value, *, limit):
    if not isinstance(value, str):
        return ""
    clean = " ".join(value.split())
    return clean if 0 < len(clean) <= limit else ""


def verified_page_capture(*, page_url, capture_time, context, rows, fields):
    """Accept only records bound to the native scoped DOM capture."""
    if not _page_address(page_url) or not context:
        return None
    selected = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        limits = {"title": 180, "price_text": 48, "compact_text": 250}
        record = {key: _literal(row.get(key), limit=limits[key]) for key in fields}
        if not record.get("title") or not all(record.values()):
            continue
        if "price_text" in record and not any(char.isdecimal() for char in record["price_text"]):
            continue
        if record not in selected:
            selected.append(record)
        if len(selected) == MAX_PAGE_RECORDS:
            break
    if not selected:
        return None
    return {"format": _RECORD_KIND, "page_url": page_url, "captured_at": capture_time,
            "context": list(context), "records": selected}


def verified_browser_pages(results, *, previous=()):
    """Recover bounded verified snapshots from admitted native browser receipts."""
    pages = {}
    for capture in [*previous, *(
        capture for result in results
        if result.tool_name == "browser_read_pages" and result.status in {"completed", "failed"}
        and isinstance(result.output, Mapping)
        for capture in result.output.get("verified_pages") or ()
    )]:
        if not isinstance(capture, Mapping) or capture.get("format") != _RECORD_KIND:
            continue
        url, context = capture.get("page_url"), capture.get("context")
        records = capture.get("records")
        if (not _page_address(url) or not isinstance(context, list) or not context
                or not all(isinstance(value, str) for value in context)
                or not isinstance(records, list) or not records
                or not isinstance(capture.get("captured_at"), (int, float))):
            continue
        key = url, tuple(context)
        if key not in pages or capture["captured_at"] >= pages[key]["captured_at"]:
            pages.pop(key, None)
            pages[key] = dict(capture)
    return list(pages.values())[-MAX_VERIFIED_PAGES:]


def render_verified_browser_pages(pages, *, unresolved_url=None):
    """Render the stored fields without a provider or an extra model call."""
    from nullion.chat_text import ReplySection, UserMessage, render_user_message
    from nullion.redaction import redact_text

    def escape(value):
        return redact_text(value).translate(str.maketrans(
            {"&": "&amp;", "<": "&lt;", ">": "&gt;", "[": r"\[", "]": r"\]", "*": r"\*", "`": r"\`"}
        ))

    sections = []
    for capture in verified_browser_pages((), previous=pages):
        url = capture["page_url"]
        address = _page_address(url)
        link = quote(redact_text(url), safe=":/?&=%#@!$+,;~")
        rows = []
        for record in capture["records"][:MAX_PAGE_RECORDS]:
            if not isinstance(record, Mapping) or not isinstance(record.get("title"), str):
                continue
            pieces = [escape(record["title"])]
            if isinstance(record.get("price_text"), str):
                pieces.append(escape(record["price_text"]))
            detail = escape(record["compact_text"]) if isinstance(record.get("compact_text"), str) else ""
            heading = " · ".join(pieces)
            if detail and detail != heading:
                heading += f" — {detail}"
            rows.append(heading)
        if rows:
            context = " · ".join(escape(value) for value in capture["context"])
            sections.append(ReplySection(heading=address.hostname, items=tuple(rows),
                paragraphs=(context, f"[Captured results](<{link}>)")))
    if not sections:
        return None
    unresolved = _page_address(unresolved_url)
    summary = (f"I couldn’t finish verifying {unresolved.hostname}. I captured these results:"
               if unresolved else "I captured these results before the lookup ended:")
    return render_user_message(UserMessage(summary=summary, sections=tuple(sections)))

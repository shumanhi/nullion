"""Real chat backend for Project Nullion."""

from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
import gzip
import inspect
import os
import re
import socket
import urllib.request
from ipaddress import ip_address
from urllib.parse import quote_plus, urlparse

# Try to import optional decompressors; advertise their encodings only when
# we can actually decode the response. This lets us look like a real browser
# (which always advertises br/zstd) without claiming what we can't handle.
try:
    import brotli as _brotli  # type: ignore
    _HAVE_BROTLI = True
except ImportError:
    _brotli = None
    _HAVE_BROTLI = False
try:
    import zstandard as _zstd  # type: ignore
    _HAVE_ZSTD = True
except ImportError:
    _zstd = None
    _HAVE_ZSTD = False

# Mimic a real browser. Sites like Bing fingerprint missing sec-ch-ua + sec-fetch-*
# headers as bot traffic and serve a CAPTCHA (which is why our search_web returned
# zero results until we switched to DuckDuckGo). UA + client-hints + sec-fetch-*
# all need to match — Chromium ships them as a coherent set.
#
# IMPORTANT: built once at module import (= app launch) using the actual host OS,
# so a Windows/Linux install doesn't ship Mac headers (which would be inconsistent
# with the host's real TCP/TLS fingerprint). Users cannot override these — they're
# part of the runtime, not user-configurable settings.
#
# Pinned to Chrome 134 (current stable line in 2026). Bump in lockstep with
# Chrome's stable channel; UA drift years behind real Chrome is itself a bot tell.
_CHROME_MAJOR = "134"


def _detect_browser_profile() -> tuple[str, str]:
    """Return (User-Agent, Sec-Ch-Ua-Platform) matching the host OS.

    Bot detectors cross-check UA against the platform claim and against
    TCP/TLS fingerprints (which always reveal the real OS). Mismatches are
    the loudest bot signal short of an outright `python-urllib` UA, so we
    always claim the OS we're actually running on.
    """
    import platform as _platform
    system = _platform.system()
    if system == "Windows":
        # Win11 reports "Windows NT 10.0; Win64; x64" in real Chrome too.
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{_CHROME_MAJOR}.0.0.0 Safari/537.36"
        )
        return ua, '"Windows"'
    if system == "Darwin":
        ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{_CHROME_MAJOR}.0.0.0 Safari/537.36"
        )
        return ua, '"macOS"'
    # Linux / BSD / unknown: present as Linux x86_64. Real Chrome on these
    # OSes also reports the same string regardless of distro.
    ua = (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{_CHROME_MAJOR}.0.0.0 Safari/537.36"
    )
    return ua, '"Linux"'


_BROWSER_UA, _SEC_CH_UA_PLATFORM = _detect_browser_profile()

_ACCEPT_ENCODING_PARTS = ["gzip", "deflate"]
if _HAVE_BROTLI:
    _ACCEPT_ENCODING_PARTS.append("br")
if _HAVE_ZSTD:
    _ACCEPT_ENCODING_PARTS.append("zstd")

_BROWSER_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": ", ".join(_ACCEPT_ENCODING_PARTS),
    "Cache-Control": "max-age=0",
    # Chromium client hints — present in EVERY real Chrome request and absent
    # in most stdlib/curl scrapers. Their absence is a strong bot signal.
    "Sec-Ch-Ua": (
        f'"Chromium";v="{_CHROME_MAJOR}", '
        f'"Not:A-Brand";v="24", '
        f'"Google Chrome";v="{_CHROME_MAJOR}"'
    ),
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": _SEC_CH_UA_PLATFORM,  # matches the host OS
    # sec-fetch-* describe the request's intent. "navigate / document / none"
    # is what Chrome sends when you type a URL into the address bar — the
    # most browser-like profile we can claim for a top-level fetch.
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}


def _callable_accepts_keyword(callable_obj, keyword: str) -> bool:
    try:
        parameters = inspect.signature(callable_obj).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        parameter.name == keyword or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _read_response(response, max_bytes: int = 65536) -> bytes:
    """Read response body, decompressing gzip/deflate if the server compressed it.

    The previous implementation read max_bytes of the COMPRESSED stream then
    handed it to gzip.decompress(), which raises "Compressed file ended
    before the end-of-stream marker was reached" the moment the truncation
    landed mid-block (i.e. on every large page). We now decompress
    incrementally, stopping when we have max_bytes of *decompressed* output,
    so partial reads are handled gracefully.
    """
    import zlib
    encoding = (response.headers.get("Content-Encoding") or "").lower()

    # Brotli: full-stream decode (small enough for our max_bytes budget).
    if encoding == "br" and _HAVE_BROTLI:
        try:
            raw = response.read(max(max_bytes * 10, 65536))
            return _brotli.decompress(raw)[:max_bytes]
        except Exception as exc:
            _logger.warning("brotli decompression failed: %s", exc)
            return b""
    # Zstandard: streaming decoder.
    if encoding == "zstd" and _HAVE_ZSTD:
        try:
            raw = response.read(max(max_bytes * 10, 65536))
            dctx = _zstd.ZstdDecompressor()
            return dctx.decompress(raw, max_output_size=max_bytes * 4)[:max_bytes]
        except Exception as exc:
            _logger.warning("zstd decompression failed: %s", exc)
            return b""
    if encoding in ("gzip", "deflate"):
        # Pick the right wbits for streaming decompression.
        # gzip → 16 + MAX_WBITS, raw deflate → -MAX_WBITS, zlib-wrapped → MAX_WBITS.
        if encoding == "gzip":
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        else:
            decoder = zlib.decompressobj()  # auto-detect zlib header
        out = bytearray()
        chunk_size = 8192
        # Read up to ~10x max_bytes of compressed data as a safety bound;
        # text generally compresses 3–5x so this comfortably covers max_bytes
        # of decompressed output without runaway reads.
        compressed_budget = max(max_bytes * 10, 65536)
        consumed = 0
        while len(out) < max_bytes and consumed < compressed_budget:
            chunk = response.read(min(chunk_size, compressed_budget - consumed))
            if not chunk:
                break
            consumed += len(chunk)
            try:
                out.extend(decoder.decompress(chunk))
            except zlib.error as exc:
                # Some servers send raw deflate without zlib headers — retry.
                if encoding == "deflate" and not out:
                    decoder = zlib.decompressobj(-zlib.MAX_WBITS)
                    try:
                        out.extend(decoder.decompress(chunk))
                        continue
                    except zlib.error:
                        pass
                # Keep what we have rather than blowing up the whole tool call.
                _logger.warning("decompression aborted (%s) after %d bytes: %s", encoding, len(out), exc)
                break
        # Try to flush — but don't fail the whole read if we cut off mid-stream.
        try:
            out.extend(decoder.flush())
        except zlib.error:
            pass
        return bytes(out[:max_bytes])

    return response.read(max_bytes)

from nullion.task_planner import TaskPlanner
from nullion.tools import ToolRegistry


class ChatBackendUnavailableError(RuntimeError):
    """Raised when the configured chat backend cannot produce a reply."""


def _strip_html(text: str) -> str:
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", text))).strip()


def _validate_fetch_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http:// and https:// URLs are allowed.")
    host = parsed.hostname
    if not isinstance(host, str) or not host:
        raise ValueError("URL host is required.")
    lowered = host.strip().lower()
    if lowered == "localhost":
        raise ValueError("Localhost URLs are not allowed.")
    try:
        literal_host = ip_address(lowered)
    except ValueError:
        try:
            address_infos = socket.getaddrinfo(lowered, parsed.port or None, proto=socket.IPPROTO_TCP)
        except OSError as exc:
            raise ValueError(f"Unable to resolve URL host: {lowered}") from exc
        resolved_ip: str | None = None
        for family, socktype, proto, canonname, sockaddr in address_infos:
            del family, socktype, proto, canonname
            if not isinstance(sockaddr, tuple) or not sockaddr:
                continue
            candidate = sockaddr[0]
            if not isinstance(candidate, str):
                continue
            addr = ip_address(candidate)
            if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
                raise ValueError("Only globally routable http:// and https:// URLs are allowed.")
            resolved_ip = candidate
        if resolved_ip is None:
            raise ValueError("Unable to resolve URL host to a routable address.")
        # Return the URL with the resolved IP to prevent DNS TOCTOU race.
        safe_url = url.replace(host, resolved_ip, 1)
        return safe_url
    if literal_host.is_private or literal_host.is_loopback or literal_host.is_link_local or literal_host.is_reserved:
        raise ValueError("Only globally routable http:// and https:// URLs are allowed.")
    return url


def fetch_url_snapshot(url: str, timeout_seconds: int = 20) -> dict[str, object]:
    _validate_fetch_url(url)
    req = urllib.request.Request(url, headers=_BROWSER_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        content_type = response.headers.get_content_type()
        status_code = getattr(response, "status", 200)
        payload = _read_response(response, max_bytes=4096)
    body = payload.decode("utf-8", "ignore")
    title = None
    title_match = re.search(r"<title[^>]*>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
    if title_match:
        title = _strip_html(title_match.group(1)) or None
    return {
        "url": url,
        "status_code": status_code,
        "content_type": content_type,
        "title": title,
        "text": _strip_html(body),
        "body": body,
    }


def _format_web_fetch_output(output: dict[str, object]) -> str | None:
    url = output.get("url")
    if not isinstance(url, str) or not url:
        return None
    lines = [f"- URL: {url}"]
    content_type = output.get("content_type")
    if isinstance(content_type, str) and content_type:
        lines.append(f"- Content-Type: {content_type}")
    title = output.get("title")
    if isinstance(title, str) and title:
        lines.append(f"- Title: {title}")
    text = output.get("text")
    if isinstance(text, str) and text:
        lines.append(f"- Snippet: {text[:280]}")
    return "\n".join(lines)


class _SimpleSearchResultParser(HTMLParser):
    def __init__(self, *, limit: int) -> None:
        super().__init__()
        self._limit = max(1, limit)
        self._in_anchor = False
        self._results: list[dict[str, object]] = []
        self._current_url: str | None = None
        self._current_title: list[str] = []
        self._current_snippet: list[str] = []

    @property
    def results(self) -> list[dict[str, object]]:
        return list(self._results)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:  # type: ignore[override]
        lowered = tag.lower()
        attrs_map = {name.lower(): value or "" for name, value in attrs}
        if lowered == "a":
            href = attrs_map.get("href", "")
            self._in_anchor = bool(href and href.startswith(("http://", "https://")))
            if self._in_anchor:
                self._current_url = href
        if lowered in {"p", "span"} and self._current_url:
            # Best-effort snippet text capture around common result blocks.
            self._current_snippet.append("")

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if not self._current_url:
            return
        text = data.strip()
        if not text:
            return
        if self._in_anchor:
            self._current_title.append(text)
        elif self._current_snippet is not None:
            self._current_snippet.append(text)

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        lowered = tag.lower()
        if lowered == "a" and self._in_anchor:
            if self._current_url and len(self._results) < self._limit:
                title = " ".join(self._current_title).strip()
                snippet = " ".join(segment for segment in self._current_snippet if segment).strip()
                if title and self._current_url:
                    self._results.append(
                        {
                            "title": title,
                            "url": self._current_url,
                            "snippet": snippet,
                        }
                    )
            self._in_anchor = False
            self._current_url = None
            self._current_title = []
            self._current_snippet = []


def _format_web_search_results(results: list[dict[str, object]]) -> str | None:
    lines: list[str] = []
    for result in results:
        title = result.get("title")
        url = result.get("url")
        snippet = result.get("snippet")
        if not isinstance(title, str) or not title or not isinstance(url, str) or not url:
            continue
        line = f"- {title} — {url}"
        if isinstance(snippet, str) and snippet:
            line += f" — {snippet}"
        lines.append(line)
    return "\n".join(lines) or None


import logging as _logging
_logger = _logging.getLogger(__name__)


def _ddg_html_search(query: str, limit: int) -> list[dict[str, object]]:
    """Scrape DuckDuckGo's no-JS HTML endpoint. Far more bot-friendly than
    Bing (which has been serving CAPTCHA pages to scrapers since ~2026).
    Returns parsed result dicts; raises on transport failure."""
    import urllib.parse as _up
    body_data = _up.urlencode({"q": query, "kl": "us-en"}).encode()
    headers = {
        **_BROWSER_HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://duckduckgo.com/",
    }
    request = urllib.request.Request(
        "https://html.duckduckgo.com/html/", data=body_data, headers=headers,
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        body = _read_response(response, max_bytes=131072).decode("utf-8", "ignore")
    # DDG HTML wraps each result anchor with class="result__a"; titles in
    # the anchor text, snippets in a sibling .result__snippet block.
    results: list[dict[str, object]] = []
    # Pull each result block to keep title + snippet co-located.
    block_pat = re.compile(
        r'<div[^>]+class="[^"]*\bresult\b[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    anchor_pat = re.compile(
        r'<a[^>]+class="[^"]*\bresult__a\b[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    snippet_pat = re.compile(
        r'class="[^"]*\bresult__snippet\b[^"]*"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for block in block_pat.findall(body):
        a = anchor_pat.search(block)
        if not a:
            continue
        href = a.group(1).strip()
        title = _strip_html(a.group(2)).strip()
        # DDG wraps URLs with /l/?uddg=...
        m = re.search(r"[?&]uddg=([^&]+)", href)
        if m:
            href = _up.unquote(m.group(1))
        snippet_match = snippet_pat.search(block)
        snippet = _strip_html(snippet_match.group(1)).strip() if snippet_match else ""
        if title and href.startswith(("http://", "https://")):
            results.append({"title": title, "url": href, "snippet": snippet})
        if len(results) >= limit:
            break
    return results


def _bing_html_search(query: str, limit: int) -> list[dict[str, object]]:
    """Legacy Bing scraper kept as fallback. As of 2026 Bing serves a CAPTCHA
    challenge to most non-browser User-Agents, so this returns [] in practice
    — but we keep it so a future header tweak doesn't require a code change."""
    search_url = f"https://www.bing.com/search?q={quote_plus(query)}"
    request = urllib.request.Request(search_url, headers=_BROWSER_HEADERS)
    with urllib.request.urlopen(request, timeout=20) as response:
        body = _read_response(response, max_bytes=65536).decode("utf-8", "ignore")
    parser = _SimpleSearchResultParser(limit=limit)
    parser.feed(body)
    return parser.results[:limit]


def search_web(query: str, limit: int = 5) -> list[dict[str, object]]:
    """Builtin web search. Tries DuckDuckGo HTML first (reliable), then Bing.

    Raises RuntimeError if BOTH backends produce zero usable results, so the
    caller (and the model) can distinguish 'tool failed' from 'no relevant
    news exists'. Previously a CAPTCHA / bot-block silently returned [], and
    the model would conclude there was simply no news — misleading the user.
    """
    backends = [("duckduckgo", _ddg_html_search), ("bing", _bing_html_search)]
    raw_results: list[dict[str, object]] = []
    backend_errors: list[str] = []
    used_backend = ""
    for name, backend in backends:
        try:
            raw_results = backend(query, limit)
        except Exception as exc:
            backend_errors.append(f"{name}: {exc.__class__.__name__}: {exc}")
            _logger.warning("search_web backend %s failed: %s", name, exc)
            continue
        if raw_results:
            used_backend = name
            break
        backend_errors.append(f"{name}: returned zero results (likely bot-blocked)")

    safe_results: list[dict[str, object]] = []
    for result in raw_results[:limit]:
        result_url = result.get("url")
        if not isinstance(result_url, str):
            continue
        try:
            _validate_fetch_url(result_url)
            safe_results.append(result)
        except ValueError:
            _logger.warning("search_web: skipping result URL that failed SSRF check: %s", result_url)

    if not safe_results:
        # Make this VISIBLE — the previous behaviour returned [] silently and
        # the model would tell the user "no news found" when in reality the
        # tool was bot-blocked. Raising lets _short_error_text surface the
        # exact backend failures (CAPTCHA, 5xx, etc.) in the chat UI.
        detail = " | ".join(backend_errors) or "unknown reason"
        raise RuntimeError(
            f"web_search returned no usable results for '{query[:80]}'. "
            f"All search backends failed or were blocked: {detail}. "
            "If you've configured Brave / Google CSE / Perplexity API keys, "
            "switch to that provider in Settings → Setup → Search provider."
        )
    if used_backend and used_backend != "duckduckgo":
        # Quiet log breadcrumb when we fell back; doesn't affect output.
        _logger.info("search_web served %d results via fallback backend: %s", len(safe_results), used_backend)
    return safe_results


def fetch_external_context(message: str, *, limit: int = 5) -> str | None:
    tokens = [token.strip("()[]<>") for token in message.split()]
    for token in tokens:
        if token.startswith(("http://", "https://")):
            try:
                return _format_web_fetch_output(fetch_url_snapshot(token))
            except ValueError:
                return None
    results = search_web(message, limit=limit)
    return _format_web_search_results(results)


def _resolve_active_tool_registry(
    *,
    active_tool_registry: ToolRegistry | None,
    live_tool_registry: ToolRegistry | None,
) -> ToolRegistry | None:
    return active_tool_registry if active_tool_registry is not None else live_tool_registry


def _build_system_prompt(
    *,
    conversation_context: str | None = None,
    memory_context: str | None = None,
) -> str | None:
    sections: list[str] = []
    if conversation_context:
        sections.append(f"Recent conversation:\n{conversation_context}")
    if memory_context:
        sections.append(f"Known user memory:\n{memory_context}")
    if not sections:
        return None
    return "\n\n".join(sections)


def _build_orchestrator_conversation_history(
    *,
    conversation_context: str | None,
    memory_context: str | None,
) -> list[dict[str, object]]:
    prompt = _build_system_prompt(
        conversation_context=conversation_context,
        memory_context=memory_context,
    )
    if not prompt:
        return []
    return [{"role": "system", "content": [{"type": "text", "text": prompt}]}]


def _feature_enabled(name: str, *, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _try_dispatch_mini_agents(
    *,
    agent_orchestrator,
    conversation_id: str,
    principal_id: str,
    message: str,
    tool_registry: ToolRegistry,
    policy_store,
    approval_store,
    has_attachments: bool,
) -> str | None:
    if (
        has_attachments
        or agent_orchestrator is None
        or not hasattr(agent_orchestrator, "dispatch_request_sync")
        or not _feature_enabled("NULLION_TASK_DECOMPOSITION_ENABLED")
        or not _feature_enabled("NULLION_MULTI_AGENT_ENABLED")
    ):
        return None
    plan = TaskPlanner().build_execution_plan(
        user_message=message,
        principal_id=principal_id,
        active_task_frame=None,
    )
    if not plan.can_dispatch_mini_agents:
        return None
    try:
        dispatch_result = agent_orchestrator.dispatch_request_sync(
            conversation_id=conversation_id,
            principal_id=principal_id,
            user_message=message,
            tool_registry=tool_registry,
            policy_store=policy_store,
            approval_store=approval_store,
            single_task_fast_path=False,
        )
    except Exception:
        _logger.debug("Mini-agent dispatch failed; falling back to normal chat turn", exc_info=True)
        return None
    if not getattr(dispatch_result, "dispatched", True):
        return None
    task_count = getattr(dispatch_result, "task_count", None)
    acknowledgment = getattr(dispatch_result, "acknowledgment", None)
    if isinstance(acknowledgment, str) and acknowledgment.strip():
        return acknowledgment.strip()
    return f"Working on {task_count or 'the'} task(s)."


def generate_chat_reply(
    *,
    message: str,
    attachments: list[dict[str, str]] | None = None,
    conversation_context: str | None = None,
    memory_context: str | None = None,
    external_context_fetcher=fetch_external_context,
    live_tool_registry: ToolRegistry | None = None,
    active_tool_registry: ToolRegistry | None = None,
    internal_context_fetcher=None,
    live_tool_invoker=None,
    live_tool_result_recorder=None,
    live_information_resolution_recorder=None,
    agent_orchestrator=None,
    model_client=None,
    policy_store=None,
    approval_store=None,
    conversation_id: str = "chat-backend",
    principal_id: str = "telegram_chat",
) -> str:
    del external_context_fetcher, live_tool_invoker, live_tool_result_recorder, live_information_resolution_recorder
    del internal_context_fetcher

    tool_registry = _resolve_active_tool_registry(
        active_tool_registry=active_tool_registry,
        live_tool_registry=live_tool_registry,
    )

    created_orchestrator_from_model_client = False
    if agent_orchestrator is None and model_client is not None:
        from nullion.agent_orchestrator import AgentOrchestrator

        agent_orchestrator = AgentOrchestrator(model_client=model_client)
        created_orchestrator_from_model_client = True

    if agent_orchestrator is None:
        raise ChatBackendUnavailableError("No model client configured for chat backend.")

    conversation_history = _build_orchestrator_conversation_history(
        conversation_context=conversation_context,
        memory_context=memory_context,
    )
    user_content_blocks = None
    if attachments:
        from nullion.chat_attachments import chat_attachment_content_blocks, normalize_chat_attachments

        normalized_attachments = normalize_chat_attachments(attachments)
        if normalized_attachments:
            user_content_blocks = chat_attachment_content_blocks(message, normalized_attachments)

    run_turn = agent_orchestrator.run_turn
    turn_kwargs = {
        "conversation_id": conversation_id,
        "principal_id": principal_id,
        "user_message": message,
        "conversation_history": conversation_history,
        "tool_registry": tool_registry or ToolRegistry(),
        "policy_store": policy_store,
        "approval_store": approval_store,
    }
    if _callable_accepts_keyword(run_turn, "user_content_blocks"):
        turn_kwargs["user_content_blocks"] = user_content_blocks

    dispatched_reply = None
    if not created_orchestrator_from_model_client:
        dispatched_reply = _try_dispatch_mini_agents(
            agent_orchestrator=agent_orchestrator,
            conversation_id=conversation_id,
            principal_id=principal_id,
            message=message,
            tool_registry=tool_registry or ToolRegistry(),
            policy_store=policy_store,
            approval_store=approval_store,
            has_attachments=bool(user_content_blocks),
        )
    if dispatched_reply is not None:
        return dispatched_reply

    try:
        result = run_turn(**turn_kwargs)
    except Exception as exc:
        raise ChatBackendUnavailableError(f"Agent orchestrator error: {exc}") from exc
    final_text = getattr(result, "final_text", None)
    if isinstance(final_text, str) and final_text.strip():
        return final_text.strip()
    if getattr(result, "suspended_for_approval", False):
        approval_id = getattr(result, "approval_id", None)
        if approval_id:
            return f"Tool approval requested: {approval_id}"
        return "Tool approval requested."
    raise ChatBackendUnavailableError("Agent orchestrator returned an empty reply.")


__all__ = [
    "ChatBackendUnavailableError",
    "fetch_external_context",
    "fetch_url_snapshot",
    "generate_chat_reply",
    "search_web",
]

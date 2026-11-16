"""Platform-neutral chat streaming helpers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import asyncio
import re
import threading
from typing import Awaitable, Callable, Iterable


class LatestTextUpdateQueue:
    """Coalesce transport previews without blocking the model's token reader.

    At most one transport operation is in flight. New drafts replace queued
    drafts; they never queue another network edit for every token. Finalization
    waits for ownership of an existing send, then publishes the final answer.
    """

    def __init__(self, loop, publish: Callable[[str], Awaitable[object]], *, interval: float = 1.0,
                 initial_delay: float = 0.0):
        self._loop = loop
        self._publish = publish
        self._interval = interval
        self._initial_delay = initial_delay
        self._lock = threading.Lock()
        self._latest: str | None = None
        self._worker = None
        self._error: BaseException | None = None
        self._wake = asyncio.Event()

    def submit(self, text: str) -> None:
        with self._lock:
            self._latest = text
            if self._worker is None:
                self._worker = asyncio.run_coroutine_threadsafe(self._run(), self._loop)

    async def _run(self) -> None:
        try:
            # Select the newest value after the initial delay, so a delayed
            # progress notice starts with real completed work, not a stale ack.
            if self._initial_delay:
                delay, self._initial_delay = self._initial_delay, 0
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=delay)
                except TimeoutError:
                    pass
                self._wake.clear()
            while True:
                with self._lock:
                    text, self._latest = self._latest, None
                    if text is None:
                        self._worker = None
                        return
                await self._publish(text)
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self._interval)
                except TimeoutError:
                    pass
                self._wake.clear()
        except BaseException as exc:
            with self._lock:
                self._error = exc
                self._latest = None
                self._worker = None

    async def drain(self) -> None:
        # The final answer supersedes drafts not yet sent. Never cancel an
        # in-flight initial send: its acceptance may already be ambiguous.
        with self._lock:
            self._latest = None
            worker = self._worker
        self._wake.set()
        if worker is not None:
            await asyncio.wrap_future(worker)
        with self._lock:
            error, self._error = self._error, None
        if error is not None:
            raise error


class ChatStreamingMode(str, Enum):
    """Best available progress delivery mode for a chat platform."""

    CHUNKS = "chunks"
    MESSAGE_EDITS = "message_edits"
    TYPING_ONLY = "typing_only"
    FINAL_ONLY = "final_only"


@dataclass(frozen=True, slots=True)
class ChatPlatformCapabilities:
    """Capabilities exposed by a chat surface or adapter."""

    name: str
    supports_chunks: bool = False
    supports_message_edits: bool = False
    supports_typing_indicator: bool = False


WEB_CHAT_CAPABILITIES = ChatPlatformCapabilities(
    name="web",
    supports_chunks=True,
    supports_typing_indicator=True,
)


TELEGRAM_CHAT_CAPABILITIES = ChatPlatformCapabilities(
    name="telegram",
    supports_message_edits=True,
    supports_typing_indicator=True,
)


def platform_supports_text_streaming(capabilities: ChatPlatformCapabilities) -> bool:
    """Return True when a platform can show incremental response text."""

    return capabilities.supports_chunks or capabilities.supports_message_edits


def streaming_enabled_by_default(capabilities: ChatPlatformCapabilities) -> bool:
    """Default streaming on for platforms that can present incremental text."""

    return platform_supports_text_streaming(capabilities)


def select_chat_streaming_mode(
    capabilities: ChatPlatformCapabilities,
    *,
    streaming_enabled: bool | None = None,
) -> ChatStreamingMode:
    """Choose the richest stable streaming mode for a platform."""

    enabled = streaming_enabled_by_default(capabilities) if streaming_enabled is None else streaming_enabled
    if not enabled:
        return ChatStreamingMode.FINAL_ONLY
    if capabilities.supports_chunks:
        return ChatStreamingMode.CHUNKS
    if capabilities.supports_message_edits:
        return ChatStreamingMode.MESSAGE_EDITS
    if capabilities.supports_typing_indicator:
        return ChatStreamingMode.TYPING_ONLY
    return ChatStreamingMode.FINAL_ONLY


def chunk_chat_text(text: str, *, max_chars: int = 56) -> list[str]:
    """Split text into display-friendly chunks without losing whitespace."""

    if not text:
        return []
    if max_chars < 8:
        raise ValueError("max_chars must be at least 8")

    tokens = re.findall(r"\S+\s*", text)
    chunks: list[str] = []
    current = ""
    for token in tokens:
        if current and len(current) + len(token) > max_chars:
            chunks.append(current)
            current = token
        else:
            current += token
        while len(current) > max_chars * 2:
            chunks.append(current[:max_chars])
            current = current[max_chars:]
    if current:
        chunks.append(current)
    return chunks


def iter_chat_text_chunks(text: str, *, max_chars: int = 56) -> Iterable[str]:
    """Yield chunks for callers that prefer an iterator interface."""

    yield from chunk_chat_text(text, max_chars=max_chars)

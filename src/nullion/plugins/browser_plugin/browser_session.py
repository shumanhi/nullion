"""Browser plugin — session pool and BrowserSession dataclass."""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol


class BrowserBackend(Protocol):
    """Minimal interface that Playwright and CDP backends both satisfy."""

    async def navigate(self, session_id: str, url: str) -> str: ...
    async def click(self, session_id: str, selector: str) -> None: ...
    async def type_text(self, session_id: str, selector: str, text: str) -> None: ...
    async def extract_text(self, session_id: str, selector: str | None) -> str: ...
    async def screenshot(self, session_id: str) -> bytes: ...
    async def scroll(self, session_id: str, direction: str, amount: int) -> None: ...
    async def wait_for(
        self, session_id: str, selector: str | None, url_pattern: str | None, timeout: float
    ) -> None: ...
    async def find(self, session_id: str, selector: str) -> list[dict[str, str]]: ...
    async def run_js(self, session_id: str, script: str) -> Any: ...
    async def close_session(self, session_id: str) -> None: ...
    async def shutdown(self) -> None: ...


@dataclass
class BrowserSession:
    session_id: str
    backend_name: str
    created_at: float
    metadata: dict[str, Any] = field(default_factory=dict)


class BrowserSessionPool:
    """Tracks open browser sessions keyed by session_id."""

    def __init__(self) -> None:
        self._sessions: dict[str, BrowserSession] = {}
        self._lock = asyncio.Lock()

    async def create(self, backend_name: str) -> str:
        session_id = str(uuid.uuid4())[:8]
        import time
        async with self._lock:
            self._sessions[session_id] = BrowserSession(
                session_id=session_id,
                backend_name=backend_name,
                created_at=time.time(),
            )
        return session_id

    async def get(self, session_id: str) -> BrowserSession | None:
        async with self._lock:
            return self._sessions.get(session_id)

    async def remove(self, session_id: str) -> bool:
        async with self._lock:
            return self._sessions.pop(session_id, None) is not None

    def list_sessions(self) -> list[BrowserSession]:
        return list(self._sessions.values())

"""Browser plugin — tool implementations (backend-agnostic)."""
from __future__ import annotations

import asyncio
import base64
import threading
from typing import Any

from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy, BrowserPolicyViolation
from nullion.plugins.browser_plugin.browser_session import BrowserBackend, BrowserSessionPool
from nullion.tools import ToolInvocation, ToolResult


def _ok(invocation: ToolInvocation, output: dict[str, Any]) -> ToolResult:
    return ToolResult(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        status="completed",
        output=output,
    )


def _fail(invocation: ToolInvocation, message: str) -> ToolResult:
    return ToolResult(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        status="failed",
        output={},
        error=message,
    )


_BROWSER_LOOP: asyncio.AbstractEventLoop | None = None
_BROWSER_LOOP_THREAD: threading.Thread | None = None
_BROWSER_LOOP_LOCK = threading.Lock()

# Cap concurrent browser-tool calls so that a flood of parallel agent tasks
# fails fast instead of starving the worker thread pool.
_MAX_CONCURRENT_BROWSER_OPS = 8
_BROWSER_SEMAPHORE = threading.Semaphore(_MAX_CONCURRENT_BROWSER_OPS)


def _ensure_browser_loop() -> asyncio.AbstractEventLoop:
    global _BROWSER_LOOP, _BROWSER_LOOP_THREAD
    with _BROWSER_LOOP_LOCK:
        if _BROWSER_LOOP is not None and _BROWSER_LOOP.is_running():
            return _BROWSER_LOOP
        loop = asyncio.new_event_loop()

        def _runner() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=_runner, name="nullion-browser-loop", daemon=True)
        thread.start()
        _BROWSER_LOOP = loop
        _BROWSER_LOOP_THREAD = thread
        return loop


def _run(coro) -> Any:
    """Run a browser coroutine on one shared loop.

    Browser backends keep session state and async locks. Running each sync tool
    call through a fresh event loop can wedge those locks across navigate /
    screenshot pairs, especially when attached to a visible CDP browser.

    A semaphore caps concurrency so that parallel agent tasks cannot exhaust
    the worker thread pool while waiting on browser I/O.
    """
    if not _BROWSER_SEMAPHORE.acquire(blocking=False):
        close = getattr(coro, "close", None)
        if close is not None:
            close()
        raise RuntimeError(
            f"Browser operation queue full — too many concurrent requests "
            f"(max {_MAX_CONCURRENT_BROWSER_OPS})"
        )
    try:
        loop = _ensure_browser_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return future.result(timeout=60)
        except Exception:
            future.cancel()
            raise
    finally:
        _BROWSER_SEMAPHORE.release()


class BrowserTools:
    """Sync wrappers around the async backend, registered as kernel tools."""

    def __init__(self, backend: BrowserBackend, pool: BrowserSessionPool, policy: BrowserPolicy) -> None:
        self._backend = backend
        self._pool = pool
        self._policy = policy
        self._cleanup_lock = threading.Lock()
        self._sessions_by_scope: dict[str, set[str]] = {}

    def _session_id(self, invocation: ToolInvocation) -> str:
        return str(invocation.arguments.get("session_id", "default"))

    def _cleanup_scope(self, invocation: ToolInvocation) -> str:
        return str(invocation.capsule_id or invocation.principal_id or "global")

    def _remember_session(self, invocation: ToolInvocation, session_id: str) -> None:
        scope = self._cleanup_scope(invocation)
        with self._cleanup_lock:
            self._sessions_by_scope.setdefault(scope, set()).add(session_id)

    def _forget_session(self, session_id: str) -> None:
        with self._cleanup_lock:
            empty_scopes: list[str] = []
            for scope, session_ids in self._sessions_by_scope.items():
                session_ids.discard(session_id)
                if not session_ids:
                    empty_scopes.append(scope)
            for scope in empty_scopes:
                self._sessions_by_scope.pop(scope, None)

    def close_tracked_sessions(self, scope_id: str | None = None) -> None:
        with self._cleanup_lock:
            if scope_id is None:
                session_ids = {
                    session_id
                    for scoped_session_ids in self._sessions_by_scope.values()
                    for session_id in scoped_session_ids
                }
                self._sessions_by_scope.clear()
            else:
                session_ids = set(self._sessions_by_scope.pop(str(scope_id), set()))
        for session_id in sorted(session_ids):
            try:
                _run(self._backend.close_session(session_id))
            except Exception:
                continue

    # ── Tools ─────────────────────────────────────────────────────────────────

    def browser_navigate(self, invocation: ToolInvocation) -> ToolResult:
        url = invocation.arguments.get("url", "")
        if not url:
            return _fail(invocation, "Missing required argument: url")
        try:
            self._policy.check_url(str(url))
        except BrowserPolicyViolation as e:
            return _fail(invocation, str(e))

        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            result = _run(self._backend.navigate(session_id, str(url)))
            return _ok(invocation, {"result": result, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Navigation failed: {e}")

    def browser_click(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector", "")
        if not selector:
            return _fail(invocation, "Missing required argument: selector")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.click(session_id, str(selector)))
            return _ok(invocation, {"clicked": selector, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Click failed: {e}")

    def browser_type(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector", "")
        text = invocation.arguments.get("text", "")
        if not selector:
            return _fail(invocation, "Missing required argument: selector")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.type_text(session_id, str(selector), str(text)))
            return _ok(invocation, {"typed": len(str(text)), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Type failed: {e}")

    def browser_extract_text(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector") or None
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            text = _run(self._backend.extract_text(session_id, selector))
            return _ok(invocation, {"text": text, "length": len(text), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Extract text failed: {e}")

    def browser_screenshot(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            png_bytes = _run(self._backend.screenshot(session_id))
            b64 = base64.b64encode(png_bytes).decode()
            return _ok(invocation, {"image_base64": b64, "size_bytes": len(png_bytes), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Screenshot failed: {e}")

    def browser_scroll(self, invocation: ToolInvocation) -> ToolResult:
        direction = str(invocation.arguments.get("direction", "down"))
        if direction not in {"up", "down"}:
            return _fail(invocation, "direction must be 'up' or 'down'")
        amount = int(invocation.arguments.get("amount", 500))
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.scroll(session_id, direction, amount))
            return _ok(invocation, {"scrolled": direction, "amount": amount, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Scroll failed: {e}")

    def browser_wait_for(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector") or None
        url_pattern = invocation.arguments.get("url_pattern") or None
        timeout = float(invocation.arguments.get("timeout", 10.0))
        if not selector and not url_pattern:
            return _fail(invocation, "Provide selector or url_pattern")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            _run(self._backend.wait_for(session_id, selector, url_pattern, timeout))
            return _ok(invocation, {"waited_for": selector or url_pattern, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Wait failed: {e}")

    def browser_find(self, invocation: ToolInvocation) -> ToolResult:
        selector = invocation.arguments.get("selector", "")
        if not selector:
            return _fail(invocation, "Missing required argument: selector")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            elements = _run(self._backend.find(session_id, str(selector)))
            return _ok(invocation, {"elements": elements, "count": len(elements), "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"Find failed: {e}")

    def browser_run_js(self, invocation: ToolInvocation) -> ToolResult:
        script = invocation.arguments.get("script", "")
        if not script:
            return _fail(invocation, "Missing required argument: script")
        session_id = self._session_id(invocation)
        self._remember_session(invocation, session_id)
        try:
            result = _run(self._backend.run_js(session_id, str(script)))
            return _ok(invocation, {"result": result, "session_id": session_id})
        except Exception as e:
            return _fail(invocation, f"JavaScript execution failed: {e}")

    def browser_close(self, invocation: ToolInvocation) -> ToolResult:
        session_id = self._session_id(invocation)
        try:
            _run(self._backend.close_session(session_id))
            self._forget_session(session_id)
            return _ok(invocation, {"closed": session_id})
        except Exception as e:
            return _fail(invocation, f"Close failed: {e}")

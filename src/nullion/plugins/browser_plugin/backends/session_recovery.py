"""Bounded reconnects and explicitly selected, task-local headless sessions."""
from __future__ import annotations

from urllib.error import URLError


class BrowserRecoveryRequired(ConnectionError):
    def __init__(self, *, retry_attempted: bool = True):
        super().__init__("The configured browser could not be reconnected. Its saved profile was preserved. Ask whether to retry the existing browser or continue this task headless; headless does not share its login.")
        self.retry_attempted = retry_attempted

    def recovery_output(self):
        return {"result_kind": "user_choice_required",
            "user_question": "I couldn't reconnect to the browser. Public pages can often work headless; account-only pages may still need the original signed-in browser. What should I do?",
            "failure_reason": "browser_connection_unavailable", "reconnect_attempted": self.retry_attempted,
            "saved_profile_preserved": True, "login_requirement": "task_dependent",
            "headless_shares_existing_login": False,
            "recovery_tool": "browser_open", "requires_user_choice": True,
            "recovery_options": [
                {"number": 1, "mode": "configured", "label": "Retry the existing browser after reopening it"},
                {"number": 2, "mode": "headless", "label": "Continue this task headless (public pages; separate login state)"}]}


class SessionRecoveryBackend:
    # Repeating a click/submission after transport loss could duplicate an
    # action. Only navigation and observations can be replayed automatically.
    _REPLAYABLE = frozenset({"open", "navigate", "extract_text", "screenshot", "find", "share_session_context"})

    def __init__(self, configured):
        self._configured = configured
        self.BACKEND_NAME = configured.BACKEND_NAME
        self._headless = {}

    def mode_for_session(self, session_id):
        return "headless" if session_id in self._headless else "configured"

    async def share_session_context(self, parent, child):
        if parent in self._headless:
            self._headless[child] = self._headless[parent]
        backend = self._headless.get(parent, self._configured)
        share = getattr(backend, "share_session_context", None)
        if share is not None:
            await self._invoke('share_session_context', parent, child)

    async def select_mode(self, session_id, mode):
        if mode == "headless":
            if session_id not in self._headless:
                from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
                self._headless[session_id] = PlaywrightBackend(headless=True)
        elif mode == "configured":
            backend = self._headless.pop(session_id, None)
            if backend is not None:
                await backend.close_session(session_id)
                if backend not in self._headless.values():
                    await backend.shutdown()
        else:
            raise ValueError("Unknown browser recovery mode")

    def __getattr__(self, name):
        original = getattr(self._configured, name)
        if not callable(original) or name.startswith("_") or name == "connection_notice":
            return original

        async def invoke(session_id, *args, **kwargs):
            return await self._invoke(name, session_id, *args, **kwargs)
        return invoke

    async def _invoke(self, name, session_id, *args, **kwargs):
        backend = self._headless.get(session_id, self._configured)
        async def perform():
            import inspect
            pending=getattr(backend,name)(session_id,*args,**kwargs)
            return await pending if inspect.isawaitable(pending) else pending
        try:
            return await perform()
        except Exception as error:
            if backend is not self._configured or not self._disconnected(error):
                raise
            reconnect = getattr(self._configured, "reconnect", None)
            if reconnect is not None:
                try:
                    await reconnect(session_id)
                    if name in self._REPLAYABLE:
                        return await perform()
                    # Reconnection cannot establish a lost action's outcome.
                    raise error
                except Exception as retry_error:
                    if retry_error is error:
                        raise
                    raise BrowserRecoveryRequired() from retry_error
            raise BrowserRecoveryRequired(retry_attempted=False) from error

    def _disconnected(self, error):
        from websockets.exceptions import ConnectionClosed
        if isinstance(error, (ConnectionError, URLError, ConnectionClosed)):
            return True
        backend = getattr(self._configured, "_backend", self._configured)
        browser = getattr(backend, "_browser", None)
        connected = getattr(browser, "is_connected", None)
        return callable(connected) and not connected()

    async def close_session(self, session_id):
        backend = self._headless.get(session_id)
        if backend is not None:
            closed = await backend.close_session(session_id)
            self._headless.pop(session_id, None)
            if backend not in self._headless.values():
                await backend.shutdown()
            return closed
        return await self._configured.close_session(session_id)

    async def shutdown(self):
        for backend in {id(value): value for value in self._headless.values()}.values():
            await backend.shutdown()
        self._headless.clear()
        await self._configured.shutdown()

"""Browser plugin — auto-launch backend.

Tries to connect to an existing Chrome/Brave CDP session.
If none is running, launches Chrome automatically (visible window),
then connects via CDP so the user can watch.
Falls back to Playwright headless if Chrome is not found on this system.

Environment vars:
  NULLION_BROWSER_CDP_URL   override CDP URL (default: http://localhost:9222)
  NULLION_BROWSER_HEADLESS  set to "true" to force headless Playwright fallback
"""
from __future__ import annotations

import asyncio
import os
import platform
import shutil
import subprocess
import time
from typing import Any


_CDP_PORT = 9222

# Candidate Chrome/Chromium paths by platform
_CHROME_CANDIDATES: dict[str, list[str]] = {
    "Darwin": [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    ],
    "Linux": [
        "google-chrome",
        "google-chrome-stable",
        "chromium-browser",
        "chromium",
        "brave-browser",
    ],
    "Windows": [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
    ],
}


def _find_chrome() -> str | None:
    explicit_path = os.environ.get("NULLION_BROWSER_PATH", "").strip()
    if explicit_path and os.path.exists(explicit_path):
        return explicit_path

    system = platform.system()
    candidates = list(_CHROME_CANDIDATES.get(system, []))
    preferred = os.environ.get("NULLION_BROWSER_PREFERRED", "").strip().lower()
    if preferred:
        candidates.sort(key=lambda candidate: 0 if preferred in candidate.lower() else 1)

    for candidate in candidates:
        if os.path.isabs(candidate):
            if os.path.exists(candidate):
                return candidate
        else:
            found = shutil.which(candidate)
            if found:
                return found
    return None


def _cdp_url() -> str:
    return os.environ.get("NULLION_BROWSER_CDP_URL", f"http://localhost:{_CDP_PORT}")


def _cdp_port() -> int:
    from urllib.parse import urlparse

    parsed = urlparse(_cdp_url())
    return parsed.port or _CDP_PORT


def _is_cdp_reachable() -> bool:
    """Quick TCP check — is something already listening on the CDP port?"""
    import socket
    port = _cdp_port()
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


_launched_proc: subprocess.Popen | None = None


def _launch_chrome(chrome_path: str) -> None:
    global _launched_proc
    if _is_cdp_reachable():
        return  # already running

    user_data_dir = os.environ.get(
        "NULLION_BROWSER_USER_DATA_DIR",
        os.path.expanduser("~/.nullion/browser-profile"),
    )
    os.makedirs(user_data_dir, exist_ok=True)
    port = _cdp_port()

    args = [
        chrome_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-default-apps",
    ]

    # On macOS open a new window even if Chrome is already running
    if platform.system() == "Darwin":
        args.append("--new-window")

    _launched_proc = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait up to 5 s for the CDP port to open
    for _ in range(25):
        time.sleep(0.2)
        if _is_cdp_reachable():
            return

    raise RuntimeError(
        f"Launched Chrome but CDP port {port} never opened. "
        "Try setting NULLION_BROWSER_CDP_URL if Chrome uses a different port."
    )


class AutoBackend:
    """Auto-launch browser backend.

    On first use:
      1. If CDP port is already open → attach via CDP (user sees it)
      2. Elif Chrome/Brave found → launch it with --remote-debugging-port, attach
      3. Else → fall back to Playwright headless

    Subsequent calls reuse the same browser.
    """

    BACKEND_NAME = "auto"

    def __init__(self) -> None:
        self._backend: Any = None
        self._lock = asyncio.Lock()

    async def _ensure_backend(self) -> Any:
        async with self._lock:
            if self._backend is not None:
                return self._backend

            force_headless = os.environ.get("NULLION_BROWSER_HEADLESS", "").lower() == "true"

            if not force_headless:
                # 1. CDP already reachable?
                if _is_cdp_reachable():
                    self._backend = self._make_cdp()
                    return self._backend

                # 2. Launch Chrome if available
                chrome = _find_chrome()
                if chrome:
                    _launch_chrome(chrome)
                    self._backend = self._make_cdp()
                    return self._backend

            # 3. Headless Playwright fallback
            self._backend = self._make_playwright()
            return self._backend

    def _make_cdp(self) -> Any:
        from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
        return CDPBackend()

    def _make_playwright(self) -> Any:
        from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
        os.environ.setdefault("NULLION_BROWSER_HEADLESS", "true")
        return PlaywrightBackend()

    # ── Delegate all BrowserBackend protocol methods ──────────────────────────

    async def navigate(self, session_id: str, url: str) -> str:
        b = await self._ensure_backend()
        return await b.navigate(session_id, url)

    async def click(self, session_id: str, selector: str) -> None:
        b = await self._ensure_backend()
        await b.click(session_id, selector)

    async def type_text(self, session_id: str, selector: str, text: str) -> None:
        b = await self._ensure_backend()
        await b.type_text(session_id, selector, text)

    async def extract_text(self, session_id: str, selector: str | None) -> str:
        b = await self._ensure_backend()
        return await b.extract_text(session_id, selector)

    async def screenshot(self, session_id: str) -> bytes:
        b = await self._ensure_backend()
        return await b.screenshot(session_id)

    async def scroll(self, session_id: str, direction: str, amount: int) -> None:
        b = await self._ensure_backend()
        await b.scroll(session_id, direction, amount)

    async def wait_for(
        self,
        session_id: str,
        selector: str | None,
        url_pattern: str | None,
        timeout: float,
    ) -> None:
        b = await self._ensure_backend()
        await b.wait_for(session_id, selector, url_pattern, timeout)

    async def find(self, session_id: str, selector: str) -> list[dict[str, str]]:
        b = await self._ensure_backend()
        return await b.find(session_id, selector)

    async def run_js(self, session_id: str, script: str) -> Any:
        b = await self._ensure_backend()
        return await b.run_js(session_id, script)

    async def close_session(self, session_id: str) -> None:
        if self._backend:
            await self._backend.close_session(session_id)

    async def shutdown(self) -> None:
        if self._backend:
            await self._backend.shutdown()
        global _launched_proc
        if _launched_proc is not None:
            try:
                _launched_proc.terminate()
            except Exception:
                pass
            _launched_proc = None

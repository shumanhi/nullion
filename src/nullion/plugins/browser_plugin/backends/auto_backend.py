"""Browser plugin — auto-launch backend.

Tries to connect to an existing Chrome/Brave CDP session.
If none is running, launches Chrome automatically (visible window),
then connects via CDP so the user can watch.
Falls back to Playwright headless if Chrome is not found on this system.

Environment vars:
  NULLION_BROWSER_CDP_URL   override CDP URL (default: http://127.0.0.1:9222)
  NULLION_BROWSER_HEADLESS  set to "true" to force headless Playwright fallback
"""
from __future__ import annotations

import asyncio
import json
import os
import platform
import shutil
import socket
import subprocess
import time
from typing import Any
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen


_CDP_PORT = 9222
_DEFAULT_CDP_HOST = "127.0.0.1"
_BROWSER_KINDS = ("chrome", "chromium", "brave", "edge")

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


def _browser_kind_from_text(value: str | None) -> str | None:
    text = (value or "").lower()
    if "brave" in text:
        return "brave"
    if "microsoft edge" in text or "msedge" in text or "edge" in text:
        return "edge"
    if "chromium" in text:
        return "chromium"
    if "google chrome" in text or "chrome" in text:
        return "chrome"
    return None


def _preferred_browser_kind() -> str | None:
    preferred = os.environ.get("NULLION_BROWSER_PREFERRED", "").strip().lower()
    return preferred if preferred in _BROWSER_KINDS else None


def _candidate_matches_preference(candidate: str, preferred: str | None) -> bool:
    return not preferred or _browser_kind_from_text(candidate) == preferred


def _find_chrome() -> str | None:
    explicit_path = os.environ.get("NULLION_BROWSER_PATH", "").strip()
    if explicit_path and os.path.exists(explicit_path):
        return explicit_path

    system = platform.system()
    candidates = list(_CHROME_CANDIDATES.get(system, []))
    preferred = _preferred_browser_kind()
    if preferred:
        candidates.sort(key=lambda candidate: 0 if _candidate_matches_preference(candidate, preferred) else 1)

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
    from nullion.plugins.browser_plugin.backends.cdp_backend import normalized_cdp_url

    return normalized_cdp_url(os.environ.get("NULLION_BROWSER_CDP_URL", f"http://{_DEFAULT_CDP_HOST}:{_CDP_PORT}"))


def _cdp_url_for(host: str, port: int) -> str:
    parsed = urlparse(_cdp_url())
    if host in {"localhost", "::1"}:
        host = _DEFAULT_CDP_HOST
    return urlunparse(parsed._replace(netloc=f"{host}:{port}"))


def _cdp_endpoint() -> tuple[str, int]:
    parsed = urlparse(_cdp_url())
    host = parsed.hostname or _DEFAULT_CDP_HOST
    if host in {"localhost", "::1"}:
        host = _DEFAULT_CDP_HOST
    return host, parsed.port or _CDP_PORT


def _set_cdp_port(port: int) -> None:
    parsed = urlparse(_cdp_url())
    host = parsed.hostname or _DEFAULT_CDP_HOST
    if host in {"localhost", "::1"}:
        host = _DEFAULT_CDP_HOST
    os.environ["NULLION_BROWSER_CDP_URL"] = urlunparse(parsed._replace(netloc=f"{host}:{port}"))


def _cdp_port() -> int:
    return _cdp_endpoint()[1]


def _is_cdp_reachable() -> bool:
    """Quick TCP check — is something already listening on the CDP port?"""
    host, port = _cdp_endpoint()
    return _is_cdp_reachable_at(host, port)


def _is_cdp_reachable_at(host: str, port: int) -> bool:
    """Quick TCP check — is something already listening on a CDP port?"""
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _port_is_available(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
        return True
    except OSError:
        return False


def _next_available_cdp_port(start: int) -> int:
    host, _port = _cdp_endpoint()
    if host in {"::1", "localhost"}:
        host = _DEFAULT_CDP_HOST
    for port in range(max(start, 1024), max(start, 1024) + 50):
        if _port_is_available(host, port):
            return port
    raise RuntimeError("Could not find an available local CDP port for the selected browser.")


def _cdp_browser_kind_from_version_url(base_url: str) -> str | None:
    url = base_url.rstrip("/") + "/json/version"
    try:
        with urlopen(url, timeout=1) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return _browser_kind_from_text(
        " ".join(
            str(payload.get(key) or "")
            for key in ("Browser", "User-Agent")
        )
    )


def _cdp_browser_kind_from_version() -> str | None:
    return _cdp_browser_kind_from_version_url(_cdp_url())


def _cdp_owner_browser_kind() -> str | None:
    """Best-effort local process owner check for the configured CDP port."""
    _host, port = _cdp_endpoint()
    return _cdp_owner_browser_kind_for_port(port)


def _cdp_owner_browser_kind_for_port(port: int) -> str | None:
    """Best-effort local process owner check for a CDP port."""
    system = platform.system()
    if system not in {"Darwin", "Linux"}:
        return None
    try:
        lsof = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-Fp"],
            check=False,
            capture_output=True,
            text=True,
            timeout=1,
        )
    except Exception:
        return None
    pids = [
        line[1:].strip()
        for line in lsof.stdout.splitlines()
        if line.startswith("p") and line[1:].strip().isdigit()
    ]
    for pid in pids:
        try:
            ps = subprocess.run(
                ["ps", "-p", pid, "-o", "comm="],
                check=False,
                capture_output=True,
                text=True,
                timeout=1,
            )
        except Exception:
            continue
        kind = _browser_kind_from_text(ps.stdout)
        if kind:
            return kind
    return None


def _running_cdp_matches_preference() -> bool:
    preferred = _preferred_browser_kind()
    if not preferred:
        return True
    running = _cdp_owner_browser_kind() or _cdp_browser_kind_from_version()
    return running is None or running == preferred


def _preferred_cdp_scan_ports(host: str, current_port: int) -> list[int]:
    configured_default = _CDP_PORT
    ports = [current_port, configured_default]
    ports.extend(range(configured_default + 1, configured_default + 50))
    seen: set[int] = set()
    ordered: list[int] = []
    for port in ports:
        if port in seen or not (1024 <= port <= 65535):
            continue
        seen.add(port)
        ordered.append(port)
    return ordered


def _find_reachable_preferred_cdp_port() -> int | None:
    preferred = _preferred_browser_kind()
    if not preferred:
        return None
    host, current_port = _cdp_endpoint()
    for port in _preferred_cdp_scan_ports(host, current_port):
        if not _is_cdp_reachable_at(host, port):
            continue
        base_url = _cdp_url_for(host, port)
        running = _cdp_owner_browser_kind_for_port(port) or _cdp_browser_kind_from_version_url(base_url)
        if running == preferred:
            return port
    return None


def _reuse_preferred_cdp_if_available() -> bool:
    port = _find_reachable_preferred_cdp_port()
    if port is None:
        return False
    _set_cdp_port(port)
    return True


_launched_proc: subprocess.Popen | None = None


def _browser_profile_dir(chrome_path: str) -> str:
    configured = os.environ.get("NULLION_BROWSER_USER_DATA_DIR", "").strip()
    if configured:
        return os.path.expanduser(configured)
    kind = _browser_kind_from_text(chrome_path) or "chrome"
    return os.path.expanduser(f"~/.nullion/browser-profile-{kind}")


def _launch_chrome(chrome_path: str) -> None:
    global _launched_proc
    if _is_cdp_reachable() and _running_cdp_matches_preference():
        return  # already running
    if _reuse_preferred_cdp_if_available():
        return
    if _is_cdp_reachable():
        _set_cdp_port(_next_available_cdp_port(_cdp_port() + 1))

    user_data_dir = _browser_profile_dir(chrome_path)
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
                if _is_cdp_reachable() and _running_cdp_matches_preference():
                    self._backend = self._make_cdp()
                    return self._backend
                if _reuse_preferred_cdp_if_available():
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

    async def open(self, session_id: str) -> str:
        b = await self._ensure_backend()
        return await b.open(session_id)

    async def navigate(self, session_id: str, url: str) -> str:
        b = await self._ensure_backend()
        return await b.navigate(session_id, url)

    async def click(self, session_id: str, selector: str) -> None:
        b = await self._ensure_backend()
        await b.click(session_id, selector)

    async def click_element(self, session_id: str, target: dict[str, Any]) -> None:
        b = await self._ensure_backend()
        await b.click_element(session_id, target)

    async def type_text(self, session_id: str, selector: str, text: str) -> None:
        b = await self._ensure_backend()
        await b.type_text(session_id, selector, text)

    async def type_field(self, session_id: str, target: dict[str, Any], text: str) -> None:
        b = await self._ensure_backend()
        await b.type_field(session_id, target, text)

    async def extract_text(self, session_id: str, selector: str | None) -> str:
        b = await self._ensure_backend()
        return await b.extract_text(session_id, selector)

    async def screenshot(self, session_id: str, mode: str = "auto"):
        b = await self._ensure_backend()
        return await b.screenshot(session_id, mode=mode)

    async def scroll(self, session_id: str, direction: str, amount: int) -> None:
        b = await self._ensure_backend()
        await b.scroll(session_id, direction, amount)

    async def wait_for(
        self,
        session_id: str,
        selector: str | None,
        url_pattern: str | None,
        text: str | None,
        timeout: float,
    ) -> None:
        b = await self._ensure_backend()
        try:
            await b.wait_for(session_id, selector, url_pattern, text, timeout)
        except TypeError:
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

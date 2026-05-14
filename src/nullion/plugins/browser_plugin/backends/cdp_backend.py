"""Browser plugin — CDP (attach to existing Chrome/Brave) backend.

Connects to a browser that was launched with --remote-debugging-port=9222.

Requires: pip install playwright  (uses Playwright's CDP support)

To launch Chrome with CDP enabled:
    /Applications/Google Chrome.app/Contents/MacOS/Google Chrome \
        --remote-debugging-port=9222 --no-first-run

Set NULLION_BROWSER_CDP_URL=http://127.0.0.1:9222
"""
from __future__ import annotations

import asyncio
import base64
import fnmatch
import json
import os
from types import SimpleNamespace
from typing import Any
from urllib.request import Request, urlopen
from urllib.parse import urlparse, urlunparse

from nullion.plugins.browser_plugin.browser_session import (
    BrowserScreenshotResult,
    auto_screenshot_uses_full_page,
)

try:
    from playwright.async_api import async_playwright, CDPSession, Browser, Page
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False


DEFAULT_CDP_URL = "http://127.0.0.1:9222"


def _require_playwright() -> None:
    if not _PLAYWRIGHT_AVAILABLE:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright"
        )


def normalized_cdp_url(raw_url: str | None = None) -> str:
    """Use IPv4 loopback for local CDP to avoid localhost resolving to ::1."""
    url = (raw_url or os.environ.get("NULLION_BROWSER_CDP_URL") or DEFAULT_CDP_URL).strip()
    if not url:
        return DEFAULT_CDP_URL
    parsed = urlparse(url)
    if parsed.hostname in {"localhost", "::1"}:
        port = parsed.port or 9222
        netloc = f"127.0.0.1:{port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return url


def _json_url(cdp_url: str, path: str) -> str:
    return f"{cdp_url.rstrip('/')}/{path.lstrip('/')}"


def _read_json_url(url: str) -> object:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _cdp_targets(cdp_url: str) -> list[dict[str, object]]:
    payload = _read_json_url(_json_url(cdp_url, "/json/list"))
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


async def _async_cdp_targets(cdp_url: str) -> list[dict[str, object]]:
    return await asyncio.to_thread(_cdp_targets, cdp_url)


async def _async_new_cdp_target(cdp_url: str) -> dict[str, object] | None:
    def create() -> dict[str, object] | None:
        request = Request(_json_url(cdp_url, "/json/new?about:blank"), method="PUT")
        try:
            with urlopen(request, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    return await asyncio.to_thread(create)


class _RawCDPClient:
    def __init__(self, websocket_url: str) -> None:
        self.websocket_url = websocket_url
        self._ws = None
        self._next_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._event_waiters: dict[str, list[asyncio.Future]] = {}
        self._reader_task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._ws is not None:
            return
        import websockets

        self._ws = await websockets.connect(self.websocket_url, max_size=None)
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self) -> None:
        try:
            async for raw_message in self._ws:
                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError:
                    continue
                message_id = message.get("id") if isinstance(message, dict) else None
                if isinstance(message_id, int):
                    future = self._pending.pop(message_id, None)
                    if future is not None and not future.done():
                        if "error" in message:
                            future.set_exception(RuntimeError(str(message.get("error"))))
                        else:
                            future.set_result(message.get("result") or {})
                    continue
                method = message.get("method") if isinstance(message, dict) else None
                if isinstance(method, str):
                    waiters = self._event_waiters.pop(method, [])
                    for future in waiters:
                        if not future.done():
                            future.set_result(message.get("params") or {})
        except Exception as exc:
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(exc)
            for waiters in list(self._event_waiters.values()):
                for future in waiters:
                    if not future.done():
                        future.set_exception(exc)

    async def send(self, method: str, params: dict[str, object] | None = None, *, timeout: float = 10) -> dict[str, object]:
        await self.connect()
        async with self._send_lock:
            self._next_id += 1
            message_id = self._next_id
            future = asyncio.get_running_loop().create_future()
            self._pending[message_id] = future
            await self._ws.send(json.dumps({"id": message_id, "method": method, "params": params or {}}))
        return await asyncio.wait_for(future, timeout=timeout)

    async def wait_event(self, method: str, *, timeout: float) -> dict[str, object]:
        future = asyncio.get_running_loop().create_future()
        self._event_waiters.setdefault(method, []).append(future)
        return await asyncio.wait_for(future, timeout=timeout)

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except BaseException:
                pass
            self._reader_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            finally:
                self._ws = None


class _RawCDPPage:
    def __init__(self, target: dict[str, object]) -> None:
        self.target_id = str(target.get("id") or "")
        self.url = str(target.get("url") or "")
        self._client = _RawCDPClient(str(target.get("webSocketDebuggerUrl") or ""))
        self._closed = False

    def is_closed(self) -> bool:
        return self._closed

    async def _ensure_page_enabled(self) -> None:
        await self._client.send("Page.enable", timeout=5)
        try:
            await self._client.send("Runtime.enable", timeout=5)
        except Exception:
            pass

    async def goto(self, url: str, **_kwargs: object) -> object:
        await self._ensure_page_enabled()
        await self._client.send("Page.navigate", {"url": url}, timeout=10)
        try:
            await self._client.wait_event("Page.domContentEventFired", timeout=30)
        except Exception:
            try:
                await self._client.wait_event("Page.loadEventFired", timeout=2)
            except Exception:
                pass
        self.url = url
        return SimpleNamespace(status=0)

    async def click(self, selector: str, **_kwargs: object) -> None:
        await self.evaluate(
            f"(() => {{ const el = document.querySelector({json.dumps(selector)}); if (!el) throw new Error('selector not found'); el.click(); }})()"
        )

    async def fill(self, selector: str, text: str) -> None:
        await self.evaluate(
            "(() => {"
            f" const el = document.querySelector({json.dumps(selector)});"
            " if (!el) throw new Error('selector not found');"
            f" el.value = {json.dumps(text)};"
            " el.dispatchEvent(new Event('input', {bubbles: true}));"
            " el.dispatchEvent(new Event('change', {bubbles: true}));"
            "})()"
        )

    async def inner_text(self, selector: str) -> str:
        value = await self.evaluate(
            f"(() => {{ const el = document.querySelector({json.dumps(selector)}); return el ? el.innerText : ''; }})()"
        )
        return str(value or "")

    def locator(self, selector: str) -> object:
        page = self

        class _Locator:
            @property
            def first(self) -> "_Locator":
                return self

            async def inner_text(self, **_kwargs: object) -> str:
                return await page.inner_text(selector)

        return _Locator()

    async def evaluate(self, script: str) -> object:
        payload = await self._client.send(
            "Runtime.evaluate",
            {"expression": script, "awaitPromise": True, "returnByValue": True},
            timeout=10,
        )
        result = payload.get("result") if isinstance(payload, dict) else {}
        if isinstance(result, dict) and result.get("subtype") == "error":
            raise RuntimeError(str(result.get("description") or result.get("value") or "browser script failed"))
        return result.get("value") if isinstance(result, dict) else None

    async def wait_for_selector(self, selector: str, timeout: int) -> None:
        deadline = asyncio.get_running_loop().time() + (timeout / 1000)
        while True:
            exists = await self.evaluate(f"Boolean(document.querySelector({json.dumps(selector)}))")
            if exists:
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"Timed out waiting for selector {selector!r}")
            await asyncio.sleep(0.1)

    async def wait_for_url(self, url_pattern: str, timeout: int) -> None:
        deadline = asyncio.get_running_loop().time() + (timeout / 1000)
        while True:
            current = str(await self.evaluate("location.href") or "")
            if fnmatch.fnmatch(current, url_pattern):
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"Timed out waiting for URL {url_pattern!r}")
            await asyncio.sleep(0.1)

    async def wait_for_load_state(self, _state: str, timeout: int) -> None:
        try:
            await self._client.wait_event("Page.loadEventFired", timeout=max(0.1, timeout / 1000))
        except Exception:
            pass

    async def query_selector_all(self, selector: str) -> list[object]:
        items = await self.evaluate(
            "(() => Array.from(document.querySelectorAll("
            f"{json.dumps(selector)}"
            ")).slice(0, 50).map(el => ({"
            "tag: el.tagName.toLowerCase(), text: (el.innerText || '').trim(), href: el.getAttribute('href') || ''"
            "})))()"
        )
        if not isinstance(items, list):
            return []

        class _Element:
            def __init__(self, item: dict[str, object]) -> None:
                self.item = item

            async def inner_text(self) -> str:
                return str(self.item.get("text") or "")

            async def evaluate(self, _script: str) -> str:
                return str(self.item.get("tag") or "")

            async def get_attribute(self, name: str) -> str:
                return str(self.item.get(name) or "")

        return [_Element(item) for item in items if isinstance(item, dict)]

    async def layout_metrics(self) -> dict[str, int]:
        metrics = await self._client.send("Page.getLayoutMetrics", timeout=2)
        layout_viewport = metrics.get("layoutViewport") if isinstance(metrics.get("layoutViewport"), dict) else {}
        visual_viewport = metrics.get("visualViewport") if isinstance(metrics.get("visualViewport"), dict) else {}
        content_size = metrics.get("contentSize") if isinstance(metrics.get("contentSize"), dict) else {}
        viewport_width = int(layout_viewport.get("clientWidth") or visual_viewport.get("clientWidth") or 0)
        viewport_height = int(layout_viewport.get("clientHeight") or visual_viewport.get("clientHeight") or 0)
        document_width = int(content_size.get("width") or viewport_width or 0)
        document_height = int(content_size.get("height") or viewport_height or 0)
        return {
            "viewportWidth": max(0, viewport_width),
            "viewportHeight": max(0, viewport_height),
            "documentWidth": max(0, document_width),
            "documentHeight": max(0, document_height),
        }

    async def capture_screenshot(self, params: dict[str, object]) -> dict[str, object]:
        return await self._client.send("Page.captureScreenshot", params, timeout=8)

    async def bring_to_front(self) -> None:
        try:
            await self._client.send("Page.bringToFront", timeout=2)
        except Exception:
            pass

    async def close(self) -> None:
        self._closed = True
        await self._client.close()


class _RawCDPContext:
    def __init__(self, cdp_url: str, targets: list[dict[str, object]]) -> None:
        self.cdp_url = cdp_url
        self.pages = [_RawCDPPage(target) for target in targets if target.get("webSocketDebuggerUrl")]

    async def new_page(self) -> "_RawCDPPage":
        target = await _async_new_cdp_target(self.cdp_url)
        if not target or not target.get("webSocketDebuggerUrl"):
            raise RuntimeError("Connected to CDP, but could not create a new browser tab.")
        page = _RawCDPPage(target)
        self.pages.append(page)
        return page


class _RawCDPBrowser:
    def __init__(self, cdp_url: str) -> None:
        self.cdp_url = cdp_url
        self.contexts: list[_RawCDPContext] = []

    def is_connected(self) -> bool:
        return True

    async def refresh(self) -> "_RawCDPBrowser":
        targets = [
            target
            for target in await _async_cdp_targets(self.cdp_url)
            if str(target.get("type") or "") == "page"
        ]
        self.contexts = [_RawCDPContext(self.cdp_url, targets)]
        return self


class CDPBackend:
    """Attaches to a running Chrome or Brave window via the DevTools Protocol.

    The user can see every action in real time in their existing browser.
    Good for OAuth flows, sites that block headless browsers, and human-in-the-loop
    workflows where the operator wants to watch or intervene.
    """

    BACKEND_NAME = "cdp"

    def __init__(self) -> None:
        _require_playwright()
        self._cdp_url = normalized_cdp_url()
        self._playwright = None
        self._browser: "Browser | _RawCDPBrowser | None" = None
        self._pages: dict[str, "Page | _RawCDPPage"] = {}
        self._lock = asyncio.Lock()

    async def _ensure_raw_browser(self) -> "_RawCDPBrowser":
        browser = _RawCDPBrowser(self._cdp_url)
        self._browser = await browser.refresh()
        return browser

    async def _ensure_browser(self) -> "Browser | _RawCDPBrowser":
        if self._browser is None or not self._browser.is_connected():
            pw = await async_playwright().__aenter__()
            self._playwright = pw
            try:
                self._browser = await pw.chromium.connect_over_cdp(self._cdp_url)
            except Exception as exc:
                message = str(exc)
                if "Browser.setDownloadBehavior" not in message and "Browser context management is not supported" not in message:
                    raise
                try:
                    await pw.__aexit__(None, None, None)
                except Exception:
                    pass
                self._playwright = None
                return await self._ensure_raw_browser()
        return self._browser

    async def _get_page(self, session_id: str) -> "Page | _RawCDPPage":
        async with self._lock:
            if session_id not in self._pages:
                browser = await self._ensure_browser()
                # Use the first existing context (the user's real browser session)
                contexts = browser.contexts
                if contexts:
                    ctx = contexts[0]
                else:
                    raise RuntimeError(
                        "Connected to CDP, but no visible browser context was available. "
                        "Open a normal Chrome, Brave, or Edge tab in the remote-debugging browser and try again."
                    )
                pages = [page for page in ctx.pages if not page.is_closed()]
                if pages:
                    self._pages[session_id] = pages[-1]
                else:
                    try:
                        self._pages[session_id] = await ctx.new_page()
                    except Exception as exc:
                        message = str(exc)
                        if "Browser context management is not supported" in message:
                            raise RuntimeError(
                                "Connected to CDP, but this browser does not allow Playwright to create a new tab. "
                                "Open a normal Chrome, Brave, or Edge tab in the remote-debugging browser and try again."
                            ) from exc
                        raise
        return self._pages[session_id]

    # ── BrowserBackend protocol ───────────────────────────────────────────────

    async def navigate(self, session_id: str, url: str) -> str:
        page = await self._get_page(session_id)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        status = response.status if response else 0
        return f"Navigated to {url} (status {status}) — visible in your browser"

    async def click(self, session_id: str, selector: str) -> None:
        page = await self._get_page(session_id)
        await page.click(selector, timeout=10_000)

    async def type_text(self, session_id: str, selector: str, text: str) -> None:
        page = await self._get_page(session_id)
        await page.fill(selector, text)

    async def extract_text(self, session_id: str, selector: str | None) -> str:
        page = await self._get_page(session_id)
        if selector:
            el = page.locator(selector).first
            return await el.inner_text(timeout=5_000)
        return await page.inner_text("body")

    async def _layout_metrics(self, client: "CDPSession") -> dict[str, int]:
        metrics = await asyncio.wait_for(client.send("Page.getLayoutMetrics"), timeout=2)
        if not isinstance(metrics, dict):
            return {}
        layout_viewport = metrics.get("layoutViewport") if isinstance(metrics.get("layoutViewport"), dict) else {}
        visual_viewport = metrics.get("visualViewport") if isinstance(metrics.get("visualViewport"), dict) else {}
        content_size = metrics.get("contentSize") if isinstance(metrics.get("contentSize"), dict) else {}
        viewport_width = int(layout_viewport.get("clientWidth") or visual_viewport.get("clientWidth") or 0)
        viewport_height = int(layout_viewport.get("clientHeight") or visual_viewport.get("clientHeight") or 0)
        document_width = int(content_size.get("width") or viewport_width or 0)
        document_height = int(content_size.get("height") or viewport_height or 0)
        return {
            "viewportWidth": max(0, viewport_width),
            "viewportHeight": max(0, viewport_height),
            "documentWidth": max(0, document_width),
            "documentHeight": max(0, document_height),
        }

    async def screenshot(self, session_id: str, mode: str = "auto") -> BrowserScreenshotResult:
        page = await self._get_page(session_id)
        client = None
        if isinstance(page, _RawCDPPage):
            await page.bring_to_front()
        else:
            client = await page.context.new_cdp_session(page)
            try:
                await asyncio.wait_for(client.send("Page.bringToFront"), timeout=2)
            except Exception:
                pass
        layout: dict[str, int] = {}
        try:
            if isinstance(page, _RawCDPPage):
                layout = await page.layout_metrics()
            else:
                layout = await self._layout_metrics(client)
        except Exception:
            layout = {}
        viewport_width = layout.get("viewportWidth")
        viewport_height = layout.get("viewportHeight")
        document_width = layout.get("documentWidth")
        document_height = layout.get("documentHeight")
        requested_mode = mode if mode in {"auto", "viewport", "full_page"} else "auto"
        exceeds_viewport = bool(
            viewport_width
            and viewport_height
            and document_width
            and document_height
            and (document_width > viewport_width + 1 or document_height > viewport_height + 1)
        )
        full_page = requested_mode == "full_page" or (
            requested_mode == "auto"
            and auto_screenshot_uses_full_page(getattr(page, "url", None), exceeds_viewport)
        )
        params: dict[str, object] = {
            "format": "png",
            "captureBeyondViewport": full_page,
            "fromSurface": True,
        }
        if full_page and document_width and document_height:
            params["clip"] = {
                "x": 0,
                "y": 0,
                "width": document_width,
                "height": document_height,
                "scale": 1,
            }
        if isinstance(page, _RawCDPPage):
            payload = await page.capture_screenshot(params)
        else:
            payload = await asyncio.wait_for(
                client.send("Page.captureScreenshot", params),
                timeout=8,
            )
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, str) or not data:
            raise RuntimeError("CDP did not return screenshot data.")
        return BrowserScreenshotResult(
            data=base64.b64decode(data),
            mode="full_page" if full_page else "viewport",
            requested_mode=requested_mode,
            viewport_width=viewport_width,
            viewport_height=viewport_height,
            document_width=document_width,
            document_height=document_height,
            is_clipped=not full_page and exceeds_viewport,
        )

    async def scroll(self, session_id: str, direction: str, amount: int) -> None:
        page = await self._get_page(session_id)
        delta_y = amount if direction == "down" else -amount
        await page.evaluate(f"window.scrollBy(0, {delta_y})")

    async def wait_for(
        self,
        session_id: str,
        selector: str | None,
        url_pattern: str | None,
        timeout: float,
    ) -> None:
        page = await self._get_page(session_id)
        timeout_ms = int(timeout * 1000)
        if selector:
            await page.wait_for_selector(selector, timeout=timeout_ms)
        elif url_pattern:
            await page.wait_for_url(url_pattern, timeout=timeout_ms)
        else:
            await page.wait_for_load_state("networkidle", timeout=timeout_ms)

    async def find(self, session_id: str, selector: str) -> list[dict[str, str]]:
        page = await self._get_page(session_id)
        elements = await page.query_selector_all(selector)
        results = []
        for el in elements[:50]:
            text = (await el.inner_text()).strip()
            tag = await el.evaluate("e => e.tagName.toLowerCase()")
            href = await el.get_attribute("href") or ""
            results.append({"tag": tag, "text": text[:200], "href": href})
        return results

    async def run_js(self, session_id: str, script: str) -> Any:
        page = await self._get_page(session_id)
        return await page.evaluate(script)

    async def close_session(self, session_id: str) -> None:
        async with self._lock:
            page = self._pages.pop(session_id, None)
        if page:
            try:
                await page.close()
            except Exception:
                pass

    async def shutdown(self) -> None:
        for session_id in list(self._pages):
            await self.close_session(session_id)
        # Don't close the browser itself — it's the user's existing window
        if self._playwright:
            try:
                await self._playwright.__aexit__(None, None, None)
            except Exception:
                pass

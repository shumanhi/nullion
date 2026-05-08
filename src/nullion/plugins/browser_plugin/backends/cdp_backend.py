"""Browser plugin — CDP (attach to existing Chrome/Brave) backend.

Connects to a browser that was launched with --remote-debugging-port=9222.

Requires: pip install playwright  (uses Playwright's CDP support)

To launch Chrome with CDP enabled:
    /Applications/Google Chrome.app/Contents/MacOS/Google Chrome \
        --remote-debugging-port=9222 --no-first-run

Set NULLION_BROWSER_CDP_URL=http://localhost:9222
"""
from __future__ import annotations

import asyncio
import base64
import os
from typing import Any

from nullion.plugins.browser_plugin.browser_session import (
    BrowserScreenshotResult,
    auto_screenshot_uses_full_page,
)

try:
    from playwright.async_api import async_playwright, CDPSession, Browser, Page
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False


def _require_playwright() -> None:
    if not _PLAYWRIGHT_AVAILABLE:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright"
        )


class CDPBackend:
    """Attaches to a running Chrome or Brave window via the DevTools Protocol.

    The user can see every action in real time in their existing browser.
    Good for OAuth flows, sites that block headless browsers, and human-in-the-loop
    workflows where the operator wants to watch or intervene.
    """

    BACKEND_NAME = "cdp"

    def __init__(self) -> None:
        _require_playwright()
        cdp_url = os.environ.get("NULLION_BROWSER_CDP_URL", "http://localhost:9222")
        self._cdp_url = cdp_url
        self._playwright = None
        self._browser: "Browser | None" = None
        self._pages: dict[str, "Page"] = {}
        self._lock = asyncio.Lock()

    async def _ensure_browser(self) -> "Browser":
        if self._browser is None or not self._browser.is_connected():
            pw = await async_playwright().__aenter__()
            self._playwright = pw
            self._browser = await pw.chromium.connect_over_cdp(self._cdp_url)
        return self._browser

    async def _get_page(self, session_id: str) -> "Page":
        async with self._lock:
            if session_id not in self._pages:
                browser = await self._ensure_browser()
                # Use the first existing context (the user's real browser session)
                contexts = browser.contexts
                if contexts:
                    ctx = contexts[0]
                else:
                    ctx = await browser.new_context()
                self._pages[session_id] = await ctx.new_page()
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
        client = await page.context.new_cdp_session(page)
        try:
            await asyncio.wait_for(client.send("Page.bringToFront"), timeout=2)
        except Exception:
            pass
        layout: dict[str, int] = {}
        try:
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

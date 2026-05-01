"""Browser plugin — Playwright headless backend.

Requires: pip install playwright && playwright install chromium
"""
from __future__ import annotations

import asyncio
import base64
import os
from typing import Any

try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False


def _require_playwright() -> None:
    if not _PLAYWRIGHT_AVAILABLE:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright && playwright install chromium"
        )


class PlaywrightBackend:
    """Headless Chromium backend via Playwright.

    One Page per session_id. Sessions are isolated browser contexts.
    """

    BACKEND_NAME = "playwright"

    def __init__(self) -> None:
        _require_playwright()
        self._playwright = None
        self._browser: "Browser | None" = None
        self._pages: dict[str, "Page"] = {}
        self._lock = asyncio.Lock()
        self._headless = os.environ.get("NULLION_BROWSER_HEADLESS", "true").lower() != "false"

    async def _ensure_browser(self) -> "Browser":
        if self._browser is None or not self._browser.is_connected():
            pw = await async_playwright().__aenter__()
            self._playwright = pw
            self._browser = await pw.chromium.launch(headless=self._headless)
        return self._browser

    async def _get_page(self, session_id: str) -> "Page":
        async with self._lock:
            if session_id not in self._pages:
                browser = await self._ensure_browser()
                ctx: "BrowserContext" = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    )
                )
                self._pages[session_id] = await ctx.new_page()
        return self._pages[session_id]

    # ── BrowserBackend protocol ───────────────────────────────────────────────

    async def navigate(self, session_id: str, url: str) -> str:
        page = await self._get_page(session_id)
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        status = response.status if response else 0
        return f"Navigated to {url} (status {status})"

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

    async def screenshot(self, session_id: str) -> bytes:
        page = await self._get_page(session_id)
        return await page.screenshot(type="png", timeout=8_000)

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
        for el in elements[:50]:  # cap at 50
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
                await page.context.close()
            except Exception:
                pass

    async def shutdown(self) -> None:
        for session_id in list(self._pages):
            await self.close_session(session_id)
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.__aexit__(None, None, None)
            except Exception:
                pass

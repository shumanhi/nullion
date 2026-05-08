"""Browser plugin — Playwright headless backend.

Requires: pip install playwright && playwright install chromium
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import platform
from typing import Any

from nullion.plugins.browser_plugin.browser_session import (
    BrowserScreenshotResult,
    auto_screenshot_uses_full_page,
)

try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False

try:
    from playwright_stealth import Stealth
    _STEALTH_AVAILABLE = True
except ImportError:
    Stealth = None  # type: ignore[assignment]
    _STEALTH_AVAILABLE = False


logger = logging.getLogger(__name__)


def _require_playwright() -> None:
    if not _PLAYWRIGHT_AVAILABLE:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright && playwright install chromium"
        )


def _context_user_agent(browser_version: str) -> str:
    """Return a UA aligned with the actual bundled Chromium build."""

    version = str(browser_version or "").strip() or "0.0.0.0"
    system = platform.system()
    if system == "Windows":
        platform_token = "Windows NT 10.0; Win64; x64"
    elif system == "Darwin":
        platform_token = "Macintosh; Intel Mac OS X 10_15_7"
    else:
        platform_token = "X11; Linux x86_64"
    return (
        f"Mozilla/5.0 ({platform_token}) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{version} Safari/537.36"
    )


def _navigator_platform() -> str:
    system = platform.system()
    if system == "Windows":
        return "Win32"
    if system == "Darwin":
        return "MacIntel"
    return "Linux x86_64"


def _stealth_context_manager(playwright_context_manager: Any) -> Any:
    if not _STEALTH_AVAILABLE or Stealth is None:
        logger.warning("playwright-stealth is not installed; continuing without headless stealth patches.")
        return playwright_context_manager
    return Stealth(navigator_platform_override=_navigator_platform()).use_async(playwright_context_manager)


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
            playwright_context_manager: Any = async_playwright()
            if self._headless:
                playwright_context_manager = _stealth_context_manager(playwright_context_manager)
            pw = await playwright_context_manager.__aenter__()
            self._playwright = playwright_context_manager
            self._browser = await pw.chromium.launch(headless=self._headless)
        return self._browser

    async def _get_page(self, session_id: str) -> "Page":
        async with self._lock:
            if session_id not in self._pages:
                browser = await self._ensure_browser()
                ctx: "BrowserContext" = await browser.new_context(
                    user_agent=_context_user_agent(browser.version),
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

    async def _page_layout(self, page: "Page") -> dict[str, int]:
        layout = await page.evaluate(
            """() => {
                const doc = document.documentElement || {};
                const body = document.body || {};
                const viewportWidth = window.innerWidth || doc.clientWidth || 0;
                const viewportHeight = window.innerHeight || doc.clientHeight || 0;
                const documentWidth = Math.max(
                    doc.scrollWidth || 0,
                    body.scrollWidth || 0,
                    viewportWidth
                );
                const documentHeight = Math.max(
                    doc.scrollHeight || 0,
                    body.scrollHeight || 0,
                    viewportHeight
                );
                return { viewportWidth, viewportHeight, documentWidth, documentHeight };
            }"""
        )
        if not isinstance(layout, dict):
            return {}
        return {
            key: max(0, int(layout.get(key) or 0))
            for key in ("viewportWidth", "viewportHeight", "documentWidth", "documentHeight")
        }

    async def screenshot(self, session_id: str, mode: str = "auto") -> BrowserScreenshotResult:
        page = await self._get_page(session_id)
        requested_mode = mode if mode in {"auto", "viewport", "full_page"} else "auto"
        layout = await self._page_layout(page)
        viewport_width = layout.get("viewportWidth")
        viewport_height = layout.get("viewportHeight")
        document_width = layout.get("documentWidth")
        document_height = layout.get("documentHeight")
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
        data = await page.screenshot(type="png", full_page=full_page, timeout=8_000)
        return BrowserScreenshotResult(
            data=data,
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

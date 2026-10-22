from __future__ import annotations

import asyncio
import base64
import gc
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote
from types import SimpleNamespace

import pytest
import websockets

from nullion.plugins.browser_plugin.backends import auto_backend, cdp_backend, playwright_backend
from nullion.plugins.browser_plugin import register_browser_tools
from nullion.plugins.browser_plugin.browser_config import DEFAULT_AGENT_BROWSER_SESSION_ID, browser_user_data_dir
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_session import BrowserScreenshotResult, BrowserSessionPool
from nullion.plugins.browser_plugin.browser_tools import (
    BrowserTools,
    _browser_assert_page_state_script,
    _browser_extract_detail_script,
    _browser_extract_items_script,
    _browser_type_id_script,
    _run,
)
from nullion.tools import ToolInvocation, ToolRegistry
from nullion import web_app


def inv(tool_name: str, arguments: dict[str, object], *, capsule_id: str | None = "scope") -> ToolInvocation:
    session_id = str(arguments.get("session_id") or "").strip()
    return ToolInvocation(
        invocation_id=f"inv-{tool_name}",
        tool_name=tool_name,
        principal_id="operator",
        arguments=arguments,
        capsule_id=capsule_id,
        flow_context={"browser_session_id": session_id} if session_id and session_id != "default" else {},
    )


def scheduled_inv(
    tool_name: str,
    arguments: dict[str, object],
    *,
    scope: str,
) -> ToolInvocation:
    return ToolInvocation(
        invocation_id=f"inv-{tool_name}",
        tool_name=tool_name,
        principal_id="operator",
        arguments=arguments,
        capsule_id="cron-capsule",
        flow_context={
            "scheduled_task_run": True,
            "isolated_browser_session": True,
            "browser_session_scope": scope,
        },
    )


def _new_playwright_backend_or_skip(monkeypatch, session_id: str = "playwright-availability"):
    pytest.importorskip("playwright.async_api")
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "true")
    backend = playwright_backend.PlaywrightBackend()
    try:
        _run(backend.open(session_id))
    except Exception as exc:
        _run(backend.shutdown())
        message = str(exc)
        if "Executable doesn't exist" in message or "playwright install" in message:
            pytest.skip("Playwright browser executable is not installed")
        raise
    _run(backend.close_session(session_id))
    return backend


class CompleteBackend:
    BACKEND_NAME = "fake"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple] = []
        self.closed: list[str] = []
        self.shutdown_called = False

    async def navigate(self, session_id: str, url: str) -> str:
        self.calls.append(("navigate", session_id, url))
        if self.fail:
            raise RuntimeError("no route")
        return f"navigated:{url}"

    async def open(self, session_id: str) -> str:
        self.calls.append(("open", session_id))
        if self.fail:
            raise RuntimeError("no browser")
        return f"opened:{session_id}"

    async def click(self, session_id: str, selector: str) -> None:
        self.calls.append(("click", session_id, selector))

    async def click_element(self, session_id: str, target: dict[str, object]) -> None:
        self.calls.append(("click_element", session_id, target))

    async def type_text(self, session_id: str, selector: str, text: str) -> None:
        self.calls.append(("type", session_id, selector, text))

    async def type_field(self, session_id: str, target: dict[str, object], text: str) -> None:
        self.calls.append(("type_field", session_id, target, text))

    async def extract_text(self, session_id: str, selector: str | None) -> str:
        self.calls.append(("extract", session_id, selector))
        return "hello page"

    async def screenshot(self, session_id: str, mode: str = "auto") -> BrowserScreenshotResult:
        self.calls.append(("screenshot", session_id, mode))
        return BrowserScreenshotResult(
            data=b"png",
            mode="full_page",
            requested_mode=mode,
            viewport_width=1280,
            viewport_height=720,
            document_width=1280,
            document_height=1500,
            is_clipped=False,
        )

    async def scroll(self, session_id: str, direction: str, amount: int) -> None:
        self.calls.append(("scroll", session_id, direction, amount))

    async def wait_for(
        self,
        session_id: str,
        selector: str | None,
        url_pattern: str | None,
        text: str | None,
        timeout: float,
    ) -> None:
        self.calls.append(("wait", session_id, selector, url_pattern, text, timeout))

    async def find(self, session_id: str, selector: str) -> list[dict[str, str]]:
        self.calls.append(("find", session_id, selector))
        return [{"tag": "a", "text": "Example", "href": "https://example.com"}]

    async def run_js(self, session_id: str, script: str):
        self.calls.append(("js", session_id, script))
        if "ambiguous_detail_scope" in script and "record_scope" in script:
            return {
                "ok": True,
                "selector": "#detail",
                "record_scope": "selected_root",
                "page": {"url": "https://example.com/item/1", "title": "Example item"},
                "record": {
                    "title": "Example item",
                    "url": "https://example.com/item/1",
                    "price": "$12.34",
                    "details": "Example item current detail",
                },
            }
        if "source_index" in script and "numeric_snippets" in script:
            return {
                "url": "https://example.com/results",
                "title": "Results",
                "item_count": 1,
                "items": [
                    {
                        "source_index": 1,
                        "url": "https://example.com/item/1",
                        "url_path_depth": 2,
                        "title": "Example item",
                        "link_text": "Example item",
                        "image_url": "https://example.com/item/1.png",
                        "image_alt": "Example item photo",
                        "compact_text": "Example item $12.34",
                        "numeric_snippets": ["Example item $12.34"],
                    }
                ],
            }
        if "element_count" in script and "__nullionBrowserElementSeq" in script:
            return {
                "url": "https://example.com",
                "title": "Example",
                "active_element_id": "",
                "element_count": 1,
                "elements": [{"element_id": "n-1", "role": "button", "text": "Go"}],
            }
        if "clicked: true" in script:
            return {"ok": True, "clicked": True, "element_id": "n-1"}
        if "after_value" in script and "typed:" in script:
            return {"ok": True, "element_id": "n-2", "after_value": "abc", "typed": 3, "verified": True}
        if "selected_text" in script and "no_compatible_option" in script:
            return {
                "ok": True,
                "element_id": "n-3",
                "after_value": "Knockdown Center",
                "selected_text": "Knockdown Center",
                "verified": True,
                "options": ["Knockdown Center"],
            }
        if "forbidden_found" in script and "required" in script:
            return {
                "ok": True,
                "url": "https://example.com",
                "title": "Example",
                "required": [{"expected": "Knockdown Center", "match": "Knockdown Center"}],
                "forbidden": [],
                "missing": [],
                "forbidden_found": [],
                "candidates": ["Knockdown Center"],
            }
        return {"ok": True}

    async def close_session(self, session_id: str) -> None:
        self.closed.append(session_id)

    async def shutdown(self) -> None:
        self.shutdown_called = True


class BlankDefaultThenActiveAssertBackend(CompleteBackend):
    BACKEND_NAME = "auto"

    def __init__(self) -> None:
        super().__init__()
        self.active_session_id = ""

    async def navigate(self, session_id: str, url: str) -> str:
        self.active_session_id = session_id
        return await super().navigate(session_id, url)

    async def run_js(self, session_id: str, script: str):
        self.calls.append(("js", session_id, script))
        if "forbidden_found" in script and "required" in script:
            if session_id == self.active_session_id:
                return {
                    "ok": True,
                    "url": "https://www.amazon.com/gp/css/order-history",
                    "title": "Amazon.com",
                    "required": [{"expected": "Amazon", "match": "Amazon"}],
                    "forbidden": [],
                    "missing": [],
                    "forbidden_found": [],
                    "candidates": ["Amazon"],
                }
            return {
                "ok": False,
                "url": "about:blank",
                "title": "",
                "required": [{"expected": "Amazon", "match": ""}],
                "forbidden": [],
                "missing": [{"expected": "Amazon", "match": ""}],
                "forbidden_found": [],
                "candidates": [],
            }
        return await super().run_js(session_id, script)


class IllegalReturnOnceBackend(CompleteBackend):
    async def run_js(self, session_id: str, script: str):
        self.calls.append(("js", session_id, script))
        if not script.lstrip().startswith("async () =>"):
            raise RuntimeError("SyntaxError: Illegal return statement")
        return {"wrapped": True}


class StaleElementBackend(CompleteBackend):
    async def run_js(self, session_id: str, script: str):
        self.calls.append(("js", session_id, script))
        if "element_count" in script and "__nullionBrowserElementSeq" in script:
            return {
                "url": "https://example.com",
                "title": "Example",
                "active_element_id": "",
                "element_count": 2,
                "elements": [
                    {"element_id": "n-1", "role": "button", "text": "See prices", "editable": False},
                    {
                        "element_id": "n-2",
                        "role": "textbox",
                        "label": "Pickup location",
                        "placeholder": "Where from?",
                        "editable": True,
                    },
                ],
            }
        if "clicked: true" in script:
            return {"ok": False, "reason": "element_not_found", "element_id": "n-1"}
        if "after_value" in script and "typed:" in script:
            if '"n-1"' in script:
                return {"ok": False, "reason": "element_not_editable", "element_id": "n-1"}
            return {
                "ok": True,
                "element_id": "n-2",
                "before_value": "",
                "after_value": "",
                "typed": 17,
                "verified": False,
            }
        return {"ok": True}


class HiddenEditableBackend(CompleteBackend):
    async def run_js(self, session_id: str, script: str):
        self.calls.append(("js", session_id, script))
        if "element_count" in script and "__nullionBrowserElementSeq" in script:
            return {
                "url": "https://example.com",
                "title": "Example",
                "active_element_id": "",
                "element_count": 1,
                "elements": [
                    {
                        "element_id": "n-1",
                        "role": "textbox",
                        "label": "Where",
                        "placeholder": "Search destinations",
                        "name": "query",
                        "editable": True,
                        "visible": False,
                    },
                ],
            }
        if "after_value" in script and "typed:" in script:
            raise AssertionError("hidden editable controls should use semantic type_field before JS assignment")
        return {"ok": True}


class ZeroMatchLocator:
    def __init__(self) -> None:
        self.click_calls = 0

    async def count(self) -> int:
        return 0

    def nth(self, index: int):
        raise AssertionError("zero-match locators should not enumerate candidates")

    def first(self):
        return self

    async def click(self, **kwargs) -> None:
        self.click_calls += 1
        raise AssertionError("zero-match semantic locators should not click")

    async def dispatch_event(self, *args, **kwargs) -> None:
        raise AssertionError("zero-match semantic locators should not dispatch")


class ZeroMatchPage:
    def __init__(self) -> None:
        self.zero_locator = ZeroMatchLocator()

    def get_by_text(self, text: str):
        return self.zero_locator

    def locator(self, selector: str):
        return self.zero_locator

    async def evaluate(self, *args, **kwargs):
        return False


class TextFallbackLocator:
    def first(self):
        return self


class TextFallbackPage:
    def __init__(self) -> None:
        self.scripts: list[str] = []

    def locator(self, selector: str):
        return TextFallbackLocator()

    async def evaluate(self, script: str, *args, **kwargs):
        self.scripts.append(script)
        if "const needle" in script:
            return False
        return True


class SemanticTypeFallbackLocator:
    def __init__(self, *, kind: str, token: str) -> None:
        self.kind = kind
        self.token = token
        self.value = ""

    def first(self):
        return self

    async def count(self) -> int:
        if self.kind == "selector":
            raise RuntimeError("Unexpected token '&' while parsing css selector")
        return 1

    def nth(self, _index: int):
        return self

    async def is_enabled(self, *args, **kwargs) -> bool:
        return True

    async def is_visible(self, *args, **kwargs) -> bool:
        return True

    async def fill(self, text: str, **_kwargs) -> None:
        if self.kind == "selector":
            raise RuntimeError("Unexpected token '&' while parsing css selector")
        self.value = text

    async def input_value(self, **_kwargs) -> str:
        return self.value


class SemanticTypeFallbackPage:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.locators: list[SemanticTypeFallbackLocator] = []

    def _locator(self, kind: str, token: str) -> SemanticTypeFallbackLocator:
        self.calls.append((kind, token))
        locator = SemanticTypeFallbackLocator(kind=kind, token=token)
        self.locators.append(locator)
        return locator

    def locator(self, selector: str):
        return self._locator("selector", selector)

    def get_by_label(self, text: str):
        return self._locator("label", text)

    def get_by_placeholder(self, text: str):
        return self._locator("placeholder", text)

    def get_by_text(self, text: str):
        return self._locator("text", text)

    async def evaluate(self, *args, **kwargs):
        return False


class TimeoutThenDomLocator:
    def first(self):
        return self

    async def count(self) -> int:
        return 1

    def nth(self, _index: int):
        return self

    async def is_enabled(self, *args, **kwargs) -> bool:
        return True

    async def is_visible(self, *args, **kwargs) -> bool:
        return True

    async def fill(self, _text: str, **_kwargs) -> None:
        raise RuntimeError('Timeout 10000ms exceeded waiting for locator("Pick-Up Location")')

    async def input_value(self, **_kwargs) -> str:
        return ""


class SemanticDomTypePage:
    def __init__(self) -> None:
        self.label_locator = TimeoutThenDomLocator()
        self.evaluate_payloads: list[dict[str, object]] = []

    def get_by_label(self, _text: str):
        return self.label_locator

    def locator(self, _selector: str):
        return self.label_locator

    async def evaluate(self, script: str, payload: dict[str, object] | None = None):
        if "const rawTargets" in script and payload is not None:
            self.evaluate_payloads.append(payload)
            return {"ok": True, "before": "", "after": payload.get("text"), "matched": "pick up location"}
        return False


class GenericInputLocator:
    def first(self):
        return self

    async def count(self) -> int:
        return 1

    def nth(self, _index: int):
        return self

    async def is_enabled(self, *args, **kwargs) -> bool:
        return True

    async def is_visible(self, *args, **kwargs) -> bool:
        return True

    async def fill(self, _text: str, **_kwargs) -> None:
        return None

    async def input_value(self, **_kwargs):
        raise RuntimeError("value unavailable")


class GenericInputPage:
    def __init__(self) -> None:
        self.locator_obj = GenericInputLocator()
        self.fills = 0

    def locator(self, _selector: str):
        return self.locator_obj

    def fill(self, _selector: str, _text: str, **_kwargs) -> None:
        self.fills += 1

    async def evaluate(self, script: str, payload: dict[str, object] | None = None):
        if "const rawTargets" in script:
            return {"ok": False, "reason": "no_semantic_target"}
        return False


def test_cdp_urls_normalize_localhost_to_ipv4_loopback(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://localhost:9222")
    assert cdp_backend.normalized_cdp_url() == "http://127.0.0.1:9222"
    assert cdp_backend.normalized_cdp_url("http://localhost:9222") == "http://127.0.0.1:9222"
    assert cdp_backend.normalized_cdp_url("http://[::1]:9222") == "http://127.0.0.1:9222"
    assert cdp_backend.normalized_cdp_url("http://192.168.1.10:9333") == "http://192.168.1.10:9333"

    monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://localhost:9333")
    assert auto_backend._cdp_url() == "http://127.0.0.1:9333"
    assert auto_backend._cdp_endpoint() == ("127.0.0.1", 9333)


def test_semantic_browser_click_fails_fast_when_target_has_zero_matches() -> None:
    page = ZeroMatchPage()
    backend = object.__new__(playwright_backend.PlaywrightBackend)

    async def get_page(session_id: str):
        return page

    backend._get_page = get_page

    with pytest.raises(RuntimeError, match="No matching element found"):
        _run(backend.click_element("s1", {"text": "dummy"}))
    assert page.zero_locator.click_calls == 0

    with pytest.raises(RuntimeError, match="No matching element found"):
        _run(cdp_backend._click_target_with_fallback(page, {"text": "dummy"}))
    assert page.zero_locator.click_calls == 0

    candidates = _run(cdp_backend._candidate_locators_for_target(page, {"selector": ".late"}))
    assert candidates == [page.zero_locator]


def test_cdp_semantic_click_falls_back_to_dom_target_when_locator_has_no_click() -> None:
    page = TextFallbackPage()

    _run(cdp_backend._click_target_with_fallback(page, {"text": "Download"}))

    assert page.scripts
    assert "Download" in page.scripts[-1]
    assert "target not found" in page.scripts[-1]


def test_semantic_browser_type_field_falls_back_when_selector_is_visible_label() -> None:
    label = "Pick-up & Return Location (ZIP, City or Airport)* * Required Field"

    playwright_page = SemanticTypeFallbackPage()
    playwright = object.__new__(playwright_backend.PlaywrightBackend)

    async def get_playwright_page(session_id: str):
        return playwright_page

    playwright._get_page = get_playwright_page
    _run(playwright.type_field("s1", {"selector": label}, "Providence, RI"))

    assert ("selector", label) in playwright_page.calls
    assert ("label", label) in playwright_page.calls
    assert any(locator.kind == "label" and locator.value == "Providence, RI" for locator in playwright_page.locators)

    cdp_page = SemanticTypeFallbackPage()
    _run(cdp_backend._type_target_with_fallback(cdp_page, {"selector": label}, "Providence, RI"))

    assert ("selector", label) in cdp_page.calls
    assert ("label", label) in cdp_page.calls
    assert any(locator.kind == "label" and locator.value == "Providence, RI" for locator in cdp_page.locators)


def test_semantic_browser_type_field_uses_dom_target_after_label_timeout() -> None:
    playwright_page = SemanticDomTypePage()
    playwright = object.__new__(playwright_backend.PlaywrightBackend)

    async def get_playwright_page(session_id: str):
        return playwright_page

    playwright._get_page = get_playwright_page
    _run(playwright.type_field("s1", {"label": "Pick-Up Location"}, "Providence, RI"))

    assert playwright_page.evaluate_payloads[-1]["target"] == {"label": "Pick-Up Location"}
    assert playwright_page.evaluate_payloads[-1]["text"] == "Providence, RI"

    cdp_page = SemanticDomTypePage()
    _run(cdp_backend._type_target_with_fallback(cdp_page, {"label": "Pick-Up Location"}, "Providence, RI"))

    assert cdp_page.evaluate_payloads[-1]["target"] == {"label": "Pick-Up Location"}
    assert cdp_page.evaluate_payloads[-1]["text"] == "Providence, RI"


def test_semantic_browser_type_field_rejects_unverified_generic_input_selector() -> None:
    playwright_page = GenericInputPage()
    playwright = object.__new__(playwright_backend.PlaywrightBackend)

    async def get_playwright_page(session_id: str):
        return playwright_page

    playwright._get_page = get_playwright_page
    with pytest.raises(RuntimeError, match="Typed text did not appear"):
        _run(playwright.type_field("s1", {"selector": "input"}, "Providence, RI"))

    cdp_page = GenericInputPage()
    with pytest.raises(RuntimeError, match="value unavailable|Typed text did not appear"):
        _run(cdp_backend._type_target_with_fallback(cdp_page, {"selector": "input"}, "Providence, RI"))
    assert cdp_page.fills == 0


def test_browser_type_field_rejects_hidden_selector_without_writing() -> None:
    class HiddenLocator:
        def __init__(self) -> None:
            self.fill_calls = 0

        @property
        def first(self):
            return self

        async def count(self):
            return 1

        def nth(self, _index):
            return self

        async def is_visible(self, **_kwargs):
            return False

        async def is_enabled(self, **_kwargs):
            return True

        async def fill(self, *_args, **_kwargs):
            self.fill_calls += 1
            raise AssertionError("hidden selector should not be filled")

        async def input_value(self, **_kwargs):
            return ""

    class HiddenSelectorPage:
        def __init__(self) -> None:
            self.locator_obj = HiddenLocator()
            self.fill_calls = 0
            self.evaluations: list[tuple[str, tuple[object, ...]]] = []

        def locator(self, _selector):
            return self.locator_obj

        async def fill(self, *_args, **_kwargs):
            self.fill_calls += 1
            raise AssertionError("raw hidden selector should not be filled")

        async def evaluate(self, script, *args):
            self.evaluations.append((script, args))
            if "visible && enabled && editable" in script:
                return False
            if "payload && payload.target" in script:
                return {"ok": False, "reason": "target_not_found"}
            return False

    playwright_page = HiddenSelectorPage()
    playwright = object.__new__(playwright_backend.PlaywrightBackend)
    playwright._get_page = lambda session_id: asyncio.sleep(0, result=playwright_page)

    with pytest.raises(RuntimeError, match="hidden|visible editable"):
        _run(playwright.type_field("s1", {"selector": "#pickupLocationTextBox"}, "Providence, RI"))

    assert playwright_page.locator_obj.fill_calls == 0
    assert playwright_page.fill_calls == 0

    cdp_page = HiddenSelectorPage()
    with pytest.raises(RuntimeError, match="hidden|visible editable"):
        _run(cdp_backend._type_target_with_fallback(cdp_page, {"selector": "#pickupLocationTextBox"}, "Providence, RI"))

    assert cdp_page.locator_obj.fill_calls == 0
    assert cdp_page.fill_calls == 0


def test_browser_type_id_script_rejects_hidden_inputs() -> None:
    script = _browser_type_id_script("n-1", "Providence, RI")

    assert "element_not_visible" in script
    assert "type !== 'hidden'" in script


def test_browser_extract_items_script_preserves_url_identity_after_nested_unwrap() -> None:
    script = _browser_extract_items_script(max_items=5)
    url_resolution = script.split("const absoluteUrl", 1)[1].split("const tokenSet", 1)[0]

    assert "nestedHttpUrl" in script
    assert "looksLikeNestedUrl" in script
    assert "if (!looksLikeNestedUrl(decoded)) continue;" in script
    assert "decodedRaw.matchAll(/https?:\\/\\//ig)" in script
    assert "stableTextKey" in script
    assert "seenTitles" in script
    assert "const resolvedItemUrl = (value) => nestedHttpUrl(value) || absoluteUrl(value);" in script
    assert "url: resolvedUrl || url" in script
    assert "canonical_url: resolvedUrl || url" in script
    assert "url.hash = ''" not in url_resolution
    assert "url.search = ''" not in url_resolution
    assert ".toLowerCase()" not in url_resolution
    assert "textOf(rawRootText, 180)" in script
    assert "snippets.length >= 3" in script
    assert "priceSnippets" in script
    assert "searchQueryTokens" in script
    assert "matchesSearchQuery" in script
    assert "query_tokens: searchQueryTokens" in script
    assert "price_text: prices[0] || ''" in script


def test_browser_cdp_url_tracks_known_preferred_browser_defaults() -> None:
    assert web_app._browser_cdp_url_for_preference("http://localhost:9222", "brave") == "http://127.0.0.1:9223"
    assert web_app._browser_cdp_url_for_preference("http://127.0.0.1:9223", "chrome") == "http://127.0.0.1:9222"
    assert web_app._browser_cdp_url_for_preference("http://127.0.0.1:9444", "brave") == "http://127.0.0.1:9444"


def test_auto_backend_cdp_reachability_uses_normalized_endpoint(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_connect(endpoint, timeout):
        calls.append(endpoint)
        assert timeout == 1
        return Conn()

    monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://localhost:9444")
    monkeypatch.setattr(auto_backend.socket, "create_connection", fake_connect)

    assert auto_backend._is_cdp_reachable() is True
    assert calls == [("127.0.0.1", 9444)]


def test_auto_backend_profile_dir_uses_active_runtime_home(monkeypatch, tmp_path) -> None:
    lane_home = tmp_path / "stage-home"
    monkeypatch.setenv("NULLION_HOME", str(lane_home))
    monkeypatch.delenv("NULLION_BROWSER_USER_DATA_DIR", raising=False)

    assert browser_user_data_dir("brave") == lane_home / "browser-profiles" / "brave"
    assert auto_backend._browser_profile_dir("/Applications/Brave Browser.app/Contents/MacOS/Brave Browser") == str(
        lane_home / "browser-profiles" / "brave"
    )


def test_auto_backend_profile_dir_honors_explicit_override(monkeypatch, tmp_path) -> None:
    explicit = tmp_path / "shared-browser-profile"
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "lane-home"))
    monkeypatch.setenv("NULLION_BROWSER_USER_DATA_DIR", str(explicit))

    assert auto_backend._browser_profile_dir("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome") == str(
        explicit
    )


def test_browser_tools_cover_success_failure_and_cleanup(monkeypatch, tmp_path) -> None:
    backend = CompleteBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    screenshot_path = tmp_path / "shot.png"
    monkeypatch.setattr(
        "nullion.plugins.browser_plugin.browser_tools.artifact_path_for_generated_workspace_file",
        lambda **kwargs: screenshot_path,
    )

    assert tools.browser_navigate(inv("browser_navigate", {})).error == "Missing required argument: url"
    assert tools.browser_open(inv("browser_open", {"session_id": "s1"})).output["result"] == "opened:s1"
    assert "Only http/https" in tools.browser_navigate(inv("browser_navigate", {"url": "file:///etc/passwd"})).error
    assert tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com", "session_id": "s1"})).output["result"] == "navigated:https://example.com"
    assert tools.browser_click(inv("browser_click", {})).error == "Missing required argument: selector"
    assert tools.browser_click(inv("browser_click", {"selector": "#go", "session_id": "s1"})).output["clicked"] == "#go"
    assert tools.browser_click_element(inv("browser_click_element", {"text": "See prices", "session_id": "s1"})).output["clicked"] == {"text": "See prices"}
    assert ("click_element", "s1", {"text": "See prices"}) in backend.calls
    assert tools.browser_type(inv("browser_type", {})).error == "Missing required argument: selector"
    assert tools.browser_type(inv("browser_type", {"selector": "input", "text": "abc", "session_id": "s1"})).output["typed"] == 3
    assert tools.browser_type_field(inv("browser_type_field", {"label": "Where from?", "text": "116 West", "session_id": "s1"})).output["typed"] == 8
    assert ("type_field", "s1", {"label": "Where from?"}, "116 West") in backend.calls
    snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "s1"}))
    assert snapshot.output["snapshot"]["elements"][0]["element_id"] == "n-1"
    extracted_items = tools.browser_extract_items(inv("browser_extract_items", {"max_items": 5, "session_id": "s1"}))
    assert extracted_items.output["item_count"] == 1
    assert extracted_items.output["items"][0]["url"] == "https://example.com/item/1"
    assert extracted_items.output["items"][0]["image_url"] == "https://example.com/item/1.png"
    extracted_detail = tools.browser_extract_detail(
        inv("browser_extract_detail", {"selector": "#detail", "session_id": "s1"})
    )
    assert extracted_detail.output["kind"] == "browser_dom_detail"
    assert extracted_detail.output["origin"] == "runtime_owned_dom_extractor"
    assert extracted_detail.output["record"]["price"] == "$12.34"
    assert tools.browser_click_id(inv("browser_click_id", {"element_id": "n-1", "session_id": "s1"})).output["clicked"] == "n-1"
    typed_id = tools.browser_type_id(inv("browser_type_id", {"element_id": "n-2", "text": "abc", "session_id": "s1"}))
    assert typed_id.output["typed"] == 3
    selected = tools.browser_select_combobox(
        inv(
            "browser_select_combobox",
            {"label": "Dropoff location", "query": "Knockdown Center", "expected_text": "Knockdown Center", "session_id": "s1"},
        )
    )
    assert selected.output["selected"] == "Knockdown Center"
    assert tools.browser_extract_text(inv("browser_extract_text", {"selector": "body", "session_id": "s1"})).output["length"] == 10
    screenshot = tools.browser_screenshot(inv("browser_screenshot", {"session_id": "s1"}))
    assert screenshot.output["artifact_path"] == str(screenshot_path)
    assert screenshot.output["artifact_paths"] == [str(screenshot_path)]
    assert screenshot.output["format"] == "png"
    assert screenshot.output["mode"] == "full_page"
    assert screenshot.output["requested_mode"] == "auto"
    assert screenshot.output["viewport_height"] == 720
    assert screenshot.output["document_height"] == 1500
    assert screenshot.output["is_clipped"] is False
    assert screenshot_path.read_bytes() == b"png"
    assert ("screenshot", "s1", "auto") in backend.calls
    assert tools.browser_screenshot(inv("browser_screenshot", {"session_id": "s1", "mode": "sideways"})).error == (
        "mode must be one of: auto, viewport, full_page"
    )
    assert tools.browser_scroll(inv("browser_scroll", {"direction": "sideways"})).error == "direction must be 'up' or 'down'"
    assert tools.browser_scroll(inv("browser_scroll", {"direction": "up", "amount": 100, "session_id": "s1"})).output["amount"] == 100
    assert tools.browser_wait_for(inv("browser_wait_for", {})).error == "Provide selector, url_pattern, or text"
    assert tools.browser_wait_for(inv("browser_wait_for", {"selector": "#ready", "session_id": "s1"})).output["waited_for"] == "#ready"
    assert tools.browser_wait_for(inv("browser_wait_for", {"text": "See prices", "session_id": "s1"})).output["waited_for"] == "See prices"
    assert tools.browser_assert_page_state(
        inv("browser_assert_page_state", {"required": ["Knockdown Center"], "session_id": "s1"})
    ).output["verified"] is True
    assert tools.browser_find(inv("browser_find", {})).error == "Missing required argument: selector"
    assert tools.browser_find(inv("browser_find", {"selector": "a", "session_id": "s1"})).output["count"] == 1
    assert tools.browser_run_js(inv("browser_run_js", {})).error == "Missing required argument: script"
    assert tools.browser_run_js(inv("browser_run_js", {"script": "1+1", "session_id": "s1"})).output["result"] == {"ok": True}
    return_backend = IllegalReturnOnceBackend()
    return_tools = BrowserTools(backend=return_backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    assert return_tools.browser_run_js(
        inv("browser_run_js", {"script": "return document.location.href", "session_id": "s1"})
    ).output["result"] == {"wrapped": True}
    assert return_backend.calls[-1][2].startswith("async () =>")
    assert tools.browser_close(inv("browser_close", {"session_id": "s1"})).output["closed"] == "s1"
    assert "s1" in backend.closed

    failing = BrowserTools(backend=CompleteBackend(fail=True), pool=BrowserSessionPool(), policy=BrowserPolicy())
    assert failing.browser_navigate(inv("browser_navigate", {"url": "https://example.com"})).error.startswith("Navigation failed:")

    backend.closed.clear()
    tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com", "session_id": "s2"}, capsule_id="scope-a"))
    tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com", "session_id": "s3"}, capsule_id="scope-b"))
    tools.close_tracked_sessions()
    assert backend.closed[-2:] == ["s2", "s3"]


def test_browser_evidence_read_revalidates_only_the_same_turn_typed_contract() -> None:
    base_url = "https://example.com/product?variant=liquid"
    anchored_url = f"{base_url}#details"

    class EvidenceBackend(CompleteBackend):
        page_url = base_url

        async def click_element(self, session_id: str, target: dict[str, object]) -> None:
            await super().click_element(session_id, target)
            self.page_url = anchored_url

        async def run_js(self, session_id: str, script: str):
            self.calls.append(("js", session_id, script))
            if "forbidden_found" in script and "const args" in script:
                return {
                    "ok": True,
                    "url": self.page_url,
                    "selector": "#product",
                    "required": [{"expected": "Liquid product", "match": "Liquid product"}],
                    "forbidden": [{"expected": "solid bar", "match": ""}],
                    "missing": [],
                    "forbidden_found": [],
                }
            return {"title": "Liquid product", "url": self.page_url, "price": "$12"}

    tools = BrowserTools(
        backend=EvidenceBackend(),
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )
    assertion = tools.browser_assert_page_state(
        inv(
            "browser_assert_page_state",
            {
                "required": ["Liquid product"],
                "forbidden": ["solid bar"],
                "selector": "#product",
                "session_id": "s1",
            },
        )
    )
    assert assertion.output["verified"] is True
    tools.browser_click_element(
        inv("browser_click_element", {"role": "link", "name": "Details", "session_id": "s1"})
    )
    evidence = tools.browser_run_js(
        inv("browser_run_js", {"script": "() => ({title: document.title})", "session_id": "s1"})
    )

    revalidated = evidence.output["page_state_assertion"]
    assert revalidated["schema_version"] == 1
    assert revalidated["kind"] == "page_state_assertion"
    assert revalidated["session_id"] == "s1"
    assert revalidated["verified"] is True
    assert revalidated["result"]["url"] == anchored_url
    other_turn = tools.browser_run_js(
        inv(
            "browser_run_js",
            {"script": "() => ({title: document.title})", "session_id": "s1"},
            capsule_id="different-turn",
        )
    )
    assert "page_state_assertion" not in other_turn.output


def test_browser_run_js_attaches_only_an_independent_runtime_dom_detail() -> None:
    literal_record = {
        "title": "Synthesized title",
        "url": "https://example.test/products/current",
        "price": "$1.00",
        "details": "Synthesized detail",
    }
    dom_record = {
        "title": "Rendered product",
        "url": "https://example.test/products/current",
        "price": "$19.00",
        "details": "Rendered product detail from the current DOM",
    }

    class ProvenanceBackend(CompleteBackend):
        async def run_js(self, session_id: str, script: str):
            self.calls.append(("js", session_id, script))
            if "ambiguous_detail_scope" in script and "record_scope" in script:
                return {
                    "ok": True,
                    "selector": None,
                    "record_scope": "selected_root",
                    "page": {
                        "url": "https://example.test/products/current",
                        "title": "Rendered product",
                    },
                    "record": dom_record,
                }
            return literal_record

    tools = BrowserTools(
        backend=ProvenanceBackend(),
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )
    result = tools.browser_run_js(
        inv(
            "browser_run_js",
            {"script": "() => ({title: 'literal'})", "session_id": "detail-session"},
        )
    )

    assert result.output["result"] == literal_record
    extraction = result.output["detail_extraction"]
    assert extraction["kind"] == "browser_dom_detail"
    assert extraction["origin"] == "runtime_owned_dom_extractor"
    assert extraction["record"] == dom_record
    assert extraction["record"] != result.output["result"]


def test_failed_assertion_replaces_prior_success_without_replay_after_evidence_read() -> None:
    class ChangingAssertionBackend(CompleteBackend):
        assertion_count = 0

        async def run_js(self, session_id: str, script: str):
            self.calls.append(("js", session_id, script))
            if "forbidden_found" in script and "const args" in script:
                self.assertion_count += 1
                expected = "First state" if self.assertion_count == 1 else "Second state"
                verified = self.assertion_count == 1
                return {
                    "ok": verified,
                    "url": "https://example.test/current",
                    "selector": None,
                    "required": [{"expected": expected, "match": expected if verified else ""}],
                    "forbidden": [],
                    "missing": [] if verified else [{"expected": expected, "match": ""}],
                    "forbidden_found": [],
                }
            return {"title": "Current evidence", "url": "https://example.test/current"}

    tools = BrowserTools(
        backend=ChangingAssertionBackend(),
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )
    first = tools.browser_assert_page_state(
        inv("browser_assert_page_state", {"session_id": "s1", "required": ["First state"]})
    )
    second = tools.browser_assert_page_state(
        inv("browser_assert_page_state", {"session_id": "s1", "required": ["Second state"]})
    )
    evidence = tools.browser_run_js(
        inv("browser_run_js", {"session_id": "s1", "script": "() => ({title: document.title})"})
    )

    assert first.output["verified"] is True
    assert second.output["verified"] is False
    assert "page_state_assertion" not in evidence.output


def test_shared_session_cleanup_and_failed_navigation_clear_turn_assertion_contract() -> None:
    class SharedBackend(CompleteBackend):
        BACKEND_NAME = "cdp"

        async def run_js(self, session_id: str, script: str):
            self.calls.append(("js", session_id, script))
            if "forbidden_found" in script and "const args" in script:
                return {
                    "ok": True,
                    "url": "https://example.test/current",
                    "selector": None,
                    "required": [{"expected": "Current state", "match": "Current state"}],
                    "forbidden": [],
                    "missing": [],
                    "forbidden_found": [],
                }
            return {"title": "Current state", "url": "https://example.test/current"}

        async def navigate(self, session_id: str, url: str) -> str:
            raise RuntimeError("navigation failed after page mutation")

    tools = BrowserTools(backend=SharedBackend(), pool=BrowserSessionPool(), policy=BrowserPolicy())
    assertion = tools.browser_assert_page_state(
        inv("browser_assert_page_state", {"required": ["Current state"]})
    )
    assert assertion.output["verified"] is True
    failed_navigation = tools.browser_navigate(
        inv("browser_navigate", {"url": "https://example.test/next"})
    )
    assert failed_navigation.status == "failed"
    after_navigation = tools.browser_run_js(
        inv("browser_run_js", {"script": "() => ({title: document.title})"})
    )
    assert "page_state_assertion" not in after_navigation.output

    tools.browser_assert_page_state(
        inv("browser_assert_page_state", {"required": ["Current state"]})
    )
    tools.close_tracked_sessions("scope")
    after_cleanup = tools.browser_run_js(
        inv("browser_run_js", {"script": "() => ({title: document.title})"})
    )
    assert "page_state_assertion" not in after_cleanup.output


def test_browser_operation_lock_allows_independent_sessions_to_progress_concurrently() -> None:
    class ConcurrentBackend(CompleteBackend):
        def __init__(self) -> None:
            super().__init__()
            self.entered: set[str] = set()
            self.both_entered = threading.Event()
            self.release = threading.Event()

        async def click(self, session_id: str, selector: str) -> None:
            self.entered.add(session_id)
            if len(self.entered) == 2:
                self.both_entered.set()
            await asyncio.to_thread(self.release.wait, 2)

    backend = ConcurrentBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    results: list[object] = []

    def click(session_id: str) -> None:
        results.append(
            tools.browser_click(
                inv("browser_click", {"session_id": session_id, "selector": "button"})
            )
        )

    threads = [
        threading.Thread(target=click, args=("session-one",)),
        threading.Thread(target=click, args=("session-two",)),
    ]
    try:
        for thread in threads:
            thread.start()
        assert backend.both_entered.wait(1), "independent browser sessions were globally serialized"
    finally:
        backend.release.set()
        for thread in threads:
            thread.join(timeout=2)

    assert len(results) == 2
    assert all(getattr(result, "status", None) == "completed" for result in results)


def test_browser_cleanup_waits_for_inflight_read_before_closing_session() -> None:
    class BlockingReadBackend(CompleteBackend):
        def __init__(self) -> None:
            super().__init__()
            self.read_started = threading.Event()
            self.release_read = threading.Event()

        async def run_js(self, session_id: str, script: str):
            self.read_started.set()
            await asyncio.to_thread(self.release_read.wait, 2)
            return {
                "url": "https://example.test/current",
                "title": "Current page",
                "elements": [],
                "element_count": 0,
            }

    backend = BlockingReadBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    read_result: list[object] = []
    cleanup_finished = threading.Event()

    read_thread = threading.Thread(
        target=lambda: read_result.append(
            tools.browser_snapshot(
                inv(
                    "browser_snapshot",
                    {"session_id": "shared-session"},
                    capsule_id="scope-a",
                )
            )
        )
    )
    cleanup_thread = threading.Thread(
        target=lambda: (
            tools.close_tracked_sessions("scope-a"),
            cleanup_finished.set(),
        )
    )
    try:
        read_thread.start()
        assert backend.read_started.wait(1)
        cleanup_thread.start()
        assert not cleanup_finished.wait(0.1), "cleanup bypassed the in-flight session read"
        assert backend.closed == []
    finally:
        backend.release_read.set()
        read_thread.join(timeout=2)
        cleanup_thread.join(timeout=2)

    assert len(read_result) == 1
    assert getattr(read_result[0], "status", None) == "completed"
    assert cleanup_finished.is_set()
    assert backend.closed == ["shared-session"]


def test_browser_cleanup_does_not_close_or_forget_new_shared_session_owner() -> None:
    class BlockingCloseBackend(CompleteBackend):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = threading.Event()
            self.release_close = threading.Event()

        async def close_session(self, session_id: str) -> None:
            self.close_started.set()
            await asyncio.to_thread(self.release_close.wait, 2)
            self.closed.append(session_id)

    backend = BlockingCloseBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    first = tools.browser_navigate(
        inv(
            "browser_navigate",
            {"session_id": "shared-session", "url": "https://example.test/a"},
            capsule_id="scope-a",
        )
    )
    assert first.status == "completed"

    cleanup_thread = threading.Thread(target=tools.close_tracked_sessions, args=("scope-a",))
    second_result: list[object] = []
    second_finished = threading.Event()
    second_thread = threading.Thread(
        target=lambda: (
            second_result.append(
                tools.browser_navigate(
                    inv(
                        "browser_navigate",
                        {"session_id": "shared-session", "url": "https://example.test/b"},
                        capsule_id="scope-b",
                    )
                )
            ),
            second_finished.set(),
        )
    )
    try:
        cleanup_thread.start()
        assert backend.close_started.wait(1)
        second_thread.start()
        assert not second_finished.wait(0.1), "new owner bypassed the session lifecycle lock"
    finally:
        backend.release_close.set()
        cleanup_thread.join(timeout=2)
        second_thread.join(timeout=2)

    assert backend.closed == ["shared-session"]
    assert len(second_result) == 1
    assert getattr(second_result[0], "status", None) == "completed"
    assert tools._sessions_by_scope == {"scope-b": {"shared-session"}}


def test_browser_scroll_waits_for_same_session_evidence_read() -> None:
    class BlockingEvidenceBackend(CompleteBackend):
        def __init__(self) -> None:
            super().__init__()
            self.read_started = threading.Event()
            self.release_read = threading.Event()
            self.scroll_started = threading.Event()

        async def run_js(self, session_id: str, script: str):
            self.read_started.set()
            await asyncio.to_thread(self.release_read.wait, 2)
            return {"url": "https://example.test/current", "title": "Current page"}

        async def scroll(self, session_id: str, direction: str, amount: int) -> None:
            self.scroll_started.set()
            await super().scroll(session_id, direction, amount)

    backend = BlockingEvidenceBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    results: list[object] = []
    read_thread = threading.Thread(
        target=lambda: results.append(
            tools.browser_run_js(
                inv(
                    "browser_run_js",
                    {"session_id": "shared-session", "script": "() => document.title"},
                )
            )
        )
    )
    scroll_thread = threading.Thread(
        target=lambda: results.append(
            tools.browser_scroll(
                inv(
                    "browser_scroll",
                    {"session_id": "shared-session", "direction": "down", "amount": 100},
                )
            )
        )
    )
    try:
        read_thread.start()
        assert backend.read_started.wait(1)
        scroll_thread.start()
        assert not backend.scroll_started.wait(0.1), "scroll raced the same-session evidence read"
    finally:
        backend.release_read.set()
        read_thread.join(timeout=2)
        scroll_thread.join(timeout=2)

    assert backend.scroll_started.is_set()
    assert len(results) == 2
    assert all(getattr(result, "status", None) == "completed" for result in results)


def test_shared_session_navigation_preserves_other_turn_contract_for_false_revalidation() -> None:
    class SharedPageBackend(CompleteBackend):
        BACKEND_NAME = "cdp"

        def __init__(self) -> None:
            super().__init__()
            self.page = "Product A"

        async def navigate(self, session_id: str, url: str) -> str:
            self.page = "Product B"
            return f"navigated:{url}"

        async def run_js(self, session_id: str, script: str):
            if "forbidden_found" in script and "const args" in script:
                verified = self.page == "Product A"
                return {
                    "ok": verified,
                    "url": f"https://example.test/{self.page[-1].lower()}",
                    "selector": "#product",
                    "required": [
                        {
                            "expected": "Product A",
                            "match": "Product A" if verified else "",
                        }
                    ],
                    "forbidden": [],
                    "missing": [] if verified else [{"expected": "Product A", "match": ""}],
                    "forbidden_found": [],
                }
            return {
                "title": self.page,
                "url": f"https://example.test/{self.page[-1].lower()}",
                "price": "$99" if self.page == "Product B" else "$12",
            }

    tools = BrowserTools(backend=SharedPageBackend(), pool=BrowserSessionPool(), policy=BrowserPolicy())
    assertion = tools.browser_assert_page_state(
        inv(
            "browser_assert_page_state",
            {"required": ["Product A"], "selector": "#product"},
            capsule_id="turn-a",
        )
    )
    assert assertion.output["verified"] is True

    tools.browser_navigate(
        inv(
            "browser_navigate",
            {"url": "https://example.test/b"},
            capsule_id="turn-b",
        )
    )
    evidence = tools.browser_run_js(
        inv(
            "browser_run_js",
            {"script": "() => ({title: document.title})"},
            capsule_id="turn-a",
        )
    )

    typed = evidence.output["page_state_assertion"]
    assert typed["verified"] is False
    assert typed["contract"]["required"] == ["Product A"]
    assert typed["result"]["required"] == [{"expected": "Product A", "match": ""}]
    assert typed["result"]["missing"] == [{"expected": "Product A", "match": ""}]


def test_capsule_scope_pins_active_explicit_session_across_parallel_turns() -> None:
    class SharedBackend(CompleteBackend):
        BACKEND_NAME = "cdp"

        async def run_js(self, session_id: str, script: str):
            if "forbidden_found" in script and "const args" in script:
                label = "Product A" if session_id == "session-a" else "Product B"
                return {
                    "ok": True,
                    "url": f"https://example.test/{label[-1].lower()}",
                    "selector": None,
                    "required": [{"expected": label, "match": label}],
                    "forbidden": [],
                    "missing": [],
                    "forbidden_found": [],
                }
            label = "Product A" if session_id == "session-a" else "Product B"
            return {"title": label, "url": f"https://example.test/{label[-1].lower()}"}

    tools = BrowserTools(backend=SharedBackend(), pool=BrowserSessionPool(), policy=BrowserPolicy())
    tools.browser_navigate(
        inv(
            "browser_navigate",
            {"url": "https://example.test/a", "session_id": "session-a"},
            capsule_id="turn-a",
        )
    )
    tools.browser_assert_page_state(
        inv(
            "browser_assert_page_state",
            {"required": ["Product A"], "session_id": "session-a"},
            capsule_id="turn-a",
        )
    )
    tools.browser_navigate(
        inv(
            "browser_navigate",
            {"url": "https://example.test/b", "session_id": "session-b"},
            capsule_id="turn-b",
        )
    )

    evidence = tools.browser_run_js(
        inv(
            "browser_run_js",
            {"script": "() => ({title: document.title})"},
            capsule_id="turn-a",
        )
    )

    assert evidence.output["session_id"] == "session-a"
    assert evidence.output["result"]["title"] == "Product A"
    assert evidence.output["page_state_assertion"]["verified"] is True


def test_evidence_revalidation_canonicalizes_missing_scope_as_typed_false_outcome() -> None:
    class MissingScopeBackend(CompleteBackend):
        assertion_count = 0

        async def run_js(self, session_id: str, script: str):
            if "forbidden_found" in script and "const args" in script:
                self.assertion_count += 1
                if self.assertion_count == 1:
                    return {
                        "ok": True,
                        "url": "https://example.test/a",
                        "selector": "#product",
                        "required": [{"expected": "Product A", "match": "Product A"}],
                        "forbidden": [{"expected": "Product B", "match": ""}],
                        "missing": [],
                        "forbidden_found": [],
                    }
                return {
                    "ok": False,
                    "reason": "scope_not_found",
                    "url": "https://example.test/b",
                }
            return {"title": "Product B", "url": "https://example.test/b", "price": "$99"}

    tools = BrowserTools(backend=MissingScopeBackend(), pool=BrowserSessionPool(), policy=BrowserPolicy())
    assertion = tools.browser_assert_page_state(
        inv(
            "browser_assert_page_state",
            {
                "session_id": "session-a",
                "required": ["Product A"],
                "forbidden": ["Product B"],
                "selector": "#product",
            },
        )
    )
    assert assertion.output["verified"] is True
    evidence = tools.browser_run_js(
        inv(
            "browser_run_js",
            {"session_id": "session-a", "script": "() => ({title: document.title})"},
        )
    )

    typed = evidence.output["page_state_assertion"]
    assert typed["verified"] is False
    assert typed["result"]["reason"] == "scope_not_found"
    assert typed["result"]["required"] == [{"expected": "Product A", "match": ""}]
    assert typed["result"]["forbidden"] == [{"expected": "Product B", "match": ""}]
    assert typed["result"]["selector"] == "#product"


def test_browser_assert_page_state_rejects_inconsistent_backend_success() -> None:
    class InconsistentAssertionBackend(CompleteBackend):
        async def run_js(self, session_id: str, script: str):
            return {
                "ok": True,
                "url": "https://example.test/wrong-product",
                "selector": "#product",
                "required": [{"expected": "Liquid sulfur", "match": ""}],
                "forbidden": [{"expected": "Solid bar", "match": "Solid bar"}],
                "missing": [],
                "forbidden_found": [],
            }

    tools = BrowserTools(
        backend=InconsistentAssertionBackend(),
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )
    assertion = tools.browser_assert_page_state(
        inv(
            "browser_assert_page_state",
            {
                "session_id": "session-a",
                "required": ["Liquid sulfur"],
                "forbidden": ["Solid bar"],
                "selector": "#product",
            },
        )
    )

    assert assertion.status == "completed"
    assert assertion.output["verified"] is False
    assert assertion.output["missing_required"] == ["Liquid sulfur"]
    assert assertion.output["forbidden_found"] == ["Solid bar"]
    assert assertion.output["result"]["ok"] is False
    assert assertion.output["result"]["missing"] == [
        {"expected": "Liquid sulfur", "match": ""}
    ]
    assert assertion.output["result"]["forbidden_found"] == [
        {"expected": "Solid bar", "match": "Solid bar"}
    ]
    assert tools._page_assertion_contracts == {}


def test_evidence_revalidation_rejects_inconsistent_backend_success() -> None:
    class InconsistentRevalidationBackend(CompleteBackend):
        assertion_count = 0

        async def run_js(self, session_id: str, script: str):
            if "forbidden_found" not in script or "const args" not in script:
                return {
                    "title": "Liquid sulfur",
                    "url": "https://example.test/wrong-product",
                    "price": "$12",
                }
            self.assertion_count += 1
            if self.assertion_count == 1:
                return {
                    "ok": True,
                    "url": "https://example.test/right-product",
                    "selector": "#product",
                    "required": [{"expected": "Liquid sulfur", "match": "Liquid sulfur"}],
                    "forbidden": [{"expected": "Solid bar", "match": ""}],
                    "missing": [],
                    "forbidden_found": [],
                }
            return {
                "ok": True,
                "url": "https://example.test/wrong-product",
                "selector": "#product",
                "required": [{"expected": "Liquid sulfur", "match": ""}],
                "forbidden": [{"expected": "Solid bar", "match": "Solid bar"}],
                "missing": [],
                "forbidden_found": [],
            }

    tools = BrowserTools(
        backend=InconsistentRevalidationBackend(),
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )
    assertion = tools.browser_assert_page_state(
        inv(
            "browser_assert_page_state",
            {
                "session_id": "session-a",
                "required": ["Liquid sulfur"],
                "forbidden": ["Solid bar"],
                "selector": "#product",
            },
        )
    )
    evidence = tools.browser_run_js(
        inv(
            "browser_run_js",
            {"session_id": "session-a", "script": "() => ({title: document.title})"},
        )
    )

    assert assertion.output["verified"] is True
    typed = evidence.output["page_state_assertion"]
    assert typed["verified"] is False
    assert typed["result"]["ok"] is False
    assert typed["result"]["missing"] == [{"expected": "Liquid sulfur", "match": ""}]
    assert typed["result"]["forbidden_found"] == [
        {"expected": "Solid bar", "match": "Solid bar"}
    ]


def test_session_operation_locks_do_not_accumulate_after_calls_complete() -> None:
    tools = BrowserTools(backend=CompleteBackend(), pool=BrowserSessionPool(), policy=BrowserPolicy())
    for index in range(100):
        tools.browser_click(
            inv(
                "browser_click",
                {"session_id": f"session-{index}"},
                capsule_id=f"turn-{index}",
            )
        )
    gc.collect()
    assert len(tools._session_operation_locks) == 0


def test_closed_session_is_purged_from_active_session_index() -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    result = tools.browser_navigate(
        inv(
            "browser_navigate",
            {"url": "https://example.com"},
            capsule_id="turn-cleanup",
        )
    )
    session_id = result.output["session_id"]

    assert any(
        active_session_id == session_id
        for active_session_id, _remembered_at in tools._active_sessions_by_principal.values()
    )

    tools.close_tracked_sessions("turn-cleanup")

    assert not any(
        active_session_id == session_id
        for active_session_id, _remembered_at in tools._active_sessions_by_principal.values()
    )
    assert backend.closed == [session_id]


def test_browser_wait_for_preserves_runtime_type_error_from_current_backend() -> None:
    class RuntimeTypeErrorBackend(CompleteBackend):
        async def wait_for(
            self,
            session_id: str,
            selector: str | None,
            url_pattern: str | None,
            text: str | None,
            timeout: float,
        ) -> None:
            raise TypeError("backend runtime error")

    tools = BrowserTools(
        backend=RuntimeTypeErrorBackend(),
        pool=BrowserSessionPool(),
        policy=BrowserPolicy(),
    )

    result = tools.browser_wait_for(
        inv("browser_wait_for", {"text": "Ready", "timeout": 3, "session_id": "s1"})
    )

    assert result.status == "failed"
    assert result.error == "Wait failed: backend runtime error"


def test_browser_screenshot_normalizes_high_dpi_png_to_layout_pixels(monkeypatch, tmp_path) -> None:
    from io import BytesIO

    from PIL import Image

    class RetinaScreenshotBackend(CompleteBackend):
        async def screenshot(self, session_id: str, mode: str = "auto") -> BrowserScreenshotResult:
            self.calls.append(("screenshot", session_id, mode))
            image = Image.new("RGB", (400, 300), "white")
            output = BytesIO()
            image.save(output, format="PNG")
            return BrowserScreenshotResult(
                data=output.getvalue(),
                mode="viewport",
                requested_mode=mode,
                viewport_width=200,
                viewport_height=150,
                document_width=200,
                document_height=150,
                is_clipped=False,
            )

    backend = RetinaScreenshotBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    screenshot_path = tmp_path / "retina-shot.png"
    monkeypatch.setattr(
        "nullion.plugins.browser_plugin.browser_tools.artifact_path_for_generated_workspace_file",
        lambda **kwargs: screenshot_path,
    )

    screenshot = tools.browser_screenshot(inv("browser_screenshot", {"session_id": "s1"}))

    assert screenshot.status == "completed"
    assert screenshot.output["image_width"] == 200
    assert screenshot.output["image_height"] == 150
    assert screenshot.output["original_image_width"] == 400
    assert screenshot.output["original_image_height"] == 300
    assert screenshot.output["normalized_device_scale_factor"] == 2.0
    with Image.open(screenshot_path) as saved:
        assert saved.size == (200, 150)


def test_scheduled_browser_tools_reuse_only_their_explicit_scope_page(monkeypatch, tmp_path) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    monkeypatch.setattr(
        "nullion.plugins.browser_plugin.browser_tools.artifact_path_for_generated_workspace_file",
        lambda **kwargs: artifact_root / "screenshot.png",
    )
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    default_result = tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com"}, capsule_id=None))
    first = tools.browser_navigate(
        scheduled_inv("browser_navigate", {"url": "https://example.com/a"}, scope="cron:one")
    )
    second = tools.browser_screenshot(scheduled_inv("browser_screenshot", {}, scope="cron:one"))
    other = tools.browser_navigate(
        scheduled_inv("browser_navigate", {"url": "https://example.com/b"}, scope="cron:two")
    )

    assert default_result.output["session_id"].startswith("task-")
    assert first.output["session_id"].startswith("task-")
    assert second.output["session_id"] == first.output["session_id"]
    assert other.output["session_id"].startswith("task-")
    assert other.output["session_id"] != first.output["session_id"]
    assert default_result.output["session_id"] not in {
        first.output["session_id"],
        other.output["session_id"],
    }


def test_browser_id_tools_recover_from_stale_snapshot_metadata() -> None:
    backend = StaleElementBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "s1"}))
    assert snapshot.status == "completed"

    clicked = tools.browser_click_id(inv("browser_click_id", {"element_id": "n-1", "session_id": "s1"}))
    assert clicked.status == "completed"
    assert clicked.output["recovered_by"] == "cached_semantic_target"
    assert ("click_element", "s1", {"role": "button", "text": "See prices", "name": "See prices"}) in backend.calls

    typed = tools.browser_type_id(
        inv("browser_type_id", {"element_id": "n-2", "text": "116 West 87th St", "session_id": "s1"})
    )
    assert typed.status == "completed"
    assert typed.output["recovered_by"] == "cached_semantic_target"
    assert (
        "type_field",
        "s1",
        {"label": "Pickup location", "placeholder": "Where from?", "role": "textbox"},
        "116 West 87th St",
    ) in backend.calls


def test_browser_type_id_does_not_recover_noneditable_snapshot_target() -> None:
    backend = StaleElementBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    assert tools.browser_snapshot(inv("browser_snapshot", {"session_id": "s1"})).status == "completed"

    typed = tools.browser_type_id(
        inv("browser_type_id", {"element_id": "n-1", "text": "Knockdown Center", "session_id": "s1"})
    )

    assert typed.status == "failed"
    assert typed.error == "Type failed: element_not_editable"
    assert not any(call[0] == "type_field" for call in backend.calls)


def test_browser_type_field_failure_returns_structured_recovery_fields() -> None:
    class FailingTypeFieldBackend(CompleteBackend):
        async def type_field(self, session_id: str, target: dict[str, object], text: str) -> None:
            self.calls.append(("type_field", session_id, target, text))
            raise RuntimeError("Matching field is not editable.")

        async def run_js(self, session_id: str, script: str):
            self.calls.append(("js", session_id, script))
            if "element_count" in script and "__nullionBrowserElementSeq" in script:
                return {
                    "url": "https://example.com",
                    "title": "Example",
                    "active_element_id": "",
                    "element_count": 1,
                    "elements": [
                        {
                            "element_id": "n-2",
                            "role": "textbox",
                            "label": "Pickup location",
                            "placeholder": "City or airport",
                            "name": "pickup",
                            "type": "text",
                            "visible": True,
                            "disabled": False,
                            "editable": True,
                        }
                    ],
                }
            return await super().run_js(session_id, script)

    backend = FailingTypeFieldBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    typed = tools.browser_type_field(
        inv("browser_type_field", {"session_id": "s1", "label": "Return date", "text": "2026-07-05"})
    )

    assert typed.status == "failed"
    assert typed.output["reason"] == "field_not_editable"
    assert typed.output["target"] == {"label": "Return date"}
    assert typed.output["visible_editable_fields"][0]["label"] == "Pickup location"
    assert typed.output["visible_editable_fields"][0]["element_id"] == "n-2"


def test_browser_type_id_prefers_semantic_fill_for_hidden_editable_snapshot_target() -> None:
    backend = HiddenEditableBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    assert tools.browser_snapshot(inv("browser_snapshot", {"session_id": "s1"})).status == "completed"

    typed = tools.browser_type_id(
        inv("browser_type_id", {"element_id": "n-1", "text": "Provincetown", "session_id": "s1"})
    )

    assert typed.status == "completed"
    assert typed.output["recovered_by"] == "hidden_cached_semantic_target"
    assert (
        "type_field",
        "s1",
        {"label": "Where", "placeholder": "Search destinations", "name": "query", "role": "textbox"},
        "Provincetown",
    ) in backend.calls


def test_registered_browser_tools_expose_required_argument_schemas(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_BROWSER_BACKEND", "playwright")
    registry = ToolRegistry()
    register_browser_tools(registry)

    definitions = {definition["name"]: definition["input_schema"] for definition in registry.list_tool_definitions()}

    assert definitions["browser_click"]["required"] == ["selector"]
    assert definitions["browser_click_element"]["required"] == []
    assert definitions["browser_click_id"]["required"] == ["element_id"]
    assert definitions["browser_type"]["required"] == ["selector", "text"]
    assert definitions["browser_type_field"]["required"] == ["text"]
    assert definitions["browser_type_id"]["required"] == ["element_id", "text"]
    assert definitions["browser_select_combobox"]["required"] == ["query"]
    assert definitions["browser_find"]["required"] == ["selector"]
    assert definitions["browser_extract_items"]["required"] == []
    assert "max_items" in definitions["browser_extract_items"]["properties"]
    assert definitions["browser_extract_detail"]["required"] == []
    assert "selector" in definitions["browser_extract_detail"]["properties"]
    assert definitions["browser_run_js"]["required"] == ["script"]
    assert definitions["browser_navigate"]["required"] == ["url"]
    assert definitions["browser_scroll"]["required"] == ["direction"]
    assert "text" in definitions["browser_wait_for"]["properties"]
    assert "required" in definitions["browser_assert_page_state"]["properties"]
    assert "session_id" in definitions["browser_click"]["properties"]

    result = registry.invoke(ToolInvocation("bad-click", "browser_click", "operator", {}))
    assert result.status == "failed"
    assert result.output["reason"] == "invalid_tool_arguments"
    assert result.output["missing_required"] == ["selector"]
    assert registry.get_spec("browser_run_js").requires_approval is False
    assert {"browser_navigate", "browser_extract_text"} <= set(
        registry.get_spec("browser_assert_page_state").continuation_tools
    )
    browser_description = registry.get_spec("browser_navigate").description
    assert "configured agent browser backend" in browser_description
    assert "Chromium browser" not in browser_description


def test_visible_browser_uses_isolated_task_sessions_across_surfaces(monkeypatch) -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    opened = tools.browser_navigate(
        ToolInvocation(
            invocation_id="open-browser",
            tool_name="browser_navigate",
            principal_id="web:operator",
            arguments={"url": "https://example.com", "session_id": "default"},
            capsule_id=None,
        )
    )
    agent = tools.browser_extract_text(
        ToolInvocation(
            invocation_id="agent-browser",
            tool_name="browser_extract_text",
            principal_id="telegram:123",
            arguments={},
            capsule_id="turn-agent",
        )
    )
    slack_agent = tools.browser_extract_text(
        ToolInvocation(
            invocation_id="slack-agent-browser",
            tool_name="browser_extract_text",
            principal_id="slack:T123",
            arguments={},
            capsule_id="turn-agent-2",
        )
    )

    session_ids = {
        opened.output["session_id"],
        agent.output["session_id"],
        slack_agent.output["session_id"],
    }
    assert len(session_ids) == 3
    assert all(session_id.startswith("task-") for session_id in session_ids)
    assert ("navigate", opened.output["session_id"], "https://example.com") in backend.calls
    assert ("extract", agent.output["session_id"], None) in backend.calls
    assert ("extract", slack_agent.output["session_id"], None) in backend.calls

    tools.close_tracked_sessions("turn-agent")
    assert backend.closed == [agent.output["session_id"]]


def test_visible_browser_does_not_share_page_state_by_workspace(monkeypatch) -> None:
    monkeypatch.setattr(
        "nullion.connections.workspace_id_for_principal",
        lambda principal: "workspace_alpha" if str(principal).endswith(":alpha") else "workspace_beta",
    )
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    alpha_web = tools.browser_navigate(
        ToolInvocation(
            invocation_id="alpha-web",
            tool_name="browser_navigate",
            principal_id="workspace:alpha",
            arguments={"url": "https://example.com/alpha"},
            capsule_id="turn-alpha-web",
        )
    )
    alpha_telegram = tools.browser_extract_text(
        ToolInvocation(
            invocation_id="alpha-telegram",
            tool_name="browser_extract_text",
            principal_id="telegram:alpha",
            arguments={},
            capsule_id="turn-alpha-telegram",
        )
    )
    beta_web = tools.browser_navigate(
        ToolInvocation(
            invocation_id="beta-web",
            tool_name="browser_navigate",
            principal_id="workspace:beta",
            arguments={"url": "https://example.com/beta"},
            capsule_id="turn-beta-web",
        )
    )

    session_ids = {
        alpha_web.output["session_id"],
        alpha_telegram.output["session_id"],
        beta_web.output["session_id"],
    }
    assert len(session_ids) == 3
    assert all(session_id.startswith("task-") for session_id in session_ids)


def test_visible_browser_scopes_model_session_ids_to_the_current_task(monkeypatch) -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    opened = tools.browser_open(
        ToolInvocation(
            invocation_id="open-browser",
            tool_name="browser_open",
            principal_id="workspace:alpha",
            arguments={"session_id": "default"},
            capsule_id=None,
        )
    )
    agent = tools.browser_navigate(
        ToolInvocation(
            invocation_id="agent-browser",
            tool_name="browser_navigate",
            principal_id="workspace:alpha",
            arguments={"url": "https://example.com", "session_id": "agent-made-session"},
            capsule_id="turn-agent",
        )
    )
    isolated = tools.browser_navigate(
        ToolInvocation(
            invocation_id="agent-isolated-browser",
            tool_name="browser_navigate",
            principal_id="workspace:alpha",
            arguments={"url": "https://example.org", "session_id": "isolated:agent-made-session"},
            capsule_id="turn-agent",
        )
    )

    assert opened.output["session_id"].startswith("task-")
    assert agent.output["session_id"].startswith("task-")
    assert isolated.output["session_id"].startswith("task-")
    assert agent.output["session_id"] != "agent-made-session"
    assert isolated.output["session_id"] != "isolated:agent-made-session"
    assert agent.output["session_id"] != opened.output["session_id"]
    assert isolated.output["session_id"] != agent.output["session_id"]
    assert ("navigate", agent.output["session_id"], "https://example.com") in backend.calls
    assert ("navigate", isolated.output["session_id"], "https://example.org") in backend.calls


def test_visible_browser_typed_scope_reuses_model_session_across_capsules(
    monkeypatch,
    tmp_path,
) -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    monkeypatch.setattr(
        "nullion.plugins.browser_plugin.browser_tools.artifact_path_for_generated_workspace_file",
        lambda **kwargs: tmp_path / "typed-scope-screenshot.png",
    )

    navigate = tools.browser_navigate(
        ToolInvocation(
            invocation_id="typed-scope-navigate",
            tool_name="browser_navigate",
            principal_id="telegram:operator",
            arguments={
                "url": "https://example.com/typed-scope",
                "session_id": "model-browser-session",
            },
            capsule_id="navigate-capsule",
            flow_context={"browser_session_scope": "telegram-workflow-1"},
        )
    )
    screenshot = tools.browser_screenshot(
        ToolInvocation(
            invocation_id="typed-scope-screenshot",
            tool_name="browser_screenshot",
            principal_id="telegram:operator",
            arguments={"session_id": "model-browser-session"},
            capsule_id="screenshot-capsule",
            flow_context={"browser_session_scope": "telegram-workflow-1"},
        )
    )
    isolated = tools.browser_navigate(
        ToolInvocation(
            invocation_id="other-typed-scope-navigate",
            tool_name="browser_navigate",
            principal_id="telegram:operator",
            arguments={
                "url": "https://example.com/other-scope",
                "session_id": "model-browser-session",
            },
            capsule_id="other-capsule",
            flow_context={"browser_session_scope": "telegram-workflow-2"},
        )
    )

    assert navigate.status == "completed"
    assert screenshot.status == "completed"
    assert screenshot.output["session_id"] == navigate.output["session_id"]
    assert isolated.output["session_id"] != navigate.output["session_id"]
    assert navigate.output["session_id"].startswith("task-")
    assert navigate.output["session_id"] != "model-browser-session"


def test_visible_browser_does_not_serialize_concurrent_tasks_on_a_model_session_id() -> None:
    class BlockingBackend(CompleteBackend):
        BACKEND_NAME = "cdp"

        def __init__(self) -> None:
            super().__init__()
            self.entered_sessions: set[str] = set()
            self.entered_lock = threading.Lock()
            self.both_entered = threading.Event()
            self.release = threading.Event()

        async def navigate(self, session_id: str, url: str) -> str:
            with self.entered_lock:
                self.entered_sessions.add(session_id)
                if len(self.entered_sessions) == 2:
                    self.both_entered.set()
            if not await asyncio.to_thread(self.release.wait, 2):
                raise RuntimeError("test browser operation was not released")
            return f"navigated:{url}"

    backend = BlockingBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    results = []

    def navigate(capsule_id: str) -> None:
        results.append(
            tools.browser_navigate(
                ToolInvocation(
                    invocation_id=f"navigate-{capsule_id}",
                    tool_name="browser_navigate",
                    principal_id="workspace:operator",
                    arguments={
                        "url": f"https://example.com/{capsule_id}",
                        "session_id": "browser-session",
                    },
                    capsule_id=capsule_id,
                )
            )
        )

    threads = [
        threading.Thread(target=navigate, args=("turn-a",)),
        threading.Thread(target=navigate, args=("turn-b",)),
    ]
    for thread in threads:
        thread.start()
    try:
        entered_concurrently = backend.both_entered.wait(1)
    finally:
        backend.release.set()
        for thread in threads:
            thread.join(2)

    assert entered_concurrently is True
    assert all(not thread.is_alive() for thread in threads)
    assert len(results) == 2
    session_ids = {result.output["session_id"] for result in results}
    assert len(session_ids) == 2
    assert all(session_id.startswith("task-") for session_id in session_ids)
    assert "browser-session" not in session_ids


def test_visible_browser_exact_agent_default_is_the_only_model_shared_session() -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    first = tools.browser_open(
        ToolInvocation(
            invocation_id="first-agent-default",
            tool_name="browser_open",
            principal_id="workspace:alpha",
            arguments={"session_id": DEFAULT_AGENT_BROWSER_SESSION_ID},
            capsule_id="turn-a",
        )
    )
    second = tools.browser_open(
        ToolInvocation(
            invocation_id="second-agent-default",
            tool_name="browser_open",
            principal_id="workspace:beta",
            arguments={"session_id": DEFAULT_AGENT_BROWSER_SESSION_ID},
            capsule_id="turn-b",
        )
    )

    assert first.output["session_id"] == DEFAULT_AGENT_BROWSER_SESSION_ID
    assert second.output["session_id"] == DEFAULT_AGENT_BROWSER_SESSION_ID


def test_visible_browser_runtime_session_binding_overrides_model_session_argument() -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    result = tools.browser_navigate(
        ToolInvocation(
            invocation_id="runtime-bound-browser",
            tool_name="browser_navigate",
            principal_id="telegram:operator",
            arguments={
                "url": "https://example.com/runtime-bound",
                "session_id": "model-selected-session",
            },
            capsule_id="turn-runtime-bound",
            flow_context={"browser_session_id": "runtime-bound-session"},
        )
    )

    assert result.output["session_id"] == "runtime-bound-session"
    assert (
        "navigate",
        "runtime-bound-session",
        "https://example.com/runtime-bound",
    ) in backend.calls
    assert not any(call[1] == "model-selected-session" for call in backend.calls)


def test_headless_browser_default_session_remains_scoped(monkeypatch) -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "playwright"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    first = tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com"}, capsule_id="turn-a"))
    second = tools.browser_navigate(inv("browser_navigate", {"url": "https://example.org"}, capsule_id="turn-b"))

    assert first.output["session_id"].startswith("default-")
    assert second.output["session_id"].startswith("default-")
    assert first.output["session_id"] != second.output["session_id"]


def test_auto_visible_browser_default_session_is_task_scoped(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_BROWSER_HEADLESS", raising=False)
    backend = CompleteBackend()
    backend.BACKEND_NAME = "auto"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    opened = tools.browser_navigate(
        ToolInvocation(
            invocation_id="open-browser",
            tool_name="browser_navigate",
            principal_id="web:operator",
            arguments={"url": "https://example.com", "session_id": "default"},
            capsule_id=None,
        )
    )
    agent = tools.browser_extract_text(
        ToolInvocation(
            invocation_id="agent-browser",
            tool_name="browser_extract_text",
            principal_id="telegram:123",
            arguments={},
            capsule_id="turn-agent",
        )
    )

    assert opened.output["session_id"].startswith("task-")
    assert agent.output["session_id"].startswith("task-")
    assert agent.output["session_id"] != opened.output["session_id"]


def test_visible_browser_explicit_scope_selects_a_dedicated_session(monkeypatch) -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    opened = tools.browser_navigate(
        ToolInvocation(
            invocation_id="open-browser",
            tool_name="browser_navigate",
            principal_id="web:operator",
            arguments={"url": "https://example.com", "session_id": "default"},
            capsule_id=None,
        )
    )
    agent = tools.browser_extract_text(
        ToolInvocation(
            invocation_id="agent-browser",
            tool_name="browser_extract_text",
            principal_id="telegram:123",
            arguments={},
            capsule_id="turn-agent",
            flow_context={"isolated_browser_session": True, "browser_session_scope": "agent:isolated"},
        )
    )

    assert opened.output["session_id"].startswith("task-")
    assert agent.output["session_id"].startswith("task-")
    assert agent.output["session_id"] != opened.output["session_id"]


def test_visible_browser_shares_page_only_with_explicit_session_binding() -> None:
    backend = CompleteBackend()
    backend.BACKEND_NAME = "cdp"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    first = tools.browser_navigate(
        ToolInvocation(
            invocation_id="first-bound-browser",
            tool_name="browser_navigate",
            principal_id="web:operator",
            arguments={"url": "https://example.com/first"},
            capsule_id="turn-first",
            flow_context={"browser_session_id": "shared-explicit-session"},
        )
    )
    second = tools.browser_extract_text(
        ToolInvocation(
            invocation_id="second-bound-browser",
            tool_name="browser_extract_text",
            principal_id="telegram:operator",
            arguments={},
            capsule_id="turn-second",
            flow_context={"browser_session_id": "shared-explicit-session"},
        )
    )

    assert first.output["session_id"] == "shared-explicit-session"
    assert second.output["session_id"] == first.output["session_id"]

    tools.close_tracked_sessions("turn-first")
    assert backend.closed == []
    tools.close_tracked_sessions("turn-second")
    assert backend.closed == ["shared-explicit-session"]


def test_auto_headless_browser_default_session_remains_scoped(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "true")
    backend = CompleteBackend()
    backend.BACKEND_NAME = "auto"
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    first = tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com"}, capsule_id="turn-a"))
    second = tools.browser_navigate(inv("browser_navigate", {"url": "https://example.org"}, capsule_id="turn-b"))

    assert first.output["session_id"].startswith("default-")
    assert second.output["session_id"].startswith("default-")
    assert first.output["session_id"] != second.output["session_id"]


def test_playwright_backend_user_agent_matches_runtime_chromium(monkeypatch) -> None:
    monkeypatch.setattr(playwright_backend.platform, "system", lambda: "Darwin")

    ua = playwright_backend._context_user_agent("147.0.7727.15")

    assert "Chrome/147.0.7727.15" in ua
    assert "Chrome/120" not in ua
    assert "HeadlessChrome" not in ua
    assert "Macintosh; Intel Mac OS X" in ua


def test_playwright_stealth_uses_host_platform(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class StealthSpy:
        def __init__(self, **kwargs) -> None:
            captured["kwargs"] = kwargs

        def use_async(self, context_manager):
            captured["context_manager"] = context_manager
            return "wrapped-context"

    monkeypatch.setattr(playwright_backend.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(playwright_backend, "_STEALTH_AVAILABLE", True)
    monkeypatch.setattr(playwright_backend, "Stealth", StealthSpy)

    assert playwright_backend._stealth_context_manager("raw-context") == "wrapped-context"
    assert captured["context_manager"] == "raw-context"
    assert captured["kwargs"] == {"navigator_platform_override": "MacIntel"}


@pytest.mark.asyncio
async def test_playwright_backend_applies_stealth_only_when_headless(monkeypatch) -> None:
    wrapped_calls: list[object] = []
    launch_calls: list[dict[str, object]] = []

    class Browser:
        version = "145.0.7632.6"

        def is_connected(self) -> bool:
            return True

    async def launch(**kwargs):
        launch_calls.append(kwargs)
        return Browser()

    class ContextManager:
        def __init__(self, name: str) -> None:
            self.name = name

        async def __aenter__(self):
            return SimpleNamespace(chromium=SimpleNamespace(launch=launch))

        async def __aexit__(self, exc_type, exc, tb):
            return False

    raw_context = ContextManager("raw")
    wrapped_context = ContextManager("wrapped")
    monkeypatch.setattr(playwright_backend, "async_playwright", lambda: raw_context)

    def fake_stealth_context_manager(context_manager):
        wrapped_calls.append(context_manager)
        return wrapped_context

    monkeypatch.setattr(playwright_backend, "_stealth_context_manager", fake_stealth_context_manager)

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "true")
    headless_backend = playwright_backend.PlaywrightBackend()
    await headless_backend._ensure_browser()
    assert wrapped_calls == [raw_context]
    assert headless_backend._playwright is wrapped_context
    assert launch_calls[-1] == {"headless": True}

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    visible_backend = playwright_backend.PlaywrightBackend()
    await visible_backend._ensure_browser()
    assert wrapped_calls == [raw_context]
    assert visible_backend._playwright is raw_context
    assert launch_calls[-1] == {"headless": False}


@pytest.mark.asyncio
async def test_playwright_backend_uses_installed_browser_when_bundled_runtime_is_missing(monkeypatch) -> None:
    launch_calls: list[dict[str, object]] = []
    system_browser = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"

    class Browser:
        version = "149.0.0.0"

        def is_connected(self) -> bool:
            return True

    class Chromium:
        executable_path = "/missing/ms-playwright/chrome-headless-shell"

        async def launch(self, **kwargs):
            launch_calls.append(kwargs)
            return Browser()

    class ContextManager:
        async def __aenter__(self):
            return SimpleNamespace(chromium=Chromium())

        async def __aexit__(self, exc_type, exc, tb):
            return False

    context = ContextManager()
    monkeypatch.setattr(playwright_backend, "async_playwright", lambda: context)
    monkeypatch.setattr(playwright_backend, "_stealth_context_manager", lambda manager: manager)
    monkeypatch.setattr(playwright_backend, "_system_chromium_executable", lambda: system_browser)
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "true")

    backend = playwright_backend.PlaywrightBackend()
    browser = await backend._ensure_browser()

    assert browser.is_connected() is True
    assert launch_calls == [{"headless": True, "executable_path": system_browser}]


@pytest.mark.asyncio
async def test_playwright_backend_retries_installed_browser_after_bundled_launch_failure(monkeypatch) -> None:
    launch_calls: list[dict[str, object]] = []
    system_browser = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

    class Browser:
        version = "149.0.0.0"

        def is_connected(self) -> bool:
            return True

    class Chromium:
        executable_path = "/existing/ms-playwright/chrome-headless-shell"

        async def launch(self, **kwargs):
            launch_calls.append(kwargs)
            if "executable_path" not in kwargs:
                raise RuntimeError("bundled browser failed to start")
            return Browser()

    monkeypatch.setattr(playwright_backend.os.path, "isfile", lambda path: path == Chromium.executable_path)
    monkeypatch.setattr(playwright_backend, "_system_chromium_executable", lambda: system_browser)

    browser = await playwright_backend._launch_playwright_browser(Chromium(), headless=True)

    assert browser.is_connected() is True
    assert launch_calls == [
        {"headless": True},
        {"headless": True, "executable_path": system_browser},
    ]


@pytest.mark.asyncio
async def test_auto_backend_selection_and_delegation(monkeypatch) -> None:
    fake = CompleteBackend()
    backend = auto_backend.AutoBackend()
    backend._backend = fake
    assert await backend.navigate("s", "https://example.com") == "navigated:https://example.com"
    await backend.click("s", "#go")
    await backend.type_text("s", "input", "abc")
    assert await backend.extract_text("s", None) == "hello page"
    assert (await backend.screenshot("s")).data == b"png"
    await backend.scroll("s", "down", 25)
    await backend.wait_for("s", "#ready", None, None, 1)
    assert await backend.find("s", "a")
    assert await backend.run_js("s", "1+1") == {"ok": True}
    await backend.close_session("s")
    await backend.shutdown()
    assert fake.shutdown_called is True

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "true")
    fresh = auto_backend.AutoBackend()
    monkeypatch.setattr(fresh, "_make_playwright", lambda: fake)
    assert await fresh._ensure_backend() is fake

    cdp = CompleteBackend()
    fresh2 = auto_backend.AutoBackend()
    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: True)
    monkeypatch.setattr(fresh2, "_make_cdp", lambda: cdp)
    assert await fresh2._ensure_backend() is cdp

    launched = CompleteBackend()
    fresh3 = auto_backend.AutoBackend()
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: False)
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: "/Applications/Chrome")
    monkeypatch.setattr(auto_backend, "_launch_chrome", lambda path: None)
    monkeypatch.setattr(fresh3, "_make_cdp", lambda: launched)
    assert await fresh3._ensure_backend() is launched


@pytest.mark.asyncio
async def test_auto_backend_falls_back_to_chromium_with_notice_when_cdp_browser_unavailable(monkeypatch) -> None:
    fallback = CompleteBackend()
    fresh = auto_backend.AutoBackend()

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: False)
    monkeypatch.setattr(auto_backend, "_reuse_preferred_cdp_if_available", lambda: False)
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: None)
    monkeypatch.setattr(fresh, "_make_playwright", lambda: fallback)

    assert await fresh._ensure_backend() is fallback
    assert fresh.connection_notice()
    assert "Could not connect to the shared browser through CDP" in fresh.connection_notice()
    assert await fresh.navigate("s", "https://example.com") == "navigated:https://example.com"


@pytest.mark.asyncio
async def test_auto_backend_falls_back_to_chromium_with_notice_when_browser_launch_fails(monkeypatch) -> None:
    fallback = CompleteBackend()
    fresh = auto_backend.AutoBackend()

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: False)
    monkeypatch.setattr(auto_backend, "_reuse_preferred_cdp_if_available", lambda: False)
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    monkeypatch.setattr(auto_backend, "_launch_chrome", lambda path: (_ for _ in ()).throw(RuntimeError("launch failed")))
    monkeypatch.setattr(fresh, "_make_playwright", lambda: fallback)

    assert await fresh._ensure_backend() is fallback
    assert fresh.connection_notice()
    assert await fresh.open("s") == "opened:s"


def test_browser_tools_surface_auto_cdp_fallback_notice(monkeypatch) -> None:
    fallback = CompleteBackend()
    backend = auto_backend.AutoBackend()

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: False)
    monkeypatch.setattr(auto_backend, "_reuse_preferred_cdp_if_available", lambda: False)
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: None)
    monkeypatch.setattr(backend, "_make_playwright", lambda: fallback)

    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    result = tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com"}))

    assert result.status == "completed"
    assert result.output["browser_connection_notice"].startswith(
        "Could not connect to the shared browser through CDP"
    )
    assert result.output["used_shared_authenticated_browser"] is False
    assert ("navigate", result.output["session_id"], "https://example.com") in fallback.calls


def test_auto_backend_browser_preference_uses_exact_browser_order(monkeypatch) -> None:
    candidates = [
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    monkeypatch.setattr(auto_backend.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(auto_backend, "_CHROME_CANDIDATES", {"Darwin": candidates})
    monkeypatch.setattr(auto_backend.os.path, "exists", lambda path: path in candidates)

    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "chrome")
    assert auto_backend._find_chrome() == "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "brave")
    assert auto_backend._find_chrome() == "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"


def test_auto_backend_launches_selected_browser_on_free_port_when_other_browser_owns_cdp(
    monkeypatch,
    tmp_path,
) -> None:
    launched: dict[str, object] = {}

    class Proc:
        pass

    lane_home = tmp_path / "runtime-home"
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("NULLION_HOME", str(lane_home))
    monkeypatch.delenv("NULLION_DATA_DIR", raising=False)
    monkeypatch.delenv("NULLION_BROWSER_USER_DATA_DIR", raising=False)
    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "chrome")
    monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://127.0.0.1:9222")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: True)
    monkeypatch.setattr(auto_backend, "_running_cdp_matches_preference", lambda: False)
    monkeypatch.setattr(auto_backend, "_reuse_preferred_cdp_if_available", lambda: False)
    monkeypatch.setattr(auto_backend, "_next_available_cdp_port", lambda start: 9333)
    monkeypatch.setattr(auto_backend.platform, "system", lambda: "Darwin")

    def fake_popen(args, **kwargs):
        launched["args"] = args
        launched["kwargs"] = kwargs
        return Proc()

    monkeypatch.setattr(auto_backend.subprocess, "Popen", fake_popen)

    auto_backend._launch_chrome("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")

    args = launched["args"]
    assert "--remote-debugging-port=9333" in args
    assert f"--user-data-dir={lane_home}/browser-profiles/chrome" in args
    assert "--new-window" not in args
    assert os.environ["NULLION_BROWSER_CDP_URL"] == "http://127.0.0.1:9333"


@pytest.mark.asyncio
async def test_auto_backend_reuses_preferred_browser_on_adjacent_cdp_port(monkeypatch) -> None:
    cdp = CompleteBackend()
    fresh = auto_backend.AutoBackend()

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "brave")
    monkeypatch.delenv("NULLION_BROWSER_CDP_URL", raising=False)
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: True)
    monkeypatch.setattr(auto_backend, "_running_cdp_matches_preference", lambda: False)
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable_at", lambda host, port: port in {9222, 9223})
    monkeypatch.setattr(
        auto_backend,
        "_cdp_owner_browser_kind_for_port",
        lambda port: {9222: "chrome", 9223: "brave"}.get(port),
    )
    monkeypatch.setattr(auto_backend, "_cdp_browser_kind_from_version_url", lambda url: None)
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: pytest.fail("should reuse visible Brave"))
    monkeypatch.setattr(auto_backend, "_launch_chrome", lambda path: pytest.fail("should not launch another browser"))
    monkeypatch.setattr(fresh, "_make_cdp", lambda: cdp)

    assert await fresh._ensure_backend() is cdp
    assert os.environ["NULLION_BROWSER_CDP_URL"] == "http://127.0.0.1:9223"


@pytest.mark.asyncio
async def test_auto_backend_keeps_explicit_cdp_url_instead_of_adjacent_profile(monkeypatch) -> None:
    cdp = CompleteBackend()
    fresh = auto_backend.AutoBackend()
    launched_paths: list[str] = []

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "brave")
    monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://127.0.0.1:9223")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: False)
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable_at", lambda host, port: port == 9222)
    monkeypatch.setattr(auto_backend, "_cdp_owner_browser_kind_for_port", lambda port: "brave")
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser")
    monkeypatch.setattr(auto_backend, "_launch_chrome", lambda path: launched_paths.append(path))
    monkeypatch.setattr(fresh, "_make_cdp", lambda: cdp)

    assert await fresh._ensure_backend() is cdp
    assert launched_paths == ["/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"]
    assert os.environ["NULLION_BROWSER_CDP_URL"] == "http://127.0.0.1:9223"


@pytest.mark.asyncio
async def test_auto_backend_does_not_attach_to_mismatched_running_browser(monkeypatch) -> None:
    cdp = CompleteBackend()
    fresh = auto_backend.AutoBackend()
    launched_paths: list[str] = []

    monkeypatch.setenv("NULLION_BROWSER_HEADLESS", "false")
    monkeypatch.setenv("NULLION_BROWSER_PREFERRED", "chrome")
    monkeypatch.setattr(auto_backend, "_is_cdp_reachable", lambda: True)
    monkeypatch.setattr(auto_backend, "_running_cdp_matches_preference", lambda: False)
    monkeypatch.setattr(auto_backend, "_reuse_preferred_cdp_if_available", lambda: False)
    monkeypatch.setattr(auto_backend, "_find_chrome", lambda: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    monkeypatch.setattr(auto_backend, "_launch_chrome", lambda path: launched_paths.append(path))
    monkeypatch.setattr(fresh, "_make_cdp", lambda: cdp)

    assert await fresh._ensure_backend() is cdp
    assert launched_paths == ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]


@pytest.mark.asyncio
async def test_playwright_backend_page_methods_and_shutdown() -> None:
    class Page:
        def __init__(self) -> None:
            self.context = SimpleNamespace(close=lambda: asyncio.sleep(0))
            self.screenshot_kwargs: dict[str, object] = {}
            self.url = "https://example.com"

        async def goto(self, *args, **kwargs):
            return SimpleNamespace(status=201)

        async def click(self, *args, **kwargs):
            return None

        async def fill(self, *args):
            return None

        def locator(self, selector):
            return SimpleNamespace(first=SimpleNamespace(inner_text=lambda timeout: asyncio.sleep(0, result="selected")))

        async def inner_text(self, selector):
            return "body text"

        async def screenshot(self, **kwargs):
            self.screenshot_kwargs = kwargs
            return b"shot"

        async def evaluate(self, script):
            if "scrollHeight" in script:
                return {
                    "viewportWidth": 1280,
                    "viewportHeight": 720,
                    "documentWidth": 1280,
                    "documentHeight": 1500,
                }
            return {"script": script}

        async def wait_for_selector(self, selector, timeout):
            return None

        async def wait_for_url(self, url, timeout, wait_until="load"):
            assert callable(url) and url("https://example.com/nested/path")
            assert wait_until == "commit"
            return None

        async def wait_for_load_state(self, state, timeout):
            return None

        async def query_selector_all(self, selector):
            element = SimpleNamespace(
                inner_text=lambda: asyncio.sleep(0, result=" link "),
                evaluate=lambda script: asyncio.sleep(0, result="a"),
                get_attribute=lambda name: asyncio.sleep(0, result="https://example.com"),
            )
            return [element]

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._pages = {"s": page}
    backend._browser = SimpleNamespace(close=lambda: asyncio.sleep(0))
    playwright = AsyncContext()
    backend._playwright = playwright
    backend._lock = asyncio.Lock()
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]
    assert await backend.navigate("s", "https://example.com") == "Navigated to https://example.com (status 201)"
    await backend.click("s", "#go")
    await backend.type_text("s", "input", "abc")
    assert await backend.extract_text("s", "body") == "selected"
    assert await backend.extract_text("s", None) == "body text"
    screenshot = await backend.screenshot("s")
    assert screenshot.data == b"shot"
    assert screenshot.mode == "viewport"
    assert screenshot.requested_mode == "auto"
    assert screenshot.is_clipped is True
    assert page.screenshot_kwargs["full_page"] is False
    full_page_screenshot = await backend.screenshot("s", mode="full_page")
    assert full_page_screenshot.data == b"shot"
    assert full_page_screenshot.mode == "full_page"
    assert full_page_screenshot.is_clipped is False
    assert page.screenshot_kwargs["full_page"] is True
    page.url = "file:///tmp/generated-report.html"
    local_auto_screenshot = await backend.screenshot("s")
    assert local_auto_screenshot.data == b"shot"
    assert local_auto_screenshot.mode == "full_page"
    assert local_auto_screenshot.requested_mode == "auto"
    assert local_auto_screenshot.is_clipped is False
    assert page.screenshot_kwargs["full_page"] is True
    viewport_screenshot = await backend.screenshot("s", mode="viewport")
    assert viewport_screenshot.data == b"shot"
    assert viewport_screenshot.mode == "viewport"
    assert viewport_screenshot.is_clipped is True
    assert page.screenshot_kwargs["full_page"] is False
    await backend.scroll("s", "up", 5)
    await backend.wait_for("s", "#ready", None, None, 1)
    await backend.wait_for("s", None, "https://example.com/*", None, 1)
    await backend.wait_for("s", None, None, None, 1)
    assert await backend.find("s", "a") == [{"tag": "a", "text": "link", "href": "https://example.com"}]
    assert await backend.run_js("s", "1+1") == {"script": "1+1"}
    await playwright_backend.PlaywrightBackend.close_session(backend, "s")
    await playwright_backend.PlaywrightBackend.shutdown(backend)


@pytest.mark.asyncio
async def test_playwright_type_text_falls_back_to_dom_assignment_when_fill_times_out() -> None:
    class Page:
        def __init__(self) -> None:
            self.evaluate_calls: list[str] = []

        async def fill(self, selector, text, **kwargs):
            raise TimeoutError("Page.fill: Timeout 30000ms exceeded")

        async def evaluate(self, script):
            self.evaluate_calls.append(script)

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.type_text("s", "input[aria-label='Where from?']", "116 West 87th St")

    assert page.evaluate_calls
    assert "input[aria-label='Where from?']" in page.evaluate_calls[0]
    assert "116 West 87th St" in page.evaluate_calls[0]


@pytest.mark.asyncio
async def test_playwright_click_falls_back_to_dom_click_when_actionability_times_out() -> None:
    class Page:
        def __init__(self) -> None:
            self.evaluate_calls: list[str] = []

        async def click(self, selector, **kwargs):
            raise TimeoutError("Page.click: Timeout 10000ms exceeded")

        async def evaluate(self, script):
            self.evaluate_calls.append(script)

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.click("s", "input[type='text']:first-of-type")

    assert page.evaluate_calls
    assert "input[type='text']:first-of-type" in page.evaluate_calls[0]
    assert "MouseEvent" in page.evaluate_calls[0]


@pytest.mark.asyncio
async def test_playwright_click_uses_locator_force_before_dom_for_playwright_selectors() -> None:
    class Locator:
        def __init__(self) -> None:
            self.click_calls: list[dict[str, object]] = []

        async def click(self, **kwargs):
            self.click_calls.append(kwargs)

    class Page:
        def __init__(self) -> None:
            self.locator_obj = Locator()

        async def click(self, selector, **kwargs):
            raise TimeoutError("Page.click: Timeout 5000ms exceeded")

        def locator(self, selector):
            assert selector == "button:has-text('See prices')"
            return self.locator_obj

        async def evaluate(self, script):
            raise AssertionError("DOM fallback should not run for Playwright selector")

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.click("s", "button:has-text('See prices')")

    assert page.locator_obj.click_calls == [{"force": True, "timeout": 5000}]


@pytest.mark.asyncio
async def test_playwright_semantic_click_and_type_use_accessible_locators() -> None:
    class Locator:
        def __init__(self, name: str) -> None:
            self.name = name
            self.click_calls: list[dict[str, object]] = []
            self.fill_calls: list[tuple[str, dict[str, object]]] = []

        @property
        def first(self):
            return self

        async def click(self, **kwargs):
            self.click_calls.append(kwargs)

        async def fill(self, text, **kwargs):
            self.fill_calls.append((text, kwargs))

    class Page:
        def __init__(self) -> None:
            self.role_locator = Locator("role")
            self.label_locator = Locator("label")

        def get_by_role(self, role, **kwargs):
            assert role == "button"
            assert kwargs == {"name": "See prices"}
            return self.role_locator

        def get_by_label(self, label):
            assert label == "Where from?"
            return self.label_locator

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.click_element("s", {"role": "button", "name": "See prices"})
    await backend.type_field("s", {"label": "Where from?"}, "116 West")

    assert page.role_locator.click_calls == [{"timeout": 5000}]
    assert page.label_locator.fill_calls == [("116 West", {"timeout": 3000})]


@pytest.mark.asyncio
async def test_playwright_semantic_type_recovers_when_label_resolves_but_fill_times_out() -> None:
    class Locator:
        def __init__(self) -> None:
            self.evaluate_calls: list[tuple[str, str, dict[str, object]]] = []

        @property
        def first(self):
            return self

        async def fill(self, text, **kwargs):
            raise TimeoutError("Locator.fill: Timeout 10000ms exceeded")

        async def press_sequentially(self, text, **kwargs):
            raise TimeoutError("Locator.press_sequentially: Timeout 10000ms exceeded")

        async def evaluate(self, script, text, **kwargs):
            self.evaluate_calls.append((script, text, kwargs))
            return {"ok": True, "clicked": True, "editable": False}

        async def input_value(self, **kwargs):
            return None

    class Page:
        def __init__(self) -> None:
            self.locator = Locator()

        def get_by_label(self, label):
            assert label == "Select a date."
            return self.locator

        async def evaluate(self, script):
            return False

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.type_field("s", {"label": "Select a date."}, "in an hour")

    assert page.locator.evaluate_calls
    assert page.locator.evaluate_calls[0][1] == "in an hour"
    assert page.locator.evaluate_calls[0][2]["timeout"] == 3000


@pytest.mark.asyncio
async def test_playwright_semantic_click_prefers_active_combobox_suggestion() -> None:
    class Page:
        def __init__(self) -> None:
            self.evaluate_calls: list[str] = []
            self.get_by_text_called = False

        async def evaluate(self, script):
            self.evaluate_calls.append(script)
            return True

        def get_by_text(self, text):
            self.get_by_text_called = True
            raise AssertionError("active combobox option should be tried before global text locator")

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.click_element("s", {"text": "Knockdown Center"})

    assert page.evaluate_calls
    assert "aria-controls" in page.evaluate_calls[0]
    assert "Knockdown Center" in page.evaluate_calls[0]
    assert page.get_by_text_called is False


@pytest.mark.asyncio
async def test_playwright_semantic_type_tries_visible_candidates_until_value_sticks() -> None:
    class Locator:
        def __init__(self, name: str, *, visible: bool, sticks: bool) -> None:
            self.name = name
            self.visible = visible
            self.sticks = sticks
            self.value = ""
            self.fill_calls: list[str] = []

        async def is_visible(self, **kwargs):
            return self.visible

        async def is_enabled(self, **kwargs):
            return True

        async def fill(self, text, **kwargs):
            self.fill_calls.append(text)
            self.value = text if self.sticks else ""

        async def input_value(self, **kwargs):
            return self.value

    class LocatorList:
        def __init__(self) -> None:
            self.items = [
                Locator("hidden", visible=False, sticks=True),
                Locator("visible-no-stick", visible=True, sticks=False),
                Locator("visible-stick", visible=True, sticks=True),
            ]

        @property
        def first(self):
            return self.items[0]

        async def count(self):
            return len(self.items)

        def nth(self, index):
            return self.items[index]

    class Page:
        def __init__(self) -> None:
            self.locators = LocatorList()

        def get_by_label(self, label):
            assert label == "pickup location input"
            return self.locators

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.type_field("s", {"label": "pickup location input"}, "116 West")

    hidden, visible_no_stick, visible_stick = page.locators.items
    assert hidden.fill_calls == []
    assert visible_no_stick.fill_calls == ["116 West"]
    assert visible_stick.fill_calls == ["116 West"]


@pytest.mark.asyncio
async def test_playwright_semantic_type_skips_disabled_duplicate_candidates() -> None:
    class Locator:
        def __init__(self, name: str, *, enabled: bool) -> None:
            self.name = name
            self.enabled = enabled
            self.value = ""
            self.fill_calls: list[str] = []

        async def is_visible(self, **kwargs):
            return True

        async def is_enabled(self, **kwargs):
            return self.enabled

        async def fill(self, text, **kwargs):
            self.fill_calls.append(text)
            self.value = text

        async def input_value(self, **kwargs):
            return self.value

    class LocatorList:
        def __init__(self) -> None:
            self.items = [Locator("disabled", enabled=False), Locator("enabled", enabled=True)]

        @property
        def first(self):
            return self.items[0]

        async def count(self):
            return len(self.items)

        def nth(self, index):
            return self.items[index]

    class Page:
        def __init__(self) -> None:
            self.locators = LocatorList()

        def get_by_label(self, label):
            return self.locators

        async def evaluate(self, script):
            return False

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.type_field("s", {"label": "Pickup location"}, "116 West 87th St")

    disabled, enabled = page.locators.items
    assert disabled.fill_calls == []
    assert enabled.fill_calls == ["116 West 87th St"]


@pytest.mark.asyncio
async def test_playwright_semantic_type_commits_active_combobox_suggestion() -> None:
    class Locator:
        def __init__(self) -> None:
            self.value = ""

        async def fill(self, text, **kwargs):
            self.value = text

        async def input_value(self, **kwargs):
            return self.value

    class Page:
        def __init__(self) -> None:
            self.locator = Locator()
            self.evaluate_calls: list[str] = []
            self.active_has_options = True

        def get_by_label(self, label):
            assert label == "Dropoff location"
            return SimpleNamespace(first=self.locator)

        async def evaluate(self, script):
            self.evaluate_calls.append(script)
            if "querySelectorAll" in script:
                return self.active_has_options
            return True

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    await backend.type_field("s", {"label": "Dropoff location"}, "Knockdown Center, Maspeth, NY")

    assert page.locator.value == "Knockdown Center, Maspeth, NY"
    assert page.evaluate_calls
    assert "aria-controls" in page.evaluate_calls[0]
    assert "Knockdown Center, Maspeth, NY" in page.evaluate_calls[-1]


@pytest.mark.asyncio
async def test_playwright_semantic_type_fails_when_active_options_do_not_match() -> None:
    class Locator:
        async def fill(self, text, **kwargs):
            return None

        async def input_value(self, **kwargs):
            return "116 West 87th St"

    class Page:
        def get_by_label(self, label):
            return SimpleNamespace(first=Locator())

        async def evaluate(self, script):
            if "some((el)" in script:
                return True
            return False

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="No compatible active combobox suggestion"):
        await backend.type_field("s", {"label": "Pickup location"}, "116 West 87th St")


def test_playwright_semantic_type_clicks_matching_option_not_menu_container(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    html = """
    <!doctype html>
    <html>
      <body>
        <label for="pickup">Pickup location</label>
        <input id="pickup" aria-label="Pickup location" role="combobox" aria-controls="pickup-options" />
        <div id="pickup-options" role="listbox" onclick="document.getElementById('pickup').value='Harlem Meer 36 W 110th St, New York, NY'">
          <div role="option" onclick="event.stopPropagation(); document.getElementById('pickup').value='Harlem Meer 36 W 110th St, New York, NY'">Harlem Meer 36 W 110th St, New York, NY</div>
          <div role="option" onclick="event.stopPropagation(); document.getElementById('pickup').value='116 W 87th St, New York, NY'">116 W 87th St, New York, NY</div>
        </div>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("menu-container", "data:text/html," + quote(html)))
        _run(backend.type_field("menu-container", {"label": "Pickup location"}, "116 West 87th St, New York, NY"))
        value = _run(backend.run_js("menu-container", "document.getElementById('pickup').value"))
        assert value == "116 W 87th St, New York, NY"
    finally:
        _run(backend.close_session("menu-container"))
        _run(backend.shutdown())


@pytest.mark.parametrize("routes_to_incompatible_state", [False, True])
def test_browser_evidence_revalidation_handles_anchor_and_delayed_hash_spa_route(
    monkeypatch,
    routes_to_incompatible_state: bool,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    session_id = "evidence-revalidation"
    href = "#/product/red" if routes_to_incompatible_state else "#details"
    click_handler = (
        "event.preventDefault(); setTimeout(() => { history.pushState({}, '', '#/product/red'); "
        "document.querySelector('main').textContent = 'Red model'; }, 75);"
        if routes_to_incompatible_state
        else ""
    )
    html = f"""
    <!doctype html>
    <html>
      <body>
        <a href="{href}" onclick="{click_handler}">Open details</a>
        {'<div id="/product/red" hidden>Legacy fragment target</div>' if routes_to_incompatible_state else ''}
        <main id="details">Blue model</main>
      </body>
    </html>
    """
    html_bytes = html.encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html_bytes)))
            self.end_headers()
            self.wfile.write(html_bytes)

        def log_message(self, _format: str, *args: object) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    page_url = f"http://127.0.0.1:{server.server_port}/product"
    try:
        _run(backend.navigate(session_id, page_url))
        asserted = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["Blue model"],
                    "forbidden": ["Red model"],
                    "selector": "main",
                },
            )
        )
        assert asserted.output["verified"] is True
        click_result = tools.browser_click_element(
            inv(
                "browser_click_element",
                {"role": "link", "name": "Open details", "session_id": session_id},
            )
        )
        evidence = tools.browser_run_js(
            inv(
                "browser_run_js",
                {
                    "session_id": session_id,
                    "script": (
                        "async () => { await new Promise((resolve) => setTimeout(resolve, 150)); "
                        "return {title: document.querySelector('main').textContent, url: location.href}; }"
                    ),
                },
            )
        )
        assertion = evidence.output["page_state_assertion"]
        assert assertion["verified"] is (not routes_to_incompatible_state)
        if routes_to_incompatible_state:
            assert assertion["result"]["forbidden_found"]
            assert evidence.output["result"]["title"] == "Red model"
            click_result.output["navigation"] = {
                "kind": "in_document_anchor",
                "verified": True,
                "from_url": page_url,
                "to_url": f"{page_url}#/product/red",
                "target_url": f"{page_url}#/product/red",
            }
            from nullion.response_sanitizer import sanitize_user_visible_reply

            reply = sanitize_user_visible_reply(
                user_message="Find the verified blue model.",
                reply=f"Red model — $10\nOpen: {evidence.output['result']['url']}",
                tool_results=[asserted, click_result, evidence],
            )
            assert reply is not None
            assert reply == f"Red model — $10\nOpen: {evidence.output['result']['url']}"
        else:
            assert assertion["result"]["url"].endswith("#details")
            assert evidence.output["result"]["title"] == "Blue model"
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)


def test_browser_id_and_combobox_tools_work_against_real_dynamic_page(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <label for="pickup">Pickup location</label>
        <input id="pickup" aria-label="Pickup location" role="combobox" aria-controls="pickup-options" />
        <div id="pickup-options" role="listbox"></div>
        <button id="prices" onclick="document.body.dataset.clicked='prices'">See prices</button>
        <script>
          const pickup = document.getElementById('pickup');
          const options = document.getElementById('pickup-options');
          pickup.addEventListener('input', () => {
            options.innerHTML = '';
            setTimeout(() => {
              options.innerHTML = `
                <div role="option" onclick="document.getElementById('pickup').value='100 W 26th St'">100 W 26th St</div>
                <div role="option" onclick="document.getElementById('pickup').value='116 W 87th St'">116 W 87th St</div>
              `;
            }, 250);
          });
        </script>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("real-dynamic", "data:text/html," + quote(html)))
        snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "real-dynamic"}))
        elements = snapshot.output["snapshot"]["elements"]
        pickup_id = next(element["element_id"] for element in elements if element.get("label") == "Pickup location")
        button_id = next(element["element_id"] for element in elements if element.get("text") == "See prices")

        selected = tools.browser_select_combobox(
            inv(
                "browser_select_combobox",
                {
                    "session_id": "real-dynamic",
                    "element_id": pickup_id,
                    "query": "116 West 87th St",
                    "expected_text": "116 W 87th St",
                },
            )
        )
        assert selected.status == "completed"
        assert selected.output["selected"] == "116 W 87th St"

        clicked = tools.browser_click_id(
            inv("browser_click_id", {"session_id": "real-dynamic", "element_id": button_id})
        )
        assert clicked.status == "completed"
        assert _run(backend.run_js("real-dynamic", "document.body.dataset.clicked")) == "prices"
    finally:
        _run(backend.close_session("real-dynamic"))
        _run(backend.shutdown())


def test_browser_combobox_tool_refuses_wrong_dynamic_option(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <label for="pickup">Pickup location</label>
        <input id="pickup" aria-label="Pickup location" role="combobox" aria-controls="pickup-options" />
        <div id="pickup-options" role="listbox">
          <div role="option" onclick="document.getElementById('pickup').value='100 W 26th St'">100 W 26th St</div>
        </div>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("wrong-option", "data:text/html," + quote(html)))
        snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "wrong-option"}))
        pickup_id = snapshot.output["snapshot"]["elements"][0]["element_id"]
        selected = tools.browser_select_combobox(
            inv(
                "browser_select_combobox",
                {
                    "session_id": "wrong-option",
                    "element_id": pickup_id,
                    "query": "116 West 87th St",
                    "expected_text": "116 W 87th St",
                },
            )
        )
        assert selected.status == "failed"
        assert selected.error == "Combobox selection failed: no_compatible_option"
        assert selected.output["result"]["options"] == ["100 W 26th St"]
    finally:
        _run(backend.close_session("wrong-option"))
        _run(backend.shutdown())


def test_browser_combobox_tool_refuses_wrong_harlem_pickup_option(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <label for="pickup">Pickup location</label>
        <input id="pickup" aria-label="Pickup location" role="combobox" aria-controls="pickup-options" />
        <div id="pickup-options" role="listbox">
          <div role="option" onclick="document.getElementById('pickup').value='Harlem Meer 36 W 110th St, New York, NY'">Harlem Meer 36 W 110th St, New York, NY</div>
        </div>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("wrong-harlem-option", "data:text/html," + quote(html)))
        snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "wrong-harlem-option"}))
        pickup_id = snapshot.output["snapshot"]["elements"][0]["element_id"]
        selected = tools.browser_select_combobox(
            inv(
                "browser_select_combobox",
                {
                    "session_id": "wrong-harlem-option",
                    "element_id": pickup_id,
                    "query": "116 West 87th St, New York, NY",
                    "expected_text": "116 W 87th St, New York, NY",
                },
            )
        )
        assert selected.status == "failed"
        assert selected.error == "Combobox selection failed: no_compatible_option"
        assert selected.output["result"]["options"] == ["Harlem Meer 36 W 110th St, New York, NY"]
    finally:
        _run(backend.close_session("wrong-harlem-option"))
        _run(backend.shutdown())


def test_browser_assert_page_state_verifies_requested_values_and_rejects_wrong_pickup(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <label for="pickup">Pickup location</label>
        <input id="pickup" aria-label="Pickup location" value="Harlem Meer 36 W 110th St, New York, NY" />
        <label for="dropoff">Dropoff location</label>
        <input id="dropoff" aria-label="Dropoff location" value="Knockdown Center" />
        <button>See prices</button>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("assert-wrong-pickup", "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": "assert-wrong-pickup",
                    "required": ["116 West 87th St", "Knockdown Center"],
                    "forbidden": ["Harlem Meer 36 W 110th St"],
                },
            )
        )
        assert result.status == "completed"
        assert result.output["verified"] is False
        assert result.output["missing_required"] == ["116 West 87th St"]
        assert result.output["forbidden_found"] == ["Harlem Meer 36 W 110th St"]
        assert result.output["result"]["missing"][0]["expected"] == "116 West 87th St"
        assert result.output["result"]["forbidden_found"][0]["match"] == "Harlem Meer 36 W 110th St, New York, NY"
    finally:
        _run(backend.close_session("assert-wrong-pickup"))
        _run(backend.shutdown())


def test_browser_assert_page_state_reports_missing_required_text_as_unverified_observation(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <main>Noticias principales</main>
        <article>Titular actual visible</article>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("assert-missing-required", "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": "assert-missing-required",
                    "required": ["Titular que ya no esta visible"],
                },
            )
        )
        assert result.status == "completed"
        assert result.output["verified"] is False
        assert result.output["missing_required"] == ["Titular que ya no esta visible"]
    finally:
        _run(backend.close_session("assert-missing-required"))
        _run(backend.shutdown())


def test_browser_assert_page_state_fails_blank_page_before_reporting_completion(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    try:
        _run(backend.open("assert-blank-page"))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": "assert-blank-page",
                    "required": ["Kirkland Signature Super B-Complex with Electrolytes"],
                },
            )
        )
        assert result.status == "failed"
        assert "Browser page is blank" in (result.error or "")
    finally:
        _run(backend.close_session("assert-blank-page"))
        _run(backend.shutdown())


def test_browser_assert_page_state_reuses_explicit_scope_across_capsules(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_BROWSER_HEADLESS", raising=False)
    backend = BlankDefaultThenActiveAssertBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    navigate = tools.browser_navigate(
        ToolInvocation(
            "nav",
            "browser_navigate",
            "telegram_chat",
            {"url": "https://www.amazon.com/gp/css/order-history"},
            capsule_id="browser-capsule",
            flow_context={"browser_session_scope": "browser-workflow"},
        )
    )
    assert navigate.status == "completed"

    result = tools.browser_assert_page_state(
        ToolInvocation(
            "assert",
            "browser_assert_page_state",
            "telegram_chat",
            {"required": ["Amazon"]},
            capsule_id="assert-capsule",
            flow_context={"browser_session_scope": "browser-workflow"},
        )
    )

    assert result.status == "completed"
    assert result.output["verified"] is True
    assert result.output["session_id"] == navigate.output["session_id"]
    js_sessions = [call[1] for call in backend.calls if call[0] == "js"]
    assert js_sessions == [navigate.output["session_id"]]


def test_browser_assert_page_state_does_not_cross_capsules_when_retry_loses_context(monkeypatch) -> None:
    monkeypatch.delenv("NULLION_BROWSER_HEADLESS", raising=False)
    backend = BlankDefaultThenActiveAssertBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    navigate = tools.browser_navigate(
        ToolInvocation(
            "nav",
            "browser_navigate",
            "operator",
            {"url": "https://www.amazon.com/gp/css/order-history"},
            capsule_id="browser-capsule",
            flow_context={
                "conversation_id": "telegram:100001",
                "request_id": "telegram-update:443397597",
            },
        )
    )
    assert navigate.status == "completed"

    result = tools.browser_assert_page_state(
        ToolInvocation(
            "assert",
            "browser_assert_page_state",
            "operator",
            {"required": ["Amazon"]},
            capsule_id="schema-followup-capsule",
        )
    )

    assert result.status == "failed"
    assert "Browser page is blank" in (result.error or "")
    js_sessions = [call[1] for call in backend.calls if call[0] == "js"]
    assert len(js_sessions) == 1
    assert js_sessions[0] != navigate.output["session_id"]


def test_browser_assert_page_state_does_not_escape_runtime_bound_session_on_blank_page() -> None:
    class BoundSessionBackend(CompleteBackend):
        BACKEND_NAME = "cdp"

        async def run_js(self, session_id: str, script: str):
            self.calls.append(("js", session_id, script))
            if session_id == "runtime-bound-session":
                return {
                    "ok": False,
                    "url": "about:blank",
                    "title": "",
                    "required": [{"expected": "Amazon", "match": ""}],
                    "forbidden": [],
                    "missing": [{"expected": "Amazon", "match": ""}],
                    "forbidden_found": [],
                }
            return {
                "ok": True,
                "url": "https://www.amazon.com/gp/css/order-history",
                "title": "Amazon.com",
                "required": [{"expected": "Amazon", "match": "Amazon"}],
                "forbidden": [],
                "missing": [],
                "forbidden_found": [],
            }

    backend = BoundSessionBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    tools.browser_navigate(
        inv(
            "browser_navigate",
            {"session_id": "other-active-session", "url": "https://www.amazon.com"},
            capsule_id="bound-scope",
        )
    )

    result = tools.browser_assert_page_state(
        ToolInvocation(
            "bound-assert",
            "browser_assert_page_state",
            "operator",
            {"required": ["Amazon"]},
            capsule_id="bound-scope",
            flow_context={"browser_session_id": "runtime-bound-session"},
        )
    )

    assert result.status == "failed"
    assert "Browser page is blank" in (result.error or "")
    js_sessions = [call[1] for call in backend.calls if call[0] == "js"]
    assert js_sessions == ["runtime-bound-session"]


def test_browser_assert_page_state_passes_when_requested_values_are_visible(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <label for="pickup">Pickup location</label>
        <input id="pickup" aria-label="Pickup location" value="116 W 87th St, New York, NY" />
        <label for="dropoff">Dropoff location</label>
        <input id="dropoff" aria-label="Dropoff location" value="Knockdown Center" />
        <main>UberX $42.10 Comfort $55.00</main>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("assert-good-state", "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": "assert-good-state",
                    "required": ["116 W 87th St", "Knockdown Center", "UberX"],
                    "forbidden": ["Harlem Meer 36 W 110th St"],
                },
            )
        )
        assert result.status == "completed"
        matches = {item["expected"]: item["match"] for item in result.output["result"]["required"]}
        assert matches["116 W 87th St"] == "116 W 87th St, New York, NY"
        assert matches["Knockdown Center"] == "Knockdown Center"
        assert matches["UberX"] == "UberX $42.10 Comfort $55.00"
    finally:
        _run(backend.close_session("assert-good-state"))
        _run(backend.shutdown())


def test_browser_assert_page_state_keeps_meaningful_aggregate_evidence_after_many_short_nodes(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    short_nodes = "".join(f"<span>{chr(65 + (index % 26))}</span>" for index in range(140))
    html = f"""
    <!doctype html>
    <html>
      <head><title>Dermaharmony 5% Sulfur Liquid Castile Soap</title></head>
      <body>
        <nav>{short_nodes}</nav>
        <main id="product-detail">
          <h1>Dermaharmony 5% Sulfur and 2% Salicylic Acid Liquid Castile Soap</h1>
          <p>ACTIVE INGREDIENTS: 5% Sulfur and 2% Salicylic Acid.</p>
          <p>Liquid wash, 4 fl oz bottle. Price $9.99.</p>
        </main>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("assert-product-evidence", "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": "assert-product-evidence",
                    "required": ["Dermaharmony", "5% Sulfur", "Liquid Castile Soap"],
                    "selector": "#product-detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is True
        assert result.output["result"]["selector"] == "#product-detail"
        assert result.output["result"]["candidate_count"] > 0
        matches = {item["expected"]: item["match"] for item in result.output["result"]["required"]}
        assert "ACTIVE INGREDIENTS" in matches["5% Sulfur"]
        assert "Liquid Castile Soap" in matches["Liquid Castile Soap"]
    finally:
        _run(backend.close_session("assert-product-evidence"))
        _run(backend.shutdown())


def test_browser_extract_detail_accepts_selected_real_dom_and_rejects_catalog_dom(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    detail_html = """
    <!doctype html>
    <html>
      <head><title>Rendered Liquid Cleanser</title></head>
      <body>
        <main>
          <section id="product-detail">
            <h1 itemprop="name">Rendered Liquid Cleanser</h1>
            <span itemprop="price" content="$19.00">$19.00</span>
            <p itemprop="description">A liquid cleanser with 5% sulfur.</p>
            <p class="availability">In stock</p>
          </section>
        </main>
      </body>
    </html>
    """
    catalog_htmls = [
        """
        <main>
          <article itemscope><h2 itemprop="name"><a href="/one">Product One</a></h2><span itemprop="price">$10</span></article>
          <article itemscope><h2 itemprop="name"><a href="/two">Product Two</a></h2><span itemprop="price">$20</span></article>
        </main>
        """,
        """
        <main>
          <div class="product"><h1><a href="/one">Product One</a></h1><span class="price">$10</span></div>
          <div class="product"><h1><a href="/two">Product Two</a></h1><span class="price">$20</span></div>
        </main>
        """,
        """
        <main>
          <div><h2><a href="/one">Product One</a></h2><p>First description</p></div>
          <div><h2><a href="/two">Product Two</a></h2><p>Second description</p></div>
        </main>
        """,
        """
        <html>
          <head><title>Sulfur collection</title></head>
          <body>
            <main>
              <div class="product"><a href="/one">Liquid Sulfur Wash One</a><p>Liquid sulfur cleanser with a long catalog description.</p></div>
              <div class="product"><a href="/two">Liquid Sulfur Wash Two</a><p>Another sulfur cleanser with a long catalog description.</p></div>
            </main>
          </body>
        </html>
        """,
        """
        <main>
          <h1>Sulfur collection</h1>
          <div class="product"><a href="/one">Liquid Sulfur Wash One</a><p>Liquid sulfur cleanser with a long catalog description.</p></div>
          <div class="product"><a href="/two">Liquid Sulfur Wash Two</a><p>Another sulfur cleanser with a long catalog description.</p></div>
        </main>
        """,
    ]
    recommendation_html = """
    <main>
      <article itemscope>
        <h2 itemprop="name"><a href="/related">Related Sulfur Bar</a></h2>
        <span itemprop="price">$5</span>
      </article>
      <section id="primary-product">
        <h1>Main Liquid Sulfur Wash</h1>
        <span class="price">$20</span>
        <p itemprop="description">The selected liquid product detail.</p>
      </section>
    </main>
    """
    conflicting_price_html = """
    <main id="product">
      <h1>Current Product</h1>
      <span class="price old">$29.99</span>
      <span class="price current">$19.99</span>
      <p itemprop="description">The selected current product detail.</p>
    </main>
    """
    nested_price_html = """
    <style>
      .a-offscreen { position: absolute; width: 1px; height: 1px; overflow: hidden; }
    </style>
    <main id="product">
      <h1>Amazon-style Current Product</h1>
      <div class="a-price">
        <span class="a-offscreen">$19.99</span>
        <span aria-hidden="true"><span class="a-price-symbol">$</span><span class="a-price-whole">19</span><span class="a-price-decimal">.</span><span class="a-price-fraction">99</span></span>
      </div>
      <p itemprop="description">The selected product has one rendered nested price.</p>
    </main>
    """
    try:
        _run(backend.navigate("detail-dom", "data:text/html," + quote(detail_html)))
        detail = tools.browser_extract_detail(
            inv(
                "browser_extract_detail",
                {"session_id": "detail-dom", "selector": "#product-detail"},
            )
        )

        assert detail.status == "completed"
        assert detail.output["schema_version"] == 1
        assert detail.output["kind"] == "browser_dom_detail"
        assert detail.output["origin"] == "runtime_owned_dom_extractor"
        assert detail.output["record_scope"] == "selected_root"
        assert detail.output["record"]["title"] == "Rendered Liquid Cleanser"
        assert detail.output["record"]["price"] == "$19.00"
        assert "5% sulfur" in detail.output["record"]["description"]

        _run(backend.navigate("recommendation-dom", "data:text/html," + quote(recommendation_html)))
        recommendation = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": "recommendation-dom"})
        )

        assert recommendation.status == "completed"
        assert recommendation.output["record"]["title"] == "Main Liquid Sulfur Wash"
        assert recommendation.output["record"]["price"] == "$20"
        assert "Related Sulfur Bar" not in recommendation.output["record"]["details"]

        _run(backend.navigate("conflicting-price-dom", "data:text/html," + quote(conflicting_price_html)))
        conflicting_price = tools.browser_extract_detail(
            inv(
                "browser_extract_detail",
                {"session_id": "conflicting-price-dom", "selector": "#product"},
            )
        )

        assert conflicting_price.status == "completed"
        assert "price" not in conflicting_price.output["record"]
        assert conflicting_price.output["record"]["price_candidates"] == ["$29.99", "$19.99"]

        _run(backend.navigate("nested-price-dom", "data:text/html," + quote(nested_price_html)))
        nested_price = tools.browser_extract_detail(
            inv(
                "browser_extract_detail",
                {"session_id": "nested-price-dom", "selector": "#product"},
            )
        )

        assert nested_price.status == "completed"
        assert nested_price.output["record"]["price"] == "$19.99"
        assert "price_candidates" not in nested_price.output["record"]

        for index, catalog_html in enumerate(catalog_htmls, start=1):
            session_id = f"catalog-dom-{index}"
            _run(backend.navigate(session_id, "data:text/html," + quote(catalog_html)))
            catalog = tools.browser_extract_detail(
                inv("browser_extract_detail", {"session_id": session_id})
            )

            assert catalog.status == "failed"
            assert "did not expose a substantive detail record" in (catalog.error or "")
    finally:
        _run(backend.close_session("detail-dom"))
        _run(backend.close_session("recommendation-dom"))
        _run(backend.close_session("conflicting-price-dom"))
        _run(backend.close_session("nested-price-dom"))
        for index in range(1, len(catalog_htmls) + 1):
            _run(backend.close_session(f"catalog-dom-{index}"))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("primary_title", "selected_label", "unselected_label", "related_label"),
    [
        ("Primary cleanser", "Selected offer", "Alternate offer", "Related record"),
        ("主要な洗浄料", "選択済み", "別の候補", "関連する記録"),
    ],
)
def test_browser_extract_detail_uses_structural_price_roles_independent_of_labels(
    monkeypatch,
    primary_title: str,
    selected_label: str,
    unselected_label: str,
    related_label: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html>
      <head>
        <title>{primary_title}</title>
        <style>
          .primary-price {{ font-size: 30px; font-weight: 700; }}
          .subordinate-price {{ font-size: 12px; font-weight: 400; }}
          .list-value {{ text-decoration: line-through; }}
        </style>
      </head>
      <body>
        <main id="detail">
          <h1>{primary_title}</h1>
          <article>
            <h2><a href="/related">{related_label}</a></h2>
            <span class="price">$77.00</span>
          </article>
          <ul>
            <li role="radio" aria-checked="false">
              <span>{unselected_label}</span>
              <span class="price">$31.00</span>
            </li>
            <li role="radio" aria-checked="true">
              <span>{selected_label}</span>
              <span class="price primary-price">$19.00</span>
              <span class="price subordinate-price">$1.90</span>
            </li>
          </ul>
          <span class="price list-value">$29.00</span>
          <p itemprop="description">{primary_title}</p>
        </main>
      </body>
    </html>
    """
    session_id = f"structural-price-roles-{ord(primary_title[0])}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$19.00"
        assert "price_candidates" not in record
        observations = {observation["value"]: observation for observation in record["price_observations"]}
        assert observations["$19.00"]["role"] == "selected_offer_price"
        assert observations["$19.00"]["status"] == "current_candidate"
        assert observations["$77.00"]["reasons"] == ["distinct_record_island"]
        assert observations["$31.00"]["reasons"] == ["explicitly_unselected_offer"]
        assert observations["$1.90"]["reasons"] == ["subordinate_visual_prominence"]
        assert observations["$29.00"]["reasons"] == ["struck_or_list_value"]
        for rejected_price in ("$77.00", "$31.00", "$1.90", "$29.00"):
            assert rejected_price not in record["details"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_preserves_equal_current_price_conflicts(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current record</h1>
      <div>
        <label><input type="checkbox" checked> Unrelated preference</label>
        <span class="price">$19.00</span>
        <span class="price">$21.00</span>
      </div>
      <p itemprop="description">Two equally eligible current values remain unresolved.</p>
    </main>
    """
    session_id = "equal-current-price-conflict"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert record["price_candidates"] == ["$19.00", "$21.00"]
        assert [
            observation["status"] for observation in record["price_observations"]
        ] == ["current_candidate", "current_candidate"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_keeps_selected_offer_with_its_own_heading(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current cleanser</h1>
      <ul>
        <li role="radio" aria-checked="false">
          <h2>Subscription</h2>
          <span class="price">$17.00</span>
        </li>
        <li role="radio" aria-checked="true">
          <h2>One-time purchase</h2>
          <span class="price">$19.00</span>
        </li>
      </ul>
      <p itemprop="description">The selected purchase offer is current.</p>
    </main>
    """
    session_id = "selected-offer-own-heading"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$19.00"
        observations = {item["value"]: item for item in record["price_observations"]}
        assert observations["$19.00"]["role"] == "selected_offer_price"
        assert observations["$19.00"]["status"] == "current_candidate"
        assert observations["$17.00"]["reasons"] == ["explicitly_unselected_offer"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_associates_sibling_radio_with_offer_row(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current cleanser</h1>
      <ul>
        <li><input type="radio" name="offer"><h2>Subscription</h2><span class="price">$17.00</span></li>
        <li><input type="radio" name="offer" checked><h2>One time</h2><span class="price">$19.00</span></li>
      </ul>
      <p itemprop="description">The selected offer control is a sibling of its price.</p>
    </main>
    """
    session_id = "sibling-radio-offer-row"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$19.00"
        observations = {item["value"]: item for item in record["price_observations"]}
        assert observations["$19.00"]["role"] == "selected_offer_price"
        assert observations["$17.00"]["reasons"] == ["explicitly_unselected_offer"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_does_not_promote_checked_add_on_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Laptop</h1>
      <span class="price">$1000.00</span>
      <label>
        <input type="checkbox" checked>
        Add warranty
        <span class="price">$99.00</span>
      </label>
      <p itemprop="description">The optional control is not a product offer selector.</p>
    </main>
    """
    session_id = "checked-add-on-price"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$1000.00"
        observations = {item["value"]: item for item in record["price_observations"]}
        assert observations["$99.00"]["reasons"] == ["non_offer_control_price"]
        assert observations["$99.00"]["status"] == "excluded"
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_excludes_sibling_checkbox_add_on_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Laptop</h1>
      <span class="price">$1000.00</span>
      <div>
        <input type="checkbox" checked>
        <span>Add warranty</span>
        <span class="price" style="font-size: 28px">$99.00</span>
      </div>
      <p itemprop="description">The checkbox and add-on price share a local wrapper.</p>
    </main>
    """
    session_id = "sibling-checkbox-add-on"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$1000.00"
        observations = {item["value"]: item for item in record["price_observations"]}
        assert observations["$99.00"]["reasons"] == ["non_offer_control_price"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_excludes_singleton_selected_radio_add_on_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Laptop</h1>
      <span class="price">$1000.00</span>
      <div>
        <input type="radio" checked>
        <span>Optional financing</span>
        <span class="price" style="font-size: 28px">$99.00</span>
      </div>
      <p itemprop="description">A singleton radio is an optional control, not an offer group.</p>
    </main>
    """
    session_id = "singleton-radio-add-on"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert result.output["record"]["price"] == "$1000.00"
        observations = {
            item["value"]: item for item in result.output["record"]["price_observations"]
        }
        assert observations["$99.00"]["reasons"] == ["non_offer_control_price"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_withholds_untyped_selected_radio_group_over_primary_price(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Laptop</h1>
      <span class="price">$1000.00</span>
      <section>
        <label><input name="gift" type="radio" checked>Gift wrap <span class="price" style="font-size:28px">$99.00</span></label>
        <label><input name="gift" type="radio">No gift <span class="price">$0.00</span></label>
      </section>
      <p itemprop="description">An untyped radio group cannot replace a separate primary price.</p>
    </main>
    """
    session_id = "untyped-radio-group-add-on"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"$1000.00", "$99.00"}
        assert record["price_conflict"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_excludes_selected_related_record(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current product</h1>
      <span class="price">$1000.00</span>
      <section>
        <article role="radio" aria-checked="true">
          <h2><a href="/related">Related product</a></h2>
          <span class="price" style="font-size: 28px">$99.00</span>
        </article>
        <article role="radio" aria-checked="false">
          <h2><a href="/other">Other product</a></h2>
          <span class="price">$89.00</span>
        </article>
      </section>
      <p itemprop="description">Linked records remain separate product islands.</p>
    </main>
    """
    session_id = "selected-related-record"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "failed"
        assert "substantive detail record" in (result.error or "")
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_excludes_checkbox_add_on_with_multiple_prices(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Laptop</h1>
      <span class="price">$1000.00</span>
      <div>
        <input type="checkbox" checked>
        <span>Add warranty</span>
        <s class="price">$129.00</s>
        <span class="price" style="font-size: 28px">$99.00</span>
      </div>
      <p itemprop="description">Both optional add-on prices belong to the checkbox wrapper.</p>
    </main>
    """
    session_id = "checkbox-multiple-add-on-prices"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert result.output["record"]["price"] == "$1000.00"
        observations = {
            item["value"]: item for item in result.output["record"]["price_observations"]
        }
        assert "non_offer_control_price" in observations["$99.00"]["reasons"]
        assert "non_offer_control_price" in observations["$129.00"]["reasons"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "control_markup",
    [
        '<label><input type="checkbox" checked>Warranty</label>',
        '<span><input type="checkbox" checked></span>',
    ],
)
def test_browser_extract_detail_excludes_nested_checkbox_sibling_price(
    monkeypatch,
    control_markup: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <main id="detail">
      <h1>Laptop</h1>
      <span class="price">$1000.00</span>
      <div>
        {control_markup}
        <span class="price" style="font-size: 28px">$99.00</span>
      </div>
      <p itemprop="description">The nested checkbox and price share one bounded wrapper.</p>
    </main>
    """
    session_id = f"nested-checkbox-sibling-{len(control_markup)}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert result.output["record"]["price"] == "$1000.00"
        observations = {
            item["value"]: item for item in result.output["record"]["price_observations"]
        }
        assert "non_offer_control_price" in observations["$99.00"]["reasons"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_excludes_aria_switch_add_on_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Laptop</h1>
      <span class="price">$1000.00</span>
      <div>
        <button role="switch" aria-checked="true">Warranty</button>
        <span class="price" style="font-size: 28px">$99.00</span>
      </div>
      <p itemprop="description">A switch controls an optional add-on.</p>
    </main>
    """
    session_id = "aria-switch-add-on"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert result.output["record"]["price"] == "$1000.00"
        observations = {
            item["value"]: item for item in result.output["record"]["price_observations"]
        }
        assert "non_offer_control_price" in observations["$99.00"]["reasons"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reports_schema_offer_visual_conflict(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Structured current record</h1>
      <div itemprop="offers" itemscope itemtype="https://schema.org/Offer">
        <meta itemprop="price" content="25.00">
        <a href="/shipping">Shipping details</a>
      </div>
      <span class="price">$19.00</span>
      <div itemscope itemtype="https://schema.org/Offer">
        <meta itemprop="price" content="77.00">
      </div>
      <p itemprop="description">The schema offer is the strongest structural price role.</p>
    </main>
    """
    session_id = "schema-offer-price"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"25.00", "$19.00"}
        assert record["price_conflict"] is True
        observations = {observation["value"]: observation for observation in record["price_observations"]}
        assert observations["25.00"]["role"] == "schema_offer_price"
        assert observations["25.00"]["status"] == "current_candidate"
        assert observations["25.00"]["reasons"] == []
        assert observations["$19.00"]["status"] == "current_candidate"
        assert observations["$19.00"]["conflict"] is True
        assert observations["77.00"]["reasons"] == ["distinct_record_island"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_pairs_schema_price_with_owned_currency(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Structured product</h1>
      <div itemprop="offers" itemscope itemtype="https://schema.org/Offer">
        <meta itemprop="price" content="19.00">
        <meta itemprop="priceCurrency" content="USD">
      </div>
      <span class="price">$19.00</span>
      <p itemprop="description">The schema amount and currency belong to this offer.</p>
    </main>
    """
    session_id = "schema-owned-currency"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "USD 19.00"
        observations = {item["value"]: item for item in record["price_observations"]}
        assert observations["USD 19.00"]["role"] == "schema_offer_price"
        assert observations["USD 19.00"]["status"] == "current_candidate"
        assert observations["$19.00"]["status"] == "superseded_role"
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("currency", "amount", "visible_price"),
    [
        ("USD", "1299.00", "$1,299.00"),
        ("EUR", "19.00", "19,00 €"),
        ("USD", "299", "$٢٩٩"),
    ],
)
def test_browser_extract_detail_reconciles_equivalent_schema_and_visible_prices(
    monkeypatch,
    currency: str,
    amount: str,
    visible_price: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Current product</h1>
      <meta itemprop="price" content="{amount}">
      <meta itemprop="priceCurrency" content="{currency}">
      <span class="price">{visible_price}</span>
      <p itemprop="description">Equivalent structured and visible price evidence.</p>
    </main>
    """
    session_id = f"equivalent-schema-visible-{currency}-{len(visible_price)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == f"{currency} {amount}"
        assert record["price_aliases"] == [visible_price]
        assert "price_candidates" not in record
        assert "price_conflict" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("language", "currency", "visible_price"),
    [
        ("en-US", "USD", "US$299"),
        ("en-US", "AUD", "AU$299"),
        ("zh-CN", "CNY", "299元"),
    ],
)
def test_browser_extract_detail_reconciles_runtime_localized_currency_alias(
    monkeypatch,
    language: str,
    currency: str,
    visible_price: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="{language}">
      <body>
        <main id="detail" itemscope itemtype="https://schema.org/Product">
          <h1 itemprop="name">Current product</h1>
          <meta itemprop="price" content="299">
          <meta itemprop="priceCurrency" content="{currency}">
          <span class="price">{visible_price}</span>
          <p itemprop="description">Equivalent runtime-localized price evidence.</p>
        </main>
      </body>
    </html>
    """
    session_id = f"localized-price-alias-{currency}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == f"{currency} 299"
        assert record["price_aliases"] == [visible_price]
        assert "price_candidates" not in record
        assert "price_conflict" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("language", "currency", "visible_price", "normalized_visible"),
    [
        ("zh-CN", "USD", "299元", "299元"),
        ("zh-CN", "EUR", "299元", "299元"),
        ("en-US", "USD", "299円", "299円"),
        ("sv-SE", "USD", "299 kr", "299 kr"),
        ("zh-CN", "CNY", "价格美元299", "美元299"),
    ],
)
def test_browser_extract_detail_rejects_localized_currency_conflicts(
    monkeypatch,
    language: str,
    currency: str,
    visible_price: str,
    normalized_visible: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="{language}"><body>
      <main id="detail" itemscope itemtype="https://schema.org/Product">
        <h1 itemprop="name">Current product</h1>
        <meta itemprop="price" content="299">
        <meta itemprop="priceCurrency" content="{currency}">
        <span class="price">{visible_price}</span>
      </main>
    </body></html>
    """
    session_id = f"localized-price-conflict-{language}-{currency}-{len(visible_price)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {f"{currency} 299", normalized_visible}
        assert record["price_conflict"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("language", "currency", "flat_price", "normalized_visible"),
    [
        ("zh-CN", "CNY", "价格人民币299", "人民币299"),
        ("en-US", "EUR", "Price €299", "€299"),
        ("en-US", "USD", "Price $299", "$299"),
        ("en-US", "USD", "Price US$299", "US$299"),
        ("es-PE", "PEN", "Price S/ 299", "S/ 299"),
        ("ru-RU", "RUB", "Цена руб. 299", "руб. 299"),
        ("ru-RU", "RUB", "Цена 299 руб.", "299 руб."),
        ("pl-PL", "PLN", "Cena 299 zł", "299 zł"),
    ],
)
def test_browser_extract_detail_reads_currency_marker_inside_flat_price_label(
    monkeypatch,
    language: str,
    currency: str,
    flat_price: str,
    normalized_visible: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="{language}"><body>
      <main id="detail" itemscope itemtype="https://schema.org/Product">
        <h1 itemprop="name">Current product</h1>
        <meta itemprop="price" content="299">
        <meta itemprop="priceCurrency" content="{currency}">
        <span class="price">{flat_price}</span>
      </main>
    </body></html>
    """
    session_id = f"flat-localized-price-{language}-{currency}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == f"{currency} 299"
        assert record["price_aliases"] == [normalized_visible]
        assert "price_conflict" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("price_markup", "expected_price", "expected_alias"),
    [
        (
            '<span class="product-price" data-price="299">Price $299</span>',
            "$299",
            "299",
        ),
        (
            '<meta itemprop="price" content="299">'
            '<meta itemprop="priceCurrency" content="USD">'
            '<span class="product-price">Price $299</span>',
            "USD 299",
            "$299",
        ),
    ],
)
def test_browser_extract_detail_uses_typed_evidence_for_generic_flat_price_alias(
    monkeypatch,
    price_markup: str,
    expected_price: str,
    expected_alias: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><html lang="en-US"><body>
      <main id="detail" itemscope itemtype="https://schema.org/Product">
        <h1 itemprop="name">Current product</h1>{price_markup}
      </main>
    </body></html>
    """
    session_id = f"generic-flat-price-alias-{len(price_markup)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == expected_price
        assert expected_alias in record["price_aliases"]
        assert "price_conflict" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_withholds_uncorroborated_generic_flat_price_label(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1><span class="product-price">Shipping $5</span>
    </main>
    """
    session_id = "uncorroborated-generic-flat-label"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert "uncorroborated_labeled_visual_price" in record["price_observations"][0]["reasons"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize("flat_price", ["Shipping $5", "Price $299"])
def test_browser_extract_detail_withholds_uncorroborated_labeled_flat_price(
    monkeypatch,
    flat_price: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><html lang="en-US"><body>
      <main id="detail"><h1>Current product</h1><span class="price">{flat_price}</span></main>
    </body></html>
    """
    session_id = f"uncorroborated-flat-label-{len(flat_price)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert record["price_observations"][0]["status"] == "excluded"
        assert "uncorroborated_labeled_visual_price" in record["price_observations"][0]["reasons"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("price_markup", "expected_price", "expected_alias"),
    [
        ('<span class="price">$19</span><span class="price">$19.00</span>', "$19", "$19.00"),
        ('<span class="price">$٢٩٩</span><span class="price">$299</span>', "$٢٩٩", "$299"),
        ('<span class="price" data-price="19">$19</span>', "$19", "19"),
        ('<span class="price" data-price="1299">$1,299.00</span>', "$1,299.00", "1299"),
    ],
)
def test_browser_extract_detail_merges_same_role_equivalent_prices(
    monkeypatch,
    price_markup: str,
    expected_price: str,
    expected_alias: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><main id="detail">
      <h1>Current product</h1>{price_markup}
    </main>
    """
    session_id = f"same-role-equivalent-{len(price_markup)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == expected_price
        assert expected_alias in record["price_aliases"]
        assert "price_conflict" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "price_markup",
    [
        '<span class="price">$19<sup>99</sup></span>',
        '<span class="price">$19<span>99</span></span>',
        '<span class="price">19<span>99</span>€</span>',
    ],
)
def test_browser_extract_detail_never_concatenates_split_cents_as_whole_price(
    monkeypatch,
    price_markup: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"<!doctype html><main id='detail'><h1>Current product</h1>{price_markup}</main>"
    session_id = f"split-cents-{len(price_markup)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        rendered_prices = [
            str(record.get("price") or ""),
            *[str(value) for value in record.get("price_candidates", [])],
        ]
        assert all("1999" not in value.replace(",", "") for value in rendered_prices)
        assert "price" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "flat_price",
    [
        "$19–$29",
        "From $19 to $29",
        "Was $29 now $19",
        "Save $5, price $19",
        "USD 19 (CAD 29)",
        "Was USD 29 now USD 19",
        "2 for $10",
    ],
)
def test_browser_extract_detail_withholds_multi_amount_flat_price_copy(
    monkeypatch,
    flat_price: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"<!doctype html><main id='detail'><h1>Current product</h1><span class='price'>{flat_price}</span></main>"
    session_id = f"multi-amount-flat-price-{len(flat_price)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert "price" not in result.output["record"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reconstructs_painted_accessibility_price_fragments(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="a-price">
        <span class="a-offscreen" style="position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)">$19.99</span>
        <span aria-hidden="true"><span>$</span><span>19</span><span>.</span><span>99</span></span>
      </span>
    </main>
    """
    session_id = "painted-accessibility-price-fragments"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert result.output["record"]["price"] == "$19.99"
        assert "price_conflict" not in result.output["record"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_accepts_painted_root_aria_hidden_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price" aria-hidden="true">$19.99</span>
    </main>
    """
    session_id = "painted-root-aria-hidden-price"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.99"]},
            )
        )

        assert detail.status == "completed"
        assert detail.output["record"]["price"] == "$19.99"
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "background_image",
    [
        "linear-gradient(red, blue)",
        "linear-gradient(red, blue),linear-gradient(green, yellow)",
    ],
)
def test_browser_gradient_painted_text_is_detail_and_assertion_evidence(
    monkeypatch,
    background_image: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price" style="
        display:inline-block;
        background-image:{background_image};
        background-clip:text;
        -webkit-background-clip:text;
        color:transparent;
        -webkit-text-fill-color:transparent
      ">$19.99</span>
    </main>
    """
    session_id = f"gradient-painted-price-{abs(hash(background_image))}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.99"]},
            )
        )

        assert detail.status == "completed"
        assert detail.output["record"]["price"] == "$19.99"
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_auto_sized_gradient_at_large_percentage_position_still_paints_text(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price" style="
        display:inline-block;
        background-image:linear-gradient(red,blue);
        background-size:auto;
        background-position:200% 200%;
        background-repeat:no-repeat;
        background-clip:text;
        -webkit-background-clip:text;
        color:transparent;
        -webkit-text-fill-color:transparent
      ">$19.99</span>
    </main>
    """
    session_id = "gradient-auto-size-large-percentage-position"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.99"]},
            )
        )

        assert detail.status == "completed"
        assert detail.output["record"]["price"] == "$19.99"
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_calc_sized_gradient_is_detail_and_assertion_evidence(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price" style="
        display:inline-block;
        background-image:linear-gradient(red,blue);
        background-size:calc(100% - 1px) calc(100% - 1px);
        background-position:0 0;
        background-repeat:no-repeat;
        background-clip:text;
        -webkit-background-clip:text;
        color:transparent;
        -webkit-text-fill-color:transparent
      ">$19.99</span>
    </main>
    """
    session_id = "gradient-calc-size"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.99"]},
            )
        )

        assert detail.status == "completed"
        assert detail.output["record"]["price"] == "$19.99"
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_nonzero_calc_sized_mask_preserves_detail_and_assertion_evidence(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price" style="
        display:inline-block;
        -webkit-mask-image:linear-gradient(black,black);
        -webkit-mask-size:calc(100% - 1px) calc(100% - 1px);
        -webkit-mask-position:0 0;
        -webkit-mask-repeat:no-repeat
      ">$19.99</span>
    </main>
    """
    session_id = "mask-calc-size-visible"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.99"]},
            )
        )

        assert detail.status == "completed"
        assert detail.output["record"]["price"] == "$19.99"
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "concealment_style",
    [
        "color:rgb(0 0 0 / 0)",
        "-webkit-text-fill-color:transparent",
        (
            "display:inline-block;"
            "background-image:linear-gradient(transparent,transparent);"
            "background-clip:text;-webkit-background-clip:text;"
            "color:transparent;-webkit-text-fill-color:transparent"
        ),
        (
            "display:inline-block;"
            "background-image:linear-gradient(red,blue);"
            "background-size:0 0;background-repeat:no-repeat;"
            "background-clip:text;-webkit-background-clip:text;"
            "color:transparent;-webkit-text-fill-color:transparent"
        ),
        (
            "display:inline-block;"
            "background-image:linear-gradient(red,blue),"
            "linear-gradient(transparent,transparent);"
            "background-size:0 0,auto;"
            "background-clip:text;-webkit-background-clip:text;"
            "color:transparent;-webkit-text-fill-color:transparent"
        ),
        (
            "display:inline-block;"
            "background-image:linear-gradient(red,blue);"
            "background-position:10000px 10000px;background-repeat:no-repeat;"
            "background-clip:text;-webkit-background-clip:text;"
            "color:transparent;-webkit-text-fill-color:transparent"
        ),
        (
            "display:inline-block;"
            "background-image:linear-gradient(red,blue);"
            "background-position:right 0 bottom 10000px;background-repeat:no-repeat;"
            "background-clip:text;-webkit-background-clip:text;"
            "color:transparent;-webkit-text-fill-color:transparent"
        ),
        (
            "display:inline-block;"
            "background-image:linear-gradient(red,blue);"
            "background-size:10px 10px;"
            "background-position:0 10000px;background-repeat:repeat-x;"
            "background-clip:text;-webkit-background-clip:text;"
            "color:transparent;-webkit-text-fill-color:transparent"
        ),
        (
            "display:inline-block;"
            "background-image:linear-gradient(red,blue);"
            "background-size:calc(100% - 1000px) auto;background-repeat:no-repeat;"
            "background-clip:text;-webkit-background-clip:text;"
            "color:transparent;-webkit-text-fill-color:transparent"
        ),
        "filter:opacity(0)",
        "display:inline-block;transform:scale(0)",
        "display:inline-block;clip-path:inset(50%)",
        "display:inline-block;clip-path:path('M 0 0')",
        (
            "display:inline-block;"
            "-webkit-mask-image:linear-gradient(transparent,transparent)"
        ),
        (
            "display:inline-block;"
            "-webkit-mask-image:linear-gradient(black,black);"
            "-webkit-mask-size:0 0"
        ),
        (
            "display:inline-block;"
            "-webkit-mask-image:linear-gradient(black,black);"
            "-webkit-mask-size:10px 10px;"
            "-webkit-mask-position:0 10000px;"
            "-webkit-mask-repeat:repeat-x"
        ),
        (
            "position:absolute;width:1px;height:1px;margin:-1px;"
            "overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap"
        ),
        "position:absolute;left:-9999px",
    ],
)
def test_browser_price_evidence_ignores_nonpainted_descendant_text(
    monkeypatch,
    concealment_style: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price">$19.00</span>
      <span class="price" style="display:inline-block;width:120px;height:24px">
        <span style="{concealment_style}">$999.00</span>
      </span>
    </main>
    """
    session_id = f"nonpainted-price-{abs(hash(concealment_style))}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        hidden_assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$999.00"]},
            )
        )
        visible_assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.00"]},
            )
        )

        assert detail.status == "completed"
        record = detail.output["record"]
        assert record["price"] == "$19.00"
        assert all(
            "999" not in str(value)
            for value in [record.get("price"), *record.get("price_candidates", [])]
        )
        assert hidden_assertion.output["verified"] is False
        assert visible_assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_price_evidence_keeps_scroll_reachable_below_fold_content(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <div style="height:2500px"></div>
      <span class="price">$19.00</span>
    </main>
    """
    session_id = "below-fold-painted-price"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.00"]},
            )
        )

        assert detail.output["record"]["price"] == "$19.00"
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reports_same_amount_cross_currency_conflict(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Current product</h1>
      <meta itemprop="price" content="19.00">
      <meta itemprop="priceCurrency" content="EUR">
      <span class="price">$19.00</span>
      <p itemprop="description">The currency markers conflict.</p>
    </main>
    """
    session_id = "schema-visible-currency-conflict"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"EUR 19.00", "$19.00"}
        assert record["price_conflict"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_rejects_ambiguous_three_digit_separator_equivalence(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Current product</h1>
      <meta itemprop="price" content="19.999">
      <meta itemprop="priceCurrency" content="KWD">
      <span class="price">KWD 19,999</span>
      <p itemprop="description">A three-digit separator is ambiguous without locale proof.</p>
    </main>
    """
    session_id = "ambiguous-three-digit-price-separator"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"KWD 19.999", "KWD 19,999"}
        assert record["price_conflict"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_rejects_mutually_incompatible_currency_aliases(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Current product</h1>
      <meta itemprop="price" content="19.00">
      <span class="price">$19.00</span>
      <span class="price">€19.00</span>
      <p itemprop="description">Currency aliases must agree with each other.</p>
    </main>
    """
    session_id = "incompatible-currency-aliases"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"19.00", "$19.00", "€19.00"}
        assert record["price_conflict"] is True
        assert "price_aliases" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reconciles_native_sar_currency_marker(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html lang="ar-SA">
      <body>
        <main id="detail" itemscope itemtype="https://schema.org/Product">
          <h1 itemprop="name">Current product</h1>
          <meta itemprop="price" content="299">
          <meta itemprop="priceCurrency" content="SAR">
          <span class="price">ر.س ٢٩٩</span>
          <p itemprop="description">Equivalent native Saudi Riyal price evidence.</p>
        </main>
      </body>
    </html>
    """
    session_id = "native-sar-currency-marker"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "SAR 299"
        assert record["price_aliases"] == ["ر.س ٢٩٩"]
        assert "price_candidates" not in record
        assert "price_conflict" not in record
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reports_schema_conflict_through_deep_wrappers(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    wrappers = "<div>" * 7
    closers = "</div>" * 7
    html = f"""
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Current product</h1>
      <meta itemprop="price" content="99.00">
      <meta itemprop="priceCurrency" content="USD">
      {wrappers}<span class="price">$19.00</span>{closers}
      <p itemprop="description">Styling wrappers do not create a separate record.</p>
    </main>
    """
    session_id = "deep-wrapper-schema-conflict"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"USD 99.00", "$19.00"}
        assert record["price_conflict"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_does_not_borrow_nested_schema_currency(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Structured product</h1>
      <meta itemprop="price" content="19.00">
      <meta itemprop="priceCurrency" content="USD">
      <div itemprop="offers" itemscope itemtype="https://schema.org/Offer">
        <meta itemprop="price" content="29.00">
        <meta itemprop="priceCurrency" content="EUR">
      </div>
      <p itemprop="description">Each amount keeps the currency from its own schema scope.</p>
    </main>
    """
    session_id = "nested-schema-currency-owner"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        observations = {item["value"]: item for item in result.output["record"]["price_observations"]}
        assert "USD 19.00" in observations
        assert "EUR 19.00" not in observations
        assert "EUR 29.00" in observations
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize("tag", ["span", "input"])
def test_browser_extract_detail_prefers_typed_data_price_over_generic_value(
    monkeypatch,
    tag: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    closing = "</span>" if tag == "span" else ""
    text = "$19.99" if tag == "span" else ""
    html = (
        "<!doctype html><main id='detail'><h1>Current record</h1>"
        f"<{tag} data-price='$19.99' value='1'>{text}{closing}"
        "<p itemprop='description'>The typed price attribute is authoritative.</p></main>"
    )
    session_id = f"typed-data-price-{tag}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert result.output["record"]["price"] == "$19.99"
        assert "1" not in {
            item["value"] for item in result.output["record"]["price_observations"]
        }
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "price_markup",
    [
        '<span class="price" aria-label="$99.00">$19.00</span>',
        '<span class="price" data-price="$99.00">$19.00</span>',
    ],
)
def test_browser_extract_detail_reports_attribute_rendered_price_conflict(
    monkeypatch,
    price_markup: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = (
        "<!doctype html><main id='detail'><h1>Current record</h1>"
        f"{price_markup}"
        "<p itemprop='description'>Conflicting current-page price evidence.</p></main>"
    )
    session_id = f"price-attribute-conflict-{len(price_markup)}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"$19.00", "$99.00"}
        assert record["price_conflict"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reports_schema_rendered_price_conflict(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail" itemscope itemtype="https://schema.org/Product">
      <h1 itemprop="name">Current record</h1>
      <meta itemprop="price" content="99.00">
      <meta itemprop="priceCurrency" content="USD">
      <span class="price">$19.00</span>
      <p itemprop="description">Conflicting schema and visible amounts.</p>
    </main>
    """
    session_id = "schema-rendered-price-conflict"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert "price" not in record
        assert set(record["price_candidates"]) == {"USD 99.00", "$19.00"}
        assert record["price_conflict"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("price", "language"),
    [
        ("฿299", "th-TH"),
        ("₹२९९", "hi-IN"),
        ("ر.س ٢٩٩", "ar-SA"),
        ("299円", "ja-JP"),
        ("299元", "zh-CN"),
        ("299원", "ko-KR"),
        ("299 руб.", "ru-RU"),
    ],
)
def test_browser_extract_detail_accepts_unicode_currency_and_decimal_digits(
    monkeypatch,
    price: str,
    language: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="{language}">
      <head><meta charset="utf-8"></head>
      <body>
        <main id="detail">
          <h1>Unicode-priced record</h1>
          <span class="price">{price}</span>
          <p itemprop="description">A current price rendered with locale-native notation.</p>
        </main>
      </body>
    </html>
    """
    session_id = f"unicode-price-{ord(price[0])}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert result.output["record"]["price"] == price
        assert result.output["record"]["price_observations"][0]["status"] == "current_candidate"
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "non_price",
    [
        "Save up to 299 points",
        "Only 2 left",
        "4.5 out of 5 stars",
        "Save $5 today",
        "SKU 123",
        "ABC 123",
        "4,5 stars",
        "Version 2.0",
        "Size 2.5 oz",
        "e.g. 2",
        "2年",
        "2月",
        "2個",
    ],
)
def test_browser_extract_detail_rejects_integer_marketing_copy_as_price(
    monkeypatch,
    non_price: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <main id="detail">
      <h1>Current record</h1>
      <span class="price">{non_price}</span>
      <p itemprop="description">A substantive record without a verified current price.</p>
    </main>
    """
    session_id = f"non-price-{len(non_price)}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert "price" not in result.output["record"]
        assert "price_candidates" not in result.output["record"]
        assert "price_observations" not in result.output["record"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_rejects_bare_rating_from_weak_price_class(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current record</h1>
      <span class="price-rating">4.5</span>
      <p itemprop="description">The weak class identifies a rating, not a current price.</p>
    </main>
    """
    session_id = "weak-price-rating"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert "price" not in result.output["record"]
        assert "price_candidates" not in result.output["record"]
        assert "price_observations" not in result.output["record"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize("non_price", ["нов 2", "рос 2", "япо 2", "авс 2"])
def test_browser_extract_detail_rejects_prefixes_of_localized_currency_names(
    monkeypatch,
    non_price: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="ru-RU">
      <head><meta charset="utf-8"></head>
      <body>
        <main id="detail">
          <h1>Current record</h1>
          <span class="price">{non_price}</span>
          <p itemprop="description">This is not a locale currency marker.</p>
        </main>
      </body>
    </html>
    """
    session_id = f"locale-prefix-non-price-{len(non_price)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert "price" not in result.output["record"]
        assert "price_candidates" not in result.output["record"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize("non_price", ["2시", "2권", "2도"])
def test_browser_extract_detail_rejects_korean_non_currency_counters(
    monkeypatch,
    non_price: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="ko-KR">
      <head><meta charset="utf-8"></head>
      <body><main id="detail">
        <h1>Current record</h1>
        <span class="price">{non_price}</span>
        <p itemprop="description">This counter is not a currency.</p>
      </main></body>
    </html>
    """
    session_id = f"korean-non-currency-{ord(non_price[-1])}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert "price" not in result.output["record"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_bounds_high_cardinality_price_candidates(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    def extract(count: int, session_id: str) -> float:
        nodes = "".join(
            f'<span class="price">маркер{chr(0x4E00 + i)} 123</span>'
            for i in range(count)
        )
        html = (
            "<!doctype html><html lang='ru-RU'><head><meta charset='utf-8'></head>"
            f"<body><main id='detail'><h1>Current record</h1>{nodes}"
            "<p itemprop='description'>No current price is present.</p></main></body></html>"
        )
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        started = time.perf_counter()
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        elapsed = time.perf_counter() - started
        assert result.status == "completed"
        assert "price" not in result.output["record"]
        return elapsed

    try:
        baseline = extract(40, "price-cardinality-40")
        high_cardinality = extract(500, "price-cardinality-500")
        assert high_cardinality < baseline * 4 + 0.75
    finally:
        _run(backend.close_session("price-cardinality-40"))
        _run(backend.close_session("price-cardinality-500"))
        _run(backend.shutdown())


def test_browser_extract_detail_withholds_ambiguous_split_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current record</h1>
      <span class="price">
        <span class="currency">$</span><span class="whole">19</span><sup class="fraction">99</sup>
      </span>
      <p itemprop="description">The DOM omits an explicit decimal separator.</p>
    </main>
    """
    session_id = "ambiguous-split-price"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        assert "price" not in result.output["record"]
        assert "price_candidates" not in result.output["record"]
        assert "price_observations" not in result.output["record"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_ignores_hidden_stale_availability(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price">$19.99</span>
      <div class="availability-old" style="display:none">Out of Stock</div>
      <div class="availability-current">In Stock</div>
      <p itemprop="description">3% sulfur cleanser</p>
    </main>
    """
    session_id = "hidden-stale-availability"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "selector": "#detail",
                    "required": ["In Stock"],
                    "forbidden": ["Out of Stock"],
                },
            )
        )

        assert detail.status == "completed"
        assert detail.output["record"]["availability"] == "In Stock"
        assert "Out of Stock" not in detail.output["record"]["details"]
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_prefers_visible_availability_over_stale_metadata(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <meta itemprop="availability" content="Out of Stock">
      <span class="price">$19.99</span>
      <div class="availability-current">In Stock</div>
      <p itemprop="description">3% sulfur cleanser</p>
    </main>
    """
    session_id = "stale-metadata-availability"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "selector": "#detail",
                    "required": ["In Stock"],
                    "forbidden": ["Out of Stock"],
                },
            )
        )

        assert detail.status == "completed"
        assert detail.output["record"]["availability"] == "In Stock"
        assert "Out of Stock" not in detail.output["record"]["details"]
        assert assertion.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_augments_auto_scope_with_sibling_buy_box_availability(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="product-page">
      <section id="primary-detail">
        <h1>Current cleanser</h1>
        <span class="price">$19.99</span>
        <p itemprop="description">3% sulfur cleanser</p>
      </section>
      <article itemscope>
        <h2 itemprop="name"><a href="/related">Related cleanser</a></h2>
        <span class="price">$7.99</span>
        <div class="availability">Out of Stock</div>
      </article>
      <div>
        <h2><a href="/plain-related">Plain related cleanser</a></h2>
        <span itemprop="price">$8.99</span>
        <div itemprop="availability">Preorder</div>
      </div>
      <aside id="buy-box">
        <div class="availability">In Stock</div>
      </aside>
    </main>
    """
    session_id = "sibling-buy-box-availability"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id})
        )

        assert detail.status == "completed"
        record = detail.output["record"]
        assert record["title"] == "Current cleanser"
        assert record["price"] == "$19.99"
        assert record["availability"] == "In Stock"
        assert "Related cleanser" not in record["details"]
        assert "Plain related cleanser" not in record["details"]
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reads_structured_link_availability(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <link itemprop="availability" href="https://schema.org/InStock">
      <span class="price">$19.99</span>
      <p itemprop="description">3% sulfur cleanser</p>
    </main>
    """
    session_id = "structured-link-availability"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert detail.status == "completed"
        assert detail.output["record"]["availability"] == "In Stock"
        assert (
            detail.output["record"]["availability_source_url"]
            == "https://schema.org/InStock"
        )
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_normalizes_structured_meta_availability_uri(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <meta itemprop="availability" content="https://schema.org/PreOrder">
      <span class="price">$19.99</span>
      <p itemprop="description">3% sulfur cleanser</p>
    </main>
    """
    session_id = "structured-meta-availability-uri"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert detail.status == "completed"
        assert detail.output["record"]["availability"] == "Pre Order"
        assert (
            detail.output["record"]["availability_source_url"]
            == "https://schema.org/PreOrder"
        )
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "current_attribute",
    [
        'aria-current="true"',
        'aria-selected="true"',
        "data-current",
        "data-selected",
    ],
)
def test_browser_extract_detail_prefers_structured_current_availability(
    monkeypatch,
    current_attribute: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price">$19.99</span>
      <div class="availability-old">Out of Stock</div>
      <div class="availability-new" {current_attribute}>In Stock</div>
      <p itemprop="description">3% sulfur cleanser</p>
    </main>
    """
    session_id = f"structured-current-availability-{abs(hash(current_attribute))}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert detail.status == "completed"
        assert detail.output["record"]["availability"] == "In Stock"
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("availability_rows", "expected"),
    [
        (
            """
            <div aria-selected="false"><span class="availability">Explicitly unselected</span></div>
            <div><span class="availability">Unknown selection</span></div>
            <div aria-selected="true"><span class="availability">Selected availability</span></div>
            """,
            "Selected availability",
        ),
        (
            """
            <div aria-selected="false"><span class="availability">Explicitly unselected</span></div>
            <div><span class="availability">Unknown selection</span></div>
            """,
            "Unknown selection",
        ),
        (
            """
            <div><span class="availability">Unknown selection</span></div>
            <div aria-current="page"><span class="availability">Current availability</span></div>
            """,
            "Current availability",
        ),
    ],
)
def test_browser_extract_detail_inherits_structured_availability_state_from_ancestor(
    monkeypatch,
    availability_rows: str,
    expected: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><main id="detail">
      <h1>Current product</h1>
      <span class="price">$19.99</span>
      {availability_rows}
      <p itemprop="description">3% sulfur cleanser</p>
    </main>
    """
    session_id = f"inherited-availability-state-{abs(hash(expected))}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert detail.status == "completed"
        assert detail.output["record"]["availability"] == expected
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_uses_accessible_price_and_suppresses_split_rendering(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current record</h1>
      <span class="price" aria-label="$19.99">
        <span>$</span><span>19</span><sup>99</sup>
      </span>
      <p itemprop="description">The accessible value carries the decimal separator.</p>
    </main>
    """
    session_id = "accessible-split-price"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$19.99"
        assert "price_candidates" not in record
        assert "$1999" not in {
            item["value"] for item in record["price_observations"]
        }
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    "nonpainted_markup",
    [
        "",
        '<span style="display:none">$19.99</span>',
    ],
)
def test_browser_extract_detail_withholds_accessibility_only_price(
    monkeypatch,
    nonpainted_markup: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html><main id="detail">
      <h1>Current record</h1>
      <span class="price" aria-label="$19.99"
            style="display:inline-block;width:80px;height:20px">{nonpainted_markup}</span>
    </main>
    """
    session_id = f"accessibility-only-price-{len(nonpainted_markup)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        detail = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "selector": "#detail", "required": ["$19.99"]},
            )
        )

        assert detail.status == "completed"
        assert "price" not in detail.output["record"]
        assert "price_candidates" not in detail.output["record"]
        assert assertion.output["verified"] is False
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_excludes_opacity_zero_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current record</h1>
      <span class="price" style="opacity: 0">$99.00</span>
      <span class="price">$19.00</span>
      <p itemprop="description">Only the visible price is current.</p>
    </main>
    """
    session_id = "opacity-zero-price"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$19.00"
        assert "$99.00" not in {
            item["value"] for item in record["price_observations"]
        }
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_excludes_content_visibility_hidden_price(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <h1>Current record</h1>
      <span class="price" style="content-visibility: hidden; width: 100px; height: 20px">$99.00</span>
      <span class="price">$19.00</span>
      <p itemprop="description">Only the rendered price is current.</p>
    </main>
    """
    session_id = "content-visibility-hidden-price"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id, "selector": "#detail"})
        )

        assert result.status == "completed"
        record = result.output["record"]
        assert record["price"] == "$19.00"
        assert "$99.00" not in {
            item["value"] for item in record["price_observations"]
        }
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_reuses_remembered_assertion_selector(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main>
      <section id="target-detail">
        <h1>Target record</h1>
        <span class="price">$19.00</span>
        <p itemprop="description">Exact assertion scope.</p>
      </section>
      <article>
        <h2><a href="/other">Other record</a></h2>
        <span class="price">$99.00</span>
      </article>
    </main>
    """
    session_id = "remembered-assertion-selector"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["Target record", "$19.00"],
                    "selector": "#target-detail",
                },
            )
        )
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id})
        )

        assert assertion.status == "completed"
        assert assertion.output["verified"] is True
        assert result.status == "completed"
        assert result.output["selector"] == "#target-detail"
        assert result.output["record"]["price"] == "$19.00"
        assert "Other record" not in result.output["record"]["details"]
        embedded_assertion = result.output["page_state_assertion"]
        assert embedded_assertion["verified"] is True
        assert embedded_assertion["contract"]["selector"] == "#target-detail"
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_extract_detail_keeps_safe_narrowing_for_remembered_body_selector(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <body>
      <main>
        <div>
          <h1>Target record</h1>
          <span class="price">$19.00</span>
          <p itemprop="description">Exact primary detail.</p>
        </div>
        <article>
          <h2><a href="/other">Other record</a></h2>
          <span class="price">$99.00</span>
        </article>
      </main>
    </body>
    """
    session_id = "remembered-body-selector"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        assertion = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["Target record", "$19.00"],
                    "selector": "body",
                },
            )
        )
        result = tools.browser_extract_detail(
            inv("browser_extract_detail", {"session_id": session_id})
        )

        assert assertion.status == "completed"
        assert assertion.output["verified"] is True
        assert result.status == "completed"
        assert result.output["selector"] == "body"
        assert result.output["record"]["price"] == "$19.00"
        assert "Other record" not in result.output["record"]["details"]
        assert result.output["page_state_assertion"]["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_reports_missing_scope_as_typed_failure(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    try:
        _run(backend.navigate("assert-missing-scope", "data:text/html," + quote("<main>Visible result</main>")))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": "assert-missing-scope",
                    "required": ["Visible result"],
                    "selector": "#missing-result-region",
                },
            )
        )

        assert result.status == "failed"
        assert result.output["result"]["reason"] == "scope_not_found"
        assert result.output["selector"] == "#missing-result-region"
    finally:
        _run(backend.close_session("assert-missing-scope"))
        _run(backend.shutdown())


def test_browser_assert_page_state_does_not_match_dates_by_shared_year(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <main>
          <section>Pick-Up Date 29 Jun 2026</section>
          <section>Return Date 15 Jul 2026</section>
          <section>Pick-Up Time 12:00 PM</section>
          <span>2026</span>
        </main>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("assert-date-year-match", "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": "assert-date-year-match",
                    "required": ["06/29/2026", "07/15/2026", "12:00 PM"],
                    "forbidden": ["06/13/2026", "06/14/2026", "Error"],
                },
            )
        )
        assert result.status == "completed"
        assert result.output["verified"] is True
        matches = {item["expected"]: item["match"] for item in result.output["result"]["required"]}
        assert matches["06/29/2026"] == "Pick-Up Date 29 Jun 2026"
        assert matches["07/15/2026"] == "Return Date 15 Jul 2026"
        assert result.output["result"]["forbidden_found"] == []
    finally:
        _run(backend.close_session("assert-date-year-match"))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("required", "visible_text"),
    [
        ("2026/07/05", "2026年7月5日"),
        ("2026年7月5日", "2026/07/05"),
        ("13 Jul 2026", "2026/07/13"),
        ("5. Juli 2026", "2026-07-05"),
        ("2026-07-05", "5. Juli 2026"),
    ],
)
def test_browser_assert_page_state_matches_structurally_equivalent_dates(
    monkeypatch,
    required: str,
    visible_text: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"<!doctype html><main id='detail'>{visible_text}</main>"
    session_id = f"assert-equivalent-date-{len(required)}-{len(visible_text)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {"session_id": session_id, "required": [required], "selector": "#detail"},
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_matches_unicode_text_inside_sentence(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="ja-JP">
      <head><meta charset="utf-8"></head>
      <body><section id="detail">この商品価格は299円です</section></body>
    </html>
    """
    session_id = "assert-unicode-sentence"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["商品価格", "299円"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is True
        assert result.output["result"]["missing"] == []
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("visible_text", "verified"),
    [
        ("価格299円在庫あり", True),
        ("価格1299円在庫あり", False),
        ("価格2990円在庫あり", False),
    ],
)
def test_browser_assert_page_state_matches_adjacent_cjk_currency_text(
    monkeypatch,
    visible_text: str,
    verified: bool,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="ja-JP">
      <head><meta charset="utf-8"></head>
      <body><section id="detail">{visible_text}</section></body>
    </html>
    """
    session_id = "assert-adjacent-cjk-currency"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["299円"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is verified
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize("visible_text", ["7 Jun 2026", "6 Jul 2026"])
def test_browser_assert_page_state_rejects_ambiguous_numeric_date_cross_format(
    monkeypatch,
    visible_text: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="en-GB"><body><main id="detail">{visible_text}</main></body></html>
    """
    session_id = f"assert-ambiguous-date-{len(visible_text)}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["06/07/2026"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is False
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_rejects_ambiguous_candidate_date_token_fallback(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html lang="en-GB"><body><main id="detail">Appointment 04/03/2026</main></body></html>
    """
    session_id = "assert-ambiguous-candidate-date"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["2026-03-04"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is False
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_checks_every_candidate_date_before_token_fallback(
    monkeypatch,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html><main id="detail">
      Archive 13/02/2026. Appointment 04/03/2026
    </main>
    """
    session_id = "assert-every-ambiguous-candidate-date"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["2026-03-04"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is False
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize("visible_text", ["Jun 29, 2026", "June 29, 2026"])
def test_browser_assert_page_state_matches_unambiguous_comma_date(
    monkeypatch,
    visible_text: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"<!doctype html><main id='detail'>{visible_text}</main>"
    session_id = f"assert-comma-date-{len(visible_text)}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["06/29/2026"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("required", "visible_text"),
    [
        ("02/29/2025", "Mar 1 2025"),
        ("04/31/2026", "May 1 2026"),
    ],
)
def test_browser_assert_page_state_rejects_impossible_date_rollover(
    monkeypatch,
    required: str,
    visible_text: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"<!doctype html><main id='detail'>{visible_text}</main>"
    session_id = f"assert-impossible-date-{len(required)}-{len(visible_text)}"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": [required],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is False
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("language", "required", "visible_text", "verified"),
    [
        ("en-US", "USD 299", "Price US$299", True),
        ("en-US", "AUD 299", "Price AU$299", True),
        ("en-US", "CAD 299", "Price CA$299", True),
        ("zh-CN", "CNY 299", "价格299元", True),
        ("en-US", "EUR 299", "Price US$299", False),
        ("en-US", "USD 299", "Price AU$299", False),
        ("zh-CN", "EUR 299", "价格299元", False),
    ],
)
def test_browser_assert_page_state_resolves_runtime_currency_aliases(
    monkeypatch,
    language: str,
    required: str,
    visible_text: str,
    verified: bool,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"""
    <!doctype html>
    <html lang="{language}">
      <body><main id="detail">{visible_text}</main></body>
    </html>
    """
    session_id = f"assert-runtime-currency-{language}-{len(required)}-{len(visible_text)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": [required],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is verified
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("required", "visible_text"),
    [
        ("bar", "barcode"),
        ("bar", "foobar"),
        ("bar code", "foobar code shown"),
        ("2", "20"),
        ("2 items", "12 items"),
        ("20 ml", "120 ml"),
        ("é", "café"),
        ("é item", "café item"),
        ("вар", "товар"),
        ("бар код", "амбар код"),
        ("Blue Lamp", "B Lamp"),
        ("B Lamp $19.99", "Blue Lamp $19.99"),
        ("B Lamp 06/29/2026", "Blue Lamp on 06/29/2026"),
        ("$19.00", "€19.00"),
        ("5%", "Rated 5 stars"),
        ("2%", "Size 2 oz"),
    ],
)
def test_browser_assert_page_state_rejects_boundary_and_semantic_marker_mismatches(
    monkeypatch,
    required: str,
    visible_text: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"<!doctype html><main id='detail'>{visible_text}</main>"
    session_id = f"assert-semantic-mismatch-{len(required)}-{len(visible_text)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": [required],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is False
        assert result.output["result"]["missing"][0]["expected"] == required
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


@pytest.mark.parametrize(
    ("required", "visible_text"),
    [
        ("A12", "Model A 12 is selected"),
        ("5% sulfur", "Contains ٥٪ sulfur."),
        ("$299", "Price $٢٩٩."),
        ("USD 19", "usd 19"),
        ("19 USD", "19 usd"),
    ],
)
def test_browser_assert_page_state_matches_structurally_equivalent_unicode_tokens(
    monkeypatch,
    required: str,
    visible_text: str,
) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = f"<!doctype html><main id='detail'>{visible_text}</main>"
    session_id = f"assert-unicode-token-equivalence-{len(required)}-{len(visible_text)}"
    try:
        _run(backend.navigate(session_id, "data:text/html;charset=utf-8," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": [required],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_accepts_visually_painted_aria_hidden_text(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <span aria-hidden="true">Selected $19.00</span>
      <span>Other visible state</span>
    </main>
    """
    session_id = "assert-aria-hidden"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["Selected $19.00"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is True
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_excludes_opacity_zero_text(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <span style="opacity: 0">Secret 42</span>
      <span>Visible state</span>
    </main>
    """
    session_id = "assert-opacity-zero"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["Secret 42"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is False
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_excludes_content_visibility_hidden_text(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <main id="detail">
      <span style="content-visibility: hidden; width: 100px; height: 20px">Secret 42</span>
      <span>Visible state</span>
    </main>
    """
    session_id = "assert-content-visibility-hidden"
    try:
        _run(backend.navigate(session_id, "data:text/html," + quote(html)))
        result = tools.browser_assert_page_state(
            inv(
                "browser_assert_page_state",
                {
                    "session_id": session_id,
                    "required": ["Secret 42"],
                    "selector": "#detail",
                },
            )
        )

        assert result.status == "completed"
        assert result.output["verified"] is False
    finally:
        _run(backend.close_session(session_id))
        _run(backend.shutdown())


def test_browser_assert_page_state_script_disallows_short_reverse_substring_match() -> None:
    script = _browser_assert_page_state_script(
        required=["06/29/2026"],
        forbidden=["06/13/2026", "Error"],
    )

    assert "expectedNorm.includes(candidateNorm)" not in script
    assert "monthNumbers" not in script
    assert "dateSignatures" in script
    assert "hasAdjacencyScript" in script
    assert ".slice(0, 80)" not in script


def test_browser_combobox_tool_retries_expected_text_and_rejects_partial_place_name(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <label for="place">Dropoff location</label>
        <input id="place" aria-label="Dropoff location" role="combobox" aria-controls="place-options" />
        <div id="place-options" role="listbox"></div>
        <script>
          const place = document.getElementById('place');
          const options = document.getElementById('place-options');
          place.addEventListener('input', () => {
            if (place.value === 'Knockdown Center') {
              options.innerHTML = '<div role="option" onclick="place.value=\\'Knockdown Center\\'">Knockdown Center</div>';
            } else {
              options.innerHTML = '<div role="option" onclick="place.value=\\'Center\\'">Center</div>';
            }
          });
        </script>
      </body>
    </html>
    """
    try:
        _run(backend.navigate("place-retry", "data:text/html," + quote(html)))
        snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "place-retry"}))
        place_id = snapshot.output["snapshot"]["elements"][0]["element_id"]
        selected = tools.browser_select_combobox(
            inv(
                "browser_select_combobox",
                {
                    "session_id": "place-retry",
                    "element_id": place_id,
                    "query": "Knockdown Center Queens NY",
                    "expected_text": "Knockdown Center",
                },
            )
        )
        assert selected.status == "completed"
        assert selected.output["selected"] == "Knockdown Center"

        _run(backend.navigate("place-retry", "data:text/html," + quote(html)))
        snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "place-retry"}))
        place_id = snapshot.output["snapshot"]["elements"][0]["element_id"]
        partial = tools.browser_select_combobox(
            inv(
                "browser_select_combobox",
                {
                    "session_id": "place-retry",
                    "element_id": place_id,
                    "query": "Center",
                    "expected_text": "Knockdown Center",
                },
            )
        )
        assert partial.status == "completed"
        assert partial.output["selected"] == "Knockdown Center"
    finally:
        _run(backend.close_session("place-retry"))
        _run(backend.shutdown())


def test_browser_snapshot_includes_named_hidden_editable_controls(monkeypatch) -> None:
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    html = """
    <!doctype html>
    <html>
      <body>
        <input aria-label="Where" style="visibility:hidden; display:block; width:160px; height:20px" />
      </body>
    </html>
    """
    try:
        _run(backend.navigate("hidden-editable", "data:text/html," + quote(html)))
        snapshot = tools.browser_snapshot(inv("browser_snapshot", {"session_id": "hidden-editable"}))
        elements = snapshot.output["snapshot"]["elements"]

        where = next(element for element in elements if element.get("label") == "Where")
        assert where["editable"] is True
        assert where["visible"] is False
    finally:
        _run(backend.close_session("hidden-editable"))
        _run(backend.shutdown())


@pytest.mark.asyncio
async def test_playwright_fallback_failures_preserve_original_browser_error() -> None:
    class Page:
        async def click(self, selector, **kwargs):
            raise TimeoutError("Page.click: Timeout 10000ms exceeded")

        async def fill(self, selector, text, **kwargs):
            raise TimeoutError("Page.fill: Timeout 30000ms exceeded")

        async def evaluate(self, script):
            raise RuntimeError("dom fallback failed")

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]

    with pytest.raises(TimeoutError, match="Page.click"):
        await backend.click("s", "input[type='text']:first-of-type")
    with pytest.raises(TimeoutError, match="Page.fill"):
        await backend.type_text("s", "input[aria-label='Where from?']", "116 West 87th St")


@pytest.mark.asyncio
async def test_raw_cdp_locator_supports_click_type_and_value_fallback_methods() -> None:
    class Client:
        def __init__(self) -> None:
            self.expressions: list[str] = []

        async def send(self, name, payload=None, timeout=None):  # noqa: ANN001
            if name != "Runtime.evaluate":
                return {}
            expression = str((payload or {}).get("expression") or "")
            self.expressions.append(expression)
            value: object = True
            if "innerText" in expression:
                value = "Visible text"
            elif "'value' in el" in expression:
                value = "typed value"
            elif "getAttribute" in expression:
                value = "field title"
            elif "(el, text)" in expression:
                value = {"ok": True}
            return {"result": {"value": value}}

    page = cdp_backend._RawCDPPage({"webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/1"})
    page._client = Client()
    locator = page.locator("#field").first

    await locator.click()
    await locator.dispatch_event("click")
    await locator.fill("typed value")
    assert await locator.inner_text() == "Visible text"
    assert await locator.input_value() == "typed value"
    assert await locator.get_attribute("title") == "field title"
    assert await locator.is_visible() is True
    assert await locator.is_enabled() is True
    assert await locator.evaluate("(el, text) => ({ok: !!el, text})", "typed value") == {"ok": True}
    assert any("#field" in expression for expression in page._client.expressions)


@pytest.mark.asyncio
async def test_raw_cdp_dedicated_session_cleanup_closes_the_browser_target(monkeypatch) -> None:
    target = {
        "id": "task-target-1",
        "type": "page",
        "url": "about:blank",
        "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/task-target-1",
    }
    closed_targets: list[tuple[str, str]] = []

    async def new_target(_url: str) -> dict[str, object]:
        return target

    async def close_target(url: str, target_id: str) -> bool:
        closed_targets.append((url, target_id))
        return True

    monkeypatch.setattr(cdp_backend, "_async_new_cdp_target", new_target)
    monkeypatch.setattr(cdp_backend, "_async_close_cdp_target", close_target)

    context = cdp_backend._RawCDPContext("http://127.0.0.1:9222", [])
    browser = cdp_backend._RawCDPBrowser("http://127.0.0.1:9222")
    browser.contexts = [context]
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._cdp_url = "http://127.0.0.1:9222"
    backend._browser = browser
    backend._playwright = None
    backend._pages = {}
    backend._lock = asyncio.Lock()

    page = await cdp_backend.CDPBackend._get_page(backend, "task-session")
    assert page.target_id == "task-target-1"

    await cdp_backend.CDPBackend.close_session(backend, "task-session")

    assert closed_targets == [("http://127.0.0.1:9222", "task-target-1")]
    assert page.is_closed()
    assert backend._pages == {}
    assert context.pages == []


@pytest.mark.asyncio
async def test_cdp_backend_page_methods_screenshot_and_shutdown() -> None:
    class Client:
        def __init__(self) -> None:
            self.capture_payloads: list[dict[str, object]] = []

        async def send(self, name, payload=None):
            if name == "Page.getLayoutMetrics":
                return {
                    "layoutViewport": {"clientWidth": 1280, "clientHeight": 720},
                    "contentSize": {"width": 1280, "height": 1500},
                }
            if name == "Page.captureScreenshot":
                self.capture_payloads.append(payload or {})
                return {"data": base64.b64encode(b"shot").decode()}
            return {}

    class Context:
        def __init__(self) -> None:
            self.client = Client()

        async def new_cdp_session(self, page):
            return self.client

    class Page:
        def __init__(self) -> None:
            self.context = Context()
            self.url = "https://example.com"

        async def goto(self, *args, **kwargs):
            return None

        async def click(self, *args, **kwargs):
            return None

        async def fill(self, *args):
            return None

        def locator(self, selector):
            return SimpleNamespace(first=SimpleNamespace(inner_text=lambda timeout: asyncio.sleep(0, result="selected")))

        async def inner_text(self, selector):
            return "body text"

        async def evaluate(self, script):
            return {"script": script}

        async def wait_for_selector(self, selector, timeout):
            return None

        async def wait_for_url(self, url, timeout):
            return None

        async def wait_for_load_state(self, state, timeout):
            return None

        async def query_selector_all(self, selector):
            element = SimpleNamespace(
                inner_text=lambda: asyncio.sleep(0, result=" link "),
                evaluate=lambda script: asyncio.sleep(0, result="a"),
                get_attribute=lambda name: asyncio.sleep(0, result=""),
            )
            return [element]

        async def close(self):
            return None

    page = Page()
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {"s": page}
    playwright = AsyncContext()
    backend._playwright = playwright
    backend._lock = asyncio.Lock()
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]
    assert await backend.navigate("s", "https://example.com") == "Navigated to https://example.com (status 0) — visible in your browser"
    await backend.click("s", "#go")
    await backend.type_text("s", "input", "abc")
    assert await backend.extract_text("s", "body") == "selected"
    assert await backend.extract_text("s", None) == "body text"
    screenshot = await backend.screenshot("s")
    assert screenshot.data == b"shot"
    assert screenshot.mode == "viewport"
    assert screenshot.document_height == 1500
    assert screenshot.is_clipped is True
    assert page.context.client.capture_payloads[-1]["captureBeyondViewport"] is False
    assert "clip" not in page.context.client.capture_payloads[-1]
    full_page_screenshot = await backend.screenshot("s", mode="full_page")
    assert full_page_screenshot.data == b"shot"
    assert full_page_screenshot.mode == "full_page"
    assert page.context.client.capture_payloads[-1]["captureBeyondViewport"] is True
    assert "clip" in page.context.client.capture_payloads[-1]
    page.url = "file:///tmp/generated-report.html"
    local_auto_screenshot = await backend.screenshot("s")
    assert local_auto_screenshot.data == b"shot"
    assert local_auto_screenshot.mode == "full_page"
    assert local_auto_screenshot.requested_mode == "auto"
    assert page.context.client.capture_payloads[-1]["captureBeyondViewport"] is True
    assert "clip" in page.context.client.capture_payloads[-1]
    viewport_screenshot = await backend.screenshot("s", mode="viewport")
    assert viewport_screenshot.data == b"shot"
    assert viewport_screenshot.mode == "viewport"
    assert viewport_screenshot.is_clipped is True
    assert page.context.client.capture_payloads[-1]["captureBeyondViewport"] is False
    await backend.scroll("s", "down", 5)
    await backend.wait_for("s", "#ready", None, None, 1)
    await backend.wait_for("s", None, "https://example.com/*", None, 1)
    await backend.wait_for("s", None, None, None, 1)
    assert await backend.find("s", "a") == [{"tag": "a", "text": "link", "href": ""}]
    assert await backend.run_js("s", "1+1") == {"script": "1+1"}
    await cdp_backend.CDPBackend.close_session(backend, "s")
    await cdp_backend.CDPBackend.shutdown(backend)
    assert playwright.stopped is True


@pytest.mark.asyncio
async def test_cdp_backend_reuses_existing_visible_page_without_context_management() -> None:
    class Page:
        def __init__(self) -> None:
            self.closed = False

        def is_closed(self) -> bool:
            return self.closed

    class Context:
        def __init__(self, pages) -> None:
            self.pages = pages
            self.new_page_called = False

        async def new_page(self):
            self.new_page_called = True
            raise AssertionError("existing CDP pages must be reused")

    page = Page()
    context = Context([page])
    browser = SimpleNamespace(contexts=[context])
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {}
    backend._lock = asyncio.Lock()
    backend._ensure_browser = lambda: asyncio.sleep(0, result=browser)  # type: ignore[method-assign]

    assert await cdp_backend.CDPBackend._get_page(backend, DEFAULT_AGENT_BROWSER_SESSION_ID) is page
    assert context.new_page_called is False


@pytest.mark.asyncio
async def test_cdp_backend_shutdown_closes_only_backend_owned_pages() -> None:
    class Page:
        def __init__(self) -> None:
            self.closed = False

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

    borrowed_user_page = Page()
    owned_task_page = Page()
    playwright = AsyncContext()
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {
        DEFAULT_AGENT_BROWSER_SESSION_ID: borrowed_user_page,
        "task-session": owned_task_page,
    }
    backend._owned_page_ids = {id(owned_task_page)}
    backend._playwright = playwright
    backend._lock = asyncio.Lock()

    await cdp_backend.CDPBackend.shutdown(backend)

    assert borrowed_user_page.closed is False
    assert owned_task_page.closed is True
    assert backend._pages == {}
    assert backend._owned_page_ids == set()
    assert playwright.stopped is True
    assert backend._browser is None


@pytest.mark.asyncio
async def test_cdp_raw_shutdown_detaches_all_borrowed_wrappers_without_closing_user_targets(
    monkeypatch,
) -> None:
    targets = [
        {
            "id": "borrowed-user-target-1",
            "type": "page",
            "url": "https://example.test/one",
            "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/borrowed-user-target-1",
        },
        {
            "id": "borrowed-user-target-2",
            "type": "page",
            "url": "https://example.test/two",
            "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/borrowed-user-target-2",
        },
    ]
    closed_targets: list[str] = []

    async def close_target(_url: str, target_id: str) -> bool:
        closed_targets.append(target_id)
        return True

    monkeypatch.setattr(cdp_backend, "_async_close_cdp_target", close_target)
    context = cdp_backend._RawCDPContext("http://127.0.0.1:9222", targets)
    borrowed_mapped, borrowed_unassigned = context.pages
    browser = cdp_backend._RawCDPBrowser("http://127.0.0.1:9222")
    browser.contexts = [context]
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._browser = browser
    backend._playwright = None
    backend._pages = {DEFAULT_AGENT_BROWSER_SESSION_ID: borrowed_mapped}
    backend._owned_page_ids = set()
    backend._lock = asyncio.Lock()

    await cdp_backend.CDPBackend.shutdown(backend)

    assert borrowed_mapped.is_closed()
    assert borrowed_unassigned.is_closed()
    assert closed_targets == []
    assert context.pages == []
    assert backend._pages == {}
    assert backend._owned_page_ids == set()
    assert backend._browser is None


@pytest.mark.asyncio
async def test_cdp_backend_preserves_all_preexisting_user_pages() -> None:
    class Page:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

    class Context:
        def __init__(self, pages) -> None:
            self.pages = pages
            self.new_page_called = False

        async def new_page(self):
            self.new_page_called = True
            raise AssertionError("existing CDP pages must be reused")

    old_blank = Page("brave://newtab/")
    useful = Page("https://example.com")
    new_blank = Page("about:blank")
    context = Context([old_blank, useful, new_blank])
    browser = SimpleNamespace(contexts=[context])
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {}
    backend._lock = asyncio.Lock()
    backend._ensure_browser = lambda: asyncio.sleep(0, result=browser)  # type: ignore[method-assign]

    assert await cdp_backend.CDPBackend._get_page(backend, DEFAULT_AGENT_BROWSER_SESSION_ID) is new_blank
    assert old_blank.closed is False
    assert new_blank.closed is False
    assert useful.closed is False
    assert context.new_page_called is False


@pytest.mark.asyncio
async def test_cdp_backend_keeps_dedicated_task_pages_distinct_and_preserves_user_tabs() -> None:
    class Page:
        def __init__(self, url: str, name: str) -> None:
            self.url = url
            self.name = name
            self.closed = False

        def is_closed(self) -> bool:
            return self.closed

        async def close(self) -> None:
            self.closed = True

    class Context:
        def __init__(self, pages) -> None:
            self.pages = pages
            self.created: list[Page] = []

        async def new_page(self):
            page = Page("about:blank", f"task-{len(self.created) + 1}")
            self.created.append(page)
            self.pages.append(page)
            return page

    user_page = Page("https://logged-in.example/account", "user-page")
    user_blank = Page("about:blank", "user-blank")
    context = Context([user_page, user_blank])
    browser = SimpleNamespace(contexts=[context])
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {}
    backend._lock = asyncio.Lock()
    backend._ensure_browser = lambda: asyncio.sleep(0, result=browser)  # type: ignore[method-assign]

    first = await cdp_backend.CDPBackend._get_page(backend, "task-a")
    second = await cdp_backend.CDPBackend._get_page(backend, "task-b")
    visible_default = await cdp_backend.CDPBackend._get_page(
        backend,
        DEFAULT_AGENT_BROWSER_SESSION_ID,
    )

    assert first is context.created[0]
    assert second is context.created[1]
    assert first is not second
    assert first.closed is False
    assert second.closed is False
    assert user_page.closed is False
    assert user_blank.closed is False
    assert visible_default is user_blank


@pytest.mark.asyncio
async def test_cdp_backend_treats_legacy_isolated_name_as_normal_nondefault_session() -> None:
    class Page:
        def __init__(self, url: str = "https://example.com") -> None:
            self.url = url
            self.closed = False

        def is_closed(self) -> bool:
            return self.closed

    class Context:
        def __init__(self, pages) -> None:
            self.pages = pages
            self.new_page_called = False
            self.created = Page("about:blank")

        async def new_page(self):
            self.new_page_called = True
            self.pages.append(self.created)
            return self.created

    existing = Page("https://logged-in.example/account")
    context = Context([existing])
    browser = SimpleNamespace(contexts=[context])
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {}
    backend._lock = asyncio.Lock()
    backend._ensure_browser = lambda: asyncio.sleep(0, result=browser)  # type: ignore[method-assign]

    page = await cdp_backend.CDPBackend._get_page(backend, "isolated-cron-run")

    assert page is context.created
    assert context.new_page_called is True
    assert existing in context.pages


@pytest.mark.asyncio
async def test_cdp_backend_refuses_existing_page_when_dedicated_tab_is_unsupported() -> None:
    class Page:
        url = "https://logged-in.example/account"
        closed = False

        def is_closed(self) -> bool:
            return False

    class Context:
        pages = [Page()]

        async def new_page(self):
            raise RuntimeError(
                "BrowserType.connect_over_cdp: Protocol error "
                "(Browser.setDownloadBehavior): Browser context management is not supported"
            )

    browser = SimpleNamespace(contexts=[Context()])
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {}
    backend._lock = asyncio.Lock()
    backend._ensure_browser = lambda: asyncio.sleep(0, result=browser)  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="does not allow Playwright to create a new tab"):
        await cdp_backend.CDPBackend._get_page(backend, "isolated-cron-run")

    assert Context.pages[0].closed is False


def test_browser_navigate_cdp_reports_dedicated_page_failure_without_reusing_existing_page() -> None:
    class Page:
        def __init__(self) -> None:
            self.closed = False
            self.goto_url = ""

        def is_closed(self) -> bool:
            return self.closed

        async def goto(self, url, **kwargs):
            self.goto_url = url
            return SimpleNamespace(status=200)

    class Context:
        def __init__(self, page) -> None:
            self.pages = [page]
            self.new_page_called = False

        async def new_page(self):
            self.new_page_called = True
            raise RuntimeError("Browser context management is not supported")

    page = Page()
    context = Context(page)
    browser = SimpleNamespace(contexts=[context], is_connected=lambda: True)
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {}
    backend._lock = asyncio.Lock()
    backend._browser = browser
    backend._ensure_browser = lambda: asyncio.sleep(0, result=browser)  # type: ignore[method-assign]

    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    result = tools.browser_navigate(inv("browser_navigate", {"url": "https://cnn.com", "session_id": "telegram-shot"}))

    assert result.status == "failed"
    assert "does not allow Playwright to create a new tab" in (result.error or "")
    assert page.goto_url == ""
    assert context.new_page_called is True


@pytest.mark.asyncio
async def test_cdp_backend_reports_unsupported_new_tab_in_existing_browser() -> None:
    class Context:
        pages = []

        async def new_page(self):
            raise RuntimeError(
                "BrowserType.connect_over_cdp: Protocol error "
                "(Browser.setDownloadBehavior): Browser context management is not supported"
            )

    browser = SimpleNamespace(contexts=[Context()])
    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._pages = {}
    backend._lock = asyncio.Lock()
    backend._ensure_browser = lambda: asyncio.sleep(0, result=browser)  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="does not allow Playwright to create a new tab"):
        await cdp_backend.CDPBackend._get_page(backend, "s")


@pytest.mark.asyncio
async def test_cdp_backend_does_not_create_bootstrap_target_before_playwright_attach(monkeypatch) -> None:
    calls: list[str] = []

    class Browser:
        contexts = [SimpleNamespace(pages=[SimpleNamespace(is_closed=lambda: False)])]

        def is_connected(self) -> bool:
            return True

    class Chromium:
        async def connect_over_cdp(self, url):
            calls.append(f"connect:{url}")
            return Browser()

    class PlaywrightContext:
        async def __aenter__(self):
            return SimpleNamespace(chromium=Chromium())

        async def __aexit__(self, exc_type, exc, tb):
            calls.append("playwright-exit")
            return False

    async def targets(_url):
        calls.append("targets")
        return []

    async def new_target(_url):
        calls.append("new-target")
        return {
            "id": "target-1",
            "type": "page",
            "url": "about:blank",
            "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/1",
        }

    monkeypatch.setattr(cdp_backend, "async_playwright", lambda: PlaywrightContext())
    monkeypatch.setattr(cdp_backend, "_async_cdp_targets", targets)
    monkeypatch.setattr(cdp_backend, "_async_new_cdp_target", new_target)

    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._cdp_url = "http://127.0.0.1:9222"
    backend._browser = None
    backend._playwright = None
    backend._pages = {}
    backend._lock = asyncio.Lock()

    browser = await cdp_backend.CDPBackend._ensure_browser(backend)

    assert browser.is_connected()
    assert calls == ["targets", "connect:http://127.0.0.1:9222"]


@pytest.mark.asyncio
async def test_cdp_raw_fallback_adopts_its_owned_bootstrap_target(monkeypatch) -> None:
    calls: list[str] = []

    class Chromium:
        async def connect_over_cdp(self, _url):
            calls.append("connect")
            raise RuntimeError("force raw fallback")

    class Playwright:
        chromium = Chromium()

        async def stop(self):
            calls.append("playwright-stop")

    class PlaywrightContext:
        async def __aenter__(self):
            return Playwright()

        async def __aexit__(self, exc_type, exc, tb):
            calls.append("playwright-exit")
            return False

    async def targets(_url):
        calls.append("targets")
        return []

    async def new_target(_url):
        calls.append("new-target")
        return {
            "id": "bootstrap-target",
            "type": "page",
            "url": "about:blank",
            "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/bootstrap-target",
        }

    monkeypatch.setattr(cdp_backend, "async_playwright", lambda: PlaywrightContext())
    monkeypatch.setattr(cdp_backend, "_async_cdp_targets", targets)
    monkeypatch.setattr(cdp_backend, "_async_new_cdp_target", new_target)

    backend = cdp_backend.CDPBackend.__new__(cdp_backend.CDPBackend)
    backend._cdp_url = "http://127.0.0.1:9222"
    backend._browser = None
    backend._playwright = None
    backend._pages = {}
    backend._owned_page_ids = set()
    backend._lock = asyncio.Lock()

    page = await cdp_backend.CDPBackend._get_page(backend, "task-session")

    assert page.target_id == "bootstrap-target"
    assert page._close_target_on_close is True
    assert backend._pages == {"task-session": page}
    assert backend._owned_page_ids == {id(page)}
    assert calls == ["targets", "connect", "playwright-stop", "targets", "new-target"]


@pytest.mark.asyncio
async def test_cdp_backend_falls_back_to_raw_devtools_when_playwright_context_management_fails(monkeypatch) -> None:
    received_methods: list[str] = []
    screenshot_png = b"\x89PNG\r\n" + (b"x" * 1_800_000)

    async def handler(websocket) -> None:
        async for raw_message in websocket:
            message = json.loads(raw_message)
            method = message["method"]
            received_methods.append(method)
            result = {}
            if method == "Page.getLayoutMetrics":
                result = {
                    "layoutViewport": {"clientWidth": 800, "clientHeight": 600},
                    "contentSize": {"width": 800, "height": 600},
                }
            elif method == "Page.captureScreenshot":
                result = {"data": base64.b64encode(screenshot_png).decode("ascii")}
            await websocket.send(json.dumps({"id": message["id"], "result": result}))
            if method == "Page.navigate":
                await websocket.send(json.dumps({"method": "Page.domContentEventFired", "params": {}}))

    server = await websockets.serve(handler, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        websocket_url = f"ws://127.0.0.1:{port}/devtools/page/1"

        class Chromium:
            async def connect_over_cdp(self, _url):
                raise RuntimeError(
                    "BrowserType.connect_over_cdp: Protocol error "
                    "(Browser.setDownloadBehavior): Browser context management is not supported"
                )

        class PlaywrightContext:
            async def __aenter__(self):
                return SimpleNamespace(chromium=Chromium())

            async def __aexit__(self, exc_type, exc, tb):
                return False

        monkeypatch.setattr(cdp_backend, "async_playwright", lambda: PlaywrightContext())
        monkeypatch.setattr(
            cdp_backend,
            "_async_cdp_targets",
            lambda _url: asyncio.sleep(
                0,
                result=[
                    {
                        "id": "target-1",
                        "type": "page",
                        "url": "about:blank",
                        "webSocketDebuggerUrl": websocket_url,
                    }
                ],
            ),
        )
        monkeypatch.setattr(
            cdp_backend,
            "_async_new_cdp_target",
            lambda _url: asyncio.sleep(
                0,
                result={
                    "id": "task-target-2",
                    "type": "page",
                    "url": "about:blank",
                    "webSocketDebuggerUrl": websocket_url,
                },
            ),
        )
        monkeypatch.setattr(
            cdp_backend,
            "_async_close_cdp_target",
            lambda _url, _target_id: asyncio.sleep(0, result=True),
        )
        monkeypatch.setenv("NULLION_BROWSER_CDP_URL", "http://127.0.0.1:9222")

        backend = cdp_backend.CDPBackend()
        result = await backend.navigate("telegram-shot", "https://cnn.com")
        screenshot = await backend.screenshot("telegram-shot")

        assert result == "Navigated to https://cnn.com (status 0) — visible in your browser"
        assert screenshot.data == screenshot_png
        assert screenshot.mode == "viewport"
        assert "Page.navigate" in received_methods
        assert "Page.captureScreenshot" in received_methods
        assert "Browser.setDownloadBehavior" not in received_methods
        await backend.shutdown()
    finally:
        server.close()
        await server.wait_closed()


class AsyncContext:
    def __init__(self) -> None:
        self.stopped = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def stop(self):
        self.stopped = True


@pytest.mark.parametrize("label", ["日付", "Fecha", "التاريخ"])
@pytest.mark.parametrize("restriction", ["", "disabled", "ambiguous", "reset"])
def test_native_combobox_commits_exact_option_and_dispatches_events(monkeypatch, label, restriction):
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    disabled = "disabled" if restriction == "disabled" else ""
    duplicate = '<option value="other">2026-10-16</option>' if restriction == "ambiguous" else ""
    reset = "this.value='2026-10-13';" if restriction == "reset" else ""
    html = f"""<html><head><meta charset="utf-8"></head><body><select aria-label="{label}"
      oninput="document.body.dataset.input='yes'"
      onchange="{reset}document.body.dataset.applied=this.value">
      <option value="2026-10-13" selected>2026-10-13</option>
      <option value="2026-10-16" {disabled}>2026-10-16</option>{duplicate}</select></body></html>"""
    try:
        _run(backend.navigate("native-select", "data:text/html," + quote(html)))
        selected = tools.browser_select_combobox(inv("browser_select_combobox", {
            "session_id": "native-select", "label": label, "query": "2026-10-16"}))
        value = _run(backend.run_js("native-select", "document.querySelector('select').value"))
        if restriction:
            assert selected.status == "failed"
            assert value == "2026-10-13"
        else:
            assert selected.status == "completed"
            assert value == "2026-10-16"
            assert _run(backend.run_js("native-select", "document.body.dataset.applied")) == value
            assert _run(backend.run_js("native-select", "document.body.dataset.input")) == "yes"
    finally:
        _run(backend.close_session("native-select"))
        _run(backend.shutdown())


def test_native_combobox_does_not_use_an_unrelated_label(monkeypatch):
    backend = _new_playwright_backend_or_skip(monkeypatch)
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())
    try:
        _run(backend.navigate("wrong-label", "data:text/html," + quote(
            '<select aria-label="Currency"><option>EUR</option><option>USD</option></select>')))
        selected = tools.browser_select_combobox(inv("browser_select_combobox", {
            "session_id": "wrong-label", "label": "日付", "query": "USD"}))
        assert selected.status == "failed"
        assert selected.output["result"]["reason"] == "combobox_not_found"
        assert _run(backend.run_js("wrong-label", "document.querySelector('select').value")) == "EUR"
    finally:
        _run(backend.close_session("wrong-label"))
        _run(backend.shutdown())

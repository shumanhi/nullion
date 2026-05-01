from __future__ import annotations

import asyncio
import base64
from types import SimpleNamespace

import pytest

from nullion.plugins.browser_plugin.backends import auto_backend, cdp_backend, playwright_backend
from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
from nullion.plugins.browser_plugin.browser_tools import BrowserTools
from nullion.tools import ToolInvocation


def inv(tool_name: str, arguments: dict[str, object], *, capsule_id: str | None = "scope") -> ToolInvocation:
    return ToolInvocation(
        invocation_id=f"inv-{tool_name}",
        tool_name=tool_name,
        principal_id="operator",
        arguments=arguments,
        capsule_id=capsule_id,
    )


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

    async def click(self, session_id: str, selector: str) -> None:
        self.calls.append(("click", session_id, selector))

    async def type_text(self, session_id: str, selector: str, text: str) -> None:
        self.calls.append(("type", session_id, selector, text))

    async def extract_text(self, session_id: str, selector: str | None) -> str:
        self.calls.append(("extract", session_id, selector))
        return "hello page"

    async def screenshot(self, session_id: str) -> bytes:
        self.calls.append(("screenshot", session_id))
        return b"png"

    async def scroll(self, session_id: str, direction: str, amount: int) -> None:
        self.calls.append(("scroll", session_id, direction, amount))

    async def wait_for(self, session_id: str, selector: str | None, url_pattern: str | None, timeout: float) -> None:
        self.calls.append(("wait", session_id, selector, url_pattern, timeout))

    async def find(self, session_id: str, selector: str) -> list[dict[str, str]]:
        self.calls.append(("find", session_id, selector))
        return [{"tag": "a", "text": "Example", "href": "https://example.com"}]

    async def run_js(self, session_id: str, script: str):
        self.calls.append(("js", session_id, script))
        return {"ok": True}

    async def close_session(self, session_id: str) -> None:
        self.closed.append(session_id)

    async def shutdown(self) -> None:
        self.shutdown_called = True


def test_browser_tools_cover_success_failure_and_cleanup(monkeypatch) -> None:
    backend = CompleteBackend()
    tools = BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy())

    assert tools.browser_navigate(inv("browser_navigate", {})).error == "Missing required argument: url"
    assert "Only http/https" in tools.browser_navigate(inv("browser_navigate", {"url": "file:///etc/passwd"})).error
    assert tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com", "session_id": "s1"})).output["result"] == "navigated:https://example.com"
    assert tools.browser_click(inv("browser_click", {})).error == "Missing required argument: selector"
    assert tools.browser_click(inv("browser_click", {"selector": "#go", "session_id": "s1"})).output["clicked"] == "#go"
    assert tools.browser_type(inv("browser_type", {})).error == "Missing required argument: selector"
    assert tools.browser_type(inv("browser_type", {"selector": "input", "text": "abc", "session_id": "s1"})).output["typed"] == 3
    assert tools.browser_extract_text(inv("browser_extract_text", {"selector": "body", "session_id": "s1"})).output["length"] == 10
    assert tools.browser_screenshot(inv("browser_screenshot", {"session_id": "s1"})).output["image_base64"] == base64.b64encode(b"png").decode()
    assert tools.browser_scroll(inv("browser_scroll", {"direction": "sideways"})).error == "direction must be 'up' or 'down'"
    assert tools.browser_scroll(inv("browser_scroll", {"direction": "up", "amount": 100, "session_id": "s1"})).output["amount"] == 100
    assert tools.browser_wait_for(inv("browser_wait_for", {})).error == "Provide selector or url_pattern"
    assert tools.browser_wait_for(inv("browser_wait_for", {"selector": "#ready", "session_id": "s1"})).output["waited_for"] == "#ready"
    assert tools.browser_find(inv("browser_find", {})).error == "Missing required argument: selector"
    assert tools.browser_find(inv("browser_find", {"selector": "a", "session_id": "s1"})).output["count"] == 1
    assert tools.browser_run_js(inv("browser_run_js", {})).error == "Missing required argument: script"
    assert tools.browser_run_js(inv("browser_run_js", {"script": "1+1", "session_id": "s1"})).output["result"] == {"ok": True}
    assert tools.browser_close(inv("browser_close", {"session_id": "s1"})).output["closed"] == "s1"
    assert "s1" in backend.closed

    failing = BrowserTools(backend=CompleteBackend(fail=True), pool=BrowserSessionPool(), policy=BrowserPolicy())
    assert failing.browser_navigate(inv("browser_navigate", {"url": "https://example.com"})).error.startswith("Navigation failed:")

    backend.closed.clear()
    tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com", "session_id": "s2"}, capsule_id="scope-a"))
    tools.browser_navigate(inv("browser_navigate", {"url": "https://example.com", "session_id": "s3"}, capsule_id="scope-b"))
    tools.close_tracked_sessions()
    assert backend.closed[-2:] == ["s2", "s3"]


@pytest.mark.asyncio
async def test_auto_backend_selection_and_delegation(monkeypatch) -> None:
    fake = CompleteBackend()
    backend = auto_backend.AutoBackend()
    backend._backend = fake
    assert await backend.navigate("s", "https://example.com") == "navigated:https://example.com"
    await backend.click("s", "#go")
    await backend.type_text("s", "input", "abc")
    assert await backend.extract_text("s", None) == "hello page"
    assert await backend.screenshot("s") == b"png"
    await backend.scroll("s", "down", 25)
    await backend.wait_for("s", "#ready", None, 1)
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
async def test_playwright_backend_page_methods_and_shutdown() -> None:
    class Page:
        def __init__(self) -> None:
            self.context = SimpleNamespace(close=lambda: asyncio.sleep(0))

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
            return b"shot"

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
                get_attribute=lambda name: asyncio.sleep(0, result="https://example.com"),
            )
            return [element]

    page = Page()
    backend = playwright_backend.PlaywrightBackend.__new__(playwright_backend.PlaywrightBackend)
    backend._pages = {"s": page}
    backend._browser = SimpleNamespace(close=lambda: asyncio.sleep(0))
    backend._playwright = AsyncContext()
    backend._lock = asyncio.Lock()
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]
    assert await backend.navigate("s", "https://example.com") == "Navigated to https://example.com (status 201)"
    await backend.click("s", "#go")
    await backend.type_text("s", "input", "abc")
    assert await backend.extract_text("s", "body") == "selected"
    assert await backend.extract_text("s", None) == "body text"
    assert await backend.screenshot("s") == b"shot"
    await backend.scroll("s", "up", 5)
    await backend.wait_for("s", "#ready", None, 1)
    await backend.wait_for("s", None, "https://example.com/*", 1)
    await backend.wait_for("s", None, None, 1)
    assert await backend.find("s", "a") == [{"tag": "a", "text": "link", "href": "https://example.com"}]
    assert await backend.run_js("s", "1+1") == {"script": "1+1"}
    await playwright_backend.PlaywrightBackend.close_session(backend, "s")
    await playwright_backend.PlaywrightBackend.shutdown(backend)


@pytest.mark.asyncio
async def test_cdp_backend_page_methods_screenshot_and_shutdown() -> None:
    class Client:
        async def send(self, name, payload=None):
            if name == "Page.captureScreenshot":
                return {"data": base64.b64encode(b"shot").decode()}
            return {}

    class Context:
        async def new_cdp_session(self, page):
            return Client()

    class Page:
        context = Context()

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
    backend._playwright = AsyncContext()
    backend._lock = asyncio.Lock()
    backend._get_page = lambda session_id: asyncio.sleep(0, result=page)  # type: ignore[method-assign]
    assert await backend.navigate("s", "https://example.com") == "Navigated to https://example.com (status 0) — visible in your browser"
    await backend.click("s", "#go")
    await backend.type_text("s", "input", "abc")
    assert await backend.extract_text("s", "body") == "selected"
    assert await backend.extract_text("s", None) == "body text"
    assert await backend.screenshot("s") == b"shot"
    await backend.scroll("s", "down", 5)
    await backend.wait_for("s", "#ready", None, 1)
    await backend.wait_for("s", None, "https://example.com/*", 1)
    await backend.wait_for("s", None, None, 1)
    assert await backend.find("s", "a") == [{"tag": "a", "text": "link", "href": ""}]
    assert await backend.run_js("s", "1+1") == {"script": "1+1"}
    await cdp_backend.CDPBackend.close_session(backend, "s")
    await cdp_backend.CDPBackend.shutdown(backend)


class AsyncContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

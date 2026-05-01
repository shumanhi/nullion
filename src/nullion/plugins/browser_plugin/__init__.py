"""Browser plugin — register_browser_tools() entry point.

Usage:
    from nullion.plugins.browser_plugin import register_browser_tools
    register_browser_tools(registry)

Or via environment:
    NULLION_PLUGINS=browser  (picked up by the runtime bootstrap)

Backend selection (NULLION_BROWSER_BACKEND):
    auto        — default: attach to existing Chrome if running, otherwise
                  auto-launch Chrome/Brave/Edge, fall back to Playwright headless
    cdp         — attach to already-running Chrome (--remote-debugging-port=9222)
    playwright  — always use Playwright headless Chromium
"""
from __future__ import annotations

import os

from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy, get_default_policy
from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
from nullion.plugins.browser_plugin.browser_tools import BrowserTools
from nullion.tools import ToolInvocation, ToolRegistry, ToolRiskLevel, ToolSideEffectClass, ToolSpec


def _make_spec(
    name: str,
    description: str,
    *,
    risk: ToolRiskLevel = ToolRiskLevel.MEDIUM,
    side_effect: ToolSideEffectClass = ToolSideEffectClass.READ,
    requires_approval: bool = False,
    timeout: int = 30,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        risk_level=risk,
        side_effect_class=side_effect,
        requires_approval=requires_approval,
        timeout_seconds=timeout,
    )


def register_browser_tools(
    registry: ToolRegistry,
    *,
    policy: BrowserPolicy | None = None,
) -> None:
    """Register all browser_* tools into the given ToolRegistry.

    Picks the backend from NULLION_BROWSER_BACKEND env var (default: playwright).
    """
    backend_name = os.environ.get("NULLION_BROWSER_BACKEND", "auto").lower()

    if backend_name == "cdp":
        from nullion.plugins.browser_plugin.backends.cdp_backend import CDPBackend
        backend = CDPBackend()
    elif backend_name == "playwright":
        from nullion.plugins.browser_plugin.backends.playwright_backend import PlaywrightBackend
        backend = PlaywrightBackend()
    else:  # "auto" or anything unrecognised
        from nullion.plugins.browser_plugin.backends.auto_backend import AutoBackend
        backend = AutoBackend()

    pool = BrowserSessionPool()
    effective_policy = policy or get_default_policy()
    tools = BrowserTools(backend=backend, pool=pool, policy=effective_policy)
    registry.mark_plugin_installed("browser_plugin")
    registry.register_cleanup_hook(tools.close_tracked_sessions)

    registry.register(
        _make_spec(
            "browser_navigate",
            "Navigate to a URL in the browser. Returns when the page has loaded.",
            risk=ToolRiskLevel.MEDIUM,
            side_effect=ToolSideEffectClass.READ,
            timeout=30,
        ),
        tools.browser_navigate,
    )
    registry.register(
        _make_spec(
            "browser_click",
            "Click an element on the current page identified by a CSS selector.",
            risk=ToolRiskLevel.MEDIUM,
            side_effect=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout=15,
        ),
        tools.browser_click,
    )
    registry.register(
        _make_spec(
            "browser_type",
            "Type text into a field identified by a CSS selector.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.WRITE,
            timeout=10,
        ),
        tools.browser_type,
    )
    registry.register(
        _make_spec(
            "browser_extract_text",
            "Extract visible text from the page or a specific element (CSS selector).",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
        ),
        tools.browser_extract_text,
    )
    registry.register(
        _make_spec(
            "browser_screenshot",
            "Capture the current viewport as a PNG image (returns base64).",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
        ),
        tools.browser_screenshot,
    )
    registry.register(
        _make_spec(
            "browser_scroll",
            "Scroll the page up or down by a given pixel amount.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=5,
        ),
        tools.browser_scroll,
    )
    registry.register(
        _make_spec(
            "browser_wait_for",
            "Wait for a CSS selector to appear or for the URL to match a pattern.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=30,
        ),
        tools.browser_wait_for,
    )
    registry.register(
        _make_spec(
            "browser_find",
            "Find all elements matching a CSS selector. Returns tag, text, and href for each.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
        ),
        tools.browser_find,
    )
    registry.register(
        _make_spec(
            "browser_run_js",
            "Execute JavaScript on the current page and return the result.",
            risk=ToolRiskLevel.HIGH,
            side_effect=ToolSideEffectClass.WRITE,
            requires_approval=True,
            timeout=15,
        ),
        tools.browser_run_js,
    )
    registry.register(
        _make_spec(
            "browser_close",
            "Close the browser session.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.WRITE,
            timeout=5,
        ),
        tools.browser_close,
    )

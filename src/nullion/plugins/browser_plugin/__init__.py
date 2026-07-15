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
    input_schema: dict[str, object] | None = None,
    continuation_tools: tuple[str, ...] = (),
) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=description,
        risk_level=risk,
        side_effect_class=side_effect,
        requires_approval=requires_approval,
        timeout_seconds=timeout,
        input_schema=input_schema,
        continuation_tools=continuation_tools,
    )


def _object_schema(
    properties: dict[str, object],
    *,
    required: list[str] | None = None,
) -> dict[str, object]:
    return {
        "type": "object",
        "properties": {
            **properties,
            "session_id": {
                "type": "string",
                "description": "Optional browser session id. Omit for the current workspace browser session.",
            },
        },
        "required": required or [],
        "additionalProperties": False,
    }


_BROWSER_TARGET_PROPERTIES: dict[str, object] = {
    "selector": {"type": "string", "description": "CSS or Playwright selector. Prefer semantic fields when available."},
    "label": {"type": "string", "description": "Accessible label text for the element or field."},
    "placeholder": {"type": "string", "description": "Placeholder text for an input field."},
    "role": {"type": "string", "description": "ARIA role such as button, textbox, link, combobox, or option."},
    "name": {"type": "string", "description": "Accessible name used with role, or visible name fallback."},
    "text": {"type": "string", "description": "Visible text for a clickable element."},
}


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
            "browser_open",
            "Open or focus the configured agent browser session without navigating away from the current page.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            input_schema=_object_schema({}),
        ),
        tools.browser_open,
    )
    registry.register(
        _make_spec(
            "browser_navigate",
            "Navigate the configured agent browser backend to an HTTP/HTTPS URL, or to a local HTML file generated inside this workspace. Returns when the page has loaded.",
            risk=ToolRiskLevel.MEDIUM,
            side_effect=ToolSideEffectClass.READ,
            timeout=30,
            continuation_tools=(
                "browser_snapshot",
                "browser_extract_text",
                "browser_extract_items",
                "browser_click_element",
                "browser_type_field",
                "browser_wait_for",
                "browser_assert_page_state",
                "browser_screenshot",
                "browser_image_collect",
            ),
            input_schema=_object_schema(
                {
                    "url": {
                        "type": "string",
                        "description": "HTTP/HTTPS URL, or a local HTML file path/file URL inside this workspace.",
                    }
                },
                required=["url"],
            ),
        ),
        tools.browser_navigate,
    )
    registry.register(
        _make_spec(
            "browser_click",
            "Click an element on the current page identified by a CSS or Playwright selector. Prefer browser_click_element for QA/navigation.",
            risk=ToolRiskLevel.MEDIUM,
            side_effect=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout=15,
            input_schema=_object_schema(
                {
                    "selector": {
                        "type": "string",
                        "description": "CSS selector for the element to click, such as button[type=submit] or [aria-label='Search'].",
                    }
                },
                required=["selector"],
            ),
        ),
        tools.browser_click,
    )
    registry.register(
        _make_spec(
            "browser_click_element",
            "Click an element using semantic page targets: label, visible text, role/name, placeholder, or selector. Prefer this over raw CSS for QA/navigation.",
            risk=ToolRiskLevel.MEDIUM,
            side_effect=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout=15,
            input_schema=_object_schema(_BROWSER_TARGET_PROPERTIES),
        ),
        tools.browser_click_element,
    )
    registry.register(
        _make_spec(
            "browser_type",
            "Type text into a field identified by a CSS or Playwright selector. Prefer browser_type_field for QA/navigation.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.WRITE,
            timeout=10,
            input_schema=_object_schema(
                {
                    "selector": {"type": "string", "description": "CSS selector for the input or editable element."},
                    "text": {"type": "string", "description": "Text to type into the selected element."},
                },
                required=["selector", "text"],
            ),
        ),
        tools.browser_type,
    )
    registry.register(
        _make_spec(
            "browser_type_field",
            "Type text into a field using semantic page targets: label, placeholder, role/name, or selector. Prefer this over raw CSS for QA/navigation.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.WRITE,
            timeout=15,
            input_schema=_object_schema(
                {
                    **{key: value for key, value in _BROWSER_TARGET_PROPERTIES.items() if key != "text"},
                    "text": {"type": "string", "description": "Text to type into the matched field."},
                },
                required=["text"],
            ),
        ),
        tools.browser_type_field,
    )
    registry.register(
        _make_spec(
            "browser_snapshot",
            "Inspect the current page and return visible interactive elements with stable element_id values for reliable follow-up actions.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            continuation_tools=(
                "browser_click_id",
                "browser_type_id",
                "browser_click_element",
                "browser_type_field",
                "browser_wait_for",
                "browser_assert_page_state",
            ),
            input_schema=_object_schema(
                {
                    "max_elements": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 250,
                        "description": "Maximum number of visible interactive elements to return. Defaults to 120.",
                    }
                }
            ),
        ),
        tools.browser_snapshot,
    )
    registry.register(
        _make_spec(
            "browser_extract_items",
            (
                "Extract compact structured item rows from the current rendered page, including direct hrefs, "
                "visible title/text, image URLs, and numeric snippets. Prefer this over browser_snapshot when "
                "building artifacts from lists, tables, cards, search results, or listings."
            ),
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            continuation_tools=("browser_run_js", "browser_image_collect"),
            input_schema=_object_schema(
                {
                    "max_items": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "description": "Maximum compact item rows to return. Defaults to 30.",
                    },
                    "selector": {
                        "type": "string",
                        "description": "Optional CSS selector limiting extraction to a result/list/table region.",
                    },
                }
            ),
        ),
        tools.browser_extract_items,
    )
    registry.register(
        _make_spec(
            "browser_click_id",
            "Click a visible page element by element_id from browser_snapshot. Prefer this after taking a snapshot.",
            risk=ToolRiskLevel.MEDIUM,
            side_effect=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout=10,
            input_schema=_object_schema(
                {
                    "element_id": {
                        "type": "string",
                        "description": "Stable element_id returned by browser_snapshot.",
                    }
                },
                required=["element_id"],
            ),
        ),
        tools.browser_click_id,
    )
    registry.register(
        _make_spec(
            "browser_type_id",
            "Type into an editable page element by element_id from browser_snapshot and verify the field value changed.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.WRITE,
            timeout=10,
            input_schema=_object_schema(
                {
                    "element_id": {
                        "type": "string",
                        "description": "Stable element_id returned by browser_snapshot.",
                    },
                    "text": {"type": "string", "description": "Text to enter into the field."},
                    "clear": {
                        "type": "boolean",
                        "description": "Clear the existing value before typing. Defaults to true.",
                    },
                },
                required=["element_id", "text"],
            ),
        ),
        tools.browser_type_id,
    )
    registry.register(
        _make_spec(
            "browser_select_combobox",
            "Fill a dynamic combobox/autocomplete field, select a compatible visible option, and fail instead of choosing the wrong option.",
            risk=ToolRiskLevel.MEDIUM,
            side_effect=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout=15,
            input_schema=_object_schema(
                {
                    "element_id": {
                        "type": "string",
                        "description": "Optional stable element_id returned by browser_snapshot.",
                    },
                    "label": {"type": "string", "description": "Accessible label for the combobox."},
                    "placeholder": {"type": "string", "description": "Placeholder text for the combobox."},
                    "name": {"type": "string", "description": "Name attribute for the combobox."},
                    "query": {"type": "string", "description": "Text to type into the combobox."},
                    "expected_text": {
                        "type": "string",
                        "description": "Expected selected option/value. Defaults to query.",
                    },
                },
                required=["query"],
            ),
        ),
        tools.browser_select_combobox,
    )
    registry.register(
        _make_spec(
            "browser_extract_text",
            "Extract visible text from the page or a specific element (CSS selector).",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            continuation_tools=("browser_run_js", "browser_image_collect", "browser_screenshot"),
            input_schema=_object_schema(
                {
                    "selector": {
                        "type": "string",
                        "description": "Optional CSS selector. If omitted, extract visible text from the whole page.",
                    }
                }
            ),
        ),
        tools.browser_extract_text,
    )
    registry.register(
        _make_spec(
            "browser_screenshot",
            "Capture the browser page as a PNG artifact and return its path. "
            "Defaults to auto mode. Use viewport for ordinary chat screenshots; use full_page only when "
            "the user explicitly asks for the whole page.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            input_schema=_object_schema(
                {
                    "mode": {
                        "type": "string",
                        "enum": ["auto", "viewport", "full_page"],
                        "description": "Screenshot mode. Defaults to auto.",
                    }
                }
            ),
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
            input_schema=_object_schema(
                {
                    "direction": {
                        "type": "string",
                        "enum": ["up", "down"],
                        "description": "Scroll direction.",
                    },
                    "amount": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Pixel amount to scroll. Defaults to 600.",
                    },
                },
                required=["direction"],
            ),
        ),
        tools.browser_scroll,
    )
    registry.register(
        _make_spec(
            "browser_wait_for",
            "Wait for any page condition: a CSS selector, URL glob, or visible page text. Use multiple conditions when a site may update in place instead of navigating.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=30,
            input_schema=_object_schema(
                {
                    "selector": {"type": "string", "description": "CSS selector to wait for."},
                    "url_pattern": {
                        "type": "string",
                        "description": "Optional URL glob pattern to wait for.",
                    },
                    "text": {
                        "type": "string",
                        "description": "Visible page text to wait for, useful when a form updates without changing URL.",
                    },
                    "timeout": {
                        "type": "number",
                        "minimum": 0.1,
                        "description": "Timeout in seconds. Defaults to 10.",
                    },
                }
            ),
        ),
        tools.browser_wait_for,
    )
    registry.register(
        _make_spec(
            "browser_assert_page_state",
            (
                "Verify that the current rendered page, or an optional CSS-scoped region, contains required state "
                "and omits forbidden state before reporting a browser task complete. Use a scope selector when "
                "unrelated navigation, recommendations, or sidebars may contain forbidden text. An unverified "
                "result is evidence to inspect another structured page surface, not proof that the task is blocked."
            ),
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            continuation_tools=(
                "browser_navigate",
                "browser_extract_text",
                "browser_extract_items",
                "browser_snapshot",
                "browser_find",
                "browser_run_js",
                "browser_wait_for",
                "browser_screenshot",
            ),
            input_schema=_object_schema(
                {
                    "required": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Visible text or selected values that must be present. Address-like values require compatible numeric tokens.",
                    },
                    "forbidden": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Visible text or selected values that must not be present inside the selected scope.",
                    },
                    "selector": {
                        "type": "string",
                        "description": (
                            "Optional CSS selector that limits both required and forbidden checks to one relevant "
                            "page region. Omit only when the assertion intentionally applies to the whole page."
                        ),
                    },
                }
            ),
        ),
        tools.browser_assert_page_state,
    )
    registry.register(
        _make_spec(
            "browser_find",
            "Find all elements matching a CSS selector. Returns tag, text, and href for each.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            input_schema=_object_schema(
                {
                    "selector": {
                        "type": "string",
                        "description": "CSS selector for elements to find.",
                    }
                },
                required=["selector"],
            ),
        ),
        tools.browser_find,
    )
    registry.register(
        _make_spec(
            "browser_run_js",
            "Execute JavaScript on the current page and return the result.",
            risk=ToolRiskLevel.HIGH,
            side_effect=ToolSideEffectClass.WRITE,
            # Browser automation sessions are already an explicit user-controlled
            # tool surface; keep JS execution unblocked so scripted QA flows can
            # inspect and manipulate local app state without a second approval.
            requires_approval=False,
            timeout=15,
            input_schema=_object_schema(
                {
                    "script": {
                        "type": "string",
                        "description": "JavaScript expression or function body to execute in the current page.",
                    }
                },
                required=["script"],
            ),
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
            input_schema=_object_schema({}),
        ),
        tools.browser_close,
    )

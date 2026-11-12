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
from nullion.plugins.browser_plugin.browser_tools import BrowserTools, BROWSER_ITEM_FIELDS
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
    source_artifact_extensions: tuple[str, ...] = (),
    source_artifact_only: bool = False,
    capability_tags: tuple[str, ...] = (),
    observation_setup_tools: tuple[str, ...] = (),
    observation_repair_parameters: tuple[str, ...] = (),
    observation_inline_setup_tools: tuple[str, ...] = (),
    context_argument_bindings: tuple[tuple[str, str], ...] = (),
    discovery_parameters: tuple[str, ...] = (),
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
        source_artifact_extensions=source_artifact_extensions,
        source_artifact_only=source_artifact_only,
        capability_tags=capability_tags,
        observation_setup_tools=observation_setup_tools,
        observation_repair_parameters=observation_repair_parameters,
        observation_inline_setup_tools=observation_inline_setup_tools,
        context_argument_bindings=context_argument_bindings,
        discovery_parameters=discovery_parameters,
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
    model_client_getter=None,
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
    from nullion.plugins.browser_plugin.browser_use_adapter import browser_use_available, browser_task_handler
    if model_client_getter is not None and browser_use_available():
        registry.register(
            _make_spec(
                "browser_run_task",
                "Delegate dynamic multi-step forms and result comparisons to Browser Use. For simple public text, use lightweight readers. Request browser_run_task in both tool_names and required_tool_names to delegate. Select this tool by exact name in both tool_names and required_tool_names through request_tool_scope only when delegating a full visual workflow. An optional tool_names entry does not launch or expose this several-minute worker; ordinary web research retains lightweight readers. If a dynamic workflow needs delegation later, request its exact required scope then. This tool captures PNG screenshots as internal evidence together with grounded observations and unknown requirements. Screenshots are delivered only when the typed task scope requests PNG attachments; otherwise return the findings as text. It fulfills requested screenshot capture without browser_screenshot. Its isolated browser session is not shared with low-level browser tools. Scope only this tool for a visual navigation task with screenshots; do not require a separate capture tool. Reuses the configured model. Prefer this for dynamic multi-step forms and result comparisons from the start. Do not duplicate the same form workflow with a long low-level tool sequence before delegating it. For long pages, use captured-text query search to locate required rows. A verified no-result condition within the user's exact constraints is an outcome; do not broaden to different locations or constraints without permission. For readable public pages, use navigation and structured extraction directly; do not start a full visual worker just to read ordinary text or collect image URLs. When a visual workflow genuinely needs multiple sources, use one coherent multi-page objective rather than separate full workers for each source. Stop when the requested observations are captured and avoid optional detail exploration. Delegate only outcomes needed for the original user objective; do not make extra detail or a preferred intermediate source mandatory. For open-ended research, choose accessible, substantiated candidates before deep exploration; replace a blocked optional candidate instead of exhausting full workers on it. Preserve subjects and sources explicitly required by the user. Does not book or purchase. May take several minutes.",
                risk=ToolRiskLevel.MEDIUM, side_effect=ToolSideEffectClass.WRITE, timeout=650,
                continuation_tools=("browser_run_task", "web_search", "web_fetch"),
                source_artifact_extensions=(".png",),
                discovery_parameters=("max_steps",),
                capability_tags=("source_step_context", "explicit_scope_only", "required_scope_only"),
                input_schema={"type": "object", "properties": {
                    "task": {"type": "string", "description": "Structured browser objective with constraints and expected observations"},
                    "url": {"type": "string", "description": "HTTP/HTTPS target page"},
                    "max_steps": {"type": "integer", "minimum": 1, "maximum": 30}},
                    "required": ["task", "url"], "additionalProperties": False},
            ), browser_task_handler(model_client_getter, effective_policy),
        )

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
            context_argument_bindings=(("session_id", "browser_session_id"),),
            continuation_tools=(
                "web_search",
                "web_fetch",
                "browser_run_task",
                "browser_snapshot",
                "browser_extract_text",
                "browser_extract_items",
                "browser_extract_detail",
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
                "browser_screenshot",
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
                "visible title/text, image URLs, numeric snippets, enclosing section labels, and coverage. "
                "For section-based monitoring, scope to that verified region and inspect coverage; a ranked subset does not prove absence. "
                "When a scope is missing or truncated, available_scopes contains selectors observed in this DOM with example text. "
                "Select a relevant observed scope and test it; do not guess a page landmark or save a truncated collection as complete. "
                "For bounded live observations use fields to return only needed titles and stable URLs, without rich image/price data. "
                "This dedicated READ tool can be repeated by live trackers; arbitrary JavaScript cannot. "
                "Prefer this over browser_snapshot when "
                "building artifacts from lists, tables, cards, search results, or listings."
            ),
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            observation_setup_tools=("browser_navigate",),
            observation_repair_parameters=("selector",),
            context_argument_bindings=(("session_id", "browser_session_id"),),
            continuation_tools=("browser_extract_detail", "browser_run_js", "browser_image_collect"),
            input_schema=_object_schema(
                {
                    "fields": {
                        "type": "array", "minItems": 1, "maxItems": len(BROWSER_ITEM_FIELDS), "uniqueItems": True,
                        "items": {"type": "string", "enum": list(BROWSER_ITEM_FIELDS)},
                        "description": "Optional item projection. For small article observations choose title and canonical_url. Section membership and source indices remain present; coverage is never changed by projection. Omit for the full item rows.",
                    },
                    "max_items": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "description": "Maximum compact item rows to return. Defaults to 30.",
                    },
                    "section_heading": {
                        "type": "string",
                        "description": "Exact visible heading of the section to read, in any language. Includes its item links and section context without requiring CSS.",
                    },
                    "selector": {
                        "type": "string",
                        "description": "Optional native CSS selector (document.querySelectorAll) limiting extraction to a result/list/table region. Playwright text pseudo-selectors are not CSS. Prefer section_heading for a labeled section.",
                    },
                }
            ),
        ),
        tools.browser_extract_items,
    )
    registry.register(
        _make_spec(
            "browser_extract_detail",
            (
                "Read one authoritative detail record from the current rendered DOM with Nullion's fixed "
                "typed extractor. Use this for a selected product, booking, venue, event, or other detail page; "
                "do not use browser_run_js output as verified record evidence."
            ),
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            continuation_tools=("browser_assert_page_state", "browser_image_collect", "browser_screenshot"),
            input_schema=_object_schema(
                {
                    "selector": {
                        "type": "string",
                        "description": (
                            "Optional CSS selector limiting extraction to the verified detail region. "
                            "Use the same scope as browser_assert_page_state when one is available."
                        ),
                    }
                }
            ),
        ),
        tools.browser_extract_detail,
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
            (
                "Extract visible text from the page or a specific element (CSS selector). "
                "Check the observed page against the requested inputs and constraints. A loaded URL or generic "
                "landing page does not prove a search was submitted or filters applied. When matching records are "
                "missing, inspect interactive elements and complete the form or use another available source. "
                "Collect every requested comparison side before producing the final report. "
                "For repeatable live observations, set ready_selector or ready_text from an observed stable results landmark. "
                "A freshly navigated page can expose a shell before its data renders; a verified readiness condition is required before saving this text read as a live probe. Use a stable label or result container, not a value that may change."
            ),
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            observation_setup_tools=("browser_navigate", "browser_wait_for"),
            observation_inline_setup_tools=("browser_wait_for",),
            observation_repair_parameters=("ready_selector", "ready_text"),
            context_argument_bindings=(("session_id", "browser_session_id"),),
            continuation_tools=(
                "browser_snapshot", "browser_type_id", "browser_click_id", "browser_select_combobox",
                "browser_assert_page_state", "browser_extract_items", "browser_extract_detail",
                "browser_run_js", "browser_image_collect", "browser_screenshot",
            ),
            input_schema=_object_schema(
                {
                    "selector": {
                        "type": "string",
                        "description": "Optional CSS selector. If omitted, extract visible text from the whole page.",
                    },
                    "ready_selector": {"type": "string", "minLength": 1, "maxLength": 500,
                        "description": "Observed stable result-content CSS selector to wait for before reading, including every live poll. Do not use a guessed selector or the page shell."},
                    "ready_text": {"type": "string", "minLength": 1, "maxLength": 200,
                        "description": "Observed stable results label that must render before reading. Preserve it in saved probe arguments; avoid changing values, timestamps or full headlines."}
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
            source_artifact_extensions=(".png",),
            source_artifact_only=True,
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
            "Wait for any page condition. Provide at least one non-empty selector, url_pattern, or text. Use multiple conditions when a site may update in place instead of navigating. For selected form values, use browser_assert_page_state with each expected value separately; a text wait does not validate input values or a combined label that the page never displays. If the wait fails, inspect the page with browser_snapshot or browser_extract_text before choosing a new condition; do not repeat the same wait blindly.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=30,
            continuation_tools=("browser_snapshot", "browser_extract_text", "browser_assert_page_state"),
            input_schema={**_object_schema(
                {
                    "selector": {"type": "string", "description": "CSS selector to wait for."},
                    "url_pattern": {
                        "type": "string",
                        "description": "Optional URL glob pattern. * matches any sequence, including path separators. This checks the current URL without waiting for unrelated page resources.",
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
            ), "anyOf": [
                {"required": [key], "properties": {key: {"type": "string", "minLength": 1}}}
                for key in ("selector", "url_pattern", "text")
            ]},
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
                "browser_extract_detail",
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
            "Find matching elements for locator discovery. Returns at most 50 element labels, each clipped to 200 characters, with explicit text truncation metadata. This is not a full content read. For monitoring page content, prepare browser_extract_text or a complete scoped browser_extract_items read instead.",
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
    # Exact DOM targets must survive the generic review preview's collection
    # sampling. This callback only projects the adapter's retained observations.
    for name in ('browser_click_id', 'browser_type_id', 'browser_select_combobox', 'browser_click_element'):
        registry.get_spec(name).action_review_context = tools.action_review_context

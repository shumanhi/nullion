"""Browser plugin — register_browser_tools() entry point.

Usage:
    from nullion.plugins.browser_plugin import register_browser_tools
    register_browser_tools(registry)

Or via environment:
    NULLION_PLUGINS=browser  (picked up by the runtime bootstrap)

Backend selection (NULLION_BROWSER_BACKEND):
    auto        — default: attach to existing Chrome if running, otherwise
                  auto-launch Chrome/Brave/Edge; reconnect before offering headless recovery
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
    observation_requires_setup: bool = False,
    observation_repair_parameters: tuple[str, ...] = (),
    observation_inline_setup_tools: tuple[str, ...] = (),
    context_argument_bindings: tuple[tuple[str, str], ...] = (),
    discovery_parameters: tuple[str, ...] = (),
    delegated_tools: tuple[str, ...] = (),
    independent_batch: tuple[str, str] | tuple[()] = (),
    batch_item_required_fields: tuple[str, ...] = (),
    independent_batch_validator=None,
    code_mode_allowed: bool | None = None,
    code_mode_observation_tools: tuple[str, ...] = (),
    code_mode_ui_interaction: bool = False,
) -> ToolSpec:
    return ToolSpec(
        name=name,
        code_mode_allowed=code_mode_allowed,
        code_mode_observation_tools=code_mode_observation_tools,
        code_mode_ui_interaction=code_mode_ui_interaction,
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
        observation_requires_setup=observation_requires_setup,
        observation_repair_parameters=observation_repair_parameters,
        observation_inline_setup_tools=observation_inline_setup_tools,
        context_argument_bindings=context_argument_bindings,
        discovery_parameters=discovery_parameters,
        delegated_tools=delegated_tools,
        independent_batch=independent_batch,
        batch_item_required_fields=batch_item_required_fields,
        independent_batch_validator=independent_batch_validator,
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

    from nullion.plugins.browser_plugin.backends.session_recovery import SessionRecoveryBackend
    backend = SessionRecoveryBackend(backend)
    pool = BrowserSessionPool()
    effective_policy = policy or get_default_policy()
    tools = BrowserTools(backend=backend, pool=pool, policy=effective_policy)
    registry.mark_plugin_installed("browser_plugin")
    registry.register_cleanup_hook(tools.close_tracked_sessions, group="browser_plugin")
    from nullion.plugins.browser_plugin.browser_reads import PageReadBatch, can_batch_read_plan
    registry.register(
        _make_spec(
            "browser_read_pages",
            "Read rendered pages without an inner model loop. Use read_kind=text for a general read. "
            "Set expected_heading only for read_kind=detail. Batch independent URLs with independent_pages=true. "
            "For known URLs and source comparisons, navigation, observed readiness conditions "
            "and structured item/detail or text extraction run together. Omit url for a single read of "
            "the current task tab. Multiple explicit URLs run in order by default. Set independent_pages=true "
            "to read two independent pages at a time, sharing the browser profile without closing the user's tab. "
            "Do not batch pages that change shared search/account state or depend on each other's actions. "
            "For independent URLs, prefer one pages batch; independent_pages=true also allows the runtime "
            "to combine adjacent independent read calls without another model round. "
            "For a comparison of specific products or listings, batch their observed detail URLs with "
            "read_kind=detail and each expected_heading as shown in the listing. This verifies the rendered "
            "detail identity and surfaces title, price, and observed_text from the matched record. "
            "Use observed_text to verify selected options and distinguish the displayed price from other offers. "
            "In code_mode return title, price if present, observed_text and source_page_url from each detail. "
            "Use observed readiness selectors/text for dynamic results. When collecting directly reportable rows, "
            "set verify_records on a scoped item read. Verify the requested values in visible page controls, "
            "such as origin, destination, and date, and choose the required item fields. Use title and price_text "
            "when reading priced options. Verified rows can be delivered even if a later source fails. "
            "A read without a readiness criterion "
            "is an observation, not proof that filters were applied or results are absent. If a page needs "
            "interaction, inspect the returned data and request native controls or browser_run_task then. "
            "When a text read exposes a link to a more specific result page, read its captured URL. "
            "If only the visible label was retained, use follow_link_text with the observed source URL to open "
            "that exact link. Use this for reading destinations, not submitting forms. "
            "Use retain_link_source with follow_link_text to verify and keep useful overview rows as part "
            "of the same read. If the destination fails, those confirmed facts still remain available. "
            "Keep the original user requirements; do not invent mandatory counts or extra research fields. "
            "Once the requested evidence is sufficient, compose the answer rather than starting optional exploration. "
            "Text reads include document_title, visible headings, links and coverage. A complete heading_coverage with zero "
            "headings verifies their absence in that DOM scope; distinguish the document title from a visible heading "
            "instead of repeatedly waiting for a heading that is absent. "
            "When a unique visible detail heading matches the page title, a text read also returns observed_title "
            "and observed_text. Use those record facts before inspecting the full document. "
            "Returns exact source URLs, structured records, coverage and per-page failures. No screenshots or files.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=45,
            capability_tags=("web",),
            delegated_tools=("browser_navigate", "browser_wait_for", "browser_extract_text", "browser_extract_items", "browser_extract_detail"),
            continuation_tools=("browser_snapshot", "browser_navigate", "browser_extract_text", "browser_run_task"),
            context_argument_bindings=(("session_id", "browser_session_id"),),
            independent_batch=("pages", "independent_pages"),
            batch_item_required_fields=("url",),
            independent_batch_validator=lambda arguments, principal_id: can_batch_read_plan(
                tools, arguments, principal_id),
            code_mode_allowed=True,
            input_schema=PageReadBatch.model_json_schema(),
        ),
        tools.browser_read_pages,
    )
    from nullion.plugins.browser_plugin.browser_use_adapter import browser_use_available, browser_task_handler
    if model_client_getter is not None and browser_use_available():
        registry.register(
            _make_spec(
                "browser_run_task",
                "Delegate adaptive browser interaction and dynamic multi-step forms to Browser Use. Prefer lightweight readers such as browser_read_pages, web_fetch and web_search for known URLs and public-page research. Request browser_run_task in both tool_names and required_tool_names to delegate. Select this tool by exact name in both tool_names and required_tool_names through request_tool_scope only when delegating a full visual workflow. An optional tool_names entry does not launch or expose this several-minute worker; ordinary web research retains lightweight readers. If a dynamic workflow needs delegation later, request its exact required scope then. This tool captures PNG screenshots as internal evidence together with grounded observations and unknown requirements. Screenshots are delivered only when the typed task scope requests PNG attachments; otherwise return the findings as text. It fulfills requested screenshot capture without browser_screenshot. In Auto/CDP mode it uses the configured managed browser and its existing login. If this controller cannot attach, continue with ordinary browser tools on that same session; never replace it with a fresh browser. When the current capture includes an observed link to the intended results, read its exact URL directly with browser_read_pages before trying adaptive interaction or clicking its label. Explicit Playwright mode uses separate workspace-owned browser state and cannot borrow a managed-browser login. For an explicitly requested visual navigation task with screenshots, scope this tool; do not require a separate capture tool. Reuses the configured model. Use browser_read_pages first for readable pages and comparisons of known URLs. When captured page state or a registered recipe establishes stable controls, use a verified code_mode sequence of the scoped native browser tools. Check each prerequisite and the resulting page; stop the sequence at an unknown state. Delegate when captured page state establishes that adaptive interaction is needed; do not turn every comparison into a full visual workflow. Do not duplicate the same form workflow with a long low-level tool sequence before delegating it. For long pages, use captured-text query search to locate required rows. A verified no-result condition within the user's exact constraints is an outcome; do not broaden to different locations or constraints without permission. For readable public pages, use navigation and structured extraction directly; do not start a full visual worker just to read ordinary text or collect image URLs. When a visual workflow genuinely needs multiple sources, use one coherent multi-page objective rather than separate full workers for each source. Stop when the requested observations are captured and avoid optional detail exploration. Delegate only outcomes needed for the original user objective; do not make extra detail or a preferred intermediate source mandatory. For open-ended research, choose accessible, substantiated candidates before deep exploration; replace a blocked optional candidate instead of exhausting full workers on it. Preserve subjects and sources explicitly required by the user. Does not book or purchase. May take several minutes.",
                risk=ToolRiskLevel.MEDIUM, side_effect=ToolSideEffectClass.WRITE, timeout=650,
                continuation_tools=("browser_run_task", "web_search", "web_fetch"),
                source_artifact_extensions=(".png",),
                delegated_tools=("browser_navigate", "browser_wait_for", "browser_extract_text", "browser_extract_items", "browser_extract_detail"),
                discovery_parameters=("max_steps",),
                capability_tags=("source_step_context", "explicit_scope_only", "required_scope_only"),
                # The native handler performs its own request-bound read
                # preflight before adaptive interaction. An outer Code Mode
                # prerequisite would reject the call before that read can run.
                input_schema={"type": "object", "properties": {
                    "task": {"type": "string", "description": "Structured browser objective with constraints and expected observations"},
                    "url": {"type": "string", "description": "HTTP/HTTPS target page"},
                    "max_steps": {"type": "integer", "minimum": 1, "maximum": 30},
                    "read_urls": {"type":"array","maxItems":6,"uniqueItems":True,"items":{"type":"string"},
                        "description":"For a read objective involving multiple sources, include every required source URL here. These pages must be independent; dependent navigation needs separate calls."},
                    "independent_pages": {"type":"boolean","default":False,
                        "description":"True only when the listed read_urls can be read independently; required for multiple sources in one read invocation."},
                    "execution_kind": {"type":"string","enum":["read","adaptive_interaction"],"default":"read",
                        "description":"Use read for research and public pages. Adaptive interaction starts a nested browser agent only for a remaining interactive or visual requirement established by page evidence."},
                    "interaction_reason": {"type":"string","minLength":1,"maxLength":1000,
                        "description":"Required for adaptive_interaction. Identify the observed control, visual state or native access failure and the requested interaction it requires; more research alone is not sufficient."}},
                    "required": ["task", "url"], "additionalProperties": False},
            ), browser_task_handler(model_client_getter, effective_policy, mode_getter=tools.session_browser_mode,
                                    connection_getter=tools.session_browser_connection,
                                    read_preflight=tools.browser_task_read_preflight),
        )

    registry.register(
        _make_spec(
            "browser_open",
            "Open or focus the configured browser; omit mode normally. Headless recovery requires a verified user selection. Retry its configured connection first. If recovery_options are returned, ask numbered choices and wait for the user. Public-page work may need no login, while account work may require restoring the configured session. The headless choice applies only to this task, preserves other tasks and the configured browser, and does not inherit its login.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=20,
            input_schema=_object_schema({"mode": {"type": "string", "enum": ["configured", "headless"], "description": "Omit normally. Explicit headless recovery requires the user to choose it first; do not assume every task needs login."},
                "retention_request_evidence": {"type":"string","minLength":1,"maxLength":2000,"description":"Required for keep_open=true: quote the current user's explicit request to leave the browser open for handoff or follow-up. Tool names in a research instruction do not establish a handoff request."},
                "keep_open": {"type": "boolean", "default": False, "description": "True only when the user explicitly wants the browser context left open for follow-up. Retains this conversation's session until browser_close, /stop or shutdown. Ordinary research tabs close when the request finishes."}}),
        ),
        tools.browser_open,
    )
    registry.register(
        _make_spec(
            "browser_navigate",
            "Use the configured agent browser backend to open an HTTP/HTTPS URL or a local HTML file in this workspace. A successful navigation confirms the document opened. Dynamic results may still be loading; use ready_selector or ready_text on the read.",
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
            code_mode_allowed=True,
            input_schema=_object_schema(
                {
                    "url": {
                        "type": "string",
                        "description": "HTTP/HTTPS URL, or a local HTML file path/file URL inside this workspace.",
                    },
                    "retention_request_evidence": {"type":"string","minLength":1,"maxLength":2000,"description":"Required for keep_open=true: quote the current user's explicit request to leave this page open for handoff or follow-up."},
                    "keep_open": {"type": "boolean", "default": False, "description": "True only for an explicit request to leave this page open for follow-up; preserves its state for this conversation until browser_close, /stop or shutdown. Omit for ordinary research."}
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
            "Inspect the current page and return visible interactive elements with stable element_id values for reliable follow-up actions. "
            "heading_links are exact URLs from visible linked headings. When reading multiple independent details, "
            "batch their captured URLs with browser_read_pages before considering click and back navigation. "
            "A link to a destination is not evidence that the destination is open. Follow observed links and verify the destination URL "
            "and actual controls before claiming a requested form or control is ready. If elements are omitted, inspect retained evidence "
            "or use browser_find rather than assuming the missing controls exist or are absent.",
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            continuation_tools=(
                "browser_read_pages",
                "browser_click_id",
                "browser_type_id",
                "browser_screenshot",
                "browser_click_element",
                "browser_type_field",
                "browser_wait_for",
                "browser_assert_page_state",
            ),
            observation_setup_tools=("browser_navigate", "browser_wait_for"),
            observation_inline_setup_tools=("browser_wait_for",),
            observation_repair_parameters=("ready_selector", "ready_text"),
            observation_requires_setup=True,
            code_mode_allowed=True,
            input_schema=_object_schema(
                {
                    "ready_selector": {"type": "string", "minLength": 1, "maxLength": 500, "description": "Observed source-content selector to wait for before each read and background poll."},
                    "ready_text": {"type": "string", "minLength": 1, "maxLength": 200, "description": "Observed stable source-content label to wait for before each read and background poll."},
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
                "A complete, nonempty collection in a visible selector or section_heading verifies readiness. "
                "For dynamic results or a collection that can legitimately be empty, wait on an observed results landmark with ready_selector or ready_text. "
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
            observation_setup_tools=("browser_navigate", "browser_wait_for"),
            observation_inline_setup_tools=("browser_wait_for",),
            observation_requires_setup=True,
            observation_repair_parameters=("selector", "ready_selector", "ready_text"),
            context_argument_bindings=(("session_id", "browser_session_id"),),
            continuation_tools=("browser_extract_detail", "browser_run_js", "browser_image_collect"),
            code_mode_allowed=True,
            input_schema=_object_schema(
                {
                    "ready_selector": {"type": "string", "minLength": 1, "maxLength": 500,
                        "description": "Observed results-content selector that must become visible before extracting rows."},
                    "ready_text": {"type": "string", "minLength": 1, "maxLength": 200,
                        "description": "Observed stable results-content text to wait for before extracting rows."},
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
                "Read one authoritative record and current control counts. For form/navigation work, inspect "
                "browser_snapshot and follow observed links to actual controls; mentions alone do not establish the target state. "
                "Use the fixed typed DOM extractor for a selected product, booking, venue, event, or other detail page; "
                "do not use browser_run_js output as verified record evidence."
            ),
            risk=ToolRiskLevel.LOW,
            side_effect=ToolSideEffectClass.READ,
            timeout=10,
            continuation_tools=("browser_assert_page_state", "browser_snapshot", "browser_find", "browser_navigate",
                "browser_click_element", "browser_click_id", "browser_image_collect", "browser_screenshot"),
            observation_setup_tools=("browser_navigate", "browser_wait_for"),
            observation_inline_setup_tools=("browser_wait_for",),
            observation_repair_parameters=("ready_selector", "ready_text"),
            observation_requires_setup=True,
            code_mode_allowed=True,
            input_schema=_object_schema(
                {
                    "ready_selector": {"type": "string", "minLength": 1, "maxLength": 500, "description": "Observed source-content selector to wait for before each read and background poll."},
                    "ready_text": {"type": "string", "minLength": 1, "maxLength": 200, "description": "Observed stable source-content label to wait for before each read and background poll."},
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
                "Extract visible text and a bounded index of exact visible links from the page or a CSS region. "
                "For candidate recommendations, comparisons and ordering/booking links, prefer browser_read_pages "
                "or browser_extract_items/detail so record boundaries and hrefs stay together. Use observed direct "
                "links in the answer. Discovery labels do not establish specific items, prices or inventory; read "
                "the relevant detail before claiming those facts. "
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
            observation_requires_setup=True,
            observation_inline_setup_tools=("browser_wait_for",),
            observation_repair_parameters=("ready_selector", "ready_text"),
            context_argument_bindings=(("session_id", "browser_session_id"),),
            continuation_tools=(
                "browser_snapshot", "browser_type_id", "browser_click_id", "browser_select_combobox",
                "browser_assert_page_state", "browser_extract_items", "browser_extract_detail",
                "browser_run_js", "browser_image_collect", "browser_screenshot",
            ),
            code_mode_allowed=True,
            input_schema=_object_schema(
                {
                    "selector": {
                        "type": "string",
                        "description": "Optional CSS selector. If omitted, extract visible text from the whole page.",
                    },
                    "include_links": {"type": "boolean", "default": True,
                        "description": "Set false for a text-only observation, omitting the link index to keep a live probe small. Text and source URL remain intact."},
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
            "Capture a web page or populated local document as PNG. about:blank is valid after rendering content; untouched empty tabs need content or navigation first. Return the artifact path. "
            "Defaults to auto mode. Use viewport for ordinary chat screenshots; use full_page only when "
            "the user explicitly asks for the whole page. Attach the verified image and still answer any "
            "requested instructions, recommendations, or other text in the final reply. If the PDF viewer is blank, "
            "retry with the exact pdf_url from the failure receipt to render the source document.",
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
                    },
                    "pdf_url": {"type": "string", "maxLength": 4000,
                                "description": "Exact already-loaded public PDF URL from a blank-capture receipt. Enables a boundary-checked fetch and local render."},
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
            code_mode_allowed=True,
            code_mode_ui_interaction=True,
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
                "Verify actual destination text and control counts. A link or mention of a form is not the opened form; "
                "inspect its observed controls before claiming completion. Verify that the page or a CSS region contains required state "
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
            code_mode_observation_tools=("browser_read_pages", "browser_navigate", "browser_snapshot", "browser_extract_text"),
            continuation_tools=("browser_read_pages", "browser_navigate", "browser_snapshot", "browser_extract_text",
                                "browser_extract_items", "browser_extract_detail", "browser_wait_for"),
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
            "Close only the selected browser session, not every tab or window. A closed_count of zero means no selected session was open. Never claim all browser tabs are closed from this single-session receipt.",
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
    # Independent page batches are request snapshots. Native UI navigation and
    # controls invalidate them; live reads otherwise keep their fresh behavior.
    registry.get_spec('browser_read_pages').same_request_read_cache = True
    registry.get_spec('browser_read_pages').read_cache_context_keys = ('browser_session_scope', 'browser_session_id')
    for name in ('browser_open', 'browser_navigate', 'browser_run_js', 'browser_scroll'):
        registry.get_spec(name).invalidates_read_cache = True

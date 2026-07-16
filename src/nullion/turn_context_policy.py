"""Typed context and tool eligibility policy for chat turns."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import logging
import os
from pathlib import Path
import re
from typing import Iterable, Mapping
from urllib.parse import urlparse

from nullion import runtime_cache
from nullion.connector_prompt_context import (
    active_connector_provider_context_snapshot,
    cached_active_connector_app_id_providers,
    mentioned_connector_app_ids,
)
from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.cron_execution_tools import (
    CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS,
    CRON_EXECUTION_BLOCKED_TOOLS,
)
from nullion.task_frames import TaskFrameContinuationMode, extract_url_target
from nullion.tools import (
    VALID_ATTACHMENT_EXTENSIONS,
    ToolInvocation,
    ToolResult,
    ToolRiskLevel,
    ToolSideEffectClass,
    ToolSpec,
)

logger = logging.getLogger(__name__)


_CONTEXT_LINK_DISPOSITIONS = frozenset(
    {
        ConversationTurnDisposition.CONTINUE,
        ConversationTurnDisposition.REVISE,
        ConversationTurnDisposition.INTERRUPT,
        ConversationTurnDisposition.BACKGROUND_FOLLOW_UP,
    }
)

_URL_BOUNDARY_TOOLS = frozenset(
    {
        "browser_click",
        "browser_click_element",
        "browser_click_id",
        "browser_open",
        "browser_navigate",
        "browser_extract_text",
        "browser_extract_items",
        "browser_find",
        "browser_image_collect",
        "browser_run_js",
        "browser_assert_page_state",
        "browser_select_combobox",
        "browser_scroll",
        "browser_screenshot",
        "browser_snapshot",
        "browser_type",
        "browser_type_field",
        "browser_type_id",
        "browser_wait_for",
        "browser_close",
        "file_download",
        "web_fetch",
    }
)
_BROWSER_INTERACTION_SCOPE_TOOLS = (
    "browser_open",
    "browser_navigate",
    "browser_extract_text",
    "browser_extract_items",
    "browser_find",
    "browser_snapshot",
    "browser_click_element",
    "browser_click_id",
    "browser_type_field",
    "browser_type_id",
    "browser_assert_page_state",
    "browser_select_combobox",
    "browser_scroll",
    "browser_run_js",
    "browser_wait_for",
)
_DEFAULT_WEB_SCOPE_TOOLS = frozenset({"file_download", "web_fetch", *_BROWSER_INTERACTION_SCOPE_TOOLS})
_OPEN_URL_SCOPE_TOOLS = frozenset(
    {
        "browser_open",
        "browser_navigate",
        "browser_extract_text",
        "browser_extract_items",
        "browser_assert_page_state",
        "file_download",
        "web_fetch",
    }
)
_BROWSER_VISUAL_CAPTURE_TOOLS = frozenset({"browser_image_collect", "browser_screenshot"})
_PDF_EXTENSIONS = frozenset({".pdf"})
_PRESENTATION_EXTENSIONS = frozenset({".ppt", ".pptx"})
_SPREADSHEET_EXTENSIONS = frozenset({".xls", ".xlsx"})
_DOCUMENT_EXTENSIONS = frozenset({".doc", ".docx"})
_PLANNER_FALLBACK_ARTIFACT_EXTENSIONS = (".csv", ".docx", ".html", ".pdf", ".pptx", ".xlsx")
_TEXT_WRITE_EXTENSIONS = frozenset({"", ".csv", ".htm", ".html", ".json", ".md", ".svg", ".tsv", ".txt", ".yaml", ".yml"})
_STRUCTURED_ARTIFACT_TOOLS = frozenset(
    {
        "document_create",
        "pdf_create",
        "pdf_edit",
        "presentation_create",
        "spreadsheet_create",
    }
)
_ARCHIVE_TOOLS = frozenset({"archive_create", "archive_extract"})
_ARTIFACT_EXTENSION_TOOLS = {extension: ("file_write",) for extension in _TEXT_WRITE_EXTENSIONS if extension}
_ARTIFACT_EXTENSION_TOOLS.update(
    {
    ".csv": ("file_write",),
    ".doc": ("document_create",),
    ".docx": ("document_create",),
    ".pdf": ("pdf_create", "pdf_edit"),
    ".ppt": ("presentation_create",),
    ".pptx": ("presentation_create",),
    ".tsv": ("file_write",),
    ".xls": ("spreadsheet_create",),
    ".xlsx": ("spreadsheet_create",),
    }
)
_EMBEDDED_MEDIA_ARTIFACT_EXTENSION_ALIASES = {
    ".doc": ".docx",
    ".htm": ".html",
    ".ppt": ".pptx",
    ".xls": ".xlsx",
}
_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS = frozenset({".docx", ".html", ".pdf", ".pptx", ".xlsx"})
_SCOPE_ACTION_REQUIRED_TOOL_CANDIDATES = frozenset(
    {
        "archive_create",
        "archive_extract",
        "create_cron",
        "delete_cron",
        "delete_reminder",
        "document_create",
        "file_download",
        "file_patch",
        "file_write",
        "image_generate",
        "pdf_create",
        "pdf_edit",
        "presentation_create",
        "run_cron",
        "set_reminder",
        "spreadsheet_create",
        "terminal_exec",
        "toggle_cron",
        "update_cron",
        "update_reminder",
    }
)
_SCOPE_SOURCE_REQUIRED_TOOL_CANDIDATES = frozenset(
    {
        "browser_assert_page_state",
        "browser_extract_items",
        "browser_extract_text",
        "browser_image_collect",
        "browser_navigate",
        "browser_open",
        "browser_run_js",
        "browser_screenshot",
        "calendar_list",
        "email_attachment_read",
        "email_read",
        "email_search",
        "file_read",
        "file_search",
        "market_quote",
        "weather_forecast",
        "web_fetch",
        "workspace_summary",
    }
)
_SCOPE_REQUIRED_TOOL_CANDIDATES = (
    _SCOPE_ACTION_REQUIRED_TOOL_CANDIDATES | _SCOPE_SOURCE_REQUIRED_TOOL_CANDIDATES
)
_CONNECTOR_TOOLS = frozenset({"connector_request"})
_CONNECTOR_CAPABILITY_TAGS = frozenset({"connector"})
_CONNECTOR_TYPED_TOOLS = frozenset({
    "connector_request",
    "email_send",
    "email_search",
    "email_read",
    "email_attachment_read",
    "calendar_list",
    "calendar_create",
    "calendar_update",
    "calendar_respond",
    "calendar_delete",
})
_CONNECTOR_FIRST_CLASS_READ_TOOLS = frozenset({"email_search", "email_read", "email_attachment_read", "calendar_list"})
_CONNECTOR_FIRST_CLASS_WRITE_TOOLS = frozenset({
    "email_send",
    "calendar_create",
    "calendar_update",
    "calendar_respond",
    "calendar_delete",
})
_TURN_DECISION_REQUIRED_TOOL_CANDIDATES = _SCOPE_REQUIRED_TOOL_CANDIDATES | _CONNECTOR_FIRST_CLASS_WRITE_TOOLS
_DIRECT_PUBLIC_READ_CAPABILITY_TAGS = frozenset({"market_data", "quote", "weather", "forecast"})
_SOURCE_MEDIA_TOOLS = frozenset({"audio_transcribe", "image_extract_text"})
_SCHEDULER_READ_TOOLS = frozenset({"list_crons", "list_reminders"})
_SCHEDULER_RUN_TOOLS = frozenset({"run_cron"})
_SCHEDULER_MUTATE_TOOLS = frozenset({
    "create_cron",
    "update_cron",
    "delete_cron",
    "delete_reminder",
    "toggle_cron",
    "set_reminder",
    "update_reminder",
})
_SCHEDULER_TOOLS = frozenset(_SCHEDULER_READ_TOOLS | _SCHEDULER_RUN_TOOLS | _SCHEDULER_MUTATE_TOOLS)
_DEFAULT_REMINDER_SCOPE_TOOLS = frozenset({"set_reminder", "list_reminders", "delete_reminder", "update_reminder"})
_SKILL_PACK_TOOLS = frozenset({"skill_pack_read"})
_SKILL_PACK_CAPABILITY_TAGS = frozenset({"skill_pack"})
_CONVERSATION_HISTORY_TOOLS = frozenset({"chat_history_search"})
_CONVERSATION_HISTORY_CAPABILITY_TAGS = frozenset({"conversation_history"})
_LOCAL_FILE_READ_TOOLS = frozenset({"file_read", "file_search", "workspace_summary"})
_LOCAL_FILE_TOOLS = frozenset({"archive_create", "archive_extract", "file_read", "file_search", "file_write", "file_patch", "workspace_summary"})
_LOCAL_FILE_MUTATION_TOOLS = _LOCAL_FILE_TOOLS - _LOCAL_FILE_READ_TOOLS
_LOCAL_FILE_LISTING_TOOLS = frozenset({"file_read", "file_search", "workspace_summary"})
_LOCAL_SHELL_TOOLS = frozenset({"terminal_exec"})
_SCOPE_REQUEST_TOOL_NAME = "request_tool_scope"
_PLAIN_DIRECT_SAFE_READ_TOOLS = frozenset({"market_quote", "weather_forecast"})
_SCOPE_REQUEST_TOOL_SPEC = ToolSpec(
    name=_SCOPE_REQUEST_TOOL_NAME,
    description=(
        "Request the exact tool family needed for this turn when visible tools are insufficient. "
        "Call this before saying a registered capability is unavailable. "
        "Use connector for authenticated external account APIs such as mail, calendars, docs, "
        "and connected services. For account-evidence or source-discovery requests, request connector "
        "scope so the runtime can report active sources and expose safe structured read tools. "
        "Include tool_names when the exact structured account tool is already known rather than widening "
        "a generic connector scope into every account tool. "
        "When a successful result from an exact source or verification tool is essential to the requested "
        "outcome, include that exact name in both tool_names and required_tool_names. This lets the runtime "
        "prevent a confident final reply from bypassing missing source evidence and report a structurally "
        "unavailable source when no compatible provider or registered tool exists. "
        "Use scheduler_* only for Nullion's own recurring jobs and "
        "reminders. If the current turn asks to manually start, trigger, run now, rerun, or otherwise execute "
        "an existing scheduled task, request scheduler_run with scheduler_action=\"run\" and include run_cron "
        "when registered. If the current turn both locates/identifies an existing scheduled task and executes it, "
        "request scheduler_run, not scheduler_read. Use scheduler_selection_policy=\"user_selected\" when the "
        "task id, number, name, or linked task state is already identified; use \"delegate_one\" only when the "
        "agent should choose exactly one eligible task from structured list_crons output. "
        "Use conversation_history for saved turns in the current chat that are older "
        "than the visible prompt context. Use web for public websites, live public lookups, and public source data "
        "that will be written into a local artifact such as CSV, HTML, PDF, DOCX, or XLSX. Use skill_pack only "
        "when detailed installed-pack reference docs are needed. Capabilities: web, "
        "scheduler_read, scheduler_run, scheduler_mutate, connector, skill_pack, conversation_history, weather, "
        "market_data, "
        "image_generation, local_files, local_shell. "
        "When scheduler_run should run exactly one eligible scheduled task chosen from structured list_crons output, "
        "set scheduler_selection_policy to delegate_one. "
        "When the turn needs a downloadable or attached file in a specific format, include artifact_extensions with the required "
        "final suffix so the runtime can expose the matching structured artifact tool. "
        "When that final artifact must contain images, screenshots, generated visuals, or other media inside "
        "the file itself, also include the same suffix in embedded_media_artifact_extensions so the runtime "
        "can expose media collection/generation tools and verify embedded media bytes."
    ),
    risk_level=ToolRiskLevel.LOW,
    side_effect_class=ToolSideEffectClass.READ,
    requires_approval=False,
    timeout_seconds=1,
    input_schema={
        "type": "object",
        "properties": {
            "capabilities": {
                "type": "array",
                "description": (
                    "Tool families to expose for this same turn. connector exposes active connected-account "
                    "source metadata and safe structured read tools unless tool_names names exact structured "
                    "account tools; "
                    "scheduler_read/run/mutate exposes Nullion cron/reminder "
                    "tools only; conversation_history exposes saved turns in the current chat; "
                "web exposes public web/browser tools; browser is accepted as an alias for web; skill_pack exposes exact "
                    "installed skill-pack docs; weather, market_data, and image_generation expose their direct tools; "
                    "local_files exposes structured local file read/search tools by default and write/artifact "
                    "tools only when exact tool_names or artifact_extensions request them; local_shell exposes "
                    "local terminal execution when registered."
                ),
                "items": {
                    "type": "string",
                    "enum": [
                        "web",
                        "browser",
                        "scheduler_read",
                        "scheduler_run",
                        "scheduler_mutate",
                        "connector",
                        "skill_pack",
                        "conversation_history",
                        "weather",
                        "market_data",
                        "image_generation",
                        "local_files",
                        "local_shell",
                    ],
                },
            },
            "connector_app_ids": {
                "type": "array",
                "description": "Optional connected app ids required by the turn, using ids exposed by active connector metadata.",
                "items": {"type": "string"},
            },
            "source_user_requested": {
                "type": "boolean",
                "description": (
                    "Set true only when the user explicitly asked to use a connected account/source in this turn, "
                    "or this turn is continuing a prior connected-source selection. Do not set true merely because "
                    "an account source might be useful."
                ),
            },
            "source_evidence": {
                "type": "string",
                "description": (
                    "For a new connected-source selection, copy the shortest exact substring from the current user "
                    "turn that names or clearly selects that connected source. The runtime validates this against "
                    "the current turn. Omit it only when typed conversation state already links this turn to a "
                    "previously selected connector source."
                ),
            },
            "tool_names": {
                "type": "array",
                "description": (
                    "Exact registered tools to expose inside the requested capability family. "
                    "Required for typed connector tools such as email_search, email_read, email_attachment_read, "
                    "calendar_list, calendar_create, calendar_update, calendar_respond, calendar_delete, and email_send."
                ),
                "items": {
                    "type": "string",
                    "enum": [
                        "connector_request",
                        "email_send",
                        "email_search",
                        "email_read",
                        "email_attachment_read",
                        "calendar_list",
                        "calendar_create",
                        "calendar_update",
                        "calendar_respond",
                        "calendar_delete",
                        "chat_history_search",
                        "list_crons",
                        "list_reminders",
                        "run_cron",
                        "create_cron",
                        "update_cron",
                        "delete_cron",
                        "delete_reminder",
                        "toggle_cron",
                        "set_reminder",
                        "update_reminder",
                        "web_search",
                        "web_fetch",
                        "browser_open",
                        "browser_navigate",
                        "browser_click",
                        "browser_click_element",
                        "browser_click_id",
                        "browser_type",
                        "browser_type_field",
                        "browser_type_id",
                        "browser_select_combobox",
                        "browser_snapshot",
                        "browser_extract_items",
                        "browser_extract_text",
                        "browser_find",
                        "browser_image_collect",
                        "browser_scroll",
                        "browser_wait_for",
                        "browser_assert_page_state",
                        "browser_screenshot",
                        "browser_run_js",
                        "browser_close",
                        "file_download",
                        "market_quote",
                        "weather_forecast",
                        "image_generate",
                        "document_create",
                        "pdf_create",
                        "pdf_edit",
                        "presentation_create",
                        "spreadsheet_create",
                        "workspace_summary",
                        "archive_create",
                        "archive_extract",
                        "file_read",
                        "file_search",
                        "file_write",
                        "file_patch",
                        "terminal_exec",
                    ],
                },
            },
            "required_tool_names": {
                "type": "array",
                "description": (
                    "Subset of tool_names whose successful current-turn result is essential before a final reply. "
                    "Use this for an explicitly required public/account source or verification step. Artifact, "
                    "scheduler, connector-write, and other action tools are already treated as required when exact."
                ),
                "items": {"type": "string"},
            },
            "artifact_extensions": {
                "type": "array",
                "description": (
                    "Optional final artifact suffixes required by this turn, such as .xlsx, .pdf, "
                    ".pptx, or .docx. Use this only for a requested downloadable/attached artifact; "
                    "the runtime validates the suffix and exposes the matching structured artifact tool. "
                    "If the artifact itself must contain embedded media, repeat that suffix in "
                    "embedded_media_artifact_extensions instead of relying on artifact_extensions alone."
                ),
                "items": {"type": "string", "enum": sorted(VALID_ATTACHMENT_EXTENSIONS)},
            },
            "excluded_artifact_extensions": {
                "type": "array",
                "description": (
                    "Formats mentioned in the request that are not final deliverables, including source, intermediate, "
                    "sidecar, example, or explicitly prohibited attachment formats. These override artifact_extensions."
                ),
                "items": {"type": "string", "enum": sorted(VALID_ATTACHMENT_EXTENSIONS)},
            },
            "scheduler_selection_policy": {
                "type": "string",
                "description": (
                    "Only valid with scheduler_run. Use user_selected when the user already identified an exact "
                    "scheduled task by id/number/name/linked task state. Use delegate_one only when the user "
                    "explicitly delegated choosing exactly one eligible scheduled task to run after list_crons."
                ),
                "enum": ["none", "user_selected", "delegate_one"],
            },
            "scheduler_toggle_enabled": {
                "type": "boolean",
                "description": (
                    "Only valid with scheduler_mutate and toggle_cron. True means the requested final state is enabled; "
                    "false means the requested final state is disabled."
                ),
            },
            "scheduler_target_scope": {
                "type": "string",
                "description": (
                    "Only valid with scheduler_mutate. Use all_current_workspace when the structured request "
                    "applies the same scheduler mutation to every eligible scheduled task in the current workspace."
                ),
                "enum": ["none", "all_current_workspace"],
            },
            "scheduler_action": {
                "type": "string",
                "description": (
                    "Optional explicit scheduler action for this same scope request. Use run when the turn needs "
                    "to execute an existing scheduled task now; mutate when changing saved jobs or reminders; "
                    "inspect only for read-only inventory/status."
                ),
                "enum": ["inspect", "run", "mutate"],
            },
            "embedded_media_artifact_extensions": {
                "type": "array",
                "description": (
                    "Optional final artifact suffixes that must contain requested image/screenshot/media files "
                    "embedded inside the artifact itself, not merely linked, captioned, attached separately, or listed as URLs. "
                    "Use this for visual workbooks, image-bearing PDFs/DOCX/PPTX, flyers, posters, playbooks, "
                    "or reports where the final file must include actual media bytes."
                ),
                "items": {"type": "string", "enum": sorted(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS)},
            },
            "required_embedded_media_extensions": {
                "type": "array",
                "description": (
                    "Alias for embedded_media_artifact_extensions accepted by structured planners. "
                    "Use final artifact suffixes that must contain requested media bytes."
                ),
                "items": {"type": "string", "enum": sorted(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS)},
            },
            "reason": {"type": "string", "description": "Brief structured reason for the requested scope."},
        },
        "required": ["capabilities"],
        "additionalProperties": False,
    },
    capability_tags=("scope_request",),
)
_KNOWN_PRIOR_TOOL_SCOPES = frozenset(
    {"connector", "conversation_history", "scheduler", "scheduler_mutate", "scheduler_read", "scheduler_run", "skill_pack", "web"}
)
_WEB_ACTIONS = frozenset({"none", "open_url", "live_research", "browser_interaction"})
_SCHEDULER_ACTIONS = frozenset({"none", "inspect", "run", "mutate"})
_SKILL_PACK_ACTIONS = frozenset({"none", "reference", "connector"})
_REQUESTED_OUTCOMES = frozenset({"unspecified", "generated_media"})
_TOOL_SCOPE_DECISION_CACHE_NAMESPACE = "tool_scope.decision"
_TOOL_SCOPE_DECISION_CACHE_VERSION = "v50"
_TOOL_SCOPE_DECISION_CACHE_TTL_SECONDS = 24 * 60 * 60
_SCOPE_CAPABILITY_ALIASES = {
    "browser": "web",
    "browser_tools": "web",
    "browser_interaction": "web",
}


def _normalize_scope_capability(value: object) -> str:
    capability = str(value or "").strip().lower()
    return _SCOPE_CAPABILITY_ALIASES.get(capability, capability)


def _normalize_connector_app_id(value: object) -> str:
    return str(value or "").strip().lower()


def _unique_connector_app_ids(values: Iterable[object]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        app_id = _normalize_connector_app_id(value)
        if app_id and app_id not in seen:
            seen.add(app_id)
            ordered.append(app_id)
    return tuple(ordered)


_EMAIL_ADDRESS_TARGET_RE = re.compile(
    r"(?<![\w.+-])[\w.!#$%&'*+/=?^_`{|}~-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63}(?![\w-])"
)


def _has_email_address_target(value: object) -> bool:
    return _EMAIL_ADDRESS_TARGET_RE.search(str(value or "")) is not None


def _validated_artifact_extensions(value: object) -> tuple[str, ...]:
    values = value if isinstance(value, list) else ()
    extensions: list[str] = []
    for raw in values:
        extension = str(raw or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        if extension not in VALID_ATTACHMENT_EXTENSIONS:
            continue
        extensions.append(extension)
    return tuple(extensions)


def _validated_embedded_media_artifact_extensions(value: object) -> tuple[str, ...]:
    values = value if isinstance(value, list) else ()
    extensions: list[str] = []
    for raw in values:
        extension = str(raw or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        extension = _EMBEDDED_MEDIA_ARTIFACT_EXTENSION_ALIASES.get(extension, extension)
        if extension not in _EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS:
            continue
        if extension not in extensions:
            extensions.append(extension)
    return tuple(extensions)


def _validated_scope_request_embedded_media_extensions(arguments: Mapping[str, object]) -> tuple[str, ...]:
    extensions: list[str] = []
    for key in ("embedded_media_artifact_extensions", "required_embedded_media_extensions"):
        for extension in _validated_embedded_media_artifact_extensions(arguments.get(key)):
            if extension not in extensions:
                extensions.append(extension)
    return tuple(extensions)


def _browser_visual_capture_allowed_by_extensions(
    tool_name: str,
    *,
    requested_extensions: Iterable[str],
    embedded_media_extensions: Iterable[str],
) -> bool:
    requested = {str(extension or "").strip().lower() for extension in requested_extensions}
    embedded = {str(extension or "").strip().lower() for extension in embedded_media_extensions}
    if tool_name == "browser_image_collect":
        return bool(embedded.intersection(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS))
    if tool_name == "browser_screenshot":
        return ".png" in requested or bool(embedded.intersection(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS))
    return True


def _exact_scope_tools_for_capability(capability: str, tool_names: Iterable[str]) -> tuple[str, ...]:
    capability_tools: frozenset[str]
    if capability == "connector":
        capability_tools = _CONNECTOR_TYPED_TOOLS | _SKILL_PACK_TOOLS
    elif capability == "scheduler_read":
        capability_tools = _SCHEDULER_READ_TOOLS
    elif capability == "scheduler_run":
        capability_tools = _SCHEDULER_READ_TOOLS | _SCHEDULER_RUN_TOOLS
    elif capability == "scheduler_mutate":
        capability_tools = _SCHEDULER_READ_TOOLS | _SCHEDULER_MUTATE_TOOLS
    elif capability == "web":
        capability_tools = _URL_BOUNDARY_TOOLS | frozenset({"web_search"})
    elif capability == "skill_pack":
        capability_tools = _SKILL_PACK_TOOLS
    elif capability == "conversation_history":
        capability_tools = _CONVERSATION_HISTORY_TOOLS
    elif capability == "weather":
        capability_tools = frozenset({"weather_forecast"})
    elif capability == "market_data":
        capability_tools = frozenset({"market_quote"})
    elif capability == "image_generation":
        capability_tools = frozenset({"image_generate"})
    elif capability == "local_files":
        capability_tools = _LOCAL_FILE_TOOLS | _ARCHIVE_TOOLS | _STRUCTURED_ARTIFACT_TOOLS
    elif capability == "local_shell":
        capability_tools = _LOCAL_SHELL_TOOLS
    else:
        return ()
    return tuple(dict.fromkeys(tool_name for tool_name in tool_names if tool_name in capability_tools))


_LOCAL_ARTIFACT_SOURCE_COMPANIONS: dict[str, tuple[str, ...]] = {
    "file_download": ("file_read", "workspace_summary", "file_search", "archive_extract"),
    "archive_extract": ("file_read", "workspace_summary", "file_search"),
    "browser_image_collect": ("file_read", "workspace_summary", "file_search"),
    "browser_screenshot": ("file_read", "workspace_summary", "file_search"),
}
_EMBEDDED_MEDIA_SOURCE_TOOLS = frozenset(
    {
        "archive_extract",
        "browser_image_collect",
        "browser_screenshot",
        "file_download",
        "file_read",
        "image_generate",
        "spreadsheet_create",
        "terminal_exec",
    }
)


def _scope_tool_names_can_source_embedded_media(tool_names: Iterable[str]) -> bool:
    return bool(
        {
            str(tool_name or "").strip()
            for tool_name in tool_names
            if str(tool_name or "").strip()
        }.intersection(_EMBEDDED_MEDIA_SOURCE_TOOLS)
    )


def _with_local_artifact_companion_tools(
    tool_names: Iterable[str],
    *,
    available_tools: Iterable[str],
) -> tuple[str, ...]:
    available = {str(tool or "").strip() for tool in available_tools if str(tool or "").strip()}
    requested: list[str] = [
        str(tool or "").strip()
        for tool in tool_names
        if str(tool or "").strip() and str(tool or "").strip() in available
    ]
    requested_set = set(requested)
    for tool_name in tuple(requested):
        for companion in _LOCAL_ARTIFACT_SOURCE_COMPANIONS.get(tool_name, ()):
            if companion in available and companion not in requested_set:
                requested.append(companion)
                requested_set.add(companion)
    return tuple(dict.fromkeys(requested))


def _scheduler_tools_with_required_read_scope(tool_names: Iterable[str]) -> tuple[str, ...]:
    requested = tuple(dict.fromkeys(str(tool_name or "").strip() for tool_name in tool_names if str(tool_name or "").strip()))
    requested_set = set(requested)
    if requested_set.intersection(_SCHEDULER_RUN_TOOLS | _SCHEDULER_MUTATE_TOOLS):
        return tuple(dict.fromkeys([*sorted(_SCHEDULER_READ_TOOLS), *requested]))
    return requested


def _scheduler_tools_allowed_for_action(scheduler_action: str) -> frozenset[str]:
    action = str(scheduler_action or "").strip().lower()
    if action == "inspect":
        return _SCHEDULER_READ_TOOLS
    if action == "run":
        return _SCHEDULER_READ_TOOLS | _SCHEDULER_RUN_TOOLS
    if action == "mutate":
        return _SCHEDULER_READ_TOOLS | _SCHEDULER_MUTATE_TOOLS
    return frozenset()


def _filter_scheduler_requested_tool_names_for_action(
    tool_names: Iterable[str],
    scheduler_action: str,
) -> tuple[str, ...]:
    allowed_scheduler_tools = _scheduler_tools_allowed_for_action(scheduler_action)
    filtered: list[str] = []
    for tool_name in tool_names:
        name = str(tool_name or "").strip()
        if not name:
            continue
        if name in _SCHEDULER_TOOLS and name not in allowed_scheduler_tools:
            continue
        if name not in filtered:
            filtered.append(name)
    return tuple(filtered)


def _dedicated_artifact_tool_names_for_extensions(
    extensions: Iterable[str],
    *,
    available_tools: Iterable[str],
) -> frozenset[str]:
    available = {str(tool_name or "").strip() for tool_name in available_tools if str(tool_name or "").strip()}
    tool_names: set[str] = set()
    for raw_extension in extensions:
        extension = str(raw_extension or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        for tool_name in _ARTIFACT_EXTENSION_TOOLS.get(extension, ()):
            if (
                tool_name in available
                and tool_name not in _LOCAL_SHELL_TOOLS
                and tool_name != "file_patch"
            ):
                tool_names.add(tool_name)
    return frozenset(tool_names)


@dataclass(frozen=True, slots=True)
class TurnToolEvidence:
    has_url_target: bool = False
    has_attachments: bool = False
    has_email_address_target: bool = False
    requested_extensions: tuple[str, ...] = ()
    existing_named_artifact_extensions: tuple[str, ...] = ()
    existing_named_artifact_requires_new_content: bool = False
    context_linked: bool = False
    saved_history_available: bool = False
    slash_prefixed_literal: bool = False
    numbered_option_selected: bool = False
    prior_tool_scopes: tuple[str, ...] = ()
    current_user_message: str = ""

    @property
    def artifact_requested(self) -> bool:
        return bool(self.requested_extensions)

    @property
    def existing_named_artifact_requested(self) -> bool:
        return bool(self.existing_named_artifact_extensions)

    def has_prior_tool_scope(self, scope: str) -> bool:
        return scope in set(self.prior_tool_scopes)


@dataclass(frozen=True, slots=True)
class TurnToolScopeDecision:
    requested_outcome: str = "unspecified"
    web_action: str = "none"
    scheduler_action: str = "none"
    scheduler_toggle_enabled: bool | None = None
    scheduler_selection_policy: str = "none"
    scheduler_target_scope: str = "none"
    skill_pack_action: str = "none"
    connector_app_ids: tuple[str, ...] = ()
    connector_source_user_requested: bool = False
    connector_source_evidence: str = ""
    requested_tool_names: tuple[str, ...] = ()
    required_tool_names: tuple[str, ...] = ()
    requested_artifact_extensions: tuple[str, ...] = ()
    excluded_artifact_extensions: tuple[str, ...] = ()
    required_embedded_media_extensions: tuple[str, ...] = ()
    confidence: float = 0.0
    valid: bool = False

    @property
    def allow_web_tools(self) -> bool:
        return self.web_action in {"open_url", "live_research", "browser_interaction"}

    @property
    def allow_scheduler_tools(self) -> bool:
        return self.scheduler_action in {"inspect", "run", "mutate"}

    @property
    def allow_connector_tools(self) -> bool:
        return self.skill_pack_action == "connector" and self.connector_source_user_requested

    @property
    def allow_skill_pack_tools(self) -> bool:
        return self.skill_pack_action in {"reference", "connector"}


class ScopedTurnToolRegistry:
    """Read-through registry view that hides tools not backed by turn evidence."""

    def __init__(
        self,
        delegate,
        *,
        evidence: TurnToolEvidence,
        tool_scope_decision: TurnToolScopeDecision | None = None,
    ) -> None:
        self._delegate = delegate
        self._evidence = evidence
        self.turn_tool_scope_decision = tool_scope_decision or TurnToolScopeDecision()
        self._active_connector_read_tools: frozenset[str] | None = None

    def _delegate_tool_names(self) -> set[str]:
        try:
            names = {str(getattr(spec, "name", "") or "") for spec in self._delegate.list_specs()}
        except Exception:
            try:
                names = {str(definition.get("name") or "") for definition in self._delegate.list_tool_definitions()}
            except Exception:
                return set()
        can_invoke = getattr(self._delegate, "can_invoke_tool", None)
        if callable(can_invoke):
            names = {name for name in names if can_invoke(name)}
        return {name for name in names if name}

    def _artifact_scheduler_scope_is_ambiguous(self) -> bool:
        if self.turn_tool_scope_decision.valid:
            return False
        requested_extensions = set(self._evidence.requested_extensions)
        if len(requested_extensions) <= 1:
            return False
        # File-format evidence is a structured request for artifact tools. The
        # mere presence of scheduler tools in the runtime registry is not enough
        # to hide those artifact tools; scheduler mutation/run tools remain gated
        # below unless a typed scope decision asks for them.
        return False

    def _continuation_tool_names_for_exact_tools(
        self,
        tool_names: Iterable[str],
        *,
        available: set[str],
        capability: str,
    ) -> tuple[str, ...]:
        continuations: list[str] = []
        for tool_name in dict.fromkeys(str(name or "").strip() for name in tool_names if str(name or "").strip()):
            try:
                spec = self._delegate.get_spec(tool_name)
            except Exception:
                spec = None
            for continuation in getattr(spec, "continuation_tools", ()) or ():
                continuation_name = str(continuation or "").strip()
                if continuation_name and continuation_name in available:
                    continuations.append(continuation_name)
            if capability == "web" and tool_name == "web_search":
                continuations.extend(name for name in ("web_fetch", *_BROWSER_INTERACTION_SCOPE_TOOLS) if name in available)
        return tuple(dict.fromkeys(continuations))

    def can_invoke_tool(self, name: str) -> bool:
        tool_name = str(name or "").strip()
        if tool_name == _SCOPE_REQUEST_TOOL_NAME:
            return True
        if not tool_name or not self._is_allowed_tool_name(tool_name):
            return False
        can_invoke = getattr(self._delegate, "can_invoke_tool", None)
        if callable(can_invoke):
            return bool(can_invoke(tool_name))
        return tool_name in self._delegate_tool_names()

    @staticmethod
    def _definition_tags(definition: dict[str, object]) -> frozenset[str]:
        return frozenset(
            str(tag).strip().lower()
            for tag in (definition.get("capability_tags") or ())
            if str(tag).strip()
        )

    @staticmethod
    def _definition_with_visible_continuations(
        definition: dict[str, object],
        visible_tool_names: frozenset[str],
    ) -> dict[str, object]:
        sanitized = dict(definition)
        raw_continuations = sanitized.get("continuation_tools")
        if isinstance(raw_continuations, str):
            candidates = (raw_continuations,)
        elif isinstance(raw_continuations, (list, tuple, set)):
            candidates = raw_continuations
        else:
            sanitized.pop("continuation_tools", None)
            return sanitized
        tool_name = str(sanitized.get("name") or "").strip()
        continuations = [
            name
            for name in (str(candidate or "").strip() for candidate in candidates)
            if name and name in visible_tool_names and name != tool_name
        ]
        if continuations:
            sanitized["continuation_tools"] = list(dict.fromkeys(continuations))
        else:
            sanitized.pop("continuation_tools", None)
        return sanitized

    @staticmethod
    def _spec_tags(spec: object) -> frozenset[str]:
        return frozenset(
            str(tag).strip().lower()
            for tag in (getattr(spec, "capability_tags", ()) or ())
            if str(tag).strip()
        )

    def _is_scheduler_tool_name(self, tool_name: str) -> bool:
        if tool_name in CRON_EXECUTION_BLOCKED_TOOLS:
            return True
        try:
            tags = self._spec_tags(self._delegate.get_spec(tool_name))
        except KeyError:
            return False
        return bool(tags.intersection(CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS))

    def _is_connector_tool_name(self, tool_name: str) -> bool:
        if tool_name in _CONNECTOR_TOOLS:
            return True
        try:
            tags = self._spec_tags(self._delegate.get_spec(tool_name))
        except KeyError:
            return False
        return bool(tags.intersection(_CONNECTOR_CAPABILITY_TAGS))

    def _connector_app_id_from_invocation(self, invocation: ToolInvocation) -> str:
        tool_name = str(getattr(invocation, "tool_name", "") or "").strip()
        if tool_name in {"email_search", "email_read", "email_attachment_read", "email_send"}:
            return "google-mail"
        if tool_name in {"calendar_list", "calendar_create", "calendar_update", "calendar_respond", "calendar_delete"}:
            return "google-calendar"
        if tool_name == "connector_request":
            raw_url = invocation.arguments.get("url") if isinstance(invocation.arguments, dict) else None
            if isinstance(raw_url, str) and raw_url.strip():
                try:
                    from urllib.parse import urlparse

                    first_segment = (urlparse(raw_url.strip()).path or "").strip("/").split("/", 1)[0]
                    return _normalize_connector_app_id(first_segment)
                except Exception:
                    return ""
        return ""

    def _is_skill_pack_tool_name(self, tool_name: str) -> bool:
        if tool_name in _SKILL_PACK_TOOLS:
            return True
        try:
            tags = self._spec_tags(self._delegate.get_spec(tool_name))
        except KeyError:
            return False
        return bool(tags.intersection(_SKILL_PACK_CAPABILITY_TAGS))

    def _is_plain_independent_turn(self) -> bool:
        substantive_requested_tool_names = set(
            self.turn_tool_scope_decision.requested_tool_names
        ).difference(_PLAIN_DIRECT_SAFE_READ_TOOLS)
        return not (
            self._evidence.context_linked
            or self._evidence.has_url_target
            or self._evidence.has_attachments
            or self._evidence.has_email_address_target
            or self._evidence.artifact_requested
            or self._evidence.existing_named_artifact_requested
            or self.turn_tool_scope_decision.allow_web_tools
            or self.turn_tool_scope_decision.allow_scheduler_tools
            or self.turn_tool_scope_decision.allow_connector_tools
            or self.turn_tool_scope_decision.allow_skill_pack_tools
            or bool(substantive_requested_tool_names)
            or bool(self.turn_tool_scope_decision.requested_artifact_extensions)
            or bool(self.turn_tool_scope_decision.required_embedded_media_extensions)
        )

    def _requested_names_in(self, names: frozenset[str]) -> frozenset[str]:
        requested = frozenset(str(name or "") for name in self.turn_tool_scope_decision.requested_tool_names)
        return requested.intersection(names)

    def _active_connector_default_read_tools(self) -> frozenset[str]:
        if self._active_connector_read_tools is not None:
            return self._active_connector_read_tools
        read_tools = {
            name
            for name in _connector_structured_tools_from_context(_active_connector_provider_context())
            if name in _CONNECTOR_FIRST_CLASS_READ_TOOLS
        }
        self._active_connector_read_tools = frozenset(read_tools)
        return self._active_connector_read_tools

    def _should_collect_embedded_web_media_without_shell(self) -> bool:
        artifact_extensions = set(self.turn_tool_scope_decision.required_embedded_media_extensions)
        if not artifact_extensions.intersection(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS):
            return False
        if "browser_image_collect" not in self._delegate_tool_names():
            return False
        return True

    def _contextual_browser_screenshot_allowed(self) -> bool:
        return self._evidence.context_linked and self._evidence.has_prior_tool_scope("web")

    def _has_confirmed_connector_source_context(self) -> bool:
        return bool(
            self.turn_tool_scope_decision.connector_source_user_requested
            or (self._evidence.context_linked and self._evidence.has_prior_tool_scope("connector"))
        )

    def _browser_visual_capture_allowed(self, tool_name: str, requested_extensions: set[str]) -> bool:
        trusted_requested_extensions = set(self._evidence.requested_extensions)
        if self._evidence.artifact_requested or self.turn_tool_scope_decision.valid:
            trusted_requested_extensions.update(requested_extensions)
        trusted_embedded_extensions = (
            self.turn_tool_scope_decision.required_embedded_media_extensions
            if self._evidence.artifact_requested or self.turn_tool_scope_decision.valid
            else ()
        )
        if tool_name == "browser_image_collect":
            return self._should_collect_embedded_web_media_without_shell()
        if tool_name == "browser_screenshot":
            return (
                _browser_visual_capture_allowed_by_extensions(
                    tool_name,
                    requested_extensions=trusted_requested_extensions,
                    embedded_media_extensions=trusted_embedded_extensions,
                )
                or self._should_collect_embedded_web_media_without_shell()
                or self._contextual_browser_screenshot_allowed()
            )
        return True

    def _scope_request_allows_artifact_tools(
        self,
        *,
        capabilities: Iterable[str],
        artifact_extensions: Iterable[str] = (),
        embedded_media_extensions: Iterable[str],
    ) -> bool:
        capability_set = {str(capability or "").strip().lower() for capability in capabilities}
        existing = self.turn_tool_scope_decision
        return bool(
            self._evidence.artifact_requested
            or self._evidence.has_url_target
            or self._evidence.has_attachments
            or existing.requested_artifact_extensions
            or existing.required_embedded_media_extensions
            or tuple(artifact_extensions)
            or tuple(embedded_media_extensions)
            or capability_set.intersection({"web", "image_generation"})
        )

    def _is_allowed_tool_name(self, tool_name: str) -> bool:
        requested_extensions = set(self._evidence.requested_extensions).union(
            set(self.turn_tool_scope_decision.requested_artifact_extensions)
        ).union(
            set(self.turn_tool_scope_decision.required_embedded_media_extensions)
        )
        existing_named_extensions = set(self._evidence.existing_named_artifact_extensions)
        if tool_name == _SCOPE_REQUEST_TOOL_NAME:
            return True
        if self._artifact_scheduler_scope_is_ambiguous():
            return False
        if existing_named_extensions and not self._evidence.existing_named_artifact_requires_new_content:
            if tool_name == "spreadsheet_create" and existing_named_extensions.intersection(_SPREADSHEET_EXTENSIONS):
                return False
            if tool_name == "presentation_create" and existing_named_extensions.intersection(_PRESENTATION_EXTENSIONS):
                return False
            if tool_name == "document_create" and existing_named_extensions.intersection(_DOCUMENT_EXTENSIONS):
                return False
            if tool_name == "pdf_create" and existing_named_extensions.intersection(_PDF_EXTENSIONS):
                return False
        if tool_name in _SOURCE_MEDIA_TOOLS and not (
            self._evidence.has_attachments or self._evidence.context_linked
        ):
            return False
        if (
            tool_name == "file_write"
            and requested_extensions
            and not set(requested_extensions).intersection(_TEXT_WRITE_EXTENSIONS)
            and any(extension not in _TEXT_WRITE_EXTENSIONS for extension in requested_extensions)
        ):
            return False
        if tool_name in _LOCAL_SHELL_TOOLS and self._should_collect_embedded_web_media_without_shell():
            return False
        if tool_name in _BROWSER_VISUAL_CAPTURE_TOOLS and not self._browser_visual_capture_allowed(
            tool_name,
            requested_extensions,
        ):
            return False
        scheduler_action = str(self.turn_tool_scope_decision.scheduler_action or "").strip().lower()
        requested_tool_names = set(self.turn_tool_scope_decision.requested_tool_names)
        is_scheduler_tool = self._is_scheduler_tool_name(tool_name)
        if (
            tool_name in _LOCAL_FILE_MUTATION_TOOLS
            and tool_name not in requested_tool_names
            and not (
                tool_name == "file_write"
                and requested_extensions.intersection(_TEXT_WRITE_EXTENSIONS)
            )
        ):
            return False
        if scheduler_action == "mutate" and not is_scheduler_tool:
            return tool_name in {"file_read", "file_search", "workspace_summary"} and self._evidence.has_attachments
        if (
            self._evidence.artifact_requested
            and tool_name in {"file_read", "file_search", "workspace_summary"}
            and not (
                self._evidence.has_attachments
                or self._evidence.context_linked
                or self._evidence.slash_prefixed_literal
                or self._evidence.existing_named_artifact_requested
            )
        ):
            if (
                tool_name in requested_tool_names
                and (
                    self._evidence.has_url_target
                    or self.turn_tool_scope_decision.allow_web_tools
                )
            ):
                return True
            return False
        if scheduler_action in {"inspect", "run"} and not is_scheduler_tool and tool_name not in requested_tool_names:
            allowed_by_other_scope = (
                (tool_name in _URL_BOUNDARY_TOOLS and (self._evidence.has_url_target or self.turn_tool_scope_decision.allow_web_tools))
                or (self._is_skill_pack_tool_name(tool_name) and self.turn_tool_scope_decision.allow_skill_pack_tools)
                or (self._is_connector_tool_name(tool_name) and self.turn_tool_scope_decision.allow_connector_tools)
                or (
                    tool_name in _CONVERSATION_HISTORY_TOOLS
                    and self._evidence.context_linked
                    and (
                        self._evidence.saved_history_available
                        or self._evidence.has_prior_tool_scope("conversation_history")
                    )
                )
            )
            if not allowed_by_other_scope:
                return False
        if is_scheduler_tool:
            allowed_scheduler_tools = _scheduler_tools_allowed_for_action(
                scheduler_action
            )
            requested_scheduler_tools = self._requested_names_in(_SCHEDULER_TOOLS).intersection(
                allowed_scheduler_tools
            )
            if requested_scheduler_tools:
                return tool_name in set(_scheduler_tools_with_required_read_scope(requested_scheduler_tools))
            if allowed_scheduler_tools:
                return tool_name in allowed_scheduler_tools
            if self._evidence.context_linked and self._evidence.has_prior_tool_scope("scheduler"):
                return tool_name in _SCHEDULER_READ_TOOLS
            return False
        if self._is_connector_tool_name(tool_name):
            requested_connector_tools = self._requested_names_in(_CONNECTOR_TYPED_TOOLS)
            if not self._has_confirmed_connector_source_context():
                return False
            if tool_name in _CONNECTOR_TOOLS and tool_name in requested_tool_names:
                return self.turn_tool_scope_decision.allow_connector_tools
            if (
                tool_name == "email_read"
                and not self._evidence.context_linked
                and "email_search" not in requested_connector_tools
            ):
                return False
            if requested_connector_tools:
                if not self.turn_tool_scope_decision.allow_connector_tools:
                    return False
                if (
                    tool_name == "email_read"
                    and "email_search" in requested_connector_tools
                    and "email_read" in self._active_connector_default_read_tools()
                ):
                    return True
                return tool_name in requested_connector_tools
            if self.turn_tool_scope_decision.allow_connector_tools:
                return False
            return self._evidence.context_linked and self._evidence.has_prior_tool_scope("connector")
        if self._is_skill_pack_tool_name(tool_name):
            return self.turn_tool_scope_decision.allow_skill_pack_tools or (
                self._evidence.context_linked
                and (
                    self._evidence.has_prior_tool_scope("skill_pack")
                    or self._evidence.has_prior_tool_scope("connector")
                    or self.turn_tool_scope_decision.allow_connector_tools
                )
            )
        if tool_name in _CONVERSATION_HISTORY_TOOLS:
            return (
                tool_name in requested_tool_names
                or (
                    self._evidence.context_linked
                    and (
                        self._evidence.saved_history_available
                        or self._evidence.has_prior_tool_scope("conversation_history")
                    )
                )
            )
        if tool_name in {"market_quote", "weather_forecast", "image_generate"}:
            return tool_name in requested_tool_names
        if tool_name == "file_read" and self._evidence.slash_prefixed_literal and not self._evidence.has_attachments:
            return False
        if tool_name in _URL_BOUNDARY_TOOLS:
            if (
                tool_name == "browser_screenshot"
                and self._evidence.has_url_target
                and ".png" in requested_extensions
            ):
                return True
            if tool_name == "browser_screenshot" and self._contextual_browser_screenshot_allowed():
                return True
            if (
                self._should_collect_embedded_web_media_without_shell()
                and tool_name in {"browser_image_collect", "web_fetch", *_DEFAULT_WEB_SCOPE_TOOLS}
            ):
                return True
            requested_url_tools = self._requested_names_in(_URL_BOUNDARY_TOOLS)
            if requested_url_tools:
                if not (
                    self._evidence.has_url_target
                    or self.turn_tool_scope_decision.allow_web_tools
                    or self._should_collect_embedded_web_media_without_shell()
                ):
                    return False
                if (
                    "browser_image_collect" in requested_url_tools
                    and self._should_collect_embedded_web_media_without_shell()
                    and tool_name in _DEFAULT_WEB_SCOPE_TOOLS
                    ):
                        return True
                return tool_name in requested_url_tools
            if tool_name not in _DEFAULT_WEB_SCOPE_TOOLS:
                return False
            if (
                str(self.turn_tool_scope_decision.web_action or "").strip().lower() == "open_url"
                and not self.turn_tool_scope_decision.requested_tool_names
            ):
                return (
                    tool_name in _OPEN_URL_SCOPE_TOOLS
                    and (self._evidence.has_url_target or self.turn_tool_scope_decision.allow_web_tools)
                )
            return self._evidence.has_url_target or self.turn_tool_scope_decision.allow_web_tools
        if tool_name in {"pdf_create", "pdf_edit"}:
            return bool(requested_extensions.intersection(_PDF_EXTENSIONS))
        if tool_name == "presentation_create":
            return bool(requested_extensions.intersection(_PRESENTATION_EXTENSIONS))
        if tool_name == "spreadsheet_create":
            if requested_extensions.intersection({".csv", ".tsv"}) and tool_name in requested_tool_names:
                return True
            return bool(requested_extensions.intersection(_SPREADSHEET_EXTENSIONS))
        if tool_name == "document_create":
            return bool(requested_extensions.intersection(_DOCUMENT_EXTENSIONS))
        if tool_name in _LOCAL_SHELL_TOOLS:
            return tool_name in requested_tool_names
        if tool_name in requested_tool_names and not self._is_connector_tool_name(tool_name):
            return True
        if self._evidence.artifact_requested:
            if tool_name == "file_write":
                return bool(requested_extensions.intersection(_TEXT_WRITE_EXTENSIONS))
            return False
        if self._evidence.context_linked:
            return False
        if self._is_plain_independent_turn():
            return tool_name == _SCOPE_REQUEST_TOOL_NAME
        return True

    def get_spec(self, name: str):
        if name == _SCOPE_REQUEST_TOOL_NAME:
            return _SCOPE_REQUEST_TOOL_SPEC
        if not self.can_invoke_tool(name):
            raise KeyError(f"Unknown tool: {name}")
        return self._delegate.get_spec(name)

    def list_specs(self) -> list[object]:
        specs = [
            spec
            for spec in self._delegate.list_specs()
            if self.can_invoke_tool(str(getattr(spec, "name", "") or ""))
        ]
        if self._is_allowed_tool_name(_SCOPE_REQUEST_TOOL_NAME):
            specs.insert(0, _SCOPE_REQUEST_TOOL_SPEC)
        return specs

    def list_tool_definitions(self, *args, **kwargs) -> list[dict[str, object]]:
        definitions = [
            definition
            for definition in self._delegate.list_tool_definitions(*args, **kwargs)
            if self.can_invoke_tool(str(definition.get("name") or ""))
        ]
        if self._is_allowed_tool_name(_SCOPE_REQUEST_TOOL_NAME):
            definitions.insert(
                0,
                {
                    "name": _SCOPE_REQUEST_TOOL_SPEC.name,
                    "description": _SCOPE_REQUEST_TOOL_SPEC.description,
                    "input_schema": _SCOPE_REQUEST_TOOL_SPEC.input_schema,
                    "capability_tags": list(_SCOPE_REQUEST_TOOL_SPEC.capability_tags),
                    "side_effect_class": _SCOPE_REQUEST_TOOL_SPEC.side_effect_class.value,
                    "risk_level": _SCOPE_REQUEST_TOOL_SPEC.risk_level.value,
                    "requires_approval": False,
                },
            )
        visible_tool_names = frozenset(str(definition.get("name") or "") for definition in definitions)
        return [
            self._definition_with_visible_continuations(definition, visible_tool_names)
            for definition in definitions
        ]

    def _tool_names_for_scope_request(self, arguments: dict[str, object]) -> tuple[str, ...]:
        raw_capabilities = arguments.get("capabilities")
        capabilities = tuple(
            dict.fromkeys(
                _normalize_scope_capability(value)
                for value in (raw_capabilities if isinstance(raw_capabilities, list) else ())
                if _normalize_scope_capability(value)
            )
        )
        explicit_scheduler_action = str(arguments.get("scheduler_action") or "").strip().lower()
        if explicit_scheduler_action in {"inspect", "run", "mutate"}:
            scheduler_capability = {
                "inspect": "scheduler_read",
                "run": "scheduler_run",
                "mutate": "scheduler_mutate",
            }[explicit_scheduler_action]
            capabilities = tuple(
                dict.fromkeys(
                    [
                        *(
                            capability
                            for capability in capabilities
                            if capability not in {"scheduler_read", "scheduler_run", "scheduler_mutate"}
                        ),
                        scheduler_capability,
                    ]
                )
            )
        raw_tool_names = arguments.get("tool_names")
        exact_tool_names = tuple(
            dict.fromkeys(
                str(value or "").strip()
                for value in (raw_tool_names if isinstance(raw_tool_names, list) else ())
                if str(value or "").strip()
            )
        )
        scheduler_scope_must_stay_read_only = (
            self._evidence.has_prior_tool_scope("scheduler_mutate")
            and set(capabilities).intersection({"scheduler_run", "scheduler_mutate"})
        )
        if scheduler_scope_must_stay_read_only:
            capabilities = tuple(
                dict.fromkeys(
                    [
                        *(
                            capability
                            for capability in capabilities
                            if capability not in {"scheduler_run", "scheduler_mutate"}
                        ),
                        "scheduler_read",
                    ]
                )
            )
            exact_tool_names = _filter_scheduler_requested_tool_names_for_action(exact_tool_names, "inspect")
        embedded_media_extensions = _validated_scope_request_embedded_media_extensions(arguments)
        validated_artifact_extensions = list(
            _validated_artifact_extensions(arguments.get("artifact_extensions"))
        )
        requested_artifact_extensions = (
            *validated_artifact_extensions,
            *(
                extension
                for extension in embedded_media_extensions
                if extension not in validated_artifact_extensions
            ),
        )
        artifact_scope_allowed = self._scope_request_allows_artifact_tools(
            capabilities=capabilities,
            artifact_extensions=requested_artifact_extensions,
            embedded_media_extensions=embedded_media_extensions,
        )
        if not artifact_scope_allowed:
            requested_artifact_extensions = ()
        if (
            "local_files" in capabilities
            and set(exact_tool_names).intersection(_LOCAL_FILE_LISTING_TOOLS)
            and not self._evidence.artifact_requested
            and not self._evidence.has_attachments
            and not self._evidence.has_url_target
            and not embedded_media_extensions
        ):
            requested_artifact_extensions = ()
        local_file_write_tools_requested = bool(
            set(exact_tool_names).intersection({"archive_create", "file_patch", "file_write"})
        )
        local_file_listing_without_artifact_request = (
            "local_files" in capabilities
            and not self._evidence.artifact_requested
            and not self._evidence.has_attachments
            and not self._evidence.has_url_target
            and not requested_artifact_extensions
            and not embedded_media_extensions
            and not local_file_write_tools_requested
        )
        requested: list[str] = []
        available = self._delegate_tool_names()
        raw_connector_app_ids = arguments.get("connector_app_ids")
        requested_connector_app_ids = _unique_connector_app_ids(
            raw_connector_app_ids if isinstance(raw_connector_app_ids, list) else ()
        )
        connector_specific_exact_tools = set(exact_tool_names).intersection(
            (_CONNECTOR_TYPED_TOOLS - _CONNECTOR_TOOLS) | _CONNECTOR_FIRST_CLASS_WRITE_TOOLS
        )
        generic_connector_scope_requested = (
            bool(arguments.get("source_user_requested"))
            and "connector" in capabilities
            and not requested_connector_app_ids
            and not connector_specific_exact_tools
        )
        generic_single_connector_source_selected = False
        if generic_connector_scope_requested:
            active_app_ids = _active_connector_app_ids_from_context(_active_connector_provider_context())
            generic_single_connector_source_selected = len(active_app_ids) == 1
        source_request_has_selected_target = bool(
            requested_connector_app_ids
            or connector_specific_exact_tools
            or self._evidence.numbered_option_selected
            or generic_single_connector_source_selected
        )
        connector_source_user_requested = (
            (bool(arguments.get("source_user_requested")) and source_request_has_selected_target)
            or (self._evidence.context_linked and self._evidence.has_prior_tool_scope("connector"))
            or self.turn_tool_scope_decision.connector_source_user_requested
        )
        connector_account_workflow = connector_source_user_requested and (
            "connector" in capabilities or self.turn_tool_scope_decision.connector_source_user_requested
        )
        scheduler_mutation_scope = "scheduler_mutate" in capabilities
        scheduler_run_scope = "scheduler_run" in capabilities
        if scheduler_run_scope:
            requested_artifact_extensions = ()
            embedded_media_extensions = ()
        suppress_web_artifact_shell = (
            bool(set(requested_artifact_extensions).intersection(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS))
            and "browser_image_collect" in available
            and (self._evidence.has_url_target or "web" in capabilities or self.turn_tool_scope_decision.allow_web_tools)
        )
        for capability in capabilities:
            if scheduler_run_scope and capability not in {"scheduler_run", "scheduler_read"}:
                continue
            if scheduler_mutation_scope and capability not in {"scheduler_mutate", "scheduler_read"}:
                if capability == "local_files" and self._evidence.has_attachments:
                    requested.extend(["file_read", "file_search", "workspace_summary"])
                continue
            if capability == "local_files":
                requested.extend(sorted(_LOCAL_FILE_TOOLS | _ARCHIVE_TOOLS))
                exact_local_tools = _exact_scope_tools_for_capability(capability, exact_tool_names)
                if local_file_listing_without_artifact_request:
                    exact_local_tools = tuple(
                        tool_name for tool_name in exact_local_tools if tool_name not in _STRUCTURED_ARTIFACT_TOOLS
                    )
                requested.extend(exact_local_tools)
                for extension in requested_artifact_extensions:
                    requested.extend(_ARTIFACT_EXTENSION_TOOLS.get(extension, ()))
                continue
            if capability == "local_shell" and (
                suppress_web_artifact_shell
                or connector_account_workflow
                or "connector" in capabilities
            ):
                continue
            exact_for_capability = _exact_scope_tools_for_capability(capability, exact_tool_names)
            if exact_for_capability:
                exact_requested_extensions = set(self._evidence.requested_extensions).union(
                    requested_artifact_extensions
                )
                exact_for_capability = tuple(
                    tool_name
                    for tool_name in exact_for_capability
                    if tool_name not in _BROWSER_VISUAL_CAPTURE_TOOLS
                    or self._browser_visual_capture_allowed(tool_name, exact_requested_extensions)
                )
            if exact_for_capability:
                exact_available = tuple(tool_name for tool_name in exact_for_capability if tool_name in available)
                if exact_available:
                    if capability in {"scheduler_run", "scheduler_mutate"}:
                        requested.extend(_scheduler_tools_with_required_read_scope(exact_available))
                    else:
                        requested.extend(exact_available)
                        requested.extend(
                            self._continuation_tool_names_for_exact_tools(
                                exact_available,
                                available=available,
                                capability=capability,
                            )
                        )
                    if capability == "web" and any(
                        tool_name.startswith("browser_") for tool_name in exact_available
                    ):
                        requested.extend(_BROWSER_INTERACTION_SCOPE_TOOLS)
                        if self._contextual_browser_screenshot_allowed():
                            requested.append("browser_screenshot")
                    continue
            if capability == "web":
                requested.extend(["web_search", "file_download", "web_fetch", *_BROWSER_INTERACTION_SCOPE_TOOLS])
                if self._contextual_browser_screenshot_allowed():
                    requested.append("browser_screenshot")
            elif capability == "scheduler_read":
                requested.extend(sorted(_SCHEDULER_READ_TOOLS))
            elif capability == "scheduler_run":
                requested.extend(sorted(_SCHEDULER_READ_TOOLS | _SCHEDULER_RUN_TOOLS))
            elif capability == "scheduler_mutate":
                requested.extend(
                    [
                        "list_crons",
                        "list_reminders",
                        "create_cron",
                        "update_cron",
                        "delete_cron",
                        "delete_reminder",
                        "toggle_cron",
                        "set_reminder",
                        "update_reminder",
                    ]
                )
            elif capability == "connector":
                if not connector_source_user_requested:
                    continue
                if exact_for_capability:
                    requested.extend(exact_for_capability)
                else:
                    requested.extend(sorted(_CONNECTOR_TOOLS | _SKILL_PACK_TOOLS))
            elif capability == "skill_pack":
                requested.extend(sorted(_SKILL_PACK_TOOLS))
            elif capability == "conversation_history":
                requested.extend(sorted(_CONVERSATION_HISTORY_TOOLS))
            elif capability == "weather":
                requested.append("weather_forecast")
            elif capability == "market_data":
                requested.append("market_quote")
            elif capability == "image_generation":
                requested.append("image_generate")
            elif capability == "local_files":
                requested.extend(sorted(_LOCAL_FILE_TOOLS | _ARCHIVE_TOOLS))
                for extension in requested_artifact_extensions:
                    requested.extend(_ARTIFACT_EXTENSION_TOOLS.get(extension, ()))
            elif capability == "local_shell":
                requested.extend(sorted(_LOCAL_SHELL_TOOLS))
        if not scheduler_mutation_scope:
            for extension in requested_artifact_extensions:
                requested.extend(_ARTIFACT_EXTENSION_TOOLS.get(extension, ()))
        web_media_artifact_extensions = set(requested_artifact_extensions).intersection(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS)
        if (
            not scheduler_mutation_scope
            and web_media_artifact_extensions
            and "browser_image_collect" in available
            and (self._evidence.has_url_target or "web" in capabilities or self.turn_tool_scope_decision.allow_web_tools)
        ):
            requested.append("browser_image_collect")
            requested.extend(_BROWSER_INTERACTION_SCOPE_TOOLS)
            requested.append("file_download")
            requested.append("web_fetch")
            requested = [tool_name for tool_name in requested if tool_name not in _LOCAL_SHELL_TOOLS]
        if (
            not scheduler_mutation_scope
            and ".png" in set(requested_artifact_extensions)
            and "browser_screenshot" in available
            and (self._evidence.has_url_target or "web" in capabilities or self.turn_tool_scope_decision.allow_web_tools)
        ):
            requested.append("browser_screenshot")
            requested.extend(_BROWSER_INTERACTION_SCOPE_TOOLS)
            requested.append("file_download")
            requested.append("web_fetch")
            requested = [tool_name for tool_name in requested if tool_name not in _LOCAL_SHELL_TOOLS]
        if (
            requested_artifact_extensions
            and not set(requested_artifact_extensions).intersection(_TEXT_WRITE_EXTENSIONS)
            and any(extension not in _TEXT_WRITE_EXTENSIONS for extension in requested_artifact_extensions)
        ):
            requested = [tool_name for tool_name in requested if tool_name != "file_write"]
        if _dedicated_artifact_tool_names_for_extensions(
            requested_artifact_extensions,
            available_tools=available,
        ):
            requested = [tool_name for tool_name in requested if tool_name not in _LOCAL_SHELL_TOOLS]
        if local_file_listing_without_artifact_request:
            requested = [
                tool_name
                for tool_name in requested
                if tool_name not in {"archive_create", "file_patch", "file_write"}
            ]
        if not artifact_scope_allowed:
            requested = [tool_name for tool_name in requested if tool_name not in _STRUCTURED_ARTIFACT_TOOLS]
        if (
            "local_files" in capabilities
            and "terminal_exec" in available
            and not scheduler_mutation_scope
            and not connector_account_workflow
            and "connector" not in capabilities
            and not requested_artifact_extensions
            and not embedded_media_extensions
        ):
            requested.append("terminal_exec")
        return _with_local_artifact_companion_tools(requested, available_tools=available)

    def _required_tool_names_for_scope_request(
        self,
        arguments: dict[str, object],
        available_tools: Iterable[str],
    ) -> tuple[str, ...]:
        available = set(available_tools)
        raw_tool_names = arguments.get("tool_names")
        exact_tool_names = tuple(
            dict.fromkeys(
                str(value or "").strip()
                for value in (raw_tool_names if isinstance(raw_tool_names, list) else ())
                if str(value or "").strip()
            )
        )
        raw_required_tool_names = arguments.get("required_tool_names")
        explicit_required_tool_names = {
            str(value or "").strip()
            for value in (
                raw_required_tool_names if isinstance(raw_required_tool_names, list) else ()
            )
            if str(value or "").strip()
        }
        embedded_media_extensions = _validated_scope_request_embedded_media_extensions(arguments)
        validated_artifact_extensions = list(
            _validated_artifact_extensions(arguments.get("artifact_extensions"))
        )
        requested_artifact_extensions = (
            *validated_artifact_extensions,
            *(
                extension
                for extension in embedded_media_extensions
                if extension not in validated_artifact_extensions
            ),
        )
        dedicated_artifact_tools = _dedicated_artifact_tool_names_for_extensions(
            requested_artifact_extensions,
            available_tools=available,
        )
        connector_write_tools = [
            tool_name for tool_name in exact_tool_names if tool_name in _CONNECTOR_FIRST_CLASS_WRITE_TOOLS
        ]
        implicit_required_candidates = _SCOPE_ACTION_REQUIRED_TOOL_CANDIDATES | (
            _CONNECTOR_FIRST_CLASS_WRITE_TOOLS if len(connector_write_tools) == 1 else frozenset()
        )
        return tuple(
            tool_name
            for tool_name in exact_tool_names
            if (
                tool_name in implicit_required_candidates
                or (
                    tool_name in explicit_required_tool_names
                    and tool_name in _SCOPE_REQUIRED_TOOL_CANDIDATES
                )
            )
            and (tool_name not in _LOCAL_SHELL_TOOLS or not dedicated_artifact_tools)
        )

    @staticmethod
    def _required_web_entry_tool_for_scope(
        tool_names: Iterable[str],
        required_tool_names: Iterable[str],
    ) -> tuple[str, ...]:
        """Require one callable public-web entry point when web scope is selected.

        The capability itself is typed model output. Once selected, it must not
        silently degrade into weather/market-only work while claiming that web
        research was part of the completed request.
        """

        available = {str(name or "").strip() for name in tool_names if str(name or "").strip()}
        required = {
            str(name or "").strip()
            for name in required_tool_names
            if str(name or "").strip()
        }
        web_source_tools = {"web_fetch", "browser_navigate", "browser_open"}
        if required.intersection(web_source_tools):
            return ()
        for candidate in ("web_fetch", "browser_navigate", "browser_open"):
            if candidate in available:
                return (candidate,)
        return ()

    def _safe_connector_read_tools_for_active_sources(
        self,
        active_connector_providers: Iterable[object],
    ) -> tuple[str, ...]:
        available = self._delegate_tool_names()
        structured_tools = _connector_structured_tools_from_context(active_connector_providers)
        return tuple(
            sorted(
                tool_name
                for tool_name in structured_tools.intersection(_CONNECTOR_FIRST_CLASS_READ_TOOLS)
                if tool_name in available
            )
        )

    def apply_scope_request(self, invocation: ToolInvocation) -> tuple[ToolResult, "ScopedTurnToolRegistry"]:
        scope_arguments = dict(invocation.arguments)
        explicit_artifact_extensions = _validated_artifact_extensions(scope_arguments.get("artifact_extensions"))
        explicit_embedded_extensions = _validated_scope_request_embedded_media_extensions(scope_arguments)
        explicit_excluded_extensions = _validated_artifact_extensions(
            scope_arguments.get("excluded_artifact_extensions")
        )
        if not explicit_artifact_extensions and not explicit_embedded_extensions:
            typed_artifact_extensions = tuple(
                dict.fromkeys(
                    [
                        *self.turn_tool_scope_decision.requested_artifact_extensions,
                        *self.turn_tool_scope_decision.required_embedded_media_extensions,
                    ]
                )
            )
            inherited_artifact_extensions = (
                typed_artifact_extensions
                if self.turn_tool_scope_decision.valid and typed_artifact_extensions
                else tuple(dict.fromkeys(self._evidence.requested_extensions))
            )
            if inherited_artifact_extensions:
                scope_arguments["artifact_extensions"] = list(inherited_artifact_extensions)
        tool_names = self._tool_names_for_scope_request(scope_arguments)
        embedded_media_extensions = _validated_scope_request_embedded_media_extensions(scope_arguments)
        artifact_extensions = tuple(
            dict.fromkeys(
                [
                    *_validated_artifact_extensions(scope_arguments.get("artifact_extensions")),
                    *embedded_media_extensions,
                ]
            )
        )
        excluded_artifact_extensions = tuple(
            dict.fromkeys(
                [
                    *self.turn_tool_scope_decision.excluded_artifact_extensions,
                    *explicit_excluded_extensions,
                ]
            )
        )
        if excluded_artifact_extensions:
            excluded_set = set(excluded_artifact_extensions)
            artifact_extensions = tuple(
                extension for extension in artifact_extensions if extension not in excluded_set
            )
            embedded_media_extensions = tuple(
                extension for extension in embedded_media_extensions if extension not in excluded_set
            )
        required_tool_names = self._required_tool_names_for_scope_request(
            scope_arguments,
            tool_names,
        )
        if embedded_media_extensions and not _scope_tool_names_can_source_embedded_media(tool_names):
            embedded_media_extensions = ()
        raw_capabilities = invocation.arguments.get("capabilities")
        capabilities = {
            _normalize_scope_capability(value)
            for value in (raw_capabilities if isinstance(raw_capabilities, list) else ())
            if _normalize_scope_capability(value)
        }
        explicit_scheduler_action = str(invocation.arguments.get("scheduler_action") or "").strip().lower()
        if explicit_scheduler_action in {"inspect", "run", "mutate"}:
            capabilities.difference_update({"scheduler_read", "scheduler_run", "scheduler_mutate"})
            capabilities.add(
                {
                    "inspect": "scheduler_read",
                    "run": "scheduler_run",
                    "mutate": "scheduler_mutate",
                }[explicit_scheduler_action]
            )
        requested_scheduler_selection_policy = str(
            invocation.arguments.get("scheduler_selection_policy") or "none"
        ).strip().lower()
        if requested_scheduler_selection_policy not in {"none", "user_selected", "delegate_one"}:
            requested_scheduler_selection_policy = "none"
        requested_scheduler_target_scope = str(
            invocation.arguments.get("scheduler_target_scope") or "none"
        ).strip().lower()
        if requested_scheduler_target_scope not in {"none", "all_current_workspace"}:
            requested_scheduler_target_scope = "none"
        requested_scheduler_toggle_enabled = (
            invocation.arguments.get("scheduler_toggle_enabled")
            if isinstance(invocation.arguments.get("scheduler_toggle_enabled"), bool)
            else None
        )
        scheduler_scope_must_stay_read_only = (
            self._evidence.has_prior_tool_scope("scheduler_mutate")
            and capabilities.intersection({"scheduler_run", "scheduler_mutate"})
        )
        if scheduler_scope_must_stay_read_only:
            capabilities = set(capabilities)
            capabilities.difference_update({"scheduler_run", "scheduler_mutate"})
            capabilities.add("scheduler_read")
            tool_names = _filter_scheduler_requested_tool_names_for_action(tool_names, "inspect")
            required_tool_names = _filter_scheduler_requested_tool_names_for_action(required_tool_names, "inspect")
            requested_scheduler_selection_policy = "none"
            requested_scheduler_target_scope = "none"
            requested_scheduler_toggle_enabled = None
        scheduler_mutation_scope = "scheduler_mutate" in capabilities
        if "web" in capabilities and not scheduler_mutation_scope:
            required_tool_names = tuple(
                dict.fromkeys(
                    [
                        *required_tool_names,
                        *self._required_web_entry_tool_for_scope(tool_names, required_tool_names),
                    ]
                )
            )
        if scheduler_mutation_scope:
            artifact_extensions = ()
            embedded_media_extensions = ()
        elif not self._scope_request_allows_artifact_tools(
            capabilities=capabilities,
            artifact_extensions=artifact_extensions,
            embedded_media_extensions=embedded_media_extensions,
        ):
            artifact_extensions = ()
            embedded_media_extensions = ()
        raw_connector_app_ids = invocation.arguments.get("connector_app_ids")
        requested_connector_app_ids = _unique_connector_app_ids(
            raw_connector_app_ids if isinstance(raw_connector_app_ids, list) else ()
        )
        connector_app_ids = requested_connector_app_ids
        raw_tool_names = invocation.arguments.get("tool_names")
        explicitly_requested_tools = {
            str(value or "").strip()
            for value in (raw_tool_names if isinstance(raw_tool_names, list) else ())
            if str(value or "").strip()
        }
        connector_specific_requested_tools = explicitly_requested_tools.intersection(
            (_CONNECTOR_TYPED_TOOLS - _CONNECTOR_TOOLS) | _CONNECTOR_FIRST_CLASS_WRITE_TOOLS
        )
        typed_connector_tool_requested = bool(
            explicitly_requested_tools.intersection(_CONNECTOR_TYPED_TOOLS - _CONNECTOR_TOOLS)
        )
        connector_write_tool_requested = bool(explicitly_requested_tools.intersection(_CONNECTOR_FIRST_CLASS_WRITE_TOOLS))
        requested_connector_app_matches_typed_tools = _requested_connector_app_ids_match_typed_tools(
            requested_connector_app_ids,
            explicitly_requested_tools,
        )
        generic_connector_scope_requested = bool(
            "connector" in capabilities
            and invocation.arguments.get("source_user_requested")
            and not connector_specific_requested_tools
            and not requested_connector_app_ids
        )
        active_connector_providers = _active_connector_provider_context() if "connector" in capabilities else ()
        active_app_ids = _active_connector_app_ids_from_context(active_connector_providers)
        generic_single_connector_source_selected = generic_connector_scope_requested and len(active_app_ids) == 1
        source_request_has_selected_target = bool(
            requested_connector_app_ids
            or typed_connector_tool_requested
            or connector_write_tool_requested
            or self._evidence.numbered_option_selected
            or generic_single_connector_source_selected
        )
        source_evidence = str(invocation.arguments.get("source_evidence") or "").strip()
        normalized_source_evidence = " ".join(source_evidence.split()).casefold()
        normalized_current_message = " ".join(self._evidence.current_user_message.split()).casefold()
        source_evidence_verified = bool(
            normalized_source_evidence
            and normalized_current_message
            and normalized_source_evidence in normalized_current_message
        )
        connector_source_user_requested = (
            (
                bool(invocation.arguments.get("source_user_requested"))
                and source_request_has_selected_target
                and source_evidence_verified
            )
            or (self._evidence.context_linked and self._evidence.has_prior_tool_scope("connector"))
            or self.turn_tool_scope_decision.connector_source_user_requested
        )
        connector_account_workflow = connector_source_user_requested and (
            "connector" in capabilities or self.turn_tool_scope_decision.connector_source_user_requested
        )
        if connector_account_workflow:
            tool_names = tuple(tool_name for tool_name in tool_names if tool_name not in _LOCAL_SHELL_TOOLS)
            required_tool_names = tuple(
                tool_name for tool_name in required_tool_names if tool_name not in _LOCAL_SHELL_TOOLS
            )
        has_active_app_ids = bool(active_app_ids)
        active_provider_ids = {
            _normalize_connector_app_id(provider.get("provider_id"))
            for provider in active_connector_providers
            if isinstance(provider, dict)
        }
        requested_connector_provider_id_match = bool(requested_connector_app_ids) and all(
            app_id in active_provider_ids for app_id in requested_connector_app_ids
        )
        connector_structured_tools = (
            _connector_structured_tools_from_context(active_connector_providers)
            if "connector" in capabilities
            else set()
        )
        unmatched_requested_connector_app = False
        if "connector" in capabilities and not active_connector_providers:
            tool_names = tuple(
                tool_name
                for tool_name in tool_names
                if tool_name in explicitly_requested_tools
                or tool_name not in (_CONNECTOR_TYPED_TOOLS | _SKILL_PACK_TOOLS)
            )
        if "connector" in capabilities and not connector_source_user_requested:
            tool_names = tuple(
                tool_name
                for tool_name in tool_names
                if tool_name not in (_CONNECTOR_TYPED_TOOLS | _CONNECTOR_TOOLS | _SKILL_PACK_TOOLS)
            )
            connector_app_ids = ()
        if (
            "local_files" in capabilities
            and explicitly_requested_tools.intersection(_LOCAL_FILE_LISTING_TOOLS)
            and not self._evidence.artifact_requested
            and not self._evidence.has_attachments
            and not self._evidence.has_url_target
            and not embedded_media_extensions
        ):
            artifact_extensions = ()
            tool_names = tuple(tool_name for tool_name in tool_names if tool_name not in _STRUCTURED_ARTIFACT_TOOLS)
        if "connector" in capabilities and connector_source_user_requested:
            connector_app_ids = (
                tuple(app_id for app_id in connector_app_ids if app_id in set(active_app_ids))
                if has_active_app_ids
                else ()
            )
            if not connector_app_ids and typed_connector_tool_requested and has_active_app_ids:
                connector_app_ids = tuple(
                    dict.fromkeys(
                        app_id
                        for app_id in (
                            _connector_app_id_for_typed_tool(tool_name)
                            for tool_name in explicitly_requested_tools
                        )
                        if app_id and (not active_app_ids or app_id in set(active_app_ids))
                    )
                )
            unmatched_requested_connector_app = bool(requested_connector_app_ids) and not (
                bool(connector_app_ids)
                or requested_connector_provider_id_match
                or (not has_active_app_ids and requested_connector_app_matches_typed_tools)
            )
            if not connector_app_ids and not typed_connector_tool_requested and not unmatched_requested_connector_app:
                connector_app_ids = active_app_ids
            # Generic connector-provider ids are not app ids. Only connector
            # metadata that exposes concrete app ids, such as google-mail, can
            # narrow typed tools by app scope.
            selected_app_ids = set(connector_app_ids) if has_active_app_ids else set()
            generic_connector_without_selected_source = (
                not typed_connector_tool_requested
                and not connector_app_ids
                and not has_active_app_ids
                and not self._evidence.numbered_option_selected
            )
            if explicitly_requested_tools and typed_connector_tool_requested and not selected_app_ids and active_app_ids:
                default_connector_tools = set()
            elif unmatched_requested_connector_app:
                default_connector_tools = set()
            elif generic_connector_without_selected_source:
                default_connector_tools = set()
            elif explicitly_requested_tools:
                default_connector_tools = _default_connector_tools_for_scope(
                    connector_structured_tools,
                    explicitly_requested_tools=explicitly_requested_tools,
                )
            else:
                default_connector_tools = set(_CONNECTOR_TOOLS)
            allowed_connector_tools = set(default_connector_tools)
            email_attachment_read_requested = bool(
                "email_attachment_read" in explicitly_requested_tools
                or "email_attachment_read" in required_tool_names
                or self._evidence.has_attachments
                or artifact_extensions
                or embedded_media_extensions
                or self.turn_tool_scope_decision.requested_artifact_extensions
                or self.turn_tool_scope_decision.required_embedded_media_extensions
            )
            email_attachment_read_companion = bool(
                "email_read" in explicitly_requested_tools
                or "email_read" in required_tool_names
            )
            if not email_attachment_read_requested and not email_attachment_read_companion:
                allowed_connector_tools.discard("email_attachment_read")
                tool_names = tuple(tool_name for tool_name in tool_names if tool_name != "email_attachment_read")
            if not typed_connector_tool_requested:
                allowed_connector_tools.update(_SKILL_PACK_TOOLS)
                allowed_connector_tools.difference_update(_CONNECTOR_FIRST_CLASS_WRITE_TOOLS)
                if not email_attachment_read_requested and not email_attachment_read_companion:
                    allowed_connector_tools.discard("email_attachment_read")
            if connector_structured_tools:
                if (
                    "email_read" in allowed_connector_tools
                    and "email_search" in connector_structured_tools
                    and "email_search" in self._delegate_tool_names()
                    and not self._evidence.context_linked
                ):
                    allowed_connector_tools.add("email_search")
                    tool_names = tuple(dict.fromkeys([*tool_names, "email_search"]))
                if (
                    "email_search" in allowed_connector_tools
                    and "email_read" in connector_structured_tools
                    and "email_read" in self._delegate_tool_names()
                ):
                    allowed_connector_tools.add("email_read")
                    tool_names = tuple(dict.fromkeys([*tool_names, "email_read"]))
                if (
                    "email_read" in allowed_connector_tools
                    and "email_attachment_read" in connector_structured_tools
                    and "email_attachment_read" in self._delegate_tool_names()
                    and (email_attachment_read_requested or email_attachment_read_companion)
                ):
                    allowed_connector_tools.add("email_attachment_read")
                    tool_names = tuple(dict.fromkeys([*tool_names, "email_attachment_read"]))
                if (
                    "email_attachment_read" in allowed_connector_tools
                    and "email_read" in connector_structured_tools
                    and "email_read" in self._delegate_tool_names()
                ):
                    allowed_connector_tools.add("email_read")
                    tool_names = tuple(dict.fromkeys([*tool_names, "email_read"]))
                tool_names = tuple(
                    tool_name
                    for tool_name in tool_names
                    if (
                        tool_name not in (_CONNECTOR_TYPED_TOOLS | _SKILL_PACK_TOOLS)
                        or (
                            tool_name in allowed_connector_tools
                            and _connector_tool_allowed_for_app_scope(tool_name, selected_app_ids)
                        )
                    )
                )
        elif "connector" in capabilities:
            connector_app_ids = ()
        connector_read_source_tools = self._safe_connector_read_tools_for_active_sources(active_connector_providers)
        available_sources = _connector_read_source_summaries(
            active_connector_providers,
            available_tool_names=connector_read_source_tools,
        )
        source_selection_required = (
            "connector" in capabilities
            and bool(available_sources)
            and not bool(set(tool_names).intersection(_CONNECTOR_TYPED_TOOLS))
            and not connector_source_user_requested
        )
        connector_source_evidence_required = bool(
            "connector" in capabilities
            and (typed_connector_tool_requested or connector_write_tool_requested)
            and not connector_source_user_requested
        )
        typed_connector_missing_selected_app_scope = (
            "connector" in capabilities
            and typed_connector_tool_requested
            and has_active_app_ids
            and not connector_app_ids
        )
        missing_connector_app_scope = (
            "connector" in capabilities
            and (typed_connector_tool_requested or bool(requested_connector_app_ids))
            and not connector_source_evidence_required
            and (
                unmatched_requested_connector_app
                or not connector_source_user_requested
                or typed_connector_missing_selected_app_scope
            )
        )
        existing = self.turn_tool_scope_decision
        scheduler_action = existing.scheduler_action
        if "scheduler_mutate" in capabilities:
            scheduler_action = "mutate"
        elif "scheduler_run" in capabilities:
            scheduler_action = "run"
        elif "scheduler_read" in capabilities:
            scheduler_action = "inspect"
        scheduler_selection_policy = "none"
        if scheduler_action == "run":
            scheduler_selection_policy = existing.scheduler_selection_policy
            if requested_scheduler_selection_policy != "none":
                scheduler_selection_policy = requested_scheduler_selection_policy
        scheduler_target_scope = "none"
        if scheduler_action == "mutate":
            scheduler_target_scope = existing.scheduler_target_scope
            if requested_scheduler_target_scope != "none":
                scheduler_target_scope = requested_scheduler_target_scope
        scheduler_toggle_enabled = existing.scheduler_toggle_enabled
        if scheduler_action == "mutate" and requested_scheduler_toggle_enabled is not None:
            scheduler_toggle_enabled = requested_scheduler_toggle_enabled
        elif scheduler_action != "mutate":
            scheduler_toggle_enabled = None
        merged_requested_tool_names = _filter_scheduler_requested_tool_names_for_action(
            tuple(dict.fromkeys([*existing.requested_tool_names, *tool_names])),
            scheduler_action,
        )
        merged_required_tool_names = _filter_scheduler_requested_tool_names_for_action(
            tuple(dict.fromkeys([*existing.required_tool_names, *required_tool_names])),
            scheduler_action,
        )
        web_action = (
            existing.web_action
            if scheduler_mutation_scope
            else "live_research"
            if "web" in capabilities
            else existing.web_action
        )
        skill_pack_action = existing.skill_pack_action
        connector_tools_exposed = bool(set(tool_names).intersection(_CONNECTOR_TYPED_TOOLS | _SKILL_PACK_TOOLS))
        if (
            "connector" in capabilities
            and active_connector_providers
            and connector_tools_exposed
            and not missing_connector_app_scope
        ):
            skill_pack_action = "connector"
        elif "skill_pack" in capabilities:
            skill_pack_action = "reference"
        widened = ScopedTurnToolRegistry(
            self._delegate,
            evidence=self._evidence,
            tool_scope_decision=TurnToolScopeDecision(
                web_action=web_action,
                scheduler_action=scheduler_action,
                scheduler_toggle_enabled=scheduler_toggle_enabled,
                scheduler_selection_policy=scheduler_selection_policy,
                scheduler_target_scope=scheduler_target_scope,
                skill_pack_action=skill_pack_action,
                connector_app_ids=connector_app_ids,
                connector_source_user_requested=connector_source_user_requested,
                connector_source_evidence=(
                    source_evidence
                    if source_evidence_verified
                    else existing.connector_source_evidence
                ),
                requested_tool_names=merged_requested_tool_names,
                required_tool_names=merged_required_tool_names,
                requested_artifact_extensions=tuple(
                    extension
                    for extension in dict.fromkeys([*existing.requested_artifact_extensions, *artifact_extensions])
                    if extension not in set(excluded_artifact_extensions)
                ),
                excluded_artifact_extensions=excluded_artifact_extensions,
                required_embedded_media_extensions=tuple(
                    dict.fromkeys([*existing.required_embedded_media_extensions, *embedded_media_extensions])
                ),
                confidence=max(existing.confidence, 1.0),
                valid=True,
            ),
        )
        callable_tool_names = tuple(tool_name for tool_name in tool_names if widened.can_invoke_tool(tool_name))
        callable_required_tool_names = tuple(
            tool_name for tool_name in merged_required_tool_names if widened.can_invoke_tool(tool_name)
        )
        unavailable_required_tool_names = tuple(
            tool_name
            for tool_name in merged_required_tool_names
            if tool_name not in set(callable_required_tool_names)
        )
        connector_source_unavailable = bool(
            "connector" in capabilities
            and invocation.arguments.get("source_user_requested")
            and not active_connector_providers
        )
        unavailable_tools = tuple(
            sorted(_CONNECTOR_TYPED_TOOLS | _CONNECTOR_TOOLS)
            if connector_source_unavailable
            else ()
        )
        return (
            ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={
                    "scope_requested": True,
                    "capabilities": sorted(capabilities),
                    "scheduler_action": scheduler_action,
                    "scheduler_toggle_enabled": widened.turn_tool_scope_decision.scheduler_toggle_enabled,
                    "scheduler_selection_policy": widened.turn_tool_scope_decision.scheduler_selection_policy,
                    "scheduler_target_scope": widened.turn_tool_scope_decision.scheduler_target_scope,
                    "available_tools": list(callable_tool_names),
                    "required_tool_names": list(callable_required_tool_names),
                    "requested_required_tool_names": list(merged_required_tool_names),
                    "unavailable_required_tool_names": list(unavailable_required_tool_names),
                    "unavailable_tools": list(unavailable_tools),
                    "unavailable_capabilities": ["connector"] if connector_source_unavailable else [],
                    "connector_source_unavailable": connector_source_unavailable,
                    "artifact_extensions": list(artifact_extensions),
                    "excluded_artifact_extensions": list(excluded_artifact_extensions),
                    "embedded_media_artifact_extensions": list(embedded_media_extensions),
                    "connector_app_ids": list(connector_app_ids),
                    "available_sources": available_sources,
                    "source_selection_required": source_selection_required,
                    "connector_source_evidence_required": connector_source_evidence_required,
                    "source_evidence_verified": source_evidence_verified,
                    "active_connector_providers": list(active_connector_providers),
                    "missing_connector_app_scope": missing_connector_app_scope,
                    "message": (
                        "Connected-account tools were not exposed because the request did not include verified "
                        "current-turn source evidence. Retry only if the user selected that source, and include "
                        "an exact source_evidence substring from the current user turn."
                        if connector_source_evidence_required
                        else (
                        "Active connected-account read sources are available. Ask the user to choose a source "
                        "before reading account data, then call request_tool_scope again with exact tool_names."
                        if source_selection_required
                        else (
                        "Tool scope updated. Continue the same user request using the newly available tools."
                        if tool_names
                        else (
                            "Typed connector tools require connector_app_ids. Call request_tool_scope again "
                            "with a matching active app id such as google-mail for email or google-calendar "
                            "for calendar, or request a non-connector tool family if account access is not intended."
                        )
                        if missing_connector_app_scope
                        else "No registered tools matched the requested capability scope."
                        )
                        )
                    ),
                    "suppress_activity": True,
                },
            ),
            widened,
        )

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
        if invocation.tool_name == _SCOPE_REQUEST_TOOL_NAME:
            result, _ = self.apply_scope_request(invocation)
            return result
        if self._is_connector_tool_name(invocation.tool_name) and self.turn_tool_scope_decision.allow_connector_tools:
            app_id = self._connector_app_id_from_invocation(invocation)
            allowed_app_ids = set(self.turn_tool_scope_decision.connector_app_ids)
            if allowed_app_ids and app_id not in allowed_app_ids:
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="denied",
                    output={
                        "reason": "connector_app_not_in_turn_scope",
                        "connector_app_id": app_id,
                        "allowed_connector_app_ids": sorted(allowed_app_ids),
                        "suppress_activity": True,
                    },
                    error=f"Connector app is not in this turn scope: {app_id}",
                )
        if self.can_invoke_tool(invocation.tool_name):
            return self._delegate.invoke(invocation)
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="denied",
            output={
                "reason": "tool_requires_structured_turn_scope",
                "has_url_target": self._evidence.has_url_target,
                "has_attachments": self._evidence.has_attachments,
                "requested_extensions": list(self._evidence.requested_extensions),
                "context_linked": self._evidence.context_linked,
                "slash_prefixed_literal": self._evidence.slash_prefixed_literal,
                "web_action": self.turn_tool_scope_decision.web_action,
                "scheduler_action": self.turn_tool_scope_decision.scheduler_action,
                "skill_pack_action": self.turn_tool_scope_decision.skill_pack_action,
                "requested_tool_names": list(self.turn_tool_scope_decision.requested_tool_names),
                "required_tool_names": list(self.turn_tool_scope_decision.required_tool_names),
                "requested_artifact_extensions": list(self.turn_tool_scope_decision.requested_artifact_extensions),
                "suppress_activity": True,
            },
            error=f"Tool requires structured turn evidence: {invocation.tool_name}",
        )

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


def turn_is_context_linked(conversation_result: object | None) -> bool:
    turn = getattr(conversation_result, "turn", None)
    if getattr(turn, "parent_turn_id", None) is not None:
        return True
    disposition = getattr(turn, "disposition", None)
    disposition_value = str(getattr(disposition, "value", disposition) or "").strip()
    if disposition_value == ConversationTurnDisposition.INDEPENDENT.value:
        # The persisted turn relationship is authoritative. A stale task-frame
        # continuation hint must not reopen history or tool scope after /new.
        return False
    if disposition in _CONTEXT_LINK_DISPOSITIONS:
        return True
    try:
        normalized_disposition = ConversationTurnDisposition(str(disposition))
    except (TypeError, ValueError):
        normalized_disposition = None
    if normalized_disposition in _CONTEXT_LINK_DISPOSITIONS:
        return True
    continuation = getattr(conversation_result, "task_frame_continuation", None)
    mode = getattr(continuation, "mode", None)
    if mode is None:
        return False
    try:
        normalized_mode = TaskFrameContinuationMode(str(getattr(mode, "value", mode)))
    except (TypeError, ValueError):
        return False
    return normalized_mode is not TaskFrameContinuationMode.START_NEW


def should_include_prior_turn_messages(conversation_result: object | None, *, has_prior_turns: bool) -> bool:
    return bool(has_prior_turns and turn_is_context_linked(conversation_result))


def build_turn_tool_evidence(
    *,
    user_message: str,
    conversation_result: object | None,
    has_attachments: bool = False,
    requested_extensions: Iterable[str] | None = None,
    existing_named_artifact_extensions: Iterable[str] | None = None,
    existing_named_artifact_requires_new_content: bool = False,
    saved_history_available: bool = False,
    numbered_option_selected: bool = False,
    prior_tool_scopes: Iterable[str] | None = None,
) -> TurnToolEvidence:
    normalized_extensions = tuple(
        dict.fromkeys(
            extension
            for extension in (
                str(raw or "").strip().lower()
                for raw in (requested_extensions or ())
            )
            if extension.startswith(".")
        )
    )
    normalized_prior_tool_scopes = tuple(
        dict.fromkeys(
            scope
            for scope in (
                str(raw or "").strip().lower()
                for raw in (prior_tool_scopes or ())
            )
            if scope in _KNOWN_PRIOR_TOOL_SCOPES
        )
    )
    normalized_existing_named_artifact_extensions = tuple(
        dict.fromkeys(
            extension
            for extension in (
                str(raw or "").strip().lower()
                for raw in (existing_named_artifact_extensions or ())
            )
            if extension.startswith(".")
        )
    )
    return TurnToolEvidence(
        has_url_target=extract_url_target(user_message) is not None,
        has_attachments=bool(has_attachments),
        has_email_address_target=_has_email_address_target(user_message),
        requested_extensions=normalized_extensions,
        existing_named_artifact_extensions=normalized_existing_named_artifact_extensions,
        existing_named_artifact_requires_new_content=bool(existing_named_artifact_requires_new_content),
        context_linked=turn_is_context_linked(conversation_result),
        saved_history_available=bool(saved_history_available),
        slash_prefixed_literal=is_slash_prefixed_literal_message(user_message),
        numbered_option_selected=bool(numbered_option_selected),
        prior_tool_scopes=normalized_prior_tool_scopes,
        current_user_message=str(user_message or ""),
    )


def is_slash_prefixed_literal_message(user_message: object) -> bool:
    return str(user_message or "").strip().startswith("/")


def _registry_has_scoped_special_tools(registry) -> bool:
    try:
        specs = registry.list_specs()
    except Exception:
        specs = ()
    for spec in specs:
        name = str(getattr(spec, "name", "") or "")
        tags = ScopedTurnToolRegistry._spec_tags(spec)
        if (
            name in CRON_EXECUTION_BLOCKED_TOOLS
            or tags.intersection(CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS)
            or name in _URL_BOUNDARY_TOOLS
            or name in _CONNECTOR_TOOLS
            or tags.intersection(_CONNECTOR_CAPABILITY_TAGS)
            or name in _SKILL_PACK_TOOLS
            or tags.intersection(_SKILL_PACK_CAPABILITY_TAGS)
            or name in _LOCAL_SHELL_TOOLS
            or name in {"market_quote", "weather_forecast"}
            or tags.intersection(_DIRECT_PUBLIC_READ_CAPABILITY_TAGS)
        ):
            return True
    if specs:
        return False
    for definition in registry.list_tool_definitions():
        name = str(definition.get("name") or "")
        tags = ScopedTurnToolRegistry._definition_tags(definition)
        if (
            name in CRON_EXECUTION_BLOCKED_TOOLS
            or tags.intersection(CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS)
            or name in _URL_BOUNDARY_TOOLS
            or name in _CONNECTOR_TOOLS
            or tags.intersection(_CONNECTOR_CAPABILITY_TAGS)
            or name in _SKILL_PACK_TOOLS
            or tags.intersection(_SKILL_PACK_CAPABILITY_TAGS)
            or name in _LOCAL_SHELL_TOOLS
            or name in {"market_quote", "weather_forecast"}
            or tags.intersection(_DIRECT_PUBLIC_READ_CAPABILITY_TAGS)
        ):
            return True
    return False


def _extract_response_text(response: object) -> str:
    if not isinstance(response, dict):
        return ""
    content = response.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(block.get("text") or "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        )
    return ""


def _parse_turn_tool_scope_decision(text: str) -> TurnToolScopeDecision:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return TurnToolScopeDecision()
    try:
        payload = json.loads(text[start : end + 1])
    except Exception:
        return TurnToolScopeDecision()
    if not isinstance(payload, dict):
        return TurnToolScopeDecision()
    requested_outcome = str(payload.get("requested_outcome") or "unspecified").strip().lower()
    if requested_outcome not in _REQUESTED_OUTCOMES:
        requested_outcome = "unspecified"
    web_action = str(payload.get("web_action") or "none").strip().lower()
    if web_action not in _WEB_ACTIONS:
        web_action = "none"
    scheduler_action = str(payload.get("scheduler_action") or "none").strip().lower()
    if scheduler_action not in _SCHEDULER_ACTIONS:
        scheduler_action = "none"
    raw_scheduler_toggle_enabled = payload.get("scheduler_toggle_enabled")
    scheduler_toggle_enabled = (
        raw_scheduler_toggle_enabled
        if isinstance(raw_scheduler_toggle_enabled, bool)
        else None
    )
    scheduler_selection_policy = str(payload.get("scheduler_selection_policy") or "none").strip().lower()
    if scheduler_selection_policy not in {"none", "user_selected", "delegate_one"}:
        scheduler_selection_policy = "none"
    scheduler_target_scope = str(payload.get("scheduler_target_scope") or "none").strip().lower()
    if scheduler_target_scope not in {"none", "all_current_workspace"}:
        scheduler_target_scope = "none"
    skill_pack_action = str(payload.get("skill_pack_action") or "none").strip().lower()
    if skill_pack_action not in _SKILL_PACK_ACTIONS:
        skill_pack_action = "none"
    connector_app_ids = _unique_connector_app_ids(payload.get("connector_app_ids") if isinstance(payload.get("connector_app_ids"), list) else ())
    requested_tool_names = tuple(
        dict.fromkeys(
            str(tool_name or "").strip()
            for tool_name in (payload.get("requested_tool_names") if isinstance(payload.get("requested_tool_names"), list) else ())
            if str(tool_name or "").strip()
        )
    )
    required_tool_names = tuple(
        dict.fromkeys(
            str(tool_name or "").strip()
            for tool_name in (payload.get("required_tool_names") if isinstance(payload.get("required_tool_names"), list) else ())
            if str(tool_name or "").strip()
        )
    )
    excluded_artifact_extensions = _validated_artifact_extensions(
        payload.get("excluded_artifact_extensions")
    )
    excluded_artifact_extension_set = set(excluded_artifact_extensions)
    required_embedded_media_extensions = tuple(
        extension
        for extension in _validated_embedded_media_artifact_extensions(
            payload.get("required_embedded_media_extensions")
        )
        if extension not in excluded_artifact_extension_set
    )
    validated_artifact_extensions = list(
        extension
        for extension in _validated_artifact_extensions(payload.get("requested_artifact_extensions"))
        if extension not in excluded_artifact_extension_set
    )
    requested_artifact_extensions = tuple(
        [
            *validated_artifact_extensions,
            *(
                extension
                for extension in required_embedded_media_extensions
                if extension not in validated_artifact_extensions
            ),
        ]
    )
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    connector_source_user_requested = bool(payload.get("connector_source_user_requested"))
    connector_source_evidence = str(payload.get("connector_source_evidence") or "").strip()
    return TurnToolScopeDecision(
        requested_outcome=requested_outcome,
        web_action=web_action,
        scheduler_action=scheduler_action,
        scheduler_toggle_enabled=scheduler_toggle_enabled,
        scheduler_selection_policy=scheduler_selection_policy if scheduler_action == "run" else "none",
        scheduler_target_scope=scheduler_target_scope if scheduler_action == "mutate" else "none",
        skill_pack_action=skill_pack_action,
        connector_app_ids=connector_app_ids if skill_pack_action == "connector" else (),
        connector_source_user_requested=connector_source_user_requested,
        connector_source_evidence=connector_source_evidence,
        requested_tool_names=requested_tool_names,
        required_tool_names=required_tool_names,
        requested_artifact_extensions=requested_artifact_extensions,
        excluded_artifact_extensions=excluded_artifact_extensions,
        required_embedded_media_extensions=required_embedded_media_extensions,
        confidence=max(0.0, min(1.0, confidence)),
        valid=True,
    )


def _tool_scope_decision_to_payload(decision: TurnToolScopeDecision) -> dict[str, object]:
    return {
        "requested_outcome": decision.requested_outcome,
        "web_action": decision.web_action,
        "scheduler_action": decision.scheduler_action,
        "scheduler_toggle_enabled": decision.scheduler_toggle_enabled,
        "scheduler_selection_policy": decision.scheduler_selection_policy,
        "scheduler_target_scope": decision.scheduler_target_scope,
        "skill_pack_action": decision.skill_pack_action,
        "connector_app_ids": list(decision.connector_app_ids),
        "connector_source_user_requested": decision.connector_source_user_requested,
        "connector_source_evidence": decision.connector_source_evidence,
        "requested_tool_names": list(decision.requested_tool_names),
        "required_tool_names": list(decision.required_tool_names),
        "requested_artifact_extensions": list(decision.requested_artifact_extensions),
        "excluded_artifact_extensions": list(decision.excluded_artifact_extensions),
        "required_embedded_media_extensions": list(decision.required_embedded_media_extensions),
        "confidence": decision.confidence,
        "valid": decision.valid,
    }


def _tool_scope_decision_from_payload(payload: object) -> TurnToolScopeDecision | None:
    if not isinstance(payload, dict):
        return None
    requested_outcome = str(payload.get("requested_outcome") or "unspecified").strip().lower()
    if requested_outcome not in _REQUESTED_OUTCOMES:
        return None
    web_action = str(payload.get("web_action") or "none").strip().lower()
    scheduler_action = str(payload.get("scheduler_action") or "none").strip().lower()
    skill_pack_action = str(payload.get("skill_pack_action") or "none").strip().lower()
    if web_action not in _WEB_ACTIONS or scheduler_action not in _SCHEDULER_ACTIONS or skill_pack_action not in _SKILL_PACK_ACTIONS:
        return None
    raw_scheduler_toggle_enabled = payload.get("scheduler_toggle_enabled")
    scheduler_toggle_enabled = (
        raw_scheduler_toggle_enabled
        if isinstance(raw_scheduler_toggle_enabled, bool)
        else None
    )
    scheduler_selection_policy = str(payload.get("scheduler_selection_policy") or "none").strip().lower()
    if scheduler_selection_policy not in {"none", "user_selected", "delegate_one"}:
        scheduler_selection_policy = "none"
    scheduler_target_scope = str(payload.get("scheduler_target_scope") or "none").strip().lower()
    if scheduler_target_scope not in {"none", "all_current_workspace"}:
        scheduler_target_scope = "none"
    connector_app_ids = _unique_connector_app_ids(payload.get("connector_app_ids") if isinstance(payload.get("connector_app_ids"), list) else ())
    requested_tool_names = tuple(
        dict.fromkeys(
            str(tool_name or "").strip()
            for tool_name in (payload.get("requested_tool_names") if isinstance(payload.get("requested_tool_names"), list) else ())
            if str(tool_name or "").strip()
        )
    )
    required_tool_names = tuple(
        dict.fromkeys(
            str(tool_name or "").strip()
            for tool_name in (payload.get("required_tool_names") if isinstance(payload.get("required_tool_names"), list) else ())
            if str(tool_name or "").strip()
        )
    )
    excluded_artifact_extensions = _validated_artifact_extensions(
        payload.get("excluded_artifact_extensions")
    )
    excluded_artifact_extension_set = set(excluded_artifact_extensions)
    required_embedded_media_extensions = tuple(
        extension
        for extension in _validated_embedded_media_artifact_extensions(
            payload.get("required_embedded_media_extensions")
        )
        if extension not in excluded_artifact_extension_set
    )
    validated_artifact_extensions = list(
        extension
        for extension in _validated_artifact_extensions(payload.get("requested_artifact_extensions"))
        if extension not in excluded_artifact_extension_set
    )
    requested_artifact_extensions = tuple(
        [
            *validated_artifact_extensions,
            *(
                extension
                for extension in required_embedded_media_extensions
                if extension not in validated_artifact_extensions
            ),
        ]
    )
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    connector_source_user_requested = bool(payload.get("connector_source_user_requested"))
    connector_source_evidence = str(payload.get("connector_source_evidence") or "").strip()
    return TurnToolScopeDecision(
        requested_outcome=requested_outcome,
        web_action=web_action,
        scheduler_action=scheduler_action,
        scheduler_toggle_enabled=scheduler_toggle_enabled,
        scheduler_selection_policy=scheduler_selection_policy if scheduler_action == "run" else "none",
        scheduler_target_scope=scheduler_target_scope if scheduler_action == "mutate" else "none",
        skill_pack_action=skill_pack_action,
        connector_app_ids=connector_app_ids if skill_pack_action == "connector" else (),
        connector_source_user_requested=connector_source_user_requested,
        connector_source_evidence=connector_source_evidence,
        requested_tool_names=requested_tool_names,
        required_tool_names=required_tool_names,
        requested_artifact_extensions=requested_artifact_extensions,
        excluded_artifact_extensions=excluded_artifact_extensions,
        required_embedded_media_extensions=required_embedded_media_extensions,
        confidence=max(0.0, min(1.0, confidence)),
        valid=bool(payload.get("valid")),
    )


def _tool_scope_registry_signature(registry: object) -> object:
    try:
        specs = registry.list_specs()
    except Exception:
        try:
            definitions = registry.list_tool_definitions()
        except Exception:
            return (type(registry).__name__, "unavailable")
        return tuple(
            (
                str(definition.get("name") or ""),
                tuple(str(tag) for tag in (definition.get("capability_tags") or ())),
                str(definition.get("side_effect_class") or ""),
                str(definition.get("risk_level") or ""),
                bool(definition.get("requires_approval")),
            )
            for definition in definitions
        )
    return tuple(
        (
            str(getattr(spec, "name", "") or ""),
            tuple(str(tag) for tag in (getattr(spec, "capability_tags", ()) or ())),
            str(getattr(getattr(spec, "side_effect_class", None), "value", "")),
            str(getattr(getattr(spec, "risk_level", None), "value", "")),
            bool(getattr(spec, "requires_approval", False)),
        )
        for spec in specs
    )


def _tool_scope_classifier_max_tokens() -> int:
    try:
        value = int(os.environ.get("NULLION_TOOL_SCOPE_CLASSIFIER_MAX_TOKENS", "96"))
    except ValueError:
        value = 96
    return max(32, value)


def _tool_scope_classifier_timeout_seconds() -> float:
    try:
        value = float(os.environ.get("NULLION_TOOL_SCOPE_CLASSIFIER_TIMEOUT_SECONDS", "5"))
    except ValueError:
        value = 5.0
    return max(0.5, value)


def _active_connector_provider_context() -> list[dict[str, object]]:
    """Structured connector/package facts for the scope classifier.

    This is runtime evidence, not prompt parsing: installed packages and active
    connections describe which connector-backed tool families may be relevant.
    """
    try:
        from nullion.connections import load_connection_registry
        from nullion.skill_pack_installer import (
            get_installed_skill_pack,
            list_installed_skill_packs,
            list_skill_pack_reference_paths,
        )
    except Exception:
        return []
    providers: list[dict[str, object]] = []
    try:
        connections = load_connection_registry().connections
    except Exception:
        connections = []
    try:
        installed_packs = tuple(list_installed_skill_packs())
    except Exception:
        installed_packs = ()
    for connection in connections:
        provider_id = str(getattr(connection, "provider_id", "") or "").strip()
        if not provider_id or not getattr(connection, "active", True):
            continue
        normalized = provider_id.lower()
        if not (normalized.startswith("skill_pack_connector_") or normalized.endswith("_connector_provider")):
            continue
        permission_mode = str(getattr(connection, "permission_mode", "") or "read")
        entry: dict[str, object] = {
            "provider_id": provider_id,
            "display_name": str(getattr(connection, "display_name", "") or provider_id),
            "permission_mode": permission_mode,
            "credential_scope": str(getattr(connection, "credential_scope", "") or "workspace"),
            "structured_tools": ["connector_request"],
        }
        raw_structured_tools = getattr(connection, "structured_tools", None)
        if raw_structured_tools is not None:
            structured_tools = [
                str(tool or "").strip()
                for tool in raw_structured_tools
                if str(tool or "").strip() in _CONNECTOR_TYPED_TOOLS
            ]
            if structured_tools:
                entry["structured_tools"] = list(dict.fromkeys(["connector_request", *structured_tools]))
        elif normalized.endswith("_connector_provider"):
            structured_tools = ["connector_request", "email_search", "email_read", "email_attachment_read", "calendar_list"]
            if permission_mode == "write":
                structured_tools.extend(
                    ["email_send", "calendar_create", "calendar_update", "calendar_respond", "calendar_delete"]
                )
            entry["structured_tools"] = structured_tools
        skill_pack_id = _installed_connector_skill_pack_id_for_connection(
            provider_id,
            connection=connection,
            installed_packs=installed_packs,
        )
        pack = get_installed_skill_pack(skill_pack_id) if skill_pack_id else None
        if pack is not None:
            entry["skill_pack_id"] = getattr(pack, "pack_id", skill_pack_id)
            try:
                reference_paths = list(list_skill_pack_reference_paths(pack.pack_id))
            except Exception:
                reference_paths = []
            if reference_paths:
                entry["reference_paths"] = reference_paths[:500]
                entry["reference_path_count"] = len(reference_paths)
                app_ids: list[str] = []
                for path in reference_paths:
                    parts = str(path or "").split("/")
                    if len(parts) >= 3 and parts[0] == "references" and parts[-1].lower() == "readme.md":
                        app_id = parts[1].strip().lower()
                        if app_id and app_id not in app_ids:
                            app_ids.append(app_id)
                if app_ids:
                    entry["active_app_ids"] = app_ids[:500]
                    read_tools: list[str] = []
                    write_tools: list[str] = []
                    if "google-mail" in app_ids:
                        read_tools.extend(["email_search", "email_read", "email_attachment_read"])
                        if permission_mode == "write":
                            write_tools.append("email_send")
                    if "google-calendar" in app_ids:
                        read_tools.append("calendar_list")
                        if permission_mode == "write":
                            write_tools.extend(["calendar_create", "calendar_update", "calendar_respond", "calendar_delete"])
                    structured_tools = ["connector_request", *read_tools, *write_tools]
                    entry["structured_tools"] = structured_tools
        providers.append(entry)
    try:
        from nullion.connector_prompt_context import cache_active_connector_app_ids

        cache_active_connector_app_ids(providers)
    except Exception:
        logger.debug("Unable to cache active connector app ids", exc_info=True)
    return providers[:12]


def _installed_connector_skill_pack_id_for_connection(
    provider_id: str,
    *,
    connection: object | None,
    installed_packs: Iterable[object],
) -> str:
    normalized_provider = str(provider_id or "").strip().lower()
    packs = tuple(installed_packs)
    for candidate in packs:
        candidate_id = str(getattr(candidate, "pack_id", "") or "").strip().lower()
        slug = re.sub(r"[^a-z0-9]+", "_", candidate_id).strip("_")
        if candidate_id and f"skill_pack_connector_{slug}" == normalized_provider:
            return candidate_id
    for candidate in packs:
        candidate_id = str(getattr(candidate, "pack_id", "") or "").strip().lower()
        if candidate_id and _connection_matches_installed_connector_pack(connection, candidate):
            return candidate_id
    return ""


def _connection_matches_installed_connector_pack(connection: object | None, pack: object) -> bool:
    if connection is None:
        return False
    provider_profile = str(getattr(connection, "provider_profile", "") or "").strip()
    if not provider_profile:
        return False
    pack_bases = _installed_connector_pack_base_urls(pack)
    if not pack_bases or not any(_connector_base_urls_match(provider_profile, candidate) for candidate in pack_bases):
        return False
    required_envs = set(_installed_connector_pack_required_env_vars(pack))
    if not required_envs:
        return False
    credential_ref = str(getattr(connection, "credential_ref", "") or "").strip().removeprefix("env:")
    return bool(credential_ref and credential_ref in required_envs)


def _installed_connector_pack_required_env_vars(pack: object) -> tuple[str, ...]:
    pack_path = Path(str(getattr(pack, "path", "") or ""))
    if not pack_path.exists():
        return ()
    env_vars: list[str] = []
    for skill_file in sorted(pack_path.rglob("SKILL.md")):
        try:
            text = skill_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        in_env_block = False
        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            stripped = line.strip()
            if stripped == "env:":
                in_env_block = True
                continue
            if in_env_block and stripped.startswith("- "):
                name = stripped.removeprefix("- ").strip().strip("'\"")
                if re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
                    env_vars.append(name)
                continue
            if in_env_block and stripped and not line.startswith((" ", "\t")):
                in_env_block = False
    return tuple(dict.fromkeys(env_vars))


def _installed_connector_pack_base_urls(pack: object) -> tuple[str, ...]:
    pack_path = Path(str(getattr(pack, "path", "") or ""))
    if not pack_path.exists():
        return ()
    urls: list[str] = []
    for skill_file in sorted(pack_path.rglob("SKILL.md")):
        try:
            text = skill_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        base_context_lines = 0
        for line in text.splitlines():
            normalized_line = line.lower()
            starts_base_context = (
                "base url" in normalized_line
                or "base_url" in normalized_line
                or "endpoint base" in normalized_line
            )
            if starts_base_context:
                base_context_lines = 8
            if not starts_base_context and base_context_lines <= 0:
                continue
            for match in re.finditer(r"https?://[^\s`'\"<>)]+", line):
                raw_url = match.group(0).rstrip(".,;:")
                parsed = urlparse(raw_url)
                if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                    continue
                if any(marker in raw_url for marker in ("{", "}", "<", ">")):
                    path = parsed.path
                    cut_positions = [pos for marker in ("{", "<") if (pos := path.find(marker)) >= 0]
                    if cut_positions:
                        path = path[: min(cut_positions)]
                    raw_url = f"{parsed.scheme}://{parsed.netloc}{path.rstrip('/')}/"
                urls.append(raw_url)
            if base_context_lines > 0:
                base_context_lines -= 1
    return tuple(dict.fromkeys(_normalized_connector_base_url(url) for url in urls if _normalized_connector_base_url(url)))


def _normalized_connector_base_url(value: object) -> str:
    parsed = urlparse(str(value or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    netloc = parsed.netloc.lower()
    path = (parsed.path or "/").rstrip("/") + "/"
    return f"{parsed.scheme.lower()}://{netloc}{path}"


def _connector_base_urls_match(left: object, right: object) -> bool:
    left_url = _normalized_connector_base_url(left)
    right_url = _normalized_connector_base_url(right)
    if not left_url or not right_url:
        return False
    left_parsed = urlparse(left_url)
    right_parsed = urlparse(right_url)
    if left_parsed.scheme != right_parsed.scheme or left_parsed.netloc != right_parsed.netloc:
        return False
    left_path = left_parsed.path.rstrip("/") + "/"
    right_path = right_parsed.path.rstrip("/") + "/"
    return left_path.startswith(right_path) or right_path.startswith(left_path)


def _runtime_has_active_connector_provider() -> bool:
    try:
        from nullion.connections import load_connection_registry
    except Exception:
        return False
    try:
        connections = load_connection_registry().connections
    except Exception:
        return False
    for connection in connections:
        provider_id = str(getattr(connection, "provider_id", "") or "").strip().lower()
        if not provider_id or not getattr(connection, "active", True):
            continue
        if provider_id.startswith("skill_pack_connector_") or provider_id.endswith("_connector_provider"):
            return True
    return False


def _active_connector_app_ids_from_context(providers: Iterable[object]) -> tuple[str, ...]:
    app_ids: list[object] = []
    for provider in providers:
        if isinstance(provider, Mapping):
            raw = provider.get("active_app_ids")
            if isinstance(raw, (list, tuple)):
                app_ids.extend(raw)
    return _unique_connector_app_ids(app_ids)


def _active_connector_app_id_providers_for_scope_decision(
    *,
    allow_runtime_load: bool = True,
) -> tuple[dict[str, tuple[str, ...]], ...]:
    cached = cached_active_connector_app_id_providers(allow_persistent=allow_runtime_load)
    if cached:
        return cached
    if not allow_runtime_load:
        return ()
    app_ids = _active_connector_app_ids_from_context(_active_connector_provider_context())
    if not app_ids:
        return ()
    return ({"active_app_ids": app_ids},)


def _structured_connector_tool_family_app_ids_for_message(
    user_message: str,
    providers: Iterable[object],
) -> tuple[str, ...]:
    def _normalized_identifier_tokens(value: object) -> set[str]:
        tokens = set(re.findall(r"[a-z0-9]+", str(value or "").lower()))
        # This is identifier-token normalization for structured tool/app ids, not a
        # product synonym list. It lets user text like "emails" match the
        # registered tool family "email_*" without hardcoding account prose.
        tokens.update(token[:-1] for token in tuple(tokens) if len(token) > 3 and token.endswith("s"))
        return tokens

    message_tokens = _normalized_identifier_tokens(user_message)
    if not message_tokens:
        return ()
    mentioned: list[str] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        raw_active_app_ids = provider.get("active_app_ids")
        active_app_ids = set(
            _unique_connector_app_ids(raw_active_app_ids if isinstance(raw_active_app_ids, (list, tuple)) else ())
        )
        if not active_app_ids:
            continue
        raw_tools = provider.get("structured_tools")
        structured_tools = raw_tools if isinstance(raw_tools, (list, tuple)) else ()
        for raw_tool_name in structured_tools:
            tool_name = str(raw_tool_name or "").strip()
            if tool_name not in _CONNECTOR_TYPED_TOOLS:
                continue
            app_id = _connector_app_id_for_typed_tool(tool_name)
            if not app_id or app_id not in active_app_ids or app_id in mentioned:
                continue
            family = tool_name.split("_", 1)[0].strip().lower()
            if family and _normalized_identifier_tokens(family).intersection(message_tokens):
                mentioned.append(app_id)
    return tuple(mentioned)


def _mentioned_active_connector_app_ids_for_scope(
    user_message: str,
    active_connector_providers: Iterable[object],
) -> tuple[str, ...]:
    providers = _active_connector_app_id_providers_for_scope_decision()
    mentioned = tuple(
        dict.fromkeys(
            (
                *mentioned_connector_app_ids(user_message, providers),
                *_structured_connector_tool_family_app_ids_for_message(user_message, providers),
            )
        )
    )
    if not mentioned:
        return ()
    active_app_ids = set(_active_connector_app_ids_from_context(active_connector_providers))
    return tuple(app_id for app_id in mentioned if app_id in active_app_ids)


def _connector_structured_tools_from_context(providers: Iterable[object]) -> set[str]:
    tools: set[str] = set()
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        structured = provider.get("structured_tools")
        if isinstance(structured, (list, tuple)):
            tools.update(str(tool or "").strip() for tool in structured if str(tool or "").strip())
    return tools


def _connector_read_source_summaries(
    providers: Iterable[object],
    *,
    available_tool_names: Iterable[object],
) -> list[dict[str, object]]:
    available = {str(tool or "").strip() for tool in available_tool_names if str(tool or "").strip()}
    summaries: list[dict[str, object]] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        raw_tools = provider.get("structured_tools")
        structured_tools = {
            str(tool or "").strip()
            for tool in (raw_tools if isinstance(raw_tools, (list, tuple)) else ())
            if str(tool or "").strip()
        }
        read_tools = sorted(structured_tools.intersection(_CONNECTOR_FIRST_CLASS_READ_TOOLS).intersection(available))
        if not read_tools:
            continue
        raw_app_ids = provider.get("active_app_ids")
        app_ids = _unique_connector_app_ids(raw_app_ids if isinstance(raw_app_ids, (list, tuple)) else ())
        summaries.append(
            {
                "display_name": str(provider.get("display_name") or "Connected account").strip(),
                "active_app_ids": list(app_ids),
                "read_tools": read_tools,
                "permission_mode": str(provider.get("permission_mode") or "read").strip() or "read",
            }
        )
    return summaries[:12]


def _default_connector_tools_for_scope(
    connector_structured_tools: set[str],
    *,
    explicitly_requested_tools: set[str],
) -> set[str]:
    selected = set(connector_structured_tools)
    if explicitly_requested_tools:
        selected.intersection_update(explicitly_requested_tools)
    first_class_read_tools = selected.intersection(_CONNECTOR_FIRST_CLASS_READ_TOOLS)
    if first_class_read_tools and "connector_request" not in explicitly_requested_tools:
        selected.discard("connector_request")
    return selected


def _connector_tool_allowed_for_app_scope(tool_name: str, selected_app_ids: set[str]) -> bool:
    if not selected_app_ids:
        return True
    if tool_name in {"connector_request", "skill_pack_read"}:
        return True
    if tool_name in {"email_send", "email_search", "email_read", "email_attachment_read"}:
        return "google-mail" in selected_app_ids
    if tool_name in {"calendar_list", "calendar_create", "calendar_update", "calendar_respond", "calendar_delete"}:
        return "google-calendar" in selected_app_ids
    return True


def _connector_app_id_for_typed_tool(tool_name: str) -> str | None:
    if tool_name in {"email_send", "email_search", "email_read", "email_attachment_read"}:
        return "google-mail"
    if tool_name in {"calendar_list", "calendar_create", "calendar_update", "calendar_respond", "calendar_delete"}:
        return "google-calendar"
    return None


def _requested_connector_app_ids_match_typed_tools(
    requested_app_ids: Iterable[object],
    requested_tool_names: Iterable[object],
) -> bool:
    app_ids = set(_unique_connector_app_ids(requested_app_ids))
    if not app_ids:
        return False
    tool_app_ids = {
        app_id
        for app_id in (
            _connector_app_id_for_typed_tool(str(tool_name or "").strip())
            for tool_name in requested_tool_names
        )
        if app_id
    }
    return bool(tool_app_ids) and app_ids.issubset(tool_app_ids)


def _connector_source_token_scope_decision(
    *,
    user_message: str,
    registry,
    active_connector_providers: Iterable[object],
    requested_artifact_extensions: Iterable[str] = (),
    required_embedded_media_extensions: Iterable[str] = (),
) -> TurnToolScopeDecision | None:
    app_ids = _mentioned_active_connector_app_ids_for_scope(user_message, active_connector_providers)
    if not app_ids:
        return None
    try:
        available_names = {str(getattr(spec, "name", "") or "") for spec in registry.list_specs()}
    except Exception:
        try:
            available_names = {str(definition.get("name") or "") for definition in registry.list_tool_definitions()}
        except Exception:
            available_names = set()
    connector_tools = _connector_structured_tools_from_context(active_connector_providers).intersection(available_names)
    selected_app_ids = set(app_ids)
    include_attachment_read = bool(requested_artifact_extensions or required_embedded_media_extensions)
    read_tools = tuple(
        sorted(
            tool_name
            for tool_name in connector_tools
            if tool_name in _CONNECTOR_FIRST_CLASS_READ_TOOLS
            and _connector_tool_allowed_for_app_scope(tool_name, selected_app_ids)
            and (tool_name != "email_attachment_read" or include_attachment_read)
        )
    )
    if not read_tools:
        return None
    return TurnToolScopeDecision(
        skill_pack_action="connector",
        connector_app_ids=app_ids,
        connector_source_user_requested=True,
        requested_tool_names=read_tools,
        required_tool_names=read_tools,
        requested_artifact_extensions=tuple(requested_artifact_extensions),
        required_embedded_media_extensions=tuple(required_embedded_media_extensions),
        confidence=1.0,
        valid=True,
    )


def _connector_source_token_decision_can_skip_classifier(decision: TurnToolScopeDecision) -> bool:
    requested = set(decision.requested_tool_names)
    return bool(requested) and requested.issubset({"email_search", "email_read"})


def _email_target_scope_decision(
    *,
    registry,
    active_connector_providers: Iterable[object],
    requested_artifact_extensions: Iterable[str] = (),
    required_embedded_media_extensions: Iterable[str] = (),
) -> TurnToolScopeDecision | None:
    try:
        available_names = {str(getattr(spec, "name", "") or "") for spec in registry.list_specs()}
    except Exception:
        try:
            available_names = {str(definition.get("name") or "") for definition in registry.list_tool_definitions()}
        except Exception:
            available_names = set()
    connector_tools = _connector_structured_tools_from_context(active_connector_providers).intersection(available_names)
    if "email_send" not in connector_tools:
        return None
    active_app_ids = set(_active_connector_app_ids_from_context(active_connector_providers))
    if active_app_ids and "google-mail" not in active_app_ids:
        return None
    app_ids = ("google-mail",) if active_app_ids else ()
    return TurnToolScopeDecision(
        skill_pack_action="connector",
        connector_app_ids=app_ids,
        connector_source_user_requested=True,
        requested_tool_names=("email_send",),
        required_tool_names=("email_send",),
        requested_artifact_extensions=tuple(requested_artifact_extensions),
        required_embedded_media_extensions=tuple(required_embedded_media_extensions),
        confidence=1.0,
        valid=True,
    )


def _decision_declares_tool_backed_artifact(
    *,
    requested_tool_names: Iterable[str],
    requested_artifact_extensions: Iterable[str],
    required_embedded_media_extensions: Iterable[str],
) -> bool:
    requested_tools = {str(name or "").strip() for name in requested_tool_names if str(name or "").strip()}
    requested_extensions = {
        str(extension or "").strip().lower()
        for extension in requested_artifact_extensions
        if str(extension or "").strip()
    }
    embedded_extensions = {
        str(extension or "").strip().lower()
        for extension in required_embedded_media_extensions
        if str(extension or "").strip()
    }
    if "browser_image_collect" in requested_tools and embedded_extensions.intersection(
        _EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS
    ):
        return True
    if embedded_extensions.intersection(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS):
        return True
    for extension in requested_extensions:
        if requested_tools.intersection(_ARTIFACT_EXTENSION_TOOLS.get(extension, ())):
            return True
    if "image_generate" in requested_tools and requested_extensions.intersection({".jpg", ".jpeg", ".png", ".webp"}):
        return True
    return False


def _validated_turn_tool_scope_decision(
    decision: TurnToolScopeDecision,
    *,
    evidence: TurnToolEvidence,
    registry,
    active_connector_providers: Iterable[object],
) -> TurnToolScopeDecision:
    normalized_current_user_message = " ".join(
        str(getattr(evidence, "current_user_message", "") or "").split()
    ).casefold()
    normalized_connector_source_evidence = " ".join(
        str(getattr(decision, "connector_source_evidence", "") or "").split()
    ).casefold()
    connector_source_evidence_verified = bool(
        normalized_connector_source_evidence
        and normalized_current_user_message
        and normalized_connector_source_evidence in normalized_current_user_message
    )
    prior_connector_source_selected = bool(
        evidence.context_linked and evidence.has_prior_tool_scope("connector")
    )
    connector_source_user_requested = bool(
        decision.connector_source_user_requested
        and (connector_source_evidence_verified or prior_connector_source_selected)
    )
    connector_source_evidence = (
        str(getattr(decision, "connector_source_evidence", "") or "").strip()
        if connector_source_evidence_verified
        else ""
    )
    requested_tool_names = _validated_requested_tool_names(
        decision.requested_tool_names,
        registry=registry,
        allow_connector=(
            decision.skill_pack_action == "connector"
            and connector_source_user_requested
        ),
        active_connector_providers=active_connector_providers,
    )
    image_generation_unavailable = (
        decision.requested_outcome == "generated_media"
        and "image_generate" not in {
            str(getattr(spec, "name", "") or "")
            for spec in registry.list_specs()
        }
    )
    if image_generation_unavailable:
        requested_tool_names = tuple(
            tool_name
            for tool_name in requested_tool_names
            if tool_name not in _URL_BOUNDARY_TOOLS
            and tool_name not in {"web_search", "browser_image_collect"}
        )
    requested_extension_set = {
        str(extension or "").strip().lower()
        for extension in decision.requested_artifact_extensions
        if str(extension or "").strip()
    }
    browser_screenshot_artifact_contract = (
        "browser_screenshot" in requested_tool_names
        and ".png" in requested_extension_set
        and (
            evidence.artifact_requested
            or (evidence.context_linked and evidence.has_prior_tool_scope("web"))
        )
        and (
            evidence.has_url_target
            or (evidence.context_linked and evidence.has_prior_tool_scope("web"))
        )
    )
    trust_decision_artifact_contract = bool(
        evidence.artifact_requested
        or browser_screenshot_artifact_contract
        or _decision_declares_tool_backed_artifact(
            requested_tool_names=requested_tool_names,
            requested_artifact_extensions=decision.requested_artifact_extensions,
            required_embedded_media_extensions=decision.required_embedded_media_extensions,
        )
    )
    decision_artifact_extensions = (
        tuple(decision.requested_artifact_extensions)
        if trust_decision_artifact_contract
        else ()
    )
    trusted_requested_extensions = (
        decision_artifact_extensions
        if decision_artifact_extensions and len(set(evidence.requested_extensions)) > 1
        else tuple(dict.fromkeys([*evidence.requested_extensions, *decision_artifact_extensions]))
    )
    trusted_excluded_extensions = tuple(
        dict.fromkeys(
            [
                *decision.excluded_artifact_extensions,
                *(
                    extension
                    for extension in evidence.requested_extensions
                    if decision_artifact_extensions and extension not in set(trusted_requested_extensions)
                ),
            ]
        )
    )
    if trusted_excluded_extensions:
        trusted_requested_extensions = tuple(
            extension
            for extension in trusted_requested_extensions
            if extension not in set(trusted_excluded_extensions)
        )
    trusted_embedded_extensions = (
        decision.required_embedded_media_extensions
        if trust_decision_artifact_contract
        else ()
    )
    requested_tool_names = tuple(
        tool_name
        for tool_name in requested_tool_names
        if tool_name not in _BROWSER_VISUAL_CAPTURE_TOOLS
        or _browser_visual_capture_allowed_by_extensions(
            tool_name,
            requested_extensions=trusted_requested_extensions,
            embedded_media_extensions=trusted_embedded_extensions,
        )
        or (
            tool_name == "browser_screenshot"
            and (
                evidence.artifact_requested
                or (evidence.context_linked and evidence.has_prior_tool_scope("web"))
            )
            and (
                evidence.has_url_target
                or (evidence.context_linked and evidence.has_prior_tool_scope("web"))
            )
        )
    )
    requested_tool_names = _filter_scheduler_requested_tool_names_for_action(
        requested_tool_names,
        decision.scheduler_action,
    )
    if (
        trusted_embedded_extensions
        and requested_tool_names
        and not _scope_tool_names_can_source_embedded_media(requested_tool_names)
    ):
        trusted_embedded_extensions = ()
    if (
        trusted_embedded_extensions
        and not set(trusted_requested_extensions).intersection(_EMBEDDED_MEDIA_ARTIFACT_EXTENSIONS)
    ):
        trusted_embedded_extensions = ()
    requested_tool_set = set(requested_tool_names)
    required_tool_candidates = (
        _TURN_DECISION_REQUIRED_TOOL_CANDIDATES
        if decision.skill_pack_action == "connector"
        else _SCOPE_REQUIRED_TOOL_CANDIDATES
    )
    required_tool_names = tuple(
        tool_name
        for tool_name in decision.required_tool_names
        if tool_name in requested_tool_set and tool_name in required_tool_candidates
    )
    if evidence.has_email_address_target and "email_send" in requested_tool_set and "email_send" not in required_tool_names:
        required_tool_names = tuple(dict.fromkeys([*required_tool_names, "email_send"]))
    if decision.skill_pack_action != "connector":
        return TurnToolScopeDecision(
            requested_outcome=decision.requested_outcome,
            web_action="none" if image_generation_unavailable else decision.web_action,
            scheduler_action=decision.scheduler_action,
            scheduler_toggle_enabled=decision.scheduler_toggle_enabled
            if "toggle_cron" in requested_tool_names
            else None,
            scheduler_selection_policy=decision.scheduler_selection_policy
            if decision.scheduler_action == "run"
            else "none",
            scheduler_target_scope=decision.scheduler_target_scope
            if decision.scheduler_action == "mutate"
            else "none",
            skill_pack_action=decision.skill_pack_action,
            requested_tool_names=requested_tool_names,
            required_tool_names=required_tool_names,
            requested_artifact_extensions=trusted_requested_extensions,
            excluded_artifact_extensions=trusted_excluded_extensions,
            required_embedded_media_extensions=trusted_embedded_extensions,
            confidence=decision.confidence,
            valid=decision.valid,
        )
    providers = tuple(active_connector_providers)
    if not providers:
        return TurnToolScopeDecision(
            requested_outcome=decision.requested_outcome,
            web_action="none" if image_generation_unavailable else decision.web_action,
            scheduler_action=decision.scheduler_action,
            scheduler_toggle_enabled=decision.scheduler_toggle_enabled
            if "toggle_cron" in requested_tool_names
            else None,
            scheduler_selection_policy=decision.scheduler_selection_policy
            if decision.scheduler_action == "run"
            else "none",
            scheduler_target_scope=decision.scheduler_target_scope
            if decision.scheduler_action == "mutate"
            else "none",
            skill_pack_action="none",
            requested_tool_names=(),
            required_tool_names=(),
            requested_artifact_extensions=trusted_requested_extensions,
            excluded_artifact_extensions=trusted_excluded_extensions,
            required_embedded_media_extensions=trusted_embedded_extensions,
            confidence=decision.confidence,
            valid=decision.valid,
        )
    active_app_ids = set(_active_connector_app_ids_from_context(active_connector_providers))
    if not connector_source_user_requested:
        return TurnToolScopeDecision(
            requested_outcome=decision.requested_outcome,
            web_action="none" if image_generation_unavailable else decision.web_action,
            scheduler_action=decision.scheduler_action,
            scheduler_toggle_enabled=decision.scheduler_toggle_enabled
            if "toggle_cron" in requested_tool_names
            else None,
            scheduler_selection_policy=decision.scheduler_selection_policy
            if decision.scheduler_action == "run"
            else "none",
            scheduler_target_scope=decision.scheduler_target_scope
            if decision.scheduler_action == "mutate"
            else "none",
            skill_pack_action="none",
            requested_tool_names=(),
            required_tool_names=(),
            requested_artifact_extensions=trusted_requested_extensions,
            excluded_artifact_extensions=trusted_excluded_extensions,
            required_embedded_media_extensions=trusted_embedded_extensions,
            confidence=decision.confidence,
            valid=decision.valid,
        )
    if not active_app_ids:
        return TurnToolScopeDecision(
            requested_outcome=decision.requested_outcome,
            web_action="none" if image_generation_unavailable else decision.web_action,
            scheduler_action=decision.scheduler_action,
            scheduler_toggle_enabled=decision.scheduler_toggle_enabled
            if "toggle_cron" in requested_tool_names
            else None,
            scheduler_selection_policy=decision.scheduler_selection_policy
            if decision.scheduler_action == "run"
            else "none",
            scheduler_target_scope=decision.scheduler_target_scope
            if decision.scheduler_action == "mutate"
            else "none",
            skill_pack_action=decision.skill_pack_action,
            connector_source_user_requested=connector_source_user_requested,
            connector_source_evidence=connector_source_evidence,
            connector_app_ids=(),
            requested_tool_names=requested_tool_names,
            required_tool_names=required_tool_names,
            requested_artifact_extensions=trusted_requested_extensions,
            excluded_artifact_extensions=trusted_excluded_extensions,
            required_embedded_media_extensions=trusted_embedded_extensions,
            confidence=decision.confidence,
            valid=decision.valid,
        )
    selected_app_ids = tuple(app_id for app_id in decision.connector_app_ids if app_id in active_app_ids)
    if not selected_app_ids:
        return TurnToolScopeDecision(
            requested_outcome=decision.requested_outcome,
            web_action="none" if image_generation_unavailable else decision.web_action,
            scheduler_action=decision.scheduler_action,
            scheduler_toggle_enabled=decision.scheduler_toggle_enabled
            if "toggle_cron" in requested_tool_names
            else None,
            scheduler_selection_policy=decision.scheduler_selection_policy
            if decision.scheduler_action == "run"
            else "none",
            scheduler_target_scope=decision.scheduler_target_scope
            if decision.scheduler_action == "mutate"
            else "none",
            skill_pack_action="none",
            requested_tool_names=(),
            required_tool_names=(),
            requested_artifact_extensions=trusted_requested_extensions,
            excluded_artifact_extensions=trusted_excluded_extensions,
            required_embedded_media_extensions=trusted_embedded_extensions,
            confidence=decision.confidence,
            valid=decision.valid,
        )
    return TurnToolScopeDecision(
        requested_outcome=decision.requested_outcome,
        web_action="none" if image_generation_unavailable else decision.web_action,
        scheduler_action=decision.scheduler_action,
        scheduler_toggle_enabled=decision.scheduler_toggle_enabled
        if "toggle_cron" in requested_tool_names
        else None,
        scheduler_selection_policy=decision.scheduler_selection_policy
        if decision.scheduler_action == "run"
        else "none",
        scheduler_target_scope=decision.scheduler_target_scope
        if decision.scheduler_action == "mutate"
        else "none",
        skill_pack_action=decision.skill_pack_action,
        connector_app_ids=selected_app_ids,
        connector_source_user_requested=connector_source_user_requested,
        connector_source_evidence=connector_source_evidence,
        requested_tool_names=requested_tool_names,
        required_tool_names=required_tool_names,
        requested_artifact_extensions=trusted_requested_extensions,
        excluded_artifact_extensions=trusted_excluded_extensions,
        required_embedded_media_extensions=trusted_embedded_extensions,
        confidence=decision.confidence,
        valid=decision.valid,
    )


def _validated_requested_tool_names(
    requested_tool_names: Iterable[object],
    *,
    registry,
    allow_connector: bool,
    active_connector_providers: Iterable[object],
) -> tuple[str, ...]:
    try:
        available_names = {str(getattr(spec, "name", "") or "") for spec in registry.list_specs()}
    except Exception:
        try:
            available_names = {str(definition.get("name") or "") for definition in registry.list_tool_definitions()}
        except Exception:
            available_names = set()
    connector_structured_tools = _connector_structured_tools_from_context(active_connector_providers)
    validated: list[str] = []
    for raw_name in requested_tool_names:
        name = str(raw_name or "").strip()
        if not name or name not in available_names:
            continue
        if name in _CONNECTOR_TYPED_TOOLS and (not allow_connector or name not in connector_structured_tools):
            continue
        if name not in validated:
            validated.append(name)
    return tuple(validated)


def _tool_scope_model_signature(model_client: object | None) -> object:
    if model_client is None:
        return ("none",)
    return (
        type(model_client).__name__,
        str(getattr(model_client, "provider", "") or ""),
        str(getattr(model_client, "model", "") or ""),
        str(getattr(model_client, "base_url", "") or ""),
        str(getattr(model_client, "reasoning_effort", "") or ""),
    )


def _tool_scope_cache_key(
    *,
    user_message: str,
    evidence: TurnToolEvidence,
    registry: object,
    model_client: object | None,
    active_connector_providers: Iterable[object] = (),
    skill_pack_index: str = "",
    force_model_decision: bool = False,
    assistant_no_tool_draft: str = "",
) -> dict[str, object]:
    return {
        "user_turn": str(user_message or ""),
        "assistant_no_tool_draft": str(assistant_no_tool_draft or ""),
        "force_model_decision": bool(force_model_decision),
        "evidence": {
            "context_linked": evidence.context_linked,
            "has_url_target": evidence.has_url_target,
            "has_attachments": evidence.has_attachments,
            "has_email_address_target": evidence.has_email_address_target,
            "requested_extensions": list(evidence.requested_extensions),
            "slash_prefixed_literal": evidence.slash_prefixed_literal,
            "numbered_option_selected": evidence.numbered_option_selected,
            "prior_tool_scopes": list(evidence.prior_tool_scopes),
        },
        "active_connector_providers": list(active_connector_providers),
        "skill_pack_index": str(skill_pack_index or ""),
        "registry": _tool_scope_registry_signature(registry),
        "model": _tool_scope_model_signature(model_client),
    }


def _refine_ambiguous_artifact_extension_contract(
    *,
    model_client: object,
    user_message: str,
    evidence: TurnToolEvidence,
    decision: TurnToolScopeDecision,
) -> TurnToolScopeDecision:
    candidates = tuple(dict.fromkeys(evidence.requested_extensions))
    candidate_set = set(candidates)
    if (
        len(candidate_set) <= 1
        or decision.excluded_artifact_extensions
        or set(decision.requested_artifact_extensions) != candidate_set
    ):
        return decision
    system = (
        "Return only a JSON object with exactly these keys: "
        '{"final_artifact_extensions":[".xlsx"],"excluded_artifact_extensions":[".json"]}. '
        "Partition every candidate extension into exactly one list based on the user turn. "
        "A final artifact is a file the user wants delivered or attached as an output. "
        "Exclude inputs, sources, intermediates, sidecars, examples, and formats the user says not to create, "
        "attach, or deliver. Do not omit a candidate and do not add extensions outside candidates."
    )
    prompt = {
        "user_turn": user_message,
        "candidate_extensions": list(candidates),
    }
    try:
        response = model_client.create(
            messages=[{"role": "user", "content": [{"type": "text", "text": json.dumps(prompt, ensure_ascii=False)}]}],
            tools=[],
            max_tokens=240,
            system=system,
            timeout=max(15.0, _tool_scope_classifier_timeout_seconds()),
        )
        response_text = _extract_response_text(response)
        start = response_text.find("{")
        end = response_text.rfind("}")
        if start < 0 or end <= start:
            return decision
        payload = json.loads(response_text[start : end + 1])
    except Exception:
        logger.debug("Artifact extension contract refinement failed", exc_info=True)
        return decision
    if not isinstance(payload, dict):
        return decision
    final_extensions = _validated_artifact_extensions(payload.get("final_artifact_extensions"))
    excluded_extensions = _validated_artifact_extensions(payload.get("excluded_artifact_extensions"))
    final_set = set(final_extensions)
    excluded_set = set(excluded_extensions)
    if (
        not final_set
        or final_set & excluded_set
        or final_set | excluded_set != candidate_set
    ):
        return decision
    return replace(
        decision,
        requested_artifact_extensions=tuple(
            extension for extension in candidates if extension in final_set
        ),
        excluded_artifact_extensions=tuple(
            extension for extension in candidates if extension in excluded_set
        ),
        required_embedded_media_extensions=tuple(
            extension
            for extension in decision.required_embedded_media_extensions
            if extension in final_set
        ),
    )


def build_turn_tool_scope_decision(
    *,
    model_client: object | None,
    user_message: str,
    evidence: TurnToolEvidence,
    registry,
    force_model_decision: bool = False,
    assistant_no_tool_draft: str | None = None,
    cache_decision: bool = True,
) -> TurnToolScopeDecision:
    if (
        model_client is None
        or not _registry_has_scoped_special_tools(registry)
        or (not force_model_decision and not turn_tool_evidence_needs_model_scope_decision(evidence))
    ):
        return TurnToolScopeDecision()
    if (
        not force_model_decision
        and getattr(evidence, "artifact_requested", False)
        and not getattr(evidence, "has_url_target", False)
        and not getattr(evidence, "has_attachments", False)
        and len(set(getattr(evidence, "requested_extensions", ()) or ())) <= 1
    ):
        # Output-extension evidence is already enough for ScopedTurnToolRegistry
        # to expose the matching artifact tools. Keep the classifier out of the
        # pre-model path; request_tool_scope can still widen the turn from
        # structured model output if the artifact task needs browser, connector,
        # scheduler, shell, or source-media tools.
        return TurnToolScopeDecision()
    active_connector_providers = _active_connector_provider_context()
    if str(assistant_no_tool_draft or "").strip():
        skill_pack_index = ""
    else:
        try:
            from nullion.config import load_settings
            from nullion.skill_pack_installer import format_cached_enabled_skill_pack_index_for_prompt

            settings = load_settings()
            skill_pack_index = format_cached_enabled_skill_pack_index_for_prompt(
                tuple(getattr(settings, "enabled_skill_packs", ()) or ()),
                max_total_chars=900,
            )
        except Exception:
            skill_pack_index = ""
    cache_key = _tool_scope_cache_key(
        user_message=user_message,
        evidence=evidence,
        registry=registry,
        model_client=model_client,
        active_connector_providers=active_connector_providers,
        skill_pack_index=skill_pack_index,
        force_model_decision=force_model_decision,
        assistant_no_tool_draft=assistant_no_tool_draft or "",
    )
    cache_enabled = cache_decision and not bool(str(assistant_no_tool_draft or "").strip())
    if cache_enabled:
        cached = runtime_cache.get_json(
            _TOOL_SCOPE_DECISION_CACHE_NAMESPACE,
            cache_key,
            version=_TOOL_SCOPE_DECISION_CACHE_VERSION,
            ttl_seconds=_TOOL_SCOPE_DECISION_CACHE_TTL_SECONDS,
            persistent=True,
        )
        cached_decision = _tool_scope_decision_from_payload(cached.value) if cached.hit else None
        if cached_decision is not None:
            return _validated_turn_tool_scope_decision(
                cached_decision,
                evidence=evidence,
                registry=registry,
                active_connector_providers=active_connector_providers,
            )
    registered_special_tool_names = {
        str(getattr(spec, "name", "") or "")
        for spec in registry.list_specs()
        if str(getattr(spec, "name", "") or "")
    }
    available_special_tool_scopes: list[str] = []
    if registered_special_tool_names.intersection(_URL_BOUNDARY_TOOLS):
        available_special_tool_scopes.append("web_or_browser")
    if registered_special_tool_names.intersection(_SCHEDULER_TOOLS):
        available_special_tool_scopes.append("scheduler")
    if registered_special_tool_names.intersection(_SKILL_PACK_TOOLS):
        available_special_tool_scopes.append("skill_pack_reference")
    if registered_special_tool_names.intersection(_CONNECTOR_TOOLS | _CONNECTOR_TYPED_TOOLS):
        available_special_tool_scopes.append("connector_gateway")
    if registered_special_tool_names.intersection(_CONVERSATION_HISTORY_TOOLS):
        available_special_tool_scopes.append("conversation_history")
    if "weather_forecast" in registered_special_tool_names:
        available_special_tool_scopes.append("weather")
    if "market_quote" in registered_special_tool_names:
        available_special_tool_scopes.append("market_data")
    if "image_generate" in registered_special_tool_names:
        available_special_tool_scopes.append("image_generation")
    if registered_special_tool_names.intersection(_LOCAL_FILE_TOOLS | _ARCHIVE_TOOLS):
        available_special_tool_scopes.append("local_files")
    if registered_special_tool_names.intersection(_LOCAL_SHELL_TOOLS):
        available_special_tool_scopes.append("local_shell")
    unavailable_special_tool_scopes = [
        scope
        for scope in ("weather", "market_data", "image_generation")
        if scope not in available_special_tool_scopes
    ]
    prompt = {
        "surface": "ordinary_chat",
        "context_linked": evidence.context_linked,
        "has_url_target": evidence.has_url_target,
        "has_attachments": evidence.has_attachments,
        "has_email_address_target": evidence.has_email_address_target,
        "requested_extensions": list(evidence.requested_extensions),
        "numbered_option_selected": evidence.numbered_option_selected,
        "prior_tool_scopes": list(evidence.prior_tool_scopes),
        "active_connector_providers": active_connector_providers,
        "installed_skill_pack_index": skill_pack_index,
        "available_special_tool_scopes": available_special_tool_scopes,
        "unavailable_special_tool_scopes": unavailable_special_tool_scopes,
        "registered_special_tool_names": sorted(registered_special_tool_names),
        "available_direct_read_tools": sorted(
            {
                str(getattr(spec, "name", "") or "")
                for spec in registry.list_specs()
            }.intersection({"calendar_list", "market_quote", "weather_forecast"})
        ),
        "user_turn": user_message,
    }
    if assistant_no_tool_draft:
        prompt["assistant_no_tool_draft"] = str(assistant_no_tool_draft or "")[:1600]
    system = (
        "Return only a JSON object matching this schema: "
        '{"requested_outcome":"unspecified|generated_media",'
        '"web_action":"none|open_url|live_research|browser_interaction",'
        '"scheduler_action":"none|inspect|run|mutate","scheduler_toggle_enabled":null,'
        '"scheduler_selection_policy":"none|user_selected|delegate_one",'
        '"scheduler_target_scope":"none|all_current_workspace",'
        '"skill_pack_action":"none|reference|connector","connector_app_ids":["normalized-app-id"],'
        '"connector_source_user_requested":false,"connector_source_evidence":"",'
        '"requested_tool_names":["registered-tool-name"],"required_tool_names":["registered-tool-name"],'
        '"requested_artifact_extensions":[".xlsx"],"excluded_artifact_extensions":[".json"],'
        '"required_embedded_media_extensions":[".xlsx"],'
        '"confidence":0.0}. '
        "Use web_action=open_url for explicit URL/domain targets, live_research for requests that need current public information, "
        "and browser_interaction for a user-visible webpage workflow. "
        "Put a tool in required_tool_names only when its successful structured result is essential to satisfy the current user turn, "
        "and always include it in requested_tool_names too. This includes explicitly requested read/source tools as well as write or "
        "delivery tools. A cross-domain request may require several independent read tools and several artifact tools. "
        "When the user explicitly requires a current public webpage or web source as part of the result, choose an exact registered "
        "web source tool such as browser_navigate or web_fetch and include it in both requested_tool_names and required_tool_names. "
        "When the turn explicitly requires live calendar, weather, or market data and the matching exact tool is available, include "
        "calendar_list, weather_forecast, or market_quote respectively in both lists. Do not mark optional or merely helpful tools as required. "
        "Use the weather scope and requested_tool_names=[\"weather_forecast\"] for live weather or forecast data when registered. "
        "available_direct_read_tools is authoritative structured runtime evidence. When one of those exact tools directly answers "
        "the user_turn, select it instead of generic web research. assistant_no_tool_draft is diagnostic evidence only and must not "
        "override the tool family implied by the original user_turn and available_direct_read_tools. "
        "available_special_tool_scopes and registered_special_tool_names are authoritative runtime evidence. Never select a tool family "
        "listed in unavailable_special_tool_scopes, and never substitute a different capability family that changes the requested outcome. "
        "In particular, public web/search results are not a substitute for newly generated visual media when image_generation is unavailable; "
        "choose no web tools so the normal product response can explain the missing configured capability. "
        "Set requested_outcome=generated_media whenever the requested final outcome is newly generated image or bitmap media; "
        "otherwise use requested_outcome=unspecified. This outcome field is required even when the matching tool is unavailable. "
        "Do not choose weather merely because a request mentions a day, date, time, or location; choose weather only when the requested answer is meteorological. "
        "Use the market_data scope and requested_tool_names=[\"market_quote\"] for structured public ticker quote rows when registered. "
        "When active_connector_providers expose google-calendar/calendar_list and the requested answer is the user's calendar, events, schedule, or agenda, choose connector with connector_app_ids=[\"google-calendar\"] and requested_tool_names=[\"calendar_list\"]. "
        "When active_connector_providers expose google-mail/email_search/email_read and the requested answer is the user's mailbox, inbox, mail, email messages, or unread mail, choose connector with connector_app_ids=[\"google-mail\"] and requested_tool_names=[\"email_search\",\"email_read\"]. "
        "Use the image_generation scope and requested_tool_names=[\"image_generate\"] for newly generated visual media when registered. "
        "For page or listing image assets that must become local artifact files, include browser_image_collect in requested_tool_names when registered. "
        "For requests that need newly created visual art, illustrations, poster/flyer imagery, book visuals, or other generated bitmap assets, include image_generate in requested_tool_names when registered. "
        "When web-derived artifact rows must come from lists, tables, cards, search results, or listings, prefer browser_extract_items over browser_snapshot or broad page text. "
        "When the browser page is already open and direct page fetching cannot see image assets, use browser_extract_items first, or browser_run_js to extract rendered image URLs, then pass those URLs to browser_image_collect. "
        "For web-derived artifact rows, browser_extract_items/browser_run_js should return compact per-item row objects with direct row URLs and image URLs instead of broad page dumps. "
        "When embedded media for a typed artifact must come from URL/page assets, request browser_image_collect and the matching artifact tool; "
        "do not request terminal_exec for normal web-image materialization while browser_image_collect is registered. "
        "When embedded media for a typed artifact must be newly generated rather than collected from a page, request image_generate and the matching artifact tool. "
        "For browser workflows that must prove visible page state, include browser_assert_page_state in requested_tool_names when registered. "
        "For screenshot capture of a current or prior browser page, include browser_screenshot in requested_tool_names "
        'and ".png" in requested_artifact_extensions when registered. '
        "Use web_action=none when the request can be answered without web/browser tools. "
        "Use scheduler actions only for scheduled-task or reminder control. "
        "For requests to configure, create, change, cancel, or otherwise set up recurring or future scheduled work, "
        "choose scheduler_action=mutate and include the exact registered scheduler mutation tool. "
        "For requests to manually start, trigger, run now, rerun, or otherwise execute existing scheduled tasks, "
        "choose scheduler_action=run and include run_cron in requested_tool_names when registered; include list_crons "
        "when the target scheduled task must be selected from existing jobs. If the same turn asks to locate, find, "
        "identify, or choose a scheduled task and also execute/start/trigger/run it, the scheduler_action is run, not inspect. "
        "Set scheduler_selection_policy=user_selected when the current turn already identifies an exact scheduled task "
        "by id, number, name, or linked task state. Set scheduler_selection_policy=delegate_one only when the user "
        "explicitly asks the agent to choose exactly one eligible scheduled task and run it. Otherwise use none. "
        "scheduler_action=inspect is only for reading or choosing among existing scheduled tasks; "
        "do not choose inspect when the user is asking to establish a new scheduled/recurring job, execute an existing scheduled task now, "
        "or make existing scheduled tasks/reminders end in a specific enabled/disabled/active/inactive state. "
        "A scheduler read/list result is not a valid final answer for a requested scheduler state change; choose mutate and include the registered mutation tool. "
        "Treat any web, research, or artifact production described as part of that future scheduled job as job instructions, "
        "not as current-turn web or artifact requirements. "
        "Do not request browser, local file, shell, or artifact tools only because the future scheduled job will need them. "
        "When a scheduled-task mutation has a clear exact action, include the exact registered mutation tool "
        "in requested_tool_names and required_tool_names. Use toggle_cron, with scheduler_toggle_enabled set to the desired final enabled state, "
        "for enabling or disabling one selected cron or an eligible set discovered through list_crons. Set scheduler_target_scope=all_current_workspace "
        "only when the structured request applies the same toggle_cron state to every eligible scheduled task in the current workspace. "
        "Use delete_cron only when the user is removing the saved scheduled task itself. "
        "When assistant_no_tool_draft is present after a scheduler read-only result, use it as evidence that the previous answer failed to complete the requested scheduler action. "
        "If the current user turn requires a final scheduler state change and the draft only listed scheduler objects or asked for setup details, choose scheduler_action=mutate "
        "and include the registered scheduler mutation tool that can perform the requested state change; do not leave the action as inspect. "
        "Do not choose scheduler just because a saved task could answer the domain. "
        "Use requested_tool_names=[\"chat_history_search\"] when the answer may be in saved current-conversation turns that are not visible in the prompt context. "
        "Use connector only when the user explicitly asks this turn to use a connected external API/account/source, "
        "or this turn is linked to prior connector source selection. Set connector_source_user_requested=true only in those cases. "
        "For a new source request, copy the shortest exact substring from user_turn that names or selects the connected source into "
        "connector_source_evidence. The runtime rejects new connector scope when that exact evidence is absent from user_turn. "
        "Leave connector_source_evidence empty only when typed context_linked and prior_tool_scopes already prove connector continuity. "
        "If numbered_option_selected is true, treat the selected option text in user_turn as the current structured user choice. "
        "If that selected option is a connected source choice, connector_source_user_requested may be true and the selected source "
        "text must still be copied into connector_source_evidence. "
        "Do not choose connector merely because connected account data might be useful. "
        "When using connector for a named external app, account, or destination, include its normalized app id in connector_app_ids even if active_app_ids does not list it; "
        "if no active provider context advertises that app id through active_app_ids, skill_pack_id/reference_paths, or other structured provider metadata, do not fall back to a generic connector provider or shell. "
        "When using connector for an active listed source, include exact app IDs from active_app_ids in connector_app_ids. "
        "When the connector action requires a specific structured account tool, include exact names from active_connector_providers.structured_tools in requested_tool_names. "
        'Use requested_tool_names=["file_search"] for local workspace file search/listing when file_search is registered. '
        "Do not use file_search as the final capability for local computer, installed software, package-manager, shell, service, process, path, or environment changes; "
        "those require local_shell/terminal_exec or a more specific computer-control capability with the normal approval flow. "
        'Use requested_tool_names=["file_patch"] for structured local text edits when file_patch is registered. '
        'Use requested_tool_names=["workspace_summary"] for workspace inventory/summarization when workspace_summary is registered. '
        "When a requested final deliverable has a file format, put the validated final suffix in "
        "requested_artifact_extensions and include the matching registered artifact tool in requested_tool_names. "
        "Only include formats the user wants delivered as final outputs. Exclude formats mentioned solely as inputs, "
        "source files, intermediate files, sidecars, examples, or files the user says must not be attached or delivered. "
        "Put every such mentioned-but-not-deliverable format in excluded_artifact_extensions; exclusions override requested formats. "
        "When that final deliverable must contain requested media embedded inside the artifact itself, also put "
        "the matching suffix in required_embedded_media_extensions. "
        'Use requested_tool_names=["terminal_exec"] when the turn requires local shell execution, local system inspection, installed software/package changes, service/process control, environment/path checks, or other computer-level work and dedicated file tools are unavailable or insufficient. '
        "Do not request terminal_exec as a fallback for connected-account or connector upload/download actions. "
        "Apps that appear only in unassociated skill-pack docs are not account access; apps in reference_paths attached to an active provider context may use skill_pack_read and connector_request to check the provider before claiming access. "
        "Do not use connector gateways as a generic web-search fallback for ordinary chat. "
        "Use skill_pack reference only when an allowed connector or specialized capability needs its installed docs. "
        "When assistant_no_tool_draft is present, use it only as recovery evidence that the previous draft failed "
        "to produce a grounded tool-backed result; classify the user's current request from the schema and registered "
        "runtime capabilities, then validate against available tools. Do not treat assistant_no_tool_draft as a new "
        "user request. When uncertain, choose none."
    )
    try:
        # Tool-scope classification is only a latency optimization. If this
        # model call is slow or unavailable, the normal request_tool_scope
        # fallback remains visible and can widen tools from structured output.
        response = model_client.create(
            messages=[{"role": "user", "content": [{"type": "text", "text": json.dumps(prompt, ensure_ascii=False)}]}],
            tools=[],
            max_tokens=_tool_scope_classifier_max_tokens(),
            system=system,
            timeout=_tool_scope_classifier_timeout_seconds(),
        )
    except Exception:
        logger.debug("Turn tool-scope decision failed; using structured fallback scope", exc_info=True)
        if len(set(evidence.requested_extensions)) > 1:
            fallback_decision = TurnToolScopeDecision(
                requested_artifact_extensions=tuple(dict.fromkeys(evidence.requested_extensions)),
                confidence=1.0,
                valid=True,
            )
            refined_fallback = _refine_ambiguous_artifact_extension_contract(
                model_client=model_client,
                user_message=user_message,
                evidence=evidence,
                decision=fallback_decision,
            )
            if refined_fallback.excluded_artifact_extensions:
                if cache_enabled:
                    runtime_cache.set_json(
                        _TOOL_SCOPE_DECISION_CACHE_NAMESPACE,
                        cache_key,
                        _tool_scope_decision_to_payload(refined_fallback),
                        version=_TOOL_SCOPE_DECISION_CACHE_VERSION,
                        ttl_seconds=_TOOL_SCOPE_DECISION_CACHE_TTL_SECONDS,
                        persistent=True,
                        max_entries=128,
                    )
                return refined_fallback
        return TurnToolScopeDecision()
    decision = _validated_turn_tool_scope_decision(
        _parse_turn_tool_scope_decision(_extract_response_text(response)),
        evidence=evidence,
        registry=registry,
        active_connector_providers=active_connector_providers,
    )
    if not decision.valid:
        return TurnToolScopeDecision()
    decision = _refine_ambiguous_artifact_extension_contract(
        model_client=model_client,
        user_message=user_message,
        evidence=evidence,
        decision=decision,
    )
    if cache_enabled:
        runtime_cache.set_json(
            _TOOL_SCOPE_DECISION_CACHE_NAMESPACE,
            cache_key,
            _tool_scope_decision_to_payload(decision),
            version=_TOOL_SCOPE_DECISION_CACHE_VERSION,
            ttl_seconds=_TOOL_SCOPE_DECISION_CACHE_TTL_SECONDS,
            persistent=True,
            max_entries=128,
        )
    return decision


def _tool_scope_decision_has_substantive_scope(decision: object | None) -> bool:
    if decision is None:
        return False
    requested_tool_names = {
        str(name or "").strip()
        for name in (getattr(decision, "requested_tool_names", ()) or ())
        if str(name or "").strip()
    }
    required_tool_names = {
        str(name or "").strip()
        for name in (getattr(decision, "required_tool_names", ()) or ())
        if str(name or "").strip()
    }
    non_bootstrap_requested = requested_tool_names - {_SCOPE_REQUEST_TOOL_NAME}
    non_bootstrap_required = required_tool_names - {_SCOPE_REQUEST_TOOL_NAME}
    return bool(
        getattr(decision, "allow_web_tools", False)
        or getattr(decision, "allow_scheduler_tools", False)
        or getattr(decision, "allow_connector_tools", False)
        or getattr(decision, "allow_skill_pack_tools", False)
        or non_bootstrap_requested
        or non_bootstrap_required
        or bool(getattr(decision, "requested_artifact_extensions", ()))
        or bool(getattr(decision, "required_embedded_media_extensions", ()))
    )


def _turn_tool_evidence_has_structured_scope(evidence: TurnToolEvidence) -> bool:
    return bool(
        evidence.has_url_target
        or evidence.has_attachments
        or evidence.has_email_address_target
        or evidence.artifact_requested
        or evidence.context_linked
        or evidence.numbered_option_selected
        or evidence.prior_tool_scopes
    )


_PLANNER_ARTIFACT_SCOPE_TOOLS = (
    "archive_create",
    "archive_extract",
    "browser_navigate",
    "browser_extract_text",
    "browser_extract_items",
    "browser_image_collect",
    "browser_run_js",
    "browser_screenshot",
    "file_download",
    "file_read",
    "file_search",
    "workspace_summary",
    "file_write",
    "document_create",
    "pdf_create",
    "pdf_edit",
    "presentation_create",
    "spreadsheet_create",
)


def _planner_materialized_registry(
    base_registry,
    *,
    evidence: TurnToolEvidence,
    decision: TurnToolScopeDecision | None,
) -> ScopedTurnToolRegistry:
    existing = decision or TurnToolScopeDecision()
    requested_extensions = tuple(
        dict.fromkeys(
            [
                *tuple(getattr(evidence, "requested_extensions", ()) or ()),
                *tuple(getattr(existing, "requested_artifact_extensions", ()) or ()),
                *_PLANNER_FALLBACK_ARTIFACT_EXTENSIONS,
            ]
        )
    )
    planner_evidence = TurnToolEvidence(
        has_url_target=True,
        has_attachments=True,
        has_email_address_target=evidence.has_email_address_target,
        requested_extensions=requested_extensions,
        context_linked=evidence.context_linked,
        saved_history_available=evidence.saved_history_available,
        slash_prefixed_literal=evidence.slash_prefixed_literal,
        numbered_option_selected=evidence.numbered_option_selected,
        prior_tool_scopes=evidence.prior_tool_scopes,
    )
    return ScopedTurnToolRegistry(
        base_registry,
        evidence=planner_evidence,
        tool_scope_decision=TurnToolScopeDecision(
            web_action=(
                existing.web_action
                if str(existing.web_action or "").strip().lower() not in {"", "none"}
                else "browser_interaction"
            ),
            scheduler_action=existing.scheduler_action,
            scheduler_toggle_enabled=existing.scheduler_toggle_enabled,
            scheduler_selection_policy=existing.scheduler_selection_policy,
            scheduler_target_scope=existing.scheduler_target_scope,
            skill_pack_action=existing.skill_pack_action,
            connector_app_ids=existing.connector_app_ids,
            connector_source_user_requested=existing.connector_source_user_requested,
            connector_source_evidence=existing.connector_source_evidence,
            requested_tool_names=tuple(
                dict.fromkeys(
                    [
                        *tuple(getattr(existing, "requested_tool_names", ()) or ()),
                        *_PLANNER_ARTIFACT_SCOPE_TOOLS,
                    ]
                )
            ),
            required_tool_names=tuple(getattr(existing, "required_tool_names", ()) or ()),
            requested_artifact_extensions=requested_extensions,
            required_embedded_media_extensions=tuple(
                getattr(existing, "required_embedded_media_extensions", ()) or ()
            ),
            confidence=max(float(getattr(existing, "confidence", 0.0) or 0.0), 1.0),
            valid=True,
        ),
    )


def materialize_mini_agent_tool_scope_registry(
    registry,
    *,
    model_client: object | None,
    user_message: str,
    evidence: TurnToolEvidence | None = None,
    planner_requested: bool = False,
):
    """Resolve fixed DeepAgent tool scope before mini-agent dispatch."""

    existing_decision = getattr(registry, "turn_tool_scope_decision", None)
    base_registry = getattr(registry, "_delegate", registry)
    materialized_evidence = evidence
    if materialized_evidence is None:
        registry_evidence = getattr(registry, "_evidence", None)
        materialized_evidence = registry_evidence if isinstance(registry_evidence, TurnToolEvidence) else TurnToolEvidence()
    if planner_requested and _tool_scope_decision_has_substantive_scope(existing_decision):
        return _planner_materialized_registry(
            base_registry,
            evidence=materialized_evidence,
            decision=existing_decision,
        )
    if _tool_scope_decision_has_substantive_scope(existing_decision):
        return registry
    decision = build_turn_tool_scope_decision(
        model_client=model_client,
        user_message=user_message,
        evidence=materialized_evidence,
        registry=base_registry,
        force_model_decision=True,
        cache_decision=False,
    )
    if planner_requested:
        return _planner_materialized_registry(
            base_registry,
            evidence=materialized_evidence,
            decision=decision,
        )
    if not _tool_scope_decision_has_substantive_scope(decision):
        if _turn_tool_evidence_has_structured_scope(materialized_evidence):
            return ScopedTurnToolRegistry(
                base_registry,
                evidence=materialized_evidence,
                tool_scope_decision=decision,
            )
        return registry
    return ScopedTurnToolRegistry(base_registry, evidence=materialized_evidence, tool_scope_decision=decision)


class PlainFastPathToolRegistry:
    """Pass-through registry for turns that do not need special scoped tools."""

    def __init__(self, delegate: object) -> None:
        self._delegate = delegate
        self.turn_tool_scope_decision = getattr(delegate, "turn_tool_scope_decision", None)

    def list_specs(self):
        list_specs = getattr(self._delegate, "list_specs", None)
        if callable(list_specs):
            return list_specs()
        return []

    def list_tool_definitions(self, *args, **kwargs):
        list_tool_definitions = getattr(self._delegate, "list_tool_definitions", None)
        if callable(list_tool_definitions):
            return list_tool_definitions(*args, **kwargs)
        return []

    def get_spec(self, name: str):
        return self._delegate.get_spec(name)

    def invoke(self, invocation):
        return self._delegate.invoke(invocation)

    def can_invoke_tool(self, name: str) -> bool:
        can_invoke = getattr(self._delegate, "can_invoke_tool", None)
        if callable(can_invoke):
            return bool(can_invoke(name))
        try:
            self._delegate.get_spec(name)
        except Exception:
            return False
        return True

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


class PlainNoToolFastPathRegistry:
    """Empty registry for first-pass turns with no structured tool evidence."""

    def __init__(self, delegate: object) -> None:
        self._delegate = delegate
        self.turn_tool_scope_decision = TurnToolScopeDecision(valid=True)

    def list_specs(self):
        return []

    def list_tool_definitions(self, *args, **kwargs):
        return []

    def get_spec(self, name: str):
        raise KeyError(name)

    def invoke(self, invocation):
        raise KeyError(getattr(invocation, "tool_name", ""))

    def can_invoke_tool(self, name: str) -> bool:
        return False

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


def _plain_no_tool_fast_path_allowed(evidence: TurnToolEvidence) -> bool:
    return not (
        evidence.context_linked
        or evidence.has_url_target
        or evidence.has_attachments
        or evidence.has_email_address_target
        or evidence.artifact_requested
        or evidence.numbered_option_selected
        or evidence.prior_tool_scopes
    )


def _plain_direct_safe_read_tool_names(registry: object) -> tuple[str, ...]:
    try:
        specs = tuple(registry.list_specs())
    except Exception:
        return ()
    names: list[str] = []
    for spec in specs:
        name = str(getattr(spec, "name", "") or "")
        if name not in _PLAIN_DIRECT_SAFE_READ_TOOLS:
            continue
        if getattr(spec, "side_effect_class", None) != ToolSideEffectClass.READ:
            continue
        if bool(getattr(spec, "requires_approval", True)):
            continue
        names.append(name)
    return tuple(sorted(names))


def turn_tool_registry_for_evidence(
    registry: object,
    *,
    evidence: TurnToolEvidence,
    model_client: object | None,
    user_message: str,
    skip_tool_scope_decision: bool,
) -> object:
    """Build the platform-neutral scoped registry for one chat turn."""

    if getattr(registry, "scheduled_task_execution_registry", False):
        # Cron execution has already been narrowed by typed scheduled-task
        # policy. Re-running ordinary chat scoping can hide the real cron tools.
        return registry
    if skip_tool_scope_decision:
        if evidence.saved_history_available and _registry_has_scoped_special_tools(registry):
            return scoped_turn_tool_registry(
                registry,
                evidence=evidence,
                model_client=None,
                user_message=user_message,
            )
        if _plain_no_tool_fast_path_allowed(evidence) and not turn_tool_scope_decision_may_apply(
            evidence,
            user_message=user_message,
        ):
            direct_safe_read_tools = _plain_direct_safe_read_tool_names(registry)
            if direct_safe_read_tools:
                return ScopedTurnToolRegistry(
                    registry,
                    evidence=evidence,
                    tool_scope_decision=TurnToolScopeDecision(
                        requested_tool_names=direct_safe_read_tools,
                        confidence=1.0,
                        valid=True,
                    ),
                )
            return PlainNoToolFastPathRegistry(registry)
        if _registry_has_scoped_special_tools(registry):
            return scoped_turn_tool_registry(
                registry,
                evidence=evidence,
                model_client=None,
                user_message=user_message,
            )
        return PlainFastPathToolRegistry(registry)
    return scoped_turn_tool_registry(
        registry,
        evidence=evidence,
        model_client=model_client,
        user_message=user_message,
    )


def _turn_needs_active_connector_context(
    evidence: TurnToolEvidence,
    *,
    user_message: str | None = None,
) -> bool:
    if getattr(evidence, "has_email_address_target", False):
        return True
    if getattr(evidence, "context_linked", False) and evidence.has_prior_tool_scope("connector"):
        return True
    if getattr(evidence, "numbered_option_selected", False) and evidence.has_prior_tool_scope("connector"):
        return True
    try:
        providers = _active_connector_app_id_providers_for_scope_decision(allow_runtime_load=True)
        if (
            mentioned_connector_app_ids(user_message or "", providers)
            or _structured_connector_tool_family_app_ids_for_message(user_message or "", providers)
        ):
            return True
        active_providers = tuple(_active_connector_provider_context())
        return bool(
            active_providers
            and (
                mentioned_connector_app_ids(user_message or "", active_providers)
                or _structured_connector_tool_family_app_ids_for_message(user_message or "", active_providers)
            )
        )
    except Exception:
        logger.debug("Unable to evaluate cached connector context need", exc_info=True)
        return False


def scoped_turn_tool_registry(
    registry,
    *,
    evidence: TurnToolEvidence,
    model_client: object | None = None,
    user_message: str | None = None,
):
    force_model_decision = False
    mentioned_connector_app_ids_for_turn: tuple[str, ...] = ()
    active_connector_providers = (
        _active_connector_provider_context()
        if _turn_needs_active_connector_context(evidence, user_message=user_message)
        else ()
    )
    if not turn_tool_evidence_needs_model_scope_decision(evidence):
        try:
            mentioned_connector_app_ids_for_turn = _mentioned_active_connector_app_ids_for_scope(
                user_message or "",
                active_connector_providers,
            )
            connector_decision = _connector_source_token_scope_decision(
                user_message=user_message or "",
                registry=registry,
                active_connector_providers=active_connector_providers,
            )
            if connector_decision is not None and _connector_source_token_decision_can_skip_classifier(
                connector_decision
            ):
                return ScopedTurnToolRegistry(
                    registry,
                    evidence=evidence,
                    tool_scope_decision=connector_decision,
                )
            force_model_decision = bool(mentioned_connector_app_ids_for_turn)
        except Exception:
            logger.debug("Unable to evaluate active connector app mention", exc_info=True)
            force_model_decision = False
    decision = build_turn_tool_scope_decision(
        model_client=model_client,
        user_message=user_message or "",
        evidence=evidence,
        registry=registry,
        force_model_decision=force_model_decision,
    )
    if mentioned_connector_app_ids_for_turn and not getattr(decision, "allow_connector_tools", False):
        connector_decision = _connector_source_token_scope_decision(
            user_message=user_message or "",
            registry=registry,
            active_connector_providers=active_connector_providers,
            requested_artifact_extensions=getattr(decision, "requested_artifact_extensions", ()),
            required_embedded_media_extensions=getattr(decision, "required_embedded_media_extensions", ()),
        )
        if connector_decision is not None:
            decision = connector_decision
    if evidence.has_email_address_target and not getattr(decision, "allow_connector_tools", False):
        email_decision = _email_target_scope_decision(
            registry=registry,
            active_connector_providers=active_connector_providers,
            requested_artifact_extensions=getattr(decision, "requested_artifact_extensions", ()),
            required_embedded_media_extensions=getattr(decision, "required_embedded_media_extensions", ()),
        )
        if email_decision is not None:
            decision = email_decision
    if evidence.context_linked and not _registry_has_scoped_special_tools(registry):
        return registry
    return ScopedTurnToolRegistry(registry, evidence=evidence, tool_scope_decision=decision)


def turn_tool_evidence_needs_model_scope_decision(evidence: TurnToolEvidence) -> bool:
    return bool(
        getattr(evidence, "has_url_target", False)
        or getattr(evidence, "has_attachments", False)
        or getattr(evidence, "has_email_address_target", False)
        or getattr(evidence, "artifact_requested", False)
        or getattr(evidence, "numbered_option_selected", False)
    )


def turn_tool_scope_decision_may_apply(evidence: TurnToolEvidence, *, user_message: str = "") -> bool:
    if turn_tool_evidence_needs_model_scope_decision(evidence):
        return True
    try:
        # This helper runs on the hot path for ordinary chat. Connector app ids
        # are warmed/cached separately. Installed connectors are capability
        # inventory, not routing evidence; require a structured signal or an
        # exact cached app-id mention before waking the scope classifier.
        providers = _active_connector_app_id_providers_for_scope_decision(allow_runtime_load=False)
        return bool(
            mentioned_connector_app_ids(user_message, providers)
            or _structured_connector_tool_family_app_ids_for_message(user_message, providers)
        )
    except Exception:
        logger.debug("Unable to evaluate active connector app scope evidence", exc_info=True)
        return False


def tool_registry_allows_skill_pack_context(registry) -> bool:
    try:
        registry.get_spec(_SCOPE_REQUEST_TOOL_NAME)
        return True
    except KeyError:
        pass
    try:
        registry.get_spec("skill_pack_read")
        return True
    except KeyError:
        return False


def tool_registry_allows_skill_pack_prompt_context(registry) -> bool:
    decision = getattr(registry, "turn_tool_scope_decision", None)
    if decision is not None and getattr(decision, "skill_pack_action", "none") == "reference":
        return True
    return False


def tool_registry_allows_connector_context(registry) -> bool:
    try:
        registry.get_spec("connector_request")
        return True
    except KeyError:
        return False


__all__ = [
    "ScopedTurnToolRegistry",
    "TurnToolScopeDecision",
    "TurnToolEvidence",
    "build_turn_tool_scope_decision",
    "build_turn_tool_evidence",
    "is_slash_prefixed_literal_message",
    "materialize_mini_agent_tool_scope_registry",
    "PlainFastPathToolRegistry",
    "PlainNoToolFastPathRegistry",
    "scoped_turn_tool_registry",
    "should_include_prior_turn_messages",
    "tool_registry_allows_connector_context",
    "tool_registry_allows_skill_pack_context",
    "tool_registry_allows_skill_pack_prompt_context",
    "turn_tool_registry_for_evidence",
    "turn_tool_scope_decision_may_apply",
    "turn_tool_evidence_needs_model_scope_decision",
    "turn_is_context_linked",
]

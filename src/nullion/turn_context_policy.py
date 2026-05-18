"""Typed context and tool eligibility policy for chat turns."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from typing import Iterable

from nullion import runtime_cache
from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.cron_execution_tools import (
    CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS,
    CRON_EXECUTION_BLOCKED_TOOLS,
)
from nullion.task_frames import TaskFrameContinuationMode, extract_url_target
from nullion.tools import ToolInvocation, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec

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
        "browser_navigate",
        "browser_extract_text",
        "browser_find",
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
        "web_fetch",
    }
)
_BROWSER_INTERACTION_SCOPE_TOOLS = (
    "browser_navigate",
    "browser_extract_text",
    "browser_find",
    "browser_snapshot",
    "browser_click_element",
    "browser_click_id",
    "browser_type_field",
    "browser_type_id",
    "browser_assert_page_state",
    "browser_select_combobox",
    "browser_scroll",
    "browser_wait_for",
)
_PDF_EXTENSIONS = frozenset({".pdf"})
_PRESENTATION_EXTENSIONS = frozenset({".ppt", ".pptx"})
_SPREADSHEET_EXTENSIONS = frozenset({".csv", ".tsv", ".xls", ".xlsx"})
_TEXT_WRITE_EXTENSIONS = frozenset({"", ".csv", ".htm", ".html", ".json", ".md", ".svg", ".tsv", ".txt", ".yaml", ".yml"})
_CONNECTOR_TOOLS = frozenset({"connector_request"})
_CONNECTOR_CAPABILITY_TAGS = frozenset({"connector"})
_CONNECTOR_TYPED_TOOLS = frozenset({
    "connector_request",
    "email_send",
    "email_search",
    "email_read",
    "calendar_list",
})
_SCHEDULER_READ_TOOLS = frozenset({"list_crons", "list_reminders"})
_SCHEDULER_RUN_TOOLS = frozenset({"run_cron"})
_SCHEDULER_MUTATE_TOOLS = frozenset({
    "set_reminder",
    "create_cron",
    "update_cron",
    "delete_cron",
    "enable_cron",
    "disable_cron",
})
_SKILL_PACK_TOOLS = frozenset({"skill_pack_read"})
_SKILL_PACK_CAPABILITY_TAGS = frozenset({"skill_pack"})
_LOCAL_SHELL_TOOLS = frozenset({"terminal_exec"})
_SCOPE_REQUEST_TOOL_NAME = "request_tool_scope"
_SCOPE_REQUEST_TOOL_SPEC = ToolSpec(
    name=_SCOPE_REQUEST_TOOL_NAME,
    description=(
        "Request the exact tool family needed for this turn when visible tools are insufficient. "
        "Call this before saying a registered capability is unavailable. "
        "Use connector for authenticated external account APIs such as mail, calendars, docs, "
        "and connected services. Use scheduler_* only for Nullion's own recurring jobs and "
        "reminders. Use web for public websites and live public lookups. Use skill_pack only "
        "when detailed installed-pack reference docs are needed. Capabilities: web, "
        "scheduler_read, scheduler_run, scheduler_mutate, connector, skill_pack, weather, "
        "image_generation, local_files, local_shell."
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
                    "Tool families to expose for this same turn. connector exposes connected "
                    "account/API tools; scheduler_read/run/mutate exposes Nullion cron/reminder "
                    "tools only; web exposes public web/browser tools; skill_pack exposes exact "
                    "installed skill-pack docs; weather and image_generation expose their direct tools; "
                    "local_shell exposes local terminal execution when registered."
                ),
                "items": {
                    "type": "string",
                    "enum": [
                        "web",
                        "scheduler_read",
                        "scheduler_run",
                        "scheduler_mutate",
                        "connector",
                        "skill_pack",
                        "weather",
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
            "tool_names": {
                "type": "array",
                "description": "Optional exact registered tools to expose inside the requested capability family.",
                "items": {
                    "type": "string",
                    "enum": [
                        "connector_request",
                        "email_send",
                        "email_search",
                        "email_read",
                        "calendar_list",
                        "list_crons",
                        "list_reminders",
                        "run_cron",
                        "set_reminder",
                        "create_cron",
                        "update_cron",
                        "delete_cron",
                        "enable_cron",
                        "disable_cron",
                        "web_search",
                        "web_fetch",
                        "browser_navigate",
                        "browser_extract_text",
                        "browser_assert_page_state",
                        "browser_screenshot",
                        "weather_forecast",
                        "image_generate",
                        "file_read",
                        "file_write",
                        "terminal_exec",
                    ],
                },
            },
            "reason": {"type": "string", "description": "Brief structured reason for the requested scope."},
        },
        "required": ["capabilities"],
        "additionalProperties": False,
    },
    capability_tags=("scope_request",),
)
_KNOWN_PRIOR_TOOL_SCOPES = frozenset({"connector", "scheduler", "skill_pack", "web"})
_WEB_ACTIONS = frozenset({"none", "open_url", "live_research", "browser_interaction"})
_SCHEDULER_ACTIONS = frozenset({"none", "inspect", "run", "mutate"})
_SKILL_PACK_ACTIONS = frozenset({"none", "reference", "connector"})
_TOOL_SCOPE_DECISION_CACHE_NAMESPACE = "tool_scope.decision"
_TOOL_SCOPE_DECISION_CACHE_VERSION = "v8"
_TOOL_SCOPE_DECISION_CACHE_TTL_SECONDS = 24 * 60 * 60
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


def _exact_scope_tools_for_capability(capability: str, tool_names: Iterable[str]) -> tuple[str, ...]:
    capability_tools: frozenset[str]
    if capability == "connector":
        capability_tools = _CONNECTOR_TYPED_TOOLS | _SKILL_PACK_TOOLS
    elif capability == "scheduler_read":
        capability_tools = _SCHEDULER_READ_TOOLS
    elif capability == "scheduler_run":
        capability_tools = _SCHEDULER_READ_TOOLS | _SCHEDULER_RUN_TOOLS
    elif capability == "scheduler_mutate":
        capability_tools = _SCHEDULER_READ_TOOLS | _SCHEDULER_RUN_TOOLS | _SCHEDULER_MUTATE_TOOLS
    elif capability == "web":
        capability_tools = _URL_BOUNDARY_TOOLS | frozenset({"web_search"})
    elif capability == "skill_pack":
        capability_tools = _SKILL_PACK_TOOLS
    elif capability == "weather":
        capability_tools = frozenset({"weather_forecast"})
    elif capability == "image_generation":
        capability_tools = frozenset({"image_generate"})
    elif capability == "local_files":
        capability_tools = frozenset({"file_read", "file_write"})
    elif capability == "local_shell":
        capability_tools = _LOCAL_SHELL_TOOLS
    else:
        return ()
    return tuple(dict.fromkeys(tool_name for tool_name in tool_names if tool_name in capability_tools))


@dataclass(frozen=True, slots=True)
class TurnToolEvidence:
    has_url_target: bool = False
    has_attachments: bool = False
    requested_extensions: tuple[str, ...] = ()
    context_linked: bool = False
    slash_prefixed_literal: bool = False
    prior_tool_scopes: tuple[str, ...] = ()

    @property
    def artifact_requested(self) -> bool:
        return bool(self.requested_extensions)

    def has_prior_tool_scope(self, scope: str) -> bool:
        return scope in set(self.prior_tool_scopes)


@dataclass(frozen=True, slots=True)
class TurnToolScopeDecision:
    web_action: str = "none"
    scheduler_action: str = "none"
    skill_pack_action: str = "none"
    connector_app_ids: tuple[str, ...] = ()
    requested_tool_names: tuple[str, ...] = ()
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
        return self.skill_pack_action == "connector"

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

    def _delegate_tool_names(self) -> set[str]:
        try:
            return {str(getattr(spec, "name", "") or "") for spec in self._delegate.list_specs()}
        except Exception:
            try:
                return {str(definition.get("name") or "") for definition in self._delegate.list_tool_definitions()}
            except Exception:
                return set()

    @staticmethod
    def _definition_tags(definition: dict[str, object]) -> frozenset[str]:
        return frozenset(
            str(tag).strip().lower()
            for tag in (definition.get("capability_tags") or ())
            if str(tag).strip()
        )

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
        return not (
            self._evidence.context_linked
            or self._evidence.has_url_target
            or self._evidence.has_attachments
            or self._evidence.artifact_requested
            or self.turn_tool_scope_decision.allow_web_tools
            or self.turn_tool_scope_decision.allow_scheduler_tools
            or self.turn_tool_scope_decision.allow_connector_tools
            or self.turn_tool_scope_decision.allow_skill_pack_tools
            or bool(self.turn_tool_scope_decision.requested_tool_names)
        )

    def _requested_names_in(self, names: frozenset[str]) -> frozenset[str]:
        requested = frozenset(str(name or "") for name in self.turn_tool_scope_decision.requested_tool_names)
        return requested.intersection(names)

    def _is_allowed_tool_name(self, tool_name: str) -> bool:
        if tool_name == _SCOPE_REQUEST_TOOL_NAME:
            return True
        if tool_name in set(self.turn_tool_scope_decision.requested_tool_names):
            return True
        if self._is_scheduler_tool_name(tool_name):
            requested_scheduler_tools = self._requested_names_in(
                _SCHEDULER_READ_TOOLS | _SCHEDULER_RUN_TOOLS | _SCHEDULER_MUTATE_TOOLS
            )
            if requested_scheduler_tools:
                return tool_name in requested_scheduler_tools
            if self.turn_tool_scope_decision.scheduler_action == "run":
                return tool_name in (_SCHEDULER_RUN_TOOLS | _SCHEDULER_READ_TOOLS)
            if self.turn_tool_scope_decision.scheduler_action == "inspect":
                return tool_name in _SCHEDULER_READ_TOOLS
            if self.turn_tool_scope_decision.scheduler_action == "mutate":
                return True
            return self._evidence.context_linked and self._evidence.has_prior_tool_scope("scheduler")
        if self._is_connector_tool_name(tool_name):
            requested_connector_tools = self._requested_names_in(_CONNECTOR_TYPED_TOOLS)
            if requested_connector_tools:
                return tool_name in requested_connector_tools
            return (
                self.turn_tool_scope_decision.allow_connector_tools
                or (self._evidence.context_linked and self._evidence.has_prior_tool_scope("connector"))
            )
        if self._is_skill_pack_tool_name(tool_name):
            return self.turn_tool_scope_decision.allow_skill_pack_tools or (
                self._evidence.context_linked
                and (
                    self._evidence.has_prior_tool_scope("skill_pack")
                    or self._evidence.has_prior_tool_scope("connector")
                    or self.turn_tool_scope_decision.allow_connector_tools
                )
            )
        if self._evidence.context_linked:
            return True
        if tool_name == "file_read" and self._evidence.slash_prefixed_literal and not self._evidence.has_attachments:
            return False
        if tool_name == "file_write" and any(
            extension not in _TEXT_WRITE_EXTENSIONS
            for extension in self._evidence.requested_extensions
        ):
            return False
        if tool_name in _URL_BOUNDARY_TOOLS:
            if tool_name == "browser_screenshot" and not self._evidence.has_url_target:
                return False
            return (
                self._evidence.has_url_target
                or self.turn_tool_scope_decision.allow_web_tools
            )
        if tool_name in {"pdf_create", "pdf_edit"}:
            return bool(set(self._evidence.requested_extensions).intersection(_PDF_EXTENSIONS))
        if tool_name == "presentation_create":
            return bool(set(self._evidence.requested_extensions).intersection(_PRESENTATION_EXTENSIONS))
        if tool_name == "spreadsheet_create":
            return bool(set(self._evidence.requested_extensions).intersection(_SPREADSHEET_EXTENSIONS))
        if self._is_plain_independent_turn():
            return tool_name == _SCOPE_REQUEST_TOOL_NAME
        return True

    def get_spec(self, name: str):
        if name == _SCOPE_REQUEST_TOOL_NAME:
            return _SCOPE_REQUEST_TOOL_SPEC
        if not self._is_allowed_tool_name(name):
            raise KeyError(f"Unknown tool: {name}")
        return self._delegate.get_spec(name)

    def list_specs(self) -> list[object]:
        specs = [
            spec
            for spec in self._delegate.list_specs()
            if self._is_allowed_tool_name(str(getattr(spec, "name", "") or ""))
        ]
        if self._is_allowed_tool_name(_SCOPE_REQUEST_TOOL_NAME):
            specs.insert(0, _SCOPE_REQUEST_TOOL_SPEC)
        return specs

    def list_tool_definitions(self, *args, **kwargs) -> list[dict[str, object]]:
        definitions = [
            definition
            for definition in self._delegate.list_tool_definitions(*args, **kwargs)
            if self._is_allowed_tool_name(str(definition.get("name") or ""))
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
        return definitions

    def _tool_names_for_scope_request(self, arguments: dict[str, object]) -> tuple[str, ...]:
        raw_capabilities = arguments.get("capabilities")
        capabilities = tuple(
            dict.fromkeys(
                str(value or "").strip().lower()
                for value in (raw_capabilities if isinstance(raw_capabilities, list) else ())
                if str(value or "").strip()
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
        requested: list[str] = []
        for capability in capabilities:
            exact_for_capability = _exact_scope_tools_for_capability(capability, exact_tool_names)
            if exact_for_capability:
                requested.extend(exact_for_capability)
                if capability == "web" and any(
                    tool_name.startswith("browser_") for tool_name in exact_for_capability
                ):
                    requested.extend(_BROWSER_INTERACTION_SCOPE_TOOLS)
                continue
            if capability == "web":
                requested.extend(["web_search", "web_fetch", *_BROWSER_INTERACTION_SCOPE_TOOLS])
                if self._evidence.context_linked or self._evidence.has_prior_tool_scope("web"):
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
                        "set_reminder",
                        "create_cron",
                        "update_cron",
                        "delete_cron",
                        "enable_cron",
                        "disable_cron",
                        "run_cron",
                    ]
                )
            elif capability == "connector":
                requested.extend(sorted(_CONNECTOR_TYPED_TOOLS | _SKILL_PACK_TOOLS))
            elif capability == "skill_pack":
                requested.extend(sorted(_SKILL_PACK_TOOLS))
            elif capability == "weather":
                requested.append("weather_forecast")
            elif capability == "image_generation":
                requested.append("image_generate")
            elif capability == "local_files":
                requested.extend(["file_read", "file_write"])
            elif capability == "local_shell":
                requested.extend(sorted(_LOCAL_SHELL_TOOLS))
        available = self._delegate_tool_names()
        return tuple(dict.fromkeys(tool_name for tool_name in requested if tool_name in available))

    def apply_scope_request(self, invocation: ToolInvocation) -> tuple[ToolResult, "ScopedTurnToolRegistry"]:
        tool_names = self._tool_names_for_scope_request(invocation.arguments)
        raw_capabilities = invocation.arguments.get("capabilities")
        capabilities = {
            str(value or "").strip().lower()
            for value in (raw_capabilities if isinstance(raw_capabilities, list) else ())
            if str(value or "").strip()
        }
        raw_connector_app_ids = invocation.arguments.get("connector_app_ids")
        connector_app_ids = _unique_connector_app_ids(raw_connector_app_ids if isinstance(raw_connector_app_ids, list) else ())
        active_connector_providers = _active_connector_provider_context() if "connector" in capabilities else ()
        active_app_ids = _active_connector_app_ids_from_context(active_connector_providers)
        raw_tool_names = invocation.arguments.get("tool_names")
        explicitly_requested_tools = {
            str(value or "").strip()
            for value in (raw_tool_names if isinstance(raw_tool_names, list) else ())
            if str(value or "").strip()
        }
        if "connector" in capabilities and not active_connector_providers:
            tool_names = tuple(
                tool_name
                for tool_name in tool_names
                if tool_name in explicitly_requested_tools
                or tool_name not in (_CONNECTOR_TYPED_TOOLS | _SKILL_PACK_TOOLS)
            )
        if "connector" in capabilities:
            connector_app_ids = tuple(app_id for app_id in connector_app_ids if app_id in set(active_app_ids)) or active_app_ids
        existing = self.turn_tool_scope_decision
        scheduler_action = existing.scheduler_action
        if "scheduler_mutate" in capabilities:
            scheduler_action = "mutate"
        elif "scheduler_run" in capabilities:
            scheduler_action = "run"
        elif "scheduler_read" in capabilities:
            scheduler_action = "inspect"
        web_action = "live_research" if "web" in capabilities else existing.web_action
        skill_pack_action = existing.skill_pack_action
        if "connector" in capabilities and active_connector_providers:
            skill_pack_action = "connector"
        elif "skill_pack" in capabilities:
            skill_pack_action = "reference"
        widened = ScopedTurnToolRegistry(
            self._delegate,
            evidence=self._evidence,
            tool_scope_decision=TurnToolScopeDecision(
                web_action=web_action,
                scheduler_action=scheduler_action,
                skill_pack_action=skill_pack_action,
                connector_app_ids=connector_app_ids,
                requested_tool_names=tuple(dict.fromkeys([*existing.requested_tool_names, *tool_names])),
                confidence=max(existing.confidence, 1.0),
                valid=True,
            ),
        )
        return (
            ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={
                    "scope_requested": True,
                    "capabilities": sorted(capabilities),
                    "available_tools": list(tool_names),
                    "connector_app_ids": list(connector_app_ids),
                    "active_connector_providers": list(active_connector_providers),
                    "message": (
                        "Tool scope updated. Continue the same user request using the newly available tools."
                        if tool_names
                        else "No registered tools matched the requested capability scope."
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
            if app_id and allowed_app_ids and app_id not in allowed_app_ids:
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
        if self._is_allowed_tool_name(invocation.tool_name):
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
    return TurnToolEvidence(
        has_url_target=extract_url_target(user_message) is not None,
        has_attachments=bool(has_attachments),
        requested_extensions=normalized_extensions,
        context_linked=turn_is_context_linked(conversation_result),
        slash_prefixed_literal=is_slash_prefixed_literal_message(user_message),
        prior_tool_scopes=normalized_prior_tool_scopes,
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
    web_action = str(payload.get("web_action") or "none").strip().lower()
    if web_action not in _WEB_ACTIONS:
        web_action = "none"
    scheduler_action = str(payload.get("scheduler_action") or "none").strip().lower()
    if scheduler_action not in _SCHEDULER_ACTIONS:
        scheduler_action = "none"
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
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return TurnToolScopeDecision(
        web_action=web_action,
        scheduler_action=scheduler_action,
        skill_pack_action=skill_pack_action,
        connector_app_ids=connector_app_ids if skill_pack_action == "connector" else (),
        requested_tool_names=requested_tool_names,
        confidence=max(0.0, min(1.0, confidence)),
        valid=True,
    )


def _tool_scope_decision_to_payload(decision: TurnToolScopeDecision) -> dict[str, object]:
    return {
        "web_action": decision.web_action,
        "scheduler_action": decision.scheduler_action,
        "skill_pack_action": decision.skill_pack_action,
        "connector_app_ids": list(decision.connector_app_ids),
        "requested_tool_names": list(decision.requested_tool_names),
        "confidence": decision.confidence,
        "valid": decision.valid,
    }


def _tool_scope_decision_from_payload(payload: object) -> TurnToolScopeDecision | None:
    if not isinstance(payload, dict):
        return None
    web_action = str(payload.get("web_action") or "none").strip().lower()
    scheduler_action = str(payload.get("scheduler_action") or "none").strip().lower()
    skill_pack_action = str(payload.get("skill_pack_action") or "none").strip().lower()
    if web_action not in _WEB_ACTIONS or scheduler_action not in _SCHEDULER_ACTIONS or skill_pack_action not in _SKILL_PACK_ACTIONS:
        return None
    connector_app_ids = _unique_connector_app_ids(payload.get("connector_app_ids") if isinstance(payload.get("connector_app_ids"), list) else ())
    requested_tool_names = tuple(
        dict.fromkeys(
            str(tool_name or "").strip()
            for tool_name in (payload.get("requested_tool_names") if isinstance(payload.get("requested_tool_names"), list) else ())
            if str(tool_name or "").strip()
        )
    )
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return TurnToolScopeDecision(
        web_action=web_action,
        scheduler_action=scheduler_action,
        skill_pack_action=skill_pack_action,
        connector_app_ids=connector_app_ids if skill_pack_action == "connector" else (),
        requested_tool_names=requested_tool_names,
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
    for connection in connections:
        provider_id = str(getattr(connection, "provider_id", "") or "").strip()
        if not provider_id or not getattr(connection, "active", True):
            continue
        normalized = provider_id.lower()
        if not (normalized.startswith("skill_pack_connector_") or normalized.endswith("_connector_provider")):
            continue
        entry: dict[str, object] = {
            "provider_id": provider_id,
            "display_name": str(getattr(connection, "display_name", "") or provider_id),
            "permission_mode": str(getattr(connection, "permission_mode", "") or "read"),
            "credential_scope": str(getattr(connection, "credential_scope", "") or "workspace"),
            "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
        }
        if entry["permission_mode"] == "write":
            entry["structured_tools"] = ["connector_request", "email_send", "email_search", "email_read", "calendar_list"]
        skill_pack_id = ""
        try:
            installed_packs = list_installed_skill_packs()
        except Exception:
            installed_packs = ()
        for candidate in installed_packs:
            candidate_id = str(getattr(candidate, "pack_id", "") or "").strip().lower()
            slug = "".join(ch if ch.isalnum() else "_" for ch in candidate_id).strip("_")
            if f"skill_pack_connector_{slug}" == normalized:
                skill_pack_id = candidate_id
                break
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
        providers.append(entry)
    return providers[:12]


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
        if isinstance(provider, dict):
            raw = provider.get("active_app_ids")
            if isinstance(raw, list):
                app_ids.extend(raw)
    return _unique_connector_app_ids(app_ids)


def _validated_turn_tool_scope_decision(
    decision: TurnToolScopeDecision,
    *,
    registry,
    active_connector_providers: Iterable[object],
) -> TurnToolScopeDecision:
    requested_tool_names = _validated_requested_tool_names(
        decision.requested_tool_names,
        registry=registry,
        allow_connector=decision.skill_pack_action == "connector",
        active_connector_providers=active_connector_providers,
    )
    if decision.skill_pack_action != "connector":
        return TurnToolScopeDecision(
            web_action=decision.web_action,
            scheduler_action=decision.scheduler_action,
            skill_pack_action=decision.skill_pack_action,
            requested_tool_names=requested_tool_names,
            confidence=decision.confidence,
            valid=decision.valid,
        )
    providers = tuple(active_connector_providers)
    if not providers:
        return TurnToolScopeDecision(
            web_action=decision.web_action,
            scheduler_action=decision.scheduler_action,
            skill_pack_action="none",
            requested_tool_names=(),
            confidence=decision.confidence,
            valid=decision.valid,
        )
    active_app_ids = set(_active_connector_app_ids_from_context(active_connector_providers))
    if not active_app_ids:
        return decision
    selected_app_ids = tuple(app_id for app_id in decision.connector_app_ids if app_id in active_app_ids)
    if not selected_app_ids:
        return TurnToolScopeDecision(
            web_action=decision.web_action,
            scheduler_action=decision.scheduler_action,
            skill_pack_action="none",
            requested_tool_names=(),
            confidence=decision.confidence,
            valid=decision.valid,
        )
    return TurnToolScopeDecision(
        web_action=decision.web_action,
        scheduler_action=decision.scheduler_action,
        skill_pack_action=decision.skill_pack_action,
        connector_app_ids=selected_app_ids,
        requested_tool_names=requested_tool_names,
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
    connector_structured_tools: set[str] = set()
    for provider in active_connector_providers:
        if not isinstance(provider, dict):
            continue
        structured = provider.get("structured_tools")
        if isinstance(structured, list):
            connector_structured_tools.update(str(tool or "").strip() for tool in structured if str(tool or "").strip())
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
    memory_context: str = "",
    evidence: TurnToolEvidence,
    registry: object,
    model_client: object | None,
    active_connector_providers: Iterable[object] = (),
    skill_pack_index: str = "",
) -> dict[str, object]:
    return {
        "user_turn": str(user_message or ""),
        "memory_context": str(memory_context or "")[:1200],
        "evidence": {
            "context_linked": evidence.context_linked,
            "has_url_target": evidence.has_url_target,
            "has_attachments": evidence.has_attachments,
            "requested_extensions": list(evidence.requested_extensions),
            "slash_prefixed_literal": evidence.slash_prefixed_literal,
            "prior_tool_scopes": list(evidence.prior_tool_scopes),
        },
        "active_connector_providers": list(active_connector_providers),
        "skill_pack_index": str(skill_pack_index or ""),
        "registry": _tool_scope_registry_signature(registry),
        "model": _tool_scope_model_signature(model_client),
    }


def build_turn_tool_scope_decision(
    *,
    model_client: object | None,
    user_message: str,
    memory_context: str = "",
    evidence: TurnToolEvidence,
    registry,
    force_model_decision: bool = False,
) -> TurnToolScopeDecision:
    has_memory_context = bool(str(memory_context or "").strip())
    if (
        model_client is None
        or not _registry_has_scoped_special_tools(registry)
        or (
            not force_model_decision
            and not has_memory_context
            and not turn_tool_evidence_needs_model_scope_decision(evidence)
        )
    ):
        return TurnToolScopeDecision()
    active_connector_providers = _active_connector_provider_context()
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
        memory_context=memory_context,
        evidence=evidence,
        registry=registry,
        model_client=model_client,
        active_connector_providers=active_connector_providers,
        skill_pack_index=skill_pack_index,
    )
    cached = runtime_cache.get_json(
        _TOOL_SCOPE_DECISION_CACHE_NAMESPACE,
        cache_key,
        version=_TOOL_SCOPE_DECISION_CACHE_VERSION,
        ttl_seconds=_TOOL_SCOPE_DECISION_CACHE_TTL_SECONDS,
        persistent=True,
    )
    cached_decision = _tool_scope_decision_from_payload(cached.value) if cached.hit else None
    if cached_decision is not None:
        return cached_decision
    prompt = {
        "surface": "ordinary_chat",
        "context_linked": evidence.context_linked,
        "has_url_target": evidence.has_url_target,
        "has_attachments": evidence.has_attachments,
        "requested_extensions": list(evidence.requested_extensions),
        "prior_tool_scopes": list(evidence.prior_tool_scopes),
        "active_connector_providers": active_connector_providers,
        "installed_skill_pack_index": skill_pack_index,
        "known_user_memory": str(memory_context or "")[:1200],
        "available_special_tool_scopes": [
            "web_or_browser",
            "scheduler",
            "skill_pack_reference",
            "connector_gateway",
            "local_shell",
        ],
        "user_turn": user_message,
    }
    system = (
        "Return only a JSON object matching this schema: "
        '{"web_action":"none|open_url|live_research|browser_interaction",'
        '"scheduler_action":"none|inspect|run|mutate",'
        '"skill_pack_action":"none|reference|connector","connector_app_ids":["active-app-id"],'
        '"requested_tool_names":["registered-tool-name"],"confidence":0.0}. '
        "Use web_action=open_url for explicit URL/domain targets, live_research for requests that need current public information, "
        "and browser_interaction for a user-visible webpage workflow. "
        "For browser workflows that must prove visible page state, include browser_assert_page_state in requested_tool_names when registered. "
        "For screenshot capture of a current or prior browser page, include browser_screenshot in requested_tool_names when registered. "
        "Use web_action=none when the request can be answered without web/browser tools. "
        "Use known_user_memory only as durable user preference context for this same turn; do not treat it as a separate task. "
        "Use scheduler actions only for scheduled-task or reminder control. "
        "For one-off reminder creation, request/use set_reminder; use create_cron only for recurring scheduled jobs. "
        "Do not choose scheduler just because a saved task could answer the domain. "
        "Use connector only when the request needs a connected external API/account and active_connector_providers lists an active_app_ids value that can satisfy it. "
        "When using connector, include exact app IDs from active_app_ids in connector_app_ids. "
        "When the connector action requires a specific structured account tool, include exact names from active_connector_providers.structured_tools in requested_tool_names. "
        'Use requested_tool_names=["terminal_exec"] when the turn requires local shell execution and terminal_exec is registered. '
        "Do not choose connector for apps that appear only in skill-pack references or docs. "
        "Do not use connector gateways as a generic web-search fallback for ordinary chat. "
        "Use skill_pack reference only when an allowed connector or specialized capability needs its installed docs. "
        "When uncertain, choose none."
    )
    try:
        response = model_client.create(
            messages=[{"role": "user", "content": [{"type": "text", "text": json.dumps(prompt, ensure_ascii=False)}]}],
            tools=[],
            max_tokens=_tool_scope_classifier_max_tokens(),
            system=system,
        )
    except Exception:
        logger.debug("Turn tool-scope decision failed; using structured fallback scope", exc_info=True)
        return TurnToolScopeDecision()
    decision = _validated_turn_tool_scope_decision(
        _parse_turn_tool_scope_decision(_extract_response_text(response)),
        registry=registry,
        active_connector_providers=active_connector_providers,
    )
    if not decision.valid:
        return TurnToolScopeDecision()
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


def materialize_mini_agent_tool_scope_registry(
    registry,
    *,
    model_client: object | None,
    user_message: str,
):
    """Resolve fixed DeepAgent tool scope before mini-agent dispatch."""

    existing_decision = getattr(registry, "turn_tool_scope_decision", None)
    if existing_decision is not None and (
        getattr(existing_decision, "allow_web_tools", False)
        or getattr(existing_decision, "allow_scheduler_tools", False)
        or getattr(existing_decision, "allow_connector_tools", False)
        or getattr(existing_decision, "allow_skill_pack_tools", False)
        or bool(getattr(existing_decision, "requested_tool_names", ()))
    ):
        return registry
    base_registry = getattr(registry, "_delegate", registry)
    evidence = TurnToolEvidence()
    decision = build_turn_tool_scope_decision(
        model_client=model_client,
        user_message=user_message,
        evidence=evidence,
        registry=base_registry,
        force_model_decision=True,
    )
    if not (
        decision.allow_web_tools
        or decision.allow_scheduler_tools
        or decision.allow_connector_tools
        or decision.allow_skill_pack_tools
        or decision.requested_tool_names
    ):
        return registry
    return ScopedTurnToolRegistry(base_registry, evidence=evidence, tool_scope_decision=decision)


def scoped_turn_tool_registry(
    registry,
    *,
    evidence: TurnToolEvidence,
    model_client: object | None = None,
    user_message: str | None = None,
    memory_context: str = "",
):
    decision = build_turn_tool_scope_decision(
        model_client=model_client,
        user_message=user_message or "",
        memory_context=memory_context,
        evidence=evidence,
        registry=registry,
    )
    if evidence.context_linked and not _registry_has_scoped_special_tools(registry):
        return registry
    return ScopedTurnToolRegistry(registry, evidence=evidence, tool_scope_decision=decision)


def turn_tool_evidence_needs_model_scope_decision(evidence: TurnToolEvidence) -> bool:
    context_linked = bool(getattr(evidence, "context_linked", False))
    return bool(
        getattr(evidence, "has_url_target", False)
        or getattr(evidence, "has_attachments", False)
        or getattr(evidence, "artifact_requested", False)
        or (context_linked and getattr(evidence, "prior_tool_scopes", ()))
    )


def turn_tool_scope_decision_may_apply(
    evidence: TurnToolEvidence,
    *,
    user_message: str = "",
    memory_context: str = "",
) -> bool:
    del user_message
    return bool(str(memory_context or "").strip()) or turn_tool_evidence_needs_model_scope_decision(evidence)


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
    evidence = getattr(registry, "_evidence", None)
    if evidence is not None and getattr(evidence, "context_linked", False):
        has_prior_scope = getattr(evidence, "has_prior_tool_scope", None)
        if callable(has_prior_scope) and (has_prior_scope("skill_pack") or has_prior_scope("connector")):
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
    "scoped_turn_tool_registry",
    "should_include_prior_turn_messages",
    "tool_registry_allows_connector_context",
    "tool_registry_allows_skill_pack_context",
    "tool_registry_allows_skill_pack_prompt_context",
    "turn_tool_scope_decision_may_apply",
    "turn_tool_evidence_needs_model_scope_decision",
    "turn_is_context_linked",
]

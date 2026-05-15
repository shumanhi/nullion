"""Typed context and tool eligibility policy for chat turns."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import re
from typing import Iterable
import urllib.parse
import urllib.request

from nullion import runtime_cache
from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.cron_execution_tools import (
    CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS,
    CRON_EXECUTION_BLOCKED_TOOLS,
)
from nullion.task_frames import TaskFrameContinuationMode, extract_url_target
from nullion.tools import ToolInvocation, ToolResult

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
        "browser_navigate",
        "browser_extract_text",
        "browser_find",
        "browser_run_js",
        "browser_scroll",
        "browser_screenshot",
        "browser_snapshot",
        "browser_type",
        "browser_wait_for",
        "browser_close",
        "web_fetch",
    }
)
_PDF_EXTENSIONS = frozenset({".pdf"})
_PRESENTATION_EXTENSIONS = frozenset({".ppt", ".pptx"})
_SPREADSHEET_EXTENSIONS = frozenset({".csv", ".tsv", ".xls", ".xlsx"})
_TEXT_WRITE_EXTENSIONS = frozenset({"", ".csv", ".htm", ".html", ".json", ".md", ".svg", ".tsv", ".txt", ".yaml", ".yml"})
_CONNECTOR_TOOLS = frozenset({"connector_request"})
_CONNECTOR_CAPABILITY_TAGS = frozenset({"connector"})
_SKILL_PACK_TOOLS = frozenset({"skill_pack_read"})
_SKILL_PACK_CAPABILITY_TAGS = frozenset({"skill_pack"})
_KNOWN_PRIOR_TOOL_SCOPES = frozenset({"connector", "scheduler", "skill_pack", "web"})
_WEB_ACTIONS = frozenset({"none", "open_url", "live_research", "browser_interaction"})
_SCHEDULER_ACTIONS = frozenset({"none", "inspect", "run", "mutate"})
_SKILL_PACK_ACTIONS = frozenset({"none", "reference", "connector"})
_TOOL_SCOPE_DECISION_CACHE_NAMESPACE = "tool_scope.decision"
_TOOL_SCOPE_DECISION_CACHE_VERSION = "v6"
_TOOL_SCOPE_DECISION_CACHE_TTL_SECONDS = 24 * 60 * 60
_MATON_ACTIVE_APPS_CACHE_NAMESPACE = "maton.active_apps"
_MATON_ACTIVE_APPS_CACHE_VERSION = "v1"
_MATON_ACTIVE_APPS_CACHE_TTL_SECONDS = 5 * 60
_SCHEDULER_REFERENCE_MIN_TOKEN_OVERLAP = 2


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
_SCHEDULER_REFERENCE_MIN_TOKEN_LENGTH = 4


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
        return self.skill_pack_action == "connector" and bool(self.connector_app_ids)

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
        if invocation.tool_name == "email_send":
            return "google-mail"
        raw_url = invocation.arguments.get("url") if isinstance(invocation.arguments, dict) else None
        if not isinstance(raw_url, str):
            return ""
        try:
            parsed = urllib.parse.urlparse(raw_url)
        except Exception:
            return ""
        if parsed.netloc.lower() != "api.maton.ai":
            return ""
        return _normalize_connector_app_id(parsed.path.strip("/").split("/", 1)[0])

    def _is_skill_pack_tool_name(self, tool_name: str) -> bool:
        if tool_name in _SKILL_PACK_TOOLS:
            return True
        try:
            tags = self._spec_tags(self._delegate.get_spec(tool_name))
        except KeyError:
            return False
        return bool(tags.intersection(_SKILL_PACK_CAPABILITY_TAGS))

    def _is_allowed_tool_name(self, tool_name: str) -> bool:
        if self._is_scheduler_tool_name(tool_name):
            return self.turn_tool_scope_decision.allow_scheduler_tools or (
                self._evidence.context_linked and self._evidence.has_prior_tool_scope("scheduler")
            )
        if self._is_connector_tool_name(tool_name):
            return (
                self.turn_tool_scope_decision.allow_connector_tools
                or self._evidence.context_linked
                or self._evidence.has_prior_tool_scope("connector")
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
            return self._evidence.has_url_target or self.turn_tool_scope_decision.allow_web_tools
        if tool_name in {"pdf_create", "pdf_edit"}:
            return bool(set(self._evidence.requested_extensions).intersection(_PDF_EXTENSIONS))
        if tool_name == "presentation_create":
            return bool(set(self._evidence.requested_extensions).intersection(_PRESENTATION_EXTENSIONS))
        if tool_name == "spreadsheet_create":
            return bool(set(self._evidence.requested_extensions).intersection(_SPREADSHEET_EXTENSIONS))
        return True

    def get_spec(self, name: str):
        if not self._is_allowed_tool_name(name):
            raise KeyError(f"Unknown tool: {name}")
        return self._delegate.get_spec(name)

    def list_specs(self) -> list[object]:
        return [
            spec
            for spec in self._delegate.list_specs()
            if self._is_allowed_tool_name(str(getattr(spec, "name", "") or ""))
        ]

    def list_tool_definitions(self, *args, **kwargs) -> list[dict[str, object]]:
        return [
            definition
            for definition in self._delegate.list_tool_definitions(*args, **kwargs)
            if self._is_allowed_tool_name(str(definition.get("name") or ""))
        ]

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
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
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return TurnToolScopeDecision(
        web_action=web_action,
        scheduler_action=scheduler_action,
        skill_pack_action=skill_pack_action,
        connector_app_ids=connector_app_ids if skill_pack_action == "connector" else (),
        confidence=max(0.0, min(1.0, confidence)),
        valid=True,
    )


def _tool_scope_decision_to_payload(decision: TurnToolScopeDecision) -> dict[str, object]:
    return {
        "web_action": decision.web_action,
        "scheduler_action": decision.scheduler_action,
        "skill_pack_action": decision.skill_pack_action,
        "connector_app_ids": list(decision.connector_app_ids),
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
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return TurnToolScopeDecision(
        web_action=web_action,
        scheduler_action=scheduler_action,
        skill_pack_action=skill_pack_action,
        connector_app_ids=connector_app_ids if skill_pack_action == "connector" else (),
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
            "structured_tools": ["connector_request"],
        }
        if entry["permission_mode"] == "write":
            entry["structured_tools"] = ["connector_request", "email_send"]
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
                entry["reference_paths"] = list(list_skill_pack_reference_paths(pack.pack_id))[:6]
            except Exception:
                pass
        if normalized == "skill_pack_connector_maton_ai_api_gateway_skill":
            entry["active_app_ids"] = list(_active_maton_app_ids())
        providers.append(entry)
    return providers[:12]


def _active_maton_app_ids() -> tuple[str, ...]:
    cached = runtime_cache.get_json(
        _MATON_ACTIVE_APPS_CACHE_NAMESPACE,
        "workspace",
        version=_MATON_ACTIVE_APPS_CACHE_VERSION,
        ttl_seconds=_MATON_ACTIVE_APPS_CACHE_TTL_SECONDS,
        persistent=True,
    )
    cached_apps = cached.value if cached.hit else None
    if isinstance(cached_apps, list):
        return _unique_connector_app_ids(cached_apps)
    api_key = os.environ.get("MATON_API_KEY", "").strip()
    if not api_key:
        env_path = os.environ.get("NULLION_ENV_FILE", "").strip()
        if env_path:
            try:
                for line in Path(env_path).read_text().splitlines():
                    key, sep, value = line.partition("=")
                    if sep and key.strip() == "MATON_API_KEY":
                        api_key = value.strip().strip('"').strip("'")
                        break
            except Exception:
                api_key = ""
    if not api_key:
        return ()
    try:
        request = urllib.request.Request("https://api.maton.ai/connections?status=ACTIVE")
        request.add_header("Authorization", f"Bearer {api_key}")
        with urllib.request.urlopen(request, timeout=3.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        logger.debug("Could not load Maton active app ids", exc_info=True)
        return ()
    connections = payload.get("connections") if isinstance(payload, dict) else ()
    if not isinstance(connections, list):
        connections = []
    apps = _unique_connector_app_ids(
        connection.get("app")
        for connection in connections
        if isinstance(connection, dict) and str(connection.get("status") or "").upper() == "ACTIVE"
    )
    runtime_cache.set_json(
        _MATON_ACTIVE_APPS_CACHE_NAMESPACE,
        "workspace",
        list(apps),
        version=_MATON_ACTIVE_APPS_CACHE_VERSION,
        ttl_seconds=_MATON_ACTIVE_APPS_CACHE_TTL_SECONDS,
        persistent=True,
        max_entries=4,
    )
    return apps


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
    active_connector_providers: Iterable[object],
) -> TurnToolScopeDecision:
    if decision.skill_pack_action != "connector":
        return decision
    active_app_ids = set(_active_connector_app_ids_from_context(active_connector_providers))
    selected_app_ids = tuple(app_id for app_id in decision.connector_app_ids if app_id in active_app_ids)
    if not selected_app_ids:
        return TurnToolScopeDecision(
            web_action=decision.web_action,
            scheduler_action=decision.scheduler_action,
            skill_pack_action="none",
            confidence=decision.confidence,
            valid=decision.valid,
        )
    return TurnToolScopeDecision(
        web_action=decision.web_action,
        scheduler_action=decision.scheduler_action,
        skill_pack_action=decision.skill_pack_action,
        connector_app_ids=selected_app_ids,
        confidence=decision.confidence,
        valid=decision.valid,
    )


def _scheduler_reference_tokens(value: object) -> set[str]:
    tokens: set[str] = set()
    for token in re.findall(r"[a-z0-9]+", str(value or "").lower()):
        if len(token) < _SCHEDULER_REFERENCE_MIN_TOKEN_LENGTH:
            continue
        tokens.add(token)
        for suffix in ("ing", "ers", "er", "ed", "s"):
            if token.endswith(suffix) and len(token) - len(suffix) >= _SCHEDULER_REFERENCE_MIN_TOKEN_LENGTH:
                tokens.add(token[: -len(suffix)])
    return tokens


def _message_references_scheduler_job(user_message: str) -> bool:
    try:
        from nullion.crons import list_crons
    except Exception:
        return False
    try:
        jobs = list_crons(workspace_id=None)
    except Exception:
        return False
    message_text = str(user_message or "")
    message_tokens = _scheduler_reference_tokens(message_text)
    if not message_tokens:
        return False
    for job in jobs:
        job_id = str(getattr(job, "id", "") or "").strip().lower()
        if job_id and job_id in message_text.lower():
            return True
        job_tokens = _scheduler_reference_tokens(
            " ".join(
                (
                    str(getattr(job, "name", "") or ""),
                    str(getattr(job, "task", "") or ""),
                )
            )
        )
        if len(message_tokens.intersection(job_tokens)) >= _SCHEDULER_REFERENCE_MIN_TOKEN_OVERLAP:
            return True
    return False


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
) -> dict[str, object]:
    return {
        "user_turn": str(user_message or ""),
        "evidence": {
            "context_linked": evidence.context_linked,
            "has_url_target": evidence.has_url_target,
            "has_attachments": evidence.has_attachments,
            "requested_extensions": list(evidence.requested_extensions),
            "slash_prefixed_literal": evidence.slash_prefixed_literal,
            "prior_tool_scopes": list(evidence.prior_tool_scopes),
        },
        "active_connector_providers": list(active_connector_providers),
        "registry": _tool_scope_registry_signature(registry),
        "model": _tool_scope_model_signature(model_client),
    }


def build_turn_tool_scope_decision(
    *,
    model_client: object | None,
    user_message: str,
    evidence: TurnToolEvidence,
    registry,
) -> TurnToolScopeDecision:
    scheduler_scope_may_apply = _message_references_scheduler_job(user_message)
    connector_scope_may_apply = _runtime_has_active_connector_provider()
    if (
        model_client is None
        or not _registry_has_scoped_special_tools(registry)
        or (
            not scheduler_scope_may_apply
            and not connector_scope_may_apply
            and not turn_tool_evidence_needs_model_scope_decision(evidence)
        )
    ):
        return TurnToolScopeDecision()
    active_connector_providers = _active_connector_provider_context()
    cache_key = _tool_scope_cache_key(
        user_message=user_message,
        evidence=evidence,
        registry=registry,
        model_client=model_client,
        active_connector_providers=active_connector_providers,
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
        "available_special_tool_scopes": [
            "web_or_browser",
            "scheduler",
            "skill_pack_reference",
            "connector_gateway",
        ],
        "user_turn": user_message,
    }
    system = (
        "Return only a JSON object matching this schema: "
        '{"web_action":"none|open_url|live_research|browser_interaction",'
        '"scheduler_action":"none|inspect|run|mutate",'
        '"skill_pack_action":"none|reference|connector","connector_app_ids":["active-app-id"],"confidence":0.0}. '
        "Use web_action=open_url for explicit URL/domain targets, live_research for requests that need current public information, "
        "and browser_interaction for a user-visible webpage workflow. "
        "Use web_action=none when the request can be answered without web/browser tools. "
        "Use scheduler actions only for scheduled-task or reminder control. "
        "Do not choose scheduler just because a saved task could answer the domain. "
        "Use connector only when the request needs a connected external API/account and active_connector_providers lists an active_app_ids value that can satisfy it. "
        "When using connector, include exact app IDs from active_app_ids in connector_app_ids. "
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
        logger.debug("Turn tool-scope decision failed; hiding special tools", exc_info=True)
        return TurnToolScopeDecision()
    decision = _validated_turn_tool_scope_decision(
        _parse_turn_tool_scope_decision(_extract_response_text(response)),
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


def scoped_turn_tool_registry(
    registry,
    *,
    evidence: TurnToolEvidence,
    model_client: object | None = None,
    user_message: str | None = None,
):
    decision = build_turn_tool_scope_decision(
        model_client=model_client,
        user_message=user_message or "",
        evidence=evidence,
        registry=registry,
    )
    if evidence.context_linked and not _registry_has_scoped_special_tools(registry):
        return registry
    return ScopedTurnToolRegistry(registry, evidence=evidence, tool_scope_decision=decision)


def turn_tool_evidence_needs_model_scope_decision(evidence: TurnToolEvidence) -> bool:
    return bool(
        getattr(evidence, "has_url_target", False)
        or getattr(evidence, "has_attachments", False)
        or getattr(evidence, "artifact_requested", False)
        or getattr(evidence, "prior_tool_scopes", ())
    )


def turn_tool_scope_decision_may_apply(evidence: TurnToolEvidence, *, user_message: str = "") -> bool:
    return (
        turn_tool_evidence_needs_model_scope_decision(evidence)
        or _message_references_scheduler_job(user_message)
        or _runtime_has_active_connector_provider()
    )


def tool_registry_allows_skill_pack_context(registry) -> bool:
    try:
        registry.get_spec("skill_pack_read")
        return True
    except KeyError:
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
    "scoped_turn_tool_registry",
    "should_include_prior_turn_messages",
    "tool_registry_allows_connector_context",
    "tool_registry_allows_skill_pack_context",
    "turn_tool_scope_decision_may_apply",
    "turn_tool_evidence_needs_model_scope_decision",
    "turn_is_context_linked",
]

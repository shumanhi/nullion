"""Scoped tool registry view for scheduled task execution."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
import logging
import time
from typing import Iterable

from nullion.tools import ToolInvocation, ToolResult

logger = logging.getLogger(__name__)

CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS = frozenset({"scheduler"})
CRON_EXECUTION_BLOCKED_TOOLS = frozenset(
    {
        "create_cron",
        "delete_cron",
        "delete_reminder",
        "list_crons",
        "list_reminders",
        "run_cron",
        "set_reminder",
        "toggle_cron",
        "update_reminder",
    }
)
CRON_EXECUTION_CONNECTOR_CAPABILITY_TAGS = frozenset({"connector"})
CRON_EXECUTION_CONNECTOR_TOOLS = frozenset({"connector_request"})
CRON_EXECUTION_STRUCTURED_READ_SIDE_EFFECTS = frozenset({"read"})
CRON_EXECUTION_STRUCTURED_WRITE_SIDE_EFFECTS = frozenset({"account_write"})
CRON_CONNECTOR_SCOPE_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class CronConnectorScopeDecision:
    """Connector tool access proven for a scheduled task run."""

    allow_connector_tools: bool = False
    provider_ids: tuple[str, ...] = ()
    confidence: float = 0.0
    valid: bool = False
    write_tool_names: tuple[str, ...] = ()
    delivery_mode: str = ""


class CronExecutionToolRegistry:
    """Read-through registry view for an already-running scheduled task."""

    scheduled_task_execution_registry = True

    def __init__(
        self,
        delegate,
        *,
        allowed_tool_names: Iterable[str] | None = None,
        allowed_structured_tool_names: Iterable[str] | None = None,
        allow_connector_tools: bool = False,
        connector_provider_ids: Iterable[str] | None = None,
        checkpoint_journal=None,
        browser_session_id: str | None = None,
    ) -> None:
        self._delegate = delegate
        self._checkpoint_journal = checkpoint_journal
        self._browser_session_id = browser_session_id
        self._allowed_tool_names = (
            frozenset(str(name).strip() for name in allowed_tool_names if str(name).strip())
            if allowed_tool_names is not None
            else None
        )
        self._allowed_structured_tool_names = frozenset(
            str(name).strip()
            for name in (allowed_structured_tool_names or ())
            if str(name).strip()
        )
        self._allow_connector_tools = bool(allow_connector_tools)
        self._connector_provider_ids = frozenset(
            str(provider_id).strip()
            for provider_id in (connector_provider_ids or ())
            if str(provider_id).strip()
        )

    @staticmethod
    def _spec_tags(spec: object) -> frozenset[str]:
        return frozenset(
            str(tag).strip().lower()
            for tag in (getattr(spec, "capability_tags", ()) or ())
            if str(tag).strip()
        )

    @staticmethod
    def _connector_provider_id_from_invocation(invocation: ToolInvocation) -> str:
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        return provider_id

    def _is_allowed_by_structured_plan(self, spec_name: str) -> bool:
        if self._allowed_tool_names is None:
            return True
        return spec_name in self._allowed_tool_names

    def _is_blocked_spec(self, spec: object) -> bool:
        spec_name = str(getattr(spec, "name", "") or "").strip()
        if spec_name in CRON_EXECUTION_BLOCKED_TOOLS:
            return True
        return bool(
            self._spec_tags(spec).intersection(CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS)
        )

    def _is_connector_spec(self, spec: object) -> bool:
        spec_name = str(getattr(spec, "name", "") or "").strip()
        if spec_name in CRON_EXECUTION_CONNECTOR_TOOLS:
            return True
        return bool(
            self._spec_tags(spec).intersection(CRON_EXECUTION_CONNECTOR_CAPABILITY_TAGS)
        )

    @staticmethod
    def _side_effect_value(spec: object) -> str:
        side_effect = getattr(spec, "side_effect_class", "")
        return str(getattr(side_effect, "value", side_effect)).strip().lower()

    def _is_connector_gateway_spec(self, spec: object) -> bool:
        spec_name = str(getattr(spec, "name", "") or "").strip()
        if spec_name in CRON_EXECUTION_CONNECTOR_TOOLS:
            return True
        tags = self._spec_tags(spec)
        return bool(tags) and tags.issubset(CRON_EXECUTION_CONNECTOR_CAPABILITY_TAGS)

    def _is_structured_passthrough_spec(self, spec: object) -> bool:
        if (
            self._is_blocked_spec(spec)
            or not self._is_connector_spec(spec)
            or self._is_connector_gateway_spec(spec)
        ):
            return False
        return self._side_effect_value(spec) in CRON_EXECUTION_STRUCTURED_READ_SIDE_EFFECTS

    def _is_structured_connector_write_spec(self, spec: object) -> bool:
        return (
            self._is_connector_spec(spec)
            and not self._is_connector_gateway_spec(spec)
            and self._side_effect_value(spec) in CRON_EXECUTION_STRUCTURED_WRITE_SIDE_EFFECTS
        )

    def _is_connector_spec_available(self, spec: object) -> bool:
        if not self._is_connector_spec(spec):
            return True
        spec_name = str(getattr(spec, "name", "") or "").strip()
        if spec_name and spec_name in self._allowed_structured_tool_names:
            return True
        if self._is_structured_connector_write_spec(spec):
            return False
        return self._allow_connector_tools

    @staticmethod
    def _requires_approval(spec: object) -> bool:
        return bool(getattr(spec, "requires_approval", False))

    def _is_invokable_spec(self, spec: object) -> bool:
        spec_name = str(getattr(spec, "name", "") or "").strip()
        return (
            self._is_allowed_by_structured_plan(spec_name)
            and not self._is_blocked_spec(spec)
            and self._is_connector_spec_available(spec)
        )

    def _is_listed_spec(self, spec: object) -> bool:
        # Keep approval-gated tools visible to planner/decomposer surfaces so
        # structured plans can request them and trigger the normal approval flow.
        return self._is_invokable_spec(spec)

    def get_spec(self, name: str):
        spec = self._delegate.get_spec(name)
        if not self._is_invokable_spec(spec):
            raise KeyError(f"Unknown tool: {name}")
        return spec

    def list_specs(self) -> list[object]:
        return [
            spec
            for spec in self._delegate.list_specs()
            if self._is_listed_spec(spec)
        ]

    def list_tool_definitions(self, *args, **kwargs) -> list[dict[str, object]]:
        definitions = self._delegate.list_tool_definitions(*args, **kwargs)
        allowed_names = {str(getattr(spec, "name", "") or "") for spec in self.list_specs()}
        return [
            definition
            for definition in definitions
            if str(definition.get("name") or "") in allowed_names
        ]

    def filesystem_allowed_roots(self):
        return self._delegate.filesystem_allowed_roots()

    def list_installed_plugins(self) -> list[str]:
        return self._delegate.list_installed_plugins()

    def is_plugin_installed(self, plugin_name: str) -> bool:
        return self._delegate.is_plugin_installed(plugin_name)

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
        if self._browser_session_id:
            invocation = replace(invocation,flow_context={**(invocation.flow_context or {}),
                "browser_session_id":self._browser_session_id,"browser_session_scope":self._browser_session_id})
        try:
            spec = self._delegate.get_spec(invocation.tool_name)
        except KeyError:
            spec = None
        if spec is not None and self._is_blocked_spec(spec):
            blocked_tags = sorted(
                self._spec_tags(spec).intersection(CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS)
            )
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="denied",
                output={
                    "reason": "cron_execution_capability_denied",
                    "denied_tools": sorted(CRON_EXECUTION_BLOCKED_TOOLS),
                    "denied_capability_tags": blocked_tags,
                    "tool_capability_tags": sorted(self._spec_tags(spec)),
                    "suppress_activity": True,
                },
                error=f"Capability denied during scheduled task execution: {invocation.tool_name}",
            )
        if spec is not None and not self._is_allowed_by_structured_plan(invocation.tool_name):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="denied",
                output={
                    "reason": "cron_execution_tool_not_in_structured_plan",
                    "allowed_tools": sorted(self._allowed_tool_names or ()),
                    "suppress_activity": True,
                },
                error=f"Tool not granted by scheduled task plan: {invocation.tool_name}",
            )
        if spec is not None and self._is_connector_spec(spec):
            if invocation.tool_name in self._allowed_structured_tool_names:
                return self._delegate.invoke(invocation)
            if self._is_structured_connector_write_spec(spec):
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="denied",
                    output={
                        "reason": "cron_execution_account_write_denied",
                        "suppress_activity": True,
                    },
                    error=(
                        "Account-write connector tools are not available for scheduled task delivery "
                        "without an explicit structured write grant."
                    ),
                )
            provider_id = self._connector_provider_id_from_invocation(invocation)
            if not self._allow_connector_tools:
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="denied",
                    output={
                        "reason": "cron_execution_connector_scope_denied",
                        "suppress_activity": True,
                    },
                    error=(
                        "Connector tools are not available for this scheduled task run "
                        "without structured connector scope evidence."
                    ),
                )
            if self._connector_provider_ids and provider_id not in self._connector_provider_ids:
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="denied",
                    output={
                        "reason": "cron_execution_connector_provider_denied",
                        "allowed_provider_ids": sorted(self._connector_provider_ids),
                        "provider_id": provider_id,
                        "suppress_activity": True,
                    },
                    error=f"Connector provider not granted for this scheduled task run: {provider_id}",
                )
        path = self._checkpoint_journal.before(invocation) if self._checkpoint_journal else None
        try:
            return self._delegate.invoke(invocation)
        finally:
            if self._checkpoint_journal:
                self._checkpoint_journal.after(path, None)

    def register_cleanup_hook(self, hook) -> None:
        self._delegate.register_cleanup_hook(hook)

    def run_cleanup_hooks(self, *, scope_id: str | None = None) -> None:
        self._delegate.run_cleanup_hooks(scope_id=scope_id)

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


def _registry_has_connector_tools(registry) -> bool:
    try:
        specs = registry.list_specs()
    except Exception:
        return False
    for spec in specs:
        name = str(getattr(spec, "name", "") or "").strip()
        tags = CronExecutionToolRegistry._spec_tags(spec)
        if (
            name in CRON_EXECUTION_CONNECTOR_TOOLS
            or tags.intersection(CRON_EXECUTION_CONNECTOR_CAPABILITY_TAGS)
        ):
            return True
    return False


def structured_cron_passthrough_tool_names(registry) -> tuple[str, ...]:
    """Return registered connector tools safe to expose from structured metadata."""

    try:
        specs = registry.list_specs()
    except Exception:
        return ()
    classifier = CronExecutionToolRegistry(registry)
    names: list[str] = []
    for spec in specs:
        name = str(getattr(spec, "name", "") or "").strip()
        if not name:
            continue
        if not classifier._is_structured_passthrough_spec(spec):
            continue
        names.append(name)
    return tuple(dict.fromkeys(names))


def _connector_provider_id_looks_external(provider_id: object) -> bool:
    normalized = str(provider_id or "").strip().lower()
    return normalized.startswith("skill_pack_connector_") or normalized.endswith("_connector_provider")


def _connected_connector_provider_summaries(principal_id: str | None) -> tuple[dict[str, object], ...]:
    try:
        from nullion.connections import connection_for_principal, load_connection_registry
    except Exception:
        return ()
    active_context_by_provider: dict[str, dict[str, object]] = {}
    try:
        from nullion.turn_context_policy import _active_connector_provider_context

        for provider in _active_connector_provider_context():
            if not isinstance(provider, dict):
                continue
            provider_id = str(provider.get("provider_id") or "").strip()
            if provider_id:
                active_context_by_provider[provider_id] = provider
    except Exception:
        active_context_by_provider = {}
    summaries: list[dict[str, object]] = []
    seen: set[str] = set()
    try:
        connections = load_connection_registry().connections
    except Exception:
        return ()
    for connection in connections:
        provider_id = str(getattr(connection, "provider_id", "") or "").strip()
        if (
            not provider_id
            or provider_id in seen
            or not getattr(connection, "active", True)
            or not _connector_provider_id_looks_external(provider_id)
        ):
            continue
        try:
            scoped = connection_for_principal(principal_id, provider_id)
        except Exception:
            scoped = None
        if scoped is None:
            continue
        seen.add(provider_id)
        summary: dict[str, object] = {
            "provider_id": provider_id,
            "display_name": str(getattr(connection, "display_name", "") or provider_id),
            "permission_mode": str(getattr(connection, "permission_mode", "") or "read"),
        }
        active_context = active_context_by_provider.get(provider_id) or {}
        # Cron scope classification needs the same structured connector facts as
        # ordinary chat. Keep the payload compact: app ids and tool names are
        # enough to map Gmail/calendar-style tasks without loading skill docs.
        active_app_ids = active_context.get("active_app_ids")
        if isinstance(active_app_ids, list):
            summary["active_app_ids"] = [
                str(app_id).strip()
                for app_id in active_app_ids
                if str(app_id).strip()
            ][:160]
        structured_tools = active_context.get("structured_tools")
        if isinstance(structured_tools, list):
            summary["structured_tools"] = [
                str(tool_name).strip()
                for tool_name in structured_tools
                if str(tool_name).strip()
            ][:20]
        skill_pack_id = str(active_context.get("skill_pack_id") or "").strip()
        if skill_pack_id:
            summary["skill_pack_id"] = skill_pack_id
        profile = str(getattr(connection, "provider_profile", "") or "").strip()
        if profile:
            summary["provider_profile"] = profile
        summaries.append(summary)
    return tuple(summaries)


def _has_structured_connector_scope_evidence(
    *,
    registry,
    planned_tool_names: Iterable[str] | None,
) -> bool:
    planned_tools = {
        str(tool_name or "").strip()
        for tool_name in (planned_tool_names or ())
        if str(tool_name or "").strip()
    }
    if not planned_tools:
        return False
    if planned_tools.intersection(CRON_EXECUTION_CONNECTOR_TOOLS):
        return True
    for tool_name in planned_tools:
        try:
            spec = registry.get_spec(tool_name)
        except Exception:
            continue
        if CronExecutionToolRegistry._spec_tags(spec).intersection(
            CRON_EXECUTION_CONNECTOR_CAPABILITY_TAGS
        ):
            return True
    return False


def _strings_from_structured_value(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        normalized = value.strip()
        return (normalized,) if normalized else ()
    if isinstance(value, list):
        values: list[str] = []
        for item in value:
            values.extend(_strings_from_structured_value(item))
        return tuple(dict.fromkeys(values))
    return ()


def _extract_structured_connector_scope_ids(text: str) -> tuple[set[str], set[str]]:
    try:
        payload = json.loads(text)
    except Exception:
        return set(), set()
    if not isinstance(payload, dict):
        return set(), set()
    provider_keys = {
        "connector_provider_id",
        "connector_provider_ids",
        "provider_id",
        "provider_ids",
        "selected_provider_id",
        "selected_provider_ids",
    }
    app_keys = {
        "active_app_id",
        "active_app_ids",
        "app_id",
        "app_ids",
        "selected_app_id",
        "selected_app_ids",
    }
    provider_ids: set[str] = set()
    app_ids: set[str] = set()
    for key, value in payload.items():
        normalized_key = str(key or "").strip()
        if normalized_key in provider_keys:
            provider_ids.update(_strings_from_structured_value(value))
        elif normalized_key in app_keys:
            app_ids.update(_strings_from_structured_value(value))
    return provider_ids, app_ids


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


def _parse_cron_connector_scope_decision(
    text: str,
    *,
    connected_provider_ids: set[str],
) -> CronConnectorScopeDecision:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return CronConnectorScopeDecision()
    try:
        payload = json.loads(text[start : end + 1])
    except Exception:
        return CronConnectorScopeDecision()
    if not isinstance(payload, dict):
        return CronConnectorScopeDecision()
    requires_connector = payload.get("requires_account_connector") is True
    try:
        confidence = float(payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    provider_ids = tuple(
        dict.fromkeys(
            provider_id
            for provider_id in (
                str(item or "").strip()
                for item in (
                    payload.get("provider_ids")
                    if isinstance(payload.get("provider_ids"), list)
                    else ()
                )
            )
            if provider_id in connected_provider_ids
        )
    )
    allow = bool(requires_connector and provider_ids and confidence >= 0.65)
    return CronConnectorScopeDecision(
        allow_connector_tools=allow,
        provider_ids=provider_ids if allow else (),
        confidence=max(0.0, min(1.0, confidence)),
        valid=True,
    )


def build_cron_connector_scope_decision(
    *,
    model_client: object | None,
    user_message: str,
    principal_id: str,
    registry,
    planned_tool_names: Iterable[str] | None = None,
    resolve_execution_contract: bool = False,
    checkpoint_path: str | None = None,
) -> CronConnectorScopeDecision:
    """Return connector scope from structured model output plus runtime connections."""
    if model_client is None or not _registry_has_connector_tools(registry):
        return CronConnectorScopeDecision()
    started = time.perf_counter()
    provider_summaries = _connected_connector_provider_summaries(principal_id)
    if not provider_summaries:
        return CronConnectorScopeDecision()
    connected_provider_ids = {
        str(provider.get("provider_id") or "").strip()
        for provider in provider_summaries
        if str(provider.get("provider_id") or "").strip()
    }
    structured_provider_ids, structured_app_ids = _extract_structured_connector_scope_ids(user_message)
    explicit_provider_ids = tuple(
        provider_id for provider_id in sorted(connected_provider_ids) if provider_id in structured_provider_ids
    )
    if explicit_provider_ids and not resolve_execution_contract:
        logger.info(
            "cron connector scope allowed principal_id=%s providers=%d reason=stored_provider_id",
            principal_id,
            len(explicit_provider_ids),
        )
        return CronConnectorScopeDecision(
            allow_connector_tools=True,
            provider_ids=explicit_provider_ids,
            confidence=1.0,
            valid=True,
        )
    explicit_app_provider_ids: list[str] = []
    for provider in provider_summaries:
        provider_id = str(provider.get("provider_id") or "").strip()
        if not provider_id:
            continue
        active_app_ids = provider.get("active_app_ids")
        for app_id in active_app_ids if isinstance(active_app_ids, list) else ():
            app_id_text = str(app_id or "").strip()
            if app_id_text and app_id_text in structured_app_ids:
                explicit_app_provider_ids.append(provider_id)
                break
    if explicit_app_provider_ids and not resolve_execution_contract:
        selected = tuple(dict.fromkeys(explicit_app_provider_ids))
        logger.info(
            "cron connector scope allowed principal_id=%s providers=%d reason=stored_active_app_id",
            principal_id,
            len(selected),
        )
        return CronConnectorScopeDecision(
            allow_connector_tools=True,
            provider_ids=selected,
            confidence=1.0,
            valid=True,
        )
    planned_tool_names_tuple = tuple(planned_tool_names or ())
    if planned_tool_names_tuple and not _has_structured_connector_scope_evidence(
        registry=registry,
        planned_tool_names=planned_tool_names_tuple,
    ):
        # Planner previews are display hints and can be produced before
        # connector scope is granted. Continue to the structured classifier so a
        # browser/search-shaped preview cannot hide the account connector a cron
        # actually needs.
        logger.info(
            "cron connector scope continuing principal_id=%s providers=%d reason=no_structured_connector_evidence",
            principal_id,
            len(provider_summaries),
        )
    classifier = CronExecutionToolRegistry(registry)
    write_specs = [spec for spec in registry.list_specs()
                   if classifier._is_structured_connector_write_spec(spec) and not classifier._is_blocked_spec(spec)]
    write_catalog = [{"name": spec.name, "description": spec.description,
                      "requires_approval": bool(spec.requires_approval)} for spec in write_specs]
    cache_key = {"principal_id": principal_id, "task": user_message,
                 "providers": provider_summaries, "writes": write_catalog,
                 "planned_tools": planned_tool_names_tuple, "runtime_path": str(checkpoint_path or "")}
    if resolve_execution_contract:
        from nullion.runtime_cache import get_json, set_json
        cached = get_json("cron.execution_contract", cache_key, version="v1",
                          persistent=checkpoint_path is not None, db_path=checkpoint_path)
        if cached.hit and isinstance(cached.value, dict):
            value = cached.value
            available_writes = {spec.name for spec in write_specs}
            if (isinstance(value.get("allow_connector_tools"), bool)
                    and isinstance(value.get("provider_ids"), list)
                    and all(name in connected_provider_ids for name in value["provider_ids"])
                    and isinstance(value.get("write_tool_names"), list)
                    and all(name in available_writes for name in value["write_tool_names"])
                    and isinstance(value.get("confidence"), (int, float))
                    and (not value["allow_connector_tools"] or value["confidence"] >= 0.65)
                    and (not value["write_tool_names"] or (value["allow_connector_tools"] and any(
                        provider.get("provider_id") in value["provider_ids"] and provider.get("permission_mode") == "write"
                        for provider in provider_summaries)))
                    and value.get("delivery_mode") in {"report_each_run", "verified_matches_only"}):
                return CronConnectorScopeDecision(
                    allow_connector_tools=value["allow_connector_tools"],
                    provider_ids=tuple(value["provider_ids"]), confidence=value["confidence"], valid=True,
                    write_tool_names=tuple(value["write_tool_names"]), delivery_mode=value["delivery_mode"])
    prompt = {
        "surface": "scheduled_task_execution",
        "available_connector_providers": list(provider_summaries),
        "scheduled_task": user_message,
        "available_structured_write_tools": write_catalog if resolve_execution_contract else [],
    }
    system = (
        "Return only a JSON object matching this schema: "
        '{"requires_account_connector":true|false,"provider_ids":["provider_id"],"confidence":0.0}. '
        "Set requires_account_connector=true only when this scheduled task must use a connected "
        "workspace/account/API connector provider from available_connector_providers. "
        "A task can map to a provider through provider_id, display_name, skill_pack_id, "
        "structured_tools, or active_app_ids. "
        "Do not use connector gateways as a public web, browser, search, scraping, or generic HTTP fallback; "
        "choose false for tasks that can use public web/browser/file/report tools. "
        "Choose false when the task does not clearly map to one of the available connector provider ids. "
        "When uncertain, choose false."
    )
    if resolve_execution_contract:
        system += (
            ' Also include "write_tool_names":[], and "delivery_mode":"report_each_run"|"verified_matches_only". '
            'Select only exact available structured write tool names needed to carry out actions explicitly '
            'requested by the saved task, including conditional actions. This selects capabilities only; '
            'normal permission and approval checks still apply. Do not select account writes merely to '
            'send the scheduled report; the scheduler delivers it. Choose verified_matches_only for '
            'conditional monitors/reminders that should notify only for new actionable matches; choose '
            'report_each_run for reports requested on every run. Determine these from the full saved task '
            'in any language, never from its title alone. Never invent tool names or provider ids.'
        )
    try:
        response = model_client.create(
            messages=[
                {
                    "role": "user",
                    "content": [{"type": "text", "text": json.dumps(prompt, ensure_ascii=False)}],
                }
            ],
            tools=[],
            max_tokens=600 if resolve_execution_contract else 180,
            system=system,
            timeout=CRON_CONNECTOR_SCOPE_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.debug("Cron connector-scope decision failed; hiding connector tools", exc_info=True)
        return CronConnectorScopeDecision()
    completion = response.get("completion", {}) if isinstance(response, dict) else {}
    if isinstance(completion, dict) and completion.get("status") in {"incomplete", "failed", "cancelled"}:
        return CronConnectorScopeDecision()
    elapsed_ms = (time.perf_counter() - started) * 1000
    decision = _parse_cron_connector_scope_decision(
        _extract_response_text(response),
        connected_provider_ids=connected_provider_ids,
    )
    if not decision.valid:
        logger.info(
            "cron connector scope skipped principal_id=%s elapsed_ms=%.1f providers=%d reason=invalid_model_response",
            principal_id,
            elapsed_ms,
            len(provider_summaries),
        )
        return CronConnectorScopeDecision()
    if resolve_execution_contract:
        try:
            payload = json.loads(_extract_response_text(response))
            selected = payload.get("write_tool_names", [])
            mode = payload.get("delivery_mode", "")
            if (not isinstance(selected, list) or any(not isinstance(name, str) for name in selected)
                    or mode not in {"", "report_each_run", "verified_matches_only"}):
                return CronConnectorScopeDecision()
            # The registry and connection permissions validate model scope. The
            # normal delegate remains the only authority to execute an action.
            writable = any(provider.get("provider_id") in decision.provider_ids and provider.get("permission_mode") == "write"
                           for provider in provider_summaries)
            available_writes = {spec.name for spec in write_specs}
            writes = tuple(dict.fromkeys(name for name in selected if name in available_writes))
            if selected and (not writable or not decision.allow_connector_tools
                             or len(writes) != len(set(selected))):
                return CronConnectorScopeDecision()
            decision = replace(decision, write_tool_names=writes, delivery_mode=mode)
            if mode:  # Do not cache incomplete legacy responses or transient failures.
                from dataclasses import asdict
                set_json("cron.execution_contract", cache_key, asdict(decision), version="v1",
                         persistent=checkpoint_path is not None, db_path=checkpoint_path, max_entries=256)
        except (TypeError, ValueError, KeyError):
            return CronConnectorScopeDecision()
    logger.info(
        "cron connector scope decided principal_id=%s elapsed_ms=%.1f allow=%s providers=%d selected=%d confidence=%.2f",
        principal_id,
        elapsed_ms,
        decision.allow_connector_tools,
        len(provider_summaries),
        len(decision.provider_ids),
        decision.confidence,
    )
    return decision


__all__ = [
    "CRON_EXECUTION_BLOCKED_CAPABILITY_TAGS",
    "CRON_EXECUTION_BLOCKED_TOOLS",
    "CRON_EXECUTION_CONNECTOR_CAPABILITY_TAGS",
    "CRON_EXECUTION_CONNECTOR_TOOLS",
    "CRON_EXECUTION_STRUCTURED_READ_SIDE_EFFECTS",
    "CRON_EXECUTION_STRUCTURED_WRITE_SIDE_EFFECTS",
    "CronConnectorScopeDecision",
    "CronExecutionToolRegistry",
    "build_cron_connector_scope_decision",
    "structured_cron_passthrough_tool_names",
]

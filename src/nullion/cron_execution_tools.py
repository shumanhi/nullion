"""Scoped tool registry view for scheduled task execution."""

from __future__ import annotations

from dataclasses import dataclass
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
        "list_crons",
        "list_reminders",
        "run_cron",
        "set_reminder",
        "toggle_cron",
    }
)
CRON_EXECUTION_CONNECTOR_CAPABILITY_TAGS = frozenset({"connector"})
CRON_EXECUTION_CONNECTOR_TOOLS = frozenset({"connector_request"})
CRON_CONNECTOR_SCOPE_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class CronConnectorScopeDecision:
    """Connector tool access proven for a scheduled task run."""

    allow_connector_tools: bool = False
    provider_ids: tuple[str, ...] = ()
    confidence: float = 0.0
    valid: bool = False


class CronExecutionToolRegistry:
    """Read-through registry view for an already-running scheduled task."""

    def __init__(
        self,
        delegate,
        *,
        allowed_tool_names: Iterable[str] | None = None,
        allow_connector_tools: bool = False,
        connector_provider_ids: Iterable[str] | None = None,
    ) -> None:
        self._delegate = delegate
        self._allowed_tool_names = (
            frozenset(str(name).strip() for name in allowed_tool_names if str(name).strip())
            if allowed_tool_names is not None
            else None
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
        return str(invocation.arguments.get("provider_id") or "").strip()

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

    def _is_connector_spec_available(self, spec: object) -> bool:
        if not self._is_connector_spec(spec):
            return True
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
        return self._delegate.invoke(invocation)

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


def _connector_provider_id_looks_external(provider_id: object) -> bool:
    normalized = str(provider_id or "").strip().lower()
    return normalized.startswith("skill_pack_connector_") or normalized.endswith("_connector_provider")


def _connected_connector_provider_summaries(principal_id: str | None) -> tuple[dict[str, object], ...]:
    try:
        from nullion.connections import connection_for_principal, load_connection_registry
    except Exception:
        return ()
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
        profile = str(getattr(connection, "provider_profile", "") or "").strip()
        if profile:
            summary["provider_profile"] = profile
        summaries.append(summary)
    return tuple(summaries)


def _has_structured_connector_scope_evidence(
    *,
    planned_tool_names: Iterable[str] | None,
) -> bool:
    planned_tools = {
        str(tool_name or "").strip()
        for tool_name in (planned_tool_names or ())
        if str(tool_name or "").strip()
    }
    return bool(planned_tools.intersection(CRON_EXECUTION_CONNECTOR_TOOLS))


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
    requires_connector = bool(payload.get("requires_account_connector"))
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
) -> CronConnectorScopeDecision:
    """Return connector scope from structured model output plus runtime connections."""
    if model_client is None or not _registry_has_connector_tools(registry):
        return CronConnectorScopeDecision()
    started = time.perf_counter()
    provider_summaries = _connected_connector_provider_summaries(principal_id)
    if not provider_summaries:
        return CronConnectorScopeDecision()
    planned_tool_names_tuple = tuple(planned_tool_names or ())
    if planned_tool_names_tuple and not _has_structured_connector_scope_evidence(
        planned_tool_names=planned_tool_names_tuple,
    ):
        logger.info(
            "cron connector scope skipped principal_id=%s providers=%d reason=no_structured_connector_evidence",
            principal_id,
            len(provider_summaries),
        )
        return CronConnectorScopeDecision()
    connected_provider_ids = {
        str(provider.get("provider_id") or "").strip()
        for provider in provider_summaries
        if str(provider.get("provider_id") or "").strip()
    }
    prompt = {
        "surface": "scheduled_task_execution",
        "available_connector_providers": list(provider_summaries),
        "scheduled_task": user_message,
    }
    system = (
        "Return only a JSON object matching this schema: "
        '{"requires_account_connector":true|false,"provider_ids":["provider_id"],"confidence":0.0}. '
        "Set requires_account_connector=true only when this scheduled task must use a connected "
        "workspace/account/API connector provider from available_connector_providers. "
        "Do not use connector gateways as a public web, browser, search, scraping, or generic HTTP fallback; "
        "choose false for tasks that can use public web/browser/file/report tools. "
        "Choose false when the task does not clearly map to one of the available connector provider ids. "
        "When uncertain, choose false."
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
            max_tokens=180,
            system=system,
            timeout=CRON_CONNECTOR_SCOPE_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.debug("Cron connector-scope decision failed; hiding connector tools", exc_info=True)
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
    "CronConnectorScopeDecision",
    "CronExecutionToolRegistry",
    "build_cron_connector_scope_decision",
]

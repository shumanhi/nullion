"""Structured approval context helpers.

Approval prompts are surfaced through web, Telegram, Slack, Discord, and
operator commands.  Keep provenance in the approval context so every renderer
can show the same "what triggered this" fact without parsing user text.
"""

from __future__ import annotations

from collections.abc import Mapping


FLOW_TRIGGER_CONTEXT_KEY = "trigger_flow"

_KNOWN_CHANNEL_LABELS = {
    "web": "Web chat",
    "telegram": "Telegram chat",
    "slack": "Slack chat",
    "discord": "Discord chat",
    "cron": "Scheduled task",
}


def _string_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _channel_from_principal(principal_id: str | None) -> tuple[str | None, str | None]:
    principal = _string_value(principal_id)
    if principal is None:
        return None, None
    channel, separator, target = principal.partition(":")
    if separator:
        channel = channel.strip().lower()
        target = target.strip() or None
        if channel:
            return channel, target
    return None, None


def build_trigger_flow_context(
    *,
    principal_id: str | None,
    invocation_id: str | None = None,
    capsule_id: str | None = None,
    flow_kind: str = "tool_invocation",
) -> dict[str, object]:
    """Return a provider-neutral provenance block for an approval request."""

    channel, target = _channel_from_principal(principal_id)
    source = channel or "runtime"
    label = _KNOWN_CHANNEL_LABELS.get(source)
    if label is None:
        label = f"{source.replace('_', ' ').title()} flow" if source != "runtime" else "Runtime flow"

    context: dict[str, object] = {
        "kind": flow_kind,
        "source": source,
        "label": label,
    }
    principal = _string_value(principal_id)
    if principal:
        context["principal_id"] = principal
    if target:
        context["target_id"] = target
    invocation = _string_value(invocation_id)
    if invocation:
        context["invocation_id"] = invocation
    capsule = _string_value(capsule_id)
    if capsule:
        context["capsule_id"] = capsule
    return context


def approval_trigger_flow(approval_or_context: object) -> dict[str, object] | None:
    """Extract a normalized trigger-flow block from an approval or context dict."""

    context: object
    if isinstance(approval_or_context, Mapping):
        context = approval_or_context
    else:
        context = getattr(approval_or_context, "context", None)
    if not isinstance(context, Mapping):
        return None
    raw = context.get(FLOW_TRIGGER_CONTEXT_KEY)
    if not isinstance(raw, Mapping):
        return None
    normalized: dict[str, object] = {}
    for key in ("kind", "source", "label", "principal_id", "target_id", "invocation_id", "capsule_id"):
        value = _string_value(raw.get(key))
        if value is not None:
            normalized[key] = value
    if not normalized:
        return None
    normalized.setdefault("label", "Runtime flow")
    return normalized


def approval_trigger_flow_label(approval_or_context: object) -> str | None:
    flow = approval_trigger_flow(approval_or_context)
    if flow is None:
        return None
    label = _string_value(flow.get("label")) or "Runtime flow"
    details = []
    principal = _string_value(flow.get("principal_id"))
    if principal:
        details.append(principal)
    capsule = _string_value(flow.get("capsule_id"))
    if capsule:
        details.append(f"capsule {capsule[:8]}")
    invocation = _string_value(flow.get("invocation_id"))
    if invocation:
        details.append(f"call {invocation[:12]}")
    return f"{label} ({' · '.join(details)})" if details else label


__all__ = [
    "FLOW_TRIGGER_CONTEXT_KEY",
    "approval_trigger_flow",
    "approval_trigger_flow_label",
    "build_trigger_flow_context",
]

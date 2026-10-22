"""Request-bound recovery authority, independent of tool discovery.

Only an existing structured plan can establish this contract. Discovery and
tool results may supply evidence and read continuations, never new write
authority. Shell execution remains governed by ToolExecutor, not by guesses
about a command's effects.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from nullion.tools import ToolResult, ToolSideEffectClass


@dataclass(frozen=True, slots=True)
class RequestRecoveryContract:
    decision: object
    action_tools: frozenset[str]
    source_tags: frozenset[str] = frozenset()

    @classmethod
    def from_registry(cls, registry):
        decision = getattr(registry, "turn_tool_scope_decision", None)
        if not getattr(decision, "valid", False):
            return None
        if not callable(getattr(registry, "get_spec", None)) or not callable(getattr(registry, "list_specs", None)):
            return None
        names = set(decision.requested_tool_names) | set(decision.required_tool_names)
        # A structured capability or format can select actions without naming
        # them individually. Snapshot only tools callable in that original
        # scoped view, never the unscoped runtime inventory.
        names.update(spec.name for spec in registry.list_specs())
        actions = set()
        source_tags = set()
        for name in names:
            try:
                spec = registry.get_spec(name)
            except KeyError:
                continue
            if spec.side_effect_class != ToolSideEffectClass.READ:
                actions.add(name)
            if name in decision.requested_tool_names and "connector" in spec.capability_tags:
                source_tags.update(set(spec.capability_tags) - {"connector", "account_read", "account_write"})
        return cls(decision, frozenset(actions), frozenset(source_tags))

    def bind(self, registry):
        if isinstance(registry, RequestRecoveryRegistry) and registry.contract is self:
            return registry
        return RequestRecoveryRegistry(registry, self)


class RequestRecoveryRegistry:
    """Keep delegate permissions while bounding new recovery actions."""

    def __init__(self, delegate, contract):
        self._delegate = delegate
        self.contract = contract
        current = getattr(delegate, "turn_tool_scope_decision", None)
        original = contract.decision
        self.turn_tool_scope_decision = replace(current,
            requested_outcome=original.requested_outcome,
            scheduler_effect_actions=original.allowed_scheduler_actions,
            connector_source_user_requested=original.connector_source_user_requested,
            connector_source_evidence=original.connector_source_evidence,
            connector_app_ids=original.connector_app_ids,
            source_artifact_delivery_requested=original.source_artifact_delivery_requested,
            source_artifact_request_evidence=original.source_artifact_request_evidence,
            source_artifact_retrieval_only=original.source_artifact_retrieval_only,
            excluded_artifact_extensions=original.excluded_artifact_extensions,
            requested_artifact_extensions=original.requested_artifact_extensions,
            required_embedded_media_extensions=original.required_embedded_media_extensions,
            required_tool_names=tuple(dict.fromkeys((*original.required_tool_names,
                *(name for name in current.required_tool_names if self._effect_allowed(name))))),
        ) if current is not None else original

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    def _effect_allowed(self, name):
        if name == "request_tool_scope":
            return True
        try:
            spec = self._delegate.get_spec(name)
        except KeyError:
            return False
        if spec.side_effect_class == ToolSideEffectClass.DANGEROUS_EXEC:
            # This is availability, not approval or a read-only shell sandbox.
            return True
        if spec.side_effect_class == ToolSideEffectClass.READ:
            checker = getattr(self._delegate, "_is_connector_tool_name", None)
            connected = "connector" in spec.capability_tags or (callable(checker) and checker(name))
            if connected:
                current = getattr(self._delegate, "turn_tool_scope_decision", None)
                original = self.contract.decision
                same_source = (set(getattr(current, "connector_app_ids", ())).issubset(original.connector_app_ids)
                    if original.connector_app_ids else bool(self.contract.source_tags.intersection(spec.capability_tags)))
                return bool(original.connector_source_user_requested and same_source)
            return True
        return name in self.contract.action_tools

    def can_invoke_tool(self, name):
        check = getattr(self._delegate, "can_invoke_tool", None)
        return self._effect_allowed(name) and (not callable(check) or check(name))

    def get_spec(self, name):
        if not self.can_invoke_tool(name):
            raise KeyError(f"Tool not authorized for request recovery: {name}")
        return self._delegate.get_spec(name)

    def list_specs(self):
        return [spec for spec in self._delegate.list_specs() if self.can_invoke_tool(spec.name)]

    def list_tool_definitions(self, *args, **kwargs):
        definitions = [dict(item) for item in self._delegate.list_tool_definitions(*args, **kwargs)
            if self.can_invoke_tool(item["name"])]
        visible = {item["name"] for item in definitions}
        for item in definitions:
            if "continuation_tools" in item:
                item["continuation_tools"] = [name for name in item["continuation_tools"] if name in visible]
        return definitions

    def invoke(self, invocation):
        if not self.can_invoke_tool(invocation.tool_name):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "denied",
                {"reason": "turn_effect_not_authorized"},
                error="Recovery cannot add an action absent from the original request plan.")
        return self._delegate.invoke(invocation)

    def apply_scope_request(self, invocation):
        apply = getattr(self._delegate, "apply_scope_request", None)
        if not callable(apply):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "denied",
                {"reason": "capability_not_granted"},
                error="The current registry does not support scope changes."), self
        result, registry = apply(invocation)
        bound = self.contract.bind(registry)
        if result.status == "completed" and isinstance(result.output, dict):
            output = dict(result.output)
            for key in ("available_tools", "required_tool_names"):
                if isinstance(output.get(key), list):
                    output[key] = [name for name in output[key] if bound.can_invoke_tool(name)]
            for key in ("requested_required_tool_names", "unavailable_required_tool_names"):
                if isinstance(output.get(key), list):
                    output[key] = [name for name in output[key]
                        if name in self.contract.decision.required_tool_names or bound._effect_allowed(name)]
            decision = bound.turn_tool_scope_decision
            output.update(
                requested_outcome=decision.requested_outcome,
                scheduler_effect_actions=list(decision.allowed_scheduler_actions),
                connector_source_user_requested=decision.connector_source_user_requested,
                connector_source_evidence=decision.connector_source_evidence,
                source_artifact_delivery_requested=decision.source_artifact_delivery_requested,
                source_artifact_request_evidence=decision.source_artifact_request_evidence,
                read_only_source_delivery=decision.source_artifact_retrieval_only,
                artifact_extensions=list(decision.requested_artifact_extensions),
                embedded_media_artifact_extensions=list(decision.required_embedded_media_extensions),
                excluded_artifact_extensions=list(decision.excluded_artifact_extensions),
            )
            result = replace(result, output=output)
        return result, bound

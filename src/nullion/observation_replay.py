"""Portable, adapter-owned prerequisites for background source observations.

This protocol is independent of sites and language. Replaying a source cannot
add write authority, bypass account consent, or change the saved matcher.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from uuid import uuid4

from jsonschema import validate, ValidationError, SchemaError


def validated_observation_setup(registry, source_tool, output):
    from nullion.tools import ToolSideEffectClass

    if not isinstance(output, Mapping) or not output.get('observation_setup'):
        return []
    allowed = getattr(registry.get_spec(source_tool), 'observation_setup_tools', ())
    if not allowed:
        return []
    steps = output['observation_setup']
    if not isinstance(steps, list) or not 1 <= len(steps) <= 3:
        raise ValueError('Observation prerequisites must contain one to three READ steps')
    for step in steps:
        if not isinstance(step, dict) or step.get('tool_name') not in allowed or not isinstance(step.get('arguments'), dict):
            raise ValueError('Observation prerequisite is not declared by the source adapter')
        spec = registry.get_spec(step['tool_name'])
        if (spec.side_effect_class != ToolSideEffectClass.READ or spec.requires_approval
                or spec.delegated_tools or spec.source_artifact_extensions or spec.source_artifact_only
                or not 0 < spec.timeout_seconds <= 30):
            raise ValueError('Observation prerequisites must be bounded dedicated READ tools')
        try:
            validate(step['arguments'], spec.input_schema or {'type': 'object'})
        except (ValidationError, SchemaError) as exc:
            raise ValueError('Observation prerequisite arguments violate the registered schema') from exc
    return deepcopy(steps)


def observation_source_session_key(registry, probe, contexts, index):
    """Group equivalent prepared sources without borrowing another page's DOM."""
    from nullion.workflow_recovery import fingerprint

    def canonical(step):
        arguments = dict(step['arguments'])
        try:
            bindings = registry.get_spec(step['tool_name']).context_argument_bindings
        except KeyError:
            bindings = ()  # Replay validates a removed capability separately.
        for argument, _ in bindings:
            arguments.pop(argument, None)
        return {'tool_name': step['tool_name'], 'arguments': arguments}

    recipes = [context.get('observation_setup') if isinstance(context, Mapping) else None
               for context in contexts]
    recipe = recipes[index] if index < len(recipes) else None
    if not recipe:
        # An explicit prerequisite belongs to the same source as the later
        # read, even when its creator supplied a process-local session alias.
        current = canonical(probe[index])
        recipe = next((steps for steps in recipes if steps
                       and current in [canonical(step) for step in steps]), None)
    return fingerprint([canonical(step) for step in recipe])[:12] if recipe else None


def replay_observation_setup(registry, invocation, steps, runtime_store, *, completed_steps=()):
    from nullion.runtime import invoke_tool_with_boundary_policy
    from nullion.tools import ToolInvocation, ToolResult, normalize_tool_status

    # Revalidate persisted recipes against current adapter metadata on every
    # poll. Removing a capability or changing its effects takes effect now.
    try:
        setup = validated_observation_setup(registry, invocation.tool_name, {'observation_setup': steps})
        if not setup:
            raise ValueError('The adapter no longer permits observation prerequisites')
    except (ValueError, KeyError, TypeError):
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
            {'reason': 'observation_setup_contract'}, 'The saved source prerequisites no longer match the current adapter.')
    from nullion.workflow_recovery import fingerprint
    def identity(step, context):
        arguments = dict(step['arguments'])
        spec = registry.get_spec(step['tool_name'])
        for argument, context_key in getattr(spec, 'context_argument_bindings', ()):
            if context_key in context:
                arguments[argument] = context[context_key]
        target = {key: context[key] for key in ('browser_session_scope', 'browser_session_owner', 'browser_session_id')
                  if key in context}
        return fingerprint([step['tool_name'], arguments, target])
    context = invocation.flow_context or {}
    # A completion on another target cannot satisfy this page's setup. Never
    # rebind the past execution to the current target when comparing receipts.
    completed = {identity(step, step.get('flow_context') or {}) for step in completed_steps}
    inline = set(getattr(registry.get_spec(invocation.tool_name), 'observation_inline_setup_tools', ()))
    for step in setup:
        if step['tool_name'] in inline:
            continue
        key = identity(step, context)
        if key in completed:
            continue
        prerequisite = ToolInvocation('setup-' + uuid4().hex, step['tool_name'], invocation.principal_id,
            step['arguments'], capsule_id=invocation.capsule_id, flow_context=dict(invocation.flow_context or {}))
        result = invoke_tool_with_boundary_policy(runtime_store, prerequisite, registry=registry)
        if normalize_tool_status(result.status) != 'completed':
            return result
        completed.add(key)
        if isinstance(completed_steps, list):
            completed_steps.append({**deepcopy(step), 'flow_context': deepcopy(context)})
    return None

"""Validated tool-owned contracts for correcting a call without replacing its tool."""

from collections.abc import Mapping


def tool_repair_kind(result: object) -> str | None:
    if getattr(result, "status", None) != "failed":
        return None
    output = getattr(result, "output", None)
    if not isinstance(output, Mapping):
        return None
    recovery = output.get("recovery")
    if not isinstance(recovery, Mapping):
        return None
    if recovery.get("retry_tool_name") != getattr(result, "tool_name", None):
        return None
    kind = recovery.get("kind")
    if kind == "repair_precondition" and output.get("reason") == "tool_precondition_unmet":
        tools = recovery.get("required_tool_names")
        if isinstance(tools, list) and tools and all(isinstance(name, str) and name.strip() for name in tools):
            return kind
    if kind == "correct_arguments" and output.get("reason") == "invalid_tool_arguments":
        constraints = recovery.get("argument_constraints")
        if isinstance(constraints, Mapping) and constraints:
            return kind
    return None

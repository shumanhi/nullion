"""Validated tool-owned contracts for correcting a call without replacing its tool."""

from collections.abc import Mapping

MAX_UNEXECUTED_PLAN_REVISIONS = 3


def unresolved_plan_revision_count(results, tool_name):
    """Changed arguments are proposals, not execution progress."""
    count = 0
    for row in reversed(list(results)):
        if row.tool_name != tool_name:
            continue
        if row.status == 'completed':
            break
        if tool_repair_kind(row) == 'revise_plan' and not getattr(row, 'recovered_by', ()):
            count += 1
    return count


def exhausted_plan_revision_tool(results):
    counts, completed = {}, set()
    for row in reversed(list(results)):
        if row.tool_name in completed:
            continue
        if row.status == 'completed':
            completed.add(row.tool_name)
        elif tool_repair_kind(row) == 'revise_plan' and not getattr(row, 'recovered_by', ()):
            counts[row.tool_name] = counts.get(row.tool_name, 0) + 1
            if counts[row.tool_name] >= MAX_UNEXECUTED_PLAN_REVISIONS:
                return row.tool_name
    return None


def plan_revision_recovery(tool_name: str) -> dict:
    return {"kind": "revise_plan", "retry_tool_name": tool_name, "approval_bypass": False,
        "instruction": "Revise the plan using the review issues and current request. "
            "Retry this tool with corrected arguments only if its action is still requested. "
            "Otherwise omit the unrequested action and finish the actual request using verified evidence. "
            "This call did not execute. Normal preflight, "
            "approval, policy and cancellation checks still apply."}


def resolve_plan_revisions(results, completed, *, preflight_proceeded: bool) -> None:
    """Retire rejected proposals only after an admitted execution of that tool.

    This receipt says the proposal was corrected, not that the user's goal is
    fulfilled. Denials and failures from actions that actually ran remain open.
    """
    if not preflight_proceeded or completed.status != "completed":
        return
    for previous in results:
        if previous.invocation_id == completed.invocation_id:
            break
        if previous.tool_name == completed.tool_name and tool_repair_kind(previous) == "revise_plan":
            previous.recovered_by = (completed.invocation_id,)


def tool_repair_kind(result: object) -> str | None:
    if getattr(result, "status", None) not in {"failed", "denied"}:
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
    if (kind == "retry_in_workspace_scratch" and output.get("reason") == "workspace_scratch_path_required"
            and getattr(result, "tool_name", None) in {"terminal_exec", "file_write", "file_patch"}
            and output.get("command_executed") is False and recovery.get("approval_bypass") is False
            and output.get("requires_approval") is False and isinstance(output.get("suggested_path"), str)):
        return kind
    if getattr(result, "status", None) != "failed":
        return None
    if (kind == 'select_retained_artifact' and output.get('reason') == 'artifact_read_reference_selection_required'
            and getattr(result, 'tool_name', None) == 'file_read' and output.get('executed') is False
            and recovery.get('approval_bypass') is False):
        candidates = recovery.get('candidate_paths')
        if isinstance(candidates, list) and candidates and all(isinstance(path, str) and path.strip() for path in candidates):
            return kind
    if (kind == "revise_plan" and output.get("reason") == "request_plan_conflict"
            and output.get("executed") is False and output.get("review_decision") == "revise"
            and output.get("retryable") is True and recovery.get("approval_bypass") is False):
        return kind
    if (kind == "try_non_destructive_creation" and output.get("reason") == "artifact_deletion_alternative_available"
            and getattr(result, "tool_name", None) == "terminal_exec" and output.get("command_executed") is False
            and recovery.get("approval_bypass") is False):
        return kind
    if kind == "inspect_page" and output.get("reason") == "browser_condition_unmet":
        tools = recovery.get("required_tool_names")
        if isinstance(tools, list) and tools and all(isinstance(name, str) and name.strip() for name in tools):
            return kind
    if kind == "repair_precondition" and output.get("reason") == "tool_precondition_unmet":
        tools = recovery.get("required_tool_names")
        if isinstance(tools, list) and tools and all(isinstance(name, str) and name.strip() for name in tools):
            return kind
    if kind == "correct_arguments" and output.get("reason") in ("invalid_tool_arguments", "inventory_changed", "source_not_ready"):
        constraints = recovery.get("argument_constraints")
        if isinstance(constraints, Mapping) and constraints:
            return kind
    return None

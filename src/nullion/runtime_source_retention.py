"""Keep runtime-owned source evidence out of agent-directed cron cleanup."""
from __future__ import annotations

from pathlib import Path
import shlex
from typing import Iterable

from nullion.tools import ToolInvocation, ToolResult


def retained_runtime_source_cleanup_result(
    invocation: ToolInvocation, results: Iterable[ToolResult], *, registry: object,
) -> ToolResult | None:
    context = invocation.flow_context or {}
    if invocation.tool_name != "terminal_exec" or context.get("scheduled_task_run") is not True:
        return None
    command = invocation.arguments.get("command")
    if not isinstance(command, str):
        return None
    # This is a restrictive command safety check, not intent classification.
    # Never swallow compound commands, expansion, recursive deletion or other work.
    if any(character in command for character in "\n\r;|&><`$*?[]{}()"):
        return None
    try:
        tokens = shlex.split(command)
    except ValueError:
        return None
    if not tokens or tokens[0] not in {"rm", "/bin/rm", "/usr/bin/rm"}:
        return None
    paths: list[str] = []
    options_done = False
    for token in tokens[1:]:
        if not options_done and token == "--":
            options_done = True
        elif not options_done and token in {"-f", "--force"}:
            continue
        elif not options_done and token.startswith("-"):
            return None
        else:
            paths.append(token)
    if not paths:
        return None
    # Only adapter-produced source descriptors in this run establish ownership.
    owned: set[Path] = set()
    for result in results:
        if result.tool_name != "terminal_exec" or result.status != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        for descriptor in output.get("artifact_descriptors") or ():
            if (isinstance(descriptor, dict) and descriptor.get("role") == "source"
                    and descriptor.get("kind") == "terminal_output" and isinstance(descriptor.get("path"), str)):
                owned.add(Path(descriptor["path"]).expanduser().absolute())
    if not owned:
        return None
    from nullion.tools import _terminal_execution_cwd_from_registry
    cwd = _terminal_execution_cwd_from_registry(registry)
    targets: list[Path] = []
    for raw in paths:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            if cwd is None:
                return None
            path = cwd / path
        path = path.absolute()
        # Reject symlink aliases and traversal; keep unknown targets on the
        # ordinary destructive-action approval path.
        if ".." in path.parts or path.is_symlink() or path not in owned or not path.is_file():
            return None
        targets.append(path)
    return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {
        "reason": "runtime_source_retained",
        "command_executed": False,
        "deleted_paths": [],
        "retained_paths": [str(path) for path in targets],
        "message": "No cleanup is needed for runtime-owned source evidence. The files were retained. Continue the scheduled task without deleting them.",
    })

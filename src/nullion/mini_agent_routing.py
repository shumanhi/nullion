"""Routing policy for when not to spend a mini-agent run."""

from __future__ import annotations

from typing import Any, Iterable


_ARTIFACT_DELIVERY_TOOLS = frozenset(
    {
        "file_write",
        "pdf_create",
        "pdf_edit",
        "presentation_create",
        "spreadsheet_create",
    }
)
_TEXT_ARTIFACT_EXTENSIONS = frozenset({"", ".csv", ".htm", ".html", ".json", ".md", ".svg", ".tsv", ".txt", ".yaml", ".yml"})
_EXTENSION_ARTIFACT_TOOLS = {
    ".pdf": frozenset({"pdf_create", "pdf_edit"}),
    ".ppt": frozenset({"presentation_create"}),
    ".pptx": frozenset({"presentation_create"}),
    ".csv": frozenset({"file_write", "spreadsheet_create"}),
    ".tsv": frozenset({"file_write", "spreadsheet_create"}),
    ".xls": frozenset({"spreadsheet_create"}),
    ".xlsx": frozenset({"spreadsheet_create"}),
}
_SIMPLE_ARTIFACT_DISPOSITIONS = frozenset({"single_turn", "sequential_mission"})


def should_route_without_mini_agents(message: str, *, has_attachments: bool = False) -> bool:
    """Return True only for structural cases that should never fork mini-agents."""

    return bool(has_attachments)


def should_keep_dag_plan_in_direct_turn(
    dag_plan: Any,
    *,
    available_tools: Iterable[str] | None = None,
    requested_extensions: Iterable[str] | None = None,
) -> bool:
    """Return True for small structured artifact workflows that should not fork.

    The decision uses only model-produced plan shape, requested file extensions,
    and tool metadata. It deliberately does not inspect topic words in the user
    request or task titles.
    """

    tasks = tuple(getattr(dag_plan, "tasks", ()) or ())
    if not tasks or len(tasks) > 2:
        return False
    disposition = str(getattr(dag_plan, "disposition", "") or "").strip()
    if disposition and disposition not in _SIMPLE_ARTIFACT_DISPOSITIONS:
        return False
    normalized_extensions = {
        str(extension or "").strip().lower()
        for extension in (requested_extensions or ())
        if str(extension or "").strip()
    }
    available_tool_names = {
        str(tool or "").strip()
        for tool in (available_tools or ())
        if str(tool or "").strip()
    }
    if normalized_extensions and _artifact_tools_for_extensions(normalized_extensions).intersection(available_tool_names):
        return True
    for task in tasks:
        tool_scope = {
            str(tool or "").strip()
            for tool in (getattr(task, "tool_scope", ()) or ())
            if str(tool or "").strip()
        }
        if tool_scope.intersection(_ARTIFACT_DELIVERY_TOOLS):
            return True
    return False


def _artifact_tools_for_extensions(extensions: Iterable[str]) -> frozenset[str]:
    tools: set[str] = set()
    for extension in extensions:
        normalized = str(extension or "").strip().lower()
        if not normalized:
            continue
        if normalized in _TEXT_ARTIFACT_EXTENSIONS:
            tools.add("file_write")
        tools.update(_EXTENSION_ARTIFACT_TOOLS.get(normalized, ()))
    return frozenset(tools)


__all__ = ["should_keep_dag_plan_in_direct_turn", "should_route_without_mini_agents"]

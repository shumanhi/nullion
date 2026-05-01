"""Typed read-model helpers for Nullion system context snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from nullion.tools import ToolRegistry, ToolSpec

CORE_FALLBACK_TOOL_NAMES = ("file_read", "file_write", "terminal_exec", "web_fetch")
_PLUGIN_CAPABILITIES: dict[str, tuple[str, ...]] = {
    "search_plugin": ("web_search",),
    "browser_plugin": ("browser_navigate",),
    "workspace_plugin": ("file_search", "file_patch", "workspace_summary"),
    "email_plugin": ("email_search", "email_read"),
    "calendar_plugin": ("calendar_list",),
}
_PLUGIN_BY_CAPABILITY: dict[str, str] = {
    capability: plugin
    for plugin, capabilities in _PLUGIN_CAPABILITIES.items()
    for capability in capabilities
}


@dataclass(frozen=True, slots=True)
class SystemContextToolSummary:
    name: str
    description: str
    side_effect_class: str
    risk_level: str
    requires_approval: bool
    source: str = "core fallback"
    availability: str = "available"


@dataclass(frozen=True, slots=True)
class SystemContextSection:
    title: str
    lines: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SystemContextSnapshot:
    project_summary: str | None = None
    goals: tuple[str, ...] = ()
    initial_focus: tuple[str, ...] = ()
    installed_plugins: tuple[str, ...] = ()
    core_fallback_tool_names: tuple[str, ...] = ()
    available_tools: tuple[SystemContextToolSummary, ...] = ()
    sections: tuple[SystemContextSection, ...] = ()


def _normalize_lines(lines: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped:
            normalized.append(stripped)
    return tuple(normalized)


def _tool_summary_from_spec(spec: ToolSpec) -> SystemContextToolSummary:
    source_plugin = _PLUGIN_BY_CAPABILITY.get(spec.name)
    source = f"plugin path:{source_plugin}" if source_plugin is not None else "core fallback"
    availability = "approval required" if spec.requires_approval else "available"
    return SystemContextToolSummary(
        name=spec.name,
        description=spec.description,
        side_effect_class=spec.side_effect_class.value,
        risk_level=spec.risk_level.value,
        requires_approval=spec.requires_approval,
        source=source,
        availability=availability,
    )


def _unavailable_tool_summary(*, name: str, source: str) -> SystemContextToolSummary:
    return SystemContextToolSummary(
        name=name,
        description="",
        side_effect_class="",
        risk_level="",
        requires_approval=False,
        source=source,
        availability="unavailable",
    )


def _build_tool_summaries(
    *,
    tool_registry: ToolRegistry,
    installed_plugins: Sequence[str],
) -> tuple[SystemContextToolSummary, ...]:
    specs = tool_registry.list_specs()
    spec_names = {spec.name for spec in specs}
    summaries = [_tool_summary_from_spec(spec) for spec in specs]

    for name in CORE_FALLBACK_TOOL_NAMES:
        if name not in spec_names:
            summaries.append(_unavailable_tool_summary(name=name, source="core fallback"))

    for plugin_name in sorted(installed_plugins):
        for capability in _PLUGIN_CAPABILITIES.get(plugin_name, ()):
            if capability not in spec_names:
                summaries.append(_unavailable_tool_summary(name=capability, source=f"plugin path:{plugin_name}"))
    return tuple(summaries)


def _derive_core_fallback_tool_names(tool_registry: ToolRegistry | None) -> tuple[str, ...]:
    if tool_registry is None:
        return ()
    spec_names = {spec.name for spec in tool_registry.list_specs()}
    return tuple(name for name in CORE_FALLBACK_TOOL_NAMES if name in spec_names)


def _sections_from_mapping(sections: Mapping[str, Sequence[str]]) -> tuple[SystemContextSection, ...]:
    normalized: list[SystemContextSection] = []
    for title, lines in sections.items():
        stripped_title = title.strip()
        normalized_lines = _normalize_lines(lines)
        if not stripped_title or not normalized_lines:
            continue
        normalized.append(SystemContextSection(title=stripped_title, lines=normalized_lines))
    return tuple(normalized)


def build_system_context_snapshot(
    *,
    project_summary: str | None = None,
    goals: Sequence[str] = (),
    initial_focus: Sequence[str] = (),
    tool_registry: ToolRegistry | None = None,
    installed_plugins: Sequence[str] | None = None,
    available_tools: Sequence[SystemContextToolSummary] | None = None,
    sections: Mapping[str, Sequence[str]] | Sequence[SystemContextSection] | None = None,
) -> SystemContextSnapshot:
    normalized_summary = project_summary.strip() if isinstance(project_summary, str) else None
    normalized_goals = _normalize_lines(goals)
    normalized_focus = _normalize_lines(initial_focus)
    if installed_plugins is not None:
        normalized_plugins = tuple(plugin for plugin in _normalize_lines(installed_plugins))
    elif tool_registry is not None:
        normalized_plugins = tuple(tool_registry.list_installed_plugins())
    else:
        normalized_plugins = ()
    normalized_core_fallback_tool_names = _derive_core_fallback_tool_names(tool_registry)

    if available_tools is not None:
        normalized_tools = tuple(available_tools)
    elif tool_registry is not None:
        normalized_tools = _build_tool_summaries(
            tool_registry=tool_registry,
            installed_plugins=normalized_plugins,
        )
    else:
        normalized_tools = ()

    if sections is None:
        normalized_sections = ()
    elif isinstance(sections, Mapping):
        normalized_sections = _sections_from_mapping(sections)
    else:
        normalized_sections = tuple(section for section in sections if section.title and section.lines)

    return SystemContextSnapshot(
        project_summary=normalized_summary or None,
        goals=normalized_goals,
        initial_focus=normalized_focus,
        installed_plugins=normalized_plugins,
        core_fallback_tool_names=normalized_core_fallback_tool_names,
        available_tools=normalized_tools,
        sections=normalized_sections,
    )


def format_system_context_for_prompt(snapshot: SystemContextSnapshot) -> str:
    blocks: list[str] = []

    if snapshot.project_summary:
        blocks.append(f"Project summary:\n{snapshot.project_summary}")

    if snapshot.goals:
        blocks.append("Project goals:\n" + "\n".join(f"- {goal}" for goal in snapshot.goals))

    if snapshot.initial_focus:
        blocks.append("Initial focus:\n" + "\n".join(f"- {item}" for item in snapshot.initial_focus))

    if snapshot.installed_plugins:
        blocks.append("Installed plugins:\n" + "\n".join(f"- {plugin}" for plugin in snapshot.installed_plugins))

    if snapshot.core_fallback_tool_names:
        blocks.append(
            "Core fallback contract:\n"
            "- Any task Nullion claims is doable should remain possible through core tools.\n"
            "- Builder can orchestrate slower fallback plans when plugins are absent.\n"
            f"- Core fallback tools: {', '.join(snapshot.core_fallback_tool_names)}"
        )

    if snapshot.available_tools:
        tool_lines = []
        for tool in snapshot.available_tools:
            line = f"- {tool.name} [{tool.source} • {tool.availability}]"
            if tool.availability != "unavailable":
                approval_text = "approval required" if tool.requires_approval else "direct"
                line += f" ({tool.side_effect_class}, {tool.risk_level}, {approval_text})"
            if tool.description:
                line += f" — {tool.description}"
            tool_lines.append(line)
        blocks.append("Available tools:\n" + "\n".join(tool_lines))

    for section in snapshot.sections:
        blocks.append(f"{section.title}:\n" + "\n".join(f"- {line}" for line in section.lines))

    return "\n\n".join(blocks)


__all__ = [
    "CORE_FALLBACK_TOOL_NAMES",
    "SystemContextSection",
    "SystemContextSnapshot",
    "SystemContextToolSummary",
    "build_system_context_snapshot",
    "format_system_context_for_prompt",
]

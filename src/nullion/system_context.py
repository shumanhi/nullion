"""Typed read-model helpers for Nullion system context snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from nullion.tips import IMAGE_GENERATION_SETUP_TIP, format_setup_tip
from nullion.tools import ToolRegistry, ToolSpec

CORE_FALLBACK_TOOL_NAMES = ("file_read", "file_write", "terminal_exec", "web_fetch")
_PLUGIN_CAPABILITIES: dict[str, tuple[str, ...]] = {
    "search_plugin": ("web_search", "file_download"),
    "browser_plugin": ("browser_navigate",),
    "workspace_plugin": ("archive_create", "archive_extract", "file_search", "file_patch", "workspace_summary"),
    "email_plugin": ("email_search", "email_read"),
    "calendar_plugin": ("calendar_list", "calendar_create", "calendar_update", "calendar_respond", "calendar_delete"),
    "media_plugin": ("audio_transcribe", "image_extract_text", "image_generate"),
}
_PLUGIN_BY_CAPABILITY: dict[str, str] = {
    capability: plugin
    for plugin, capabilities in _PLUGIN_CAPABILITIES.items()
    for capability in capabilities
}
_UNAVAILABLE_TOOL_DESCRIPTIONS = {
    "image_generate": (
        "API image generation is not configured. If you create local fallback images with core tools, "
        "say they are local fallback images and use this setup tip format: "
        f"{format_setup_tip(IMAGE_GENERATION_SETUP_TIP)}"
    ),
}


@dataclass(frozen=True, slots=True)
class SystemContextToolSummary:
    name: str
    description: str
    side_effect_class: str
    risk_level: str
    requires_approval: bool
    capability_tags: tuple[str, ...] = ()
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
        capability_tags=tuple(
            str(tag).strip().lower()
            for tag in (getattr(spec, "capability_tags", ()) or ())
            if str(tag).strip()
        ),
        source=source,
        availability=availability,
    )


def _unavailable_tool_summary(*, name: str, source: str) -> SystemContextToolSummary:
    return SystemContextToolSummary(
        name=name,
        description=_UNAVAILABLE_TOOL_DESCRIPTIONS.get(name, ""),
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
            if tool.capability_tags:
                line += f" tags={','.join(tool.capability_tags)}"
            if tool.description:
                line += f" — {tool.description}"
            tool_lines.append(line)
        blocks.append("Available tools:\n" + "\n".join(tool_lines))

    for section in snapshot.sections:
        blocks.append(f"{section.title}:\n" + "\n".join(f"- {line}" for line in section.lines))

    return "\n\n".join(blocks)


def format_compact_system_context_for_prompt(snapshot: SystemContextSnapshot) -> str:
    """Compact stable context for chat turns that already pass full tool schemas."""
    blocks: list[str] = []
    installed_plugins = tuple(getattr(snapshot, "installed_plugins", ()) or ())
    core_fallback_tool_names = tuple(getattr(snapshot, "core_fallback_tool_names", ()) or ())
    available_tools = tuple(getattr(snapshot, "available_tools", ()) or ())
    sections = tuple(getattr(snapshot, "sections", ()) or ())
    if installed_plugins:
        blocks.append("Installed plugins: " + ", ".join(installed_plugins))
    if core_fallback_tool_names:
        blocks.append(
            "Core fallback tools: " + ", ".join(core_fallback_tool_names)
        )
    if available_tools:
        tool_lines = []
        for tool in available_tools:
            suffix_parts = [str(getattr(tool, "availability", "") or "available")]
            if getattr(tool, "requires_approval", False):
                suffix_parts.append("approval")
            capability_tags = tuple(getattr(tool, "capability_tags", ()) or ())
            if capability_tags:
                suffix_parts.append("tags=" + ",".join(str(tag) for tag in capability_tags))
            tool_lines.append(f"- {getattr(tool, 'name', 'tool')} ({'; '.join(suffix_parts)})")
        blocks.append("Available tool names:\n" + "\n".join(tool_lines))
    for section in sections:
        title = getattr(section, "title", "")
        lines = tuple(getattr(section, "lines", ()) or ())
        if title and lines:
            blocks.append(f"{title}:\n" + "\n".join(f"- {line}" for line in lines))
    return "\n\n".join(blocks)


__all__ = [
    "CORE_FALLBACK_TOOL_NAMES",
    "SystemContextSection",
    "SystemContextSnapshot",
    "SystemContextToolSummary",
    "build_system_context_snapshot",
    "format_compact_system_context_for_prompt",
    "format_system_context_for_prompt",
]

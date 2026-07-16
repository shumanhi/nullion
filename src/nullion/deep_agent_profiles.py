"""Built-in Deep Agents profile hints for delegated Nullion tasks."""

from __future__ import annotations
from functools import lru_cache
from typing import Any


_PROFILE_DEFINITIONS: dict[str, dict[str, str]] = {
    "research": {
        "name": "research_agent",
        "description": "Research web or external information and return sourced, concise findings.",
        "system_prompt": "Focus on gathering evidence with the available research tools before answering.",
    },
    "repo_analysis": {
        "name": "repo_analysis_agent",
        "description": "Inspect repository files and explain implementation evidence.",
        "system_prompt": "Read relevant files first, cite concrete paths or symbols, and avoid guessing.",
    },
    "artifact": {
        "name": "artifact_agent",
        "description": "Create or update files, reports, screenshots, or other deliverable artifacts.",
        "system_prompt": "Use artifact-producing tools for deliverables and verify created paths before claiming completion.",
    },
    "artifact_verifier": {
        "name": "artifact_verifier_agent",
        "description": "Verify reports, screenshots, and other artifact delivery evidence before final user-visible claims.",
        "system_prompt": "Check that artifact paths, expected formats, and delivery evidence are present before reporting success.",
    },
    "browser": {
        "name": "browser_agent",
        "description": "Navigate, inspect, screenshot, or test browser-based workflows.",
        "system_prompt": (
            "Keep browser actions scoped and preserve the active session while pursuing the requested outcome. "
            "Treat one failed tool, selector, extraction, or assertion as a local path failure rather than a terminal "
            "task blocker. Continue with semantically distinct structured browser surfaces that are available, such as "
            "scoped text, structured items, stable element ids, page state, JavaScript inspection, snapshots, or a "
            "direct source page. Prefer later, stronger structured evidence over an earlier negative observation. "
            "Report a blocker only after viable alternatives have been attempted and the concrete external blocker "
            "remains visible in runtime evidence."
        ),
    },
    "doctor": {
        "name": "doctor_agent",
        "description": "Diagnose Nullion runtime health, services, approvals, or recovery actions.",
        "system_prompt": "Prioritize diagnosis, explain the evidence, and request approval for risky repair actions.",
    },
    "migration": {
        "name": "migration_agent",
        "description": "Plan and execute code migrations or dependency-backed refactors.",
        "system_prompt": "Make small verifiable changes, preserve existing behavior, and run focused verification.",
    },
    "scheduled_job": {
        "name": "scheduled_job_agent",
        "description": "Run, inspect, and summarize scheduled cron, reminder, or monitoring workflows.",
        "system_prompt": "Treat scheduled work as run-inspect-notify: execute the scoped task, inspect whether notification is needed, and summarize any artifact or escalation.",
    },
}

_BUILTIN_SKILL_SOURCE = "/skills/nullion/"
_TAG_PROFILE_RULES: tuple[tuple[frozenset[str], str], ...] = (
    (frozenset({"scheduler", "cron", "reminder"}), "scheduled_job"),
    (frozenset({"doctor", "health", "service", "recovery"}), "doctor"),
    (frozenset({"migration", "refactor", "dependency"}), "migration"),
    (frozenset({"artifact", "file", "document", "pdf", "spreadsheet", "presentation", "media", "image_generation"}), "artifact"),
    (frozenset({"browser", "ui", "screenshot"}), "browser"),
    (frozenset({"public_web", "web", "network", "search", "weather", "forecast"}), "research"),
    (frozenset({"filesystem", "workspace", "repo"}), "repo_analysis"),
)

_STRUCTURED_TOOL_PROFILE_HINTS: dict[str, tuple[str, ...]] = {
    "file_read": ("repo_analysis",),
    "file_search": ("repo_analysis",),
    "repo_search": ("repo_analysis",),
}
def _dedupe_profiles(profiles: list[str]) -> list[str]:
    return [profile for profile in dict.fromkeys(profiles) if profile in _PROFILE_DEFINITIONS]


def _normalize_tags(tags: Any) -> tuple[str, ...]:
    return tuple(sorted({str(tag).strip().lower() for tag in (tags or ()) if str(tag).strip()}))


def _profiles_for_tags(tags: tuple[str, ...]) -> list[str]:
    tag_set = set(tags)
    profiles: list[str] = []
    for rule_tags, profile in _TAG_PROFILE_RULES:
        if tag_set.intersection(rule_tags):
            profiles.append(profile)
    return profiles


def _tool_profile_shape(tool_definitions: Any) -> tuple[tuple[str, tuple[str, ...]], ...]:
    shape: list[tuple[str, tuple[str, ...]]] = []
    for definition in tool_definitions or ():
        if not isinstance(definition, dict):
            continue
        name = str(definition.get("name") or "").strip()
        if not name:
            continue
        shape.append((name, _normalize_tags(definition.get("capability_tags"))))
    return tuple(sorted(shape))


@lru_cache(maxsize=128)
def _cached_tool_profile_metadata(
    shape: tuple[tuple[str, tuple[str, ...]], ...],
    ) -> tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...]:
    rows: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = []
    for name, tags in shape:
        profiles = _dedupe_profiles(_profiles_for_tags(tags))
        rows.append((name, tags, tuple(profiles)))
    return tuple(rows)


def deep_agent_tool_profile_metadata(tool_definitions: Any) -> dict[str, dict[str, tuple[str, ...]]]:
    """Return compact cached profile metadata keyed by tool registry shape."""

    rows = _cached_tool_profile_metadata(_tool_profile_shape(tool_definitions))
    return {
        "tool_capability_tags": {name: tags for name, tags, _profiles in rows},
        "tool_profiles": {name: profiles for name, _tags, profiles in rows},
    }


def deep_agent_task_metadata_for_tools(
    allowed_tools: Any,
    tool_profile_metadata: dict[str, dict[str, tuple[str, ...]]] | None,
) -> dict[str, object]:
    """Return compact task metadata for Deep Agents profile selection."""

    if not tool_profile_metadata:
        return {}
    tool_names = [str(tool).strip() for tool in (allowed_tools or ()) if str(tool).strip()]
    tags_by_tool = tool_profile_metadata.get("tool_capability_tags", {})
    profiles_by_tool = tool_profile_metadata.get("tool_profiles", {})
    tags: list[str] = []
    profiles: list[str] = []
    for tool_name in tool_names:
        tags.extend(tags_by_tool.get(tool_name, ()))
        profiles.extend(profiles_by_tool.get(tool_name, ()))
    metadata: dict[str, object] = {}
    if tags:
        metadata["tool_capability_tags"] = _normalize_tags(tags)
    profiles = _dedupe_profiles(profiles)
    if profiles:
        metadata["deep_agent_profiles"] = profiles
    return metadata


def clear_deep_agent_profile_caches() -> None:
    """Clear cached compact tool-profile metadata after registry changes."""

    _cached_tool_profile_metadata.cache_clear()


def deep_agent_profile_names_for_task(task: Any) -> list[str]:
    """Infer reusable Deep Agents profiles from structured scoped tools."""
    metadata = getattr(task, "metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    profiles: list[str] = []

    if metadata.get("artifact_role") == "verify":
        profiles.append("artifact_verifier")
    elif metadata.get("requires_artifact_delivery"):
        profiles.append("artifact")
    profiles.extend(str(profile) for profile in (metadata.get("deep_agent_profiles") or ()))
    profiles.extend(_profiles_for_tags(_normalize_tags(metadata.get("tool_capability_tags"))))
    for tool_name in getattr(task, "allowed_tools", None) or ():
        profiles.extend(_STRUCTURED_TOOL_PROFILE_HINTS.get(str(tool_name).strip(), ()))

    return _dedupe_profiles(profiles)


def deep_agent_skills_for_task(task: Any) -> list[str]:
    """Return Deep Agents skill source paths for inferred built-in profiles."""
    return [_BUILTIN_SKILL_SOURCE] if deep_agent_profile_names_for_task(task) else []


def deep_agent_skill_files_for_task(task: Any) -> dict[str, dict[str, str]]:
    """Return in-memory SKILL.md files for inferred built-in profiles."""
    files: dict[str, dict[str, str]] = {}
    for profile in deep_agent_profile_names_for_task(task):
        skill_name = profile.replace("_", "-")
        definition = _PROFILE_DEFINITIONS[profile]
        content = (
            "---\n"
            f"name: {skill_name}\n"
            f"description: {definition['description']}\n"
            "---\n\n"
            f"# {skill_name.title()} Skill\n\n"
            "## When To Use\n"
            f"Use this skill when a delegated Nullion task matches the {skill_name} profile.\n\n"
            "## Instructions\n"
            f"{definition['system_prompt']}\n"
        )
        files[f"{_BUILTIN_SKILL_SOURCE}{skill_name}/SKILL.md"] = {"content": content, "encoding": "utf-8"}
    return files


def deep_agent_subagents_for_task(task: Any) -> list[dict[str, str]]:
    """Return Deep Agents subagent specs for the inferred built-in profiles."""
    return [dict(_PROFILE_DEFINITIONS[profile]) for profile in deep_agent_profile_names_for_task(task)]


__all__ = [
    "clear_deep_agent_profile_caches",
    "deep_agent_profile_names_for_task",
    "deep_agent_skill_files_for_task",
    "deep_agent_skills_for_task",
    "deep_agent_subagents_for_task",
    "deep_agent_task_metadata_for_tools",
    "deep_agent_tool_profile_metadata",
]

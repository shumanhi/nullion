"""Built-in Deep Agents profile hints for delegated Nullion tasks."""

from __future__ import annotations
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
        "system_prompt": "Keep browser actions scoped, report visible state, and recover from navigation or capture failures.",
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


def deep_agent_profile_names_for_task(task: Any) -> list[str]:
    """Infer reusable Deep Agents profiles from structured scoped tools."""
    tools = {str(tool).lower() for tool in (getattr(task, "allowed_tools", None) or [])}
    profiles: list[str] = []

    if tools & {"web_search", "web_fetch"}:
        profiles.append("research")
    if tools & {"file_read", "file_search", "workspace_summary"}:
        profiles.append("repo_analysis")
    if any(tool.startswith("browser_") for tool in tools):
        profiles.append("browser")
    if tools & {"file_write", "pdf_create", "pdf_edit", "render", "image_generate"}:
        profiles.append("artifact")
        profiles.append("artifact_verifier")

    return list(dict.fromkeys(profiles))


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
    "deep_agent_profile_names_for_task",
    "deep_agent_skill_files_for_task",
    "deep_agent_skills_for_task",
    "deep_agent_subagents_for_task",
]

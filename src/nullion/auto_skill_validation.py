"""Deep Agents validation descriptors for auto-skill proposals."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


AUTO_SKILL_SOURCE = "/skills/auto-skill/"


@dataclass(slots=True, frozen=True)
class AutoSkillValidationPlan:
    status: str
    skill_source: str
    skill_files: dict[str, dict[str, str]]
    subagents: tuple[dict[str, str], ...]
    golden_workflows: tuple[dict[str, object], ...]
    checks: tuple[str, ...]


def build_auto_skill_validation_plan(proposal: Any) -> AutoSkillValidationPlan:
    slug = _slugify(getattr(proposal, "title", "") or "auto-skill")
    skill_path = f"{AUTO_SKILL_SOURCE}{slug}/SKILL.md"
    steps = [str(step).strip() for step in (getattr(proposal, "steps", None) or []) if str(step).strip()]
    trigger = str(getattr(proposal, "trigger", "") or "").strip()
    summary = str(getattr(proposal, "summary", "") or "").strip()
    title = str(getattr(proposal, "title", "") or "Auto Skill").strip()
    skill_md = _skill_markdown(slug=slug, title=title, summary=summary, trigger=trigger, steps=steps)
    return AutoSkillValidationPlan(
        status="ready",
        skill_source=AUTO_SKILL_SOURCE,
        skill_files={skill_path: {"content": skill_md, "encoding": "utf-8"}},
        subagents=(
            {
                "name": "auto_skill_validator",
                "description": "Validate proposed Nullion skills against representative workflows before installation.",
                "system_prompt": (
                    "Load the proposed skill, run the representative workflow mentally or with safe read-only tools, "
                    "and report whether the trigger, steps, and expected outcome are coherent."
                ),
            },
        ),
        golden_workflows=(
            {
                "name": "trigger-workflow",
                "prompt": trigger,
                "expected_steps": steps,
                "expected_outcome": summary,
            },
        ),
        checks=(
            "loads_as_deep_agent_skill",
            "has_trigger",
            "has_ordered_steps",
            "golden_workflow_declared",
        ),
    )


def build_auto_skill_validation_snapshot(proposal: Any) -> dict[str, object]:
    plan = build_auto_skill_validation_plan(proposal)
    return {
        "status": plan.status,
        "skill_source": plan.skill_source,
        "skill_files": plan.skill_files,
        "subagents": list(plan.subagents),
        "golden_workflows": list(plan.golden_workflows),
        "checks": list(plan.checks),
    }


def build_auto_skill_validation_task(
    proposal: Any,
    *,
    group_id: str,
    conversation_id: str,
    principal_id: str,
):
    from nullion.task_queue import TaskPriority, TaskRecord, TaskStatus, make_task_id

    plan = build_auto_skill_validation_plan(proposal)
    workflow = plan.golden_workflows[0] if plan.golden_workflows else {}
    task = TaskRecord(
        task_id=make_task_id(),
        group_id=group_id,
        conversation_id=conversation_id,
        principal_id=principal_id,
        title=f"Validate skill: {getattr(proposal, 'title', 'Auto Skill')}",
        description=(
            "Validate this proposed Nullion skill using the provided Deep Agents skill source and golden workflow. "
            f"Trigger: {workflow.get('prompt', '')}. "
            f"Expected outcome: {workflow.get('expected_outcome', '')}. "
            f"Expected steps: {', '.join(str(step) for step in workflow.get('expected_steps', []) or [])}."
        ),
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        allowed_tools=[],
        dependencies=[],
    )
    task.deep_agent_skills = [plan.skill_source]
    task.deep_agent_skill_files = dict(plan.skill_files)
    task.deep_agent_subagents = list(plan.subagents)
    return task


async def run_auto_skill_validation(
    proposal: Any,
    *,
    model_client: Any,
    tool_registry: Any,
    policy_store: Any,
    approval_store: Any,
    context_bus: Any,
    progress_queue: Any,
    group_id: str,
    conversation_id: str,
    principal_id: str,
    agent_id: str = "auto-skill-validator",
    runner: Any = None,
):
    from nullion.mini_agent_runner import MiniAgentConfig, MiniAgentRunner

    task = build_auto_skill_validation_task(
        proposal,
        group_id=group_id,
        conversation_id=conversation_id,
        principal_id=principal_id,
    )
    effective_runner = runner or MiniAgentRunner()
    return await effective_runner.run(
        MiniAgentConfig(agent_id=agent_id, task=task),
        anthropic_client=model_client,
        tool_registry=tool_registry,
        policy_store=policy_store,
        approval_store=approval_store,
        context_bus=context_bus,
        progress_queue=progress_queue,
    )


def _skill_markdown(*, slug: str, title: str, summary: str, trigger: str, steps: list[str]) -> str:
    lines = [
        "---",
        f"name: {slug}",
        f"description: {summary or title}",
        "---",
        "",
        f"# {title}",
        "",
        "## Trigger",
        trigger or title,
        "",
        "## Steps",
    ]
    lines.extend(f"{index}. {step}" for index, step in enumerate(steps, start=1))
    return "\n".join(lines) + "\n"


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return slug[:64] or "auto-skill"


__all__ = [
    "AUTO_SKILL_SOURCE",
    "AutoSkillValidationPlan",
    "build_auto_skill_validation_plan",
    "build_auto_skill_validation_snapshot",
    "build_auto_skill_validation_task",
    "run_auto_skill_validation",
]

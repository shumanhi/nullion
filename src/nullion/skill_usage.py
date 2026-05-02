"""Shared learned-skill usage hints for chat surfaces."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

LEARNED_SKILL_INJECT_MIN_SCORE = 4
DEFAULT_LEARNED_SKILL_USAGE_LIMIT = 3

@dataclass(frozen=True, slots=True)
class LearnedSkillUsageHint:
    title: str
    titles: tuple[str, ...]
    prompt: str


class _SkillUsageState(TypedDict, total=False):
    skill_id: str
    explicitly_selected: bool
    should_include: bool


def _skill_usage_selection_node(state: _SkillUsageState) -> dict[str, object]:
    return {
        "should_include": bool(state.get("explicitly_selected") and state.get("skill_id")),
    }


@lru_cache(maxsize=1)
def _compiled_skill_usage_graph():
    graph = StateGraph(_SkillUsageState)
    graph.add_node("selection", _skill_usage_selection_node)
    graph.add_edge(START, "selection")
    graph.add_edge("selection", END)
    return graph.compile()


def _skill_usage_decision(skill_id: str) -> bool:
    final_state = _compiled_skill_usage_graph().invoke(
        {"skill_id": skill_id, "explicitly_selected": True},
        config={"configurable": {"thread_id": "learned-skill-usage"}},
    )
    return bool(final_state.get("should_include"))


def build_learned_skill_usage_hint(
    store,
    user_message: str,
    *,
    skill_ids: Iterable[str] | None = None,
    min_score: int = LEARNED_SKILL_INJECT_MIN_SCORE,
    limit: int | None = DEFAULT_LEARNED_SKILL_USAGE_LIMIT,
) -> LearnedSkillUsageHint | None:
    """Build a learned-skill prompt only from explicit structured skill IDs.

    Free-form user prompts are intentionally not token-matched here. Callers that
    want a learned skill to influence routing must pass IDs selected by a typed
    UI action, command, runtime plan, or another verified structured signal.
    """
    if not skill_ids:
        return None
    stored_skills = getattr(store, "skills", None)
    if not isinstance(stored_skills, dict):
        return None
    selected = []
    seen: set[str] = set()
    for skill_id in skill_ids:
        normalized_id = str(skill_id or "").strip()
        if not normalized_id or normalized_id in seen:
            continue
        skill = stored_skills.get(normalized_id)
        if skill is None:
            continue
        if not _skill_usage_decision(normalized_id):
            continue
        selected.append(skill)
        seen.add(normalized_id)
        if limit is not None and len(selected) >= limit:
            break
    if not selected:
        return None
    prompt_sections: list[str] = ["Relevant learned skills:"]
    for skill_index, skill in enumerate(selected, start=1):
        steps_text = "\n".join(f"    {index + 1}. {step}" for index, step in enumerate(skill.steps))
        prompt_sections.append(
            f"{skill_index}. {skill.title}\n"
            f"   Trigger: {skill.trigger}\n"
            f"   Follow these steps:\n{steps_text}"
        )
    titles = tuple(skill.title for skill in selected)
    return LearnedSkillUsageHint(
        title=titles[0],
        titles=titles,
        prompt=(
            "\n".join(prompt_sections)
            + "\n"
            f"Adapt as needed for the user's specific request."
        ),
    )


__all__ = [
    "DEFAULT_LEARNED_SKILL_USAGE_LIMIT",
    "LEARNED_SKILL_INJECT_MIN_SCORE",
    "LearnedSkillUsageHint",
    "build_learned_skill_usage_hint",
]

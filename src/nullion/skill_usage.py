"""Shared learned-skill usage hints for chat surfaces."""

from __future__ import annotations

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
    user_message: str
    skill_text: str
    message_requests_artifact: bool
    skill_produces_artifact: bool
    should_include: bool


def _skill_usage_artifact_signal_node(state: _SkillUsageState) -> dict[str, object]:
    return {
        "message_requests_artifact": False,
        "skill_produces_artifact": False,
    }


def _skill_usage_decision_node(state: _SkillUsageState) -> dict[str, object]:
    return {
        "should_include": bool(
            state.get("message_requests_artifact") or not state.get("skill_produces_artifact")
        )
    }


@lru_cache(maxsize=1)
def _compiled_skill_usage_graph():
    graph = StateGraph(_SkillUsageState)
    graph.add_node("artifact_signals", _skill_usage_artifact_signal_node)
    graph.add_node("decision", _skill_usage_decision_node)
    graph.add_edge(START, "artifact_signals")
    graph.add_edge("artifact_signals", "decision")
    graph.add_edge("decision", END)
    return graph.compile()


def _skill_usage_decision(user_message: str, skill) -> bool:
    skill_text = " ".join(
        [
            str(getattr(skill, "title", "") or ""),
            str(getattr(skill, "trigger", "") or ""),
            str(getattr(skill, "summary", "") or ""),
            " ".join(str(step or "") for step in getattr(skill, "steps", ()) or ()),
        ]
    )
    final_state = _compiled_skill_usage_graph().invoke(
        {"user_message": user_message or "", "skill_text": skill_text},
        config={"configurable": {"thread_id": "learned-skill-usage"}},
    )
    return bool(final_state.get("should_include"))


def build_learned_skill_usage_hint(
    store,
    user_message: str,
    *,
    min_score: int = LEARNED_SKILL_INJECT_MIN_SCORE,
    limit: int | None = DEFAULT_LEARNED_SKILL_USAGE_LIMIT,
) -> LearnedSkillUsageHint | None:
    from nullion.runtime import _recommend_skills_with_scores

    stored_skills = getattr(store, "skills", None)
    recommendation_limit = len(stored_skills) if limit is None and stored_skills is not None else limit
    if recommendation_limit is None:
        recommendation_limit = 1000
    scored = [
        (score, skill)
        for score, skill in _recommend_skills_with_scores(store, user_message, limit=recommendation_limit)
        if score >= min_score and _skill_usage_decision(user_message, skill)
    ]
    if not scored:
        return None
    prompt_sections: list[str] = ["Relevant learned skills:"]
    for skill_index, (_score, skill) in enumerate(scored, start=1):
        steps_text = "\n".join(f"    {index + 1}. {step}" for index, step in enumerate(skill.steps))
        prompt_sections.append(
            f"{skill_index}. {skill.title}\n"
            f"   Trigger: {skill.trigger}\n"
            f"   Follow these steps:\n{steps_text}"
        )
    titles = tuple(skill.title for _score, skill in scored)
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

from __future__ import annotations

from types import SimpleNamespace

from nullion.skills import SkillRecord
from nullion.skill_usage import build_learned_skill_usage_hint


def test_artifact_skill_hint_is_suppressed_without_file_request(monkeypatch) -> None:
    from nullion import runtime

    skill = SkillRecord(
        skill_id="news-pdf",
        title="Create news PDF with images",
        summary="Create a PDF artifact from news results.",
        trigger="news pdf with images",
        steps=["Search for news.", "Write a PDF file."],
    )
    monkeypatch.setattr(runtime, "_recommend_skills_with_scores", lambda store, text, limit=3: [(10, skill)])

    hint = build_learned_skill_usage_hint(SimpleNamespace(skills={"news-pdf": skill}), "Can u get me today's news?")

    assert hint is None


def test_artifact_skill_hint_is_allowed_when_user_requests_file(monkeypatch) -> None:
    from nullion import runtime

    skill = SkillRecord(
        skill_id="news-pdf",
        title="Create news PDF with images",
        summary="Create a PDF artifact from news results.",
        trigger="news pdf with images",
        steps=["Search for news.", "Write a PDF file."],
    )
    monkeypatch.setattr(runtime, "_recommend_skills_with_scores", lambda store, text, limit=3: [(10, skill)])

    hint = build_learned_skill_usage_hint(
        SimpleNamespace(skills={"news-pdf": skill}),
        "Can u get me today's news in a PDF with images?",
    )

    assert hint is not None
    assert hint.titles == ("Create news PDF with images",)

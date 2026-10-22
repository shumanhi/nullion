from __future__ import annotations

from types import SimpleNamespace

from nullion.skills import SKILL_WRITE_DELETE_CONSENT_STEP, SkillRecord
from nullion.skill_usage import build_learned_skill_usage_hint


def test_skill_hint_uses_recommendation_score_without_file_request_words(monkeypatch) -> None:
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
        "Can u get me today's news?",
        skill_ids=("news-pdf",),
    )

    assert hint is not None


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
        skill_ids=("news-pdf",),
    )

    assert hint is not None
    assert hint.titles == ("Create news PDF with images",)


def test_reused_skill_hint_requires_explicit_consent_for_write_or_delete(monkeypatch) -> None:
    from nullion import runtime

    skill = SkillRecord(
        skill_id="drive-delete",
        title="Delete Drive document",
        summary="Delete a Google Drive document.",
        trigger="Drive document deletion.",
        steps=["Find the document.", "Delete the document."],
    )
    monkeypatch.setattr(runtime, "_recommend_skills_with_scores", lambda store, text, limit=3: [(10, skill)])

    hint = build_learned_skill_usage_hint(
        SimpleNamespace(skills={"drive-delete": skill}),
        "Clean up this Drive document.",
        skill_ids=("drive-delete",),
    )

    assert hint is not None
    assert f"Consent guard: {SKILL_WRITE_DELETE_CONSENT_STEP}" in hint.prompt
    assert "explicit confirmation in chat" in hint.prompt
    assert "accepting or reusing this skill is not consent" in hint.prompt

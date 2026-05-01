from __future__ import annotations

from nullion.attachment_format_graph import plan_attachment_format


def test_attachment_format_graph_plans_explicit_formats() -> None:
    text = plan_attachment_format("send as text file")
    word = plan_attachment_format("make a word document")
    missing = plan_attachment_format("send it")

    assert text.extension == ".txt"
    assert text.evidence == "text_file_phrase"
    assert word.extension == ".docx"
    assert missing.extension is None

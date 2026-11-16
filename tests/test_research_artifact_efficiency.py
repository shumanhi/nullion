import json
from copy import deepcopy

import pytest

from nullion.tools import ToolResult


def test_compact_program_findings_survive_optional_native_inventory():
    from nullion.code_mode import compact_output
    operations = [{"invocation_id": str(index), "tool_name": "source_read", "status": "completed",
                   "operation_kind": "read", "reused": False} for index in range(13)]
    value = {"findings": [{"date": "2026-11-21", "region": "US", "source_url": "https://source.test/a"}],
             "unresolved_questions": []}
    raw = {"value": value, "operations": operations, "reads": deepcopy(operations),
           "native_observations": [{"output": {"body": "x" * 9000}}]}
    before = deepcopy(raw)
    for _ in range(3):
        compact = compact_output(raw, max_chars=8000)
        assert compact["value"] == value
        assert compact["operations"] == operations
        assert len(json.dumps(compact)) < 8000
    assert raw == before


def test_adapter_calendar_projection_keeps_schedule_and_exact_identity():
    from nullion.code_mode import _default_observations
    from nullion.tools import _calendar_record_collections
    link = "https://source.test/calendar?opaque=" + "q" * 650
    events = [{"id": str(index), "htmlLink": link, "summary": "予定" + str(index),
               "start": {"dateTime": "2026-11-21T10:53:00-05:00"},
               "end": {"dateTime": "2026-11-21T12:58:00-05:00"},
               "description": "long optional notes " * 1000} for index in range(7)]
    output = {"results": events, "source_record_collections": _calendar_record_collections()}
    view = _default_observations([ToolResult("calendar", "calendar_list", "completed", output)], max_chars=10000)
    table = view["observations"][0]["output"]["collection_views"][0]
    assert len(table["rows"]) == 7
    assert all(row[table["columns"].index("htmlLink")] == link for row in table["rows"])
    assert "2026-11-21T10:53:00-05:00" in json.dumps(table)
    assert events[0]["description"] == "long optional notes " * 1000


@pytest.mark.parametrize("extension,producer", [(".pdf", "pdf_create"), (".docx", "document_create"),
                                               (".pptx", "presentation_create"), (".xlsx", "spreadsheet_create")])
def test_inspection_cannot_revoke_a_producers_embedded_media_receipt(tmp_path, extension, producer):
    from nullion.chat_operator import _completed_artifact_extensions_without_embedded_media
    path = str(tmp_path / ("result" + extension)); image = str(tmp_path / "art.png")
    produced = ToolResult("created", producer, "completed", {"path": path,
        "embedded_html_images": [{"path": image}]})
    inspected = ToolResult("inspected", "file_read", "completed", {"path": path, "content": "Readable text"})
    assert _completed_artifact_extensions_without_embedded_media([produced, inspected], required_image_paths=(image,)) == set()
    # A real replacement without media is still a missing-media receipt.
    overwritten = ToolResult("replaced", producer, "completed", {"path": path})
    assert extension in _completed_artifact_extensions_without_embedded_media([produced, overwritten], required_image_paths=(image,))
    assert _completed_artifact_extensions_without_embedded_media([overwritten, produced], required_image_paths=(image,)) == set()
    from nullion.chat_operator import _completed_artifact_paths_without_current_media
    assert _completed_artifact_paths_without_current_media([overwritten, produced, inspected], required_image_paths=(image,)) == set()


def test_pdf_pixels_are_bound_to_current_pdf_and_preview_bytes(tmp_path):
    from pypdf import PdfWriter
    from nullion.pdf_preview import render_pdf_previews
    from nullion.agent_orchestrator import _tool_result_visual_reference_blocks
    path = tmp_path / "report.pdf"
    writer = PdfWriter(); writer.add_blank_page(width=400, height=300)
    with path.open("wb") as stream: writer.write(stream)
    metadata = render_pdf_previews(path)
    assert metadata["preview_page_count"] == 1 and not metadata["visual_content_verified"]
    result = ToolResult("pdf", "pdf_create", "completed", {"path": str(path), **metadata})
    assert any(block["type"] == "image" for block in _tool_result_visual_reference_blocks(result))
    preview = tmp_path / ".nullion-pdf-previews" / (metadata["pdf_sha256"] + "-page-1.jpg")
    preview.write_bytes(b"tampered")
    assert not _tool_result_visual_reference_blocks(result)
    path.write_bytes(b"changed PDF")
    assert not _tool_result_visual_reference_blocks(result)


def test_card_pages_validate_and_escape_content_without_changing_facts():
    from nullion.pdf_design import render_card_pages, card_pages_schema
    from pydantic import ValidationError
    pages = render_card_pages([{"title": "旅 <37>", "cards": [{"heading": "固定", "text": "10:53 AM\n<script>data</script>"}]}])
    assert "旅 &lt;37&gt;" in pages[0] and "10:53 AM" in pages[0]
    assert "<script>" not in pages[0] and "&lt;script&gt;data&lt;/script&gt;" in pages[0]
    assert "$ref" not in json.dumps(card_pages_schema())
    with pytest.raises(ValidationError):
        render_card_pages([{"title": "Report", "cards": [{"heading": "Label", "text": "Data", "wide": "yes"}]}])
    illustrated = render_card_pages([{"title": "Report", "cards": [{"heading": "Label", "text": "Data"}]}], image_paths=["/art/photo.png"])[0]
    assert 'class="n-card-cover"><img src="/art/photo.png"' in illustrated


@pytest.mark.parametrize("caption", ["📅", "---", "資料を添付しました。", "الملف جاهز"])
def test_shared_attachment_caption_keeps_readable_unicode_content(tmp_path, monkeypatch, caption):
    from pypdf import PdfWriter
    from nullion.artifacts import artifact_root_for_principal
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    principal = "web:caption-contract"
    root = artifact_root_for_principal(principal); root.mkdir(parents=True, exist_ok=True)
    path = root / "report.pdf"
    writer = PdfWriter(); writer.add_blank_page(width=400, height=300)
    with path.open("wb") as stream: writer.write(stream)
    delivery = prepare_reply_for_platform_delivery(caption + "\n\nMEDIA:" + str(path), principal_id=principal)
    assert delivery.attachments == (path,)
    assert any(c.isalnum() for c in delivery.text)
    if any(c.isalnum() for c in caption):
        assert caption in delivery.text


def test_native_pdf_cards_are_visible_in_compact_tool_metadata(tmp_path):
    from nullion.tools import build_default_tool_registry
    registry = build_default_tool_registry(workspace_root=tmp_path)
    assert "card_pages" in registry.get_spec("pdf_create").description[:200]
    definition = next(t for t in registry.list_tool_definitions() if t["name"] == "pdf_create")
    assert "card_pages" in definition["input_schema"]["properties"]


@pytest.mark.parametrize('arguments', [{'card_pages': []}, {'card_pages': [{}], 'text_pages': ['body']}])
def test_invalid_card_arguments_are_repairable_before_rendering(tmp_path, arguments):
    from nullion.tools import _build_pdf_create_handler, ToolInvocation
    from nullion.tool_repair_contract import tool_repair_kind
    result = _build_pdf_create_handler(tmp_path)(ToolInvocation('bad-cards', 'pdf_create', 'web:cards', arguments))
    assert result.status == 'failed' and result.output['executed'] is False
    assert tool_repair_kind(result) == 'correct_arguments'


@pytest.mark.parametrize("pointer,column", [("/documents", "url"), ("/details", "source_page_url"), ("/items", "source_page_url")])
def test_live_assertion_reads_exact_host_owned_page_identity(pointer, column):
    from tests.e2e.test_live_stage_code_mode import _completed_native_source_urls
    source = "https://source.test/exact?opaque=37"
    observed = {"tool_name": "code_mode", "status": "completed", "output": {
        "value": {"source_url": "https://invented.test/"},
        "native_observations": [{"tool_name": "browser_read_pages", "status": "completed", "output": {
            "collection_views": [{"json_pointer": pointer, "columns": [column], "rows": [[source]]}]}}]}}
    assert _completed_native_source_urls([observed]) == [source]

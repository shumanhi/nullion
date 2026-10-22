from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_probe_module():
    script = Path(__file__).resolve().parents[1] / "scripts" / "live_stage_chat_contract_probe.py"
    spec = importlib.util.spec_from_file_location("live_stage_chat_contract_probe", script)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_live_stage_artifact_visual_validation_requires_visible_text(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1><p>Readable artifact content.</p></body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
    )

    downloaded = probe._download_and_check_artifacts(
        "http://127.0.0.1:8753",
        [{"name": html.name, "path": str(html)}],
        scenario=scenario,
        timeout=1,
    )

    assert downloaded[0]["visible_text_chars"] >= len("STAGE VISUAL QA")


def test_live_stage_artifact_visual_validation_requires_each_artifact_and_unavailable_disclosure(
    tmp_path: Path,
) -> None:
    probe = _load_probe_module()
    first = tmp_path / "first.html"
    second = tmp_path / "second.html"
    first.write_text(
        "<!doctype html><html><body><h1>PACK QA</h1><p>Calendar status: unavailable. "
        "This readable briefing includes enough supporting detail for visual validation.</p></body></html>",
        encoding="utf-8",
    )
    second.write_text(
        "<!doctype html><html><body><h1>PACK QA</h1><p>Calendar summary omitted. "
        "This readable briefing includes enough supporting detail for visual validation.</p></body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="per_artifact_source_disclosure",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_each=("PACK QA",),
        required_artifact_text_if_tool_unavailable=(("calendar_list", "Calendar status: unavailable"),),
    )

    with pytest.raises(AssertionError, match="structured unavailable source"):
        probe._download_and_check_artifacts(
            "http://127.0.0.1:8753",
            [
                {"name": first.name, "path": str(first)},
                {"name": second.name, "path": str(second)},
            ],
            scenario=scenario,
            timeout=1,
            structured_unavailable_tools={"calendar_list"},
        )

    second.write_text(
        "<!doctype html><html><body><h1>PACK QA</h1><p>Calendar status: unavailable. "
        "This readable briefing includes enough supporting detail for visual validation.</p></body></html>",
        encoding="utf-8",
    )
    downloaded = probe._download_and_check_artifacts(
        "http://127.0.0.1:8753",
        [
            {"name": first.name, "path": str(first)},
            {"name": second.name, "path": str(second)},
        ],
        scenario=scenario,
        timeout=1,
        structured_unavailable_tools={"calendar_list"},
    )

    assert len(downloaded) == 2


def test_live_stage_artifact_visual_validation_requires_visual_assets_when_configured(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1>"
        "<p>Readable artifact content.</p><svg viewBox='0 0 10 10'><circle cx='5' cy='5' r='4'/></svg>"
        "</body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
    )

    downloaded = probe._download_and_check_artifacts(
        "http://127.0.0.1:8753",
        [{"name": html.name, "path": str(html)}],
        scenario=scenario,
        timeout=1,
    )

    assert downloaded[0]["visual_asset_count"] >= 1


def test_live_stage_artifact_visual_validation_rejects_svg_only_html_when_raster_required(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1>"
        "<p>Readable artifact content with source and itinerary details.</p>"
        "<svg viewBox='0 0 10 10'><circle cx='5' cy='5' r='4'/></svg>"
        "</body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
        require_raster_html_images=True,
    )

    with pytest.raises(AssertionError, match="did not embed any raster data-image assets"):
        probe._download_and_check_artifacts(
            "http://127.0.0.1:8753",
            [{"name": html.name, "path": str(html)}],
            scenario=scenario,
            timeout=1,
        )


def test_live_stage_artifact_visual_validation_accepts_embedded_raster_html_image(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1>"
        "<p>Readable artifact content with source and itinerary details.</p>"
        "<img alt='source visual' src='data:image/png;base64,iVBORw0KGgo='>"
        "</body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
        require_raster_html_images=True,
    )

    downloaded = probe._download_and_check_artifacts(
        "http://127.0.0.1:8753",
        [{"name": html.name, "path": str(html)}],
        scenario=scenario,
        timeout=1,
    )

    assert downloaded[0]["visual_asset_count"] >= 1


def test_live_stage_artifact_visual_validation_allows_placeholder_word_in_non_visual_prose(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1>"
        "<p>Readable artifact content with source and itinerary details.</p>"
        "<img alt='source visual' src='data:image/png;base64,iVBORw0KGgo='>"
        "<footer>Images are embedded from source raster files; no SVG placeholders are used.</footer>"
        "</body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
        require_raster_html_images=True,
    )

    downloaded = probe._download_and_check_artifacts(
        "http://127.0.0.1:8753",
        [{"name": html.name, "path": str(html)}],
        scenario=scenario,
        timeout=1,
    )

    assert downloaded[0]["visual_asset_count"] >= 1


def test_live_stage_artifact_visual_validation_ignores_placeholder_terms_inside_embedded_data_url(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1>"
        "<p>Readable artifact content with source and itinerary details.</p>"
        "<img alt='source visual' src='data:image/png;base64,AAAAtodoAAAA'>"
        "</body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
        require_raster_html_images=True,
    )

    downloaded = probe._download_and_check_artifacts(
        "http://127.0.0.1:8753",
        [{"name": html.name, "path": str(html)}],
        scenario=scenario,
        timeout=1,
    )

    assert downloaded[0]["visual_asset_count"] >= 1


def test_live_stage_artifact_visual_validation_rejects_placeholder_image_attributes(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1>"
        "<p>Readable artifact content with source and itinerary details.</p>"
        "<img alt='placeholder visual' src='data:image/png;base64,iVBORw0KGgo='>"
        "</body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
        require_raster_html_images=True,
    )

    with pytest.raises(AssertionError, match="placeholder visual content"):
        probe._download_and_check_artifacts(
            "http://127.0.0.1:8753",
            [{"name": html.name, "path": str(html)}],
            scenario=scenario,
            timeout=1,
        )


def test_live_stage_artifact_visual_validation_requires_minimum_office_media_count(tmp_path: Path) -> None:
    probe = _load_probe_module()
    openpyxl = pytest.importorskip("openpyxl")
    pil_image = pytest.importorskip("PIL.Image")
    xl_image = pytest.importorskip("openpyxl.drawing.image")

    workbook_path = tmp_path / "stage_visual.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Name", "Image"])
    for index in range(2):
        image_path = tmp_path / f"source-{index}.png"
        pil_image.new("RGB", (16, 16), color=(20 + index, 90, 160)).save(image_path)
        ws.append([f"STAGE VISUAL QA {index}", None])
        ws.add_image(xl_image.Image(str(image_path)), f"B{index + 2}")
    wb.save(workbook_path)

    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".xlsx",),
        required_embedded_media_extensions=(".xlsx",),
        min_embedded_media_count=3,
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
    )

    with pytest.raises(AssertionError, match="expected at least 3"):
        probe._download_and_check_artifacts(
            "http://127.0.0.1:8753",
            [{"name": workbook_path.name, "path": str(workbook_path)}],
            scenario=scenario,
            timeout=1,
        )

    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".xlsx",),
        required_embedded_media_extensions=(".xlsx",),
        min_embedded_media_count=2,
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
    )

    downloaded = probe._download_and_check_artifacts(
        "http://127.0.0.1:8753",
        [{"name": workbook_path.name, "path": str(workbook_path)}],
        scenario=scenario,
        timeout=1,
    )

    assert downloaded[0]["visual_asset_count"] >= 2


def test_live_stage_artifact_visual_validation_rejects_missing_visual_assets(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>STAGE VISUAL QA</h1><p>Readable artifact content.</p></body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
        require_visual_assets=True,
    )

    with pytest.raises(AssertionError, match="expected embedded images or visual assets"):
        probe._download_and_check_artifacts(
            "http://127.0.0.1:8753",
            [{"name": html.name, "path": str(html)}],
            scenario=scenario,
            timeout=1,
        )


def test_live_stage_artifact_visual_validation_rejects_filename_only_receipt(tmp_path: Path) -> None:
    probe = _load_probe_module()
    missing = tmp_path / "stage_missing.html"
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
    )

    with pytest.raises(AssertionError, match="local path does not exist"):
        probe._download_and_check_artifacts(
            "http://127.0.0.1:8753",
            [{"name": missing.name, "path": str(missing)}],
            scenario=scenario,
            timeout=1,
        )


def test_live_stage_artifact_visual_validation_rejects_missing_artifact_phrase(tmp_path: Path) -> None:
    probe = _load_probe_module()
    html = tmp_path / "stage_visual.html"
    html.write_text(
        "<!doctype html><html><body><h1>Different heading</h1><p>Readable artifact content.</p></body></html>",
        encoding="utf-8",
    )
    scenario = probe.Scenario(
        name="visual_contract",
        prompt="",
        max_average_ms=None,
        max_single_ms=None,
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("STAGE VISUAL QA",),
    )

    with pytest.raises(AssertionError, match="missing required text inside downloaded artifacts"):
        probe._download_and_check_artifacts(
            "http://127.0.0.1:8753",
            [{"name": html.name, "path": str(html)}],
            scenario=scenario,
            timeout=1,
        )

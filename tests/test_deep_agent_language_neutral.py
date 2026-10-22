from dataclasses import dataclass
import json
from types import SimpleNamespace
import zipfile

from nullion.deep_agent_profiles import deep_agent_profile_names_for_task
from nullion.deep_agent_runner import (
    _artifact_verification_failure_for_task,
    _artifact_verification_success_output_for_task,
    _artifact_content_obligations_from_context,
    _browser_item_records_from_context,
    _context_out_for_task,
    _structured_row_count_contract_from_context,
    _task_requires_user_file_delivery,
)
from nullion.tools import ToolResult


@dataclass
class Task:
    title: str = ""
    description: str = ""
    finish: object | None = None
    output: object | None = None
    metadata: dict[str, object] | None = None
    allowed_tools: list[str] | None = None


def test_deep_agent_delivery_requirement_ignores_prompt_words():
    task = Task(title="send report", description="create pdf file")

    assert not _task_requires_user_file_delivery(task)


def test_deep_agent_delivery_requirement_uses_structured_metadata():
    task = Task(metadata={"requires_artifact_delivery": True, "required_artifact_kind": "pdf"})

    assert _task_requires_user_file_delivery(task)


def test_artifact_profile_does_not_attach_verifier_to_generator_task():
    task = Task(
        allowed_tools=["pdf_create"],
        metadata={"requires_artifact_delivery": True, "required_artifact_kind": "pdf"},
    )

    assert deep_agent_profile_names_for_task(task) == ["artifact"]


def test_artifact_verifier_profile_uses_structured_task_role():
    task = Task(
        allowed_tools=["file_read"],
        metadata={"artifact_role": "verify", "requires_artifact_delivery": True},
    )

    assert deep_agent_profile_names_for_task(task) == ["artifact_verifier", "repo_analysis"]


def test_context_output_carries_structured_workspace_file_paths(tmp_path):
    image = tmp_path / "alpha.png"
    image.write_bytes(b"png")
    manifest = tmp_path / "manifest.json"
    manifest.write_text("[]", encoding="utf-8")
    config = SimpleNamespace(task=SimpleNamespace(task_id="task-extract", group_id="grp-context"))

    extracted_context = _context_out_for_task(
        config,
        output_text="Extracted files.",
        deliverable_artifacts=[],
        tool_results=[
            ToolResult(
                "extract",
                "archive_extract",
                "completed",
                {
                    "entries": [{"name": "alpha.png", "path": str(image), "bytes": 3, "media_type": "image/png"}],
                    "manifest_json_path": str(manifest),
                },
            )
        ],
    )

    assert str(image) in extracted_context["workspace_file_paths"]
    assert str(manifest) in extracted_context["workspace_file_paths"]

    inspected_context = _context_out_for_task(
        config,
        output_text="Prepared rows.",
        deliverable_artifacts=[],
        tool_results=[],
        context_in=extracted_context,
    )

    assert str(image) in inspected_context["workspace_file_paths"]
    assert str(manifest) in inspected_context["workspace_file_paths"]


def test_artifact_verifier_accepts_typed_context_when_no_observation_tools(tmp_path, monkeypatch):
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    workbook = artifact_root / "current-run.xlsx"
    workbook.write_bytes(b"workbook")
    monkeypatch.setattr("nullion.deep_agent_runner.artifact_root_for_principal", lambda principal_id: artifact_root)
    task = SimpleNamespace(
        principal_id="principal",
        allowed_tools=[],
        metadata={"artifact_role": "verify", "requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )

    failure = _artifact_verification_failure_for_task(
        SimpleNamespace(task=task),
        context_in={"artifact_paths": [str(workbook)]},
        output_text="Verified.",
        tool_results=[],
        deliverable_artifacts=[],
    )

    assert failure is None

    success_text = _artifact_verification_success_output_for_task(
        SimpleNamespace(task=task),
        context_in={"artifact_paths": [str(workbook)]},
    )

    assert success_text == "Verified current-run artifact: current-run.xlsx"


def test_artifact_verifier_rejects_blank_spreadsheet_when_structured_context_has_records(tmp_path, monkeypatch):
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    workbook = artifact_root / "current-run.xlsx"
    with zipfile.ZipFile(workbook, "w") as package:
        package.writestr("xl/worksheets/sheet1.xml", "<worksheet><sheetData><row r='1'/></sheetData></worksheet>")
    monkeypatch.setattr("nullion.deep_agent_runner.artifact_root_for_principal", lambda principal_id: artifact_root)
    task = SimpleNamespace(
        principal_id="principal",
        allowed_tools=[],
        metadata={"artifact_role": "verify", "requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )

    failure = _artifact_verification_failure_for_task(
        SimpleNamespace(task=task),
        context_in={
            "artifact_paths": [str(workbook)],
            "records": [
                {
                    "title": "Structured record 001",
                    "url": "https://example.test/items/001",
                    "image_url": "https://example.test/assets/001.png",
                    "source_fields": {"record_id": "R-001", "status_value": "ready"},
                }
            ],
        },
        output_text="Verified.",
        tool_results=[],
        deliverable_artifacts=[],
    )

    assert failure is not None
    assert "structured records" in failure


def test_structured_handoff_reads_markdown_tables_without_domain_fields():
    context = {
        "output": "\n".join(
            [
                "| Label | Location | State |",
                "|---|---|---|",
                "| Alpha | https://example.test/a | Ready |",
                "| Beta | https://example.test/b | Waiting |",
            ]
        )
    }

    records = _browser_item_records_from_context(context)

    assert [record["title"] for record in records] == ["Alpha", "Beta"]
    assert [record["url"] for record in records] == ["https://example.test/a", "https://example.test/b"]
    assert records[0]["extra_fields"]["State"] == "Ready"


def test_structured_handoff_prefers_direct_table_rows_and_enriches_from_sidecar(tmp_path):
    sidecar = tmp_path / "records.json"
    sidecar.write_text(
        json.dumps(
            {
                "schema": "nullion.context_handoff.structured_records.v1",
                "records": [
                    {
                        "title": "Alpha alternate view",
                        "url": "https://example.test/a",
                        "image_url": "https://example.test/assets/a.png",
                        "source_fields": {"rating_value": "ready"},
                    },
                    {
                        "title": "Gamma extra record",
                        "url": "https://example.test/c",
                        "image_url": "https://example.test/assets/c.png",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    context = {
        "output": "\n".join(
            [
                "| Row | Display | Target |",
                "|---:|---|---|",
                "| 1 | Alpha | https://example.test/a |",
                "| 2 | Beta | https://example.test/b |",
            ]
        ),
        "workspace_files": [{"path": str(sidecar)}],
    }

    records = _browser_item_records_from_context(context)

    assert [record["title"] for record in records] == ["Alpha", "Beta"]
    assert [record["url"] for record in records] == ["https://example.test/a", "https://example.test/b"]
    assert records[0]["image_url"] == "https://example.test/assets/a.png"
    assert records[0]["extra_fields"]["Rating Value"] == "ready"
    assert _structured_row_count_contract_from_context(context) == 2


def test_structured_handoff_prefers_larger_file_records_over_shallow_context_summary(tmp_path):
    sidecar = tmp_path / "records.json"
    sidecar.write_text(
        json.dumps(
            {
                "schema": "nullion.context_handoff.structured_records.v1",
                "records": [
                    {
                        "title": f"Asset {index:03d}",
                        "url": f"https://example.test/assets/{index:03d}",
                        "image_url": f"https://example.test/media/{index:03d}.png",
                        "source_fields": {"status_value": "ready"},
                    }
                    for index in range(1, 4)
                ],
            }
        ),
        encoding="utf-8",
    )
    context = {
        "schema": "nullion.dependency_context.v1",
        "dependencies": [
            {
                "output": "Summary: collection data was found at https://example.test/source.json.",
            }
        ],
        "workspace_files": [{"path": str(sidecar)}],
    }

    records = _browser_item_records_from_context(context)

    assert [record["title"] for record in records] == ["Asset 001", "Asset 002", "Asset 003"]
    assert all(record["image_url"].startswith("https://example.test/media/") for record in records)


def test_structured_handoff_dedupes_repeated_url_observations():
    context = {
        "records": [
            {
                "title": "Alpha from first view",
                "url": "https://example.test/a",
                "image_url": "https://example.test/a-1.png",
            },
            {
                "title": "Alpha from alternate view",
                "url": "https://example.test/a",
                "image_url": "https://example.test/a-2.png",
            },
        ]
    }

    records = _browser_item_records_from_context(context)
    obligations = _artifact_content_obligations_from_context(context)

    assert len(records) == 1
    assert obligations["spreadsheet_row_floor"] == 1


def test_artifact_verifier_requires_observed_path_when_observation_tools_available(tmp_path, monkeypatch):
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    workbook = artifact_root / "current-run.xlsx"
    workbook.write_bytes(b"workbook")
    monkeypatch.setattr("nullion.deep_agent_runner.artifact_root_for_principal", lambda principal_id: artifact_root)
    task = SimpleNamespace(
        principal_id="principal",
        allowed_tools=["file_read"],
        metadata={"artifact_role": "verify", "requires_artifact_delivery": True, "required_artifact_kind": "xlsx"},
    )

    failure = _artifact_verification_failure_for_task(
        SimpleNamespace(task=task),
        context_in={"artifact_paths": [str(workbook)]},
        output_text="Verified.",
        tool_results=[],
        deliverable_artifacts=[],
    )

    assert failure is not None
    assert "observed no current-run artifact path" in failure

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run Builder capability dependency delivery e2e tests",
)


def _array_schema_paths_missing_items(schema: object, path: tuple[str, ...] = ()) -> list[str]:
    missing: list[str] = []
    if isinstance(schema, dict):
        if schema.get("type") == "array" and "items" not in schema:
            missing.append(".".join(path) or "<root>")
        for key, value in schema.items():
            missing.extend(_array_schema_paths_missing_items(value, (*path, str(key))))
    elif isinstance(schema, list):
        for index, value in enumerate(schema):
            missing.extend(_array_schema_paths_missing_items(value, (*path, str(index))))
    return missing


def _assert_codex_responses_schema_compatible(tools: list[dict[str, object]]) -> list[dict[str, object]]:
    from nullion.model_clients import CodexResponsesModelClient

    converted = CodexResponsesModelClient._to_tools(tools)
    failures: list[str] = []
    for tool in converted:
        name = str(tool.get("name") or "<unnamed>")
        for path in _array_schema_paths_missing_items(tool.get("parameters")):
            failures.append(f"{name}: {path}")
    assert failures == []
    return converted


def _spreadsheet_tool_schema(converted_tools: list[dict[str, object]]) -> dict[str, object]:
    for tool in converted_tools:
        if tool.get("name") == "spreadsheet_create":
            parameters = tool.get("parameters")
            assert isinstance(parameters, dict)
            return parameters
    raise AssertionError("spreadsheet_create was not advertised to the provider")


def _presentation_tool_schema(converted_tools: list[dict[str, object]]) -> dict[str, object]:
    for tool in converted_tools:
        if tool.get("name") == "presentation_create":
            parameters = tool.get("parameters")
            assert isinstance(parameters, dict)
            return parameters
    raise AssertionError("presentation_create was not advertised to the provider")


def test_core_tool_schemas_are_valid_for_codex_responses_requests(tmp_path: Path, monkeypatch) -> None:
    from nullion.tools import create_core_tool_registry

    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    registry = create_core_tool_registry(workspace_root=tmp_path)

    converted = _assert_codex_responses_schema_compatible(registry.list_tool_definitions())
    spreadsheet_schema = _spreadsheet_tool_schema(converted)
    presentation_schema = _presentation_tool_schema(converted)
    row_items = spreadsheet_schema["properties"]["rows"]["items"]
    row_array_branch = row_items["anyOf"][1]

    assert row_array_branch["type"] == "array"
    assert "items" in row_array_branch
    assert presentation_schema["properties"]["slides"]["items"]["properties"]["image_paths"]["items"]["type"] == "string"


def test_websocket_spreadsheet_request_uses_valid_schema_and_delivers_real_xlsx(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import create_core_tool_registry
    from nullion.web_app import create_app
    from openpyxl import load_workbook
    from PIL import Image

    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_WEB_ACCESS_ENABLED", "1")
    monkeypatch.setenv("NULLION_TERMINAL_ENABLED", "1")
    monkeypatch.setenv("NULLION_TASK_DECOMPOSITION_ENABLED", "0")
    monkeypatch.setenv("NULLION_MULTI_AGENT_ENABLED", "0")
    monkeypatch.setenv("NULLION_SKILL_LEARNING_ENABLED", "0")
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspace-storage"))

    image_path = tmp_path / "reference.png"
    Image.new("RGB", (240, 160), color=(255, 0, 0)).save(image_path)
    workbook_path = tmp_path / "downloaded.xlsx"

    class SchemaCheckingSpreadsheetModel:
        calls = 0
        saw_spreadsheet_schema = False
        invoked_spreadsheet_tool = False

        def create(self, *, messages, tools, **kwargs):  # noqa: ANN001, ANN003
            del messages, kwargs
            self.calls += 1
            converted = _assert_codex_responses_schema_compatible(list(tools))
            tool_names = {str(tool.get("name") or "") for tool in converted}
            self.saw_spreadsheet_schema = self.saw_spreadsheet_schema or "spreadsheet_create" in tool_names
            if "spreadsheet_create" not in tool_names:
                return {
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": '{"config_action": null}'}],
                }
            if not self.invoked_spreadsheet_tool:
                self.invoked_spreadsheet_tool = True
                return {
                    "stop_reason": "tool_use",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "spreadsheet-create-1",
                            "name": "spreadsheet_create",
                            "input": {
                                "output_path": "/workspace/artifacts/structured-results.xlsx",
                                "title": "structured results",
                                "sheet_name": "matches",
                                "columns": ["Item", "Link", "Score"],
                                "rows": [
                                    {
                                        "Item": "reference match",
                                        "Link": "https://example.test/item",
                                        "Score": 0.98,
                                        "image_path": str(image_path),
                                    },
                                    ["array row", "https://example.test/array", 0.74],
                                ],
                            },
                        }
                    ],
                }
            return {
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "Created the requested workbook."}],
            }

    model_client = SchemaCheckingSpreadsheetModel()
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = create_core_tool_registry(workspace_root=tmp_path)
    app = create_app(runtime, orchestrator=AgentOrchestrator(model_client=model_client), registry=registry)

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json(
                {
                    "text": "Necesito resultados.xlsx",
                    "conversation_id": "web:e2e",
                    "stream": False,
                    "turn_id": "turn-spreadsheet-schema",
                }
            )
            done: dict[str, object] = {}
            while True:
                event = websocket.receive_json()
                if event.get("type") == "done":
                    done = event
                    break

        assert model_client.saw_spreadsheet_schema is True
        artifacts = done.get("artifacts")
        assert isinstance(artifacts, list) and len(artifacts) == 1
        artifact = artifacts[0]
        assert artifact["name"] == "resultados.xlsx"
        assert artifact["media_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        downloaded = client.get(artifact["url"])

    assert downloaded.status_code == 200
    assert downloaded.content.startswith(b"PK")
    workbook_path.write_bytes(downloaded.content)
    workbook = load_workbook(workbook_path)
    worksheet = workbook["matches"]
    assert worksheet["A2"].value == "reference match"
    assert worksheet["B2"].hyperlink.target == "https://example.test/item"
    assert worksheet["A3"].value == "array row"
    with zipfile.ZipFile(workbook_path) as workbook_zip:
        assert any(name.startswith("xl/media/") for name in workbook_zip.namelist())


def test_builder_dependency_proposal_can_be_reviewed_and_accepted_from_web_api(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion import builder_capabilities
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    def fake_install(dependency_id: str) -> dict[str, object]:
        return {
            "dependency_id": dependency_id,
            "package": dependency_id,
            "requirement": f"{dependency_id}>=0",
            "command": ["python", "-m", "pip", "install", dependency_id],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "installed": True,
            "installed_version": "9.9.9",
        }

    def fake_install_dependency(dependency) -> dict[str, object]:
        return {
            "dependency_id": dependency.dependency_id,
            "package": dependency.package,
            "requirement": dependency.requirement,
            "command": ["python", "-m", "pip", "install", dependency.requirement],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "installed": True,
            "installed_version": "8.8.8",
        }

    real_status = builder_capabilities.capability_dependency_status

    def fake_dependency_status(dependency):
        if dependency.package == "pandas":
            return SimpleNamespace(dependency=dependency, installed=True, installed_version="8.8.8")
        return real_status(dependency)

    monkeypatch.setattr(builder_capabilities, "install_capability_dependency", fake_install)
    monkeypatch.setattr(builder_capabilities, "install_dependency", fake_install_dependency)
    monkeypatch.setattr(builder_capabilities, "capability_dependency_status", fake_dependency_status)

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))

    with TestClient(app) as client:
        proposed = client.post("/api/capabilities/dependencies/soundfile/propose")
        assert proposed.status_code == 200
        proposal = proposed.json()["proposal"]
        assert proposal["approval_mode"] == "dependency"
        assert "create or update files/artifacts" in proposal["acceptance_benefit"]

        status = client.get("/api/status?full=1").json()
        pending = [
            item
            for item in status["builder_proposals"]
            if item["proposal_id"] == proposal["proposal_id"]
        ]
        assert pending
        assert pending[0]["approval_mode"] == "dependency"
        assert pending[0]["acceptance_benefit"] == proposal["acceptance_benefit"]
        assert "dependency:soundfile" in pending[0]["suggested_tags"]
        assert status["health"]["attention_needed"] >= 1

        accepted = client.post(f"/api/builder/proposals/{proposal['proposal_id']}/accept")
        assert accepted.status_code == 200
        assert accepted.json()["type"] == "dependency"
        assert accepted.json()["result"]["installed_version"] == "9.9.9"

        refreshed = client.get("/api/status?full=1").json()
        accepted_records = [
            item
            for item in refreshed["builder_proposals"]
            if item["proposal_id"] == proposal["proposal_id"]
        ]
        assert accepted_records[0]["status"] == "accepted"

        custom = client.post(
            "/api/capabilities/custom-dependency/propose",
            json={
                "package": "pandas",
                "import_name": "pandas",
                "license": "BSD-3-Clause",
                "github_url": "https://github.com/pandas-dev/pandas",
                "git_ref": "v2.2.0",
                "docs_url": "https://pandas.pydata.org/docs/",
                "summary": "Use for dataframe analysis when a local Python workflow needs it.",
            },
        )
        assert custom.status_code == 200
        custom_proposal = custom.json()["proposal"]
        custom_status = client.get("/api/status?full=1").json()
        custom_pending = [
            item
            for item in custom_status["builder_proposals"]
            if item["proposal_id"] == custom_proposal["proposal_id"]
        ]
        assert custom_pending
        assert custom_pending[0]["approval_mode"] == "dependency"
        assert "custom_dependency" in custom_pending[0]["suggested_tags"]
        assert "package:pandas" in custom_pending[0]["suggested_tags"]
        assert any("git+https://github.com/pandas-dev/pandas.git@v2.2.0" in step for step in custom_pending[0]["suggested_steps"])

        custom_accepted = client.post(f"/api/builder/proposals/{custom_proposal['proposal_id']}/accept")
        assert custom_accepted.status_code == 200
        assert custom_accepted.json()["type"] == "dependency"
        assert custom_accepted.json()["result"]["package"] == "pandas"
        assert custom_accepted.json()["result"]["installed_version"] == "8.8.8"

        from nullion.builder_capabilities import format_installed_dependency_context

        context = format_installed_dependency_context(runtime)
        assert context is not None
        assert "pandas" in context
        assert "import `pandas`" in context
        assert "dataframe analysis" in context


def test_installed_presentation_dependency_is_used_to_create_real_pptx(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from fastapi.testclient import TestClient
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolInvocation, ToolRegistry, create_core_tool_registry
    from nullion.web_app import create_app

    monkeypatch.setenv("NULLION_FILE_ACCESS_ENABLED", "1")
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=None, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))

    with TestClient(app) as client:
        proposed = client.post("/api/capabilities/dependencies/python-pptx/propose")
        assert proposed.status_code == 200
        proposal = proposed.json()["proposal"]
        accepted = client.post(f"/api/builder/proposals/{proposal['proposal_id']}/accept")
        assert accepted.status_code == 200
        assert accepted.json()["type"] == "dependency"
        assert accepted.json()["result"]["installed"] is True

    registry = create_core_tool_registry(workspace_root=tmp_path)
    output_path = tmp_path / "capability-installed.pptx"
    result = registry.invoke(
        ToolInvocation(
            "e2e-presentation-create",
            "presentation_create",
            "web:e2e",
            {
                "output_path": str(output_path),
                "title": "Capability install proof",
                "slides": [
                    {
                        "title": "Installed library used",
                        "body": "This deck was created after accepting the python-pptx Builder proposal.",
                        "bullets": ["Builder install completed", "presentation_create produced a real pptx"],
                    }
                ],
            },
        )
    )
    assert result.status == "completed"
    assert Path(str(result.output["path"])) == output_path
    assert output_path.exists()
    assert output_path.read_bytes().startswith(b"PK")
    with zipfile.ZipFile(output_path) as deck_zip:
        names = set(deck_zip.namelist())
    assert "ppt/presentation.xml" in names
    assert "ppt/slides/slide1.xml" in names

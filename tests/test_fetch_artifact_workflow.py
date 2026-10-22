from __future__ import annotations

from pathlib import Path

from nullion.fetch_artifact_workflow import run_fetch_artifact_workflow
from nullion.runtime import bootstrap_persistent_runtime
from nullion.tools import ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


def test_fetch_artifact_workflow_writes_text_artifact(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")

    result = run_fetch_artifact_workflow(
        runtime,
        prompt="fetch cnn.com headlines and attach report.txt",
        tool_results=[ToolResult("fetch", "web_fetch", "completed", {"body": "headline one"})],
        principal_id="workspace:demo",
    )

    assert result.completed is True
    assert Path(result.artifact_paths[0]).read_text(encoding="utf-8") == "headline one"
    assert result.artifact_paths[0].endswith(".txt")


def test_fetch_artifact_workflow_creates_pdf_with_normalized_text_pages(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    seen_arguments: list[dict[str, object]] = []

    def pdf_handler(invocation):
        seen_arguments.append(dict(invocation.arguments))
        path = Path(str(invocation.arguments["output_path"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4\n% test\n")
        return ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {"artifact_path": str(path)})

    registry.register(
        ToolSpec(
            name="pdf_create",
            description="Create PDF",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=30,
        ),
        pdf_handler,
    )

    result = run_fetch_artifact_workflow(
        runtime,
        prompt="fetch bbc.com latest news and attach report.pdf",
        tool_results=[ToolResult("fetch", "web_fetch", "completed", {"text": "story one"})],
        registry=registry,
        principal_id="workspace:demo",
    )

    assert result.completed is True
    assert result.artifact_paths[0].endswith(".pdf")
    assert seen_arguments[0]["text_pages"] == ["story one"]


import pytest

@pytest.mark.parametrize('extension',['.txt','.html','.pdf'])
def test_missing_scoped_artifact_writer_returns_failure_without_throwing(tmp_path,monkeypatch,extension):
    monkeypatch.setenv('NULLION_WORKSPACE_STORAGE_ROOT',str(tmp_path/'workspaces'))
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    result=run_fetch_artifact_workflow(runtime,prompt='添付して。',requested_extension=extension,
        registry=ToolRegistry(),principal_id='web:bounded',
        tool_results=[ToolResult('source','web_fetch','completed',{'body':'source content'})])
    assert not result.completed and not result.artifact_paths
    assert 'unavailable' in result.error
    assert not list((tmp_path/'workspaces').rglob('*'+extension))

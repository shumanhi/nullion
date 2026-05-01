from __future__ import annotations

from types import SimpleNamespace

from nullion.artifact_workflow_graph import run_pre_chat_artifact_workflow
from nullion.image_generation_delivery import ImageArtifactRequest, ImageGenerationDeliveryResult
from nullion.screenshot_delivery import ScreenshotDeliveryResult
from nullion.tools import ToolResult


def test_artifact_workflow_graph_prefers_screenshot_result(monkeypatch) -> None:
    screenshot = ScreenshotDeliveryResult(
        status="completed",
        url="https://example.com",
        artifact_paths=["/tmp/screenshot.png"],
        tool_results=[ToolResult("shot", "browser_screenshot", "completed", {})],
    )
    image = ImageGenerationDeliveryResult(matched=True, completed=True, artifact_paths=["/tmp/image.png"])

    monkeypatch.setattr("nullion.artifact_workflow_graph.capture_screenshot_artifact_for_request", lambda *args, **kwargs: screenshot)
    monkeypatch.setattr("nullion.artifact_workflow_graph.generate_image_artifact_for_request", lambda *args, **kwargs: image)

    result = run_pre_chat_artifact_workflow(
        SimpleNamespace(store=object()),
        prompt="capture example.com",
        registry=None,
        principal_id="telegram_chat",
    )

    assert result.kind == "screenshot"
    assert result.completed
    assert result.artifact_paths == ["/tmp/screenshot.png"]
    assert result.image_result is None


def test_artifact_workflow_graph_falls_through_to_image(monkeypatch) -> None:
    image = ImageGenerationDeliveryResult(
        matched=True,
        completed=True,
        artifact_paths=["/tmp/image.png"],
        tool_results=[ToolResult("img", "image_generate", "completed", {})],
    )

    monkeypatch.setattr("nullion.artifact_workflow_graph.capture_screenshot_artifact_for_request", lambda *args, **kwargs: None)
    monkeypatch.setattr("nullion.artifact_workflow_graph.generate_image_artifact_for_request", lambda *args, **kwargs: image)

    result = run_pre_chat_artifact_workflow(
        SimpleNamespace(store=object()),
        prompt="make the visual different",
        registry=None,
        principal_id="telegram_chat",
        source_image_path="/tmp/source.png",
    )

    assert result.kind == "image"
    assert result.completed
    assert result.artifact_paths == ["/tmp/image.png"]


def test_artifact_workflow_graph_reports_not_matched(monkeypatch) -> None:
    monkeypatch.setattr("nullion.artifact_workflow_graph.capture_screenshot_artifact_for_request", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "nullion.artifact_workflow_graph.generate_image_artifact_for_request",
        lambda *args, **kwargs: ImageGenerationDeliveryResult(matched=False),
    )

    result = run_pre_chat_artifact_workflow(
        SimpleNamespace(store=object()),
        prompt="normal chat",
        registry=None,
        principal_id="telegram_chat",
    )

    assert result.kind == "none"
    assert result.status == "not_matched"
    assert not result.matched


def test_artifact_workflow_graph_executes_from_structured_image_request(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_generate(*args, **kwargs):
        seen["request"] = kwargs.get("request")
        seen["source_path"] = kwargs.get("source_path")
        return ImageGenerationDeliveryResult(matched=True, completed=True, artifact_paths=["/tmp/edited.png"])

    monkeypatch.setattr("nullion.artifact_workflow_graph.generate_image_artifact_for_request", fake_generate)

    result = run_pre_chat_artifact_workflow(
        SimpleNamespace(store=object()),
        prompt="add a hat",
        registry=None,
        principal_id="telegram_chat",
        source_image_path="/tmp/source.png",
    )

    assert result.kind == "image"
    assert isinstance(seen["request"], ImageArtifactRequest)
    assert seen["request"].kind == "edit"
    assert seen["source_path"] == "/tmp/source.png"


def test_artifact_workflow_graph_executes_from_structured_screenshot_request(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_capture(*args, **kwargs):
        seen["request"] = kwargs.get("request")
        request = kwargs["request"]
        return ScreenshotDeliveryResult(status="failed", url=request.url, error="expected test stop")

    monkeypatch.setattr("nullion.artifact_workflow_graph.capture_screenshot_artifact_for_request", fake_capture)

    result = run_pre_chat_artifact_workflow(
        SimpleNamespace(store=object()),
        prompt="screenshot example.com",
        registry=None,
        principal_id="telegram_chat",
    )

    assert result.kind == "screenshot"
    assert result.status == "failed"
    assert seen["request"].url == "https://example.com"

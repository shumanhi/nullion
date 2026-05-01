from __future__ import annotations

import base64
from dataclasses import dataclass
from types import SimpleNamespace

from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolSideEffectClass, ToolSpec
from nullion.tools import ToolRiskLevel


def _result(tool_name: str, status: str, output: dict[str, object] | None = None, error: str | None = None) -> ToolResult:
    return ToolResult(
        invocation_id=f"inv-{tool_name}",
        tool_name=tool_name,
        status=status,
        output=output or {},
        error=error,
    )


def _browser_registry(*, include_screenshot: bool = True) -> ToolRegistry:
    registry = ToolRegistry()
    for name in ("browser_navigate", "browser_screenshot"):
        if name == "browser_screenshot" and not include_screenshot:
            continue
        registry.register(
            ToolSpec(
                name=name,
                description=name,
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=2,
            ),
            lambda invocation: _result(invocation.tool_name, "completed"),
        )
    return registry


@dataclass
class RuntimeDouble:
    store: object = None


def test_parse_screenshot_request_requires_capture_intent_and_normalizes_url() -> None:
    from nullion.screenshot_delivery import _normalize_url, parse_screenshot_request

    assert parse_screenshot_request("please screenshot https://Example.com/path).").url == "https://Example.com/path"
    assert parse_screenshot_request("capture example.org now").url == "https://example.org"
    assert parse_screenshot_request("visit example.org") is None
    assert parse_screenshot_request("screenshot localhost") is None
    assert parse_screenshot_request(123) is None
    assert _normalize_url(" ") is None
    assert _normalize_url("http://") is None
    assert _normalize_url("http://localhost") is None


def test_screenshot_result_helpers_extract_approval_and_session() -> None:
    from nullion import screenshot_delivery as delivery

    approved = _result("browser_navigate", "denied", {"reason": "approval_required", "approval_id": "ap-1"})
    assert delivery._approval_id_from_result(approved) == "ap-1"
    assert delivery._approval_id_from_result(_result("browser_navigate", "failed", {"approval_id": "ap-1"})) is None
    assert delivery._session_id_from_result(_result("browser_navigate", "completed", {"session_id": "sess-1"})) == "sess-1"
    assert delivery._session_id_from_result(_result("browser_navigate", "completed", {"session_id": ""})) == "default"


def test_materialize_png_decodes_base64_and_removes_inline_payload(tmp_path, monkeypatch) -> None:
    from nullion import screenshot_delivery as delivery

    artifact = tmp_path / "screenshot.png"
    monkeypatch.setattr(delivery, "artifact_path_for_generated_workspace_file", lambda **kwargs: artifact)
    output = {"image_base64": base64.b64encode(b"png-bytes").decode("ascii")}

    path = delivery._materialize_png(RuntimeDouble(), output, principal_id="workspace:one")

    assert path == str(artifact)
    assert artifact.read_bytes() == b"png-bytes"
    assert output == {"path": str(artifact)}
    assert delivery._materialize_png(RuntimeDouble(), {"image_base64": "not base64"}) is None
    assert delivery._materialize_png(RuntimeDouble(), {"image_base64": "===="}) is None
    assert delivery._materialize_png(RuntimeDouble(), {"image_base64": ""}) is None


def test_invoke_tool_bounded_returns_result_and_converts_handler_exception(monkeypatch) -> None:
    from nullion import screenshot_delivery as delivery

    registry = _browser_registry()
    invocation = ToolInvocation("inv-1", "browser_navigate", "operator", {"url": "https://example.com"})
    monkeypatch.setattr(delivery, "invoke_tool", lambda store, invocation, registry: _result(invocation.tool_name, "ok"))

    assert delivery._invoke_tool_bounded(RuntimeDouble(store=object()), registry, invocation).status == "ok"

    monkeypatch.setattr(delivery, "invoke_tool", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    failed = delivery._invoke_tool_bounded(RuntimeDouble(store=object()), registry, invocation)
    assert failed.status == "failed"
    assert failed.output == {"reason": "handler_exception"}
    assert failed.error == "boom"


def test_invoke_tool_bounded_reports_timeout(monkeypatch) -> None:
    from nullion import screenshot_delivery as delivery

    class Future:
        cancelled = False

        def result(self, timeout):
            raise delivery.FutureTimeoutError()

        def cancel(self):
            self.cancelled = True

    future = Future()

    class Executor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def submit(self, *args, **kwargs):
            return future

        def shutdown(self, **kwargs):
            self.shutdown_kwargs = kwargs

    monkeypatch.setattr(delivery, "ThreadPoolExecutor", Executor)
    invocation = ToolInvocation("inv-timeout", "browser_navigate", "operator", {"url": "https://example.com"})

    result = delivery._invoke_tool_bounded(RuntimeDouble(store=object()), _browser_registry(), invocation)

    assert future.cancelled is True
    assert result.status == "failed"
    assert result.output == {"reason": "tool_timeout", "timeout_seconds": 2}
    assert "timed out after 2 seconds" in result.error


def test_capture_screenshot_handles_missing_tools_and_approval(monkeypatch) -> None:
    from nullion import screenshot_delivery as delivery

    missing = delivery.capture_screenshot_artifact(
        RuntimeDouble(),
        _browser_registry(include_screenshot=False),
        prompt="screenshot example.com",
        principal_id="web:operator",
    )
    assert missing.status == "failed"
    assert "not registered" in missing.error

    monkeypatch.setattr(
        delivery,
        "_invoke_tool_bounded",
        lambda runtime, registry, invocation: _result(
            invocation.tool_name,
            "denied",
            {"reason": "approval_required", "approval_id": "ap-99"},
        ),
    )
    approval = delivery.capture_screenshot_artifact(
        RuntimeDouble(),
        _browser_registry(),
        prompt="screenshot https://example.com",
        principal_id="web:operator",
    )
    assert approval.needs_approval
    assert approval.approval_id == "ap-99"
    assert approval.tool_name == "browser_navigate"


def test_capture_screenshot_reports_navigation_and_capture_failures(monkeypatch) -> None:
    from nullion import screenshot_delivery as delivery

    monkeypatch.setattr(
        delivery,
        "_invoke_tool_bounded",
        lambda runtime, registry, invocation: _result(invocation.tool_name, "failed", error="navigation blocked"),
    )
    failed_navigation = delivery.capture_screenshot_artifact(
        RuntimeDouble(),
        _browser_registry(),
        prompt="screenshot example.com",
        principal_id="operator",
    )
    assert failed_navigation.status == "failed"
    assert failed_navigation.error == "navigation blocked"

    calls: list[str] = []

    def fake_capture_failure(runtime, registry, invocation):
        calls.append(invocation.tool_name)
        if invocation.tool_name == "browser_navigate":
            return _result(invocation.tool_name, "completed", {"session_id": "sess"})
        return _result(invocation.tool_name, "failed", error="capture failed")

    monkeypatch.setattr(delivery, "_invoke_tool_bounded", fake_capture_failure)
    failed_capture = delivery.capture_screenshot_artifact(
        RuntimeDouble(),
        _browser_registry(),
        prompt="capture example.com",
        principal_id="workspace:one",
    )
    assert calls == ["browser_navigate", "browser_screenshot"]
    assert failed_capture.status == "failed"
    assert failed_capture.error == "capture failed"


def test_capture_screenshot_handles_capture_approval_and_missing_image(monkeypatch) -> None:
    from nullion import screenshot_delivery as delivery

    def fake_capture_approval(runtime, registry, invocation):
        if invocation.tool_name == "browser_navigate":
            return _result(invocation.tool_name, "completed", {"session_id": "sess"})
        return _result(invocation.tool_name, "denied", {"reason": "approval_required", "approval_id": "ap-capture"})

    monkeypatch.setattr(delivery, "_invoke_tool_bounded", fake_capture_approval)
    approval = delivery.capture_screenshot_artifact(
        RuntimeDouble(),
        _browser_registry(),
        prompt="capture example.com",
        principal_id="workspace:one",
    )
    assert approval.needs_approval
    assert approval.tool_name == "browser_screenshot"

    def fake_missing_image(runtime, registry, invocation):
        if invocation.tool_name == "browser_navigate":
            return _result(invocation.tool_name, "completed", {"session_id": "sess"})
        return _result(invocation.tool_name, "completed", {"ok": True})

    monkeypatch.setattr(delivery, "_invoke_tool_bounded", fake_missing_image)
    missing_image = delivery.capture_screenshot_artifact(
        RuntimeDouble(),
        _browser_registry(),
        prompt="capture example.com",
        principal_id="workspace:one",
    )
    assert missing_image.status == "failed"
    assert "did not return image data" in missing_image.error


def test_capture_screenshot_materializes_completed_artifact(tmp_path, monkeypatch) -> None:
    from nullion import screenshot_delivery as delivery

    artifact = tmp_path / "completed.png"
    calls: list[ToolInvocation] = []

    def fake_invoke(runtime, registry, invocation):
        calls.append(invocation)
        if invocation.tool_name == "browser_navigate":
            return _result(invocation.tool_name, "completed", {"session_id": "sess"})
        return _result(invocation.tool_name, "completed", {"image_base64": base64.b64encode(b"image").decode("ascii")})

    monkeypatch.setattr(delivery, "_invoke_tool_bounded", fake_invoke)
    monkeypatch.setattr(delivery, "artifact_path_for_generated_workspace_file", lambda **kwargs: artifact)

    result = delivery.capture_screenshot_artifact(
        RuntimeDouble(),
        _browser_registry(),
        prompt="screen shot https://example.com/dashboard",
        principal_id="telegram_chat",
    )

    assert result.completed
    assert result.artifact_paths == [str(artifact)]
    assert artifact.read_bytes() == b"image"
    assert [call.tool_name for call in calls] == ["browser_navigate", "browser_screenshot"]
    assert calls[0].principal_id == "operator"
    assert calls[0].arguments["url"] == "https://example.com/dashboard"
    assert calls[1].arguments["session_id"] == "sess"


def test_capture_screenshot_returns_none_when_prompt_is_not_a_request() -> None:
    from nullion import screenshot_delivery as delivery

    assert (
        delivery.capture_screenshot_artifact(
            RuntimeDouble(),
            _browser_registry(),
            prompt="tell me about example.com",
            principal_id="operator",
        )
        is None
    )

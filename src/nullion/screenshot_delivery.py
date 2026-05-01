"""Platform-neutral screenshot request handling."""

from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from functools import lru_cache
import re
from typing import Any, TypedDict
from urllib.parse import urlparse
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.artifacts import artifact_path_for_generated_workspace_file
from nullion.runtime import invoke_tool
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, normalize_tool_status


_EXPLICIT_URL_RE = re.compile(r"https?://[^\s<>()\]]+", re.IGNORECASE)
_DOMAIN_RE = re.compile(r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ScreenshotRequest:
    url: str


@dataclass(frozen=True, slots=True)
class ScreenshotDeliveryResult:
    status: str
    url: str
    artifact_paths: list[str] = field(default_factory=list)
    approval_id: str | None = None
    tool_name: str | None = None
    error: str | None = None
    tool_results: list[ToolResult] = field(default_factory=list)

    @property
    def needs_approval(self) -> bool:
        return self.status == "approval_required" and bool(self.approval_id)

    @property
    def completed(self) -> bool:
        return self.status == "completed" and bool(self.artifact_paths)


def _clean_url_token(value: str) -> str:
    return value.strip().strip(".,;:!?)]}'\"")


def _normalize_url(value: str) -> str | None:
    cleaned = _clean_url_token(value)
    if not cleaned:
        return None
    if not cleaned.startswith(("http://", "https://")):
        cleaned = f"https://{cleaned}"
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    if "." not in parsed.hostname:
        return None
    return cleaned


def parse_screenshot_request(prompt: str) -> ScreenshotRequest | None:
    """Free-form prompts are not parsed into screenshot intent locally."""

    return None


def _approval_id_from_result(result: ToolResult) -> str | None:
    output = result.output if isinstance(result.output, dict) else {}
    approval_id = output.get("approval_id")
    if normalize_tool_status(result.status) == "denied" and output.get("reason") == "approval_required" and isinstance(approval_id, str):
        return approval_id
    return None


def _session_id_from_result(result: ToolResult) -> str:
    output = result.output if isinstance(result.output, dict) else {}
    session_id = output.get("session_id")
    return session_id if isinstance(session_id, str) and session_id else "default"


def _materialize_png(runtime, output: dict[str, object], *, principal_id: str | None = None) -> str | None:
    image_base64 = output.get("image_base64")
    if not isinstance(image_base64, str) or not image_base64:
        return None
    try:
        image_bytes = base64.b64decode(image_base64)
    except Exception:
        return None
    if not image_bytes:
        return None
    artifact_path = artifact_path_for_generated_workspace_file(
        principal_id=principal_id,
        suffix=".png",
        stem="screenshot",
    )
    artifact_path.write_bytes(image_bytes)
    output["path"] = str(artifact_path)
    output.pop("image_base64", None)
    return str(artifact_path)


def _get_tool_spec(registry: ToolRegistry, tool_name: str):
    try:
        return registry.get_spec(tool_name)
    except KeyError:
        return None


def _invoke_tool_bounded(runtime, registry: ToolRegistry, invocation: ToolInvocation) -> ToolResult:
    spec = _get_tool_spec(registry, invocation.tool_name)
    timeout_seconds = max(1, min(int(getattr(spec, "timeout_seconds", 20) or 20), 35))
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"nullion-{invocation.tool_name}")
    future = executor.submit(invoke_tool, runtime.store, invocation, registry=registry)
    try:
        result = future.result(timeout=timeout_seconds)
    except FutureTimeoutError:
        future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="failed",
            output={"reason": "tool_timeout", "timeout_seconds": timeout_seconds},
            error=f"{invocation.tool_name} timed out after {timeout_seconds} seconds.",
        )
    except Exception as exc:
        executor.shutdown(wait=False, cancel_futures=True)
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="failed",
            output={"reason": "handler_exception"},
            error=str(exc),
        )
    executor.shutdown(wait=False)
    return result


class _ScreenshotWorkflowState(TypedDict, total=False):
    runtime: Any
    registry: ToolRegistry
    request: ScreenshotRequest
    principal_id: str
    principal: str
    session_id: str
    navigate_result: ToolResult
    screenshot_result: ToolResult
    tool_results: list[ToolResult]
    result: ScreenshotDeliveryResult


def _screenshot_prepare_node(state: _ScreenshotWorkflowState) -> dict[str, object]:
    registry = state["registry"]
    request = state["request"]
    if _get_tool_spec(registry, "browser_navigate") is None or _get_tool_spec(registry, "browser_screenshot") is None:
        return {
            "result": ScreenshotDeliveryResult(
                status="failed",
                url=request.url,
                error=(
                    "Browser screenshot tools are not registered. Enable the browser backend in settings, "
                    "then restart Nullion."
                ),
            )
        }
    principal_id = str(state.get("principal_id") or "")
    principal = "operator" if principal_id in {"telegram_chat"} or principal_id.startswith("web:") else principal_id
    return {"principal": principal, "tool_results": []}


def _screenshot_route_prepared(state: _ScreenshotWorkflowState) -> str:
    return END if state.get("result") is not None else "navigate"


def _screenshot_navigate_node(state: _ScreenshotWorkflowState) -> dict[str, object]:
    runtime = state["runtime"]
    registry = state["registry"]
    request = state["request"]
    principal = state["principal"]
    navigate = _invoke_tool_bounded(
        runtime,
        registry,
        ToolInvocation(
            invocation_id=f"screenshot-navigate-{uuid4().hex}",
            tool_name="browser_navigate",
            principal_id=principal,
            arguments={"url": request.url, "session_id": "screenshot"},
            capsule_id=None,
        ),
    )
    tool_results = [*state.get("tool_results", []), navigate]
    approval_id = _approval_id_from_result(navigate)
    if approval_id:
        return {
            "navigate_result": navigate,
            "tool_results": tool_results,
            "result": ScreenshotDeliveryResult(
                status="approval_required",
                url=request.url,
                approval_id=approval_id,
                tool_name="browser_navigate",
                tool_results=tool_results,
            ),
        }
    if normalize_tool_status(navigate.status) != "completed":
        return {
            "navigate_result": navigate,
            "tool_results": tool_results,
            "result": ScreenshotDeliveryResult(
                status="failed",
                url=request.url,
                error=navigate.error or "Browser navigation failed.",
                tool_results=tool_results,
            ),
        }
    return {"navigate_result": navigate, "tool_results": tool_results, "session_id": _session_id_from_result(navigate)}


def _screenshot_route_navigated(state: _ScreenshotWorkflowState) -> str:
    return END if state.get("result") is not None else "capture"


def _screenshot_capture_node(state: _ScreenshotWorkflowState) -> dict[str, object]:
    runtime = state["runtime"]
    registry = state["registry"]
    request = state["request"]
    principal = state["principal"]
    screenshot = _invoke_tool_bounded(
        runtime,
        registry,
        ToolInvocation(
            invocation_id=f"screenshot-capture-{uuid4().hex}",
            tool_name="browser_screenshot",
            principal_id=principal,
            arguments={"session_id": state.get("session_id") or "default"},
            capsule_id=None,
        ),
    )
    tool_results = [*state.get("tool_results", []), screenshot]
    approval_id = _approval_id_from_result(screenshot)
    if approval_id:
        return {
            "screenshot_result": screenshot,
            "tool_results": tool_results,
            "result": ScreenshotDeliveryResult(
                status="approval_required",
                url=request.url,
                approval_id=approval_id,
                tool_name="browser_screenshot",
                tool_results=tool_results,
            ),
        }
    if normalize_tool_status(screenshot.status) != "completed":
        return {
            "screenshot_result": screenshot,
            "tool_results": tool_results,
            "result": ScreenshotDeliveryResult(
                status="failed",
                url=request.url,
                error=screenshot.error or "Browser screenshot failed.",
                tool_results=tool_results,
            ),
        }
    return {"screenshot_result": screenshot, "tool_results": tool_results}


def _screenshot_route_captured(state: _ScreenshotWorkflowState) -> str:
    return END if state.get("result") is not None else "materialize"


def _screenshot_materialize_node(state: _ScreenshotWorkflowState) -> dict[str, object]:
    runtime = state["runtime"]
    request = state["request"]
    screenshot = state["screenshot_result"]
    tool_results = list(state.get("tool_results", []))
    output = screenshot.output if isinstance(screenshot.output, dict) else {}
    path = _materialize_png(runtime, output, principal_id=state.get("principal"))
    if not path:
        return {
            "result": ScreenshotDeliveryResult(
                status="failed",
                url=request.url,
                error="Browser screenshot completed but did not return image data.",
                tool_results=tool_results,
            )
        }
    return {
        "result": ScreenshotDeliveryResult(
            status="completed",
            url=request.url,
            artifact_paths=[path],
            tool_results=tool_results,
        )
    }


@lru_cache(maxsize=1)
def _compiled_screenshot_workflow_graph():
    graph = StateGraph(_ScreenshotWorkflowState)
    graph.add_node("prepare", _screenshot_prepare_node)
    graph.add_node("navigate", _screenshot_navigate_node)
    graph.add_node("capture", _screenshot_capture_node)
    graph.add_node("materialize", _screenshot_materialize_node)
    graph.add_edge(START, "prepare")
    graph.add_conditional_edges("prepare", _screenshot_route_prepared, {"navigate": "navigate", END: END})
    graph.add_conditional_edges("navigate", _screenshot_route_navigated, {"capture": "capture", END: END})
    graph.add_conditional_edges("capture", _screenshot_route_captured, {"materialize": "materialize", END: END})
    graph.add_edge("materialize", END)
    return graph.compile()


def capture_screenshot_artifact(
    runtime,
    registry: ToolRegistry,
    *,
    prompt: str,
    principal_id: str,
) -> ScreenshotDeliveryResult | None:
    request = parse_screenshot_request(prompt)
    if request is None:
        return None
    return capture_screenshot_artifact_for_request(
        runtime,
        registry,
        request=request,
        principal_id=principal_id,
    )


def capture_screenshot_artifact_for_request(
    runtime,
    registry: ToolRegistry,
    *,
    request: ScreenshotRequest,
    principal_id: str,
) -> ScreenshotDeliveryResult:
    final_state = _compiled_screenshot_workflow_graph().invoke(
        {"runtime": runtime, "registry": registry, "request": request, "principal_id": principal_id},
        config={"configurable": {"thread_id": f"screenshot:{principal_id}:{request.url}"}},
    )
    result = final_state.get("result")
    if isinstance(result, ScreenshotDeliveryResult):
        return result
    return ScreenshotDeliveryResult(status="failed", url=request.url, error="Screenshot workflow did not produce a result.")


__all__ = [
    "ScreenshotDeliveryResult",
    "ScreenshotRequest",
    "capture_screenshot_artifact",
    "capture_screenshot_artifact_for_request",
    "parse_screenshot_request",
]

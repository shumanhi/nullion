"""LangGraph orchestration for pre-chat artifact workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.image_generation_delivery import (
    ImageArtifactRequest,
    ImageGenerationDeliveryResult,
    generate_image_artifact_for_request,
    parse_image_edit_request,
    parse_image_generation_request,
)
from nullion.screenshot_delivery import (
    ScreenshotDeliveryResult,
    ScreenshotRequest,
    capture_screenshot_artifact_for_request,
    parse_screenshot_request,
)
from nullion.tools import ToolRegistry, ToolResult


ArtifactWorkflowKind = Literal["none", "screenshot", "image"]
ArtifactWorkflowStatus = Literal["not_matched", "completed", "approval_required", "failed"]


class ArtifactWorkflowState(TypedDict, total=False):
    runtime: Any
    registry: ToolRegistry | None
    prompt: str
    principal_id: str
    source_image_path: str | None
    screenshot_request: ScreenshotRequest | None
    image_request: ImageArtifactRequest | None
    screenshot_result: ScreenshotDeliveryResult | None
    image_result: ImageGenerationDeliveryResult | None
    kind: ArtifactWorkflowKind
    status: ArtifactWorkflowStatus
    artifact_paths: list[str]
    tool_results: list[ToolResult]
    approval_id: str | None
    error: str | None


@dataclass(slots=True)
class ArtifactWorkflowResult:
    kind: ArtifactWorkflowKind
    status: ArtifactWorkflowStatus
    artifact_paths: list[str] = field(default_factory=list)
    tool_results: list[ToolResult] = field(default_factory=list)
    approval_id: str | None = None
    error: str | None = None
    screenshot_result: ScreenshotDeliveryResult | None = None
    image_result: ImageGenerationDeliveryResult | None = None

    @property
    def matched(self) -> bool:
        return self.kind != "none" and self.status != "not_matched"

    @property
    def completed(self) -> bool:
        return self.status == "completed" and bool(self.artifact_paths)

    @property
    def needs_approval(self) -> bool:
        return self.status == "approval_required" and bool(self.approval_id)


def _decide_screenshot_request_node(state: ArtifactWorkflowState) -> dict[str, object]:
    return {"screenshot_request": parse_screenshot_request(state.get("prompt") or "")}


def _decide_image_request_node(state: ArtifactWorkflowState) -> dict[str, object]:
    if state.get("screenshot_request") is not None:
        return {"image_request": None}
    prompt = state.get("prompt") or ""
    source_image_path = state.get("source_image_path")
    if source_image_path and parse_image_edit_request(prompt):
        return {"image_request": ImageArtifactRequest(kind="edit")}
    if parse_image_generation_request(prompt):
        return {"image_request": ImageArtifactRequest(kind="generate")}
    return {"image_request": None}


def _run_screenshot_node(state: ArtifactWorkflowState) -> dict[str, object]:
    request = state.get("screenshot_request")
    if request is None:
        return {"screenshot_result": None}
    registry = state.get("registry") or ToolRegistry()
    try:
        result = capture_screenshot_artifact_for_request(
            state["runtime"],
            registry,
            request=request,
            principal_id=state["principal_id"],
        )
    except AttributeError:
        result = None
    if result is None:
        return {"screenshot_result": None}
    if result.needs_approval:
        return {
            "screenshot_result": result,
            "kind": "screenshot",
            "status": "approval_required",
            "approval_id": result.approval_id,
            "tool_results": result.tool_results,
        }
    if result.completed:
        return {
            "screenshot_result": result,
            "kind": "screenshot",
            "status": "completed",
            "artifact_paths": result.artifact_paths,
            "tool_results": result.tool_results,
        }
    return {
        "screenshot_result": result,
        "kind": "screenshot",
        "status": "failed",
        "error": result.error,
        "tool_results": result.tool_results,
    }


def _run_image_node(state: ArtifactWorkflowState) -> dict[str, object]:
    if state.get("kind") == "screenshot":
        return {}
    request = state.get("image_request")
    if request is None:
        result = ImageGenerationDeliveryResult(matched=False)
        return {"image_result": result, "kind": "none", "status": "not_matched"}
    result = generate_image_artifact_for_request(
        state["runtime"],
        prompt=state["prompt"],
        registry=state.get("registry"),
        principal_id=state["principal_id"],
        request=request,
        source_path=state.get("source_image_path"),
    )
    if not result.matched:
        return {"image_result": result, "kind": "none", "status": "not_matched"}
    if result.suspended_for_approval:
        return {
            "image_result": result,
            "kind": "image",
            "status": "approval_required",
            "approval_id": result.approval_id,
            "tool_results": result.tool_results,
        }
    if result.completed:
        return {
            "image_result": result,
            "kind": "image",
            "status": "completed",
            "artifact_paths": result.artifact_paths,
            "tool_results": result.tool_results,
        }
    return {
        "image_result": result,
        "kind": "image",
        "status": "failed",
        "error": result.error,
        "tool_results": result.tool_results,
    }


@lru_cache(maxsize=1)
def _compiled_artifact_workflow_graph():
    graph = StateGraph(ArtifactWorkflowState)
    graph.add_node("decide_screenshot_request", _decide_screenshot_request_node)
    graph.add_node("decide_image_request", _decide_image_request_node)
    graph.add_node("run_screenshot", _run_screenshot_node)
    graph.add_node("run_image", _run_image_node)
    graph.add_edge(START, "decide_screenshot_request")
    graph.add_edge("decide_screenshot_request", "decide_image_request")
    graph.add_edge("decide_image_request", "run_screenshot")
    graph.add_edge("run_screenshot", "run_image")
    graph.add_edge("run_image", END)
    return graph.compile()


def run_pre_chat_artifact_workflow(
    runtime: object,
    *,
    prompt: str,
    registry: ToolRegistry | None,
    principal_id: str,
    source_image_path: str | None = None,
) -> ArtifactWorkflowResult:
    final_state = _compiled_artifact_workflow_graph().invoke(
        {
            "runtime": runtime,
            "registry": registry,
            "prompt": prompt,
            "principal_id": principal_id,
            "source_image_path": source_image_path,
            "kind": "none",
            "status": "not_matched",
            "artifact_paths": [],
            "tool_results": [],
        },
        config={"configurable": {"thread_id": f"artifact:{principal_id}"}},
    )
    return ArtifactWorkflowResult(
        kind=final_state.get("kind", "none"),
        status=final_state.get("status", "not_matched"),
        artifact_paths=list(final_state.get("artifact_paths") or []),
        tool_results=list(final_state.get("tool_results") or []),
        approval_id=final_state.get("approval_id"),
        error=final_state.get("error"),
        screenshot_result=final_state.get("screenshot_result"),
        image_result=final_state.get("image_result"),
    )


__all__ = [
    "ArtifactWorkflowResult",
    "ArtifactWorkflowState",
    "run_pre_chat_artifact_workflow",
]

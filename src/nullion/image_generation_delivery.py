"""Platform-neutral image generation request handling."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
import shutil
from typing import Any, Literal, TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.artifacts import artifact_path_for_generated_workspace_file
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, normalize_tool_status


@dataclass(slots=True)
class ImageGenerationDeliveryResult:
    matched: bool
    completed: bool = False
    suspended_for_approval: bool = False
    approval_id: str | None = None
    artifact_paths: list[str] = field(default_factory=list)
    tool_results: list[ToolResult] = field(default_factory=list)
    error: str | None = None
    fallback_used: bool = False


@dataclass(frozen=True, slots=True)
class ImageArtifactRequest:
    kind: Literal["generate", "edit"]


def parse_image_generation_request(prompt: str) -> bool:
    """Free-form prompts are not parsed into image-generation intent locally."""

    return False


def parse_image_edit_request(prompt: str) -> bool:
    """Free-form prompts are not parsed into image-edit intent locally."""

    return False


def _get_tool_spec(registry: ToolRegistry | None, name: str):
    if registry is None:
        return None
    try:
        return registry.get_spec(name)
    except KeyError:
        return None


def _approval_id_from_result(result: ToolResult) -> str | None:
    output = result.output if isinstance(result.output, dict) else {}
    value = output.get("approval_id")
    return value if isinstance(value, str) and value else None


def _friendly_image_generation_error(error: str | None) -> str:
    detail = str(error or "").strip()
    if (
        "local_media_provider requires NULLION_IMAGE_GENERATE_COMMAND" in detail
        or detail == "image_generate provider is not configured"
    ):
        return "Image generation is not configured. Enable an image generation provider in Settings."
    return detail or "Image generation provider failed."


def _source_image_fallback_result(
    *,
    source_image_path: str | None,
    output_path: Path,
    error: str | None,
    tool_results: list[ToolResult] | None = None,
) -> ImageGenerationDeliveryResult | None:
    if not source_image_path:
        return None
    source = Path(source_image_path).expanduser()
    if not source.is_file():
        return None
    fallback_path = output_path.with_suffix(source.suffix or output_path.suffix)
    try:
        fallback_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, fallback_path)
    except OSError:
        return None
    return ImageGenerationDeliveryResult(
        matched=True,
        completed=True,
        artifact_paths=[str(fallback_path)],
        tool_results=tool_results or [],
        error=_friendly_image_generation_error(error),
        fallback_used=True,
    )


class _ImageArtifactWorkflowState(TypedDict, total=False):
    runtime: Any
    prompt: str
    registry: ToolRegistry | None
    principal_id: str
    request: ImageArtifactRequest
    source_image_path: str | None
    output_path: Path
    invocation: ToolInvocation
    tool_result: ToolResult
    result: ImageGenerationDeliveryResult


def _image_artifact_prepare_node(state: _ImageArtifactWorkflowState) -> dict[str, object]:
    request = state["request"]
    source_image_path = state.get("source_image_path")
    registry = state.get("registry")
    if request.kind == "edit" and not source_image_path:
        return {"result": ImageGenerationDeliveryResult(matched=True, error="Image edit requested but no source image was available.")}
    if _get_tool_spec(registry, "image_generate") is None:
        return {
            "result": ImageGenerationDeliveryResult(
                matched=True,
                error="Image generation is not configured. Enable the media plugin and image provider in Settings.",
            )
        }
    if registry is None:
        return {"result": ImageGenerationDeliveryResult(matched=True, error="Image generation tool registry is unavailable.")}

    source_suffix = Path(source_image_path).suffix if source_image_path else ""
    output_path = artifact_path_for_generated_workspace_file(
        principal_id=str(state.get("principal_id") or ""),
        suffix=source_suffix if source_suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp"} else ".png",
        stem="generated-image",
    )
    invocation = ToolInvocation(
        invocation_id=f"image-generate-{uuid4().hex}",
        tool_name="image_generate",
        principal_id=str(state.get("principal_id") or ""),
        arguments={
            "prompt": _image_artifact_prompt(str(state.get("prompt") or ""), request=request),
            "output_path": str(output_path),
            "size": "1024x1024",
            **({"source_path": source_image_path} if source_image_path else {}),
        },
        capsule_id=None,
    )
    return {"output_path": output_path, "invocation": invocation}


def _image_artifact_prompt(prompt: str, *, request: ImageArtifactRequest) -> str:
    cleaned = " ".join(str(prompt or "").split()).strip()
    if request.kind != "edit":
        return cleaned
    return (
        "Edit the attached source image. Keep the same subject, identity, pose, and overall composition. "
        "Apply this requested visual change clearly and visibly: "
        f"{cleaned}. "
        "Return the edited image itself, not an unchanged copy."
    )


def _image_artifact_route_prepared(state: _ImageArtifactWorkflowState) -> str:
    return END if state.get("result") is not None else "invoke_tool"


def _image_artifact_invoke_tool_node(state: _ImageArtifactWorkflowState) -> dict[str, object]:
    source_image_path = state.get("source_image_path")
    output_path = state["output_path"]
    invocation = state["invocation"]
    runtime = state.get("runtime")
    registry = state.get("registry")
    try:
        from nullion.runtime import invoke_tool_with_boundary_policy

        result = invoke_tool_with_boundary_policy(runtime.store, invocation, registry=registry)
    except Exception as exc:
        fallback = _source_image_fallback_result(
            source_image_path=source_image_path,
            output_path=output_path,
            error=str(exc),
        )
        if fallback is not None:
            return {"result": fallback}
        return {"result": ImageGenerationDeliveryResult(matched=True, error=str(exc))}
    return {"tool_result": result}


def _image_artifact_route_invoked(state: _ImageArtifactWorkflowState) -> str:
    return END if state.get("result") is not None else "finalize"


def _image_artifact_finalize_node(state: _ImageArtifactWorkflowState) -> dict[str, object]:
    result = state["tool_result"]
    source_image_path = state.get("source_image_path")
    output_path = state["output_path"]
    status = normalize_tool_status(result.status)
    approval_id = _approval_id_from_result(result)
    if status in {"denied", "approval_required", "blocked", "suspended"} and approval_id:
        return {
            "result": ImageGenerationDeliveryResult(
                matched=True,
                suspended_for_approval=True,
                approval_id=approval_id,
                tool_results=[result],
            )
        }
    if status != "completed":
        fallback = _source_image_fallback_result(
            source_image_path=source_image_path,
            output_path=output_path,
            error=result.error,
            tool_results=[result],
        )
        if fallback is not None:
            return {"result": fallback}
        return {
            "result": ImageGenerationDeliveryResult(
                matched=True,
                tool_results=[result],
                error=_friendly_image_generation_error(result.error),
            )
        }
    output = result.output if isinstance(result.output, dict) else {}
    artifact_path = output.get("path") or output.get("output_path") or str(output_path)
    artifact_paths = [artifact_path] if isinstance(artifact_path, str) and artifact_path else [str(output_path)]
    if not any(Path(path).expanduser().is_file() for path in artifact_paths):
        fallback = _source_image_fallback_result(
            source_image_path=source_image_path,
            output_path=output_path,
            error="Image generation provider returned no image file.",
            tool_results=[result],
        )
        if fallback is not None:
            return {"result": fallback}
    return {
        "result": ImageGenerationDeliveryResult(
            matched=True,
            completed=True,
            artifact_paths=artifact_paths,
            tool_results=[result],
        )
    }


@lru_cache(maxsize=1)
def _compiled_image_artifact_workflow_graph():
    graph = StateGraph(_ImageArtifactWorkflowState)
    graph.add_node("prepare", _image_artifact_prepare_node)
    graph.add_node("invoke_tool", _image_artifact_invoke_tool_node)
    graph.add_node("finalize", _image_artifact_finalize_node)
    graph.add_edge(START, "prepare")
    graph.add_conditional_edges("prepare", _image_artifact_route_prepared, {"invoke_tool": "invoke_tool", END: END})
    graph.add_conditional_edges("invoke_tool", _image_artifact_route_invoked, {"finalize": "finalize", END: END})
    graph.add_edge("finalize", END)
    return graph.compile()


def generate_image_artifact(
    runtime,
    *,
    prompt: str,
    registry: ToolRegistry | None,
    principal_id: str,
    source_path: str | None = None,
) -> ImageGenerationDeliveryResult:
    source_image_path = source_path.strip() if isinstance(source_path, str) and source_path.strip() else None
    if not parse_image_generation_request(prompt) and not (source_image_path and parse_image_edit_request(prompt)):
        return ImageGenerationDeliveryResult(matched=False)
    return generate_image_artifact_for_request(
        runtime,
        prompt=prompt,
        registry=registry,
        principal_id=principal_id,
        request=ImageArtifactRequest(kind="edit" if source_image_path else "generate"),
        source_path=source_image_path,
    )


def generate_image_artifact_for_request(
    runtime,
    *,
    prompt: str,
    registry: ToolRegistry | None,
    principal_id: str,
    request: ImageArtifactRequest,
    source_path: str | None = None,
) -> ImageGenerationDeliveryResult:
    source_image_path = source_path.strip() if isinstance(source_path, str) and source_path.strip() else None
    if request.kind == "edit" and not source_image_path:
        return ImageGenerationDeliveryResult(matched=True, error="Image edit requested but no source image was available.")
    final_state = _compiled_image_artifact_workflow_graph().invoke(
        {
            "runtime": runtime,
            "prompt": prompt,
            "registry": registry,
            "principal_id": principal_id,
            "request": request,
            "source_image_path": source_image_path,
        },
        config={"configurable": {"thread_id": f"image-artifact:{principal_id}"}},
    )
    result = final_state.get("result")
    if isinstance(result, ImageGenerationDeliveryResult):
        return result
    return ImageGenerationDeliveryResult(matched=True, error="Image generation workflow did not produce a result.")


__all__ = [
    "ImageArtifactRequest",
    "ImageGenerationDeliveryResult",
    "generate_image_artifact",
    "generate_image_artifact_for_request",
    "parse_image_edit_request",
    "parse_image_generation_request",
]

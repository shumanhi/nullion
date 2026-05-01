"""LangGraph workflow for turning fetched content into verified artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field
import html
from functools import lru_cache
from pathlib import Path
import re
from typing import Any, TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_path_for_generated_workspace_file,
    artifact_root_for_principal,
)
from nullion.attachment_format_graph import plan_attachment_format
from nullion.runtime import invoke_tool_with_boundary_policy
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, normalize_tool_status


_SUPPORTED_FETCH_ARTIFACT_EXTENSIONS = frozenset({".html", ".txt", ".md", ".pdf"})
_SCRIPT_TAG_RE = re.compile(r"<script\b[^>]*>.*?</script\s*>", re.IGNORECASE | re.DOTALL)


@dataclass(slots=True)
class FetchArtifactWorkflowResult:
    matched: bool = False
    completed: bool = False
    artifact_paths: list[str] = field(default_factory=list)
    tool_results: list[ToolResult] = field(default_factory=list)
    error: str | None = None


class FetchArtifactState(TypedDict, total=False):
    runtime: Any
    prompt: str
    reply: str
    tool_results: list[ToolResult]
    registry: ToolRegistry | None
    principal_id: str | None
    extension: str | None
    content: str | None
    source_url: str | None
    artifact_paths: list[str]
    created_path: str | None
    write_attempts: int
    error: str | None


def run_fetch_artifact_workflow(
    runtime: object,
    *,
    prompt: str,
    reply: str = "",
    tool_results: list[ToolResult] | tuple[ToolResult, ...],
    registry: ToolRegistry | None = None,
    principal_id: str | None = None,
) -> FetchArtifactWorkflowResult:
    """Create a requested artifact from completed fetch/tool output."""

    final_state = _compiled_fetch_artifact_workflow().invoke(
        {
            "runtime": runtime,
            "prompt": prompt,
            "reply": reply,
            "tool_results": list(tool_results),
            "registry": registry,
            "principal_id": principal_id,
            "artifact_paths": [],
            "write_attempts": 0,
        },
        config={"configurable": {"thread_id": f"fetch-artifact:{principal_id or 'default'}"}},
    )
    paths = list(final_state.get("artifact_paths") or [])
    extension = final_state.get("extension")
    return FetchArtifactWorkflowResult(
        matched=extension in _SUPPORTED_FETCH_ARTIFACT_EXTENSIONS,
        completed=bool(paths),
        artifact_paths=paths,
        tool_results=list(final_state.get("tool_results") or []),
        error=final_state.get("error"),
    )


@lru_cache(maxsize=1)
def _compiled_fetch_artifact_workflow():
    graph = StateGraph(FetchArtifactState)
    graph.add_node("plan", _plan_node)
    graph.add_node("extract", _extract_content_node)
    graph.add_node("write", _write_artifact_node)
    graph.add_node("verify", _verify_artifact_node)
    graph.add_edge(START, "plan")
    graph.add_edge("plan", "extract")
    graph.add_edge("extract", "write")
    graph.add_conditional_edges("write", _route_after_write, {"retry": "write", "verify": "verify"})
    graph.add_edge("verify", END)
    return graph.compile()


def _plan_node(state: FetchArtifactState) -> dict[str, object]:
    extension = plan_attachment_format(state.get("prompt") or "").extension
    if extension not in _SUPPORTED_FETCH_ARTIFACT_EXTENSIONS:
        return {"extension": None}
    return {"extension": extension, "error": None}


def _extract_content_node(state: FetchArtifactState) -> dict[str, object]:
    extension = state.get("extension")
    if extension not in _SUPPORTED_FETCH_ARTIFACT_EXTENSIONS:
        return {}
    fetch_result = _latest_completed_tool_result(state.get("tool_results") or [], tool_name="web_fetch")
    terminal_result = None
    content = None
    source_url = None
    if fetch_result is not None:
        content = _web_fetch_body_for_artifact(fetch_result, extension=extension)
        output = fetch_result.output if isinstance(fetch_result.output, dict) else {}
        source_url = output.get("url") if isinstance(output.get("url"), str) else None
    if not isinstance(content, str) or not content.strip():
        terminal_result = _latest_completed_tool_result(state.get("tool_results") or [], tool_name="terminal_exec")
        if terminal_result is not None:
            content = _terminal_output_for_artifact(terminal_result)
    if not isinstance(content, str) or not content.strip():
        return {"error": "No completed fetch output was available to materialize."}
    return {"content": content, "source_url": source_url}


def _write_artifact_node(state: FetchArtifactState) -> dict[str, object]:
    attempts = int(state.get("write_attempts") or 0) + 1
    extension = state.get("extension")
    content = state.get("content")
    if extension not in _SUPPORTED_FETCH_ARTIFACT_EXTENSIONS or not isinstance(content, str) or not content.strip():
        return {"write_attempts": attempts}
    if extension == ".html":
        content = _viewable_static_html(content, source_url=state.get("source_url"))
    if extension == ".pdf":
        return _write_pdf_artifact(state, content=content, attempts=attempts)
    return _write_text_artifact(state, content=content, extension=extension, attempts=attempts)


def _route_after_write(state: FetchArtifactState) -> str:
    if state.get("created_path"):
        return "verify"
    if state.get("error") and int(state.get("write_attempts") or 0) < 2:
        return "retry"
    return "verify"


def _verify_artifact_node(state: FetchArtifactState) -> dict[str, object]:
    created_path = state.get("created_path")
    principal_id = state.get("principal_id")
    if not created_path:
        return {}
    descriptor = artifact_descriptor_for_path(Path(created_path), artifact_root=artifact_root_for_principal(principal_id))
    if descriptor is None:
        return {"artifact_paths": [], "error": f"Created artifact was not downloadable: {created_path}"}
    return {"artifact_paths": [descriptor.path], "error": None}


def _write_text_artifact(
    state: FetchArtifactState,
    *,
    content: str,
    extension: str,
    attempts: int,
) -> dict[str, object]:
    path = artifact_path_for_generated_workspace_file(
        principal_id=state.get("principal_id"),
        suffix=extension,
        stem=_safe_stem_for_prompt(state.get("prompt") or "fetched-content"),
    )
    registry = state.get("registry")
    runtime = state.get("runtime")
    if (
        registry is not None
        and hasattr(registry, "get_spec")
        and runtime is not None
        and getattr(runtime, "store", None) is not None
    ):
        result = invoke_tool_with_boundary_policy(
            runtime.store,
            ToolInvocation(
                invocation_id=f"fetch-artifact-file_write-{uuid4().hex}",
                tool_name="file_write",
                principal_id=state.get("principal_id") or "operator",
                arguments={"path": str(path), "content": content},
                capsule_id=None,
            ),
            registry=registry,
        )
        tool_results = [*list(state.get("tool_results") or []), result]
        if normalize_tool_status(result.status) != "completed":
            return {
                "tool_results": tool_results,
                "write_attempts": attempts,
                "error": result.error or "file_write failed",
            }
        output = result.output if isinstance(result.output, dict) else {}
        return {
            "tool_results": tool_results,
            "created_path": str(output.get("path") or path),
            "write_attempts": attempts,
            "error": None,
        }
    try:
        path.write_text(content, encoding="utf-8")
        return {"created_path": str(path), "write_attempts": attempts, "error": None}
    except Exception as exc:
        return {"write_attempts": attempts, "error": str(exc) or exc.__class__.__name__}


def _write_pdf_artifact(state: FetchArtifactState, *, content: str, attempts: int) -> dict[str, object]:
    registry = state.get("registry")
    runtime = state.get("runtime")
    if registry is None or not hasattr(registry, "get_spec") or runtime is None or getattr(runtime, "store", None) is None:
        return {"write_attempts": attempts, "error": "pdf_create requires an active tool registry"}
    path = artifact_path_for_generated_workspace_file(
        principal_id=state.get("principal_id"),
        suffix=".pdf",
        stem=_safe_stem_for_prompt(state.get("prompt") or "fetched-report"),
    )
    result = invoke_tool_with_boundary_policy(
        runtime.store,
        ToolInvocation(
            invocation_id=f"fetch-artifact-pdf_create-{uuid4().hex}",
            tool_name="pdf_create",
            principal_id=state.get("principal_id") or "operator",
            arguments={
                "output_path": str(path),
                "text_pages": [_trim_pdf_page_text(content)],
                "title": _pdf_title_for_prompt(state.get("prompt") or "Fetched report"),
            },
            capsule_id=None,
        ),
        registry=registry,
    )
    tool_results = [*list(state.get("tool_results") or []), result]
    if normalize_tool_status(result.status) != "completed":
        return {"tool_results": tool_results, "write_attempts": attempts, "error": result.error or "pdf_create failed"}
    output = result.output if isinstance(result.output, dict) else {}
    return {
        "tool_results": tool_results,
        "created_path": str(output.get("artifact_path") or output.get("path") or path),
        "write_attempts": attempts,
        "error": None,
    }


def _latest_completed_tool_result(
    tool_results: list[ToolResult] | tuple[ToolResult, ...],
    *,
    tool_name: str,
) -> ToolResult | None:
    for result in reversed(tool_results):
        if result.tool_name == tool_name and normalize_tool_status(result.status) == "completed":
            return result
    return None


def _web_fetch_body_for_artifact(result: ToolResult, *, extension: str) -> str | None:
    output = result.output if isinstance(result.output, dict) else {}
    if extension == ".html":
        keys = ("body", "raw_body", "html")
    else:
        keys = ("body", "raw_body", "text", "html")
    for key in keys:
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _terminal_output_for_artifact(result: ToolResult) -> str | None:
    output = result.output if isinstance(result.output, dict) else {}
    for key in ("stdout", "stderr"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _viewable_static_html(source_html: str, *, source_url: str | None = None) -> str:
    body = _SCRIPT_TAG_RE.sub("", source_html)
    base_tag = f'<base href="{html.escape(source_url, quote=True)}">' if source_url else ""
    notice = (
        "<meta name=\"nullion-source\" content=\"Fetched source response; scripts removed for safe local viewing\">"
        "<style>body:before{content:'Saved source response - scripts disabled for local viewing';"
        "display:block;padding:10px 12px;margin:0 0 12px 0;background:#111827;color:#e5e7eb;"
        "font:13px -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;}</style>"
    )
    head_insert = base_tag + notice
    if re.search(r"<head\b[^>]*>", body, flags=re.IGNORECASE):
        return re.sub(r"(<head\b[^>]*>)", r"\1" + head_insert, body, count=1, flags=re.IGNORECASE)
    if re.search(r"<html\b[^>]*>", body, flags=re.IGNORECASE):
        return re.sub(r"(<html\b[^>]*>)", r"\1<head>" + head_insert + "</head>", body, count=1, flags=re.IGNORECASE)
    return "<!doctype html><html><head>" + head_insert + "</head><body><pre>" + html.escape(source_html) + "</pre></body></html>"


def _safe_stem_for_prompt(prompt: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(prompt or "").strip().lower()).strip(".-")
    return cleaned[:48] or "fetched-artifact"


def _pdf_title_for_prompt(prompt: str) -> str:
    return " ".join(str(prompt or "Fetched report").split())[:120] or "Fetched report"


def _trim_pdf_page_text(text: str) -> str:
    return str(text or "").strip()[:24_000]


__all__ = ["FetchArtifactWorkflowResult", "FetchArtifactState", "run_fetch_artifact_workflow"]

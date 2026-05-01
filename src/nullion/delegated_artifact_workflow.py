"""LangGraph finalization for delegated task artifacts."""

from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import TypedDict

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from nullion.artifacts import (
    artifact_descriptor_for_path,
    artifact_path_for_generated_workspace_file,
    artifact_root_for_principal,
)
from nullion.attachment_format_graph import plan_attachment_format
from nullion.task_queue import TaskGroup


logger = logging.getLogger(__name__)

_TEXT_ARTIFACT_EXTENSIONS = frozenset({".txt", ".md", ".csv", ".json", ".yaml", ".yml"})


class DelegatedArtifactState(TypedDict, total=False):
    group: TaskGroup
    requested_extension: str | None
    should_materialize: bool
    principal_id: str | None
    existing_artifacts: list[str]
    candidate_text: str
    created_artifacts: list[str]
    error: str | None
    write_attempts: int


def finalize_delegated_artifacts(group: TaskGroup) -> list[str]:
    """Materialize missing text artifacts for a completed mini-agent group."""

    existing = _created_artifacts_from_group_metadata(group)
    if existing:
        return existing
    try:
        final_state = _compiled_delegated_artifact_graph().invoke(
            {"group": group, "write_attempts": 0},
            config={"configurable": {"thread_id": f"delegated-artifacts:{group.group_id}"}},
        )
        created = list(final_state.get("created_artifacts") or [])
        if created:
            _record_created_artifacts(group, created)
        return created
    except Exception as exc:
        logger.debug("Delegated artifact finalization failed: %s", exc, exc_info=True)
        return []


@lru_cache(maxsize=1)
def _compiled_delegated_artifact_graph():
    graph = StateGraph(DelegatedArtifactState)
    graph.add_node("plan", _plan_requested_artifact)
    graph.add_node("validate_existing", _validate_existing_artifacts)
    graph.add_node("collect_content", _collect_candidate_content)
    graph.add_node("write_artifact", _write_missing_artifact)
    graph.add_edge(START, "plan")
    graph.add_edge("plan", "validate_existing")
    graph.add_edge("validate_existing", "collect_content")
    graph.add_edge("collect_content", "write_artifact")
    graph.add_conditional_edges("write_artifact", _route_after_write, {"retry": "write_artifact", END: END})
    return graph.compile(checkpointer=MemorySaver())


def _plan_requested_artifact(state: DelegatedArtifactState) -> dict[str, object]:
    group = state["group"]
    extension = plan_attachment_format(group.original_message).extension
    should_materialize = extension in _TEXT_ARTIFACT_EXTENSIONS
    return {
        "requested_extension": extension,
        "should_materialize": should_materialize,
        "principal_id": _principal_id_for_group(group),
        "created_artifacts": [],
        "existing_artifacts": [],
        "error": None,
    }


def _validate_existing_artifacts(state: DelegatedArtifactState) -> dict[str, object]:
    if not state.get("should_materialize"):
        return {}
    group = state["group"]
    principal_id = state.get("principal_id")
    artifact_root = artifact_root_for_principal(principal_id)
    requested_extension = state.get("requested_extension")
    existing: list[str] = []
    for task in group.tasks:
        result = task.result
        if result is None:
            continue
        for raw_path in result.artifacts or []:
            path = Path(str(raw_path))
            descriptor = artifact_descriptor_for_path(path, artifact_root=artifact_root)
            if descriptor is None:
                continue
            if requested_extension and path.suffix.lower() != requested_extension:
                continue
            existing.append(str(path.expanduser().resolve()))
    return {"existing_artifacts": list(dict.fromkeys(existing))}


def _collect_candidate_content(state: DelegatedArtifactState) -> dict[str, object]:
    if not state.get("should_materialize") or state.get("existing_artifacts"):
        return {}
    group = state["group"]
    success_sections: list[str] = []
    fallback_sections: list[str] = []
    for task in group.tasks:
        result = task.result
        if result is None:
            continue
        content = _result_content(result.output, result.context_out)
        if not content:
            continue
        title = str(task.title or "Task").strip()
        section = f"{title}\n{'=' * min(len(title), 72)}\n{content}"
        if result.status == "success":
            success_sections.append(section)
        else:
            fallback_sections.append(section)
    sections = success_sections or fallback_sections
    if not sections:
        return {}
    header = f"Original request: {group.original_message.strip()}"
    return {"candidate_text": f"{header}\n\n" + "\n\n".join(sections).strip() + "\n"}


def _write_missing_artifact(state: DelegatedArtifactState) -> dict[str, object]:
    attempts = int(state.get("write_attempts") or 0) + 1
    if (
        not state.get("should_materialize")
        or state.get("existing_artifacts")
        or not str(state.get("candidate_text") or "").strip()
    ):
        return {"write_attempts": attempts}
    principal_id = state.get("principal_id")
    extension = str(state.get("requested_extension") or ".txt")
    try:
        path = artifact_path_for_generated_workspace_file(
            principal_id=principal_id,
            suffix=extension,
            stem=_artifact_stem_for_request(state["group"].original_message),
        )
        path.write_text(str(state["candidate_text"]), encoding="utf-8")
        artifact_root = artifact_root_for_principal(principal_id)
        if artifact_descriptor_for_path(path, artifact_root=artifact_root) is None:
            return {"error": f"created artifact was not deliverable: {path}", "write_attempts": attempts}
        return {"created_artifacts": [str(path)], "error": None, "write_attempts": attempts}
    except Exception as exc:
        return {"error": str(exc) or exc.__class__.__name__, "write_attempts": attempts}


def _route_after_write(state: DelegatedArtifactState) -> str:
    if state.get("created_artifacts"):
        return END
    if state.get("error") and int(state.get("write_attempts") or 0) < 2:
        return "retry"
    return END


def _principal_id_for_group(group: TaskGroup) -> str | None:
    for task in group.tasks:
        if task.principal_id:
            return task.principal_id
    return group.conversation_id


def _result_content(output: object, context_out: object) -> str:
    text = str(output or "").strip()
    if text:
        return text
    if context_out is None:
        return ""
    if isinstance(context_out, str):
        return context_out.strip()
    try:
        return json.dumps(context_out, indent=2, ensure_ascii=False, default=str).strip()
    except TypeError:
        return str(context_out).strip()


def _artifact_stem_for_request(message: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", str(message or "").lower()).strip("-")
    text = re.sub(r"-+", "-", text)
    return (text[:56].strip("-") or "delegated-result")


def _created_artifacts_from_group_metadata(group: TaskGroup) -> list[str]:
    metadata = getattr(group, "planner_metadata", {}) or {}
    raw_paths = metadata.get("created_artifacts")
    if not isinstance(raw_paths, list):
        return []
    principal_id = _principal_id_for_group(group)
    artifact_root = artifact_root_for_principal(principal_id)
    paths: list[str] = []
    for raw_path in raw_paths:
        if not isinstance(raw_path, str):
            continue
        descriptor = artifact_descriptor_for_path(Path(raw_path), artifact_root=artifact_root)
        if descriptor is not None:
            paths.append(descriptor.path)
    return paths


def _record_created_artifacts(group: TaskGroup, paths: list[str]) -> None:
    try:
        metadata = getattr(group, "planner_metadata", None)
        if not isinstance(metadata, dict):
            return
        existing = [path for path in metadata.get("created_artifacts", []) if isinstance(path, str)]
        metadata["created_artifacts"] = list(dict.fromkeys([*existing, *paths]))
    except Exception:
        logger.debug("Could not record delegated artifact metadata", exc_info=True)


__all__ = [
    "DelegatedArtifactState",
    "finalize_delegated_artifacts",
]

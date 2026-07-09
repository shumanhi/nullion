"""LangGraph-backed orchestration for messaging turns."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from html import unescape
from pathlib import Path
import re
import inspect
import logging
from typing import Any, Callable, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.approval_markers import (
    ORPHAN_APPROVAL_REQUEST_REPLY,
    split_tool_approval_marker,
    strip_tool_approval_marker,
)
from nullion.attachment_format_graph import VALID_ATTACHMENT_EXTENSIONS
from nullion import messaging_adapters as adapters
from nullion.response_fulfillment_contract import strip_unselected_artifact_references


logger = logging.getLogger(__name__)

_INTERNAL_STATE_ATTACHMENT_SUFFIXES = frozenset({".json", ".jsonl", ".db", ".sqlite", ".sqlite3"})
_IMAGE_ATTACHMENT_SUFFIXES = frozenset({".apng", ".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"})
_LOCAL_DISCOVERY_TOOL_NAMES = frozenset({"file_search", "file_read", "workspace_summary", "request_tool_scope"})
_SCHEDULER_MUTATION_TOOL_NAMES = frozenset({"create_cron", "delete_cron", "toggle_cron", "update_cron"})
_LOCAL_DISCOVERY_CLEANUP_TOOL_NAMES = frozenset({
    "archive_create",
    "document_create",
    "file_patch",
    "file_write",
    "html_create",
    "pdf_create",
    "presentation_create",
    "spreadsheet_create",
})


@dataclass(slots=True)
class PlatformChatRequest:
    platform: str
    text: str
    conversation_id: str | None = None
    chat_id: str | None = None
    turn_id: str | None = None
    attachments: list[dict[str, str]] | None = None
    settings: object | None = None
    request_id: str | None = None
    message_id: str | None = None
    model_client: object | None = None
    agent_orchestrator: object | None = None
    service: object | None = None
    activity_callback: Callable[[dict[str, str]], None] | None = None
    text_delta_callback: Callable[[str], None] | None = None
    append_activity_trace: bool = True
    allow_mini_agents: bool = False
    turn_dispatch_decision: object | None = None
    cancellation_checker: Callable[[], bool] | None = None
    conversation_ingress_id: str | None = None
    reply_context: dict[str, object] | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PlatformChatResponse:
    text: str | None
    type: str = "message"
    thinking: str = ""
    artifacts: list[dict[str, object]] = field(default_factory=list)
    artifact_paths: list[str] = field(default_factory=list)
    artifact_candidate_paths: list[str] = field(default_factory=list)
    tool_results: list[object] = field(default_factory=list)
    suspended_for_approval: bool = False
    approval_id: str | None = None
    reply_already_sent: bool = False
    mini_agent_dispatch: bool = False
    task_group_id: str | None = None
    planner_status_text: str | None = None
    progress_status_text: str | None = None
    planner_status_owned_by_background: bool = False


def platform_chat_id(platform: str, conversation_id: str | None = None, chat_id: str | None = None) -> str | None:
    """Return the canonical chat id passed into the shared chat operator."""
    if chat_id is not None and str(chat_id).strip():
        return str(chat_id).strip()
    normalized_platform = str(platform or "").strip().lower() or "telegram"
    raw_conversation = str(conversation_id or "").strip()
    if not raw_conversation:
        return None
    if ":" in raw_conversation:
        return raw_conversation
    if normalized_platform == "web":
        return f"web:{raw_conversation}"
    if normalized_platform in {"telegram", "slack", "discord"}:
        return raw_conversation
    return f"{normalized_platform}:{raw_conversation}"


def _coerce_stored_tool_results(raw_results: object) -> list[object]:
    if not isinstance(raw_results, list):
        return []
    from nullion.tools import ToolResult

    results: list[object] = []
    for item in raw_results:
        if isinstance(item, ToolResult):
            results.append(item)
            continue
        if not isinstance(item, dict):
            continue
        output = item.get("output")
        results.append(
            ToolResult(
                str(item.get("invocation_id") or ""),
                str(item.get("tool_name") or item.get("name") or ""),
                str(item.get("status") or ""),
                output if isinstance(output, dict) else {},
                str(item.get("error")) if item.get("error") is not None else None,
            )
        )
    return results


def _path_identity_variants(path: object) -> tuple[str, ...]:
    raw_path = str(path or "").strip()
    if not raw_path:
        return ()
    path_obj = Path(raw_path).expanduser()
    variants = [str(path_obj)]
    for marker in ("workspaces", "artifacts"):
        if marker not in path_obj.parts:
            continue
        marker_index = path_obj.parts.index(marker)
        if marker_index < len(path_obj.parts) - 1:
            variants.append(str(Path(*path_obj.parts[marker_index:])))
    try:
        variants.append(str(path_obj.resolve()))
    except OSError:
        pass
    return tuple(dict.fromkeys(variants))


_TERMINAL_STDOUT_PATH_RE = re.compile(
    r"(?<![\w./~-])(?P<path>(?:~|/)[^\s`'\"<>]+?\.[A-Za-z0-9]{1,16})(?![\w/-])"
)
_BARE_FILENAME_LISTING_RE = re.compile(r"^[^\s/\\\x00]{1,255}\.[A-Za-z0-9]{1,16}$")
_RELATIVE_PATH_LISTING_RE = re.compile(r"^(?![A-Za-z][A-Za-z0-9+.-]*:)[^\s\x00]{1,512}\.[A-Za-z0-9]{1,16}$")


def _pathlike_stdout_listing_lines(text: object) -> tuple[str, ...]:
    paths: list[str] = []
    for raw_line in str(text or "").splitlines():
        candidate = raw_line.strip().strip("`'\"<>")
        if not candidate or "\x00" in candidate:
            continue
        expanded = Path(candidate.removeprefix("file:")).expanduser()
        if candidate.startswith(("~", "/", "file:")) or expanded.is_absolute():
            paths.append(candidate.removeprefix("file:"))
        elif _BARE_FILENAME_LISTING_RE.match(candidate):
            paths.append(candidate)
        elif ("/" in candidate or "\\" in candidate) and _RELATIVE_PATH_LISTING_RE.match(candidate):
            paths.append(candidate)
    return tuple(dict.fromkeys(paths))


def _terminal_stdout_paths_from_tool_results(tool_results: list[object]) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "") != "terminal_exec":
            continue
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        stdout = output.get("stdout")
        if not isinstance(stdout, str) or not stdout.strip():
            continue
        lines = [line.strip().strip("`'\"<>") for line in stdout.splitlines() if line.strip()]
        if lines and len(_pathlike_stdout_listing_lines(stdout)) == len(lines):
            paths.extend(_pathlike_stdout_listing_lines(stdout))
            continue
        for match in _TERMINAL_STDOUT_PATH_RE.finditer(stdout):
            raw_path = match.group("path").strip().rstrip(".,;:)]}")
            if raw_path:
                paths.append(raw_path)
    return tuple(dict.fromkeys(paths))


def _file_search_discovery_paths_from_tool_results(tool_results: list[object]) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "") != "file_search":
            continue
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        matches = output.get("matches")
        if isinstance(matches, (list, tuple)):
            for match in matches:
                if isinstance(match, str) and match.strip():
                    paths.append(match)
                elif isinstance(match, dict) and isinstance(match.get("path"), str) and match["path"].strip():
                    paths.append(match["path"])
        match_details = output.get("match_details")
        if isinstance(match_details, (list, tuple)):
            for match in match_details:
                if isinstance(match, dict) and isinstance(match.get("path"), str) and match["path"].strip():
                    paths.append(match["path"])
    return tuple(dict.fromkeys(paths))


def _requested_output_filenames(*texts: str | None) -> set[str]:
    try:
        from nullion.chat_operator import _requested_output_filenames_from_texts

        return {name.lower() for name in _requested_output_filenames_from_texts(*texts)}
    except Exception:
        return set()


def _single_filename_delivery_descriptor(text: str | None) -> set[str]:
    value = str(text or "").strip()
    if not value or "\n" in value:
        return set()
    filenames = _requested_output_filenames(value)
    if len(filenames) != 1:
        return set()
    filename = next(iter(filenames))
    return {filename} if value.strip("`'\"<> ").lower() == filename else set()


def _requested_attachment_output_extensions(text: str | None) -> tuple[str, ...]:
    try:
        from nullion.chat_operator import _requested_attachment_extensions

        return tuple(
            _requested_attachment_extensions(
                str(text or ""),
                model_client=None,
                allow_model_planning=False,
            )
        )
    except Exception:
        return ()


def _filter_internal_state_artifact_paths(
    artifact_paths: list[str],
    *,
    request_text: str | None = None,
) -> list[str]:
    if not artifact_paths:
        return artifact_paths
    requested_extensions = set(_requested_attachment_output_extensions(request_text))
    try:
        from nullion.artifacts import is_unrequested_internal_sidecar_artifact
    except Exception:
        is_unrequested_internal_sidecar_artifact = None  # type: ignore[assignment]
    return [
        path
        for path in artifact_paths
        if (
            (
                Path(str(path or "")).suffix.lower() not in _INTERNAL_STATE_ATTACHMENT_SUFFIXES
                or Path(str(path or "")).suffix.lower() in requested_extensions
            )
            and not (
                is_unrequested_internal_sidecar_artifact is not None
                and is_unrequested_internal_sidecar_artifact(path, requested_extensions=requested_extensions)
            )
        )
    ]


def _visible_html_text_length(path: str) -> int:
    try:
        text = Path(path).read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return 0
    text = re.sub(r"(?is)<(script|style|svg|canvas)\b.*?</\1>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return len(" ".join(unescape(text).split()))


def _filter_sparse_duplicate_html_artifacts(artifact_paths: list[str]) -> list[str]:
    html_paths = [
        path
        for path in artifact_paths
        if Path(str(path or "")).suffix.lower() in {".html", ".htm"}
    ]
    if len(html_paths) < 2:
        return artifact_paths
    non_visual_helper_paths = [
        path for path in html_paths if not _is_visual_helper_html_artifact_path(path)
    ]
    if non_visual_helper_paths:
        non_visual_helper_identities = {
            identity
            for path in non_visual_helper_paths
            for identity in _path_identity_variants(path)
        }
        filtered = [
            path
            for path in artifact_paths
            if Path(str(path or "")).suffix.lower() not in {".html", ".htm"}
            or not _is_visual_helper_html_artifact_path(path)
            or _path_matches_any_identity(path, non_visual_helper_identities)
        ]
        if any(Path(str(path or "")).suffix.lower() in {".html", ".htm"} for path in filtered):
            artifact_paths = filtered
            html_paths = [
                path
                for path in artifact_paths
                if Path(str(path or "")).suffix.lower() in {".html", ".htm"}
            ]
            if len(html_paths) < 2:
                return artifact_paths
    substantive_html_paths = {
        path for path in html_paths if _visible_html_text_length(path) >= 40
    }
    if not substantive_html_paths:
        return artifact_paths
    return [
        path
        for path in artifact_paths
        if Path(str(path or "")).suffix.lower() not in {".html", ".htm"}
        or path in substantive_html_paths
    ]


def _is_visual_helper_html_artifact_path(path: object) -> bool:
    stem = Path(str(path or "")).stem.casefold()
    return stem.endswith("_visual") or stem.endswith("-visual") or stem.endswith(".visual")


def _path_matches_any_identity(path: str, identities: set[str]) -> bool:
    return any(identity in identities for identity in _path_identity_variants(path))


def _scoped_or_requested_artifact_extensions(
    request_text: str | None,
    tool_results: list[object],
) -> tuple[str, ...]:
    extensions: list[str] = []
    try:
        from nullion.response_fulfillment_contract import scoped_artifact_extensions_from_tool_results

        extensions.extend(scoped_artifact_extensions_from_tool_results(tool_results))  # type: ignore[arg-type]
    except Exception:
        pass
    if not extensions:
        extensions.extend(_requested_attachment_output_extensions(request_text))
    normalized: list[str] = []
    for raw_extension in extensions:
        extension = str(raw_extension or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        if extension == ".htm":
            extension = ".html"
        if extension not in normalized:
            normalized.append(extension)
    return tuple(normalized)


def _reply_media_delivery_paths(
    stored_reply: str | None,
    *,
    request_text: str | None,
    tool_results: list[object],
) -> list[str]:
    if not stored_reply:
        return []
    requested_extensions = set(_scoped_or_requested_artifact_extensions(request_text, tool_results))
    try:
        from nullion.artifacts import is_unrequested_internal_sidecar_artifact
    except Exception:
        is_unrequested_internal_sidecar_artifact = None  # type: ignore[assignment]
    supporting_identities = _supporting_reply_media_identities_from_tool_results(tool_results)
    suppress_supporting_media = _tool_results_have_non_image_delivery_artifact(tool_results)
    paths: list[str] = []
    for candidate in _media_paths_from_reply_text(stored_reply):
        path = Path(candidate).expanduser()
        suffix = path.suffix.lower()
        if suffix == ".htm":
            suffix = ".html"
        if not suffix:
            continue
        if suppress_supporting_media and _path_matches_any_identity(str(path), supporting_identities):
            continue
        if suffix in _IMAGE_ATTACHMENT_SUFFIXES and suffix not in requested_extensions:
            continue
        if is_unrequested_internal_sidecar_artifact is not None and is_unrequested_internal_sidecar_artifact(
            path,
            requested_extensions=requested_extensions,
        ):
            continue
        try:
            if not path.is_file() or path.stat().st_size <= 0:
                continue
        except OSError:
            continue
        paths.append(str(path))
    return list(dict.fromkeys(paths))


def _tool_results_have_non_image_delivery_artifact(tool_results: list[object]) -> bool:
    if not tool_results:
        return False
    try:
        from nullion.artifacts import ARTIFACT_DELIVERY_ROLES, artifact_paths_from_output_descriptors
    except Exception:
        ARTIFACT_DELIVERY_ROLES = frozenset({"deliverable", "deliver_receipt", "verify"})  # type: ignore[assignment]
        artifact_paths_from_output_descriptors = None  # type: ignore[assignment]
    structured_artifact_tools = {
        "archive_create",
        "document_create",
        "file_write",
        "html_create",
        "pdf_create",
        "pdf_edit",
        "presentation_create",
        "spreadsheet_create",
    }
    for result in tool_results:
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        paths: list[str] = []
        if artifact_paths_from_output_descriptors is not None:
            try:
                paths.extend(artifact_paths_from_output_descriptors(output, roles=ARTIFACT_DELIVERY_ROLES))
            except Exception:
                pass
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        if tool_name in structured_artifact_tools:
            paths.extend(_artifact_path_values_from_output(output))
        for path in paths:
            suffix = Path(str(path or "")).suffix.lower()
            if suffix == ".htm":
                suffix = ".html"
            if suffix and suffix not in _IMAGE_ATTACHMENT_SUFFIXES:
                return True
    return False


def _supporting_reply_media_identities_from_tool_results(tool_results: list[object]) -> set[str]:
    identities: set[str] = set()
    for path in _supporting_artifact_candidate_paths_from_tool_results(tool_results):
        identities.update(_path_identity_variants(path))
    for result in tool_results:
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        descriptors = output.get("artifact_descriptors")
        if not isinstance(descriptors, list):
            continue
        for descriptor in descriptors:
            if not isinstance(descriptor, dict):
                continue
            role = str(descriptor.get("role") or "").strip().lower()
            if role in {"deliverable", "deliver_receipt", "verify"}:
                continue
            descriptor_path = str(descriptor.get("path") or descriptor.get("artifact_path") or "").strip()
            if descriptor_path:
                identities.update(_path_identity_variants(descriptor_path))
    return identities


def _merge_reply_media_delivery_paths(
    artifact_paths: list[str],
    *,
    stored_reply: str | None,
    request_text: str | None,
    tool_results: list[object],
) -> list[str]:
    reply_paths = _reply_media_delivery_paths(
        stored_reply,
        request_text=request_text,
        tool_results=tool_results,
    )
    if not reply_paths:
        return artifact_paths
    return list(dict.fromkeys([*artifact_paths, *reply_paths]))


def _paths_not_selected(paths: list[str], selected_paths: list[str]) -> list[str]:
    selected_identities = {
        identity
        for path in selected_paths
        for identity in _path_identity_variants(path)
    }
    if not selected_identities:
        return list(paths)
    return [
        path
        for path in paths
        if not _path_matches_any_identity(path, selected_identities)
    ]


def _filter_artifact_paths_for_embedded_media_contract(
    artifact_paths: list[str],
    tool_results: list[object],
) -> list[str]:
    if not artifact_paths or not tool_results:
        return artifact_paths
    try:
        from nullion.response_fulfillment_contract import (
            artifact_completed_embedded_media_paths,
            artifact_media_required_extensions,
            normalize_artifact_media_required_extensions,
        )
    except Exception:
        return artifact_paths
    required_extensions = set(artifact_media_required_extensions(tool_results))  # type: ignore[arg-type]
    if not required_extensions:
        return artifact_paths

    def path_satisfies_embedded_media_requirement(path: str) -> bool:
        suffix = Path(str(path or "")).suffix.lower()
        if suffix == ".htm":
            suffix = ".html"
        if suffix == ".html":
            path_obj = Path(str(path or "")).expanduser()
            try:
                return path_obj.is_file() and path_obj.stat().st_size > 0
            except OSError:
                return False
        return any(identity in completed_media_identities for identity in _path_identity_variants(path))

    media_scope_package_extensions: set[str] = set()
    media_scope_keys = {
        "required_embedded_media_extensions",
        "embedded_media_artifact_extensions",
        "media_required_artifact_extensions",
    }
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "") != "request_tool_scope":
            continue
        if str(getattr(result, "status", "") or "").lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        if not any(normalize_artifact_media_required_extensions(output.get(key)) for key in media_scope_keys):
            continue
        for key in ("artifact_extensions", "requested_artifact_extensions"):
            media_scope_package_extensions.update(
                normalize_artifact_media_required_extensions(output.get(key))
            )
    completed_media_paths = [
        str(path)
        for path in artifact_completed_embedded_media_paths(tool_results)  # type: ignore[arg-type]
        if str(path or "").strip()
    ]
    completed_media_identities = {
        identity
        for path in completed_media_paths
        for identity in _path_identity_variants(path)
    }
    completed_media_suffixes = {
        Path(path).suffix.lower()
        for path in completed_media_paths
        if str(path or "").strip()
    }
    source_role_identities: set[str] = set()
    has_non_image_artifact = any(
        Path(str(path or "")).suffix.lower() not in _IMAGE_ATTACHMENT_SUFFIXES
        for path in artifact_paths
        if str(path or "").strip()
    )
    if has_non_image_artifact:
        for value in _supporting_artifact_candidate_paths_from_tool_results(tool_results):
            source_role_identities.update(_path_identity_variants(value))
    for result in tool_results:
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        if has_non_image_artifact and tool_name == "browser_screenshot":
            for value in _artifact_path_values_from_output(output):
                source_role_identities.update(_path_identity_variants(value))
            continue
        descriptors = output.get("artifact_descriptors")
        if not isinstance(descriptors, list):
            continue
        for descriptor in descriptors:
            if not isinstance(descriptor, dict):
                continue
            if str(descriptor.get("role") or "").strip().lower() != "source":
                continue
            descriptor_path = str(descriptor.get("path") or "").strip()
            if descriptor_path:
                source_role_identities.update(_path_identity_variants(descriptor_path))
    required_suffix_paths = [
        path
        for path in artifact_paths
        if Path(str(path or "")).suffix.lower() in required_extensions
    ]
    filtered_paths: list[str] = []
    for path in artifact_paths:
        suffix = Path(str(path or "")).suffix.lower()
        if source_role_identities and _path_matches_any_identity(path, source_role_identities):
            continue
        if suffix in required_extensions:
            if completed_media_identities and suffix not in completed_media_suffixes:
                continue
            if suffix == ".html" and not path_satisfies_embedded_media_requirement(path):
                continue
            filtered_paths.append(path)
            continue
        if media_scope_package_extensions and suffix not in media_scope_package_extensions:
            continue
        filtered_paths.append(path)
    if filtered_paths:
        return list(dict.fromkeys(filtered_paths))
    if completed_media_identities:
        completed_required_paths = [
            path
            for path in required_suffix_paths
            if _path_matches_any_identity(path, completed_media_identities)
        ]
        if completed_required_paths:
            return list(dict.fromkeys(completed_required_paths))
    if required_suffix_paths:
        return list(dict.fromkeys(required_suffix_paths))
    return artifact_paths


def _filter_artifact_paths_for_tool_scope_contract(
    artifact_paths: list[str],
    tool_results: list[object],
) -> list[str]:
    if not artifact_paths or not tool_results:
        return artifact_paths
    try:
        from nullion.artifacts import is_unrequested_internal_sidecar_artifact
        from nullion.response_fulfillment_contract import scoped_artifact_extensions_from_tool_results
    except Exception:
        return artifact_paths
    requested_extensions = tuple(scoped_artifact_extensions_from_tool_results(tool_results))  # type: ignore[arg-type]
    if not requested_extensions:
        return artifact_paths
    requested = {
        ".html" if str(extension or "").strip().lower() == ".htm" else str(extension or "").strip().lower()
        for extension in requested_extensions
        if str(extension or "").strip()
    }
    filtered = [
        path
        for path in artifact_paths
        if (
            (".html" if Path(str(path or "")).suffix.lower() == ".htm" else Path(str(path or "")).suffix.lower())
            in requested
        )
        and not is_unrequested_internal_sidecar_artifact(
            path,
            requested_extensions=requested,
        )
    ]
    return list(dict.fromkeys(filtered)) or artifact_paths


def _filter_last_mile_helper_artifact_paths(
    artifact_paths: list[str],
    tool_results: list[object],
) -> list[str]:
    if not artifact_paths or not tool_results:
        return artifact_paths
    helper_identities: set[str] = set()
    helper_suffixes: set[str] = set()
    normal_suffixes: set[str] = set()
    for result in tool_results:
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        paths = _artifact_path_values_from_output(output)
        if output.get("last_mile_materialized") is True:
            for path in paths:
                suffix = Path(str(path or "")).suffix.lower()
                if suffix:
                    helper_suffixes.add(suffix)
                helper_identities.update(_path_identity_variants(path))
        else:
            for path in paths:
                suffix = Path(str(path or "")).suffix.lower()
                if suffix:
                    normal_suffixes.add(suffix)
    if not helper_identities or not (helper_suffixes & normal_suffixes):
        return artifact_paths
    return [
        path
        for path in artifact_paths
        if not (
            Path(str(path or "")).suffix.lower() in normal_suffixes
            and _path_matches_any_identity(path, helper_identities)
        )
    ]


def _line_is_artifact_filename_only(line: str, names: set[str]) -> bool:
    stripped = str(line or "").strip()
    if not stripped:
        return False
    stripped = re.sub(r"^(?:[-*•]|\d+[.)])\s+", "", stripped).strip()
    stripped = stripped.strip("`'\" ")
    return stripped in names


def _artifact_delivery_label_for_path(path: str) -> str:
    suffix = Path(str(path or "")).suffix.lower()
    return {
        ".csv": "CSV",
        ".docx": "DOCX document",
        ".html": "HTML file",
        ".htm": "HTML file",
        ".json": "JSON",
        ".pdf": "PDF",
        ".pptx": "PPTX deck",
        ".svg": "SVG",
        ".txt": "text file",
        ".xlsx": "XLSX workbook",
    }.get(suffix, suffix.lstrip(".").upper() or "file")


def _strip_suppressed_artifact_names_from_line(line: str, suppressed_names: set[str]) -> str | None:
    if not suppressed_names or not any(name in line for name in suppressed_names):
        return line
    marker = re.search(r"\bFiles:\s*", line)
    if not marker:
        return None if _line_is_artifact_filename_only(line, suppressed_names) else line
    prefix = line[: marker.end()]
    suffix = line[marker.end() :]
    terminal = "." if suffix.rstrip().endswith(".") else ""
    items = [
        item.strip()
        for item in suffix.rstrip(".").split(",")
        if item.strip()
    ]
    kept = [item for item in items if not any(name in item for name in suppressed_names)]
    if not kept:
        return None
    return f"{prefix}{', '.join(kept)}{terminal}"


def _strip_suppressed_artifact_labels_from_line(line: str, suppressed_labels: set[str]) -> str:
    if not suppressed_labels or "attached the requested artifacts:" not in line.lower():
        return line
    pattern = re.compile(
        r"(?P<prefix>attached the requested artifacts:\s*)(?P<labels>[^.\n]+)(?P<suffix>\.?)",
        re.IGNORECASE,
    )

    def _replace(match: re.Match[str]) -> str:
        labels = [label.strip() for label in match.group("labels").split(",") if label.strip()]
        kept = [
            label
            for label in labels
            if label.strip().casefold() not in {item.casefold() for item in suppressed_labels}
        ]
        if not kept:
            return match.group(0)
        return f"{match.group('prefix')}{', '.join(kept)}{match.group('suffix')}"

    return pattern.sub(_replace, line)


def _strip_unselected_artifact_filename_lines(
    text: str | None,
    *,
    selected_paths: list[str],
    candidate_paths: list[str],
) -> str | None:
    return strip_unselected_artifact_references(
        text,
        selected_paths=selected_paths,
        candidate_paths=candidate_paths,
    )


def _filter_local_discovery_artifact_paths(
    artifact_paths: list[str],
    tool_results: list[object],
    *,
    request_text: str | None = None,
    stored_reply: str | None = None,
) -> list[str]:
    if not artifact_paths or not tool_results:
        return artifact_paths
    if _tool_results_include_direct_existing_artifact_delivery(tool_results):
        return artifact_paths
    if _tool_results_are_unrequested_local_discovery_cleanup(
        tool_results,
        request_text=request_text,
    ):
        return []
    if _tool_results_have_requested_generated_artifact_path(
        tool_results,
        request_text=request_text,
        stored_reply=stored_reply,
    ):
        return artifact_paths
    if _tool_results_have_typed_deliverable_contract(tool_results):
        return artifact_paths
    requested_names = _requested_output_filenames(request_text, stored_reply)
    file_search_paths = _file_search_discovery_paths_from_tool_results(tool_results)
    terminal_paths = tuple(
        path
        for path in _terminal_stdout_paths_from_tool_results(tool_results)
        if Path(path).name.lower() not in requested_names
    )
    discovery_paths = tuple(dict.fromkeys([*file_search_paths, *terminal_paths]))
    if not discovery_paths:
        return artifact_paths
    discovery_identities = {
        identity
        for path in discovery_paths
        for identity in _path_identity_variants(path)
    }
    if not discovery_identities:
        return artifact_paths
    return [
        path
        for path in artifact_paths
        if not any(identity in discovery_identities for identity in _path_identity_variants(path))
    ]


def _tool_results_are_local_discovery_or_sidecar(
    tool_results: list[object],
    *,
    request_text: str | None = None,
    stored_reply: str | None = None,
    delivery_text: str | None = None,
) -> bool:
    if not tool_results:
        return False
    if _tool_results_include_direct_existing_artifact_delivery(tool_results):
        return False
    if _tool_results_are_unrequested_local_discovery_cleanup(
        tool_results,
        request_text=request_text,
    ):
        return True
    if _tool_results_have_requested_generated_artifact_path(
        tool_results,
        request_text=request_text,
        stored_reply=stored_reply,
        delivery_text=delivery_text,
    ):
        return False
    if _tool_results_have_typed_deliverable_contract(tool_results):
        return False
    try:
        from nullion.chat_operator import (
            _completed_tools_are_only_local_discovery_or_file_write,
            _local_discovery_paths_from_tool_results,
        )

        discovery_paths = _local_discovery_paths_from_tool_results(tool_results)  # type: ignore[arg-type]
        if not discovery_paths:
            return False
        return bool(_completed_tools_are_only_local_discovery_or_file_write(tool_results))  # type: ignore[arg-type]
    except Exception:
        return False


def _tool_scope_declares_requested_artifacts(tool_results: list[object]) -> bool:
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "").strip() != "request_tool_scope":
            continue
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        for key in (
            "artifact_extensions",
            "requested_artifact_extensions",
            "embedded_media_artifact_extensions",
            "required_embedded_media_extensions",
            "media_required_artifact_extensions",
        ):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                return True
            if isinstance(value, (list, tuple)) and any(str(item or "").strip() for item in value):
                return True
    return False


def _tool_results_are_unrequested_local_discovery_cleanup(
    tool_results: list[object],
    *,
    request_text: str | None = None,
) -> bool:
    if _tool_scope_declares_requested_artifacts(tool_results):
        return False
    if _tool_results_have_requested_generated_artifact_path(
        tool_results,
        request_text=request_text,
    ):
        return False
    saw_local_discovery = False
    saw_cleanup_artifact = False
    for result in tool_results:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status not in {"completed", "failed"}:
            continue
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        if tool_name in _LOCAL_DISCOVERY_TOOL_NAMES:
            saw_local_discovery = True
            continue
        output = getattr(result, "output", None)
        if status == "completed" and tool_name in _LOCAL_DISCOVERY_CLEANUP_TOOL_NAMES:
            if isinstance(output, dict) and _output_has_explicit_deliverable_descriptor(output):
                return False
            saw_cleanup_artifact = True
            continue
        if tool_name == "terminal_exec" and isinstance(output, dict):
            lines = [line.strip() for line in str(output.get("stdout") or "").splitlines() if line.strip()]
            if lines and len(_pathlike_stdout_listing_lines(output.get("stdout"))) == len(lines):
                saw_local_discovery = True
                continue
        return False
    return saw_local_discovery and saw_cleanup_artifact


def _output_has_explicit_deliverable_descriptor(output: dict[str, object]) -> bool:
    descriptors = output.get("artifact_descriptors")
    if not isinstance(descriptors, list):
        return False
    deliverable_roles = {"primary", "deliverable", "delivery", "attachment", "deliver_receipt", "verify"}
    for descriptor in descriptors:
        if not isinstance(descriptor, dict):
            continue
        role = str(descriptor.get("role") or "").strip().lower()
        if role in deliverable_roles:
            return True
    return False


def _tool_results_have_requested_generated_artifact_path(
    tool_results: list[object],
    *,
    request_text: str | None = None,
    stored_reply: str | None = None,
    delivery_text: str | None = None,
) -> bool:
    requested_names = _requested_output_filenames(request_text, stored_reply, delivery_text)
    if not requested_names:
        return False
    for result in tool_results:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status != "completed":
            continue
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        if tool_name not in {"file_write", "spreadsheet_create", "document_create", "pdf_create", "presentation_create"}:
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        if tool_name == "file_write" and output.get("requested_filename_alias") is True:
            continue
        for path in _artifact_path_values_from_output(output):
            if Path(str(path or "")).name.lower() in requested_names:
                return True
    return False


def _artifact_path_values_from_output(output: dict[str, object]) -> tuple[str, ...]:
    paths: list[str] = []
    for key in ("path", "artifact_path", "output_path"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            paths.append(value.strip())
    for key in ("paths", "artifact_paths", "artifacts"):
        value = output.get(key)
        if not isinstance(value, (list, tuple)):
            continue
        for item in value:
            if isinstance(item, str) and item.strip():
                paths.append(item.strip())
            elif isinstance(item, dict):
                for nested_key in ("path", "artifact_path", "output_path"):
                    nested_value = item.get(nested_key)
                    if isinstance(nested_value, str) and nested_value.strip():
                        paths.append(nested_value.strip())
    descriptors = output.get("artifact_descriptors")
    if isinstance(descriptors, list):
        for descriptor in descriptors:
            if not isinstance(descriptor, dict):
                continue
            descriptor_path = descriptor.get("path") or descriptor.get("artifact_path")
            if isinstance(descriptor_path, str) and descriptor_path.strip():
                paths.append(descriptor_path.strip())
    return tuple(dict.fromkeys(paths))


def _supporting_media_path_values_from_output(output: dict[str, object]) -> tuple[str, ...]:
    paths: list[str] = []
    for key in (
        "embedded_images",
        "source_image_paths",
        "image_paths",
        "embedded_screenshots",
        "source_screenshot_paths",
        "screenshot_paths",
        "optimized_image_paths",
    ):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            paths.append(value.strip())
        elif isinstance(value, (list, tuple)):
            paths.extend(str(item).strip() for item in value if isinstance(item, str) and item.strip())
    html_images = output.get("embedded_html_images")
    if isinstance(html_images, (list, tuple)):
        for item in html_images:
            if isinstance(item, dict):
                value = item.get("path") or item.get("source")
                if isinstance(value, str) and value.strip():
                    paths.append(value.strip())
            elif isinstance(item, str) and item.strip():
                paths.append(item.strip())
    return tuple(dict.fromkeys(paths))


def _supporting_artifact_candidate_paths_from_tool_results(tool_results: list[object]) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        paths.extend(_supporting_media_path_values_from_output(output))
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        if tool_name in {"browser_screenshot", "browser_image_collect", "file_download"}:
            paths.extend(_artifact_path_values_from_output(output))
    return tuple(dict.fromkeys(path for path in paths if str(path or "").strip()))


def _completed_file_write_html_artifact_paths(tool_results: list[object]) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "").strip() != "file_write":
            continue
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        for path in _artifact_path_values_from_output(output):
            if Path(str(path or "")).suffix.lower() in {".html", ".htm"}:
                paths.append(path)
    return tuple(dict.fromkeys(paths))


def _completed_file_write_artifact_paths(tool_results: list[object]) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "").strip() != "file_write":
            continue
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        paths.extend(_artifact_path_values_from_output(output))
    return tuple(dict.fromkeys(path for path in paths if str(path or "").strip()))


def _tool_results_have_typed_deliverable_contract(tool_results: list[object]) -> bool:
    if not tool_results:
        return False
    try:
        from nullion.artifacts import ARTIFACT_DELIVERY_ROLES, artifact_paths_from_output_descriptors
    except Exception:
        ARTIFACT_DELIVERY_ROLES = frozenset({"primary", "deliverable", "delivery", "attachment"})  # type: ignore[assignment]
        artifact_paths_from_output_descriptors = None  # type: ignore[assignment]

    structured_artifact_tools = frozenset({
        "archive_create",
        "document_create",
        "html_create",
        "image_generate",
        "pdf_create",
        "pdf_edit",
        "presentation_create",
        "spreadsheet_create",
    })
    for result in tool_results:
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        if artifact_paths_from_output_descriptors is not None:
            try:
                if artifact_paths_from_output_descriptors(output, roles=ARTIFACT_DELIVERY_ROLES):
                    return True
            except Exception:
                pass
        if tool_name not in structured_artifact_tools:
            continue
        for key in ("path", "artifact_path"):
            if isinstance(output.get(key), str) and str(output.get(key)).strip():
                return True
        for key in ("paths", "artifact_paths", "artifacts"):
            value = output.get(key)
            if isinstance(value, (list, tuple)) and any(
                isinstance(item, str) and item.strip()
                or isinstance(item, dict) and str(item.get("path") or item.get("artifact_path") or "").strip()
                for item in value
            ):
                return True
    return False


def _tool_results_include_direct_existing_artifact_delivery(tool_results: list[object]) -> bool:
    for result in tool_results:
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        if output.get("direct_existing_artifact") is not True:
            continue
        if str(output.get("action") or "").strip().lower() != "deliver":
            continue
        for key in ("path", "artifact_path", "output_path"):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                return True
        for key in ("matches", "paths", "artifact_paths", "artifacts"):
            values = output.get(key)
            if isinstance(values, str) and values.strip():
                return True
            if isinstance(values, (list, tuple)) and any(
                isinstance(item, str) and item.strip()
                or isinstance(item, dict)
                and str(item.get("path") or item.get("artifact_path") or item.get("output_path") or "").strip()
                for item in values
            ):
                return True
    return False


def _local_discovery_reply_from_tool_results(tool_results: list[object]) -> str | None:
    try:
        from nullion.chat_operator import _local_discovery_paths_from_tool_results

        paths = tuple(_local_discovery_paths_from_tool_results(tool_results))  # type: ignore[arg-type]
    except Exception:
        paths = ()
    if not paths:
        return None
    lines = [f"I found {len(paths)} file{'s' if len(paths) != 1 else ''}:"]
    for path in paths:
        name = Path(str(path or "")).name or str(path or "").strip()
        if name:
            lines.append(f"- {name}")
    return "\n".join(lines).strip() or None


def _completed_run_cron_receipt_reply(tool_results: list[object]) -> str | None:
    from nullion.tools import normalize_tool_status

    for result in reversed(tuple(tool_results or ())):
        if str(getattr(result, "tool_name", "") or "") != "run_cron":
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = getattr(result, "output", None)
        payload = output if isinstance(output, dict) else {}
        for key in ("message", "result_text", "text", "final_text", "summary", "result"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return "Manual scheduled task run started."
    return None


def _delivery_metadata_from_tool_results(tool_results: list[object]) -> dict[str, object]:
    if not tool_results:
        return {}
    metadata: dict[str, object] = {}
    try:
        from nullion.messaging_delivery_contract import (
            deferred_cron_dispatch_from_tool_results,
            foreground_reply_should_be_suppressed,
        )
    except Exception:
        return metadata
    try:
        dispatch = deferred_cron_dispatch_from_tool_results(tool_results)
    except Exception:
        dispatch = None
    if dispatch is not None:
        status_text = str(getattr(dispatch, "planner_status_text", "") or "")
        metadata.update(
            {
                "mini_agent_dispatch": True,
                "task_group_id": str(getattr(dispatch, "task_group_id", "") or ""),
                "planner_status_text": status_text,
                "progress_status_text": status_text,
                "planner_status_owned_by_background": True,
            }
        )
        if bool(getattr(dispatch, "should_suppress_foreground_reply", False)) and not status_text.strip():
            metadata["reply_already_sent"] = True
    try:
        foreground_suppressed = bool(foreground_reply_should_be_suppressed(tool_results))
    except Exception:
        foreground_suppressed = False
    if foreground_suppressed and not str(metadata.get("planner_status_text") or "").strip():
        metadata["reply_already_sent"] = True
    return metadata


def _delivery_metadata_from_reply(reply: object) -> dict[str, object]:
    metadata: dict[str, object] = {}
    if bool(getattr(reply, "reply_already_sent", False)):
        metadata["reply_already_sent"] = True
    if bool(getattr(reply, "mini_agent_dispatch", False)):
        metadata["mini_agent_dispatch"] = True
    for key in (
        "task_group_id",
        "planner_status_text",
        "progress_status_text",
    ):
        value = str(getattr(reply, key, "") or "").strip()
        if value:
            metadata[key] = value
    if bool(getattr(reply, "planner_status_owned_by_background", False)):
        metadata["planner_status_owned_by_background"] = True
    return metadata


def _latest_stored_turn_delivery_evidence(
    runtime: object,
    conversation_id: str | None,
    *,
    request_text: str | None = None,
) -> tuple[str | None, list[str], list[object], list[str]]:
    if not conversation_id:
        return None, [], [], []
    store = getattr(runtime, "store", runtime)
    list_events = getattr(store, "list_recent_conversation_events", None)
    if not callable(list_events):
        return None, [], [], []
    try:
        events = list_events(conversation_id, event_type="conversation.chat_turn", limit=1)
    except Exception:
        return None, [], [], []
    if not events:
        return None, [], [], []
    event = events[-1]
    if not isinstance(event, dict):
        return None, [], [], []
    stored_user_message = event.get("user_message")
    if isinstance(stored_user_message, str) and isinstance(request_text, str):
        normalized_stored = " ".join(stored_user_message.split())
        normalized_request = " ".join(request_text.split())
        if normalized_stored and normalized_request and normalized_stored != normalized_request:
            return None, [], [], []
    from nullion.response_fulfillment_contract import (
        artifact_completed_embedded_media_paths,
        artifact_paths_from_tool_results,
    )

    tool_results = _coerce_stored_tool_results(event.get("tool_results"))
    assistant_reply = event.get("assistant_reply")
    stored_reply = assistant_reply if isinstance(assistant_reply, str) else None
    stored_event_artifact_paths = [
        str(path)
        for path in (event.get("artifact_paths") or ())
        if isinstance(path, str) and path.strip()
    ]
    reply_media_paths = [
        str(path)
        for path in _media_paths_from_reply_text(stored_reply or "")
        if str(path or "").strip()
    ]
    raw_artifact_paths = [
        str(path)
        for path in (
            *stored_event_artifact_paths,
            *artifact_paths_from_tool_results(tool_results),
            *_completed_file_write_artifact_paths(tool_results),
            *_completed_file_write_html_artifact_paths(tool_results),
            *artifact_completed_embedded_media_paths(tool_results),
            *reply_media_paths,
        )
        if str(path or "").strip()
    ]
    artifact_paths = list(raw_artifact_paths)
    artifact_paths = _filter_local_discovery_artifact_paths(
        artifact_paths,
        tool_results,
        request_text=request_text,
        stored_reply=stored_reply,
    )
    artifact_paths = _filter_internal_state_artifact_paths(
        artifact_paths,
        request_text=request_text,
    )
    artifact_paths = _filter_artifact_paths_for_embedded_media_contract(
        artifact_paths,
        tool_results,
    )
    artifact_paths = _filter_artifact_paths_for_tool_scope_contract(
        artifact_paths,
        tool_results,
    )
    artifact_paths = _filter_last_mile_helper_artifact_paths(
        artifact_paths,
        tool_results,
    )
    artifact_paths = _merge_reply_media_delivery_paths(
        artifact_paths,
        stored_reply=stored_reply,
        request_text=request_text,
        tool_results=tool_results,
    )
    artifact_paths = _filter_internal_state_artifact_paths(
        artifact_paths,
        request_text=request_text,
    )
    artifact_paths = _filter_last_mile_helper_artifact_paths(
        artifact_paths,
        tool_results,
    )
    artifact_paths = _filter_sparse_duplicate_html_artifacts(artifact_paths)
    requested_names = _requested_output_filenames(request_text)
    if requested_names:
        requested_artifact_paths = [
            path
            for path in artifact_paths
            if Path(path).name.lower() in requested_names
        ]
        if requested_artifact_paths:
            artifact_paths = requested_artifact_paths
    return (
        stored_reply,
        list(dict.fromkeys(artifact_paths)),
        tool_results,
        list(dict.fromkeys(raw_artifact_paths)),
    )


_ATTACHMENT_FAILURE_TEXTS = {
    "I couldn't attach the requested file. The task is still open.",
    "I couldn't attach all of the requested files. The task is still open.",
}


def _tool_results_include_pending_approval(tool_results: list[object]) -> bool:
    for result in tool_results or ():
        status = str(
            result.get("status")
            if isinstance(result, dict)
            else getattr(result, "status", "")
        ).strip().lower()
        output = result.get("output") if isinstance(result, dict) else getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        reason = str(output.get("reason") or "").strip()
        if reason != "approval_required" and output.get("requires_approval") is not True:
            continue
        if status in {"denied", "approval_required", "suspended", "blocked"}:
            return True
    return False


def _same_path_set(left: list[str], right: list[str]) -> bool:
    left_identities = {
        identity
        for path in left
        for identity in _path_identity_variants(path)
    }
    right_identities = {
        identity
        for path in right
        for identity in _path_identity_variants(path)
    }
    return bool(left_identities or right_identities) and left_identities == right_identities


def _attachment_summary_for_paths(paths: list[str]) -> str:
    path_values = [Path(path) for path in paths if str(path or "").strip()]
    if not path_values:
        return "Done."
    labels = list(
        dict.fromkeys(
            {
                ".csv": "CSV",
                ".docx": "DOCX document",
                ".html": "HTML file",
                ".htm": "HTML file",
                ".pdf": "PDF",
                ".pptx": "PPTX deck",
                ".svg": "SVG",
                ".txt": "text file",
                ".xlsx": "XLSX workbook",
            }.get(path.suffix.lower(), path.suffix.lower().lstrip(".").upper() or "file")
            for path in path_values
        )
    )
    if len(path_values) == 1:
        return f"Done - attached the requested {labels[0]}."
    preview_names = ", ".join(path.name for path in path_values[:5])
    more = "" if len(path_values) <= 5 else f", and {len(path_values) - 5} more"
    return f"Done - attached the requested artifacts: {', '.join(labels)}.\nFiles: {preview_names}{more}."


def _attachment_summary_text_mismatches_paths(text: str | None, paths: list[str]) -> bool:
    normalized = str(text or "").strip().lower()
    if "attached the requested" not in normalized or not paths:
        return False
    if len(paths) != 1:
        return "files" not in normalized
    suffix = Path(paths[0]).suffix.lower()
    labels = {
        ".csv": ("csv",),
        ".docx": ("document", "docx"),
        ".html": ("html",),
        ".htm": ("html",),
        ".pdf": ("pdf",),
        ".pptx": ("presentation", "pptx"),
        ".svg": ("svg",),
        ".txt": ("text", "txt"),
        ".xlsx": ("workbook", "xlsx"),
    }.get(suffix, ("file",))
    return not any(label in normalized for label in labels)


def _reply_references_selected_artifact_paths(text: str | None, paths: list[str]) -> bool:
    reply = str(text or "")
    if not reply or not paths:
        return False
    media_identities = {
        identity
        for path in _media_paths_from_reply_text(reply)
        for identity in _path_identity_variants(path)
    }
    for raw_path in paths:
        path = Path(str(raw_path or "")).expanduser()
        if not str(path):
            return False
        if str(path) in reply or (path.name and (path.name in reply or f"artifacts/{path.name}" in reply)):
            continue
        if media_identities and any(identity in media_identities for identity in _path_identity_variants(path)):
            continue
        return False
    return True


def _append_missing_media_lines(text: str | None, artifact_paths: list[str]) -> str | None:
    if not artifact_paths:
        return text
    current_text = "" if text is None else str(text)
    existing = {str(Path(path).expanduser()) for path in _media_paths_from_reply_text(current_text)}
    media_lines = [
        f"MEDIA:{path}"
        for path in artifact_paths
        if str(Path(path).expanduser()) not in existing
    ]
    if not media_lines:
        return current_text
    return "\n\n".join(part for part in (current_text.strip(), "\n".join(media_lines)) if part)


def _media_paths_from_reply_text(text: str) -> list[Path]:
    from nullion.artifacts import media_candidate_paths_from_text

    return media_candidate_paths_from_text(text)


def _browser_screenshot_paths_from_tool_results(tool_results: list[object]) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results:
        if str(getattr(result, "tool_name", "") or "") != "browser_screenshot":
            continue
        if str(getattr(result, "status", "") or "").strip().lower() != "completed":
            continue
        output = getattr(result, "output", None)
        if not isinstance(output, dict):
            continue
        for key in ("path", "artifact_path"):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                paths.append(value.strip())
        values = output.get("artifact_paths")
        if isinstance(values, (list, tuple)):
            paths.extend(str(value).strip() for value in values if isinstance(value, str) and value.strip())
    return tuple(dict.fromkeys(paths))


def _strip_media_lines_for_paths(text: str, paths: tuple[str, ...]) -> str:
    if not text or not paths:
        return text
    identities = {
        identity
        for path in paths
        for identity in _path_identity_variants(path)
    }
    lines: list[str] = []
    for line in str(text).splitlines():
        stripped = line.strip()
        if stripped.startswith("MEDIA:"):
            media_path = stripped.split(":", 1)[1].strip()
            if any(identity in identities for identity in _path_identity_variants(media_path)):
                continue
        lines.append(line)
    return "\n".join(lines).strip()


def _browser_extract_text_fallback_for_unrequested_screenshot(
    text: str,
    tool_results: list[object],
    *,
    request_text: str | None,
) -> str:
    if ".png" in set(_requested_attachment_output_extensions(request_text)):
        return text
    screenshot_paths = _browser_screenshot_paths_from_tool_results(tool_results)
    if not screenshot_paths:
        return text
    reply_media = _media_paths_from_reply_text(text)
    if not any(
        any(identity in {variant for path in screenshot_paths for variant in _path_identity_variants(path)} for identity in _path_identity_variants(path))
        for path in reply_media
    ):
        return text
    stripped = _strip_media_lines_for_paths(text, screenshot_paths)
    normalized = " ".join(str(stripped or "").casefold().split())
    screenshot_only = not normalized or ("screenshot" in normalized and "attach" in normalized)
    if not screenshot_only:
        return text
    try:
        from nullion.response_sanitizer import _latest_completed_browser_extract_text

        extracted = _latest_completed_browser_extract_text(tool_results)  # type: ignore[arg-type]
    except Exception:
        extracted = None
    if isinstance(extracted, str) and extracted.strip() and len(extracted.strip()) <= 1200:
        return extracted.strip()
    return text


def _required_attachment_extensions_for_platform_delivery(
    request_text: str,
    artifact_paths: list[str],
    tool_results: list[object] | None = None,
) -> tuple[str, ...]:
    try:
        from nullion.chat_operator import _requested_attachment_extensions

        requested_extensions = list(
            _requested_attachment_extensions(
                request_text,
                model_client=None,
                allow_model_planning=False,
            )
        )
    except Exception:
        requested_extensions = []
    try:
        from nullion.response_fulfillment_contract import scoped_artifact_extensions_from_tool_results

        scoped_extensions = tuple(scoped_artifact_extensions_from_tool_results(tool_results or ()))  # type: ignore[arg-type]
    except Exception:
        scoped_extensions = ()
    if scoped_extensions:
        present_extensions = {
            ".html" if Path(str(path or "")).suffix.lower() == ".htm" else Path(str(path or "")).suffix.lower()
            for path in artifact_paths
            if str(path or "").strip()
        }
        merged_extensions = list(scoped_extensions)
        for extension in requested_extensions:
            normalized = ".html" if str(extension or "").strip().lower() == ".htm" else str(extension or "").strip().lower()
            if normalized and normalized in present_extensions and normalized not in merged_extensions:
                merged_extensions.append(normalized)
        return tuple(merged_extensions)
    return tuple(requested_extensions)


def _completed_scheduler_mutation_without_artifact_evidence(
    tool_results: list[object],
    *,
    raw_artifact_paths: list[str],
) -> bool:
    if raw_artifact_paths:
        return False
    completed_tool_names = {
        str(getattr(result, "tool_name", "") or "").strip()
        for result in tool_results or ()
        if str(getattr(result, "status", "") or "").strip().lower() == "completed"
    }
    if not completed_tool_names.intersection(_SCHEDULER_MUTATION_TOOL_NAMES):
        return False
    artifact_tool_names = {
        "archive_create",
        "document_create",
        "file_write",
        "html_create",
        "image_generate",
        "pdf_create",
        "presentation_create",
        "spreadsheet_create",
        "terminal_exec",
    }
    return not bool(completed_tool_names.intersection(artifact_tool_names))


def _restore_same_stem_file_write_package_artifacts(
    artifact_paths: list[str],
    *,
    raw_artifact_paths: list[str],
    tool_results: list[object],
    required_attachment_extensions: tuple[str, ...],
) -> list[str]:
    if not artifact_paths or not raw_artifact_paths or not required_attachment_extensions:
        return artifact_paths
    selected_stems = {
        Path(str(path or "")).stem
        for path in artifact_paths
        if str(path or "").strip()
    }
    if not selected_stems:
        return artifact_paths
    required_extensions = {
        ".html" if str(extension or "").strip().lower() == ".htm" else str(extension or "").strip().lower()
        for extension in required_attachment_extensions
        if str(extension or "").strip()
    }
    file_write_identities = {
        identity
        for path in _completed_file_write_artifact_paths(tool_results)
        for identity in _path_identity_variants(path)
    }
    if not file_write_identities:
        return artifact_paths
    try:
        from nullion.artifacts import is_unrequested_internal_sidecar_artifact
    except Exception:
        is_unrequested_internal_sidecar_artifact = None  # type: ignore[assignment]
    restored = list(artifact_paths)
    restored_identities = {
        identity
        for path in restored
        for identity in _path_identity_variants(path)
    }
    for raw_path in raw_artifact_paths:
        path = str(raw_path or "").strip()
        if not path:
            continue
        parsed = Path(path)
        suffix = parsed.suffix.lower()
        if suffix == ".htm":
            suffix = ".html"
        if suffix in required_extensions:
            continue
        if suffix not in VALID_ATTACHMENT_EXTENSIONS:
            continue
        if suffix in _IMAGE_ATTACHMENT_SUFFIXES or suffix in _INTERNAL_STATE_ATTACHMENT_SUFFIXES:
            continue
        if parsed.stem not in selected_stems:
            continue
        if not _path_matches_any_identity(path, file_write_identities):
            continue
        if is_unrequested_internal_sidecar_artifact is not None and is_unrequested_internal_sidecar_artifact(
            path,
            requested_extensions=required_extensions,
        ):
            continue
        if _path_matches_any_identity(path, restored_identities):
            continue
        restored.append(path)
        restored_identities.update(_path_identity_variants(path))
    return list(dict.fromkeys(restored))


def _pending_runtime_approval_exists(runtime: object, approval_id: str | None) -> bool:
    if not approval_id:
        return False
    store = getattr(runtime, "store", None)
    if store is None:
        return False
    try:
        approval = store.get_approval_request(approval_id)
    except Exception:
        approval = None
    if approval is not None:
        status = getattr(getattr(approval, "status", None), "value", getattr(approval, "status", None))
        if str(status or "").strip().lower() == "pending":
            return True
    get_suspended_turn = getattr(store, "get_suspended_turn", None)
    if not callable(get_suspended_turn):
        return False
    try:
        return get_suspended_turn(approval_id) is not None
    except Exception:
        return False


def _latest_pending_runtime_approval_for_turn(
    runtime: object,
    *,
    principal_id: str | None,
    conversation_id: str | None,
    chat_id: str | None,
) -> str | None:
    store = getattr(runtime, "store", None)
    list_approval_requests = getattr(store, "list_approval_requests", None) if store is not None else None
    if not callable(list_approval_requests):
        return None
    identities = {
        str(value or "").strip()
        for value in (principal_id, conversation_id, chat_id)
        if str(value or "").strip()
    }
    if not identities:
        return None
    try:
        approvals = list_approval_requests() or []
    except Exception:
        logger.debug("Unable to list approvals while recovering platform approval state", exc_info=True)
        return None
    candidates = []
    for approval in approvals:
        status = getattr(getattr(approval, "status", None), "value", getattr(approval, "status", None))
        if str(status or "").strip().lower() != "pending":
            continue
        requested_by = str(getattr(approval, "requested_by", "") or "").strip()
        context = getattr(approval, "context", None)
        context_conversation_id = ""
        if isinstance(context, dict):
            context_conversation_id = str(context.get("conversation_id") or "").strip()
        if requested_by not in identities and context_conversation_id not in identities:
            continue
        candidates.append(approval)
    if not candidates:
        return None

    def _created_at_key(approval: object) -> str:
        created_at = getattr(approval, "created_at", None)
        isoformat = getattr(created_at, "isoformat", None)
        if callable(isoformat):
            try:
                return str(isoformat())
            except Exception:
                return ""
        return str(created_at or "")

    return str(getattr(max(candidates, key=_created_at_key), "approval_id", "") or "").strip() or None


def run_platform_chat_request(runtime: object, request: PlatformChatRequest) -> PlatformChatResponse:
    """Execute a platform chat turn through the one shared chat-operator path."""
    from nullion.chat_operator import handle_chat_operator_message as _handle_chat_operator_message

    principal_id = platform_chat_id(request.platform, request.conversation_id, request.chat_id)
    text = _handle_chat_operator_message(
        runtime,
        request.text,
        chat_id=principal_id,
        attachments=request.attachments,
        settings=request.settings,
        request_id=request.request_id,
        message_id=request.message_id,
        turn_id=request.turn_id,
        model_client=request.model_client,
        agent_orchestrator=request.agent_orchestrator,
        service=request.service,
        activity_callback=request.activity_callback,
        text_delta_callback=request.text_delta_callback,
        append_activity_trace=request.append_activity_trace,
        allow_mini_agents=request.allow_mini_agents,
        turn_dispatch_decision=request.turn_dispatch_decision,
        cancellation_checker=request.cancellation_checker,
        conversation_ingress_id=request.conversation_ingress_id,
        reply_context=request.reply_context,
    )
    marker = split_tool_approval_marker(text)
    approval_id = marker.approval_id if marker is not None else None
    if approval_id and _pending_runtime_approval_exists(runtime, approval_id):
        return PlatformChatResponse(
            text=text,
            suspended_for_approval=True,
            approval_id=approval_id,
        )
    if marker is not None:
        text = str(marker.remainder or "").strip() or ORPHAN_APPROVAL_REQUEST_REPLY
    if str(text or "").strip() == ORPHAN_APPROVAL_REQUEST_REPLY:
        recovered_approval_id = _latest_pending_runtime_approval_for_turn(
            runtime,
            principal_id=principal_id,
            conversation_id=request.conversation_id,
            chat_id=request.chat_id,
        )
        if recovered_approval_id:
            return PlatformChatResponse(
                text=f"Tool approval requested: {recovered_approval_id}",
                suspended_for_approval=True,
                approval_id=recovered_approval_id,
            )
    from nullion.messaging_adapters import delivery_contract_for_turn, prepare_reply_for_platform_delivery

    stored_reply, stored_artifact_paths, stored_tool_results, raw_stored_artifact_paths = _latest_stored_turn_delivery_evidence(
        runtime,
        principal_id,
        request_text=request.text,
    )
    delivery_metadata = _delivery_metadata_from_reply(text)
    delivery_metadata.update(_delivery_metadata_from_tool_results(stored_tool_results))
    delivery_text = (
        stored_reply
        if isinstance(stored_reply, str) and stored_reply.strip()
        else text
    )
    completed_run_cron_reply = _completed_run_cron_receipt_reply(stored_tool_results)
    if completed_run_cron_reply:
        delivery_text = completed_run_cron_reply
        stored_artifact_paths = []
        raw_stored_artifact_paths = []
    current_reply_raw_artifact_paths = _reply_media_delivery_paths(
        text,
        request_text=request.text,
        tool_results=stored_tool_results,
    )
    current_reply_artifact_paths = list(current_reply_raw_artifact_paths)
    requested_current_names = _requested_output_filenames(request.text, text)
    requested_current_extensions = set(_requested_attachment_output_extensions(request.text))
    if current_reply_artifact_paths and (requested_current_names or requested_current_extensions):
        current_reply_artifact_paths = [
            path
            for path in current_reply_artifact_paths
            if (
                not requested_current_names
                or Path(path).name.lower() in requested_current_names
            )
            and (
                not requested_current_extensions
                or Path(path).suffix.lower() in requested_current_extensions
            )
        ]
    elif current_reply_artifact_paths:
        current_reply_artifact_paths = []
    if current_reply_artifact_paths and stored_tool_results:
        terminal_stdout_identities = {
            identity
            for path in _terminal_stdout_paths_from_tool_results(stored_tool_results)
            for identity in _path_identity_variants(path)
        }
        if terminal_stdout_identities:
            current_reply_artifact_paths = [
                path
                for path in current_reply_artifact_paths
                if _path_matches_any_identity(path, terminal_stdout_identities)
            ]
        else:
            current_reply_artifact_paths = []
    if current_reply_artifact_paths and not stored_artifact_paths:
        stored_artifact_paths = current_reply_artifact_paths
        raw_stored_artifact_paths = list(dict.fromkeys([*raw_stored_artifact_paths, *current_reply_artifact_paths]))
        delivery_text = text
    local_discovery_or_sidecar = _tool_results_are_local_discovery_or_sidecar(
        stored_tool_results,
        request_text=request.text,
        stored_reply=stored_reply,
        delivery_text=delivery_text,
    )
    if completed_run_cron_reply:
        local_discovery_or_sidecar = False
    if local_discovery_or_sidecar and current_reply_artifact_paths:
        local_discovery_or_sidecar = False
    if local_discovery_or_sidecar:
        stored_artifact_paths = []
        discovery_reply = _local_discovery_reply_from_tool_results(stored_tool_results)
        if discovery_reply:
            delivery_text = discovery_reply
        elif stored_reply:
            stored_media_paths = tuple(str(path) for path in _media_paths_from_reply_text(stored_reply))
            delivery_text = (
                _strip_media_lines_for_paths(stored_reply, stored_media_paths)
                if stored_media_paths
                else stored_reply
            )
    if isinstance(delivery_text, str):
        previous_delivery_text = delivery_text
        delivery_text = _browser_extract_text_fallback_for_unrequested_screenshot(
            delivery_text,
            stored_tool_results,
            request_text=request.text,
        )
        if delivery_text != previous_delivery_text:
            screenshot_identities = {
                identity
                for path in _browser_screenshot_paths_from_tool_results(stored_tool_results)
                for identity in _path_identity_variants(path)
            }
            if screenshot_identities:
                stored_artifact_paths = [
                    path
                    for path in stored_artifact_paths
                    if not _path_matches_any_identity(path, screenshot_identities)
                ]
    if isinstance(delivery_text, str) and stored_tool_results and not stored_artifact_paths:
        try:
            from nullion.response_sanitizer import sanitize_user_visible_reply
        except Exception:
            pass
        else:
            delivery_text = (
                sanitize_user_visible_reply(
                    user_message=request.text,
                    reply=delivery_text,
                    tool_results=stored_tool_results,
                    source="agent",
                )
                or delivery_text
            )
    artifact_candidate_paths = list(
        dict.fromkeys(
            str(path)
            for path in (
                *raw_stored_artifact_paths,
                *_supporting_artifact_candidate_paths_from_tool_results(stored_tool_results),
            )
            if str(path or "").strip()
        )
    )
    required_attachment_extensions = _required_attachment_extensions_for_platform_delivery(
        request.text,
        stored_artifact_paths,
        stored_tool_results,
    )
    if (
        required_attachment_extensions
        and not stored_artifact_paths
        and _completed_scheduler_mutation_without_artifact_evidence(
            stored_tool_results,
            raw_artifact_paths=raw_stored_artifact_paths,
        )
    ):
        required_attachment_extensions = ()
    if required_attachment_extensions and stored_artifact_paths:
        stored_artifact_paths = [
            path
            for path in stored_artifact_paths
            if Path(str(path or "")).suffix.lower() in set(required_attachment_extensions)
        ]
        stored_artifact_paths = _restore_same_stem_file_write_package_artifacts(
            stored_artifact_paths,
            raw_artifact_paths=raw_stored_artifact_paths,
            tool_results=stored_tool_results,
            required_attachment_extensions=required_attachment_extensions,
        )
    if stored_artifact_paths:
        if isinstance(text, str) and text.strip() in _ATTACHMENT_FAILURE_TEXTS and stored_reply:
            delivery_text = stored_reply
        if isinstance(delivery_text, str):
            artifact_text_candidate_paths = list(
                dict.fromkeys(
                    [
                        *raw_stored_artifact_paths,
                        *_supporting_artifact_candidate_paths_from_tool_results(stored_tool_results),
                    ]
                )
            )
            delivery_text = _strip_media_lines_for_paths(
                delivery_text,
                tuple(_paths_not_selected(artifact_text_candidate_paths, stored_artifact_paths)),
            )
            delivery_text = _strip_unselected_artifact_filename_lines(
                delivery_text,
                selected_paths=stored_artifact_paths,
                candidate_paths=artifact_text_candidate_paths,
            )
        delivery_text = _append_missing_media_lines(delivery_text, stored_artifact_paths)
        if isinstance(delivery_text, str):
            try:
                from nullion.response_sanitizer import sanitize_user_visible_reply
            except Exception:
                pass
            else:
                delivery_text = (
                    sanitize_user_visible_reply(
                        user_message=request.text,
                        reply=delivery_text,
                        tool_results=stored_tool_results,
                        source="agent",
                    )
                    or delivery_text
                )
    delivery = prepare_reply_for_platform_delivery(
        delivery_text,
        principal_id=principal_id,
        delivery_contract=delivery_contract_for_turn(
                request.text,
                reply=delivery_text,
                artifact_paths=stored_artifact_paths,
                requires_attachment_delivery=bool(stored_artifact_paths or required_attachment_extensions),
                required_attachment_extensions=required_attachment_extensions,
            ),
        )
    response_text = delivery.text
    attachment_paths = [str(path) for path in delivery.attachments]
    if local_discovery_or_sidecar:
        attachment_paths = []
        response_text = _local_discovery_reply_from_tool_results(stored_tool_results) or response_text
    elif stored_artifact_paths and (
        not _same_path_set(attachment_paths, stored_artifact_paths)
        or str(response_text or "").strip() in _ATTACHMENT_FAILURE_TEXTS
        or (
            str(response_text or "").strip()
            == str(_local_discovery_reply_from_tool_results(stored_tool_results) or "").strip()
            and not _reply_references_selected_artifact_paths(response_text, stored_artifact_paths)
        )
        or (
            _attachment_summary_text_mismatches_paths(response_text, stored_artifact_paths)
            and not _reply_references_selected_artifact_paths(response_text, stored_artifact_paths)
        )
        or (
            current_reply_raw_artifact_paths
            and not _reply_references_selected_artifact_paths(text, stored_artifact_paths)
        )
    ):
        attachment_paths = list(stored_artifact_paths)
        if not _tool_results_include_pending_approval(stored_tool_results):
            response_text = _attachment_summary_for_paths(attachment_paths)
    if response_text is None and delivery.attachments:
        response_text = (
            "Attached the requested files."
            if len(delivery.attachments) != 1
            else "Attached the requested file."
        )
    return PlatformChatResponse(
        text=response_text,
        artifact_paths=attachment_paths,
        artifact_candidate_paths=artifact_candidate_paths,
        reply_already_sent=bool(delivery_metadata.get("reply_already_sent", False)),
        mini_agent_dispatch=bool(delivery_metadata.get("mini_agent_dispatch", False)),
        task_group_id=str(delivery_metadata.get("task_group_id") or "") or None,
        planner_status_text=str(delivery_metadata.get("planner_status_text") or "") or None,
        progress_status_text=str(delivery_metadata.get("progress_status_text") or "") or None,
        planner_status_owned_by_background=bool(delivery_metadata.get("planner_status_owned_by_background", False)),
        tool_results=list(stored_tool_results),
    )


class MessagingTurnState(TypedDict, total=False):
    service: Any
    ingress: adapters.MessagingIngress
    before_decision_snapshot: Any
    raw_reply: str | None
    raw_artifact_paths: tuple[str, ...]
    raw_reply_already_sent: bool
    visible_reply: str | None
    reply: str | None
    artifact_paths: tuple[str, ...]
    reply_already_sent: bool
    delivery_contract: adapters.DeliveryContract
    status: Literal["running", "reply_ready", "no_reply"]
    turn_dispatch_decision: Any
    text_delta_callback: Any
    activity_callback: Any


@dataclass(frozen=True, slots=True)
class MessagingTurnResult:
    reply: str | None
    delivery_contract: adapters.DeliveryContract
    status: Literal["reply_ready", "no_reply"]
    reply_already_sent: bool = False
    artifact_paths: tuple[str, ...] = ()


def _capture_decision_snapshot_node(state: MessagingTurnState) -> dict[str, object]:
    return {
        "before_decision_snapshot": adapters._capture_messaging_decision_snapshot(state["service"]),
        "status": "running",
    }


def _handle_text_message_accepts_kw(handler: object, name: str) -> bool:
    try:
        parameters = inspect.signature(handler).parameters
    except (TypeError, ValueError):
        return False
    if name in parameters:
        return True
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())


def _run_service_node(state: MessagingTurnState) -> dict[str, object]:
    ingress = state["ingress"]
    service = state["service"]
    handler = service.handle_text_message

    kwargs = {
        "text": ingress.text,
        "chat_id": ingress.operator_chat_id,
        "reminder_chat_id": ingress.reminder_chat_id,
        "attachments": list(ingress.attachments),
        "request_id": ingress.request_id,
        "message_id": ingress.message_id,
    }
    if _handle_text_message_accepts_kw(handler, "platform"):
        kwargs["platform"] = ingress.channel
    if _handle_text_message_accepts_kw(handler, "turn_dispatch_decision"):
        kwargs["turn_dispatch_decision"] = state.get("turn_dispatch_decision")
    if _handle_text_message_accepts_kw(handler, "text_delta_callback"):
        kwargs["text_delta_callback"] = state.get("text_delta_callback")
    if _handle_text_message_accepts_kw(handler, "activity_callback"):
        kwargs["activity_callback"] = state.get("activity_callback")
    if _handle_text_message_accepts_kw(handler, "conversation_ingress_id"):
        kwargs["conversation_ingress_id"] = ingress.request_id or ingress.message_id
    raw_reply = handler(**kwargs)
    artifact_paths = tuple(
        str(path)
        for path in getattr(raw_reply, "artifact_paths", ()) or ()
        if str(path or "").strip()
    )
    reply = getattr(raw_reply, "text", raw_reply)
    return {
        "raw_reply": reply,
        "raw_artifact_paths": artifact_paths,
        "raw_reply_already_sent": bool(getattr(raw_reply, "reply_already_sent", False)),
    }


def _finalize_reply_node(state: MessagingTurnState) -> dict[str, object]:
    ingress = state["ingress"]
    raw_reply = state.get("raw_reply")
    marker = split_tool_approval_marker(raw_reply)
    visible_reply = strip_tool_approval_marker(raw_reply)
    before_decision_snapshot = state["before_decision_snapshot"]
    runtime = getattr(state["service"], "runtime", None)
    if (
        marker is not None
        and marker.approval_id
        and marker.approval_id not in before_decision_snapshot.pending_approval_ids
        and not _pending_runtime_approval_exists(runtime, marker.approval_id)
    ):
        visible_reply = ORPHAN_APPROVAL_REQUEST_REPLY
    fallbacks = adapters._new_decision_text_fallbacks(state["service"], before_decision_snapshot)
    if marker is not None and (
        not marker.approval_id
        or marker.approval_id in before_decision_snapshot.pending_approval_ids
        or _pending_runtime_approval_exists(runtime, marker.approval_id)
    ):
        fallbacks = (
            *fallbacks,
            *adapters._approval_text_fallback_for_marker(runtime, marker.approval_id),
        )
    reply = adapters._append_decision_fallbacks(visible_reply, fallbacks)
    adapters.save_messaging_chat_history(ingress, reply)
    if runtime is not None:
        try:
            runtime.checkpoint(force=True)
        except Exception:
            logger.debug("Unable to checkpoint messaging turn graph final state", exc_info=True)
    raw_reply_already_sent = bool(state.get("raw_reply_already_sent"))
    reply_already_sent = raw_reply_already_sent and reply == str(raw_reply)
    artifact_paths = tuple(
        str(path)
        for path in state.get("raw_artifact_paths", ()) or ()
        if str(path or "").strip()
    )
    return {
        "visible_reply": visible_reply,
        "reply": reply,
        "artifact_paths": artifact_paths,
        "reply_already_sent": reply_already_sent,
        "delivery_contract": adapters.delivery_contract_for_runtime_turn(
            getattr(state["service"], "runtime", None),
            ingress.operator_chat_id,
            ingress.text,
            reply=reply,
            inbound_attachments=ingress.attachments,
            artifact_paths=artifact_paths,
            requires_attachment_delivery=bool(artifact_paths),
        ),
        "status": "reply_ready" if reply is not None else "no_reply",
    }


@lru_cache(maxsize=1)
def _compiled_messaging_turn_graph():
    graph = StateGraph(MessagingTurnState)
    graph.add_node("capture_decision_snapshot", _capture_decision_snapshot_node)
    graph.add_node("run_service", _run_service_node)
    graph.add_node("finalize_reply", _finalize_reply_node)
    graph.add_edge(START, "capture_decision_snapshot")
    graph.add_edge("capture_decision_snapshot", "run_service")
    graph.add_edge("run_service", "finalize_reply")
    graph.add_edge("finalize_reply", END)
    return graph.compile()


def run_messaging_turn_graph(
    service: object,
    ingress: adapters.MessagingIngress,
    *,
    turn_dispatch_decision=None,
    text_delta_callback=None,
    activity_callback=None,
) -> MessagingTurnResult:
    final_state = _compiled_messaging_turn_graph().invoke(
        {
            "service": service,
            "ingress": ingress,
            "turn_dispatch_decision": turn_dispatch_decision,
            "text_delta_callback": text_delta_callback,
            "activity_callback": activity_callback,
        },
        config={"configurable": {"thread_id": ingress.request_id or ingress.operator_chat_id}},
    )
    delivery_contract = final_state.get("delivery_contract")
    if not isinstance(delivery_contract, adapters.DeliveryContract):
        delivery_contract = adapters.DeliveryContract.message_only()
    status = final_state.get("status")
    if status not in {"reply_ready", "no_reply"}:
        status = "no_reply"
    return MessagingTurnResult(
        reply=final_state.get("reply"),
        delivery_contract=delivery_contract,
        status=status,
        reply_already_sent=bool(final_state.get("reply_already_sent")),
        artifact_paths=tuple(
            str(path)
            for path in final_state.get("artifact_paths", ()) or ()
            if str(path or "").strip()
        ),
    )


__all__ = [
    "MessagingTurnResult",
    "MessagingTurnState",
    "PlatformChatRequest",
    "PlatformChatResponse",
    "platform_chat_id",
    "run_messaging_turn_graph",
    "run_platform_chat_request",
]

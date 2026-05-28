"""Platform-neutral final response fulfillment checks."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from nullion.artifacts import artifact_descriptors_for_paths, media_candidate_paths_from_text
from nullion.attachment_format_graph import ATTACHMENT_TOKEN_EXTENSIONS, is_domain_suffix_extension
from nullion.execution_outcome import ExecutionOutcomeEvaluation, ExecutionStatus, build_execution_outcome
from nullion.messaging_adapters import delivery_contract_for_turn
from nullion.task_frames import TaskFrameStatus
from nullion.tools import ToolInvocation, ToolResult, normalize_tool_status


USER_VISIBLE_TEXT_KEYS = ("result_text", "delivery_text", "final_text", "text", "message", "summary", "content", "stdout")
EMPTY_USER_VISIBLE_TEXTS = frozenset({"", "(no reply)", "no reply"})
STRUCTURED_ARTIFACT_PRODUCER_TOOLS = frozenset(
    {
        "document_create",
        "file_write",
        "pdf_create",
        "pdf_edit",
        "image_generate",
        "browser_screenshot",
        "presentation_create",
        "spreadsheet_create",
    }
)
IMAGE_ATTACHMENT_EXTENSIONS = frozenset({".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"})
_MEDIA_ARTIFACT_TOOL_EXTENSIONS = {
    "document_create": ".docx",
    "pdf_create": ".pdf",
    "pdf_edit": ".pdf",
    "presentation_create": ".pptx",
    "spreadsheet_create": ".xlsx",
}
_MEDIA_ARTIFACT_EXTENSION_ALIASES = {
    ".doc": ".docx",
    ".ppt": ".pptx",
    ".xls": ".xlsx",
}
_MEDIA_ARTIFACT_LABELS = {
    ".docx": "document",
    ".pdf": "PDF",
    ".pptx": "presentation",
    ".xlsx": "spreadsheet",
}
_MEDIA_ARTIFACT_FAILURE_REASONS = frozenset(
    {
        "artifact_media_embed_failed",
        "artifact_media_inputs_failed",
        "artifact_media_required_by_prior_failure",
        "artifact_media_required_by_prior_collection",
        "artifact_media_required_by_turn_contract",
        "spreadsheet_embed_paths_failed",
        "remote_image_paths_not_supported",
        "spreadsheet_media_required_by_prior_failure",
        "duplicate_image_paths_for_distinct_rows",
    }
)
_MEDIA_ARTIFACT_SCOPE_KEYS = (
    "required_embedded_media_extensions",
    "embedded_media_artifact_extensions",
    "media_required_artifact_extensions",
)
_SPREADSHEET_MEDIA_INPUT_KEYS = frozenset(
    {
        "image",
        "image_path",
        "image_paths",
        "screenshot",
        "screenshot_path",
        "screenshot_paths",
    }
)


@dataclass(frozen=True, slots=True)
class ResponseFulfillmentDecision:
    satisfied: bool
    reply: str
    missing_requirements: tuple[str, ...] = ()


def _text_from_value(value: object) -> str:
    if isinstance(value, str):
        text = value.strip()
        return "" if text.lower() in EMPTY_USER_VISIBLE_TEXTS else text
    if isinstance(value, dict):
        for key in USER_VISIBLE_TEXT_KEYS:
            text = _text_from_value(value.get(key))
            if text:
                return text
        nested = value.get("result")
        if isinstance(nested, dict):
            return _text_from_value(nested)
        return ""
    for key in USER_VISIBLE_TEXT_KEYS:
        if hasattr(value, key):
            text = _text_from_value(getattr(value, key))
            if text:
                return text
    return ""


def _has_nonempty_media_value(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set)):
        return any(_has_nonempty_media_value(item) for item in value)
    return True


def _artifact_media_tool_extension(tool_name: object) -> str | None:
    return _MEDIA_ARTIFACT_TOOL_EXTENSIONS.get(str(tool_name or "").strip())


def _artifact_media_label(extension: str | None) -> str:
    return _MEDIA_ARTIFACT_LABELS.get(str(extension or "").strip().lower(), "artifact")


def normalize_artifact_media_required_extensions(value: Iterable[object] | object | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        values: Iterable[object] = (value,)
    elif isinstance(value, Iterable):
        values = value
    else:
        values = (value,)
    extensions: list[str] = []
    for raw_extension in values:
        extension = str(raw_extension or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        extension = _MEDIA_ARTIFACT_EXTENSION_ALIASES.get(extension, extension)
        if extension in _MEDIA_ARTIFACT_LABELS and extension not in extensions:
            extensions.append(extension)
    return tuple(extensions)


def _nested_media_inputs_present(value: object, *, keys: frozenset[str]) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key or "").strip().lower() in keys and _has_nonempty_media_value(nested):
                return True
            if isinstance(nested, (dict, list, tuple)) and _nested_media_inputs_present(nested, keys=keys):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_nested_media_inputs_present(item, keys=keys) for item in value)
    return False


def _artifact_invocation_has_media_inputs(invocation: ToolInvocation) -> bool:
    arguments = invocation.arguments if isinstance(invocation.arguments, dict) else {}
    media_keys = set(_SPREADSHEET_MEDIA_INPUT_KEYS)
    if invocation.tool_name == "pdf_edit":
        media_keys.update({"append_image_paths", "append_screenshot_paths"})
    return _nested_media_inputs_present(arguments, keys=frozenset(media_keys))


def _artifact_media_count(output: dict[str, object]) -> int:
    count = 0
    for key in ("embedded_images", "embedded_screenshots", "source_image_paths", "source_screenshot_paths"):
        values = output.get(key)
        if isinstance(values, (list, tuple)):
            count += sum(1 for value in values if isinstance(value, str) and value.strip())
    return count


def _artifact_result_paths(result: ToolResult) -> tuple[str, ...]:
    output = result.output if isinstance(result.output, dict) else {}
    paths: list[str] = []
    for key in ("path", "artifact_path"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            paths.append(value)
    for key in ("artifact_paths", "artifacts"):
        values = output.get(key)
        if isinstance(values, (list, tuple)):
            paths.extend(str(value).strip() for value in values if isinstance(value, str) and value.strip())
    return tuple(dict.fromkeys(paths))


def _artifact_result_reports_media_failure(result: ToolResult) -> bool:
    if _artifact_media_tool_extension(getattr(result, "tool_name", None)) is None:
        return False
    output = result.output if isinstance(result.output, dict) else {}
    reason = str(output.get("reason") or "").strip()
    if reason in _MEDIA_ARTIFACT_FAILURE_REASONS:
        return True
    if reason == "incomplete_required_row_values" and _has_nonempty_media_value(output.get("missing_image_rows")):
        return True
    if normalize_tool_status(getattr(result, "status", None)) == "completed":
        return False
    return _has_nonempty_media_value(output.get("skipped_images")) or _has_nonempty_media_value(
        output.get("remote_image_urls")
    ) or _has_nonempty_media_value(
        output.get("failed_image_paths")
    ) or _has_nonempty_media_value(
        output.get("failed_screenshot_paths")
    )


def _browser_image_collection_failed(result: ToolResult) -> bool:
    if str(getattr(result, "tool_name", "") or "") != "browser_image_collect":
        return False
    if normalize_tool_status(getattr(result, "status", None)) != "completed":
        return True
    output = result.output if isinstance(result.output, dict) else {}
    image_paths = output.get("image_paths")
    return not isinstance(image_paths, (list, tuple)) or not any(
        isinstance(path, str) and path.strip() for path in image_paths
    )


def _browser_image_collection_paths(result: ToolResult) -> tuple[str, ...]:
    if str(getattr(result, "tool_name", "") or "") != "browser_image_collect":
        return ()
    if normalize_tool_status(getattr(result, "status", None)) != "completed":
        return ()
    output = result.output if isinstance(result.output, dict) else {}
    paths: list[str] = []
    for key in ("image_paths", "artifact_paths"):
        values = output.get(key)
        if isinstance(values, (list, tuple)):
            paths.extend(str(value).strip() for value in values if isinstance(value, str) and value.strip())
    return tuple(dict.fromkeys(paths))


def _browser_page_context_used(result: ToolResult) -> bool:
    tool_name = str(getattr(result, "tool_name", "") or "")
    return tool_name.startswith("browser_") and tool_name not in {"browser_image_collect", "browser_screenshot"}


def _artifact_result_required_media_extensions(result: ToolResult) -> tuple[str, ...]:
    output = result.output if isinstance(result.output, dict) else {}
    extensions: list[str] = []
    for key in _MEDIA_ARTIFACT_SCOPE_KEYS:
        for extension in normalize_artifact_media_required_extensions(output.get(key)):
            if extension not in extensions:
                extensions.append(extension)
    if not extensions and _artifact_result_reports_media_failure(result):
        extension = _artifact_media_tool_extension(getattr(result, "tool_name", None))
        if extension:
            extensions.append(extension)
    return tuple(extensions)


def artifact_media_required_extensions(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    extensions: list[str] = []
    collected_browser_images_pending = False
    for result in tool_results or ():
        if _browser_image_collection_paths(result):
            collected_browser_images_pending = True
            continue
        extension = _artifact_media_tool_extension(getattr(result, "tool_name", None))
        if collected_browser_images_pending and extension is not None:
            output = result.output if isinstance(result.output, dict) else {}
            if normalize_tool_status(getattr(result, "status", None)) == "completed" and _artifact_media_count(output) > 0:
                collected_browser_images_pending = False
            elif extension not in extensions:
                extensions.append(extension)
        for extension in _artifact_result_required_media_extensions(result):
            if extension not in extensions:
                extensions.append(extension)
    return tuple(extensions)


def artifact_media_embedding_was_required(tool_results: Iterable[ToolResult] | None) -> bool:
    return bool(artifact_media_required_extensions(tool_results))


def artifact_media_embedding_obligation_outstanding(tool_results: Iterable[ToolResult] | None) -> bool:
    outstanding: set[str] = set()
    collected_browser_images_pending = False
    for result in tool_results or ():
        if _browser_image_collection_paths(result):
            collected_browser_images_pending = True
            continue
        for extension in _artifact_result_required_media_extensions(result):
            outstanding.add(extension)
        extension = _artifact_media_tool_extension(getattr(result, "tool_name", None))
        if extension is None:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if normalize_tool_status(getattr(result, "status", None)) == "completed" and _artifact_media_count(output) > 0:
            outstanding.discard(extension)
            collected_browser_images_pending = False
            continue
        if collected_browser_images_pending:
            outstanding.add(extension)
        if _artifact_result_reports_media_failure(result):
            outstanding.add(extension)
    return bool(outstanding)


def artifact_completed_embedded_media_paths(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results or ():
        if _artifact_media_tool_extension(getattr(result, "tool_name", None)) is None:
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if _artifact_media_count(output) <= 0:
            continue
        paths.extend(_artifact_result_paths(result))
    return tuple(dict.fromkeys(paths))


def artifact_plain_replacement_artifact_paths(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    outstanding: set[str] = set()
    paths: list[str] = []
    collected_browser_images_pending = False
    for result in tool_results or ():
        if _browser_image_collection_paths(result):
            collected_browser_images_pending = True
            continue
        for extension in _artifact_result_required_media_extensions(result):
            outstanding.add(extension)
        extension = _artifact_media_tool_extension(getattr(result, "tool_name", None))
        if extension is None:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if normalize_tool_status(getattr(result, "status", None)) == "completed":
            if _artifact_media_count(output) > 0:
                outstanding.discard(extension)
                collected_browser_images_pending = False
            elif collected_browser_images_pending:
                paths.extend(_artifact_result_paths(result))
            elif extension in outstanding:
                paths.extend(_artifact_result_paths(result))
            continue
        if _artifact_result_reports_media_failure(result):
            outstanding.add(extension)
    return tuple(dict.fromkeys(paths))


def artifact_media_plain_replacement_guard_result(
    invocation: ToolInvocation,
    prior_tool_results: Iterable[ToolResult] | None,
    *,
    required_embedded_media_extensions: Iterable[object] | None = None,
) -> ToolResult | None:
    prior_results = tuple(prior_tool_results or ())
    if invocation.tool_name == "image_generate" and (
        any(_browser_image_collection_failed(result) for result in prior_results)
        or any(_browser_page_context_used(result) for result in prior_results)
    ):
        error = (
            "This turn has browser/page evidence for sourced web image assets. "
            "Do not generate replacement images for sourced image assets; inspect the rendered page for image URLs "
            "or report that source images could not be collected."
        )
        return ToolResult(
            invocation.invocation_id,
            invocation.tool_name,
            "failed",
            {
                "reason": "sourced_image_collection_failed",
                "message": error,
                "required_tools": ["browser_extract_items", "browser_image_collect"],
            },
            error,
        )
    extension = _artifact_media_tool_extension(invocation.tool_name)
    if extension is None:
        return None
    outstanding_extensions = set(normalize_artifact_media_required_extensions(required_embedded_media_extensions))
    collected_browser_images_available = False
    for result in prior_tool_results or ():
        if _browser_image_collection_paths(result):
            collected_browser_images_available = True
            continue
        outstanding_extensions.update(_artifact_result_required_media_extensions(result))
        result_extension = _artifact_media_tool_extension(getattr(result, "tool_name", None))
        if result_extension is None:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if normalize_tool_status(getattr(result, "status", None)) == "completed" and _artifact_media_count(output) > 0:
            outstanding_extensions.discard(result_extension)
            collected_browser_images_available = False
            continue
        if _artifact_result_reports_media_failure(result):
            outstanding_extensions.add(result_extension)
    if collected_browser_images_available:
        outstanding_extensions.add(extension)
    if extension not in outstanding_extensions:
        return None
    if _artifact_invocation_has_media_inputs(invocation):
        return None
    label = _artifact_media_label(extension)
    first_attempt_required = extension in set(normalize_artifact_media_required_extensions(required_embedded_media_extensions))
    reason = (
        "artifact_media_required_by_turn_contract"
        if first_attempt_required
        else "artifact_media_required_by_prior_collection"
        if collected_browser_images_available
        else "artifact_media_required_by_prior_failure"
    )
    error_prefix = (
        f"This turn requires the {label} artifact to contain embedded image/screenshot media. "
        if first_attempt_required
        else f"Browser/page image artifacts were already collected for this turn's {label} artifact. "
        if collected_browser_images_available
        else f"A previous {label} artifact tool call in this turn attempted image/screenshot embedding and failed. "
    )
    error = (
        error_prefix
        + "Do not create or deliver a replacement artifact without embedded media; fetch or save local raster "
        "artifact files first, then retry with row- or artifact-aligned image/screenshot path inputs."
    )
    return ToolResult(
        invocation.invocation_id,
        invocation.tool_name,
        "failed",
        {
            "reason": reason,
            "message": error,
            "artifact_extensions": [extension],
            "embedded_media_artifact_extensions": [extension],
            "required_arguments": ["image_paths", "screenshot_paths"],
        },
        error,
    )


def spreadsheet_media_embedding_was_required(tool_results: Iterable[ToolResult] | None) -> bool:
    return "spreadsheet_create" in {
        result.tool_name
        for result in tool_results or ()
        if _artifact_result_reports_media_failure(result)
    }


def spreadsheet_media_embedding_obligation_outstanding(tool_results: Iterable[ToolResult] | None) -> bool:
    return artifact_media_embedding_obligation_outstanding(
        result for result in tool_results or () if result.tool_name == "spreadsheet_create"
    )


def spreadsheet_completed_embedded_media_paths(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    return artifact_completed_embedded_media_paths(
        result for result in tool_results or () if result.tool_name == "spreadsheet_create"
    )


def spreadsheet_plain_replacement_artifact_paths(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    return artifact_plain_replacement_artifact_paths(
        result for result in tool_results or () if result.tool_name == "spreadsheet_create"
    )


def spreadsheet_plain_replacement_guard_result(
    invocation: ToolInvocation,
    prior_tool_results: Iterable[ToolResult] | None,
) -> ToolResult | None:
    return artifact_media_plain_replacement_guard_result(invocation, prior_tool_results)


def user_visible_text_from_output(output: object) -> str:
    """Extract deliverable text from provider/tool output without assuming a concrete result type."""
    return _text_from_value(output)


def _clean_tool_evidence_text(value: object) -> str:
    text = html.unescape(str(value or "")).strip()
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _needs_tool_evidence_fallback(reply: str) -> bool:
    return _text_from_value(reply) == ""


def guaranteed_user_visible_text(*, subject: object, output: object, kind: str = "task") -> str:
    """Return user-visible text or a deterministic diagnostic fallback.

    The delivery boundary should not trust a provider's final prose alone. This
    helper makes any runner/tool/platform result explicit before downstream
    channel formatting decides whether it is web text, chat text, media, or an
    attachment.
    """
    text = user_visible_text_from_output(output)
    if text:
        return text
    name = str(getattr(subject, "name", "") or subject or kind).strip() or kind
    return (
        f"{kind.capitalize()} '{name}' completed, but the runner returned no user-visible text. "
        "Check the task prompt, tool results, or generated artifacts for why no report was produced."
    )


def artifact_paths_from_tool_results(tool_results: Iterable[ToolResult] | None) -> list[str]:
    paths: list[str] = []
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        tool_name = str(getattr(result, "tool_name", "") or "")
        output = result.output if isinstance(result.output, dict) else {}
        if tool_name == "browser_screenshot":
            continue
        if tool_name in {"file_patch", "file_write"}:
            path = output.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path)
        for key in ("artifact_path",):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                paths.append(value)
        for key in ("artifact_paths", "artifacts"):
            values = output.get(key)
            if isinstance(values, (list, tuple)):
                paths.extend(value for value in values if isinstance(value, str) and value.strip())
    return list(dict.fromkeys(paths))


def _completed_image_fallback_used(tool_results: Iterable[ToolResult] | None) -> bool:
    for result in tool_results or ():
        if result.tool_name != "image_generate":
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if output.get("fallback_used") is True:
            return True
    return False


def _valid_artifact_paths(paths: Iterable[str], *, artifact_roots: Iterable[Path]) -> list[str]:
    valid: list[str] = []
    seen: set[str] = set()
    for root in artifact_roots:
        for descriptor in artifact_descriptors_for_paths(list(paths), artifact_root=root):
            if descriptor.path in seen:
                continue
            seen.add(descriptor.path)
            valid.append(descriptor.path)
    return valid


def _existing_deliverable_paths(paths: Iterable[str], *, artifact_roots: Iterable[Path]) -> list[str]:
    valid = _valid_artifact_paths(paths, artifact_roots=artifact_roots)
    seen = set(valid)
    roots = [root.expanduser().resolve() for root in artifact_roots]
    for raw_path in paths:
        try:
            path = Path(raw_path).expanduser().resolve()
        except (OSError, RuntimeError, ValueError):
            continue
        if str(path) in seen:
            continue
        if not path.is_file() or path.stat().st_size <= 0:
            continue
        if not any(_is_relative_to(path, root) for root in roots):
            continue
        seen.add(str(path))
        valid.append(str(path))
    return valid


def _required_extension_for_artifact_kind(artifact_kind: str | None) -> str | None:
    normalized = str(artifact_kind or "").strip().lower().removeprefix(".")
    if not normalized or normalized == "file":
        return None
    extension = ATTACHMENT_TOKEN_EXTENSIONS.get(normalized)
    if extension is not None:
        return extension
    direct_extension = f".{normalized}"
    if re.fullmatch(r"\.[a-z0-9]{1,16}", direct_extension):
        return direct_extension
    return None


def _normalize_required_extensions(required_attachment_extensions: Iterable[str] | None) -> tuple[str, ...]:
    extensions: list[str] = []
    for extension in required_attachment_extensions or ():
        normalized = str(extension or "").strip().lower()
        if not normalized:
            continue
        if not normalized.startswith("."):
            normalized = f".{normalized}"
        if is_domain_suffix_extension(normalized):
            continue
        if normalized not in extensions:
            extensions.append(normalized)
    return tuple(extensions)


def _paths_matching_required_extension(paths: Iterable[str], required_extension: str | None) -> list[str]:
    if required_extension is None:
        return list(paths)
    return [path for path in paths if Path(path).suffix.lower() == required_extension]


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _reply_valid_media_paths(reply: str, *, artifact_roots: Iterable[Path]) -> list[str]:
    candidates = [str(path) for path in media_candidate_paths_from_text(reply)]
    return _existing_deliverable_paths(candidates, artifact_roots=artifact_roots)


def _reply_mentions_only_wrong_extension(
    reply: str,
    *,
    artifact_roots: Iterable[Path],
    required_extension: str | None,
) -> bool:
    if required_extension is None:
        return False
    reply_media_paths = _reply_valid_media_paths(reply, artifact_roots=artifact_roots)
    return bool(reply_media_paths) and not bool(_paths_matching_required_extension(reply_media_paths, required_extension))


def _corrected_artifact_attachment_reply(path: str, required_extension: str | None) -> str:
    label = _artifact_media_label(required_extension)
    return f"Done — attached the requested {label}.\n\nMEDIA:{path}"


def _active_frame_for_contract(store, conversation_id: str):
    active_frame_id = store.get_active_task_frame_id(conversation_id)
    if not isinstance(active_frame_id, str) or not active_frame_id:
        return None
    frame = store.get_task_frame(active_frame_id)
    if frame is None or frame.status not in {
        TaskFrameStatus.ACTIVE,
        TaskFrameStatus.RUNNING,
        TaskFrameStatus.WAITING_APPROVAL,
        TaskFrameStatus.WAITING_INPUT,
        TaskFrameStatus.VERIFYING,
    }:
        return None
    return frame


def _completed_tool_evidence_reply(tool_results: Iterable[ToolResult] | None) -> str | None:
    fetch_lines: list[str] = []
    search_fallback_text: str | None = None
    saw_search_output = False
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if result.tool_name == "web_search":
            saw_search_output = bool(output) or saw_search_output
            text = _clean_tool_evidence_text(user_visible_text_from_output(output))
            if text:
                search_fallback_text = text
        elif result.tool_name == "web_fetch":
            title = _clean_tool_evidence_text(output.get("title"))
            url = _clean_tool_evidence_text(output.get("url"))
            if title or url:
                fetch_lines.append(f"- **{title or url}**" + (f" — {url}" if title and url else ""))

    if search_fallback_text:
        return search_fallback_text
    if fetch_lines:
        return "I fetched the requested page:\n\n" + "\n".join(fetch_lines)
    if saw_search_output:
        return "The search completed, but it did not return a clean user-visible summary."
    return None


def _missing_dependency_reply(tool_results: Iterable[ToolResult] | None) -> str | None:
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) == "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if output.get("reason") != "missing_dependency":
            continue
        package = str(output.get("package") or output.get("dependency") or "").strip()
        if not package:
            continue
        license_name = str(output.get("license") or "").strip()
        install_command = str(output.get("install_command") or "").strip()
        tool_name = str(getattr(result, "tool_name", "") or "tool").strip()
        lines = [
            f"I need the open-source `{package}` package before I can create this file with `{tool_name}`.",
        ]
        if license_name:
            lines.append(f"License: {license_name}.")
        if install_command:
            lines.append(f"Install command: `{install_command}`.")
        lines.append("Please approve installing it, then I can retry the file delivery.")
        return "\n".join(lines)
    return None


def _failed_artifact_producer_setup_reply(tool_results: Iterable[ToolResult] | None) -> str | None:
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) not in {"failed", "denied"}:
            continue
        tool_name = str(getattr(result, "tool_name", "") or "").strip()
        if tool_name not in STRUCTURED_ARTIFACT_PRODUCER_TOOLS:
            continue
        error = str(getattr(result, "error", "") or "").strip()
        output = result.output if isinstance(result.output, dict) else {}
        reason = str(output.get("reason") or "").strip()
        if reason == "binary_file_read_boundary":
            continue
        if tool_name == "image_generate":
            detail = error or reason or "image generation did not complete"
            lines = [
                "I couldn't generate the requested image because the configured image provider is not ready.",
                f"Tool result: {detail}.",
                "Options:",
                "1. Add the missing image provider API key in Settings, then retry.",
                "2. Switch image generation to a configured local command or provider.",
                "3. Ask me to create a non-image fallback artifact such as a PDF, SVG, or Markdown brief.",
            ]
            return "\n".join(lines)
        if error or reason:
            label = tool_name.replace("_", " ")
            return f"I couldn't create the requested file with `{label}`: {error or reason}."
    return None


def evaluate_response_fulfillment(
    *,
    store,
    conversation_id: str,
    user_message: str,
    reply: str,
    tool_results: Iterable[ToolResult] | None = None,
    artifact_paths: Iterable[str] | None = None,
    artifact_roots: Iterable[Path] = (),
    platform_artifact_count: int = 0,
    required_attachment_extensions: Iterable[str] | None = None,
    required_embedded_media_extensions: Iterable[str] | None = None,
    required_tool_names: Iterable[str] | None = None,
    excluded_artifact_paths: Iterable[str] | None = None,
) -> ResponseFulfillmentDecision:
    """Validate that a final response is backed by required runtime evidence."""
    frame = _active_frame_for_contract(store, conversation_id)
    normalized_results = [
        (result, normalize_tool_status(getattr(result, "status", None)))
        for result in (tool_results or ())
    ]
    completed_tool_names = {result.tool_name for result, status in normalized_results if status == "completed"}
    attempted_tool_names = {result.tool_name for result, _status in normalized_results}

    explicit_required_extensions = _normalize_required_extensions(required_attachment_extensions)
    required_tools: set[str] = set()
    required_tools.update(
        str(tool_name or "").strip()
        for tool_name in (required_tool_names or ())
        if str(tool_name or "").strip()
    )
    requires_artifact = False
    artifact_kind = explicit_required_extensions[0].removeprefix(".") if explicit_required_extensions else "file"
    if frame is not None:
        required_tools.update(frame.finish.required_tool_completion)
        requires_artifact = bool(frame.finish.requires_artifact_delivery)
        artifact_kind = frame.finish.required_artifact_kind or frame.output.artifact_kind or artifact_kind
        if frame.status is TaskFrameStatus.WAITING_INPUT and not normalized_results and not artifact_paths:
            # Waiting-input frames represent an explicit clarification turn.
            # Do not convert that question into an artifact-delivery failure.
            return ResponseFulfillmentDecision(True, reply)
    required_extension = _required_extension_for_artifact_kind(artifact_kind)
    if required_extension is None and explicit_required_extensions:
        required_extension = explicit_required_extensions[0]
    if explicit_required_extensions:
        requires_artifact = True
    explicit_media_required_extensions = normalize_artifact_media_required_extensions(required_embedded_media_extensions)
    media_required_extensions = tuple(
        dict.fromkeys(
            [
                *explicit_media_required_extensions,
                *artifact_media_required_extensions(result for result, _status in normalized_results),
            ]
        )
    )
    media_required = bool(media_required_extensions)
    if media_required:
        requires_artifact = True
        if required_extension is None:
            required_extension = media_required_extensions[0]
            artifact_kind = _artifact_media_label(required_extension)

    excluded_paths = {
        str(Path(path).expanduser())
        for path in (excluded_artifact_paths or ())
        if str(path or "").strip()
    }
    combined_artifact_paths = [
        *(artifact_paths or ()),
        *artifact_paths_from_tool_results(result for result, _status in normalized_results),
    ]
    invalid_plain_media_artifact_paths = {
        str(Path(path).expanduser())
        for path in artifact_plain_replacement_artifact_paths(result for result, _status in normalized_results)
    }
    if invalid_plain_media_artifact_paths:
        combined_artifact_paths = [
            path
            for path in combined_artifact_paths
            if str(path or "").strip() and str(Path(path).expanduser()) not in invalid_plain_media_artifact_paths
        ]
    if excluded_paths:
        combined_artifact_paths = [
            path
            for path in combined_artifact_paths
            if str(path or "").strip() and str(Path(path).expanduser()) not in excluded_paths
        ]
    delivery_contract = delivery_contract_for_turn(
        None,
        reply=reply,
        artifact_paths=combined_artifact_paths,
        requires_attachment_delivery=requires_artifact,
        required_attachment_extensions=(
            *explicit_required_extensions,
            *((required_extension,) if required_extension else ()),
        ),
    )
    if not requires_artifact and delivery_contract.requires_attachment_delivery:
        requires_artifact = True

    valid_artifacts = _paths_matching_required_extension(
        _existing_deliverable_paths(combined_artifact_paths, artifact_roots=artifact_roots),
        required_extension,
    )
    if (
        not valid_artifacts
        and required_extension in IMAGE_ATTACHMENT_EXTENSIONS
        and _completed_image_fallback_used(result for result, _status in normalized_results)
    ):
        valid_artifacts = _paths_matching_required_extension(
            _existing_deliverable_paths(combined_artifact_paths, artifact_roots=artifact_roots),
            ".svg",
        )
    valid_reply_media = _paths_matching_required_extension(
        _reply_valid_media_paths(reply, artifact_roots=artifact_roots),
        required_extension,
    )
    completed_tool_names_for_requirements = set(completed_tool_names)
    if "file_write" in required_tools and "file_patch" in completed_tool_names and valid_artifacts:
        completed_tool_names_for_requirements.add("file_write")
    missing: list[str] = []
    missing_tools = sorted(tool for tool in required_tools if tool not in completed_tool_names_for_requirements)
    if missing_tools:
        missing.append(f"required tool completion: {', '.join(missing_tools)}")

    has_deliverable = (
        (platform_artifact_count > 0 and required_extension is None)
        or bool(valid_artifacts)
        or bool(valid_reply_media)
    )
    if media_required:
        embedded_media_paths = artifact_completed_embedded_media_paths(result for result, _status in normalized_results)
        valid_embedded_media_artifacts = _paths_matching_required_extension(
            _existing_deliverable_paths(embedded_media_paths, artifact_roots=artifact_roots),
            required_extension,
        )
        if not valid_embedded_media_artifacts:
            missing.append(f"{_artifact_media_label(required_extension)} attachment with embedded media")
    if requires_artifact and not has_deliverable:
        missing.append(f"{artifact_kind} attachment")
    if missing:
        setup_reply = _failed_artifact_producer_setup_reply(result for result, status in normalized_results if status != "completed")
        if setup_reply:
            return ResponseFulfillmentDecision(False, setup_reply, tuple(missing))
        missing_dependency_reply = _missing_dependency_reply(result for result, status in normalized_results if status != "completed")
        if missing_dependency_reply:
            return ResponseFulfillmentDecision(False, missing_dependency_reply, tuple(missing))
        attachment_missing = any("attachment" in item for item in missing)
        if attachment_missing:
            response = "I couldn't attach the requested file. The task is still open."
        elif attempted_tool_names:
            response = "I couldn't complete the requested operation. The task is still open."
        else:
            response = "I need more output before this task can be completed."
        return ResponseFulfillmentDecision(False, response, tuple(missing))

    if valid_artifacts and _reply_mentions_only_wrong_extension(
        reply,
        artifact_roots=artifact_roots,
        required_extension=required_extension,
    ):
        return ResponseFulfillmentDecision(
            True,
            _corrected_artifact_attachment_reply(valid_artifacts[0], required_extension),
        )

    evidence_reply = (
        _completed_tool_evidence_reply(result for result, _status in normalized_results)
        if _needs_tool_evidence_fallback(reply)
        else None
    )
    if evidence_reply:
        return ResponseFulfillmentDecision(True, evidence_reply)

    return ResponseFulfillmentDecision(True, reply)


def evaluate_response_execution_outcome(
    *,
    store,
    conversation_id: str,
    user_message: str,
    reply: str,
    tool_results: Iterable[ToolResult] | None = None,
    artifact_paths: Iterable[str] | None = None,
    artifact_roots: Iterable[Path] = (),
    platform_artifact_count: int = 0,
    required_attachment_extensions: Iterable[str] | None = None,
    required_embedded_media_extensions: Iterable[str] | None = None,
    required_tool_names: Iterable[str] | None = None,
    excluded_artifact_paths: Iterable[str] | None = None,
    delivery_status: str | None = None,
) -> ExecutionOutcomeEvaluation:
    """Return fulfillment and normalized outcome without adding a model hop."""
    decision = evaluate_response_fulfillment(
        store=store,
        conversation_id=conversation_id,
        user_message=user_message,
        reply=reply,
        tool_results=tool_results,
        artifact_paths=artifact_paths,
        artifact_roots=artifact_roots,
        platform_artifact_count=platform_artifact_count,
        required_attachment_extensions=required_attachment_extensions,
        required_embedded_media_extensions=required_embedded_media_extensions,
        required_tool_names=required_tool_names,
        excluded_artifact_paths=excluded_artifact_paths,
    )
    combined_artifacts = [
        *(artifact_paths or ()),
        *artifact_paths_from_tool_results(tool_results),
    ]
    delivered_artifacts = combined_artifacts if platform_artifact_count or delivery_status in {"sent", "saved"} else ()
    outcome = build_execution_outcome(
        requested_outcome=user_message,
        user_visible_message=decision.reply,
        tool_results=tool_results,
        artifacts_created=combined_artifacts,
        artifacts_delivered=delivered_artifacts,
        delivery_status=delivery_status,
        fulfillment_satisfied=decision.satisfied,
        missing_requirements=decision.missing_requirements,
    )
    reply = decision.reply
    if outcome.status is ExecutionStatus.NEEDS_USER_ACTION and outcome.failure_reason:
        reply = _needs_user_action_reply(outcome.failure_reason, outcome.next_action)
    satisfied = decision.satisfied and outcome.status not in {
        ExecutionStatus.NEEDS_USER_ACTION,
        ExecutionStatus.FAILED,
        ExecutionStatus.CANCELLED,
    }
    return ExecutionOutcomeEvaluation(
        reply=reply,
        satisfied=satisfied,
        missing_requirements=decision.missing_requirements,
        outcome=outcome,
    )


def _needs_user_action_reply(failure_reason: str, next_action: str | None) -> str:
    lines = [f"I need one more step before I can complete this: {failure_reason}."]
    if next_action:
        lines.append(next_action)
    return " ".join(lines)


__all__ = [
    "ResponseFulfillmentDecision",
    "artifact_completed_embedded_media_paths",
    "artifact_media_embedding_obligation_outstanding",
    "artifact_media_embedding_was_required",
    "artifact_media_plain_replacement_guard_result",
    "artifact_media_required_extensions",
    "artifact_paths_from_tool_results",
    "artifact_plain_replacement_artifact_paths",
    "evaluate_response_execution_outcome",
    "evaluate_response_fulfillment",
    "guaranteed_user_visible_text",
    "normalize_artifact_media_required_extensions",
    "spreadsheet_completed_embedded_media_paths",
    "spreadsheet_media_embedding_obligation_outstanding",
    "spreadsheet_media_embedding_was_required",
    "spreadsheet_plain_replacement_artifact_paths",
    "spreadsheet_plain_replacement_guard_result",
    "user_visible_text_from_output",
]

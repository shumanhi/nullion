"""Platform-neutral final response fulfillment checks."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from nullion.artifacts import artifact_descriptors_for_paths, media_candidate_paths_from_text
from nullion.attachment_format_graph import ATTACHMENT_TOKEN_EXTENSIONS, is_domain_suffix_extension
from nullion.messaging_adapters import delivery_contract_for_turn
from nullion.task_frames import TaskFrameStatus
from nullion.tools import ToolResult, normalize_tool_status


USER_VISIBLE_TEXT_KEYS = ("result_text", "delivery_text", "final_text", "text", "message", "summary", "content", "stdout")
EMPTY_USER_VISIBLE_TEXTS = frozenset({"", "(no reply)", "no reply"})
STRUCTURED_ARTIFACT_PRODUCER_TOOLS = frozenset(
    {
        "file_write",
        "pdf_create",
        "pdf_edit",
        "render",
        "image_generate",
        "browser_screenshot",
    }
)
IMAGE_ATTACHMENT_EXTENSIONS = frozenset({".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"})
PDF_IMAGE_PLACEHOLDER_MARKERS = (
    b"photo cue",
    b"image placeholder",
    b"scenic image",
    b"placeholder image",
)
LOCAL_FILE_CLAIM_MARKERS = (
    "found",
    "located",
    "matching file",
    "file is named",
    "file named",
    "local file",
    "on your computer",
    "path is",
)
LOCAL_PATH_RE = re.compile(
    r"(?:~|/Users/|/home/|/tmp/|[A-Za-z]:\\)[^\s`\"'<>|]+",
    flags=re.IGNORECASE,
)
FILE_NAME_RE = re.compile(
    r"(?<![\w@.-])([A-Za-z0-9][A-Za-z0-9 _()+@#%~,'=-]{0,180}\.[A-Za-z][A-Za-z0-9]{0,15})(?![\w@.-])"
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
        if tool_name not in STRUCTURED_ARTIFACT_PRODUCER_TOOLS:
            continue
        if tool_name == "browser_screenshot":
            continue
        if tool_name == "file_read" and output.get("binary") is True:
            continue
        if tool_name == "file_write":
            path = output.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path)
        if tool_name == "image_generate":
            for key in ("path", "output_path"):
                path = output.get(key)
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


def _completed_image_artifact_paths(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    paths: list[str] = []
    for result in tool_results or ():
        if result.tool_name != "image_generate":
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        for key in ("path", "output_path"):
            path = output.get(key)
            if isinstance(path, str) and path.strip():
                paths.append(path.strip())
    return tuple(dict.fromkeys(paths))


def _pdf_appears_to_embed_images(path: str) -> bool:
    try:
        payload = Path(path).read_bytes()
    except (OSError, ValueError):
        return False
    if pdf_has_unfulfilled_image_placeholders(path):
        return False
    return re.search(rb"/Subtype\s*/Image\b", payload, flags=re.IGNORECASE) is not None


def pdf_has_unfulfilled_image_placeholders(path: str) -> bool:
    try:
        payload = Path(path).read_bytes().lower()
    except (OSError, ValueError):
        return False
    # Placeholder labels only matter after a structured media requirement is
    # known. A plain PDF deliverable must not be rejected just because it uses
    # words like "photo cue" as normal document content.
    if any(marker in payload for marker in PDF_IMAGE_PLACEHOLDER_MARKERS):
        return True
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(path))
        text = "\n".join(page.extract_text() or "" for page in reader.pages).lower()
    except Exception:
        return False
    return any(marker.decode("utf-8", errors="ignore") in text for marker in PDF_IMAGE_PLACEHOLDER_MARKERS)


def _pdf_requires_embedded_images(
    pdf_paths: Iterable[str],
    *,
    tool_results: Iterable[ToolResult] | None,
    image_contract_required: bool = False,
) -> bool:
    if not image_contract_required:
        return False
    image_paths = _completed_image_artifact_paths(tool_results)
    if any(pdf_has_unfulfilled_image_placeholders(path) for path in pdf_paths):
        return True
    if not image_paths:
        return False
    # A generated image in the same PDF delivery turn is structured evidence
    # that visuals were part of the artifact contract. The final PDF must carry
    # those visuals itself; a separate image file cannot make placeholder panels
    # inside the delivered PDF acceptable.
    return any(not _pdf_appears_to_embed_images(path) for path in pdf_paths)


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


def _file_evidence_paths(tool_results: Iterable[ToolResult] | None, artifact_paths: Iterable[str] | None) -> set[str]:
    evidence: set[str] = {str(path).strip() for path in (artifact_paths or ()) if str(path or "").strip()}
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        if str(getattr(result, "tool_name", "") or "") in {"file_read", "file_search", "file_write"}:
            for key in ("path", "artifact_path"):
                value = output.get(key)
                if isinstance(value, str) and value.strip():
                    evidence.add(value.strip())
            matches = output.get("matches")
            if isinstance(matches, (list, tuple)):
                for value in matches:
                    if isinstance(value, dict):
                        for key in ("path", "name", "filename", "file"):
                            nested = value.get(key)
                            if isinstance(nested, str) and nested.strip():
                                evidence.add(nested.strip())
                    elif str(value or "").strip():
                        evidence.add(str(value).strip())
            entries = output.get("entries")
            if isinstance(entries, (list, tuple)):
                evidence.update(str(value).strip() for value in entries if str(value or "").strip())
        if str(getattr(result, "tool_name", "") or "") == "terminal_exec":
            evidence.update(_terminal_stdout_file_evidence(output.get("stdout")))
        for key in ("artifact_paths", "artifacts"):
            values = output.get(key)
            if isinstance(values, (list, tuple)):
                evidence.update(str(value).strip() for value in values if str(value or "").strip())
    return evidence


def _terminal_stdout_file_evidence(stdout: object) -> set[str]:
    if not isinstance(stdout, str) or not stdout.strip():
        return set()
    evidence: set[str] = set()
    for raw_line in stdout.splitlines():
        line = raw_line.strip().strip("`'\"")
        if not line or len(line) > 512:
            continue
        # terminal_exec is the no-auth local fallback for file discovery. Its
        # stdout can be trusted as tool evidence only for concrete file-looking
        # tokens, not for arbitrary prose the model might echo back.
        evidence.update(match.group(0).strip("`'\".,)") for match in LOCAL_PATH_RE.finditer(line))
        if not LOCAL_PATH_RE.search(line):
            evidence.update(match.group(1).strip("`'\".,)") for match in FILE_NAME_RE.finditer(line))
    return {item for item in evidence if item}


def file_evidence_paths_from_tool_results(
    tool_results: Iterable[ToolResult] | None,
    artifact_paths: Iterable[str] | None = None,
) -> set[str]:
    """Expose the same local-file evidence set used by final-answer fulfillment guards."""
    return _file_evidence_paths(tool_results, artifact_paths)


def _file_claim_tokens(reply: str) -> set[str]:
    tokens = {match.group(0).strip("`'\".,)") for match in LOCAL_PATH_RE.finditer(reply or "")}
    for match in FILE_NAME_RE.finditer(reply or ""):
        token = match.group(1).strip("`'\".,)")
        suffix = Path(token).suffix.lower()
        if is_domain_suffix_extension(suffix) or "@" in token:
            continue
        tokens.add(token)
    return {token for token in tokens if token}


def _token_has_file_evidence(token: str, evidence_paths: set[str]) -> bool:
    token_name = Path(token).name.lower()
    token_norm = str(Path(token).expanduser()).lower()
    for raw_path in evidence_paths:
        candidate = str(raw_path or "").strip()
        if not candidate:
            continue
        candidate_name = Path(candidate).name.lower()
        candidate_norm = str(Path(candidate).expanduser()).lower()
        if token_name and token_name == candidate_name:
            return True
        if token_norm and token_norm == candidate_norm:
            return True
    return False


def _local_file_terminal_boundary_reply(tool_results: Iterable[ToolResult] | None) -> str | None:
    saw_empty_file_search = False
    saw_terminal_fallback = False
    for result in tool_results or ():
        status = normalize_tool_status(getattr(result, "status", None))
        output = result.output if isinstance(result.output, dict) else {}
        tool_name = str(getattr(result, "tool_name", "") or "")
        if tool_name == "file_search" and status == "completed":
            matches = output.get("matches")
            files = output.get("files")
            saw_empty_file_search = (
                (isinstance(matches, list) and len(matches) == 0)
                or (isinstance(files, list) and len(files) == 0)
                or saw_empty_file_search
            )
        if tool_name == "terminal_exec":
            saw_terminal_fallback = True
    if not (saw_empty_file_search and saw_terminal_fallback):
        return None
    return (
        "I searched the currently allowed file scope and tried the local shell fallback, but I still do not have "
        "a verified matching file to report.\n\n"
        "Please reply with one option:\n"
        "1. Search my home folder\n"
        "2. Search Documents and Downloads\n"
        "3. I will send the exact folder path"
    )


def _unverified_local_file_claim_reply(
    reply: str,
    *,
    user_message: str,
    tool_results: Iterable[ToolResult] | None,
    artifact_paths: Iterable[str] | None,
) -> str | None:
    tool_results_tuple = tuple(tool_results or ())
    tokens = _file_claim_tokens(reply)
    if not tokens:
        return None
    lowered = str(reply or "").lower()
    has_local_path = any(LOCAL_PATH_RE.search(token) for token in tokens)
    has_claim_marker = any(marker in lowered for marker in LOCAL_FILE_CLAIM_MARKERS)
    attempted_file_discovery = any(
        str(getattr(result, "tool_name", "") or "") in {"file_read", "file_search", "file_write", "terminal_exec"}
        for result in tool_results_tuple
    )
    user_tokens = _file_claim_tokens(user_message)
    novel_file_tokens = tokens.difference(user_tokens)
    if not has_local_path and not has_claim_marker and not (attempted_file_discovery and novel_file_tokens):
        return None
    evidence_paths = _file_evidence_paths(tool_results_tuple, artifact_paths)
    if tokens and all(_token_has_file_evidence(token, evidence_paths) for token in tokens):
        return None
    terminal_boundary = _local_file_terminal_boundary_reply(tool_results_tuple)
    if terminal_boundary is not None:
        # Fulfillment runs after platform-specific delivery code too. Keep the
        # same fallback contract here so a later Web/Telegram pass cannot
        # replace a shell-fallback miss with a vague "couldn't verify" answer.
        return terminal_boundary
    # Local-file discovery is a trust boundary: the product may only claim a
    # novel filename/path when completed tool evidence supplied it. This guard
    # is token/evidence based so non-English replies get the same protection.
    return (
        "I couldn't verify that file location from a completed tool result, "
        "so I should not claim I found it. Please send the file, provide a folder to search, "
        "or ask me to search an allowed location."
    )


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
            return (
                f"I couldn't create the requested artifact with `{tool_name}` because {error or reason}. "
                f"Please update the {label} setup or choose another deliverable format, then retry."
            )
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
    required_extension = _required_extension_for_artifact_kind(artifact_kind)
    if required_extension is None and explicit_required_extensions:
        required_extension = explicit_required_extensions[0]
    if explicit_required_extensions:
        requires_artifact = True

    excluded_paths = {
        str(Path(path).expanduser())
        for path in (excluded_artifact_paths or ())
        if str(path or "").strip()
    }
    combined_artifact_paths = [
        *(artifact_paths or ()),
        *artifact_paths_from_tool_results(result for result, _status in normalized_results),
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

    missing: list[str] = []
    missing_tools = sorted(tool for tool in required_tools if tool not in completed_tool_names)
    if missing_tools:
        missing.append(f"required tool completion: {', '.join(missing_tools)}")

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
    has_deliverable = (
        (platform_artifact_count > 0 and required_extension is None)
        or bool(valid_artifacts)
        or bool(valid_reply_media)
    )
    if requires_artifact and not has_deliverable:
        missing.append(f"{artifact_kind} attachment")
    if required_extension == ".pdf" and valid_artifacts and _pdf_requires_embedded_images(
        valid_artifacts,
        tool_results=(result for result, _status in normalized_results),
        image_contract_required=(
            "image_generate" in required_tools
            or any(result.tool_name == "image_generate" for result, _status in normalized_results)
        ),
    ):
        missing.append("pdf attachment with embedded images")

    if missing:
        setup_reply = _failed_artifact_producer_setup_reply(result for result, status in normalized_results if status != "completed")
        if setup_reply:
            return ResponseFulfillmentDecision(False, setup_reply, tuple(missing))
        attachment_missing = any("attachment" in item for item in missing)
        if attachment_missing:
            response = "I couldn't attach the requested file. The task is still open."
        elif attempted_tool_names:
            response = "I couldn't complete the requested operation. The task is still open."
        else:
            response = "I need more output before this task can be completed."
        return ResponseFulfillmentDecision(False, response, tuple(missing))

    evidence_reply = (
        _completed_tool_evidence_reply(result for result, _status in normalized_results)
        if _needs_tool_evidence_fallback(reply)
        else None
    )
    if evidence_reply:
        return ResponseFulfillmentDecision(True, evidence_reply)

    unverified_file_reply = _unverified_local_file_claim_reply(
        reply,
        user_message=user_message,
        tool_results=(result for result, _status in normalized_results),
        artifact_paths=combined_artifact_paths,
    )
    if unverified_file_reply:
        return ResponseFulfillmentDecision(False, unverified_file_reply, ("verified local file evidence",))

    return ResponseFulfillmentDecision(True, reply)


__all__ = [
    "ResponseFulfillmentDecision",
    "artifact_paths_from_tool_results",
    "evaluate_response_fulfillment",
    "file_evidence_paths_from_tool_results",
    "guaranteed_user_visible_text",
    "pdf_has_unfulfilled_image_placeholders",
    "user_visible_text_from_output",
]

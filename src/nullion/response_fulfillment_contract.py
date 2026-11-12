"""Platform-neutral final response fulfillment checks."""

from __future__ import annotations

import html
import logging
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import unquote, urlparse
import zipfile

from nullion.artifacts import (
    ARTIFACT_DELIVERY_ROLES,
    artifact_format_extension,
    artifact_descriptors_for_paths,
    artifact_paths_from_output_descriptors,
    is_unrequested_internal_sidecar_artifact,
    media_candidate_paths_from_text,
    output_has_artifact_descriptors,
)
from nullion.attachment_format_graph import (
    ATTACHMENT_TOKEN_EXTENSIONS,
    VALID_ATTACHMENT_EXTENSIONS,
    is_domain_suffix_extension,
)
from nullion.execution_outcome import ExecutionOutcomeEvaluation, ExecutionStatus, build_execution_outcome
from nullion.messaging_adapters import delivery_contract_for_turn
from nullion.task_frames import TaskFrameStatus
from nullion.tools import ToolInvocation, ToolResult, normalize_tool_status


USER_VISIBLE_TEXT_KEYS = ("result_text", "delivery_text", "final_text", "text", "message", "summary", "content")
EMPTY_USER_VISIBLE_TEXTS = frozenset({"", "(no reply)", "no reply"})
STRUCTURED_ARTIFACT_PRODUCER_TOOLS = frozenset(
    {
        "document_create",
        "file_write",
        "html_create",
        "pdf_create",
        "pdf_edit",
        "image_generate",
        "browser_screenshot",
        "presentation_create",
        "spreadsheet_create",
    }
)
SCHEDULER_MUTATION_TOOLS = frozenset(
    {
        "create_cron",
        "delete_cron",
        "delete_reminder",
        "set_reminder",
        "toggle_cron",
        "update_cron",
        "update_reminder",
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
_MEDIA_CAPABLE_ARTIFACT_EXTENSIONS = frozenset({".docx", ".html", ".htm", ".pdf", ".pptx", ".xlsx"})
_MEDIA_ARTIFACT_EXTENSION_ALIASES = {
    ".doc": ".docx",
    ".htm": ".html",
    ".ppt": ".pptx",
    ".xls": ".xlsx",
}
_MEDIA_ARTIFACT_LABELS = {
    ".csv": "CSV",
    ".docx": "document",
    ".html": "HTML",
    ".pdf": "PDF",
    ".pptx": "presentation",
    ".svg": "SVG",
    ".tsv": "TSV",
    ".txt": "text",
    ".xlsx": "spreadsheet",
}
_OFFICE_EMBEDDED_MEDIA_PREFIXES = {
    ".docx": ("word/media/",),
    ".pptx": ("ppt/media/",),
    ".xlsx": ("xl/media/",),
}
_RASTER_IMAGE_ATTACHMENT_EXTENSIONS = frozenset({".gif", ".jpeg", ".jpg", ".png", ".webp"})
_HTML_IMAGE_SOURCE_RE = re.compile(
    r"""<(?:img|source)\b[^>]*\bsrc\s*=\s*["'](?P<src>[^"']+)["']""",
    flags=re.IGNORECASE,
)
_HTML_CSS_IMAGE_SOURCE_RE = re.compile(
    r"""url\(\s*["']?(?P<src>[^"')]+)["']?\s*\)""",
    flags=re.IGNORECASE,
)
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
_UNVERIFIED_BROWSER_SCREENSHOT_URLS = frozenset({"", "about:blank", "chrome://newtab/", "brave://newtab/"})
_TERMINAL_STDOUT_ATTACHMENT_PATH_RE = re.compile(
    r"(?<![\w./~-])(?P<path>(?:~|/)[^\s`'\"<>]+?\.[A-Za-z0-9]{1,16})(?![\w/-])"
)
_BARE_FILENAME_LISTING_RE = re.compile(r"^[^\s/\\\x00]{1,255}\.[A-Za-z0-9]{1,16}$")
_RELATIVE_PATH_LISTING_RE = re.compile(r"^(?![A-Za-z][A-Za-z0-9+.-]*:)[^\s\x00]{1,512}\.[A-Za-z0-9]{1,16}$")


@dataclass(frozen=True, slots=True)
class ResponseFulfillmentDecision:
    satisfied: bool
    reply: str
    missing_requirements: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _ArtifactScopeExtensionGroup:
    index: int
    extensions: tuple[str, ...]
    media_extensions: tuple[str, ...]


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



def _artifact_invocation_media_extension(invocation: ToolInvocation) -> str | None:
    """Resolve a native creator's actual format from its structured target."""
    extension = _artifact_media_tool_extension(invocation.tool_name)
    if extension is None:
        return None
    target = invocation.arguments.get("output_path") or invocation.arguments.get("path")
    if isinstance(target, str):
        suffix = Path(target).suffix.lower()
        if suffix in _MEDIA_CAPABLE_ARTIFACT_EXTENSIONS:
            return ".html" if suffix == ".htm" else suffix
        if invocation.tool_name == "spreadsheet_create" and suffix in {".csv", ".tsv"}:
            return None
    return extension

def _artifact_result_is_producer_receipt(result: ToolResult) -> bool:
    """A referenced path is not evidence that an inspection rewrote a file."""
    name = str(getattr(result, "tool_name", ""))
    output = result.output if isinstance(result.output, Mapping) else {}
    if _artifact_media_tool_extension(name) or name in {"file_write", "file_patch", "terminal_exec"}:
        return True
    descriptors = output.get("artifact_descriptors")
    if isinstance(descriptors, (list, tuple)) and descriptors:
        return any(isinstance(row, Mapping) and row.get("role") == "deliverable" for row in descriptors)
    # Third-party writers must declare generated artifacts through their result
    # schema. Reader output containing only path/content retains read semantics.
    return bool(output.get("artifact_path") or output.get("artifact_paths")
        or output.get("artifact_producer_receipt_id"))


def _artifact_result_media_extension(result: ToolResult) -> str | None:
    static_extension = _artifact_media_tool_extension(getattr(result, "tool_name", None))
    output = result.output if isinstance(result.output, dict) else {}
    paths = tuple(_artifact_result_paths(result))
    for path in paths:
        suffix = Path(path).suffix.lower()
        if suffix in _MEDIA_CAPABLE_ARTIFACT_EXTENSIONS:
            return ".html" if suffix == ".htm" else suffix
    if (str(getattr(result, "tool_name", "")) == "spreadsheet_create" and paths
            and all(Path(path).suffix.lower() in {".csv", ".tsv"} for path in paths)):
        return None
    return static_extension


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


def _normalize_valid_attachment_extensions(value: Iterable[object] | object | None) -> tuple[str, ...]:
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
        if extension in VALID_ATTACHMENT_EXTENSIONS and extension not in extensions:
            extensions.append(extension)
    return tuple(extensions)


def _result_like_tool_name(result: object) -> str:
    if isinstance(result, Mapping):
        return str(result.get("tool_name") or result.get("name") or "")
    return str(getattr(result, "tool_name", "") or "")


def _result_like_status(result: object) -> object:
    if isinstance(result, Mapping):
        return result.get("status")
    return getattr(result, "status", None)


def _result_like_output(result: object) -> Mapping[str, object]:
    output = result.get("output") if isinstance(result, Mapping) else getattr(result, "output", None)
    return output if isinstance(output, Mapping) else {}


def _result_like_paths(result: object) -> tuple[str, ...]:
    output = _result_like_output(result)
    paths: list[str] = []
    for key in ("path", "file_path", "output_path", "artifact_path"):
        value = output.get(key)
        if isinstance(value, str) and value.strip():
            paths.append(value.strip())
    for key in ("artifact_paths", "artifacts"):
        values = output.get(key)
        if isinstance(values, (list, tuple, set)):
            for value in values:
                if isinstance(value, str) and value.strip():
                    paths.append(value.strip())
                elif isinstance(value, Mapping):
                    for path_key in ("path", "artifact_path", "file_path", "output_path"):
                        path_value = value.get(path_key)
                        if isinstance(path_value, str) and path_value.strip():
                            paths.append(path_value.strip())
                            break
    descriptors = output.get("artifact_descriptors")
    if isinstance(descriptors, (list, tuple, set)):
        for descriptor in descriptors:
            if not isinstance(descriptor, Mapping):
                continue
            for path_key in ("path", "artifact_path", "file_path", "output_path"):
                path_value = descriptor.get(path_key)
                if isinstance(path_value, str) and path_value.strip():
                    paths.append(path_value.strip())
                    break
    return tuple(dict.fromkeys(paths))


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
    if _nested_media_inputs_present(arguments, keys=frozenset(media_keys)):
        return True
    # HTML image elements are structured media inputs too. The PDF handler
    # resolves/validates their sources and final fulfillment inspects the bytes.
    from nullion.artifact_evidence import inline_svg_fragments
    return any(isinstance(page, str) and (_HTML_IMAGE_SOURCE_RE.search(page) or inline_svg_fragments(page))
               for page in arguments.get("html_pages", ()) or ())


def _browser_screenshot_path_identities(
    tool_results: Iterable[ToolResult] | None,
    *,
    verified: bool | None = None,
) -> set[str]:
    identities: set[str] = set()
    for result in tool_results or ():
        if str(getattr(result, "tool_name", "") or "") != "browser_screenshot":
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, dict) else {}
        page_url = str(output.get("page_url") or "").strip().lower()
        result_verified = page_url not in _UNVERIFIED_BROWSER_SCREENSHOT_URLS
        if verified is not None and result_verified is not verified:
            continue
        for key in ("path", "artifact_path"):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                identities.update(_path_identity_variants(value))
        values = output.get("artifact_paths")
        if isinstance(values, (list, tuple)):
            for value in values:
                if isinstance(value, str) and value.strip():
                    identities.update(_path_identity_variants(value))
    return identities


def _artifact_media_count(
    output: dict[str, object],
    *,
    known_browser_screenshot_identities: set[str] | None = None,
    verified_browser_screenshot_identities: set[str] | None = None,
) -> int:
    count = 0
    known_screenshots = known_browser_screenshot_identities or set()
    verified_screenshots = verified_browser_screenshot_identities or set()
    for key in ("embedded_images", "source_image_paths"):
        values = output.get(key)
        if isinstance(values, (list, tuple)):
            count += sum(1 for value in values if isinstance(value, str) and value.strip())
    for key in ("embedded_screenshots", "source_screenshot_paths"):
        values = output.get(key)
        if not isinstance(values, (list, tuple)):
            continue
        for value in values:
            if not isinstance(value, str) or not value.strip():
                continue
            identities = set(_path_identity_variants(value))
            if identities & known_screenshots and not identities & verified_screenshots:
                continue
            count += 1
    html_images = output.get("embedded_html_images")
    if isinstance(html_images, (list, tuple)):
        count += sum(
            1
            for value in html_images
            if (
                isinstance(value, dict)
                and isinstance(value.get("path"), str)
                and value.get("path", "").strip()
            )
            or (isinstance(value, str) and value.strip())
        )
    return count


def artifact_output_media_count(output: object) -> int:
    """Return media evidence count reported by a structured tool output."""
    if not isinstance(output, dict):
        return 0
    return _artifact_media_count(output)


def _office_artifact_embedded_media_count(path: Path) -> int:
    prefixes = _OFFICE_EMBEDDED_MEDIA_PREFIXES.get(path.suffix.lower())
    if not prefixes:
        return 0
    try:
        with zipfile.ZipFile(path) as archive:
            return sum(
                1
                for name in archive.namelist()
                if any(name.startswith(prefix) and not name.endswith("/") for prefix in prefixes)
            )
    except (OSError, zipfile.BadZipFile):
        return 0


def _html_raster_media_count(path: Path) -> int:
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return 0
    count = 0
    for match in list(_HTML_IMAGE_SOURCE_RE.finditer(content)) + list(_HTML_CSS_IMAGE_SOURCE_RE.finditer(content)):
        source = html.unescape(str(match.group("src") or "").strip())
        if not source:
            continue
        lowered = source.casefold()
        if lowered.startswith("data:image/svg"):
            continue
        if lowered.startswith("data:image/"):
            count += 1
            continue
        parsed_path = urlparse(source).path if "://" in source else source
        if Path(parsed_path).suffix.casefold() in _RASTER_IMAGE_ATTACHMENT_EXTENSIONS:
            count += 1
    return count


def _pdf_embedded_media_count(path: Path) -> int:
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(path))
        count = 0
        for page in reader.pages:
            resources = page.get("/Resources") or {}
            xobjects = resources.get("/XObject") or {}
            try:
                items = xobjects.items()
            except AttributeError:
                items = ()
            for _, raw_obj in items:
                try:
                    obj = raw_obj.get_object()
                except Exception:
                    obj = raw_obj
                if obj.get("/Subtype") == "/Image":
                    count += 1
        if count > 0:
            return count
    except Exception:
        pass
    try:
        data = path.read_bytes()
    except OSError:
        return 0
    return data.count(b"/Subtype /Image")


def artifact_file_embedded_media_count(path: str | Path) -> int:
    """Return embedded/attached media count visible in an artifact file."""
    artifact_path = Path(path).expanduser()
    suffix = artifact_path.suffix.casefold()
    if suffix in _RASTER_IMAGE_ATTACHMENT_EXTENSIONS:
        try:
            return 1 if artifact_path.is_file() and artifact_path.stat().st_size > 0 else 0
        except OSError:
            return 0
    if suffix in _OFFICE_EMBEDDED_MEDIA_PREFIXES:
        return _office_artifact_embedded_media_count(artifact_path)
    if suffix in {".html", ".htm"}:
        from nullion.artifact_evidence import self_contained_html_svg_count
        try:
            svg_count = self_contained_html_svg_count(artifact_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError):
            svg_count = 0
        return _html_raster_media_count(artifact_path) + svg_count
    if suffix == ".pdf":
        return _pdf_embedded_media_count(artifact_path)
    return 0


def _artifact_result_media_count(
    result: ToolResult,
    *,
    known_browser_screenshot_identities: set[str] | None = None,
    verified_browser_screenshot_identities: set[str] | None = None,
) -> int:
    output = result.output if isinstance(result.output, dict) else {}
    count = _artifact_media_count(
        output,
        known_browser_screenshot_identities=known_browser_screenshot_identities,
        verified_browser_screenshot_identities=verified_browser_screenshot_identities,
    )
    if count > 0:
        return count
    for raw_path in _artifact_result_paths(result):
        count += artifact_file_embedded_media_count(raw_path)
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
            for value in values:
                if isinstance(value, str) and value.strip():
                    paths.append(value.strip())
                elif isinstance(value, Mapping):
                    for path_key in ("path", "artifact_path", "file_path"):
                        path_value = value.get(path_key)
                        if isinstance(path_value, str) and path_value.strip():
                            paths.append(path_value.strip())
                            break
    descriptors = output.get("artifact_descriptors")
    if isinstance(descriptors, (list, tuple)):
        for descriptor in descriptors:
            if not isinstance(descriptor, Mapping):
                continue
            for path_key in ("path", "artifact_path", "file_path"):
                path_value = descriptor.get(path_key)
                if isinstance(path_value, str) and path_value.strip():
                    paths.append(path_value.strip())
                    break
    return tuple(dict.fromkeys(paths))


def _artifact_scope_extension_groups(results: Iterable[object]) -> tuple[_ArtifactScopeExtensionGroup, ...]:
    groups: list[_ArtifactScopeExtensionGroup] = []
    for index, result in enumerate(results):
        if _result_like_tool_name(result) != "request_tool_scope":
            continue
        if normalize_tool_status(_result_like_status(result)) != "completed":
            continue
        output = _result_like_output(result)
        extensions: list[str] = []
        for key in ("artifact_extensions", "requested_artifact_extensions", "required_artifact_extensions"):
            for extension in _normalize_valid_attachment_extensions(output.get(key)):
                if extension not in extensions:
                    extensions.append(extension)
        media_extensions: list[str] = []
        for key in _MEDIA_ARTIFACT_SCOPE_KEYS:
            for extension in normalize_artifact_media_required_extensions(output.get(key)):
                if extension not in media_extensions:
                    media_extensions.append(extension)
                if extension not in extensions:
                    extensions.append(extension)
        if extensions:
            groups.append(
                _ArtifactScopeExtensionGroup(
                    index=index,
                    extensions=tuple(extensions),
                    media_extensions=tuple(media_extensions),
                )
            )
    return tuple(groups)


def _completed_artifact_extension_scores(results: Iterable[object]) -> Counter[str]:
    scores: Counter[str] = Counter()
    for result in results:
        if normalize_tool_status(_result_like_status(result)) != "completed":
            continue
        tool_name = _result_like_tool_name(result)
        output = dict(_result_like_output(result))
        for path in artifact_paths_from_output_descriptors(output, roles=ARTIFACT_DELIVERY_ROLES):
            suffix = Path(path).suffix.lower()
            if suffix in VALID_ATTACHMENT_EXTENSIONS:
                scores[suffix] += 8
        if output_has_artifact_descriptors(output):
            continue
        weight = 1
        if tool_name in STRUCTURED_ARTIFACT_PRODUCER_TOOLS:
            weight = 2 if tool_name == "file_write" else 4
        for path in _result_like_paths(result):
            suffix = Path(path).suffix.lower()
            if suffix in VALID_ATTACHMENT_EXTENSIONS:
                scores[suffix] += weight
    return scores


def _completed_artifact_extensions_in_order(results: Iterable[object]) -> tuple[str, ...]:
    extensions: list[str] = []
    for result in results:
        if normalize_tool_status(_result_like_status(result)) != "completed":
            continue
        output = dict(_result_like_output(result))
        paths = [
            *artifact_paths_from_output_descriptors(output, roles=ARTIFACT_DELIVERY_ROLES),
            *_result_like_paths(result),
        ]
        for path in paths:
            suffix = Path(path).suffix.lower()
            if suffix == ".htm":
                suffix = ".html"
            if suffix in VALID_ATTACHMENT_EXTENSIONS and suffix not in extensions:
                extensions.append(suffix)
    return tuple(extensions)


def _completed_structural_artifact_extensions_in_order(results: Iterable[object]) -> tuple[str, ...]:
    non_image_extensions: list[str] = []
    image_extensions: list[str] = []
    support_extensions: list[str] = []

    def add_extension(extension: str, *, image: bool = False) -> None:
        if extension == ".htm":
            extension = ".html"
        if extension not in VALID_ATTACHMENT_EXTENSIONS:
            return
        bucket = image_extensions if image else non_image_extensions
        if extension not in bucket:
            bucket.append(extension)

    for result in results:
        if normalize_tool_status(_result_like_status(result)) != "completed":
            continue
        tool_name = _result_like_tool_name(result)
        output = dict(_result_like_output(result))
        if output_has_artifact_descriptors(output):
            paths = artifact_paths_from_output_descriptors(output, roles=ARTIFACT_DELIVERY_ROLES)
            if not paths:
                continue
        else:
            # Source-file and changed-object identities are evidence, not
            # deliverable declarations. Infer formats only from producers or
            # explicit artifact fields, never an incidental read/edit path.
            receipt = output.get("action_receipt")
            ordinary_file_action = isinstance(receipt, dict) and receipt.get("object_type") == "file"
            if tool_name in STRUCTURED_ARTIFACT_PRODUCER_TOOLS and not ordinary_file_action:
                paths = list(_result_like_paths(result))
            else:
                paths = []
                for key in ("artifact_path", "artifact_paths", "artifacts"):
                    value = output.get(key)
                    if isinstance(value, str):
                        paths.append(value)
                    elif isinstance(value, (list, tuple)):
                        paths.extend(path for path in value if isinstance(path, str))
        if tool_name in _MEDIA_ARTIFACT_TOOL_EXTENSIONS:
            expected_extension = _MEDIA_ARTIFACT_TOOL_EXTENSIONS[tool_name]
            if not paths:
                add_extension(expected_extension)
                continue
            if any(Path(path).suffix.lower() in {expected_extension, ".htm"} for path in paths):
                add_extension(expected_extension)
                continue
        for path in paths:
            suffix = Path(path).suffix.lower()
            if suffix == ".htm":
                suffix = ".html"
            if suffix not in VALID_ATTACHMENT_EXTENSIONS:
                continue
            if is_unrequested_internal_sidecar_artifact(path, requested_extensions=()):
                continue
            if suffix in {".json", ".log", ".md", ".txt"}:
                if suffix not in support_extensions:
                    support_extensions.append(suffix)
                continue
            if suffix in IMAGE_ATTACHMENT_EXTENSIONS:
                if tool_name == "image_generate":
                    add_extension(suffix, image=True)
                continue
            if tool_name in STRUCTURED_ARTIFACT_PRODUCER_TOOLS or tool_name == "terminal_exec":
                add_extension(suffix)
    if non_image_extensions:
        return tuple(non_image_extensions)
    if image_extensions:
        return tuple(image_extensions)
    return tuple(support_extensions)


def _union_scope_extensions(groups: Iterable[_ArtifactScopeExtensionGroup]) -> tuple[str, ...]:
    extensions: list[str] = []
    for group in groups:
        for extension in group.extensions:
            if extension not in extensions:
                extensions.append(extension)
    return tuple(extensions)


def scoped_artifact_extensions_from_tool_results(tool_results: Iterable[object] | None, *, user_message: str | None = None) -> tuple[str, ...]:
    """Return the best final artifact-extension contract from typed tool evidence.

    A turn can request helper artifacts while building the final answer. Delivery
    must not union every intermediate `request_tool_scope` extension, or support
    files become user-facing attachments. Prefer the scope backed by completed
    deliverable artifacts, and prefer media-bearing scopes when they exist.
    """
    results = tuple(tool_results or ())
    from nullion.request_delivery_requirements import reviewed_request_delivery_requirements
    current = reviewed_request_delivery_requirements(results, user_message)
    if current is not None:
        return tuple(current['required_attachment_extensions'])
    from nullion.artifact_lifecycle import reviewed_delivery_extensions
    reviewed = reviewed_delivery_extensions(results)
    if reviewed is not None:
        # A current completed file manifest owns delivery. Intermediate scope
        # guesses and packaging exclusions cannot narrow its reviewed siblings.
        return reviewed

    groups = _artifact_scope_extension_groups(results)
    if not groups:
        return _completed_structural_artifact_extensions_in_order(results)
    scores = _completed_artifact_extension_scores(results)
    media_completed_suffixes: set[str] = set()
    for result in results:
        if normalize_tool_status(_result_like_status(result)) != "completed":
            continue
        output = _result_like_output(result)
        if _artifact_media_count(output) <= 0:
            continue
        for path in _result_like_paths(result):
            suffix = Path(path).suffix.lower()
            if suffix in _MEDIA_CAPABLE_ARTIFACT_EXTENSIONS:
                media_completed_suffixes.add(".html" if suffix == ".htm" else suffix)
    candidates = tuple(group for group in groups if group.media_extensions) or groups

    def group_score(group: _ArtifactScopeExtensionGroup) -> tuple[int, int, int, int]:
        artifact_score = sum(scores.get(extension, 0) for extension in group.extensions)
        media_score = 0
        if group.media_extensions:
            media_score += 20
            media_score += sum(30 for extension in group.media_extensions if extension in media_completed_suffixes)
        matched_extension_count = sum(1 for extension in group.extensions if scores.get(extension, 0) > 0)
        return (media_score + artifact_score, matched_extension_count, len(group.extensions), -group.index)

    best = max(candidates, key=group_score)
    best_score = group_score(best)[0]
    if best.extensions and set(best.extensions).issubset(IMAGE_ATTACHMENT_EXTENSIONS):
        non_image_extensions = tuple(
            extension
            for extension in _completed_artifact_extensions_in_order(results)
            if extension not in IMAGE_ATTACHMENT_EXTENSIONS
        )
        if len(non_image_extensions) >= 2:
            return non_image_extensions
    if best_score <= 0 and not best.media_extensions:
        return _union_scope_extensions(groups)
    return best.extensions


def artifact_path_embedded_media_count(
    path: str | Path,
    *,
    tool_results: Iterable[ToolResult] | None = None,
) -> int:
    """Return media evidence for one artifact path from file bytes and tool metadata."""
    identities = set(_path_identity_variants(path))
    metadata_count = 0
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        result_paths = _artifact_result_paths(result)
        if not result_paths:
            continue
        if not any(_path_matches_identities(raw_path, identities) for raw_path in result_paths):
            continue
        output = result.output if isinstance(result.output, dict) else {}
        metadata_count += _artifact_media_count(output)
    return max(metadata_count, artifact_file_embedded_media_count(path))


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



def _artifact_failure_requires_media(result: ToolResult) -> bool:
    if not _artifact_result_reports_media_failure(result):
        return False
    requirements = getattr(result, "response_requirements", None)
    native = requirements.get("_native_artifact_media") if isinstance(requirements, dict) else None
    if (isinstance(native, dict) and native.get("version") == 1
            and native.get("extension") == _artifact_result_media_extension(result)
            and isinstance(native.get("required"), bool)):
        return native["required"]
    # Legacy receipts retain their original semantics. New native executions
    # carry a runtime-owned contract; provider output cannot supply that flag.
    return True

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




def _path_identity_set(paths: Iterable[object]) -> set[str]:
    return {
        identity
        for path in paths
        for identity in _path_identity_variants(path)
    }


def _path_matches_identities(path: object, identities: set[str]) -> bool:
    return any(identity in identities for identity in _path_identity_variants(path))


def _collected_browser_image_path_identities(tool_results: Iterable[ToolResult] | None) -> set[str]:
    results = tuple(tool_results or ())
    identities = {
        identity
        for result in results
        for path in _browser_image_collection_paths(result)
        for identity in _path_identity_variants(path)
    }
    if not identities:
        return identities
    # A later successful source-image download can replace an earlier browser
    # collection candidate. Provenance follows typed source receipts, not the
    # incidental choice of acquisition tool or the first downloaded filename.
    for result in results:
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, Mapping) else {}
        for descriptor in output.get("artifact_descriptors") or ():
            if (isinstance(descriptor, Mapping) and descriptor.get("role") == "source"
                    and isinstance(descriptor.get("path"), str)
                    and Path(descriptor["path"]).suffix.lower() in _RASTER_IMAGE_ATTACHMENT_EXTENSIONS):
                identities.update(_path_identity_variants(descriptor["path"]))
    return identities


def _artifact_result_embedded_source_image_paths(result: ToolResult) -> tuple[str, ...]:
    output = result.output if isinstance(result.output, dict) else {}
    paths: list[str] = []
    for key in ("embedded_images", "source_image_paths", "image_paths"):
        values = output.get(key)
        if isinstance(values, str) and values.strip():
            paths.append(values.strip())
        elif isinstance(values, (list, tuple)):
            paths.extend(str(value).strip() for value in values if isinstance(value, str) and value.strip())
    html_images = output.get("embedded_html_images")
    if isinstance(html_images, (list, tuple)):
        for item in html_images:
            if isinstance(item, dict):
                value = item.get("path")
                if isinstance(value, str) and value.strip():
                    paths.append(value.strip())
            elif isinstance(item, str) and item.strip():
                paths.append(item.strip())
    return tuple(dict.fromkeys(paths))


def _completed_media_artifact_paths_matching_collected_image(
    tool_results: Iterable[ToolResult] | None,
    *,
    extension: str,
    collected_image_identities: set[str],
) -> tuple[str, ...]:
    if not collected_image_identities:
        return ()
    paths: list[str] = []
    for result in tool_results or ():
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        if _artifact_result_media_extension(result) != extension:
            continue
        if not any(
            _path_matches_identities(path, collected_image_identities)
            for path in _artifact_result_embedded_source_image_paths(result)
        ):
            continue
        paths.extend(_artifact_result_paths_matching_extension(result, extension))
    return tuple(dict.fromkeys(paths))


def _browser_page_context_used(result: ToolResult) -> bool:
    """Detect structured source-image evidence, not merely browser use.

    A page's numeric/text observations can support a generated chart. They do
    not declare that the user requested source photographs. Explicit asset
    candidates and failed image collection still retain their provenance guard.
    """
    tool_name = str(getattr(result, "tool_name", "") or "")
    if not tool_name.startswith("browser_") or tool_name in {"browser_image_collect", "browser_screenshot"}:
        return False
    image_fields = {"img", "image_url", "image_src", "image_paths", "image_urls", "images", "source_image_paths"}
    def has_assets(value, depth=0):
        if depth > 8:
            return False
        if isinstance(value, dict):
            return any((key in image_fields and _has_nonempty_media_value(item))
                       or has_assets(item, depth + 1) for key, item in value.items())
        return isinstance(value, (list, tuple)) and any(has_assets(item, depth + 1) for item in value)
    return has_assets(getattr(result, "output", None))


def _artifact_result_required_media_extensions(result: ToolResult) -> tuple[str, ...]:
    output = result.output if isinstance(result.output, dict) else {}
    extensions: list[str] = []
    for key in _MEDIA_ARTIFACT_SCOPE_KEYS:
        for extension in normalize_artifact_media_required_extensions(output.get(key)):
            if extension not in extensions:
                extensions.append(extension)
    if not extensions and _artifact_failure_requires_media(result):
        extension = _artifact_result_media_extension(result)
        if extension:
            extensions.append(extension)
    return tuple(extensions)


def artifact_media_required_extensions(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    results = tuple(tool_results or ())
    extensions: list[str] = []
    requested_extensions = _artifact_requested_extensions(results)
    collected_image_identities = _collected_browser_image_path_identities(results)
    if collected_image_identities:
        for result in results:
            if not _artifact_result_is_producer_receipt(result):
                continue
            extension = _artifact_result_media_extension(result)
            if extension is None:
                continue
            if requested_extensions and extension not in requested_extensions:
                continue
            if normalize_tool_status(getattr(result, "status", None)) != "completed":
                continue
            if any(
                _path_matches_identities(path, collected_image_identities)
                for path in _artifact_result_embedded_source_image_paths(result)
            ):
                continue
            if extension not in extensions:
                extensions.append(extension)
    for result in results:
        for extension in _artifact_result_required_media_extensions(result):
            if requested_extensions and extension not in requested_extensions:
                continue
            if extension not in extensions:
                extensions.append(extension)
    return tuple(extensions)


def _artifact_requested_extensions(results: Iterable[ToolResult]) -> set[str]:
    result_tuple = tuple(results or ())
    if not any(
        str(getattr(result, "tool_name", "") or "") == "request_tool_scope"
        and normalize_tool_status(getattr(result, "status", None)) == "completed"
        for result in result_tuple
    ):
        return set()
    return set(scoped_artifact_extensions_from_tool_results(result_tuple))


def artifact_media_embedding_was_required(tool_results: Iterable[ToolResult] | None) -> bool:
    return bool(artifact_media_required_extensions(tool_results))


def artifact_media_embedding_obligation_outstanding(tool_results: Iterable[ToolResult] | None) -> bool:
    results = tuple(tool_results or ())
    outstanding: set[str] = set()
    collected_image_identities = _collected_browser_image_path_identities(results)
    browser_screenshot_identities = _browser_screenshot_path_identities(results)
    verified_browser_screenshot_identities = _browser_screenshot_path_identities(results, verified=True)
    if collected_image_identities:
        for result in results:
            if not _artifact_result_is_producer_receipt(result):
                continue
            extension = _artifact_result_media_extension(result)
            if extension is None:
                continue
            if normalize_tool_status(getattr(result, "status", None)) != "completed":
                continue
            if any(
                _path_matches_identities(path, collected_image_identities)
                for path in _artifact_result_embedded_source_image_paths(result)
            ):
                continue
            outstanding.add(extension)
    for result in results:
        for extension in _artifact_result_required_media_extensions(result):
            outstanding.add(extension)
        if not _artifact_result_is_producer_receipt(result):
            continue
        extension = _artifact_result_media_extension(result)
        if extension is None:
            continue
        media_count = _artifact_result_media_count(
            result,
            known_browser_screenshot_identities=browser_screenshot_identities,
            verified_browser_screenshot_identities=verified_browser_screenshot_identities,
        )
        if normalize_tool_status(getattr(result, "status", None)) == "completed" and media_count > 0:
            if collected_image_identities and not any(
                _path_matches_identities(path, collected_image_identities)
                for path in _artifact_result_embedded_source_image_paths(result)
            ):
                outstanding.add(extension)
                continue
            outstanding.discard(extension)
            continue
        if _artifact_failure_requires_media(result):
            outstanding.add(extension)
    return bool(outstanding)


def artifact_completed_embedded_media_paths(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    results = tuple(tool_results or ())
    browser_screenshot_identities = _browser_screenshot_path_identities(results)
    verified_browser_screenshot_identities = _browser_screenshot_path_identities(results, verified=True)
    paths: list[str] = []
    for result in results:
        if _artifact_result_media_extension(result) is None:
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        if _artifact_result_media_count(
            result,
            known_browser_screenshot_identities=browser_screenshot_identities,
            verified_browser_screenshot_identities=verified_browser_screenshot_identities,
        ) <= 0:
            continue
        paths.extend(_artifact_result_paths(result))
    return tuple(dict.fromkeys(paths))


def _artifact_result_paths_matching_extension(result: ToolResult, extension: str) -> tuple[str, ...]:
    normalized = str(extension or "").strip().lower()
    if not normalized:
        return ()
    return tuple(path for path in _artifact_result_paths(result) if Path(path).suffix.lower() == normalized)


def artifact_valid_plain_media_siblings(tool_results, selected_paths, *, required_extensions=()) -> frozenset[str]:
    """Allow separately selected plain outputs beside a verified media output.

    A typed final manifest can preserve multiple requested files of one format;
    it cannot satisfy the media obligation with an unselected older draft.
    """
    from nullion.artifact_lifecycle import delivery_manifest
    selected = tuple(dict.fromkeys(str(path) for path in selected_paths))
    selected_ids = _path_identity_set(selected)
    required_counts = Counter(required_extensions)
    manifest = delivery_manifest(tool_results)
    if manifest is not None:
        manifest_ids = _path_identity_set(manifest)
        for extension, count in Counter(Path(path).suffix.lower() for path in selected).items():
            if all(_path_matches_identities(path, manifest_ids) for path in selected):
                required_counts[extension] = max(required_counts[extension], count)
    verified_formats = {
        Path(path).suffix.lower()
        for path in artifact_completed_embedded_media_paths(tool_results)
        if _path_matches_identities(path, selected_ids) and Path(path).is_file()
    }
    return frozenset(path for path in selected
        if required_counts[Path(path).suffix.lower()] > 1 and Path(path).suffix.lower() in verified_formats)


def artifact_plain_replacement_artifact_paths(tool_results: Iterable[ToolResult] | None) -> tuple[str, ...]:
    results = tuple(tool_results or ())
    outstanding: set[str] = set()
    paths: list[str] = []
    collected_image_identities = _collected_browser_image_path_identities(results)
    browser_screenshot_identities = _browser_screenshot_path_identities(results)
    verified_browser_screenshot_identities = _browser_screenshot_path_identities(results, verified=True)
    if collected_image_identities:
        for result in results:
            if not _artifact_result_is_producer_receipt(result):
                continue
            extension = _artifact_result_media_extension(result)
            if extension is None:
                continue
            if normalize_tool_status(getattr(result, "status", None)) != "completed":
                continue
            if any(
                _path_matches_identities(path, collected_image_identities)
                for path in _artifact_result_embedded_source_image_paths(result)
            ):
                continue
            paths.extend(_artifact_result_paths_matching_extension(result, extension))
    for result in results:
        for extension in _artifact_result_required_media_extensions(result):
            outstanding.add(extension)
        if not _artifact_result_is_producer_receipt(result):
            continue
        extension = _artifact_result_media_extension(result)
        if extension is None:
            continue
        if normalize_tool_status(getattr(result, "status", None)) == "completed":
            if _artifact_result_media_count(
                result,
                known_browser_screenshot_identities=browser_screenshot_identities,
                verified_browser_screenshot_identities=verified_browser_screenshot_identities,
            ) > 0:
                outstanding.discard(extension)
            elif extension in outstanding:
                paths.extend(_artifact_result_paths_matching_extension(result, extension))
            continue
        if _artifact_failure_requires_media(result):
            outstanding.add(extension)
    # A repair can overwrite the same managed path. The old receipt remains
    # audit history; its missing media must not invalidate the verified newer
    # file. Only the latest completed producer for that exact path can clear it.
    latest_by_path = {}
    for result in results:
        if not _artifact_result_is_producer_receipt(result):
            continue
        extension = _artifact_result_media_extension(result)
        if extension is None or normalize_tool_status(result.status) != "completed":
            continue
        for path in _artifact_result_paths_matching_extension(result, extension):
            latest_by_path.update({variant: result for variant in _path_identity_variants(path)})
    repaired = set()
    for path in paths:
        latest = next((latest_by_path[variant] for variant in _path_identity_variants(path)
                       if variant in latest_by_path), None)
        if latest is None:
            continue
        if (_artifact_result_media_count(latest,
                known_browser_screenshot_identities=browser_screenshot_identities,
                verified_browser_screenshot_identities=verified_browser_screenshot_identities) > 0
                and (not collected_image_identities or any(
                    _path_matches_identities(source, collected_image_identities)
                    for source in _artifact_result_embedded_source_image_paths(latest)))):
            repaired.update(_path_identity_variants(path))
    paths = [path for path in paths if not _path_matches_identities(path, repaired)]
    from nullion.artifact_lifecycle import delivery_manifest
    manifest = delivery_manifest(results)
    allowed = artifact_valid_plain_media_siblings(results, manifest) if manifest is not None else frozenset()
    allowed_ids = _path_identity_set(allowed)
    return tuple(path for path in dict.fromkeys(paths) if not _path_matches_identities(path, allowed_ids))


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
    extension = _artifact_invocation_media_extension(invocation)
    if (invocation.tool_name == "spreadsheet_create"
            and Path(str(invocation.arguments.get("output_path") or "")).suffix.lower() in {".csv", ".tsv"}):
        # These native spreadsheet output variants are data exports. They
        # cannot embed media and do not replace the visual workbook sibling.
        return None
    if extension is None:
        return None
    outstanding_extensions = set(normalize_artifact_media_required_extensions(required_embedded_media_extensions))
    browser_screenshot_identities = _browser_screenshot_path_identities(prior_results)
    verified_browser_screenshot_identities = _browser_screenshot_path_identities(prior_results, verified=True)
    collected_browser_images_available = False
    prior_media_failure = False
    for result in prior_results:
        if _browser_image_collection_paths(result):
            collected_browser_images_available = True
            continue
        outstanding_extensions.update(_artifact_result_required_media_extensions(result))
        result_extension = _artifact_result_media_extension(result)
        if result_extension is None:
            continue
        output = result.output if isinstance(result.output, dict) else {}
        media_count = _artifact_result_media_count(
            result,
            known_browser_screenshot_identities=browser_screenshot_identities,
            verified_browser_screenshot_identities=verified_browser_screenshot_identities,
        )
        if normalize_tool_status(getattr(result, "status", None)) == "completed" and media_count > 0:
            outstanding_extensions.discard(result_extension)
            continue
        if _artifact_failure_requires_media(result):
            outstanding_extensions.add(result_extension)
            if result_extension == extension:
                prior_media_failure = True
    if collected_browser_images_available:
        outstanding_extensions.add(extension)
    if extension not in outstanding_extensions:
        return None
    if _artifact_invocation_has_media_inputs(invocation):
        return None
    if invocation.tool_name == "pdf_edit":
        # Edits inherit media from the input PDF. New image arguments or a
        # preliminary read are unnecessary; the permission-checked handler
        # inspects the saved PDF and final fulfillment checks its real media.
        return None
    target = invocation.arguments.get("output_path") or invocation.arguments.get("path")
    if isinstance(target, str) and Path(target).is_absolute():
        # A distinct plain sibling is only a candidate. The typed selector and
        # final per-format media check still require the verified illustrated
        # output; a plain draft cannot replace it. This permits a separate memo
        # beside a poster without treating both as revisions of one document.
        target_id = str(Path(target).expanduser().resolve())
        if any(Path(path).suffix.lower() == extension and Path(path).is_file()
               and str(Path(path).expanduser().resolve()) != target_id
               for path in artifact_completed_embedded_media_paths(prior_results)):
            return None
    label = _artifact_media_label(extension)
    first_attempt_required = extension in set(normalize_artifact_media_required_extensions(required_embedded_media_extensions))
    reason = (
        "artifact_media_required_by_prior_failure"
        if prior_media_failure
        else "artifact_media_required_by_prior_collection"
        if collected_browser_images_available
        else "artifact_media_required_by_turn_contract"
        if first_attempt_required
        else "artifact_media_required_by_prior_failure"
    )
    error_prefix = (
        f"A previous {label} artifact tool call in this turn attempted image/screenshot embedding and failed. "
        if prior_media_failure
        else f"Browser/page image artifacts were already collected for this turn's {label} artifact. "
        if collected_browser_images_available
        else f"This turn requires the {label} artifact to contain embedded image/screenshot media. "
        if first_attempt_required
        else f"A previous {label} artifact tool call in this turn attempted image/screenshot embedding and failed. "
    )
    error = (
        error_prefix
        + "Supply the actual supported artwork and preserve the requested layout and formats. "
        "PDF/HTML can use complete self-contained inline SVG drawings; native image-path adapters also "
        "support compatible local artwork. Do not regenerate a separate plain sibling or the whole package."
    )
    from nullion.artifact_evidence import html_svg_media_summary
    pages = invocation.arguments.get("html_pages") or ()
    pages = pages if isinstance(pages, (list, tuple)) else ()
    text_pages = invocation.arguments.get("text_pages") or ()
    text_pages = text_pages if isinstance(text_pages, (list, tuple)) else ()
    media_input_summary = {
        "html_page_count": len(pages),
        "text_page_count": len(text_pages),
        "html_media": [html_svg_media_summary(page) for page in pages[:20] if isinstance(page, str)],
    }
    return ToolResult(
        invocation.invocation_id,
        invocation.tool_name,
        "failed",
        {
            "reason": reason,
            "message": error,
            "media_input_summary": media_input_summary,
            "artifact_extensions": [extension],
            "embedded_media_artifact_extensions": [extension],
            "required_arguments": (["image_paths", "screenshot_paths", "html_pages"]
                                   if extension == ".pdf" else ["image_paths", "screenshot_paths"]),
        },
        error,
    )


def spreadsheet_media_embedding_was_required(tool_results: Iterable[ToolResult] | None) -> bool:
    return "spreadsheet_create" in {
        result.tool_name
        for result in tool_results or ()
        if _artifact_failure_requires_media(result)
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
    normalized_results = [
        result
        for result in tool_results or ()
        if normalize_tool_status(getattr(result, "status", None)) == "completed"
    ]
    scoped_required_extensions = _artifact_requested_extensions(normalized_results)
    consumed_file_write_paths = _consumed_file_write_artifact_paths(normalized_results)
    substantive_tool_names = [
        str(getattr(result, "tool_name", "") or "")
        for result in normalized_results
        if str(getattr(result, "tool_name", "") or "") != "request_tool_scope"
    ]
    standalone_browser_screenshot = bool(substantive_tool_names) and set(substantive_tool_names) == {"browser_screenshot"}
    deliverable_descriptor_paths: list[str] = []
    for result in normalized_results:
        output = result.output if isinstance(result.output, dict) else {}
        deliverable_descriptor_paths.extend(
            artifact_paths_from_output_descriptors(output, roles=ARTIFACT_DELIVERY_ROLES)
        )
    has_deliverable_descriptors = bool(deliverable_descriptor_paths)
    paths: list[str] = []
    for result in normalized_results:
        tool_name = str(getattr(result, "tool_name", "") or "")
        output = result.output if isinstance(result.output, dict) else {}
        if tool_name == "browser_screenshot":
            if scoped_required_extensions and ".png" not in scoped_required_extensions:
                continue
            if not scoped_required_extensions and not standalone_browser_screenshot:
                continue
        if tool_name == "terminal_exec":
            paths.extend(
                path
                for path in _terminal_exec_stdout_artifact_paths(output)
                if not scoped_required_extensions
                or artifact_format_extension(path) in scoped_required_extensions
            )
        if output_has_artifact_descriptors(output):
            paths.extend(artifact_paths_from_output_descriptors(output, roles=ARTIFACT_DELIVERY_ROLES))
            continue
        if has_deliverable_descriptors and tool_name not in (STRUCTURED_ARTIFACT_PRODUCER_TOOLS - {"file_write"}):
            if not (tool_name == "file_write" and scoped_required_extensions):
                continue
        if has_deliverable_descriptors and tool_name == "file_write" and not scoped_required_extensions:
            continue
        if tool_name in {"file_patch", "file_write"}:
            receipt = output.get("action_receipt")
            ordinary_file_action = bool(
                isinstance(receipt, dict)
                and receipt.get("type") == "action_receipt"
                and receipt.get("object_type") == "file"
                and not scoped_required_extensions
            )
            # A changed file's identity is an action receipt, not a request to
            # attach it. Explicit artifact descriptors/paths and scoped output
            # formats remain authoritative delivery evidence.
            path = None if ordinary_file_action else output.get("path")
            if isinstance(path, str) and path.strip():
                if tool_name == "file_write" and _path_matches_identity_set(path, consumed_file_write_paths):
                    continue
                if (
                    scoped_required_extensions
                    and artifact_format_extension(path) not in scoped_required_extensions
                ):
                    continue
                paths.append(path)
        elif tool_name in STRUCTURED_ARTIFACT_PRODUCER_TOOLS:
            path = output.get("path")
            if isinstance(path, str) and path.strip():
                suffix = Path(path).suffix.lower()
                if suffix in VALID_ATTACHMENT_EXTENSIONS:
                    paths.append(path)
        for key in ("artifact_path",):
            value = output.get(key)
            if isinstance(value, str) and value.strip():
                if (
                    tool_name in {"file_write", "terminal_exec"}
                    and tool_name == "file_write"
                    and _path_matches_identity_set(value, consumed_file_write_paths)
                ):
                    continue
                if (
                    tool_name in {"file_write", "terminal_exec"}
                    and scoped_required_extensions
                    and artifact_format_extension(value) not in scoped_required_extensions
                ):
                    continue
                paths.append(value)
        for key in ("artifact_paths", "artifacts"):
            values = output.get(key)
            if isinstance(values, (list, tuple)):
                paths.extend(
                    value
                    for value in values
                    if isinstance(value, str)
                    and value.strip()
                    and not (
                        tool_name == "file_write"
                        and _path_matches_identity_set(value, consumed_file_write_paths)
                    )
                    and not (
                        tool_name in {"file_write", "terminal_exec"}
                        and scoped_required_extensions
                        and artifact_format_extension(value) not in scoped_required_extensions
                    )
                )
    from nullion.artifact_lifecycle import filter_delivery_paths
    return filter_delivery_paths(list(dict.fromkeys(paths)), tool_results)


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


def _path_matches_identity_set(path: object, identities: set[str]) -> bool:
    return any(identity in identities for identity in _path_identity_variants(path))


def _iter_nested_string_values(value: object, *, limit: int = 200) -> tuple[str, ...]:
    strings: list[str] = []

    def visit(item: object) -> None:
        if len(strings) >= limit:
            return
        if isinstance(item, str):
            if item.strip():
                strings.append(item.strip())
            return
        if isinstance(item, Mapping):
            for nested in item.values():
                visit(nested)
                if len(strings) >= limit:
                    return
            return
        if isinstance(item, (list, tuple)):
            for nested in item:
                visit(nested)
                if len(strings) >= limit:
                    return

    visit(value)
    return tuple(strings)


def _path_reference_identity_variants(value: object) -> tuple[str, ...]:
    text = str(value or "").strip()
    if not text:
        return ()
    candidates: list[str] = []
    try:
        parsed = urlparse(text)
        if parsed.scheme == "file" and parsed.path:
            candidates.append(unquote(parsed.path))
    except Exception:
        pass
    candidates.append(text)
    variants: list[str] = []
    for candidate in candidates:
        variants.extend(_path_identity_variants(candidate))
    return tuple(dict.fromkeys(variants))


def _consumed_file_write_artifact_paths(tool_results: Iterable[ToolResult] | None) -> set[str]:
    results = list(tool_results or ())
    consumed: set[str] = set()
    for index, result in enumerate(results):
        if str(getattr(result, "tool_name", "") or "") != "file_write":
            continue
        if normalize_tool_status(getattr(result, "status", None)) != "completed":
            continue
        output = result.output if isinstance(result.output, Mapping) else {}
        path = output.get("path") or output.get("artifact_path")
        if not isinstance(path, str) or not path.strip():
            continue
        identities = set(_path_identity_variants(path))
        if not identities:
            continue
        for later in results[index + 1 :]:
            if normalize_tool_status(getattr(later, "status", None)) != "completed":
                continue
            tool_name = str(getattr(later, "tool_name", "") or "")
            if tool_name not in {"browser_navigate", "browser_open"}:
                continue
            later_output = later.output if isinstance(later.output, Mapping) else {}
            references = {
                identity
                for value in _iter_nested_string_values(later_output)
                for identity in _path_reference_identity_variants(value)
            }
            if identities & references:
                consumed.update(identities)
                break
    return consumed


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


def strip_unselected_artifact_references(
    text: str | None,
    *,
    selected_paths: Iterable[str],
    candidate_paths: Iterable[str],
) -> str | None:
    """Remove user-visible references to artifacts filtered out of delivery."""
    candidate_list = [str(path) for path in candidate_paths if str(path or "").strip()]
    if not text or not candidate_list:
        return text
    selected_names = {Path(path).name for path in selected_paths if str(path or "").strip()}
    suppressed_names = {
        Path(path).name
        for path in candidate_list
        if Path(path).name and Path(path).name not in selected_names
    }
    if not suppressed_names:
        return text
    suppressed_labels = {
        _artifact_delivery_label_for_path(path)
        for path in candidate_list
        if Path(path).name in suppressed_names
    }
    suppressed_suffixes = {
        Path(path).suffix.lower()
        for path in candidate_list
        if Path(path).name in suppressed_names and Path(path).suffix
    }
    lines: list[str] = []
    for raw_line in str(text).splitlines():
        line = _strip_suppressed_artifact_names_from_line(raw_line, suppressed_names)
        if line is None:
            continue
        lines.append(_strip_suppressed_artifact_labels_from_line(line, suppressed_labels))
    cleaned = "\n".join(lines).strip()
    if not cleaned:
        return None
    folded = cleaned.casefold()
    if any(name.casefold() in folded for name in suppressed_names):
        return None
    if any(label.casefold() in folded for label in suppressed_labels):
        return None
    if any(suffix and suffix.casefold() in folded for suffix in suppressed_suffixes):
        return None
    return cleaned


def _terminal_exec_stdout_artifact_paths(output: dict[str, object]) -> list[str]:
    stdout = output.get("stdout")
    if not isinstance(stdout, str) or not stdout.strip():
        return []
    if _terminal_exec_stdout_is_path_listing(output):
        return []
    paths: list[str] = []
    for match in _TERMINAL_STDOUT_ATTACHMENT_PATH_RE.finditer(stdout):
        raw_path = match.group("path").strip().rstrip(".,;:)]}")
        if not raw_path:
            continue
        suffix = Path(raw_path).suffix.lower()
        if suffix not in VALID_ATTACHMENT_EXTENSIONS:
            continue
        paths.append(raw_path)
    return paths


def _pathlike_lines_from_stdout(text: object) -> tuple[str, ...]:
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


def _terminal_exec_stdout_is_path_listing(output: dict[str, object]) -> bool:
    stdout = str(output.get("stdout") or "")
    stderr = str(output.get("stderr") or "").strip()
    if stderr:
        return False
    lines = [line.strip().strip("`'\"<>") for line in stdout.splitlines() if line.strip()]
    if not lines:
        return False
    return len(_pathlike_lines_from_stdout(stdout)) == len(lines)


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


def _required_extension_counts(required_attachment_extensions: Iterable[str] | None) -> dict[str, int]:
    counts: dict[str, int] = {}
    for extension in required_attachment_extensions or ():
        normalized = str(extension or "").strip().lower()
        if not normalized:
            continue
        if not normalized.startswith("."):
            normalized = f".{normalized}"
        normalized = artifact_format_extension("file" + normalized)
        if is_domain_suffix_extension(normalized):
            continue
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _paths_matching_required_extension(paths: Iterable[str], required_extension: str | None) -> list[str]:
    if required_extension is None:
        return list(paths)
    return [path for path in paths if artifact_format_extension(path) == artifact_format_extension("file" + required_extension)]


def _path_extension_counts(paths: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    seen_paths: set[str] = set()
    for raw_path in paths:
        if not str(raw_path or "").strip():
            continue
        path = str(Path(raw_path).expanduser())
        if path in seen_paths:
            continue
        seen_paths.add(path)
        suffix = artifact_format_extension(path)
        if not suffix:
            continue
        counts[suffix] = counts.get(suffix, 0) + 1
    return counts


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


def _artifact_package_delivery_reply(paths: Iterable[str]) -> str:
    deliverable_paths = [str(path) for path in paths if str(path or "").strip()]
    if not deliverable_paths:
        return "Done — attached the requested artifacts."
    if len(deliverable_paths) == 1:
        return _corrected_artifact_attachment_reply(deliverable_paths[0], Path(deliverable_paths[0]).suffix.lower())
    names = [Path(path).name for path in deliverable_paths]
    lines = "\n".join(f"- {name}" for name in names)
    return f"Done — attached the requested artifact package:\n\n{lines}"


def _reply_is_internal_missing_tool_scope_leak(reply: str) -> bool:
    text = _text_from_value(reply).casefold()
    if not text:
        return False
    return "request_tool_scope" in text or "callable namespace" in text


def _reply_is_artifact_attachment_failure_reply(reply: str) -> bool:
    text = _text_from_value(reply).strip()
    return text in {
        "I couldn't attach the requested file.",
        "I couldn't attach all of the requested files.",
        "I couldn't attach the requested file. The task is still open.",
        "I couldn't attach all of the requested files. The task is still open.",
    }


def _completed_scheduler_mutation_without_artifact_attempt(
    completed_tool_names: set[str],
    attempted_tool_names: set[str],
) -> bool:
    return bool(completed_tool_names.intersection(SCHEDULER_MUTATION_TOOLS)) and not bool(
        attempted_tool_names.intersection(STRUCTURED_ARTIFACT_PRODUCER_TOOLS)
    )


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
        return ("A required dependency is unavailable. Installation requires approval before retrying.")

    return None


def _failed_artifact_producer_setup_reply(tool_results: Iterable[ToolResult] | None, *, provider_rejections_only=False) -> str | None:
    for result in reversed(tuple(tool_results or ())):
        field = result.get if isinstance(result, Mapping) else lambda key, default=None: getattr(result, key, default)
        if normalize_tool_status(field("status")) not in {"failed", "denied"}:
            continue
        tool_name = str(field("tool_name", "") or "").strip()
        if tool_name not in STRUCTURED_ARTIFACT_PRODUCER_TOOLS:
            continue
        error = str(field("error", "") or "").strip()
        output = field("output") if isinstance(field("output"), Mapping) else {}
        reason = str(output.get("reason") or "").strip()
        status = output.get("http_status_code")
        if isinstance(status, int):
            explanations = {
                401: "The file provider rejected authentication (HTTP 401). Update its credentials in Settings, then retry.",
                402: "The file provider requires payment or credits (HTTP 402). Add credits or update its billing, then retry.",
                403: "The file provider denied this request (HTTP 403). Check its permissions and model access, then retry.",
                429: "The file provider is rate limited (HTTP 429). Wait before retrying.",
            }
            if status in explanations:
                return explanations[status]
            if status >= 500:
                return f"The file provider failed (HTTP {status}). Retry when the provider is available."
        if provider_rejections_only:
            continue
        if reason == "binary_file_read_boundary":
            continue
        if reason == "provider_not_configured":
            lines = [
                "The requested file could not be generated because its provider is not configured.",
                "Options:",
                "1. Configure the provider in Settings, then retry.",
                "2. Choose another configured generator.",
                "3. Choose another output format.",
            ]
            return "\n".join(lines)
        if reason == "missing_dependency":
            return "The requested file could not be generated because a required dependency is unavailable. Approve its installation before retrying."
        if reason == "handler_timeout":
            return "The file-generation step timed out. The requested file has not been confirmed."
        if reason == "request_plan_conflict":
            return "The proposed file-generation step was withheld because its plan needed correction. The requested file has not been confirmed."
        if error or reason:
            return "A file-generation step failed. The requested file has not been confirmed."
    return None


def artifact_delivery_failure_reply(tool_results, *, default="I couldn't attach the requested file.") -> str:
    """Shared failure content for fulfillment and platform delivery boundaries."""
    return _failed_artifact_producer_setup_reply(tool_results, provider_rejections_only=True) or default


def _reply_is_failed_artifact_producer_setup_reply(
    reply: str,
    tool_results: Iterable[ToolResult] | None,
) -> bool:
    text = _text_from_value(reply)
    if not text:
        return False
    normalized_text = text.rstrip(".")
    for result in tool_results or ():
        setup_reply = _failed_artifact_producer_setup_reply((result,))
        if setup_reply and normalized_text == setup_reply.rstrip("."):
            return True
        # Content-safety compatibility for old formatter output: the exact
        # private diagnostic is unsafe in any surrounding language. This
        # changes only copy after files have independently passed selection
        # and validation; it grants no delivery or completion authority.
        diagnostic = str(getattr(result, 'error', '') or '').strip().rstrip('.')
        if (result.tool_name in STRUCTURED_ARTIFACT_PRODUCER_TOOLS
                and len(diagnostic) >= 40 and diagnostic in normalized_text):
            return True
    return False


def operation_failure_reply(tool_results: Iterable[ToolResult], *, required_tool_names=None) -> str:
    """Explain typed failures without leaking provider payloads or promising a retry."""
    from nullion.browser_capture_contract import repaired_browser_results

    tool_results = repaired_browser_results(tool_results)
    failures = [result for result in tool_results if normalize_tool_status(result.status) in {"failed", "denied"}]
    if required_tool_names is not None:
        failures = [r for r in failures if r.tool_name in set(required_tool_names)]
    elif any(normalize_tool_status(r.status) == "completed" and isinstance(r.output, dict)
             and (r.output.get('text') or r.output.get('source_record_collections')
                  or r.output.get('action_receipt')) for r in tool_results):
        # Useful evidence is not proof of universal recovery. Without a
        # requirement/failure binding it is also not proof that an earlier
        # failed attempt caused the current gap.
        failures = []
    if failures and all(isinstance(r.output, dict) and r.output.get('executed') is False
                        and r.output.get('reason') == 'request_plan_conflict' for r in failures):
        return "A proposed step was withheld because its plan needed correction. The requested result remains unconfirmed."
    browser_failures = [result for result in failures if result.tool_name.startswith("browser_")]
    if browser_failures:
        reasons = {result.output.get("reason") for result in browser_failures
                   if isinstance(result.output, dict) and isinstance(result.output.get("reason"), str)}
        if "handler_timeout" in reasons:
            return "I couldn't finish because a browser step timed out. The requested result wasn't confirmed."
        if "browser_action_failed" in reasons:
            return "I couldn't finish because the page interaction failed. The requested result wasn't confirmed."
        return "I couldn't finish because the browser operation failed. The requested result wasn't confirmed."
    if any(normalize_tool_status(result.status) == "denied" for result in failures):
        return "I couldn't complete the request because the required action wasn't authorized."
    if failures:
        return "I couldn't complete the request because a required step failed."
    return "I couldn't complete the request because a required step didn't run."


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
    normalized_results = [
        (result, normalize_tool_status(getattr(result, "status", None)))
        for result in (tool_results or ())
    ]
    from nullion.request_delivery_requirements import reviewed_request_delivery_requirements
    reviewed_requirements = reviewed_request_delivery_requirements(
        [result for result, _status in normalized_results], user_message)
    from nullion.execution_review_evidence import reviewed_execution_evidence
    execution_review = reviewed_execution_evidence([r for r, _ in normalized_results], user_message, reply)
    frame = None if reviewed_requirements is not None else _active_frame_for_contract(store, conversation_id)
    if reviewed_requirements is not None:
        required_attachment_extensions = reviewed_requirements['required_attachment_extensions']
    completed_tool_names = {result.tool_name for result, status in normalized_results if status == "completed"}
    attempted_tool_names = {result.tool_name for result, _status in normalized_results}

    current_scopes = [result.output for result, status in normalized_results
        if result.tool_name == "request_tool_scope" and status == "completed" and isinstance(result.output, Mapping)]
    from nullion.source_selection import current_read_only_local_scope
    read_only_lookup = current_read_only_local_scope(result for result, _status in normalized_results)
    superseded_tools = set(current_scopes[-1].get("superseded_unselected_required_tool_names", ())) if read_only_lookup else set()
    # These names come only from the runtime's blocked-registry scope result,
    # after checking ToolSpec.READ. Retiring an implementation never supplies
    # evidence for the source goal, which the independent outcome gate checks.
    failed_tools = {result.tool_name for result, status in normalized_results if status in {"failed", "denied", "error"}}
    for scope in current_scopes:
        superseded_tools.update(name for name in scope.get("retired_preparatory_read_tools", ()) if name in failed_tools)
    required_tool_names = tuple(name for name in (required_tool_names or ()) if name not in superseded_tools)
    if reviewed_requirements is not None and 'eligible_evidence_tool_names' in reviewed_requirements:
        required_tool_names = tuple(name for name in required_tool_names
            if name in reviewed_requirements['eligible_evidence_tool_names'])
    excluded_extensions = set(_normalize_valid_attachment_extensions(
        current_scopes[-1].get("excluded_artifact_extensions", ()))) if current_scopes else set()
    from nullion.artifact_lifecycle import restored_delivery_paths
    excluded_extensions.difference_update(artifact_format_extension(path) for path in restored_delivery_paths(
        result for result, _status in normalized_results))
    explicit_required_extension_counts = {} if read_only_lookup else {extension: count for extension, count in
        _required_extension_counts(required_attachment_extensions).items() if extension not in excluded_extensions}
    explicit_required_extensions = tuple(explicit_required_extension_counts)
    required_tools: set[str] = set()
    required_tools.update(
        str(tool_name or "").strip()
        for tool_name in (required_tool_names or ())
        if str(tool_name or "").strip()
    )
    requires_artifact = False
    artifact_kind = explicit_required_extensions[0].removeprefix(".") if explicit_required_extensions else "file"
    if frame is not None:
        required_tools.update(name for name in frame.finish.required_tool_completion if name not in superseded_tools)
        requires_artifact = bool(frame.finish.requires_artifact_delivery) and not read_only_lookup
        if not read_only_lookup:
            artifact_kind = frame.finish.required_artifact_kind or frame.output.artifact_kind or artifact_kind
        if frame.status is TaskFrameStatus.WAITING_INPUT and not normalized_results and not artifact_paths:
            # Waiting-input frames represent an explicit clarification turn.
            # Do not convert that question into an artifact-delivery failure.
            return ResponseFulfillmentDecision(True, reply)
    required_extension = _required_extension_for_artifact_kind(artifact_kind)
    if required_extension in excluded_extensions:
        # A current validated scope can replace the output contract inherited
        # from an older task frame. A filename-only follow-up must not recreate
        # or reattach an explicitly excluded prior report.
        required_extension = None
        requires_artifact = False
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
    media_required_extensions = tuple(extension for extension in media_required_extensions if extension not in excluded_extensions)
    if reviewed_requirements is not None:
        media_required_extensions = tuple(extension for extension in media_required_extensions
            if extension in reviewed_requirements['required_attachment_extensions'])
    if explicit_required_extensions:
        media_required_extensions = tuple(
            extension
            for extension in media_required_extensions
            if extension in explicit_required_extensions
        )
    media_required = bool(media_required_extensions)
    if media_required:
        requires_artifact = True
        if required_extension is None:
            required_extension = media_required_extensions[0]
            artifact_kind = _artifact_media_label(required_extension).lower()
    if _completed_scheduler_mutation_without_artifact_attempt(completed_tool_names, attempted_tool_names):
        explicit_required_extension_counts = {}
        explicit_required_extensions = ()
        media_required_extensions = ()
        media_required = False
        requires_artifact = False
        required_extension = None

    reply_valid_media_paths = _reply_valid_media_paths(reply, artifact_roots=artifact_roots)
    reply_valid_media_identities = _path_identity_set(reply_valid_media_paths)
    excluded_paths = {
        str(Path(path).expanduser())
        for path in (excluded_artifact_paths or ())
        if str(path or "").strip()
        and not _path_matches_identities(path, reply_valid_media_identities)
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
        # An extension-level media obligation requires a verified media output;
        # it does not turn a separately requested linked/plain sibling into a
        # failed replacement. Only current, explicitly selected files qualify.
        allowed_siblings = artifact_valid_plain_media_siblings(
            [result for result, _status in normalized_results],
            _existing_deliverable_paths(artifact_paths or (), artifact_roots=artifact_roots),
            required_extensions=tuple(extension for extension, count in explicit_required_extension_counts.items()
                for _ in range(count)))
        allowed_ids = _path_identity_set(allowed_siblings)
        invalid_plain_media_artifact_paths = {path for path in invalid_plain_media_artifact_paths
            if not _path_matches_identities(path, allowed_ids)}
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
    # A scope that did not request source delivery cannot acquire a new
    # attachment obligation solely from a model-written MEDIA pointer. Real
    # producer receipts and explicit frame/format requirements still apply.
    source_delivery_not_requested = bool(
        (reviewed_requirements is not None and not reviewed_requirements['required_attachment_extensions'])
        or (current_scopes and current_scopes[-1].get("source_artifact_delivery_requested") is False))
    delivery_contract = delivery_contract_for_turn(
        None,
        reply=("" if source_delivery_not_requested and not requires_artifact and not combined_artifact_paths else reply),
        artifact_paths=combined_artifact_paths,
        requires_attachment_delivery=requires_artifact,
        required_attachment_extensions=(
            *explicit_required_extensions,
            *((required_extension,) if required_extension else ()),
        ),
    )
    if not requires_artifact and delivery_contract.requires_attachment_delivery:
        requires_artifact = True

    existing_deliverables = _existing_deliverable_paths(combined_artifact_paths, artifact_roots=artifact_roots)
    valid_artifacts = _paths_matching_required_extension(existing_deliverables, required_extension)
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
        (
            path
            for path in reply_valid_media_paths
            if str(Path(path).expanduser()) not in excluded_paths
        ),
        required_extension,
    )
    completed_tool_names_for_requirements = set(completed_tool_names)
    # A failed browser read is a terminal observation the model can explain.
    # Requiring that read to succeed replaces its useful error with a generic
    # unfinished-task message and forces more reads of the same error page.
    browser_read_tools = {
        "browser_navigate", "browser_open", "browser_extract_text",
        "browser_extract_items", "browser_extract_detail", "browser_snapshot",
        "browser_find", "browser_assert_page_state", "web_search", "web_fetch",
    }
    required_tools.difference_update(
        result.tool_name for result, status in normalized_results
        if status == "failed" and result.tool_name in browser_read_tools
    )
    if "file_write" in required_tools and "file_patch" in completed_tool_names and valid_artifacts:
        completed_tool_names_for_requirements.add("file_write")
    missing: list[str] = []
    missing_tools = sorted(tool for tool in required_tools if tool not in completed_tool_names_for_requirements)
    if missing_tools:
        missing.extend(f"tool:{tool}" for tool in missing_tools)
    delivered_extension_counts = _path_extension_counts((*existing_deliverables, *valid_reply_media))
    for extension, required_count in explicit_required_extension_counts.items():
        if delivered_extension_counts.get(extension, 0) < required_count:
            missing.append(f"attachment:{extension}")

    has_deliverable = (
        (platform_artifact_count > 0 and required_extension is None)
        or bool(valid_artifacts)
        or bool(valid_reply_media)
    )
    if media_required:
        embedded_media_paths = artifact_completed_embedded_media_paths(result for result, _status in normalized_results)
        existing_embedded_media_artifacts = _existing_deliverable_paths(
            embedded_media_paths,
            artifact_roots=artifact_roots,
        )
        collected_image_identities = _collected_browser_image_path_identities(
            result for result, _status in normalized_results
        )
        for media_extension in media_required_extensions:
            if collected_image_identities:
                valid_embedded_media_artifacts = _existing_deliverable_paths(
                    _completed_media_artifact_paths_matching_collected_image(
                        (result for result, _status in normalized_results),
                        extension=media_extension,
                        collected_image_identities=collected_image_identities,
                    ),
                    artifact_roots=artifact_roots,
                )
            else:
                valid_embedded_media_artifacts = _paths_matching_required_extension(
                    existing_embedded_media_artifacts,
                    media_extension,
                )
            if not valid_embedded_media_artifacts:
                missing.append(f"attachment_with_embedded_media:{media_extension}")
    if requires_artifact and not has_deliverable:
        requirement_kind = artifact_kind
        if requirement_kind and requirement_kind != "artifact" and not requirement_kind.startswith("."):
            requirement_kind = f".{requirement_kind}"
        requirement = f"attachment:{requirement_kind}"
        if requirement not in missing:
            missing.append(requirement)
    if missing:
        logging.getLogger(__name__).warning(
            "response fulfillment incomplete conversation_id=%s missing=%s completed=%s",
            conversation_id, tuple(missing), tuple(sorted(completed_tool_names)))
        review = execution_review
        partial_reply = (review.partial_reply if review else None) or (
            reviewed_requirements.get('partial_reply') if reviewed_requirements else None)
        if partial_reply:
            return ResponseFulfillmentDecision(False, partial_reply, tuple(missing))
        # Producer failures cannot explain an unrelated missing action. Use
        # only failure receipts relevant to the unmet output/tool contract.
        failure_results = [result for result, status in normalized_results if status != "completed"
            and (any(item.startswith(('attachment:', 'attachment_with_embedded_media:')) for item in missing)
                 or result.tool_name in missing_tools)]
        setup_reply = _failed_artifact_producer_setup_reply(failure_results)
        if setup_reply and not existing_deliverables:
            return ResponseFulfillmentDecision(False, setup_reply, tuple(missing))
        missing_dependency_reply = _missing_dependency_reply(failure_results)
        if missing_dependency_reply and not existing_deliverables:
            return ResponseFulfillmentDecision(False, missing_dependency_reply, tuple(missing))
        missing_attachments = [
            item
            for item in missing
            if item.startswith(("attachment:", "attachment_with_embedded_media:"))
        ]
        attachment_missing = bool(missing_attachments)
        if attachment_missing:
            if (reviewed_requirements and isinstance(reviewed_requirements.get('partial_reply'), str)
                    and len(missing_attachments) == len(missing)):
                return ResponseFulfillmentDecision(False, reviewed_requirements['partial_reply'], tuple(missing))
            response = (
                "I couldn't attach all of the requested files."
                if existing_deliverables and (len(missing_attachments) > 1 or len(explicit_required_extensions) > 1)
                else "I couldn't attach the requested file."
            )
        elif attempted_tool_names:
            response = operation_failure_reply((result for result, _status in normalized_results),
                required_tool_names=missing_tools)
        else:
            response = "I need more output before this task can be completed."
        if attachment_missing:
            response = artifact_delivery_failure_reply(failure_results, default=response)
        return ResponseFulfillmentDecision(False, response, tuple(missing))

    if execution_review is not None and not execution_review.complete:
        return ResponseFulfillmentDecision(False, execution_review.partial_reply or reply,
            ("outcome:unconfirmed",))

    if valid_artifacts and _reply_mentions_only_wrong_extension(
        reply,
        artifact_roots=artifact_roots,
        required_extension=required_extension,
    ):
        return ResponseFulfillmentDecision(
            True,
            _corrected_artifact_attachment_reply(valid_artifacts[0], required_extension),
        )
    if requires_artifact and existing_deliverables and _reply_is_failed_artifact_producer_setup_reply(
        reply,
        (result for result, status in normalized_results if status != "completed"),
    ):
        requested_deliverables = [
            path
            for path in existing_deliverables
            if not explicit_required_extensions or Path(path).suffix.lower() in explicit_required_extensions
        ]
        return ResponseFulfillmentDecision(
            True,
            _artifact_package_delivery_reply(requested_deliverables or existing_deliverables),
        )
    if existing_deliverables and _reply_is_artifact_attachment_failure_reply(reply):
        requested_deliverables = [
            path
            for path in existing_deliverables
            if not explicit_required_extensions or Path(path).suffix.lower() in explicit_required_extensions
        ]
        return ResponseFulfillmentDecision(
            True,
            _artifact_package_delivery_reply(requested_deliverables or existing_deliverables),
        )
    if requires_artifact and platform_artifact_count > 0 and _reply_is_internal_missing_tool_scope_leak(reply):
        return ResponseFulfillmentDecision(
            True,
            _artifact_package_delivery_reply(artifact_paths or valid_artifacts),
        )

    evidence_reply = (
        _completed_tool_evidence_reply(result for result, _status in normalized_results)
        if _needs_tool_evidence_fallback(reply)
        else None
    )
    if evidence_reply:
        return ResponseFulfillmentDecision(True, evidence_reply)

    return ResponseFulfillmentDecision(True, reply)


def _display_missing_requirement(requirement: object) -> str:
    text = str(requirement or "").strip()
    if text.startswith("attachment_with_embedded_media:"):
        extension = text.removeprefix("attachment_with_embedded_media:").lstrip(".")
        return f"{extension} attachment with embedded media" if extension else "attachment with embedded media"
    if text.startswith("attachment:"):
        extension = text.removeprefix("attachment:").lstrip(".")
        return f"{extension} attachment" if extension else "attachment"
    if text.startswith("tool:"):
        tool = text.removeprefix("tool:")
        return f"{tool} tool" if tool else "tool"
    return text


def _display_missing_requirements(requirements: Iterable[object] | None) -> tuple[str, ...]:
    return tuple(
        label
        for requirement in (requirements or ())
        for label in (_display_missing_requirement(requirement),)
        if label
    )


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
    reply_valid_media_paths = _reply_valid_media_paths(decision.reply, artifact_roots=artifact_roots)
    reply_valid_media_identities = _path_identity_set(reply_valid_media_paths)
    excluded_paths = {
        str(Path(path).expanduser())
        for path in (excluded_artifact_paths or ())
        if str(path or "").strip()
        and not _path_matches_identities(path, reply_valid_media_identities)
    }
    combined_artifacts = [
        *(artifact_paths or ()),
        *artifact_paths_from_tool_results(tool_results),
        *reply_valid_media_paths,
    ]
    if excluded_paths:
        combined_artifacts = [
            path
            for path in combined_artifacts
            if str(path or "").strip() and str(Path(path).expanduser()) not in excluded_paths
        ]
    delivered_artifacts = combined_artifacts if platform_artifact_count or delivery_status in {"sent", "saved"} else ()
    from nullion.execution_review_evidence import reviewed_execution_evidence
    review = reviewed_execution_evidence(tool_results, user_message, reply)
    outcome = build_execution_outcome(
        requested_outcome=user_message,
        user_visible_message=decision.reply,
        tool_results=tool_results,
        artifacts_created=combined_artifacts,
        artifacts_delivered=delivered_artifacts,
        delivery_status=delivery_status,
        fulfillment_satisfied=decision.satisfied,
        missing_requirements=_display_missing_requirements(decision.missing_requirements),
        reviewed_execution=review,
    )
    reply = decision.reply
    required_delivered_artifacts: list[str] = []
    if decision.satisfied and required_attachment_extensions:
        required_extensions = _normalize_required_extensions(required_attachment_extensions)
        existing_artifacts = _existing_deliverable_paths(combined_artifacts, artifact_roots=artifact_roots)
        required_delivered_artifacts = [
            path for path in existing_artifacts if Path(path).suffix.lower() in required_extensions
        ]
    if (
        required_delivered_artifacts
        and outcome.status is ExecutionStatus.PARTIALLY_SUCCEEDED
        and outcome.failed_tools
    ):
        reply = _artifact_package_delivery_reply(required_delivered_artifacts)
    if outcome.status is ExecutionStatus.NEEDS_USER_ACTION and outcome.failure_reason:
        notice = _needs_user_action_reply(outcome.failure_reason, outcome.next_action)
        # A request/evidence-bound partial answer already contains the owner's
        # findings and actionable blocker. Rewriting it here invalidates that
        # binding; a second adapter would then replace it with only a notice.
        reply = review.partial_reply if review and review.partial_reply else notice
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
    "strip_unselected_artifact_references",
    "user_visible_text_from_output",
]

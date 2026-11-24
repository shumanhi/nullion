"""Platform-neutral artifact helpers for chat adapters."""

from __future__ import annotations

import codecs
from dataclasses import dataclass
from email.message import Message
import hashlib
import html
from html.parser import HTMLParser
import mimetypes
import os
from pathlib import Path
import re
import tempfile
from typing import Iterable
from urllib.parse import quote
from uuid import uuid4

_MIN_HTML_ARTIFACT_BYTES = 64


def _selected_delivery_receipt(result, requirement_contract):
    def field(value, key, default=None):
        return value.get(key, default) if isinstance(value, dict) else getattr(value, key, default)
    return any(field(row, 'requirement_kind')=='deliverable' and field(row, 'delivery_required', True)
        and field(result, 'tool_name') in field(row, 'eligible_evidence_tool_names', ())
        and (not field(row, 'eligible_evidence_invocation_ids', ())
             or field(result, 'invocation_id') in field(row, 'eligible_evidence_invocation_ids'))
        for row in requirement_contract)


def demote_unrequested_source_artifacts(tool_results, *, source_only_tool_names,
                                       requirement_contract, required_extensions) -> list[str]:
    """A scope hint cannot make an evidence-only tool's files user outputs."""
    def field(value, key, default=None):
        return value.get(key, default) if isinstance(value, dict) else getattr(value, key, default)
    formats = set(normalize_artifact_extensions(required_extensions))
    demoted = []
    for result in tool_results or ():
        name = field(result, "tool_name")
        output = field(result, "output")
        if name not in source_only_tool_names or not isinstance(output, dict):
            continue
        for descriptor in output.get("artifact_descriptors") or ():
            if not isinstance(descriptor, dict) or descriptor.get("role") != "deliverable":
                continue
            path = descriptor.get("path")
            if isinstance(path, str) and (not _selected_delivery_receipt(result, requirement_contract)
                    or artifact_format_extension(path) not in formats):
                descriptor["role"] = ARTIFACT_ROLE_SOURCE
                demoted.append(path)
    return list(dict.fromkeys(demoted))


def requested_source_artifact_candidates(tool_results, *, artifact_roots, requirement_contract=(),
                                        required_extensions=(), promote=False) -> list[str]:
    """Bind current source files to independently requested deliverable outcomes.

    Merely taking a screenshot or reading a file does not request its delivery.
    A validated material contract must name its producer as eligible evidence
    and request its format. Final-version selection remains the lifecycle's job.
    """
    from nullion.tools import normalize_tool_status

    def field(value, key, default=None):
        return value.get(key, default) if isinstance(value, dict) else getattr(value, key, default)

    eligible = {name for row in requirement_contract
                if field(row, "requirement_kind") == "deliverable" and field(row, "delivery_required", True)
                for name in field(row, "eligible_evidence_tool_names", ())}
    formats = set(normalize_artifact_extensions(required_extensions))
    if promote and (not eligible or not formats):
        return []
    paths = []
    for result in tool_results or ():
        if normalize_tool_status(field(result, "status")) != "completed":
            continue
        if promote and not _selected_delivery_receipt(result, requirement_contract):
            continue
        output = field(result, "output")
        if not isinstance(output, dict):
            continue
        descriptors = output.get("artifact_descriptors")
        if not isinstance(descriptors, list):
            continue
        for index, source in enumerate(descriptors):
            if not isinstance(source, dict) or source.get("role") != ARTIFACT_ROLE_SOURCE:
                continue
            raw_path = source.get("path")
            if not isinstance(raw_path, str) or not Path(raw_path).is_absolute():
                continue
            try:
                path = Path(raw_path).resolve()
                if promote and artifact_format_extension(path) not in formats:
                    continue
                descriptor = next((value for root in artifact_roots
                    if (value := artifact_descriptor_for_path(path, artifact_root=Path(root))) is not None), None)
                if descriptor is None or source.get("size_bytes") != descriptor.size_bytes:
                    continue
                if source.get("artifact_id") and source["artifact_id"] != descriptor.artifact_id:
                    continue
            except (OSError, RuntimeError, ValueError):
                continue
            paths.append(str(path))
            if promote:
                descriptors[index] = {**source, **descriptor.to_dict(), "role": "deliverable",
                    "origin": "requested_current_source_artifact"}
    return list(dict.fromkeys(paths))


def history_artifact_reference_paths(tool_results) -> list[str]:
    """Return available artifact identities from this turn's typed history evidence."""
    paths = []
    for result in tool_results or ():
        field = result.get if isinstance(result, dict) else lambda key, default=None: getattr(result, key, default)
        if field("tool_name") != "chat_history_search" or field("status") != "completed":
            continue
        output = field("output")
        if not isinstance(output, dict):
            continue
        for key in ("matches", "structured_matches"):
            for record in output.get(key, []) if isinstance(output.get(key), list) else ():
                if not isinstance(record, dict):
                    continue
                for ref in record.get("artifact_refs", []):
                    if isinstance(ref, dict) and ref.get("availability") == "available" and isinstance(ref.get("path"), str):
                        paths.append(ref["path"])
    return list(dict.fromkeys(paths))


def register_selected_existing_source_artifacts(*, reply: str, user_message: str, tool_results, artifact_roots, source_scope_decision=None) -> list[str]:
    """Verify and expose inspected workspace files selected by a source-delivery plan."""
    from nullion.tools import normalize_tool_status
    def field(result, name, default=None):
        return result.get(name, default) if isinstance(result, dict) else getattr(result, name, default)
    def output(result):
        value = field(result, "output")
        return value if isinstance(value, dict) else {}
    results = list(tool_results or ())
    scopes = [output(item) for item in results if field(item, "tool_name") == "request_tool_scope"
              and normalize_tool_status(field(item, "status")) == "completed"]
    if source_scope_decision is not None and getattr(source_scope_decision, 'valid', True):
        scopes.append({"source_artifact_delivery_requested": getattr(source_scope_decision, "source_artifact_delivery_requested", False),
                       "source_artifact_request_evidence": getattr(source_scope_decision, "source_artifact_request_evidence", ""),
                       "required_tool_names": getattr(source_scope_decision, "required_tool_names", ())})
    user_text = " ".join(str(user_message or "").split())
    current_scopes = scopes[-1:]
    if current_scopes and current_scopes[-1].get('source_artifact_delivery_requested') is False:
        return []
    declared_delivery = any(scope.get("source_artifact_delivery_requested") is True
               and (quote := " ".join(str(scope.get("source_artifact_request_evidence") or "").split()))
               and quote in user_text for scope in current_scopes)
    history_paths = history_artifact_reference_paths(results)
    if not declared_delivery and not history_paths:
        return []
    producers = {"file_write", "file_patch", "pdf_create", "pdf_edit", "document_create", "presentation_create",
                 "spreadsheet_create", "archive_create", "image_generate", "terminal_exec"}
    if any(producers.intersection(scope.get("required_tool_names", scope.get("requested_required_tool_names", ())) or ()) for scope in scopes):
        return []
    selected = tuple(dict.fromkeys(media_candidate_paths_from_text(reply)))
    reads = [item for item in results if field(item, "tool_name") == "file_read"
             and normalize_tool_status(field(item, "status")) == "completed"]
    histories = [item for item in results if field(item, "tool_name") == "chat_history_search"
                 and normalize_tool_status(field(item, "status")) == "completed"]
    searches = [item for item in results if field(item, "tool_name") == "file_search"
                and normalize_tool_status(field(item, "status")) == "completed"]
    if declared_delivery:
        # An exact filename in the authorized request plus one native lookup
        # identity selects an existing file. It needs no rewrite, content read,
        # or model-generated MEDIA directive. Ambiguous identities stay pending.
        requested = list(dict.fromkeys(path for item in searches
            for path in output(item).get('matches', ()) if isinstance(path, str)
            and text_references_file_path(user_message, path)))
        if len(requested) == 1:
            selected = (Path(requested[0]),)
    if not selected or not (reads or histories or searches):
        return []
    roots = tuple(artifact_roots)
    verified = []
    for path in selected:
        try:
            resolved = path.expanduser().resolve()
            matching = next((item for item in reads if declared_delivery and output(item).get("path")
                             and Path(str(output(item)["path"])).expanduser().resolve() == resolved), None)
            if matching is None:
                matching = next((item for item in histories if any(
                    Path(value).expanduser().resolve() == resolved for value in history_artifact_reference_paths([item]))), None)
            if matching is None and declared_delivery:
                matching = next((item for item in searches if any(
                    isinstance(value, str) and Path(value).expanduser().resolve() == resolved
                    for value in output(item).get('matches', ()))), None)
            descriptor = next((value for root in roots if (value := artifact_descriptor_for_path(resolved, artifact_root=root)) is not None), None)
        except (OSError, RuntimeError, ValueError):
            return []
        if matching is None or descriptor is None:
            return []
        verified.append((matching, descriptor))
    for item, descriptor in verified:
        payload = {**descriptor.to_dict(), "role": "deliverable", "origin": "existing_workspace_artifact"}
        # Reading an existing file may also emit a source descriptor. Its old
        # role must not suppress the same explicitly selected, verified file.
        for result in results:
            data = output(result)
            descriptors = data.get("artifact_descriptors")
            if not isinstance(descriptors, list):
                continue
            reconciled = []
            for value in descriptors:
                try:
                    same_path = isinstance(value, dict) and value.get("path") and Path(str(value["path"])).expanduser().resolve() == Path(descriptor.path)
                except (OSError, RuntimeError, ValueError):
                    same_path = False
                reconciled.append(payload if same_path else value)
            if reconciled != descriptors:
                if isinstance(result, dict):
                    result["output"] = {**data, "artifact_descriptors": reconciled}
                else:
                    result.output = {**data, "artifact_descriptors": reconciled}
        data = output(item)
        descriptors = list(data.get("artifact_descriptors") or [])
        if not any(isinstance(value, dict) and value.get("path") == descriptor.path and value.get("role") == "deliverable" for value in descriptors):
            descriptors.append(payload)
        enriched = {**data, "artifact_descriptors": descriptors}
        if isinstance(item, dict):
            item["output"] = enriched
        else:
            item.output = enriched
    return [descriptor.path for _, descriptor in verified]


def text_references_file_path(text: str, path: str | Path) -> bool:
    """Match a concrete file identity without accepting filename prefixes."""
    text = re.sub(r"(?ms)^[ \t]*(`{3,}|~{3,})[^\n]*\n.*?^[ \t]*\1[ \t]*$", "", text)
    candidate = Path(path)
    return any(
        re.search(r"(?<![\w./\\-])" + re.escape(value) + r"(?![\w/\\-]|\.[\w])", text, re.IGNORECASE)
        for value in (str(candidate), candidate.name) if value
    )

_MAX_SUPPORTING_ASSET_HTML_CANDIDATES = 20
_MAX_SUPPORTING_ASSET_HTML_BYTES = 1_500_000
_HTML_ARTIFACT_SUFFIXES = frozenset({".html", ".htm"})
_IMAGE_ARTIFACT_SUFFIXES = frozenset({".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"})
_BLOCKED_DOWNLOAD_SUFFIXES = frozenset(
    {
        ".bash",
        ".bat",
        ".cmd",
        ".command",
        ".js",
        ".mjs",
        ".ps1",
        ".py",
        ".rb",
        ".scpt",
        ".sh",
        ".zsh",
    }
)
_MEDIA_DIRECTIVE_PREFIX = "MEDIA:"
_ARTIFACT_DIRECTIVE_PREFIX = "ARTIFACT:"
_ATTACHMENT_DIRECTIVE_PREFIXES = (_MEDIA_DIRECTIVE_PREFIX, _ARTIFACT_DIRECTIVE_PREFIX)
_ATTACHMENT_DIRECTIVE_WORDS = ("MEDIA", "ARTIFACT")
_MEDIA_DIRECTIVE_STRIP_CHARS = "\ufeff\u200b\u200c\u200d"
ARTIFACT_ROLE_DELIVERABLE = "deliverable"
ARTIFACT_ROLE_SOURCE = "source"
ARTIFACT_ROLE_INTERMEDIATE = "intermediate"
ARTIFACT_DELIVERY_ROLES = frozenset({"deliverable", "deliver_receipt", "verify"})
_ARTIFACT_MEDIA_TYPES_BY_SUFFIX = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}
_INTERNAL_SIDECAR_ARTIFACT_SUFFIXES = frozenset({".json", ".log", ".md", ".txt"})
_INTERNAL_SIDECAR_ARTIFACT_TOKENS = frozenset(
    {
        "debug",
        "diagnostic",
        "diagnostics",
        "manifest",
        "metadata",
        "raw",
        "sidecar",
        "status",
        "trace",
    }
)
_FENCED_HTML_BLOCK_RE = re.compile(
    r"```(?P<language>html|htm)\s*\n(?P<body>[\s\S]*?)```",
    flags=re.IGNORECASE,
)
_HTML_DOCUMENT_RE = re.compile(r"<\s*(?:!doctype\s+html|html|body|head)\b", flags=re.IGNORECASE)
_HTML_FRAGMENT_RE = re.compile(r"<\s*(?:div|table|section|article|main|style|p|h[1-6]|img|a)\b", flags=re.IGNORECASE)
_MIN_INLINE_HTML_ARTIFACT_CHARS = 500


def nullion_data_home() -> Path:
    data_dir = os.environ.get("NULLION_DATA_DIR")
    if isinstance(data_dir, str) and data_dir.strip():
        return Path(data_dir).expanduser().resolve()
    nullion_home = os.environ.get("NULLION_HOME")
    if isinstance(nullion_home, str) and nullion_home.strip():
        return Path(nullion_home).expanduser().resolve()
    return (Path.home() / ".nullion").resolve()


@dataclass(frozen=True, slots=True)
class ArtifactDescriptor:
    artifact_id: str
    name: str
    path: str
    media_type: str
    size_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.artifact_id,
            "name": self.name,
            "path": self.path,
            "media_type": self.media_type,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class MediaDirective:
    path: Path
    prefix: str = ""


def artifact_root_for_runtime(runtime) -> Path:
    checkpoint_path = getattr(runtime, "checkpoint_path", None)
    if checkpoint_path is not None:
        return Path(checkpoint_path).expanduser().resolve().parent / ".nullion-artifacts"
    return nullion_data_home() / ".nullion-artifacts"


def ensure_artifact_root(runtime) -> Path:
    root = artifact_root_for_runtime(runtime)
    root.mkdir(parents=True, exist_ok=True)
    return root


def artifact_root_for_principal(principal_id: str | None) -> Path:
    from nullion.workspace_storage import workspace_storage_roots_for_principal

    return workspace_storage_roots_for_principal(principal_id).artifacts


def path_is_within(path: Path, root: Path) -> bool:
    try:
        path.expanduser().resolve().relative_to(root.expanduser().resolve())
        return True
    except ValueError:
        return False


def artifact_format_extension(path: str | Path) -> str:
    """Normalize a path's known format alias without inspecting user prose."""
    suffix = Path(path).suffix.lower()
    return ".html" if suffix == ".htm" else suffix


def normalize_artifact_extensions(extensions: Iterable[str] | None) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_extension in extensions or ():
        extension = str(raw_extension or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        extension = artifact_format_extension("file" + extension)
        if extension not in normalized:
            normalized.append(extension)
    return tuple(normalized)


def is_unrequested_internal_sidecar_artifact(
    path: str | Path,
    *,
    requested_extensions: Iterable[str] | None = None,
) -> bool:
    candidate = Path(str(path or "")).expanduser()
    suffix = candidate.suffix.lower()
    if not suffix:
        return False
    if suffix in set(normalize_artifact_extensions(requested_extensions)):
        return False
    if suffix in {".json", ".log"}:
        return True
    tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", candidate.stem.lower())
        if token
    }
    if suffix == ".json":
        return bool(tokens & _INTERNAL_SIDECAR_ARTIFACT_TOKENS)
    if suffix in {".md", ".txt"}:
        return bool(tokens & _INTERNAL_SIDECAR_ARTIFACT_TOKENS)
    return False


def _deliverable_html_document(path: Path, size: int) -> bool:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as stream:
            sample = stream.read(512).lower()
        if "<html" not in sample:
            return False
        if size >= _MIN_HTML_ARTIFACT_BYTES:
            return True
        # Admit compact documents without exposing short renderer fragments.
        if re.match(r"(?:\s|<!--.*?-->)*<!doctype[ \t\n\f\r]+html[ \t\n\f\r]*>",
                    sample.lstrip("\ufeff"), flags=re.DOTALL) is None:
            return False
        from nullion.artifact_validation import validate_artifact_paths
        return validate_artifact_paths([str(path)]).ok
    except OSError:
        return False


def artifact_descriptor_for_path(path: Path, *, artifact_root: Path) -> ArtifactDescriptor | None:
    resolved_path = path.expanduser().resolve()
    if not path_is_within(resolved_path, artifact_root) or not resolved_path.is_file():
        return None
    stat = resolved_path.stat()
    if stat.st_size == 0:
        return None
    if resolved_path.suffix.lower() in _BLOCKED_DOWNLOAD_SUFFIXES:
        return None
    if resolved_path.suffix.lower() in _HTML_ARTIFACT_SUFFIXES and not _deliverable_html_document(resolved_path, stat.st_size):
        return None
    artifact_id = hashlib.sha256(
        f"{resolved_path}:{stat.st_mtime_ns}:{stat.st_size}".encode("utf-8")
    ).hexdigest()[:24]
    return ArtifactDescriptor(
        artifact_id=artifact_id,
        name=resolved_path.name,
        path=str(resolved_path),
        media_type=(
            _ARTIFACT_MEDIA_TYPES_BY_SUFFIX.get(resolved_path.suffix.lower())
            or mimetypes.guess_type(resolved_path.name)[0]
            or "application/octet-stream"
        ),
        size_bytes=stat.st_size,
    )


def is_deliverable_explicit_media_path(path: Path) -> bool:
    """Return whether an explicit MEDIA/ARTIFACT path is safe to upload."""
    if not path.is_absolute():
        return False
    resolved_path = path.expanduser().resolve()
    if not resolved_path.is_file():
        return False
    try:
        stat = resolved_path.stat()
    except OSError:
        return False
    if stat.st_size == 0:
        return False
    if resolved_path.suffix.lower() in _BLOCKED_DOWNLOAD_SUFFIXES:
        return False
    if resolved_path.suffix.lower() in _HTML_ARTIFACT_SUFFIXES:
        return _deliverable_html_document(resolved_path, stat.st_size)
    return True


def artifact_descriptors_for_paths(
    artifact_paths: list[str] | tuple[str, ...] | None,
    *,
    artifact_root: Path,
) -> list[ArtifactDescriptor]:
    descriptors: list[ArtifactDescriptor] = []
    for raw_path in artifact_paths or []:
        if not isinstance(raw_path, str) or not raw_path:
            continue
        descriptor = artifact_descriptor_for_path(Path(raw_path), artifact_root=artifact_root)
        if descriptor is not None:
            descriptors.append(descriptor)
    return descriptors


def artifact_output_descriptor(
    path: str | Path,
    *,
    role: str = ARTIFACT_ROLE_DELIVERABLE,
    kind: str | None = None,
    label: str | None = None,
) -> dict[str, object]:
    resolved_path = Path(path).expanduser()
    descriptor: dict[str, object] = {
        "path": str(resolved_path),
        "role": role,
    }
    if kind:
        descriptor["kind"] = kind
    if label:
        descriptor["label"] = label
    suffix = resolved_path.suffix.lower()
    if suffix:
        descriptor["extension"] = suffix
    media_type = _ARTIFACT_MEDIA_TYPES_BY_SUFFIX.get(suffix) or mimetypes.guess_type(resolved_path.name)[0]
    if media_type:
        descriptor["media_type"] = media_type
    try:
        if resolved_path.is_file():
            descriptor["size_bytes"] = resolved_path.stat().st_size
    except OSError:
        pass
    return descriptor


def artifact_paths_from_output_descriptors(
    output: object,
    *,
    roles: Iterable[str] | None = None,
) -> list[str]:
    if not isinstance(output, dict):
        return []
    role_set = set(roles or ARTIFACT_DELIVERY_ROLES)
    paths: list[str] = []
    descriptors = output.get("artifact_descriptors")
    if isinstance(descriptors, (list, tuple)):
        for descriptor in descriptors:
            if not isinstance(descriptor, dict):
                continue
            role = descriptor.get("role")
            if not isinstance(role, str) or role not in role_set:
                continue
            path = descriptor.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path)
    return list(dict.fromkeys(paths))


def output_has_artifact_descriptors(output: object) -> bool:
    return isinstance(output, dict) and isinstance(output.get("artifact_descriptors"), (list, tuple))



UNVERIFIED_ARTIFACT_PUBLICATION_REPLY = "I can’t verify that this request was completed."


def scope_declares_message_only(tool_results: Iterable[object] | None) -> bool:
    """A validated message scope cannot be expanded by authored file paths."""
    from nullion.tools import normalize_tool_status

    for result in reversed(tuple(tool_results or ())):
        name = result.get("tool_name") if isinstance(result, dict) else getattr(result, "tool_name", None)
        status = result.get("status") if isinstance(result, dict) else getattr(result, "status", None)
        if name != "request_tool_scope" or normalize_tool_status(status) != "completed":
            continue
        output = result.get("output") if isinstance(result, dict) else getattr(result, "output", None)
        if isinstance(output, dict) and isinstance(output.get("artifact_extensions"), list):
            return (not output["artifact_extensions"]
                    and not output.get("embedded_media_artifact_extensions")
                    and output.get("source_artifact_delivery_requested") is False)
    return False


def filter_message_scope_media_reply(reply: str, *, tool_results, approved_paths: Iterable[str]) -> str:
    """Retain only authored media selected from the turn's artifact receipts."""
    if not scope_declares_message_only(tool_results) or not media_candidate_paths_from_text(reply):
        return reply
    approved = {Path(path).expanduser().resolve() for path in filter_source_artifact_paths(approved_paths, tool_results)}
    kept, matched, rejected = [], False, False
    for line in reply.splitlines():
        directive = parse_media_directive_line(line)
        if directive is None:
            kept.append(line)
        elif directive.path.expanduser().resolve() in approved:
            matched = True
            kept.append(line)
        else:
            rejected = True
    if rejected:
        return "\n".join(kept).strip() if matched else UNVERIFIED_ARTIFACT_PUBLICATION_REPLY
    return reply


def filter_source_artifact_paths(paths: Iterable[str], tool_results: Iterable[object]) -> list[str]:
    """Explicit source/intermediate roles survive prose and raw path forwarding."""
    tool_results = tuple(tool_results or ())
    blocked: set[str] = set()
    deliverable: set[str] = set()
    excluded_extensions: set[str] = set()
    def identity(path: str) -> str:
        return str(Path(path).expanduser().resolve())
    for result in tool_results or ():
        output = result.get("output") if isinstance(result, dict) else getattr(result, "output", None)
        name = result.get("tool_name") if isinstance(result, dict) else getattr(result, "tool_name", None)
        status = result.get("status") if isinstance(result, dict) else getattr(result, "status", None)
        if name == "request_tool_scope" and status == "completed" and isinstance(output, dict):
            excluded_extensions.update(normalize_artifact_extensions(output.get("excluded_artifact_extensions") or ()))
        if name == "browser_run_task" and isinstance(output, dict) and output.get("result_kind") == "source_observations":
            audit = output.get("execution_audit")
            observed_paths = output.get("artifact_paths")
            # Older compact receipts can retain these paths without descriptors.
            if (isinstance(audit, dict) and audit.get("version") == 1
                    and audit.get("captures_are_internal") is True
                    and audit.get("deliverable_file_count") == 0 and audit.get("downloaded_file_count") == 0
                    and isinstance(observed_paths, (list, tuple))):
                blocked.update(identity(path) for path in observed_paths if isinstance(path, str) and path.strip())
        if not output_has_artifact_descriptors(output):
            continue
        for descriptor in output["artifact_descriptors"]:
            if not isinstance(descriptor, dict) or not isinstance(descriptor.get("path"), str):
                continue
            role = descriptor.get("role")
            if role in ARTIFACT_DELIVERY_ROLES:
                deliverable.add(identity(descriptor["path"]))
            elif role in {ARTIFACT_ROLE_SOURCE, ARTIFACT_ROLE_INTERMEDIATE}:
                blocked.add(identity(descriptor["path"]))
    blocked.difference_update(deliverable)
    from nullion.artifact_lifecycle import restored_delivery_paths
    restored = restored_delivery_paths(tool_results)
    return [path for path in paths if identity(path) not in blocked
            and (artifact_format_extension(path) not in excluded_extensions
                 or identity(path) in restored)]

def promote_supporting_asset_artifact_paths(
    artifact_paths: list[str] | tuple[str, ...] | None,
    *,
    artifact_roots: Iterable[Path],
) -> list[str]:
    """Replace HTML supporting image assets with the HTML artifact that owns them."""

    resolved_paths = _resolve_artifact_candidate_paths(artifact_paths, artifact_roots=artifact_roots)
    if not resolved_paths:
        return []
    image_paths = [path for path in resolved_paths if path.suffix.lower() in _IMAGE_ARTIFACT_SUFFIXES]
    if not image_paths:
        return [str(path) for path in resolved_paths]

    html_candidates = _referencing_html_candidates(resolved_paths, image_paths, artifact_roots=artifact_roots)
    if not html_candidates:
        return [str(path) for path in resolved_paths]

    supporting_assets: set[Path] = set()
    owning_html: list[Path] = []
    seen_html: set[Path] = set()
    for html_path in html_candidates:
        try:
            if html_path.stat().st_size > _MAX_SUPPORTING_ASSET_HTML_BYTES:
                continue
            html_text = html_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for image_path in image_paths:
            if _html_references_asset(html_path, html_text, image_path):
                supporting_assets.add(image_path)
                if html_path not in seen_html:
                    seen_html.add(html_path)
                    owning_html.append(html_path)

    if not supporting_assets:
        return [str(path) for path in resolved_paths]

    selected: list[Path] = []
    for path in resolved_paths:
        if path in supporting_assets:
            continue
        if path not in selected:
            selected.append(path)
    for html_path in owning_html:
        if html_path not in selected:
            selected.append(html_path)
    return [str(path) for path in selected]


def _resolve_artifact_candidate_paths(
    artifact_paths: list[str] | tuple[str, ...] | None,
    *,
    artifact_roots: Iterable[Path],
) -> list[Path]:
    roots = tuple(dict.fromkeys(Path(root).expanduser().resolve() for root in artifact_roots))
    resolved: list[Path] = []
    seen: set[Path] = set()
    for raw_path in artifact_paths or ():
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        path = Path(raw_path).expanduser()
        candidates = [path] if path.is_absolute() else [root / path for root in roots]
        for candidate in candidates:
            try:
                resolved_candidate = candidate.resolve()
            except (OSError, RuntimeError, ValueError):
                continue
            if not resolved_candidate.is_file() or resolved_candidate in seen:
                continue
            if not any(path_is_within(resolved_candidate, root) for root in roots):
                continue
            seen.add(resolved_candidate)
            resolved.append(resolved_candidate)
            break
    return resolved


def _referencing_html_candidates(
    resolved_paths: list[Path],
    image_paths: list[Path],
    *,
    artifact_roots: Iterable[Path],
) -> list[Path]:
    roots = tuple(dict.fromkeys(Path(root).expanduser().resolve() for root in artifact_roots))
    candidates: list[Path] = []
    seen: set[Path] = set()

    def add(candidate: Path) -> None:
        try:
            resolved = candidate.expanduser().resolve()
        except (OSError, RuntimeError, ValueError):
            return
        if resolved in seen or not resolved.is_file() or resolved.suffix.lower() not in _HTML_ARTIFACT_SUFFIXES:
            return
        if not any(path_is_within(resolved, root) for root in roots):
            return
        seen.add(resolved)
        candidates.append(resolved)

    for path in resolved_paths:
        add(path)
    for image_path in image_paths:
        add(image_path.with_suffix(".html"))
        parent = image_path.parent
        for html_path in sorted(parent.glob("*.htm*"))[:_MAX_SUPPORTING_ASSET_HTML_CANDIDATES]:
            add(html_path)
        for html_path in sorted(parent.parent.glob("*.htm*"))[:_MAX_SUPPORTING_ASSET_HTML_CANDIDATES]:
            add(html_path)

    try:
        candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    except OSError:
        pass
    return candidates[:_MAX_SUPPORTING_ASSET_HTML_CANDIDATES]


def _html_references_asset(html_path: Path, html_text: str, asset_path: Path) -> bool:
    try:
        relative = os.path.relpath(asset_path, html_path.parent).replace(os.sep, "/")
    except ValueError:
        return False
    references = {
        relative,
        f"./{relative}",
        quote(relative),
        quote(f"./{relative}"),
    }
    return any(reference in html_text for reference in references)


def artifact_path_for_generated_file(runtime, *, suffix: str, stem: str = "nullion-artifact") -> Path:
    root = ensure_artifact_root(runtime)
    normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    return root / f"{stem}-{uuid4().hex[:12]}{normalized_suffix}"


def artifact_path_for_generated_workspace_file(
    *,
    principal_id: str | None,
    suffix: str,
    stem: str = "nullion-artifact",
    filename: str | None = None,
) -> Path:
    root = artifact_root_for_principal(principal_id)
    normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    if filename is not None:
        # Keep the public filename independent from collision-free storage.
        # Allocate a private export namespace rather than changing the name or
        # overwriting another request's artifact with the same requested name.
        if (not isinstance(filename, str) or not filename or filename in {'.', '..'}
                or any(char in filename for char in ('/', '\\', '\0'))
                or Path(filename).suffix.lower() != normalized_suffix.lower()):
            raise ValueError('Use a filename with the declared artifact extension')
        root.mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(prefix='export-', dir=root)) / filename
    return root / f"{stem}-{uuid4().hex[:12]}{normalized_suffix}"


class _HTMLDocumentEncoding(HTMLParser):
    """Locate real encoding tags without rewriting body, script or style text."""

    # Older supported Python versions only treat script and style as raw text.
    CDATA_CONTENT_ELEMENTS = ("script", "style", "iframe", "noembed", "noframes", "xmp")

    def __init__(self, text: str):
        super().__init__(convert_charrefs=False)
        self.offsets = [0]
        for line in text.split('\n')[:-1]:
            self.offsets.append(self.offsets[-1] + len(line) + 1)
        self.head_end = self.html_end = self.doctype_end = None
        self.encoding_tags = []
        self.declared_encodings = []
        self.literal_element = None
        self.feed(text)

    def _offset(self):
        line, column = self.getpos()
        return self.offsets[line - 1] + column

    def handle_starttag(self, tag, attrs):
        if self.literal_element:
            return
        if tag in {"title", "textarea"}:
            self.literal_element = tag
        start = self._offset()
        end = start + len(self.get_starttag_text())
        if tag == 'head' and self.head_end is None:
            self.head_end = end
        elif tag == 'html' and self.html_end is None:
            self.html_end = end
        if tag == 'meta':
            attributes = dict(attrs)
            if "charset" in attributes:
                self.encoding_tags.append((start, end))
                self.declared_encodings.append(attributes["charset"])
            elif str(attributes.get("http-equiv") or "").lower() == "content-type":
                self.encoding_tags.append((start, end))
                self.declared_encodings.append(_mime_charset(attributes.get("content")))

    def handle_endtag(self, tag):
        if tag == self.literal_element:
            self.literal_element = None

    def handle_decl(self, decl):
        if decl.lower().startswith('doctype html') and self.doctype_end is None:
            self.doctype_end = self._offset() + len(decl) + 3


def _mime_charset(content_type: str | None) -> str | None:
    if not content_type:
        return None
    message = Message()
    message["Content-Type"] = content_type
    return message.get_content_charset()


def _bom_encoding(data: bytes) -> str | None:
    if data.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    if data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return "utf-16"
    return None


def declared_html_encoding(prefix: bytes) -> str | None:
    """Return a BOM or the first recognized HTML charset in the prescan window."""
    if bom := _bom_encoding(prefix):
        return bom
    if b"charset" not in prefix[:1024].lower():
        return None
    from webencodings import lookup

    # Scan ASCII markup without interpreting the non-ASCII content bytes.
    parser = _HTMLDocumentEncoding(prefix[:1024].decode("latin-1"))
    for label in parser.declared_encodings:
        encoding = lookup(label) if label else None
        if encoding is None:
            continue
        if encoding.name in {"utf-16le", "utf-16be"}:
            return "utf-8"
        if encoding.name == "x-user-defined":
            return "cp1252"
        return encoding.codec_info.name
    return None


def decode_html_document(data: bytes, *, content_type: str | None = None, errors: str = "strict") -> str:
    """Decode before normalizing or modifying HTML, without losing source bytes."""
    if bom := _bom_encoding(data):
        return data.decode(bom, errors)
    if label := _mime_charset(content_type):
        from webencodings import lookup

        if encoding := lookup(label):
            return encoding.codec_info.decode(data, errors)[0]
    return data.decode(declared_html_encoding(data) or "utf-8", errors)


def _normalize_document_encoding(text: str, title: str) -> str:
    parsed = _HTMLDocumentEncoding(text)
    for start, end in reversed(parsed.encoding_tags):
        text = text[:start] + text[end:]
    parsed = _HTMLDocumentEncoding(text)
    meta = '<meta charset="utf-8">'
    if parsed.head_end is not None:
        insertion, addition = parsed.head_end, meta
    else:
        insertion = parsed.html_end or parsed.doctype_end or 0
        addition = '<head>' + meta + '<title>' + html.escape(title, quote=True) + '</title></head>'
    result = text[:insertion] + addition + text[insertion:]
    # A long leading declaration/comment cannot be relocated without changing
    # source order. A UTF-8 signature makes decoding certain in that case.
    declaration_end = insertion + addition.index(meta) + len(meta)
    if len(result[:declaration_end].encode('utf-8')) > 1024 and not result.startswith('\ufeff'):
        result = '\ufeff' + result
    return result


def ensure_utf8_html_encoding(text: str, *, title: str) -> str:
    """Keep HTML bytes stable when a UTF-8 write already decodes correctly."""
    encoding = declared_html_encoding(text[:1024].encode("utf-8"))
    if encoding in {"utf-8", "utf-8-sig"} or (encoding is None and text.isascii()):
        return text
    return _normalize_document_encoding(text, title)


def normalize_html_document(html_text: str, *, title: str = "Nullion HTML Preview") -> str:
    text = str(html_text or "").removeprefix("\ufeff").strip()
    if not text:
        return ""
    if _HTML_DOCUMENT_RE.search(text):
        return _normalize_document_encoding(text, title)
    escaped_title = html.escape(title, quote=True)
    document = (
        "<!doctype html>\n"
        "<html>\n"
        "<head>\n"
        '  <meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"  <title>{escaped_title}</title>\n"
        "</head>\n"
        "<body>\n"
        f"{text}\n"
        "</body>\n"
        "</html>\n"
    )
    return _normalize_document_encoding(document.strip(), title)


def _looks_like_substantial_html(text: str) -> bool:
    stripped = str(text or "").strip()
    if len(stripped) < _MIN_INLINE_HTML_ARTIFACT_CHARS:
        return False
    return bool(_HTML_DOCUMENT_RE.search(stripped[:2048]) or _HTML_FRAGMENT_RE.search(stripped[:2048]))


def materialize_inline_html_reply_artifact(
    reply: str | None,
    *,
    principal_id: str | None,
    stem: str = "html-preview",
) -> tuple[str | None, str | None]:
    """Promote substantial fenced HTML in a reply into a workspace artifact."""

    if reply is None or "```" not in str(reply):
        return reply, None
    text = str(reply)
    match = _FENCED_HTML_BLOCK_RE.search(text)
    if match is None:
        return reply, None
    html_text = str(match.group("body") or "").strip()
    if not _looks_like_substantial_html(html_text):
        return reply, None
    artifact_path = artifact_path_for_generated_workspace_file(
        principal_id=principal_id,
        suffix=".html",
        stem=stem,
    )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(normalize_html_document(html_text), encoding="utf-8")
    before = text[: match.start()].rstrip()
    after = text[match.end() :].strip()
    parts = [part for part in (before, after) if part]
    parts.append("I attached the HTML preview so you can open it.")
    parts.append(f"MEDIA:{artifact_path}")
    return "\n\n".join(parts), str(artifact_path)


def is_safe_artifact_path(path: Path, *, artifact_root: Path | None = None) -> bool:
    if not path.is_absolute():
        return False
    resolved_path = path.resolve(strict=False)
    if artifact_root is not None and path_is_within(resolved_path, artifact_root):
        return True
    if resolved_path.parent.name == ".nullion-artifacts":
        return True
    return False


def _media_directive_path_text(value: str) -> str:
    """One protocol line carries one complete path, optionally quoted."""
    value=value.strip()
    if value.startswith('"') and value.endswith('"'):
        import json
        try:
            decoded=json.loads(value)
        except ValueError:
            pass
        else:
            if isinstance(decoded,str):
                return decoded
    for start,end in (('"','"'),("'","'"),('`','`'),('<','>')):
        if len(value)>=2 and value.startswith(start) and value.endswith(end):
            return value.strip('`') if start=='`' else value[1:-1]
    return value


def parse_media_directive_line(raw_line: str) -> MediaDirective | None:
    line = str(raw_line or "").strip().lstrip(_MEDIA_DIRECTIVE_STRIP_CHARS).strip()
    directive_prefix = ""
    media_index = -1
    for prefix in _ATTACHMENT_DIRECTIVE_PREFIXES:
        candidate_index = line.find(prefix)
        if candidate_index >= 0 and (media_index < 0 or candidate_index < media_index):
            directive_prefix = prefix
            media_index = candidate_index
    if media_index < 0 or not directive_prefix:
        for word in _ATTACHMENT_DIRECTIVE_WORDS:
            word_index = line.find(f"{word} ")
            if word_index < 0:
                continue
            candidate_prefix = line[:word_index].strip()
            if candidate_prefix and candidate_prefix.lstrip("-*•> ").strip():
                continue
            attachment_path = _media_directive_path_text(line[word_index + len(word) :])
            if not _looks_like_attachment_path(attachment_path):
                continue
            raw_text = str(raw_line or "")
            raw_media_index = raw_text.find(word)
            prefix = raw_text[:raw_media_index].strip() if raw_media_index >= 0 else ""
            prefix = prefix.lstrip(_MEDIA_DIRECTIVE_STRIP_CHARS).strip()
            if prefix and not prefix.lstrip("-*•> ").strip():
                prefix = ""
            return MediaDirective(path=Path(attachment_path), prefix=prefix)
        return None
    raw_text = str(raw_line or "")
    raw_media_index = raw_text.find(directive_prefix)
    prefix = raw_text[:raw_media_index].strip() if raw_media_index >= 0 else ""
    prefix = prefix.lstrip(_MEDIA_DIRECTIVE_STRIP_CHARS).strip()
    if prefix and not prefix.lstrip("-*•> ").strip():
        prefix = ""
    attachment_path = line[media_index:].removeprefix(directive_prefix).strip()
    if not attachment_path:
        return None
    attachment_path = _media_directive_path_text(attachment_path)
    if not attachment_path:
        return None
    return MediaDirective(path=Path(attachment_path), prefix=prefix)


def _looks_like_attachment_path(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    first_part = text.replace("\\", "/").split("/", 1)[0]
    if first_part in {"artifacts", "files", "media"}:
        return True
    if text.startswith(("/", "~")):
        return True
    if text.startswith("file:"):
        return True
    if "/" not in text and "\\" not in text and not text.startswith("."):
        suffix = Path(text).suffix
        return 1 < len(suffix) <= 16
    return len(text) >= 3 and text[1:3] in {":\\", ":/"} and text[0].isalpha()


def media_candidate_paths_from_text(text: str) -> list[Path]:
    paths: list[Path] = []
    for raw_line in str(text or "").splitlines():
        directive = parse_media_directive_line(raw_line)
        if directive is not None:
            paths.append(directive.path)
    lines = str(text or "").splitlines()
    index = 0
    while index < len(lines):
        current = lines[index].strip().lstrip(_MEDIA_DIRECTIVE_STRIP_CHARS).strip()
        following = lines[index + 1].strip().strip("`'\"<>") if index + 1 < len(lines) else ""
        if current in {"MEDIA", "ARTIFACT"} and following:
            paths.append(Path(following))
            index += 2
            continue
        index += 1
    return list(dict.fromkeys(paths))


def split_media_reply_attachments(
    reply: str,
    *,
    is_safe_attachment_path,
    resolve_attachment_path=None,
) -> tuple[str | None, tuple[Path, ...]]:
    def _resolve(path: Path) -> Path:
        if resolve_attachment_path is None:
            return path
        resolved = resolve_attachment_path(path)
        return resolved if isinstance(resolved, Path) else path

    caption_lines: list[str] = []
    attachment_paths: list[Path] = []
    lines = str(reply or "").splitlines()
    index = 0
    while index < len(lines):
        raw_line = lines[index]
        directive = parse_media_directive_line(raw_line)
        if directive is None:
            current = raw_line.strip().lstrip(_MEDIA_DIRECTIVE_STRIP_CHARS).strip()
            following = lines[index + 1].strip().strip("`'\"<>") if index + 1 < len(lines) else ""
            if current in {"MEDIA", "ARTIFACT"} and following:
                split_path = _resolve(Path(following))
                if is_safe_attachment_path(split_path) and split_path.is_file():
                    attachment_paths.append(split_path)
                    index += 2
                    continue
            caption_lines.append(raw_line)
            index += 1
            continue
        if directive.prefix:
            caption_lines.append(directive.prefix)
        attachment_path = _resolve(directive.path)
        if is_safe_attachment_path(attachment_path):
            if attachment_path.is_file():
                attachment_paths.append(attachment_path)
            else:
                caption_lines.append(f"Attachment unavailable: {attachment_path.name or directive.path.name or 'file'}")
        index += 1
    caption = "\n".join(caption_lines).strip() or None
    return caption, tuple(dict.fromkeys(attachment_paths))

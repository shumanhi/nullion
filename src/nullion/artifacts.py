"""Platform-neutral artifact helpers for chat adapters."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import html
import mimetypes
import os
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import quote
from uuid import uuid4

_MIN_HTML_ARTIFACT_BYTES = 64
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


def normalize_artifact_extensions(extensions: Iterable[str] | None) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_extension in extensions or ():
        extension = str(raw_extension or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
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


def artifact_descriptor_for_path(path: Path, *, artifact_root: Path) -> ArtifactDescriptor | None:
    resolved_path = path.expanduser().resolve()
    if not path_is_within(resolved_path, artifact_root) or not resolved_path.is_file():
        return None
    stat = resolved_path.stat()
    if stat.st_size == 0:
        return None
    if resolved_path.suffix.lower() in _BLOCKED_DOWNLOAD_SUFFIXES:
        return None
    if resolved_path.suffix.lower() in _HTML_ARTIFACT_SUFFIXES:
        sample = resolved_path.read_text(encoding="utf-8", errors="ignore")[:512].strip().lower()
        if stat.st_size < _MIN_HTML_ARTIFACT_BYTES or "<html" not in sample:
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
        try:
            sample = resolved_path.read_text(encoding="utf-8", errors="ignore")[:512].strip().lower()
        except OSError:
            return False
        if stat.st_size < _MIN_HTML_ARTIFACT_BYTES or "<html" not in sample:
            return False
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
) -> Path:
    root = artifact_root_for_principal(principal_id)
    normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    return root / f"{stem}-{uuid4().hex[:12]}{normalized_suffix}"


def normalize_html_document(html_text: str, *, title: str = "Nullion HTML Preview") -> str:
    text = str(html_text or "").strip()
    if not text:
        return ""
    if _HTML_DOCUMENT_RE.search(text[:1024]):
        return text
    escaped_title = html.escape(title, quote=True)
    return (
        "<!doctype html>\n"
        "<html>\n"
        "<head>\n"
        '  <meta charset="utf-8">\n'
        '  <meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"  <title>{escaped_title}</title>\n"
        "</head>\n"
        "<body>\n"
        f"{text}\n"
        "</body>\n"
        "</html>\n"
    )


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
            attachment_path = line[word_index + len(word) :].strip().split(maxsplit=1)[0].strip("`'\"<>")
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
    attachment_path = attachment_path.split(maxsplit=1)[0].strip("`'\"<>")
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

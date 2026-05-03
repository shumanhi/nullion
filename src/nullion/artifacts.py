"""Platform-neutral artifact helpers for chat adapters."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import mimetypes
import os
from pathlib import Path
import tempfile
from uuid import uuid4

_MIN_HTML_ARTIFACT_BYTES = 64
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
_MEDIA_DIRECTIVE_STRIP_CHARS = "\ufeff\u200b\u200c\u200d"
_ARTIFACT_MEDIA_TYPES_BY_SUFFIX = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


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
    data_dir = os.environ.get("NULLION_DATA_DIR")
    if isinstance(data_dir, str) and data_dir.strip():
        return Path(data_dir).expanduser().resolve() / ".nullion-artifacts"
    return (Path.home() / ".nullion" / ".nullion-artifacts").resolve()


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


def artifact_descriptor_for_path(path: Path, *, artifact_root: Path) -> ArtifactDescriptor | None:
    resolved_path = path.expanduser().resolve()
    if not path_is_within(resolved_path, artifact_root) or not resolved_path.is_file():
        return None
    stat = resolved_path.stat()
    if stat.st_size == 0:
        return None
    if resolved_path.suffix.lower() in _BLOCKED_DOWNLOAD_SUFFIXES:
        return None
    if resolved_path.suffix.lower() in {".html", ".htm"}:
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


def is_safe_artifact_path(path: Path, *, artifact_root: Path | None = None) -> bool:
    if not path.is_absolute():
        return False
    resolved_path = path.resolve(strict=False)
    if artifact_root is not None and path_is_within(resolved_path, artifact_root):
        return True
    if resolved_path.parent.name == ".nullion-artifacts":
        return True
    temp_root = Path(tempfile.gettempdir()).resolve()
    if temp_root in resolved_path.parents:
        return resolved_path.name.startswith("nullion-artifact-")
    if Path("/tmp").resolve() in resolved_path.parents or Path("/private/tmp").resolve() in resolved_path.parents:
        return resolved_path.name.startswith("nullion-artifact-")
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


def media_candidate_paths_from_text(text: str) -> list[Path]:
    paths: list[Path] = []
    for raw_line in str(text or "").splitlines():
        directive = parse_media_directive_line(raw_line)
        if directive is not None:
            paths.append(directive.path)
    return paths


def split_media_reply_attachments(
    reply: str,
    *,
    is_safe_attachment_path,
) -> tuple[str | None, tuple[Path, ...]]:
    caption_lines: list[str] = []
    attachment_paths: list[Path] = []
    for raw_line in str(reply or "").splitlines():
        directive = parse_media_directive_line(raw_line)
        if directive is None:
            caption_lines.append(raw_line)
            continue
        if directive.prefix:
            caption_lines.append(directive.prefix)
        if is_safe_attachment_path(directive.path):
            if directive.path.is_file():
                attachment_paths.append(directive.path)
            else:
                caption_lines.append(f"Attachment unavailable: {directive.path.name or 'file'}")
    caption = "\n".join(caption_lines).strip() or None
    return caption, tuple(attachment_paths)

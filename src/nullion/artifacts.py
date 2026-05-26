"""Platform-neutral artifact helpers for chat adapters."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import mimetypes
import os
from pathlib import Path
import tempfile
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
_ARTIFACT_MEDIA_TYPES_BY_SUFFIX = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".numbers": "application/vnd.apple.numbers",
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

"""Validation for generated artifacts before user-visible delivery."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import gzip
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import tarfile
from xml.etree import ElementTree
import zipfile


MAX_TEXT_ARTIFACT_BYTES = 5 * 1024 * 1024
_TEXT_SUFFIXES = {
    ".txt",
    ".md",
    ".markdown",
    ".csv",
    ".tsv",
    ".log",
    ".rtf",
    ".ics",
    ".vcf",
    ".srt",
    ".vtt",
    ".yaml",
    ".yml",
}
_JSON_SUFFIXES = {".json", ".jsonl"}
_IMAGE_SIGNATURES = {
    ".png": b"\x89PNG\r\n\x1a\n",
    ".jpg": b"\xff\xd8\xff",
    ".jpeg": b"\xff\xd8\xff",
    ".gif": b"GIF",
    ".webp": b"RIFF",
    ".bmp": b"BM",
}
_TIFF_SUFFIXES = {".tif", ".tiff"}
_XML_SUFFIXES = {".xml", ".svg", ".kml"}
_MP4_CONTAINER_SUFFIXES = {".mp4", ".m4a", ".m4v", ".mov", ".avif", ".heic", ".heif"}
_EBML_CONTAINER_SUFFIXES = {".webm", ".mkv"}
_AUDIO_SUFFIXES = {".aac", ".flac", ".mp3", ".oga", ".ogg", ".opus", ".wav", ".weba"}
_VIDEO_SUFFIXES = {".avi", ".mpeg", ".mpg"}
_ZIP_SUFFIXES = {".zip", ".epub", ".jar", ".apk"}
_ARCHIVE_SUFFIXES = {".7z", ".rar", ".tar", ".tgz", ".gz"}
_BINARY_SIGNATURES = {
    ".7z": b"7z\xbc\xaf\x27\x1c",
    ".rar": b"Rar!\x1a\x07",
    ".wasm": b"\x00asm",
    ".sqlite": b"SQLite format 3\x00",
}
_OFFICE_REQUIRED_MEMBERS = {
    ".xlsx": ("[Content_Types].xml", "xl/workbook.xml"),
    ".docx": ("[Content_Types].xml", "word/document.xml"),
    ".pptx": ("[Content_Types].xml", "ppt/presentation.xml"),
}
_HTML_DYNAMIC_RENDER_TARGET_RE = re.compile(
    r"document\s*\.\s*getElementById\s*\(\s*['\"](?P<target>[^'\"]+)['\"]\s*\)"
    r"\s*\.\s*(?:innerHTML|textContent|appendChild)\b",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ArtifactValidationIssue:
    path: str
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class ArtifactValidationResult:
    issues: tuple[ArtifactValidationIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.issues


class _HtmlShapeParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.html_count = 0
        self.body_count = 0
        self.main_count = 0
        self.table_count = 0
        self.anchor_hrefs: list[str] = []
        self.classes: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized == "html":
            self.html_count += 1
        elif normalized == "body":
            self.body_count += 1
        elif normalized == "main":
            self.main_count += 1
        elif normalized == "table":
            self.table_count += 1
        attr_map = {name.lower(): value or "" for name, value in attrs}
        if normalized == "a" and attr_map.get("href"):
            self.anchor_hrefs.append(attr_map["href"])
        for class_name in attr_map.get("class", "").split():
            self.classes.append(class_name)


class _HtmlVisibleTextParser(HTMLParser):
    _HIDDEN_TAGS = frozenset({"head", "script", "style", "template", "noscript"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:  # noqa: ARG002
        if tag.lower() in self._HIDDEN_TAGS:
            self._hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self._HIDDEN_TAGS and self._hidden_depth:
            self._hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._hidden_depth and str(data or "").strip():
            self.parts.append(str(data))


def normalize_required_artifact_content_tokens(value: object) -> tuple[str, ...]:
    raw_values = value if isinstance(value, (list, tuple, set, frozenset)) else ()
    normalized: list[str] = []
    for raw in raw_values:
        token = " ".join(str(raw or "").split()).strip()
        if not token or len(token) > 160 or token in normalized:
            continue
        normalized.append(token)
        if len(normalized) >= 128:
            break
    return tuple(normalized)


def missing_required_artifact_content_tokens(
    content: str,
    *,
    suffix: str,
    required_tokens: object,
) -> tuple[str, ...]:
    tokens = normalize_required_artifact_content_tokens(required_tokens)
    if not tokens:
        return ()
    visible = str(content or "")
    if str(suffix or "").strip().lower() in {".html", ".htm"}:
        parser = _HtmlVisibleTextParser()
        try:
            parser.feed(visible)
            parser.close()
            visible = " ".join(parser.parts)
        except Exception:
            visible = ""
    visible = " ".join(visible.split())
    missing: list[str] = []
    for token in tokens:
        if re.fullmatch(r"[A-Za-z0-9._-]+", token):
            found = re.search(
                rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])",
                visible,
                flags=re.IGNORECASE,
            ) is not None
        else:
            found = token.casefold() in visible.casefold()
        if not found:
            missing.append(token)
    return tuple(missing)


def missing_required_artifact_content_tokens_from_path(
    path: str | Path,
    required_tokens: object,
) -> tuple[str, ...]:
    artifact_path = Path(path).expanduser()
    try:
        content = artifact_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return normalize_required_artifact_content_tokens(required_tokens)
    return missing_required_artifact_content_tokens(
        content,
        suffix=artifact_path.suffix,
        required_tokens=required_tokens,
    )


def validate_artifact_paths(paths: list[str] | tuple[str, ...]) -> ArtifactValidationResult:
    issues: list[ArtifactValidationIssue] = []
    for raw_path in dict.fromkeys(str(path or "").strip() for path in paths):
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        issues.extend(_validate_one_artifact(path))
    return ArtifactValidationResult(tuple(issues))


def _validate_one_artifact(path: Path) -> list[ArtifactValidationIssue]:
    path_text = str(path)
    try:
        stat = path.stat()
    except OSError:
        return [_issue(path_text, "artifact_missing", "Artifact path does not exist.")]
    if not path.is_file():
        return [_issue(path_text, "artifact_not_file", "Artifact path is not a file.")]
    if stat.st_size <= 0:
        return [_issue(path_text, "artifact_empty", "Artifact file is empty.")]

    suffix = path.suffix.lower()
    try:
        if suffix in _JSON_SUFFIXES:
            return _validate_json_artifact(path)
        if suffix == ".html" or suffix == ".htm":
            return _validate_html_artifact(path)
        if suffix in _TEXT_SUFFIXES:
            return _validate_text_artifact(path, stat.st_size)
        if suffix in _XML_SUFFIXES:
            return _validate_xml_artifact(path, suffix)
        if suffix == ".pdf":
            return _validate_prefix(
                path,
                b"%PDF-",
                "pdf_signature_invalid",
                "PDF artifact does not start with a PDF header.",
            )
        if suffix in _IMAGE_SIGNATURES:
            return _validate_image_artifact(path, suffix)
        if suffix in _TIFF_SUFFIXES:
            return _validate_tiff_artifact(path)
        if suffix in _MP4_CONTAINER_SUFFIXES:
            return _validate_mp4_family_artifact(path, suffix)
        if suffix in _EBML_CONTAINER_SUFFIXES:
            return _validate_prefix(
                path,
                b"\x1a\x45\xdf\xa3",
                "ebml_signature_invalid",
                f"{suffix} artifact is not EBML.",
            )
        if suffix in _AUDIO_SUFFIXES:
            return _validate_audio_artifact(path, suffix)
        if suffix in _VIDEO_SUFFIXES:
            return _validate_video_artifact(path, suffix)
        if suffix in _OFFICE_REQUIRED_MEMBERS:
            return _validate_office_artifact(path, suffix)
        if suffix in _ZIP_SUFFIXES:
            return _validate_zip_artifact(path, suffix)
        if suffix in _ARCHIVE_SUFFIXES:
            return _validate_archive_artifact(path, suffix)
        if suffix in _BINARY_SIGNATURES:
            return _validate_prefix(
                path,
                _BINARY_SIGNATURES[suffix],
                "binary_signature_invalid",
                f"{suffix} artifact has an invalid file signature.",
            )
    except OSError as exc:
        return [_issue(path_text, "artifact_unreadable", f"Artifact could not be read: {exc}")]
    except UnicodeDecodeError:
        return [_issue(path_text, "artifact_text_decode_failed", "Artifact is not valid UTF-8 text.")]
    return []


def _validate_json_artifact(path: Path) -> list[ArtifactValidationIssue]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".jsonl":
        for index, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                json.loads(line)
            except json.JSONDecodeError as exc:
                return [
                    _issue(
                        str(path),
                        "jsonl_invalid",
                        f"JSONL artifact has invalid JSON on line {index}: {exc.msg}.",
                    )
                ]
        return []
    try:
        json.loads(text)
    except json.JSONDecodeError as exc:
        return [_issue(str(path), "json_invalid", f"JSON artifact is invalid: {exc.msg}.")]
    return []


def _validate_text_artifact(path: Path, size: int) -> list[ArtifactValidationIssue]:
    if size > MAX_TEXT_ARTIFACT_BYTES:
        return [
            _issue(
                str(path),
                "text_artifact_too_large",
                "Text artifact is too large for safe platform delivery validation.",
            )
        ]
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        return [_issue(str(path), "text_artifact_blank", "Text artifact has no visible content.")]
    if path.suffix.lower() == ".csv":
        return _validate_csv_artifact(path, text)
    return []


def _validate_csv_artifact(path: Path, text: str) -> list[ArtifactValidationIssue]:
    try:
        rows = list(csv.reader(text.splitlines()))
    except csv.Error as exc:
        return [_issue(str(path), "csv_parse_failed", f"CSV artifact is invalid: {exc}.")]
    visible_rows = [row for row in rows if any(str(cell).strip() for cell in row)]
    if not visible_rows:
        return [_issue(str(path), "csv_artifact_blank", "CSV artifact has no visible rows.")]
    expected_width = len(visible_rows[0])
    if expected_width <= 0:
        return [_issue(str(path), "csv_header_empty", "CSV artifact has an empty header row.")]
    for index, row in enumerate(visible_rows[1:], start=2):
        if len(row) != expected_width:
            return [
                _issue(
                    str(path),
                    "csv_inconsistent_columns",
                    f"CSV row {index} has {len(row)} columns; expected {expected_width}.",
                )
            ]
    return []


def _validate_html_artifact(path: Path) -> list[ArtifactValidationIssue]:
    text = path.read_text(encoding="utf-8")
    stripped = text.lstrip().lower()
    issues: list[ArtifactValidationIssue] = []
    if "<html" not in stripped[:1000] and "<!doctype html" not in stripped[:1000]:
        issues.append(_issue(str(path), "html_root_missing", "HTML artifact is missing a document root."))
    parser = _HtmlShapeParser()
    parser.feed(text)
    if parser.html_count > 1 or parser.body_count > 1 or parser.main_count > 1:
        issues.append(
            _issue(
                str(path),
                "html_duplicate_document_root",
                "HTML artifact appears to contain duplicate document/page roots.",
            )
        )
    issues.extend(_validate_html_static_primary_content(path, text))
    issues.extend(_validate_html_report_consistency(path, text, parser))
    return issues


def _validate_html_static_primary_content(path: Path, text: str) -> list[ArtifactValidationIssue]:
    for target in {
        match.group("target").strip()
        for match in _HTML_DYNAMIC_RENDER_TARGET_RE.finditer(text)
        if match.group("target").strip()
    }:
        if _html_has_empty_render_target(text, target):
            return [
                _issue(
                    str(path),
                    "html_primary_content_script_dependent",
                    "HTML artifact relies on client-side JavaScript to populate visible content.",
                )
            ]
    return []


def _html_has_empty_render_target(text: str, target: str) -> bool:
    escaped = re.escape(target)
    return bool(
        re.search(
            rf"<(?P<tag>[a-z][\w:-]*)\b(?=[^>]*\bid\s*=\s*['\"]{escaped}['\"])[^>]*>\s*</(?P=tag)>",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )


def _validate_xml_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    try:
        root = ElementTree.parse(path).getroot()
    except ElementTree.ParseError as exc:
        return [_issue(str(path), "xml_invalid", f"{suffix} artifact is invalid XML: {exc}.")]
    if suffix == ".svg" and not root.tag.lower().endswith("svg"):
        return [_issue(str(path), "svg_root_invalid", "SVG artifact does not have an svg root element.")]
    return []


def _validate_html_report_consistency(path: Path, text: str, parser: _HtmlShapeParser) -> list[ArtifactValidationIssue]:
    issues: list[ArtifactValidationIssue] = []
    metric_match = re.search(
        r'<div\s+class=["\']label["\']>\s*Public mentions found\s*</div>\s*'
        r'<div\s+class=["\']metric["\']>\s*(\d+)\s*</div>',
        text,
        flags=re.IGNORECASE,
    )
    if not metric_match:
        return issues
    declared_mentions = int(metric_match.group(1))
    reddit_links = sum(1 for href in parser.anchor_hrefs if "reddit.com/" in href.lower())
    chart_bars = sum(1 for class_name in parser.classes if class_name == "barwrap")
    observed_counts = [count for count in (reddit_links, chart_bars) if count > 0]
    if observed_counts and any(count != declared_mentions for count in observed_counts):
        details = ", ".join(
            f"{name}={value}"
            for name, value in (
                ("declared", declared_mentions),
                ("reddit_links", reddit_links),
                ("chart_bars", chart_bars),
            )
            if value
        )
        issues.append(
            _issue(
                str(path),
                "html_report_count_mismatch",
                f"HTML report totals disagree with rendered evidence ({details}).",
            )
        )
    return issues


def _validate_prefix(path: Path, prefix: bytes, code: str, message: str) -> list[ArtifactValidationIssue]:
    with path.open("rb") as handle:
        head = handle.read(len(prefix))
    if head != prefix:
        return [_issue(str(path), code, message)]
    return []


def _validate_image_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    prefix = _IMAGE_SIGNATURES[suffix]
    with path.open("rb") as handle:
        head = handle.read(max(12, len(prefix)))
    if not head.startswith(prefix):
        return [_issue(str(path), "image_signature_invalid", f"{suffix} artifact has an invalid image signature.")]
    if suffix == ".webp" and head[8:12] != b"WEBP":
        return [_issue(str(path), "image_signature_invalid", "WebP artifact has an invalid WEBP signature.")]
    return []


def _validate_tiff_artifact(path: Path) -> list[ArtifactValidationIssue]:
    with path.open("rb") as handle:
        head = handle.read(4)
    if head not in {b"II*\x00", b"MM\x00*"}:
        return [_issue(str(path), "image_signature_invalid", "TIFF artifact has an invalid image signature.")]
    return []


def _validate_mp4_family_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    with path.open("rb") as handle:
        head = handle.read(16)
    if len(head) < 12 or head[4:8] != b"ftyp":
        return [_issue(str(path), "mp4_container_invalid", f"{suffix} artifact is missing an ftyp box.")]
    return []


def _validate_audio_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    with path.open("rb") as handle:
        head = handle.read(16)
    if suffix == ".mp3" and (head.startswith(b"ID3") or _looks_like_mp3_frame(head)):
        return []
    if suffix == ".wav" and head.startswith(b"RIFF") and head[8:12] == b"WAVE":
        return []
    if suffix == ".flac" and head.startswith(b"fLaC"):
        return []
    if suffix in {".ogg", ".oga", ".opus"} and head.startswith(b"OggS"):
        return []
    if suffix == ".aac" and _looks_like_aac_frame(head):
        return []
    if suffix == ".weba" and head.startswith(b"\x1a\x45\xdf\xa3"):
        return []
    return [_issue(str(path), "audio_signature_invalid", f"{suffix} artifact has an invalid audio signature.")]


def _validate_video_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    with path.open("rb") as handle:
        head = handle.read(16)
    if suffix == ".avi" and head.startswith(b"RIFF") and head[8:12] == b"AVI ":
        return []
    if suffix in {".mpeg", ".mpg"} and (head.startswith(b"\x00\x00\x01\xba") or head.startswith(b"\x00\x00\x01\xb3")):
        return []
    return [_issue(str(path), "video_signature_invalid", f"{suffix} artifact has an invalid video signature.")]


def _validate_office_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    try:
        with zipfile.ZipFile(path) as archive:
            names = set(archive.namelist())
    except zipfile.BadZipFile:
        return [_issue(str(path), "office_zip_invalid", f"{suffix} artifact is not a valid zip package.")]
    missing = [name for name in _OFFICE_REQUIRED_MEMBERS[suffix] if name not in names]
    if missing:
        return [
            _issue(
                str(path),
                "office_members_missing",
                f"{suffix} artifact is missing required package members: {', '.join(missing)}.",
            )
        ]
    return []


def _validate_zip_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    try:
        with zipfile.ZipFile(path) as archive:
            archive.testzip()
    except zipfile.BadZipFile:
        return [_issue(str(path), "zip_invalid", f"{suffix} artifact is not a valid zip package.")]
    return []


def _validate_archive_artifact(path: Path, suffix: str) -> list[ArtifactValidationIssue]:
    if suffix in {".7z", ".rar"}:
        return _validate_prefix(
            path,
            _BINARY_SIGNATURES[suffix],
            "archive_signature_invalid",
            f"{suffix} artifact has an invalid archive signature.",
        )
    if suffix == ".gz":
        try:
            with gzip.open(path, "rb") as handle:
                handle.read(1)
        except OSError as exc:
            return [_issue(str(path), "gzip_invalid", f"gzip artifact is invalid: {exc}.")]
        return []
    if suffix in {".tar", ".tgz"}:
        if not tarfile.is_tarfile(path):
            return [_issue(str(path), "tar_invalid", f"{suffix} artifact is not a valid tar archive.")]
        return []
    return []


def _looks_like_mp3_frame(head: bytes) -> bool:
    return len(head) >= 2 and head[0] == 0xFF and (head[1] & 0xE0) == 0xE0


def _looks_like_aac_frame(head: bytes) -> bool:
    return len(head) >= 2 and head[0] == 0xFF and (head[1] & 0xF6) in {0xF0, 0xF2}


def _issue(path: str, code: str, message: str) -> ArtifactValidationIssue:
    return ArtifactValidationIssue(path=path, code=code, message=message)


__all__ = [
    "ArtifactValidationIssue",
    "ArtifactValidationResult",
    "validate_artifact_paths",
]

"""LangGraph planning for requested attachment formats."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import inspect
import json
import logging
import os
import re
import time
from typing import TypedDict
from urllib.parse import urlparse

from langgraph.graph import END, START, StateGraph


ATTACHMENT_TOKEN_EXTENSIONS: dict[str, str] = {
    "aac": ".aac",
    "avi": ".avi",
    "csv": ".csv",
    "doc": ".docx",
    "docx": ".docx",
    "flac": ".flac",
    "gif": ".gif",
    "htm": ".html",
    "html": ".html",
    "jpeg": ".jpg",
    "jpg": ".jpg",
    "json": ".json",
    "m4a": ".m4a",
    "m4v": ".m4v",
    "md": ".md",
    "mkv": ".mkv",
    "mov": ".mov",
    "mp3": ".mp3",
    "mp4": ".mp4",
    "mpeg": ".mpeg",
    "mpg": ".mpg",
    "oga": ".oga",
    "ogg": ".ogg",
    "opus": ".opus",
    "pdf": ".pdf",
    "png": ".png",
    "ppt": ".pptx",
    "pptx": ".pptx",
    "svg": ".svg",
    "txt": ".txt",
    "wav": ".wav",
    "weba": ".weba",
    "webp": ".webp",
    "webm": ".webm",
    "xls": ".xlsx",
    "xlsx": ".xlsx",
    "yaml": ".yaml",
    "yml": ".yaml",
}
VALID_ATTACHMENT_EXTENSIONS: tuple[str, ...] = tuple(
    sorted({*ATTACHMENT_TOKEN_EXTENSIONS.values(), ".htm", ".markdown", ".yml"})
)
EMBEDDED_MEDIA_ATTACHMENT_EXTENSIONS: tuple[str, ...] = (".docx", ".pdf", ".pptx", ".xlsx")
_GENERIC_EXTENSION_RE = re.compile(r"^\.[A-Za-z0-9]{1,16}$")
_DOMAIN_SUFFIX_EXTENSIONS = frozenset(
    {
        ".ai",
        ".app",
        ".biz",
        ".co",
        ".com",
        ".dev",
        ".edu",
        ".gov",
        ".io",
        ".net",
        ".org",
        ".us",
    }
)
_ATTACHMENT_FORMAT_MODEL_TIMEOUT_SECONDS_ENV = "NULLION_ATTACHMENT_FORMAT_MODEL_TIMEOUT_SECONDS"
_DEFAULT_ATTACHMENT_FORMAT_MODEL_TIMEOUT_SECONDS = 3.0
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AttachmentFormatPlan:
    extension: str | None = None
    evidence: str = "none"
    embedded_media_extensions: tuple[str, ...] = ()


class AttachmentFormatState(TypedDict, total=False):
    text: str
    allow_filename_tokens: bool
    extensions: list[str]
    plan: AttachmentFormatPlan


def _text_from_model_response(response: object) -> str:
    if not isinstance(response, dict):
        return ""
    parts: list[str] = []
    for block in response.get("content") or ():
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "\n".join(parts).strip()


def _structured_payload_from_model_response(response: object) -> dict[str, object] | None:
    if not isinstance(response, dict):
        return None
    for block in response.get("content") or ():
        if not isinstance(block, dict):
            continue
        if block.get("type") != "tool_use" or block.get("name") != "select_attachment_format":
            continue
        payload = block.get("input")
        if isinstance(payload, dict):
            return payload
    return None


def _parse_json_object(text: str) -> dict[str, object] | None:
    stripped = str(text or "").strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            parsed = json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


def _model_attachment_format_plan(text: str, model_client: object | None) -> AttachmentFormatPlan | None:
    create = getattr(model_client, "create", None)
    if create is None:
        return None
    timeout_seconds = _attachment_format_model_timeout_seconds()
    try:
        started_at = time.perf_counter()
        create_kwargs = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": str(text or ""),
                        }
                    ],
                }
            ],
            "tools": [
                {
                    "name": "select_attachment_format",
                    "description": "Return the required attachment file format for this user request.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "extension": {
                                "anyOf": [
                                    {"type": "string", "pattern": r"^\.[A-Za-z0-9]{1,16}$"},
                                    {"type": "null"},
                                ]
                            },
                            "confidence": {"type": "number"},
                            "embedded_media_extensions": {
                                "type": "array",
                                "description": (
                                    "Final artifact suffixes that must contain requested images, screenshots, "
                                    "generated visuals, or other media bytes inside the file itself. Use an empty "
                                    "array when media may be listed, linked, attached separately, or not requested."
                                ),
                                "items": {"type": "string", "enum": list(EMBEDDED_MEDIA_ATTACHMENT_EXTENSIONS)},
                            },
                        },
                        "required": ["extension", "embedded_media_extensions"],
                        "additionalProperties": False,
                    },
                }
            ],
            "max_tokens": 120,
            "system": (
                "Identify whether this user request specifies a required attachment file format. "
                "Call select_attachment_format with the result. "
                "extension must be a literal file extension such as .pdf, .html, or .blend, or null. "
                "embedded_media_extensions must include final artifact suffixes only when the requested output file "
                "itself must contain images, screenshots, generated visuals, or other embedded media bytes. "
                "Return an empty embedded_media_extensions array for ordinary text, table, memo, note, report, "
                "spreadsheet, PDF, HTML, or deck files unless the user specifically requires visual/media bytes "
                "inside that final file. "
                "When the required attachment is a browser/page screenshot image, use extension .png. "
                "Use null when no attachment file format is specified."
            ),
        }
        try:
            parameters = inspect.signature(create).parameters
        except (TypeError, ValueError):
            parameters = {}
        if timeout_seconds is not None and ("timeout" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )):
            create_kwargs["timeout"] = timeout_seconds
        response = create(**create_kwargs)
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        if elapsed_ms >= 1000:
            logger.info("attachment format model planner elapsed_ms=%.1f timeout_s=%s", elapsed_ms, timeout_seconds)
    except Exception as exc:
        logger.debug("Attachment format model planner failed: %s", exc)
        return None
    payload = _structured_payload_from_model_response(response) or _parse_json_object(_text_from_model_response(response))
    if payload is None:
        return None
    embedded_media_extensions = _validated_embedded_media_extensions(
        payload.get("embedded_media_extensions")
    )
    extension = payload.get("extension")
    if extension is None:
        return AttachmentFormatPlan(
            evidence="model_structured_output",
            embedded_media_extensions=embedded_media_extensions,
        )
    normalized = str(extension or "").strip().lower()
    if normalized and not normalized.startswith("."):
        normalized = f".{normalized}"
    if _GENERIC_EXTENSION_RE.fullmatch(normalized) is None:
        return None
    return AttachmentFormatPlan(
        extension=normalized,
        evidence="model_structured_output",
        embedded_media_extensions=embedded_media_extensions,
    )


def _validated_embedded_media_extensions(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    extensions: list[str] = []
    for raw_extension in value:
        extension = str(raw_extension or "").strip().lower()
        if extension and not extension.startswith("."):
            extension = f".{extension}"
        if extension in EMBEDDED_MEDIA_ATTACHMENT_EXTENSIONS and extension not in extensions:
            extensions.append(extension)
    return tuple(extensions)


def _attachment_format_model_timeout_seconds() -> float | None:
    raw = os.environ.get(_ATTACHMENT_FORMAT_MODEL_TIMEOUT_SECONDS_ENV)
    if raw is None:
        return _DEFAULT_ATTACHMENT_FORMAT_MODEL_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _DEFAULT_ATTACHMENT_FORMAT_MODEL_TIMEOUT_SECONDS
    return value if value > 0 else None


def is_domain_suffix_extension(extension: str | None) -> bool:
    normalized = str(extension or "").strip().lower()
    if normalized and not normalized.startswith("."):
        normalized = f".{normalized}"
    return normalized in _DOMAIN_SUFFIX_EXTENSIONS


def _token_around(text: str, start: int, end: int) -> str:
    left = start
    while left > 0 and not text[left - 1].isspace():
        left -= 1
    right = end
    while right < len(text) and not text[right].isspace():
        right += 1
    return text[left:right].strip("`'\"<>(),;:")


def _extension_candidate_priority(token: str) -> int:
    parts = tuple(part.lower() for part in re.split(r"[\\/]+", token) if part)
    if "artifacts" in parts:
        return 0
    if "files" in parts:
        return 2
    return 1


_STRUCTURED_ATTACHMENT_DESCRIPTOR_KEYS = frozenset({"artifact", "artifacts", "file", "filename", "output", "path"})
_SCREENSHOT_ATTACHMENT_RE = re.compile(r"\bscreenshots?\b", re.IGNORECASE)


def _is_structured_attachment_descriptor(token: str) -> bool:
    if "=" not in token:
        return False
    key = token.split("=", 1)[0].strip().lower()
    return key in _STRUCTURED_ATTACHMENT_DESCRIPTOR_KEYS


def has_unambiguous_attachment_extension_context(full_text: str, token: str) -> bool:
    token = str(token or "").strip("`'\"<>(),;:")
    if not token:
        return False
    if token.startswith("."):
        return True
    if "/" in token or "\\" in token or _is_structured_attachment_descriptor(token):
        return True
    parts = tuple(part.lower() for part in re.split(r"[\\/]+", token) if part)
    if "artifacts" in parts or "files" in parts:
        return True
    return str(full_text or "").strip("`'\"<>(),;:") == token


def _is_hidden_path_component_extension(token: str, raw_extension: str) -> bool:
    if "/" not in token and "\\" not in token:
        return False
    parts = tuple(part for part in re.split(r"[\\/]+", token) if part)
    if not parts:
        return False
    basename = parts[-1].split("=", 1)[-1]
    return basename.lower() == f".{raw_extension.lower()}"


def _extension_from_match(token: str, raw_extension: str, *, allow_filename_tokens: bool = False) -> str | None:
    if _is_hidden_path_component_extension(token, raw_extension):
        return None
    normalized = raw_extension.lower()
    mapped = ATTACHMENT_TOKEN_EXTENSIONS.get(normalized)
    parsed = urlparse(token.split("=", 1)[-1] if "://" in token else token)
    if parsed.scheme and parsed.netloc:
        if _is_hidden_path_component_extension(parsed.path, raw_extension):
            return None
        path_suffix = ""
        path_match = re.search(r"\.([A-Za-z0-9]{1,12})(?![\w/-])", parsed.path)
        if path_match is not None:
            path_suffix = f".{path_match.group(1).lower()}"
        if not path_suffix:
            return None
        if mapped is not None:
            return mapped if path_suffix == mapped else None
        extension = f".{normalized}"
        return extension if path_suffix == extension and _GENERIC_EXTENSION_RE.fullmatch(extension) else None
    if "://" in token:
        return None
    basename = re.split(r"[\\/]+", token)[-1].lower()
    if basename == f".{normalized}" and ("/" in token or "\\" in token or token.startswith("~")):
        return None
    has_path_evidence = (
        token.startswith(".")
        or "/" in token
        or "\\" in token
        or "artifacts" in tuple(part.lower() for part in re.split(r"[\\/]+", token) if part)
        or "files" in tuple(part.lower() for part in re.split(r"[\\/]+", token) if part)
    )
    if mapped is not None:
        return mapped
    extension = f".{normalized}"
    if _GENERIC_EXTENSION_RE.fullmatch(extension) is None:
        return None
    if is_domain_suffix_extension(extension):
        return None
    parts = tuple(part.lower() for part in re.split(r"[\\/]+", token) if part)
    has_path_evidence = token.startswith(".") or "/" in token or "\\" in token or "artifacts" in parts or "files" in parts
    return extension if has_path_evidence or allow_filename_tokens else None


def _explicit_extensions(text: str, *, allow_filename_tokens: bool = False) -> list[str]:
    candidates: list[tuple[int, int, str]] = []
    value = str(text or "")
    for index, match in enumerate(re.finditer(r"\.([A-Za-z0-9]{1,12})(?![\w/-])", value)):
        if match.start() > 0 and value[match.start() - 1] == ".":
            continue
        token = _token_around(value, match.start(), match.end())
        if not allow_filename_tokens and not has_unambiguous_attachment_extension_context(value, token):
            continue
        extension = _extension_from_match(
            token,
            match.group(1),
            allow_filename_tokens=allow_filename_tokens,
        )
        if extension is None:
            continue
        candidates.append((_extension_candidate_priority(token), index, extension))
    seen: list[str] = []
    for _priority, _index, extension in sorted(candidates):
        if extension not in seen:
            seen.append(extension)
    return seen


def _normalize_node(state: AttachmentFormatState) -> dict[str, object]:
    return {
        "extensions": _explicit_extensions(
            state.get("text") or "",
            allow_filename_tokens=bool(state.get("allow_filename_tokens")),
        )
    }


def _extension_token_node(state: AttachmentFormatState) -> dict[str, object]:
    extensions = state.get("extensions") or []
    if extensions:
        return {"plan": AttachmentFormatPlan(extension=extensions[0], evidence="literal_extension")}
    return {}


def _artifact_kind_node(state: AttachmentFormatState) -> dict[str, object]:
    if state.get("plan") is not None:
        return {}
    if _SCREENSHOT_ATTACHMENT_RE.search(state.get("text") or ""):
        return {"plan": AttachmentFormatPlan(extension=".png", evidence="artifact_kind")}
    return {}


def _default_node(state: AttachmentFormatState) -> dict[str, object]:
    if state.get("plan") is not None:
        return {}
    return {"plan": AttachmentFormatPlan()}


@lru_cache(maxsize=1)
def _compiled_attachment_format_graph():
    graph = StateGraph(AttachmentFormatState)
    graph.add_node("normalize", _normalize_node)
    graph.add_node("extension_token", _extension_token_node)
    graph.add_node("artifact_kind", _artifact_kind_node)
    graph.add_node("default", _default_node)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "extension_token")
    graph.add_edge("extension_token", "artifact_kind")
    graph.add_edge("artifact_kind", "default")
    graph.add_edge("default", END)
    return graph.compile()


def plan_attachment_format(
    text: str,
    *,
    model_client: object | None = None,
    include_media_requirements: bool = False,
    allow_filename_tokens: bool = False,
) -> AttachmentFormatPlan:
    final_state = _compiled_attachment_format_graph().invoke(
        {"text": text, "allow_filename_tokens": allow_filename_tokens},
        config={"configurable": {"thread_id": "attachment-format-plan"}},
    )
    plan = final_state.get("plan")
    if isinstance(plan, AttachmentFormatPlan) and plan.extension is not None and not include_media_requirements:
        return plan
    model_plan = _model_attachment_format_plan(text, model_client)
    if model_plan is not None and is_domain_suffix_extension(model_plan.extension):
        return AttachmentFormatPlan()
    if isinstance(plan, AttachmentFormatPlan) and plan.extension is not None and include_media_requirements:
        return AttachmentFormatPlan(
            extension=plan.extension,
            evidence=(
                "literal_extension+model_structured_output"
                if model_plan is not None
                else plan.evidence
            ),
            embedded_media_extensions=(
                model_plan.embedded_media_extensions if model_plan is not None else ()
            ),
        )
    if model_plan is not None:
        return model_plan
    return plan if isinstance(plan, AttachmentFormatPlan) else AttachmentFormatPlan()


__all__ = [
    "ATTACHMENT_TOKEN_EXTENSIONS",
    "EMBEDDED_MEDIA_ATTACHMENT_EXTENSIONS",
    "AttachmentFormatPlan",
    "AttachmentFormatState",
    "VALID_ATTACHMENT_EXTENSIONS",
    "has_unambiguous_attachment_extension_context",
    "is_domain_suffix_extension",
    "plan_attachment_format",
]

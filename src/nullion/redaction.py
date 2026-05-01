"""Shared sensitive-value redaction helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import is_dataclass, asdict
from typing import Any


REDACTION = "[redacted]"

_TOKEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\b(?:xoxb|xoxp|xoxa|xoxr)-[A-Za-z0-9-]{20,}\b"),
    re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"),
)

_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b("
    r"api[_-]?key|token|secret|password|passwd|pwd|authorization|bearer|access[_-]?token|"
    r"refresh[_-]?token|client[_-]?secret|private[_-]?key"
    r")\b(\s*[:=]\s*)(['\"]?)([^'\"\s,;]{8,})(\3)"
)

_HEADER_PATTERN = re.compile(r"(?i)\b(authorization\s*:\s*)(bearer|basic)\s+([A-Za-z0-9._~+/=-]{8,})")


def redact_text(text: str) -> str:
    """Replace likely secrets in free-form text with a stable placeholder."""
    redacted = str(text)
    for pattern in _TOKEN_PATTERNS:
        redacted = pattern.sub(REDACTION, redacted)
    redacted = _HEADER_PATTERN.sub(lambda m: f"{m.group(1)}{m.group(2)} {REDACTION}", redacted)
    redacted = _ASSIGNMENT_PATTERN.sub(lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}{REDACTION}{m.group(5)}", redacted)
    return redacted


def redact_value(value: Any) -> Any:
    """Recursively redact strings inside common JSON-like containers."""
    if isinstance(value, str):
        return redact_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return redact_value(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): redact_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(redact_value(item) for item in value)
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, set):
        return {redact_value(item) for item in value}
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [redact_value(item) for item in value]
    return value


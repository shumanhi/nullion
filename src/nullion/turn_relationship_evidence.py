"""Structured evidence gates for completed-turn relationship classification."""

from __future__ import annotations

from collections.abc import Iterable
import re

from nullion.attachment_format_graph import plan_attachment_format
from nullion.task_frames import extract_url_target


_FILENAME_OR_DOMAIN_EXTENSION_RE = re.compile(
    r"(?<![\w\[/])[\w.-]+\.[A-Za-z][A-Za-z0-9]{1,9}\b"
)


def has_structured_turn_relationship_evidence(
    text: str,
    *,
    attachments: Iterable[object] | None = None,
) -> bool:
    """Return true only for typed signals that can justify relationship routing."""
    if attachments:
        return True
    if extract_url_target(text) is not None:
        return True
    if plan_attachment_format(text).extension is not None:
        return True
    return bool(_FILENAME_OR_DOMAIN_EXTENSION_RE.search(str(text or "")))


__all__ = ["has_structured_turn_relationship_evidence"]

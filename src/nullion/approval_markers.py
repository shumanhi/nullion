"""Internal approval suspension markers.

These helpers only parse Nullion-authored control markers. They are not intent
classifiers for user or model prose.
"""

from __future__ import annotations

from dataclasses import dataclass


TOOL_APPROVAL_REQUESTED_MARKER = "Tool approval requested"


@dataclass(frozen=True, slots=True)
class ToolApprovalMarker:
    approval_id: str | None
    remainder: str | None = None


def split_tool_approval_marker(text: str | None) -> ToolApprovalMarker | None:
    if not isinstance(text, str):
        return None
    stripped = text.lstrip()
    if not stripped.startswith(TOOL_APPROVAL_REQUESTED_MARKER):
        return None

    first_line, separator, rest = stripped.partition("\n")
    marker_tail = first_line.removeprefix(TOOL_APPROVAL_REQUESTED_MARKER).strip()
    approval_id = None
    if marker_tail:
        if not marker_tail.startswith(":"):
            return None
        approval_id = marker_tail[1:].strip() or None

    remainder = rest.strip() if separator else ""
    return ToolApprovalMarker(approval_id=approval_id, remainder=remainder or None)


def strip_tool_approval_marker(text: str | None) -> str | None:
    marker = split_tool_approval_marker(text)
    if marker is None:
        return text
    return marker.remainder


def is_tool_approval_marker(text: str | None) -> bool:
    return split_tool_approval_marker(text) is not None


"""Shared task-card activity delivery policy for chat platforms."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import time

_PLANNER_COUNT_SUFFIX_RE = re.compile(r"\s*(?:[*•]|—|-)\s*\d+(?:\s+\S+)?\s*$", re.IGNORECASE)
_PLANNER_PREFIX_RE = re.compile(r"^\s*planner\s+", re.IGNORECASE)
_TOOL_STATUS_GLYPHS = {"✓", "→", "⊗", "⊘", "•"}
_RUNNING_SPINNER_GLYPHS: tuple[str, ...] = ("◑", "◒", "◐", "◓")


@dataclass(frozen=True, slots=True)
class PlatformActivityCapabilities:
    platform: str
    supports_live_stream: bool = False
    supports_message_edit: bool = False
    supports_task_card: bool = True
    min_update_interval_seconds: float = 1.5
    max_activity_items: int = 6


@dataclass(slots=True)
class PlatformTaskCardState:
    summary: str = ""
    activity: dict[str, str] = field(default_factory=dict)
    last_emit_at: float = 0.0
    spinner_frame: int = 0


class PlatformTaskCardStore:
    """Keep compact rendered task-card state per platform target/group."""

    def __init__(self, capabilities: PlatformActivityCapabilities) -> None:
        self._capabilities = capabilities
        self._state: dict[tuple[str, str], PlatformTaskCardState] = {}

    def update(
        self,
        *,
        target_id: str,
        group_id: str,
        status_kind: str,
        text: str,
        activity_id: str = "",
        activity_label: str = "",
        force: bool = False,
        include_activity: bool = False,
    ) -> str | None:
        target = str(target_id or "").strip()
        group = str(group_id or "").strip()
        if not target or not group:
            return None
        kind = str(status_kind or "task_summary").strip()
        body = str(text or "").strip()
        if not body:
            return None
        state = self._state.setdefault((target, group), PlatformTaskCardState())
        if kind == "task_summary":
            state.summary = body
            force = True
        elif kind == "tool_activity" and include_activity:
            key = str(activity_id or activity_label or len(state.activity)).strip()
            value = compact_tool_activity_text(body, label=activity_label)
            if value:
                state.activity[key] = value
                while len(state.activity) > self._capabilities.max_activity_items:
                    first_key = next(iter(state.activity))
                    state.activity.pop(first_key, None)
        else:
            return None
        if not state.summary:
            return None
        now = time.monotonic()
        if (
            not force
            and state.last_emit_at
            and now - state.last_emit_at < self._capabilities.min_update_interval_seconds
        ):
            return None
        state.last_emit_at = now
        summary_text, next_spinner_frame = _animate_running_status_summary(
            state.summary,
            spinner_frame=state.spinner_frame,
        )
        state.spinner_frame = next_spinner_frame
        return render_task_card_text(summary_text, state.activity.values() if include_activity else ())

    def clear(self, *, target_id: str, group_id: str) -> None:
        target = str(target_id or "").strip()
        group = str(group_id or "").strip()
        if target and group:
            self._state.pop((target, group), None)


def platform_activity_capabilities(platform: str) -> PlatformActivityCapabilities:
    normalized = str(platform or "").strip().lower()
    if normalized == "web":
        return PlatformActivityCapabilities(platform="web", supports_live_stream=True, supports_message_edit=True, min_update_interval_seconds=0.0)
    if normalized in {"telegram", "slack", "discord"}:
        return PlatformActivityCapabilities(platform=normalized, supports_message_edit=True)
    return PlatformActivityCapabilities(platform=normalized or "unknown", supports_message_edit=False, min_update_interval_seconds=8.0, max_activity_items=3)


def should_deliver_task_status(
    *,
    status_kind: str,
    planner_feed_enabled: bool,
    include_activity: bool,
) -> bool:
    if not planner_feed_enabled:
        return False
    kind = str(status_kind or "task_summary").strip()
    if kind == "task_summary":
        return True
    if kind == "tool_activity":
        return bool(include_activity)
    return False


def compact_tool_activity_text(text: str, *, label: str = "") -> str:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    if not lines:
        return ""
    activity_label = str(label or "").strip()
    while activity_label and lines:
        first_line = lines[0]
        if first_line == activity_label or f"{first_line} tools" == activity_label:
            lines = lines[1:]
            continue
        break
    if not lines:
        return activity_label
    deduped: list[str] = []
    seen: dict[str, int] = {}
    for line in lines:
        line = _compact_platform_activity_line(line)
        key = _platform_activity_line_identity(line)
        if key in seen:
            deduped[seen[key]] = line
            continue
        seen[key] = len(deduped)
        deduped.append(line)
    lines = deduped
    head = activity_label or lines[0]
    detail_lines = lines if activity_label else lines[1:]
    detail = "\n".join(f"  {line}" for line in detail_lines[:3])
    return "\n".join(part for part in (head, detail) if part)


def render_task_card_text(summary: str, activity_lines: object = ()) -> str:
    summary_text = _render_platform_planner_summary(str(summary or "").strip())
    activity = [str(line or "").strip() for line in activity_lines or () if str(line or "").strip()]
    if not activity:
        return summary_text
    return "\n\n".join([summary_text, _render_platform_activity_section(activity)])


def _animate_running_status_summary(summary: str, *, spinner_frame: int) -> tuple[str, int]:
    raw = str(summary or "")
    if not raw:
        return "", spinner_frame
    lines = raw.splitlines()
    glyph = _RUNNING_SPINNER_GLYPHS[spinner_frame % len(_RUNNING_SPINNER_GLYPHS)]
    changed = False
    animated: list[str] = []
    for line in lines:
        leading = len(line) - len(line.lstrip(" "))
        stripped = line[leading:]
        if len(stripped) >= 2 and stripped[0] in _RUNNING_SPINNER_GLYPHS and stripped[1] == " ":
            animated.append(f"{line[:leading]}{glyph}{stripped[1:]}")
            changed = True
        else:
            animated.append(line)
    if not changed:
        return raw, 0
    return "\n".join(animated), (spinner_frame + 1) % len(_RUNNING_SPINNER_GLYPHS)


def _compact_platform_activity_line(line: str) -> str:
    text = str(line or "").strip()
    if not text:
        return ""
    separator = " — " if " — " in text else " - " if " - " in text else ""
    if not separator:
        return text
    head, _, _detail = text.partition(separator)
    if head[:1] in {"✓", "→", "⊗", "⊘", "•"} or _looks_like_tool_name(head):
        return head.strip()
    return text


def _looks_like_tool_name(text: str) -> bool:
    name = str(text or "").strip()
    return bool(name) and all(ch.isalnum() or ch in {"_", ".", ":", "/"} for ch in name)


def _platform_activity_line_identity(line: str) -> str:
    text = str(line or "").strip()
    if text[:1] in _TOOL_STATUS_GLYPHS:
        tool_name = text[1:].strip()
        if tool_name:
            return f"tool:{tool_name.casefold()}"
    return text.casefold()


def _render_platform_activity_section(activity_lines: list[str]) -> str:
    rendered = ["ACTIVITY  LIVE"]
    for block in activity_lines:
        block_lines = [line.strip() for line in str(block or "").splitlines() if line.strip()]
        if not block_lines:
            continue
        rendered.append(f"→ {block_lines[0]}")
        rendered.extend(f"    {line}" for line in block_lines[1:])
    return "\n".join(rendered).strip()


def _render_platform_planner_summary(summary: str) -> str:
    lines = [line.rstrip() for line in str(summary or "").splitlines()]
    if not lines:
        return ""
    first = lines[0].strip()
    if first.startswith("Planner:"):
        label = _render_platform_planner_label(first.split(":", 1)[1])
        heading = "PLANNER"
        if label:
            heading = f"{heading}  {label}"
        rest = [line.rstrip(":") if line.lstrip().startswith(("→", "->")) else line for line in lines[1:]]
        return "\n".join([heading, *rest]).strip()
    if first.casefold() == "planner":
        return "\n".join(["PLANNER", *lines[1:]]).strip()
    return "\n".join(lines).strip()


def _render_platform_planner_label(label: str) -> str:
    compact = _PLANNER_COUNT_SUFFIX_RE.sub("", str(label or "").strip())
    compact = _PLANNER_PREFIX_RE.sub("", compact).strip()
    return compact.upper()


__all__ = [
    "PlatformActivityCapabilities",
    "PlatformTaskCardStore",
    "compact_tool_activity_text",
    "platform_activity_capabilities",
    "render_task_card_text",
    "should_deliver_task_status",
]

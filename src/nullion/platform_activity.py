"""Shared task-card activity delivery policy for chat platforms."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
import re
import time
import threading

_PLANNER_COUNT_SUFFIX_RE = re.compile(r"\s*(?:[*•]|—|-)\s*\d+(?:\s+\S+)?\s*$", re.IGNORECASE)
_PLANNER_PREFIX_RE = re.compile(r"^\s*planner\s+", re.IGNORECASE)
_PLANNER_HEADING_TEXT = "PLANNER"
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
    include_activity: bool = False
    planner_feed_enabled: bool = True
    terminal: bool = False


@dataclass(frozen=True, slots=True)
class PlatformTaskStatusUpdate:
    target_id: str
    group_id: str
    key: tuple[str, str]
    rendered_text: str | None


PlatformStatusMessageSender = Callable[[str, str], Awaitable[object | None]]
PlatformStatusMessageEditor = Callable[[object, str, str], Awaitable[bool]]
_status_publication_guard: ContextVar[Callable[[], bool] | None] = ContextVar(
    "status_publication_guard", default=None
)


def _status_publication_open() -> bool:
    guard = _status_publication_guard.get()
    return guard is None or guard()


@dataclass(slots=True)
class PlatformStatusMessageParts:
    """Task-owned native message handles, including an unacknowledged append.

    Only acknowledged handles may be edited or deleted. An uncertain append
    blocks later appends until the group is retired; a new progress tick is
    not proof that a previous non-idempotent send failed.
    """

    handles: list[object | None] = field(default_factory=list)
    payloads: list[object | None] = field(default_factory=list)
    uncertain_index: int | None = None
    complete: bool = False


def platform_status_message_confirmed(value: object) -> bool:
    """Whether every part of the last canonical card has a native receipt."""
    if isinstance(value, PlatformStatusMessageParts):
        return bool(value.complete and value.uncertain_index is None and value.handles
            and all(handle is not None for handle in value.handles))
    return bool(value)


def split_platform_status_text(text: str, *, limit: int) -> list[str]:
    """Reuse the Markdown-aware splitter with a conservative UTF-16 budget."""
    from nullion.messaging_adapters import split_reply_for_platform

    parts = []
    for part in split_reply_for_platform(text, limit=limit):
        if len(part.encode('utf-16-le')) // 2 <= limit:
            parts.append(part)
        else:
            # Astral characters occupy two UTF-16 units. Reuse the existing
            # splitter, including its balanced-code-fence handling.
            parts.extend(split_reply_for_platform(part, limit=limit // 2))
    return parts


async def deliver_platform_status_parts(*, messages, key, parts, send_part, edit_part, delete_part,
        replay_safety=None):
    """Publish transport-split canonical content without losing owned handles.

    Call under the card's existing delivery lock. Splitters and SDK callbacks
    belong to adapters; this shared lifecycle never changes the card's content.
    ``None`` from send_part is an explicit rejection, not an unknown receipt.
    """
    from nullion.delivery_acknowledgement import UnconfirmedDelivery, send_error_replay_safety

    replay_safety = replay_safety or send_error_replay_safety
    if not _status_publication_open():
        return False
    parts = tuple(parts)
    previous = messages.get(key)
    state = previous if isinstance(previous, PlatformStatusMessageParts) else PlatformStatusMessageParts(
        handles=[] if previous is None else [previous],
        payloads=[] if previous is None else [None],
    )
    # Retain progress before awaiting any external write, including partial
    # success followed by an exception or owner cancellation.
    state.complete = False
    messages[key] = state
    def unknown_delivery(index):
        identities = [getattr(handle, 'message_id', getattr(handle, 'id', handle))
            for handle in state.handles if handle is not None]
        return UnconfirmedDelivery(uncertain=(f"status-part:{index + 1}",), message_ids=identities)
    if state.uncertain_index is not None:
        raise unknown_delivery(state.uncertain_index)
    for index, part in enumerate(parts):
        if not _status_publication_open():
            return False
        handle = state.handles[index] if index < len(state.handles) else None
        if handle is not None and state.payloads[index] == part:
            continue
        if handle is not None:
            try:
                edited = await edit_part(handle, part)
            except Exception as exc:
                if replay_safety(exc) != 'rejected':
                    raise
                edited = False
            if edited:
                state.payloads[index] = part
                continue
            # Do not replace an existing part while its old copy might remain.
            if not _status_publication_open():
                return False
            if not await delete_part(handle):
                return False
            state.handles[index] = None
            state.payloads[index] = None
        if not _status_publication_open():
            return False
        try:
            sent = await send_part(part)
        except BaseException as exc:
            if not isinstance(exc, Exception) or replay_safety(exc) == 'unknown':
                state.uncertain_index = index
                if isinstance(exc, Exception):
                    raise unknown_delivery(index) from exc
            raise
        if sent is None:
            return False
        if index < len(state.handles):
            state.handles[index] = sent
            state.payloads[index] = part
        else:
            state.handles.append(sent)
            state.payloads.append(part)
    # Delete only surplus acknowledged messages owned by this exact card.
    while len(state.handles) > len(parts):
        if not _status_publication_open():
            return False
        handle = state.handles[-1]
        if handle is not None and not await delete_part(handle):
            return False
        state.handles.pop()
        state.payloads.pop()
    if not _status_publication_open():
        return False
    state.complete = True
    if len(state.handles) == 1:
        messages[key] = state.handles[0]
    elif not state.handles:
        messages.pop(key, None)
    return True


class PlatformTaskCardStore:
    """Keep compact rendered task-card state per platform target/group."""

    def __init__(self, capabilities: PlatformActivityCapabilities) -> None:
        self._capabilities = capabilities
        self._state: dict[tuple[str, str], PlatformTaskCardState] = {}
        self._lock = threading.RLock()
        self._pending: dict[tuple[str, str], asyncio.Task] = {}
        self._closed: dict[tuple[str, str], None] = {}

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
        planner_feed_enabled: bool = True,
        terminal: bool = False,
    ) -> str | None:
        with self._lock:
            return self._update(target_id=target_id, group_id=group_id, status_kind=status_kind,
                text=text, activity_id=activity_id, activity_label=activity_label, force=force,
                include_activity=include_activity, planner_feed_enabled=planner_feed_enabled, terminal=terminal)

    def _update(self, *, target_id, group_id, status_kind, text, activity_id, activity_label,
                force, include_activity, planner_feed_enabled, terminal):
        target = str(target_id or "").strip()
        group = str(group_id or "").strip()
        if not target or not group:
            return None
        if (target, group) in self._closed:
            return None
        kind = str(status_kind or "task_summary").strip()
        body = str(text or "").strip()
        if not body:
            return None
        state = self._state.setdefault((target, group), PlatformTaskCardState())
        if state.terminal and not terminal:
            return None
        state.include_activity = include_activity
        state.planner_feed_enabled = planner_feed_enabled
        state.terminal = terminal or state.terminal
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
        if not state.summary and planner_feed_enabled:
            return None
        now = time.monotonic()
        if (
            not force
            and state.last_emit_at
            and now - state.last_emit_at < self._capabilities.min_update_interval_seconds
        ):
            return None
        return self._render(state, now)

    def _render(self, state, now):
        state.last_emit_at = now
        summary_text, next_spinner_frame = _animate_running_status_summary(
            state.summary,
            spinner_frame=state.spinner_frame,
        )
        state.spinner_frame = next_spinner_frame
        return render_task_card_text(summary_text if state.planner_feed_enabled else "",
            state.activity.values() if state.include_activity else ())

    def render_current(self, *, target_id, group_id):
        with self._lock:
            state = self._state.get((target_id, group_id))
            return self._render(state, time.monotonic()) if state else None

    def publication_guard(self, *, target_id, group_id):
        """Tie in-flight writes to this exact, still-open typed card state."""
        key = (target_id, group_id)
        with self._lock:
            state = self._state.get(key)
        def is_open():
            with self._lock:
                return state is not None and self._state.get(key) is state and key not in self._closed
        return is_open

    def defer(self, *, target_id, group_id, publish):
        key = (target_id, group_id)
        with self._lock:
            state = self._state.get(key)
            if state is None or state.terminal or key in self._pending:
                return
            delay = max(0, state.last_emit_at + self._capabilities.min_update_interval_seconds - time.monotonic())
            async def flush():
                try:
                    await asyncio.sleep(delay)
                    with self._lock:
                        if self._pending.get(key) is asyncio.current_task():
                            self._pending.pop(key, None)
                    await publish()
                finally:
                    with self._lock:
                        if self._pending.get(key) is asyncio.current_task():
                            self._pending.pop(key, None)
            self._pending[key] = asyncio.create_task(flush())

    def cancel_pending(self, *, target_id, group_id):
        with self._lock:
            task = self._pending.pop((target_id, group_id), None)
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        if task is not None and task is not current:
            task.get_loop().call_soon_threadsafe(task.cancel)

    def clear(self, *, target_id: str, group_id: str) -> None:
        target = str(target_id or "").strip()
        group = str(group_id or "").strip()
        if target and group:
            self.cancel_pending(target_id=target, group_id=group)
            with self._lock:
                self._state.pop((target, group), None)
                self._closed[(target, group)] = None
                while len(self._closed) > 4096:
                    self._closed.pop(next(iter(self._closed)))


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
    kind = str(status_kind or "task_summary").strip()
    if kind == "task_summary":
        return bool(planner_feed_enabled)
    if kind == "tool_activity":
        return bool(include_activity)
    return False


def prepare_platform_task_status_update(
    *,
    target_id: str,
    group_id: str,
    text: str,
    status_kind: str,
    activity_id: str,
    activity_label: str,
    task_card_store: PlatformTaskCardStore,
    planner_feed_enabled: bool,
    include_activity: bool,
    terminal: bool = False,
) -> PlatformTaskStatusUpdate | None:
    target = str(target_id or "").strip()
    group = str(group_id or "").strip()
    if (
        not target
        or not group
        or not should_deliver_task_status(
            status_kind=status_kind,
            planner_feed_enabled=planner_feed_enabled,
            include_activity=include_activity,
        )
    ):
        return None
    rendered_status = task_card_store.update(
        target_id=target,
        group_id=group,
        status_kind=status_kind,
        text=text,
        activity_id=activity_id,
        activity_label=activity_label,
        include_activity=include_activity,
        planner_feed_enabled=planner_feed_enabled,
        terminal=terminal,
    )
    return PlatformTaskStatusUpdate(
        target_id=target,
        group_id=group,
        key=(target, group),
        rendered_text=rendered_status,
    )


async def deliver_platform_task_status_update(
    *,
    target_id: str,
    group_id: str,
    text: str,
    status_kind: str,
    activity_id: str,
    activity_label: str,
    task_card_store: PlatformTaskCardStore,
    status_messages: dict[tuple[str, str], object],
    status_locks: dict[tuple[str, str], asyncio.Lock],
    planner_feed_enabled: bool,
    include_activity: bool,
    send_new: PlatformStatusMessageSender,
    edit_existing: PlatformStatusMessageEditor,
    terminal: bool = False,
) -> bool:
    """Prepare, de-dupe, edit, or send a platform-neutral task status card."""
    key = (str(target_id or '').strip(), str(group_id or '').strip())
    lock = status_locks.setdefault(key, asyncio.Lock())
    async def publish(rendered_text):
        guard = task_card_store.publication_guard(target_id=key[0], group_id=key[1])
        token = _status_publication_guard.set(guard)
        try:
            if not guard():
                return False
            existing = status_messages.get(key)
            if existing is not None and await edit_existing(existing, key[0], rendered_text):
                return True
            if not guard():
                return False
            sent = await send_new(key[0], rendered_text)
            if sent is None:
                return False
            status_messages[key] = sent
            return True
        finally:
            _status_publication_guard.reset(token)
    async def flush_current():
        async with lock:
            rendered = task_card_store.render_current(target_id=key[0], group_id=key[1])
            if rendered:
                await publish(rendered)
    async with lock:
        update = prepare_platform_task_status_update(target_id=target_id, group_id=group_id,
            status_kind=status_kind, text=text, activity_id=activity_id, activity_label=activity_label,
            task_card_store=task_card_store, planner_feed_enabled=planner_feed_enabled,
            include_activity=include_activity, terminal=terminal)
        if update is None:
            return False
        if not update.rendered_text:
            task_card_store.defer(target_id=key[0], group_id=key[1], publish=flush_current)
            return True
        task_card_store.cancel_pending(target_id=key[0], group_id=key[1])
        return await publish(update.rendered_text)


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
    return "\n\n".join(part for part in [summary_text, _render_platform_activity_section(activity)] if part)


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
    rendered = ["ACTIVITY LIVE"]
    for block in activity_lines:
        block_lines = [line.strip() for line in str(block or "").splitlines() if line.strip()]
        if not block_lines:
            continue
        rendered.extend(_render_platform_activity_line(line) for line in block_lines)
    return "\n".join(rendered).strip()


def _render_platform_activity_line(line: str) -> str:
    text = str(line or "").strip()
    if not text:
        return ""
    if text[:1] in _TOOL_STATUS_GLYPHS:
        return text
    return text


def _render_platform_planner_summary(summary: str) -> str:
    lines = [line.rstrip() for line in str(summary or "").splitlines()]
    if not lines:
        return ""
    first = lines[0].strip()
    if first.startswith("Planner:"):
        label = _render_platform_planner_label(first.split(":", 1)[1])
        heading = _format_platform_planner_heading(label)
        rest = [line.rstrip(":") if line.lstrip().startswith(("→", "->")) else line for line in lines[1:]]
        return "\n".join([heading, "", *rest]).strip()
    if _line_is_platform_planner_heading(first):
        heading = _normalize_platform_planner_heading(first)
        rest = [line.rstrip(":") if line.lstrip().startswith(("→", "->")) else line for line in lines[1:]]
        while rest and not rest[0].strip():
            rest.pop(0)
        return "\n".join([heading, "", *rest]).strip()
    if first.casefold() == "planner":
        return "\n".join([_format_platform_planner_heading(""), "", *lines[1:]]).strip()
    return "\n".join(lines).strip()


def _format_platform_planner_heading(label: str) -> str:
    compact = str(label or "").strip()
    if compact:
        return f"🎯 **{_PLANNER_HEADING_TEXT}  {compact}**"
    return f"🎯 **{_PLANNER_HEADING_TEXT}**"


def _line_is_platform_planner_heading(line: str) -> bool:
    value = str(line or "").strip()
    if value.startswith("🎯"):
        value = value[len("🎯") :].strip()
    value = re.sub(r"[*_]+", "", value).strip()
    return value.casefold().startswith("planner")


def _normalize_platform_planner_heading(line: str) -> str:
    value = str(line or "").strip()
    if value.startswith("🎯"):
        value = value[len("🎯") :].strip()
    value = re.sub(r"[*_]+", "", value).strip()
    value = re.sub(r"\s+", " ", value).strip()
    compact = _PLANNER_PREFIX_RE.sub("", value).strip()
    label = _render_platform_planner_label(compact)
    return _format_platform_planner_heading(label)


def _render_platform_planner_label(label: str) -> str:
    compact = _PLANNER_COUNT_SUFFIX_RE.sub("", str(label or "").strip())
    compact = _PLANNER_PREFIX_RE.sub("", compact).strip()
    return compact.upper()


__all__ = [
    "PlatformActivityCapabilities",
    "PlatformTaskCardStore",
    "PlatformTaskStatusUpdate",
    "compact_tool_activity_text",
    "deliver_platform_task_status_update",
    "platform_activity_capabilities",
    "prepare_platform_task_status_update",
    "render_task_card_text",
    "should_deliver_task_status",
]

"""Shared latency phase instrumentation for user-visible turns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
import os
import time
from typing import Callable
from uuid import uuid4

ActivityCallback = Callable[[dict[str, str]], None]

PHASE_CHECK_ATTACHMENTS = ("phase-check-attachments", "Checking attachments")
PHASE_CHECK_TASK_STATE = ("phase-check-task-state", "Checking active task state")
PHASE_SELECT_TOOLS = ("phase-select-tools", "Selecting tools")
PHASE_BUILD_CONTEXT = ("phase-build-context", "Building context")
PHASE_START_MODEL = ("phase-start-model", "Starting model")
PHASE_RUN_TOOLS = ("phase-run-tools", "Running tools")
PHASE_PREPARE_ARTIFACTS = ("phase-prepare-artifacts", "Preparing artifacts")
PHASE_SAVE_CONVERSATION = ("phase-save-conversation", "Saving conversation")


def _threshold_ms(env_name: str, default: float) -> float:
    raw = os.environ.get(env_name, "")
    if raw.strip():
        try:
            return max(float(raw), 0.0)
        except ValueError:
            pass
    return default


@dataclass
class PhaseActivityTracker:
    """Emit compact phase activity and collect phase timing without prompt data."""

    activity_callback: ActivityCallback | None = None
    surface: str = "unknown"
    conversation_id: str | None = None
    turn_id: str | None = None
    logger: logging.Logger | None = None

    def __post_init__(self) -> None:
        self._started_at = time.perf_counter()
        self._last_at = self._started_at
        self._phases: list[dict[str, object]] = []

    def emit(
        self,
        phase: tuple[str, str],
        status: str = "running",
        detail: str | None = None,
    ) -> None:
        callback = self.activity_callback
        if callback is None:
            return
        phase_id, label = phase
        event = {"id": phase_id, "label": label, "status": status}
        if detail:
            event["detail"] = detail[:140]
        try:
            callback(event)
        except Exception:
            if self.logger is not None:
                self.logger.debug("Unable to emit latency phase activity", exc_info=True)

    def mark(self, phase_name: str) -> None:
        now = time.perf_counter()
        self._phases.append({
            "phase": phase_name,
            "ms": round((now - self._last_at) * 1000, 1),
        })
        self._last_at = now

    def done(self, phase: tuple[str, str], phase_name: str | None = None, detail: str | None = None) -> None:
        self.emit(phase, "done", detail)
        self.mark(phase_name or phase[1])

    def fail(self, phase: tuple[str, str], phase_name: str | None = None, detail: str | None = None) -> None:
        self.emit(phase, "failed", detail)
        self.mark(phase_name or phase[1])

    @property
    def phases(self) -> list[dict[str, object]]:
        return list(self._phases)

    def phases_json(self) -> str:
        return json.dumps(self._phases, separators=(",", ":"))

    def total_ms(self) -> float:
        return (time.perf_counter() - self._started_at) * 1000

    def log_summary(
        self,
        *,
        outcome: str,
        tool_count: int = 0,
        artifact_count: int = 0,
        level: int = logging.INFO,
    ) -> None:
        if self.logger is None:
            return
        self.logger.log(
            level,
            "turn phase timing surface=%s conversation_id=%s turn_id=%s outcome=%s tools=%s artifacts=%s total_ms=%.1f phases=%s",
            self.surface,
            self.conversation_id,
            self.turn_id,
            outcome,
            tool_count,
            artifact_count,
            self.total_ms(),
            self.phases_json(),
        )


@dataclass
class TurnLatencyRecorder:
    """Persist compact first-visible turn timings outside user memory."""

    surface: str
    conversation_id: str | None = None
    turn_id: str | None = None
    feed_visible: bool | None = None
    logger: logging.Logger | None = None

    def __post_init__(self) -> None:
        self._started_at = time.perf_counter()
        self._marks: list[dict[str, object]] = []
        self._seen: set[str] = set()
        self._metadata: dict[str, object] = {}

    def mark(self, name: str, *, once: bool = False, detail: object | None = None) -> None:
        if once and name in self._seen:
            return
        self._seen.add(name)
        event: dict[str, object] = {
            "name": name,
            "ms": round((time.perf_counter() - self._started_at) * 1000, 1),
        }
        if detail is not None:
            event["detail"] = detail
        self._marks.append(event)

    def set(self, **metadata: object) -> None:
        self._metadata.update({key: value for key, value in metadata.items() if value is not None})

    def finish(
        self,
        store: object | None,
        *,
        outcome: str,
        tool_count: int = 0,
        scoped_tool_count: int | None = None,
        artifact_count: int = 0,
        cache_hit: bool | None = None,
        phase_timings: list[dict[str, object]] | None = None,
    ) -> None:
        add_conversation_event = getattr(store, "add_conversation_event", None)
        if not callable(add_conversation_event):
            return
        total_ms = round((time.perf_counter() - self._started_at) * 1000, 1)
        payload = {
            "event_id": f"latency-timing:{self.conversation_id or self.surface}:{uuid4().hex}",
            "conversation_id": str(self.conversation_id or ""),
            "event_type": "conversation.latency_timing",
            "created_at": datetime.now(UTC).isoformat(),
            "surface": self.surface,
            "turn_id": self.turn_id,
            "feed_visible": self.feed_visible,
            "outcome": outcome,
            "tool_count": int(tool_count or 0),
            "scoped_tool_count": int(scoped_tool_count if scoped_tool_count is not None else tool_count or 0),
            "artifact_count": int(artifact_count or 0),
            "cache_hit": cache_hit,
            "total_ms": total_ms,
            "marks": list(self._marks),
            **self._metadata,
        }
        if phase_timings:
            payload["phase_timings"] = list(phase_timings)
        if self.logger is not None:
            save_ms = None
            save_start = None
            save_done = None
            for mark in self._marks:
                if mark.get("name") == "save_start" and isinstance(mark.get("ms"), (int, float)):
                    save_start = float(mark["ms"])
                elif mark.get("name") == "save_done" and isinstance(mark.get("ms"), (int, float)):
                    save_done = float(mark["ms"])
            if save_start is not None and save_done is not None:
                save_ms = save_done - save_start
            if save_ms is None and phase_timings:
                save_ms = sum(
                    float(item.get("ms") or 0.0)
                    for item in phase_timings
                    if isinstance(item, dict) and item.get("phase") == "save"
                )
            if save_ms is not None and save_ms >= _threshold_ms("NULLION_SLOW_SAVE_LOG_MS", 750.0):
                self.logger.warning(
                    "turn slow save surface=%s conversation_id=%s turn_id=%s save_ms=%.1f outcome=%s",
                    self.surface,
                    self.conversation_id,
                    self.turn_id,
                    save_ms,
                    outcome,
                )
        try:
            add_conversation_event(payload)
        except Exception:
            if self.logger is not None:
                self.logger.debug("Turn latency timing event recording failed", exc_info=True)


def record_surface_latency_timing(
    store: object | None,
    *,
    surface: str,
    conversation_id: str | None = None,
    turn_id: str | None = None,
    outcome: str,
    total_ms: float,
    phases: list[str] | tuple[str, ...] | None = None,
    message_id: str | None = None,
    request_id: str | None = None,
    feed_visible: bool | None = None,
    logger: logging.Logger | None = None,
    **metadata: object,
) -> None:
    """Persist adapter-level timing in the shared conversation latency stream."""
    add_conversation_event = getattr(store, "add_conversation_event", None)
    if not callable(add_conversation_event):
        return
    marks: list[dict[str, object]] = []
    elapsed = 0.0
    for raw in phases or ():
        label, separator, duration = str(raw).partition(":")
        mark: dict[str, object] = {"name": label or str(raw)}
        if separator and duration.endswith("ms"):
            try:
                delta = float(duration[:-2])
                elapsed += delta
                mark["delta_ms"] = round(delta, 1)
                mark["ms"] = round(elapsed, 1)
            except ValueError:
                mark["detail"] = duration
        marks.append(mark)
    payload = {
        "event_id": f"latency-timing:{conversation_id or surface}:{uuid4().hex}",
        "conversation_id": str(conversation_id or ""),
        "event_type": "conversation.latency_timing",
        "created_at": datetime.now(UTC).isoformat(),
        "surface": surface,
        "turn_id": turn_id,
        "message_id": message_id,
        "request_id": request_id,
        "feed_visible": feed_visible,
        "outcome": outcome,
        "total_ms": round(float(total_ms or 0.0), 1),
        "marks": marks,
        **{key: value for key, value in metadata.items() if value is not None},
    }
    if logger is not None:
        delivery_ms = 0.0
        for mark in marks:
            name = str(mark.get("name") or "")
            if "delivery" in name and isinstance(mark.get("delta_ms"), (int, float)):
                delivery_ms += float(mark["delta_ms"])
        if delivery_ms >= _threshold_ms("NULLION_SLOW_DELIVERY_LOG_MS", 1000.0):
            logger.warning(
                "surface slow delivery surface=%s conversation_id=%s turn_id=%s delivery_ms=%.1f outcome=%s",
                surface,
                conversation_id,
                turn_id,
                delivery_ms,
                outcome,
            )
    try:
        add_conversation_event(payload)
    except Exception:
        if logger is not None:
            logger.debug("Surface latency timing event recording failed", exc_info=True)

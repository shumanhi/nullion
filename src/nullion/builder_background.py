"""Bounded in-process background execution for Builder reflection."""

from __future__ import annotations

from collections.abc import Callable
import logging
import os
import queue
import threading

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_QUEUE: queue.Queue[tuple[str, Callable[[], None]]] | None = None
_WORKERS_STARTED = 0
_SLOTS: threading.BoundedSemaphore | None = None


def _env_int(name: str, default: int, *, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(int(raw), minimum)
    except ValueError:
        return default


def _max_workers() -> int:
    return _env_int("NULLION_BUILDER_BACKGROUND_WORKERS", 1, minimum=1)


def _pending_limit() -> int:
    return _env_int("NULLION_BUILDER_BACKGROUND_PENDING_LIMIT", 2, minimum=1)


def _worker_loop() -> None:
    while True:
        task_queue = _QUEUE
        if task_queue is None:
            return
        label, worker = task_queue.get()
        try:
            worker()
        except Exception:
            logger.debug("Builder background task failed: %s", label, exc_info=True)
        finally:
            slots = _SLOTS
            if slots is not None:
                slots.release()
            task_queue.task_done()


def _ensure_workers() -> None:
    global _QUEUE, _SLOTS, _WORKERS_STARTED
    if _QUEUE is None:
        _QUEUE = queue.Queue()
    if _SLOTS is None:
        _SLOTS = threading.BoundedSemaphore(_pending_limit())
    while _WORKERS_STARTED < _max_workers():
        _WORKERS_STARTED += 1
        thread = threading.Thread(
            target=_worker_loop,
            name=f"nullion-builder-{_WORKERS_STARTED}",
            daemon=True,
        )
        thread.start()


def _acquire_slot() -> bool:
    with _LOCK:
        _ensure_workers()
        slots = _SLOTS
        if slots is None:
            return False
        return slots.acquire(blocking=False)


def _release_slot() -> None:
    slots = _SLOTS
    if slots is not None:
        try:
            slots.release()
        except ValueError:
            pass


def schedule_builder_background_task(label: str, worker: Callable[[], None]) -> bool:
    """Submit non-user-visible Builder work without blocking the reply path."""

    normalized_label = str(label or "builder").strip() or "builder"
    if not _acquire_slot():
        logger.info("Builder background queue full; skipping task label=%s", normalized_label)
        return False
    task_queue = _QUEUE
    if task_queue is None:
        _release_slot()
        return False
    try:
        task_queue.put_nowait((normalized_label, worker))
    except Exception:
        _release_slot()
        logger.debug("Unable to queue Builder background task: %s", normalized_label, exc_info=True)
        return False
    return True


__all__ = ["schedule_builder_background_task"]

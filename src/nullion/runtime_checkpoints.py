"""Shared coalesced runtime snapshots after incremental request receipts.

Primary actions and approvals retain their synchronous native commits. This
coordinator never performs tools or changes delivery ownership.
"""
from __future__ import annotations
import inspect
import logging
import threading
import time

logger = logging.getLogger(__name__)
_CHECKPOINT_LOCK = threading.RLock()
_CHECKPOINT_INFLIGHT: dict[int, tuple[object, threading.Event]] = {}
_CHECKPOINT_DIRTY: set[int] = set()
_CHECKPOINT_FORCE: set[int] = set()

def schedule_runtime_checkpoint(runtime, *, force: bool = False) -> None:
    if runtime is None:
        return
    runtime_id = id(runtime)

    with _CHECKPOINT_LOCK:
        if force:
            _CHECKPOINT_FORCE.add(runtime_id)
        if runtime_id in _CHECKPOINT_INFLIGHT:
            _CHECKPOINT_DIRTY.add(runtime_id)
            return
        owner = (object(), threading.Event())
        _CHECKPOINT_INFLIGHT[runtime_id] = owner

    def _retire() -> None:
        # Caller holds the lock. A new writer may already own this runtime
        # when the previous thread reaches its finally block.
        if _CHECKPOINT_INFLIGHT.get(runtime_id) is owner:
            _CHECKPOINT_INFLIGHT.pop(runtime_id, None)
            _CHECKPOINT_DIRTY.discard(runtime_id)
            _CHECKPOINT_FORCE.discard(runtime_id)
            owner[1].set()

    def _run() -> None:
        try:
            while True:
                with _CHECKPOINT_LOCK:
                    requested_force = runtime_id in _CHECKPOINT_FORCE
                    _CHECKPOINT_FORCE.discard(runtime_id)
                    # Requests already queued are covered by this pass. Only
                    # requests arriving during the save need another pass.
                    _CHECKPOINT_DIRTY.discard(runtime_id)
                try:
                    checkpoint_kwargs: dict[str, object] = {}
                    if requested_force:
                        try:
                            checkpoint_signature = inspect.signature(runtime.checkpoint)
                            if "force" in checkpoint_signature.parameters or any(
                                parameter.kind is inspect.Parameter.VAR_KEYWORD
                                for parameter in checkpoint_signature.parameters.values()
                            ):
                                checkpoint_kwargs["force"] = True
                        except (TypeError, ValueError):
                            checkpoint_kwargs["force"] = True
                    runtime.checkpoint(**checkpoint_kwargs)
                except Exception:
                    logger.debug("Unable to run runtime checkpoint", exc_info=True)
                with _CHECKPOINT_LOCK:
                    if runtime_id in _CHECKPOINT_DIRTY:
                        _CHECKPOINT_DIRTY.remove(runtime_id)
                        continue
                    _retire()
                    return
        finally:
            with _CHECKPOINT_LOCK:
                _retire()

    try:
        threading.Thread(target=_run, daemon=True, name='runtime-checkpoint').start()
    except BaseException:
        with _CHECKPOINT_LOCK:
            _retire()
        raise


def wait_runtime_checkpoints(runtime, *, timeout=5.0):
    """Bound graceful shutdown without moving snapshot waits into chat turns."""
    deadline = time.monotonic() + timeout
    while True:
        with _CHECKPOINT_LOCK:
            owner = _CHECKPOINT_INFLIGHT.get(id(runtime))
        if owner is None:
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not owner[1].wait(remaining):
            return False


def checkpoint_request_completion(runtime, *, conversation_id=None):
    """Persist the current conversation incrementally; coalesce full snapshots."""
    from nullion.runtime import PersistentRuntime
    if not isinstance(runtime, PersistentRuntime):
        return runtime.checkpoint()
    from nullion.runtime_receipts import checkpoint_runtime_receipts
    checkpoint_runtime_receipts(runtime, conversation_id=conversation_id)
    schedule_runtime_checkpoint(runtime)
    return runtime.checkpoint_path

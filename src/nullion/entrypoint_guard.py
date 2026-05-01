"""Shared guards for user-facing console entrypoints."""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import sys
import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")
_NULLION_HOME_ENV = "NULLION_HOME"
_SINGLE_INSTANCE_WAIT_ENV = "NULLION_SINGLE_INSTANCE_WAIT_SECONDS"
_SINGLE_INSTANCE_DISABLED_ENV = "NULLION_DISABLE_SINGLE_INSTANCE_GUARD"


def _truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _nullion_home() -> Path:
    configured = os.environ.get(_NULLION_HOME_ENV)
    if configured and configured.strip():
        return Path(configured).expanduser()
    return Path.home() / ".nullion"


def _single_instance_wait_seconds(default: float) -> float:
    raw = os.environ.get(_SINGLE_INSTANCE_WAIT_ENV)
    if raw is None:
        return max(default, 0.0)
    try:
        return max(float(raw), 0.0)
    except ValueError:
        return max(default, 0.0)


def _lock_key(value: str) -> str:
    normalized = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value.strip().lower())
    normalized = normalized.strip(".-")
    return normalized or "default"


class SingleInstanceLock:
    """Advisory process lock backed by a small file in ``~/.nullion/locks``."""

    def __init__(self, name: str, *, lock_dir: str | Path | None = None, wait_seconds: float = 0.0) -> None:
        self.name = _lock_key(name)
        self.lock_dir = Path(lock_dir).expanduser() if lock_dir is not None else _nullion_home() / "locks"
        self.wait_seconds = max(wait_seconds, 0.0)
        self.path = self.lock_dir / f"{self.name}.lock"
        self._file = None

    def acquire(self) -> bool:
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        deadline = time.monotonic() + self.wait_seconds
        while True:
            if self._try_lock(handle):
                self._file = handle
                self._write_metadata()
                return True
            if time.monotonic() >= deadline:
                handle.close()
                return False
            time.sleep(0.1)

    def release(self) -> None:
        handle = self._file
        self._file = None
        if handle is None:
            return
        try:
            self._unlock(handle)
        finally:
            handle.close()

    def _try_lock(self, handle) -> bool:
        if os.name == "nt":
            import msvcrt

            try:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                return True
            except OSError:
                return False

        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def _unlock(self, handle) -> None:
        if os.name == "nt":
            import msvcrt

            try:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            return

        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass

    def _write_metadata(self) -> None:
        if self._file is None:
            return
        self._file.seek(0)
        self._file.truncate()
        self._file.write(f"pid={os.getpid()}\n")
        self._file.write(f"name={self.name}\n")
        self._file.flush()


@contextmanager
def single_instance(name: str, *, lock_dir: str | Path | None = None, wait_seconds: float = 0.0):
    """Yield ``True`` only when this process owns the named instance lock."""
    if _truthy_env(os.environ.get(_SINGLE_INSTANCE_DISABLED_ENV)):
        yield True
        return
    lock = SingleInstanceLock(
        name,
        lock_dir=lock_dir,
        wait_seconds=_single_instance_wait_seconds(wait_seconds),
    )
    acquired = lock.acquire()
    try:
        yield acquired
    finally:
        if acquired:
            lock.release()


def run_single_instance_entrypoint(
    name: str,
    func: Callable[[], T],
    *,
    lock_dir: str | Path | None = None,
    wait_seconds: float = 0.0,
    description: str | None = None,
) -> T | None:
    """Run a long-lived entrypoint only when no same-named instance is active."""
    with single_instance(name, lock_dir=lock_dir, wait_seconds=wait_seconds) as acquired:
        if not acquired:
            label = description or name
            print(f"{label} is already running; not starting another instance.", file=sys.stderr)
            return None
        return func()


def run_user_facing_entrypoint(func: Callable[[], T]) -> T:
    """Turn an intentional terminal cancel into a clean process exit."""
    try:
        return func()
    except KeyboardInterrupt:
        print("\n  Cancelled.", file=sys.stderr)
        raise SystemExit(130) from None

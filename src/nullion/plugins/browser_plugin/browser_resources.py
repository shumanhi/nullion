"""Bounded retention for adapter-owned scratch and source evidence only."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from portalocker import LOCK_EX, LOCK_NB, LOCK_SH, Lock
from portalocker.exceptions import LockException

_MARKER = ".nullion-browser-resource.json"
_MAX_AGE = 24 * 60 * 60


def browser_temporary_root() -> Path:
    from nullion.plugins.browser_plugin.browser_config import nullion_runtime_home
    # Chromium creates Unix-domain sockets beneath TMPDIR. Deep workspace paths
    # can exceed the OS socket limit even when ordinary file paths are valid.
    base = Path("/tmp") if os.name == "posix" else Path(tempfile.gettempdir())
    namespace = hashlib.sha256(str(nullion_runtime_home().resolve()).encode()).hexdigest()[:16]
    root = base / f"nullion-browser-{namespace}"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or (hasattr(os, "getuid") and root.stat().st_uid != os.getuid()):
        raise PermissionError("Browser temporary directory is not owned by this runtime")
    root.chmod(0o700)
    return root


def expire_browser_resources(root: Path, *, now: float | None = None) -> None:
    """Never traverse global temp folders, persistent profiles or deliverables."""
    now = time.time() if now is None else now
    if not root.is_dir() or root.is_symlink():
        return
    for child in root.iterdir():
        if child.is_symlink() or not child.is_dir():
            continue
        marker = child / _MARKER
        lease = child / ".lease"
        try:
            if marker.is_symlink() or lease.is_symlink():
                continue
            metadata = json.loads(marker.read_text())
            if metadata.get("kind") != "browser_resource" or metadata.get("version") != 1:
                continue
            if now - float(metadata["created_at"]) < _MAX_AGE or not lease.is_file():
                continue
            # Live parent/worker processes hold shared leases. A stale timestamp
            # alone never authorizes removing an active browser's files.
            with Lock(str(lease), mode="r+", flags=LOCK_EX | LOCK_NB, timeout=0):
                # Directories are never reused; a completed resource cannot
                # acquire a new task lease. Close handles before Windows rename.
                retired = child.with_name("expired-" + child.name)
            child.rename(retired)
            shutil.rmtree(retired)
        except (OSError, ValueError, KeyError, TypeError, LockException):
            continue


@contextmanager
def browser_resource_directory(root: Path, *, retain: bool = False):
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink():
        raise ValueError("Browser resource root must not be a symlink")
    expire_browser_resources(root)
    path = Path(tempfile.mkdtemp(prefix="run-", dir=root))
    ready = False
    try:
        (path / _MARKER).write_text(json.dumps({"kind": "browser_resource", "version": 1, "created_at": time.time()}))
        lease = path / ".lease"
        lease.touch(mode=0o600)
        with Lock(str(lease), mode="r+", flags=LOCK_SH | LOCK_NB, timeout=0):
            yield path
            ready = True
    finally:
        if not retain or not ready:
            shutil.rmtree(path, ignore_errors=True)

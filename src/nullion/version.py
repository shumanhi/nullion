"""Runtime version helpers."""

from __future__ import annotations

import subprocess
from importlib import metadata
from pathlib import Path


def _source_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _package_metadata_version() -> str:
    try:
        return _normalize_version(metadata.version("nullion"))
    except metadata.PackageNotFoundError:
        return ""


def _normalize_version(version: str) -> str:
    normalized = str(version or "").strip()
    return normalized[1:] if normalized.startswith("v") else normalized


def _git_version() -> str:
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--exact-match", "--match", "v[0-9]*", "HEAD"],
            cwd=_source_root(),
            capture_output=True,
            text=True,
            timeout=1,
        )
    except Exception:
        return ""
    return _normalize_version(result.stdout) if result.returncode == 0 else ""


def current_version() -> str:
    """Return the best available app version for the checked-out runtime."""
    return _git_version() or _package_metadata_version() or "0.0.0"


def version_tag() -> str:
    version = _normalize_version(current_version())
    return version if version.startswith("v") else f"v{version}"


__version__ = current_version()

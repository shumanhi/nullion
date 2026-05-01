"""Per-workspace local storage helpers."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from nullion.connections import workspace_id_for_principal


@dataclass(frozen=True, slots=True)
class WorkspaceStorageRoots:
    workspace_id: str
    root: Path
    files: Path
    uploads: Path
    media: Path
    artifacts: Path

    def all_roots(self) -> tuple[Path, ...]:
        return (self.root, self.files, self.uploads, self.media, self.artifacts)


def workspace_storage_base() -> Path:
    raw_root = os.environ.get("NULLION_WORKSPACE_STORAGE_ROOT")
    if isinstance(raw_root, str) and raw_root.strip():
        return Path(raw_root).expanduser().resolve()
    data_dir = os.environ.get("NULLION_DATA_DIR")
    if isinstance(data_dir, str) and data_dir.strip():
        return (Path(data_dir).expanduser().resolve() / "workspaces").resolve()
    return (Path.home() / ".nullion" / "workspaces").resolve()


def sanitize_workspace_id(workspace_id: str | None) -> str:
    text = re.sub(r"[^a-zA-Z0-9_.:-]+", "_", str(workspace_id or "").strip()).strip("._-:")
    return text or "workspace_admin"


def workspace_storage_roots_for_workspace(workspace_id: str | None, *, create: bool = True) -> WorkspaceStorageRoots:
    clean_workspace_id = sanitize_workspace_id(workspace_id)
    root = workspace_storage_base() / clean_workspace_id
    roots = WorkspaceStorageRoots(
        workspace_id=clean_workspace_id,
        root=root,
        files=root / "files",
        uploads=root / "uploads",
        media=root / "media",
        artifacts=root / "artifacts",
    )
    if create:
        for path in roots.all_roots():
            path.mkdir(parents=True, exist_ok=True)
    return roots


def workspace_storage_roots_for_principal(principal_id: str | None, *, create: bool = True) -> WorkspaceStorageRoots:
    return workspace_storage_roots_for_workspace(workspace_id_for_principal(principal_id), create=create)


def workspace_file_roots_for_principal(principal_id: str | None, *, create: bool = True) -> tuple[Path, ...]:
    roots = workspace_storage_roots_for_principal(principal_id, create=create)
    return (roots.root,)


def format_workspace_storage_for_prompt(*, principal_id: str | None = None) -> str:
    roots = workspace_storage_roots_for_principal(principal_id)
    return (
        f"Workspace storage for this user is workspace={roots.workspace_id} at {roots.root}.\n"
        f"- Save ordinary user files under: {roots.files}\n"
        f"- Save uploaded/input files under: {roots.uploads}\n"
        f"- Save media scratch or generated media under: {roots.media}\n"
        f"- Save downloadable artifacts under: {roots.artifacts}"
    )


__all__ = [
    "WorkspaceStorageRoots",
    "format_workspace_storage_for_prompt",
    "sanitize_workspace_id",
    "workspace_file_roots_for_principal",
    "workspace_storage_base",
    "workspace_storage_roots_for_principal",
    "workspace_storage_roots_for_workspace",
]

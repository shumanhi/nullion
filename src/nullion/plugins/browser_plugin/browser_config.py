"""Shared browser runtime configuration."""
from __future__ import annotations

import os
from pathlib import Path


DEFAULT_AGENT_BROWSER_SESSION_ID = "agent-browser-default"


def nullion_runtime_home() -> Path:
    """Return the active Nullion lane home used for browser state."""
    configured_home = str(os.environ.get("NULLION_HOME") or "").strip()
    if configured_home:
        return Path(configured_home).expanduser()
    data_dir = str(os.environ.get("NULLION_DATA_DIR") or "").strip()
    if data_dir:
        return Path(data_dir).expanduser()
    checkpoint = str(
        os.environ.get("NULLION_CHECKPOINT_PATH")
        or os.environ.get("NULLION_CHECKPOINT")
        or ""
    ).strip()
    if checkpoint:
        return Path(checkpoint).expanduser().parent
    return Path.home() / ".nullion"


def browser_user_data_dir(browser_kind: str) -> Path:
    configured = str(os.environ.get("NULLION_BROWSER_USER_DATA_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    safe_kind = "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in browser_kind).strip("-")
    return nullion_runtime_home() / "browser-profiles" / (safe_kind or "chrome")

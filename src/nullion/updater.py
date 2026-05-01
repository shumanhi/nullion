"""Safe updater with automatic rollback.

Flow:
  1. fetch()     — git fetch, compare HEAD vs origin/main
  2. snapshot()  — save pip freeze + current git commit when an update exists
  3. apply()     — reset source to origin/main + pip install -e .
  4. health()    — import check + /api/status + messaging bootstrap probes
  5. rollback()  — restore pip state + git reset if health fails

Entry points:
  CLI:    nullion-cli update
  Web UI: POST /api/update  (streams progress via SSE)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, AsyncIterator, Callable, TypedDict

from langgraph.graph import END, START, StateGraph

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

def _install_dir() -> Path:
    return Path(os.environ.get("NULLION_INSTALL_DIR", Path.home() / ".nullion"))

def _venv_pip() -> Path:
    d = _install_dir()
    for candidate in [d / "venv/bin/pip", d / "venv/Scripts/pip.exe"]:
        if candidate.exists():
            return candidate
    return Path(sys.executable).parent / "pip"

def _src_dir() -> Path:
    """Location of the cloned/checked-out source (git repo root).

    Resolution order:
    1. NULLION_SRC_DIR env var (explicit override)
    2. Walk up from this file's location to find .git root (works for editable installs)
    3. ~/.nullion/src (classic installer layout)
    """
    explicit = os.environ.get("NULLION_SRC_DIR")
    if explicit:
        return Path(explicit)

    # Walk up from this module's file to find the .git root
    candidate = Path(__file__).resolve().parent
    for _ in range(8):
        if (candidate / ".git").exists():
            return candidate
        parent = candidate.parent
        if parent == candidate:
            break
        candidate = parent

    # Fallback: installer layout
    return _install_dir() / "src"

def _backup_dir() -> Path:
    d = _install_dir() / "backups"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class UpdateProgress:
    step:    str
    message: str
    ok:      bool = True

@dataclass
class UpdateResult:
    success:        bool
    rolled_back:    bool = False
    from_version:   str  = ""
    to_version:     str  = ""
    error:          str  = ""
    snapshot_path:   str  = ""
    steps:          list[UpdateProgress] = field(default_factory=list)


@dataclass(frozen=True)
class UpdateTarget:
    channel: str
    ref:     str
    commit:  str
    label:   str


# ── Git helpers ───────────────────────────────────────────────────────────────

def _git(*args: str, cwd: Path | None = None) -> tuple[int, str]:
    cwd = cwd or _src_dir()
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.returncode, (result.stdout + result.stderr).strip()

def _current_commit(cwd: Path | None = None) -> str:
    _, out = _git("rev-parse", "--short", "HEAD", cwd=cwd)
    return out.strip()

def _current_commit_full(cwd: Path | None = None) -> str:
    _, out = _git("rev-parse", "HEAD", cwd=cwd)
    return out.strip()

def _version_label(ref: str = "HEAD", cwd: Path | None = None) -> str:
    args = ("describe", "--tags", "--always", "--dirty") if ref == "HEAD" else ("describe", "--tags", "--always", ref)
    code, out = _git(*args, cwd=cwd)
    if code == 0 and out.strip():
        return out.strip()
    code, out = _git("rev-parse", "--short", ref, cwd=cwd)
    return out.strip() if code == 0 and out.strip() else ref

def _fetch_update_refs(cwd: Path | None = None) -> None:
    code, out = _git("fetch", "--quiet", "origin", "main", cwd=cwd)
    if code != 0:
        raise RuntimeError(out or "git fetch failed")
    # Release tags may be moved while preserving a single public root commit.
    # Force-refreshing tags is best-effort so a stale tag never blocks updates.
    _git("fetch", "--quiet", "--force", "origin", "refs/tags/*:refs/tags/*", cwd=cwd)


def _remote_commit(cwd: Path | None = None) -> str:
    _fetch_update_refs(cwd=cwd)
    code, out = _git("rev-parse", "--short", "origin/main", cwd=cwd)
    if code != 0:
        raise RuntimeError(out or "could not resolve origin/main")
    return out.strip()


def _latest_release_tag(cwd: Path | None = None) -> str:
    code, out = _git("tag", "--list", "v[0-9]*", "--sort=-version:refname", cwd=cwd)
    if code != 0:
        raise RuntimeError(out or "could not list release tags")
    for line in out.splitlines():
        tag = line.strip()
        if tag:
            return tag
    raise RuntimeError("No release tags found. Use `nullion update --hash` to update to the latest repository commit.")


def _update_target(channel: str = "release", cwd: Path | None = None) -> UpdateTarget:
    """Resolve the update target.

    ``release`` follows the latest version tag. ``hash`` follows origin/main,
    which is useful for testing unreleased commits.
    """
    normalized = (channel or "release").strip().lower()
    _fetch_update_refs(cwd=cwd)
    if normalized == "hash":
        ref = "origin/main"
        code, out = _git("rev-parse", "--short", ref, cwd=cwd)
        if code != 0:
            raise RuntimeError(out or "could not resolve origin/main")
        return UpdateTarget(channel="hash", ref=ref, commit=out.strip(), label=_version_label(ref, cwd=cwd))
    if normalized != "release":
        raise ValueError(f"Unknown update channel: {channel}")

    tag = _latest_release_tag(cwd=cwd)
    code, out = _git("rev-parse", "--short", f"{tag}^{{commit}}", cwd=cwd)
    if code != 0:
        raise RuntimeError(out or f"could not resolve release tag {tag}")
    return UpdateTarget(channel="release", ref=tag, commit=out.strip(), label=tag)


def _commit_relation(local_commit: str, remote_commit: str, cwd: Path | None = None) -> str:
    """Return current, behind, ahead, or diverged for local HEAD vs target commit."""
    if local_commit == remote_commit:
        return "current"

    code, out = _git("merge-base", "--is-ancestor", local_commit, remote_commit, cwd=cwd)
    if code == 0:
        return "behind"
    if code != 1:
        raise RuntimeError(out or "could not compare local commit with origin/main")

    code, out = _git("merge-base", "--is-ancestor", remote_commit, local_commit, cwd=cwd)
    if code == 0:
        return "ahead"
    if code != 1:
        raise RuntimeError(out or "could not compare origin/main with local commit")

    return "diverged"


# ── Snapshot / rollback ───────────────────────────────────────────────────────

def _snapshot_name() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def snapshot() -> Path:
    """Freeze current pip state and record git commit. Returns snapshot dir."""
    tag  = _snapshot_name()
    snap = _backup_dir() / tag
    snap.mkdir(parents=True)

    # pip freeze
    pip    = _venv_pip()
    result = subprocess.run(
        [str(pip), "freeze"],
        capture_output=True, text=True,
    )
    (snap / "requirements.txt").write_text(result.stdout)

    # git commit
    commit = _current_commit_full()
    (snap / "git_commit.txt").write_text(commit)

    # keep only last 3 snapshots
    snaps = sorted(_backup_dir().glob("2*"))
    for old in snaps[:-3]:
        shutil.rmtree(old, ignore_errors=True)

    log.info("Snapshot saved: %s (commit %s)", snap, commit)
    return snap


# Short or full SHA-1 / SHA-256 git commit hash pattern.
_GIT_HASH_RE = re.compile(r"^[0-9a-f]{7,64}$", re.IGNORECASE)

# Lines in a pip requirements.txt that are not comments or blank must start
# with a package name token, an option flag, a URL scheme, or a path marker.
_SAFE_REQ_LINE_RE = re.compile(
    r"^(\s*#|"          # comment
    r"\s*$|"            # blank
    r"[A-Za-z0-9_\-\.]|"  # package name
    r"-[rRcCfiqe]\s|"   # common pip flags: -r, -c, -f, -i, -q, -e
    r"--[a-z]|"         # long flags like --index-url
    r"https?://|"       # URL requirement
    r"git\+|"           # VCS requirement
    r"file://|"         # file URL
    r"\./|\.\./"        # relative path
    r")"
)


def _validate_commit_hash(raw: str) -> str:
    """Return a verified full commit hash, accepting old short-hash snapshots."""
    stripped = raw.strip()
    if not _GIT_HASH_RE.match(stripped):
        raise ValueError(
            f"Refusing rollback: '{stripped[:80]}' is not a valid git commit hash"
        )
    code, out = _git("rev-parse", "--verify", f"{stripped}^{{commit}}")
    if code != 0:
        raise ValueError(
            f"Refusing rollback: '{stripped[:80]}' does not resolve to a commit"
        )
    resolved = out.strip().splitlines()[-1].strip()
    if not re.match(r"^[0-9a-f]{40,64}$", resolved, re.IGNORECASE):
        raise ValueError(
            f"Refusing rollback: '{stripped[:80]}' resolved to an invalid commit"
        )
    return resolved


def _validate_requirements_content(content: str) -> None:
    """Raise ValueError if the requirements file contains suspicious lines."""
    for i, line in enumerate(content.splitlines(), start=1):
        if not _SAFE_REQ_LINE_RE.match(line):
            raise ValueError(
                f"Refusing pip install: requirements line {i} looks unsafe: "
                f"'{line[:120]}'"
            )


def rollback(snap: Path) -> bool:
    """Restore pip state and git commit from snapshot. Returns True on success."""
    try:
        commit_file = snap / "git_commit.txt"
        req_file    = snap / "requirements.txt"

        if commit_file.exists():
            try:
                commit_hash = _validate_commit_hash(commit_file.read_text())
            except ValueError as exc:
                log.error("Rollback aborted: %s", exc)
                return False
            code, out = _git("reset", "--hard", commit_hash)
            if code != 0:
                log.error("git reset failed: %s", out)
                return False

        if req_file.exists():
            try:
                _validate_requirements_content(req_file.read_text())
            except ValueError as exc:
                log.error("Rollback aborted: %s", exc)
                return False
            pip = _venv_pip()
            result = subprocess.run(
                [str(pip), "install", "--quiet", "-r", str(req_file)],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                log.error("pip restore failed: %s", result.stderr)
                return False

        log.info("Rollback to %s successful.", snap.name)
        return True
    except Exception as exc:
        log.error("Rollback error: %s", exc)
        return False


# ── Health check ──────────────────────────────────────────────────────────────

def _probe_web_status(port: int = 8742, timeout: float = 10.0) -> bool:
    try:
        import urllib.request
        url = f"http://localhost:{port}/api/status"
        with urllib.request.urlopen(url, timeout=timeout) as r:
            if getattr(r, "status", 200) >= 400:
                return False
            data = json.loads(r.read())
            return data.get("status") != "error"
    except Exception as exc:
        log.warning("Web health probe failed on port %s: %s", port, exc)
        return False


def _default_env_path() -> Path | None:
    explicit = os.environ.get("NULLION_ENV_FILE")
    if explicit:
        return Path(explicit)
    installed = _install_dir() / ".env"
    if installed.exists():
        return installed
    local = _src_dir() / ".env"
    if local.exists():
        return local
    return None


def _default_checkpoint_path() -> Path:
    explicit = os.environ.get("NULLION_CHECKPOINT")
    if explicit:
        return Path(explicit)
    installed = _install_dir() / "runtime-store.json"
    if installed.exists():
        return installed
    return _src_dir() / "runtime-store.json"


def _probe_messaging_platform_bootstrap(
    *,
    timeout: float = 30.0,
    env_path: str | Path | None = None,
    checkpoint_path: str | Path | None = None,
) -> bool:
    env_arg = "" if env_path is None else str(env_path)
    checkpoint_arg = str(checkpoint_path or _default_checkpoint_path())
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "from pathlib import Path\n"
                "from nullion.config import load_settings\n"
                "from nullion.messaging_runtime import build_messaging_runtime_service_from_settings\n"
                "from nullion.telegram_entrypoint import (\n"
                "    build_runtime_service_from_settings,\n"
                ")\n"
                "env_path = sys.argv[1] or None\n"
                "checkpoint_path = Path(sys.argv[2])\n"
                "settings = load_settings(env_path=env_path)\n"
                "checked = []\n"
                "if settings.telegram.bot_token:\n"
                "    build_runtime_service_from_settings(checkpoint_path=checkpoint_path, env_path=env_path)\n"
                "    checked.append('telegram')\n"
                "if settings.slack.enabled:\n"
                "    from nullion.slack_app import _require_slack_settings\n"
                "    _require_slack_settings(settings)\n"
                "    build_messaging_runtime_service_from_settings(checkpoint_path=checkpoint_path, env_path=env_path)\n"
                "    checked.append('slack')\n"
                "if settings.discord.enabled:\n"
                "    from nullion.discord_app import _require_discord_settings\n"
                "    _require_discord_settings(settings)\n"
                "    build_messaging_runtime_service_from_settings(checkpoint_path=checkpoint_path, env_path=env_path)\n"
                "    checked.append('discord')\n"
                "print('ok:' + (','.join(checked) if checked else 'none'))\n"
            ),
            env_arg,
            checkpoint_arg,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=_src_dir(),
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        log.warning("Messaging platform bootstrap probe failed: %s", detail)
        return False
    return "ok:" in result.stdout


def health_check(
    port: int = 8742,
    timeout: float = 10.0,
    *,
    require_web: bool = True,
    require_messaging: bool = True,
    env_path: str | Path | None = None,
    checkpoint_path: str | Path | None = None,
    _warnings_out: list[str] | None = None,
) -> bool:
    """Try importing core module in a fresh subprocess + optionally probing /api/status.

    Running the import check in a subprocess is critical: after a git pull the
    updater's own Python process still has the OLD module code in memory, so any
    in-process import check would use stale bytecode.  A subprocess always loads
    the freshly-installed files.

    Any ``PROVIDER_WARNING: …`` lines printed by _build_runtime are collected
    into ``_warnings_out`` (if supplied) as clean human-readable strings.
    """
    # 1. Import + bootstrap check in a clean subprocess.  Importing only the
    # package is too weak: a broken app bootstrap can otherwise pass the update
    # health check and fail only after restart.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import nullion\n"
                "from nullion.web_app import _build_runtime, _STARTUP_WARNINGS\n"
                "runtime, orchestrator, registry = _build_runtime()\n"
                "# A missing model client is a warning, not a fatal error —\n"
                "# the runtime can still start; the user will see an error when they chat.\n"
                "if getattr(runtime, 'model_client', None) is None and not _STARTUP_WARNINGS:\n"
                "    print('PROVIDER_WARNING: No model provider configured. Set up a provider in Settings.')\n"
                "print('ok')\n"
            ),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # Collect any provider warnings emitted by _build_runtime.
    if _warnings_out is not None:
        for line in (result.stdout or "").splitlines():
            if line.startswith("PROVIDER_WARNING:"):
                _warnings_out.append(line.removeprefix("PROVIDER_WARNING:").strip())
    if result.returncode != 0 or "ok" not in result.stdout:
        log.warning("Import health check failed: %s", (result.stderr or result.stdout).strip())
        return False

    # 2. HTTP probe when the caller knows the web UI should be reachable.
    # CLI updates may be run while the web service is already down; in that
    # case the post-restart check is responsible for enforcing web recovery.
    if require_web and not _probe_web_status(port=port, timeout=timeout):
        return False

    # 3. Messaging adapter bootstrap probe.  This catches failures in connected
    # messaging entrypoints, including Telegram launch-time plugin/config errors,
    # without sending live messages to external platforms.
    if require_messaging:
        probe_env_path = env_path if env_path is not None else _default_env_path()
        if not _probe_messaging_platform_bootstrap(
            timeout=30.0,
            env_path=probe_env_path,
            checkpoint_path=checkpoint_path or _default_checkpoint_path(),
        ):
            return False

    return True


def fresh_health_check(
    port: int = 8742,
    timeout: float = 10.0,
    *,
    require_web: bool = True,
    require_messaging: bool = True,
    env_path: str | Path | None = None,
    checkpoint_path: str | Path | None = None,
    _warnings_out: list[str] | None = None,
) -> bool:
    """Run the installed updater's health check in a fresh Python process.

    After ``git pull`` and ``pip install -e .``, the current updater process is
    still executing the pre-update Python module.  Calling ``health_check()``
    directly would therefore use stale code.  This wrapper asks the freshly
    installed package to import and run its own health check.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "from nullion.updater import health_check\n"
                "port = int(sys.argv[1])\n"
                "timeout = float(sys.argv[2])\n"
                "require_web = sys.argv[3] == '1'\n"
                "require_messaging = sys.argv[4] == '1'\n"
                "env_path = sys.argv[5] or None\n"
                "checkpoint_path = sys.argv[6] or None\n"
                "warnings = []\n"
                "ok = health_check(\n"
                "    port=port,\n"
                "    timeout=timeout,\n"
                "    require_web=require_web,\n"
                "    require_messaging=require_messaging,\n"
                "    env_path=env_path,\n"
                "    checkpoint_path=checkpoint_path,\n"
                "    _warnings_out=warnings,\n"
                ")\n"
                "for w in warnings: print('PROVIDER_WARNING:', w)\n"
                "raise SystemExit(0 if ok else 1)\n"
            ),
            str(port),
            str(timeout),
            "1" if require_web else "0",
            "1" if require_messaging else "0",
            "" if env_path is None else str(env_path),
            "" if checkpoint_path is None else str(checkpoint_path),
        ],
        capture_output=True,
        text=True,
        timeout=45,
        cwd=_src_dir(),
    )
    if _warnings_out is not None:
        for line in (result.stdout or "").splitlines():
            if line.startswith("PROVIDER_WARNING:"):
                _warnings_out.append(line.removeprefix("PROVIDER_WARNING:").strip())
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        if detail:
            log.warning("Fresh health check failed: %s", detail)
        return False
    return True


# ── Core updater ──────────────────────────────────────────────────────────────

class _UpdateWorkflowState(TypedDict, total=False):
    emit: Callable[[UpdateProgress], None] | None
    web_port: int
    ignore_check_failures: bool
    env_path: str | Path | None
    checkpoint_path: str | Path | None
    update_channel: str
    steps: list[UpdateProgress]
    snap: Path | None
    from_commit: str
    to_commit: str
    from_label: str
    to_label: str
    target: UpdateTarget | None
    web_was_reachable: bool
    result: UpdateResult


def _update_emit(state: _UpdateWorkflowState, step: str, message: str, ok: bool = True) -> list[UpdateProgress]:
    progress = UpdateProgress(step=step, message=message, ok=ok)
    emit = state.get("emit")
    if emit:
        emit(progress)
    log.info("[%s] %s", step, message)
    return [*state.get("steps", []), progress]


async def _update_run_blocking(func):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func)


async def _update_fetch_node(state: _UpdateWorkflowState) -> dict[str, object]:
    steps = list(state.get("steps", []))
    from_commit = ""
    from_label = ""
    to_label = ""
    update_channel = str(state.get("update_channel") or "release")
    try:
        from_commit = _current_commit()
        channel_label = "repository commit" if update_channel == "hash" else "release"
        state = {**state, "steps": steps}
        steps = _update_emit(state, "fetch", f"Checking for {channel_label} updates...")
        target = await _update_run_blocking(lambda: _update_target(update_channel))
        to_commit = target.commit
        from_label = await _update_run_blocking(lambda: _version_label("HEAD"))
        to_label = target.label
        relation = await _update_run_blocking(lambda: _commit_relation(from_commit, to_commit))
        state = {**state, "steps": steps}
        if relation in {"current", "ahead"}:
            if target.channel == "release":
                suffix = " Use `nullion update --hash` to install unreleased commits." if relation == "ahead" else ""
                message = f"Already on latest release {to_label} at {from_label}.{suffix}"
            else:
                suffix = "" if relation == "current" else " Local checkout is ahead of origin/main."
                message = f"Already up to date at {from_label}.{suffix}"
            steps = _update_emit(state, "fetch", message)
            return {
                "steps": steps,
                "from_commit": from_commit,
                "to_commit": to_commit,
                "from_label": from_label,
                "to_label": to_label,
                "target": target,
                "result": UpdateResult(success=True, from_version=from_label, to_version=from_label, steps=steps),
            }
        if relation == "diverged":
            steps = _update_emit(state, "fetch", "Update history changed; source will be reset after backup.")
            state = {**state, "steps": steps}
        steps = _update_emit(state, "fetch", f"Update available: {from_label} -> {to_label}")
    except Exception as exc:
        state = {**state, "steps": steps}
        steps = _update_emit(state, "fetch", f"Could not check for updates: {exc}", ok=False)
        return {"steps": steps, "result": UpdateResult(success=False, error=str(exc), steps=steps)}
    web_was_reachable = _probe_web_status(port=int(state.get("web_port") or 8742), timeout=1.0)
    return {
        "steps": steps,
        "from_commit": from_commit,
        "to_commit": to_commit,
        "from_label": from_label,
        "to_label": to_label,
        "target": target,
        "web_was_reachable": web_was_reachable,
    }


def _update_route_after_node(state: _UpdateWorkflowState) -> str:
    return END if state.get("result") is not None else "snapshot"


async def _update_snapshot_node(state: _UpdateWorkflowState) -> dict[str, object]:
    steps = list(state.get("steps", []))
    snap: Path | None = None
    from_label = str(state.get("from_label") or "")
    to_label = str(state.get("to_label") or "")
    try:
        state = {**state, "steps": steps}
        steps = _update_emit(state, "snapshot", f"Saving backup of current version ({from_label})...")
        snap = await _update_run_blocking(snapshot)
        state = {**state, "steps": steps}
        steps = _update_emit(state, "snapshot", f"Backup saved to {snap.name}")
    except Exception as exc:
        state = {**state, "steps": steps}
        steps = _update_emit(state, "snapshot", f"Could not create backup: {exc}", ok=False)
        return {
            "steps": steps,
            "snap": snap,
            "result": UpdateResult(
                success=False,
                from_version=from_label,
                to_version=to_label,
                error=str(exc),
                snapshot_path=str(snap) if snap else "",
                steps=steps,
            ),
        }
    return {"steps": steps, "snap": snap}


def _update_route_after_snapshot(state: _UpdateWorkflowState) -> str:
    return END if state.get("result") is not None else "apply"


async def _update_apply_node(state: _UpdateWorkflowState) -> dict[str, object]:
    steps = list(state.get("steps", []))
    snap = state.get("snap")
    from_label = str(state.get("from_label") or "")
    to_label = str(state.get("to_label") or "")
    target = state.get("target")
    update_channel = str(state.get("update_channel") or "release")
    try:
        target_ref = target.ref if target is not None else "origin/main"
        apply_label = "latest repository commit" if update_channel == "hash" else "latest release"
        state = {**state, "steps": steps}
        steps = _update_emit(state, "apply", f"Applying {apply_label}...")
        code, out = await _update_run_blocking(lambda: _git("reset", "--hard", target_ref))
        if code != 0:
            raise RuntimeError(f"git reset failed: {out}")
        code, out = await _update_run_blocking(lambda: _git("clean", "-fd"))
        if code != 0:
            raise RuntimeError(f"git clean failed: {out}")

        state = {**state, "steps": steps}
        steps = _update_emit(state, "apply", "Installing updated dependencies...")
        pip    = _venv_pip()
        src    = _src_dir()
        install_result = await _update_run_blocking(
            lambda: subprocess.run(
                [str(pip), "install", "--quiet", "-e", str(src)],
                capture_output=True, text=True,
            ),
        )
        if install_result.returncode != 0:
            raise RuntimeError(f"pip install failed: {install_result.stderr}")

        state = {**state, "steps": steps}
        steps = _update_emit(state, "apply", "Update installed.")
    except Exception as exc:
        state = {**state, "steps": steps}
        steps = _update_emit(state, "apply", f"Update failed: {exc}", ok=False)
        state = {**state, "steps": steps}
        steps = _update_emit(state, "rollback", "Rolling back to previous version...")
        ok = await _update_run_blocking(lambda: rollback(snap))
        msg = "Rollback successful. Previous version restored." if ok else "Rollback also failed — check logs."
        state = {**state, "steps": steps}
        steps = _update_emit(state, "rollback", msg, ok=ok)
        return {
            "steps": steps,
            "result": UpdateResult(
                success=False,
                rolled_back=ok,
                from_version=from_label,
                to_version=to_label,
                error=str(exc),
                snapshot_path=str(snap) if snap else "",
                steps=steps,
            ),
        }
    return {"steps": steps}


def _update_route_after_apply(state: _UpdateWorkflowState) -> str:
    return END if state.get("result") is not None else "health"


async def _update_health_node(state: _UpdateWorkflowState) -> dict[str, object]:
    steps = list(state.get("steps", []))
    snap = state.get("snap")
    from_label = str(state.get("from_label") or "")
    to_label = str(state.get("to_label") or "")
    state = {**state, "steps": steps}
    steps = _update_emit(state, "health", "Running health check...")
    await asyncio.sleep(1)  # brief pause so imports can settle
    _health_warnings: list[str] = []
    healthy = await _update_run_blocking(
        lambda: fresh_health_check(
            port=int(state.get("web_port") or 8742),
            require_web=bool(state.get("web_was_reachable", False)),
            require_messaging=True,
            env_path=state.get("env_path"),
            checkpoint_path=state.get("checkpoint_path"),
            _warnings_out=_health_warnings,
        ),
    )

    if not healthy:
        if bool(state.get("ignore_check_failures", False)):
            steps = _update_emit(state, "health", "Health check failed — continuing because checks were ignored.", ok=False)
            state = {**state, "steps": steps}
            steps = _update_emit(state, "done", f"Nullion updated from {from_label} to {to_label} with failed checks ignored.")
            return {
                "steps": steps,
                "result": UpdateResult(
                    success=True,
                    rolled_back=False,
                    from_version=from_label,
                    to_version=to_label,
                    snapshot_path=str(snap) if snap else "",
                    steps=steps,
                ),
            }
        steps = _update_emit(state, "health", "Health check failed — rolling back.", ok=False)
        ok = await _update_run_blocking(lambda: rollback(snap))
        msg = "Rollback successful. Previous version restored." if ok else "Rollback also failed — check logs."
        state = {**state, "steps": steps}
        steps = _update_emit(state, "rollback", msg, ok=ok)
        return {
            "steps": steps,
            "result": UpdateResult(
                success=False,
                rolled_back=ok,
                from_version=from_label,
                to_version=to_label,
                error="Health check failed after update.",
                snapshot_path=str(snap) if snap else "",
                steps=steps,
            ),
        }

    steps = _update_emit(state, "health", "Health check passed.")
    for _w in _health_warnings:
        state = {**state, "steps": steps}
        steps = _update_emit(state, "health", f"Warning: {_w}")
    state = {**state, "steps": steps}
    steps = _update_emit(state, "done", f"Nullion updated from {from_label} to {to_label}.")
    return {
        "steps": steps,
        "result": UpdateResult(
            success=True,
            from_version=from_label,
            to_version=to_label,
            snapshot_path=str(snap) if snap else "",
            steps=steps,
        ),
    }


@lru_cache(maxsize=1)
def _compiled_update_workflow_graph():
    graph = StateGraph(_UpdateWorkflowState)
    graph.add_node("fetch", _update_fetch_node)
    graph.add_node("snapshot", _update_snapshot_node)
    graph.add_node("apply", _update_apply_node)
    graph.add_node("health", _update_health_node)
    graph.add_edge(START, "fetch")
    graph.add_conditional_edges("fetch", _update_route_after_node, {"snapshot": "snapshot", END: END})
    graph.add_conditional_edges("snapshot", _update_route_after_snapshot, {"apply": "apply", END: END})
    graph.add_conditional_edges("apply", _update_route_after_apply, {"health": "health", END: END})
    graph.add_edge("health", END)
    return graph.compile()


async def run_update(
    emit: Callable[[UpdateProgress], None] | None = None,
    web_port: int = 8742,
    ignore_check_failures: bool = False,
    env_path: str | Path | None = None,
    checkpoint_path: str | Path | None = None,
    update_channel: str = "release",
) -> UpdateResult:
    """Full update flow. Calls emit() for each step so callers can stream progress."""
    final_state = await _compiled_update_workflow_graph().ainvoke(
        {
            "emit": emit,
            "web_port": web_port,
            "ignore_check_failures": ignore_check_failures,
            "env_path": env_path,
            "checkpoint_path": checkpoint_path,
            "update_channel": update_channel,
            "steps": [],
        },
        config={"configurable": {"thread_id": f"update:{update_channel}"}},
    )
    result = final_state.get("result")
    if isinstance(result, UpdateResult):
        return result
    steps = list(final_state.get("steps") or [])
    return UpdateResult(success=False, error="Update workflow did not produce a result.", steps=steps)

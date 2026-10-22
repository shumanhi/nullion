#!/usr/bin/env python3
"""Local Nullion prod/stage/dev operations dashboard.

This is intentionally a local-only helper for a developer workstation. It does not belong
in the app runtime; it observes and controls the three local instances using
structured runtime evidence: launchd labels, package metadata, ports, runtime
DB files, and log files.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import threading
import time
from typing import Any
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
HOME = Path.home()
UID = os.getuid()
DASHBOARD_PORT = 2020
LOCAL_OPS_LAUNCHD_LABEL = "com.nullion.local-ops-dashboard"


@dataclass(frozen=True)
class Service:
    key: str
    label: str
    log_name: str
    error_log_name: str


@dataclass(frozen=True)
class Instance:
    key: str
    title: str
    url: str
    port: int
    install_dir: Path
    expected_source: str
    services: tuple[Service, ...]

    @property
    def venv_python(self) -> Path:
        return self.install_dir / "venv" / "bin" / "python"

    @property
    def runtime_db(self) -> Path:
        return self.install_dir / "runtime.db"

    @property
    def env_file(self) -> Path:
        return self.install_dir / ".env"

    @property
    def logs_dir(self) -> Path:
        return self.install_dir / "logs"


PROD = Instance(
    key="prod",
    title="Prod",
    url="http://127.0.0.1:8742",
    port=8742,
    install_dir=HOME / ".nullion",
    expected_source=str(HOME / ".nullion" / "src"),
    services=(
        Service("web", "com.nullion.web", "nullion.log", "nullion-error.log"),
        Service("tray", "com.nullion.tray", "tray.log", "tray-error.log"),
        Service("telegram", "ai.nullion.telegram", "telegram.log", "telegram.error.log"),
    ),
)

DEV = Instance(
    key="dev",
    title="Dev",
    url="http://127.0.0.1:8752",
    port=8752,
    install_dir=HOME / ".nullion-test",
    expected_source=str(ROOT),
    services=(
        Service("web", "com.nullion.test.web", "nullion.log", "nullion-error.log"),
        Service("tray", "com.nullion.test.tray", "tray.log", "tray-error.log"),
        Service("telegram", "ai.nullion.test.telegram", "telegram.log", "telegram-error.log"),
    ),
)

STAGE = Instance(
    key="stage",
    title="Stage",
    url="http://127.0.0.1:8753",
    port=8753,
    install_dir=HOME / ".nullion-stage",
    expected_source=str(ROOT.parent / "nullion-stage-main"),
    services=(
        Service("web", "com.nullion.stage.web", "nullion.log", "nullion-error.log"),
        Service("tray", "com.nullion.stage.tray", "tray.log", "tray-error.log"),
        Service("telegram", "ai.nullion.stage.telegram", "telegram.log", "telegram-error.log"),
    ),
)

INSTANCES = (PROD, STAGE, DEV)
INSTANCE_BY_KEY = {instance.key: instance for instance in INSTANCES}
TEST_LAUNCHER = Path(os.environ.get("NULLION_STAGE_LAUNCHER", str(ROOT / "scripts" / "stage-launcher")))
APP_REPO = Path(os.environ.get("NULLION_APP_REPO", str(ROOT)))
DEV_WORKTREE_DIR = Path(os.environ.get("NULLION_DEV_WORKTREE_DIR", str(ROOT.parent / "nullion-dev-worktrees")))
STAGE_SOURCE_DIR = Path(os.environ.get("NULLION_STAGE_SOURCE_DIR", str(ROOT.parent / "nullion-stage-main")))
PINNED_BRANCH_FILE = Path(os.environ.get("NULLION_OPS_PINNED_BRANCH_FILE", str(HOME / ".nullion-ops" / "pinned-branches.json")))
LAUNCH_AGENTS_DIR = HOME / "Library" / "LaunchAgents"
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()
ACTION_JOB_TTL_SECONDS = 1800
SELF_PID = str(os.getpid())


def _run(
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 8.0,
) -> dict[str, Any]:
    started = time.time()
    try:
        run_env = os.environ.copy()
        if env:
            run_env.update(env)
        result = subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            env=run_env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return {
            "ok": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "elapsed_ms": round((time.time() - started) * 1000),
            "cmd": args,
        }
    except Exception as exc:
        return {
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
            "elapsed_ms": round((time.time() - started) * 1000),
            "cmd": args,
        }


def _compact_result(result: dict[str, Any], *, max_text: int = 900) -> dict[str, Any]:
    compact = {
        "ok": result.get("ok"),
        "returncode": result.get("returncode"),
        "elapsed_ms": result.get("elapsed_ms"),
    }
    stdout = str(result.get("stdout") or "")
    stderr = str(result.get("stderr") or "")
    if stdout:
        compact["stdout"] = stdout[-max_text:]
    if stderr:
        compact["stderr"] = stderr[-max_text:]
    return compact


def restart_ops_dashboard() -> dict[str, Any]:
    command = ["launchctl", "kickstart", "-k", f"gui/{UID}/{LOCAL_OPS_LAUNCHD_LABEL}"]

    def restart_later() -> None:
        time.sleep(0.4)
        _run(command, timeout=8)

    threading.Thread(target=restart_later, daemon=True).start()
    return {"ok": True, "message": "Dashboard restart queued.", "cmd": command}


def _kill_stale_dashboard_processes(*, port: int, current_pid: str = SELF_PID) -> dict[str, Any]:
    ps = _run(["ps", "-eo", "pid=,command="], timeout=4)
    if not ps.get("ok"):
        return {
            "ok": False,
            "error": f"failed to list processes: {ps.get('stderr') or ps.get('stdout') or 'unknown'}",
            "count": 0,
        }

    candidates: list[str] = []
    for line in str(ps.get("stdout") or "").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) < 2:
            continue
        pid, command = parts[0], parts[1]
        if pid == current_pid:
            continue
        if "local_ops_dashboard.py" not in command:
            continue
        if f"--port {port}" not in command and f"--port={port}" not in command:
            continue
        candidates.append(pid)

    killed = 0
    errors: list[str] = []
    for pid in candidates:
        terminate = _run(["kill", "-TERM", pid], timeout=4)
        if not terminate.get("ok"):
            errors.append(f"{pid}:{terminate.get('stderr') or terminate.get('stdout')}")
            continue
        killed += 1

    if candidates and not killed:
        errors.append("matches found but none were terminated")
    return {"ok": not errors, "count": killed, "candidates": candidates, "errors": errors}


def _safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value.strip())
    return cleaned.strip(".-") or "default"


def _short_timestamp_suffix() -> str:
    return datetime.now().strftime("--%I-%M-%p").lower()


def _has_short_timestamp_suffix(value: str) -> bool:
    return re.search(r"--\d{2}-\d{2}-(am|pm)$", str(value or ""), re.IGNORECASE) is not None


def _action_job_snapshot(job: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in job.items()
        if key not in {"thread"}
    }


def _action_job_event(job: dict[str, Any], message: str, *, command: list[str] | None = None, result: dict[str, Any] | None = None) -> None:
    event = {
        "time": datetime.now().isoformat(timespec="seconds"),
        "message": message,
        **({"command": command} if command else {}),
        **({"result": _compact_result(result)} if result is not None else {}),
    }
    with JOBS_LOCK:
        events = job.setdefault("events", [])
        if isinstance(events, list):
            events.append(event)
            del events[:-100]


def _run_step(
    job: dict[str, Any] | None,
    message: str,
    command: list[str],
    *,
    timeout: float = 30.0,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    if job is not None:
        _action_job_event(job, message, command=command)
    result = _run(command, timeout=timeout, cwd=cwd, env=env)
    if job is not None:
        _action_job_event(job, f"{message} {'completed' if result.get('ok') else 'failed'}", result=result)
    return result


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _repo_for_key(repo_key: str) -> Path:
    if repo_key == "app":
        return APP_REPO
    if repo_key == "test":
        return ROOT
    raise ValueError(f"unknown repo key: {repo_key}")


def _load_pin_state() -> dict[str, Any]:
    try:
        payload = json.loads(PINNED_BRANCH_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        payload = {}
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _write_pin_state(state: dict[str, Any]) -> None:
    PINNED_BRANCH_FILE.parent.mkdir(parents=True, exist_ok=True)
    PINNED_BRANCH_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(PINNED_BRANCH_FILE, 0o600)


def _http_json(url: str, *, timeout: float = 1.5) -> dict[str, Any]:
    started = time.time()
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read(1024 * 512).decode("utf-8", errors="replace")
            try:
                payload = json.loads(body) if body else None
            except json.JSONDecodeError:
                payload = body[:400]
            return {
                "ok": 200 <= response.status < 300,
                "status": response.status,
                "elapsed_ms": round((time.time() - started) * 1000),
                "payload": payload,
            }
    except URLError as exc:
        return {
            "ok": False,
            "status": 0,
            "elapsed_ms": round((time.time() - started) * 1000),
            "error": str(exc.reason if hasattr(exc, "reason") else exc),
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": 0,
            "elapsed_ms": round((time.time() - started) * 1000),
            "error": str(exc),
        }


def _http_probe(url: str, *, timeout: float = 2.0, max_bytes: int = 1024 * 1024) -> dict[str, Any]:
    started = time.time()
    request = Request(url, headers={"Accept": "text/html,application/json;q=0.9,*/*;q=0.8"})
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read(max_bytes)
            return {
                "ok": 200 <= response.status < 300,
                "status": response.status,
                "elapsed_ms": round((time.time() - started) * 1000),
                "bytes": len(body),
                "truncated": len(body) >= max_bytes,
                "content_type": response.headers.get("content-type", ""),
            }
    except Exception as exc:
        return {
            "ok": False,
            "status": 0,
            "elapsed_ms": round((time.time() - started) * 1000),
            "error": str(exc),
        }


def _websocket_probe(instance: Instance, *, timeout: float = 4.0) -> dict[str, Any]:
    if not instance.venv_python.exists():
        return {"ok": False, "error": f"missing {instance.venv_python}"}
    code = f"""
import asyncio, json, time
import websockets
async def main():
    started = time.time()
    try:
        async with websockets.connect('ws://127.0.0.1:{instance.port}/ws/chat', open_timeout={timeout!r}):
            print(json.dumps({{"ok": True, "elapsed_ms": round((time.time() - started) * 1000)}}))
    except Exception as exc:
        print(json.dumps({{"ok": False, "elapsed_ms": round((time.time() - started) * 1000), "error": type(exc).__name__ + ": " + str(exc)}}))
asyncio.run(main())
""".strip()
    result = _run([str(instance.venv_python), "-c", code], timeout=timeout + 2)
    if not result["ok"] and not result["stdout"]:
        return {"ok": False, "error": result["stderr"] or "websocket probe failed"}
    try:
        return json.loads(result["stdout"])
    except json.JSONDecodeError:
        return {"ok": False, "error": result["stdout"] or result["stderr"]}


def _parse_kv_lines(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _pip_show(instance: Instance) -> dict[str, Any]:
    if not instance.venv_python.exists():
        return {"ok": False, "error": f"missing {instance.venv_python}"}
    result = _run([str(instance.venv_python), "-m", "pip", "show", "nullion"], timeout=8)
    if not result["ok"]:
        return {"ok": False, "error": result["stderr"] or result["stdout"], "raw": result}
    data = _parse_kv_lines(result["stdout"])
    return {
        "ok": True,
        "version": data.get("Version", ""),
        "location": data.get("Location", ""),
        "editable_source": data.get("Editable project location", ""),
        "raw": data,
    }


def _prod_release_install_verification() -> dict[str, Any]:
    package = _pip_show(PROD)
    runtime_version = _runtime_version(PROD)
    location = str(package.get("location") or "")
    editable_source = str(package.get("editable_source") or "")
    import_path = str(runtime_version.get("module") or "")
    expected_site_packages = str((PROD.install_dir / "venv").resolve())
    forbidden_prefixes = (
        str(APP_REPO.resolve()),
        str(ROOT.resolve()),
        str(DEV.install_dir.resolve()),
        str(STAGE.install_dir.resolve()),
        str(DEV_WORKTREE_DIR.resolve()),
        str(STAGE_SOURCE_DIR.resolve()),
    )
    issues: list[str] = []
    if editable_source:
        issues.append(f"editable_source still set: {editable_source}")
    if expected_site_packages not in location:
        issues.append(f"package location is outside prod venv: {location}")
    if expected_site_packages not in import_path:
        issues.append(f"import path is outside prod venv: {import_path}")
    for value in (location, import_path, editable_source):
        if value and any(value.startswith(prefix) for prefix in forbidden_prefixes):
            issues.append(f"local/dev source reference remains: {value}")
            break
    return {
        "ok": package.get("ok") is True and runtime_version.get("ok") is True and not issues,
        "issues": issues,
        "package": package,
        "runtime_version": runtime_version,
    }


def _runtime_version(instance: Instance) -> dict[str, Any]:
    if not instance.venv_python.exists():
        return {"ok": False, "error": f"missing {instance.venv_python}"}
    code = """
import json
from pathlib import Path
import nullion
from nullion.version import current_version, version_tag
print(json.dumps({
  "module": str(Path(nullion.__file__).resolve()),
  "current_version": current_version(),
  "version_tag": version_tag(),
}))
""".strip()
    result = _run([str(instance.venv_python), "-c", code], timeout=8)
    if not result["ok"]:
        return {"ok": False, "error": result["stderr"] or result["stdout"], "raw": result}
    try:
        return {"ok": True, **json.loads(result["stdout"])}
    except json.JSONDecodeError:
        return {"ok": False, "error": result["stdout"], "raw": result}


def _launchctl(service: Service) -> dict[str, Any]:
    result = _run(["launchctl", "print", f"gui/{UID}/{service.label}"], timeout=4)
    if not result["ok"]:
        return {
            "key": service.key,
            "label": service.label,
            "loaded": False,
            "state": "missing",
            "pid": None,
            "program": "",
            "environment": {},
            "error": result["stderr"] or result["stdout"],
        }
    text = result["stdout"]
    state = _first_match(text, r"state = ([^\n]+)") or "unknown"
    pid_raw = _first_match(text, r"pid = ([0-9]+)")
    program = _first_match(text, r"program = ([^\n]+)") or ""
    env: dict[str, str] = {}
    for key, value in re.findall(r"\n\s+([A-Z0-9_]+) => ([^\n]+)", text):
        if key in {"NULLION_HOME", "NULLION_ENV_FILE", "XPC_SERVICE_NAME", "PATH"}:
            env[key] = value.strip()
    return {
        "key": service.key,
        "label": service.label,
        "loaded": True,
        "state": state.strip(),
        "pid": int(pid_raw) if pid_raw else None,
        "program": program.strip(),
        "environment": env,
    }


def _service_pid(instance: Instance, key: str) -> int | None:
    for service in instance.services:
        if service.key == key:
            data = _launchctl(service)
            return data.get("pid")
    return None


def _first_match(text: str, pattern: str) -> str | None:
    match = re.search(pattern, text)
    return match.group(1) if match else None


def _ps(pid: int | None) -> dict[str, Any]:
    if not pid:
        return {}
    result = _run(["ps", "-p", str(pid), "-o", "pid=,pcpu=,pmem=,rss=,etime=,command="], timeout=3)
    if not result["ok"] or not result["stdout"]:
        return {}
    parts = result["stdout"].split(None, 5)
    if len(parts) < 6:
        return {"raw": result["stdout"]}
    return {
        "pid": int(parts[0]),
        "cpu_percent": parts[1],
        "mem_percent": parts[2],
        "rss_kb": parts[3],
        "elapsed": parts[4],
        "command": parts[5],
    }


def _port_listener(port: int) -> dict[str, Any]:
    if shutil.which("lsof") is None:
        return {"ok": False, "error": "lsof not found"}
    result = _run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"], timeout=4)
    return {"ok": result["ok"], "raw": result["stdout"], "error": result["stderr"]}


def _tail(path: Path, *, lines: int = 80) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "text": ""}
    try:
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:
        return {"path": str(path), "exists": True, "text": "", "error": str(exc)}
    return {
        "path": str(path),
        "exists": True,
        "size": path.stat().st_size,
        "text": "\n".join(content[-lines:]),
    }


def _db_stats(path: Path) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "tables": {},
    }
    wal = path.with_name(path.name + "-wal")
    shm = path.with_name(path.name + "-shm")
    stats["wal_size_bytes"] = wal.stat().st_size if wal.exists() else 0
    stats["shm_size_bytes"] = shm.stat().st_size if shm.exists() else 0
    if not path.exists():
        return stats
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)
        try:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            for (table,) in rows:
                try:
                    count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                except Exception:
                    count = None
                stats["tables"][table] = count
        finally:
            conn.close()
    except Exception as exc:
        stats["error"] = str(exc)
    return stats


def _git_info(path_text: str) -> dict[str, Any]:
    path = Path(path_text).expanduser()
    if not path.exists():
        return {"ok": False, "path": str(path), "error": "missing"}
    branch = _run(["git", "-C", str(path), "branch", "--show-current"], timeout=4)
    commit = _run(["git", "-C", str(path), "rev-parse", "--short=9", "HEAD"], timeout=4)
    status = _run(["git", "-C", str(path), "status", "--short"], timeout=4)
    upstream = _run(["git", "-C", str(path), "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"], timeout=4)
    ahead = behind = None
    if upstream["ok"] and upstream["stdout"]:
        counts = _run(["git", "-C", str(path), "rev-list", "--left-right", "--count", f"{upstream['stdout']}...HEAD"], timeout=4)
        if counts["ok"]:
            parts = counts["stdout"].split()
            if len(parts) == 2:
                behind = int(parts[0])
                ahead = int(parts[1])
    return {
        "ok": branch["ok"] and commit["ok"],
        "path": str(path),
        "branch": branch["stdout"],
        "commit": commit["stdout"],
        "dirty_count": len([line for line in status["stdout"].splitlines() if line.strip()]),
        "upstream": upstream["stdout"] if upstream["ok"] else "",
        "ahead": ahead,
        "behind": behind,
    }


def _instance_source_matches_expected(instance: Instance, package: dict[str, Any]) -> bool:
    editable_source = str(package.get("editable_source") or "")
    if editable_source:
        return editable_source == instance.expected_source
    if instance.key != "prod":
        return False
    location = str(package.get("location") or "")
    return str((instance.install_dir / "venv").resolve()) in location


def _instance_source_git(instance: Instance, package: dict[str, Any]) -> dict[str, Any]:
    editable_source = str(package.get("editable_source") or "")
    if editable_source:
        return _git_info(editable_source)
    if instance.key == "prod":
        return _git_info(instance.expected_source)
    return {}


def _git_stdout(args: list[str], *, cwd: Path = APP_REPO, timeout: float = 8.0) -> str:
    result = _run(["git", "-C", str(cwd), *args], timeout=timeout)
    return result["stdout"] if result["ok"] else ""


def _worktrees_by_branch(repo: Path = APP_REPO) -> dict[str, Path]:
    worktrees: dict[str, Path] = {}
    current_path: Path | None = None
    for line in _git_stdout(["worktree", "list", "--porcelain"], cwd=repo).splitlines():
        if line.startswith("worktree "):
            current_path = Path(line.removeprefix("worktree "))
        elif line.startswith("branch ") and current_path:
            ref = line.removeprefix("branch ").strip()
            if ref.startswith("refs/heads/"):
                worktrees[ref.removeprefix("refs/heads/")] = current_path
    return worktrees


def _git_dirty_count(path: Path) -> int:
    status = _run(["git", "-C", str(path), "status", "--short"], timeout=4)
    return len([line for line in status["stdout"].splitlines() if line.strip()]) if status["ok"] else 0


def branch_rows(repo: Path = APP_REPO) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    worktrees = _worktrees_by_branch(repo)
    refs = _git_stdout(
        [
            "for-each-ref",
            "--format=%(refname)|%(refname:short)|%(committerdate:unix)|%(committerdate:relative)|%(contents:subject)|%(objectname:short)",
            "refs/heads",
            "refs/remotes/local",
            "refs/remotes/origin",
        ],
        cwd=repo,
    )
    current = _git_stdout(["branch", "--show-current"], cwd=repo)
    for line in refs.splitlines():
        parts = line.split("|", 5)
        if len(parts) != 6:
            continue
        full, short, updated_at, when, subject, commit = parts
        if full.startswith("refs/heads/"):
            branch = short
            source = "local"
            value = f"local:{branch}"
            worktree = worktrees.get(branch)
            dirty_count = _git_dirty_count(worktree) if worktree is not None else 0
            dirty_suffix = f" • dirty {dirty_count}" if dirty_count else ""
            display = f"{value}{dirty_suffix}"
            compare_ref = branch
        elif full.startswith("refs/remotes/local/"):
            branch = short.removeprefix("local/")
            source = "local-ref"
            value = f"local-ref:{branch}"
            display = value
            compare_ref = f"refs/remotes/local/{branch}"
            worktree = None
            dirty_count = 0
        elif full.startswith("refs/remotes/origin/"):
            if full == "refs/remotes/origin/HEAD":
                continue
            branch = short.removeprefix("origin/")
            source = "origin"
            value = f"origin:{branch}"
            display = value
            compare_ref = f"refs/remotes/origin/{branch}"
            worktree = None
            dirty_count = 0
        else:
            continue
        if value in seen:
            continue
        seen.add(value)
        merged = _run(["git", "-C", str(repo), "merge-base", "--is-ancestor", compare_ref, "main"], timeout=4)["ok"]
        rows.append(
            {
                "branch": branch,
                "value": value,
                "display": display,
                "source": source,
                "updated_at": int(updated_at or 0),
                "when": when,
                "subject": subject,
                "commit": commit,
                "current": branch == current,
                "merged_to_main": merged,
                "worktree": str(worktree) if worktree is not None else "",
                "dirty_count": dirty_count,
            }
        )
    return sorted(rows, key=lambda row: row.get("updated_at", 0), reverse=True)


def _sorted_prod_source_rows(branches: list[dict[str, Any]], releases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted([*branches, *releases], key=lambda row: row.get("updated_at", 0), reverse=True)


def release_tag_rows(repo: Path = APP_REPO) -> list[dict[str, Any]]:
    if not (repo / ".git").exists():
        return []
    refs = _git_stdout(
        [
            "for-each-ref",
            "--sort=-version:refname",
            "--format=%(refname:short)|%(creatordate:unix)|%(creatordate:relative)|%(contents:subject)|%(objectname:short)",
            "refs/tags/v[0-9]*",
        ],
        cwd=repo,
    )
    rows: list[dict[str, Any]] = []
    for line in refs.splitlines():
        tag, updated_at, when, subject, commit = line.split("|", 4)
        rows.append(
            {
                "branch": tag,
                "value": tag,
                "display": f"release:{tag}",
                "source": "release",
                "updated_at": int(updated_at or 0),
                "when": when,
                "subject": subject,
                "commit": commit,
                "current": False,
                "merged_to_main": True,
                "worktree": "",
                "dirty_count": 0,
            }
        )
    return rows


def _latest_release_tag(repo: Path = APP_REPO) -> str:
    rows = release_tag_rows(repo)
    return str(rows[0]["branch"]) if rows else "v0.5.0"


def _resolve_prod_selection(selection: str, source: Path, *, release_only: bool = False) -> dict[str, Any]:
    raw = str(selection or "").strip()
    if raw.startswith("release:") or raw.startswith("tag:"):
        raw = raw.split(":", 1)[1].strip()
    if not raw:
        raw = _latest_release_tag(source)
    if re.fullmatch(r"v\d+(?:\.\d+){1,3}(?:[-+][A-Za-z0-9._-]+)?", raw):
        return {"ok": True, "branch": raw, "ref": raw, "source_mode": "release_tag", "fetch": "tags"}
    if release_only:
        return {
            "ok": False,
            "error": f"reset prod only accepts release tags; refused selection: {raw}",
            "selection": raw,
        }
    resolved = _resolve_git_ref(APP_REPO, raw, default="")
    if not resolved.get("ok"):
        resolved = _resolve_git_ref(source, raw, default="")
        if resolved.get("ok"):
            return {
                "ok": True,
                "branch": resolved["branch"],
                "ref": resolved["ref"],
                "source_mode": f"{resolved['source_scope']}_prod_source",
                "fetch": "origin" if resolved["source_scope"] == "origin" else "",
            }
        return {"ok": False, "error": resolved.get("error") or f"prod source not found: {raw}", "selection": raw}
    source_scope = str(resolved["source_scope"])
    branch = str(resolved["branch"])
    if source_scope == "local":
        fetch_ref = f"refs/heads/{branch}"
    elif source_scope == "local-ref":
        fetch_ref = f"refs/remotes/local/{branch}"
    else:
        fetch_ref = f"refs/remotes/origin/{branch}"
    return {
        "ok": True,
        "branch": branch,
        "ref": "FETCH_HEAD",
        "source_mode": f"{source_scope}_app_checkout",
        "fetch": "app_checkout",
        "fetch_ref": fetch_ref,
    }


def _install_prod_from_release_source(*, job: dict[str, Any] | None = None) -> dict[str, Any]:
    source = Path(PROD.expected_source)
    venv = _ensure_instance_venv(PROD)
    if not venv["ok"]:
        return venv
    uninstall = _run_step(
        job,
        "Remove previous prod package install",
        [str(PROD.venv_python), "-m", "pip", "uninstall", "-y", "nullion"],
        timeout=60,
    )
    install = _run_step(
        job,
        "Install prod package from managed source",
        [str(PROD.venv_python), "-m", "pip", "install", "--quiet", "--no-cache-dir", str(source)],
        timeout=180,
    )
    verification = _prod_release_install_verification() if install["ok"] else {"ok": False, "issues": ["install failed"]}
    return {
        "ok": install["ok"] and verification["ok"],
        "venv": _compact_result(venv),
        "uninstall": _compact_result(uninstall),
        "install": _compact_result(install),
        "verification": verification,
        "error": None if install["ok"] and verification["ok"] else install["stderr"] or install["stdout"] or "; ".join(verification.get("issues") or []),
    }


def _resolve_git_ref(repo: Path, selection: str, *, default: str) -> dict[str, Any]:
    scope, branch = _split_ref_selection(selection, default=default)
    candidates: list[tuple[str, str, str]] = []
    if scope in {"auto", "local"}:
        candidates.append(("local", f"refs/heads/{branch}", branch))
    if scope in {"auto", "local-ref"}:
        candidates.append(("local-ref", f"refs/remotes/local/{branch}", f"refs/remotes/local/{branch}"))
    if scope in {"auto", "origin"}:
        candidates.append(("origin", f"refs/remotes/origin/{branch}", f"refs/remotes/origin/{branch}"))
    for source_scope, full_ref, checkout_ref in candidates:
        if _run(["git", "-C", str(repo), "show-ref", "--verify", "--quiet", full_ref], timeout=4)["ok"]:
            return {"ok": True, "branch": branch, "ref": checkout_ref, "source_scope": source_scope}
    return {"ok": False, "error": f"{scope} branch not found: {branch}", "branch": branch}


def _selection_value(source_scope: str, branch: str) -> str:
    return f"{source_scope}:{branch}" if source_scope in {"local", "local-ref", "origin"} else branch


def pinned_selection(repo_key: str) -> str:
    item = _load_pin_state().get(repo_key)
    if isinstance(item, dict):
        return str(item.get("selection") or "")
    return ""


def pin_branch(repo_key: str, selection: str) -> dict[str, Any]:
    repo = _repo_for_key(repo_key)
    resolved = _resolve_git_ref(repo, selection, default="")
    if not resolved["ok"]:
        return resolved
    value = _selection_value(str(resolved["source_scope"]), str(resolved["branch"]))
    state = _load_pin_state()
    state.pop("qa", None)
    for key in ("app", "test"):
        if isinstance(state.get(key), dict):
            state[key].pop("bug_tracker", None)
    state[repo_key] = {
        "selection": value,
        "branch": resolved["branch"],
        "source": resolved["source_scope"],
        "ref": resolved["ref"],
        "repo": str(repo),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    _write_pin_state(state)
    return {"ok": True, "repo": repo_key, **state[repo_key], "state_file": str(PINNED_BRANCH_FILE)}


def unpin_branch(repo_key: str) -> dict[str, Any]:
    _repo_for_key(repo_key)
    state = _load_pin_state()
    state.pop(repo_key, None)
    _write_pin_state(state)
    return {"ok": True, "repo": repo_key, "state_file": str(PINNED_BRANCH_FILE)}


def pinned_branch_status() -> dict[str, Any]:
    state = _load_pin_state()
    pins: dict[str, Any] = {"state_file": str(PINNED_BRANCH_FILE)}
    for repo_key in ("app", "test"):
        item = state.get(repo_key)
        if not isinstance(item, dict) or not item.get("selection"):
            pins[repo_key] = {"pinned": False, "repo": str(_repo_for_key(repo_key))}
            continue
        repo = _repo_for_key(repo_key)
        resolved = _resolve_git_ref(repo, str(item["selection"]), default="")
        pin = {key: value for key, value in item.items() if key not in {"commit", "bug_tracker"}}
        pins[repo_key] = {
            **pin,
            "pinned": True,
            "exists": bool(resolved.get("ok")),
            "repo": str(repo),
            "resolved": resolved if resolved.get("ok") else {},
            "error": resolved.get("error", ""),
        }
    return pins


def _branch_name_from_input(raw: str, *, repo_key: str) -> str:
    value = str(raw or "").strip()
    if not value:
        value = f"{repo_key}-{datetime.now().strftime('%Y-%m-%d-%H%M')}"
    if "/" not in value:
        value = f"nullion/{value}"
    check = _run(["git", "check-ref-format", "--branch", value], timeout=4)
    if not check["ok"]:
        raise ValueError(check["stderr"] or check["stdout"] or f"invalid branch name: {value}")
    return value


def create_and_pin_branch(repo_key: str, name: str, base_selection: str) -> dict[str, Any]:
    repo = _repo_for_key(repo_key)
    try:
        branch = _branch_name_from_input(name, repo_key=repo_key)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    base = _resolve_git_ref(repo, base_selection or "origin:main", default="origin:main")
    if not base["ok"]:
        return base
    if not _run(["git", "-C", str(repo), "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], timeout=4)["ok"]:
        created = _run(["git", "-C", str(repo), "branch", branch, str(base["ref"])], timeout=10)
        if not created["ok"]:
            return {"ok": False, "error": created["stderr"] or created["stdout"], "branch": branch}
    pinned = pin_branch(repo_key, f"local:{branch}")
    return {"ok": pinned.get("ok"), "repo": repo_key, "branch": branch, "base": base, "pin": pinned}


def _paired_branch_suffix(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        value = f"paired-{datetime.now().strftime('%Y-%m-%d')}"
    for prefix in ("nullion/", "nullion-test/"):
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


def _paired_branch_create_suffix(raw: str) -> str:
    base = _paired_branch_suffix(raw)
    if _has_short_timestamp_suffix(base):
        return base
    return f"{base}{_short_timestamp_suffix()}"


def _paired_selection(scope: str, branch: str) -> str:
    return f"{scope}:{branch}" if scope in {"local", "local-ref", "origin"} else branch


def pin_paired_work(selection: str) -> dict[str, Any]:
    scope, branch = _split_ref_selection(selection, default="")
    if not branch:
        return {"ok": False, "error": "paired branch selection is required"}
    source_scope = scope if scope in {"local", "local-ref", "origin"} else "local"
    suffix = _paired_branch_suffix(branch)
    app_selection = _paired_selection(source_scope, f"nullion/{suffix}")
    test_selection = _paired_selection(source_scope, f"nullion-test/{suffix}")
    app_resolved = _resolve_git_ref(APP_REPO, app_selection, default="")
    test_resolved = _resolve_git_ref(ROOT, test_selection, default="")
    if not app_resolved.get("ok"):
        return {"ok": False, "error": app_resolved.get("error") or "app branch not found", "app_selection": app_selection}
    if not test_resolved.get("ok"):
        return {"ok": False, "error": test_resolved.get("error") or "test branch not found", "test_selection": test_selection}
    app_pin = pin_branch("app", app_selection)
    if not app_pin.get("ok"):
        return {"ok": False, "error": app_pin.get("error") or "app pin failed", "app": app_pin}
    test_pin = pin_branch("test", test_selection)
    if not test_pin.get("ok"):
        return {"ok": False, "error": test_pin.get("error") or "test pin failed", "app": app_pin, "test": test_pin}
    return {"ok": True, "suffix": suffix, "app": app_pin, "test": test_pin}


def unpin_paired_work() -> dict[str, Any]:
    state = _load_pin_state()
    state.pop("app", None)
    state.pop("test", None)
    state.pop("qa", None)
    _write_pin_state(state)
    return {"ok": True, "state_file": str(PINNED_BRANCH_FILE)}


def create_and_pin_paired_branches(name: str, app_base_selection: str, test_base_selection: str) -> dict[str, Any]:
    suffix = _paired_branch_create_suffix(name)
    if not suffix:
        return {"ok": False, "error": "branch name is required"}
    app = create_and_pin_branch("app", f"nullion/{suffix}", app_base_selection or "origin:main")
    if not app.get("ok"):
        return {"ok": False, "error": app.get("error") or "app branch creation failed", "app": app, "suffix": suffix}
    test = create_and_pin_branch("test", f"nullion-test/{suffix}", test_base_selection or "origin:main")
    if not test.get("ok"):
        return {"ok": False, "error": test.get("error") or "test branch creation failed", "app": app, "test": test, "suffix": suffix}
    return {
        "ok": True,
        "suffix": suffix,
        "app": app,
        "test": test,
    }


def _find_worktree_for_branch(branch: str) -> Path | None:
    return _worktrees_by_branch(APP_REPO).get(branch)


def _split_ref_selection(selection: str, *, default: str) -> tuple[str, str]:
    raw = str(selection or "").strip() or default
    for prefix in ("local-ref:", "local:", "origin:"):
        if raw.startswith(prefix):
            return prefix[:-1], raw.removeprefix(prefix).strip() or default
    return "auto", raw


def _resolve_dev_source(selection: str) -> dict[str, Any]:
    scope, branch = _split_ref_selection(selection, default=_dev_branch())
    current = _git_stdout(["branch", "--show-current"])
    if scope in {"auto", "local"} and branch == current:
        return {"ok": True, "branch": branch, "path": APP_REPO, "created": False}
    existing = _find_worktree_for_branch(branch) if scope in {"auto", "local"} else None
    if existing:
        return {"ok": True, "branch": branch, "path": existing, "created": False}
    resolved = _resolve_git_ref(APP_REPO, selection, default=_dev_branch())
    if not resolved["ok"]:
        return resolved
    DEV_WORKTREE_DIR.mkdir(parents=True, exist_ok=True)
    path = DEV_WORKTREE_DIR / _safe_name(f"{scope}-{branch}")
    if not (path / ".git").exists():
        result = _run(["git", "-C", str(APP_REPO), "worktree", "add", "--detach", str(path), str(resolved["ref"])], timeout=60)
        if not result["ok"]:
            return {"ok": False, "error": result["stderr"] or result["stdout"], "result": result}
    return {"ok": True, "branch": branch, "path": path, "created": True}


def _env_summary(instance: Instance) -> dict[str, Any]:
    keys: dict[str, str] = {}
    if instance.env_file.exists():
        for line in instance.env_file.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.startswith("NULLION_"):
                keys[key] = "set" if value else "empty"
    return {"path": str(instance.env_file), "exists": instance.env_file.exists(), "keys": keys}


def instance_status(instance: Instance) -> dict[str, Any]:
    checkpoint_path = instance.runtime_db
    services = [_launchctl(service) for service in instance.services]
    for item in services:
        item["process"] = _ps(item.get("pid"))
    package = _pip_show(instance)
    runtime_version = _runtime_version(instance)
    editable_source = package.get("editable_source") or ""
    with ThreadPoolExecutor(max_workers=6) as executor:
        probes = {
            "health": executor.submit(_http_json, f"{instance.url}/api/health", timeout=3.0),
            "web_page": executor.submit(_http_probe, f"{instance.url}/", timeout=5.0),
            "websocket": executor.submit(_websocket_probe, instance, timeout=1.5),
            "version_endpoint": executor.submit(_http_json, f"{instance.url}/api/version", timeout=3.0),
            "config_endpoint": executor.submit(_http_json, f"{instance.url}/api/config", timeout=3.0),
        }
        if instance.key != "prod":
            probes["status_endpoint"] = executor.submit(_http_json, f"{instance.url}/api/status", timeout=3.0)
        probe_results = {key: future.result() for key, future in probes.items()}
    if instance.key == "prod":
        probe_results["status_endpoint"] = {
            "ok": True,
            "status": "skipped",
            "elapsed_ms": 0,
            "skipped": True,
            "reason": "prod /api/status probe disabled",
        }
    return {
        "key": instance.key,
        "title": instance.title,
        "url": instance.url,
        "port": instance.port,
        "install_dir": str(instance.install_dir),
        "checkpoint_path": str(checkpoint_path),
        "expected_source": instance.expected_source,
        "source_matches_expected": _instance_source_matches_expected(instance, package),
        **probe_results,
        "package": package,
        "runtime_version": runtime_version,
        "source_git": _instance_source_git(instance, package),
        "services": services,
        "port_listener": _port_listener(instance.port),
        "runtime_db": _db_stats(checkpoint_path),
        "env": _env_summary(instance),
        "logs": {
            service.key: {
                "stdout": _tail(instance.logs_dir / service.log_name, lines=60),
                "stderr": _tail(instance.logs_dir / service.error_log_name, lines=80),
            }
            for service in instance.services
        },
    }


def dashboard_status() -> dict[str, Any]:
    with JOBS_LOCK:
        jobs = {
            job_id: (_action_job_snapshot(job) if job.get("kind") == "action" else _job_snapshot(job))
            for job_id, job in JOBS.items()
        }
    with ThreadPoolExecutor(max_workers=len(INSTANCES)) as executor:
        instance_results = dict(zip(INSTANCES, executor.map(instance_status, INSTANCES)))
    branches = branch_rows()
    release_tags = release_tag_rows()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dashboard": {
            "repo": str(ROOT),
            "app_repo": str(APP_REPO),
            "legacy_stage_launcher": str(TEST_LAUNCHER),
            "dev_worktree_dir": str(DEV_WORKTREE_DIR),
            "stage_source_dir": str(STAGE_SOURCE_DIR),
            "pinned_branch_file": str(PINNED_BRANCH_FILE),
        },
        "pins": pinned_branch_status(),
        "branches": branches,
        "test_branches": branch_rows(ROOT),
        "release_tags": release_tags,
        "prod_sources": _sorted_prod_source_rows(branches, release_tags),
        "jobs": jobs,
        "instances": {instance.key: instance_results[instance] for instance in INSTANCES},
    }


def _dev_branch() -> str:
    branch = _run(["git", "-C", str(APP_REPO), "branch", "--show-current"], timeout=4)["stdout"]
    return branch or "nullion/2026-05-05-cron-reminder-guard"


def _default_app_selection() -> str:
    return pinned_selection("app") or _dev_branch()


def _find_instance_python() -> str | None:
    candidates = [
        os.environ.get("NULLION_TEST_PYTHON", ""),
        shutil.which("python3.13") or "",
        shutil.which("python3.12") or "",
        shutil.which("python3.11") or "",
        shutil.which("python3") or "",
        str(PROD.venv_python),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        result = _run(
            [
                candidate,
                "-c",
                "import sys; raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 14) else 1)",
            ],
            timeout=4,
        )
        if result["ok"]:
            return candidate
    return None


def _ensure_instance_env(instance: Instance) -> dict[str, Any]:
    instance.install_dir.mkdir(parents=True, exist_ok=True)
    instance.logs_dir.mkdir(parents=True, exist_ok=True)
    if not instance.env_file.exists():
        instance.env_file.write_text(
            f"# Nullion isolated {instance.key} instance.\n"
            "NULLION_TELEGRAM_BOT_TOKEN=\n"
            "NULLION_TELEGRAM_OPERATOR_CHAT_ID=\n"
            "NULLION_TELEGRAM_CHAT_ENABLED=true\n"
            "NULLION_TELEGRAM_CHAT_STREAMING_ENABLED=true\n",
            encoding="utf-8",
        )
        os.chmod(instance.env_file, 0o600)
    return {"ok": True}


def _instance_has_telegram_config(instance: Instance) -> bool:
    try:
        env_text = instance.env_file.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return False
    values = _parse_env_text(env_text)
    return (
        _truthy(values.get("NULLION_TELEGRAM_CHAT_ENABLED"))
        and bool(values.get("NULLION_TELEGRAM_BOT_TOKEN", "").strip())
        and bool(values.get("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "").strip())
    )


def _parse_env_text(env_text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in env_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _ensure_instance_venv(instance: Instance) -> dict[str, Any]:
    env = _ensure_instance_env(instance)
    if not env["ok"]:
        return env
    python = _find_instance_python()
    if not python:
        return {"ok": False, "error": f"could not find Python 3.11 through 3.13 for {instance.key} venv"}
    if instance.venv_python.exists():
        version_ok = _run(
            [
                str(instance.venv_python),
                "-c",
                "import sys; raise SystemExit(0 if (3, 11) <= sys.version_info < (3, 14) else 1)",
            ],
            timeout=4,
        )
        if not version_ok["ok"]:
            shutil.rmtree(instance.install_dir / "venv", ignore_errors=True)
    if not instance.venv_python.exists():
        created = _run([python, "-m", "venv", str(instance.install_dir / "venv")], timeout=60)
        if not created["ok"]:
            return {"ok": False, "error": created["stderr"] or created["stdout"], "result": created}
    upgraded = _run([str(instance.venv_python), "-m", "pip", "install", "--quiet", "--upgrade", "pip", "setuptools", "wheel"], timeout=120)
    if not upgraded["ok"]:
        return {"ok": False, "error": upgraded["stderr"] or upgraded["stdout"], "result": upgraded}
    return {"ok": True}


def _plist_for(instance: Instance, service: Service) -> Path:
    return LAUNCH_AGENTS_DIR / f"{service.label}.plist"


def _write_instance_plists(instance: Instance) -> None:
    LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    instance.logs_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = instance.runtime_db
    common_env = (
        f"    <key>NULLION_HOME</key><string>{instance.install_dir}</string>\n"
        f"    <key>NULLION_ENV_FILE</key><string>{instance.env_file}</string>\n"
        f"    <key>PATH</key><string>{instance.install_dir}/venv/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>\n"
    )
    specs = {
        "web": [
            str(instance.venv_python),
            "-m",
            "nullion.web_app",
            "--port",
            str(instance.port),
            "--checkpoint",
            str(checkpoint_path),
        ],
        "tray": [str(instance.install_dir / "venv" / "bin" / "nullion-tray"), "--port", str(instance.port), "--env-file", str(instance.env_file)],
        "telegram": [
            str(instance.install_dir / "venv" / "bin" / "nullion-telegram"),
            "--checkpoint",
            str(checkpoint_path),
            "--env-file",
            str(instance.env_file),
        ],
    }
    for service in instance.services:
        args = "\n".join(f"    <string>{arg}</string>" for arg in specs[service.key])
        keepalive = "<true/>" if service.key == "web" else "<dict><key>SuccessfulExit</key><false/></dict>"
        throttle = "10" if service.key != "telegram" else "5"
        _plist_for(instance, service).write_text(
            f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>{service.label}</string>
  <key>ProgramArguments</key><array>
{args}
  </array>
  <key>WorkingDirectory</key><string>{instance.install_dir}</string>
  <key>EnvironmentVariables</key><dict>
{common_env}  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key>{keepalive}
  <key>ThrottleInterval</key><integer>{throttle}</integer>
  <key>StandardOutPath</key><string>{instance.logs_dir / service.log_name}</string>
  <key>StandardErrorPath</key><string>{instance.logs_dir / service.error_log_name}</string>
</dict></plist>
""",
            encoding="utf-8",
        )


def _bootout_service(service: Service, *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    return _run_step(job, f"Stop {service.key} service", ["launchctl", "bootout", f"gui/{UID}/{service.label}"], timeout=8)


def _wait_for_service_stopped(service: Service, *, timeout: float = 10.0) -> dict[str, Any]:
    started = time.time()
    last: dict[str, Any] = {}
    while time.time() - started < timeout:
        last = _launchctl(service)
        if not last.get("loaded") or (last.get("state") != "running" and not last.get("pid")):
            return {"ok": True, "elapsed_ms": round((time.time() - started) * 1000), "last": last}
        time.sleep(0.2)
    return {"ok": False, "elapsed_ms": round((time.time() - started) * 1000), "last": last}


def _wait_for_service_running(service: Service, *, timeout: float = 12.0) -> dict[str, Any]:
    started = time.time()
    last: dict[str, Any] = {}
    while time.time() - started < timeout:
        last = _launchctl(service)
        if last.get("state") == "running" and last.get("pid"):
            return {"ok": True, "elapsed_ms": round((time.time() - started) * 1000), "last": last}
        time.sleep(0.2)
    return {"ok": False, "elapsed_ms": round((time.time() - started) * 1000), "last": last}


def _service_process_matchers(instance: Instance, service: Service) -> tuple[str, ...]:
    venv_bin = instance.install_dir / "venv" / "bin"
    if service.key == "web":
        return (f"{instance.venv_python} -m nullion.web_app",)
    if service.key == "tray":
        return (
            str(venv_bin / "nullion-tray"),
            f"{instance.venv_python} -m nullion.webview_app",
        )
    if service.key == "telegram":
        return (str(venv_bin / "nullion-telegram"),)
    return ()


def _matching_service_pids(instance: Instance, service: Service) -> list[str]:
    matchers = _service_process_matchers(instance, service)
    if not matchers:
        return []
    result = _run(["ps", "-axo", "pid=,command="], timeout=4)
    if not result["ok"]:
        return []
    pids: list[str] = []
    for line in result["stdout"].splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pid, _, command = stripped.partition(" ")
        if pid == str(os.getpid()):
            continue
        if any(matcher in command for matcher in matchers):
            pids.append(pid)
    return pids


def _terminate_stale_service_processes(instance: Instance, service: Service) -> dict[str, Any]:
    pids = _matching_service_pids(instance, service)
    if not pids:
        return {"ok": True, "pids": []}
    term = [_run(["kill", "-TERM", pid], timeout=4) for pid in pids]
    started = time.time()
    remaining: list[str] = pids
    while time.time() - started < 5:
        remaining = [pid for pid in _matching_service_pids(instance, service) if pid in pids]
        if not remaining:
            return {"ok": True, "pids": pids, "term": term, "elapsed_ms": round((time.time() - started) * 1000)}
        time.sleep(0.2)
    kill = [_run(["kill", "-KILL", pid], timeout=4) for pid in remaining]
    remaining = [pid for pid in _matching_service_pids(instance, service) if pid in pids]
    return {"ok": not remaining, "pids": pids, "term": term, "kill": kill, "remaining": remaining}


def _bootstrap_with_retry(instance: Instance, service: Service, plist: Path, *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    last: dict[str, Any] = {"ok": False, "stdout": "", "stderr": "launchctl bootstrap was not attempted", "returncode": 1}
    for attempt in range(4):
        last = _run_step(job, f"Load {service.key} service", ["launchctl", "bootstrap", f"gui/{UID}", str(plist)], timeout=8)
        if last["ok"] or _launchctl(service).get("loaded"):
            return last
        if attempt < 3:
            time.sleep(0.4)
    return last


def _bootstrap_service(instance: Instance, service: Service, *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    plist = _plist_for(instance, service)
    stale = _terminate_stale_service_processes(instance, service)
    boot = _bootstrap_with_retry(instance, service, plist, job=job)
    kick = _run_step(job, f"Kick {service.key} service", ["launchctl", "kickstart", "-k", f"gui/{UID}/{service.label}"], timeout=8)
    if not kick["ok"] and not _launchctl(service).get("loaded"):
        boot = _bootstrap_with_retry(instance, service, plist, job=job)
        kick = _run_step(job, f"Kick {service.key} service", ["launchctl", "kickstart", "-k", f"gui/{UID}/{service.label}"], timeout=8)
    running = _wait_for_service_running(service)
    return {"ok": stale["ok"] and running["ok"], "stale": stale, "bootstrap": boot, "kickstart": kick, "running": running}


def _port_listener_pids(instance: Instance) -> list[str]:
    if shutil.which("lsof") is None:
        return []
    return _run(["lsof", f"-tiTCP:{instance.port}", "-sTCP:LISTEN"], timeout=4)["stdout"].split()


def _kill_instance_port(instance: Instance, *, signal: str = "TERM") -> dict[str, Any]:
    pids = _port_listener_pids(instance)
    results = []
    for pid in pids:
        results.append(_run(["kill", f"-{signal}", pid], timeout=4))
    return {"ok": True, "pids": pids, "results": results}


def _wait_for_port_free(instance: Instance, *, timeout: float = 10.0) -> dict[str, Any]:
    started = time.time()
    pids: list[str] = []
    while time.time() - started < timeout:
        pids = _port_listener_pids(instance)
        if not pids:
            return {"ok": True, "elapsed_ms": round((time.time() - started) * 1000), "pids": []}
        time.sleep(0.2)
    return {"ok": False, "elapsed_ms": round((time.time() - started) * 1000), "pids": pids}


def stop_instance(instance: Instance, *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    results = [_bootout_service(service, job=job) for service in reversed(instance.services)]
    stopped = [_wait_for_service_stopped(service) for service in instance.services]
    stale = [_terminate_stale_service_processes(instance, service) for service in instance.services]
    port = _kill_instance_port(instance, signal="TERM")
    port_free = _wait_for_port_free(instance)
    port_kill: dict[str, Any] | None = None
    if not port_free["ok"]:
        port_kill = _kill_instance_port(instance, signal="KILL")
        port_free = _wait_for_port_free(instance, timeout=4)
    ok = all(item["ok"] for item in stopped) and all(item["ok"] for item in stale) and bool(port_free["ok"])
    return {
        "ok": ok,
        "instance": instance.key,
        "results": results,
        "stopped": stopped,
        "stale": stale,
        "port": port,
        "port_kill": port_kill,
        "port_free": port_free,
    }


def _resolve_app_ref(selection: str) -> dict[str, Any]:
    return _resolve_git_ref(APP_REPO, selection, default="origin:main")


def _assert_instance_source_isolation(instance: Instance, source: dict[str, Any]) -> dict[str, Any] | None:
    source_path = Path(str(source.get("path") or "")).resolve()
    if instance.key == "stage":
        expected = STAGE_SOURCE_DIR.resolve()
        if source_path != expected:
            return {
                "ok": False,
                "error": f"stage must use dedicated source {expected}, got {source_path}",
                "source": str(source.get("path")),
                "source_mode": source.get("source_mode"),
            }
    if instance.key == "prod" and source_path != Path(instance.expected_source).resolve():
        return {
            "ok": False,
            "error": f"prod must use release source {instance.expected_source}, got {source_path}",
            "source": str(source.get("path")),
            "source_mode": source.get("source_mode"),
        }
    return None


def _prepare_stage_source(branch: str = "origin:main", *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    scope, branch_name = _split_ref_selection(branch, default="origin:main")
    if scope in {"auto", "local"}:
        _action_job_event(job, f"Forcing stage to dedicated source {STAGE_SOURCE_DIR}") if job is not None else None
    fetch = _run_step(job, "Fetch origin", ["git", "-C", str(APP_REPO), "fetch", "--prune", "origin"], timeout=60)
    if not fetch["ok"]:
        return {"ok": False, "error": fetch["stderr"] or fetch["stdout"], "fetch": fetch}
    resolved = _resolve_app_ref(branch)
    if not resolved["ok"]:
        return {"ok": False, "error": resolved["error"], "fetch": _compact_result(fetch)}
    checkout_ref = str(resolved["ref"])
    STAGE_SOURCE_DIR.parent.mkdir(parents=True, exist_ok=True)
    if not (STAGE_SOURCE_DIR / ".git").exists():
        added = _run_step(job, "Create stage source worktree", ["git", "-C", str(APP_REPO), "worktree", "add", "--detach", str(STAGE_SOURCE_DIR), checkout_ref], timeout=60)
        if not added["ok"]:
            return {"ok": False, "error": added["stderr"] or added["stdout"], "worktree": added}
    checkout = _run_step(job, "Checkout stage source", ["git", "-C", str(STAGE_SOURCE_DIR), "checkout", "--detach", checkout_ref], timeout=30)
    reset = _run_step(job, "Reset stage source", ["git", "-C", str(STAGE_SOURCE_DIR), "reset", "--hard", checkout_ref], timeout=30)
    clean = _run_step(job, "Clean stage source", ["git", "-C", str(STAGE_SOURCE_DIR), "clean", "-fd"], timeout=30)
    commit = _run_step(job, "Read stage source commit", ["git", "-C", str(STAGE_SOURCE_DIR), "rev-parse", "--short=9", "HEAD"], timeout=4)
    return {
        "ok": checkout["ok"] and reset["ok"],
        "branch": str(resolved["branch"]),
        "ref": checkout_ref,
        "path": STAGE_SOURCE_DIR,
        "commit": commit["stdout"],
        "source_mode": str(resolved.get("source_scope") or "worktree"),
        "fetch": _compact_result(fetch),
        "checkout": _compact_result(checkout),
        "reset": _compact_result(reset),
        "clean": _compact_result(clean),
    }


def _prepare_stage_local_checkout(*, job: dict[str, Any] | None = None) -> dict[str, Any]:
    if not (APP_REPO / ".git").exists():
        return {"ok": False, "error": f"app repo is not a git checkout: {APP_REPO}"}
    commit = _run_step(job, "Read current checkout commit", ["git", "-C", str(APP_REPO), "rev-parse", "--short=9", "HEAD"], timeout=4)
    branch = _run_step(job, "Read current checkout branch", ["git", "-C", str(APP_REPO), "branch", "--show-current"], timeout=4)
    status = _run_step(job, "Read current checkout status", ["git", "-C", str(APP_REPO), "status", "--short"], timeout=4)
    return {
        "ok": commit["ok"],
        "branch": branch["stdout"] or "HEAD",
        "ref": "local-checkout",
        "path": APP_REPO,
        "commit": commit["stdout"],
        "dirty_count": len([line for line in status["stdout"].splitlines() if line.strip()]),
        "source_mode": "local_checkout",
    }


def _start_instance(
    instance: Instance,
    source: dict[str, Any],
    *,
    job: dict[str, Any] | None = None,
    skip_source_isolation: bool = False,
) -> dict[str, Any]:
    if not source["ok"]:
        return source
    if not skip_source_isolation:
        source_guard = _assert_instance_source_isolation(instance, source)
        if source_guard is not None:
            return source_guard
    elif instance.key == "prod":
        source_path = Path(str(source.get("path") or "")).resolve()
        expected = Path(instance.expected_source).resolve()
        if source_path == expected:
            return {
                "ok": False,
                "error": "local prod testing must not point at release source; use a local branch/worktree",
                "source": str(source.get("path")),
                "source_mode": source.get("source_mode"),
            }
    if job is not None:
        _action_job_event(job, f"Starting {instance.title} from {source['path']}")
    venv = _ensure_instance_venv(instance)
    if not venv["ok"]:
        return venv
    _write_instance_plists(instance)
    if job is not None:
        _action_job_event(job, f"Wrote {instance.title} launchd plists")
    stop = stop_instance(instance, job=job)
    if not stop["ok"]:
        return {"ok": False, "error": f"{instance.title} did not fully stop before start", "stop": stop}
    install = _run_step(job, "Install editable package", [str(instance.venv_python), "-m", "pip", "install", "--quiet", "--no-cache-dir", "-e", str(source["path"])], timeout=180)
    if not install["ok"]:
        return {"ok": False, "error": install["stderr"] or install["stdout"], "install": install}
    imported = _run_step(
        job,
        "Verify import source",
        [
            str(instance.venv_python),
            "-c",
            "import nullion; from pathlib import Path; print(Path(nullion.__file__).resolve())",
        ],
        timeout=8,
    )
    expected = Path(source["path"]) / "src" / "nullion" / "__init__.py"
    if imported["stdout"] != str(expected):
        return {"ok": False, "error": f"{instance.title} import points to unexpected source", "expected": str(expected), "actual": imported["stdout"]}
    migrations = _run_step(
        job,
        "Run post-update migrations",
        [
            str(instance.venv_python),
            "-c",
            "import os; from nullion.updater import run_post_update_migrations; "
            f"print(run_post_update_migrations(env_path={str(instance.env_file)!r}, checkpoint_path={str(instance.runtime_db)!r}, install_dir={str(instance.install_dir)!r}))",
        ],
        timeout=30,
    )
    launches = [
        (_action_job_event(job, f"Restart {service.key} service") if job is not None else None) or _bootstrap_service(instance, service, job=job)
        for service in instance.services
        if service.key != "telegram" or _instance_has_telegram_config(instance)
    ]
    ready = False
    for _ in range(80):
        if _http_json(f"{instance.url}/api/config", timeout=0.5)["ok"]:
            ready = True
            break
        if job is not None and _ % 10 == 0:
            _action_job_event(job, f"Waiting for {instance.title} health endpoint")
        time.sleep(0.25)
    services_ready = all(item.get("ok") for item in launches)
    webview = open_webview(instance) if ready and services_ready else {"ok": False, "skipped": True}
    message = (
        f"{instance.title} started on {source['branch']}"
        if ready and services_ready
        else f"{instance.title} did not become fully ready on {source['branch']}"
    )
    return {
        "ok": ready and services_ready,
        "message": message,
        "instance": instance.key,
        "branch": source["branch"],
        "ref": source.get("ref"),
        "checkpoint": str(instance.runtime_db),
        "source": str(source["path"]),
        "commit": source.get("commit"),
        "dirty_count": source.get("dirty_count"),
        "source_mode": source.get("source_mode") or "worktree",
        "install": _compact_result(install),
        "imported": imported["stdout"],
        "migrations": _compact_result(migrations),
        "launches": launches,
        "stop": stop,
        "webview": webview,
        "url": instance.url,
    }


def start_dev(branch: str, *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    return _start_instance(DEV, _resolve_dev_source(branch), job=job)


def start_stage(branch: str = "origin:main", *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    return _start_instance(STAGE, _prepare_stage_source(branch, job=job), job=job)


def start_stage_local_checkout(*, job: dict[str, Any] | None = None) -> dict[str, Any]:
    return _start_instance(STAGE, _prepare_stage_local_checkout(job=job), job=job)


def start_prod(
    selection: str = "",
    *,
    job: dict[str, Any] | None = None,
    allow_local: bool = False,
) -> dict[str, Any]:
    raw = str(selection or "").strip()
    tag = raw.split(":", 1)[1].strip() if raw.startswith(("release:", "tag:")) else raw
    if raw.startswith(("local:", "local-ref:", "auto:")) and not allow_local:
        return {
            "ok": False,
            "error": "prod local-source testing requires checking 'Allow local source for prod'",
            "selection": raw,
        }
    if not raw or re.fullmatch(r"v\d+(?:\.\d+){1,3}(?:[-+][A-Za-z0-9._-]+)?", tag):
        return reset_prod_to_release(raw, job=job, release_only=True)
    scope, _branch = _split_ref_selection(raw, default="")
    if scope in {"auto", "local", "local-ref"}:
        source = _resolve_dev_source(raw)
        if not source["ok"]:
            return source
        return _start_instance(PROD, source, job=job, skip_source_isolation=allow_local)
    return reset_prod_to_release(raw, job=job, release_only=False)


def reset_prod_to_release(
    release: str = "",
    *,
    job: dict[str, Any] | None = None,
    release_only: bool = True,
) -> dict[str, Any]:
    source = Path(PROD.expected_source)
    if not (source / ".git").exists():
        return {"ok": False, "error": f"prod source is not a git checkout: {source}"}
    selected = _resolve_prod_selection(release, source, release_only=release_only)
    if not selected.get("ok"):
        return selected
    ref = str(selected["ref"])
    label = str(selected["branch"])
    if selected.get("fetch") == "tags":
        fetch = _run_step(job, "Fetch prod release tags", ["git", "-C", str(source), "fetch", "--tags", "--prune", "origin"], timeout=60)
        if not _run(["git", "-C", str(source), "show-ref", "--verify", "--quiet", f"refs/tags/{ref}"], timeout=4)["ok"]:
            return {"ok": False, "error": f"release tag not found: {ref}", "fetch": _compact_result(fetch)}
    elif selected.get("fetch") == "app_checkout":
        fetch = _run_step(
            job,
            f"Fetch app checkout source {label}",
            ["git", "-C", str(source), "fetch", "--no-tags", str(APP_REPO), str(selected["fetch_ref"])],
            timeout=60,
        )
        if not fetch["ok"]:
            return {"ok": False, "error": fetch["stderr"] or fetch["stdout"] or f"failed to fetch {label}", "fetch": _compact_result(fetch)}
    elif selected.get("fetch") == "origin":
        fetch = _run_step(job, f"Fetch prod branch {label}", ["git", "-C", str(source), "fetch", "--prune", "origin", label], timeout=60)
        if not fetch["ok"]:
            return {"ok": False, "error": fetch["stderr"] or fetch["stdout"] or f"failed to fetch {label}", "fetch": _compact_result(fetch)}
    else:
        fetch = {"ok": True, "stdout": "", "stderr": "", "returncode": 0}
    status_before = _run_step(job, "Read prod source status", ["git", "-C", str(source), "status", "--short"], timeout=4)
    snapshot_path = ""
    if status_before["stdout"]:
        snapshot_dir = PROD.install_dir / "repair-snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = str(snapshot_dir / f"prod-reset-dirty-{datetime.now().strftime('%Y%m%d-%H%M%S')}.patch")
        diff = _run_step(job, "Snapshot prod dirty diff", ["git", "-C", str(source), "diff"], timeout=10)
        Path(snapshot_path).write_text(diff["stdout"], encoding="utf-8")
    checkout = _run_step(job, f"Checkout prod source {label}", ["git", "-C", str(source), "checkout", "--detach", ref], timeout=30)
    reset = _run_step(job, f"Hard reset prod source {label}", ["git", "-C", str(source), "reset", "--hard", ref], timeout=30)
    clean = _run_step(job, "Clean prod source untracked files", ["git", "-C", str(source), "clean", "-fd"], timeout=30)
    if not (checkout["ok"] and reset["ok"] and clean["ok"]):
        return {
            "ok": False,
            "error": "Prod source checkout/reset/clean failed before restart",
            "checkout": _compact_result(checkout),
            "reset": _compact_result(reset),
            "clean": _compact_result(clean),
            "source": str(source),
            "branch": label,
            "source_mode": selected["source_mode"],
        }
    stop = stop_instance(PROD, job=job)
    if not stop["ok"]:
        return {"ok": False, "error": "Prod did not fully stop before restart", "stop": stop}
    _write_instance_plists(PROD)
    if job is not None:
        _action_job_event(job, "Rewrote prod launchd plists")
    install = _install_prod_from_release_source(job=job)
    if not install["ok"]:
        return {"ok": False, "error": install.get("error") or "Prod package reinstall failed", "install": install}
    post_install_clean = _run_step(job, "Clean prod source after package install", ["git", "-C", str(source), "clean", "-fd"], timeout=30)
    if not post_install_clean["ok"]:
        return {
            "ok": False,
            "error": "Prod source did not clean after reinstall",
            "clean": _compact_result(post_install_clean),
            "install": install,
            "source": str(source),
            "branch": label,
            "source_mode": selected["source_mode"],
        }
    migrations = _run_step(
        job,
        "Run prod post-update migrations",
        [
            str(PROD.venv_python),
            "-c",
            "from nullion.updater import run_post_update_migrations; "
            f"print(run_post_update_migrations(env_path={str(PROD.env_file)!r}, checkpoint_path={str(PROD.runtime_db)!r}, install_dir={str(PROD.install_dir)!r}))",
        ],
        timeout=30,
    )
    launches = [
        (_action_job_event(job, f"Restart prod {service.key} service") if job is not None else None) or _bootstrap_service(PROD, service, job=job)
        for service in PROD.services
        if service.key != "telegram" or _instance_has_telegram_config(PROD)
    ]
    imported = _run_step(
        job,
        "Verify prod import source",
        [
            str(PROD.venv_python),
            "-c",
            "import nullion; from pathlib import Path; from nullion.version import version_tag; "
            "print(str(Path(nullion.__file__).resolve()) + '|' + version_tag())",
        ],
        timeout=8,
    )
    status_after = _run_step(job, "Read clean prod source status", ["git", "-C", str(source), "status", "--short", "--branch"], timeout=4)
    ready = False
    for _ in range(80):
        if _http_probe(f"{PROD.url}/api/health", timeout=0.5, max_bytes=4096)["ok"]:
            ready = True
            break
        if job is not None and _ % 10 == 0:
            _action_job_event(job, "Waiting for prod health endpoint")
        time.sleep(0.25)
    services_ready = all(item.get("ok") for item in launches)
    webview = open_webview(PROD) if ready and services_ready else {"ok": False, "skipped": True}
    return {
        "ok": ready and services_ready and checkout["ok"] and reset["ok"] and clean["ok"] and post_install_clean["ok"] and install["ok"],
        "message": f"Prod reset to {label}" if ready and services_ready else f"Prod reset to {label}, but services did not become fully ready",
        "instance": "prod",
        "branch": label,
        "ref": ref,
        "source_mode": selected["source_mode"],
        "source": str(source),
        "checkpoint": str(PROD.runtime_db),
        "snapshot": snapshot_path,
        "checkout": _compact_result(checkout),
        "reset": _compact_result(reset),
        "clean": _compact_result(clean),
        "post_install_clean": _compact_result(post_install_clean),
        "install": install,
        "migrations": _compact_result(migrations),
        "launches": launches,
        "stop": stop,
        "webview": webview,
        "imported": imported["stdout"],
        "status": status_after["stdout"],
        "url": PROD.url,
    }


def copy_prod_db_to_stage() -> dict[str, Any]:
    stop = stop_instance(STAGE)
    STAGE.install_dir.mkdir(parents=True, exist_ok=True)
    backup_path = None
    if STAGE.runtime_db.exists():
        backup_path = STAGE.runtime_db.with_name(f"runtime.db.before-prod-copy-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
        shutil.copy2(STAGE.runtime_db, backup_path)
    code = (
        "import sqlite3; "
        f"src=sqlite3.connect({str(PROD.runtime_db)!r}, uri=False); "
        f"dst=sqlite3.connect({str(STAGE.runtime_db)!r}); "
        "src.backup(dst); src.close(); dst.close()"
    )
    copied = _run(["python3", "-c", code], timeout=60)
    return {
        "ok": copied["ok"],
        "message": "Copied prod runtime DB to stage." if copied["ok"] else copied["stderr"] or copied["stdout"],
        "instance": "stage",
        "source": str(PROD.runtime_db),
        "target": str(STAGE.runtime_db),
        "backup": str(backup_path) if backup_path else "",
        "stop": stop,
        "copy": _compact_result(copied),
    }


def cleanup_merged_branches() -> dict[str, Any]:
    _run(["git", "-C", str(APP_REPO), "fetch", "--prune", "origin"], timeout=30)
    current = _git_stdout(["branch", "--show-current"])
    removed: list[str] = []
    skipped: list[dict[str, str]] = []
    for row in branch_rows():
        if row.get("source") != "local":
            skipped.append({"branch": row["display"], "reason": "remote ref"})
            continue
        branch = row["branch"]
        if branch in {"main", current} or not row.get("merged_to_main"):
            skipped.append({"branch": branch, "reason": "current/main/unmerged"})
            continue
        worktree = _find_worktree_for_branch(branch)
        if worktree:
            dirty = _run(["git", "-C", str(worktree), "status", "--porcelain"], timeout=4)["stdout"]
            if dirty.strip():
                skipped.append({"branch": branch, "reason": f"dirty worktree {worktree}"})
                continue
            _run(["git", "-C", str(APP_REPO), "worktree", "remove", str(worktree)], timeout=30)
        deleted = _run(["git", "-C", str(APP_REPO), "branch", "-d", branch], timeout=10)
        if deleted["ok"]:
            removed.append(branch)
        else:
            skipped.append({"branch": branch, "reason": deleted["stderr"] or deleted["stdout"]})
    return {"ok": True, "removed": removed, "skipped": skipped}


def cleanup_branch(branch: str) -> dict[str, Any]:
    scope, branch = _split_ref_selection(branch, default="")
    if scope not in {"auto", "local"}:
        return {"ok": False, "error": "cleanup only removes local branches"}
    if not branch:
        return {"ok": False, "error": "choose a branch first"}
    current = _git_stdout(["branch", "--show-current"])
    if branch in {"main", current}:
        return {"ok": False, "error": "refusing to delete main or the current app branch"}
    merged = _run(["git", "-C", str(APP_REPO), "merge-base", "--is-ancestor", branch, "main"], timeout=4)["ok"]
    if not merged:
        return {"ok": False, "error": f"{branch} is not merged to main"}
    worktree = _find_worktree_for_branch(branch)
    if worktree:
        dirty = _run(["git", "-C", str(worktree), "status", "--porcelain"], timeout=4)["stdout"]
        if dirty.strip():
            return {"ok": False, "error": f"worktree is dirty: {worktree}"}
        _run(["git", "-C", str(APP_REPO), "worktree", "remove", str(worktree)], timeout=30)
    deleted = _run(["git", "-C", str(APP_REPO), "branch", "-d", branch], timeout=10)
    return {"ok": deleted["ok"], "branch": branch, "result": deleted}


def _job_snapshot(job: dict[str, Any]) -> dict[str, Any]:
    log_path = Path(job["log_path"])
    snapshot = dict(job)
    snapshot["log_tail"] = _tail(log_path, lines=120).get("text", "")
    return snapshot


def _start_background_job(kind: str, command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    job_id = uuid4().hex[:10]
    log_dir = ROOT / ".e2e-artifacts" / "local-dashboard"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{job_id}.log"
    job = {
        "id": job_id,
        "kind": kind,
        "status": "running",
        "command": command,
        "cwd": str(cwd),
        "log_path": str(log_path),
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "finished_at": None,
        "returncode": None,
    }
    with JOBS_LOCK:
        JOBS[job_id] = job

    def runner() -> None:
        run_env = os.environ.copy()
        if env:
            run_env.update(env)
        with log_path.open("w", encoding="utf-8", errors="replace") as handle:
            handle.write("$ " + " ".join(command) + "\n\n")
            handle.flush()
            proc = subprocess.Popen(command, cwd=str(cwd), env=run_env, stdout=handle, stderr=subprocess.STDOUT, text=True)
            code = proc.wait()
        with JOBS_LOCK:
            job["status"] = "passed" if code == 0 else "failed"
            job["finished_at"] = datetime.now().isoformat(timespec="seconds")
            job["returncode"] = code

    threading.Thread(target=runner, daemon=True).start()
    return {"ok": True, "job": _job_snapshot(job)}


def start_e2e(branch: str, target: str) -> dict[str, Any]:
    source = _resolve_dev_source(branch or _default_app_selection())
    if not source["ok"]:
        return source
    command = [str(ROOT / "scripts" / "e2e-docker.sh")]
    if target.strip():
        command.append(target.strip())
    return _start_background_job("e2e", command, cwd=ROOT, env={"NULLION_APP_REPO": str(source["path"])})


def open_webview(instance: Instance) -> dict[str, Any]:
    python = instance.venv_python
    if not python.exists():
        return {"ok": False, "error": f"missing {python}"}
    env = os.environ.copy()
    env["NULLION_HOME"] = str(instance.install_dir)
    env["NULLION_ENV_FILE"] = str(instance.env_file)
    command = [
        str(python),
        "-m",
        "nullion.webview_app",
        "--host",
        "127.0.0.1",
        "--port",
        str(instance.port),
        "--env-file",
        str(instance.env_file),
        "--browser-fallback",
    ]
    started = time.time()
    try:
        subprocess.Popen(command, cwd=str(instance.install_dir), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return {
            "ok": True,
            "message": f"Opened {instance.title} web view",
            "elapsed_ms": round((time.time() - started) * 1000),
            "cmd": command,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "cmd": command}


def action_summary(name: str, result: dict[str, Any]) -> dict[str, Any]:
    if name in {"start_dev", "restart_dev", "start_stage", "restart_stage", "start_stage_local", "restart_stage_local", "restart_prod", "reset_prod"}:
        title = str(result.get("instance") or name).title()
        return {
            "ok": result.get("ok"),
            "title": f"{title} updated" if result.get("ok") else f"{title} update failed",
            "message": result.get("message") or result.get("error") or "",
            "details": {
                "branch": result.get("branch"),
                "ref": result.get("ref"),
                "source_mode": result.get("source_mode"),
                "dirty_count": result.get("dirty_count"),
                "checkpoint": result.get("checkpoint"),
                "source": result.get("source"),
                "url": result.get("url"),
                "imported": result.get("imported"),
                "snapshot": result.get("snapshot"),
                "webview": result.get("webview"),
            },
        }
    if name == "copy_prod_db_to_stage":
        return {
            "ok": result.get("ok"),
            "title": "Stage DB copied" if result.get("ok") else "Stage DB copy failed",
            "message": result.get("message") or result.get("error") or "",
            "details": {
                "source": result.get("source"),
                "target": result.get("target"),
                "backup": result.get("backup"),
            },
        }
    if name == "stop_dev":
        return {"ok": result.get("ok"), "title": "Dev stopped", "message": "Dev services were stopped.", "details": {}}
    if name == "stop_stage":
        return {"ok": result.get("ok"), "title": "Stage stopped", "message": "Stage services were stopped.", "details": {}}
    if name == "start_e2e":
        job = result.get("job") or {}
        return {
            "ok": result.get("ok"),
            "title": "E2E started" if result.get("ok") else "E2E failed to start",
            "message": f"Job {job.get('id', '')} is running." if result.get("ok") else result.get("error", ""),
            "details": {"job_id": job.get("id"), "log": job.get("log_path")},
        }
    if name == "restart_ops_dashboard":
        return {
            "ok": result.get("ok"),
            "title": "Dashboard restart queued" if result.get("ok") else "Dashboard restart failed",
            "message": result.get("message") or result.get("error") or "",
            "details": {"label": LOCAL_OPS_LAUNCHD_LABEL},
        }
    if name in {"pin_work_branch", "create_paired_branches"}:
        app = result.get("app") or {}
        test = result.get("test") or {}
        return {
            "ok": result.get("ok"),
            "title": "Work branch pinned" if result.get("ok") else "Work branch setup failed",
            "message": result.get("error") or str(result.get("suffix") or ""),
            "details": {
                "app_branch": app.get("branch") or app.get("pin", {}).get("branch"),
                "test_branch": test.get("branch") or test.get("pin", {}).get("branch"),
            },
        }
    if name == "unpin_work_branch":
        return {"ok": result.get("ok"), "title": "Work branch pins cleared", "message": result.get("error") or "", "details": result}
    if name.startswith("pin_") or name.startswith("unpin_"):
        return {
            "ok": result.get("ok"),
            "title": "Branch pin updated" if result.get("ok") else "Branch pin failed",
            "message": result.get("error") or str(result.get("selection") or ""),
            "details": result,
        }
    if name.startswith("create_"):
        pin = result.get("pin") or {}
        return {
            "ok": result.get("ok"),
            "title": "Branch created and pinned" if result.get("ok") else "Branch create failed",
            "message": result.get("error") or str(result.get("branch") or ""),
            "details": {
                "branch": result.get("branch"),
                "selection": pin.get("selection"),
                "state_file": pin.get("state_file"),
            },
        }
    if name.startswith("open_"):
        return {"ok": result.get("ok"), "title": "Opened" if result.get("ok") else "Open failed", "message": result.get("message") or result.get("error") or "", "details": {}}
    if name.startswith("cleanup"):
        return {
            "ok": result.get("ok"),
            "title": "Cleanup complete" if result.get("ok") else "Cleanup blocked",
            "message": f"Removed {len(result.get('removed', []))} branch(es)." if "removed" in result else result.get("error", ""),
            "details": result,
        }
    return {"ok": result.get("ok"), "title": name, "message": result.get("error") or result.get("message") or "", "details": {}}


def action(name: str, payload: dict[str, Any], *, job: dict[str, Any] | None = None) -> dict[str, Any]:
    raw: dict[str, Any]
    if name == "open_prod":
        raw = _run(["open", PROD.url], timeout=4)
    elif name == "restart_ops_dashboard":
        raw = restart_ops_dashboard()
    elif name == "open_dev":
        raw = _run(["open", DEV.url], timeout=4)
    elif name == "open_stage":
        raw = _run(["open", STAGE.url], timeout=4)
    elif name == "open_prod_webview":
        raw = open_webview(PROD)
    elif name == "open_dev_webview":
        raw = open_webview(DEV)
    elif name == "open_stage_webview":
        raw = open_webview(STAGE)
    elif name == "open_prod_logs":
        raw = _run(["open", str(PROD.logs_dir)], timeout=4)
    elif name == "open_dev_logs":
        raw = _run(["open", str(DEV.logs_dir)], timeout=4)
    elif name == "open_stage_logs":
        raw = _run(["open", str(STAGE.logs_dir)], timeout=4)
    elif name == "start_dev":
        raw = start_dev(str(payload.get("branch") or _default_app_selection()).strip(), job=job)
    elif name == "stop_dev":
        raw = stop_instance(DEV)
    elif name == "restart_dev":
        raw = start_dev(str(payload.get("branch") or _default_app_selection()).strip(), job=job)
    elif name == "start_stage":
        raw = start_stage(str(payload.get("stage_branch") or payload.get("branch") or "origin:main").strip(), job=job)
    elif name == "stop_stage":
        raw = stop_instance(STAGE)
    elif name == "restart_stage":
        raw = start_stage(str(payload.get("stage_branch") or payload.get("branch") or "origin:main").strip(), job=job)
    elif name == "start_stage_local":
        raw = start_stage_local_checkout(job=job)
    elif name == "restart_stage_local":
        raw = start_stage_local_checkout(job=job)
    elif name == "restart_prod":
        raw = start_prod(
            str(payload.get("prod_release") or payload.get("branch") or "").strip(),
            job=job,
            allow_local=bool(payload.get("allow_prod_local")),
        )
    elif name == "reset_prod":
        raw = reset_prod_to_release("", job=job, release_only=True)
    elif name == "copy_prod_db_to_stage":
        raw = copy_prod_db_to_stage()
    elif name == "cleanup_merged_branches":
        raw = cleanup_merged_branches()
    elif name == "cleanup_branch":
        raw = cleanup_branch(str(payload.get("branch") or ""))
    elif name == "start_e2e":
        raw = start_e2e(str(payload.get("branch") or ""), str(payload.get("target") or ""))
    elif name == "pin_work_branch":
        raw = pin_paired_work(str(payload.get("paired_branch") or payload.get("branch") or "").strip())
    elif name == "unpin_work_branch":
        raw = unpin_paired_work()
    elif name == "pin_app_branch":
        raw = pin_branch("app", str(payload.get("branch") or "").strip())
    elif name == "unpin_app_branch":
        raw = unpin_branch("app")
    elif name == "pin_test_branch":
        raw = pin_branch("test", str(payload.get("test_branch") or payload.get("branch") or "").strip())
    elif name == "unpin_test_branch":
        raw = unpin_branch("test")
    elif name == "create_app_branch":
        raw = create_and_pin_branch("app", str(payload.get("new_branch") or "").strip(), str(payload.get("branch") or "origin:main").strip())
    elif name == "create_test_branch":
        raw = create_and_pin_branch("test", str(payload.get("new_test_branch") or payload.get("new_branch") or "").strip(), str(payload.get("test_branch") or payload.get("branch") or "origin:main").strip())
    elif name == "create_paired_branches":
        raw = create_and_pin_paired_branches(
            str(payload.get("new_paired_branch") or payload.get("new_branch") or "").strip(),
            str(payload.get("base_branch") or "origin:main").strip(),
            str(payload.get("test_base_branch") or "origin:main").strip(),
        )
    else:
        raw = {"ok": False, "error": f"unknown action: {name}"}
    return {"ok": raw.get("ok"), "summary": action_summary(name, raw), "raw": raw}


LIVE_ACTIONS = {
    "start_dev",
    "restart_dev",
    "start_stage",
    "restart_stage",
    "start_stage_local",
    "restart_stage_local",
    "restart_prod",
    "reset_prod",
}


def start_live_action(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    job_id = uuid4().hex[:10]
    job = {
        "id": job_id,
        "kind": "action",
        "action": name,
        "status": "running",
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "events": [],
        "payload": payload,
    }
    with JOBS_LOCK:
        JOBS[job_id] = job

    def runner() -> None:
        try:
            _action_job_event(job, f"Action {name} started")
            result = action(name, payload, job=job)
            with JOBS_LOCK:
                job["status"] = "completed" if result.get("ok") else "failed"
                job["result"] = result
                job["finished_at"] = datetime.now().isoformat(timespec="seconds")
            _action_job_event(job, f"Action {name} {'completed' if result.get('ok') else 'failed'}")
        except Exception as exc:
            result = {"ok": False, "summary": {"ok": False, "title": "Action failed", "message": str(exc), "details": {}}, "raw": {"error": str(exc)}}
            with JOBS_LOCK:
                job["status"] = "failed"
                job["result"] = result
                job["finished_at"] = datetime.now().isoformat(timespec="seconds")
            _action_job_event(job, f"Action {name} failed: {exc}")

    thread = threading.Thread(target=runner, daemon=True)
    job["thread"] = thread
    thread.start()
    return {"ok": True, "live_job": True, "job": _action_job_snapshot(job)}


def get_live_action(job_id: str) -> dict[str, Any]:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return {"ok": False, "error": f"job not found: {job_id}"}
        return {"ok": True, "job": _action_job_snapshot(job)}


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nullion Local Ops</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #111316;
      --panel: #191c21;
      --panel-2: #20242b;
      --border: #343a45;
      --text: #f2f4f8;
      --muted: #a8afbc;
      --good: #56d68a;
      --warn: #e8bf58;
      --bad: #ff6b72;
      --accent: #7d6bff;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--text); }
    header { padding: 18px 22px; border-bottom: 1px solid var(--border); display: grid; gap: 14px; }
    .header-main { display: flex; gap: 16px; align-items: center; justify-content: space-between; }
    h1 { margin: 0; font-size: 22px; line-height: 1.2; }
    main { padding: 18px; display: grid; gap: 18px; }
    button, input, select { background: var(--panel-2); color: var(--text); border: 1px solid var(--border); border-radius: 7px; padding: 9px 11px; font: inherit; }
    button { cursor: pointer; }
    button:hover { border-color: var(--accent); }
    .primary { background: var(--accent); border-color: var(--accent); }
    .toolbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px; }
    .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
    .panel h2, .panel h3 { margin: 0; padding: 14px 16px; border-bottom: 1px solid var(--border); font-size: 17px; }
    .body { padding: 14px 16px; }
    .actions-panel { border-color: #434a59; box-shadow: 0 10px 28px rgba(0, 0, 0, .18); }
    .actions-panel h2 { padding: 16px 18px; font-size: 19px; }
    .actions-panel .body { padding: 18px; }
    .cards { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .metric { background: var(--panel-2); border: 1px solid var(--border); border-radius: 7px; padding: 10px; min-height: 74px; }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 5px; }
    .value { font-size: 15px; overflow-wrap: anywhere; }
    .status { display: inline-flex; align-items: center; gap: 7px; font-weight: 650; }
    .dot { width: 9px; height: 9px; border-radius: 99px; background: var(--muted); display: inline-block; }
    .ok .dot { background: var(--good); }
    .warn .dot { background: var(--warn); }
    .bad .dot { background: var(--bad); }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--border); vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    code { color: #d7dcff; overflow-wrap: anywhere; }
    pre { background: #0d0f12; border: 1px solid var(--border); border-radius: 7px; padding: 10px; max-height: 280px; overflow: auto; white-space: pre-wrap; color: #dce1ec; }
    details { border-top: 1px solid var(--border); }
    summary { cursor: pointer; padding: 12px 16px; color: var(--muted); }
    .logs { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    .action-output { white-space: pre-wrap; color: var(--muted); }
    .result-card { border-radius: 8px; border: 1px solid var(--border); background: var(--panel-2); padding: 12px; color: var(--text); }
    .result-card.ok { border-color: rgba(86, 214, 138, .65); }
    .result-card.bad { border-color: rgba(255, 107, 114, .75); }
    .result-title { font-weight: 750; margin-bottom: 4px; }
    .result-message { color: var(--muted); margin-bottom: 8px; }
    .command-block { margin-top: 14px; }
    .command-block .label { margin-bottom: 7px; }
    .command-block pre { max-height: 360px; margin: 0; }
    .link-row { display: flex; flex-wrap: wrap; gap: 8px; }
    .link-button { display: inline-flex; text-decoration: none; background: var(--panel-2); color: var(--text); border: 1px solid var(--border); border-radius: 7px; padding: 9px 11px; }
    .link-button:hover { border-color: var(--accent); }
    .wide-input { min-width: min(520px, 100%); }
    .short-input { min-width: min(300px, 100%); }
    .split { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
    .pin-strip { display: grid; grid-template-columns: 1fr; gap: 10px; }
    .pin-status-bar { border: 1px solid rgba(125, 107, 255, .34); border-radius: 8px; background: rgba(125, 107, 255, .08); padding: 10px; }
    .pin-pill { border: 1px solid rgba(125, 107, 255, .38); border-radius: 7px; background: rgba(32, 36, 43, .86); padding: 10px 12px; color: var(--muted); display: flex; gap: 10px; align-items: center; min-width: 0; }
    .pin-pill code { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .pin-pill strong { color: var(--text); }
    .branch-board { display: grid; gap: 14px; }
    .branch-row { display: grid; grid-template-columns: minmax(180px, 270px) minmax(320px, 1fr) auto; gap: 14px; align-items: center; padding: 2px 0; }
    .branch-title strong { display: block; font-size: 14px; }
    .branch-title span { display: block; color: var(--muted); font-size: 12px; margin-top: 2px; }
    .inline-create { border-top: 0; margin-left: min(284px, 24vw); margin-top: -6px; }
    .inline-create summary { padding: 0; color: var(--muted); font-size: 12px; }
    .inline-create .toolbar { margin-top: 8px; }
    .advanced-actions { margin-top: 16px; border-top: 1px solid var(--border); padding-top: 14px; display: grid; gap: 12px; }
    .advanced-actions-title { color: var(--muted); font-weight: 650; font-size: 13px; }
    .chart-frame { max-width: 920px; }
    canvas { display: block; width: 100%; height: 170px; background: #0d0f12; border: 1px solid var(--border); border-radius: 7px; }
    .legend { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 10px; color: var(--muted); font-size: 12px; }
    .legend span { display: inline-flex; gap: 6px; align-items: center; }
    @media (max-width: 980px) { .grid, .logs, .split, .branch-row, .pin-strip { grid-template-columns: 1fr; } .cards { grid-template-columns: 1fr 1fr; } .inline-create { margin-left: 0; } }
    @media (max-width: 620px) { .header-main { align-items: flex-start; flex-direction: column; } .cards { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <header>
    <div class="header-main">
      <div>
        <h1>Nullion Local Ops</h1>
        <div id="timestamp" class="label">Loading...</div>
      </div>
      <div class="toolbar">
        <button class="primary" onclick="refresh()">Refresh</button>
        <button onclick="runAction('restart_ops_dashboard')">Restart dashboard</button>
        <a class="link-button" href="http://127.0.0.1:8742/" target="_blank" rel="noreferrer">Prod web UI</a>
        <a class="link-button" href="http://127.0.0.1:8753/" target="_blank" rel="noreferrer">Stage web UI</a>
        <a class="link-button" href="http://127.0.0.1:8752/" target="_blank" rel="noreferrer">Dev web UI</a>
      </div>
    </div>
    <div class="pin-strip pin-status-bar" id="pinStatus"></div>
  </header>
  <main>
    <section class="panel actions-panel">
      <h2>Actions</h2>
      <div class="body">
        <div class="branch-board">
          <div class="branch-row">
            <div class="branch-title"><strong>Agent work branch</strong><span>Pairs app and nullion-test branches</span></div>
            <select class="wide-input" id="workBranchSelect" aria-label="Agent paired work branch picker"></select>
            <div class="toolbar">
              <button class="primary" onclick="runAction('pin_work_branch')">Pin all</button>
              <button onclick="runAction('unpin_work_branch')">Clear</button>
            </div>
          </div>
          <details class="inline-create">
            <summary>Create paired branches</summary>
            <div class="toolbar">
              <input class="short-input" id="newPairedBranch" aria-label="New paired branch" placeholder="branch name">
              <button onclick="runAction('create_paired_branches')">Create and pin all</button>
            </div>
          </details>
          <div class="branch-row">
            <div class="branch-title"><strong>Prod runtime</strong><span>Release tag by default. Check the local-source box to run a selected local branch on port 8742.</span></div>
            <select class="wide-input" id="prodReleaseSelect" aria-label="Prod runtime release picker"></select>
            <div class="toolbar">
              <button onclick="runAction('restart_prod')">Run prod</button>
              <button onclick="runAction('reset_prod')">Reset prod</button>
            </div>
            <label class="label" style="display:block;margin-top:8px">
              <input type="checkbox" id="allowLocalProd" onchange="renderBranchPicker(lastData && lastData.branches ? lastData.branches : [])" />
              Allow local source for prod (testing only)
            </label>
          </div>
          <div class="branch-row">
            <div class="branch-title"><strong>Dev runtime</strong><span>Branch to run on port 8752</span></div>
            <select class="wide-input" id="devBranchSelect" aria-label="Dev runtime branch picker"></select>
            <div class="toolbar">
              <button onclick="runAction('restart_dev')">Run dev</button>
            </div>
          </div>
          <div class="branch-row">
            <div class="branch-title"><strong>Stage runtime</strong><span>Branch/ref to run on port 8753</span></div>
            <select class="wide-input" id="stageBranchSelect" aria-label="Stage runtime branch picker"></select>
            <div class="toolbar">
              <button onclick="runAction('restart_stage')">Run stage</button>
            </div>
          </div>
        </div>
        <div class="advanced-actions" id="advancedRuntimeControls">
          <div class="advanced-actions-title">Extra runtime controls</div>
          <div class="toolbar" style="margin-top:12px">
            <button onclick="runAction('restart_stage_local')">Run stage from current local checkout</button>
            <button onclick="runAction('copy_prod_db_to_stage')">Copy prod DB to stage</button>
          </div>
          <div class="toolbar" style="margin-top:12px">
            <button onclick="runAction('start_dev')">Start dev</button>
            <button onclick="runAction('stop_dev')">Stop dev</button>
            <button onclick="runAction('start_stage')">Start stage</button>
            <button onclick="runAction('stop_stage')">Stop stage</button>
            <button onclick="runAction('open_prod_webview')">Prod web view</button>
            <button onclick="runAction('open_dev_webview')">Dev web view</button>
            <button onclick="runAction('open_stage_webview')">Stage web view</button>
            <button onclick="runAction('open_prod_logs')">Prod logs</button>
            <button onclick="runAction('open_dev_logs')">Dev logs</button>
            <button onclick="runAction('open_stage_logs')">Stage logs</button>
          </div>
        </div>
      </div>
      <div class="body action-output" id="actionOutput"></div>
    </section>
    <section class="panel">
      <h2>Performance</h2>
      <div class="body">
        <div class="chart-frame">
          <canvas id="perfChart" aria-label="Prod, stage, and dev response time and CPU history"></canvas>
          <div class="legend">
            <span><i class="dot" style="background:#56d68a"></i> Prod HTTP ms</span>
            <span><i class="dot" style="background:#7d6bff"></i> Stage HTTP ms</span>
            <span><i class="dot" style="background:#4db7ff"></i> Dev HTTP ms</span>
            <span><i class="dot" style="background:#e8bf58"></i> Prod web CPU</span>
            <span><i class="dot" style="background:#ff6b72"></i> Stage web CPU</span>
            <span><i class="dot" style="background:#c084fc"></i> Dev web CPU</span>
            <span id="perfSamples">0 live samples</span>
          </div>
        </div>
      </div>
    </section>
    <section class="grid" id="instances"></section>
    <section class="split">
      <section class="panel">
        <h2>E2E Runner</h2>
        <div class="body">
          <div class="toolbar">
            <input class="wide-input" id="e2eTarget" placeholder="optional pytest target, e.g. tests/e2e/test_web_surface_contracts.py">
            <button class="primary" onclick="runAction('start_e2e')">Run E2E on selected branch</button>
          </div>
          <div class="label" style="margin-top:10px">Runs Docker-isolated E2E with NULLION_APP_REPO pointed at the selected dev branch source.</div>
          <div id="jobs"></div>
        </div>
      </section>
      <section class="panel">
        <h2>Git Cleanup</h2>
        <div class="body">
          <div class="toolbar">
            <button onclick="runAction('cleanup_merged_branches')">Clean merged branches</button>
            <button onclick="runAction('cleanup_branch')">Clean selected branch</button>
          </div>
          <div class="label" style="margin-top:10px">Cleanup only removes local branches/worktrees that are clean and already merged to main.</div>
          <div id="branchTable"></div>
        </div>
      </section>
    </section>
  </main>
<script>
let lastData = null;
const perfHistory = [];

function classFor(ok, warn=false) {
  if (ok && !warn) return "ok";
  if (ok && warn) return "warn";
  return "bad";
}

function text(v) {
  if (v === null || v === undefined || v === "") return "n/a";
  return String(v);
}

function versionValue(instance) {
  const endpoint = instance.version_endpoint && instance.version_endpoint.payload;
  const runtime = instance.runtime_version || {};
  const pkg = instance.package || {};
  return [
    `endpoint ${endpoint && endpoint.current ? endpoint.current : "n/a"}`,
    `runtime ${runtime.current_version || "n/a"}`,
    `package ${pkg.version || "n/a"}`
  ].join("\n");
}

function serviceRows(instance) {
  const pkgVersion = instance.package && instance.package.version ? instance.package.version : "n/a";
  const gitBranch = instance.source_git && instance.source_git.branch ? instance.source_git.branch : "n/a";
  const gitCommit = instance.source_git && instance.source_git.commit ? instance.source_git.commit : "n/a";
  const editableSource = instance.package && instance.package.editable_source ? instance.package.editable_source : "n/a";
  return (instance.services || []).map(s => `
    <tr>
      <td>${s.key}</td>
      <td><span class="status ${classFor(s.state === "running")}"><span class="dot"></span>${text(s.state)}</span></td>
      <td>${text(s.pid)}</td>
      <td>${s.process && s.process.cpu_percent ? `${s.process.cpu_percent}% / ${s.process.mem_percent}%` : "n/a"}</td>
      <td>${escapeHtml(pkgVersion)}</td>
      <td>${escapeHtml(gitBranch)} @ ${escapeHtml(gitCommit)}</td>
      <td><code>${escapeHtml(editableSource)}</code></td>
      <td><code>${text(s.environment && s.environment.NULLION_HOME)}</code></td>
      <td><code>${text(s.environment && s.environment.NULLION_ENV_FILE)}</code></td>
    </tr>
  `).join("");
}

function tableStats(instance) {
  const tables = instance.runtime_db && instance.runtime_db.tables ? instance.runtime_db.tables : {};
  const entries = Object.entries(tables).sort((a, b) => b[1] - a[1]).slice(0, 8);
  if (!entries.length) return "No table counts";
  return entries.map(([k, v]) => `${k}: ${v}`).join("\n");
}

function logBlocks(instance) {
  return Object.entries(instance.logs || {}).map(([name, logs]) => `
    <details>
      <summary>${name} logs</summary>
      <div class="body logs">
        <div>
          <div class="label">${logs.stderr.path}</div>
          <pre>${escapeHtml(logs.stderr.text || "(empty)")}</pre>
        </div>
        <div>
          <div class="label">${logs.stdout.path}</div>
          <pre>${escapeHtml(logs.stdout.text || "(empty)")}</pre>
        </div>
      </div>
    </details>
  `).join("");
}

function renderBranches(branches) {
  const rows = (branches || []).map(row => `
    <tr>
      <td><code>${escapeHtml(row.display)}</code></td>
      <td>${escapeHtml(row.commit || "")}</td>
      <td>${escapeHtml(row.when || "")}</td>
      <td>${row.current ? "yes" : ""}</td>
      <td>${row.merged_to_main ? "yes" : "no"}</td>
      <td>${escapeHtml(row.subject || "")}</td>
    </tr>
  `).join("");
  document.getElementById("branchTable").innerHTML = `
    <table>
      <thead><tr><th>Branch</th><th>Commit</th><th>Updated</th><th>Current</th><th>Merged</th><th>Subject</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="6">No branches found</td></tr>'}</tbody>
    </table>
  `;
}

function renderPinStatus(pins) {
  pins = pins || {};
  function block(key, title) {
    const pin = pins[key] || {};
    const pinned = Boolean(pin.pinned);
    const ok = pinned && pin.exists !== false;
    const value = pinned ? `${pin.selection || pin.branch || "n/a"}` : "not pinned";
    const statusClass = pinned ? classFor(ok) : "warn";
    return `
      <div class="pin-pill">
        <span class="status ${statusClass}"><span class="dot"></span><strong>${escapeHtml(title)}</strong></span>
        <code>${escapeHtml(value)}</code>
      </div>
    `;
  }
  const target = document.getElementById("pinStatus");
  if (target) target.innerHTML = block("app", "Pinned app branch") + block("test", "Pinned nullion-test branch");
}

function renderJobs(jobs) {
  const values = Object.values(jobs || {}).sort((a, b) => String(b.started_at).localeCompare(String(a.started_at)));
  document.getElementById("jobs").innerHTML = values.slice(0, 5).map(job => `
    <details open>
      <summary>${escapeHtml(job.kind)} ${escapeHtml(job.id)} · ${escapeHtml(job.status)} · ${escapeHtml(job.started_at || "")}</summary>
      <div class="body">
        <div class="label">${escapeHtml((job.command || []).join(" "))}</div>
        <pre>${escapeHtml(job.log_tail || "(no output yet)")}</pre>
      </div>
    </details>
  `).join("") || '<div class="label">No jobs yet</div>';
}

function renderActionResult(result) {
  const summary = result.summary || {};
  const ok = Boolean(summary.ok ?? result.ok);
  const details = summary.details || {};
  const detailRows = Object.entries(details)
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .map(([key, value]) => `<div><span class="label">${escapeHtml(key)}</span><br><code>${escapeHtml(typeof value === "object" ? JSON.stringify(value) : value)}</code></div>`)
    .join("");
  return `
    <div class="result-card ${ok ? "ok" : "bad"}">
      <div class="result-title">${escapeHtml(summary.title || (ok ? "Done" : "Failed"))}</div>
      <div class="result-message">${escapeHtml(summary.message || "")}</div>
      <div class="cards">${detailRows}</div>
      <details>
        <summary>Raw details</summary>
        <pre>${escapeHtml(JSON.stringify(result.raw || result, null, 2))}</pre>
      </details>
    </div>
  `;
}

function selectedBranchRow(value) {
  const rows = lastData && Array.isArray(lastData.branches) ? lastData.branches : [];
  return rows.find(row => (row.value || row.branch) === value || row.branch === value) || null;
}

function renderRunningAction(action, branch) {
  return `
    <div class="result-card">
      <div class="result-title">Running ${escapeHtml(action)}</div>
      <div class="result-message">Selected source: <code>${escapeHtml(branch || "n/a")}</code></div>
      <div class="command-block">
        <div class="label">Live command feed</div>
        <pre id="actionLiveFeed">Waiting for backend job...</pre>
      </div>
    </div>
  `;
}

function formatCommand(command) {
  return (command || []).map(part => {
    const text = String(part);
    return /\s/.test(text) ? JSON.stringify(text) : text;
  }).join(" ");
}

function formatActionEvents(job) {
  const events = Array.isArray(job && job.events) ? job.events : [];
  if (!events.length) return "Waiting for first command...";
  return events.map(event => {
    const lines = [`[${event.time || ""}] ${event.message || ""}`];
    if (event.command) lines.push(`$ ${formatCommand(event.command)}`);
    if (event.result) {
      const result = event.result;
      lines.push(`returncode=${text(result.returncode)} elapsed_ms=${text(result.elapsed_ms)}`);
      if (result.stdout) lines.push(result.stdout);
      if (result.stderr) lines.push(result.stderr);
    }
    return lines.join("\n");
  }).join("\n\n");
}

function updateActionFeed(job) {
  const feed = document.getElementById("actionLiveFeed");
  if (!feed) return;
  feed.textContent = formatActionEvents(job);
  feed.scrollTop = feed.scrollHeight;
}

async function pollLiveAction(jobId) {
  let latest = null;
  for (;;) {
    const response = await fetch(`/api/action-job?id=${encodeURIComponent(jobId)}`);
    const payload = await response.json();
    latest = payload.job || null;
    if (latest) updateActionFeed(latest);
    if (!latest || ["completed", "failed"].includes(String(latest.status || ""))) return latest;
    await new Promise(resolve => setTimeout(resolve, 700));
  }
}

function branchForInstance(instance, fallback) {
  const sourceGit = instance && instance.source_git ? instance.source_git : {};
  const rows = lastData && Array.isArray(lastData.branches) ? lastData.branches : [];
  const branch = String(sourceGit.branch || "");
  const commit = String(sourceGit.commit || "");
  if (branch && rows.length) {
    const localMatch = rows.find(row =>
      row.source === "local" &&
      row.branch === branch &&
      (!commit || commit.startsWith(String(row.commit || "")))
    );
    const anyMatch = rows.find(row =>
      row.branch === branch &&
      (!commit || commit.startsWith(String(row.commit || "")))
    );
    const match = localMatch || anyMatch;
    if (match && (match.value || match.branch)) return match.value || match.branch;
  }
  if (branch) return `local:${branch}`;
  if (commit && rows.length) {
    const matches = rows.filter(row => commit.startsWith(String(row.commit || "")));
    const match = instance && instance.source_matches_expected
      ? (matches.find(row => row.source === "origin") || matches[0])
      : (matches.find(row => row.source === "local") || matches[0]);
    if (match && (match.value || match.branch)) return match.value || match.branch;
  }
  return fallback || "";
}

function sourceForProdInstance(instance, fallback) {
  const sourceGit = instance && instance.source_git ? instance.source_git : {};
  const sources = lastData && Array.isArray(lastData.prod_sources) ? lastData.prod_sources : [];
  const commit = String(sourceGit.commit || "");
  if (commit && sources.length) {
    const match = sources.find(row => commit.startsWith(String(row.commit || "")));
    if (match && (match.value || match.branch)) return match.value || match.branch;
  }
  return fallback || (sources[0] && (sources[0].value || sources[0].branch)) || "";
}

function prodSourcesForPicker() {
  const sources = lastData && Array.isArray(lastData.prod_sources) ? lastData.prod_sources : [];
  const allowLocalProd = document.getElementById("allowLocalProd")?.checked === true;
  if (allowLocalProd) return sources;
  return sources.filter(row => row.source === "release");
}

function renderOneBranchPicker(branches, selectId, desiredValue, preferDesired=false) {
  const select = document.getElementById(selectId);
  if (!select) return;
  const currentValue = preferDesired ? (desiredValue || select.value || "") : (select.value || desiredValue || "");
  const options = (branches || []).map(row => {
    const value = row.value || row.branch;
    const selected = value === currentValue || row.branch === currentValue ? "selected" : "";
    return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(row.display)}</option>`;
  });
  if (currentValue && !(branches || []).some(row => (row.value || row.branch) === currentValue || row.branch === currentValue)) {
    options.unshift(`<option value="${escapeHtml(currentValue)}" selected>${escapeHtml(currentValue)}</option>`);
  }
  select.innerHTML = options.join("");
}

function renderBranchPicker(branches) {
  const pins = lastData && lastData.pins ? lastData.pins : {};
  const appPin = pins.app && pins.app.pinned ? pins.app.selection : "";
  const allowLocalProd = document.getElementById("allowLocalProd")?.checked === true;
  const prodSources = prodSourcesForPicker();
  const prodDesired = allowLocalProd && appPin
    ? appPin
    : sourceForProdInstance(lastData && lastData.instances && lastData.instances.prod, "");
  renderOneBranchPicker(prodSources, "prodReleaseSelect", prodDesired, allowLocalProd);
  renderOneBranchPicker(branches, "workBranchSelect", appPin || branchForInstance(lastData && lastData.instances && lastData.instances.dev, ""));
  renderOneBranchPicker(branches, "devBranchSelect", branchForInstance(lastData && lastData.instances && lastData.instances.dev, ""));
  renderOneBranchPicker(branches, "stageBranchSelect", branchForInstance(lastData && lastData.instances && lastData.instances.stage, "origin:main"));
}

function renderInstance(instance) {
  const healthOk = instance.health && instance.health.ok;
  const pageOk = instance.web_page && instance.web_page.ok && Number(instance.web_page.elapsed_ms || 0) < 5000;
  const wsOk = instance.websocket && instance.websocket.ok;
  const statusOk = instance.status_endpoint && instance.status_endpoint.ok && Number(instance.status_endpoint.elapsed_ms || 0) < 5000;
  const usable = healthOk && pageOk && wsOk && statusOk;
  const sourceWarn = instance.key === "prod" && !instance.source_matches_expected;
  const versionPayload = instance.version_endpoint && instance.version_endpoint.payload || {};
  return `
    <article class="panel">
      <h2>${instance.title} <span class="status ${classFor(usable, sourceWarn || (healthOk && !usable))}"><span class="dot"></span>${usable ? "usable" : (healthOk ? "degraded" : "offline")}</span></h2>
      <div class="body">
        <div class="cards">
          <div class="metric"><div class="label">URL</div><div class="value"><code>${instance.url}</code></div></div>
          <div class="metric"><div class="label">API health</div><div class="value">${text(instance.health && instance.health.status)} in ${text(instance.health && instance.health.elapsed_ms)}ms</div></div>
          <div class="metric"><div class="label">Web page</div><div class="value">${text(instance.web_page && instance.web_page.status)} in ${text(instance.web_page && instance.web_page.elapsed_ms)}ms<br>${formatBytes(instance.web_page && instance.web_page.bytes)}</div></div>
          <div class="metric"><div class="label">Status API</div><div class="value">${text(instance.status_endpoint && instance.status_endpoint.status)} in ${text(instance.status_endpoint && instance.status_endpoint.elapsed_ms)}ms</div></div>
          <div class="metric"><div class="label">WebSocket</div><div class="value">${instance.websocket && instance.websocket.ok ? "connected" : "failed"} in ${text(instance.websocket && instance.websocket.elapsed_ms)}ms<br>${escapeHtml(instance.websocket && instance.websocket.error || "")}</div></div>
          <div class="metric"><div class="label">Version</div><div class="value"><pre>${escapeHtml(versionValue(instance))}</pre></div></div>
          <div class="metric"><div class="label">Current commit</div><div class="value">${text(versionPayload.current)}</div></div>
          <div class="metric"><div class="label">Latest commit</div><div class="value">${text(versionPayload.latest)}</div></div>
          <div class="metric"><div class="label">Update available</div><div class="value">${text(versionPayload.has_update)}</div></div>
          <div class="metric"><div class="label">Editable source</div><div class="value"><code>${text(instance.package && instance.package.editable_source)}</code></div></div>
          <div class="metric"><div class="label">Expected source</div><div class="value"><code>${text(instance.expected_source)}</code></div></div>
          <div class="metric"><div class="label">Git</div><div class="value">Branch: ${text(instance.source_git && instance.source_git.branch)}<br>Commit: ${text(instance.source_git && instance.source_git.commit)}<br>Uncommitted files: ${text(instance.source_git && instance.source_git.dirty_count)}<br>Upstream: ${text(instance.source_git && instance.source_git.upstream)}<br>Ahead/behind: ${text(instance.source_git && instance.source_git.ahead)} / ${text(instance.source_git && instance.source_git.behind)}</div></div>
          <div class="metric"><div class="label">Runtime DB</div><div class="value">${formatBytes(instance.runtime_db && instance.runtime_db.size_bytes)}<br>WAL ${formatBytes(instance.runtime_db && instance.runtime_db.wal_size_bytes)}</div></div>
          <div class="metric"><div class="label">Checkpoint</div><div class="value"><code>${text(instance.checkpoint_path)}</code></div></div>
          <div class="metric"><div class="label">DB tables</div><div class="value"><pre>${escapeHtml(tableStats(instance))}</pre></div></div>
          <div class="metric"><div class="label">Env file</div><div class="value"><code>${text(instance.env && instance.env.path)}</code></div></div>
        </div>
      </div>
      <details open>
        <summary>Services</summary>
        <div class="body">
          <table>
            <thead><tr><th>Service</th><th>State</th><th>PID</th><th>CPU / Mem</th><th>Package</th><th>Source commit</th><th>Editable source</th><th>NULLION_HOME</th><th>Env file</th></tr></thead>
            <tbody>${serviceRows(instance)}</tbody>
          </table>
        </div>
      </details>
      ${logBlocks(instance)}
    </article>
  `;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}

function formatBytes(value) {
  value = Number(value || 0);
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

async function refresh() {
  document.getElementById("timestamp").textContent = "Refreshing...";
  const response = await fetch("/api/status");
  lastData = await response.json();
  document.getElementById("timestamp").textContent = `Updated ${lastData.generated_at}`;
  document.getElementById("instances").innerHTML = Object.values(lastData.instances).map(renderInstance).join("");
  renderBranches(lastData.branches || []);
  renderPinStatus(lastData.pins || {});
  renderBranchPicker(lastData.branches || []);
  renderJobs(lastData.jobs || {});
  recordPerformance(lastData);
  drawPerformance();
}

function webCpu(instance) {
  const svc = (instance.services || []).find(s => s.key === "web");
  const raw = svc && svc.process && svc.process.cpu_percent;
  return Number(raw || 0);
}

function recordPerformance(data) {
  perfHistory.push({
    t: new Date(data.generated_at).toLocaleTimeString(),
    prodMs: Number(data.instances.prod.health.elapsed_ms || 0),
    stageMs: Number(data.instances.stage.health.elapsed_ms || 0),
    devMs: Number(data.instances.dev.health.elapsed_ms || 0),
    prodCpu: webCpu(data.instances.prod),
    stageCpu: webCpu(data.instances.stage),
    devCpu: webCpu(data.instances.dev)
  });
  while (perfHistory.length > 60) perfHistory.shift();
}

function drawPerformance() {
  const canvas = document.getElementById("perfChart");
  if (!canvas || !perfHistory.length) return;
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = Math.max(320, Math.floor(rect.width || 720));
  const cssHeight = Math.max(150, Math.floor(rect.height || 170));
  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const w = cssWidth;
  const h = cssHeight;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#0d0f12";
  ctx.fillRect(0, 0, w, h);
  const padL = 36;
  const padR = 12;
  const padT = 14;
  const padB = 28;
  const maxValue = Math.max(10, ...perfHistory.flatMap(p => [p.prodMs, p.stageMs, p.devMs, p.prodCpu, p.stageCpu, p.devCpu]));
  ctx.strokeStyle = "#343a45";
  ctx.lineWidth = 1;
  ctx.fillStyle = "#a8afbc";
  ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, monospace";
  for (let i = 0; i <= 4; i++) {
    const y = padT + (h - padT - padB) * (i / 4);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(w - padR, y);
    ctx.stroke();
    const label = Math.round(maxValue * (1 - i / 4));
    ctx.fillText(String(label), 8, y + 4);
  }
  function drawLine(key, color) {
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    perfHistory.forEach((point, index) => {
      const x = padL + (w - padL - padR) * (perfHistory.length === 1 ? 1 : index / (perfHistory.length - 1));
      const y = h - padB - ((Number(point[key] || 0) / maxValue) * (h - padT - padB));
      if (index === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
  drawLine("prodMs", "#56d68a");
  drawLine("stageMs", "#7d6bff");
  drawLine("devMs", "#4db7ff");
  drawLine("prodCpu", "#e8bf58");
  drawLine("stageCpu", "#ff6b72");
  drawLine("devCpu", "#c084fc");
  ctx.fillStyle = "#a8afbc";
  const latest = perfHistory[perfHistory.length - 1];
  ctx.fillText(`latest: prod ${latest.prodMs}ms / stage ${latest.stageMs}ms / dev ${latest.devMs}ms / prod CPU ${latest.prodCpu}% / stage CPU ${latest.stageCpu}% / dev CPU ${latest.devCpu}%`, padL, h - 9);
  const samples = document.getElementById("perfSamples");
  if (samples) samples.textContent = `${perfHistory.length} live sample${perfHistory.length === 1 ? "" : "s"}`;
}

async function runAction(action) {
  const output = document.getElementById("actionOutput");
  const workBranchEl = document.getElementById("workBranchSelect");
  const prodReleaseEl = document.getElementById("prodReleaseSelect");
  const devBranchEl = document.getElementById("devBranchSelect");
  const stageBranchEl = document.getElementById("stageBranchSelect");
  const newPairedBranchEl = document.getElementById("newPairedBranch");
  const workBranch = workBranchEl ? workBranchEl.value.trim() : "";
  const prodRelease = prodReleaseEl ? prodReleaseEl.value.trim() : "";
  const devBranch = devBranchEl ? devBranchEl.value.trim() : "";
  const stageBranch = stageBranchEl ? stageBranchEl.value.trim() : "";
  const allowLocalProd = document.getElementById("allowLocalProd")?.checked === true;
  let branch = devBranch;
  if (action.includes("prod")) branch = prodRelease;
  if (action === "reset_prod") branch = "latest release";
  if (action === "pin_work_branch" || action === "create_paired_branches" || action === "cleanup_branch") branch = workBranch;
  if (action.includes("stage")) branch = stageBranch || "origin:main";
  output.innerHTML = renderRunningAction(action, branch);
  const target = document.getElementById("e2eTarget") ? document.getElementById("e2eTarget").value.trim() : "";
  const response = await fetch("/api/action", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      action,
      branch,
      prod_release: prodRelease,
      dev_branch: devBranch,
      stage_branch: stageBranch,
      allow_prod_local: allowLocalProd,
      paired_branch: workBranch,
      test_branch: workBranch,
      new_paired_branch: newPairedBranchEl ? newPairedBranchEl.value.trim() : "",
      target
    })
  });
  const result = await response.json();
  if (result.live_job && result.job && result.job.id) {
    updateActionFeed(result.job);
    const job = await pollLiveAction(result.job.id);
    output.innerHTML = renderActionResult(job && job.result ? job.result : result);
  } else {
    output.innerHTML = renderActionResult(result);
  }
  if (action === "restart_ops_dashboard") {
    setTimeout(() => window.location.reload(), 2500);
    return;
  }
  await refresh();
}

refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "NullionLocalOps/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html"}:
            self._send_text(INDEX_HTML, content_type="text/html; charset=utf-8")
            return
        if parsed.path == "/api/status":
            self._send_json(dashboard_status())
            return
        if parsed.path == "/api/action-job":
            params = parse_qs(parsed.query)
            job_id = str((params.get("id") or [""])[0])
            self._send_json(get_live_action(job_id))
            return
        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/api/action":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8", errors="replace") if length else "{}"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            self._send_json({"ok": False, "error": "invalid JSON"}, status=400)
            return
        name = str(payload.get("action") or "")
        if name in LIVE_ACTIONS:
            self._send_json(start_live_action(name, payload))
            return
        self._send_json(action(name, payload))

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] {self.address_string()} {fmt % args}")

    def _send_json(self, payload: Any, *, status: int = 200) -> None:
        encoded = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def _send_text(self, payload: str, *, content_type: str, status: int = 200) -> None:
        encoded = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the local Nullion prod/stage/dev ops dashboard.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DASHBOARD_PORT, help=argparse.SUPPRESS)
    args = parser.parse_args()
    cleanup = _kill_stale_dashboard_processes(port=args.port)
    if not cleanup.get("ok"):
        print(f"Warning: failed to cleanup stale dashboard processes: {cleanup.get('errors')}")
    elif cleanup.get("count"):
        print(f"Cleaned up stale dashboard processes: {cleanup.get('count')}")
    server = ReusableThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Nullion local ops dashboard: http://{args.host}:{args.port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()

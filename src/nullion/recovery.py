"""Out-of-band Nullion recovery control plane.

This module is intentionally small and dependency-light. It should keep working
when the main web app, runtime, or chat adapters are unhealthy.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion.entrypoint_guard import run_user_facing_entrypoint
from nullion.runtime_persistence import list_runtime_store_backups, restore_runtime_store_backup
from nullion.service_control import MANAGED_SERVICES, restart_managed_service, service_names


DEFAULT_HOST = "127.0.0.1"
DEFAULT_WEB_PORT = 8742
DEFAULT_RECOVERY_PORT = 8020
NULLION_HOME = Path.home() / ".nullion"
DEFAULT_ENV_FILE = NULLION_HOME / ".env"
DEFAULT_CHECKPOINT = NULLION_HOME / "runtime.db"
DEFAULT_LOG_DIR = NULLION_HOME / "logs"
DEFAULT_CONFIG_BACKUP_DIR = NULLION_HOME / "config-backups"

SERVICE_LABELS: dict[str, tuple[str, ...]] = {
    service.name: service.launchd_labels for service in MANAGED_SERVICES
}

SERVICE_COMMAND_HINTS: dict[str, str] = {
    service.name: service.command_hint or service.name for service in MANAGED_SERVICES
}

CONFIG_BACKUP_FILES = (
    ".env",
    "credentials.json",
    "users.json",
    "preferences.json",
    "connections.json",
)


@dataclass(frozen=True)
class ServiceStatus:
    name: str
    label: str | None
    state: str
    detail: str
    running: bool
    restartable: bool
    last_exit_code: int | None = None
    runs: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "state": self.state,
            "detail": self.detail,
            "running": self.running,
            "restartable": self.restartable,
            "last_exit_code": self.last_exit_code,
            "runs": self.runs,
        }


def _load_env_file(path: Path = DEFAULT_ENV_FILE) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _http_json(url: str, *, timeout: float = 2.0) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
        data = json.loads(payload or "{}")
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def web_health(*, host: str = DEFAULT_HOST, port: int = DEFAULT_WEB_PORT) -> dict[str, Any]:
    url = f"http://{host}:{port}/api/health"
    payload = _http_json(url)
    ok = bool(payload and payload.get("status") == "ok")
    return {
        "ok": ok,
        "url": url,
        "payload": payload,
        "detail": "healthy" if ok else "unreachable or unhealthy",
    }


def _launchctl_available() -> bool:
    return sys.platform == "darwin" and shutil.which("launchctl") is not None


def _run(args: list[str], *, timeout: float = 10.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=False, timeout=timeout)


def _parse_launchctl_print(output: str) -> tuple[int | None, int | None]:
    last_exit_code: int | None = None
    runs: int | None = None
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if line.startswith("last exit code = "):
            try:
                last_exit_code = int(line.rsplit("=", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("runs = "):
            try:
                runs = int(line.rsplit("=", 1)[1].strip())
            except ValueError:
                pass
    return last_exit_code, runs


def _process_running(command_hint: str) -> bool:
    if shutil.which("pgrep") is None:
        return False
    result = _run(["pgrep", "-fl", command_hint], timeout=5)
    return result.returncode == 0 and command_hint in (result.stdout or "")


def service_status(name: str) -> ServiceStatus:
    labels = SERVICE_LABELS.get(name)
    if not labels:
        raise ValueError(f"Unknown service: {name}")
    command_hint = SERVICE_COMMAND_HINTS.get(name, name)
    process_running = _process_running(command_hint)
    launch_agents_dir = Path.home() / "Library" / "LaunchAgents"

    if _launchctl_available():
        for label in labels:
            plist = launch_agents_dir / f"{label}.plist"
            if not plist.exists():
                continue
            listed = _run(["launchctl", "list", label])
            printed = _run(["launchctl", "print", f"gui/{os.getuid()}/{label}"], timeout=10)
            last_exit_code, runs = _parse_launchctl_print((printed.stdout or "") + "\n" + (printed.stderr or ""))
            if listed.returncode == 0:
                return ServiceStatus(
                    name=name,
                    label=label,
                    state="loaded",
                    detail=f"Loaded via {label}",
                    running=True,
                    restartable=True,
                    last_exit_code=last_exit_code,
                    runs=runs,
                )
            return ServiceStatus(
                name=name,
                label=label,
                state="installed",
                detail=f"Installed but not loaded ({label})",
                running=process_running,
                restartable=True,
                last_exit_code=last_exit_code,
                runs=runs,
            )

    return ServiceStatus(
        name=name,
        label=None,
        state="running" if process_running else "missing",
        detail=f"{command_hint} process {'is running' if process_running else 'not found'}",
        running=process_running,
        restartable=False,
    )


def all_service_statuses() -> list[dict[str, Any]]:
    return [service_status(name).to_dict() for name in ("web", "telegram", "slack", "discord", "tray", "recovery")]


def restart_service(name: str) -> str:
    return restart_managed_service(name)


def begin_gateway_restart_notice() -> None:
    try:
        from nullion.gateway_notifications import begin_gateway_restart

        begin_gateway_restart(async_delivery=False)
    except Exception:
        pass


class _RestartServicesState(TypedDict, total=False):
    names: list[str]
    index: int
    results: list[dict[str, str]]


def _restart_services_expand_node(state: _RestartServicesState) -> dict[str, object]:
    names = list(state.get("names") or [])
    if names == ["all"]:
        names = service_names()
    return {"names": names, "index": 0, "results": []}


def _restart_services_route_expanded(state: _RestartServicesState) -> str:
    return "notice" if state.get("names") else END


def _restart_services_notice_node(state: _RestartServicesState) -> dict[str, object]:
    begin_gateway_restart_notice()
    return {}


def _restart_services_route_next(state: _RestartServicesState) -> str:
    return "restart_one" if int(state.get("index") or 0) < len(state.get("names") or []) else END


def _restart_services_restart_one_node(state: _RestartServicesState) -> dict[str, object]:
    names = list(state.get("names") or [])
    index = int(state.get("index") or 0)
    results = list(state.get("results") or [])
    if index >= len(names):
        return {"index": index, "results": results}
    name = names[index]
    try:
        results.append({"service": name, "ok": "true", "message": restart_service(name)})
    except Exception as exc:
        results.append({"service": name, "ok": "false", "message": str(exc)})
    return {"index": index + 1, "results": results}


@lru_cache(maxsize=1)
def _compiled_restart_services_graph():
    graph = StateGraph(_RestartServicesState)
    graph.add_node("expand", _restart_services_expand_node)
    graph.add_node("notice", _restart_services_notice_node)
    graph.add_node("restart_one", _restart_services_restart_one_node)
    graph.add_edge(START, "expand")
    graph.add_conditional_edges("expand", _restart_services_route_expanded, {"notice": "notice", END: END})
    graph.add_conditional_edges("notice", _restart_services_route_next, {"restart_one": "restart_one", END: END})
    graph.add_conditional_edges("restart_one", _restart_services_route_next, {"restart_one": "restart_one", END: END})
    return graph.compile()


def restart_services(names: list[str]) -> list[dict[str, str]]:
    final_state = _compiled_restart_services_graph().invoke(
        {"names": list(names or [])},
        config={"configurable": {"thread_id": "recovery-restart-services"}},
    )
    return list(final_state.get("results") or [])


def runtime_backups(checkpoint: Path = DEFAULT_CHECKPOINT) -> list[dict[str, Any]]:
    backups = list_runtime_store_backups(checkpoint)
    for backup in backups:
        path = Path(str(backup.get("path") or ""))
        if path.exists():
            backup["size_bytes"] = path.stat().st_size
            backup["modified_at"] = path.stat().st_mtime
    return backups


def restore_runtime_backup(*, checkpoint: Path = DEFAULT_CHECKPOINT, generation: int = 0) -> str:
    restored = restore_runtime_store_backup(checkpoint, generation=generation)
    return f"Restored runtime checkpoint from backup generation {generation}: {restored}"


def snapshot_config(*, data_dir: Path = NULLION_HOME, backup_dir: Path = DEFAULT_CONFIG_BACKUP_DIR) -> Path:
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    destination = backup_dir / timestamp
    destination.mkdir(parents=True, exist_ok=False)
    copied = 0
    for name in CONFIG_BACKUP_FILES:
        source = data_dir / name
        if source.exists() and source.is_file():
            shutil.copy2(source, destination / name)
            copied += 1
    if copied == 0:
        raise FileNotFoundError(f"No config files found in {data_dir}")
    return destination


def list_config_backups(*, backup_dir: Path = DEFAULT_CONFIG_BACKUP_DIR) -> list[dict[str, Any]]:
    if not backup_dir.exists():
        return []
    backups: list[dict[str, Any]] = []
    for candidate in sorted((p for p in backup_dir.iterdir() if p.is_dir()), reverse=True):
        files = sorted(p.name for p in candidate.iterdir() if p.is_file())
        backups.append({"name": candidate.name, "path": str(candidate), "files": files})
    return backups


def restore_config_backup(name: str = "latest", *, data_dir: Path = NULLION_HOME, backup_dir: Path = DEFAULT_CONFIG_BACKUP_DIR) -> str:
    backups = list_config_backups(backup_dir=backup_dir)
    if not backups:
        raise FileNotFoundError("No config backups available.")
    selected = backups[0] if name == "latest" else next((b for b in backups if b["name"] == name), None)
    if selected is None:
        raise FileNotFoundError(f"Config backup not found: {name}")
    backup_path = Path(str(selected["path"]))
    restored: list[str] = []
    for file_name in selected["files"]:
        if file_name in CONFIG_BACKUP_FILES:
            shutil.copy2(backup_path / file_name, data_dir / file_name)
            restored.append(file_name)
    if not restored:
        raise FileNotFoundError(f"Config backup has no restorable files: {backup_path}")
    return f"Restored config backup {selected['name']}: {', '.join(restored)}"


def recovery_status(*, host: str = DEFAULT_HOST, web_port: int = DEFAULT_WEB_PORT, checkpoint: Path = DEFAULT_CHECKPOINT) -> dict[str, Any]:
    env_exists = DEFAULT_ENV_FILE.exists()
    env_text = DEFAULT_ENV_FILE.read_text(errors="ignore") if env_exists else ""
    return {
        "ok": True,
        "web": web_health(host=host, port=web_port),
        "services": all_service_statuses(),
        "runtime": {
            "checkpoint": str(checkpoint),
            "checkpoint_exists": checkpoint.exists(),
            "backups": runtime_backups(checkpoint),
        },
        "config": {
            "env_file": str(DEFAULT_ENV_FILE),
            "env_file_exists": env_exists,
            "telegram_token_present": "NULLION_TELEGRAM_BOT_TOKEN=" in env_text,
            "backups": list_config_backups(),
        },
    }


def _telegram_recovery_identity() -> tuple[str, set[str], str]:
    """Return token, allowlist, and token source for recovery Telegram mode."""
    recovery_token = os.environ.get("NULLION_RECOVERY_TELEGRAM_BOT_TOKEN", "").strip()
    main_token = os.environ.get("NULLION_TELEGRAM_BOT_TOKEN", "").strip()
    token = recovery_token or main_token
    source = "recovery" if recovery_token else "main"
    allowed_raw = (
        os.environ.get("NULLION_RECOVERY_TELEGRAM_ALLOWED_CHAT_IDS", "").strip()
        or os.environ.get("NULLION_TELEGRAM_OPERATOR_CHAT_ID", "").strip()
    )
    allowed = {part.strip() for part in allowed_raw.split(",") if part.strip()}
    return token, allowed, source


def telegram_recovery_readiness() -> dict[str, Any]:
    token, allowed, source = _telegram_recovery_identity()
    return {
        "ready": bool(token and allowed),
        "token_source": source if token else None,
        "has_token": bool(token),
        "allowed_chat_ids": sorted(allowed),
        "takeover_required": source == "main",
    }


def handle_recovery_command(text: str, *, checkpoint: Path = DEFAULT_CHECKPOINT) -> str:
    parts = [part for part in text.strip().split() if part]
    if not parts:
        return "Recovery commands: /status, /services, /restart <service|all>, /backups, /restore runtime [generation], /restore config [latest|name]"
    command = parts[0].lower().lstrip("/")
    try:
        if command == "status":
            status = recovery_status(checkpoint=checkpoint)
            web = status["web"]
            failing = [
                svc for svc in status["services"]
                if svc.get("restartable") and not svc.get("running")
            ]
            lines = [
                f"Web: {'ok' if web.get('ok') else 'down'} ({web.get('detail')})",
                f"Runtime backups: {len(status['runtime']['backups'])}",
                f"Config backups: {len(status['config']['backups'])}",
            ]
            if failing:
                lines.append("Attention: " + ", ".join(str(svc["name"]) for svc in failing))
            if not status["config"]["telegram_token_present"]:
                lines.append("Telegram token is missing from .env.")
            return "\n".join(lines)
        if command == "services":
            return "\n".join(
                f"{svc['name']}: {svc['state']} - {svc['detail']}"
                for svc in all_service_statuses()
            )
        if command == "restart":
            names = parts[1:] or ["all"]
            return "\n".join(result["message"] for result in restart_services(names))
        if command == "backups":
            runtime_count = len(runtime_backups(checkpoint))
            config = list_config_backups()
            latest = f" Latest config: {config[0]['name']}." if config else ""
            return f"Runtime backups: {runtime_count}. Config backups: {len(config)}.{latest}"
        if command == "restore" and len(parts) >= 2:
            target = parts[1].lower()
            if target == "runtime":
                generation = int(parts[2]) if len(parts) >= 3 else 0
                return restore_runtime_backup(checkpoint=checkpoint, generation=generation)
            if target == "config":
                return restore_config_backup(parts[2] if len(parts) >= 3 else "latest")
        return "Unknown recovery command. Try /status, /services, /restart all, /backups, /restore runtime 0, or /restore config latest."
    except Exception as exc:
        return f"Recovery command failed: {exc}"


async def run_telegram_recovery_bot(*, checkpoint: Path = DEFAULT_CHECKPOINT, wait_for_takeover: bool = True) -> None:
    token, allowed, token_source = _telegram_recovery_identity()
    if not token:
        raise RuntimeError(
            "Telegram recovery mode needs NULLION_RECOVERY_TELEGRAM_BOT_TOKEN "
            "or NULLION_TELEGRAM_BOT_TOKEN."
        )
    if not allowed:
        raise RuntimeError(
            "Telegram recovery mode needs NULLION_RECOVERY_TELEGRAM_ALLOWED_CHAT_IDS "
            "or NULLION_TELEGRAM_OPERATOR_CHAT_ID."
        )
    try:
        from telegram import Update
        from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
    except ImportError as exc:
        raise RuntimeError("python-telegram-bot is required for Telegram recovery mode.") from exc

    if token_source == "main":
        while service_status("telegram").running:
            if not wait_for_takeover:
                raise RuntimeError(
                    "Refusing to poll the main Telegram bot token while ai.nullion.telegram is running. "
                    "Stop the normal Telegram service first, or configure NULLION_RECOVERY_TELEGRAM_BOT_TOKEN."
                )
            await asyncio.sleep(5)

    async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        message = update.effective_message
        if chat is None or message is None:
            return
        chat_id = str(chat.id)
        if chat_id not in allowed:
            await message.reply_text("Recovery access denied.")
            return
        reply = handle_recovery_command(message.text or "/status", checkpoint=checkpoint)
        await message.reply_text(reply[:3900])

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler(["status", "services", "restart", "backups", "restore"], on_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    try:
        while True:
            if token_source == "main" and service_status("telegram").running:
                break
            await asyncio.sleep(5)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def create_recovery_app(*, checkpoint: Path = DEFAULT_CHECKPOINT, web_port: int = DEFAULT_WEB_PORT):
    try:
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse, PlainTextResponse
    except ImportError as exc:
        raise RuntimeError("fastapi is required for recovery HTTP mode.") from exc

    app = FastAPI(title="Nullion Recovery")

    @app.get("/health")
    async def health():
        return {"ok": True, "service": "nullion-recovery"}

    @app.get("/status")
    async def status():
        return recovery_status(web_port=web_port, checkpoint=checkpoint)

    @app.post("/command")
    async def command(payload: dict[str, Any]):
        text = str(payload.get("command") or "")
        reply = handle_recovery_command(text, checkpoint=checkpoint)
        return JSONResponse({"ok": not reply.startswith("Recovery command failed:"), "reply": reply})

    @app.get("/command")
    async def command_get(q: str = "/status"):
        return PlainTextResponse(handle_recovery_command(q, checkpoint=checkpoint))

    return app


def _print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, sort_keys=True))


def cli(argv: list[str] | None = None) -> int:
    return run_user_facing_entrypoint(lambda: _cli_impl(argv))


def _cli_impl(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nullion-recovery", description="Out-of-band Nullion recovery control plane.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT))
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("status")
    sub.add_parser("services")
    restart = sub.add_parser("restart")
    restart.add_argument("services", nargs="*", default=["all"])
    sub.add_parser("backups")
    snapshot = sub.add_parser("snapshot-config")
    snapshot.add_argument("--data-dir", default=str(NULLION_HOME))
    restore = sub.add_parser("restore")
    restore.add_argument("target", choices=["runtime", "config"])
    restore.add_argument("generation_or_name", nargs="?")
    serve = sub.add_parser("serve")
    serve.add_argument("--host", default=DEFAULT_HOST)
    serve.add_argument("--port", type=int, default=DEFAULT_RECOVERY_PORT)
    serve.add_argument("--web-port", type=int, default=DEFAULT_WEB_PORT)
    telegram = sub.add_parser("telegram")
    telegram.add_argument("--once-command", default="")
    telegram.add_argument(
        "--no-wait",
        action="store_true",
        help="Fail immediately if using the main bot token while the normal Telegram service is running.",
    )

    args = parser.parse_args(argv)
    _load_env_file(Path(args.env_file).expanduser())
    checkpoint = Path(args.checkpoint).expanduser()

    if args.command in (None, "status"):
        _print_json(recovery_status(checkpoint=checkpoint))
        return 0
    if args.command == "services":
        _print_json(all_service_statuses())
        return 0
    if args.command == "restart":
        _print_json(restart_services(list(args.services or ["all"])))
        return 0
    if args.command == "backups":
        _print_json({"runtime": runtime_backups(checkpoint), "config": list_config_backups()})
        return 0
    if args.command == "snapshot-config":
        print(snapshot_config(data_dir=Path(args.data_dir).expanduser()))
        return 0
    if args.command == "restore":
        if args.target == "runtime":
            generation = int(args.generation_or_name or "0")
            print(restore_runtime_backup(checkpoint=checkpoint, generation=generation))
        else:
            print(restore_config_backup(args.generation_or_name or "latest"))
        return 0
    if args.command == "serve":
        import uvicorn

        uvicorn.run(create_recovery_app(checkpoint=checkpoint, web_port=args.web_port), host=args.host, port=args.port)
        return 0
    if args.command == "telegram":
        if args.once_command:
            print(handle_recovery_command(args.once_command, checkpoint=checkpoint))
            return 0
        asyncio.run(run_telegram_recovery_bot(checkpoint=checkpoint, wait_for_takeover=not args.no_wait))
        return 0
    parser.error(f"Unhandled command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(cli())

"""Nulliøn CLI — interactive REPL for chatting with the agent without Telegram.

Usage:
    nullion-cli                     # interactive mode
    nullion-cli --env-file .env     # load settings from a .env file
    nullion-cli --one-shot "Hi"     # non-interactive: print reply and exit

Slash commands available during a session:
    /new      Clear conversation history and start fresh
    /status   Show active task frames, pending approvals, reminders
    /tools    List registered tools and their risk levels
    /skills   List learned skills
    /quit     Exit
    /help     Show this command list
"""
from __future__ import annotations

import argparse
import errno
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Any

from nullion.entrypoint_guard import run_user_facing_entrypoint
from nullion.version import version_tag


_BANNER_TEMPLATE = """
  ┌─────────────────────────────────┐
  │  Nulliøn  ·  CLI  ·  {version:<7} │
  │  Type /help for commands        │
  └─────────────────────────────────┘
"""
_BANNER = _BANNER_TEMPLATE.format(version=version_tag())

_HELP_TEXT = """\
Commands:
  /new      Clear conversation history and start fresh
  /status   Show active task frames, pending approvals, reminders
  /tools    List registered tools and their risk levels
  /skills   List learned skills
  /quit     Exit (/exit also works)
  /help     Show this help
"""


# ── entry point ───────────────────────────────────────────────────────────────

def cli() -> None:
    return run_user_facing_entrypoint(_cli_impl)


def _cli_impl() -> None:
    parser = argparse.ArgumentParser(prog="nullion-cli", description="Nulliøn interactive CLI")
    parser.add_argument("--env-file", default=None, help="Path to .env file (default: ~/.nullion/.env)")
    parser.add_argument("--one-shot", default=None, metavar="MESSAGE", help="Send one message and exit")
    parser.add_argument("--no-banner", action="store_true", help="Suppress startup banner")

    sub = parser.add_subparsers(dest="command")
    update_parser = sub.add_parser("update", help="Update Nullion to the latest release (auto-rollback on failure)")
    update_parser.add_argument("--port", type=int, default=8742, help="Web UI port for health check")
    update_parser.add_argument(
        "--ignore-checks",
        "--force",
        action="store_true",
        dest="ignore_check_failures",
        help="Install the update even if post-update health checks fail",
    )
    update_parser.add_argument(
        "--hash",
        action="store_true",
        dest="update_to_hash",
        help="Update to the latest repository commit instead of the latest release tag",
    )
    skill_pack_parser = sub.add_parser("skill-pack", help="Install, list, or enable skill packs")
    skill_pack_sub = skill_pack_parser.add_subparsers(dest="skill_pack_command")
    skill_pack_install = skill_pack_sub.add_parser("install", help="Install a SKILL.md skill pack from a folder or Git URL")
    skill_pack_install.add_argument("source", help="Local OpenClaw skills folder or Git repository URL")
    skill_pack_install.add_argument("--id", dest="pack_id", default=None, help="Pack ID, e.g. owner/pack")
    skill_pack_install.add_argument("--no-enable", action="store_true", help="Install without adding to NULLION_ENABLED_SKILL_PACKS")
    skill_pack_install.add_argument("--force", action="store_true", help="Replace an existing installed pack")
    skill_pack_sub.add_parser("list", help="List installed skill packs")
    skill_pack_enable = skill_pack_sub.add_parser("enable", help="Enable an installed skill pack")
    skill_pack_enable.add_argument("pack_id", help="Pack ID, e.g. owner/pack")

    args = parser.parse_args()

    # ── update subcommand ────────────────────────────────────────────────────
    if args.command == "update":
        _run_update_cli(
            getattr(args, "port", 8742),
            ignore_check_failures=getattr(args, "ignore_check_failures", False),
            update_channel="hash" if getattr(args, "update_to_hash", False) else "release",
        )
        return
    if args.command == "skill-pack":
        _run_skill_pack_cli(args)
        return

    _load_env(args.env_file)

    try:
        runtime, orchestrator, registry = _build_runtime()
    except Exception as exc:
        print(f"\n  ✗  Could not start Nullion: {exc}", file=sys.stderr)
        print("  →  Check the Nullion logs for startup details.", file=sys.stderr)
        sys.exit(1)

    if args.one_shot:
        reply = _send_message(args.one_shot, runtime=runtime, orchestrator=orchestrator, registry=registry)
        print(reply)
        return

    if not args.no_banner:
        print(_BANNER)

    conversation_id = f"cli:{os.getpid()}"
    history: list[dict[str, Any]] = []

    while True:
        try:
            user_input = input("  You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n  Bye!")
            break

        if not user_input:
            continue

        # Slash commands
        if user_input.startswith("/"):
            cmd = user_input.lower().split()[0]
            if cmd in ("/quit", "/exit", "/q"):
                print("  Bye!")
                break
            elif cmd == "/new":
                history.clear()
                print("  ✓  Starting fresh.")
                continue
            elif cmd == "/help":
                print(textwrap.indent(_HELP_TEXT, "  "))
                continue
            elif cmd == "/status":
                _print_status(runtime)
                continue
            elif cmd == "/tools":
                _print_tools(registry)
                continue
            elif cmd == "/skills":
                _print_skills(runtime)
                continue
            else:
                print(f"  Unknown command: {cmd}  (try /help)")
                continue

        # Normal message
        try:
            from nullion.chat_operator import run_turn as _run_turn  # noqa: F401
        except ImportError:
            pass

        try:
            reply = _send_message(
                user_input,
                runtime=runtime,
                orchestrator=orchestrator,
                registry=registry,
                conversation_id=conversation_id,
                history=history,
            )
        except Exception as exc:
            reply = f"[Error: {exc}]"

        print(f"\n  Nullion: {reply}\n")

        # Accumulate history for context
        history.append({"role": "user", "content": [{"type": "text", "text": user_input}]})
        history.append({"role": "assistant", "content": [{"type": "text", "text": reply}]})


# ── update command ───────────────────────────────────────────────────────────

def _wait_for_web_health(port: int = 8742, *, timeout: float = 45.0, interval: float = 1.5) -> bool:
    import json
    import time
    import urllib.request

    deadline = time.monotonic() + timeout
    url = f"http://localhost:{port}/api/health"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=min(interval, 2.0)) as response:
                if getattr(response, "status", 200) >= 400:
                    time.sleep(interval)
                    continue
                payload = json.loads(response.read())
                if payload.get("status") == "ok":
                    return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def _run_update_cli(
    port: int = 8742,
    *,
    ignore_check_failures: bool = False,
    update_channel: str = "release",
) -> None:
    import asyncio
    from pathlib import Path
    from nullion.updater import run_update, UpdateProgress

    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    RED    = "\033[31m"
    CYAN   = "\033[36m"
    RESET  = "\033[0m"
    BOLD   = "\033[1m"

    print(f"\n{BOLD}  Checking for updates…{RESET}\n")

    def emit(p: UpdateProgress) -> None:
        icon  = f"{GREEN}✓{RESET}" if p.ok else f"{RED}✗{RESET}"
        label = f"{CYAN}[{p.step}]{RESET}"
        print(f"  {icon}  {label}  {p.message}")

    result = asyncio.run(
        run_update(
            emit=emit,
            web_port=port,
            ignore_check_failures=ignore_check_failures,
            update_channel=update_channel,
        )
    )

    print()
    if result.success:
        if result.from_version == result.to_version:
            # No actual update happened — don't disturb a webview the user
            # already has open. Only ensure tray is up.
            print(f"  {GREEN}Already up to date:{RESET} {result.from_version}")
            _open_update_entrypoint(port=port, version_changed=False)
        else:
            print(f"  {GREEN}{BOLD}Updated successfully:{RESET}  {result.from_version} → {result.to_version}")
            print()
            try:
                answer = input(f"  {BOLD}Restart Nullion now?{RESET} [Y/n] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = "n"
            if answer in ("", "y", "yes"):
                print(f"  {CYAN}Restarting…{RESET}")
                _service_cmd(_detect_service_manager(), "restart")
                if _wait_for_web_health(port):
                    print(f"  {GREEN}Web UI is healthy on port {port}.{RESET}")
                    # Real version change + restart: drop the stale webview
                    # so the user sees the new code.
                    _open_update_entrypoint(port=port, version_changed=True)
                else:
                    print(f"  {RED}Web UI did not come back on port {port}; rolling back…{RESET}")
                    try:
                        from nullion.updater import rollback
                        snap = Path(result.snapshot_path)
                        rolled_back = bool(result.snapshot_path) and snap.exists() and rollback(snap)
                    except Exception:
                        rolled_back = False
                    if rolled_back:
                        print(f"  {YELLOW}Rollback restored {result.from_version}. Restarting previous version…{RESET}")
                        _service_cmd(_detect_service_manager(), "restart")
                        if _wait_for_web_health(port):
                            print(f"  {GREEN}Previous version is healthy on port {port}.{RESET}")
                            # Rolled back to the same version that was
                            # already running — no webview swap needed.
                            _open_update_entrypoint(port=port, version_changed=False)
                        else:
                            print(f"  {RED}Rollback completed, but the web UI is still not healthy. Check ~/.nullion/logs/.{RESET}")
                    else:
                        print(f"  {RED}Rollback could not be completed. Check ~/.nullion/logs/.{RESET}")
            else:
                # User declined the restart — running version unchanged.
                print(f"  {YELLOW}Run `nullion --restart` when ready.{RESET}")
    else:
        print(f"  {RED}Update failed:{RESET} {result.error}")
        if result.rolled_back:
            print(f"  {YELLOW}Rolled back to previous version automatically.{RESET}")
        else:
            print(f"  {RED}Rollback also failed — check logs at ~/.nullion/logs/{RESET}")
    print()
    sys.exit(0 if result.success else 1)


def _run_skill_pack_cli(args) -> None:
    if not getattr(args, "skill_pack_command", None):
        print("Usage: nullion-cli skill-pack {install,list,enable}", file=sys.stderr)
        sys.exit(2)
    if args.skill_pack_command == "list":
        from nullion.skill_pack_installer import list_installed_skill_packs

        packs = list_installed_skill_packs()
        if not packs:
            print("No skill packs installed.")
            return
        for pack in packs:
            warning_text = f" ({len(pack.warnings)} warning(s))" if pack.warnings else ""
            print(f"{pack.pack_id} — {pack.skills_count} skill(s){warning_text}")
            print(f"  source: {pack.source}")
            if pack.path:
                print(f"  path: {pack.path}")
        return
    if args.skill_pack_command == "enable":
        pack_id = _enable_skill_pack_id(args.pack_id)
        print(f"Enabled {pack_id}. Restart Nullion to load it.")
        return
    if args.skill_pack_command == "install":
        from nullion.skill_pack_installer import install_skill_pack

        try:
            pack = install_skill_pack(args.source, pack_id=args.pack_id, force=bool(args.force))
        except Exception as exc:
            print(f"Install failed: {exc}", file=sys.stderr)
            sys.exit(1)
        print(f"Installed {pack.pack_id} from {pack.source}")
        print(f"  {pack.skills_count} SKILL.md file(s)")
        if pack.warnings:
            print("  Review warnings:")
            for warning in pack.warnings[:10]:
                print(f"  - {warning}")
        if not args.no_enable:
            _enable_skill_pack_id(pack.pack_id)
            print("Enabled. Restart Nullion to load it.")
        else:
            print(f"Not enabled. Run: nullion-cli skill-pack enable {pack.pack_id}")
        return
    print(f"Unknown skill-pack command: {args.skill_pack_command}", file=sys.stderr)
    sys.exit(2)


def _enable_skill_pack_id(pack_id: str) -> str:
    from nullion.skill_pack_installer import BUILTIN_SKILL_PACK_PROMPTS, get_installed_skill_pack, normalize_pack_id

    normalized = normalize_pack_id(pack_id)
    if get_installed_skill_pack(normalized) is None and normalized != "google/skills" and normalized not in BUILTIN_SKILL_PACK_PROMPTS:
        raise SystemExit(f"Skill pack is not installed: {normalized}")
    env_path = Path(os.environ.get("NULLION_ENV_FILE") or os.path.expanduser("~/.nullion/.env"))
    env_path.parent.mkdir(parents=True, exist_ok=True)
    current = []
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.strip().startswith("NULLION_ENABLED_SKILL_PACKS="):
                _, _, value = line.partition("=")
                current = [item.strip().strip('"').strip("'") for item in value.split(",") if item.strip().strip('"').strip("'")]
                break
    if normalized not in current:
        current.append(normalized)
    _merge_env_updates(env_path, {"NULLION_ENABLED_SKILL_PACKS": ", ".join(current)})
    os.environ["NULLION_ENABLED_SKILL_PACKS"] = ", ".join(current)
    return normalized


def _merge_env_updates(env_path: Path, updates: dict[str, str]) -> None:
    lines: list[str] = []
    written: set[str] = set()
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                lines.append(line)
                continue
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                lines.append(f'{key}="{updates[key]}"')
                written.add(key)
            else:
                lines.append(line)
    for key, value in updates.items():
        if key not in written:
            lines.append(f'{key}="{value}"')
    env_path.write_text("\n".join(lines) + "\n")


# ── core send ─────────────────────────────────────────────────────────────────

def _send_message(
    message: str,
    *,
    runtime,
    orchestrator,
    registry,
    conversation_id: str = "cli:0",
    history: list[dict] | None = None,
) -> str:
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.response_sanitizer import sanitize_user_visible_reply

    result = orchestrator.run_turn(
        conversation_id=conversation_id,
        principal_id=conversation_id,
        user_message=message,
        conversation_history=history or [],
        tool_registry=registry,
        policy_store=runtime.store,
        approval_store=runtime.store,
    )

    if result.suspended_for_approval:
        return (
            f"⏸  Approval required (id: {result.approval_id})\n"
            "   The bot needs your permission to proceed. "
            "In Telegram you'd tap Approve — CLI approval flow coming soon."
        )

    reply = result.final_text or "(no reply)"
    return sanitize_user_visible_reply(
        user_message=message,
        reply=reply,
        tool_results=getattr(result, "tool_results", None),
        source="agent",
    ) or reply


# ── status / tools / skills display ──────────────────────────────────────────

def _print_status(runtime) -> None:
    store = runtime.store
    lines = [""]

    # Active task frames
    active_frames = [
        f for f in store.task_frames.values()
        if str(f.status) in ("active", "running", "waiting_approval")
    ]
    if active_frames:
        lines.append("  Task frames:")
        for frame in active_frames:
            target = f" → {frame.target.value}" if frame.target else ""
            lines.append(f"    [{frame.status}] {frame.summary}{target}")
    else:
        lines.append("  No active task frames.")

    # Pending approvals
    pending = [a for a in store.list_approval_requests() if a.status == "pending"]
    if pending:
        lines.append(f"\n  Pending approvals: {len(pending)}")
        for a in pending[:3]:
            lines.append(f"    {a.approval_id[:8]}…  {a.tool_name}")
    else:
        lines.append("  No pending approvals.")

    # Reminders
    reminders = [r for r in store.list_reminders() if not getattr(r, "delivered_at", None)]
    if reminders:
        lines.append(f"\n  Reminders: {len(reminders)}")
    else:
        lines.append("  No pending reminders.")

    print("\n".join(lines))


def _print_tools(registry) -> None:
    specs = list(registry.list_specs())
    if not specs:
        print("  No tools registered.")
        return
    print(f"\n  {len(specs)} tools registered:\n")
    for spec in sorted(specs, key=lambda s: s.name):
        risk = getattr(spec, "risk_level", None)
        risk_label = f"  [{risk.value}]" if risk is not None else ""
        desc = (spec.description or "")[:60]
        if len(spec.description or "") > 60:
            desc += "…"
        print(f"    {spec.name:<28}{risk_label:<14} {desc}")
    print()


def _print_skills(runtime) -> None:
    skills = list(runtime.store.skills.values())
    if not skills:
        print("  No skills learned yet.")
        return
    print(f"\n  {len(skills)} skills:\n")
    for skill in sorted(skills, key=lambda s: s.title):
        print(f"    {skill.title}")
        print(f"      Trigger: {skill.trigger}")
    print()


# ── bootstrap ─────────────────────────────────────────────────────────────────

def _load_env(env_file: str | None) -> None:
    """Load KEY=VALUE pairs from a .env file into os.environ."""
    from nullion.config import load_default_env_file_into_environ

    load_default_env_file_into_environ(env_file)


def _parse_env_file(path: Path) -> None:
    from nullion.config import load_env_file_into_environ

    load_env_file_into_environ(path)


def _build_runtime():
    """Bootstrap a minimal runtime + orchestrator for the CLI."""
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime_persistence import build_runtime_from_settings
    from nullion.settings import Settings
    from nullion.tools import create_default_tool_registry

    settings = Settings()
    runtime = build_runtime_from_settings(settings)
    registry = create_default_tool_registry()
    model_client = _build_model_client(settings)
    runtime.model_client = model_client
    orchestrator = AgentOrchestrator(model_client=model_client)
    return runtime, orchestrator, registry


def _build_model_client(settings):
    """Build a model client from environment / settings."""
    try:
        from nullion.model_clients import build_model_client_from_settings
        return build_model_client_from_settings(settings)
    except (ImportError, AttributeError):
        pass

    provider = os.environ.get("NULLION_MODEL_PROVIDER", "").lower()
    if provider == "openai" or os.environ.get("OPENAI_API_KEY"):
        return _build_openai_client()
    if provider == "anthropic" or os.environ.get("ANTHROPIC_API_KEY"):
        return _build_anthropic_client()
    raise RuntimeError(
        "No model provider configured. "
        "Set OPENAI_API_KEY or ANTHROPIC_API_KEY in your .env file."
    )


def _build_anthropic_client():
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed. Run: pip install anthropic")
    api_key = os.environ["ANTHROPIC_API_KEY"]
    client = anthropic.Anthropic(api_key=api_key)
    model = os.environ.get("NULLION_MODEL", "").strip()
    if not model:
        raise RuntimeError("Anthropic provider requires NULLION_MODEL to be set.")

    class _AnthropicAdapter:
        def create(self, *, messages, tools, max_tokens=4096, system=None):
            kwargs: dict = dict(model=model, messages=messages, max_tokens=max_tokens)
            if system:
                kwargs["system"] = system
            if tools:
                kwargs["tools"] = tools
            resp = client.messages.create(**kwargs)
            return {
                "stop_reason": resp.stop_reason,
                "content": [
                    {"type": b.type, "text": b.text}
                    if b.type == "text"
                    else {"type": b.type, "id": b.id, "name": b.name, "input": b.input}
                    for b in resp.content
                ],
            }

    return _AnthropicAdapter()


def _build_openai_client():
    try:
        import openai
    except ImportError:
        raise RuntimeError("openai package not installed. Run: pip install openai")
    api_key = os.environ["OPENAI_API_KEY"]
    client = openai.OpenAI(api_key=api_key)
    model = os.environ.get("NULLION_MODEL", "gpt-4o")

    class _OpenAIAdapter:
        def create(self, *, messages, tools, max_tokens=4096, system=None):
            msgs = list(messages)
            if system:
                msgs = [{"role": "system", "content": system}] + msgs
            kwargs: dict = dict(model=model, messages=msgs, max_tokens=max_tokens)
            if tools:
                kwargs["tools"] = [
                    {"type": "function", "function": {"name": t["name"], "description": t.get("description", ""), "parameters": t.get("input_schema", {})}}
                    for t in tools
                ]
                kwargs["tool_choice"] = "auto"
            resp = client.chat.completions.create(**kwargs)
            choice = resp.choices[0]
            msg = choice.message
            content = []
            if msg.content:
                content.append({"type": "text", "text": msg.content})
            if msg.tool_calls:
                from nullion.model_clients import parse_tool_arguments

                for tc in msg.tool_calls:
                    content.append({"type": "tool_use", "id": tc.id, "name": tc.function.name, "input": parse_tool_arguments(tc.function.arguments)})
            stop = "tool_use" if msg.tool_calls else "end_turn"
            return {"stop_reason": stop, "content": content}

    return _OpenAIAdapter()


# ── nullion shortcut command ───────────────────────────────────────────────────
# Installed as the `nullion` entry point — quick operational shortcuts so you
# don't need to remember launchctl/systemctl invocations.

_NULLION_HOME = Path.home() / ".nullion"
_LOG_DIR      = _NULLION_HOME / "logs"
_ENV_FILE     = _NULLION_HOME / ".env"
_LAUNCHD_LABELS = (
    "com.nullion.web",
    "com.nullion.tray",
    "ai.nullion.slack",
    "com.nullion.slack",
    "ai.nullion.discord",
    "com.nullion.discord",
    "ai.nullion.recovery",
    "ai.nullion.telegram",
    "com.nullion.telegram",
)
_SYSTEMD_SERVICES = (
    "nullion.service",
    "nullion-tray.service",
    "nullion-slack.service",
    "nullion-discord.service",
    "nullion-recovery.service",
    "nullion-telegram.service",
)
_WINDOWS_TASKS = (
    "Nullion Web Dashboard",
    "Nullion Tray",
    "Nullion Slack",
    "Nullion Discord",
    "Nullion Recovery",
    "Nullion Telegram",
)

def _launchd_plist() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / "com.nullion.web.plist"

def _launchd_plist_for_label(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"

def _systemd_service() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / "nullion.service"

def _systemd_service_for_name(name: str) -> Path:
    return Path.home() / ".config" / "systemd" / "user" / name

def _detect_service_manager() -> str:
    if sys.platform == "darwin" and _launchd_plist().exists():
        return "launchd"
    if shutil.which("systemctl") and _systemd_service().exists():
        return "systemd"
    return "unknown"


def _switch_model(*, model: str | None, provider: str | None) -> None:
    """Write provider/model to encrypted credentials and .env, then restart service."""
    env_path   = Path.home() / ".nullion" / ".env"

    from nullion.credential_store import load_encrypted_credentials, save_encrypted_credentials

    credentials_db_path = Path.home() / ".nullion" / "runtime.db"
    creds: dict = load_encrypted_credentials(db_path=credentials_db_path) or {}
    if provider:
        creds["provider"] = provider
    if model:
        creds["model"] = model
    save_encrypted_credentials(creds, db_path=credentials_db_path)

    # Update .env
    updates: dict[str, str] = {}
    if provider:
        updates["NULLION_MODEL_PROVIDER"] = provider
    if model:
        updates["NULLION_MODEL"] = model
    if updates:
        lines: list[str] = []
        written: set[str] = set()
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    lines.append(line); continue
                key = stripped.split("=", 1)[0].strip()
                if key in updates:
                    lines.append(f'{key}="{updates[key]}"'); written.add(key)
                else:
                    lines.append(line)
        for k, v in updates.items():
            if k not in written:
                lines.append(f'{k}="{v}"')
        env_path.write_text("\n".join(lines) + "\n")

    parts = []
    if provider:
        parts.append(f"provider → {provider}")
    if model:
        parts.append(f"model → {model}")
    print(f"  ✓  {', '.join(parts)}")
    print("  →  Restart the service to apply: nullion --restart")


def _begin_gateway_restart_notice() -> None:
    try:
        from nullion.gateway_notifications import begin_gateway_restart

        begin_gateway_restart(async_delivery=False)
    except Exception:
        # Restart commands must remain operational even if notification state or
        # chat delivery is temporarily unavailable.
        pass


def nullion_ctl() -> None:
    return run_user_facing_entrypoint(_nullion_ctl_impl)


def _nullion_ctl_impl() -> None:
    """Top-level `nullion` command — operational shortcuts."""
    parser = argparse.ArgumentParser(
        prog="nullion",
        description="Nullion shortcuts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            examples:
              nullion --logs          tail live log output
              nullion --errors        tail error log
              nullion --dashboard     open the dashboard
              nullion --config        open .env in $EDITOR
              nullion --stop          stop the background service and tray icon
              nullion --restart       restart the service, tray icon, and Web UI
              nullion --status        show service and tray status
              nullion tray install    install/start the tray icon
              nullion update          safe update with auto-rollback
              nullion repair windows-install
              nullion uninstall       remove services, data, and installed package
        """),
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--logs",    action="store_true", help="Tail the live log")
    group.add_argument("--errors",  action="store_true", help="Tail the error log")
    group.add_argument("--dashboard", "--web", action="store_true", help="Open the Nullion dashboard")
    group.add_argument("--config",  action="store_true", help="Open ~/.nullion/.env in $EDITOR")
    group.add_argument("--stop",    action="store_true", help="Stop the background service and tray icon")
    group.add_argument("--restart", action="store_true", help="Restart the service, tray icon, and Web UI")
    group.add_argument("--status",  action="store_true", help="Show service and tray status")
    group.add_argument("--model",   metavar="MODEL",     help="Switch model (e.g. gpt-5.5, gpt-4o)")
    group.add_argument("--provider",metavar="PROVIDER",  help="Switch provider (openai, anthropic, codex)")

    sub = parser.add_subparsers(dest="command")
    up = sub.add_parser("update", help="Safe update with auto-rollback")
    up.add_argument("--port", type=int, default=8742)
    up.add_argument(
        "--ignore-checks",
        "--force",
        action="store_true",
        dest="ignore_check_failures",
        help="Install the update even if post-update health checks fail",
    )
    up.add_argument(
        "--hash",
        action="store_true",
        dest="update_to_hash",
        help="Update to the latest repository commit instead of the latest release tag",
    )
    uninstall = sub.add_parser("uninstall", help="Uninstall Nullion services, data, and package")
    uninstall.add_argument("--yes", "-y", action="store_true", help="Do not prompt for confirmation")
    uninstall.add_argument("--dry-run", action="store_true", help="Show what would be removed without deleting anything")
    uninstall.add_argument("--keep-data", action="store_true", help="Remove services/package but keep ~/.nullion data")
    repair = sub.add_parser("repair", help="Repair a broken installed Nullion environment")
    repair_sub = repair.add_subparsers(dest="repair_command")
    windows_repair = repair_sub.add_parser("windows-install", help="Repair Windows update/launcher damage")
    windows_repair.add_argument("--ref", default="origin/main", help="Git ref to reset the installed source to")
    windows_repair.add_argument("--no-fetch", action="store_true", help="Skip git fetch before resetting")
    windows_repair.add_argument("--no-start", action="store_true", help="Repair tasks without starting services")
    windows_repair.add_argument("--dry-run", action="store_true", help="Show repair actions without changing files/tasks")
    tray = sub.add_parser("tray", help="Install, start, stop, or inspect the tray icon")
    tray_sub = tray.add_subparsers(dest="tray_command")
    tray_install = tray_sub.add_parser("install", help="Install and start the tray icon")
    tray_install.add_argument("--port", type=int, default=8742, help="Web UI port")
    tray_sub.add_parser("start", help="Start the tray icon")
    tray_sub.add_parser("stop", help="Stop the tray icon")
    tray_sub.add_parser("status", help="Show tray service status")

    args = parser.parse_args()

    if args.command == "update":
        _run_update_cli(
            getattr(args, "port", 8742),
            ignore_check_failures=getattr(args, "ignore_check_failures", False),
            update_channel="hash" if getattr(args, "update_to_hash", False) else "release",
        )
        return
    if args.command == "uninstall":
        _run_uninstall_cli(
            yes=getattr(args, "yes", False),
            dry_run=getattr(args, "dry_run", False),
            keep_data=getattr(args, "keep_data", False),
        )
        return
    if args.command == "repair":
        if getattr(args, "repair_command", None) == "windows-install":
            _run_windows_install_repair(
                ref=getattr(args, "ref", "origin/main"),
                fetch=not getattr(args, "no_fetch", False),
                start=not getattr(args, "no_start", False),
                dry_run=getattr(args, "dry_run", False),
            )
            return
        repair.print_help()
        return
    if args.command == "tray":
        _run_tray_cli(getattr(args, "tray_command", None), port=getattr(args, "port", 8742))
        return

    sm = _detect_service_manager()

    if getattr(args, "model", None) or getattr(args, "provider", None):
        _switch_model(model=getattr(args, "model", None), provider=getattr(args, "provider", None))
        return

    if args.logs:
        log = _LOG_DIR / "nullion.log"
        if not log.exists():
            print(f"Log not found: {log}")
            sys.exit(1)
        os.execvp("tail", ["tail", "-f", str(log)])

    elif args.dashboard:
        _open_desktop_entrypoint(port=_default_web_port(), force_reload=False)

    elif args.errors:
        log = _LOG_DIR / "nullion-error.log"
        if not log.exists():
            print(f"Error log not found: {log}")
            sys.exit(1)
        os.execvp("tail", ["tail", "-f", str(log)])

    elif args.config:
        editor = os.environ.get("EDITOR", "nano")
        if not _ENV_FILE.exists():
            print(f"Config not found: {_ENV_FILE}")
            sys.exit(1)
        os.execvp(editor, [editor, str(_ENV_FILE)])

    elif args.stop:
        _service_cmd(sm, "stop")

    elif args.restart:
        _service_cmd(sm, "restart")
        _open_desktop_entrypoint(port=_default_web_port(), force_reload=True)

    elif args.status:
        _service_cmd(sm, "status")
        if sys.platform == "darwin" or os.name == "nt":
            _tray_status()

    else:
        # No flag — print a helpful summary
        print()
        print("  Nullion")
        print(f"  Logs:    {_LOG_DIR / 'nullion.log'}")
        print(f"  Errors:  {_LOG_DIR / 'nullion-error.log'}")
        print(f"  Config:  {_ENV_FILE}")
        sm = _detect_service_manager()
        if sm == "launchd":
            print(f"  Stop:    launchctl unload {_launchd_plist()}")
        elif sm == "systemd":
            print(f"  Stop:    systemctl --user stop nullion")
        print()
        print("  Run `nullion --help` for shortcuts.")
        print()


def _run_uninstall_cli(*, yes: bool = False, dry_run: bool = False, keep_data: bool = False) -> None:
    """Remove installer-managed Nullion services, local files, and package hooks."""
    print()
    print("  Nullion uninstall")
    print()
    print("  This will remove:")
    print("   - launchd/systemd services or Windows scheduled tasks for Nullion")
    print("   - any open Nullion desktop webview window")
    print("   - installer-managed tray/web launcher files")
    if keep_data:
        print("   - installed package entry points when they are outside ~/.nullion")
        print("   - ~/.nullion data will be kept")
    else:
        print(f"   - {_NULLION_HOME} (config, credentials, runtime data, logs, venv, source)")
        print("   - installed package entry points when they are outside ~/.nullion")
    print()

    if dry_run:
        print("  Dry run only. Nothing will be changed.")
    elif not yes:
        response = input("  Type 'uninstall' to continue: ").strip().lower()
        if response != "uninstall":
            print("  Cancelled.")
            return

    actions: list[str] = []
    actions.extend(_remove_launchd_services(dry_run=dry_run))
    actions.extend(_remove_systemd_services(dry_run=dry_run))
    actions.extend(_remove_windows_tasks(dry_run=dry_run))
    actions.append(_close_webview_for_uninstall(dry_run=dry_run))
    actions.extend(_remove_managed_launcher_files(dry_run=dry_run))
    if not keep_data:
        actions.append(_remove_path(_NULLION_HOME, dry_run=dry_run))
    actions.extend(_uninstall_python_package(dry_run=dry_run, keep_data=keep_data))

    print()
    for action in actions:
        print(f"  {action}")
    print()
    print("  Dry run complete." if dry_run else "  Nullion uninstall complete.")


def _run_uninstall_subprocess(command: list[str], *, dry_run: bool) -> None:
    import subprocess

    if dry_run:
        return
    subprocess.run(command, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


_WINDOWS_REPAIR_TASKS: tuple[dict[str, object], ...] = (
    {
        "task": "Nullion Web Dashboard",
        "bat": "start-nullion.bat",
        "module": "nullion.web_app",
        "args": ("--env-file", "{env}", "--checkpoint", "{checkpoint}"),
        "stdout": "nullion.log",
        "stderr": "nullion-error.log",
    },
    {
        "task": "Nullion Tray",
        "bat": "start-nullion-tray.bat",
        "module": "nullion.tray_app",
        "args": ("--env-file", "{env}"),
        "stdout": "tray.log",
        "stderr": "tray-error.log",
    },
    {
        "task": "Nullion Telegram",
        "bat": "start-nullion-telegram.bat",
        "module": "nullion.telegram_entrypoint",
        "args": ("--checkpoint", "{checkpoint}", "--env-file", "{env}"),
        "stdout": "telegram.log",
        "stderr": "telegram-error.log",
    },
)


def _run_windows_install_repair(
    *,
    ref: str = "origin/main",
    fetch: bool = True,
    start: bool = True,
    dry_run: bool = False,
) -> None:
    """Repair Windows installs damaged by interrupted pip/update entrypoint rewrites."""
    if os.name != "nt":
        raise SystemExit("windows-install repair is only supported on Windows.")
    actions = _repair_windows_install(ref=ref, fetch=fetch, start=start, dry_run=dry_run)
    print()
    print("  Nullion Windows install repair")
    print()
    for action in actions:
        print(f"  {action}")
    print()
    print("  Dry run complete." if dry_run else "  Repair complete.")


def _repair_windows_install(
    *,
    ref: str = "origin/main",
    fetch: bool = True,
    start: bool = True,
    dry_run: bool = False,
) -> list[str]:
    actions: list[str] = []
    home = _NULLION_HOME
    logs = _LOG_DIR
    env_file = _ENV_FILE
    checkpoint = home / "runtime.db"
    source = _windows_repair_source_dir()
    python = _windows_repair_python()
    site_packages = _windows_repair_site_packages(python)

    if not dry_run:
        logs.mkdir(parents=True, exist_ok=True)
    actions.extend(_stop_windows_repair_services(dry_run=dry_run))
    actions.extend(_clean_windows_pip_leftovers(site_packages, dry_run=dry_run))
    actions.extend(_reset_windows_repair_source(source, ref=ref, fetch=fetch, dry_run=dry_run))
    actions.append(_write_windows_repair_pth(site_packages=site_packages, source=source, dry_run=dry_run))
    actions.append(_install_windows_runtime_dependencies(python=python, source=source, dry_run=dry_run))
    actions.extend(_write_windows_repair_wrappers(
        python=python,
        env_file=env_file,
        checkpoint=checkpoint,
        logs=logs,
        dry_run=dry_run,
    ))
    actions.extend(_install_windows_repair_tasks(start=start, dry_run=dry_run))
    return actions


def _windows_repair_python() -> Path:
    candidate = _NULLION_HOME / "venv" / "Scripts" / "python.exe"
    return candidate if candidate.exists() else Path(sys.executable)


def _windows_repair_source_dir() -> Path:
    candidate = _NULLION_HOME / "venv" / "src" / "nullion"
    if candidate.exists():
        return candidate
    try:
        from nullion import updater

        return updater._src_dir()
    except Exception:
        return candidate


def _windows_repair_site_packages(python: Path) -> Path:
    fallback = _NULLION_HOME / "venv" / "Lib" / "site-packages"
    if not python.exists():
        return fallback
    result = subprocess.run(
        [str(python), "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        return Path(result.stdout.strip())
    return fallback


def _stop_windows_repair_services(*, dry_run: bool) -> list[str]:
    actions: list[str] = []
    for task in ("Nullion Web Dashboard", "Nullion Tray", "Nullion Telegram"):
        subprocess.run(
            ["schtasks", "/End", "/TN", task],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ) if not dry_run else None
        actions.append(("Would stop" if dry_run else "Stopped") + f" Windows task {task}")
    for image in ("nullion.exe", "nullion-cli.exe", "nullion-web.exe", "nullion-tray.exe", "nullion-telegram.exe", "nullion-webview.exe"):
        subprocess.run(
            ["taskkill", "/F", "/IM", image],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ) if not dry_run else None
    actions.append(("Would stop" if dry_run else "Stopped") + " installed Nullion launcher processes")
    return actions


def _clean_windows_pip_leftovers(site_packages: Path, *, dry_run: bool) -> list[str]:
    actions: list[str] = []
    if not site_packages.exists():
        return [f"Skipped missing site-packages {site_packages}"]
    for path in site_packages.iterdir():
        if path.name.startswith("~"):
            actions.append(_remove_path(path, dry_run=dry_run))
    if not actions:
        actions.append(f"No pip temporary leftovers found in {site_packages}")
    return actions


def _reset_windows_repair_source(source: Path, *, ref: str, fetch: bool, dry_run: bool) -> list[str]:
    actions: list[str] = []
    if fetch:
        if not dry_run:
            subprocess.run(["git", "-C", str(source), "fetch", "origin", "main"], check=True)
        actions.append(f"{'Would fetch' if dry_run else 'Fetched'} origin/main in {source}")
    if not dry_run:
        subprocess.run(["git", "-C", str(source), "reset", "--hard", ref], check=True)
    actions.append(f"{'Would reset' if dry_run else 'Reset'} installed source to {ref}")
    return actions


def _write_windows_repair_pth(*, site_packages: Path, source: Path, dry_run: bool) -> str:
    package_source = source / "src"
    pth = site_packages / "nullion-editable-src.pth"
    if not dry_run:
        site_packages.mkdir(parents=True, exist_ok=True)
        pth.write_text(str(package_source) + "\n", encoding="ascii")
    return f"{'Would write' if dry_run else 'Wrote'} editable source path {pth}"


def _install_windows_runtime_dependencies(*, python: Path, source: Path, dry_run: bool) -> str:
    import tomllib

    pyproject = source / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data.get("project") if isinstance(data, dict) else {}
    dependencies = project.get("dependencies") if isinstance(project, dict) else []
    requirements = [str(item).strip() for item in dependencies if str(item).strip()]
    if not requirements:
        return "No runtime dependencies found to install"
    if dry_run:
        return f"Would install {len(requirements)} runtime dependencies without rewriting Nullion launchers"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".txt") as tmp:
        tmp.write("\n".join(requirements) + "\n")
        tmp_path = Path(tmp.name)
    try:
        subprocess.run([str(python), "-m", "pip", "install", "--quiet", "--no-cache-dir", "-r", str(tmp_path)], check=True)
    finally:
        tmp_path.unlink(missing_ok=True)
    return f"Installed {len(requirements)} runtime dependencies without rewriting Nullion launchers"


def _quote_windows_batch_arg(value: str | Path) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _write_windows_repair_wrappers(
    *,
    python: Path,
    env_file: Path,
    checkpoint: Path,
    logs: Path,
    dry_run: bool,
) -> list[str]:
    actions: list[str] = []
    replacements = {
        "{env}": str(env_file),
        "{checkpoint}": str(checkpoint),
    }
    for spec in _WINDOWS_REPAIR_TASKS:
        bat = _NULLION_HOME / str(spec["bat"])
        module = str(spec["module"])
        module_args = [str(arg) for arg in spec["args"]]  # type: ignore[index]
        expanded_args = [replacements.get(arg, arg) for arg in module_args]
        stdout = logs / str(spec["stdout"])
        stderr = logs / str(spec["stderr"])
        command = " ".join([
            _quote_windows_batch_arg(python),
            "-m",
            module,
            *(_quote_windows_batch_arg(arg) for arg in expanded_args),
        ])
        content = f"@echo off\r\n{command} >> {_quote_windows_batch_arg(stdout)} 2>> {_quote_windows_batch_arg(stderr)}\r\n"
        if not dry_run:
            bat.write_text(content, encoding="ascii")
        actions.append(f"{'Would write' if dry_run else 'Wrote'} wrapper {bat}")
    return actions


def _install_windows_repair_tasks(*, start: bool, dry_run: bool) -> list[str]:
    actions: list[str] = []
    for spec in _WINDOWS_REPAIR_TASKS:
        task = str(spec["task"])
        bat = _NULLION_HOME / str(spec["bat"])
        if not dry_run:
            subprocess.run(["schtasks", "/Delete", "/TN", task, "/F"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["schtasks", "/Create", "/TN", task, "/TR", f'"{bat}"', "/SC", "ONLOGON", "/RL", "LIMITED", "/F"], check=True)
        actions.append(f"{'Would recreate' if dry_run else 'Recreated'} Windows task {task}")
        if start:
            if not dry_run:
                subprocess.run(["schtasks", "/Run", "/TN", task], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            actions.append(f"{'Would start' if dry_run else 'Started'} Windows task {task}")
    return actions


def _tray_executable() -> Path | None:
    candidates: list[Path] = []
    if os.name == "nt":
        candidates.append(_NULLION_HOME / "venv" / "Scripts" / "nullion-tray.exe")
    else:
        candidates.append(_NULLION_HOME / "venv" / "bin" / "nullion-tray")
    found = shutil.which("nullion-tray")
    if found:
        candidates.append(Path(found))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _default_web_port() -> int:
    try:
        return int(os.environ.get("NULLION_WEB_PORT", "8742"))
    except ValueError:
        return 8742


def _run_tray_cli(command: str | None, *, port: int = 8742) -> None:
    command = command or "status"
    if command == "install":
        _install_tray_autostart(port=port, start=True)
    elif command == "start":
        _start_tray_service()
    elif command == "stop":
        _stop_tray_service()
    elif command == "status":
        _tray_status()
    else:
        print("  Usage: nullion tray [install|start|stop|status]")
        sys.exit(2)


def _install_tray_autostart(*, port: int = 8742, start: bool = True) -> bool:
    exe = _tray_executable()
    if exe is None:
        print("  Could not find nullion-tray. Run the installer/update again.")
        return False
    if sys.platform == "darwin":
        _install_tray_launchd(exe=exe, port=port)
        if start:
            return _start_tray_service()
        return True
    if os.name == "nt":
        installed = _install_tray_windows_task(exe=exe, port=port)
        if start:
            if installed:
                return _start_tray_service()
            return _start_tray_directly(port=port)
        return installed
    print("  Tray auto-start installer is currently supported on macOS and Windows.")
    print(f"  You can run manually with: {exe} --port {port}")
    return False


def _xml_escape(value: str) -> str:
    """Escape a string for safe inclusion as plist <string> content."""
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _install_tray_launchd(*, exe: Path, port: int) -> None:
    plist = _launchd_plist_for_label("com.nullion.tray")
    plist.parent.mkdir(parents=True, exist_ok=True)
    # XML-escape every interpolated path. Even though our paths normally don't
    # contain '<', '>' or '&', a username or workspace folder with one of those
    # characters would silently produce a malformed plist — and launchctl's
    # response is the cryptic "Bootstrap failed: 5: Input/output error".
    body = textwrap.dedent(
        f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
        <plist version="1.0">
        <dict>
            <key>Label</key>
            <string>com.nullion.tray</string>
            <key>ProgramArguments</key>
            <array>
                <string>{_xml_escape(exe)}</string>
                <string>--port</string>
                <string>{int(port)}</string>
                <string>--env-file</string>
                <string>{_xml_escape(_ENV_FILE)}</string>
            </array>
            <key>EnvironmentVariables</key>
            <dict>
                <key>NULLION_ENV_FILE</key>
                <string>{_xml_escape(_ENV_FILE)}</string>
                <key>PATH</key>
                <string>{_xml_escape(f"{exe.parent}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")}</string>
            </dict>
            <key>WorkingDirectory</key>
            <string>{_xml_escape(_NULLION_HOME)}</string>
            <key>StandardOutPath</key>
            <string>{_xml_escape(_LOG_DIR / "tray.log")}</string>
            <key>StandardErrorPath</key>
            <string>{_xml_escape(_LOG_DIR / "tray-error.log")}</string>
            <key>RunAtLoad</key>
            <true/>
            <key>KeepAlive</key>
            <dict>
                <key>SuccessfulExit</key>
                <false/>
            </dict>
            <key>ThrottleInterval</key>
            <integer>10</integer>
        </dict>
        </plist>
        """
    )
    # Atomic write + fsync so launchctl bootstrap doesn't race the OS page
    # cache. Bootstrap returning "5: Input/output error" is sometimes launchd
    # observing a partially-written file or a stale inode.
    tmp = plist.with_suffix(plist.suffix + ".tmp")
    tmp.write_text(body)
    try:
        with open(tmp, "rb+") as f:
            os.fsync(f.fileno())
    except OSError:
        pass
    os.replace(tmp, plist)
    # Validate the plist before we hand it to launchctl. plutil's error
    # messages are vastly more useful than launchctl's "5".
    plutil = shutil.which("plutil")
    if plutil:
        lint = subprocess.run(
            [plutil, "-lint", str(plist)], check=False, capture_output=True, text=True
        )
        if lint.returncode != 0:
            print(f"  Tray plist failed plutil validation: {lint.stdout.strip() or lint.stderr.strip()}")
    # Sanity-check the executable exists; bootstrap succeeds without it but
    # the service immediately throttles + fails to launch, masking the cause.
    if not Path(str(exe)).exists():
        print(f"  Warning: tray executable not found at {exe}; service will fail to start.")
    print(f"  Installed tray LaunchAgent: {plist}")


def _summarize_subprocess_error(result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "").strip()
    if not detail:
        return f"exit code {result.returncode}"
    return detail.splitlines()[0].strip()


def _install_tray_windows_task(*, exe: Path, port: int) -> bool:
    wrapper = _NULLION_HOME / "start-nullion-tray.bat"
    wrapper.parent.mkdir(parents=True, exist_ok=True)
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    wrapper.write_text(
        textwrap.dedent(
            f"""\
            @echo off
            for /f "usebackq tokens=1,* delims==" %%A in ("{_ENV_FILE}") do (
                if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
            )
            "{exe}" --port {port} --env-file "{_ENV_FILE}" >> "{_LOG_DIR / "tray.log"}" 2>> "{_LOG_DIR / "tray-error.log"}"
            """
        )
    )
    base_command = [
        "schtasks",
        "/Create",
        "/F",
        "/TN",
        "Nullion Tray",
        "/SC",
        "ONLOGON",
        "/TR",
        f'"{wrapper}"',
        "/RL",
        "LIMITED",
    ]
    for command in (base_command + ["/IT"], base_command):
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        if result.returncode == 0:
            print("  Installed tray scheduled task: Nullion Tray")
            return True
        if "/IT" in command:
            print(f"  Task Scheduler interactive mode was refused: {_summarize_subprocess_error(result)}")
            print("  Retrying tray task without interactive-only mode.")
            continue
        print(f"  Could not install tray scheduled task: {_summarize_subprocess_error(result)}")
    print(f"  Tray can still be started manually with: {exe} --port {port} --env-file {_ENV_FILE}")
    return False


def _start_tray_service() -> bool:
    if sys.platform == "darwin":
        uid = os.getuid()
        label = "com.nullion.tray"
        plist = _launchd_plist_for_label(label)
        # bootout the previous instance and give launchd a beat to release
        # the label — otherwise bootstrap can race and return EIO (5).
        subprocess.run(
            ["launchctl", "bootout", f"gui/{uid}/{label}"],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(0.4)
        # Capture stderr so we can show the user *why* bootstrap failed
        # instead of only "Bootstrap failed: 5: Input/output error".
        bootstrap = subprocess.run(
            ["launchctl", "bootstrap", f"gui/{uid}", str(plist)],
            check=False, capture_output=True, text=True,
        )
        kickstart = subprocess.run(
            ["launchctl", "kickstart", "-k", f"gui/{uid}/{label}"],
            check=False, capture_output=True, text=True,
        )
        if bootstrap.returncode == 0 and kickstart.returncode == 0 and _tray_service_running():
            print("  Started tray icon.")
            return True
        # Surface the real launchctl error so the user has something
        # actionable rather than a generic "did not start".
        for stream_name, result in (("bootstrap", bootstrap), ("kickstart", kickstart)):
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "").strip().splitlines()
                if detail:
                    print(f"  launchctl {stream_name} ({result.returncode}): {detail[0]}")
        # Fallback: launch the tray directly so the user gets the icon
        # NOW even if launchd is being stubborn. Auto-start at next login
        # is still wired up via the plist on disk.
        exe = _tray_executable()
        if exe:
            try:
                subprocess.Popen(
                    [str(exe), "--port", str(_default_web_port())],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                print("  Started tray icon directly (launchd will retry at next login).")
                return True
            except Exception as exc:
                print(f"  Direct tray launch failed too: {exc}")
        print("  Tray icon did not start.")
        return False
    elif os.name == "nt":
        result = subprocess.run(
            ["schtasks", "/Run", "/TN", "Nullion Tray"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print("  Started tray icon.")
            return True
        print(f"  Task Scheduler could not start the tray: {_summarize_subprocess_error(result)}")
        print("  Trying direct tray launch.")
        return _start_tray_directly(port=_default_web_port())
    else:
        exe = _tray_executable()
        if exe:
            subprocess.Popen([str(exe)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("  Started tray icon.")
            return True
    return False


def _start_tray_directly(*, port: int | None = None) -> bool:
    exe = _tray_executable()
    if not exe:
        print("  Could not find nullion-tray. Run the installer/update again.")
        return False
    if not exe.exists():
        print(f"  Tray executable is missing: {exe}")
        return False
    port = port or _default_web_port()
    command = [str(exe), "--port", str(port), "--env-file", str(_ENV_FILE)]
    try:
        subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("  Started tray icon directly.")
        return True
    except FileNotFoundError:
        print(f"  Tray executable is missing: {exe}")
    except PermissionError as exc:
        print(f"  Windows blocked the tray launch: {exc}")
    except OSError as exc:
        print(f"  Direct tray launch failed: {exc}")
    return False


def _tray_service_running() -> bool:
    if sys.platform == "darwin":
        result = subprocess.run(
            ["launchctl", "print", f"gui/{os.getuid()}/com.nullion.tray"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0
    if os.name == "nt":
        result = subprocess.run(
            ["schtasks", "/Query", "/TN", "Nullion Tray"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0
    return False


def _web_ui_url(*, port: int = 8742) -> str:
    return f"http://localhost:{port}"


def _kill_existing_webview() -> None:
    """Terminate any prior nullion-webview single-instance so a fresh one can
    take its place. Used after `nullion update` restarts so the user sees the
    new version immediately instead of a stale focused window."""
    pid_file = _NULLION_HOME / "webview.pid"
    if not pid_file.exists():
        return
    try:
        existing_pid = int(pid_file.read_text().strip())
    except (ValueError, OSError):
        return
    if existing_pid <= 0:
        return
    if os.name == "nt":
        result = subprocess.run(
            ["taskkill", "/PID", str(existing_pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode not in {0, 128}:
            return
        try:
            pid_file.unlink()
        except OSError:
            pass
        return

    # Try graceful TERM first, then a brief wait, then SIGKILL when available.
    import signal
    signals = [signal.SIGTERM]
    sigkill = getattr(signal, "SIGKILL", None)
    if sigkill is not None:
        signals.append(sigkill)
    for sig in signals:
        try:
            os.kill(existing_pid, sig)
        except ProcessLookupError:
            break
        except PermissionError:
            return
        # Wait up to ~1s for it to die between TERM and KILL.
        for _ in range(10):
            time.sleep(0.1)
            try:
                os.kill(existing_pid, 0)
            except ProcessLookupError:
                break
        else:
            continue
        break
    # Clean up the stale pid file so the next webview won't refuse to start.
    try:
        pid_file.unlink()
    except OSError:
        pass


def _open_native_webview(*, port: int = 8742, force_reload: bool = False) -> bool:
    if force_reload:
        _kill_existing_webview()
    command = [
        sys.executable,
        "-m",
        "nullion.webview_app",
        "--port",
        str(port),
        "--browser-fallback",
    ]
    env_file = os.environ.get("NULLION_ENV_FILE")
    if env_file:
        command.extend(["--env-file", env_file])
    try:
        subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False


def _open_web_ui(*, port: int = 8742) -> bool:
    url = _web_ui_url(port=port)
    try:
        if sys.platform == "darwin":
            result = subprocess.run(["open", url], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return result.returncode == 0
        if os.name == "nt":
            os.startfile(url)  # type: ignore[attr-defined]
            return True
        opener = shutil.which("xdg-open")
        if opener:
            result = subprocess.run([opener, url], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return result.returncode == 0
    except Exception:
        return False
    return False


def _start_desktop_tray(*, port: int = 8742) -> bool:
    if sys.platform == "darwin" or os.name == "nt":
        try:
            return bool(_install_tray_autostart(port=port, start=True))
        except Exception as exc:
            print(f"  Tray icon setup skipped: {exc}")
    return False


def _open_desktop_entrypoint(*, port: int = 8742, force_reload: bool = False) -> None:
    _start_desktop_tray(port=port)
    if _open_native_webview(port=port, force_reload=force_reload):
        print("  Reloaded Web UI." if force_reload else "  Opened Web UI.")
        return
    if _open_web_ui(port=port):
        print(f"  Opened Web UI fallback: {_web_ui_url(port=port)}")
    else:
        print(f"  Open Web UI: {_web_ui_url(port=port)}")


def _open_update_entrypoint(*, port: int = 8742, version_changed: bool = False) -> None:
    if os.environ.get("NULLION_UPDATE_OPEN_UI", "").strip().lower() in {"0", "false", "no", "off"}:
        return
    # Only force-reload the webview when an actual version change happened.
    # On no-op runs (already up to date, user declined restart, rollback to
    # the same version) we leave the existing webview alone.
    _open_desktop_entrypoint(port=port, force_reload=version_changed)


def _close_webview_for_uninstall(*, dry_run: bool) -> str:
    pid_file = _NULLION_HOME / "webview.pid"
    if not pid_file.exists():
        return f"Skipped missing {pid_file}"
    if dry_run:
        return f"Would close Nullion webview from {pid_file}"
    _kill_existing_webview()
    return f"Closed Nullion webview from {pid_file}"


def _stop_tray_service() -> None:
    if sys.platform == "darwin":
        label = "com.nullion.tray"
        subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}/{label}"], check=False)
        subprocess.run(["launchctl", "unload", str(_launchd_plist_for_label(label))], check=False)
    elif os.name == "nt":
        subprocess.run(["schtasks", "/End", "/TN", "Nullion Tray"], check=False)
    else:
        print("  Stop is only managed automatically on macOS and Windows.")


def _tray_status() -> None:
    if sys.platform == "darwin":
        subprocess.run(["launchctl", "print", f"gui/{os.getuid()}/com.nullion.tray"], check=False)
    elif os.name == "nt":
        subprocess.run(["schtasks", "/Query", "/TN", "Nullion Tray"], check=False)
    else:
        print("  Tray status is only managed automatically on macOS and Windows.")


def _remove_launchd_services(*, dry_run: bool) -> list[str]:
    actions: list[str] = []
    if sys.platform != "darwin" and not any(_launchd_plist_for_label(label).exists() for label in _LAUNCHD_LABELS):
        return actions
    uid = os.getuid() if hasattr(os, "getuid") else None
    has_launchctl = shutil.which("launchctl") is not None
    for label in _LAUNCHD_LABELS:
        plist = _launchd_plist_for_label(label)
        if has_launchctl and uid is not None:
            _run_uninstall_subprocess(["launchctl", "bootout", f"gui/{uid}/{label}"], dry_run=dry_run)
            if plist.exists():
                _run_uninstall_subprocess(["launchctl", "unload", str(plist)], dry_run=dry_run)
        if plist.exists():
            actions.append(_remove_path(plist, dry_run=dry_run))
    return actions


def _remove_systemd_services(*, dry_run: bool) -> list[str]:
    actions: list[str] = []
    service_paths = [_systemd_service_for_name(name) for name in _SYSTEMD_SERVICES]
    has_systemctl = shutil.which("systemctl") is not None
    if not has_systemctl and not any(path.exists() for path in service_paths):
        return actions
    removed_service = False
    for service, path in zip(_SYSTEMD_SERVICES, service_paths, strict=True):
        if has_systemctl:
            _run_uninstall_subprocess(["systemctl", "--user", "disable", "--now", service], dry_run=dry_run)
        if path.exists():
            actions.append(_remove_path(path, dry_run=dry_run))
            removed_service = True
    if removed_service and has_systemctl:
        _run_uninstall_subprocess(["systemctl", "--user", "daemon-reload"], dry_run=dry_run)
    return actions


def _remove_windows_tasks(*, dry_run: bool) -> list[str]:
    actions: list[str] = []
    if os.name != "nt" and shutil.which("schtasks") is None:
        return actions
    for task in _WINDOWS_TASKS:
        _run_uninstall_subprocess(["schtasks", "/Delete", "/TN", task, "/F"], dry_run=dry_run)
        prefix = "Would delete" if dry_run else "Deleted"
        actions.append(f"{prefix} Windows scheduled task {task}")
    return actions


def _remove_managed_launcher_files(*, dry_run: bool) -> list[str]:
    actions: list[str] = []
    for path in (
        _NULLION_HOME / "start-nullion.bat",
        _NULLION_HOME / "start-nullion-tray.bat",
        _NULLION_HOME / "start-nullion-slack.bat",
        _NULLION_HOME / "start-nullion-discord.bat",
        _NULLION_HOME / "start-nullion-recovery.bat",
        _NULLION_HOME / "start-nullion-telegram.bat",
    ):
        if path.exists():
            actions.append(_remove_path(path, dry_run=dry_run))
    return actions


def _remove_path(path: Path, *, dry_run: bool) -> str:
    resolved = Path(path).expanduser()
    if not resolved.exists():
        return f"Skipped missing {resolved}"
    if not dry_run:
        if resolved.is_dir() and not resolved.is_symlink():
            try:
                _remove_tree(resolved)
            except OSError as exc:
                if os.name != "nt" or not _retryable_rmtree_error(exc):
                    raise
                _schedule_windows_tree_removal(resolved)
                return f"Scheduled removal of {resolved}"
        else:
            try:
                resolved.unlink()
            except FileNotFoundError:
                pass
    prefix = "Would remove" if dry_run else "Removed"
    return f"{prefix} {resolved}"


def _remove_tree(path: Path, *, attempts: int = 5) -> None:
    for attempt in range(max(1, attempts)):
        if not path.exists():
            return
        try:
            shutil.rmtree(path, onerror=_handle_rmtree_path_error)
            return
        except OSError as exc:
            if not _retryable_rmtree_error(exc) or attempt == attempts - 1:
                raise
            _remove_tree_contents(path)
            time.sleep(0.1 * (attempt + 1))


def _retryable_rmtree_error(exc: OSError) -> bool:
    return exc.errno in {
        errno.ENOTEMPTY,
        errno.EBUSY,
        errno.EACCES,
        errno.EPERM,
    }


def _remove_tree_contents(path: Path) -> None:
    try:
        children = list(path.iterdir())
    except FileNotFoundError:
        return
    for child in children:
        try:
            if child.is_dir() and not child.is_symlink():
                _remove_tree(child, attempts=2)
            else:
                child.unlink(missing_ok=True)
        except FileNotFoundError:
            continue
        except PermissionError:
            try:
                child.chmod(0o700)
                if child.is_dir() and not child.is_symlink():
                    _remove_tree(child, attempts=2)
                else:
                    child.unlink(missing_ok=True)
            except FileNotFoundError:
                continue


def _handle_rmtree_path_error(function: object, path: str, exc_info: tuple[type[BaseException], BaseException, object]) -> None:
    exc = exc_info[1]
    if isinstance(exc, FileNotFoundError):
        return
    if isinstance(exc, PermissionError):
        try:
            Path(path).chmod(0o700)
            function(path)  # type: ignore[misc,operator]
            return
        except FileNotFoundError:
            return
        except Exception:
            pass
    raise exc


def _schedule_windows_tree_removal(path: Path) -> None:
    target = str(Path(path).resolve()).replace('"', '""')
    script = Path(tempfile.gettempdir()) / f"nullion-uninstall-{os.getpid()}.cmd"
    script.write_text(
        "@echo off\r\n"
        "for /l %%i in (1,1,60) do (\r\n"
        f'  if not exist "{target}" goto done' + "\r\n"
        f'  rmdir /s /q "{target}" > nul 2> nul' + "\r\n"
        f'  if not exist "{target}" goto done' + "\r\n"
        "  ping 127.0.0.1 -n 2 > nul\r\n"
        ")\r\n"
        ":done\r\n"
        'del "%~f0" > nul 2> nul\r\n',
        encoding="utf-8",
    )
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
    subprocess.Popen(
        ["cmd.exe", "/d", "/c", str(script)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
    )


def _path_is_inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _uninstall_python_package(*, dry_run: bool, keep_data: bool) -> list[str]:
    executable = Path(sys.executable)
    if _path_is_inside(executable, _NULLION_HOME) and not keep_data:
        return [f"Skipped pip uninstall; removing {_NULLION_HOME} removes the bundled venv"]
    command = [sys.executable, "-m", "pip", "uninstall", "-y", "project-nullion"]
    _run_uninstall_subprocess(command, dry_run=dry_run)
    prefix = "Would run" if dry_run else "Ran"
    return [f"{prefix} {' '.join(command)}"]


def _chat_service_enabled_from_env(name: str) -> bool:
    if name == "telegram":
        return bool(os.environ.get("NULLION_TELEGRAM_BOT_TOKEN", "").strip())
    if name == "slack":
        enabled = os.environ.get("NULLION_SLACK_ENABLED", "false").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        return enabled and bool(
            os.environ.get("NULLION_SLACK_BOT_TOKEN", "").strip()
            and os.environ.get("NULLION_SLACK_APP_TOKEN", "").strip()
        )
    if name == "discord":
        enabled = os.environ.get("NULLION_DISCORD_ENABLED", "false").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        return enabled and bool(os.environ.get("NULLION_DISCORD_BOT_TOKEN", "").strip())
    return False


def _restart_launchd_chat_services() -> None:
    try:
        from nullion.service_control import restart_managed_services
    except ModuleNotFoundError as exc:
        if exc.name != "nullion.service_control":
            raise
        _restart_managed_services_legacy(manager="launchd", names=("telegram", "slack", "discord"))
        return

    for result in restart_managed_services(groups={"chat"}, continue_on_error=True):
        if result.ok:
            print(f"  {result.message}")


def _restart_systemd_chat_services() -> None:
    try:
        from nullion.service_control import restart_managed_services
    except ModuleNotFoundError as exc:
        if exc.name != "nullion.service_control":
            raise
        _restart_managed_services_legacy(manager="systemd", names=("telegram", "slack", "discord"))
        return

    for result in restart_managed_services(groups={"chat"}, continue_on_error=True):
        if result.ok:
            print(f"  {result.message}")


def _restart_all_managed_services(*, manager: str = "auto") -> None:
    try:
        from nullion.service_control import restart_managed_services
    except ModuleNotFoundError as exc:
        if exc.name != "nullion.service_control":
            raise
        _restart_managed_services_legacy(manager=manager)
        return

    for result in restart_managed_services(continue_on_error=True, manager=manager):  # type: ignore[arg-type]
        if result.ok:
            print(f"  {result.message}")


_LEGACY_SERVICE_HINTS: dict[str, dict[str, tuple[str, ...]]] = {
    "web": {
        "launchd": ("com.nullion.web",),
        "systemd": ("nullion.service",),
        "windows": ("Nullion Web Dashboard",),
    },
    "tray": {
        "launchd": ("com.nullion.tray",),
        "systemd": ("nullion-tray.service",),
        "windows": ("Nullion Tray",),
    },
    "slack": {
        "launchd": ("ai.nullion.slack", "com.nullion.slack"),
        "systemd": ("nullion-slack.service",),
        "windows": ("Nullion Slack",),
    },
    "discord": {
        "launchd": ("ai.nullion.discord", "com.nullion.discord"),
        "systemd": ("nullion-discord.service",),
        "windows": ("Nullion Discord",),
    },
    "recovery": {
        "launchd": ("ai.nullion.recovery",),
        "systemd": ("nullion-recovery.service",),
        "windows": ("Nullion Recovery",),
    },
    "telegram": {
        "launchd": ("ai.nullion.telegram", "com.nullion.telegram"),
        "systemd": ("nullion-telegram.service",),
        "windows": ("Nullion Telegram",),
    },
}


def _restart_managed_services_legacy(*, manager: str, names: tuple[str, ...] | None = None) -> None:
    """Best-effort fallback used when the installed service_control module is unavailable."""
    import subprocess

    targets = names or ("web", "tray", "slack", "discord", "recovery", "telegram")
    normalized_manager = manager
    if normalized_manager == "auto":
        normalized_manager = _detect_service_manager()
    print("  Service helper unavailable; using legacy restart path.")
    if normalized_manager == "launchd":
        uid = os.getuid()
        for name in targets:
            for label in _LEGACY_SERVICE_HINTS.get(name, {}).get("launchd", ()):
                plist = Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
                if not plist.exists():
                    continue
                target = f"gui/{uid}/{label}"
                result = subprocess.run(["launchctl", "kickstart", "-k", target], check=False)
                if result.returncode != 0:
                    subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", str(plist)], check=False)
                    subprocess.run(["launchctl", "kickstart", "-k", target], check=False)
                print(f"  Restarted {label}.")
                break
        return
    if normalized_manager == "systemd":
        for name in targets:
            for unit in _LEGACY_SERVICE_HINTS.get(name, {}).get("systemd", ()):
                unit_path = Path.home() / ".config" / "systemd" / "user" / unit
                if not unit_path.exists():
                    continue
                subprocess.run(["systemctl", "--user", "restart", unit], check=False)
                print(f"  Restarted {unit}.")
                break
        return
    if normalized_manager == "windows":
        for name in targets:
            for task in _LEGACY_SERVICE_HINTS.get(name, {}).get("windows", ()):
                query = subprocess.run(["schtasks", "/Query", "/TN", task], check=False, capture_output=True, text=True)
                if query.returncode != 0:
                    continue
                subprocess.run(["schtasks", "/End", "/TN", task], check=False)
                subprocess.run(["schtasks", "/Run", "/TN", task], check=False)
                print(f"  Restarted {task}.")
                break


def _service_cmd(sm: str, action: str) -> None:
    import subprocess
    if sm == "launchd":
        plist = str(_launchd_plist())
        if action == "stop":
            _stop_tray_service()
            subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}/com.nullion.web"], check=False)
            subprocess.run(["launchctl", "unload", plist], check=False)
            print("  Stopped.")
        elif action == "restart":
            _begin_gateway_restart_notice()
            _restart_all_managed_services(manager="launchd")
            print("  Restarted.")
        elif action == "status":
            subprocess.run(["launchctl", "list", "com.nullion.web"], check=False)
    elif sm == "systemd":
        svc = "nullion.service"
        cmds = {
            "stop":    ["systemctl", "--user", "stop", svc],
            "restart": ["systemctl", "--user", "restart", svc],
            "status":  ["systemctl", "--user", "status", svc],
        }
        if action == "restart":
            _begin_gateway_restart_notice()
        if action == "stop" and os.name == "nt":
            _stop_tray_service()
        if action == "restart":
            _restart_all_managed_services(manager="systemd")
        else:
            subprocess.run(cmds[action], check=False)
    else:
        if action == "stop" and (sys.platform == "darwin" or os.name == "nt"):
            _stop_tray_service()
        if action == "restart" and os.name == "nt":
            _begin_gateway_restart_notice()
            _restart_all_managed_services(manager="windows")
            print("  Restarted.")
            return
        print("  Could not detect service manager. Try:")
        print("    macOS:  launchctl unload ~/Library/LaunchAgents/com.nullion.web.plist")
        print("    Linux:  systemctl --user stop nullion")


if __name__ == "__main__":
    cli()

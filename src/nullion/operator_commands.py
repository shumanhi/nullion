"""Telegram-style operator commands for Nullion runtime control."""

from __future__ import annotations

from collections import Counter
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from nullion import __version__
from nullion.approval_context import approval_trigger_flow_label
from nullion.approval_display import approval_display_from_request
from nullion.approvals import (
    ApprovalRequest,
    ApprovalStatus,
    PermissionGrant,
    is_permission_grant_active,
)
from nullion.codebase_summary import build_codebase_summary, format_codebase_summary
from nullion.config import NullionSettings, load_settings, web_session_allow_duration_label, web_session_allow_expires_at
from nullion.doctor_playbooks import execute_doctor_playbook_command
from nullion.runtime import (
    PersistentRuntime,
    archive_stored_builder_proposal,
    build_runtime_status_snapshot,
    format_doctor_diagnosis_for_operator,
    get_builder_proposal,
    get_skill,
    list_builder_proposals,
    list_skills,
    reject_stored_builder_proposal,
    render_skill_for_telegram,
)
from nullion.run_activity import set_verbose_mode, verbose_mode_status_text
from nullion.runtime_status import compute_approval_pressure
from nullion.runtime_config import current_runtime_config, persist_model_name
from nullion.thinking_display import (
    set_thinking_display_enabled,
    thinking_display_status_text,
)
from nullion.plugin_catalog import get_plugin_catalog_entry, list_plugin_catalog
from nullion.skill_pack_catalog import get_skill_pack_catalog_entry, list_available_skill_packs
from nullion.system_context import build_system_context_snapshot, format_system_context_for_prompt
from nullion.memory import UserMemoryEntry, memory_owner_for_web_admin
from nullion.messaging_adapters import list_platform_delivery_receipts


@dataclass(frozen=True)
class OperatorCommandSpec:
    command: str
    description: str
    telegram_command: str | None = None
    hidden_from_suggestions: bool = False

    @property
    def head(self) -> str:
        return self.command.split()[0]


_OPERATOR_COMMAND_SPECS: tuple[OperatorCommandSpec, ...] = (
    OperatorCommandSpec("/help", "Show the quick-reference command menu", "help"),
    OperatorCommandSpec("/help commands", "Show the full command list", None),
    OperatorCommandSpec("/chat <message>", "Talk to Nullion Assistant", "chat"),
    OperatorCommandSpec("/new", "Clear conversation history and start fresh", "new"),
    OperatorCommandSpec("/verbose [off|planner|full|status]", "Choose activity and planner visibility", "verbose"),
    OperatorCommandSpec("/thinking [on|off|status]", "Show or hide provider reasoning summaries separately", "thinking"),
    OperatorCommandSpec("/streaming [on|off|status]", "Toggle streamed chat replies where supported", "streaming"),
    OperatorCommandSpec("/update [--ignore-checks|--force]", "Update Nullion safely with automatic rollback", "update"),
    OperatorCommandSpec("/restart", "Restart the bot process", "restart"),
    OperatorCommandSpec("/models", "Show available and saved models", "models"),
    OperatorCommandSpec("/model <provider> <model_name>", "Switch to a different model", "model"),
    OperatorCommandSpec("/memory [all]", "Show saved memory", "memory"),
    OperatorCommandSpec("/memory delete <n|entry_id|last-hour|24h|all>", "Delete saved memory", None),
    OperatorCommandSpec("/approvals", "List pending approval requests", "approvals"),
    OperatorCommandSpec("/approval <n|approval_id>", "Show one approval request", "approval"),
    OperatorCommandSpec("/approve <n|approval_id>", "Approve one pending request", "approve"),
    OperatorCommandSpec("/deny <n|approval_id>", "Deny one pending request", "deny"),
    OperatorCommandSpec("/grants", "List active permission grants", "grants"),
    OperatorCommandSpec("/grant <n|grant_id>", "Show one permission grant", "grant"),
    OperatorCommandSpec("/revoke-grant <n|grant_id>", "Revoke one permission grant", "revoke_grant"),
    OperatorCommandSpec("/proposals", "List Builder proposals", "proposals"),
    OperatorCommandSpec("/proposal <n|proposal_id>", "Show one Builder proposal", "proposal"),
    OperatorCommandSpec("/accept-proposal <n|proposal_id>", "Accept a saved skill proposal", "accept_proposal"),
    OperatorCommandSpec("/reject-proposal <n|proposal_id>", "Reject a saved proposal", "reject_proposal"),
    OperatorCommandSpec("/archive-proposal <n|proposal_id>", "Archive a saved proposal", "archive_proposal"),
    OperatorCommandSpec("/reminder cancel <task_id>", "Cancel a pending reminder", "reminder"),
    OperatorCommandSpec("/skills", "List saved skills", "skills"),
    OperatorCommandSpec("/skill <n|skill_id>", "Show one saved skill", "skill"),
    OperatorCommandSpec("/skill-history <n|skill_id>", "Show one skill revision history", "skill_history"),
    OperatorCommandSpec("/update-skill <skill_id> <field> <value>", "Update one skill field", "update_skill"),
    OperatorCommandSpec("/revert-skill <skill_id> <revision>", "Revert to a prior skill revision", "revert_skill"),
    OperatorCommandSpec("/ping", "Liveness check", "ping"),
    OperatorCommandSpec("/version", "Show installed version", "version"),
    OperatorCommandSpec("/health", "Show runtime health summary", "health"),
    OperatorCommandSpec("/doctor [diagnose|n|start n|complete n|dismiss n]", "Diagnose, list, or act on Doctor health actions", "doctor"),
    OperatorCommandSpec("/doctor run <id> <doctor:action>", "Run a Doctor playbook action from text-only chat", None),
    OperatorCommandSpec("/uptime", "Show current operator runtime uptime", "uptime"),
    OperatorCommandSpec("/status", "Show full runtime status", "status"),
    OperatorCommandSpec("/status active", "Show active work only", None),
    OperatorCommandSpec("/status <capsule_id>", "Show status for one capsule", None),
    OperatorCommandSpec("/deliveries [failed|partial|succeeded|all]", "Show recent platform delivery receipts", "deliveries"),
    OperatorCommandSpec("/system-context", "Show internal system context", "system_context"),
    OperatorCommandSpec("/codebase", "Show abstract codebase summary", "codebase"),
    OperatorCommandSpec("/tools", "Show available tool inventory", "tools"),
    OperatorCommandSpec("/plugins", "Show installed and available plugins", "plugins"),
    OperatorCommandSpec("/plugins available", "Show plugin catalog", None),
    OperatorCommandSpec("/plugin <plugin_id>", "Show setup details for one plugin", "plugin"),
    OperatorCommandSpec("/skill-packs", "Show enabled and available reference skill packs", "skill_packs"),
    OperatorCommandSpec("/skill-packs available", "Show skill pack catalog", None),
    OperatorCommandSpec("/skill-pack <pack_id>", "Show setup details for one skill pack", "skill_pack"),
    OperatorCommandSpec("/backups", "List backup generations", "backups"),
    OperatorCommandSpec("/restore", "Restore newest backup", "restore"),
    OperatorCommandSpec("/restore latest", "Restore newest backup", None),
    OperatorCommandSpec("/restore <generation>", "Restore a specific backup", None),
    OperatorCommandSpec("/auto-skill [conv_id]", "Scan conversation history and propose automatable skills", "auto_skill"),
    OperatorCommandSpec("/accept-skill <n>", "Accept skill proposal number n from the last /auto-skill run", "accept_skill"),
)

_TELEGRAM_COMMAND_ALIASES = {
    f"/{spec.telegram_command}": spec.head
    for spec in _OPERATOR_COMMAND_SPECS
    if spec.telegram_command and f"/{spec.telegram_command}" != spec.head
}


def operator_command_catalog() -> tuple[OperatorCommandSpec, ...]:
    return _OPERATOR_COMMAND_SPECS


def operator_command_suggestions() -> tuple[tuple[str, str], ...]:
    return tuple(
        (spec.command, spec.description)
        for spec in _OPERATOR_COMMAND_SPECS
        if not spec.hidden_from_suggestions
    )


def telegram_bot_command_menu(*, include_private_aliases: bool = True) -> tuple[tuple[str, str], ...]:
    seen: set[str] = set()
    commands: list[tuple[str, str]] = []
    for spec in _OPERATOR_COMMAND_SPECS:
        if not spec.telegram_command or spec.telegram_command in seen:
            continue
        if not include_private_aliases and spec.telegram_command == "planner_feed":
            continue
        seen.add(spec.telegram_command)
        commands.append((spec.telegram_command, spec.description))
    return tuple(commands)


def normalize_operator_command_head(head: str) -> str:
    if not head.startswith("/"):
        return head
    command_name, separator, mention = head.partition("@")
    normalized = command_name if separator and command_name and mention else head
    return _TELEGRAM_COMMAND_ALIASES.get(normalized, normalized)


def _render_command_reference(title: str, specs: tuple[OperatorCommandSpec, ...]) -> str:
    commands = {spec.command for spec in specs}
    lines = [title]
    lines.extend(
        f"{command} — {description}"
        for command, description in operator_command_suggestions()
        if command in commands
    )
    return "\n".join(lines)


_UNKNOWN_COMMAND = (
    "Unknown command.\n\n"
    "Try one of these:\n"
    "• /help — command menu\n"
    "• /chat <message> — talk to Nullion\n"
    "• /verbose full — show activity traces and planner task cards\n"
    "• /thinking on — show provider reasoning summaries separately\n"
    "• /streaming off — disable streamed replies\n"
    "• /status — current state\n"
    "• /approvals — pending approvals\n"
    "• /plugins — installed and available plugins\n"
    "• /skill-packs — reference skill packs\n"
    "• /update — update the app\n\n"
    "For the full list, send /help commands."
)


def _humanize_operator_label(value: object) -> str:
    return str(value or "").replace("_", " ").strip().capitalize()
_INVALID_RESTORE_GENERATION = "Invalid backup generation. Use /restore <generation|latest>."
_EXTRA_STATUS_ARGS = "Too many arguments for /status. Use /status, /status active, or /status <capsule_id>."
_EXTRA_RESTORE_ARGS = "Too many arguments for /restore. Use /restore, /restore latest, or /restore <generation>."
_HELP_TEXT = _render_command_reference("Nullion operator commands", _OPERATOR_COMMAND_SPECS)
_HELP_COMMANDS_TEXT = _render_command_reference("Nullion — full command reference", _OPERATOR_COMMAND_SPECS)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_README_PATH = _REPO_ROOT / "README.md"


def _append_next(lines: list[str], *items: str) -> list[str]:
    actionable = [item for item in items if item]
    if actionable:
        while lines and lines[-1] == "":
            lines.pop()
        lines.extend(["", "Next:"])
        lines.extend(f"  {item}" for item in actionable)
    return lines


def _display_ref(primary: str, index: int) -> str:
    return f"{index}. {primary}"


def _handle_verbose_command(parts: list[str]) -> str:
    if len(parts) == 1 or parts[1].strip().lower() in {"status", "show"}:
        return f"Verbose mode is {verbose_mode_status_text()}."
    if len(parts) > 2:
        return "Usage: /verbose [off|planner|full|status]"
    value = parts[1].strip().lower()
    try:
        set_verbose_mode(value)
    except ValueError:
        return "Usage: /verbose [off|planner|full|status]"
    return f"Verbose mode is {verbose_mode_status_text()}."


def _handle_thinking_command(parts: list[str]) -> str:
    if len(parts) == 1 or parts[1].strip().lower() in {"status", "show"}:
        return f"Thinking display is {thinking_display_status_text()}."
    if len(parts) > 2:
        return "Usage: /thinking [on|off|status]"
    value = parts[1].strip().lower()
    if value in {"on", "true", "yes", "1", "enable", "enabled"}:
        set_thinking_display_enabled(True)
        return "Thinking display is on."
    if value in {"off", "false", "no", "0", "disable", "disabled"}:
        set_thinking_display_enabled(False)
        return "Thinking display is off."
    return "Usage: /thinking [on|off|status]"

def _render_active_status(runtime: PersistentRuntime) -> str:
    return runtime.render_status_for_telegram(active_only=True)


def _render_status(runtime: PersistentRuntime) -> str:
    snapshot = build_runtime_status_snapshot(runtime.store)
    counts = snapshot["counts"]
    pressure = compute_approval_pressure(snapshot)
    cfg = current_runtime_config(model_client=getattr(runtime, "model_client", None))
    active_missions = sum(1 for mission in snapshot.get("missions", []) if str(mission.get("status")) in {"running", "waiting_approval", "blocked"})
    pending_approvals = pressure["pending_approval_requests"]
    active_grants = sum(1 for grant in snapshot.get("permission_grants", []) if grant.get("grant_state") == "active")
    lines = [
        "📌 Nulliøn status",
        "",
        f"Model: {cfg.provider} / {cfg.model}",
        f"Version: {__version__}",
        f"Runtime: {counts['running_capsules']} running, {active_missions} active mission(s)",
        f"Approvals: {pending_approvals} pending, {active_grants} active permission(s)",
        f"Doctor: {counts['pending_doctor_actions']} pending action(s)",
        f"Sentinel: {counts['open_sentinel_escalations']} open escalation(s)",
        f"Memory: {'on' if cfg.memory_enabled else 'off'}",
        f"Tools: web {'on' if cfg.web_access else 'off'}, browser {'on' if cfg.browser_enabled else 'off'}, files {'on' if cfg.file_access else 'off'}, terminal {'on' if cfg.terminal_enabled else 'off'}",
        f"Telegram: {'configured' if cfg.telegram_configured else 'not configured'}",
    ]
    if pending_approvals:
        lines.extend(["", "Use /approvals to review pending requests."])
    lines.extend(["", "Use /status active for detailed active work."])
    return "\n".join(lines)



def _render_health(runtime: PersistentRuntime) -> str:
    snapshot = build_runtime_status_snapshot(runtime.store)
    counts = snapshot["counts"]
    approval_pressure = compute_approval_pressure(snapshot)
    blocked_work_count = sum(1 for capsule in snapshot["capsules"] if capsule["state"] == "blocked")
    blocked_work_count += sum(
        1 for mission in snapshot.get("missions", []) if str(mission.get("status")) == "blocked"
    )
    pending_approvals = approval_pressure["pending_approval_requests"]
    pending_doctor = counts["pending_doctor_actions"]
    pending_escalations = counts["open_sentinel_escalations"]
    running = counts["running_capsules"]

    issues: list[str] = []
    if pending_approvals > 0:
        noun = "thing" if pending_approvals == 1 else "things"
        issues.append(f"{pending_approvals} {noun} waiting for your approval — use /approvals to review")
    if blocked_work_count > 0:
        noun = "task" if blocked_work_count == 1 else "tasks"
        issues.append(f"{blocked_work_count} {noun} are stuck and can't continue — use /status to see why")
    if pending_doctor > 0:
        noun = "issue" if pending_doctor == 1 else "issues"
        issues.append(f"{pending_doctor} {noun} flagged — use /status to see what needs attention")
    if pending_escalations > 0:
        issues.append(f"{pending_escalations} monitoring alert(s) — check /status for details")

    if not issues:
        status_line = "✅ Everything looks good."
    elif blocked_work_count > 0:
        status_line = f"🚫 Some tasks are blocked."
    else:
        status_line = f"⚠️ Something needs your attention."

    lines = ["🩺 Nullion health", "", status_line]
    if issues:
        lines.append("")
        for issue in issues:
            lines.append(f"• {issue}")
    if running > 0:
        lines.append("")
        noun = "task" if running == 1 else "tasks"
        lines.append(f"Currently working on {running} {noun}.")
    return "\n".join(lines)


def _render_doctor(runtime: PersistentRuntime) -> str:
    snapshot = build_runtime_status_snapshot(runtime.store)
    actions = list(snapshot.get("doctor_actions", []))
    if not actions:
        return "🩺 Doctor\n\nNo Doctor actions are pending."

    lines = ["🩺 Doctor", "", f"{len(actions)} action(s)"]
    for index, action in enumerate(actions[:10], start=1):
        action_id = str(action.get("action_id") or "")
        severity = _humanize_operator_label(action.get("severity"))
        action_type = _humanize_operator_label(action.get("action_type"))
        status = _humanize_operator_label(action.get("status"))
        summary = str(action.get("summary") or "Health action")
        lines.append("")
        lines.append(_display_ref(f"{severity} {action_type} [{status}]: {summary}", index))
        timestamp = _event_timestamp_label(action)
        if timestamp:
            lines.append(f"  Updated: {timestamp}")
        if action_id:
            lines.append(f"  ID: {action_id}")
    if len(actions) > 10:
        lines.extend(["", f"Showing 10 of {len(actions)} actions."])
    _append_next(
        lines,
        "Use /doctor <n> to inspect an action.",
        "Use /doctor start <n>, /doctor complete <n>, or /doctor dismiss <n>.",
        "Use /status for the full runtime view.",
    )
    return "\n".join(lines)


def _run_doctor_diagnose(runtime: PersistentRuntime) -> str:
    report = runtime.diagnose_runtime_health()
    return format_doctor_diagnosis_for_operator(report)


def _sorted_doctor_actions(runtime: PersistentRuntime) -> list[dict]:
    snapshot = build_runtime_status_snapshot(runtime.store)
    return list(snapshot.get("doctor_actions", []))


def _resolve_doctor_action(runtime: PersistentRuntime, token: str | None) -> dict | None:
    if token is None:
        return None
    normalized_token = _normalize_mention_suffix(token)
    direct = runtime.store.get_doctor_action(normalized_token)
    if direct is not None:
        return direct
    if normalized_token.isdigit():
        index = int(normalized_token)
        actions = _sorted_doctor_actions(runtime)
        if 1 <= index <= len(actions):
            return actions[index - 1]
    return None


def _render_doctor_action(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /doctor <n|action_id>"
    normalized_token = _normalize_mention_suffix(token)
    action = _resolve_doctor_action(runtime, token)
    if action is None:
        return f"Doctor action not found: {normalized_token}"
    action_id = str(action.get("action_id") or "")
    lines = [
        "🩺 Doctor action",
        f"ID: {action_id}",
        f"Status: {_humanize_operator_label(action.get('status'))}",
        f"Severity: {_humanize_operator_label(action.get('severity'))}",
        f"Type: {_humanize_operator_label(action.get('action_type'))}",
        f"Summary: {action.get('summary') or 'Health action'}",
    ]
    created = _operator_timestamp_label(action.get("created_at"))
    updated = _operator_timestamp_label(action.get("updated_at"))
    if created:
        lines.append(f"Created: {created}")
    if updated:
        lines.append(f"Updated: {updated}")
    if action.get("details"):
        lines.append(f"Details: {action.get('details')}")
    _append_next(
        lines,
        f"Use /doctor start {normalized_token} to mark it in progress.",
        f"Use /doctor complete {normalized_token} when handled.",
        f"Use /doctor dismiss {normalized_token} to close it without action.",
    )
    return "\n".join(lines)


def _doctor_action_cmd(runtime: PersistentRuntime, action: str, token: str | None) -> str:
    if token is None:
        return "Usage: /doctor <start|complete|dismiss> <n|action_id>"
    resolved = _resolve_doctor_action(runtime, token)
    normalized_token = _normalize_mention_suffix(token)
    if resolved is None:
        return f"Doctor action not found: {normalized_token}"
    action_id = str(resolved.get("action_id") or "")
    try:
        if action == "start":
            updated = runtime.start_doctor_action(action_id)
            return f"Started Doctor action {action_id}.\nStatus: {updated['status']}"
        if action == "complete":
            updated = runtime.complete_doctor_action(action_id)
            return f"Completed Doctor action {action_id}.\nStatus: {updated['status']}"
        if action in {"dismiss", "cancel"}:
            runtime.cancel_doctor_action(action_id, reason="Dismissed from operator command")
            return f"Dismissed Doctor action {action_id}."
    except ValueError as exc:
        return str(exc)
    return "Usage: /doctor <start|complete|dismiss> <n|action_id>"


def _doctor_run_cmd(runtime: PersistentRuntime, token: str | None, command: str | None) -> str:
    if token is None or command is None:
        return "Usage: /doctor run <n|action_id> <doctor:action>"
    resolved = _resolve_doctor_action(runtime, token)
    normalized_token = _normalize_mention_suffix(token)
    if resolved is None:
        return f"Doctor action not found: {normalized_token}"
    action_id = str(resolved.get("action_id") or "")

    try:
        result = execute_doctor_playbook_command(
            runtime,
            action_id=action_id,
            command=command,
            source_label="operator command",
        )
        return result.message
    except ValueError as exc:
        return str(exc)
    except KeyError:
        return f"Doctor action not found: {normalized_token}"


def _reminder_cmd(runtime: PersistentRuntime, parts: list[str]) -> str:
    if len(parts) != 3 or parts[1].lower() != "cancel":
        return "Usage: /reminder cancel <task_id>"
    task_id = _normalize_mention_suffix(parts[2])
    removed = runtime.store.remove_reminder(task_id)
    runtime.store.scheduled_tasks.pop(task_id, None)
    runtime.checkpoint()
    if removed:
        return f"Cancelled reminder {task_id}."
    return f"Reminder not found: {task_id}"



def _format_uptime_duration(total_seconds: int) -> str:
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)



def _normalize_command_head(head: str) -> str:
    return normalize_operator_command_head(head)



def _normalize_mention_suffix(token: str) -> str:
    value, separator, mention = token.partition("@")
    if separator and value and mention:
        return value
    return token



def _operator_timezone():
    try:
        from nullion.preferences import load_preferences, resolve_timezone

        return resolve_timezone(load_preferences().timezone)
    except Exception:
        return UTC


def _operator_timestamp_label(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(_operator_timezone()).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return text


def _event_timestamp_label(item: dict) -> str:
    return _operator_timestamp_label(item.get("updated_at") or item.get("created_at"))



def _render_uptime(runtime: PersistentRuntime, *, now: datetime | None = None) -> str:
    current_time = now if now is not None else datetime.now(UTC)
    elapsed = current_time - runtime.started_at
    total_seconds = max(int(elapsed.total_seconds()), 0)
    started = runtime.started_at.astimezone(_operator_timezone()).strftime("%Y-%m-%d %H:%M:%S %Z")
    return "\n".join(
        [
            "⏱️ Nullion uptime",
            f"Started: {started}",
            f"Uptime: {_format_uptime_duration(total_seconds)}",
        ]
    )


def _delivery_receipt_filter(parts: list[str]) -> str | None:
    if len(parts) <= 1:
        return "failed"
    if len(parts) > 2:
        return "__usage__"
    token = _normalize_mention_suffix(parts[1]).lower()
    if token in {"all", "*"}:
        return None
    if token in {"failed", "partial", "succeeded"}:
        return token
    return "__usage__"


def _delivery_receipt_line(receipt: dict[str, object], index: int) -> str:
    channel = str(receipt.get("channel") or "unknown")
    status = str(receipt.get("status") or "unknown")
    target = str(receipt.get("target_id") or "unknown")
    created = _operator_timestamp_label(receipt.get("created_at"))
    attachment_count = int(receipt.get("attachment_count") or 0)
    unavailable_count = int(receipt.get("unavailable_attachment_count") or 0)
    required = bool(receipt.get("attachment_required"))
    error = str(receipt.get("error") or "").strip()
    detail_parts = [
        f"{attachment_count} attachment{'s' if attachment_count != 1 else ''}",
        "required" if required else "message",
    ]
    if unavailable_count:
        detail_parts.append(f"{unavailable_count} unavailable")
    if error:
        detail_parts.append(error)
    suffix = f" — {created}" if created else ""
    return f"{index}. {channel}:{target} [{status}] {', '.join(detail_parts)}{suffix}"


def _render_delivery_receipts(parts: list[str]) -> str:
    status_filter = _delivery_receipt_filter(parts)
    if status_filter == "__usage__":
        return "Usage: /deliveries [failed|partial|succeeded|all]"
    receipts = list_platform_delivery_receipts(limit=10, status=status_filter)
    title = "📦 Delivery receipts"
    subtitle = "failed" if status_filter == "failed" else status_filter or "all"
    if not receipts:
        if status_filter:
            return f"{title}\n\nNo {subtitle} delivery receipts."
        return f"{title}\n\nNo delivery receipts recorded yet."
    lines = [title, f"Showing recent {subtitle} receipts:", ""]
    lines.extend(_delivery_receipt_line(receipt, index) for index, receipt in enumerate(receipts, start=1))
    _append_next(lines, "Use /deliveries all to inspect successful and partial deliveries too.")
    return "\n".join(lines)



def _render_version(runtime: PersistentRuntime) -> str:
    del runtime
    started = datetime.now(UTC).strftime("%-d %b %Y")  # e.g. "23 Apr 2026"
    return f"Nullion {__version__} — running since {started}. You're up to date."


def _backup_human_label(backup: dict) -> str:
    """Convert a backup record to a human-readable label (date/time instead of generation number)."""
    backup_name = backup.get("name") or ""

    modified_at = backup.get("modified_at")
    if isinstance(modified_at, (int, float)):
        dt = datetime.fromtimestamp(modified_at, tz=UTC).astimezone(_operator_timezone())
        return dt.strftime("%-d %b %Y, %-I:%M %p %Z")

    # Try to extract mtime from the backup path on disk.
    try:
        from pathlib import Path as _Path
        backup_path = _Path(str(backup.get("path") or backup_name))
        if backup_path.exists():
            mtime = backup_path.stat().st_mtime
            dt = datetime.fromtimestamp(mtime, tz=UTC).astimezone(_operator_timezone())
            return dt.strftime("%-d %b %Y, %-I:%M %p %Z")
    except Exception:
        pass
    # Fallback: just clean up the filename
    stem = backup_name.rsplit(".", 1)[0] if "." in backup_name else backup_name
    stem = stem.rsplit("/", 1)[-1]
    return stem or backup_name


def _render_backups(runtime: PersistentRuntime) -> str:
    backups = runtime.list_backups()
    if not backups:
        return "💾 Backups\n\nNo restore points saved yet."

    lines = [f"💾 Backups — {len(backups)} restore point(s) available", ""]
    for index, backup in enumerate(backups):
        label = _backup_human_label(backup)
        prefix = "→ Most recent: " if index == 0 else f"  {index}. "
        restore_cmd = f"/restore {backup['generation']}"
        lines.append(f"{prefix}{label}  ({restore_cmd})")

    latest_restore = runtime.latest_restore_metadata()
    if latest_restore is not None:
        source = latest_restore.get("source", "backup")
        lines.extend(["", f"Last restored from: {source}"])
    _append_next(lines, "Use /restore <n> to restore a numbered generation, or /restore latest.")
    return "\n".join(lines)


def _readme_project_context() -> tuple[str | None, tuple[str, ...], tuple[str, ...]]:
    try:
        content = _README_PATH.read_text(encoding="utf-8")
    except OSError:
        return None, (), ()

    project_summary = None
    goals: list[str] = []
    initial_focus: list[str] = []
    current_section: str | None = None

    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped == "# Project Nullion":
            continue
        if project_summary is None and not stripped.startswith("#"):
            project_summary = stripped
            continue
        if stripped == "## Goals":
            current_section = "goals"
            continue
        if stripped == "## Initial focus":
            current_section = "initial_focus"
            continue
        if stripped.startswith("## "):
            current_section = None
            continue
        if stripped.startswith("- "):
            if current_section == "goals":
                goals.append(stripped.removeprefix("- ").strip())
            elif current_section == "initial_focus":
                initial_focus.append(stripped.removeprefix("- ").strip())

    return project_summary, tuple(goals), tuple(initial_focus)


# Map internal tool names to friendly human descriptions
_TOOL_HUMAN_NAMES: dict[str, str] = {
    "web_fetch": "Browse & fetch web pages",
    "fetch_url": "Browse & fetch web pages",
    "web_search": "Search the web",
    "search_web": "Search the web",
    "file_read": "Read files in your project folder",
    "file_write": "Write files in your project folder",
    "list_files": "List files in your project folder",
    "terminal_exec": "Run terminal commands",
    "exec": "Run terminal commands",
    "send_email": "Send email",
    "read_email": "Read email",
    "search_email": "Search email",
    "list_calendar": "View calendar events",
    "read_memory": "Remember things about you",
    "write_memory": "Remember things about you",
    "store_memory": "Remember things about you",
}


def _tool_human_name(tool_name: str) -> str:
    return _TOOL_HUMAN_NAMES.get(tool_name, tool_name.replace("_", " ").capitalize())


def _render_tools(runtime: PersistentRuntime) -> str:
    snapshot = build_system_context_snapshot(tool_registry=runtime.active_tool_registry)
    if not snapshot.available_tools:
        return "🛠️ What I can do\n\nNo tools are loaded right now."

    direct_tools: list[str] = []
    approval_tools: list[str] = []
    unavailable_tools: list[str] = []
    seen: set[str] = set()

    for tool in snapshot.available_tools:
        human = _tool_human_name(tool.name)
        if human in seen:
            continue
        seen.add(human)
        if tool.availability == "unavailable":
            unavailable_tools.append(human)
        elif tool.requires_approval:
            approval_tools.append(human)
        else:
            direct_tools.append(human)

    lines = ["🛠️ What I can do", ""]
    if direct_tools:
        lines.append("Ready to use:")
        for index, item in enumerate(direct_tools, start=1):
            lines.append(f"  {index}. {item}")
        lines.append("")
    if approval_tools:
        lines.append("Available with your approval:")
        for index, item in enumerate(approval_tools, start=1):
            lines.append(f"  {index}. {item}")
        lines.append("")
    if unavailable_tools:
        lines.append("Not available right now:")
        for index, item in enumerate(unavailable_tools, start=1):
            lines.append(f"  {index}. {item}")
        lines.append("")
    if snapshot.installed_plugins:
        plugin_list = ", ".join(snapshot.installed_plugins)
        lines.append(f"Connected plugins: {plugin_list}")
    _append_next(lines, "Use /plugins to inspect installed and available capability plugins.")
    return "\n".join(lines).rstrip()


def _runtime_installed_plugins(runtime: PersistentRuntime) -> tuple[str, ...]:
    registry = getattr(runtime, "active_tool_registry", None)
    if registry is None or not hasattr(registry, "list_installed_plugins"):
        return ()
    return tuple(registry.list_installed_plugins())


def _render_plugins(runtime: PersistentRuntime, args: list[str]) -> str:
    if len(args) > 1:
        return "Usage: /plugins [available]"
    if args and args[0].strip().lower() == "available":
        lines = ["🧩 Plugin catalog", ""]
        for index, entry in enumerate(list_plugin_catalog(), start=1):
            provider_count = len(entry.providers)
            provider_label = f" • {provider_count} provider option(s)" if provider_count else ""
            lines.append(_display_ref(f"{entry.name} (`{entry.plugin_id}`) — {entry.status}{provider_label}", index))
            lines.append(f"  {entry.summary}")
        _append_next(lines, "Use /plugin <plugin_id> for setup details.")
        return "\n".join(lines)
    if args:
        return "Usage: /plugins [available]"

    installed = set(_runtime_installed_plugins(runtime))
    lines = ["🧩 Plugins", ""]
    if installed:
        lines.append("Connected:")
        for index, plugin_id in enumerate(sorted(installed), start=1):
            entry = get_plugin_catalog_entry(plugin_id)
            name = entry.name if entry else plugin_id
            lines.append(f"  {index}. {name} (`{plugin_id}`)")
    else:
        lines.append("Connected: none")

    lines.extend(["", "Available:"])
    for index, entry in enumerate(list_plugin_catalog(), start=1):
        marker = "connected" if entry.plugin_id in installed else entry.status
        lines.append(f"  {index}. {entry.name} (`{entry.plugin_id}`) — {marker}")
    lines.extend(
        [
            "",
            "Enable or disable plugins in settings/installer config.",
        ]
    )
    _append_next(lines, "Use /plugin <plugin_id> for setup details.")
    return "\n".join(lines)


def _render_plugin_detail(plugin_id: str | None) -> str:
    if not plugin_id:
        return "Usage: /plugin <plugin_id>"
    entry = get_plugin_catalog_entry(plugin_id)
    if entry is None:
        return f"Unknown plugin: {plugin_id}\nUse /plugins available to see the catalog."
    lines = [
        f"🧩 {entry.name}",
        "",
        f"ID: `{entry.plugin_id}`",
        f"Category: {entry.category}",
        f"Status: {entry.status}",
        f"Summary: {entry.summary}",
    ]
    if entry.tools:
        lines.extend(["", "Tools:"])
        for tool_name in entry.tools:
            lines.append(f"  • `{tool_name}`")
    if entry.providers:
        lines.extend(["", "Provider options:"])
        for provider in entry.providers:
            lines.append(f"  • {provider.name} (`{provider.provider_id}`) — {provider.status}")
            lines.append(f"    {provider.notes}")
    if entry.setup_hint:
        lines.extend(["", f"Setup: {entry.setup_hint}"])
    lines.extend(
        [
            "",
            "Config keys:",
            "  • NULLION_ENABLED_PLUGINS",
            "  • NULLION_PROVIDER_BINDINGS",
            "",
            "See docs/plugins.md for full setup and authoring docs.",
        ]
    )
    return "\n".join(lines)


def _runtime_settings(runtime: PersistentRuntime) -> NullionSettings:
    settings = getattr(runtime, "settings", None)
    if isinstance(settings, NullionSettings):
        return settings
    return load_settings()


def _configured_skill_packs(runtime: PersistentRuntime) -> tuple[str, ...]:
    return tuple(_runtime_settings(runtime).enabled_skill_packs)


def _render_skill_packs(runtime: PersistentRuntime, args: list[str]) -> str:
    if len(args) > 1:
        return "Usage: /skill-packs [available]"
    if args and args[0].strip().lower() == "available":
        lines = ["📚 Skill pack catalog", ""]
        for index, entry in enumerate(list_available_skill_packs(), start=1):
            lines.append(_display_ref(f"{entry.name} (`{entry.pack_id}`) — {entry.status}", index))
            lines.append(f"  {entry.summary}")
        _append_next(lines, "Use /skill-pack <pack_id> for setup details.")
        return "\n".join(lines)
    if args:
        return "Usage: /skill-packs [available]"

    configured = set(_configured_skill_packs(runtime))
    lines = ["📚 Skill packs", ""]
    if configured:
        lines.append("Enabled:")
        for index, pack_id in enumerate(sorted(configured), start=1):
            entry = get_skill_pack_catalog_entry(pack_id)
            name = entry.name if entry else pack_id
            lines.append(f"  {index}. {name} (`{pack_id}`)")
    else:
        lines.append("Enabled: none")

    lines.extend(["", "Available:"])
    for index, entry in enumerate(list_available_skill_packs(), start=1):
        marker = "enabled" if entry.pack_id in configured else entry.status
        lines.append(f"  {index}. {entry.name} (`{entry.pack_id}`) — {marker}")
    lines.extend(
        [
            "",
            "Skill packs teach Nulliøn how to approach products and workflows.",
            "They do not grant account access or install plugins by themselves.",
        ]
    )
    _append_next(lines, "Use /skill-pack <pack_id> for setup details.")
    return "\n".join(lines)


def _render_skill_pack_detail(pack_id: str | None) -> str:
    if not pack_id:
        return "Usage: /skill-pack <pack_id>"
    entry = get_skill_pack_catalog_entry(pack_id)
    if entry is None:
        return f"Unknown skill pack: {pack_id}\nUse /skill-packs available to see the catalog."
    lines = [
        f"📚 {entry.name}",
        "",
        f"ID: `{entry.pack_id}`",
        f"Status: {entry.status}",
        f"Source: {entry.source_url}",
        f"Summary: {entry.summary}",
    ]
    if entry.coverage:
        lines.extend(["", "Covers:"])
        for item in entry.coverage:
            lines.append(f"  • {item}")
    if entry.setup_hint:
        lines.extend(["", f"Setup: {entry.setup_hint}"])
    lines.extend(
        [
            "",
            "Security notes:",
            "  • Skill packs are reference instructions, not executable plugins.",
            "  • Enable plugins separately when you want account, browser, file, or network capability.",
            "  • Sentinel approval still applies to tools the skill suggests using.",
            "",
            "See docs/skill-packs.md for full setup and authoring docs.",
        ]
    )
    return "\n".join(lines)


def _render_codebase(runtime: PersistentRuntime) -> str:
    del runtime
    return format_codebase_summary(build_codebase_summary(_REPO_ROOT))


def _render_system_context(runtime: PersistentRuntime) -> str:
    project_summary, goals, initial_focus = _readme_project_context()
    snapshot = build_system_context_snapshot(
        project_summary=project_summary,
        goals=goals,
        initial_focus=initial_focus,
        tool_registry=runtime.active_tool_registry,
    )
    rendered = format_system_context_for_prompt(snapshot)
    if not rendered:
        return "🧭 Nullion system context\n\nNo internal context is available right now."
    return f"🧭 Nullion system context\n\n{rendered}"


def _render_restore(runtime: PersistentRuntime, token: str | None) -> str:

    normalized_token = _normalize_mention_suffix(token) if token is not None else None
    latest_requested = normalized_token is None or normalized_token == "latest"
    if latest_requested:
        generation = 0
    else:
        try:
            generation = int(normalized_token)
        except ValueError:
            return _INVALID_RESTORE_GENERATION

    try:
        runtime.restore_from_backup(generation=generation)
    except FileNotFoundError:
        return f"Backup generation {generation} is unavailable."

    if latest_requested:
        prefix = "Restored latest backup (generation 0)."
    else:
        prefix = f"Restored backup generation {generation}."
    return f"{prefix}\nUse /status to review the recovered runtime state."



def _render_skills(runtime: PersistentRuntime) -> str:
    skills = list_skills(runtime.store)
    if not skills:
        return "🧠 Saved skills\n\nNo saved skills yet."

    lines = ["🧠 Saved skills", ""]
    for index, skill in enumerate(skills, start=1):
        lines.append(_display_ref(f"{skill.title} — {skill.skill_id}", index))
        lines.append(f"   {skill.summary}")
    _append_next(lines, "Use /skill <n> to open one skill or /skill-history <n> for revisions.")
    return "\n".join(lines)


def _resolve_skill_token(runtime: PersistentRuntime, token: str | None):
    if token is None:
        return None
    normalized_token = _normalize_mention_suffix(token)
    direct = get_skill(runtime.store, normalized_token)
    if direct is not None:
        return direct
    if normalized_token.isdigit():
        index = int(normalized_token)
        skills = list_skills(runtime.store)
        if 1 <= index <= len(skills):
            return skills[index - 1]
    return None



def _render_skill(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /skill <n|skill_id>"
    normalized_token = _normalize_mention_suffix(token)
    skill = _resolve_skill_token(runtime, token)
    if skill is None:
        return f"Skill not found: {normalized_token}"
    return render_skill_for_telegram(skill)



def _render_skill_history(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /skill-history <n|skill_id>"
    normalized_token = _normalize_mention_suffix(token)
    skill = _resolve_skill_token(runtime, token)
    if skill is None:
        return f"Skill not found: {normalized_token}"
    lines = [
        "🕘 Skill history",
        f"{skill.title} — {skill.skill_id}",
        f"Current revision: {skill.revision}",
    ]
    if not skill.revision_history:
        lines.extend(["", "No prior revisions."])
        return "\n".join(lines)

    lines.extend(["", "Previous revisions:"])
    for revision in reversed(skill.revision_history):
        lines.append(f"- r{revision.revision}: {revision.summary}")
    return "\n".join(lines)



def _update_skill(runtime: PersistentRuntime, parts: list[str]) -> str:
    if len(parts) < 4:
        return "Usage: /update-skill <n|skill_id> <field> <value>"
    skill_record = _resolve_skill_token(runtime, parts[1])
    normalized_skill_id = skill_record.skill_id if skill_record is not None else _normalize_mention_suffix(parts[1])
    field_name = parts[2].lower()
    value = " ".join(parts[3:]).strip()
    if field_name not in {"title", "summary", "trigger"}:
        return (
            f"Unsupported field for /update-skill: {field_name}. "
            "Allowed: title, summary, trigger."
        )
    try:
        skill = runtime.update_skill(normalized_skill_id, **{field_name: value})
    except KeyError:
        return f"Skill not found: {normalized_skill_id}"
    except ValueError as exc:
        return str(exc)
    return f"Updated skill {normalized_skill_id}.\nRevision: {skill.revision}"



def _revert_skill(runtime: PersistentRuntime, skill_token: str | None, revision_token: str | None) -> str:
    if skill_token is None or revision_token is None:
        return "Usage: /revert-skill <n|skill_id> <revision>"
    skill_record = _resolve_skill_token(runtime, skill_token)
    normalized_skill_id = skill_record.skill_id if skill_record is not None else _normalize_mention_suffix(skill_token)
    normalized_revision = _normalize_mention_suffix(revision_token)
    try:
        target_revision = int(normalized_revision)
    except ValueError:
        return "Revision must be an integer."
    if target_revision <= 0:
        return "Revision must be an integer."

    skill = get_skill(runtime.store, normalized_skill_id)
    if skill is None:
        return f"Skill not found: {normalized_skill_id}"
    if target_revision == skill.revision:
        return f"Skill {normalized_skill_id} is already at revision {target_revision}."

    target_state = next(
        (revision for revision in skill.revision_history if revision.revision == target_revision),
        None,
    )
    if target_state is None:
        return f"Revision not found for skill {normalized_skill_id}: {target_revision}"

    updated = runtime.update_skill(
        normalized_skill_id,
        title=target_state.title,
        summary=target_state.summary,
        trigger=target_state.trigger,
        steps=target_state.steps,
        tags=target_state.tags,
    )
    return (
        f"Reverted skill {normalized_skill_id} to revision {target_revision}.\n"
        f"New revision: {updated.revision}"
    )



def _approval_permissions_for_request(request: ApprovalRequest) -> list[str]:
    if request.action == "use_tool":
        return [f"tool:{request.resource}"]
    return [f"{request.action}:{request.resource}"]


def _approval_list_line(approval: ApprovalRequest) -> str:
    context = approval.context if isinstance(approval.context, dict) else {}
    is_structured_tool_request = bool(context) or approval.action in {"use_tool", "allow_boundary"} or approval.request_kind == "boundary_policy"
    if not is_structured_tool_request:
        return (
            f"{approval.approval_id} — {approval.requested_by} requested "
            f"{approval.action} on {approval.resource} [{approval.status.value}]"
        )
    display = approval_display_from_request(approval)
    detail = f" — {display.detail}" if display.detail else ""
    return (
        f"{approval.approval_id} — {approval.requested_by} requested "
        f"{display.label}{detail} [{approval.status.value}]"
    )


def _parse_filter_tokens(tokens: list[str]) -> dict[str, str] | None:
    parsed: dict[str, str] = {}
    for token in tokens:
        normalized = _normalize_mention_suffix(token)
        key, separator, value = normalized.partition("=")
        if not separator or not key or not value:
            return None
        parsed[key.lower()] = value
    return parsed



_APPROVAL_STATUS_PRIORITY = {
    "pending": 0,
    "approved": 1,
    "denied": 2,
}

_GRANT_STATE_PRIORITY = {
    "active": 0,
    "expired": 1,
    "revoked": 2,
}

_DENSE_LIST_LIMIT = 5
_BUILDER_PROPOSAL_PAGE_SIZE = 10


def _grant_state(grant: PermissionGrant, *, now: datetime | None = None) -> str:
    reference = datetime.now(UTC) if now is None else now
    if grant.revoked_at is not None:
        return "revoked"
    if grant.expires_at is not None and grant.expires_at <= reference:
        return "expired"
    return "active"



def _approval_sort_key(approval: ApprovalRequest) -> tuple[object, ...]:
    return (
        _APPROVAL_STATUS_PRIORITY.get(approval.status.value, 99),
        approval.requested_by,
        approval.action,
        approval.approval_id,
    )



def _grant_sort_key(grant: PermissionGrant) -> tuple[object, ...]:
    state = _grant_state(grant)
    return (
        _GRANT_STATE_PRIORITY.get(state, 99),
        grant.principal_id,
        grant.permission,
        grant.grant_id,
    )


def _sorted_approval_requests(runtime: PersistentRuntime) -> list[ApprovalRequest]:
    return sorted(runtime.store.list_approval_requests(), key=_approval_sort_key)


def _resolve_approval_token(runtime: PersistentRuntime, token: str | None) -> ApprovalRequest | None:
    if token is None:
        return None
    normalized_token = _normalize_mention_suffix(token)
    direct = runtime.store.get_approval_request(normalized_token)
    if direct is not None:
        return direct
    if normalized_token.isdigit():
        index = int(normalized_token)
        approvals = _sorted_approval_requests(runtime)
        if 1 <= index <= len(approvals):
            return approvals[index - 1]
    return None


def _resolve_grant_token(runtime: PersistentRuntime, token: str | None) -> PermissionGrant | None:
    if token is None:
        return None
    normalized_token = _normalize_mention_suffix(token)
    direct = runtime.store.get_permission_grant(normalized_token)
    if direct is not None:
        return direct
    if normalized_token.isdigit():
        index = int(normalized_token)
        grants = sorted(runtime.store.list_permission_grants(), key=_grant_sort_key)
        if 1 <= index <= len(grants):
            return grants[index - 1]
    return None



def _render_approvals(runtime: PersistentRuntime, tokens: list[str] | None = None) -> str:
    reconcile = getattr(runtime, "reconcile_effectively_approved_pending_approvals", None)
    if callable(reconcile):
        reconcile(actor="operator_command")
    filter_tokens = tokens or []
    filters = _parse_filter_tokens(filter_tokens)
    if filters is None:
        return (
            "Usage: /approvals [status=pending|approved|denied|all] "
            "[principal=<principal_id>] [permission=<action:resource>]"
        )

    status_filter = filters.get("status", "pending")
    if status_filter not in {"pending", "approved", "denied", "all"}:
        return (
            "Usage: /approvals [status=pending|approved|denied|all] "
            "[principal=<principal_id>] [permission=<action:resource>]"
        )
    if any(key not in {"status", "principal", "permission"} for key in filters):
        return (
            "Usage: /approvals [status=pending|approved|denied|all] "
            "[principal=<principal_id>] [permission=<action:resource>]"
        )

    approvals = list(runtime.store.list_approval_requests())
    if status_filter != "all":
        approvals = [approval for approval in approvals if approval.status.value == status_filter]
    principal_filter = filters.get("principal")
    if principal_filter is not None:
        approvals = [approval for approval in approvals if approval.requested_by == principal_filter]
    permission_filter = filters.get("permission")
    if permission_filter is not None:
        approvals = [
            approval
            for approval in approvals
            if f"{approval.action}:{approval.resource}" == permission_filter
        ]

    if not approvals:
        if not filter_tokens:
            return "✅ Approval inbox\n\nNo pending approval requests."
        return "✅ Approval inbox\n\nNo approval requests matched filters."

    approvals = sorted(approvals, key=_approval_sort_key)
    if len(approvals) <= _DENSE_LIST_LIMIT:
        lines = ["✅ Approval inbox", ""]
        for index, approval in enumerate(approvals, start=1):
            lines.append(_display_ref(_approval_list_line(approval), index))
        _append_next(lines, "Use /approval <n>, /approve <n>, or /deny <n>.")
        return "\n".join(lines)

    status_counts = Counter(approval.status.value for approval in approvals)
    lines = [
        "✅ Approval inbox",
        "",
        (
            f"{status_counts.get('pending', 0)} pending"
            f" • {status_counts.get('approved', 0)} approved"
            f" • {status_counts.get('denied', 0)} denied"
        ),
    ]
    shown = approvals[:_DENSE_LIST_LIMIT]
    for index, approval in enumerate(shown, start=1):
        lines.append(_display_ref(_approval_list_line(approval), index))
    remaining = len(approvals) - len(shown)
    label = "approval request" if remaining == 1 else "approval requests"
    lines.append(f"... and {remaining} more {label}.")
    _append_next(lines, "Use /approval <n>, /approve <n>, or /deny <n> for the numbered rows shown.")
    return "\n".join(lines)



def _render_approval(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /approval <n|approval_id>"
    normalized_token = _normalize_mention_suffix(token)
    approval = _resolve_approval_token(runtime, token)
    if approval is None:
        return f"Approval request not found: {normalized_token}"
    lines = [
        "✅ Approval request",
        f"ID: {approval.approval_id}",
        f"Requested by: {approval.requested_by}",
    ]
    display = approval_display_from_request(approval)
    trigger_label = approval_trigger_flow_label(approval)
    if trigger_label:
        lines.append(f"Triggered by: {trigger_label}")
    show_display_detail = bool(approval.context) or approval.action in {"use_tool", "allow_boundary"} or approval.request_kind == "boundary_policy"
    if show_display_detail:
        lines.extend(
            [
                f"Prompt: {display.title}",
                f"Request: {display.label}",
                f"Detail: {display.detail}",
            ]
        )
    lines.extend(
        [
            f"Action: {approval.action}",
            f"Resource: {approval.resource}",
            f"Status: {approval.status.value}",
        ]
    )
    return "\n".join(lines)



def _approve_request(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /approve <n|approval_id>"
    normalized_token = _normalize_mention_suffix(token)
    approval = _resolve_approval_token(runtime, token)
    if approval is None:
        return f"Approval request not found: {normalized_token}"
    from nullion.approval_decisions import approve_request_with_mode, approval_tool_permissions, is_outbound_boundary_approval

    if is_outbound_boundary_approval(approval):
        duration = load_settings().web_session_allow_duration
        label = web_session_allow_duration_label(duration)
        try:
            decision = approve_request_with_mode(
                runtime,
                approval.approval_id,
                mode="run",
                source=f"operator command ({label})",
                run_expires_at=web_session_allow_expires_at(duration),
                auto_approve_run_boundaries=False,
            )
        except ValueError as exc:
            return str(exc)
        updated = decision.approval
        return (
            f"Approved request {approval.approval_id}.\n"
            f"Allowed all web domains ({label}).\n"
            f"Principal: {updated.requested_by}"
        )
    try:
        decision = approve_request_with_mode(
            runtime,
            approval.approval_id,
            mode="once",
            source="operator command",
        )
    except ValueError as exc:
        return str(exc)
    updated = decision.approval
    granted = approval_tool_permissions(updated)
    return (
        f"Approved request {approval.approval_id}.\n"
        f"Granted: {', '.join(granted)}\n"
        f"Principal: {updated.requested_by}"
    )



def _deny_request(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /deny <n|approval_id>"
    normalized_token = _normalize_mention_suffix(token)
    approval = _resolve_approval_token(runtime, token)
    if approval is None:
        return f"Approval request not found: {normalized_token}"
    try:
        runtime.deny_approval_request(approval.approval_id, actor="operator")
    except ValueError as exc:
        return str(exc)
    return f"Denied request {approval.approval_id}."



def _render_grants(runtime: PersistentRuntime, tokens: list[str] | None = None) -> str:
    filter_tokens = tokens or []
    filters = _parse_filter_tokens(filter_tokens)
    if filters is None:
        return (
            "Usage: /grants [status=active|inactive|all] "
            "[principal=<principal_id>] [permission=<action:resource>]"
        )

    if any(key not in {"status", "principal", "permission"} for key in filters):
        return (
            "Usage: /grants [status=active|inactive|all] "
            "[principal=<principal_id>] [permission=<action:resource>]"
        )

    status_filter = filters.get("status", "active")
    if status_filter not in {"active", "inactive", "all"}:
        return (
            "Usage: /grants [status=active|inactive|all] "
            "[principal=<principal_id>] [permission=<action:resource>]"
        )

    grants = sorted(runtime.store.list_permission_grants(), key=_grant_sort_key)
    if status_filter == "active":
        grants = [grant for grant in grants if is_permission_grant_active(grant)]
    elif status_filter == "inactive":
        grants = [grant for grant in grants if not is_permission_grant_active(grant)]

    principal_filter = filters.get("principal")
    if principal_filter is not None:
        grants = [grant for grant in grants if grant.principal_id == principal_filter]

    permission_filter = filters.get("permission")
    if permission_filter is not None:
        grants = [grant for grant in grants if grant.permission == permission_filter]

    if not grants:
        if not filter_tokens:
            return "🛡️ Permission grants\n\nNo active permission grants."
        return "🛡️ Permission grants\n\nNo permission grants matched filters."

    if len(grants) <= _DENSE_LIST_LIMIT:
        lines = ["🛡️ Permission grants", ""]
        for index, grant in enumerate(grants, start=1):
            lines.append(
                _display_ref(
                    f"{grant.grant_id} — {grant.principal_id} can {grant.permission} (from {grant.approval_id}) [{_grant_state(grant)}]",
                    index,
                )
            )
        _append_next(lines, "Use /grant <n> to inspect or /revoke-grant <n> to revoke.")
        return "\n".join(lines)

    state_counts = Counter(_grant_state(grant) for grant in grants)
    lines = [
        "🛡️ Permission grants",
        "",
        (
            f"{state_counts.get('active', 0)} active"
            f" • {state_counts.get('expired', 0)} expired"
            f" • {state_counts.get('revoked', 0)} revoked"
        ),
    ]
    shown = grants[:_DENSE_LIST_LIMIT]
    for index, grant in enumerate(shown, start=1):
        lines.append(
            _display_ref(
                f"{grant.grant_id} — {grant.principal_id} can {grant.permission} (from {grant.approval_id}) [{_grant_state(grant)}]",
                index,
            )
        )
    remaining = len(grants) - len(shown)
    label = "permission grant" if remaining == 1 else "permission grants"
    lines.append(f"... and {remaining} more {label}.")
    _append_next(lines, "Use /grant <n> or /revoke-grant <n> for the numbered rows shown.")
    return "\n".join(lines)



def _render_grant(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /grant <n|grant_id>"
    normalized_token = _normalize_mention_suffix(token)
    grant = _resolve_grant_token(runtime, token)
    if grant is None:
        return f"Permission grant not found: {normalized_token}"
    expires = "never" if grant.expires_at is None else grant.expires_at.isoformat()
    status = _grant_state(grant)
    return (
        "🛡️ Permission grant\n"
        f"ID: {grant.grant_id}\n"
        f"Principal: {grant.principal_id}\n"
        f"Permission: {grant.permission}\n"
        f"Approval: {grant.approval_id}\n"
        f"Granted by: {grant.granted_by}\n"
        f"Status: {status}\n"
        f"Expires: {expires}"
    )



def _revoke_grant(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /revoke-grant <n|grant_id>"
    normalized_token = _normalize_mention_suffix(token)
    grant = _resolve_grant_token(runtime, token)
    if grant is None:
        return f"Permission grant not found: {normalized_token}"
    try:
        runtime.revoke_permission_grant(grant.grant_id, actor="operator")
    except ValueError as exc:
        return str(exc)
    return f"Revoked grant {grant.grant_id}."



def _parse_builder_proposal_page(tokens: list[str] | None) -> int | None:
    raw_tokens = tokens or []
    if not raw_tokens:
        return 1
    if len(raw_tokens) != 1:
        return None
    token = _normalize_mention_suffix(raw_tokens[0]).strip().lower()
    if token.startswith("page="):
        token = token.split("=", 1)[1]
    if not token.isdigit():
        return None
    page = int(token)
    return page if page >= 1 else None


def _render_builder_proposals(runtime: PersistentRuntime, tokens: list[str] | None = None) -> str:
    page = _parse_builder_proposal_page(tokens)
    if page is None:
        return "Usage: /proposals [page=<n>]"
    proposals = list_builder_proposals(runtime.store)
    if not proposals:
        return "🧱 Builder proposals\n\nNo Builder proposals yet."

    total = len(proposals)
    page_count = max(1, (total + _BUILDER_PROPOSAL_PAGE_SIZE - 1) // _BUILDER_PROPOSAL_PAGE_SIZE)
    if page > page_count:
        return f"Builder proposal page not found: {page}. Available pages: 1-{page_count}."

    start = (page - 1) * _BUILDER_PROPOSAL_PAGE_SIZE
    shown = proposals[start:start + _BUILDER_PROPOSAL_PAGE_SIZE]
    lines = ["🧱 Builder proposals", ""]
    if page_count > 1:
        lines.append(f"Page {page}/{page_count} • {total} total")
    for index, record in enumerate(shown, start=start + 1):
        lines.append(_display_ref(f"{record.proposal_id} — {record.proposal.title} [{record.status}]", index))
        lines.append(f"   {record.proposal.summary}")
    next_page = f"Use /proposals page={page + 1} to load more." if page < page_count else ""
    _append_next(
        lines,
        next_page,
        "Use /proposal <n>, /accept-proposal <n>, /reject-proposal <n>, or /archive-proposal <n>.",
    )
    return "\n".join(lines)


def _resolve_builder_proposal_token(runtime: PersistentRuntime, token: str | None):
    if token is None:
        return None
    normalized_token = _normalize_mention_suffix(token)
    direct = get_builder_proposal(runtime.store, normalized_token)
    if direct is not None:
        return direct
    if normalized_token.isdigit():
        index = int(normalized_token)
        proposals = list_builder_proposals(runtime.store)
        if 1 <= index <= len(proposals):
            return proposals[index - 1]
    return None



def _render_builder_proposal(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /proposal <n|proposal_id>"
    normalized_token = _normalize_mention_suffix(token)
    record = _resolve_builder_proposal_token(runtime, token)
    if record is None:
        return f"Builder proposal not found: {normalized_token}"
    confidence_percent = int(round(record.proposal.confidence * 100))
    lines = [
        "🧱 Builder proposal",
        record.proposal.title,
        "",
        record.proposal.summary,
        f"Proposal ID: {record.proposal_id}",
        f"Status: {record.status}",
    ]
    if record.status == "accepted" and record.accepted_skill_id:
        lines.append(f"Accepted skill: {record.accepted_skill_id} (/skill {record.accepted_skill_id})")
    lines.append(f"Confidence: {confidence_percent}% • Approval mode: {record.proposal.approval_mode}")
    return "\n".join(lines)



def _accept_builder_proposal(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /accept-proposal <n|proposal_id>"
    normalized_token = _normalize_mention_suffix(token)
    record = _resolve_builder_proposal_token(runtime, token)
    if record is None:
        return f"Builder proposal not found: {normalized_token}"
    if record.proposal.approval_mode != "skill":
        return (
            f"Builder proposal {record.proposal_id} is a {record.proposal.approval_mode} proposal and cannot be accepted as a skill.\n"
            f"Use /reject-proposal {record.proposal_id} or /archive-proposal {record.proposal_id}."
        )
    try:
        skill = runtime.accept_stored_builder_skill_proposal(record.proposal_id, actor="operator")
    except ValueError as exc:
        return str(exc)
    return f"Accepted Builder proposal {record.proposal_id}.\nSaved skill: {skill.title}"


def _reject_builder_proposal(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /reject-proposal <n|proposal_id>"
    normalized_token = _normalize_mention_suffix(token)
    record = _resolve_builder_proposal_token(runtime, token)
    if record is None:
        return f"Builder proposal not found: {normalized_token}"
    try:
        reject_stored_builder_proposal(runtime.store, record.proposal_id, actor="operator")
    except KeyError:
        return f"Builder proposal not found: {normalized_token}"
    except ValueError as exc:
        return str(exc)
    runtime.checkpoint()
    return f"Rejected Builder proposal {record.proposal_id}."


def _archive_builder_proposal(runtime: PersistentRuntime, token: str | None) -> str:
    if token is None:
        return "Usage: /archive-proposal <n|proposal_id>"
    normalized_token = _normalize_mention_suffix(token)
    record = _resolve_builder_proposal_token(runtime, token)
    if record is None:
        return f"Builder proposal not found: {normalized_token}"
    try:
        archive_stored_builder_proposal(runtime.store, record.proposal_id, actor="operator")
    except KeyError:
        return f"Builder proposal not found: {normalized_token}"
    except ValueError as exc:
        return str(exc)
    runtime.checkpoint()
    return f"Archived Builder proposal {record.proposal_id}."


def _memory_timestamp(entry: UserMemoryEntry) -> datetime | None:
    return entry.updated_at or entry.created_at


def _memory_sort_key(entry: UserMemoryEntry) -> tuple[datetime, str]:
    timestamp = _memory_timestamp(entry) or datetime.min.replace(tzinfo=UTC)
    return (timestamp, entry.entry_id)


def _memory_entries_for_owner(runtime: PersistentRuntime, owner: str) -> list[UserMemoryEntry]:
    return sorted(
        [entry for entry in runtime.store.list_user_memory_entries() if entry.owner == owner],
        key=_memory_sort_key,
        reverse=True,
    )


def _resolve_memory_token(
    runtime: PersistentRuntime,
    owner: str,
    token: str | None,
) -> UserMemoryEntry | None:
    if token is None:
        return None
    normalized_token = _normalize_mention_suffix(token)
    direct = runtime.store.get_user_memory_entry(normalized_token)
    if direct is not None and direct.owner == owner:
        return direct
    if normalized_token.isdigit():
        index = int(normalized_token)
        entries = _memory_entries_for_owner(runtime, owner)
        if 1 <= index <= len(entries):
            return entries[index - 1]
    return None


def _memory_age_cutoff(token: str, *, now: datetime | None = None) -> datetime | None:
    normalized = token.strip().lower().replace("_", "-").replace(" ", "-")
    reference = now or datetime.now(UTC)
    if normalized in {"last-hour", "1h", "1-hour", "hour", "past-hour"}:
        return reference - timedelta(hours=1)
    if normalized in {"24h", "24-hrs", "24-hour", "24-hours", "last-24h", "last-day", "day"}:
        return reference - timedelta(hours=24)
    if normalized in {"7d", "7-day", "7-days", "last-week", "week"}:
        return reference - timedelta(days=7)
    return None


def _format_memory_entry(entry: UserMemoryEntry, index: int) -> str:
    value = " ".join(str(entry.value).split())
    if len(value) > 140:
        value = f"{value[:137]}..."
    updated = _memory_timestamp(entry)
    suffix = f" • {updated.strftime('%Y-%m-%d %H:%M UTC')}" if updated else ""
    return _display_ref(f"{entry.key} — {value} [{entry.kind.value}]{suffix}", index)


def _render_memory(runtime: PersistentRuntime, args: list[str], *, owner: str | None = None) -> str:
    memory_owner = owner or memory_owner_for_web_admin()
    if len(args) > 1:
        return "Usage: /memory [all]"
    if args and _normalize_mention_suffix(args[0]).lower() != "all":
        return "Usage: /memory [all]"

    entries = _memory_entries_for_owner(runtime, memory_owner)
    if not entries:
        return "🧠 Memory\n\nNo saved memory yet."

    show_all = bool(args and _normalize_mention_suffix(args[0]).lower() == "all")
    shown = entries if show_all else entries[:5]
    lines = ["🧠 Memory", "", f"{len(entries)} saved item(s)"]
    for index, entry in enumerate(shown, start=1):
        lines.append(_format_memory_entry(entry, index))
        lines.append(f"  ID: {entry.entry_id}")
    if len(shown) < len(entries):
        lines.extend(["", f"Showing {len(shown)} recent item(s). Use /memory all to see everything."])
    _append_next(
        lines,
        "Use /memory delete <n|entry_id> to delete one item.",
        "Use /memory delete last-hour, /memory delete 24h, or /memory delete all for bulk cleanup.",
    )
    return "\n".join(lines)


def _delete_memory(runtime: PersistentRuntime, args: list[str], *, owner: str | None = None, now: datetime | None = None) -> str:
    memory_owner = owner or memory_owner_for_web_admin()
    if not args or len(args) > 2:
        return "Usage: /memory delete <n|entry_id|last-hour|24h|all>"
    token = _normalize_mention_suffix(" ".join(args))
    entries = _memory_entries_for_owner(runtime, memory_owner)
    if not entries:
        return "🧠 Memory\n\nNo saved memory to delete."

    targets: list[UserMemoryEntry]
    normalized = token.lower()
    if normalized == "all":
        targets = entries
    else:
        cutoff = _memory_age_cutoff(normalized, now=now)
        if cutoff is not None:
            targets = [
                entry
                for entry in entries
                if (_memory_timestamp(entry) or datetime.min.replace(tzinfo=UTC)) >= cutoff
            ]
        else:
            entry = _resolve_memory_token(runtime, memory_owner, token)
            if entry is None:
                return f"Memory item not found: {token}"
            targets = [entry]

    for entry in targets:
        runtime.store.remove_user_memory_entry(entry.entry_id)
    runtime.checkpoint()
    if len(targets) == 1:
        return f"Deleted memory item {targets[0].entry_id}."
    return f"Deleted {len(targets)} memory item(s)."


_MODEL_PREFERENCE_KEY = "operator.model_preference"


def _do_restart() -> None:
    os.execv(sys.executable, [sys.executable] + sys.argv)


def _begin_gateway_restart_notice() -> None:
    try:
        from nullion.gateway_notifications import begin_gateway_restart

        begin_gateway_restart(settings=load_settings(), async_delivery=False)
    except Exception:
        # Keep /restart dependable even if lifecycle notification persistence or
        # chat delivery is unavailable.
        pass


def _restart_managed_services_if_available() -> str | None:
    try:
        from nullion.service_control import restart_managed_services, successful_restart_message
    except Exception:
        return None

    try:
        results = restart_managed_services(continue_on_error=True)
    except RuntimeError:
        return None
    message = successful_restart_message(results)
    return message or None


def _restart_launchd_service_if_available() -> str | None:
    return _restart_managed_services_if_available()


def _schedule_desktop_reload_for_restart(*, service: object = None) -> None:
    try:
        from nullion.desktop_entrypoint import schedule_desktop_reload

        schedule_desktop_reload(port=_update_web_port(service))
    except Exception:
        pass


def _restart_bot(runtime: PersistentRuntime, *, service: object = None) -> str:
    runtime.checkpoint()
    _begin_gateway_restart_notice()
    _schedule_desktop_reload_for_restart(service=service)
    service_restart = _restart_launchd_service_if_available()
    if service_restart is not None:
        return service_restart
    threading.Timer(0.5, _do_restart).start()
    return "Restarting Nulliøn..."


def _update_web_port(service: object = None) -> int:
    candidates = [
        getattr(service, "web_port", None) if service is not None else None,
        getattr(getattr(service, "settings", None), "web_port", None) if service is not None else None,
        os.environ.get("NULLION_WEB_PORT"),
        os.environ.get("PORT"),
        "8742",
    ]
    for candidate in candidates:
        try:
            port = int(str(candidate))
        except (TypeError, ValueError):
            continue
        if 0 < port < 65536:
            return port
    return 8742


def _run_update_flow(*, web_port: int, ignore_check_failures: bool = False, checkpoint_path: str | Path | None = None):
    from nullion.updater import run_update

    return asyncio.run(
        run_update(
            web_port=web_port,
            ignore_check_failures=ignore_check_failures,
            checkpoint_path=checkpoint_path,
        )
    )


def _format_update_steps(result) -> str:
    lines = []
    for step in getattr(result, "steps", [])[-8:]:
        icon = "✓" if getattr(step, "ok", False) else "✗"
        lines.append(f"{icon} [{step.step}] {step.message}")
    return "\n".join(lines)


def _update_app(
    runtime: PersistentRuntime,
    *,
    service: object = None,
    ignore_check_failures: bool = False,
) -> str:
    runtime.checkpoint()
    try:
        result = _run_update_flow(
            web_port=_update_web_port(service),
            ignore_check_failures=ignore_check_failures,
            checkpoint_path=getattr(runtime, "checkpoint_path", None),
        )
    except Exception as exc:
        return f"Update failed before it could complete: {exc}"

    steps = _format_update_steps(result)
    from_version = getattr(result, "from_version", "") or "unknown"
    to_version = getattr(result, "to_version", "") or "unknown"
    if getattr(result, "success", False):
        if from_version == to_version:
            heading = f"Already up to date: {from_version}."
        else:
            heading = f"Updated successfully: {from_version} → {to_version}."
            if ignore_check_failures and any(
                getattr(step, "step", "") == "health" and not getattr(step, "ok", True)
                for step in getattr(result, "steps", [])
            ):
                heading = f"{heading} Health failures were ignored."
        restart_hint = "" if from_version == to_version else "\n\nSend /restart to restart Nulliøn on the new version."
        return f"{heading}\n\n{steps}{restart_hint}".strip()

    rollback = " Rolled back automatically." if getattr(result, "rolled_back", False) else ""
    error = (getattr(result, "error", "") or "Unknown update error.").rstrip(".")
    return f"Update failed: {error}.{rollback}\n\n{steps}".strip()


def _render_models(runtime: PersistentRuntime) -> str:
    cfg = current_runtime_config(model_client=getattr(runtime, "model_client", None))

    # Three-layer model resolution: session_pref > admin_forced > global
    stored_pref = runtime.store.get_user_memory_entry(_MODEL_PREFERENCE_KEY)
    session_override_raw = stored_pref.value if (stored_pref and stored_pref.value) else None
    session_provider = None
    session_override = session_override_raw
    if isinstance(session_override_raw, str) and "::" in session_override_raw:
        session_provider, _, session_override = session_override_raw.partition("::")
        session_provider = session_provider or None

    if session_override:
        effective_model = session_override
        effective_provider = session_provider or cfg.provider
        model_source = "session override"
    elif cfg.admin_forced_model:
        effective_model = cfg.admin_forced_model
        effective_provider = cfg.provider
        model_source = "admin default"
    else:
        effective_model = cfg.model
        effective_provider = cfg.provider
        model_source = "global config"

    lines = [
        "🤖 Nulliøn models",
        f"Provider: {effective_provider}",
        f"Active: {effective_model}  ({model_source})",
    ]
    options = chat_model_options(current_provider=effective_provider, current_model=effective_model)
    if options:
        lines.extend(["", "Chat model options:"])
        for index, option in enumerate(options, start=1):
            marker = " ← active" if option["provider"] == effective_provider and option["model"] == effective_model else ""
            lines.append(f"  {index}. {option['provider']} · {option['model']}{marker}")
    if cfg.admin_forced_model and not session_override:
        lines.append(f"Admin default: {cfg.admin_forced_model} — use /model <name> to override for this session")
    elif cfg.admin_forced_model and session_override:
        lines.append(f"Admin default: {cfg.admin_forced_model} — you are overriding it in this session")
    lines += [
        "",
        "Use /model <provider> <model> or /model <model> to switch models for this session.",
        "In web Settings, each provider can save multiple chat model options as a comma-separated list; the first entry is the default.",
    ]
    return "\n".join(lines)


def _credentials_path() -> Path:
    return Path.home() / ".nullion" / "credentials.json"


def _read_credentials() -> dict[str, object]:
    try:
        payload = json.loads(_credentials_path().read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_credentials(creds: dict[str, object]) -> None:
    path = _credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(creds, indent=2) + "\n", encoding="utf-8")


def _split_model_entries(value: object) -> list[str]:
    entries = str(value or "").replace("\n", ",").split(",")
    models: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        model = entry.strip()
        if model and model not in seen:
            models.append(model)
            seen.add(model)
    return models


def chat_model_options(*, current_provider: str | None = None, current_model: str | None = None) -> list[dict[str, str]]:
    creds = _read_credentials()
    provider_models_raw = creds.get("models")
    provider_models = provider_models_raw if isinstance(provider_models_raw, dict) else {}
    keys = creds.get("keys")
    keys = keys if isinstance(keys, dict) else {}
    providers_enabled = creds.get("providers_enabled")
    providers_enabled = providers_enabled if isinstance(providers_enabled, dict) else {}
    providers: list[str] = []
    for provider in [current_provider, creds.get("provider"), *provider_models.keys(), *keys.keys()]:
        provider_s = str(provider or "").strip()
        if provider_s and provider_s != "unknown" and provider_s not in providers:
            providers.append(provider_s)
    options: list[dict[str, str]] = []
    for provider in providers:
        if providers_enabled.get(provider) is False:
            continue
        configured = bool(keys.get(provider))
        if provider == str(creds.get("provider") or ""):
            configured = configured or bool(creds.get("api_key"))
        configured = configured or bool(os.environ.get("ANTHROPIC_API_KEY" if provider == "anthropic" else "OPENAI_API_KEY"))
        if provider == "codex":
            configured = configured or bool(creds.get("refresh_token") or os.environ.get("CODEX_REFRESH_TOKEN"))
        if not configured and provider != current_provider:
            continue
        models = _split_model_entries(provider_models.get(provider, ""))
        if provider == current_provider and current_model and current_model != "unknown" and current_model not in models:
            models.insert(0, current_model)
        for model in models:
            options.append({"provider": provider, "model": model})
    return options


def chat_model_option_for_token(token: str, *, current_provider: str | None = None, current_model: str | None = None) -> dict[str, str] | None:
    provider, sep, index_text = token.partition(".")
    if not sep or not index_text.isdigit():
        return None
    matches = [option for option in chat_model_options(current_provider=current_provider, current_model=current_model) if option["provider"] == provider]
    index = int(index_text)
    if index < 0 or index >= len(matches):
        return None
    return matches[index]


def _persist_provider_model(provider: str | None, model_name: str) -> None:
    if provider:
        creds = _read_credentials()
        creds["provider"] = provider
        creds["model"] = model_name
        models_map = creds.get("models")
        if not isinstance(models_map, dict):
            models_map = {}
        existing = _split_model_entries(models_map.get(provider, ""))
        if model_name not in existing:
            existing.insert(0, model_name)
        models_map[provider] = ",".join(existing)
        creds["models"] = models_map
        _write_credentials(creds)
        os.environ["NULLION_MODEL_PROVIDER"] = provider
        os.environ["NULLION_MODEL"] = model_name
    else:
        persist_model_name(model_name)


def _switch_model(runtime: PersistentRuntime, model_name: str, *, provider: str | None = None, service: object = None) -> str:
    """Persist the model preference and, when a service is provided, hot-swap it live."""
    import logging as _logging
    _log = _logging.getLogger(__name__)

    _persist_provider_model(provider, model_name)
    from nullion.memory import UserMemoryEntry, UserMemoryKind
    now = datetime.now(UTC)
    existing = runtime.store.get_user_memory_entry(_MODEL_PREFERENCE_KEY)
    created_at = now if existing is None else existing.created_at
    runtime.store.add_user_memory_entry(
        UserMemoryEntry(
            entry_id=_MODEL_PREFERENCE_KEY,
            owner="operator",
            kind=UserMemoryKind.PREFERENCE,
            key="model",
            value=f"{provider}::{model_name}" if provider else model_name,
            source="chat_operator",
            created_at=created_at,
            updated_at=now,
        )
    )
    runtime.checkpoint()

    # Note when the session is overriding an admin-forced default
    cfg = current_runtime_config(model_client=getattr(runtime, "model_client", None))
    override_note = (
        f" (overriding admin default: {cfg.admin_forced_model})"
        if cfg.admin_forced_model and cfg.admin_forced_model != model_name
        else ""
    )

    display_name = f"{provider} · {model_name}" if provider else model_name
    if provider and service is not None and hasattr(service, "swap_provider_model_client"):
        try:
            service.swap_provider_model_client(provider, model_name)
            return f"✓ Switched to {display_name}{override_note}. Live — no restart needed."
        except Exception as exc:
            _log.warning("Hot-swap provider/model client failed: %s", exc)
            return (
                f"Model preference saved as {display_name}{override_note}, "
                f"but live swap failed ({exc}). Restart to apply."
            )
    if service is not None and hasattr(service, "swap_model_client"):
        try:
            service.swap_model_client(model_name)
            return f"✓ Switched to {display_name}{override_note}. Live — no restart needed."
        except Exception as exc:
            _log.warning("Hot-swap model client failed: %s", exc)
            return (
                f"Model preference saved as {display_name}{override_note}, "
                f"but live swap failed ({exc}). Restart to apply."
            )

    return f"Model set to {display_name}{override_note}.\nRestart this process to apply it live."


# ── Auto-skill helpers ────────────────────────────────────────────────────────

def _cmd_auto_skill(runtime: PersistentRuntime, conv_id: str | None) -> str:
    """Run conversation analysis and return a formatted proposal list."""
    from nullion.chat_store import get_chat_store
    from nullion.conversation_analyzer import (
        analyze_conversation,
        analyze_all_recent,
        cache_proposals,
    )

    store = get_chat_store()
    model_client = getattr(runtime, "model_client", None)
    if model_client is None:
        return "No model client available — cannot analyse conversation."

    existing_titles = [s.title for s in runtime.list_skills()]

    if conv_id:
        proposals = analyze_conversation(
            store, model_client, conv_id,
            existing_skill_titles=existing_titles,
        )
    else:
        proposals = analyze_all_recent(
            store, model_client,
            max_conversations=3,
            existing_skill_titles=existing_titles,
        )

    # Cache under a stable key so /accept-skill can reference by number
    cache_key = conv_id or "__recent__"
    cache_proposals(cache_key, proposals)

    if not proposals:
        return (
            "No automatable patterns detected in recent conversations.\n"
            "Tip: have a few multi-step exchanges with Nullion, then try again."
        )

    lines = [f"🧠 Found {len(proposals)} skill proposal(s):\n"]
    for i, p in enumerate(proposals, 1):
        confidence_bar = "▓" * round(p.confidence * 10) + "░" * (10 - round(p.confidence * 10))
        lines.append(
            f"{i}. **{p.title}** [{confidence_bar} {p.confidence:.0%}]\n"
            f"   {p.summary}\n"
            f"   Trigger: \"{p.trigger}\"\n"
            f"   Steps: {len(p.steps)}\n"
            f"   Evidence: {p.evidence}\n"
        )
    lines.append(f"Accept a proposal with /accept-skill <1–{len(proposals)}>")
    return "\n".join(lines)


def _cmd_accept_skill(runtime: PersistentRuntime, index_str: str | None, conv_id: str | None) -> str:
    """Accept proposal N from the cached /auto-skill run."""
    from nullion.conversation_analyzer import get_cached_proposals

    if not index_str:
        return "Usage: /accept-skill <n>  (where n is the proposal number from /auto-skill)"
    try:
        index = int(index_str)
    except ValueError:
        return f"Invalid proposal number: {index_str!r}"

    cache_key = conv_id or "__recent__"
    proposals = get_cached_proposals(cache_key)
    if not proposals:
        return "No cached proposals. Run /auto-skill first."

    if index < 1 or index > len(proposals):
        return f"Proposal {index} is out of range (1–{len(proposals)})."

    proposal = proposals[index - 1]
    try:
        skill = runtime.create_skill(**proposal.to_skill_kwargs(), actor="auto-skill")
    except Exception as exc:
        return f"Failed to create skill: {exc}"

    return (
        f"✅ Skill created: **{skill.title}** (id: {skill.skill_id})\n"
        f"Summary: {skill.summary}\n"
        f"Trigger: \"{skill.trigger}\"\n"
        f"Steps ({len(skill.steps)}): {'; '.join(skill.steps[:3])}"
        + (" …" if len(skill.steps) > 3 else "")
    )


def _dispatch_operator_command(
    runtime: PersistentRuntime,
    text: str,
    *,
    now: datetime | None = None,
    service: object = None,
    memory_owner: str | None = None,
) -> str:
    command = text.strip()
    if not command:
        return _UNKNOWN_COMMAND

    parts = command.split()
    head = _normalize_command_head(parts[0])

    if head == "/help":
        # /help commands → full slash-command reference for power users
        if len(parts) > 1 and parts[1].strip().lower() == "commands":
            return _HELP_COMMANDS_TEXT
        return _HELP_TEXT

    if head == "/verbose":
        return _handle_verbose_command(parts)

    if head == "/thinking":
        return _handle_thinking_command(parts)

    if head == "/ping":
        return "Pong."

    if head == "/approvals":
        return _render_approvals(runtime, parts[1:])

    if head == "/approval":
        if len(parts) > 2:
            return "Usage: /approval <n|approval_id>"
        token = parts[1] if len(parts) > 1 else None
        return _render_approval(runtime, token)

    if head == "/approve":
        if len(parts) > 2:
            return "Usage: /approve <n|approval_id>"
        token = parts[1] if len(parts) > 1 else None
        return _approve_request(runtime, token)

    if head == "/deny":
        if len(parts) > 2:
            return "Usage: /deny <n|approval_id>"
        token = parts[1] if len(parts) > 1 else None
        return _deny_request(runtime, token)

    if head == "/grants":
        return _render_grants(runtime, parts[1:])

    if head == "/grant":
        if len(parts) > 2:
            return "Usage: /grant <n|grant_id>"
        token = parts[1] if len(parts) > 1 else None
        return _render_grant(runtime, token)

    if head == "/revoke-grant":
        if len(parts) > 2:
            return "Usage: /revoke-grant <n|grant_id>"
        token = parts[1] if len(parts) > 1 else None
        return _revoke_grant(runtime, token)

    if head == "/proposals":
        return _render_builder_proposals(runtime, parts[1:])

    if head == "/proposal":
        if len(parts) > 2:
            return "Usage: /proposal <n|proposal_id>"
        token = parts[1] if len(parts) > 1 else None
        return _render_builder_proposal(runtime, token)

    if head == "/accept-proposal":
        if len(parts) > 2:
            return "Usage: /accept-proposal <n|proposal_id>"
        token = parts[1] if len(parts) > 1 else None
        return _accept_builder_proposal(runtime, token)

    if head == "/reject-proposal":
        if len(parts) > 2:
            return "Usage: /reject-proposal <n|proposal_id>"
        token = parts[1] if len(parts) > 1 else None
        return _reject_builder_proposal(runtime, token)

    if head == "/archive-proposal":
        if len(parts) > 2:
            return "Usage: /archive-proposal <n|proposal_id>"
        token = parts[1] if len(parts) > 1 else None
        return _archive_builder_proposal(runtime, token)

    if head == "/skills":
        return _render_skills(runtime)

    if head == "/skill":
        if len(parts) > 2:
            return "Usage: /skill <n|skill_id>"
        token = parts[1] if len(parts) > 1 else None
        return _render_skill(runtime, token)

    if head == "/skill-history":
        if len(parts) > 2:
            return "Usage: /skill-history <n|skill_id>"
        token = parts[1] if len(parts) > 1 else None
        return _render_skill_history(runtime, token)

    if head == "/update-skill":
        return _update_skill(runtime, parts)

    if head == "/revert-skill":
        if len(parts) != 3:
            return "Usage: /revert-skill <n|skill_id> <revision>"
        return _revert_skill(runtime, parts[1], parts[2])

    if head == "/version":
        return _render_version(runtime)

    if head == "/health":
        return _render_health(runtime)

    if head == "/doctor":
        if len(parts) == 1:
            return _render_doctor(runtime)
        if len(parts) == 2 and parts[1].lower() in {"diagnose", "diagnosis", "check"}:
            return _run_doctor_diagnose(runtime)
        if len(parts) == 4 and parts[1].lower() == "run":
            return _doctor_run_cmd(runtime, parts[2], parts[3])
        if len(parts) == 2:
            return _render_doctor_action(runtime, parts[1])
        if len(parts) == 3 and parts[1].lower() in {"start", "complete", "dismiss", "cancel"}:
            return _doctor_action_cmd(runtime, parts[1].lower(), parts[2])
        return "Usage: /doctor [diagnose|n|start n|complete n|dismiss n]"

    if head == "/reminder":
        return _reminder_cmd(runtime, parts)

    if head == "/uptime":
        return _render_uptime(runtime, now=now)

    if head == "/status":
        if len(parts) > 2:
            return _EXTRA_STATUS_ARGS
        if len(parts) > 1:
            token = parts[1]
            normalized_token = _normalize_mention_suffix(token)
            if normalized_token == "active":
                return _render_active_status(runtime)
            if runtime.store.get_capsule(normalized_token) is None:
                return f"Capsule not found: {normalized_token}"
            return runtime.render_status_for_telegram(capsule_id=normalized_token, active_only=False)
        return _render_status(runtime)

    if head == "/deliveries":
        return _render_delivery_receipts(parts)

    if head == "/system-context":
        return _render_system_context(runtime)

    if head == "/codebase":
        return _render_codebase(runtime)

    if head == "/tools":
        return _render_tools(runtime)

    if head == "/plugins":
        return _render_plugins(runtime, parts[1:])

    if head == "/plugin":
        if len(parts) > 2:
            return "Usage: /plugin <plugin_id>"
        return _render_plugin_detail(parts[1] if len(parts) > 1 else None)

    if head == "/skill-packs":
        return _render_skill_packs(runtime, parts[1:])

    if head == "/skill-pack":
        if len(parts) > 2:
            return "Usage: /skill-pack <pack_id>"
        return _render_skill_pack_detail(parts[1] if len(parts) > 1 else None)

    if head == "/backups":
        return _render_backups(runtime)

    if head == "/restore":
        if len(parts) > 2:
            return _EXTRA_RESTORE_ARGS
        token = parts[1] if len(parts) > 1 else None
        return _render_restore(runtime, token)

    if head == "/update":
        allowed_flags = {"--force", "--ignore-checks", "force", "ignore-checks"}
        extra_args = parts[1:]
        if any(arg not in allowed_flags for arg in extra_args):
            return "Usage: /update [--ignore-checks|--force]"
        ignore_check_failures = any(arg in allowed_flags for arg in extra_args)
        return _update_app(runtime, service=service, ignore_check_failures=ignore_check_failures)

    if head == "/restart":
        return _restart_bot(runtime, service=service)

    if head == "/models":
        return _render_models(runtime)

    if head == "/memory":
        if len(parts) >= 2 and parts[1].strip().lower() in {"delete", "del", "remove", "clear"}:
            return _delete_memory(runtime, parts[2:], owner=memory_owner, now=now)
        return _render_memory(runtime, parts[1:], owner=memory_owner)

    if head == "/model":
        if len(parts) < 2:
            return "Usage: /model <provider> <model_name> or /model <model_name>"
        provider = None
        model_name = " ".join(parts[1:]).strip()
        known_providers = {option["provider"] for option in chat_model_options()} | {"anthropic", "openai", "codex", "openrouter", "openrouter-key"}
        if len(parts) >= 3 and parts[1] in known_providers:
            provider = parts[1]
            model_name = " ".join(parts[2:]).strip()
        normalized_model_name = _normalize_mention_suffix(model_name.split()[0]) if model_name else ""
        if not normalized_model_name:
            return "Usage: /model <provider> <model_name> or /model <model_name>"
        return _switch_model(runtime, normalized_model_name, provider=provider, service=service)

    # ── Auto-skill commands ────────────────────────────────────────────────────

    if head == "/auto-skill":
        # Optional conv_id argument: /auto-skill [conv_id]
        conv_id_arg = parts[1] if len(parts) > 1 else None
        return _cmd_auto_skill(runtime, conv_id_arg)

    if head == "/accept-skill":
        index_str = parts[1] if len(parts) > 1 else None
        # Accept from the most-recently analysed conversation (or __recent__)
        return _cmd_accept_skill(runtime, index_str, conv_id=None)

    return _UNKNOWN_COMMAND


class _OperatorCommandState(TypedDict, total=False):
    runtime: PersistentRuntime
    text: str
    now: datetime | None
    service: Any
    memory_owner: str | None
    command: str
    parts: list[str]
    head: str
    response: str


def _operator_command_parse_node(state: _OperatorCommandState) -> dict[str, object]:
    command = str(state.get("text") or "").strip()
    if not command:
        return {"command": "", "parts": [], "head": "", "response": _UNKNOWN_COMMAND}
    parts = command.split()
    return {
        "command": command,
        "parts": parts,
        "head": _normalize_command_head(parts[0]),
    }


def _operator_command_route_parsed(state: _OperatorCommandState) -> str:
    return END if state.get("response") is not None else "dispatch"


def _operator_command_dispatch_node(state: _OperatorCommandState) -> dict[str, object]:
    return {
        "response": _dispatch_operator_command(
            state["runtime"],
            str(state.get("command") or state.get("text") or ""),
            now=state.get("now"),
            service=state.get("service"),
            memory_owner=state.get("memory_owner"),
        )
    }


@lru_cache(maxsize=1)
def _compiled_operator_command_graph():
    graph = StateGraph(_OperatorCommandState)
    graph.add_node("parse", _operator_command_parse_node)
    graph.add_node("dispatch", _operator_command_dispatch_node)
    graph.add_edge(START, "parse")
    graph.add_conditional_edges("parse", _operator_command_route_parsed, {"dispatch": "dispatch", END: END})
    graph.add_edge("dispatch", END)
    return graph.compile()


def handle_operator_command(
    runtime: PersistentRuntime,
    text: str,
    *,
    now: datetime | None = None,
    service: object = None,
    memory_owner: str | None = None,
) -> str:
    command = str(text or "").strip()
    head = _normalize_command_head(command.split()[0]) if command else "empty"
    final_state = _compiled_operator_command_graph().invoke(
        {
            "runtime": runtime,
            "text": text,
            "now": now,
            "service": service,
            "memory_owner": memory_owner,
        },
        config={"configurable": {"thread_id": f"operator-command:{head}"}},
    )
    return str(final_state.get("response") or _UNKNOWN_COMMAND)


__all__ = [
    "handle_operator_command",
    "normalize_operator_command_head",
    "operator_command_catalog",
    "operator_command_suggestions",
    "telegram_bot_command_menu",
]

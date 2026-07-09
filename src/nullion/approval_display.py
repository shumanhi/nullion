"""Provider-neutral approval prompt display helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from pathlib import Path
from typing import Any

from nullion.agent_turn_limits import (
    AGENT_TURN_LIMIT_EXTENSION_TOOL_NAME,
    is_agent_turn_limit_extension_request,
)
from nullion.approvals import TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND


@dataclass(frozen=True, slots=True)
class ApprovalDisplay:
    label: str
    detail: str
    title: str
    copy: str
    is_web_request: bool = False


@dataclass(frozen=True, slots=True)
class ApprovalTurnDisplay:
    label: str
    detail: str
    trigger_flow_label: str | None
    is_web_request: bool = False


_TOOL_LABELS = {
    "allow_boundary": "access an external resource",
    "web_fetch": "fetch a web page",
    "web_search": "search the web",
    "run_shell": "run a shell command",
    "terminal_exec": "run a terminal command",
    "file_write": "write a file",
    "file_read": "read a file",
    "file_delete": "delete a file",
    "send_email": "send an email",
    "email_send": "send an email",
    "send_message": "send a message",
    "browser_open": "open a browser page",
    "browser_navigate": "navigate a browser page",
    "browser_click": "click on a webpage",
    "browser_click_element": "click on a webpage",
    "browser_click_id": "click on a webpage",
    "browser_type": "type into a webpage",
    "browser_type_field": "type into a webpage",
    "browser_type_id": "type into a webpage",
    "browser_select_combobox": "select an option on a webpage",
    "browser_fill": "fill in a form",
    "browser_run_js": "run JavaScript in the browser",
    "browser_screenshot": "capture a browser screenshot",
    "browser_snapshot": "inspect browser page elements",
    "browser_extract_items": "extract browser page item rows",
    "browser_extract_text": "read browser page text",
    "doctor_check": "run a health check",
    "create_task": "create a task",
    "use_tool": "use a tool",
    "use_computer": "control the computer",
    "execute_code": "execute code",
    "install_package": "install a package",
    "memory_write": "update memory",
    "calendar_create": "add a calendar event",
    "calendar_update": "update a calendar event",
    "calendar_delete": "delete a calendar event",
    "calendar_respond": "respond to a calendar invite",
}

_WEB_ACCESS_TOOLS = {"web_fetch", "web_search"}
_CALENDAR_APPROVAL_TITLES: dict[str, tuple[str, str]] = {
    "calendar_create": ("📅", "Add this calendar event?"),
    "calendar_update": ("📅", "Update this calendar event?"),
    "calendar_delete": ("📅", "Delete this calendar event?"),
    "calendar_respond": ("📅", "Respond to this calendar invite?"),
}
_CALENDAR_APPROVAL_COPY: dict[str, str] = {
    "calendar_create": "Review the event below. Approve once to add it to your calendar, or deny to stop.",
    "calendar_update": "Review the event changes below. Approve once to update your calendar, or deny to stop.",
    "calendar_delete": "Review the event below. Approve once to delete it from your calendar, or deny to stop.",
    "calendar_respond": "Review the calendar response below. Approve once to send it, or deny to stop.",
}


def approval_inline_code(value: object) -> str:
    text = _string(value).replace("`", "'")
    return f"`{text}`"


def _conversation_source_label(conversation_id: object) -> str:
    raw = _string(conversation_id)
    if not raw:
        return ""
    channel, separator, _identity = raw.partition(":")
    if not separator:
        return ""
    label = {
        "web": "Web",
        "telegram": "Telegram",
        "slack": "Slack",
        "discord": "Discord",
    }.get(channel.strip().lower())
    return f"Source: {label}" if label else ""


def _string(value: object) -> str:
    return str(value or "").strip()


def _context(approval: Any) -> dict[str, object]:
    context = getattr(approval, "context", None)
    return context if isinstance(context, dict) else {}


def _tool_name_for(approval: Any, context: dict[str, object]) -> str:
    context_tool = context.get("tool_name")
    if isinstance(context_tool, str) and context_tool.strip():
        return context_tool.strip()
    action = _string(getattr(approval, "action", ""))
    resource = _string(getattr(approval, "resource", ""))
    if action == "use_tool" and resource:
        return resource
    return action or "tool"


def approval_label_for_tool(tool_name: str) -> str:
    normalized = _string(tool_name)
    if normalized == AGENT_TURN_LIMIT_EXTENSION_TOOL_NAME:
        return "continue with a larger tool budget"
    return _TOOL_LABELS.get(normalized, f'use "{normalized}"' if normalized else "perform an action")


def _approval_title_parts(
    tool_name: str,
    *,
    boundary_kind: str = "",
    action: str = "",
    request_kind: str = "",
    side_effect: str = "",
) -> tuple[str, str]:
    normalized_tool = _string(tool_name).lower()
    raw = f"{normalized_tool} {boundary_kind} {action} {request_kind}".lower()
    if request_kind == TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND:
        return "⚠️", "Confirm delete?"
    if request_kind == "boundary_policy" or action == "allow_boundary":
        if boundary_kind == "outbound_network":
            return "🛡️", "Allow web access?"
        if boundary_kind == "filesystem_access":
            return "📄", "Allow file access?"
        if boundary_kind == "account_access":
            return "🔐", "Allow account access?"
        return "🛡️", "Allow external access?"
    if normalized_tool in _CALENDAR_APPROVAL_TITLES:
        return _CALENDAR_APPROVAL_TITLES[normalized_tool]
    if action == "use_tool":
        side = side_effect.lower()
        if "write" in side:
            return "⚠️", "Approve write action?"
        if "exec" in side:
            return "💻", "Run this command?"
    if normalized_tool == "browser_run_js":
        return "⚠️", "Approve write action?"
    if "doctor" in raw or "health" in raw:
        return "🩺", "Run health action?"
    if "builder" in raw or "build" in raw or "proposal" in raw or "skill" in raw:
        return "🛠️", "Run builder action?"
    if normalized_tool in _WEB_ACCESS_TOOLS or "web request" in raw or "web access" in raw:
        return "🛡️", "Allow web access?"
    if "security" in raw or "external" in raw or "boundary" in raw or "permission" in raw or "access" in raw:
        return "🛡️", "Allow external access?"
    if "shell" in raw or "terminal" in raw or "command" in raw:
        return "💻", "Run this command?"
    if "write" in raw or "delete" in raw or "file" in raw:
        return "📄", "Allow file access?"
    if "message" in raw or "email" in raw:
        return "✉️", "Send this message?"
    if "memory" in raw:
        return "🧠", "Update memory?"
    if "package" in raw or "install" in raw:
        return "📦", "Install package?"
    return "🧩", "Approve action?"


def approval_emoji_for(
    tool_name: str,
    *,
    boundary_kind: str = "",
    action: str = "",
    request_kind: str = "",
    side_effect: str = "",
) -> str:
    emoji, _title = _approval_title_parts(
        tool_name,
        boundary_kind=boundary_kind,
        action=action,
        request_kind=request_kind,
        side_effect=side_effect,
    )
    return emoji


def approval_title_for(tool_name: str, *, boundary_kind: str = "", action: str = "", request_kind: str = "", side_effect: str = "") -> str:
    emoji, title = _approval_title_parts(
        tool_name,
        boundary_kind=boundary_kind,
        action=action,
        request_kind=request_kind,
        side_effect=side_effect,
    )
    return f"{emoji} {title}"


def format_approval_detail_markdown(detail: str) -> str:
    text = _string(detail)
    if not text:
        return "Request details were not provided."
    if text.startswith(("Email draft:", "Calendar event:", "Destructive terminal action:")):
        return text
    for key in ("URL", "Path", "Target", "Command", "Query", "Resource", "Operation"):
        match = re.search(rf"(?P<prefix>.*?)(?: · )?{key}:\s*(?P<value>.+)$", text, flags=re.IGNORECASE)
        if match:
            prefix = match.group("prefix").strip()
            value = match.group("value").strip()
            label = key if key == "URL" else key.capitalize()
            if prefix:
                return f"{prefix} · {label}:\n{approval_inline_code(value)}"
            return f"{label}:\n{approval_inline_code(value)}"
    if text.startswith(("http://", "https://")):
        return f"Requested URL:\n{approval_inline_code(text)}"
    return f"Request:\n{approval_inline_code(text)}"


def _metadata_detail(context: dict[str, object]) -> str:
    description = _string(context.get("tool_description"))
    risk = _string(context.get("tool_risk_level"))
    side_effect = _string(context.get("tool_side_effect_class"))
    meta = []
    if risk:
        meta.append(f"{risk} risk")
    if side_effect:
        meta.append(f"{side_effect} access")
    if description and meta:
        return f"{description} {' · '.join(meta)}."
    return description


def _email_send_review_detail(context: dict[str, object]) -> str | None:
    arguments = context.get("tool_arguments")
    if not isinstance(arguments, dict):
        return None
    recipients = arguments.get("to") or arguments.get("recipients")
    if isinstance(recipients, (list, tuple)):
        to_text = ", ".join(_string(value) for value in recipients if _string(value))
    else:
        to_text = _string(recipients)
    subject = _string(arguments.get("subject"))
    body = _string(arguments.get("body"))
    html_body = _string(arguments.get("html_body") or arguments.get("html_path"))
    preview_path = _string(context.get("html_preview_path"))
    attachments = arguments.get("attachment_paths") or arguments.get("attachments")
    if isinstance(attachments, (list, tuple)):
        attachment_lines = [_string(value) for value in attachments if _string(value)]
    else:
        attachment_text = _string(attachments)
        attachment_lines = [attachment_text] if attachment_text else []
    lines = ["Email draft:"]
    if to_text:
        lines.append(f"> To: {to_text}")
    if subject:
        lines.append(f"> Subject: {subject}")
    if html_body:
        preview_name = Path(preview_path).name if preview_path else "HTML preview"
        lines.extend(["", f"> HTML preview: {preview_name}"])
        if body:
            lines.extend(["", "> Plain-text fallback:"])
            lines.extend(f"> {line}" if line else ">" for line in body.splitlines())
    elif body:
        lines.extend(["", "> Body:"])
        lines.extend(f"> {line}" if line else ">" for line in body.splitlines())
    if attachment_lines:
        lines.extend(["", "Attachments:"])
        lines.extend(f"- {path}" for path in attachment_lines)
    return "\n".join(lines)


def _format_calendar_datetime(value: object) -> str:
    text = _string(value)
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    date_text = f"{parsed:%b} {parsed.day}, {parsed.year}"
    has_explicit_time = "T" in text or " " in text
    if not has_explicit_time:
        return date_text
    time_text = parsed.strftime("%I:%M %p").lstrip("0")
    return f"{date_text}, {time_text}"


def _calendar_event_review_detail(context: dict[str, object]) -> str | None:
    arguments = context.get("tool_arguments")
    if not isinstance(arguments, dict):
        return None

    title = _string(arguments.get("summary") or arguments.get("title") or arguments.get("event_title"))
    start = _format_calendar_datetime(arguments.get("start") or arguments.get("start_time"))
    end = _format_calendar_datetime(arguments.get("end") or arguments.get("end_time"))
    time_zone = _string(arguments.get("time_zone") or arguments.get("timezone"))
    location = _string(arguments.get("location"))
    calendar_id = _string(arguments.get("calendar_id") or arguments.get("calendar"))
    description = _string(arguments.get("description"))

    lines = ["Calendar event:"]
    if title:
        lines.append(f"> Title: {title}")
    if start and end:
        when = f"{start} to {end}"
        if time_zone:
            when = f"{when} ({time_zone})"
        lines.append(f"> When: {when}")
    elif start:
        when = start
        if time_zone:
            when = f"{when} ({time_zone})"
        lines.append(f"> When: {when}")
    if location:
        lines.append(f"> Where: {location}")
    if calendar_id:
        lines.append(f"> Calendar: {calendar_id}")
    if description:
        lines.append("> Notes:")
        lines.extend(f"> {line}" if line else ">" for line in description.splitlines())

    return "\n".join(lines) if len(lines) > 1 else None


def _destructive_target_path(target: object) -> str:
    if isinstance(target, dict):
        value = target.get("target") or target.get("path")
        return _string(value)
    return _string(target)


def _terminal_destructive_review_detail(context: dict[str, object]) -> str:
    command = _string(context.get("command"))
    target_count = context.get("destructive_target_count")
    targets = context.get("destructive_targets")
    if not isinstance(targets, (list, tuple)):
        targets = context.get("destructive_preview_targets")
    target_lines = [_destructive_target_path(target) for target in targets] if isinstance(targets, (list, tuple)) else []
    target_lines = [line for line in target_lines if line]
    try:
        total = int(target_count)
    except (TypeError, ValueError):
        total = len(target_lines)
    if total <= 0:
        total = len(target_lines)

    lines = ["Destructive terminal action:"]
    if command:
        safe_command = command.replace("`", "'")
        lines.extend(["", "Command:", f"`{safe_command}`"])
    if total:
        lines.extend(["", f"About to delete {total} path{'s' if total != 1 else ''}:"])
    elif target_lines:
        lines.extend(["", "About to delete:"])
    preview_limit = 40
    for path in target_lines[:preview_limit]:
        lines.append(f"- {path}")
    remaining = total - min(total, len(target_lines[:preview_limit]))
    if remaining > 0:
        lines.append(f"- ...and {remaining} more.")
    if len(lines) == 1:
        return "Destructive terminal action:\nReview the terminal command before deleting anything."
    return "\n".join(lines)


def _approval_placeholder(value: str) -> bool:
    normalized = " ".join(_string(value).lower().replace("_", " ").replace("-", " ").split())
    return normalized in {
        "",
        "approval required",
        "needs approval",
        "request approval",
        "requires approval",
        "capability not granted",
        "capability denied",
    }


def _tool_result_detail(output: dict[str, object], *, approval_id: str | None, tool_name: str) -> str:
    for key in ("command", "cmd", "url", "target", "resource", "path", "query", "operation"):
        value = _string(output.get(key))
        if value and not _approval_placeholder(value):
            if key in {"command", "cmd"}:
                return f"Command: {value}"
            if key == "url" or (key == "target" and value.startswith(("http://", "https://"))):
                return value
            return f"{key.replace('_', ' ').title()}: {value}"
    arguments = output.get("arguments") or output.get("args")
    if isinstance(arguments, dict):
        for key in ("command", "cmd", "url", "path", "query"):
            value = _string(arguments.get(key))
            if value and not _approval_placeholder(value):
                if key in {"command", "cmd"}:
                    return f"Command: {value}"
                if key == "url":
                    return value
                return f"{key.title()}: {value}"
    reason = _string(output.get("reason"))
    if reason and not _approval_placeholder(reason):
        return reason
    if tool_name in {"terminal_exec", "run_shell", "execute_code"}:
        return "Command details were not provided by the runtime."
    return f"Request {approval_id[:8]}" if approval_id else approval_label_for_tool(tool_name)


def approval_display_from_request(approval: Any) -> ApprovalDisplay:
    if approval is None:
        return ApprovalDisplay(
            label="perform an action",
            detail="Approval request details are unavailable.",
            title="🧩 Approve action?",
            copy="Nullion paused before taking this step. Choose whether to allow this once, remember it, or stop here.",
        )
    context = _context(approval)
    action = _string(getattr(approval, "action", ""))
    resource = _string(getattr(approval, "resource", ""))
    request_kind = _string(getattr(approval, "request_kind", ""))
    boundary_kind = _string(context.get("boundary_kind")).lower()
    side_effect = _string(context.get("tool_side_effect_class"))
    tool_name = _tool_name_for(approval, context)
    if is_agent_turn_limit_extension_request(approval):
        current_limit = _string(context.get("current_max_iterations"))
        tool_count = _string(context.get("tool_result_count"))
        source_label = _conversation_source_label(context.get("conversation_id") or getattr(approval, "requested_by", ""))
        requested_extensions = context.get("requested_extensions")
        extension_text = ""
        if isinstance(requested_extensions, (list, tuple)):
            extensions = ", ".join(_string(ext) for ext in requested_extensions if _string(ext))
            if extensions:
                extension_text = f"\nRequested artifact type: {extensions}"
        detail_parts = []
        if source_label:
            detail_parts.append(source_label)
        if current_limit:
            detail_parts.append(f"Current turn budget: {current_limit} model/tool-loop iteration(s)")
        if tool_count:
            detail_parts.append(f"Tool steps already attempted: {tool_count}")
        detail = "\n".join(detail_parts).strip() or "The current agent turn budget was reached."
        if extension_text:
            detail = f"{detail}{extension_text}"
        return ApprovalDisplay(
            label=AGENT_TURN_LIMIT_EXTENSION_TOOL_NAME,
            detail=detail,
            title="🩺 Doctor recommends more tool budget",
            copy="Doctor saw this run hit its current limit before finishing. Choose how much more budget to grant for this same request.",
            is_web_request=source_label == "Source: Web",
        )
    label = approval_label_for_tool(tool_name)
    if tool_name == "email_send":
        detail = _email_send_review_detail(context) or _metadata_detail(context) or "Email draft details were not provided."
        return ApprovalDisplay(
            label=label,
            detail=detail,
            title="✉️ Review email before sending",
            copy="Review the email below. Approve once to send it, or deny to stop.",
            is_web_request=False,
        )
    if request_kind == TERMINAL_DESTRUCTIVE_ACTION_REQUEST_KIND:
        return ApprovalDisplay(
            label="delete files with terminal_exec",
            detail=_terminal_destructive_review_detail(context),
            title=approval_title_for(
                tool_name,
                boundary_kind=boundary_kind,
                action=action,
                request_kind=request_kind,
                side_effect=side_effect,
            ),
            copy=(
                "Nullion paused before deleting anything. Review exactly what will be deleted. "
                "Approve once to run this command, or deny to stop."
            ),
            is_web_request=False,
        )
    if tool_name in _CALENDAR_APPROVAL_TITLES:
        detail = _calendar_event_review_detail(context) or _metadata_detail(context) or "Calendar event details were not provided."
        return ApprovalDisplay(
            label=label,
            detail=detail,
            title=approval_title_for(
                tool_name,
                boundary_kind=boundary_kind,
                action=action,
                request_kind=request_kind,
                side_effect=side_effect,
            ),
            copy=_CALENDAR_APPROVAL_COPY.get(tool_name, "Review the calendar action below. Approve once to continue, or deny to stop."),
            is_web_request=False,
        )

    is_boundary = request_kind == "boundary_policy" or action == "allow_boundary"
    is_web = is_boundary and boundary_kind == "outbound_network"
    if is_boundary:
        target = _string(context.get("target") or context.get("path") or resource)
        operation = _string(context.get("operation"))
        if boundary_kind == "filesystem_access":
            verb = f"{operation.capitalize()} file" if operation else "File access"
            detail = f"{verb} request" + (f" · Path: {target}" if target else "")
        elif boundary_kind == "outbound_network":
            detail = "Web request" + (f" · URL: {target}" if target else "")
        elif boundary_kind == "account_access":
            detail = "Account request" + (f" · Target: {target}" if target else "")
        else:
            detail = "Boundary request" + (f" · Target: {target}" if target else "")
    else:
        detail = _metadata_detail(context) or _string(context.get("resource") or resource or getattr(approval, "reason", ""))
        is_web = tool_name in _WEB_ACCESS_TOOLS

    title = approval_title_for(
        tool_name,
        boundary_kind=boundary_kind,
        action=action,
        request_kind=request_kind,
        side_effect=side_effect,
    )
    copy = (
        "Nullion may need a few external sites to finish this request. Choose the web access scope to continue."
        if is_web
        else "Nullion paused before taking this step. Choose whether to allow this once, remember it, or stop here."
    )
    return ApprovalDisplay(label=label, detail=detail or label, title=title, copy=copy, is_web_request=is_web)


def approval_display_from_tool_result(tool_result: Any, *, approval_id: str | None = None) -> ApprovalDisplay:
    tool_name = _string(getattr(tool_result, "tool_name", "")) or "tool"
    output = getattr(tool_result, "output", None)
    output_dict = output if isinstance(output, dict) else {}
    detail = _tool_result_detail(output_dict, approval_id=approval_id, tool_name=tool_name)
    title = approval_title_for(tool_name)
    is_web = tool_name in _WEB_ACCESS_TOOLS or detail.startswith(("http://", "https://"))
    copy = (
        "Nullion may need a few external sites to finish this request. Choose the web access scope to continue."
        if is_web
        else "Nullion paused before taking this step. Choose whether to allow this once, remember it, or stop here."
    )
    return ApprovalDisplay(
        label=approval_label_for_tool(tool_name),
        detail=detail,
        title=title,
        copy=copy,
        is_web_request=is_web,
    )


def approval_turn_display_from_result(
    runtime: Any,
    result: Any,
    *,
    trigger_label_for_approval,
) -> ApprovalTurnDisplay:
    approval_id = getattr(result, "approval_id", None)
    store = getattr(runtime, "store", None)
    get_approval = getattr(store, "get_approval_request", None)
    approval = get_approval(approval_id) if callable(get_approval) and approval_id else None
    if approval is not None:
        display = approval_display_from_request(approval)
        return ApprovalTurnDisplay(
            label=display.label,
            detail=display.detail,
            trigger_flow_label=trigger_label_for_approval(approval),
            is_web_request=display.is_web_request,
        )
    tool_results = list(getattr(result, "tool_results", []) or [])
    if tool_results:
        display = approval_display_from_tool_result(tool_results[-1], approval_id=approval_id)
        return ApprovalTurnDisplay(
            label=display.label,
            detail=display.detail,
            trigger_flow_label=None,
            is_web_request=display.is_web_request,
        )
    display = approval_display_from_request(None)
    return ApprovalTurnDisplay(
        label=display.label,
        detail=display.detail,
        trigger_flow_label=None,
        is_web_request=display.is_web_request,
    )

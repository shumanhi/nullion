"""Provider-neutral approval prompt display helpers."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


@dataclass(frozen=True, slots=True)
class ApprovalDisplay:
    label: str
    detail: str
    title: str
    copy: str
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
    "send_message": "send a message",
    "browser_open": "open a browser page",
    "browser_navigate": "navigate a browser page",
    "browser_click": "click on a webpage",
    "browser_type": "type into a webpage",
    "browser_fill": "fill in a form",
    "browser_run_js": "run JavaScript in the browser",
    "browser_screenshot": "capture a browser screenshot",
    "browser_extract_text": "read browser page text",
    "doctor_check": "run a health check",
    "create_task": "create a task",
    "use_tool": "use a tool",
    "use_computer": "control the computer",
    "execute_code": "execute code",
    "install_package": "install a package",
    "memory_write": "update memory",
}

_WEB_ACCESS_TOOLS = {"web_fetch", "web_search"}


def approval_inline_code(value: object) -> str:
    text = _string(value).replace("`", "'")
    return f"`{text}`"


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
    if request_kind == "boundary_policy" or action == "allow_boundary":
        if boundary_kind == "outbound_network":
            return "🛡️", "Allow web access?"
        if boundary_kind == "filesystem_access":
            return "📄", "Allow file access?"
        if boundary_kind == "account_access":
            return "🔐", "Allow account access?"
        return "🛡️", "Allow external access?"
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
    label = approval_label_for_tool(tool_name)

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

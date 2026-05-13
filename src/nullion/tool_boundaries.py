"""Boundary extraction helpers for Nullion tool invocations."""

from __future__ import annotations

from ipaddress import ip_address
from pathlib import Path
import re
import shlex
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from nullion.policy import BoundaryFact, BoundaryKind

if TYPE_CHECKING:
    from nullion.tools import ToolInvocation

_NETWORK_COMMAND_FAMILIES = {
    "curl",
    "curl.exe",
    "wget",
    "wget.exe",
    "invoke-webrequest",
    "invoke-restmethod",
    "iwr",
    "irm",
    "start-bitstransfer",
}
_SHELL_COMMAND_FAMILIES = {
    "powershell",
    "powershell.exe",
    "pwsh",
    "pwsh.exe",
    "cmd",
    "cmd.exe",
    "sh",
    "bash",
    "zsh",
}
_HTTP_URL_RE = re.compile(r"https?://[^\s'\"<>()\[\]{}]+", re.I)
_WEB_SEARCH_TARGET = "https://www.bing.com/*"

# Account-scoped tools: maps tool_name → (operation, account_type)
_ACCOUNT_SCOPED_TOOLS: dict[str, tuple[str, str]] = {
    "email_send":      ("send",  "email"),
    "email_read":      ("read",  "email"),
    "calendar_write":  ("write", "calendar"),
    "calendar_read":   ("read",  "calendar"),
    "contacts_read":   ("read",  "contacts"),
    "contacts_write":  ("write", "contacts"),
}

# Any tool whose name starts with this prefix is treated as account-scoped
_PLUGIN_TOOL_PREFIX = "plugin:"


def _extract_http_url(command: str) -> str | None:
    for match in _HTTP_URL_RE.finditer(command):
        candidate = match.group(0).rstrip(".,;")
        parsed = urlparse(candidate)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return candidate
    return None


def _command_tokens(command: str) -> list[str]:
    try:
        return shlex.split(command, posix=False)
    except ValueError:
        return command.split()


def _terminal_network_command_family(command: str) -> str | None:
    # This inspects structured command syntax, not the user's free-form prompt.
    for token in _command_tokens(command):
        normalized = Path(token.strip("'\"")).name.lower()
        if normalized in _NETWORK_COMMAND_FAMILIES:
            return normalized
        if normalized in _SHELL_COMMAND_FAMILIES:
            continue
    return None


def _address_class_for_host(host: str) -> str:
    lowered = host.strip().lower()
    if not lowered:
        return "unknown"
    if lowered == "localhost":
        return "localhost"
    try:
        parsed = ip_address(lowered)
    except ValueError:
        return "public"
    if parsed.is_loopback:
        return "loopback"
    if parsed.is_link_local:
        return "link_local"
    if parsed.is_unspecified:
        return "loopback"
    if parsed.is_reserved:
        return "reserved"
    if parsed.is_private:
        return "private"
    return "public"


def _network_boundary_fact(*, tool_name: str, operation: str, target: str, command_family: str | None = None) -> BoundaryFact:
    parsed = urlparse(target)
    host = parsed.hostname or ""
    attributes = {
        "scheme": parsed.scheme,
        "host": host,
        "address_class": _address_class_for_host(host),
    }
    if command_family is not None:
        attributes["command_family"] = command_family
    return BoundaryFact(
        kind=BoundaryKind.OUTBOUND_NETWORK,
        tool_name=tool_name,
        operation=operation,
        target=target,
        attributes=attributes,
    )


def _account_boundary_fact(
    *,
    tool_name: str,
    operation: str,
    account_type: str,
    target: str = "",
) -> BoundaryFact:
    """Build a BoundaryFact for an account-scoped tool invocation."""
    resolved_target = target or f"{account_type}:{operation}"
    return BoundaryFact(
        kind=BoundaryKind.ACCOUNT_ACCESS,
        tool_name=tool_name,
        operation=operation,
        target=resolved_target,
        attributes={"account_type": account_type},
    )


def _email_send_account_target(invocation: "ToolInvocation") -> str:
    provider_id = str(invocation.arguments.get("provider_id") or "").strip()
    if not provider_id:
        try:
            from nullion.connections import default_email_connector_provider_id

            provider_id = default_email_connector_provider_id(invocation.principal_id) or ""
        except Exception:
            provider_id = ""
    return f"{provider_id}:send" if provider_id else "email:send"


def _filesystem_boundary_fact(*, tool_name: str, operation: str, raw_path: object) -> BoundaryFact | None:
    if not isinstance(raw_path, str) or not raw_path:
        return None
    path = str(Path(raw_path).expanduser())
    return BoundaryFact(
        kind=BoundaryKind.FILESYSTEM_ACCESS,
        tool_name=tool_name,
        operation=operation,
        target=path,
        attributes={"path": path},
    )


def extract_boundary_facts(invocation: ToolInvocation) -> list[BoundaryFact]:
    if invocation.tool_name == "terminal_exec":
        command = invocation.arguments.get("command")
        if not isinstance(command, str):
            return []
        stripped = command.strip()
        if not stripped:
            return []
        command_family = _terminal_network_command_family(stripped)
        if command_family is None:
            return []
        target = _extract_http_url(stripped)
        if target is None:
            return []
        return [
            _network_boundary_fact(
                tool_name=invocation.tool_name,
                operation="http_get",
                target=target,
                command_family=command_family,
            )
        ]

    if invocation.tool_name == "web_fetch":
        raw_url = invocation.arguments.get("url")
        if not isinstance(raw_url, str) or not raw_url:
            return []
        return [
            _network_boundary_fact(
                tool_name=invocation.tool_name,
                operation="http_get",
                target=raw_url,
            )
        ]

    if invocation.tool_name == "connector_request":
        facts = []
        method = str(invocation.arguments.get("method") or "GET").strip().upper() or "GET"
        account_operation = "read" if method in {"GET", "HEAD"} else "write"
        raw_url = invocation.arguments.get("url")
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        if not provider_id or not isinstance(raw_url, str) or not raw_url.strip():
            return []
        if isinstance(raw_url, str) and raw_url:
            facts.append(
                _network_boundary_fact(
                    tool_name=invocation.tool_name,
                    operation=f"http_{method.lower()}",
                    target=raw_url,
                )
            )
        facts.append(
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation=account_operation,
                account_type=provider_id,
                target=provider_id if account_operation == "read" else f"{provider_id}:{method.lower()}",
            )
        )
        return facts

    if invocation.tool_name == "web_search":
        return [
            _network_boundary_fact(
                tool_name=invocation.tool_name,
                operation="search",
                target=_WEB_SEARCH_TARGET,
            )
        ]

    if invocation.tool_name == "file_read":
        fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("path"),
        )
        return [] if fact is None else [fact]

    if invocation.tool_name == "file_write":
        fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="write",
            raw_path=invocation.arguments.get("path"),
        )
        return [] if fact is None else [fact]

    if invocation.tool_name in {"audio_transcribe", "image_extract_text"}:
        fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("path"),
        )
        return [] if fact is None else [fact]

    if invocation.tool_name == "image_generate":
        facts = []
        source_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="read",
            raw_path=invocation.arguments.get("source_path"),
        )
        if source_fact is not None:
            facts.append(source_fact)
        output_fact = _filesystem_boundary_fact(
            tool_name=invocation.tool_name,
            operation="write",
            raw_path=invocation.arguments.get("output_path"),
        )
        if output_fact is not None:
            facts.append(output_fact)
        return facts

    if invocation.tool_name == "email_send":
        facts = [
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation="send",
                account_type="email",
                target=_email_send_account_target(invocation),
            )
        ]
        raw_paths = invocation.arguments.get("attachment_paths")
        if isinstance(raw_paths, str):
            raw_paths = [raw_paths]
        if isinstance(raw_paths, (list, tuple)):
            for raw_path in raw_paths:
                fact = _filesystem_boundary_fact(
                    tool_name=invocation.tool_name,
                    operation="read",
                    raw_path=raw_path,
                )
                if fact is not None:
                    facts.append(fact)
        return facts

    # Named account-scoped tools (email, calendar, contacts)
    if invocation.tool_name in _ACCOUNT_SCOPED_TOOLS:
        operation, account_type = _ACCOUNT_SCOPED_TOOLS[invocation.tool_name]
        return [
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation=operation,
                account_type=account_type,
            )
        ]

    # Generic plugin tools are account-scoped by convention
    if invocation.tool_name.startswith(_PLUGIN_TOOL_PREFIX):
        plugin_name = invocation.tool_name[len(_PLUGIN_TOOL_PREFIX):]
        return [
            _account_boundary_fact(
                tool_name=invocation.tool_name,
                operation="invoke",
                account_type=plugin_name,
            )
        ]

    return []

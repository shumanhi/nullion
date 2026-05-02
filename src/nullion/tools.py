"""Typed tool registry for UI-neutral Nullion capabilities."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import Enum
from fnmatch import fnmatch
from html import unescape
from ipaddress import ip_address
import inspect
import json
import logging
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import ssl
import subprocess
import tempfile
import textwrap
import threading
from typing import Callable, Iterable, Protocol
import urllib.error
import urllib.request
from urllib.parse import urlencode, urlparse

from nullion.approval_context import FLOW_TRIGGER_CONTEXT_KEY, build_trigger_flow_context
from nullion.approvals import (
    ApprovalRequest,
    consume_boundary_permit as consume_boundary_permit_record,
    create_approval_request,
    is_boundary_permit_active,
    is_permission_grant_active,
)
from nullion.audit import make_audit_record
from nullion.events import make_event
from nullion.policy import (
    BoundaryFact,
    BoundaryKind,
    BoundaryPolicyRequest,
    BoundaryPolicyRule,
    PolicyDecision,
    PolicyMode,
    SentinelPolicy,
    GLOBAL_PERMISSION_PRINCIPAL,
    OPERATOR_PERMISSION_PRINCIPAL,
    evaluate_boundary_request,
    normalize_outbound_network_selector,
    permission_scope_principal,
)
from nullion.prompt_injection import scan_tool_output
from nullion.redaction import redact_value
from nullion.runtime_store import RuntimeStore
from nullion.tool_boundaries import extract_boundary_facts


_WEB_FETCH_MAX_REDIRECTS = 5
_LEGACY_GLOBAL_PERMISSION_PRINCIPALS = ("operator", "workspace:workspace_admin")
logger = logging.getLogger(__name__)


class _SafeWebFetchRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow redirects, but only to HTTP/HTTPS URLs (blocks data:, file:, etc).

    Also rejects redirects that resolve to private/loopback addresses to
    prevent SSRF via redirect chains (e.g. http://legit.com → http://169.254.x.x).
    """

    max_redirections = _WEB_FETCH_MAX_REDIRECTS

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urlparse(newurl)
        if parsed.scheme not in {"http", "https"}:
            # Refuse non-HTTP redirect targets (data:, file:, ftp:, etc.)
            return None
        # Refuse redirects to private/loopback hosts
        host = parsed.hostname or ""
        try:
            from ipaddress import ip_address as _ip
            addr = _ip(host)
            if not addr.is_global:
                return None
        except ValueError:
            # Hostname — allow it; DNS resolution happens later
            pass
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_WEB_FETCH_RESOLUTION_LOCK = threading.RLock()


@dataclass(frozen=True)
class _WebFetchResolution:
    host: str
    address_infos: tuple[tuple[int, int, int, str, tuple], ...]


@contextmanager
def _pinned_web_fetch_resolution(resolution: _WebFetchResolution | None):
    if resolution is None:
        yield
        return

    with _WEB_FETCH_RESOLUTION_LOCK:
        original_getaddrinfo = socket.getaddrinfo

        def pinned_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            if isinstance(host, str) and host.strip().lower() == resolution.host:
                adjusted: list[tuple[int, int, int, str, tuple]] = []
                for info_family, info_type, info_proto, info_canonname, sockaddr in resolution.address_infos:
                    if family not in {0, info_family}:
                        continue
                    if type not in {0, info_type}:
                        continue
                    if proto not in {0, info_proto}:
                        continue
                    if not isinstance(sockaddr, tuple) or not sockaddr:
                        continue
                    if info_family == socket.AF_INET and len(sockaddr) >= 2:
                        adjusted_sockaddr = (sockaddr[0], port or sockaddr[1])
                    elif info_family == socket.AF_INET6 and len(sockaddr) >= 4:
                        adjusted_sockaddr = (sockaddr[0], port or sockaddr[1], sockaddr[2], sockaddr[3])
                    else:
                        adjusted_sockaddr = sockaddr
                    adjusted.append((info_family, info_type, info_proto, info_canonname, adjusted_sockaddr))
                if adjusted:
                    return adjusted
            return original_getaddrinfo(host, port, family, type, proto, flags)

        socket.getaddrinfo = pinned_getaddrinfo
        try:
            yield
        finally:
            socket.getaddrinfo = original_getaddrinfo


def _resolve_web_fetch_resolution(raw_url: str) -> _WebFetchResolution | None:
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        return None
    host = parsed.hostname
    if not isinstance(host, str) or not host:
        return None
    lowered = host.strip().lower()
    if lowered == "localhost":
        return None
    try:
        parsed_host = ip_address(lowered)
    except ValueError:
        try:
            address_infos = socket.getaddrinfo(lowered, parsed.port or None, proto=socket.IPPROTO_TCP)
        except OSError:
            return None
        filtered_infos: list[tuple[int, int, int, str, tuple]] = []
        for family, socktype, proto, canonname, sockaddr in address_infos:
            if family not in {socket.AF_INET, socket.AF_INET6}:
                continue
            if not isinstance(sockaddr, tuple) or not sockaddr:
                continue
            candidate = sockaddr[0]
            if not isinstance(candidate, str):
                continue
            if not ip_address(candidate).is_global:
                return None
            filtered_infos.append((family, socktype, proto, canonname, sockaddr))
        if not filtered_infos:
            return None
        return _WebFetchResolution(host=lowered, address_infos=tuple(filtered_infos))
    if not parsed_host.is_global:
        return None
    return None


def _principal_workspace_file_roots(principal_id: str | None) -> tuple[Path, ...]:
    principal = str(principal_id or "").strip()
    if not (
        principal in {"telegram_chat", "operator"}
        or principal.startswith(("user:", "workspace:", "web:", "telegram:", "slack:", "discord:"))
    ):
        return ()
    try:
        from nullion.workspace_storage import workspace_file_roots_for_principal

        return tuple(Path(root).resolve() for root in workspace_file_roots_for_principal(principal))
    except Exception:
        return ()


_FILESYSTEM_PATH_ARGUMENTS_BY_TOOL = {
    "audio_transcribe": ("path",),
    "file_read": ("path",),
    "file_write": ("path",),
    "file_patch": ("path",),
    "image_extract_text": ("path",),
    "image_generate": ("source_path", "output_path"),
}


def _resolve_virtual_workspace_path(raw_path: str, *, principal_id: str | None) -> str:
    try:
        from nullion.workspace_storage import resolve_virtual_workspace_path_for_principal

        return str(resolve_virtual_workspace_path_for_principal(raw_path, principal_id))
    except Exception:
        return raw_path


def _with_resolved_virtual_workspace_paths(invocation: ToolInvocation) -> ToolInvocation:
    path_keys = _FILESYSTEM_PATH_ARGUMENTS_BY_TOOL.get(invocation.tool_name)
    if not path_keys:
        return invocation
    arguments = dict(invocation.arguments)
    changed = False
    for key in path_keys:
        value = arguments.get(key)
        if not isinstance(value, str) or not value:
            continue
        resolved_value = _resolve_virtual_workspace_path(value, principal_id=invocation.principal_id)
        if resolved_value == value:
            continue
        arguments[key] = resolved_value
        changed = True
    if not changed:
        return invocation
    return ToolInvocation(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        principal_id=invocation.principal_id,
        arguments=arguments,
        capsule_id=invocation.capsule_id,
        trusted_filesystem_selectors=invocation.trusted_filesystem_selectors,
        flow_context=invocation.flow_context,
    )


def _effective_filesystem_roots(
    *,
    invocation: ToolInvocation,
    resolved_root: Path | None,
    resolved_allowed_roots: tuple[Path, ...] | None,
    include_principal_workspace: bool,
) -> tuple[Path, ...]:
    roots: list[Path] = []
    if resolved_allowed_roots is not None:
        roots.extend(resolved_allowed_roots)
    elif resolved_root is not None:
        roots.append(resolved_root)
    if include_principal_workspace:
        roots.extend(_principal_workspace_file_roots(invocation.principal_id))
    return tuple(dict.fromkeys(root.resolve() for root in roots))


def _path_within_any_root(path: Path, roots: tuple[Path, ...]) -> bool:
    return any(_is_within_allowed_root(path, root) for root in roots)


def _build_web_fetch_opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _SafeWebFetchRedirectHandler,
    )



def _is_global_literal_ip(host: str) -> bool:
    try:
        return ip_address(host.strip().lower()).is_global
    except ValueError:
        return False



_NETWORK_MODE_NONE = "none"
_NETWORK_MODE_LOCALHOST_ONLY = "localhost_only"
_NETWORK_MODE_APPROVED_ONLY = "approved_only"
_NETWORK_MODE_FULL = "full"
_VALID_NETWORK_MODES = {
    _NETWORK_MODE_NONE,
    _NETWORK_MODE_LOCALHOST_ONLY,
    _NETWORK_MODE_APPROVED_ONLY,
    _NETWORK_MODE_FULL,
}
_RESTRICTIVE_NETWORK_MODES = {
    _NETWORK_MODE_NONE,
    _NETWORK_MODE_LOCALHOST_ONLY,
    _NETWORK_MODE_APPROVED_ONLY,
}
_REQUIRED_RESTRICTIVE_TERMINAL_BACKEND_ATTESTED_CAPABILITIES = ("network_policy_enforced",)
_NETWORK_MODE_SCOPED_ATTESTED_CAPABILITIES = {
    _NETWORK_MODE_NONE: ("network_policy_enforced.none",),
    _NETWORK_MODE_LOCALHOST_ONLY: ("network_policy_enforced.localhost_only",),
    _NETWORK_MODE_APPROVED_ONLY: (
        "network_policy_enforced.approved_only",
        "approved_only_enforced_via_local_allowlist_proxy",
    ),
}
_EXPECTED_LAUNCHER_SUPPORTED_NETWORK_MODES = (
    _NETWORK_MODE_NONE,
    _NETWORK_MODE_LOCALHOST_ONLY,
    _NETWORK_MODE_APPROVED_ONLY,
)
_LAUNCHER_DESCRIPTOR_SCHEMA = "nullion.launcher.descriptor.v1"
_LAUNCHER_DESCRIPTOR_SOURCE_PROBE = "launcher_probe"
_LAUNCHER_DESCRIPTOR_SOURCE_LEGACY = "launcher_legacy"
_ALLOWED_LAUNCHER_DESCRIPTOR_SOURCES = {
    _LAUNCHER_DESCRIPTOR_SOURCE_PROBE,
    _LAUNCHER_DESCRIPTOR_SOURCE_LEGACY,
}
_EXPECTED_LAUNCHER_ENFORCEMENT_BY_MODE = {
    _NETWORK_MODE_NONE: "sandbox-exec deny network*",
    _NETWORK_MODE_LOCALHOST_ONLY: "sandbox-exec remote ip localhost:* only",
    _NETWORK_MODE_APPROVED_ONLY: "sandbox-exec localhost proxy only + in-process allowlist proxy",
}

_FILESYSTEM_BOUNDARY_DEFAULT = "default"
_FILESYSTEM_BOUNDARY_TRUSTED_ROOTS_ONLY = "trusted_roots_only"


def _required_attested_capabilities_for_network_mode(network_mode: str | None) -> tuple[str, ...]:
    if not _is_restrictive_network_mode(network_mode):
        return ()
    base = list(_REQUIRED_RESTRICTIVE_TERMINAL_BACKEND_ATTESTED_CAPABILITIES)
    for capability in _NETWORK_MODE_SCOPED_ATTESTED_CAPABILITIES.get(network_mode, ()):  # pragma: no branch
        if capability not in base:
            base.append(capability)
    return tuple(base)


def _serialize_boundary_fact(fact) -> dict[str, object]:
    return {
        "kind": fact.kind.value if hasattr(fact.kind, "value") else str(fact.kind),
        "target": fact.target,
        "host": fact.attributes.get("host"),
        "scheme": fact.attributes.get("scheme"),
        "address_class": fact.attributes.get("address_class"),
        "command_family": fact.attributes.get("command_family"),
    }


def _egress_attempts_for_invocation(invocation: ToolInvocation) -> list[dict[str, object]]:
    return [_serialize_boundary_fact(fact) for fact in extract_boundary_facts(invocation)]


def _selector_candidates_for_boundary_target(target: str) -> dict[str, str]:
    parsed = urlparse(target)
    if parsed.scheme and parsed.hostname:
        domain_selector = normalize_outbound_network_selector(target)
        return {
            "allow_once": domain_selector,
            "always_allow": domain_selector,
        }
    return {
        "allow_once": target,
        "always_allow": target,
    }


def _outbound_network_approval_context_from_result(result: ToolResult) -> dict[str, object] | None:
    if result.tool_name != "terminal_exec":
        return None
    output = result.output if isinstance(result.output, dict) else {}
    if output.get("reason") != "network_denied":
        return None
    if output.get("network_mode") not in {None, _NETWORK_MODE_NONE, _NETWORK_MODE_APPROVED_ONLY}:
        return None
    attempts = output.get("egress_attempts")
    if not isinstance(attempts, list):
        return None
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        if attempt.get("kind") != "outbound_network":
            continue
        target = attempt.get("target")
        if not isinstance(target, str) or not target:
            continue
        if attempt.get("address_class") != "public":
            continue
        return {
            "tool_name": "terminal_exec",
            "boundary_kind": "outbound_network",
            "target": target,
            "selector_candidates": _selector_candidates_for_boundary_target(target),
        }
    return None


def _boundary_approval_context_from_fact(fact) -> dict[str, object] | None:
    if fact.kind is BoundaryKind.OUTBOUND_NETWORK:
        if fact.attributes.get("address_class") != "public":
            return None
        return {
            "tool_name": fact.tool_name,
            "boundary_kind": fact.kind.value,
            "target": fact.target,
            "selector_candidates": _selector_candidates_for_boundary_target(fact.target),
        }
    if fact.kind is BoundaryKind.FILESYSTEM_ACCESS:
        return {
            "tool_name": fact.tool_name,
            "boundary_kind": fact.kind.value,
            "operation": fact.operation,
            "path": fact.attributes.get("path") or fact.target,
            "target": fact.target,
            "selector_candidates": {
                "allow_once": fact.target,
                "always_allow": fact.target,
            },
        }
    if fact.kind is BoundaryKind.ACCOUNT_ACCESS:
        account_type = fact.attributes.get("account_type", "unknown")
        return {
            "tool_name": fact.tool_name,
            "boundary_kind": fact.kind.value,
            "operation": fact.operation,
            "target": fact.target,
            "account_type": account_type,
            "selector_candidates": {
                "allow_once": fact.target,
                "always_allow": f"{account_type}:*",
            },
        }
    return None


def _selector_matches_target(*, selector: str, target: str) -> bool:
    parsed_target = urlparse(target)
    if parsed_target.scheme in {"http", "https"} and parsed_target.hostname:
        if normalize_outbound_network_selector(selector) == normalize_outbound_network_selector(target):
            return True
    if selector == target:
        return True
    if fnmatch(target, selector):
        return True
    if selector.endswith("/*") and target == selector[:-2]:
        return True
    if _selector_matches_www_family(selector=selector, target=target):
        return True
    return False


def _boundary_approval_match_key(*, context: dict[str, object], fallback_target: str) -> tuple[str, str] | None:
    boundary_kind = context.get("boundary_kind")
    if not isinstance(boundary_kind, str) or not boundary_kind:
        return None
    selectors = context.get("selector_candidates") if isinstance(context.get("selector_candidates"), dict) else {}
    candidates: list[object] = [
        selectors.get("always_allow") if isinstance(selectors, dict) else None,
        selectors.get("allow_once") if isinstance(selectors, dict) else None,
        context.get("target"),
        fallback_target,
    ]
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate:
            continue
        if boundary_kind == BoundaryKind.OUTBOUND_NETWORK.value:
            return (boundary_kind, normalize_outbound_network_selector(candidate))
        return (boundary_kind, candidate)
    return None


def _refresh_pending_approval_request(approval: ApprovalRequest, *, context: dict[str, object], resource: str) -> ApprovalRequest:
    merged_context = dict(approval.context or {})
    merged_context.update(context)
    return replace(
        approval,
        resource=resource,
        created_at=datetime.now(UTC),
        context=merged_context,
    )


def _selector_matches_www_family(*, selector: str, target: str) -> bool:
    selector_base_url = selector[:-2] if selector.endswith("/*") else selector
    parsed_selector = urlparse(selector_base_url)
    parsed_target = urlparse(target)
    if parsed_selector.scheme != parsed_target.scheme:
        return False
    selector_host = (parsed_selector.hostname or "").lower()
    target_host = (parsed_target.hostname or "").lower()
    if not selector_host or not target_host:
        return False
    selector_root = selector_host[4:] if selector_host.startswith("www.") else selector_host
    target_root = target_host[4:] if target_host.startswith("www.") else target_host
    if selector_root != target_root:
        return False
    if selector_host not in {selector_root, f"www.{selector_root}"}:
        return False
    if target_host not in {target_root, f"www.{target_root}"}:
        return False
    if selector.endswith("/*"):
        return True
    return parsed_selector.path.rstrip("/") == parsed_target.path.rstrip("/")


def _network_is_approved(*, target: str, approved_targets: object) -> bool:
    if not isinstance(approved_targets, (list, tuple, set, frozenset)):
        return False
    return any(isinstance(pattern, str) and _selector_matches_target(selector=pattern, target=target) for pattern in approved_targets)



def _normalize_network_mode(network_mode: object) -> str | None:
    if not isinstance(network_mode, str):
        return None
    if not network_mode:
        return None
    if network_mode in _VALID_NETWORK_MODES:
        return network_mode
    return None



def _network_attempt_allowed(*, attempt: dict[str, object], network_mode: str | None, approved_targets: object) -> bool:
    if network_mode in {None, _NETWORK_MODE_FULL}:
        return True
    if network_mode == _NETWORK_MODE_NONE:
        return False
    if network_mode == _NETWORK_MODE_LOCALHOST_ONLY:
        return attempt.get("address_class") in {"localhost", "loopback"}
    if network_mode == _NETWORK_MODE_APPROVED_ONLY:
        target = attempt.get("target")
        return isinstance(target, str) and _network_is_approved(target=target, approved_targets=approved_targets)
    return True



def _is_restrictive_network_mode(network_mode: str | None) -> bool:
    return network_mode in _RESTRICTIVE_NETWORK_MODES



def _missing_backend_attested_capabilities(
    descriptor: TerminalBackendDescriptor,
    *,
    required_capabilities: Iterable[str],
) -> tuple[str, ...]:
    return tuple(
        capability
        for capability in required_capabilities
        if capability not in descriptor.attested_capabilities
    )



def _requires_probe_derived_launcher_attestation(required_capabilities: Iterable[str]) -> bool:
    return any(isinstance(capability, str) and capability.startswith("network_policy_enforced") for capability in required_capabilities)



def _launcher_command_requires_probe_attestation(command: object) -> bool:
    if not isinstance(command, str):
        return False
    normalized_command = command.strip()
    if not normalized_command:
        return False
    return Path(normalized_command).parent != Path()



def _verify_launcher_v2_evidence(
    descriptor: TerminalBackendDescriptor,
    *,
    required_capabilities: Iterable[str],
) -> TerminalAttestationVerificationResult | None:
    evidence = descriptor.evidence
    if evidence is None or evidence.format != "nullion.launcher.v2":
        return None
    if descriptor.mode != "launcher":
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence requires launcher backend mode",
            metadata={"descriptor_mode": descriptor.mode},
        )
    payload = evidence.payload if isinstance(evidence.payload, dict) else {}
    descriptor_source = descriptor.metadata.get("descriptor_source")
    evidence_source = evidence.metadata.get("descriptor_source")
    if (
        descriptor_source not in _ALLOWED_LAUNCHER_DESCRIPTOR_SOURCES
        or evidence_source not in _ALLOWED_LAUNCHER_DESCRIPTOR_SOURCES
        or descriptor_source != evidence_source
    ):
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence provenance metadata mismatch",
            metadata={
                "descriptor_source": descriptor_source,
                "evidence_source": evidence_source,
            },
        )
    if (
        _requires_probe_derived_launcher_attestation(required_capabilities)
        and descriptor_source != _LAUNCHER_DESCRIPTOR_SOURCE_PROBE
        and _launcher_command_requires_probe_attestation(
            descriptor.metadata.get("launcher_command") or payload.get("launcher_command")
        )
    ):
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="restrictive launcher modes require probe-derived attestation",
            metadata={
                "descriptor_source": descriptor_source,
                "launcher_command": descriptor.metadata.get("launcher_command") or payload.get("launcher_command"),
            },
        )
    descriptor_schema = descriptor.metadata.get("descriptor_schema")
    evidence_schema = evidence.metadata.get("descriptor_schema")
    payload_schema = payload.get("schema")
    if (
        descriptor_schema != _LAUNCHER_DESCRIPTOR_SCHEMA
        or evidence_schema != _LAUNCHER_DESCRIPTOR_SCHEMA
        or payload_schema != _LAUNCHER_DESCRIPTOR_SCHEMA
    ):
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence schema metadata mismatch",
            metadata={
                "descriptor_schema": descriptor_schema,
                "evidence_schema": evidence_schema,
                "payload_schema": payload_schema,
            },
        )
    payload_mode = payload.get("mode")
    if payload_mode != "launcher":
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence payload mode mismatch",
            metadata={"payload_mode": payload_mode},
        )
    supported_network_modes = payload.get("supported_network_modes")
    if tuple(supported_network_modes) != _EXPECTED_LAUNCHER_SUPPORTED_NETWORK_MODES:
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence supported_network_modes mismatch",
            metadata={"supported_network_modes": supported_network_modes},
        )
    if payload.get("requires_sandbox_exec_for_restrictive_modes") is not True:
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence must require sandbox-exec for restrictive modes",
            metadata={"requires_sandbox_exec_for_restrictive_modes": payload.get("requires_sandbox_exec_for_restrictive_modes")},
        )
    if payload.get("proxy_bind_scope") != "127.0.0.1":
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence must bind approved_only proxy to 127.0.0.1",
            metadata={"proxy_bind_scope": payload.get("proxy_bind_scope")},
        )
    enforcement_by_mode = payload.get("enforcement_by_mode")
    if not isinstance(enforcement_by_mode, dict):
        return TerminalAttestationVerificationResult(
            is_valid=False,
            failure_reason="launcher evidence enforcement_by_mode missing",
            metadata={"enforcement_by_mode": enforcement_by_mode},
        )
    for mode, expected in _EXPECTED_LAUNCHER_ENFORCEMENT_BY_MODE.items():
        if enforcement_by_mode.get(mode) != expected:
            failure_reason = (
                "approved_only evidence must declare proxy-backed allowlist enforcement"
                if mode == _NETWORK_MODE_APPROVED_ONLY
                else f"launcher evidence enforcement mismatch for {mode}"
            )
            return TerminalAttestationVerificationResult(
                is_valid=False,
                failure_reason=failure_reason,
                metadata={"mode": mode, "enforcement": enforcement_by_mode.get(mode)},
            )
    return TerminalAttestationVerificationResult(is_valid=True)


class _DefaultTerminalAttestationVerifier:
    def verify(
        self,
        descriptor: TerminalBackendDescriptor,
        *,
        required_capabilities: Iterable[str],
    ) -> TerminalAttestationVerificationResult:
        missing_capabilities = _missing_backend_attested_capabilities(
            descriptor,
            required_capabilities=required_capabilities,
        )
        if missing_capabilities:
            return TerminalAttestationVerificationResult(
                is_valid=False,
                failure_reason="missing required attested capabilities: " + ", ".join(missing_capabilities),
                metadata={"missing_capabilities": list(missing_capabilities)},
            )
        launcher_v2_verification = _verify_launcher_v2_evidence(
            descriptor,
            required_capabilities=required_capabilities,
        )
        if launcher_v2_verification is not None:
            return launcher_v2_verification
        return TerminalAttestationVerificationResult(is_valid=True)



def verify_terminal_backend_attestation(
    descriptor: TerminalBackendDescriptor,
    *,
    required_capabilities: Iterable[str],
    verifier: TerminalAttestationVerifier | None = None,
) -> TerminalAttestationVerificationResult:
    active_verifier = verifier or _DefaultTerminalAttestationVerifier()
    return active_verifier.verify(descriptor, required_capabilities=required_capabilities)



_CANONICAL_TOOL_STATUSES = {
    "completed",
    "failed",
    "denied",
    "nonterminal",
    "unknown",
}
_COMPLETED_TOOL_STATUS_ALIASES = {"completed", "complete", "success", "succeeded", "ok", "done"}
_FAILED_TOOL_STATUS_ALIASES = {"failed", "failure", "error", "errored", "timeout", "timed_out", "partial"}
_DENIED_TOOL_STATUS_ALIASES = {"denied", "blocked", "approval_required", "capability_denied", "capability_not_granted"}
_NONTERMINAL_TOOL_STATUS_ALIASES = {"running", "pending", "started", "in_progress", "queued"}

class ToolRiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


_TOOL_RISK_SCORES = {
    ToolRiskLevel.LOW: 2,
    ToolRiskLevel.MEDIUM: 5,
    ToolRiskLevel.HIGH: 8,
}


class ToolSideEffectClass(str, Enum):
    READ = "read"
    WRITE = "write"
    EXTERNAL_WRITE = "external_write"
    ACCOUNT_WRITE = "account_write"
    DANGEROUS_EXEC = "dangerous_exec"


@dataclass(slots=True)
class ToolSpec:
    name: str
    description: str
    risk_level: ToolRiskLevel
    side_effect_class: ToolSideEffectClass
    requires_approval: bool
    timeout_seconds: int
    filesystem_boundary_policy: str = _FILESYSTEM_BOUNDARY_DEFAULT
    permission_scope: str = "global"
    input_schema: dict[str, object] | None = None


@dataclass(slots=True)
class ToolInvocation:
    invocation_id: str
    tool_name: str
    principal_id: str
    arguments: dict[str, object]
    capsule_id: str | None = None
    trusted_filesystem_selectors: tuple[str, ...] = ()
    flow_context: dict[str, object] | None = None


@dataclass(slots=True)
class ToolResult:
    invocation_id: str
    tool_name: str
    status: str
    output: dict[str, object]
    error: str | None = None


ToolHandler = Callable[[ToolInvocation], ToolResult]
ToolCleanupHook = Callable[[str | None], None]


def _default_input_schema_for_tool(tool_name: str) -> dict[str, object]:
    schemas: dict[str, dict[str, object]] = {
        "file_read": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "Absolute or workspace-relative file path to read."}},
            "required": ["path"],
            "additionalProperties": False,
        },
        "file_write": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Absolute or workspace-relative file path to write."},
                "content": {"type": "string", "description": "Text content to write."},
            },
            "required": ["path", "content"],
            "additionalProperties": False,
        },
        "pdf_create": {
            "type": "object",
            "properties": {
                "output_path": {
                    "type": "string",
                    "description": "Destination .pdf path. If omitted, Nullion creates one in the artifact directory.",
                },
                "image_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Existing image artifact paths to place into the PDF, one image per page.",
                },
                "text_pages": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional plain text pages to render into the PDF.",
                },
                "title": {"type": "string", "description": "Optional title used for metadata and default filename."},
                "page_size": {
                    "type": "string",
                    "enum": ["letter", "a4"],
                    "description": "Optional page size. Defaults to letter.",
                },
            },
            "additionalProperties": False,
        },
        "pdf_edit": {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "description": "Existing PDF path to edit."},
                "output_path": {
                    "type": "string",
                    "description": "Destination .pdf path. If omitted, Nullion creates one in the artifact directory.",
                },
                "page_numbers": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Optional 1-based page numbers to keep or reorder.",
                },
                "rotate_degrees": {
                    "type": "integer",
                    "enum": [0, 90, 180, 270],
                    "description": "Optional clockwise rotation applied to kept pages.",
                },
                "append_pdf_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional existing PDFs to append after the kept input pages.",
                },
                "append_image_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional image paths to append as new PDF pages.",
                },
                "append_text_pages": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional plain text pages to append.",
                },
                "title": {"type": "string", "description": "Optional title used for metadata and default filename."},
                "page_size": {
                    "type": "string",
                    "enum": ["letter", "a4"],
                    "description": "Optional page size for appended image/text pages. Defaults to letter.",
                },
            },
            "required": ["input_path"],
            "additionalProperties": False,
        },
        "terminal_exec": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "Shell command to execute."},
                "network_mode": {
                    "type": "string",
                    "enum": sorted(_VALID_NETWORK_MODES),
                    "description": "Optional network policy for the command.",
                },
            },
            "required": ["command"],
            "additionalProperties": False,
        },
        "web_fetch": {
            "type": "object",
            "properties": {"url": {"type": "string", "description": "HTTP or HTTPS URL to fetch."}},
            "required": ["url"],
            "additionalProperties": False,
        },
        "connector_request": {
            "type": "object",
            "properties": {
                "provider_id": {
                    "type": "string",
                    "description": "Workspace connection provider id.",
                },
                "url": {
                    "type": "string",
                    "description": "Full HTTP(S) gateway URL from the installed connector skill.",
                },
                "params": {
                    "type": "object",
                    "description": "Optional query parameters for the connector request.",
                },
                "method": {
                    "type": "string",
                    "enum": ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"],
                    "description": "HTTP method. Write methods require the connection permission mode to be read_write.",
                },
                "json": {
                    "type": "object",
                    "description": "Optional JSON object body for POST, PUT, PATCH, or DELETE requests.",
                },
                "body": {
                    "type": "string",
                    "description": "Optional raw request body for POST, PUT, PATCH, or DELETE requests.",
                },
            },
            "required": ["provider_id", "url"],
            "additionalProperties": False,
        },
        "skill_pack_read": {
            "type": "object",
            "properties": {
                "pack_id": {"type": "string", "description": "Installed skill pack id, such as owner/pack."},
                "path": {
                    "type": "string",
                    "description": "Relative reference file path shown in the enabled skill pack prompt.",
                },
            },
            "required": ["pack_id", "path"],
            "additionalProperties": False,
        },
        "web_search": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query."},
                "limit": {"type": "integer", "minimum": 1, "description": "Maximum number of results to return."},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "file_search": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Case-insensitive text or filename fragment to search for."},
                "limit": {"type": "integer", "minimum": 1, "description": "Maximum number of matches to return."},
            },
            "required": ["pattern"],
            "additionalProperties": False,
        },
        "file_patch": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Absolute or workspace-relative file path to edit."},
                "old_string": {"type": "string", "description": "Exact text to replace."},
                "new_string": {"type": "string", "description": "Replacement text."},
            },
            "required": ["path", "old_string", "new_string"],
            "additionalProperties": False,
        },
        "workspace_summary": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
        "email_search": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Email search query."},
                "limit": {"type": "integer", "minimum": 1, "description": "Maximum number of messages to return."},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "email_read": {
            "type": "object",
            "properties": {"id": {"type": "string", "description": "Email message id returned by email_search."}},
            "required": ["id"],
            "additionalProperties": False,
        },
        "calendar_list": {
            "type": "object",
            "properties": {
                "start": {"type": "string", "description": "Inclusive ISO-8601 start datetime."},
                "end": {"type": "string", "description": "Exclusive ISO-8601 end datetime."},
                "max": {"type": "integer", "minimum": 1, "description": "Maximum number of events to return."},
            },
            "required": ["start", "end"],
            "additionalProperties": False,
        },
        "browser_navigate": {
            "type": "object",
            "properties": {"url": {"type": "string", "description": "HTTP or HTTPS URL to open in the browser."}},
            "required": ["url"],
            "additionalProperties": False,
        },
        "audio_transcribe": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the audio file."},
                "language": {"type": "string", "description": "Optional language code."},
            },
            "required": ["path"],
            "additionalProperties": False,
        },
        "image_extract_text": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "Path to the image file."}},
            "required": ["path"],
            "additionalProperties": False,
        },
        "image_generate": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "Image generation or edit instruction."},
                "output_path": {"type": "string", "description": "Destination path for the generated image file."},
                "size": {"type": "string", "description": "Optional output size, such as 1024x1024."},
                "source_path": {"type": "string", "description": "Optional source image path for image edits."},
            },
            "required": ["prompt", "output_path"],
            "additionalProperties": False,
        },
    }
    return schemas.get(
        tool_name,
        {
            "type": "object",
            "properties": {},
            "additionalProperties": True,
        },
    )


@dataclass(frozen=True, slots=True)
class TerminalExecutionPolicy:
    network_mode: str | None
    approved_targets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TerminalExecutionResult:
    exit_code: int
    stdout: str
    stderr: str


_FIND_PATH_OPTION_ARITY = {
    "-H": 0,
    "-L": 0,
    "-P": 0,
    "-O0": 0,
    "-O1": 0,
    "-O2": 0,
    "-O3": 0,
    "-D": 1,
}
_FIND_EXPRESSION_STARTERS = {
    "(",
    "!",
    "-",
}


def _terminal_filesystem_safety_denial(
    command: str,
    *,
    allowed_roots: Iterable[Path] = (),
) -> dict[str, object] | None:
    """Reject shell commands likely to trigger broad local data traversal."""
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        tokens = shlex.split(command, posix=True)
    except Exception:
        return None
    if not tokens:
        return None

    home = str(Path.home())
    resolved_allowed_roots = tuple(Path(root).expanduser().resolve() for root in allowed_roots)
    home_variants = {home, "$HOME", "${HOME}", "~"}
    broad_root_variants = {"/", "/Users"}
    protected_home_roots = {
        str(Path(home) / "Library"),
        str(Path(home) / "Library" / "Application Support"),
        str(Path(home) / "Library" / "Containers"),
        str(Path(home) / "Library" / "Group Containers"),
    }

    for index, token in enumerate(tokens):
        if token != "find":
            continue
        path_tokens = _find_path_tokens(tokens[index + 1 :])
        if not path_tokens:
            path_tokens = ["."]
        maxdepth = _find_maxdepth(tokens[index + 1 :])
        for raw_path in path_tokens:
            normalized_path = _normalize_find_root(raw_path, home=home)
            if normalized_path in broad_root_variants:
                return {
                    "reason": "filesystem_traversal_denied",
                    "command_family": "find",
                    "path": raw_path,
                    "message": "Refusing to run a broad filesystem search from the system or users root.",
                }
            if _path_is_allowed_by_config(normalized_path, resolved_allowed_roots):
                continue
            if normalized_path in home_variants and maxdepth != 1:
                return {
                    "reason": "filesystem_traversal_denied",
                    "command_family": "find",
                    "path": raw_path,
                    "message": (
                        "Refusing to recursively search the home folder because it can cross "
                        "macOS-protected app data. Search a specific subfolder instead."
                    ),
                }
            if any(_path_is_at_or_under(normalized_path, root) for root in protected_home_roots) and maxdepth != 1:
                return {
                    "reason": "filesystem_traversal_denied",
                    "command_family": "find",
                    "path": raw_path,
                    "message": (
                        "Refusing to recursively search protected home Library data. "
                        "Use a specific file path or a narrow app folder with explicit approval."
                    ),
                }
    return None


def _find_path_tokens(tokens: list[str]) -> list[str]:
    paths: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in {";", "&&", "||", "|"}:
            break
        if token in _FIND_PATH_OPTION_ARITY:
            index += 1 + _FIND_PATH_OPTION_ARITY[token]
            continue
        if any(token.startswith(prefix) for prefix in _FIND_EXPRESSION_STARTERS):
            break
        paths.append(token)
        index += 1
    return paths


def _find_maxdepth(tokens: list[str]) -> int | None:
    for index, token in enumerate(tokens):
        if token == "-maxdepth" and index + 1 < len(tokens):
            try:
                return int(tokens[index + 1])
            except ValueError:
                return None
    return None


def _normalize_find_root(raw_path: str, *, home: str) -> str:
    if raw_path in {"$HOME", "${HOME}", "~"}:
        return raw_path
    try:
        expanded = Path(os.path.expandvars(raw_path)).expanduser()
        return str(expanded.resolve(strict=False))
    except Exception:
        return raw_path


def _path_is_at_or_under(path: str, root: str) -> bool:
    try:
        normalized_path = Path(path)
        normalized_root = Path(root)
        return normalized_path == normalized_root or normalized_root in normalized_path.parents
    except Exception:
        return False


def _path_is_allowed_by_config(path: str, allowed_roots: Iterable[Path]) -> bool:
    if path in {"$HOME", "${HOME}", "~"}:
        path = str(Path.home())
    try:
        resolved_path = Path(os.path.expandvars(path)).expanduser().resolve(strict=False)
    except Exception:
        return False
    return any(_is_within_allowed_root(resolved_path, root) for root in allowed_roots)


@dataclass(frozen=True, slots=True)
class TerminalAttestationEvidence:
    format: str
    payload: dict[str, object]
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TerminalAttestationVerificationResult:
    is_valid: bool
    failure_reason: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TerminalBackendDescriptor:
    mode: str
    attested_capabilities: tuple[str, ...]
    metadata: dict[str, object]
    evidence: TerminalAttestationEvidence | None = None


class TerminalAttestationVerifier(Protocol):
    def verify(
        self,
        descriptor: TerminalBackendDescriptor,
        *,
        required_capabilities: Iterable[str],
    ) -> TerminalAttestationVerificationResult: ...


class TerminalExecutorBackend(Protocol):
    def describe(self) -> TerminalBackendDescriptor: ...

    def run(
        self,
        command: str,
        *,
        cwd: str | None,
        timeout: int,
        policy: TerminalExecutionPolicy,
    ) -> TerminalExecutionResult: ...


class SubprocessTerminalExecutorBackend:
    _INTERRUPTED_EXIT_CODE = 130
    _INTERRUPTED_MESSAGE = "Interrupted by user."

    def describe(self) -> TerminalBackendDescriptor:
        return TerminalBackendDescriptor(
            mode="subprocess",
            attested_capabilities=(),
            metadata={},
        )

    def run(
        self,
        command: str,
        *,
        cwd: str | None,
        timeout: int,
        policy: TerminalExecutionPolicy,
    ) -> TerminalExecutionResult:
        del policy
        shell_executable = os.environ.get("NULLION_TERMINAL_SHELL") or "/bin/sh"
        try:
            completed = subprocess.run(
                command,
                shell=True,
                executable=shell_executable,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=cwd,
                check=False,
            )
        except KeyboardInterrupt:
            return TerminalExecutionResult(
                exit_code=self._INTERRUPTED_EXIT_CODE,
                stdout="",
                stderr=self._INTERRUPTED_MESSAGE,
            )
        return TerminalExecutionResult(
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


@dataclass(frozen=True, slots=True)
class SandboxLauncherInvocation:
    argv: tuple[str, ...]
    env: dict[str, str]


class SandboxLauncherTerminalExecutorBackend:
    _INTERRUPTED_EXIT_CODE = 130
    _INTERRUPTED_MESSAGE = "Interrupted by user."

    def __init__(
        self,
        *,
        launcher_command: str,
        launcher_args: Iterable[str] = (),
    ) -> None:
        self._launcher_command = launcher_command
        self._launcher_args = tuple(arg for arg in launcher_args if isinstance(arg, str) and arg)
        self._launcher_argv = (launcher_command, *self._launcher_args)
        self._descriptor_cache: TerminalBackendDescriptor | None = None

    def _legacy_descriptor(self) -> TerminalBackendDescriptor:
        return TerminalBackendDescriptor(
            mode="launcher",
            attested_capabilities=(
                "network_policy_enforced",
                "network_policy_enforced.none",
                "network_policy_enforced.localhost_only",
                "network_policy_enforced.approved_only",
                "approved_only_enforced_via_local_allowlist_proxy",
            ),
            metadata={
                "launcher_command": self._launcher_command,
                "launcher_args": self._launcher_args,
                "supported_network_modes": ("none", "localhost_only", "approved_only"),
                "descriptor_source": _LAUNCHER_DESCRIPTOR_SOURCE_LEGACY,
                "descriptor_schema": _LAUNCHER_DESCRIPTOR_SCHEMA,
            },
            evidence=TerminalAttestationEvidence(
                format="nullion.launcher.v2",
                payload={
                    "schema": _LAUNCHER_DESCRIPTOR_SCHEMA,
                    "mode": "launcher",
                    "launcher_command": self._launcher_command,
                    "launcher_args": list(self._launcher_args),
                    "supported_network_modes": ["none", "localhost_only", "approved_only"],
                    "enforcement_by_mode": {
                        "none": "sandbox-exec deny network*",
                        "localhost_only": "sandbox-exec remote ip localhost:* only",
                        "approved_only": "sandbox-exec localhost proxy only + in-process allowlist proxy",
                    },
                    "requires_sandbox_exec_for_restrictive_modes": True,
                    "proxy_bind_scope": "127.0.0.1",
                },
                metadata={
                    "descriptor_source": _LAUNCHER_DESCRIPTOR_SOURCE_LEGACY,
                    "descriptor_schema": _LAUNCHER_DESCRIPTOR_SCHEMA,
                },
            ),
        )

    def _probe_launcher_descriptor(self) -> TerminalBackendDescriptor | None:
        command_path = Path(self._launcher_command)
        if command_path.parent == Path() and shutil.which(self._launcher_command) is None:
            return None
        try:
            completed = subprocess.run(
                [*self._launcher_argv, "--describe"],
                shell=False,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except OSError:
            return None
        if completed.returncode != 0:
            return None
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        if payload.get("schema") != _LAUNCHER_DESCRIPTOR_SCHEMA:
            return None
        if payload.get("mode") != "launcher":
            return None
        supported_network_modes = tuple(
            mode for mode in payload.get("supported_network_modes", ()) if isinstance(mode, str) and mode
        )
        attested_capabilities = tuple(
            capability
            for capability in payload.get("attested_capabilities", ())
            if isinstance(capability, str) and capability
        )
        enforcement_by_mode_raw = payload.get("enforcement_by_mode")
        enforcement_by_mode = {
            mode: enforcement
            for mode, enforcement in enforcement_by_mode_raw.items()
            if isinstance(mode, str) and isinstance(enforcement, str)
        } if isinstance(enforcement_by_mode_raw, dict) else {}
        proxy_bind_scope = payload.get("proxy_bind_scope")
        return TerminalBackendDescriptor(
            mode="launcher",
            attested_capabilities=attested_capabilities,
            metadata={
                "launcher_command": self._launcher_command,
                "launcher_args": self._launcher_args,
                "supported_network_modes": supported_network_modes,
                "descriptor_source": _LAUNCHER_DESCRIPTOR_SOURCE_PROBE,
                "descriptor_schema": _LAUNCHER_DESCRIPTOR_SCHEMA,
            },
            evidence=TerminalAttestationEvidence(
                format="nullion.launcher.v2",
                payload={
                    "schema": _LAUNCHER_DESCRIPTOR_SCHEMA,
                    "mode": "launcher",
                    "launcher_command": self._launcher_command,
                    "launcher_args": list(self._launcher_args),
                    "supported_network_modes": list(supported_network_modes),
                    "enforcement_by_mode": enforcement_by_mode,
                    "requires_sandbox_exec_for_restrictive_modes": payload.get(
                        "requires_sandbox_exec_for_restrictive_modes"
                    ),
                    "proxy_bind_scope": proxy_bind_scope,
                },
                metadata={
                    "descriptor_source": _LAUNCHER_DESCRIPTOR_SOURCE_PROBE,
                    "descriptor_schema": _LAUNCHER_DESCRIPTOR_SCHEMA,
                },
            ),
        )

    def describe(self) -> TerminalBackendDescriptor:
        if self._descriptor_cache is None:
            self._descriptor_cache = self._probe_launcher_descriptor() or self._legacy_descriptor()
        return self._descriptor_cache

    def _build_invocation(self, command: str, *, policy: TerminalExecutionPolicy) -> SandboxLauncherInvocation:
        network_mode = policy.network_mode or _NETWORK_MODE_FULL
        argv = [*self._launcher_argv, "--network-mode", network_mode]
        for target in policy.approved_targets:
            argv.extend(("--approved-target", target))
        argv.extend(("--", command))
        env = {
            "NULLION_TERMINAL_NETWORK_MODE": network_mode,
            "NULLION_TERMINAL_APPROVED_TARGETS": ",".join(policy.approved_targets),
        }
        return SandboxLauncherInvocation(argv=tuple(argv), env=env)

    def run(
        self,
        command: str,
        *,
        cwd: str | None,
        timeout: int,
        policy: TerminalExecutionPolicy,
    ) -> TerminalExecutionResult:
        invocation = self._build_invocation(command, policy=policy)
        env = {**os.environ, **invocation.env}
        try:
            completed = subprocess.run(
                list(invocation.argv),
                shell=False,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=cwd,
                check=False,
                env=env,
            )
        except KeyboardInterrupt:
            return TerminalExecutionResult(
                exit_code=self._INTERRUPTED_EXIT_CODE,
                stdout="",
                stderr=self._INTERRUPTED_MESSAGE,
            )
        return TerminalExecutionResult(
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )



def normalize_tool_status(status: str | None) -> str:
    if not isinstance(status, str):
        return "unknown"
    normalized = status.strip().lower()
    if not normalized:
        return "unknown"
    if normalized in _CANONICAL_TOOL_STATUSES:
        return normalized
    if normalized in _COMPLETED_TOOL_STATUS_ALIASES:
        return "completed"
    if normalized in _FAILED_TOOL_STATUS_ALIASES:
        return "failed"
    if normalized in _DENIED_TOOL_STATUS_ALIASES:
        return "denied"
    if normalized in _NONTERMINAL_TOOL_STATUS_ALIASES:
        return "nonterminal"
    return "unknown"



def normalize_tool_result(result: ToolResult) -> ToolResult:
    return ToolResult(
        invocation_id=result.invocation_id,
        tool_name=result.tool_name,
        status=normalize_tool_status(result.status),
        output=dict(result.output),
        error=result.error,
    )



class ToolRegistry:
    def __init__(
        self,
        *,
        plugin_registration_allowed: bool = True,
        extension_registration_allowed: bool | None = None,
        filesystem_allowed_roots: Iterable[Path] | None = None,
    ) -> None:
        if extension_registration_allowed is not None:
            plugin_registration_allowed = extension_registration_allowed
        self._specs: dict[str, ToolSpec] = {}
        self._handlers: dict[str, ToolHandler] = {}
        self._cleanup_hooks: list[ToolCleanupHook] = []
        self._plugin_registration_allowed = plugin_registration_allowed
        self._installed_plugins: set[str] = set()
        self._filesystem_allowed_roots = tuple(Path(root).resolve() for root in (filesystem_allowed_roots or ()))

    def register(self, spec: ToolSpec, handler: ToolHandler) -> None:
        if spec.name in self._specs:
            raise ValueError(f"Tool already registered: {spec.name}")
        self._specs[spec.name] = spec
        self._handlers[spec.name] = handler

    def get_spec(self, name: str) -> ToolSpec:
        return self._specs[name]

    def list_specs(self) -> list[ToolSpec]:
        return [self._specs[name] for name in sorted(self._specs)]

    def list_tool_definitions(self) -> list[dict[str, object]]:
        return [
            {
                "name": spec.name,
                "description": spec.description,
                "input_schema": getattr(spec, "input_schema", None) or _default_input_schema_for_tool(spec.name),
            }
            for spec in self.list_specs()
        ]

    def filesystem_allowed_roots(self) -> tuple[Path, ...]:
        return self._filesystem_allowed_roots

    def mark_plugin_installed(self, plugin_name: str) -> None:
        normalized = plugin_name.strip()
        if normalized:
            self._installed_plugins.add(normalized)

    def list_installed_plugins(self) -> list[str]:
        return sorted(self._installed_plugins)

    def is_plugin_installed(self, plugin_name: str) -> bool:
        return plugin_name in self._installed_plugins

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
        handler = self._handlers.get(invocation.tool_name)
        if handler is None:
            raise KeyError(f"Unknown tool: {invocation.tool_name}")
        return handler(invocation)

    def register_cleanup_hook(self, hook: ToolCleanupHook) -> None:
        self._cleanup_hooks.append(hook)

    def run_cleanup_hooks(self, *, scope_id: str | None = None) -> None:
        for hook in tuple(self._cleanup_hooks):
            try:
                hook(scope_id)
            except Exception:
                logger.debug("Tool cleanup hook failed", exc_info=True)

    def require_extension_registration_allowed(self) -> None:
        if not self._plugin_registration_allowed:
            raise ValueError("Tool registry only accepts core tools")

    def require_plugin_registration_allowed(self) -> None:
        self.require_extension_registration_allowed()


def _load_sentinel_policy() -> SentinelPolicy:
    try:
        from nullion.preferences import load_preferences

        prefs = load_preferences()
        risk_threshold = 3 if prefs.auto_mode is False and prefs.sentinel_mode == "risk_based" else prefs.sentinel_risk_level
        return SentinelPolicy.from_values(
            mode=prefs.sentinel_mode,
            risk_threshold=risk_threshold,
        )
    except Exception:
        return SentinelPolicy()


def _env_flag(name: str, *, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _connector_access_enabled() -> bool:
    raw = os.environ.get("NULLION_CONNECTOR_ACCESS_ENABLED")
    if raw is not None and raw.strip():
        return _env_flag("NULLION_CONNECTOR_ACCESS_ENABLED")
    enabled_packs = str(os.environ.get("NULLION_ENABLED_SKILL_PACKS") or "").lower()
    if "connector" in enabled_packs or "api-gateway" in enabled_packs:
        return True
    try:
        from nullion.connections import load_connection_registry

        return any(
            _connector_provider_id_looks_external(getattr(connection, "provider_id", ""))
            for connection in load_connection_registry().connections
            if getattr(connection, "active", True)
        )
    except Exception:
        return False


def _connector_provider_id_looks_external(provider_id: object) -> bool:
    normalized = str(provider_id or "").strip().lower()
    return normalized.startswith("skill_pack_connector_") or normalized.endswith("_connector_provider")


def _connector_provider_id_for_pack_id(pack_id: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(pack_id or "").strip().lower()).strip("_") or "custom_skill"
    return f"skill_pack_connector_{slug}"


def _installed_connector_skill_pack_id_for_provider(provider_id: str) -> str | None:
    try:
        from nullion.skill_pack_installer import list_installed_skill_packs

        for pack in list_installed_skill_packs():
            pack_id = str(getattr(pack, "pack_id", "") or "").strip()
            if pack_id and _connector_provider_id_for_pack_id(pack_id) == provider_id:
                return pack_id
    except Exception:
        return None
    return None


def _available_connector_provider_summaries(principal_id: str | None) -> list[dict[str, object]]:
    try:
        from nullion.connections import connection_for_principal, load_connection_registry

        summaries: list[dict[str, object]] = []
        seen: set[str] = set()
        for connection in load_connection_registry().connections:
            provider_id = str(getattr(connection, "provider_id", "") or "").strip()
            if (
                not provider_id
                or provider_id in seen
                or not getattr(connection, "active", True)
                or not _connector_provider_id_looks_external(provider_id)
            ):
                continue
            if connection_for_principal(principal_id, provider_id) is None:
                continue
            seen.add(provider_id)
            summary = {
                "provider_id": provider_id,
                "display_name": str(getattr(connection, "display_name", "") or provider_id),
                "credential_scope": str(getattr(connection, "credential_scope", "") or "workspace"),
            }
            skill_pack_id = _installed_connector_skill_pack_id_for_provider(provider_id)
            if skill_pack_id:
                summary["skill_pack_id"] = skill_pack_id
            base_urls = _connector_allowed_base_urls(connection, provider_id)
            if base_urls:
                summary["base_urls"] = list(base_urls)
            summaries.append(summary)
        return summaries
    except Exception:
        return []


def _account_tool_failure_output(
    principal_id: str | None,
    *,
    query: str | None = None,
    message_id: str | None = None,
) -> dict[str, object]:
    output: dict[str, object] = {}
    if query is not None:
        output["query"] = query
    if message_id is not None:
        output["id"] = message_id
    connector_providers = _available_connector_provider_summaries(principal_id)
    if connector_providers:
        output["available_connector_providers"] = connector_providers
        output["next_step"] = (
            "A native provider failed. If an enabled connector skill covers this task, "
            "consult that skill's instructions and try connector_request with a listed provider_id "
            "before concluding account access is unavailable."
        )
    return output


def _connector_request_boundary_preapproved(invocation: "ToolInvocation", fact: BoundaryFact) -> bool:
    if invocation.tool_name != "connector_request":
        return False
    provider_id = str(invocation.arguments.get("provider_id") or "").strip()
    if not provider_id:
        return False
    try:
        from nullion.connections import connection_for_principal

        connection = connection_for_principal(invocation.principal_id, provider_id)
    except Exception:
        connection = None
    if connection is None or not getattr(connection, "active", True):
        return False
    if fact.kind is BoundaryKind.ACCOUNT_ACCESS:
        if fact.operation != "read":
            return False
        return fact.target == provider_id
    if fact.kind is BoundaryKind.OUTBOUND_NETWORK:
        raw_url = invocation.arguments.get("url")
        if not isinstance(raw_url, str) or not raw_url.strip():
            return False
        try:
            url = _connector_request_url(raw_url, invocation.arguments.get("params"), connection, provider_id)
        except Exception:
            return False
        allowed_bases = _connector_allowed_base_urls(connection, provider_id)
        return bool(allowed_bases) and any(_url_is_under_base(url, base_url) for base_url in allowed_bases)
    return False


def _boundary_risk_score(fact: BoundaryFact) -> int:
    if fact.kind is BoundaryKind.ACCOUNT_ACCESS:
        return 7
    if fact.kind is BoundaryKind.FILESYSTEM_ACCESS:
        return 6
    if fact.kind is BoundaryKind.OUTBOUND_NETWORK:
        address_class = fact.attributes.get("address_class")
        if address_class in {"private", "loopback", "localhost", "link_local", "reserved"}:
            return 9
        return 4
    return 5


def _boundary_policy_principal_for_fact(principal_id: str, fact: BoundaryFact) -> str:
    if fact.kind is BoundaryKind.FILESYSTEM_ACCESS:
        return permission_scope_principal(principal_id)
    return GLOBAL_PERMISSION_PRINCIPAL


def _matching_active_boundary_permits_for_invocation(store: RuntimeStore, invocation: "ToolInvocation"):
    principal_ids = {
        invocation.principal_id,
        permission_scope_principal(invocation.principal_id),
        GLOBAL_PERMISSION_PRINCIPAL,
        OPERATOR_PERMISSION_PRINCIPAL,
        *_LEGACY_GLOBAL_PERMISSION_PRINCIPALS,
    }
    now = datetime.now(UTC)
    facts = extract_boundary_facts(invocation)
    if not facts:
        return []
    matched = {}
    for permit in store.list_boundary_permits():
        if permit.principal_id not in principal_ids:
            continue
        if not is_boundary_permit_active(permit, now=now):
            continue
        for fact in facts:
            if fact.kind is not permit.boundary_kind:
                continue
            policy_principal = _boundary_policy_principal_for_fact(invocation.principal_id, fact)
            request = BoundaryPolicyRequest(principal_id=policy_principal, boundary=fact)
            permit_rule = BoundaryPolicyRule(
                rule_id=f"permit:{permit.permit_id}",
                principal_id=policy_principal,
                kind=permit.boundary_kind,
                mode=PolicyMode.ALLOW,
                selector=permit.selector,
                created_by=permit.granted_by,
                created_at=permit.granted_at,
                expires_at=permit.expires_at,
                revoked_at=permit.revoked_at,
                reason="boundary_permit",
            )
            if evaluate_boundary_request(request, rules=[permit_rule]) is PolicyDecision.ALLOW:
                key = (permit.principal_id, permit.boundary_kind, permit.selector)
                current = matched.get(key)
                if current is None or permit.granted_at >= current.granted_at:
                    matched[key] = permit
                break
    return list(matched.values())


def _record_wildcard_boundary_permit_accesses(
    store: RuntimeStore,
    invocation: "ToolInvocation",
    permits,
) -> None:
    facts = extract_boundary_facts(invocation)
    if not facts:
        return
    seen: set[tuple[str, str, str]] = set()
    for permit in permits:
        if permit.selector != "*":
            continue
        for fact in facts:
            if fact.kind is not permit.boundary_kind:
                continue
            domain = (
                normalize_outbound_network_selector(fact.target)
                if fact.kind is BoundaryKind.OUTBOUND_NETWORK
                else fact.target
            )
            key = (permit.permit_id, domain, fact.target)
            if key in seen:
                continue
            seen.add(key)
            payload = {
                "permit_id": permit.permit_id,
                "approval_id": permit.approval_id,
                "principal_id": invocation.principal_id,
                "permit_principal_id": permit.principal_id,
                "boundary_kind": permit.boundary_kind.value,
                "selector": permit.selector,
                "domain": domain,
                "target": fact.target,
                "operation": fact.operation,
                "tool_name": invocation.tool_name,
                "invocation_id": invocation.invocation_id,
                "capsule_id": invocation.capsule_id,
                "accessed_at": datetime.now(UTC).isoformat(),
            }
            store.add_event(make_event("boundary_permit.wildcard_access", invocation.principal_id, payload))
            store.add_audit_record(
                make_audit_record("boundary_permit.wildcard_access", invocation.principal_id, payload)
            )


class ToolExecutor:
    def __init__(
        self,
        *,
        store: RuntimeStore,
        registry: ToolRegistry,
        allowed_tool_names: Iterable[str] | None = None,
        denied_tool_names: Iterable[str] | None = None,
        sentinel_policy: SentinelPolicy | None = None,
    ) -> None:
        self._store = store
        self._registry = registry
        self._allowed_tool_names = tuple(sorted(set(allowed_tool_names or ())))
        self._denied_tool_names = tuple(sorted(set(denied_tool_names or ())))
        self._sentinel_policy = sentinel_policy or _load_sentinel_policy()

    def _record_egress_event(self, event_type: str, invocation: ToolInvocation, payload: dict[str, object]) -> None:
        details = {
            "invocation_id": invocation.invocation_id,
            "tool_name": invocation.tool_name,
            "principal_id": invocation.principal_id,
            "capsule_id": invocation.capsule_id,
            **payload,
        }
        self._store.add_event(make_event(event_type, invocation.principal_id, details))
        self._store.add_audit_record(make_audit_record(event_type, invocation.principal_id, details))

    def _record_detected_egress_attempts(self, invocation: ToolInvocation, egress_attempts: list[dict[str, object]]) -> None:
        for attempt in egress_attempts:
            self._record_egress_event("tool.egress.detected", invocation, dict(attempt))

    def _record_egress_outcome(self, invocation: ToolInvocation, result: ToolResult, egress_attempts: list[dict[str, object]]) -> None:
        if not egress_attempts:
            return
        network_mode = result.output.get("network_mode") if isinstance(result.output, dict) else None
        outcome_event = None
        if result.output.get("reason") == "network_denied":
            outcome_event = "tool.egress.denied"
        elif result.status in {"completed", "failed", "nonterminal"}:
            outcome_event = "tool.egress.allowed"
        if outcome_event is None:
            return
        for attempt in egress_attempts:
            payload = dict(attempt)
            if network_mode is not None:
                payload["network_mode"] = network_mode
            payload["tool_status"] = result.status
            self._record_egress_event(outcome_event, invocation, payload)

    def _deny_invocation(
        self,
        invocation: ToolInvocation,
        *,
        reason: str,
        error: str,
        output: dict[str, object],
    ) -> ToolResult:
        denied_details = {
            "invocation_id": invocation.invocation_id,
            "tool_name": invocation.tool_name,
            "principal_id": invocation.principal_id,
            "capsule_id": invocation.capsule_id,
            "status": "denied",
            "reason": reason,
        }
        self._store.add_event(make_event("tool.invocation.denied", invocation.principal_id, denied_details))
        self._store.add_audit_record(make_audit_record("tool.invocation.denied", invocation.principal_id, denied_details))
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="denied",
            output=output,
            error=error,
        )

    def _has_active_grant(self, *, principal_id: str, permissions: Iterable[str]) -> bool:
        required = set(permissions)
        principal_ids = {
            principal_id,
            permission_scope_principal(principal_id),
            GLOBAL_PERMISSION_PRINCIPAL,
            OPERATOR_PERMISSION_PRINCIPAL,
            *_LEGACY_GLOBAL_PERMISSION_PRINCIPALS,
        }
        for grant in self._store.list_permission_grants():
            if grant.principal_id not in principal_ids:
                continue
            if grant.permission not in required:
                continue
            if not is_permission_grant_active(grant):
                continue
            return True
        return False

    def _has_required_tool_grant(self, invocation: ToolInvocation) -> bool:
        return self._has_active_grant(
            principal_id=invocation.principal_id,
            permissions=(
                f"tool:{invocation.tool_name}",
                f"tool.{invocation.tool_name}",
                invocation.tool_name,
            ),
        )

    def _find_pending_tool_approval(self, invocation: ToolInvocation):
        for approval in self._store.list_approval_requests():
            if approval.status.value != "pending":
                continue
            if approval.requested_by != invocation.principal_id:
                continue
            if approval.action != "use_tool":
                continue
            if approval.resource != invocation.tool_name:
                continue
            return approval
        return None

    def _find_pending_boundary_policy_approval(self, invocation: ToolInvocation, *, context: dict[str, object]):
        target = context.get("target")
        if not isinstance(target, str) or not target:
            return None
        requested_workspace = permission_scope_principal(invocation.principal_id)
        requested_key = _boundary_approval_match_key(context=context, fallback_target=target)
        for approval in self._store.list_approval_requests():
            if approval.status.value != "pending":
                continue
            if permission_scope_principal(approval.requested_by) != requested_workspace:
                continue
            if approval.request_kind != "boundary_policy":
                continue
            if approval.action != "allow_boundary":
                continue
            approval_context = approval.context if isinstance(approval.context, dict) else {}
            approval_key = _boundary_approval_match_key(context=approval_context, fallback_target=approval.resource)
            if requested_key is None:
                if approval.resource != target:
                    continue
            elif approval_key != requested_key:
                continue
            return approval
        return None

    def _ensure_tool_approval_request(self, invocation: ToolInvocation):
        existing = self._find_pending_tool_approval(invocation)
        if existing is not None:
            return existing
        spec = self._registry.get_spec(invocation.tool_name)
        workspace_id = "workspace_admin"
        try:
            from nullion.connections import workspace_id_for_principal

            workspace_id = workspace_id_for_principal(invocation.principal_id)
        except Exception:
            pass
        approval = create_approval_request(
            requested_by=invocation.principal_id,
            action="use_tool",
            resource=invocation.tool_name,
            context={
                "workspace_id": workspace_id,
                FLOW_TRIGGER_CONTEXT_KEY: dict(invocation.flow_context)
                if isinstance(invocation.flow_context, dict)
                else build_trigger_flow_context(
                    principal_id=invocation.principal_id,
                    invocation_id=invocation.invocation_id,
                    capsule_id=invocation.capsule_id,
                    flow_kind="tool_invocation",
                ),
                "tool_name": invocation.tool_name,
                "tool_description": spec.description,
                "tool_risk_level": str(getattr(spec.risk_level, "value", spec.risk_level)),
                "tool_side_effect_class": str(getattr(spec.side_effect_class, "value", spec.side_effect_class)),
                "requires_approval": spec.requires_approval,
                "tool_permission_scope": spec.permission_scope,
                "tool_arguments": redact_value(dict(invocation.arguments or {})),
            },
        )
        self._store.add_approval_request(approval)
        return approval

    def _ensure_boundary_policy_approval_request(self, invocation: ToolInvocation, *, context: dict[str, object]):
        target = context.get("target")
        if not isinstance(target, str) or not target:
            return None
        workspace_id = "workspace_admin"
        try:
            from nullion.connections import workspace_id_for_principal

            workspace_id = workspace_id_for_principal(invocation.principal_id)
        except Exception:
            pass
        approval_context = {
            **context,
            "workspace_id": workspace_id,
            FLOW_TRIGGER_CONTEXT_KEY: dict(invocation.flow_context)
            if isinstance(invocation.flow_context, dict)
            else build_trigger_flow_context(
                principal_id=invocation.principal_id,
                invocation_id=invocation.invocation_id,
                capsule_id=invocation.capsule_id,
                flow_kind="boundary_policy",
            ),
        }
        existing = self._find_pending_boundary_policy_approval(invocation, context=approval_context)
        if existing is not None:
            refreshed = _refresh_pending_approval_request(existing, context=approval_context, resource=target)
            self._store.add_approval_request(refreshed)
            return refreshed
        approval = create_approval_request(
            requested_by=invocation.principal_id,
            action="allow_boundary",
            resource=target,
            request_kind="boundary_policy",
            context=approval_context,
        )
        self._store.add_approval_request(approval)
        return approval

    def _boundary_rules_for_fact(self, invocation: ToolInvocation, *, fact) -> list[BoundaryPolicyRule]:
        policy_principal = _boundary_policy_principal_for_fact(invocation.principal_id, fact)
        principal_ids = {
            invocation.principal_id,
            policy_principal,
            permission_scope_principal(invocation.principal_id),
            GLOBAL_PERMISSION_PRINCIPAL,
            OPERATOR_PERMISSION_PRINCIPAL,
            *_LEGACY_GLOBAL_PERMISSION_PRINCIPALS,
        }
        rules = [
            replace(rule, principal_id=policy_principal)
            if rule.principal_id in {OPERATOR_PERMISSION_PRINCIPAL, *_LEGACY_GLOBAL_PERMISSION_PRINCIPALS}
            else rule
            for rule in self._store.list_boundary_policy_rules()
            if rule.principal_id in principal_ids and rule.kind is fact.kind
        ]
        now = datetime.now(UTC)
        for permit in self._store.list_boundary_permits():
            if permit.principal_id not in principal_ids:
                continue
            if permit.boundary_kind is not fact.kind:
                continue
            if not is_boundary_permit_active(permit, now=now):
                continue
            rules.append(
                BoundaryPolicyRule(
                    rule_id=f"permit:{permit.permit_id}",
                    principal_id=policy_principal,
                    kind=permit.boundary_kind,
                    mode=PolicyMode.ALLOW,
                    selector=permit.selector,
                    created_by=permit.granted_by,
                    created_at=permit.granted_at,
                    expires_at=permit.expires_at,
                    revoked_at=permit.revoked_at,
                    reason="boundary_permit",
                )
            )
        return rules

    def _filesystem_boundary_allowed(self, target: str, *, principal_id: str | None = None) -> bool:
        allowed_roots = (
            *self._registry.filesystem_allowed_roots(),
            *_principal_workspace_file_roots(principal_id),
        )
        if not allowed_roots:
            return True
        try:
            path = Path(target).expanduser().resolve()
        except OSError:
            return False
        for root in allowed_roots:
            try:
                path.relative_to(root)
                return True
            except ValueError:
                continue
        return False

    def _media_filesystem_boundary_decision(self, fact) -> PolicyDecision | None:
        try:
            spec = self._registry.get_spec(fact.tool_name)
        except KeyError:
            return None
        if spec.filesystem_boundary_policy != _FILESYSTEM_BOUNDARY_TRUSTED_ROOTS_ONLY:
            return None
        return PolicyDecision.ALLOW if self._filesystem_boundary_allowed(fact.target) else PolicyDecision.DENY

    def _preflight_boundary_policy_result(self, invocation: ToolInvocation):
        if invocation.tool_name == "terminal_exec":
            return None
        for fact in extract_boundary_facts(invocation):
            if _connector_request_boundary_preapproved(invocation, fact):
                continue
            policy_principal = _boundary_policy_principal_for_fact(invocation.principal_id, fact)
            if fact.kind is BoundaryKind.OUTBOUND_NETWORK:
                request = BoundaryPolicyRequest(principal_id=policy_principal, boundary=fact)
                decision = evaluate_boundary_request(request, rules=self._boundary_rules_for_fact(invocation, fact=fact))
            elif fact.kind is BoundaryKind.FILESYSTEM_ACCESS:
                if self._filesystem_boundary_allowed(fact.target, principal_id=invocation.principal_id):
                    continue
                rules = self._boundary_rules_for_fact(invocation, fact=fact)
                media_decision = self._media_filesystem_boundary_decision(fact)
                if media_decision == PolicyDecision.ALLOW:
                    # File is within trusted roots — fast-path allow without rule lookup.
                    decision = media_decision
                else:
                    matching_rules = [
                        rule
                        for rule in rules
                        if rule.selector == fact.target
                        or fnmatch(fact.target, rule.selector)
                        or (rule.selector.endswith("/*") and fact.target == rule.selector[:-2])
                    ]
                    if matching_rules:
                        # Explicit user-granted rules take precedence even for media tools.
                        request = BoundaryPolicyRequest(principal_id=policy_principal, boundary=fact)
                        decision = evaluate_boundary_request(request, rules=matching_rules)
                    elif media_decision == PolicyDecision.DENY:
                        # Media tool with trusted-roots-only policy, file outside trusted roots,
                        # and no explicit grant — hard deny (no approval flow).
                        decision = PolicyDecision.DENY
                    else:
                        decision = PolicyDecision.ALLOW if self._filesystem_boundary_allowed(fact.target) else PolicyDecision.REQUIRE_APPROVAL
            elif fact.kind is BoundaryKind.ACCOUNT_ACCESS:
                rules = self._boundary_rules_for_fact(invocation, fact=fact)
                if rules:
                    request = BoundaryPolicyRequest(principal_id=policy_principal, boundary=fact)
                    decision = evaluate_boundary_request(request, rules=rules)
                else:
                    # Default: require approval before any account-scoped operation
                    decision = PolicyDecision.REQUIRE_APPROVAL
            else:
                continue
            decision = self._sentinel_policy.decision_for_risk(_boundary_risk_score(fact), baseline=decision)
            if decision is PolicyDecision.ALLOW:
                continue
            if decision is PolicyDecision.REQUIRE_APPROVAL:
                context = _boundary_approval_context_from_fact(fact)
                if context is None:
                    continue
                approval = self._ensure_boundary_policy_approval_request(invocation, context=context)
                if approval is None:
                    continue
                return self._deny_invocation(
                    invocation,
                    reason="approval_required",
                    error="Approval required for outbound network boundary policy",
                    output={
                        **context,
                        "reason": "approval_required",
                        "requires_approval": True,
                        "approval_id": approval.approval_id,
                    },
                )
            return self._deny_invocation(
                invocation,
                reason="boundary_denied",
                error=f"Boundary denied for tool: {invocation.tool_name}",
                output={"reason": "boundary_denied", "target": fact.target, "boundary_kind": fact.kind.value},
            )
        return None

    def _with_trusted_filesystem_selectors(self, invocation: ToolInvocation) -> ToolInvocation:
        selectors = list(invocation.trusted_filesystem_selectors)
        policy_principal = permission_scope_principal(invocation.principal_id)
        for fact in extract_boundary_facts(invocation):
            if fact.kind is not BoundaryKind.FILESYSTEM_ACCESS:
                continue
            if self._filesystem_boundary_allowed(fact.target, principal_id=invocation.principal_id):
                continue
            rules = self._boundary_rules_for_fact(invocation, fact=fact)
            matching_rules = [
                rule
                for rule in rules
                if rule.selector == fact.target
                or fnmatch(fact.target, rule.selector)
                or (rule.selector.endswith("/*") and fact.target == rule.selector[:-2])
            ]
            if not matching_rules:
                continue
            request = BoundaryPolicyRequest(principal_id=policy_principal, boundary=fact)
            if evaluate_boundary_request(request, rules=matching_rules) is PolicyDecision.ALLOW:
                selectors.extend(rule.selector for rule in matching_rules if rule.mode is PolicyMode.ALLOW)
        trusted = tuple(dict.fromkeys(selectors))
        if trusted == invocation.trusted_filesystem_selectors:
            return invocation
        return ToolInvocation(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            principal_id=invocation.principal_id,
            arguments=dict(invocation.arguments),
            capsule_id=invocation.capsule_id,
            trusted_filesystem_selectors=trusted,
            flow_context=invocation.flow_context,
        )

    def _rewrite_network_denied_result_as_boundary_approval_required(
        self,
        invocation: ToolInvocation,
        result: ToolResult,
    ) -> ToolResult:
        context = _outbound_network_approval_context_from_result(result)
        if context is None:
            return result
        approval = self._ensure_boundary_policy_approval_request(invocation, context=context)
        if approval is None:
            return result
        return ToolResult(
            invocation_id=result.invocation_id,
            tool_name=result.tool_name,
            status="denied",
            output={
                **context,
                "reason": "approval_required",
                "requires_approval": True,
                "approval_id": approval.approval_id,
            },
            error="Approval required for outbound network boundary policy",
        )

    def _record_prompt_injection_scan(self, invocation: ToolInvocation, result: ToolResult) -> None:
        if result.status != "completed":
            return
        scan = scan_tool_output(result.tool_name, result.output)
        if not scan.detected:
            return
        payload = {
            "invocation_id": result.invocation_id,
            "tool_name": result.tool_name,
            "principal_id": invocation.principal_id,
            "capsule_id": invocation.capsule_id,
            "severity": scan.severity,
            "findings": [finding.to_dict() for finding in scan.findings],
        }
        self._store.add_event(make_event("security.prompt_injection.detected", invocation.principal_id, payload))
        self._store.add_audit_record(
            make_audit_record("security.prompt_injection.detected", invocation.principal_id, payload)
        )

    def invoke(self, invocation: ToolInvocation) -> ToolResult:
        invocation = _with_resolved_virtual_workspace_paths(invocation)
        if invocation.tool_name in self._denied_tool_names:
            return self._deny_invocation(
                invocation,
                reason="capability_denied",
                error=f"Capability denied for tool: {invocation.tool_name}",
                output={"reason": "capability_denied", "denied_tools": list(self._denied_tool_names)},
            )

        if self._allowed_tool_names and invocation.tool_name not in self._allowed_tool_names:
            return self._deny_invocation(
                invocation,
                reason="capability_not_granted",
                error=f"Capability not granted for tool: {invocation.tool_name}",
                output={"reason": "capability_not_granted", "allowed_tools": list(self._allowed_tool_names)},
            )

        spec = self._registry.get_spec(invocation.tool_name)

        tool_decision = (
            self._sentinel_policy.decision_for_risk(
                _TOOL_RISK_SCORES.get(spec.risk_level, 5),
                baseline=PolicyDecision.REQUIRE_APPROVAL,
            )
            if (spec.requires_approval or self._sentinel_policy.mode.value == "ask_all")
            else PolicyDecision.ALLOW
        )
        if tool_decision is PolicyDecision.REQUIRE_APPROVAL and not self._has_required_tool_grant(invocation):
            approval = self._ensure_tool_approval_request(invocation)
            return self._deny_invocation(
                invocation,
                reason="approval_required",
                error=f"Approval required for tool: {invocation.tool_name}",
                output={
                    "reason": "approval_required",
                    "requires_approval": True,
                    "approval_id": approval.approval_id,
                },
            )

        preflight_result = self._preflight_boundary_policy_result(invocation)
        if preflight_result is not None:
            return preflight_result
        invocation = self._with_trusted_filesystem_selectors(invocation)

        egress_attempts = _egress_attempts_for_invocation(invocation)
        self._record_detected_egress_attempts(invocation, egress_attempts)

        details = {
            "invocation_id": invocation.invocation_id,
            "tool_name": invocation.tool_name,
            "principal_id": invocation.principal_id,
            "capsule_id": invocation.capsule_id,
            "status": "started",
        }
        self._store.add_event(make_event("tool.invocation.started", invocation.principal_id, details))
        self._store.add_audit_record(make_audit_record("tool.invocation.started", invocation.principal_id, details))

        try:
            result = self._registry.invoke(invocation)
        except Exception as exc:
            result = ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"reason": "handler_exception"},
                error=str(exc),
            )
        result = normalize_tool_result(result)
        self._record_egress_outcome(invocation, result, egress_attempts)
        result = self._rewrite_network_denied_result_as_boundary_approval_required(invocation, result)
        if invocation.tool_name != "terminal_exec" and result.status != "denied":
            active_permits = _matching_active_boundary_permits_for_invocation(self._store, invocation)
            _record_wildcard_boundary_permit_accesses(self._store, invocation, active_permits)
            for permit in active_permits:
                if permit.uses_remaining <= 0:
                    continue
                self._store.add_boundary_permit(consume_boundary_permit_record(permit))
        self._record_prompt_injection_scan(invocation, result)
        completed_details = {
            "invocation_id": result.invocation_id,
            "tool_name": result.tool_name,
            "principal_id": invocation.principal_id,
            "capsule_id": invocation.capsule_id,
            "status": result.status,
        }
        if result.error:
            completed_details["error"] = str(result.error).strip().replace("\n", " ")[:240]
        output = result.output if isinstance(result.output, dict) else {}
        for output_key, detail_key in (
            ("reason", "reason"),
            ("summary", "summary"),
            ("message", "message"),
            ("url", "url"),
            ("path", "path"),
        ):
            value = output.get(output_key)
            if value:
                completed_details[detail_key] = str(value).strip().replace("\n", " ")[:240]
        self._store.add_event(make_event("tool.invocation.completed", invocation.principal_id, completed_details))
        self._store.add_audit_record(make_audit_record("tool.invocation.completed", invocation.principal_id, completed_details))
        return result



def _is_within_allowed_root(path: Path, allowed_root: Path) -> bool:
    try:
        path.relative_to(allowed_root)
    except ValueError:
        return False
    return True



def _is_approved_filesystem_path(path: Path, selectors: Iterable[str]) -> bool:
    target = str(path)
    for selector in selectors:
        if not isinstance(selector, str) or not selector:
            continue
        try:
            resolved_selector = str(Path(selector).expanduser().resolve())
        except OSError:
            resolved_selector = str(Path(selector).expanduser())
        if (
            selector == target
            or resolved_selector == target
            or fnmatch(target, selector)
            or fnmatch(target, resolved_selector)
            or (selector.endswith("/*") and target == selector[:-2])
            or (resolved_selector.endswith("/*") and target == resolved_selector[:-2])
        ):
            return True
    return False


_FILE_WALK_PRUNED_DIR_NAMES = frozenset(
    {
        ".git",
        ".hg",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".svn",
        ".venv",
        "__pycache__",
        "brave-debug",
        "chrome-debug",
        "chromium-debug",
        "node_modules",
        "venv",
    }
)

_MACOS_PROTECTED_APP_DATA_SUFFIXES = (
    ("library", "application support", "addressbook"),
    ("library", "calendars"),
    ("library", "containers"),
    ("library", "group containers"),
    ("library", "mail"),
    ("library", "messages"),
    ("library", "safari"),
)


def _has_path_suffix(path: Path, suffix: tuple[str, ...]) -> bool:
    parts = tuple(part.lower() for part in path.parts)
    if len(parts) < len(suffix):
        return False
    return any(parts[index : index + len(suffix)] == suffix for index in range(len(parts) - len(suffix) + 1))


def _should_prune_filesystem_walk_dir(path: Path) -> bool:
    name = path.name.lower()
    if name in _FILE_WALK_PRUNED_DIR_NAMES:
        return True
    return any(_has_path_suffix(path, suffix) for suffix in _MACOS_PROTECTED_APP_DATA_SUFFIXES)


def _build_file_read_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    *,
    include_principal_workspace: bool = True,
) -> ToolHandler:
    resolved_root = Path(workspace_root).resolve() if workspace_root is not None else None
    resolved_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else None

    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_path = invocation.arguments.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: path",
            )
        effective_roots = _effective_filesystem_roots(
            invocation=invocation,
            resolved_root=resolved_root,
            resolved_allowed_roots=resolved_allowed_roots,
            include_principal_workspace=include_principal_workspace,
        )
        if not effective_roots:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="File access requires workspace_root or allowed_roots",
            )

        path = Path(raw_path).expanduser().resolve()
        if not _path_within_any_root(path, effective_roots) and not _is_approved_filesystem_path(
            path, invocation.trusted_filesystem_selectors
        ):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Path is outside workspace root: {path}",
            )

        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"File not found: {path}",
            )

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"path": str(path), "content": content},
            error=None,
        )

    return handler



def _build_file_write_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    *,
    include_principal_workspace: bool = True,
) -> ToolHandler:
    resolved_root = Path(workspace_root).resolve() if workspace_root is not None else None
    resolved_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else None

    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_path = invocation.arguments.get("path")
        raw_content = invocation.arguments.get("content")
        if not isinstance(raw_path, str) or not raw_path:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: path",
            )
        if not isinstance(raw_content, str):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: content",
            )
        effective_roots = _effective_filesystem_roots(
            invocation=invocation,
            resolved_root=resolved_root,
            resolved_allowed_roots=resolved_allowed_roots,
            include_principal_workspace=include_principal_workspace,
        )
        if not effective_roots:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="File access requires workspace_root or allowed_roots",
            )

        path = Path(raw_path).expanduser().resolve()
        if not _path_within_any_root(path, effective_roots) and not _is_approved_filesystem_path(
            path, invocation.trusted_filesystem_selectors
        ):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Path is outside workspace root: {path}",
            )

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(raw_content, encoding="utf-8")
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"path": str(path), "bytes_written": len(raw_content.encode("utf-8"))},
            error=None,
        )

    return handler


_PDF_PAGE_SIZES = {
    "letter": (1275, 1650),
    "a4": (1240, 1754),
}


def _pdf_default_output_path(invocation: ToolInvocation, *, title: str, roots: tuple[Path, ...]) -> Path:
    try:
        from nullion.artifacts import artifact_path_for_generated_workspace_file

        return artifact_path_for_generated_workspace_file(
            principal_id=invocation.principal_id,
            suffix=".pdf",
            stem=_safe_pdf_stem(title),
        ).resolve()
    except Exception:
        root = roots[0]
        return (root / f"{_safe_pdf_stem(title)}.pdf").resolve()


def _safe_pdf_stem(title: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(title or "nullion-artifact").strip().lower())
    cleaned = cleaned.strip(".-")
    return cleaned[:48] or "nullion-artifact"


def _coerce_string_list(value: object, *, field: str) -> tuple[list[str], str | None]:
    if value is None:
        return [], None
    if isinstance(value, str):
        text = value.strip()
        return ([text] if text else []), None
    if not isinstance(value, list):
        return [], f"{field} must be a list of strings"
    items = [str(item).strip() for item in value if isinstance(item, str) and str(item).strip()]
    return items, None


def _image_to_pdf_page(path: Path, *, page_size: tuple[int, int]):
    from PIL import Image, ImageOps

    with Image.open(path) as image:
        image = ImageOps.exif_transpose(image)
        if image.mode not in {"RGB", "L"}:
            background = Image.new("RGB", image.size, "white")
            if image.mode in {"RGBA", "LA"}:
                background.paste(image, mask=image.getchannel("A"))
                image = background
            else:
                image = image.convert("RGB")
        else:
            image = image.convert("RGB")
        fitted = ImageOps.contain(image, page_size)
        page = Image.new("RGB", page_size, "white")
        offset = ((page_size[0] - fitted.width) // 2, (page_size[1] - fitted.height) // 2)
        page.paste(fitted, offset)
        return page


def _text_to_pdf_page(text: str, *, title: str, page_size: tuple[int, int]):
    from PIL import Image, ImageDraw, ImageFont

    page = Image.new("RGB", page_size, "white")
    draw = ImageDraw.Draw(page)
    font = ImageFont.load_default()
    margin = 84
    y = margin
    if title.strip():
        draw.text((margin, y), title.strip()[:120], fill="black", font=font)
        y += 34
    max_chars = max(40, (page_size[0] - margin * 2) // 8)
    for paragraph in str(text or "").splitlines() or [""]:
        lines = textwrap.wrap(paragraph, width=max_chars) or [""]
        for line in lines:
            if y > page_size[1] - margin:
                return page
            draw.text((margin, y), line, fill="black", font=font)
            y += 20
        y += 10
    return page


def _build_pdf_pages(
    *,
    image_paths: list[str],
    text_pages: list[str],
    page_size: tuple[int, int],
    roots: tuple[Path, ...],
    invocation: ToolInvocation,
    title: str,
) -> tuple[list[object], list[str], str | None]:
    pages = []
    source_images: list[str] = []
    for raw_path in image_paths:
        image_path = Path(raw_path).expanduser().resolve()
        if not _path_within_any_root(image_path, roots) and not _is_approved_filesystem_path(
            image_path, invocation.trusted_filesystem_selectors
        ):
            return pages, source_images, f"Image path is outside workspace root: {image_path}"
        if not image_path.is_file():
            return pages, source_images, f"Image file not found: {image_path}"
        try:
            pages.append(_image_to_pdf_page(image_path, page_size=page_size))
        except Exception as exc:
            return pages, source_images, f"Could not load image file {image_path}: {exc}"
        source_images.append(str(image_path))
    for text in text_pages:
        pages.append(_text_to_pdf_page(text, title=title, page_size=page_size))
    return pages, source_images, None


def _save_pdf_pages(path: Path, pages: list[object], *, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pages[0].save(
        path,
        "PDF",
        save_all=True,
        append_images=pages[1:],
        resolution=150.0,
        title=title or None,
    )


def _build_pdf_create_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    *,
    include_principal_workspace: bool = True,
) -> ToolHandler:
    resolved_root = Path(workspace_root).resolve() if workspace_root is not None else None
    resolved_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else None

    def handler(invocation: ToolInvocation) -> ToolResult:
        effective_roots = _effective_filesystem_roots(
            invocation=invocation,
            resolved_root=resolved_root,
            resolved_allowed_roots=resolved_allowed_roots,
            include_principal_workspace=include_principal_workspace,
        )
        if not effective_roots:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="pdf_create requires workspace_root or allowed_roots",
            )

        image_paths, image_error = _coerce_string_list(invocation.arguments.get("image_paths"), field="image_paths")
        if image_error is not None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, image_error)
        text_pages, text_error = _coerce_string_list(invocation.arguments.get("text_pages"), field="text_pages")
        if text_error is not None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, text_error)
        if not image_paths and not text_pages:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="pdf_create requires at least one image_paths or text_pages entry",
            )

        title = str(invocation.arguments.get("title") or "Nullion PDF").strip()
        raw_page_size = str(invocation.arguments.get("page_size") or "letter").strip().lower()
        page_size = _PDF_PAGE_SIZES.get(raw_page_size)
        if page_size is None:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Unsupported page_size: {raw_page_size}",
            )

        raw_output_path = invocation.arguments.get("output_path")
        if isinstance(raw_output_path, str) and raw_output_path.strip():
            output_path = Path(raw_output_path).expanduser().resolve()
            if output_path.suffix.lower() != ".pdf":
                output_path = output_path.with_suffix(".pdf")
        else:
            output_path = _pdf_default_output_path(invocation, title=title, roots=effective_roots)

        if not _path_within_any_root(output_path, effective_roots) and not _is_approved_filesystem_path(
            output_path, invocation.trusted_filesystem_selectors
        ):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Output path is outside workspace root: {output_path}",
            )

        pages, source_images, page_error = _build_pdf_pages(
            image_paths=image_paths,
            text_pages=text_pages,
            page_size=page_size,
            roots=effective_roots,
            invocation=invocation,
            title=title,
        )
        if page_error is not None:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=page_error,
            )
        try:
            _save_pdf_pages(output_path, pages, title=title)
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"PDF creation failed: {exc}",
            )
        finally:
            for page in pages:
                try:
                    page.close()
                except Exception:
                    pass

        size_bytes = output_path.stat().st_size if output_path.exists() else 0
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={
                "path": str(output_path),
                "artifact_path": str(output_path),
                "artifact_paths": [str(output_path)],
                "bytes_written": size_bytes,
                "page_count": len(source_images) + len(text_pages),
                "source_image_paths": source_images,
            },
            error=None,
        )

    return handler


def _build_pdf_edit_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    *,
    include_principal_workspace: bool = True,
) -> ToolHandler:
    resolved_root = Path(workspace_root).resolve() if workspace_root is not None else None
    resolved_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else None

    def handler(invocation: ToolInvocation) -> ToolResult:
        try:
            from pypdf import PdfReader, PdfWriter
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"pdf_edit requires the pypdf package: {exc}",
            )

        effective_roots = _effective_filesystem_roots(
            invocation=invocation,
            resolved_root=resolved_root,
            resolved_allowed_roots=resolved_allowed_roots,
            include_principal_workspace=include_principal_workspace,
        )
        if not effective_roots:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "pdf_edit requires workspace_root or allowed_roots")

        raw_input_path = invocation.arguments.get("input_path")
        if not isinstance(raw_input_path, str) or not raw_input_path.strip():
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "Missing required argument: input_path")
        input_path = Path(raw_input_path).expanduser().resolve()
        if not _path_within_any_root(input_path, effective_roots) and not _is_approved_filesystem_path(
            input_path, invocation.trusted_filesystem_selectors
        ):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"Input path is outside workspace root: {input_path}")
        if not input_path.is_file():
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"PDF file not found: {input_path}")

        append_pdf_paths, append_pdf_error = _coerce_string_list(invocation.arguments.get("append_pdf_paths"), field="append_pdf_paths")
        if append_pdf_error is not None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, append_pdf_error)
        append_image_paths, append_image_error = _coerce_string_list(invocation.arguments.get("append_image_paths"), field="append_image_paths")
        if append_image_error is not None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, append_image_error)
        append_text_pages, append_text_error = _coerce_string_list(invocation.arguments.get("append_text_pages"), field="append_text_pages")
        if append_text_error is not None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, append_text_error)

        title = str(invocation.arguments.get("title") or input_path.stem or "Nullion PDF").strip()
        raw_page_size = str(invocation.arguments.get("page_size") or "letter").strip().lower()
        page_size = _PDF_PAGE_SIZES.get(raw_page_size)
        if page_size is None:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"Unsupported page_size: {raw_page_size}")

        raw_output_path = invocation.arguments.get("output_path")
        if isinstance(raw_output_path, str) and raw_output_path.strip():
            output_path = Path(raw_output_path).expanduser().resolve()
            if output_path.suffix.lower() != ".pdf":
                output_path = output_path.with_suffix(".pdf")
        else:
            output_path = _pdf_default_output_path(invocation, title=f"{title}-edited", roots=effective_roots)
        if not _path_within_any_root(output_path, effective_roots) and not _is_approved_filesystem_path(
            output_path, invocation.trusted_filesystem_selectors
        ):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"Output path is outside workspace root: {output_path}")

        raw_page_numbers = invocation.arguments.get("page_numbers")
        if raw_page_numbers is not None and not isinstance(raw_page_numbers, list):
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "page_numbers must be a list of 1-based integers")
        rotate_degrees = invocation.arguments.get("rotate_degrees")
        if rotate_degrees is None:
            rotate_degrees = 0
        if not isinstance(rotate_degrees, int) or rotate_degrees not in {0, 90, 180, 270}:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "rotate_degrees must be one of 0, 90, 180, 270")

        temp_paths: list[Path] = []
        try:
            reader = PdfReader(str(input_path))
            writer = PdfWriter()
            selected_pages = (
                [int(page_number) - 1 for page_number in raw_page_numbers if isinstance(page_number, int)]
                if isinstance(raw_page_numbers, list)
                else list(range(len(reader.pages)))
            )
            if isinstance(raw_page_numbers, list) and len(selected_pages) != len(raw_page_numbers):
                return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "page_numbers must contain only integers")
            for page_index in selected_pages:
                if page_index < 0 or page_index >= len(reader.pages):
                    return ToolResult(
                        invocation.invocation_id,
                        invocation.tool_name,
                        "failed",
                        {},
                        f"page_numbers contains out-of-range page: {page_index + 1}",
                    )
                page = reader.pages[page_index]
                if rotate_degrees:
                    page = page.rotate(rotate_degrees)
                writer.add_page(page)

            for raw_path in append_pdf_paths:
                append_path = Path(raw_path).expanduser().resolve()
                if not _path_within_any_root(append_path, effective_roots) and not _is_approved_filesystem_path(
                    append_path, invocation.trusted_filesystem_selectors
                ):
                    return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"Append PDF path is outside workspace root: {append_path}")
                if not append_path.is_file():
                    return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"Append PDF file not found: {append_path}")
                append_reader = PdfReader(str(append_path))
                for page in append_reader.pages:
                    writer.add_page(page)

            if append_image_paths or append_text_pages:
                pages, _source_images, page_error = _build_pdf_pages(
                    image_paths=append_image_paths,
                    text_pages=append_text_pages,
                    page_size=page_size,
                    roots=effective_roots,
                    invocation=invocation,
                    title=title,
                )
                if page_error is not None:
                    return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, page_error)
                temp_file = tempfile.NamedTemporaryFile(prefix="nullion-pdf-append-", suffix=".pdf", delete=False)
                temp_path = Path(temp_file.name)
                temp_file.close()
                temp_paths.append(temp_path)
                try:
                    _save_pdf_pages(temp_path, pages, title=title)
                finally:
                    for page in pages:
                        try:
                            page.close()
                        except Exception:
                            pass
                append_reader = PdfReader(str(temp_path))
                for page in append_reader.pages:
                    writer.add_page(page)

            if len(writer.pages) == 0:
                return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, "pdf_edit produced no pages")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("wb") as handle:
                writer.write(handle)
            page_count = len(PdfReader(str(output_path)).pages)
        except Exception as exc:
            return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {}, f"PDF edit failed: {exc}")
        finally:
            for temp_path in temp_paths:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass

        size_bytes = output_path.stat().st_size if output_path.exists() else 0
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={
                "path": str(output_path),
                "artifact_path": str(output_path),
                "artifact_paths": [str(output_path)],
                "bytes_written": size_bytes,
                "page_count": page_count,
            },
            error=None,
        )

    return handler



def _build_file_patch_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    *,
    include_principal_workspace: bool = True,
) -> ToolHandler:
    resolved_root = Path(workspace_root).resolve() if workspace_root is not None else None
    resolved_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else None

    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_path = invocation.arguments.get("path")
        raw_old_string = invocation.arguments.get("old_string")
        raw_new_string = invocation.arguments.get("new_string")
        raw_replace_all = invocation.arguments.get("replace_all")
        if not isinstance(raw_path, str) or not raw_path:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: path",
            )
        if not isinstance(raw_old_string, str):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: old_string",
            )
        if raw_old_string == "":
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="old_string must be non-empty",
            )
        if not isinstance(raw_new_string, str):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: new_string",
            )
        if raw_replace_all is not None and not isinstance(raw_replace_all, bool):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="replace_all must be a boolean when provided",
            )

        effective_roots = _effective_filesystem_roots(
            invocation=invocation,
            resolved_root=resolved_root,
            resolved_allowed_roots=resolved_allowed_roots,
            include_principal_workspace=include_principal_workspace,
        )
        if not effective_roots:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="file_patch requires workspace_root or allowed_roots",
            )

        path = Path(raw_path).expanduser().resolve()
        if not _path_within_any_root(path, effective_roots):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Path is outside workspace root: {path}",
            )

        try:
            content = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"File not found: {path}",
            )

        replace_all = bool(raw_replace_all)
        replacement_count = content.count(raw_old_string)
        if replacement_count == 0:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Old string not found in file: {path}",
            )
        if not replace_all and replacement_count != 1:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Old string must match exactly once in file: {path}",
            )

        new_content = content.replace(raw_old_string, raw_new_string) if replace_all else content.replace(raw_old_string, raw_new_string, 1)
        path.write_text(new_content, encoding="utf-8")
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"path": str(path), "replacements": replacement_count if replace_all else 1},
            error=None,
        )

    return handler



def _build_workspace_summary_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    *,
    include_principal_workspace: bool = True,
) -> ToolHandler:
    resolved_root = Path(workspace_root).resolve() if workspace_root is not None else None
    resolved_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else None

    def _resolve_candidate_within_scope(candidate: Path, root: Path) -> Path | None:
        try:
            resolved_candidate = candidate.resolve()
        except OSError:
            return None
        if not _is_within_allowed_root(resolved_candidate, root):
            return None
        return resolved_candidate

    def _display_path(*, root: Path, path: Path) -> str:
        relative_path = path.relative_to(root).as_posix()
        return relative_path

    def handler(invocation: ToolInvocation) -> ToolResult:
        _ = invocation.arguments  # arguments unused; cannot del a slotted dataclass attribute
        roots = _effective_filesystem_roots(
            invocation=invocation,
            resolved_root=resolved_root,
            resolved_allowed_roots=resolved_allowed_roots,
            include_principal_workspace=include_principal_workspace,
        )
        seen_directories: set[Path] = set()
        seen_files: set[Path] = set()
        extensions: Counter[str] = Counter()
        sample_files: list[str] = []

        for root in roots:
            for current_dir, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
                current_path = Path(current_dir)
                scoped_dirnames: list[str] = []
                for dirname in sorted(dirnames):
                    candidate_dir = current_path / dirname
                    if candidate_dir.is_symlink() or _should_prune_filesystem_walk_dir(candidate_dir):
                        continue
                    resolved_dir = _resolve_candidate_within_scope(candidate_dir, root)
                    if resolved_dir is None:
                        continue
                    scoped_dirnames.append(dirname)
                    if resolved_dir not in seen_directories:
                        seen_directories.add(resolved_dir)
                dirnames[:] = scoped_dirnames
                for filename in sorted(filenames):
                    candidate_file = current_path / filename
                    if candidate_file.is_symlink():
                        continue
                    resolved_file = _resolve_candidate_within_scope(candidate_file, root)
                    if resolved_file is None or resolved_file in seen_files:
                        continue
                    seen_files.add(resolved_file)
                    extensions[candidate_file.suffix.lower()] += 1
                    display_path = _display_path(root=root, path=candidate_file)
                    if len(roots) > 1:
                        display_path = f"{root}::{display_path}"
                    sample_files.append(display_path)

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={
                "roots": [str(root) for root in roots],
                "file_count": len(seen_files),
                "directory_count": len(seen_directories),
                "extensions": [
                    {"extension": extension, "count": count}
                    for extension, count in sorted(extensions.items())
                ],
                "sample_files": sorted(sample_files),
            },
            error=None,
        )

    return handler



def _build_terminal_exec_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
) -> ToolHandler:
    cwd = Path(workspace_root).resolve() if workspace_root is not None else None
    execution_cwd = cwd if cwd is not None and cwd.is_dir() else None
    resolved_allowed_roots = (
        tuple(Path(root).expanduser().resolve() for root in allowed_roots)
        if allowed_roots is not None
        else (() if cwd is None else (cwd,))
    )
    backend = terminal_executor_backend or SubprocessTerminalExecutorBackend()

    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_command = invocation.arguments.get("command")
        if not isinstance(raw_command, str) or not raw_command:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: command",
            )

        filesystem_denial = _terminal_filesystem_safety_denial(
            raw_command,
            allowed_roots=resolved_allowed_roots,
        )
        if filesystem_denial is not None:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=filesystem_denial,
                error=str(filesystem_denial.get("message") or "Filesystem traversal denied"),
            )

        egress_attempts = _egress_attempts_for_invocation(invocation)
        raw_network_mode = invocation.arguments.get("network_mode")
        network_mode = _normalize_network_mode(raw_network_mode)
        if raw_network_mode is not None and network_mode not in _VALID_NETWORK_MODES:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={
                    "reason": "invalid_network_mode",
                    "network_mode": raw_network_mode,
                    "allowed_network_modes": sorted(_VALID_NETWORK_MODES),
                },
                error=f"Invalid terminal execution network mode: {raw_network_mode}",
            )
        approved_targets = invocation.arguments.get("approved_targets")
        policy = TerminalExecutionPolicy(
            network_mode=network_mode,
            approved_targets=tuple(target for target in approved_targets if isinstance(target, str))
            if isinstance(approved_targets, (list, tuple, set, frozenset))
            else (),
        )
        has_network_egress = bool(egress_attempts)

        if _is_restrictive_network_mode(network_mode):
            backend_descriptor = backend.describe()
            required_attested_capabilities = _required_attested_capabilities_for_network_mode(network_mode)
            if backend_descriptor.mode != "subprocess":
                missing_capabilities = _missing_backend_attested_capabilities(
                    backend_descriptor,
                    required_capabilities=required_attested_capabilities,
                )
                verification = verify_terminal_backend_attestation(
                    backend_descriptor,
                    required_capabilities=required_attested_capabilities,
                    verifier=terminal_attestation_verifier,
                )
                if missing_capabilities:
                    return ToolResult(
                        invocation_id=invocation.invocation_id,
                        tool_name=invocation.tool_name,
                        status="failed",
                        output={
                            "reason": "backend_attestation_missing",
                            "network_mode": network_mode,
                            "required_attested_capabilities": list(required_attested_capabilities),
                            "backend": {
                                "mode": backend_descriptor.mode,
                                "attested_capabilities": list(backend_descriptor.attested_capabilities),
                            },
                        },
                        error=(
                            "Restrictive terminal execution requires backend attestation: "
                            + ", ".join(missing_capabilities)
                        ),
                    )
                if not verification.is_valid:
                    failure_reason = verification.failure_reason or "verification failed"
                    backend_output: dict[str, object] = {
                        "mode": backend_descriptor.mode,
                        "attested_capabilities": list(backend_descriptor.attested_capabilities),
                    }
                    if backend_descriptor.evidence is not None:
                        backend_output["evidence_format"] = backend_descriptor.evidence.format
                    return ToolResult(
                        invocation_id=invocation.invocation_id,
                        tool_name=invocation.tool_name,
                        status="failed",
                        output={
                            "reason": "backend_attestation_invalid",
                            "network_mode": network_mode,
                            "required_attested_capabilities": list(required_attested_capabilities),
                            "backend": backend_output,
                            "attestation_verification": {
                                "is_valid": verification.is_valid,
                                "failure_reason": verification.failure_reason,
                                "metadata": dict(verification.metadata),
                            },
                        },
                        error=(
                            "Restrictive terminal execution requires verified backend attestation: "
                            + failure_reason
                        ),
                    )

        if has_network_egress and not all(
            _network_attempt_allowed(
                attempt=attempt,
                network_mode=network_mode,
                approved_targets=approved_targets,
            )
            for attempt in egress_attempts
        ):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"reason": "network_denied", "network_mode": network_mode, "egress_attempts": egress_attempts},
                error="Network egress denied by terminal execution policy",
            )

        try:
            completed = backend.run(
                raw_command,
                cwd=str(execution_cwd) if execution_cwd is not None else None,
                timeout=20,
                policy=policy,
            )
        except subprocess.TimeoutExpired:
            output = {"egress_attempts": egress_attempts} if has_network_egress else {}
            if network_mode is not None:
                output["network_mode"] = network_mode
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=output,
                error="Command timed out",
            )

        output = {"stdout": completed.stdout, "stderr": completed.stderr, "exit_code": completed.exit_code}
        if network_mode is not None:
            output["network_mode"] = network_mode
        if has_network_egress:
            output["egress_attempts"] = egress_attempts
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed" if completed.exit_code == 0 else "failed",
            output=output,
            error=None if completed.exit_code == 0 else f"Command failed with exit code {completed.exit_code}",
        )

    return handler



def _http_retry_url_for_https_transport_failure(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port is not None:
        return None
    return parsed._replace(scheme="http").geturl()


def _is_https_transport_eof(exc: Exception) -> bool:
    reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
    if isinstance(reason, ssl.SSLEOFError):
        return True
    if isinstance(reason, ssl.SSLError):
        library, reason_text = reason.args[:2] if len(reason.args) >= 2 else ("", "")
        return "UNEXPECTED_EOF_WHILE_READING" in str(library) or "UNEXPECTED_EOF_WHILE_READING" in str(reason_text)
    return False


def _fetch_web_url_once(url: str, timeout_seconds: int) -> dict[str, object]:
    resolution = _resolve_web_fetch_resolution(url)
    parsed = urlparse(url)
    host = parsed.hostname
    if (
        parsed.scheme not in {"http", "https"}
        or not isinstance(host, str)
        or not host
        or (resolution is None and not _is_global_literal_ip(host))
    ):
        raise ValueError(f"Blocked URL for web_fetch: {url}")
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    opener = _build_web_fetch_opener()
    with _pinned_web_fetch_resolution(resolution):
        with opener.open(request, timeout=timeout_seconds) as response:
            status_code = getattr(response, "status", 200)
            content_type = response.headers.get_content_type()
            body = response.read(65536).decode("utf-8", "ignore")  # 64 KB
    title_match = re.search(r"<title[^>]*>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
    title = None if title_match is None else re.sub(r"\s+", " ", unescape(title_match.group(1))).strip() or None
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", body)).strip()
    return {
        "url": url,
        "status_code": status_code,
        "content_type": content_type,
        "title": title,
        "text": text,
        "body": body,
    }


def _default_web_fetcher(url: str, timeout_seconds: int) -> dict[str, object]:
    try:
        return _fetch_web_url_once(url, timeout_seconds)
    except Exception as exc:
        retry_url = _http_retry_url_for_https_transport_failure(url)
        if retry_url is None or not _is_https_transport_eof(exc):
            raise
        response = _fetch_web_url_once(retry_url, timeout_seconds)
        response["requested_url"] = url
        response["transport_fallback"] = {
            "from_scheme": "https",
            "to_scheme": "http",
            "reason": "https_transport_eof",
        }
        return response



def _build_kernel_tool_registry(
    *,
    plugin_registration_allowed: bool,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
) -> ToolRegistry:
    filesystem_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else (() if workspace_root is None else (Path(workspace_root).resolve(),))
    registry = ToolRegistry(
        plugin_registration_allowed=plugin_registration_allowed,
        filesystem_allowed_roots=filesystem_allowed_roots,
    )
    if allowed_roots is not None:
        workspace_root = None
    if _env_flag("NULLION_FILE_ACCESS_ENABLED"):
        registry.register(
            ToolSpec(
                name="file_read",
                description="Read a local file inside the workspace.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_file_read_handler(
                None
                if workspace_root is None
                else Path(workspace_root),
                allowed_roots=[Path(root) for root in allowed_roots] if allowed_roots is not None else None,
            ),
        )
        registry.register(
            ToolSpec(
                name="file_write",
                description="Write a local file inside the workspace.",
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_file_write_handler(
                None
                if workspace_root is None
                else Path(workspace_root),
                allowed_roots=[Path(root) for root in allowed_roots] if allowed_roots is not None else None,
            ),
        )
        registry.register(
            ToolSpec(
                name="pdf_create",
                description=(
                    "Create a real PDF artifact locally from existing image files and/or simple text pages. "
                    "Use this for packaging images, reports, or notes into a PDF. "
                    "Prefer this over terminal_exec or installing command-line PDF tools."
                ),
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=30,
            ),
            _build_pdf_create_handler(
                None
                if workspace_root is None
                else Path(workspace_root),
                allowed_roots=[Path(root) for root in allowed_roots] if allowed_roots is not None else None,
            ),
        )
        registry.register(
            ToolSpec(
                name="pdf_edit",
                description=(
                    "Edit a PDF locally into a new PDF artifact: keep/reorder pages, rotate pages, "
                    "append other PDFs, append image pages, or append text pages. "
                    "Prefer this over terminal_exec or installing command-line PDF tools."
                ),
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=30,
            ),
            _build_pdf_edit_handler(
                None
                if workspace_root is None
                else Path(workspace_root),
                allowed_roots=[Path(root) for root in allowed_roots] if allowed_roots is not None else None,
            ),
        )
    if _env_flag("NULLION_TERMINAL_ENABLED"):
        registry.register(
            ToolSpec(
                name="terminal_exec",
                description="Execute a local shell command.",
                risk_level=ToolRiskLevel.HIGH,
                side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
                requires_approval=True,
                timeout_seconds=20,
            ),
            _build_terminal_exec_handler(
                workspace_root,
                allowed_roots=[Path(root) for root in allowed_roots] if allowed_roots is not None else None,
                terminal_executor_backend=terminal_executor_backend,
                terminal_attestation_verifier=terminal_attestation_verifier,
            ),
        )
    if _env_flag("NULLION_WEB_ACCESS_ENABLED"):
        registry.register(
            ToolSpec(
                name="web_fetch",
                description=(
                    "Fetch a URL and return its content and response metadata. "
                    "Follows HTTP redirects automatically (up to 5 hops). "
                    "Always prefer this over terminal_exec curl for HTTP/HTTPS requests."
                ),
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_web_fetch_handler(_default_web_fetcher),
        )
    if _connector_access_enabled():
        register_connector_plugin(registry)
    if _env_flag("NULLION_SKILL_PACK_ACCESS_ENABLED", default=False):
        registry.register(
            ToolSpec(
                name="skill_pack_read",
                description=(
                    "Read an installed skill pack reference file by pack id and relative path. "
                    "Use this when an enabled skill pack lists service-specific API reference docs."
                ),
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_skill_pack_read_handler(),
        )
    return registry



def create_core_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
) -> ToolRegistry:
    return _build_kernel_tool_registry(
        plugin_registration_allowed=False,
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
        terminal_executor_backend=terminal_executor_backend,
        terminal_attestation_verifier=terminal_attestation_verifier,
    )



def create_plugin_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
) -> ToolRegistry:
    return _build_kernel_tool_registry(
        plugin_registration_allowed=True,
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
        terminal_executor_backend=terminal_executor_backend,
        terminal_attestation_verifier=terminal_attestation_verifier,
    )



def create_extension_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
) -> ToolRegistry:
    return create_plugin_tool_registry(
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
        terminal_executor_backend=terminal_executor_backend,
        terminal_attestation_verifier=terminal_attestation_verifier,
    )



def create_default_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
) -> ToolRegistry:
    return create_core_tool_registry(
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
        terminal_executor_backend=terminal_executor_backend,
        terminal_attestation_verifier=terminal_attestation_verifier,
    )



def build_default_tool_registry(
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    terminal_executor_backend: TerminalExecutorBackend | None = None,
    terminal_attestation_verifier: TerminalAttestationVerifier | None = None,
) -> ToolRegistry:
    return create_core_tool_registry(
        workspace_root=workspace_root,
        allowed_roots=allowed_roots,
        terminal_executor_backend=terminal_executor_backend,
        terminal_attestation_verifier=terminal_attestation_verifier,
    )


def _build_web_fetch_handler(
    web_fetcher: Callable[[str, int], dict[str, object]],
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_url = invocation.arguments.get("url")
        if not isinstance(raw_url, str) or not raw_url:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: url",
            )

        try:
            response = web_fetcher(raw_url, 20)
        except Exception as exc:  # pragma: no cover - caller-provided fetcher guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=str(exc),
            )

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output=response,
            error=None,
        )

    return handler


def _connector_provider_env_prefix(provider_id: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", provider_id.upper()).strip("_")


def _connector_credential_candidate_names(connection: object | None, provider_id: str) -> tuple[str, ...]:
    candidates: list[str] = []
    credential_ref = getattr(connection, "credential_ref", None)
    if isinstance(credential_ref, str) and credential_ref.strip():
        candidates.append(credential_ref.strip().removeprefix("env:"))
    prefix = _connector_provider_env_prefix(provider_id)
    if prefix:
        candidates.extend((f"{prefix}_API_KEY", f"{prefix}_TOKEN", f"{prefix}_SECRET_KEY"))
        for suffix in ("_CONNECTOR_PROVIDER", "_CONNECTOR"):
            if prefix.endswith(suffix):
                gateway_prefix = prefix.removesuffix(suffix).strip("_")
                if gateway_prefix:
                    candidates.extend(
                        (
                            f"{gateway_prefix}_API_KEY",
                            f"{gateway_prefix}_TOKEN",
                            f"{gateway_prefix}_SECRET_KEY",
                        )
                    )
        if prefix.startswith("SKILL_PACK_CONNECTOR_"):
            gateway_prefix = prefix.removeprefix("SKILL_PACK_CONNECTOR_").strip("_")
            if gateway_prefix:
                candidates.extend(
                    (
                        f"{gateway_prefix}_API_KEY",
                        f"{gateway_prefix}_TOKEN",
                        f"{gateway_prefix}_SECRET_KEY",
                    )
                )
    return tuple(dict.fromkeys(candidates))


def register_connector_plugin(registry: ToolRegistry) -> None:
    try:
        registry.get_spec("connector_request")
        return
    except KeyError:
        pass
    registry.mark_plugin_installed("connector_plugin")
    registry.register(
        ToolSpec(
            name="connector_request",
            description=(
                "Make an HTTP request to an installed connector/API gateway using the current workspace's "
                "configured connection credential. GET/HEAD are always read-only; POST, PUT, PATCH, and "
                "DELETE require that connection's permission mode to be read_write. Use enabled connector "
                "skill instructions and never reveal the credential value."
            ),
            risk_level=ToolRiskLevel.HIGH,
            side_effect_class=ToolSideEffectClass.ACCOUNT_WRITE,
            requires_approval=False,
            timeout_seconds=20,
        ),
        _build_connector_request_handler(),
    )


def _connector_connection_for_invocation(invocation: ToolInvocation, provider_id: str):
    from .connections import require_workspace_connection_for_principal

    return require_workspace_connection_for_principal(invocation.principal_id, provider_id)


def _connector_credential_value(connection: object | None, provider_id: str) -> str:
    candidates = _connector_credential_candidate_names(connection, provider_id)
    for name in candidates:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    labels = " or ".join(candidates) or "a workspace credential_ref"
    raise RuntimeError(f"{provider_id} requires {labels}")


def _connector_request_headers(connection: object | None, provider_id: str) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {_connector_credential_value(connection, provider_id)}",
        "User-Agent": "Nullion connector_request/0.1",
    }
    return headers


_CONNECTOR_READ_METHODS = {"GET", "HEAD"}
_CONNECTOR_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_CONNECTOR_ALLOWED_METHODS = _CONNECTOR_READ_METHODS | _CONNECTOR_WRITE_METHODS


def _connector_request_method(raw_method: object) -> str:
    method = str(raw_method or "GET").strip().upper() or "GET"
    if method not in _CONNECTOR_ALLOWED_METHODS:
        allowed = ", ".join(sorted(_CONNECTOR_ALLOWED_METHODS))
        raise ValueError(f"Unsupported connector_request method: {method}. Allowed methods: {allowed}")
    return method


def _connector_connection_allows_write(connection: object | None) -> bool:
    mode = str(getattr(connection, "permission_mode", "read") or "read").strip().lower().replace("-", "_")
    return mode in {"write", "read_write", "readwrite", "rw", "read_and_write"}


def _connector_request_payload(invocation: ToolInvocation, method: str) -> tuple[bytes | None, dict[str, str]]:
    if method in _CONNECTOR_READ_METHODS:
        return None, {}
    headers: dict[str, str] = {}
    if "json" in invocation.arguments and invocation.arguments.get("json") is not None:
        headers["Content-Type"] = "application/json"
        return json.dumps(invocation.arguments.get("json")).encode("utf-8"), headers
    raw_body = invocation.arguments.get("body")
    if raw_body is None:
        return None, {}
    headers["Content-Type"] = "text/plain; charset=utf-8"
    return str(raw_body).encode("utf-8"), headers


def _connector_base_url_candidate_names(connection: object | None, provider_id: str) -> tuple[str, ...]:
    names: list[str] = []
    credential_ref = getattr(connection, "credential_ref", None)
    if isinstance(credential_ref, str) and credential_ref.strip():
        ref = credential_ref.strip().removeprefix("env:")
        if ref.endswith(("_API_KEY", "_TOKEN", "_SECRET_KEY")):
            names.append(re.sub(r"_(API_KEY|TOKEN|SECRET_KEY)$", "_BASE_URL", ref))
    prefix = _connector_provider_env_prefix(provider_id)
    if prefix:
        names.append(f"{prefix}_BASE_URL")
        for suffix in ("_CONNECTOR_PROVIDER", "_CONNECTOR"):
            if prefix.endswith(suffix):
                gateway_prefix = prefix.removesuffix(suffix).strip("_")
                if gateway_prefix:
                    names.append(f"{gateway_prefix}_BASE_URL")
        if prefix.startswith("SKILL_PACK_CONNECTOR_"):
            gateway_prefix = prefix.removeprefix("SKILL_PACK_CONNECTOR_").strip("_")
            if gateway_prefix:
                names.append(f"{gateway_prefix}_BASE_URL")
    return tuple(dict.fromkeys(names))


def _connector_allowed_base_urls(connection: object | None, provider_id: str) -> tuple[str, ...]:
    urls: list[str] = []
    provider_profile = getattr(connection, "provider_profile", None)
    if isinstance(provider_profile, str) and provider_profile.strip().lower().startswith(("http://", "https://")):
        urls.append(provider_profile.strip())
    for name in _connector_base_url_candidate_names(connection, provider_id):
        raw = os.environ.get(name, "").strip()
        if raw.lower().startswith(("http://", "https://")):
            urls.append(raw)
    urls.extend(_connector_skill_pack_base_urls(provider_id))
    normalized: list[str] = []
    for url in urls:
        parsed = urlparse(url)
        if parsed.scheme in {"http", "https"} and parsed.hostname:
            normalized.append(url.rstrip("/") + "/")
    return tuple(dict.fromkeys(normalized))


def _connector_skill_pack_base_urls(provider_id: str) -> tuple[str, ...]:
    pack_id = _installed_connector_skill_pack_id_for_provider(provider_id)
    if not pack_id:
        return ()
    try:
        from nullion.skill_pack_installer import get_installed_skill_pack

        pack = get_installed_skill_pack(pack_id)
    except Exception:
        pack = None
    pack_path = Path(str(getattr(pack, "path", "") or ""))
    if not pack_path.exists():
        return ()
    urls: list[str] = []
    for skill_file in sorted(pack_path.rglob("SKILL.md")):
        try:
            text = skill_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        trusted_hosts: set[str] = set()
        base_context_lines = 0
        for line in text.splitlines():
            normalized_line = line.lower()
            starts_base_context = (
                "base url" in normalized_line
                or "base_url" in normalized_line
                or "endpoint base" in normalized_line
            )
            if starts_base_context:
                base_context_lines = 8
            is_base_line = starts_base_context or base_context_lines > 0
            for match in re.finditer(r"https?://[^\s`'\"<>)]+", line):
                raw_url = match.group(0).rstrip(".,;:")
                parsed = urlparse(raw_url)
                if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                    continue
                if any(marker in parsed.hostname for marker in ("{", "}", "<", ">")):
                    continue
                path = parsed.path or "/"
                path_has_template = "{" in path or "<" in path
                host_key = parsed.netloc.lower()
                if not is_base_line and not (path_has_template and host_key in trusted_hosts):
                    continue
                cut_positions = [pos for marker in ("{", "<") if (pos := path.find(marker)) >= 0]
                if cut_positions:
                    path = path[: min(cut_positions)]
                urls.append(f"{parsed.scheme}://{parsed.netloc}{path.rstrip('/')}/")
                trusted_hosts.add(host_key)
            if base_context_lines > 0:
                base_context_lines -= 1
    return tuple(dict.fromkeys(urls))


def _url_is_under_base(url: str, base_url: str) -> bool:
    parsed = urlparse(url)
    base = urlparse(base_url)
    if parsed.scheme != base.scheme or parsed.hostname != base.hostname:
        return False
    parsed_port = parsed.port or (443 if parsed.scheme == "https" else 80)
    base_port = base.port or (443 if base.scheme == "https" else 80)
    if parsed_port != base_port:
        return False
    base_path = (base.path or "/").rstrip("/") + "/"
    path = (parsed.path or "/").rstrip("/") + "/"
    return path.startswith(base_path)


def _connector_request_url(raw_url: str, raw_params: object, connection: object | None = None, provider_id: str = "") -> str:
    url = raw_url.strip()
    parsed = urlparse(url)
    host = parsed.hostname
    resolution = _resolve_web_fetch_resolution(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not isinstance(host, str)
        or not host
        or (resolution is None and not _is_global_literal_ip(host))
    ):
        raise ValueError(f"Blocked URL for connector_request: {url}")
    params: dict[str, str] = {}
    if isinstance(raw_params, dict):
        for key, value in raw_params.items():
            if value is None:
                continue
            params[str(key)] = str(value)
    if params:
        separator = "&" if parsed.query else "?"
        url += separator + urlencode(params)
    allowed_bases = _connector_allowed_base_urls(connection, provider_id)
    if allowed_bases and not any(_url_is_under_base(url, base_url) for base_url in allowed_bases):
        labels = ", ".join(allowed_bases)
        raise ValueError(f"Blocked URL for connector_request: {url} is not under configured connector base URL(s): {labels}")
    return url


def _build_connector_request_handler() -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        provider_id = str(invocation.arguments.get("provider_id") or "").strip()
        raw_url = invocation.arguments.get("url")
        if not provider_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: provider_id",
            )
        if not isinstance(raw_url, str) or not raw_url.strip():
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: url",
            )
        try:
            connection = _connector_connection_for_invocation(invocation, provider_id)
            method = _connector_request_method(invocation.arguments.get("method"))
            if method not in _CONNECTOR_READ_METHODS and not _connector_connection_allows_write(connection):
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output={"provider_id": provider_id, "method": method, "permission_mode": "read"},
                    error=(
                        f"{provider_id} is configured as read-only for connector_request. "
                        "Change this connection's permission mode to read_write in Settings > Users > Connections "
                        "before using POST, PUT, PATCH, or DELETE."
                    ),
                )
            url = _connector_request_url(raw_url, invocation.arguments.get("params"), connection, provider_id)
            resolution = _resolve_web_fetch_resolution(url)
            payload, payload_headers = _connector_request_payload(invocation, method)
            headers = _connector_request_headers(connection, provider_id)
            headers.update(payload_headers)
            request = urllib.request.Request(
                url,
                data=payload,
                headers=headers,
                method=method,
            )
            opener = _build_web_fetch_opener()
            with _pinned_web_fetch_resolution(resolution):
                with opener.open(request, timeout=20) as response:
                    status_code = getattr(response, "status", 200)
                    content_type = response.headers.get_content_type()
                    body = response.read(1_000_000).decode("utf-8", "ignore")
            output: dict[str, object] = {
                "url": url,
                "provider_id": provider_id,
                "method": method,
                "status_code": status_code,
                "content_type": content_type,
            }
            try:
                output["json"] = json.loads(body)
            except Exception:
                output["text"] = body[:20000]
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output=output,
                error=None,
            )
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"provider_id": provider_id},
                error=str(exc),
            )

    return handler


def _build_skill_pack_read_handler() -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        pack_id = str(invocation.arguments.get("pack_id") or "").strip()
        path = str(invocation.arguments.get("path") or "").strip()
        if not pack_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: pack_id",
            )
        if not path:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"pack_id": pack_id},
                error="Missing required argument: path",
            )
        try:
            from .skill_pack_installer import read_skill_pack_reference

            text = read_skill_pack_reference(pack_id, path)
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"pack_id": pack_id, "path": path},
                error=str(exc),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"pack_id": pack_id, "path": path, "text": text},
            error=None,
        )

    return handler


def _build_web_search_handler(
    web_searcher: Callable[[str, int], list[dict[str, object]]],
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_query = invocation.arguments.get("query")
        raw_limit = invocation.arguments.get("limit", 5)
        if not isinstance(raw_query, str) or not raw_query:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: query",
            )

        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Invalid argument: limit",
            )
        if limit < 1:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"limit must be at least 1, got {limit}",
            )

        try:
            results = web_searcher(raw_query, limit)
        except Exception as exc:  # pragma: no cover - caller-provided searcher guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=str(exc),
            )

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"query": raw_query, "results": results},
            error=None,
        )

    return handler



def _build_file_search_handler(
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
    *,
    include_principal_workspace: bool = True,
) -> ToolHandler:
    resolved_root = Path(workspace_root).resolve() if workspace_root is not None else None
    resolved_allowed_roots = tuple(Path(root).resolve() for root in allowed_roots) if allowed_roots is not None else None

    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_pattern = invocation.arguments.get("pattern")
        if not isinstance(raw_pattern, str) or not raw_pattern:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: pattern",
            )

        search_roots = _effective_filesystem_roots(
            invocation=invocation,
            resolved_root=resolved_root,
            resolved_allowed_roots=resolved_allowed_roots,
            include_principal_workspace=include_principal_workspace,
        )
        if not search_roots:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="file_search requires workspace_root or allowed_roots",
            )

        raw_limit = invocation.arguments.get("limit")
        limit = 100
        if isinstance(raw_limit, int) and raw_limit > 0:
            limit = min(raw_limit, 500)

        pattern = raw_pattern.lower()
        matches: list[str] = []
        for root in search_roots:
            for current_dir, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
                current_path = Path(current_dir)
                scoped_dirnames: list[str] = []
                for dirname in sorted(dirnames):
                    candidate_dir = current_path / dirname
                    if candidate_dir.is_symlink() or _should_prune_filesystem_walk_dir(candidate_dir):
                        continue
                    try:
                        resolved_dir = candidate_dir.resolve()
                    except OSError:
                        continue
                    if not _path_within_any_root(resolved_dir, search_roots):
                        continue
                    if resolved_root is not None and not _is_within_allowed_root(resolved_dir, resolved_root):
                        continue
                    scoped_dirnames.append(dirname)
                dirnames[:] = scoped_dirnames

                for filename in sorted(filenames):
                    if pattern not in filename.lower():
                        continue
                    candidate_file = current_path / filename
                    if candidate_file.is_symlink():
                        continue
                    try:
                        if not candidate_file.is_file():
                            continue
                        resolved_path = candidate_file.resolve()
                    except OSError:
                        continue
                    if not _path_within_any_root(resolved_path, search_roots):
                        continue
                    if resolved_root is not None and not _is_within_allowed_root(resolved_path, resolved_root):
                        continue
                    matches.append(str(resolved_path))
                    if len(matches) >= limit:
                        break
                if len(matches) >= limit:
                    break
            if len(matches) >= limit:
                break

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"matches": matches},
            error=None,
        )

    return handler


def register_search_plugin(
    registry: ToolRegistry,
    *,
    web_fetcher: Callable[[str, int], dict[str, object]] | None = None,
    web_searcher: Callable[[str, int], list[dict[str, object]]] | None = None,
) -> ToolRegistry:
    del web_fetcher
    registry.require_plugin_registration_allowed()
    registry.mark_plugin_installed("search_plugin")
    if web_searcher is not None:
        try:
            registry.get_spec("web_search")
        except KeyError:
            registry.register(
                ToolSpec(
                    name="web_search",
                    description=(
                        "Search the web and return a ranked list of result links + snippets. "
                        "PREFER THIS for any 'find', 'look up', 'search for', 'what is', "
                        "'who is', 'latest', 'news', 'today', or open-ended information request "
                        "that doesn't already have a specific URL. This is fast, cheap, and "
                        "returns multiple sources you can compare. Only fall back to "
                        "browser_navigate or web_fetch when you already have a specific URL "
                        "or when search snippets aren't enough detail."
                    ),
                    risk_level=ToolRiskLevel.LOW,
                    side_effect_class=ToolSideEffectClass.READ,
                    requires_approval=False,
                    timeout_seconds=20,
                ),
                _build_web_search_handler(web_searcher),
            )
    return registry



def register_web_extension(
    registry: ToolRegistry,
    *,
    web_fetcher: Callable[[str, int], dict[str, object]],
    web_searcher: Callable[[str, int], list[dict[str, object]]] | None = None,
) -> ToolRegistry:
    return register_search_plugin(
        registry,
        web_fetcher=web_fetcher,
        web_searcher=web_searcher,
    )



def register_email_plugin(
    registry: ToolRegistry,
    *,
    email_searcher: Callable[[str, int], list[dict[str, object]]] | None = None,
    email_reader: Callable[[str], dict[str, object]] | None = None,
) -> ToolRegistry:
    registry.require_plugin_registration_allowed()
    registry.mark_plugin_installed("email_plugin")
    if email_searcher is None:
        raise ValueError("email_plugin requires email_searcher")
    try:
        registry.get_spec("email_search")
    except KeyError:
        registry.register(
            ToolSpec(
                name="email_search",
                description="Search email messages via the configured provider.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_email_search_handler(email_searcher),
        )
    if email_reader is not None:
        try:
            registry.get_spec("email_read")
        except KeyError:
            registry.register(
                ToolSpec(
                    name="email_read",
                    description="Read a single email message via the configured provider.",
                    risk_level=ToolRiskLevel.LOW,
                    side_effect_class=ToolSideEffectClass.READ,
                    requires_approval=False,
                    timeout_seconds=20,
                ),
                _build_email_read_handler(email_reader),
            )
    return registry



def register_calendar_plugin(
    registry: ToolRegistry,
    *,
    calendar_lister: Callable[[str, str, int], list[dict[str, object]]] | None = None,
) -> ToolRegistry:
    registry.require_plugin_registration_allowed()
    registry.mark_plugin_installed("calendar_plugin")
    if calendar_lister is None:
        raise ValueError("calendar_plugin requires calendar_lister")
    try:
        registry.get_spec("calendar_list")
    except KeyError:
        registry.register(
            ToolSpec(
                name="calendar_list",
                description="List calendar events via the configured provider.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_calendar_list_handler(calendar_lister),
        )
    return registry



def _build_email_search_handler(
    email_searcher: Callable[[str, int], list[dict[str, object]]],
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_query = invocation.arguments.get("query")
        raw_limit = invocation.arguments.get("limit", 10)
        if not isinstance(raw_query, str) or not raw_query:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: query",
            )
        if not isinstance(raw_limit, int) or raw_limit <= 0:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="limit must be a positive integer",
            )
        try:
            results = _call_provider_with_principal(email_searcher, raw_query, raw_limit, principal_id=invocation.principal_id)
        except Exception as exc:  # pragma: no cover - provider guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=_account_tool_failure_output(invocation.principal_id, query=raw_query),
                error=str(exc),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"query": raw_query, "results": results},
            error=None,
        )

    return handler



def _build_email_read_handler(
    email_reader: Callable[[str], dict[str, object]],
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_id = invocation.arguments.get("id")
        if not isinstance(raw_id, str) or not raw_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: id",
            )
        try:
            message = _call_provider_with_principal(email_reader, raw_id, principal_id=invocation.principal_id)
        except Exception as exc:  # pragma: no cover - provider guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=_account_tool_failure_output(invocation.principal_id, message_id=raw_id),
                error=str(exc),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"id": raw_id, "message": message},
            error=None,
        )

    return handler


def _call_provider_with_principal(provider: Callable, *args, principal_id: str):
    try:
        parameters = inspect.signature(provider).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "principal_id" in parameters:
        return provider(*args, principal_id=principal_id)
    return provider(*args)



def _build_calendar_list_handler(
    calendar_lister: Callable[[str, str, int], list[dict[str, object]]],
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_start = invocation.arguments.get("start")
        raw_end = invocation.arguments.get("end")
        raw_max = invocation.arguments.get("max", 10)
        if not isinstance(raw_start, str) or not raw_start:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: start",
            )
        if not isinstance(raw_end, str) or not raw_end:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: end",
            )
        if not isinstance(raw_max, int) or raw_max <= 0:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="max must be a positive integer",
            )
        try:
            results = calendar_lister(raw_start, raw_end, raw_max)
        except Exception as exc:  # pragma: no cover - provider guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=_account_tool_failure_output(invocation.principal_id),
                error=str(exc),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"start": raw_start, "end": raw_end, "max": raw_max, "results": results},
            error=None,
        )

    return handler


def _missing_media_provider_result(invocation: ToolInvocation, capability: str) -> ToolResult:
    return ToolResult(
        invocation_id=invocation.invocation_id,
        tool_name=invocation.tool_name,
        status="failed",
        output={
            "reason": "provider_not_configured",
            "capability": capability,
            "setup": (
                "Enable media_plugin with a local provider binding, then configure "
                "NULLION_AUDIO_TRANSCRIBE_COMMAND, NULLION_IMAGE_OCR_COMMAND, or "
                "NULLION_IMAGE_GENERATE_COMMAND as needed."
            ),
        },
        error=f"{capability} provider is not configured",
    )


def _build_audio_transcribe_handler(
    audio_transcriber: Callable[[str, str | None], dict[str, object]] | None,
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_path = invocation.arguments.get("path")
        raw_language = invocation.arguments.get("language")
        if not isinstance(raw_path, str) or not raw_path:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: path",
            )
        if raw_language is not None and not isinstance(raw_language, str):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="language must be a string when provided",
            )
        if audio_transcriber is None:
            return _missing_media_provider_result(invocation, "audio_transcribe")
        try:
            payload = audio_transcriber(raw_path, raw_language)
        except Exception as exc:  # pragma: no cover - provider guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=str(exc),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"path": raw_path, **payload},
            error=None,
        )

    return handler


def _build_image_extract_text_handler(
    image_text_extractor: Callable[[str], dict[str, object]] | None,
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_path = invocation.arguments.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: path",
            )
        if image_text_extractor is None:
            return _missing_media_provider_result(invocation, "image_extract_text")
        try:
            payload = image_text_extractor(raw_path)
        except Exception as exc:  # pragma: no cover - provider guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=str(exc),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"path": raw_path, **payload},
            error=None,
        )

    return handler


def _build_image_generate_handler(
    image_generator: Callable[..., dict[str, object]] | None,
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_prompt = invocation.arguments.get("prompt")
        raw_output_path = invocation.arguments.get("output_path")
        raw_size = invocation.arguments.get("size")
        raw_source_path = invocation.arguments.get("source_path")
        if not isinstance(raw_prompt, str) or not raw_prompt.strip():
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: prompt",
            )
        if not isinstance(raw_output_path, str) or not raw_output_path.strip():
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: output_path",
            )
        if raw_size is not None and not isinstance(raw_size, str):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="size must be a string when provided",
            )
        if raw_source_path is not None and not isinstance(raw_source_path, str):
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="source_path must be a string when provided",
            )
        if image_generator is None:
            return _missing_media_provider_result(invocation, "image_generate")
        try:
            if raw_source_path:
                try:
                    accepts_source = len(inspect.signature(image_generator).parameters) >= 4
                except (TypeError, ValueError):
                    accepts_source = False
                if accepts_source:
                    payload = image_generator(raw_prompt, raw_output_path, raw_size, raw_source_path)
                else:
                    payload = image_generator(raw_prompt, raw_output_path, raw_size)
            else:
                payload = image_generator(raw_prompt, raw_output_path, raw_size)
        except Exception as exc:  # pragma: no cover - provider guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=str(exc),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"output_path": raw_output_path, **payload},
            error=None,
        )

    return handler


def register_media_plugin(
    registry: ToolRegistry,
    *,
    audio_transcriber: Callable[[str, str | None], dict[str, object]] | None = None,
    image_text_extractor: Callable[[str], dict[str, object]] | None = None,
    image_generator: Callable[[str, str, str | None], dict[str, object]] | None = None,
) -> ToolRegistry:
    registry.require_plugin_registration_allowed()
    registry.mark_plugin_installed("media_plugin")
    try:
        registry.get_spec("audio_transcribe")
    except KeyError:
        registry.register(
            ToolSpec(
                name="audio_transcribe",
                description="Transcribe a local audio file to text using the configured local media provider.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=60,
                filesystem_boundary_policy=_FILESYSTEM_BOUNDARY_TRUSTED_ROOTS_ONLY,
            ),
            _build_audio_transcribe_handler(audio_transcriber),
        )
    try:
        registry.get_spec("image_extract_text")
    except KeyError:
        registry.register(
            ToolSpec(
                name="image_extract_text",
                description="Extract visible text from a local image using the configured local OCR provider.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=60,
                filesystem_boundary_policy=_FILESYSTEM_BOUNDARY_TRUSTED_ROOTS_ONLY,
            ),
            _build_image_extract_text_handler(image_text_extractor),
        )
    try:
        registry.get_spec("image_generate")
    except KeyError:
        registry.register(
            ToolSpec(
                name="image_generate",
                description="Generate an image file with the configured local image generation provider.",
                risk_level=ToolRiskLevel.MEDIUM,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=120,
            ),
            _build_image_generate_handler(image_generator),
        )
    return registry



def _build_browser_navigate_handler(
    browser_navigator: Callable[[str], dict[str, object]],
) -> ToolHandler:
    def handler(invocation: ToolInvocation) -> ToolResult:
        raw_url = invocation.arguments.get("url")
        if not isinstance(raw_url, str) or not raw_url:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: url",
            )

        try:
            snapshot = browser_navigator(raw_url)
        except Exception as exc:  # pragma: no cover - caller-provided navigator guard
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=str(exc),
            )

        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output=snapshot,
            error=None,
        )

    return handler


def register_browser_plugin(
    registry: ToolRegistry,
    *,
    browser_navigator: Callable[[str], dict[str, object]],
) -> ToolRegistry:
    registry.require_plugin_registration_allowed()
    registry.mark_plugin_installed("browser_plugin")
    try:
        registry.get_spec("browser_navigate")
    except KeyError:
        registry.register(
            ToolSpec(
                name="browser_navigate",
                description=(
                    "Drive a real Chromium browser to a SPECIFIC URL you already have, "
                    "and return page metadata. Use this ONLY when (a) the user gave you "
                    "a URL, (b) you've already called web_search and need to read one of "
                    "its result pages in full, or (c) the page requires JavaScript to "
                    "render. Do NOT use this as a substitute for web_search — for any "
                    "open-ended 'find / look up / news / today / latest' question, call "
                    "web_search first; it's faster and returns multiple comparable sources."
                ),
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_browser_navigate_handler(browser_navigator),
        )
    return registry



def register_workspace_plugin(
    registry: ToolRegistry,
    *,
    workspace_root: str | Path | None = None,
    allowed_roots: list[Path] | tuple[Path, ...] | None = None,
) -> ToolRegistry:
    if workspace_root is None and allowed_roots is None:
        raise ValueError("workspace_plugin requires workspace_root or allowed_roots")
    registry.require_plugin_registration_allowed()
    registry.mark_plugin_installed("workspace_plugin")
    try:
        registry.get_spec("file_search")
    except KeyError:
        registry.register(
            ToolSpec(
                name="file_search",
                description="Search for local files inside the workspace.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_file_search_handler(
                workspace_root=workspace_root,
                allowed_roots=allowed_roots,
            ),
        )
    try:
        registry.get_spec("file_patch")
    except KeyError:
        registry.register(
            ToolSpec(
                name="file_patch",
                description="Replace text inside a local file within the workspace.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.WRITE,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_file_patch_handler(
                workspace_root=workspace_root,
                allowed_roots=allowed_roots,
            ),
        )
    try:
        registry.get_spec("workspace_summary")
    except KeyError:
        registry.register(
            ToolSpec(
                name="workspace_summary",
                description="Summarize local workspace contents inside the workspace scope.",
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=ToolSideEffectClass.READ,
                requires_approval=False,
                timeout_seconds=20,
            ),
            _build_workspace_summary_handler(
                workspace_root=workspace_root,
                allowed_roots=allowed_roots,
            ),
        )
    return registry



def register_browser_extension(
    registry: ToolRegistry,
    *,
    browser_navigator: Callable[[str], dict[str, object]],
) -> ToolRegistry:
    return register_browser_plugin(
        registry,
        browser_navigator=browser_navigator,
    )


def _build_set_reminder_handler(runtime, *, default_chat_id: str | None) -> ToolHandler:
    """Return a handler that schedules a reminder via the runtime store."""

    def handler(invocation: ToolInvocation) -> ToolResult:
        from nullion.reminders import (
            current_reminder_chat_id,
            due_at_from_relative_seconds,
            normalize_reminder_due_at,
            reminder_due_at_output,
        )

        text = invocation.arguments.get("text")
        due_at_str = invocation.arguments.get("due_at")
        due_in_seconds = invocation.arguments.get("due_in_seconds")
        chat_id = invocation.arguments.get("chat_id") or current_reminder_chat_id() or default_chat_id

        if not isinstance(text, str) or not text.strip():
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="Missing required argument: text",
            )
        has_due_at = isinstance(due_at_str, str) and bool(due_at_str.strip())
        relative_delay_seconds: float | None = None
        if isinstance(due_in_seconds, (int, float)) and not isinstance(due_in_seconds, bool):
            relative_delay_seconds = float(due_in_seconds)
        elif isinstance(due_in_seconds, str) and due_in_seconds.strip():
            try:
                relative_delay_seconds = float(due_in_seconds)
            except ValueError:
                relative_delay_seconds = None
        has_relative_delay = relative_delay_seconds is not None
        if not has_due_at and not has_relative_delay:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=(
                    "Missing required argument: due_at or due_in_seconds "
                    "(ISO 8601 datetime with timezone, or relative delay in seconds)"
                ),
            )
        if not chat_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="No chat_id available for reminder delivery.",
            )

        try:
            if has_relative_delay:
                due_at = due_at_from_relative_seconds(relative_delay_seconds)
            else:
                due_at = datetime.fromisoformat(str(due_at_str).replace("Z", "+00:00"))
                due_at = normalize_reminder_due_at(due_at)
        except (ValueError, TypeError, OverflowError) as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Invalid due_at datetime: {exc}",
            )

        try:
            task = runtime.schedule_reminder(
                chat_id=str(chat_id),
                text=text.strip(),
                due_at=due_at,
            )
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={
                    "task_id": task.task_id,
                    "text": text.strip(),
                    "chat_id": str(chat_id),
                    **reminder_due_at_output(due_at),
                },
            )
        except Exception as exc:  # noqa: BLE001
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Failed to schedule reminder: {exc}",
            )

    return handler


def _build_list_reminders_handler(runtime) -> ToolHandler:
    """Return a handler that lists all pending (undelivered) reminders."""

    def handler(invocation: ToolInvocation) -> ToolResult:
        try:
            all_reminders = runtime.store.list_reminders()
            pending = [
                {
                    "task_id": r.task_id,
                    "text": r.text,
                    "due_at": r.due_at.isoformat(),
                    "chat_id": r.chat_id,
                }
                for r in all_reminders
                if r.delivered_at is None
            ]
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={"reminders": pending, "count": len(pending)},
            )
        except Exception as exc:  # noqa: BLE001
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Failed to list reminders: {exc}",
            )

    return handler


def register_reminder_tools(
    registry: ToolRegistry,
    runtime,
    *,
    default_chat_id: str | None = None,
) -> None:
    """Register set_reminder and list_reminders tools into an existing ToolRegistry.

    These tools are runtime-store-aware: they require a live ``runtime`` reference
    because they write/read directly from ``runtime.store``.  Wire them in after
    the registry is created but before it is attached to the runtime.
    """
    registry.register(
        ToolSpec(
            name="set_reminder",
            description=(
                "Schedule a reminder message to be delivered at a specific time. "
                "Requires: text and either due_in_seconds for relative requests like "
                "'in 2 minutes', "
                "or due_at as an ISO 8601 datetime with timezone offset for absolute requests. "
                "If due_at has no offset, it is interpreted in the user's configured timezone. "
                "Optional: chat_id (defaults to operator chat)."
            ),
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=10,
            input_schema={
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Reminder message to deliver."},
                    "due_at": {
                        "type": "string",
                        "description": "Absolute ISO 8601 due time. Include timezone offset when known.",
                    },
                    "due_in_seconds": {
                        "type": "number",
                        "description": (
                            "Relative delay from the current moment, in seconds. "
                            "Prefer this for requests like 'in 2 minutes'."
                        ),
                    },
                    "chat_id": {"type": "string", "description": "Optional delivery chat ID."},
                },
                "required": ["text"],
                "additionalProperties": False,
            },
        ),
        _build_set_reminder_handler(runtime, default_chat_id=default_chat_id),
    )
    registry.register(
        ToolSpec(
            name="list_reminders",
            description="List all pending (not yet delivered) reminders.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=10,
        ),
        _build_list_reminders_handler(runtime),
    )


# ── Cron tools ────────────────────────────────────────────────────────────────

def _build_create_cron_handler(*, default_delivery_channel: str = "", default_delivery_target: str = ""):
    def _workspace_id_from_invocation(invocation: ToolInvocation, args: dict[str, object]) -> str:
        explicit = str(args.get("workspace_id") or "").strip()
        if explicit:
            return explicit
        try:
            from nullion.connections import workspace_id_for_principal

            return workspace_id_for_principal(invocation.principal_id)
        except Exception:
            return "workspace_admin"

    def handle(invocation: ToolInvocation) -> ToolResult:
        from nullion.cron_delivery import normalize_cron_delivery_channel
        from nullion.crons import add_cron
        args = invocation.arguments or {}
        name     = str(args.get("name", "")).strip()
        schedule = str(args.get("schedule", "")).strip()
        task     = str(args.get("task", "")).strip()
        enabled  = bool(args.get("enabled", True))
        workspace_id = _workspace_id_from_invocation(invocation, args)
        default_channel = normalize_cron_delivery_channel(default_delivery_channel)
        delivery_channel = normalize_cron_delivery_channel(args.get("delivery_channel")) or default_channel
        explicit_target = str(args.get("delivery_target") or "").strip()
        delivery_target = explicit_target
        if not delivery_target and (not delivery_channel or delivery_channel == default_channel):
            delivery_target = str(default_delivery_target or "").strip()
        if not name or not schedule or not task:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="name, schedule and task are all required",
            )
        try:
            job = add_cron(
                name=name,
                schedule=schedule,
                task=task,
                enabled=enabled,
                delivery_channel=delivery_channel,
                delivery_target=delivery_target,
                workspace_id=workspace_id,
            )
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error=f"Failed to create cron: {exc}",
            )
        next_info = f"  Next run: {job.next_run}" if job.next_run else ""
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={
                "id": job.id,
                "name": job.name,
                "schedule": job.schedule,
                "task": job.task,
                "workspace_id": job.workspace_id,
                "delivery_channel": job.delivery_channel,
                "delivery_target": job.delivery_target,
                "enabled": job.enabled,
                "next_run": job.next_run,
                "message": f"Cron created: '{job.name}' (id={job.id}) in workspace {job.workspace_id} — runs `{job.schedule}`{next_info}",
            },
            error=None,
        )
    return handle


def _build_list_crons_handler():
    def handle(invocation: ToolInvocation) -> ToolResult:
        from nullion.connections import workspace_id_for_principal
        from nullion.crons import list_crons

        args = invocation.arguments or {}
        include_all = bool(args.get("include_all_workspaces", False))
        workspace_id = str(args.get("workspace_id") or "").strip() or workspace_id_for_principal(invocation.principal_id)
        jobs = list_crons(workspace_id=None if include_all else workspace_id)
        if not jobs:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="completed",
                output={"crons": [], "message": "No crons scheduled."},
                error=None,
            )
        lines = []
        crons = []
        for j in jobs:
            status = "✓ enabled" if j.enabled else "✗ disabled"
            next_r = f"  next={j.next_run}" if j.next_run else ""
            lines.append(f"  [{j.id}] {j.name} | workspace={j.workspace_id} | {j.schedule} | {status}{next_r}\n    task: {j.task}")
            crons.append(
                {
                    "id": j.id,
                    "name": j.name,
                    "schedule": j.schedule,
                    "task": j.task,
                    "workspace_id": j.workspace_id,
                    "delivery_channel": j.delivery_channel,
                    "delivery_target": j.delivery_target,
                    "enabled": j.enabled,
                    "next_run": j.next_run,
                    "last_run": j.last_run,
                    "last_result": j.last_result,
                }
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"crons": crons, "message": "\n".join(lines)},
            error=None,
        )
    return handle


def _build_delete_cron_handler():
    def handle(invocation: ToolInvocation) -> ToolResult:
        from nullion.connections import workspace_id_for_principal
        from nullion.crons import get_cron, remove_cron
        args = invocation.arguments or {}
        cron_id = str(args.get("id", "")).strip()
        if not cron_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="id is required",
            )
        job = get_cron(cron_id)
        workspace_id = workspace_id_for_principal(invocation.principal_id)
        if job is not None and job.workspace_id != workspace_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": cron_id, "workspace_id": job.workspace_id},
                error=f"Cron {cron_id!r} belongs to workspace {job.workspace_id}.",
            )
        removed = remove_cron(cron_id)
        if not removed:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": cron_id},
                error=f"No cron found with id={cron_id!r}",
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={"id": cron_id, "message": f"Cron {cron_id} deleted."},
            error=None,
        )
    return handle


def _build_toggle_cron_handler():
    def handle(invocation: ToolInvocation) -> ToolResult:
        from nullion.connections import workspace_id_for_principal
        from nullion.crons import get_cron, toggle_cron
        args = invocation.arguments or {}
        cron_id = str(args.get("id", "")).strip()
        enabled = bool(args.get("enabled", True))
        if not cron_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="id is required",
            )
        existing = get_cron(cron_id)
        workspace_id = workspace_id_for_principal(invocation.principal_id)
        if existing is not None and existing.workspace_id != workspace_id:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": cron_id, "workspace_id": existing.workspace_id},
                error=f"Cron {cron_id!r} belongs to workspace {existing.workspace_id}.",
            )
        job = toggle_cron(cron_id, enabled)
        if job is None:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": cron_id},
                error=f"No cron found with id={cron_id!r}",
            )
        state = "enabled" if enabled else "disabled"
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output={
                "id": job.id,
                "name": job.name,
                "workspace_id": job.workspace_id,
                "enabled": job.enabled,
                "message": f"Cron '{job.name}' ({cron_id}) is now {state}.",
            },
            error=None,
        )
    return handle


def _build_run_cron_handler(cron_runner: Callable[[object], str | dict[str, object] | None] | None):
    def _cron_lookup_parts(value: object) -> tuple[str, tuple[str, ...]]:
        text = str(value or "").casefold()
        tokens: list[str] = []
        current: list[str] = []
        for char in text:
            if char.isalnum():
                current.append(char)
            elif current:
                tokens.append("".join(current))
                current = []
        if current:
            tokens.append("".join(current))
        return "".join(tokens), tuple(tokens)

    def _cron_name_match_rank(query: str, candidate_name: str) -> int | None:
        query_text = str(query or "").strip()
        candidate_text = str(candidate_name or "").strip()
        if not query_text or not candidate_text:
            return None
        if query_text.casefold() == candidate_text.casefold():
            return 0

        query_compact, query_tokens = _cron_lookup_parts(query_text)
        candidate_compact, candidate_tokens = _cron_lookup_parts(candidate_text)
        if not query_compact or not candidate_compact:
            return None
        if query_compact == candidate_compact:
            return 1
        if query_tokens and query_tokens == candidate_tokens:
            return 1
        if query_tokens and all(token in candidate_tokens for token in query_tokens):
            return 2
        if len(query_compact) >= 6 and query_compact in candidate_compact:
            return 3
        return None

    def _unique_cron_name_match(query: str, jobs: list[object]) -> tuple[object | None, list[object]]:
        ranked: list[tuple[int, object]] = []
        for candidate in jobs:
            rank = _cron_name_match_rank(query, str(getattr(candidate, "name", "") or ""))
            if rank is not None:
                ranked.append((rank, candidate))
        if not ranked:
            return None, []
        best_rank = min(rank for rank, _candidate in ranked)
        matches = [candidate for rank, candidate in ranked if rank == best_rank]
        if len(matches) == 1:
            return matches[0], matches
        return None, matches

    def _path_from_runner_artifact(value: object) -> str | None:
        if isinstance(value, str) and value.strip():
            return value
        if isinstance(value, dict):
            for key in ("path", "file_path", "artifact_path"):
                path = value.get(key)
                if isinstance(path, str) and path.strip():
                    return path
        return None

    def _artifact_paths_from_runner_output(runner_output: str | dict[str, object] | None) -> list[str]:
        paths: list[str] = []
        raw_paths: list[object] = []
        if isinstance(runner_output, dict):
            for key in ("artifact_paths", "artifacts"):
                value = runner_output.get(key)
                if isinstance(value, list | tuple):
                    raw_paths.extend(value)
                else:
                    raw_paths.append(value)
            text = str(runner_output.get("text") or runner_output.get("result_text") or "")
        else:
            text = runner_output if isinstance(runner_output, str) else ""
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("MEDIA:"):
                raw_paths.append(stripped.removeprefix("MEDIA:").strip())
        for raw_artifact in raw_paths:
            raw_path = _path_from_runner_artifact(raw_artifact)
            if not isinstance(raw_path, str) or not raw_path.strip():
                continue
            path = Path(raw_path).expanduser()
            try:
                from nullion.artifacts import is_safe_artifact_path

                if not is_safe_artifact_path(path):
                    continue
            except Exception:
                continue
            normalized = str(path.resolve(strict=False))
            if normalized not in paths:
                paths.append(normalized)
        return paths

    def handle(invocation: ToolInvocation) -> ToolResult:
        from datetime import timezone

        from nullion.connections import workspace_id_for_principal
        from nullion.crons import get_cron, list_crons, load_crons, save_crons

        args = invocation.arguments or {}
        raw_id = str(args.get("id", "")).strip()
        raw_name = str(args.get("name", "")).strip()
        if not raw_id and not raw_name:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={},
                error="id or name is required",
            )
        workspace_id = workspace_id_for_principal(invocation.principal_id)
        job = get_cron(raw_id) if raw_id else None
        if job is not None and job.workspace_id != workspace_id:
            job = None
        if job is None and raw_name:
            job, matches = _unique_cron_name_match(raw_name, list_crons(workspace_id=workspace_id))
            if job is None and matches:
                return ToolResult(
                    invocation_id=invocation.invocation_id,
                    tool_name=invocation.tool_name,
                    status="failed",
                    output={
                        "name": raw_name,
                        "matches": [
                            {
                                "id": item.id,
                                "name": item.name,
                                "schedule": item.schedule,
                                "workspace_id": item.workspace_id,
                                "enabled": item.enabled,
                                "next_run": item.next_run,
                            }
                            for item in matches
                        ],
                    },
                    error="Multiple crons matched that name; use id from list_crons.",
                )
        if job is None:
            lookup = raw_id or raw_name
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": raw_id, "name": raw_name},
                error=f"No cron found for {lookup!r}",
            )
        if cron_runner is None:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={
                    "id": job.id,
                    "name": job.name,
                    "task": job.task,
                    "reason": "cron_runner_not_configured",
                },
                error="This runtime can list crons but cannot run them on demand.",
            )
        try:
            runner_output = cron_runner(job)
        except Exception as exc:
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output={"id": job.id, "name": job.name},
                error=f"Failed to run cron: {exc}",
            )
        runner_failed = False
        runner_failure_reason = ""
        if isinstance(runner_output, dict):
            if runner_output.get("reached_iteration_limit"):
                runner_failed = True
                runner_failure_reason = "cron_run_reached_iteration_limit"
            elif runner_output.get("cron_delivery_failed"):
                runner_failed = True
                runner_failure_reason = "cron_delivery_failed"
            elif runner_output.get("suspended_for_approval"):
                runner_failed = True
                runner_failure_reason = "cron_run_waiting_for_approval"
        jobs = load_crons()
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        from nullion.response_fulfillment_contract import guaranteed_user_visible_text

        result_text = guaranteed_user_visible_text(subject=job, output=runner_output, kind="cron")
        for stored in jobs:
            if stored.id == job.id:
                stored.last_run = now
                stored.last_result = (
                    f"manual run failed: {runner_failure_reason}"
                    if runner_failed
                    else result_text
                )
                break
        save_crons(jobs)
        output: dict[str, object] = {
            "id": job.id,
            "name": job.name,
            "task": job.task,
            "workspace_id": job.workspace_id,
            "last_run": now,
            "message": f"Ran cron '{job.name}' ({job.id}) now.",
            "result_text": result_text,
        }
        if isinstance(runner_output, dict):
            output["result"] = runner_output
        artifact_paths = _artifact_paths_from_runner_output(runner_output)
        if artifact_paths:
            output["artifact_paths"] = artifact_paths
        if runner_failed:
            output["reason"] = runner_failure_reason
            return ToolResult(
                invocation_id=invocation.invocation_id,
                tool_name=invocation.tool_name,
                status="failed",
                output=output,
                error=(
                    "Cron run did not finish cleanly; check the Doctor action or retry after cleanup."
                    if runner_failure_reason == "cron_run_reached_iteration_limit"
                    else "Cron run did not deliver its result to the configured platform."
                    if runner_failure_reason == "cron_delivery_failed"
                    else "Cron run is waiting for approval."
                ),
            )
        return ToolResult(
            invocation_id=invocation.invocation_id,
            tool_name=invocation.tool_name,
            status="completed",
            output=output,
            error=None,
        )
    return handle


def register_cron_tools(
    registry,
    *,
    cron_runner: Callable[[object], str | dict[str, object] | None] | None = None,
    default_delivery_channel: str = "",
    default_delivery_target: str = "",
) -> None:
    """Register cron management tools into an existing ToolRegistry.

    Call this after building the registry so the agent can create, list,
    toggle and delete scheduled cron jobs.
    """
    registry.register(
        ToolSpec(
            name="create_cron",
            description=(
                "Create a new scheduled cron job. "
                "Required args: name (human-readable label), schedule (5-field cron expression, "
                "e.g. '0 9 * * 1-5' for weekdays at 9 AM), task (the natural-language instruction "
                "Nullion will execute when the cron fires). "
                "Optional: enabled (bool, default true), workspace_id, delivery_channel, delivery_target. "
                "If workspace and delivery are omitted, Nullion uses the current workspace and chat adapter defaults."
            ),
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=10,
        ),
        _build_create_cron_handler(
            default_delivery_channel=default_delivery_channel,
            default_delivery_target=default_delivery_target,
        ),
    )
    registry.register(
        ToolSpec(
            name="list_crons",
            description=(
                "List scheduled cron jobs for the current workspace with their id, workspace, schedule, "
                "enabled state, and next run time. Optional args: workspace_id, include_all_workspaces."
            ),
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ,
            requires_approval=False,
            timeout_seconds=10,
        ),
        _build_list_crons_handler(),
    )
    registry.register(
        ToolSpec(
            name="delete_cron",
            description="Delete a scheduled cron job by id. Required args: id (the cron job id).",
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=10,
        ),
        _build_delete_cron_handler(),
    )
    registry.register(
        ToolSpec(
            name="toggle_cron",
            description=(
                "Enable or disable a scheduled cron job. "
                "Required args: id (the cron job id), enabled (bool — true to enable, false to disable)."
            ),
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=10,
        ),
        _build_toggle_cron_handler(),
    )
    registry.register(
        ToolSpec(
            name="run_cron",
            description=(
                "Run an existing scheduled cron job immediately. Prefer id from list_crons. "
                "Required args: id or name. Name matching is conservative and punctuation-insensitive; "
                "ambiguous names return candidate ids instead of running a job."
            ),
            risk_level=ToolRiskLevel.MEDIUM,
            side_effect_class=ToolSideEffectClass.WRITE,
            requires_approval=False,
            timeout_seconds=120,
        ),
        _build_run_cron_handler(cron_runner),
    )


__all__ = [
    "TerminalBackendDescriptor",
    "ToolExecutor",
    "ToolInvocation",
    "ToolRegistry",
    "ToolResult",
    "ToolRiskLevel",
    "ToolSideEffectClass",
    "ToolSpec",
    "build_default_tool_registry",
    "create_core_tool_registry",
    "create_default_tool_registry",
    "create_plugin_tool_registry",
    "create_extension_tool_registry",
    "normalize_tool_result",
    "normalize_tool_status",
    "register_browser_plugin",
    "register_browser_extension",
    "register_calendar_plugin",
    "register_connector_plugin",
    "register_email_plugin",
    "register_media_plugin",
    "register_search_plugin",
    "register_reminder_tools",
    "register_web_extension",
    "register_workspace_plugin",
]
